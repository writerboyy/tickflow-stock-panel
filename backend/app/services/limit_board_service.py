"""First-board discovery and selected-symbol limit-board tracking."""
from __future__ import annotations

import logging
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.price_limits import is_risk_warning_name, limit_price, price_limit_pct
from app.services import alert_store
from app.services.limit_board_store import LimitBoardStore


logger = logging.getLogger(__name__)
_ACCOUNT_ID = "limit_board"
_DEPTH_FRESH_SECONDS = 30


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _quote_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, CN_TZ)
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(CN_TZ) if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ)
    return None


def _is_trading_time(value: datetime) -> bool:
    current = value.timetz().replace(tzinfo=None)
    return (
        clock_time(9, 30) <= current < clock_time(11, 30)
        or clock_time(13, 0) <= current < clock_time(15, 0)
    )


class LimitBoardService:
    def __init__(self, data_dir: Path, repo: Any, quote_service: Any, app_state: Any) -> None:
        self.store = LimitBoardStore(data_dir)
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._lock = threading.RLock()
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=3)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._polling_lease = False
        self._ws_registered = False
        self._ws_symbols: set[str] = set()
        self._quotes: dict[str, dict[str, Any]] = {}
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=3))
        self._history_date: date | None = None
        self._first_board_eligible: set[str] = set()
        self._history_ready = False
        self._history_reason = "正在读取近 10 个交易日涨停记录"
        self._last_scan_at: str | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.quote_service.add_fetch_listener(self._on_market_fetch)
        self._refresh_selected_consumer()
        try:
            self.quote_service.acquire_temporary_polling(
                max(1.0, float(self.quote_service.get_min_interval()))
            )
            self._polling_lease = True
        except ValueError as exc:
            self._last_error = str(exc)
        hub = self._hub()
        if hub is not None:
            hub.add_depth_listener(self.enqueue_depth)
        self._thread = threading.Thread(target=self._worker, name="limit-board", daemon=True)
        self._thread.start()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes()})

    def stop(self) -> None:
        self.quote_service.remove_fetch_listener(self._on_market_fetch)
        self.quote_service.remove_symbol_consumer(_ACCOUNT_ID)
        hub = self._hub()
        if hub is not None:
            hub.remove_depth_listener(self.enqueue_depth)
            if self._ws_registered:
                try:
                    hub.unregister(_ACCOUNT_ID)
                except Exception:  # noqa: BLE001
                    logger.debug("打板专区移除 WS 订阅失败", exc_info=True)
        self._ws_registered = False
        self._ws_symbols.clear()
        if self._polling_lease:
            self.quote_service.release_temporary_polling()
            self._polling_lease = False
        self._stop.set()
        self._enqueue({"type": "stop"})
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _hub(self):
        return getattr(getattr(self.app_state, "paper_supervisor", None), "hub", None)

    def _enqueue(self, payload: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait(payload)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            pass

    def _on_market_fetch(self) -> None:
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes()})

    def _market_mode(self) -> str:
        mode_getter = getattr(self.quote_service, "realtime_mode", None)
        if not callable(mode_getter):
            return "full_market"
        try:
            return str(mode_getter())
        except Exception:  # noqa: BLE001
            return "none"

    def enqueue_depth(self, records: list[dict[str, Any]]) -> None:
        with self._lock:
            ws_symbols = set(self._ws_symbols)
        selected = [
            dict(record) for record in records
            if str(record.get("symbol") or "").strip().upper() in ws_symbols
        ]
        if selected:
            self._enqueue({"type": "depth", "records": selected})

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                payload = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            if payload.get("type") == "stop":
                break
            try:
                if payload.get("type") == "depth":
                    self._process_depth(payload.get("records") or [])
                else:
                    self._process_quotes(payload.get("quotes") or [])
            except Exception:  # noqa: BLE001
                logger.exception("打板专区行情处理失败")
                self._last_error = "行情处理失败，请查看后端日志"

    def _runtime_for_today(self) -> dict[str, Any]:
        today = cn_today().isoformat()
        runtime = self.store.load_runtime()
        if runtime.get("trading_date") != today:
            runtime = {"trading_date": today, "symbols": {}, "blacklist": []}
            self.store.save_runtime(runtime)
            self._depth.clear()
        return runtime

    def _refresh_history(self, config: dict[str, Any]) -> None:
        today = cn_today()
        if self._history_date == today and self._history_ready:
            return
        self._history_date = today
        self._history_ready = False
        self._first_board_eligible.clear()
        lookback = max(1, int(config["settings"].get("first_board_lookback_days", 10)))
        latest, latest_date = self.repo.get_enriched_latest()
        if latest_date is None:
            self._history_reason = "历史指标缓存尚未就绪，首板扫描已暂停"
            return
        end = min(latest_date, today - timedelta(days=1))
        start = end - timedelta(days=max(30, lookback * 3))
        history = self.repo.get_enriched_range(
            start, end, columns=["symbol", "date", "signal_limit_up"],
        )
        if history is None or history.is_empty() or "signal_limit_up" not in history.columns:
            self._history_reason = "近 10 个交易日涨停记录不足，首板扫描已暂停"
            return
        dates = history["date"].unique().sort().to_list()
        if len(dates) < lookback:
            self._history_reason = f"仅有 {len(dates)} 个交易日记录，需要 {lookback} 个"
            return
        selected_dates = dates[-lookback:]
        scoped = history.filter(pl.col("date").is_in(selected_dates))
        blocked = set(
            scoped.filter(pl.col("signal_limit_up").fill_null(False).cast(pl.Boolean))["symbol"].to_list()
        )
        instruments = self.repo.get_instruments()
        universe = set(instruments["symbol"].to_list()) if not instruments.is_empty() else set()
        self._first_board_eligible = universe - blocked
        self._history_ready = True
        self._history_reason = f"已核对前 {lookback} 个交易日"

    def _process_quotes(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        self._refresh_history(config)
        full_market = self._market_mode() == "full_market"
        runtime = self._runtime_for_today()
        runtime_symbols = set(runtime.get("symbols") or {})
        selected = {str(item["symbol"]).strip().upper() for item in config["selected"]}
        names = self.repo.get_name_map(list(selected)) if selected else {}
        now = cn_now()
        updates: dict[str, dict[str, Any]] = {}
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            quote_at = _quote_time(raw.get("timestamp"))
            if quote_at is None or quote_at.date() != now.date():
                continue
            price = _finite(raw.get("last_price", raw.get("close")))
            if price is None or price <= 0:
                continue
            name = str(raw.get("name") or names.get(symbol) or symbol)
            limit_up = self._limit_up(raw, symbol, name, now.date())
            if limit_up is None:
                continue
            gap = max(0.0, limit_up / price - 1.0)
            source_modes = []
            if symbol in selected:
                source_modes.append("selected")
            scan_window = float(config["settings"].get("exit_limit_pct", 0.03))
            if (
                full_market
                and self._history_ready
                and symbol in self._first_board_eligible
                and (gap <= scan_window or symbol in runtime_symbols)
            ):
                source_modes.append("first_board")
            if not source_modes:
                continue
            quote = {
                **raw,
                "symbol": symbol,
                "name": name,
                "last_price": price,
                "limit_up": limit_up,
                "limit_gap_pct": gap,
                "timestamp": quote_at.isoformat(),
                "source_modes": source_modes,
            }
            updates[symbol] = quote
        with self._lock:
            self._quotes.update(updates)
        self._evaluate_quotes(updates, runtime, config)
        self._sync_websocket(runtime, config)
        self._last_scan_at = now.isoformat()
        self._persist_runtime(runtime)
        self._notify_updated()

    @staticmethod
    def _limit_up(raw: dict[str, Any], symbol: str, name: str, trading_date: date) -> float | None:
        authoritative = _finite(raw.get("limit_up"))
        if authoritative is not None:
            if authoritative >= 10_000:
                return None
            if authoritative > 0:
                return authoritative
        previous = _finite(raw.get("prev_close"))
        if previous is None or previous <= 0:
            return None
        pct = price_limit_pct(
            symbol, trading_date, is_risk_warning=is_risk_warning_name(name),
        )
        return limit_price(previous, pct, up=True)

    def _evaluate_quotes(
        self, updates: dict[str, dict[str, Any]], runtime: dict[str, Any], config: dict[str, Any],
    ) -> None:
        now = cn_now()
        if not _is_trading_time(now):
            return
        symbols = runtime.setdefault("symbols", {})
        blacklist = set(runtime.setdefault("blacklist", []))
        for symbol, quote in updates.items():
            quote_at = _quote_time(quote.get("timestamp"))
            now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
            if quote_at is None or (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS:
                continue
            state = symbols.setdefault(symbol, {})
            state.update({
                "name": quote["name"],
                "last_price": quote["last_price"],
                "limit_up": quote["limit_up"],
                "limit_gap_pct": quote["limit_gap_pct"],
                "source_modes": quote["source_modes"],
                "last_quote_at": quote["timestamp"],
            })
            if symbol in blacklist:
                state["status"] = "blacklisted"
                continue
            at_limit = quote["last_price"] >= quote["limit_up"] - 0.005
            if at_limit and not state.get("touched"):
                state["touched"] = True
                state["touched_at"] = now.isoformat()
                state["status"] = "touched"
                self._emit("touched", quote, state, config, "首次触及涨停价")
            if state.get("sealed") and not at_limit:
                self._mark_broken(quote, state, runtime, config, "价格离开涨停价")
            elif state.get("sealed"):
                state["status"] = "sealed"
            elif state.get("had_broken"):
                state["status"] = "broken"
            elif at_limit:
                state["status"] = "touched"
            elif quote["limit_gap_pct"] <= float(config["settings"].get("near_limit_pct", 0.02)):
                state["status"] = "near_limit"
            else:
                state["status"] = "watching"

    def _process_depth(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        now = cn_now()
        if not _is_trading_time(now):
            return
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            quote = self._quotes.get(symbol)
            state = runtime.setdefault("symbols", {}).get(symbol)
            if not quote or not state or symbol in set(runtime.get("blacklist") or []):
                continue
            quote_at = _quote_time(quote.get("timestamp"))
            now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
            if quote_at is None or (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS:
                continue
            normalized = self._normalize_depth(raw, now)
            if normalized is None:
                continue
            self._depth[symbol].append(normalized)
            recent = list(self._depth[symbol])
            if any((now - item["timestamp"]).total_seconds() > _DEPTH_FRESH_SECONDS for item in recent):
                continue
            sealed_flags = [self._sealed_snapshot(item, float(quote["limit_up"])) for item in recent]
            confirmed = len(sealed_flags) == 3 and all(sealed_flags)
            latest_sealed = sealed_flags[-1]
            state["bid1_volume"] = normalized["bid_volumes"][0] if normalized["bid_volumes"] else 0.0
            state["ask1_volume"] = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
            state["last_depth_at"] = normalized["timestamp"].isoformat()
            if state.get("sealed") and not latest_sealed:
                self._mark_broken(quote, state, runtime, config, "卖一恢复，封板状态中断")
            elif confirmed and not state.get("sealed"):
                state["sealed"] = True
                state["sealed_at"] = now.isoformat()
                if state.get("had_broken"):
                    state["had_broken"] = False
                    state["status"] = "resealed"
                    self._emit("resealed", quote, state, config, "连续 3 个五档快照确认回封")
                else:
                    state["status"] = "sealed"
            elif state.get("sealed"):
                state["status"] = "sealed"
        self._sync_websocket(runtime, config)
        self._persist_runtime(runtime)
        self._notify_updated()

    @staticmethod
    def _normalize_depth(raw: dict[str, Any], now: datetime) -> dict[str, Any] | None:
        try:
            bid_prices = [float(value) for value in (raw.get("bid_prices") or [])[:5]]
            bid_volumes = [float(value) for value in (raw.get("bid_volumes") or [])[:5]]
            ask_prices = [float(value) for value in (raw.get("ask_prices") or [])[:5]]
            ask_volumes = [float(value) for value in (raw.get("ask_volumes") or [])[:5]]
        except (TypeError, ValueError):
            return None
        timestamp = _quote_time(raw.get("timestamp")) or now
        return {
            "timestamp": timestamp,
            "bid_prices": bid_prices,
            "bid_volumes": bid_volumes,
            "ask_prices": ask_prices,
            "ask_volumes": ask_volumes,
        }

    @staticmethod
    def _sealed_snapshot(depth: dict[str, Any], limit_up: float) -> bool:
        bid_price = depth["bid_prices"][0] if depth["bid_prices"] else 0.0
        bid_volume = depth["bid_volumes"][0] if depth["bid_volumes"] else 0.0
        ask_price = depth["ask_prices"][0] if depth["ask_prices"] else 0.0
        ask_volume = depth["ask_volumes"][0] if depth["ask_volumes"] else 0.0
        return abs(bid_price - limit_up) < 0.001 and bid_volume > 0 and not (ask_price and ask_volume)

    def _mark_broken(
        self,
        quote: dict[str, Any],
        state: dict[str, Any],
        runtime: dict[str, Any],
        config: dict[str, Any],
        reason: str,
    ) -> None:
        if not state.get("sealed"):
            return
        state["sealed"] = False
        state["had_broken"] = True
        state["status"] = "broken"
        state["break_count"] = int(state.get("break_count") or 0) + 1
        state["last_broken_at"] = cn_now().isoformat()
        threshold = int(config["settings"].get("blacklist_after_breaks", 3))
        blacklisted = state["break_count"] > threshold
        if blacklisted:
            state["status"] = "blacklisted"
            state["blacklisted_at"] = cn_now().isoformat()
            values = set(runtime.setdefault("blacklist", []))
            values.add(quote["symbol"])
            runtime["blacklist"] = sorted(values)
            reason = f"{reason}；今日第 {state['break_count']} 次炸板，已纳入黑名单"
        else:
            reason = f"{reason}；今日第 {state['break_count']} 次炸板"
        self._emit("broken", quote, state, config, reason)

    def _sync_websocket(self, runtime: dict[str, Any], config: dict[str, Any]) -> None:
        hub = self._hub()
        if hub is None:
            self._last_error = "共享 WebSocket Hub 不可用"
            return
        blacklist = set(runtime.get("blacklist") or [])
        near = float(config["settings"].get("near_limit_pct", 0.02))
        exit_gap = float(config["settings"].get("exit_limit_pct", 0.03))
        exit_sustain = float(config["settings"].get("exit_sustain_seconds", 30))
        now = cn_now()
        states = runtime.setdefault("symbols", {})
        retained: set[str] = set()
        for symbol in self._ws_symbols:
            quote = self._quotes.get(symbol)
            state = states.setdefault(symbol, {})
            if quote is None or symbol in blacklist:
                state.pop("ws_exit_since", None)
                continue
            if float(quote.get("limit_gap_pct") or 1) <= exit_gap:
                state.pop("ws_exit_since", None)
                retained.add(symbol)
                continue
            exit_since = _quote_time(state.get("ws_exit_since"))
            if exit_since is None:
                state["ws_exit_since"] = now.isoformat()
                retained.add(symbol)
            elif (now - exit_since).total_seconds() < exit_sustain:
                retained.add(symbol)
        candidates = [
            quote for symbol, quote in self._quotes.items()
            if symbol not in blacklist
            and (float(quote.get("limit_gap_pct") or 1) <= near or symbol in retained)
        ]
        candidates.sort(key=lambda item: (
            0 if "selected" in item.get("source_modes", []) else 1,
            float(item.get("limit_gap_pct") or 1),
            -float(item.get("amount") or 0),
        ))
        available = hub.websocket_available(exclude=_ACCOUNT_ID)
        desired = {str(item["symbol"]) for item in candidates[:available]}
        try:
            if not desired:
                if self._ws_registered:
                    hub.unregister(_ACCOUNT_ID)
                self._ws_registered = False
                self._ws_symbols.clear()
                return
            if self._ws_registered:
                hub.update_symbols(_ACCOUNT_ID, desired)
            else:
                hub.register(_ACCOUNT_ID, "websocket", desired, "stock", self._queue)
                self._ws_registered = True
            self._ws_symbols = desired
            self._last_error = None
        except (ValueError, RuntimeError) as exc:
            self._last_error = str(exc)

    def _emit(
        self, event_type: str, quote: dict[str, Any], state: dict[str, Any], config: dict[str, Any], reason: str,
    ) -> None:
        labels = {"touched": "触板", "broken": "炸板", "resealed": "回封"}
        now = cn_now()
        event = {
            "ts": int(now.timestamp() * 1000),
            "trading_date": now.date().isoformat(),
            "source": "limit_board",
            "type": event_type,
            "rule_id": f"limit_board_{event_type}",
            "rule_name": labels[event_type],
            "symbol": quote["symbol"],
            "name": quote["name"],
            "message": f"{quote['name']}：{labels[event_type]}",
            "severity": "critical" if event_type == "broken" else "warn",
            "price": quote["last_price"],
            "limit_up": quote["limit_up"],
            "limit_gap_pct": quote["limit_gap_pct"],
            "break_count": int(state.get("break_count") or 0),
            "blacklisted": state.get("status") == "blacklisted",
            "reasons": [reason],
        }
        self.store.append_event(event)
        alert_store.append(self.store.root.parents[1], event)
        enabled = bool(config["settings"].get("notifications", {}).get(event_type, True))
        if enabled:
            publish = getattr(self.quote_service, "publish_external_alerts", None)
            if callable(publish):
                publish([event])
            else:
                self.quote_service.push_alerts([event])

    def _persist_runtime(self, runtime: dict[str, Any]) -> None:
        self.store.save_runtime(runtime)

    def _notify_updated(self) -> None:
        notify = getattr(self.quote_service, "notify_limit_board_updated", None)
        if callable(notify):
            notify()

    def _refresh_selected_consumer(self) -> None:
        config = self.store.load_config()
        symbols = {str(item["symbol"]).strip().upper() for item in config["selected"]}
        self.quote_service.set_symbol_consumer(_ACCOUNT_ID, symbols)

    def view(self) -> dict[str, Any]:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        rows = []
        for symbol, state in runtime.get("symbols", {}).items():
            modes = state.get("source_modes") or []
            if not modes:
                continue
            rows.append({"symbol": symbol, **state, "ws_active": symbol in self._ws_symbols})
        rows.sort(key=lambda item: (
            0 if item.get("status") == "blacklisted" else 1,
            float(item.get("limit_gap_pct") or 1),
        ))
        selected = []
        runtime_by_symbol = runtime.get("symbols", {})
        for item in config["selected"]:
            symbol = str(item["symbol"]).strip().upper()
            selected.append({**item, **runtime_by_symbol.get(symbol, {}), "ws_active": symbol in self._ws_symbols})
        hub = self._hub()
        capacity = hub.websocket_capacity() if hub is not None else 0
        return {
            "revision": config["revision"],
            "settings": config["settings"],
            "first_board": [item for item in rows if "first_board" in item.get("source_modes", [])],
            "selected": selected,
            "blacklist": runtime.get("blacklist", []),
            "events": self.store.events(runtime["trading_date"]),
            "runtime": {
                "trading_date": runtime["trading_date"],
                "history_ready": self._history_ready,
                "history_reason": self._history_reason,
                "last_scan_at": self._last_scan_at,
                "last_error": self._last_error,
                "websocket_status": "connected" if self._ws_registered else "idle",
                "websocket_symbols": len(self._ws_symbols),
                "websocket_capacity": capacity,
                "trading_enabled": False,
                "trading_reason": "券商交易接口待接入",
                "market_mode": self._market_mode(),
                "first_board_enabled": self._market_mode() == "full_market" and self._history_ready,
            },
        }

    def add_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        names = self.repo.get_name_map([cleaned])
        if cleaned not in names or self.repo.resolve_asset_type(cleaned) != "stock":
            raise ValueError("仅支持本地股票主数据中的 A 股标的")

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["selected"]):
                return
            value["selected"].append({
                "symbol": cleaned,
                "name": names[cleaned],
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        self._refresh_selected_consumer()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def remove_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "selected", [item for item in value["selected"] if str(item.get("symbol")) != cleaned],
            ),
        )
        self._refresh_selected_consumer()
        self._notify_updated()
        return saved
