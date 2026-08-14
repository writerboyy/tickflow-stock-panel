"""First-board discovery and selected-symbol limit-board tracking."""
from __future__ import annotations

import logging
import queue
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
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
        self._order_results: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=10)
        self._order_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="limit-board-order")
        self._order_slots = threading.BoundedSemaphore(4)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._polling_lease = False
        self._ws_registered = False
        self._ws_symbols: set[str] = set()
        self._quotes: dict[str, dict[str, Any]] = {}
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=3))
        self._history_date: date | None = None
        self._name_map_date: date | None = None
        self._name_map: dict[str, str] = {}
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
        self._refresh_symbol_consumer()
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
        self._order_executor.shutdown(wait=False, cancel_futures=True)

    def _hub(self):
        return getattr(getattr(self.app_state, "paper_supervisor", None), "hub", None)

    def _qmt(self):
        return getattr(self.app_state, "qmt_trading_service", None)

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
                order_result = self._order_results.get_nowait()
            except queue.Empty:
                order_result = None
            if order_result is not None:
                try:
                    self._apply_auto_order_result(order_result)
                except Exception:  # noqa: BLE001
                    logger.exception("打板自动委托结果处理失败")
                continue
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
        self._refresh_name_map()
        universe = {
            symbol for symbol in universe
            if not is_risk_warning_name(self._name_map.get(str(symbol).strip().upper()))
        }
        self._first_board_eligible = universe - blocked
        self._history_ready = True
        self._history_reason = f"已核对前 {lookback} 个交易日"

    def _refresh_name_map(self) -> None:
        today = cn_today()
        if self._name_map_date == today:
            return
        try:
            names = self.repo.get_name_map()
        except Exception:  # noqa: BLE001
            logger.warning("打板专区读取本地证券名称失败", exc_info=True)
            names = {}
        self._name_map = {
            str(symbol).strip().upper(): str(name).strip()
            for symbol, name in names.items()
            if str(symbol).strip() and str(name).strip()
        }
        self._name_map_date = today

    @staticmethod
    def _is_display_name(value: object, symbol: str) -> bool:
        name = str(value or "").strip()
        return bool(name) and name.upper() not in {symbol, symbol.split(".", 1)[0]}

    def _resolve_name(self, symbol: str, quote_name: object = None) -> str:
        self._refresh_name_map()
        authoritative = self._name_map.get(symbol)
        if self._is_display_name(authoritative, symbol):
            return str(authoritative)
        if self._is_display_name(quote_name, symbol):
            return str(quote_name).strip()
        return symbol

    def _process_quotes(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        self._refresh_history(config)
        full_market = self._market_mode() == "full_market"
        runtime = self._runtime_for_today()
        runtime_symbols = set(runtime.get("symbols") or {})
        selected = {str(item["symbol"]).strip().upper() for item in config["selected"]}
        board_pool = {str(item["symbol"]).strip().upper() for item in config["board_pool"]}
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
            name = self._resolve_name(symbol, raw.get("name"))
            if is_risk_warning_name(name):
                continue
            limit_up = self._limit_up(raw, symbol, name, now.date())
            if limit_up is None:
                continue
            gap = max(0.0, limit_up / price - 1.0)
            source_modes = []
            if symbol in board_pool:
                source_modes.append("board_pool")
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
            if at_limit:
                self._maybe_auto_trade(symbol, quote, state, config)
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

    def _maybe_auto_trade(
        self,
        symbol: str,
        quote: dict[str, Any],
        state: dict[str, Any],
        config: dict[str, Any],
    ) -> None:
        member = next(
            (item for item in config["board_pool"] if str(item.get("symbol")).strip().upper() == symbol),
            None,
        )
        if not member or not bool(member.get("auto_trade")) or state.get("auto_order_key"):
            return
        if is_risk_warning_name(self._resolve_name(symbol, quote.get("name"))):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "ST 风险警示股票已被打板专区过滤"
            return
        qmt = self._qmt()
        qmt_status = qmt.status() if qmt is not None else {}
        if not (
            qmt_status.get("configured")
            and qmt_status.get("state") == "ready"
            and qmt_status.get("trade_enabled")
        ):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = str(
                qmt_status.get("reason") or "QMT 实盘交易未就绪",
            )
            return
        if not self._order_slots.acquire(blocking=False):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "自动委托队列已满"
            return
        key = f"limit-board-{cn_today().strftime('%Y%m%d')}-{symbol}"
        state.update({
            "auto_order_key": key,
            "auto_order_status": "submitting",
            "auto_order_error": None,
            "auto_order_at": cn_now().isoformat(),
        })
        try:
            self._order_executor.submit(
                self._submit_auto_order,
                symbol,
                float(quote["limit_up"]),
                key,
            )
        except RuntimeError as exc:
            self._order_slots.release()
            state["auto_order_status"] = "unknown"
            state["auto_order_error"] = str(exc)

    def _submit_auto_order(self, symbol: str, limit_up: float, key: str) -> None:
        qmt = self._qmt()
        try:
            if qmt is None:
                raise RuntimeError("QMT 交易网关未初始化")
            order = qmt.submit_order({
                "idempotency_key": key,
                "strategy_name": "limit_board",
                "action": "BUY",
                "symbol": symbol,
                "volume": 100,
                "price": limit_up,
                "price_type": "LIMIT",
            })
            result = {
                "symbol": symbol,
                "key": key,
                "status": str(order.get("status") or "unknown"),
                "order_sys_id": order.get("order_sys_id"),
                "error": order.get("error"),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("打板自动委托提交失败: %s", exc)
            result = {
                "symbol": symbol,
                "key": key,
                "status": "unknown",
                "order_sys_id": None,
                "error": str(exc),
            }
        try:
            self._order_results.put(result)
        finally:
            self._order_slots.release()

    def _apply_auto_order_result(self, result: dict[str, Any]) -> None:
        runtime = self._runtime_for_today()
        symbol = str(result.get("symbol") or "").strip().upper()
        state = runtime.setdefault("symbols", {}).setdefault(symbol, {})
        if state.get("auto_order_key") != result.get("key"):
            return
        state.update({
            "auto_order_status": result.get("status") or "unknown",
            "auto_order_sys_id": result.get("order_sys_id"),
            "auto_order_error": result.get("error"),
            "auto_order_updated_at": cn_now().isoformat(),
        })
        self._persist_runtime(runtime)
        self._notify_updated()

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
            0 if "board_pool" in item.get("source_modes", []) else
            1 if "selected" in item.get("source_modes", []) else 2,
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
        name = self._resolve_name(str(quote["symbol"]), quote.get("name"))
        event = {
            "ts": int(now.timestamp() * 1000),
            "trading_date": now.date().isoformat(),
            "source": "limit_board",
            "type": event_type,
            "rule_id": f"limit_board_{event_type}",
            "rule_name": labels[event_type],
            "symbol": quote["symbol"],
            "name": name,
            "message": f"{name}：{labels[event_type]}",
            "severity": "critical" if event_type == "broken" else "warn",
            "price": quote["last_price"],
            "limit_up": quote["limit_up"],
            "limit_gap_pct": quote["limit_gap_pct"],
            "break_count": int(state.get("break_count") or 0),
            "blacklisted": state.get("status") == "blacklisted",
            "reasons": [reason],
        }
        enrich = getattr(self.quote_service, "enrich_external_alerts", None)
        if callable(enrich):
            enrich([event])
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

    def _refresh_symbol_consumer(self) -> None:
        config = self.store.load_config()
        symbols = {
            str(item["symbol"]).strip().upper()
            for key in ("selected", "board_pool")
            for item in config[key]
        }
        symbols = {
            symbol for symbol in symbols
            if not is_risk_warning_name(self._resolve_name(symbol))
        }
        self.quote_service.set_symbol_consumer(_ACCOUNT_ID, symbols)

    def _enrich_concepts(self, rows: list[dict[str, Any]]) -> None:
        enrich = getattr(self.quote_service, "enrich_external_alerts", None)
        if not rows or not callable(enrich):
            return
        try:
            enrich(rows)
        except Exception:  # noqa: BLE001
            logger.debug("打板专区题材富化失败", exc_info=True)

    @staticmethod
    def _candidate_pool(
        first_board: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
        near_limit_pct: float,
    ) -> list[dict[str, Any]]:
        """Build the user-approval queue without enabling orders implicitly."""
        pool_symbols = {
            str(row.get("symbol") or "").strip().upper()
            for row in board_pool
        }
        candidates: dict[str, dict[str, Any]] = {}
        for row, origin in [
            *((item, "first_board") for item in first_board),
            *((item, "selected") for item in selected),
        ]:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol or symbol in pool_symbols:
                continue
            current = candidates.get(symbol)
            if current is None:
                modes = sorted(set(row.get("source_modes") or []) | {origin})
                current = {
                    **row,
                    "source": "first_board" if "first_board" in modes else "selected",
                    "source_modes": modes,
                }
                candidates[symbol] = current
            else:
                modes = set(current.get("source_modes") or [])
                modes.update(row.get("source_modes") or [])
                modes.add(origin)
                current["source_modes"] = sorted(modes)
                if current.get("name") in (None, "", symbol):
                    current["name"] = row.get("name")

        scored: list[dict[str, Any]] = []
        near = max(float(near_limit_pct), 0.0001)
        status_bonus = {
            "sealed": 18.0,
            "resealed": 16.0,
            "touched": 14.0,
            "near_limit": 8.0,
            "broken": 2.0,
        }
        for row in candidates.values():
            gap = _finite(row.get("limit_gap_pct"))
            proximity = 0.0 if gap is None else max(0.0, min(1.0, 1.0 - gap / near))
            source_modes = set(row.get("source_modes") or [])
            source_bonus = 12.0 if "first_board" in source_modes else 8.0
            if "selected" in source_modes:
                source_bonus += 2.0
            bid_volume = max(0.0, _finite(row.get("bid1_volume")) or 0.0)
            liquidity_bonus = min(8.0, (bid_volume ** 0.5) / 10.0)
            break_penalty = min(18.0, float(row.get("break_count") or 0) * 6.0)
            score = round(proximity * 60.0 + source_bonus + status_bonus.get(row.get("status"), 0.0) + liquidity_bonus - break_penalty, 2)
            reasons: list[str] = []
            if "first_board" in source_modes:
                reasons.append("首板候选")
            if "selected" in source_modes:
                reasons.append("精选跟踪")
            if gap is None:
                reasons.append("等待实时行情")
            else:
                reasons.append(f"距涨停 {(gap * 100):.2f}%")
            if row.get("status") in {"sealed", "resealed", "touched"}:
                reasons.append({
                    "sealed": "已封板",
                    "resealed": "回封",
                    "touched": "已触板",
                }[row["status"]])
            if row.get("break_count"):
                reasons.append(f"炸板 {int(row['break_count'])} 次")
            row["candidate_score"] = score
            row["candidate_reasons"] = [reason for reason in reasons if reason]
            scored.append(row)
        scored.sort(key=lambda row: (
            -float(row.get("candidate_score") or 0.0),
            float(row.get("limit_gap_pct") or 1.0),
            str(row.get("symbol") or ""),
        ))
        for rank, row in enumerate(scored, start=1):
            row["candidate_rank"] = rank
        return scored

    def view(self) -> dict[str, Any]:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        rows = []
        for symbol, state in runtime.get("symbols", {}).items():
            modes = state.get("source_modes") or []
            if not modes:
                continue
            row = {"symbol": symbol, **state, "ws_active": symbol in self._ws_symbols}
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if is_risk_warning_name(row["name"]):
                continue
            rows.append(row)
        rows.sort(key=lambda item: (
            0 if item.get("status") == "blacklisted" else 1,
            float(item.get("limit_gap_pct") or 1),
        ))
        selected = []
        runtime_by_symbol = runtime.get("symbols", {})
        for item in config["selected"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {**item, **runtime_by_symbol.get(symbol, {}), "ws_active": symbol in self._ws_symbols}
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if is_risk_warning_name(row["name"]):
                continue
            selected.append(row)
        board_pool = []
        for item in config["board_pool"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {**item, **runtime_by_symbol.get(symbol, {}), "ws_active": symbol in self._ws_symbols}
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if is_risk_warning_name(row["name"]):
                continue
            board_pool.append(row)
        candidate_pool = self._candidate_pool(
            [item for item in rows if "first_board" in item.get("source_modes", [])],
            selected,
            board_pool,
            float(config["settings"].get("near_limit_pct", 0.02)),
        )
        events = []
        labels = {"touched": "触板", "broken": "炸板", "resealed": "回封"}
        for event in self.store.events(runtime["trading_date"]):
            symbol = str(event.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            event["name"] = self._resolve_name(symbol, event.get("name"))
            if is_risk_warning_name(event["name"]):
                continue
            label = str(event.get("rule_name") or labels.get(str(event.get("type"))) or "").strip()
            if label:
                event["message"] = f"{event['name']}：{label}"
            events.append(event)
        self._enrich_concepts([*rows, *selected, *board_pool, *candidate_pool, *events])
        hub = self._hub()
        capacity = hub.websocket_capacity() if hub is not None else 0
        qmt = self._qmt()
        qmt_status = qmt.status() if qmt is not None else {}
        trading_enabled = bool(
            qmt_status.get("configured")
            and qmt_status.get("state") == "ready"
            and qmt_status.get("trade_enabled")
        )
        if trading_enabled:
            trading_reason = "QMT 实盘已就绪"
        elif qmt_status.get("state") == "ready":
            trading_reason = "QMT 已连接，实盘模式未开启"
        else:
            trading_reason = str(qmt_status.get("reason") or "QMT 交易网关未就绪")
        return {
            "revision": config["revision"],
            "settings": config["settings"],
            "first_board": [item for item in rows if "first_board" in item.get("source_modes", [])],
            "selected": selected,
            "candidate_pool": candidate_pool,
            "board_pool": board_pool,
            "blacklist": [
                symbol for symbol in runtime.get("blacklist", [])
                if not is_risk_warning_name(self._resolve_name(str(symbol).strip().upper()))
            ],
            "events": events,
            "runtime": {
                "trading_date": runtime["trading_date"],
                "history_ready": self._history_ready,
                "history_reason": self._history_reason,
                "last_scan_at": self._last_scan_at,
                "last_error": self._last_error,
                "websocket_status": "connected" if self._ws_registered else "idle",
                "websocket_symbols": len(self._ws_symbols),
                "websocket_capacity": capacity,
                "trading_enabled": trading_enabled,
                "trading_reason": trading_reason,
                "market_mode": self._market_mode(),
                "first_board_enabled": self._market_mode() == "full_market" and self._history_ready,
            },
        }

    def update_notifications(
        self, notifications: dict[str, Any], revision: int,
    ) -> dict[str, Any]:
        values = {
            key: bool(notifications[key])
            for key in ("touched", "broken", "resealed")
        }

        def update(config: dict[str, Any]) -> None:
            config["settings"]["notifications"] = values

        saved = self.store.update(revision, update)
        self._notify_updated()
        return saved

    def add_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["selected"]):
                return
            value["selected"].append({
                "symbol": cleaned,
                "name": name,
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        self._refresh_symbol_consumer()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def _validated_stock(self, symbol: str) -> tuple[str, str]:
        cleaned = str(symbol).strip().upper()
        names = self.repo.get_name_map([cleaned])
        if cleaned not in names or self.repo.resolve_asset_type(cleaned) != "stock":
            raise ValueError("仅支持本地股票主数据中的 A 股标的")
        name = str(names[cleaned])
        if is_risk_warning_name(name):
            raise ValueError("打板专区已过滤 ST 风险警示股票")
        return cleaned, name

    def remove_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "selected", [item for item in value["selected"] if str(item.get("symbol")) != cleaned],
            ),
        )
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved

    def add_pool(self, symbol: str, source: str, revision: int) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["board_pool"]):
                return
            value["board_pool"].append({
                "symbol": cleaned,
                "name": name,
                "source": source,
                "auto_trade": True,
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        self._refresh_symbol_consumer()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def update_pool(self, symbol: str, auto_trade: bool, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()

        def update(value: dict[str, Any]) -> None:
            member = next(
                (item for item in value["board_pool"] if str(item.get("symbol")) == cleaned),
                None,
            )
            if member is None:
                raise ValueError("打板池中不存在该股票")
            member["auto_trade"] = bool(auto_trade)

        saved = self.store.update(revision, update)
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def remove_pool(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "board_pool",
                [item for item in value["board_pool"] if str(item.get("symbol")) != cleaned],
            ),
        )
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved
