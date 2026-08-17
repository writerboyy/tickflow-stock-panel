"""First-board discovery and candidate-pool limit-board tracking."""
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
from app.services import alert_store, premium_gene, rps_rotation
from app.services.limit_board_scoring import (
    premium_gene_detail,
    sector_detail,
    technical_detail,
)
from app.services.limit_board_store import LimitBoardStore


logger = logging.getLogger(__name__)
_ACCOUNT_ID = "limit_board"
_DEPTH_FRESH_SECONDS = 30
_MIN_LIMIT_UP_COUNT = 4
_MIN_NEXT_DAY_RED_RATE = 0.80
_MAX_FIRST_BOARD_BROKEN_RATE = 0.75
_STOCK_PRICE_TICK = 0.01
_HISTORY_RETRY_SECONDS = 60
_HISTORY_WARMUP_RETRY_SECONDS = 5
_HISTORY_WARMUP_REASON = "历史指标缓存尚未就绪，首板/反包扫描已暂停"
_PREMIUM_FILTER_COLUMNS = {
    "symbol",
    "limit_up_count",
    "next_day_red_rate",
    "first_board_broken_rate",
}
_SCORE_REFRESH_SECONDS = 15.0
_SCORE_STOCK_COLUMNS = {
    "symbol", "name", "close", "last_price", "change_pct", "amount",
    "ma5", "ma10", "ma20", "ma60", "momentum_5d", "momentum_20d",
    "vol_ratio_5d", "macd_dif", "macd_dea", "macd_hist", "rsi_14",
}


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


def _sweep_ready(
    depth: dict[str, Any], limit_up: float, max_price_levels: int = 5,
) -> bool:
    asks = [
        (price, volume)
        for price, volume in zip(
            depth.get("ask_prices") or [],
            depth.get("ask_volumes") or [],
            strict=False,
        )
        if price > 0 and volume > 0
    ]
    if not asks:
        return False
    best_ask = asks[0][0]
    remaining = limit_up - best_ask
    return -0.001 <= remaining <= max_price_levels * _STOCK_PRICE_TICK + 0.001


def _qualified_premium_stats(rows: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if (
        rows is None
        or rows.is_empty()
        or not _PREMIUM_FILTER_COLUMNS.issubset(rows.columns)
    ):
        return {}
    qualified = rows.select(sorted(_PREMIUM_FILTER_COLUMNS)).filter(
        (pl.col("limit_up_count").cast(pl.Int64, strict=False) >= _MIN_LIMIT_UP_COUNT)
        & (pl.col("next_day_red_rate").cast(pl.Float64, strict=False) >= _MIN_NEXT_DAY_RED_RATE)
        & (
            pl.col("first_board_broken_rate").cast(pl.Float64, strict=False)
            <= _MAX_FIRST_BOARD_BROKEN_RATE
        )
    )
    return {
        str(row["symbol"]).strip().upper(): {
            "limit_up_count": int(row["limit_up_count"]),
            "next_day_red_rate": float(row["next_day_red_rate"]),
            "first_board_broken_rate": float(row["first_board_broken_rate"]),
        }
        for row in qualified.iter_rows(named=True)
        if str(row.get("symbol") or "").strip()
    }


def _premium_stats_by_symbol(rows: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if rows is None or rows.is_empty() or "symbol" not in rows.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for raw in rows.iter_rows(named=True):
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        result[symbol] = {
            key: value.isoformat() if isinstance(value, date) else value
            for key, value in raw.items()
            if key != "symbol"
        }
    return result


class LimitBoardService:
    def __init__(self, data_dir: Path, repo: Any, quote_service: Any, app_state: Any) -> None:
        self.store = LimitBoardStore(data_dir)
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._lock = threading.RLock()
        self._score_lock = threading.Lock()
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
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=10))
        self._history_date: date | None = None
        self._name_map_date: date | None = None
        self._name_map: dict[str, str] = {}
        self._first_board_eligible: set[str] = set()
        self._rebound_board_eligible: set[str] = set()
        self._premium_stats: dict[str, dict[str, Any]] = {}
        self._score_refresh_at = 0.0
        self._rotation_date: date | None = None
        self._rotation_cache: dict[tuple[str, int | None], dict[str, Any]] = {}
        self._history_ready = False
        self._history_attempt_at = 0.0
        self._history_reason = "正在读取涨停历史与溢价基因过滤数据"
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
                try:
                    self._retry_history()
                except Exception:  # noqa: BLE001
                    logger.exception("打板历史数据自动重试失败")
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
            runtime = {
                "trading_date": today,
                "symbols": {},
                "blacklist": [],
                "candidate_excluded": [],
            }
            self.store.save_runtime(runtime)
            self._depth.clear()
        return runtime

    def _refresh_history(self, config: dict[str, Any]) -> None:
        today = cn_today()
        now_mono = time.monotonic()
        if self._history_date == today:
            retry_seconds = (
                _HISTORY_WARMUP_RETRY_SECONDS
                if self._history_reason == _HISTORY_WARMUP_REASON
                else _HISTORY_RETRY_SECONDS
            )
            if self._history_ready or now_mono - self._history_attempt_at < retry_seconds:
                return
        self._history_date = today
        self._history_attempt_at = now_mono
        self._history_ready = False
        self._first_board_eligible.clear()
        self._rebound_board_eligible.clear()
        self._premium_stats.clear()
        lookback = max(1, int(config["settings"].get("first_board_lookback_days", 10)))
        latest, latest_date = self.repo.get_enriched_latest()
        if latest_date is None:
            self._history_reason = _HISTORY_WARMUP_REASON
            return
        end = min(latest_date, today - timedelta(days=1))
        start = end - timedelta(days=max(30, lookback * 3))
        history = self.repo.get_enriched_range(
            start, end, columns=["symbol", "date", "signal_limit_up", "signal_broken_limit_up"],
        )
        if history is None:
            self._history_reason = _HISTORY_WARMUP_REASON
            return
        if history.is_empty() or "signal_limit_up" not in history.columns:
            self._history_reason = "近 10 个交易日涨停记录不足，首板/反包扫描已暂停"
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
        rebound = set()
        if "signal_broken_limit_up" not in scoped.columns:
            scoped = scoped.with_columns(pl.lit(False).alias("signal_broken_limit_up"))
        # 反包要求窗口内先有涨停，随后至少出现一天炸板或断板，且最近一天不能仍是涨停。
        for symbol, rows in scoped.sort(["symbol", "date"]).group_by("symbol", maintain_order=True):
            saw_limit = False
            had_break_after_limit = False
            last_was_limit = False
            for row in rows.iter_rows(named=True):
                is_limit = bool(row.get("signal_limit_up"))
                is_broken = bool(row.get("signal_broken_limit_up"))
                if saw_limit and (is_broken or not is_limit):
                    had_break_after_limit = True
                if is_limit:
                    saw_limit = True
                last_was_limit = is_limit
            if saw_limit and had_break_after_limit and not last_was_limit:
                rebound.add(symbol[0] if isinstance(symbol, tuple) else symbol)
        instruments = self.repo.get_instruments()
        universe = set(instruments["symbol"].to_list()) if not instruments.is_empty() else set()
        self._refresh_name_map()
        universe = {
            symbol for symbol in universe
            if not is_risk_warning_name(self._name_map.get(str(symbol).strip().upper()))
        }
        try:
            premium_rows = premium_gene.refresh(self.repo)
        except Exception:  # noqa: BLE001
            logger.warning("打板专区读取溢价基因快照失败", exc_info=True)
            premium_rows = pl.DataFrame()
        if (
            premium_rows is None
            or premium_rows.is_empty()
            or not _PREMIUM_FILTER_COLUMNS.issubset(premium_rows.columns)
        ):
            self._history_reason = "溢价基因数据不足，自动首板/反包已暂停"
            return
        self._premium_stats = _premium_stats_by_symbol(premium_rows)
        qualified = set(_qualified_premium_stats(premium_rows))
        self._first_board_eligible = (universe - blocked) & qualified
        self._rebound_board_eligible = (rebound & universe & qualified) - self._first_board_eligible
        self._history_ready = True
        self._history_reason = (
            f"已核对前 {lookback} 个交易日；自动候选需涨停≥4次、"
            f"次日红盘率≥80%、首板破板率≤75%（{len(qualified)} 只通过）"
        )

    def _retry_history(self) -> None:
        if self._history_ready:
            return
        previous_ready = self._history_ready
        previous_reason = self._history_reason
        self._refresh_history(self.store.load_config())
        if self._history_ready and not previous_ready:
            self._enqueue({
                "type": "market",
                "quotes": self.quote_service.get_latest_quotes(),
            })
        if self._history_ready != previous_ready or self._history_reason != previous_reason:
            self._notify_updated()

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
                and (
                    symbol in self._first_board_eligible
                    or symbol in self._rebound_board_eligible
                )
                and (gap <= scan_window or symbol in runtime_symbols)
            ):
                if symbol in self._first_board_eligible:
                    source_modes.append("first_board")
                if symbol in self._rebound_board_eligible:
                    source_modes.append("rebound_board")
            if not source_modes:
                continue
            quote = {
                **raw,
                **self._premium_stats.get(symbol, {}),
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
        rows, selected_rows, board_rows = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected_rows, board_rows,
        )
        self._refresh_candidate_scores(runtime, candidates, now)
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
            state.update({
                key: value for key, value in self._premium_stats.get(symbol, {}).items()
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
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="limit_touch",
                )
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
        sweep_price_levels = int(config["settings"].get("sweep_price_levels", 5))
        queue_confirm_snapshots = int(
            config["settings"].get("queue_confirm_snapshots", 0),
        )
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
            recent = [
                item for item in self._depth[symbol]
                if (now - item["timestamp"]).total_seconds() <= _DEPTH_FRESH_SECONDS
            ]
            self._depth[symbol] = deque(recent, maxlen=10)
            if not recent:
                continue
            sealed_flags = [self._sealed_snapshot(item, float(quote["limit_up"])) for item in recent]
            confirmed = len(sealed_flags) >= 3 and all(sealed_flags[-3:])
            latest_sealed = sealed_flags[-1]
            consecutive_sealed = 0
            for is_sealed in reversed(sealed_flags):
                if not is_sealed:
                    break
                consecutive_sealed += 1
            state["bid1_volume"] = normalized["bid_volumes"][0] if normalized["bid_volumes"] else 0.0
            state["ask1_volume"] = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
            state["last_depth_at"] = normalized["timestamp"].isoformat()
            if _sweep_ready(
                normalized,
                float(quote["limit_up"]),
                sweep_price_levels,
            ):
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="sweep",
                )
            if queue_confirm_snapshots > 0 and latest_sealed:
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="queue",
                    queue_confirmed_snapshots=consecutive_sealed,
                )
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
            bid_prices = [float(value) for value in (raw.get("bid_prices") or [])[:10]]
            bid_volumes = [float(value) for value in (raw.get("bid_volumes") or [])[:10]]
            ask_prices = [float(value) for value in (raw.get("ask_prices") or [])[:10]]
            ask_volumes = [float(value) for value in (raw.get("ask_volumes") or [])[:10]]
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
        *,
        runtime: dict[str, Any] | None = None,
        trigger_mode: str = "queue",
        queue_confirmed_snapshots: int = 0,
    ) -> None:
        member = next(
            (item for item in config["board_pool"] if str(item.get("symbol")).strip().upper() == symbol),
            None,
        )
        if not member or not bool(member.get("auto_trade")) or state.get("auto_order_key"):
            return
        order_mode = str(member.get("order_mode") or "sweep")
        if trigger_mode != "limit_touch" and order_mode != trigger_mode:
            return
        if trigger_mode == "limit_touch" and order_mode not in {"sweep", "queue"}:
            return
        if order_mode == "queue":
            required_snapshots = int(
                config["settings"].get("queue_confirm_snapshots", 0),
            )
            if required_snapshots > 0 and (
                not state.get("touched")
                or queue_confirmed_snapshots < required_snapshots
            ):
                return
            wait_seconds = int(config["settings"].get("queue_wait_seconds", 0))
            if wait_seconds > 0:
                touched_at = _quote_time(state.get("touched_at"))
                now = cn_now()
                now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
                if (
                    touched_at is None
                    or (now_aware - touched_at).total_seconds() < wait_seconds
                ):
                    return
        max_boards = int(config["settings"].get("max_auto_board_count", 0))
        if max_boards > 0:
            active_runtime = runtime if runtime is not None else self._runtime_for_today()
            used_boards = sum(
                1
                for item in (active_runtime.get("symbols") or {}).values()
                if item.get("auto_order_key")
            )
            if used_boards >= max_boards:
                state["auto_order_status"] = "blocked"
                state["auto_order_error"] = f"已达到每日自动打板上限（{max_boards} 只）"
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
        volume, volume_error = self._auto_order_volume(
            float(quote["limit_up"]), config,
        )
        if volume_error:
            self._order_slots.release()
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = volume_error
            return
        key = f"limit-board-{cn_today().strftime('%Y%m%d')}-{symbol}"
        state.update({
            "auto_order_key": key,
            "auto_order_status": "submitting",
            "auto_order_mode": order_mode,
            "auto_order_error": None,
            "auto_order_at": cn_now().isoformat(),
            "auto_order_volume": volume,
            "auto_order_amount": round(float(quote["limit_up"]) * volume, 2),
        })
        try:
            self._order_executor.submit(
                self._submit_auto_order,
                symbol,
                float(quote["limit_up"]),
                key,
                volume,
            )
        except RuntimeError as exc:
            self._order_slots.release()
            state["auto_order_status"] = "unknown"
            state["auto_order_error"] = str(exc)

    @staticmethod
    def _auto_order_volume(
        limit_up: float, config: dict[str, Any],
    ) -> tuple[int, str | None]:
        amount = _finite(config["settings"].get("order_amount_per_board", 0))
        if amount is None or amount < 0:
            return 0, "单板下单资金配置无效"
        if amount == 0:
            return 100, None
        if limit_up <= 0 or limit_up != limit_up:
            return 0, "涨停价无效，已阻止自动委托"
        volume = int(amount / (limit_up * 100)) * 100
        if volume < 100:
            return 0, "单板下单资金不足一手，已阻止自动委托"
        return volume, None

    def _submit_auto_order(self, symbol: str, limit_up: float, key: str, volume: int) -> None:
        qmt = self._qmt()
        try:
            if qmt is None:
                raise RuntimeError("QMT 交易网关未初始化")
            order = qmt.submit_order({
                "idempotency_key": key,
                "strategy_name": "limit_board",
                "action": "BUY",
                "symbol": symbol,
                "volume": volume,
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
            if (
                quote is None
                or symbol in blacklist
                or "board_pool" not in quote.get("source_modes", [])
            ):
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
            and "board_pool" in quote.get("source_modes", [])
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
        rebound_board: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build the user-approval queue without enabling orders implicitly."""
        pool_symbols = {
            str(row.get("symbol") or "").strip().upper()
            for row in board_pool
        }
        candidates: dict[str, dict[str, Any]] = {}
        for row, origin in [
            *((item, "first_board") for item in first_board),
            *((item, "rebound_board") for item in rebound_board),
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
                    "source": (
                        "first_board" if "first_board" in modes
                        else "rebound_board" if "rebound_board" in modes
                        else "manual"
                    ),
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

        return list(candidates.values())

    @staticmethod
    def _rank_candidates(
        candidates: list[dict[str, Any]], score_cache: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result = []
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "").strip().upper()
            score = score_cache.get(symbol) or {
                "candidate_score": None,
                "candidate_rank": None,
                "candidate_score_state": "unavailable",
                "candidate_score_as_of": None,
                "candidate_score_detail": {},
                "candidate_reasons": [],
            }
            result.append({**candidate, **score})
        result.sort(key=lambda row: (
            row.get("candidate_score") is None,
            -float(row.get("candidate_score") or 0.0),
            -float(((row.get("candidate_score_detail") or {}).get("sector") or {}).get("score") or 0.0),
            -float(((row.get("candidate_score_detail") or {}).get("premium_gene") or {}).get("score") or 0.0),
            -float(((row.get("candidate_score_detail") or {}).get("technical") or {}).get("score") or 0.0),
            str(row.get("symbol") or ""),
        ))
        rank = 0
        for row in result:
            if row.get("candidate_score") is None:
                row["candidate_rank"] = None
                continue
            rank += 1
            row["candidate_rank"] = rank
        return result

    def _view_collections(
        self, runtime: dict[str, Any], config: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        runtime_by_symbol = runtime.get("symbols", {})
        rows = []
        for symbol, state in runtime_by_symbol.items():
            if not state.get("source_modes"):
                continue
            row = {"symbol": symbol, **state, "ws_active": symbol in self._ws_symbols}
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                rows.append(row)
        rows.sort(key=lambda item: (
            0 if item.get("status") == "blacklisted" else 1,
            float(item.get("limit_gap_pct") or 1),
        ))

        selected = []
        for item in config["selected"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {
                **item,
                **runtime_by_symbol.get(symbol, {}),
                "ws_active": symbol in self._ws_symbols,
            }
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                selected.append(row)

        board_pool = []
        for item in config["board_pool"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {
                "order_mode": "sweep",
                **item,
                **runtime_by_symbol.get(symbol, {}),
                "ws_active": symbol in self._ws_symbols,
            }
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                board_pool.append(row)
        return rows, selected, board_pool

    def _candidate_rows_for_runtime(
        self,
        runtime: dict[str, Any],
        rows: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        excluded = {
            str(symbol).strip().upper()
            for symbol in runtime.get("candidate_excluded") or []
        }
        eligible = [
            item for item in rows
            if str(item.get("symbol") or "").strip().upper() not in excluded
        ]
        return self._candidate_pool(
            [item for item in eligible if "first_board" in item.get("source_modes", [])],
            [item for item in eligible if "rebound_board" in item.get("source_modes", [])],
            [
                item for item in selected
                if str(item.get("symbol") or "").strip().upper() not in excluded
            ],
            board_pool,
        )

    def _rotation(self, kind: str, level: int | None, today: date) -> dict[str, Any]:
        if self._rotation_date != today:
            self._rotation_cache.clear()
            self._rotation_date = today
        key = (kind, level)
        cached = self._rotation_cache.get(key)
        if cached is not None:
            return cached
        try:
            value = rps_rotation.build_rps_rotation(self.repo, 7, kind, level)
        except Exception:  # noqa: BLE001
            logger.warning("打板备选池读取板块轮动失败: %s", kind, exc_info=True)
            return {}
        if value.get("dates") and value.get("columns"):
            self._rotation_cache[key] = value
        return value

    def _candidate_stock_snapshot(
        self, now: datetime,
    ) -> tuple[pl.DataFrame, dict[str, dict[str, Any]]]:
        getter = getattr(self.quote_service, "get_enriched_today", None)
        if not callable(getter):
            return pl.DataFrame(), {}
        stock_df, stock_date = getter()
        if stock_date != now.date() or stock_df is None or stock_df.is_empty():
            return pl.DataFrame(), {}
        columns = [column for column in stock_df.columns if column in _SCORE_STOCK_COLUMNS]
        if "symbol" not in columns:
            return pl.DataFrame(), {}
        rows = {}
        for raw in stock_df.select(columns).iter_rows(named=True):
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            rows[symbol] = {
                **raw,
                "symbol": symbol,
                "name": self._resolve_name(symbol, raw.get("name")),
            }
        return stock_df, rows

    @staticmethod
    def _score_reasons(
        candidate: dict[str, Any], detail: dict[str, dict[str, Any]],
    ) -> list[str]:
        modes = set(candidate.get("source_modes") or [])
        reasons = []
        if "first_board" in modes:
            reasons.append("首板候选")
        if "rebound_board" in modes:
            reasons.append("反包候选")
        if "selected" in modes:
            reasons.append("手工加入")
        sector = detail.get("sector") or {}
        gene = detail.get("premium_gene") or {}
        technical = detail.get("technical") or {}
        if sector:
            reasons.append(
                f"{sector.get('name') or '板块'} {sector.get('rotation_label') or '数据不足'}"
                f" · {sector.get('leadership') or 'follower'}"
            )
        if gene:
            reasons.append(f"涨停基因 {float(gene.get('score') or 0):.1f}/30")
        if technical:
            reasons.append(f"技术面 {float(technical.get('score') or 0):.1f}/20")
        return reasons

    def _refresh_candidate_scores(
        self,
        runtime: dict[str, Any],
        candidates: list[dict[str, Any]],
        now: datetime,
    ) -> bool:
        symbols = {
            str(item.get("symbol") or "").strip().upper()
            for item in candidates if item.get("symbol")
        }
        previous_cache = runtime.get("candidate_scores") or {}
        missing = any(symbol not in previous_cache for symbol in symbols)
        now_mono = time.monotonic()
        if not missing and now_mono - self._score_refresh_at < _SCORE_REFRESH_SECONDS:
            return False
        if not self._score_lock.acquire(blocking=False):
            return False
        try:
            now_mono = time.monotonic()
            previous_cache = runtime.get("candidate_scores") or {}
            missing = any(symbol not in previous_cache for symbol in symbols)
            if not missing and now_mono - self._score_refresh_at < _SCORE_REFRESH_SECONDS:
                return False
            stock_df, stock_rows = self._candidate_stock_snapshot(now)
            sector_service = getattr(self.app_state, "sector_monitor_service", None)
            targets_by_symbol: dict[str, dict[str, list[dict[str, Any]]]] = {}
            targets_by_key: dict[str, dict[str, Any]] = {}
            if sector_service is not None:
                for symbol in symbols:
                    concepts = sector_service.targets_for_symbol(symbol, kind="concept")
                    industries = sector_service.targets_for_symbol(
                        symbol, kind="industry", industry_level=2,
                    )
                    targets_by_symbol[symbol] = {
                        "concept": concepts,
                        "industry": industries,
                    }
                    for target in [*concepts, *industries]:
                        targets_by_key[str(target.get("key") or "")] = target
            snapshots = {}
            if sector_service is not None and targets_by_key and not stock_df.is_empty():
                index_getter = getattr(self.quote_service, "get_index_quotes", None)
                index_df = index_getter() if callable(index_getter) else pl.DataFrame()
                try:
                    snapshots = sector_service.build_snapshots(
                        stock_df,
                        index_df,
                        list(targets_by_key.values()),
                        set(),
                        now=now.timestamp(),
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("打板备选池板块快照计算失败", exc_info=True)
            rotations = {
                "concept": self._rotation("concept", None, now.date()),
                "industry": self._rotation("industry", 2, now.date()),
            } if sector_service is not None else {}

            refreshed: dict[str, dict[str, Any]] = {}
            for candidate in candidates:
                symbol = str(candidate.get("symbol") or "").strip().upper()
                previous = previous_cache.get(symbol) or {}
                previous_detail = previous.get("candidate_score_detail") or {}
                gene = premium_gene_detail(self._premium_stats.get(symbol) or {})
                technical = technical_detail(
                    stock_rows.get(symbol) or {}, as_of=now.isoformat(),
                )
                sector = None
                symbol_targets = targets_by_symbol.get(symbol) or {}
                for kind in ("concept", "industry"):
                    available = []
                    for target in symbol_targets.get(kind, []):
                        key = str(target.get("key") or "")
                        snapshot = snapshots.get(key)
                        if snapshot is None:
                            continue
                        value = sector_detail(
                            symbol=symbol,
                            target=target,
                            snapshot=snapshot,
                            rotation=rotations.get(kind) or {},
                            stock_rows=stock_rows,
                            member_symbols=sector_service.member_symbols(key),
                            today=now.date(),
                        )
                        if value is not None:
                            value["as_of"] = now.isoformat()
                            available.append(value)
                    if available:
                        sector = max(
                            available,
                            key=lambda item: (float(item["score"]), str(item.get("name") or "")),
                        )
                        break
                fresh = {
                    "sector": sector,
                    "premium_gene": gene,
                    "technical": technical,
                }
                detail = {}
                cached_component = False
                for key, value in fresh.items():
                    if value is not None:
                        detail[key] = value
                    elif previous_detail.get(key):
                        detail[key] = previous_detail[key]
                        cached_component = True
                complete = all(detail.get(key) for key in ("sector", "premium_gene", "technical"))
                score = round(sum(float(detail[key]["score"]) for key in detail), 1) if complete else None
                state = "cached" if complete and cached_component else "live" if complete else "unavailable"
                refreshed[symbol] = {
                    "candidate_score": score,
                    "candidate_rank": None,
                    "candidate_score_state": state,
                    "candidate_score_as_of": now.isoformat(),
                    "candidate_score_detail": detail,
                    "candidate_reasons": self._score_reasons(candidate, detail),
                }
            changed = refreshed != previous_cache
            runtime["candidate_scores"] = refreshed
            self._score_refresh_at = now_mono
            return changed
        finally:
            self._score_lock.release()

    def view(self) -> dict[str, Any]:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        rows, selected, board_pool = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected, board_pool,
        )
        if self._refresh_candidate_scores(runtime, candidates, cn_now()):
            self._persist_runtime(runtime)
        candidate_pool = self._rank_candidates(
            candidates, runtime.get("candidate_scores") or {},
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
            "first_board": [
                item for item in rows
                if {"first_board", "rebound_board"} & set(item.get("source_modes", []))
            ],
            "rebound_board": [item for item in rows if "rebound_board" in item.get("source_modes", [])],
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

    def update_advanced_settings(
        self, settings: dict[str, Any], revision: int,
    ) -> dict[str, Any]:
        values = {
            "sweep_price_levels": int(settings["sweep_price_levels"]),
            "queue_wait_seconds": int(settings["queue_wait_seconds"]),
            "queue_confirm_snapshots": int(settings["queue_confirm_snapshots"]),
            "order_amount_per_board": float(settings["order_amount_per_board"]),
            "max_auto_board_count": int(settings["max_auto_board_count"]),
            "near_limit_pct": float(settings["near_limit_pct"]),
            "exit_limit_pct": float(settings["exit_limit_pct"]),
            "exit_sustain_seconds": int(settings["exit_sustain_seconds"]),
            "first_board_lookback_days": int(settings["first_board_lookback_days"]),
            "blacklist_after_breaks": int(settings["blacklist_after_breaks"]),
        }
        if values["exit_limit_pct"] < values["near_limit_pct"]:
            raise ValueError("扫描退出阈值不能小于临板 WS 阈值")

        def update(config: dict[str, Any]) -> None:
            config["settings"].update(values)

        saved = self.store.update(revision, update)
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes()})
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
        runtime = self._runtime_for_today()
        excluded = set(runtime.get("candidate_excluded") or [])
        if cleaned in excluded:
            excluded.remove(cleaned)
            runtime["candidate_excluded"] = sorted(excluded)
            self._persist_runtime(runtime)
        self._refresh_symbol_consumer()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def add_candidate(self, symbol: str, revision: int) -> dict[str, Any]:
        """Add a manual candidate while retaining the legacy storage schema."""
        return self.add_selected(symbol, revision)

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

    def remove_candidate(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "selected", [
                    item for item in value["selected"]
                    if str(item.get("symbol")).strip().upper() != cleaned
                ],
            ),
        )
        runtime = self._runtime_for_today()
        excluded = set(runtime.get("candidate_excluded") or [])
        excluded.add(cleaned)
        runtime["candidate_excluded"] = sorted(excluded)
        self._persist_runtime(runtime)
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
                "order_mode": "sweep",
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        self._refresh_symbol_consumer()
        self._enqueue({"type": "market", "quotes": self.quote_service.get_latest_quotes({cleaned})})
        self._notify_updated()
        return saved

    def update_pool(
        self,
        symbol: str,
        auto_trade: bool,
        order_mode: str,
        revision: int,
    ) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        cleaned_mode = str(order_mode or "").strip().lower()
        if cleaned_mode not in {"sweep", "queue"}:
            raise ValueError("打板方式必须是扫板或排板")

        def update(value: dict[str, Any]) -> None:
            member = next(
                (item for item in value["board_pool"] if str(item.get("symbol")) == cleaned),
                None,
            )
            if member is None:
                raise ValueError("打板池中不存在该股票")
            member["auto_trade"] = bool(auto_trade)
            member["order_mode"] = cleaned_mode

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
