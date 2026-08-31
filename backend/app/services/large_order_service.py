"""实时大单候选与开盘啦深挖服务。

热路径只消费 QuoteService 已经缓存的快照；开盘啦请求在独立线程中执行。
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.price_limits import is_risk_warning_name, limit_price, price_limit_pct
from app.plugins.kaipanla.client import KaipanlaClient, KaipanlaRequestError
from app.plugins.kaipanla.credentials import load_credentials
from app.plugins.kaipanla.parsers import (
    ResponseShapeError,
    parse_large_order_intents,
    parse_large_order_trades,
)
from app.services.large_order_store import LargeOrderStore, SCHEMA_VERSION
from app.services.ingestion_manifest import stable_content_hash

logger = logging.getLogger(__name__)

_LARGE_ORDER_WEBHOOK_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="large-order-webhook",
)

WINDOWS = (15, 60, 300)
BASELINE_BUCKETS = 120
DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "score_threshold": 75,
    "cooldown_seconds": 120,
    "deep_dive_interval_seconds": 60,
    "max_deep_dive_symbols": 3,
    "candidate_limit": 50,
    "min_limit_up_gap_pct": 0.02,
    "market_segments": ("main", "star", "chinext"),
    "exclude_bse": True,
    "exclude_st": True,
    "version": "large_orders_v2",
}


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _robust_z(value: float, values: list[float]) -> float:
    if len(values) < 5:
        return 0.0
    median = _median(values)
    mad = _median([abs(item - median) for item in values])
    scale = 1.4826 * mad
    if scale <= 1e-9:
        return 0.0
    return (value - median) / scale


def _large_threshold(values: list[float]) -> float:
    if len(values) < 5:
        return 1_000_000.0
    median = _median(values)
    mad = _median([abs(item - median) for item in values])
    return max(1_000_000.0, median + 3.0 * 1.4826 * mad)


def _as_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)
    if isinstance(value, (int, float)):
        try:
            seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=CN_TZ)
        except (OverflowError, OSError, ValueError):
            return None
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(CN_TZ) if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ)
        except ValueError:
            return None
    return None


def _session_datetime(value: object, trade_date: date) -> datetime | None:
    text = str(value or "").strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.replace(
            year=trade_date.year,
            month=trade_date.month,
            day=trade_date.day,
            tzinfo=CN_TZ,
        )
    return None


class LargeOrderService:
    """持仓池的大单证据聚合；持仓模块未初始化时保留旧调用兼容。"""

    def __init__(self, quote_service=None) -> None:
        self._quote_service = quote_service
        self._app_state = None
        self._lock = threading.RLock()
        self._running = False
        self._pending_snapshot: list[dict] | None = None
        self._snapshot_running = False
        self._snapshot_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="large-orders")
        self._deep_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="large-orders-kaipanla")
        self._states: dict[str, dict[str, Any]] = {}
        self._score_symbols: set[str] = set()
        self._rankings: dict[int, tuple[dict[str, Any], ...]] = {window: () for window in WINDOWS}
        self._storage: LargeOrderStore | None = None
        self._deep_pending: set[str] = set()
        self._last_deep_at: dict[str, float] = {}
        self._deep_calls_date = cn_today()
        self._deep_calls_used = 0
        self._cooldown_until: dict[str, float] = {}
        self._last_update_ms: int | None = None
        self._last_calculation_ms = 0.0
        self._last_error: str | None = None
        self._trade_date = cn_today()
        self._instrument_limits_date: date | None = None
        self._instrument_limits: dict[str, dict[str, Any]] = {}
        self._filtered_near_limit_count = 0
        self._unassessable_count = 0
        self._config = dict(DEFAULTS)

    def set_app_state(self, app_state) -> None:
        self._app_state = app_state
        repo = getattr(app_state, "repo", None)
        data_dir = getattr(getattr(repo, "store", None), "data_dir", None)
        if data_dir is not None and self._storage is None:
            self._storage = LargeOrderStore(data_dir)

    def start(self) -> None:
        if self._running:
            return
        from app.services import preferences

        self._config.update(preferences.get_large_orders_preferences())
        if self._storage is not None:
            try:
                self._storage.start()
                self._storage.compact_unsealed_days(today=self._trade_date)
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"大单存储启动失败: {exc}"
                logger.exception("实时大单存储启动失败")
        self._running = True
        if self._quote_service is not None:
            self._quote_service.add_fetch_listener(self._on_quote_fetch)
        logger.info("实时大单服务已启动 (enabled=%s)", self._config["enabled"])

    def stop(self) -> None:
        self._running = False
        if self._quote_service is not None:
            try:
                self._quote_service.remove_fetch_listener(self._on_quote_fetch)
            except (KeyError, ValueError):
                pass
        with self._lock:
            self._pending_snapshot = None
            self._deep_pending.clear()
        self._snapshot_executor.shutdown(wait=True, cancel_futures=False)
        self._deep_executor.shutdown(wait=True, cancel_futures=False)
        if self._storage is not None:
            self._storage.stop(compact_date=self._trade_date)

    def update_preferences(self, updates: dict[str, Any]) -> dict:
        from app.services import preferences

        current = preferences.set_large_orders_preferences(updates)
        with self._lock:
            self._config.update(current)
            rankings, filtered_near_limit, unassessable = self._build_rankings_locked(time.time())
            self._rankings = rankings
            self._filtered_near_limit_count = filtered_near_limit
            self._unassessable_count = unassessable
        if self._quote_service is not None:
            self._quote_service.notify_large_orders_updated()
        return current

    @staticmethod
    def _market_segment(symbol: str, name: object = None) -> str:
        normalized = str(symbol).strip().upper()
        code = normalized.split(".", 1)[0]
        if is_risk_warning_name(str(name or "")):
            return "st"
        if normalized.endswith(".BJ") or code.startswith(("4", "8")):
            return "bse"
        if code.startswith(("688", "689")):
            return "star"
        if code.startswith(("300", "301")):
            return "chinext"
        return "main"

    def _is_filtered_symbol(self, symbol: str, name: object = None) -> bool:
        configured = self._config.get("market_segments")
        if configured is None:
            configured = ["main", "star", "chinext"]
            if not self._config.get("exclude_bse", True):
                configured.append("bse")
            if not self._config.get("exclude_st", True):
                configured.append("st")
        return self._market_segment(symbol, name) not in configured

    def _position_symbols(self) -> set[str] | None:
        """返回当前持仓池；None 表示持仓服务尚未完成启动。"""
        if self._app_state is None or not hasattr(self._app_state, "position_risk_service"):
            return None
        service = getattr(self._app_state, "position_risk_service", None)
        if service is None:
            return set()
        try:
            return {
                str(item.get("symbol") or "").strip().upper()
                for item in service.store.load().get("positions", [])
                if item.get("symbol")
            }
        except Exception:  # noqa: BLE001
            return set()

    def set_score_symbols(self, symbols: set[str]) -> None:
        """Add the limit-board candidate set to the existing realtime flow scope."""
        cleaned = {
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        }
        with self._lock:
            self._score_symbols = cleaned

    def _scope_symbols(self) -> set[str] | None:
        position_symbols = self._position_symbols()
        if position_symbols is None:
            return None
        with self._lock:
            return position_symbols | set(self._score_symbols)

    @staticmethod
    def _new_window_tracker(now_ts: float, window: int) -> dict[str, Any]:
        return {
            "events": deque(),
            "buy": 0.0,
            "sell": 0.0,
            "bucket_id": int(now_ts // window),
            "bucket_amount": 0.0,
            "history": deque(maxlen=BASELINE_BUCKETS),
        }

    def _new_state(self, symbol: str, name: object, now_ts: float) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "name": name or symbol,
            "snapshots": deque(maxlen=360),
            "flows": deque(maxlen=720),
            "windows": {
                window: self._new_window_tracker(now_ts, window)
                for window in WINDOWS
            },
            "trade_events": deque(maxlen=300),
            "intent_events": deque(maxlen=300),
            "net_flow_points": deque(maxlen=360),
            "trade_ids": set(),
            "intent_ids": set(),
            "net_flow_ids": set(),
            "last_side": 0,
            "deep_source": "proxy_only",
            "deep_error": None,
            "last_deep_ms": None,
            "change_pct": None,
            "limit_up_price": None,
            "limit_up_gap_pct": None,
            "price_limit_assessable": False,
            "no_price_limit": False,
        }

    @staticmethod
    def _advance_window_tracker(tracker: dict[str, Any], now_ts: float, window: int) -> None:
        bucket_id = int(now_ts // window)
        current_id = int(tracker["bucket_id"])
        if bucket_id > current_id:
            history = tracker["history"]
            history.append(float(tracker["bucket_amount"]))
            skipped = min(bucket_id - current_id - 1, BASELINE_BUCKETS)
            history.extend(0.0 for _ in range(skipped))
            tracker["bucket_id"] = bucket_id
            tracker["bucket_amount"] = 0.0

        cutoff = now_ts - window
        events = tracker["events"]
        while events and events[0][0] < cutoff:
            _ts, buy, sell = events.popleft()
            tracker["buy"] -= buy
            tracker["sell"] -= sell
        if abs(tracker["buy"]) < 1e-6:
            tracker["buy"] = 0.0
        if abs(tracker["sell"]) < 1e-6:
            tracker["sell"] = 0.0

    def _append_proxy_flow(self, state: dict[str, Any], flow: dict[str, float]) -> None:
        state["flows"].append(flow)
        amount = float(flow["amount"])
        for window, tracker in state["windows"].items():
            self._advance_window_tracker(tracker, float(flow["ts"]), window)
            buy = float(flow["buy"])
            sell = float(flow["sell"])
            tracker["events"].append((float(flow["ts"]), buy, sell))
            tracker["buy"] += buy
            tracker["sell"] += sell
            tracker["bucket_amount"] += amount

    def _reset_proxy_flows(self, state: dict[str, Any], now_ts: float) -> None:
        state["flows"].clear()
        state["windows"] = {
            window: self._new_window_tracker(now_ts, window)
            for window in WINDOWS
        }

    def _refresh_instrument_limits(self, trade_date: date) -> None:
        if self._instrument_limits_date == trade_date:
            return
        limits: dict[str, dict[str, Any]] = {}
        repo = getattr(self._app_state, "repo", None) if self._app_state else None
        if repo is not None:
            try:
                instruments = repo.get_instruments()
                wanted = [
                    column
                    for column in ("symbol", "as_of", "limit_up")
                    if column in instruments.columns
                ]
                if "symbol" in wanted:
                    for row in instruments.select(wanted).to_dicts():
                        symbol = str(row.get("symbol") or "").strip().upper()
                        if symbol:
                            limits[symbol] = row
            except Exception:  # noqa: BLE001
                logger.debug("实时大单读取涨停价维表失败", exc_info=True)
        self._instrument_limits = limits
        self._instrument_limits_date = trade_date

    def _update_price_context(
        self,
        state: dict[str, Any],
        raw: dict[str, Any],
        *,
        symbol: str,
        price: float,
        trade_date: date,
    ) -> None:
        name = str(raw.get("name") or state["name"] or symbol)
        prev_close = _finite(raw.get("prev_close"))
        change_pct = _finite(raw.get("change_pct"))
        if change_pct is None and prev_close is not None and prev_close > 0:
            change_pct = price / prev_close - 1.0

        limit_up_value: float | None = None
        no_price_limit = False
        instrument = self._instrument_limits.get(symbol)
        if instrument and instrument.get("as_of") == trade_date:
            authoritative = _finite(instrument.get("limit_up"))
            if authoritative is not None and authoritative >= 10_000:
                no_price_limit = True
            elif authoritative is not None and authoritative > 0:
                limit_up_value = authoritative
        if limit_up_value is None and not no_price_limit and prev_close is not None and prev_close > 0:
            limit_pct = price_limit_pct(
                symbol,
                trade_date,
                is_risk_warning=is_risk_warning_name(name),
            )
            limit_up_value = limit_price(prev_close, limit_pct, up=True)

        state["change_pct"] = change_pct
        state["limit_up_price"] = limit_up_value
        state["limit_up_gap_pct"] = (
            limit_up_value / price - 1.0
            if limit_up_value is not None and price > 0
            else None
        )
        state["price_limit_assessable"] = no_price_limit or limit_up_value is not None
        state["no_price_limit"] = no_price_limit

    def _on_quote_fetch(self) -> None:
        """行情线程只复制缓存并投递最新任务，不执行开盘啦请求。"""
        if not self._running or not self._config.get("enabled", True) or self._quote_service is None:
            return
        scope_symbols = self._scope_symbols()
        if scope_symbols == set():
            with self._lock:
                self._states.clear()
                self._rankings = {window: () for window in WINDOWS}
            return
        snapshot = self._quote_service.get_latest_quotes(scope_symbols)
        with self._lock:
            self._pending_snapshot = snapshot
            if self._snapshot_running:
                return
            self._snapshot_running = True
        self._snapshot_executor.submit(self._drain_snapshots)

    def _drain_snapshots(self) -> None:
        while self._running:
            with self._lock:
                snapshot = self._pending_snapshot
                self._pending_snapshot = None
                if snapshot is None:
                    self._snapshot_running = False
                    return
            try:
                self._process_snapshot(snapshot)
            except Exception:  # noqa: BLE001
                logger.exception("实时大单快照处理失败")

    def _reset_for_new_day(self) -> None:
        with self._lock:
            self._states.clear()
            self._rankings = {window: () for window in WINDOWS}
            self._last_deep_at.clear()
            self._cooldown_until.clear()
            self._deep_calls_date = self._trade_date
            self._deep_calls_used = 0
            self._instrument_limits_date = None
            self._instrument_limits = {}
            self._filtered_near_limit_count = 0
            self._unassessable_count = 0

    def _process_snapshot(self, records: list[dict]) -> None:
        today = cn_today()
        if today != self._trade_date:
            previous_date = self._trade_date
            self._trade_date = today
            if self._storage is not None:
                try:
                    self._storage.compact(previous_date)
                    self._storage.cleanup_raw_archives(today=today)
                    self._storage.cleanup_orderbook_history(today=today)
                except Exception:  # noqa: BLE001
                    logger.exception("实时大单跨日存储处理失败: %s", previous_date)
            self._reset_for_new_day()
        if not records:
            return
        scope_symbols = self._scope_symbols()
        if scope_symbols is not None:
            with self._lock:
                for stale_symbol in set(self._states) - scope_symbols:
                    self._states.pop(stale_symbol, None)
            records = [
                row for row in records
                if str(row.get("symbol") or "").strip().upper() in scope_symbols
            ]
            if not records:
                return
        now = cn_now()
        now_ts = now.timestamp()
        calculation_started = time.perf_counter()
        self._refresh_instrument_limits(today)
        index_symbols: set[str] = set()
        repo = getattr(self._app_state, "repo", None) if self._app_state else None
        if repo is not None:
            try:
                index_symbols = set(repo.get_index_symbol_set())
            except Exception:  # noqa: BLE001
                index_symbols = set()

        flow_events: list[dict[str, Any]] = []
        event_ts_ms = int(now.timestamp() * 1000)
        with self._lock:
            for raw in records:
                symbol = str(raw.get("symbol") or "").strip().upper()
                if not symbol or symbol in index_symbols:
                    continue
                price = _finite(raw.get("last_price", raw.get("close")))
                amount = _finite(raw.get("amount"))
                volume = _finite(raw.get("volume"))
                if price is None or amount is None or volume is None or amount < 0 or volume < 0:
                    continue
                state = self._states.get(symbol)
                if state is None:
                    state = self._new_state(symbol, raw.get("name"), now_ts)
                    self._states[symbol] = state
                state["name"] = raw.get("name") or state["name"]
                previous = state["snapshots"][-1] if state["snapshots"] else None
                state["snapshots"].append({"ts": now_ts, "price": price, "amount": amount, "volume": volume})
                self._update_price_context(
                    state,
                    raw,
                    symbol=symbol,
                    price=price,
                    trade_date=today,
                )
                if previous is None:
                    continue
                delta_amount = amount - previous["amount"]
                delta_volume = volume - previous["volume"]
                if delta_amount < 0 or delta_volume < 0:
                    # 交易日切换/上游重置，绝不能把重置量当作资金脉冲。
                    self._reset_proxy_flows(state, now_ts)
                    continue
                if delta_amount <= 0 and delta_volume <= 0:
                    continue
                price_delta = price - previous["price"]
                if price_delta > 0:
                    side = 1
                elif price_delta < 0:
                    side = -1
                else:
                    side = state["last_side"] or 1
                state["last_side"] = side
                flow = {
                    "ts": now_ts,
                    "amount": delta_amount,
                    "volume": delta_volume,
                    "buy": delta_amount if side > 0 else 0.0,
                    "sell": delta_amount if side < 0 else 0.0,
                    "price": price,
                }
                self._append_proxy_flow(state, flow)
                flow_events.append({
                    "trade_date": today,
                    "event_ts_ms": event_ts_ms,
                    "symbol": symbol,
                    "name": state["name"],
                    "price": price,
                    "amount": delta_amount,
                    "volume": delta_volume,
                    "delta_amount": delta_amount,
                    "delta_volume": delta_volume,
                    "buy_amount": delta_amount if side > 0 else 0.0,
                    "sell_amount": delta_amount if side < 0 else 0.0,
                    "side": side,
                    "source": "tickflow_proxy",
                    "event_id": f"proxy:{symbol}:{event_ts_ms}:{delta_amount}:{delta_volume}:{price}",
                    "received_at_ms": event_ts_ms,
                    "schema_version": SCHEMA_VERSION,
                    "parser_version": "large_orders_proxy_v1",
                })

            rankings, filtered_near_limit, unassessable = self._build_rankings_locked(now_ts)
            self._rankings = rankings
            self._filtered_near_limit_count = filtered_near_limit
            self._unassessable_count = unassessable
            self._last_update_ms = int(time.time() * 1000)
            self._last_calculation_ms = (time.perf_counter() - calculation_started) * 1000
        if self._storage is not None and flow_events:
            self._storage.submit("proxy_flow", flow_events)
        self._request_orderbook_targets()
        self._schedule_deep_dive()
        if self._quote_service is not None:
            self._quote_service.notify_large_orders_updated()

    def _request_orderbook_targets(self) -> None:
        depth_service = getattr(self._app_state, "depth_service", None) if self._app_state else None
        if depth_service is None:
            return
        scope_symbols = self._scope_symbols()
        if scope_symbols is not None:
            depth_service.request_symbols(scope_symbols)
            return
        symbols = {str(row["symbol"]) for row in self._rankings.get(60, ())}
        try:
            from app.services import preferences
            symbols.update(
                symbol
                for symbol in preferences.get_realtime_watchlist_symbols()
                if not self._is_filtered_symbol(symbol, self._states.get(symbol, {}).get("name"))
            )
        except Exception:  # noqa: BLE001
            pass
        depth_service.request_symbols(symbols)

    def record_depth_snapshots(self, snapshots: list[dict[str, Any]]) -> None:
        """Persist monitored five-level snapshots without blocking quote processing."""
        if self._storage is None or not snapshots:
            return
        now_ms = int(time.time() * 1000)
        rows = []
        position_symbols = self._position_symbols()
        scope_symbols = self._scope_symbols()
        watchlist = set()
        try:
            from app.services import preferences
            watchlist = set(preferences.get_realtime_watchlist_symbols())
        except Exception:  # noqa: BLE001
            pass
        for snapshot in snapshots:
            if scope_symbols is not None and snapshot.get("symbol") not in scope_symbols:
                continue
            row = dict(snapshot)
            row.update({
                "trade_date": self._trade_date,
                "event_ts_ms": int(snapshot.get("fetched_at_ms") or now_ms),
                "name": self._states.get(snapshot["symbol"], {}).get("name", snapshot["symbol"]),
                "price": None,
                "amount": None,
                "volume": None,
                "source": "tickflow_depth5",
                "event_id": f"depth:{snapshot['symbol']}:{snapshot.get('fetched_at_ms', now_ms)}",
                "received_at_ms": now_ms,
                "schema_version": SCHEMA_VERSION,
                "parser_version": "depth5_v1",
                "target_kind": (
                    "position" if position_symbols is not None
                    else "watchlist" if snapshot["symbol"] in watchlist
                    else "candidate"
                ),
            })
            rows.append(row)
        self._storage.submit("orderbook_snapshot", rows)

    def _window_metrics_locked(self, state: dict[str, Any], window: int, now_ts: float) -> dict[str, Any]:
        tracker = state["windows"][window]
        self._advance_window_tracker(tracker, now_ts, window)
        proxy_buy = max(0.0, float(tracker["buy"]))
        proxy_sell = max(0.0, float(tracker["sell"]))
        proxy_amount = proxy_buy + proxy_sell
        cutoff = now_ts - window
        trades = []
        for item in state["trade_events"]:
            event_time = _as_datetime(
                item.get("timestamp") or item.get("event_time") or item.get("time")
            )
            if event_time is not None and event_time.timestamp() >= cutoff:
                trades.append(item)
        active_buy = sum(float(item.get("amount") or 0) for item in trades if item.get("direction") == "active_buy")
        active_sell = sum(float(item.get("amount") or 0) for item in trades if item.get("direction") == "active_sell")
        precise = active_buy + active_sell > 0
        intents = []
        for item in state["intent_events"]:
            event_time = _as_datetime(
                item.get("timestamp") or item.get("event_time") or item.get("time")
            )
            if event_time is not None and event_time.timestamp() >= cutoff:
                intents.append(item)
        cancel_count = sum(bool(item.get("cancel_flag")) for item in intents)
        buy, sell = (active_buy, active_sell) if precise else (proxy_buy, proxy_sell)
        amount = buy + sell
        baseline = [float(item) for item in tracker["history"] if _finite(item) is not None]
        threshold = _large_threshold(baseline)
        zscore = _robust_z(proxy_amount, baseline)
        return {
            "amount": amount,
            "buy": buy,
            "sell": sell,
            "net": buy - sell,
            "buy_ratio": buy / amount if amount > 0 else 0.0,
            "zscore": zscore,
            "threshold": threshold,
            "max_order": max((float(item.get("amount") or 0) for item in trades), default=0.0),
            "precise": precise,
            "intent_count": len(intents),
            "cancel_count": cancel_count,
            "cancel_rate": cancel_count / len(intents) if intents else 0.0,
        }

    def _net_flow_metrics_locked(self, state: dict[str, Any]) -> dict[str, Any]:
        points = [
            item
            for item in state["net_flow_points"]
            if _finite(item.get("net_amount")) is not None
            and _as_datetime(item.get("timestamp")) is not None
        ]
        if not points:
            return {"available": False}
        points.sort(key=lambda item: float(item["timestamp"]))
        latest = points[-1]
        latest_ts = float(latest["timestamp"])
        window_points = points[-6:]
        baseline = window_points[0]
        latest_amount = float(latest["net_amount"])
        baseline_amount = float(baseline["net_amount"])
        elapsed_minutes = float(max(len(window_points) - 1, 1))
        delta = latest_amount - baseline_amount
        speed = delta / elapsed_minutes
        return {
            "available": True,
            "net_flow_amount": latest_amount,
            "net_flow_delta": delta,
            "net_flow_speed": speed,
            "net_flow_direction": "rising" if speed > 0 else "falling" if speed < 0 else "flat",
            "net_flow_as_of": datetime.fromtimestamp(latest_ts, tz=CN_TZ).isoformat(),
            "net_flow_window_minutes": elapsed_minutes,
        }

    def _build_rankings_locked(
        self,
        now_ts: float,
    ) -> tuple[dict[int, tuple[dict[str, Any], ...]], int, int]:
        rows_by_window: dict[int, list[dict[str, Any]]] = {window: [] for window in WINDOWS}
        depth_metrics: dict[str, dict[str, float]] = {}
        depth_service = getattr(self._app_state, "depth_service", None) if self._app_state else None
        if depth_service is not None:
            try:
                depth_metrics = depth_service.get_cached_metrics(set(self._states))
            except Exception:  # noqa: BLE001
                depth_metrics = {}
        filtered_near_limit = 0
        unassessable = 0
        position_symbols = self._position_symbols()
        scope_symbols = self._scope_symbols()
        for symbol, state in self._states.items():
            if position_symbols is None and self._is_filtered_symbol(symbol, state.get("name")):
                continue
            metrics_by_window = {
                window: self._window_metrics_locked(state, window, now_ts)
                for window in WINDOWS
            }
            net_flow = self._net_flow_metrics_locked(state)
            if (
                not any(metrics["amount"] > 0 for metrics in metrics_by_window.values())
                and not net_flow["available"]
            ):
                continue
            limit_gap = _finite(state.get("limit_up_gap_pct"))
            latest = state["snapshots"][-1] if state["snapshots"] else {}
            change_pct = _finite(state.get("change_pct"))
            book = depth_metrics.get(symbol, {})
            imbalance = float(book.get("book_imbalance") or 0)
            serialized_windows = {
                str(window): {
                    key: value
                    for key, value in metrics.items()
                    if key != "precise"
                }
                for window, metrics in metrics_by_window.items()
            }
            for window, metrics in metrics_by_window.items():
                if metrics["amount"] <= 0 and not net_flow["available"]:
                    continue
                price_confirmed = (change_pct is not None and change_pct > 0) or metrics["buy_ratio"] >= 0.65
                score = (
                    min(1.0, max(0.0, metrics["net"] / max(metrics["threshold"] * 3.0, 1.0))) * 35.0
                    + min(1.0, max(0.0, metrics["zscore"] / 5.0)) * 25.0
                    + min(1.0, max(0.0, (metrics["buy_ratio"] - 0.5) / 0.3)) * 15.0
                    + (15.0 if price_confirmed else 0.0)
                    + min(1.0, max(0.0, imbalance)) * 10.0
                )
                deep = bool(metrics["precise"])
                net_flow_available = bool(net_flow["available"])
                confidence = (
                    "high"
                    if deep and score >= 75 and metrics["buy_ratio"] >= 0.65
                    else "medium"
                )
                rows_by_window[window].append({
                    "symbol": symbol,
                    "name": state["name"],
                    "score": round(min(100.0, score), 2),
                    "confidence": confidence,
                    "source": (
                        "kaipanla" if deep
                        else "kaipanla_net_flow" if net_flow_available
                        else "tick_proxy"
                    ),
                    "data_quality": (
                        "precise" if deep
                        else "net_flow" if net_flow_available
                        else "proxy_only"
                    ),
                    "active_buy_amount": round(metrics["buy"], 2),
                    "active_sell_amount": round(metrics["sell"], 2),
                    "net_buy_amount": round(metrics["net"], 2),
                    "buy_ratio": round(metrics["buy_ratio"], 4),
                    "max_order_amount": round(metrics["max_order"], 2),
                    "intent_count": metrics["intent_count"],
                    "cancel_rate": round(metrics["cancel_rate"], 4),
                    "change_pct": round(change_pct, 6) if change_pct is not None else None,
                    "limit_up_price": round(float(state["limit_up_price"]), 2) if state.get("limit_up_price") is not None else None,
                    "limit_up_gap_pct": round(limit_gap, 6) if limit_gap is not None else None,
                    "last_seen_ts": round(float(latest["ts"]), 3) if latest.get("ts") is not None else None,
                    "freshness_ms": max(0, int((now_ts - float(latest.get("ts") or now_ts)) * 1000)),
                    "large_threshold": round(metrics["threshold"], 2),
                    "zscore": round(metrics["zscore"], 3),
                    "ofi": round(float(book.get("ofi") or 0), 2),
                    "book_imbalance": round(imbalance, 4),
                    **net_flow,
                    "windows": serialized_windows,
                    "explanation": self._explanation(metrics, deep, price_confirmed),
                })
        limit = int(self._config.get("candidate_limit", 50))
        rankings: dict[int, tuple[dict[str, Any], ...]] = {}
        for window, rows in rows_by_window.items():
            rows.sort(key=lambda row: (row["score"], row["net_buy_amount"]), reverse=True)
            rankings[window] = tuple(rows[:limit])
        return rankings, filtered_near_limit, unassessable

    @staticmethod
    def _explanation(metrics: dict[str, float], deep: bool, price_confirmed: bool) -> str:
        parts = ["主动成交" if deep else "快照方向代理", "净买额为正" if metrics["net"] > 0 else "净买额偏弱"]
        if metrics["zscore"] >= 2.5:
            parts.append("成交额突增")
        if price_confirmed:
            parts.append("价格确认")
        return "、".join(parts)

    @staticmethod
    def _cancel_rate_locked(state: dict[str, Any]) -> float:
        intents = list(state["intent_events"])
        if not intents:
            return 0.0
        return round(sum(bool(item.get("cancel_flag")) for item in intents) / len(intents), 4)

    def _schedule_deep_dive(self) -> None:
        if not self._config.get("enabled", True) or load_credentials() is None:
            return
        now = time.time()
        ranked = list(self._rankings.get(60, ()))
        position_symbols = self._position_symbols()
        scope_symbols = self._scope_symbols()
        watchlist: list[str] = []
        if position_symbols is None:
            try:
                from app.services import preferences

                watchlist = preferences.get_realtime_watchlist_symbols()
            except Exception:  # noqa: BLE001
                pass
        with self._lock:
            if self._deep_calls_date != cn_today():
                self._deep_calls_date = cn_today()
                self._deep_calls_used = 0
            eligible_watchlist = [
                symbol
                for symbol in watchlist
                if not self._is_filtered_symbol(symbol, self._states.get(symbol, {}).get("name"))
            ]
            symbols = (
                sorted(scope_symbols or set())
                if scope_symbols is not None
                else list(dict.fromkeys(eligible_watchlist + [str(row["symbol"]) for row in ranked]))
            )
            limit = max(0, int(self._config.get("max_deep_dive_symbols", 3)))
            interval = max(15.0, float(self._config.get("deep_dive_interval_seconds", 60)))
            for symbol in symbols:
                if len(self._deep_pending) >= limit or symbol in self._deep_pending:
                    break
                if now - self._last_deep_at.get(symbol, 0.0) < interval:
                    continue
                self._deep_pending.add(symbol)
                self._last_deep_at[symbol] = now
                self._deep_calls_used += 2
                self._deep_executor.submit(self._deep_dive, symbol)

    def _deep_dive(self, symbol: str) -> None:
        try:
            asyncio.run(self._deep_dive_async(symbol))
        finally:
            with self._lock:
                self._deep_pending.discard(symbol)

    async def _deep_dive_async(self, symbol: str) -> None:
        credentials = load_credentials()
        if credentials is None:
            return
        trade_payload: dict[str, Any] | None = None
        intent_payload: dict[str, Any] | None = None
        stock_id = symbol.split(".", 1)[0]
        try:
            async with KaipanlaClient(credentials=credentials, attempts=1) as client:
                trade_payload = await client.request(13, {"StockID": stock_id})
                intent_payload = await client.request(14, {"StockID": stock_id})
        except (KaipanlaRequestError, ValueError) as exc:
            with self._lock:
                state = self._states.get(symbol)
                if state:
                    state["deep_error"] = str(exc)
                    state["deep_source"] = "proxy_only"
            self._last_error = "开盘啦深挖暂不可用"
            return
        if self._storage is not None:
            raw_batch = int(time.time() * 1000)
            for kind, payload, endpoint in (
                ("kaipanla_trade", trade_payload, 13),
                ("kaipanla_intent", intent_payload, 14),
            ):
                if payload is None:
                    continue
                try:
                    content_hash = stable_content_hash(payload)
                    self._storage.archive_payload(
                        kind,
                        self._trade_date,
                        f"{symbol}-{endpoint}-{raw_batch}-{content_hash[:16]}",
                        payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._last_error = f"开盘啦原始响应归档失败: {exc}"
                    logger.exception("开盘啦原始响应归档失败 symbol=%s endpoint=%s", symbol, endpoint)
        try:
            trades = parse_large_order_trades(trade_payload or {}, symbol)
        except (ResponseShapeError, ValueError) as exc:
            with self._lock:
                state = self._states.get(symbol)
                if state:
                    state["deep_error"] = str(exc)
                    state["deep_source"] = "proxy_only"
            self._last_error = "开盘啦深挖响应解析失败"
            return
        intent_error = None
        try:
            intents = parse_large_order_intents(intent_payload or {}, symbol)
        except (ResponseShapeError, ValueError) as exc:
            intents = []
            intent_error = str(exc)
        now_ms = int(time.time() * 1000)
        trade_rows: list[dict[str, Any]] = []
        intent_rows: list[dict[str, Any]] = []
        calculation_started = time.perf_counter()
        with self._lock:
            state = self._states.get(symbol)
            if state is None:
                state = self._new_state(symbol, symbol, time.time())
                self._states[symbol] = state
            for event in trades:
                if event["event_id"] in state["trade_ids"]:
                    continue
                state["trade_ids"].add(event["event_id"])
                state["trade_events"].append({**event, "event_time": event.get("time")})
                event_time = _as_datetime(event.get("timestamp") or event.get("time"))
                trade_rows.append({
                    "trade_date": self._trade_date,
                    "event_ts_ms": int(event_time.timestamp() * 1000) if event_time else now_ms,
                    "symbol": symbol,
                    "name": state["name"],
                    "price": event.get("price"),
                    "amount": event.get("amount"),
                    "volume": event.get("volume"),
                    "source": "kaipanla_13",
                    "event_id": event["event_id"],
                    "received_at_ms": now_ms,
                    "schema_version": SCHEMA_VERSION,
                    "parser_version": "kaipanla_v1",
                    "direction": event.get("direction"),
                    "direction_code": event.get("direction_code"),
                    "event_time": event.get("time"),
                })
            for event in intents:
                if event["event_id"] not in state["intent_ids"]:
                    state["intent_ids"].add(event["event_id"])
                    state["intent_events"].append(event)
                    intent_ts = _as_datetime(event.get("timestamp") or event.get("time"))
                    intent_rows.append({
                        "trade_date": self._trade_date,
                        "event_ts_ms": int(intent_ts.timestamp() * 1000) if intent_ts else now_ms,
                        "symbol": symbol,
                        "name": state["name"],
                        "price": event.get("price"),
                        "amount": event.get("amount"),
                        "volume": event.get("volume"),
                        "source": "kaipanla_14",
                        "event_id": event["event_id"],
                        "received_at_ms": now_ms,
                        "schema_version": SCHEMA_VERSION,
                        "parser_version": "kaipanla_v1",
                        "order_id": event.get("order_id"),
                        "side": event.get("side"),
                        "side_code": event.get("side_code"),
                        "limit_flag": event.get("limit_flag"),
                        "limit_flag_code": event.get("limit_flag_code"),
                        "cancel_flag": event.get("cancel_flag"),
                        "cancel_flag_code": event.get("cancel_flag_code"),
                        "event_time": event.get("time"),
                        "raw_tail": event.get("raw_tail"),
                    })
            state["intent_ids"] = {item["event_id"] for item in state["intent_events"]}
            state["deep_source"] = "kaipanla"
            state["deep_error"] = intent_error
            state["last_deep_ms"] = now_ms
            rankings, filtered_near_limit, unassessable = self._build_rankings_locked(time.time())
            self._rankings = rankings
            self._filtered_near_limit_count = filtered_near_limit
            self._unassessable_count = unassessable
            alerts = self._build_alerts_locked(symbol)
            self._last_update_ms = now_ms
            self._last_calculation_ms = (time.perf_counter() - calculation_started) * 1000
            self._last_error = "开盘啦委托响应解析失败" if intent_error else None
        if self._storage is not None:
            self._storage.submit("kaipanla_trade", trade_rows)
            self._storage.submit("kaipanla_intent", intent_rows)
        if self._quote_service is not None:
            self._quote_service.notify_large_orders_updated()
            if alerts:
                self._quote_service.push_alerts(alerts)
                self._persist_alerts(alerts)
                self._dispatch_alert_notifications(alerts)

    def _build_alerts_locked(self, symbol: str) -> list[dict]:
        if self._quote_service is not None:
            quote_status = self._quote_service.status()
            quote_age = quote_status.get("quote_age_ms")
            interval = float(quote_status.get("interval_s") or 6)
            if quote_age is None or quote_age < 0 or quote_age > max(interval * 2, 30) * 1000:
                return []
        row = next((item for item in self._rankings.get(60, ()) if item["symbol"] == symbol), None)
        if row is None or row["source"] != "kaipanla" or row["score"] < float(self._config["score_threshold"]):
            return []
        metrics = row["windows"]["60"]
        now = time.time()
        if metrics["net"] <= metrics["threshold"] or metrics["buy_ratio"] < 0.65 or metrics["zscore"] < 2.5:
            return []
        if now < self._cooldown_until.get(symbol, 0.0):
            return []
        self._cooldown_until[symbol] = now + float(self._config["cooldown_seconds"])
        position_mode = self._position_symbols() is not None
        return [{
            "ts": int(now * 1000),
            "source": "position_risk" if position_mode else "large_order",
            "type": "large_order_buy",
            "rule_id": self._config["version"],
            "rule_name": "实时大单",
            "symbol": symbol,
            "name": row["name"],
            "message": f"{row['name']} 主力买入候选：60秒主动净买额 {row['net_buy_amount']:,.0f} 元，评分 {row['score']:.0f}",
            "price": None,
            "change_pct": row["change_pct"],
            "signals": [row["explanation"]],
            "severity": "warn",
            "conditions": [
                "active_buy_ratio>=65%",
                "zscore>=2.5",
                "price_confirmed",
            ],
            "logic": "and",
        }]

    def _persist_alerts(self, alerts: list[dict]) -> None:
        try:
            if self._app_state and getattr(self._app_state, "repo", None):
                from app.services import alert_store

                alert_store.append_many(self._app_state.repo.store.data_dir, alerts)
        except Exception:  # noqa: BLE001
            logger.debug("大单告警归档失败", exc_info=True)

    @staticmethod
    def _dispatch_alert_notifications(alerts: list[dict]) -> None:
        """按全局消息渠道异步投递大单告警，不阻塞行情或深挖线程。"""
        from app.services import preferences, webhook_adapter

        channels = set(preferences.get_webhook_default_channels())
        if not channels:
            return
        feishu_url = preferences.get_feishu_webhook_url()
        feishu_secret = preferences.get_feishu_webhook_secret()
        wecom_url = preferences.get_wecom_webhook_url()
        for alert in alerts:
            title = str(alert.get("rule_name") or "实时大单")
            body = str(alert.get("message") or "").strip()
            if not body:
                continue
            if "feishu" in channels and feishu_url:
                _LARGE_ORDER_WEBHOOK_EXECUTOR.submit(
                    webhook_adapter.send_feishu,
                    feishu_url,
                    title,
                    body,
                    feishu_secret,
                )
            if "wecom" in channels and wecom_url:
                _LARGE_ORDER_WEBHOOK_EXECUTOR.submit(
                    webhook_adapter.send_wecom,
                    wecom_url,
                    title,
                    body,
                )

    def status(self) -> dict:
        quote_status = self._quote_service.status() if self._quote_service is not None else {}
        quote_age = quote_status.get("quote_age_ms")
        interval = float(quote_status.get("interval_s") or 6)
        stale = quote_age is None or quote_age < 0 or quote_age > max(interval * 2, 30) * 1000
        ranking = self._rankings.get(60, ())
        precise = sum(1 for row in ranking if row.get("data_quality") == "precise")
        net_flow = sum(1 for row in ranking if row.get("data_quality") == "net_flow")
        return {
            "enabled": bool(self._config.get("enabled", True)),
            "running": self._running,
            "data_source": "kaipanla" if load_credentials() else "proxy_only",
            "mode": "stale" if stale else "live",
            "stale": stale,
            "coverage_count": quote_status.get("symbol_count", 0),
            "candidate_count": len(ranking),
            "precise_count": precise,
            "net_flow_count": net_flow,
            "filtered_near_limit_count": self._filtered_near_limit_count,
            "unassessable_count": self._unassessable_count,
            "last_updated_ms": self._last_update_ms,
            "last_calculation_ms": round(self._last_calculation_ms, 2),
            "last_error": self._last_error,
            "market_phase": quote_status.get("market_phase"),
            "is_trading_hours": quote_status.get("is_trading_hours", False),
            "config_version": self._config["version"],
            "deep_dive_symbol_limit": int(self._config.get("max_deep_dive_symbols", 3)),
            "deep_dive_request_count": self._deep_calls_used,
            "storage": self._storage.status() if self._storage is not None else {
                "enabled": False,
                "queued_rows": 0,
                "written_rows": 0,
                "dropped_rows": 0,
                "invalid_rows": 0,
                "last_flush_ms": None,
                "last_error": None,
                "storage_root": None,
            },
        }

    def ranking(
        self,
        window: int = 60,
        scope: str = "all",
        mode: str = "combined",
    ) -> dict:
        if window not in WINDOWS:
            window = 60
        rows = [dict(row) for row in self._rankings.get(window, ())]
        if mode == "execution":
            rows = [row for row in rows if row.get("data_quality") == "precise"]
        elif mode == "intent":
            rows = [row for row in rows if int(row.get("intent_count") or 0) > 0]
        if scope == "watchlist":
            try:
                from app.services import preferences

                symbols = set(preferences.get_realtime_watchlist_symbols())
                rows = [row for row in rows if row["symbol"] in symbols]
            except Exception:  # noqa: BLE001
                rows = []
        quote_status = self._quote_service.status() if self._quote_service is not None else {}
        quote_age = quote_status.get("quote_age_ms")
        interval = float(quote_status.get("interval_s") or 6)
        stale = quote_age is None or quote_age < 0 or quote_age > max(interval * 2, 30) * 1000
        return {
            "rows": rows,
            "count": len(rows),
            "window": window,
            "scope": scope,
            "mode": mode,
            "stale": stale,
            "last_updated_ms": self._last_update_ms,
        }

    def history(
        self,
        trade_date,
        *,
        kind: str | None = None,
        mode: str = "combined",
        symbol: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        cursor: str | None = None,
        limit: int = 1000,
        order: str = "asc",
    ) -> dict:
        kinds_by_mode = {
            "combined": ("proxy_flow", "kaipanla_trade", "kaipanla_intent", "orderbook_snapshot"),
            "execution": ("proxy_flow", "kaipanla_trade"),
            "intent": ("kaipanla_intent",),
        }
        kinds = (kind,) if kind else kinds_by_mode.get(mode, kinds_by_mode["combined"])
        if self._storage is None:
            return {
                "rows": [],
                "count": 0,
                "has_more": False,
                "truncated": False,
                "next_cursor": None,
                "kind": kind,
                "kinds": list(kinds),
                "mode": mode,
                "date": trade_date.isoformat(),
            }
        result = self._storage.query_events(
            trade_date,
            kinds=kinds,
            symbol=symbol,
            from_ms=from_ms,
            to_ms=to_ms,
            cursor=cursor,
            limit=limit,
            order=order,
        )
        return {
            **result,
            "kind": kind or (kinds[0] if len(kinds) == 1 else None),
            "kinds": list(kinds),
            "mode": mode,
            "date": trade_date.isoformat(),
        }

    def available_history_dates(self, limit: int = 30) -> list[str]:
        return self._storage.available_dates(limit=limit) if self._storage is not None else []

    @staticmethod
    def _aggregate_reconciliation_frame(
        frame: pl.DataFrame,
        *,
        kind: str,
    ) -> pl.DataFrame:
        keys = ["symbol", "bucket_start_ms"]
        if frame.is_empty():
            return pl.DataFrame()
        frame = frame.with_columns(
            ((pl.col("event_ts_ms") // 60_000) * 60_000).alias("bucket_start_ms")
        )
        name = pl.col("name").drop_nulls().first().alias("name")
        if kind == "proxy_flow":
            return frame.group_by(keys).agg(
                name,
                pl.col("buy_amount").fill_null(0).sum().alias("proxy_buy_amount"),
                pl.col("sell_amount").fill_null(0).sum().alias("proxy_sell_amount"),
                pl.len().alias("proxy_event_count"),
            )
        if kind == "kaipanla_trade":
            return frame.group_by(keys).agg(
                name,
                pl.when(pl.col("direction") == "active_buy")
                .then(pl.col("amount").fill_null(0))
                .otherwise(0)
                .sum()
                .alias("precise_buy_amount"),
                pl.when(pl.col("direction") == "active_sell")
                .then(pl.col("amount").fill_null(0))
                .otherwise(0)
                .sum()
                .alias("precise_sell_amount"),
                pl.len().alias("precise_event_count"),
            )
        return frame.group_by(keys).agg(
            name,
            pl.len().alias("intent_count"),
            pl.col("cancel_flag").fill_null(False).cast(pl.Int64).sum().alias("cancel_count"),
        )

    def reconciliation(
        self,
        trade_date: date,
        *,
        symbol: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 1000,
        order: str = "desc",
    ) -> dict:
        if self._storage is None:
            return {
                "rows": [],
                "count": 0,
                "truncated": False,
                "date": trade_date.isoformat(),
                "summary": self._reconciliation_summary(pl.DataFrame(), None),
            }
        frames = []
        for kind in ("proxy_flow", "kaipanla_trade", "kaipanla_intent"):
            source = self._storage.read_day(
                kind,
                trade_date,
                symbol=symbol,
                from_ms=from_ms,
                to_ms=to_ms,
            )
            aggregated = self._aggregate_reconciliation_frame(source, kind=kind)
            if not aggregated.is_empty():
                frames.append(aggregated)
        merged = pl.DataFrame()
        for frame in frames:
            if merged.is_empty():
                merged = frame
                continue
            merged = merged.join(
                frame,
                on=["symbol", "bucket_start_ms"],
                how="full",
                coalesce=True,
                suffix="_incoming",
            ).with_columns(
                pl.coalesce("name", "name_incoming").alias("name")
            ).drop("name_incoming")
        data_dir = self._storage.data_dir
        from app.plugins.kaipanla.storage import read_funds_large_order_reference

        reference = read_funds_large_order_reference(
            data_dir,
            trade_date,
            symbol=symbol,
        )
        reference_available = not reference.is_empty()
        if not merged.is_empty():
            numeric_defaults = {
                "proxy_buy_amount": 0.0,
                "proxy_sell_amount": 0.0,
                "proxy_event_count": 0,
                "precise_buy_amount": 0.0,
                "precise_sell_amount": 0.0,
                "precise_event_count": 0,
                "intent_count": 0,
                "cancel_count": 0,
            }
            merged = merged.with_columns(
                [
                    pl.col(column).fill_null(value).alias(column)
                    if column in merged.columns
                    else pl.lit(value).alias(column)
                    for column, value in numeric_defaults.items()
                ]
            )
            if reference_available:
                merged = merged.join(reference, on="symbol", how="left")
            else:
                merged = merged.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias("main_net_amount_over_300k")
                )
            proxy_total = pl.col("proxy_buy_amount") + pl.col("proxy_sell_amount")
            precise_total = pl.col("precise_buy_amount") + pl.col("precise_sell_amount")
            merged = merged.with_columns(
                (pl.col("proxy_buy_amount") - pl.col("proxy_sell_amount")).alias("proxy_net_amount"),
                (pl.col("precise_buy_amount") - pl.col("precise_sell_amount")).alias("precise_net_amount"),
                (pl.col("cancel_count") / pl.col("intent_count")).fill_nan(0).alias("cancel_rate"),
                pl.when(proxy_total > 0)
                .then((precise_total / proxy_total).clip(0, 1))
                .otherwise(None)
                .alias("precise_coverage"),
            ).with_columns(
                (pl.col("precise_net_amount") - pl.col("proxy_net_amount")).alias("net_difference"),
                pl.when(
                    (pl.col("proxy_event_count") > 0)
                    & (pl.col("precise_event_count") > 0)
                    & pl.col("main_net_amount_over_300k").is_null()
                )
                .then(pl.lit("reference_missing"))
                .when((pl.col("proxy_event_count") > 0) & (pl.col("precise_event_count") > 0))
                .then(pl.lit("matched"))
                .when(pl.col("proxy_event_count") > 0)
                .then(pl.lit("proxy_only"))
                .when(pl.col("precise_event_count") > 0)
                .then(pl.lit("precise_only"))
                .otherwise(pl.lit("intent_only"))
                .alias("status"),
            )
        summary = self._reconciliation_summary(merged, reference)
        if merged.is_empty():
            rows = []
            truncated = False
        else:
            merged = merged.sort(
                ["bucket_start_ms", "symbol"],
                descending=[order == "desc", order == "desc"],
            )
            truncated = merged.height > limit
            rows = merged.head(limit).to_dicts()
        return {
            "rows": rows,
            "count": len(rows),
            "truncated": truncated,
            "date": trade_date.isoformat(),
            "summary": summary,
        }

    @staticmethod
    def _reconciliation_summary(
        merged: pl.DataFrame,
        reference: pl.DataFrame | None,
    ) -> dict[str, Any]:
        reference_values = []
        if reference is not None and "main_net_amount_over_300k" in reference.columns:
            reference_values = reference["main_net_amount_over_300k"].drop_nulls().to_list()
        if merged.is_empty():
            return {
                "proxy_net_amount": 0.0,
                "precise_net_amount": 0.0,
                "net_difference": 0.0,
                "matched_buckets": 0,
                "precise_coverage": 0.0,
                "daily_reference_net": sum(reference_values) if reference_values else None,
                "reference_status": "available" if reference_values else "reference_missing",
            }
        proxy_net = float(merged["proxy_net_amount"].sum())
        precise_net = float(merged["precise_net_amount"].sum())
        proxy_buckets = int((merged["proxy_event_count"] > 0).sum())
        matched = int(
            ((merged["proxy_event_count"] > 0) & (merged["precise_event_count"] > 0)).sum()
        )
        return {
            "proxy_net_amount": proxy_net,
            "precise_net_amount": precise_net,
            "net_difference": precise_net - proxy_net,
            "matched_buckets": matched,
            "precise_coverage": matched / proxy_buckets if proxy_buckets else 0.0,
            "daily_reference_net": sum(reference_values) if reference_values else None,
            "reference_status": "available" if reference_values else "reference_missing",
        }

    def tape(self, symbol: str) -> dict:
        normalized = str(symbol).strip().upper()
        with self._lock:
            state = self._states.get(normalized)
            if state is None:
                return {"symbol": normalized, "trades": [], "intents": [], "timeline": [], "source": "proxy_only"}
            timeline = []
            for point in list(state["flows"])[-300:]:
                timeline.append({"ts": point["ts"], "amount": point["amount"], "buy": point["buy"], "sell": point["sell"], "price": point["price"]})
            return {
                "symbol": normalized,
                "name": state["name"],
                "trades": list(state["trade_events"]),
                "intents": list(state["intent_events"]),
                "net_flow": list(state["net_flow_points"]),
                "timeline": timeline,
                "source": state["deep_source"],
                "last_deep_ms": state["last_deep_ms"],
                "error": state["deep_error"],
            }

    def analysis(self, symbol: str, *, limit: int = 120) -> dict[str, Any]:
        normalized = str(symbol).strip().upper()
        depth_service = getattr(self._app_state, "depth_service", None) if self._app_state else None
        orderbook = {}
        if depth_service is not None:
            orderbook = depth_service.get_cached_orderbooks({normalized}).get(normalized, {})
        snapshot_rows: list[dict] = []
        if self._storage is not None:
            result = self._storage.query("orderbook_snapshot", self._trade_date, symbol=normalized, limit=limit, order="asc")
            snapshot_rows = result["rows"]
        ranking = next((row for row in self._rankings.get(60, ()) if row["symbol"] == normalized), None)
        return {
            "symbol": normalized,
            "name": (ranking or {}).get("name", normalized),
            "ranking": ranking,
            "orderbook": orderbook or None,
            "orderbook_history": snapshot_rows,
            "tape": self.tape(normalized),
            "evidence": {
                "proxy": bool(ranking),
                "execution": bool(ranking and ranking.get("data_quality") == "precise"),
                "intent": bool(ranking and ranking.get("intent_count", 0)),
                "orderbook": bool(orderbook),
            },
            "degraded_reason": None if orderbook else "当前标的不在盘口采样池或数据源无五档能力",
        }
