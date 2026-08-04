"""模拟盘共享行情与独立策略进程运行时。"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import queue
import threading
import time
from collections import deque
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, time as clock_time, timedelta
from hashlib import sha256
from itertools import groupby
from pathlib import Path
from typing import Any

from app.free_strategy.bars import Bar, rows_to_bars
from app.free_strategy.continuation import compact_paper_checkpoint
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine, Quote, RiskConfig
from app.free_strategy.store import PaperAccountStore, now_iso
from app.market_time import as_cn_naive, cn_naive_from_timestamp, cn_naive_now, cn_today

logger = logging.getLogger(__name__)
_PAPER_WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="paper-webhook")

MARKET_MODES = {"bar_1m", "bar_1d", "poll_3s", "websocket"}
LEGACY_MARKET_MODES = {"bar_5m", "bar_30m"}
QUOTE_MODES = {"poll_3s", "websocket"}
WS_SYMBOL_LIMIT = 100
_WORKER_RESTART_LIMIT = 3
_WORKER_RESTART_WINDOW_SECONDS = 300.0


def state_market_mode(state: dict[str, Any]) -> str:
    explicit = state.get("market_mode") or state.get("config", {}).get("market_mode")
    if explicit:
        return str(explicit)
    timeframe = str(state.get("config", {}).get("timeframe", "1d"))
    return {"1m": "bar_1m", "5m": "bar_5m", "30m": "bar_30m"}.get(timeframe, "bar_1d")


def _compatible_checkpoint(source: str, checkpoint: dict[str, Any]) -> dict[str, Any]:
    """Migrate checkpoint state keys that changed with a strategy template revision."""
    migrated = deepcopy(checkpoint)
    state = migrated.get("state")
    if not isinstance(state, dict):
        return migrated
    legacy = state.get("five_fortunes")
    if (
        "five_fortunes_v2" in source
        and isinstance(legacy, dict)
        and legacy.get("version") == "2.0"
        and "five_fortunes_v2" not in state
    ):
        state["five_fortunes_v2"] = legacy
        state.pop("five_fortunes", None)
    return migrated


@dataclass
class _Subscription:
    account_id: str
    mode: str
    symbols: set[str]
    asset_type: str
    input_queue: Any
    last_bar: str = ""
    valuation_symbols: set[str] | None = None
    execution_mode: str = "full_bar"
    scheduled_times: tuple[str, ...] = ()
    last_dispatch_cutoff: str = ""
    last_dispatch_at: float = 0.0


def _put_latest(target: Any, payload: dict[str, Any]) -> None:
    try:
        target.put_nowait(payload)
        return
    except queue.Full:
        pass
    try:
        target.get_nowait()
    except queue.Empty:
        pass
    try:
        target.put_nowait(payload)
    except queue.Full:
        logger.warning("模拟盘行情队列持续拥塞，丢弃账户 %s 的一批行情", payload.get("account_id"))


def _put_bar_batch(target: Any, payload: dict[str, Any]) -> bool:
    try:
        target.put(payload, block=True, timeout=0.5)
        return True
    except queue.Full:
        return False


def _quote_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    symbol = str(raw.get("symbol") or "")
    price = raw.get("last_price", raw.get("close"))
    if not symbol or price is None:
        return None
    timestamp = raw.get("timestamp") or cn_naive_now().isoformat()
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()
    elif isinstance(timestamp, (int, float)):
        seconds = float(timestamp) / 1000 if float(timestamp) > 10_000_000_000 else float(timestamp)
        timestamp = cn_naive_from_timestamp(seconds).isoformat()
    return {
        "symbol": symbol,
        "timestamp": str(timestamp),
        "last_price": float(price),
        "prev_close": raw.get("prev_close"),
        "open": raw.get("open"),
        "high": raw.get("high"),
        "low": raw.get("low"),
        "volume": float(raw.get("volume") or 0),
        "amount": float(raw.get("amount") or 0),
        "name": raw.get("name"),
        "limit_up": raw.get("limit_up"),
        "limit_down": raw.get("limit_down"),
        "suspended": bool(raw.get("suspended", False)),
    }


def _quotes_from_records(records: list[dict[str, Any]]) -> list[Quote]:
    quotes = []
    for raw in records:
        values = dict(raw)
        parsed = datetime.fromisoformat(str(values["timestamp"]).replace("Z", "+00:00"))
        values["timestamp"] = as_cn_naive(parsed)
        quotes.append(Quote(**values))
    return quotes


class MarketDataHub:
    """跨账户共享行情轮询、闭合 K 线时钟和 WebSocket 连接。"""

    def __init__(self, quote_service: Any, repo: Any) -> None:
        self.quote_service = quote_service
        self.repo = repo
        self._lock = threading.RLock()
        self._subscriptions: dict[str, _Subscription] = {}
        self._poll_leased = False
        self._bar_stop = threading.Event()
        self._bar_thread: threading.Thread | None = None
        self._stream = None
        self._ws_symbols: set[str] = set()
        self._ws_state = "disconnected"
        self._ws_error: str | None = None
        self._ws_disconnected_at: datetime | None = None
        self._last_quote_at: str | None = None
        self._bar_attempts: dict[tuple[str, str], tuple[datetime, float]] = {}

    def register(
        self,
        account_id: str,
        mode: str,
        symbols: set[str],
        asset_type: str,
        input_queue: Any,
        last_bar: str = "",
        valuation_symbols: set[str] | None = None,
        execution_mode: str = "full_bar",
        scheduled_times: list[str] | tuple[str, ...] = (),
    ) -> None:
        if mode not in MARKET_MODES | LEGACY_MARKET_MODES:
            raise ValueError(f"不支持的行情模式: {mode}")
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        valuation = {str(symbol).strip().upper() for symbol in (valuation_symbols or set()) if str(symbol).strip()}
        with self._lock:
            if mode == "websocket":
                combined = cleaned | self._websocket_symbols(exclude=account_id)
                if len(combined) > WS_SYMBOL_LIMIT:
                    raise ValueError(f"WebSocket 去重订阅最多 {WS_SYMBOL_LIMIT} 只，当前需要 {len(combined)} 只")
            previous = self._subscriptions.get(account_id)
            self._subscriptions[account_id] = _Subscription(
                account_id,
                mode,
                cleaned,
                asset_type,
                input_queue,
                last_bar,
                valuation,
                execution_mode,
                tuple(scheduled_times),
            )
            try:
                if mode == "poll_3s" and not self._poll_leased:
                    self.quote_service.add_fetch_listener(self._on_poll_quotes)
                    self.quote_service.acquire_temporary_polling(3.0)
                    self._poll_leased = True
                if mode.startswith("bar_"):
                    self._ensure_bar_thread()
                if mode == "websocket":
                    self._sync_websocket()
                self.quote_service.set_symbol_consumer(f"paper:{account_id}", valuation)
            except Exception:
                if previous is None:
                    self._subscriptions.pop(account_id, None)
                else:
                    self._subscriptions[account_id] = previous
                if mode == "poll_3s" and not any(sub.mode == "poll_3s" for sub in self._subscriptions.values()):
                    self.quote_service.remove_fetch_listener(self._on_poll_quotes)
                    if self._poll_leased:
                        self.quote_service.release_temporary_polling()
                        self._poll_leased = False
                if mode == "websocket":
                    try:
                        self._sync_websocket()
                    except Exception:  # noqa: BLE001
                        logger.exception("WebSocket 订阅回滚失败")
                if previous is None:
                    self.quote_service.remove_symbol_consumer(f"paper:{account_id}")
                else:
                    self.quote_service.set_symbol_consumer(
                        f"paper:{account_id}", previous.valuation_symbols or set(),
                    )
                raise

    def unregister(self, account_id: str) -> None:
        with self._lock:
            removed = self._subscriptions.pop(account_id, None)
            if removed is None:
                return
            self.quote_service.remove_symbol_consumer(f"paper:{account_id}")
            if removed.mode == "poll_3s" and not any(s.mode == "poll_3s" for s in self._subscriptions.values()):
                self.quote_service.remove_fetch_listener(self._on_poll_quotes)
                self.quote_service.release_temporary_polling()
                self._poll_leased = False
            if removed.mode == "websocket":
                self._sync_websocket()
            if removed.mode.startswith("bar_") and not any(s.mode.startswith("bar_") for s in self._subscriptions.values()):
                self._bar_stop.set()

    def update_symbols(
        self,
        account_id: str,
        symbols: set[str],
        valuation_symbols: set[str] | None = None,
        last_bar: str | None = None,
    ) -> None:
        with self._lock:
            subscription = self._subscriptions.get(account_id)
            if subscription is None:
                return
            cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
            if subscription.mode == "websocket" and len(cleaned | self._websocket_symbols(exclude=account_id)) > WS_SYMBOL_LIMIT:
                raise ValueError("运行时股票池扩容超过 WebSocket 100 只上限")
            subscription.symbols = cleaned
            if valuation_symbols is not None:
                subscription.valuation_symbols = {
                    str(symbol).strip().upper()
                    for symbol in valuation_symbols
                    if str(symbol).strip()
                }
                self.quote_service.set_symbol_consumer(
                    f"paper:{account_id}", subscription.valuation_symbols,
                )
            if last_bar is not None:
                subscription.last_bar = str(last_bar)
            if subscription.mode == "websocket":
                self._sync_websocket()

    def has_subscription(self, account_id: str) -> bool:
        with self._lock:
            return account_id in self._subscriptions

    def _on_poll_quotes(self) -> None:
        self._dispatch_quotes("poll_3s", self._cached_quote_records())

    def _cached_quote_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for asset_type in ("stock", "etf"):
            frame = self.quote_service.get_quotes_compat(asset_type)
            if frame is not None and not frame.is_empty():
                records.extend(frame.to_dicts())
        get_index_quotes = getattr(self.quote_service, "get_index_quotes", None)
        if get_index_quotes is not None:
            frame = get_index_quotes()
            if frame is not None and not frame.is_empty():
                records.extend(frame.to_dicts())
        return records

    def _dispatch_quotes(self, mode: str, records: list[dict[str, Any]]) -> None:
        normalized = [item for item in (_quote_record(row) for row in records) if item is not None]
        if not normalized:
            return
        self._last_quote_at = max(str(item["timestamp"]) for item in normalized)
        by_symbol = {str(item["symbol"]): item for item in normalized}
        with self._lock:
            targets = [sub for sub in self._subscriptions.values() if sub.mode == mode]
        for sub in targets:
            selected = [by_symbol[symbol] for symbol in sub.symbols if symbol in by_symbol]
            if selected:
                _put_latest(sub.input_queue, {"type": "quotes", "account_id": sub.account_id, "quotes": selected})

    def _ensure_bar_thread(self) -> None:
        if self._bar_thread and self._bar_thread.is_alive():
            return
        self._bar_stop.clear()
        self._bar_thread = threading.Thread(target=self._bar_loop, name="paper-bars", daemon=True)
        self._bar_thread.start()

    def _bar_loop(self) -> None:
        from app.free_strategy.process import _read_rows

        while not self._bar_stop.wait(1.0):
            now = cn_naive_now()
            with self._lock:
                targets = [sub for sub in self._subscriptions.values() if sub.mode.startswith("bar_")]
            scheduled = [sub for sub in targets if sub.execution_mode == "scheduled"]
            self._dispatch_scheduled_clocks(scheduled, now)
            targets = [sub for sub in targets if sub.execution_mode != "scheduled"]
            groups: dict[tuple[str, str], list[_Subscription]] = {}
            for sub in targets:
                groups.setdefault((sub.mode, sub.asset_type), []).append(sub)
            for (mode, asset_type), subscriptions in groups.items():
                cutoff = _closed_bar_cutoff(mode, now)
                parsed_last = []
                for sub in subscriptions:
                    if not sub.last_bar:
                        continue
                    try:
                        parsed_last.append(datetime.fromisoformat(sub.last_bar))
                    except ValueError:
                        continue
                if parsed_last and all(value >= cutoff for value in parsed_last) and len(parsed_last) == len(subscriptions):
                    continue
                attempt = self._bar_attempts.get((mode, asset_type))
                if attempt and attempt[0] == cutoff and time.monotonic() - attempt[1] < 5:
                    continue
                self._bar_attempts[(mode, asset_type)] = (cutoff, time.monotonic())
                symbols = sorted(set().union(*(sub.symbols for sub in subscriptions)))
                if not symbols:
                    continue
                timeframe = {"bar_1m": "1m", "bar_5m": "5m", "bar_30m": "30m"}.get(mode, "1d")
                after = min(parsed_last, default=datetime.combine(cutoff.date(), clock_time.min) - timedelta(microseconds=1))
                try:
                    rows = list(_read_rows(
                        self.repo,
                        symbols,
                        after.date(),
                        cutoff.date(),
                        asset_type,
                        timeframe,
                        require_all_symbols=False,
                        allow_empty=True,
                        after=after,
                        until=cutoff,
                    ))
                except ValueError:
                    continue
                except Exception:  # noqa: BLE001
                    logger.exception("模拟盘闭合 K 线读取失败")
                    continue
                if mode in {"bar_1m", "bar_5m", "bar_30m"}:
                    minutes = int(timeframe[:-1])
                    rows = [bar for bar in rows if bar.timestamp + timedelta(minutes=minutes - 1) <= cutoff]
                for sub in subscriptions:
                    fresh = [
                        bar for bar in rows
                        if bar.symbol in sub.symbols and bar.timestamp.isoformat() > sub.last_bar
                    ]
                    if not fresh:
                        continue
                    payload = {
                        "type": "bars",
                        "account_id": sub.account_id,
                        "bars": [bar.as_dict() for bar in fresh],
                    }
                    if _put_bar_batch(sub.input_queue, payload):
                        sub.last_bar = max(bar.timestamp.isoformat() for bar in fresh)

    def _dispatch_scheduled_clocks(
        self,
        subscriptions: list[_Subscription],
        now: datetime,
    ) -> None:
        for sub in subscriptions:
            cutoff = _closed_bar_cutoff(sub.mode, now)
            try:
                last_bar = datetime.fromisoformat(sub.last_bar) if sub.last_bar else None
            except ValueError:
                last_bar = None
            boundaries = [
                datetime.combine(cutoff.date(), clock_time.fromisoformat(value))
                for value in sub.scheduled_times
            ]
            if cutoff.time() >= clock_time(15, 0):
                boundaries.append(datetime.combine(cutoff.date(), clock_time(15, 0)))
            if not any(
                boundary <= cutoff and (last_bar is None or boundary > last_bar)
                for boundary in boundaries
            ):
                continue
            cutoff_value = cutoff.isoformat()
            monotonic_now = time.monotonic()
            if (
                sub.last_dispatch_cutoff == cutoff_value
                and monotonic_now - sub.last_dispatch_at < 5
            ):
                continue
            if _put_bar_batch(sub.input_queue, {
                "type": "scheduled_clock",
                "account_id": sub.account_id,
                "cutoff": cutoff_value,
            }):
                sub.last_dispatch_cutoff = cutoff_value
                sub.last_dispatch_at = monotonic_now

    def _websocket_symbols(self, *, exclude: str | None = None) -> set[str]:
        return set().union(*(
            sub.symbols for sub in self._subscriptions.values()
            if sub.mode == "websocket" and sub.account_id != exclude
        )) if any(sub.mode == "websocket" and sub.account_id != exclude for sub in self._subscriptions.values()) else set()

    def _sync_websocket(self) -> None:
        desired = self._websocket_symbols()
        if len(desired) > WS_SYMBOL_LIMIT:
            raise ValueError(f"WebSocket 去重订阅最多 {WS_SYMBOL_LIMIT} 只")
        if not desired:
            if self._stream is not None and self._ws_symbols:
                self._stream.unsubscribe("quotes", sorted(self._ws_symbols))
                self._stream.close()
                self._stream = None
            self._ws_symbols.clear()
            self._ws_state = "disconnected"
            return
        if self._stream is None:
            from app.tickflow.client import get_paid_realtime_client

            client = get_paid_realtime_client()
            if client is None:
                raise ValueError("未配置可用的 TickFlow Key")
            from tickflow.resources.stream import MarketStream

            self._stream = MarketStream(client._client)  # SDK 公共客户端只缓存一个不可重启的 stream 实例。
            self._stream.on_quotes(self._on_websocket_quotes)
            self._stream.on_error(self._on_websocket_error)
            self._stream.subscribe("quotes", sorted(desired))
            self._ws_symbols = set(desired)
            self._ws_state = "connecting"
            self._stream.connect(block=False)
            return
        added = desired - self._ws_symbols
        removed = self._ws_symbols - desired
        if added:
            self._stream.subscribe("quotes", sorted(added))
        if removed:
            self._stream.unsubscribe("quotes", sorted(removed))
        self._ws_symbols = set(desired)

    def _on_websocket_quotes(self, records: list[dict[str, Any]]) -> None:
        self.quote_service.record_quotes(records)
        recovering = self._ws_state in {"reconnecting", "error"}
        self._ws_state = "connected"
        self._ws_error = None
        if recovering:
            self._dispatch_recovery_snapshot()
        self._dispatch_quotes("websocket", records)

    def _on_websocket_error(self, message: str) -> None:
        self._ws_state = "reconnecting"
        self._ws_error = str(message)
        self._ws_disconnected_at = cn_naive_now()
        with self._lock:
            targets = [sub for sub in self._subscriptions.values() if sub.mode == "websocket"]
        for sub in targets:
            _put_latest(sub.input_queue, {"type": "gap", "reason": str(message), "account_id": sub.account_id})

    def _dispatch_recovery_snapshot(self) -> None:
        records: list[dict[str, Any]] = []
        try:
            from app.tickflow.client import get_paid_realtime_client

            client = get_paid_realtime_client()
            if client is not None and self._ws_symbols:
                records = list(client.quotes.get(symbols=sorted(self._ws_symbols)) or [])
        except Exception as exc:  # noqa: BLE001
            logger.warning("WebSocket 重连快照拉取失败，使用本地最新快照: %s", exc)
        if not records:
            records = self._cached_quote_records()
        by_symbol = {str(row.get("symbol")): row for row in records}
        from app.free_strategy.process import _read_rows

        with self._lock:
            targets = [sub for sub in self._subscriptions.values() if sub.mode == "websocket"]
        for sub in targets:
            bars: list[Bar] = []
            try:
                bars = _read_rows(self.repo, sorted(sub.symbols), cn_today(), cn_today(), sub.asset_type, "1m")
            except ValueError:
                pass
            if self._ws_disconnected_at is not None:
                bars = [bar for bar in bars if bar.timestamp >= self._ws_disconnected_at.replace(tzinfo=None)]
            snapshot = [item for symbol in sub.symbols if (item := _quote_record(by_symbol.get(symbol, {}))) is not None]
            _put_latest(sub.input_queue, {
                "type": "recovery",
                "account_id": sub.account_id,
                "bars": [bar.as_dict() for bar in bars],
                "quotes": snapshot,
            })
        self._ws_disconnected_at = None

    def status(self) -> dict[str, Any]:
        quote_status = self.quote_service.status()
        with self._lock:
            running = list(self._subscriptions.values())
        return {
            "running_accounts": len(running),
            "mode_counts": {mode: sum(sub.mode == mode for sub in running) for mode in sorted(MARKET_MODES | LEGACY_MARKET_MODES)},
            "poll_3s": {
                "active": any(sub.mode == "poll_3s" for sub in running),
                "available": self.quote_service.get_min_interval() <= 3,
                "min_interval_s": self.quote_service.get_min_interval(),
                "interval_s": quote_status.get("interval_s"),
                "actual_fetch_ms": quote_status.get("fetch_ms"),
            },
            "websocket": {
                "status": self._ws_state,
                "symbols": len(self._ws_symbols),
                "capacity": WS_SYMBOL_LIMIT,
                "last_error": self._ws_error,
            },
            "last_quote_at": self._last_quote_at,
            "quote_service": quote_status,
        }

    def close(self) -> None:
        with self._lock:
            account_ids = list(self._subscriptions)
        for account_id in account_ids:
            self.unregister(account_id)
        self._bar_stop.set()
        if self._bar_thread:
            self._bar_thread.join(timeout=3)
        if self._stream is not None:
            self._stream.close()
        self._ws_state = "disconnected"


def _engine_from_state(
    state: dict[str, Any],
    account_root: Path,
    data_dir: Path,
    *,
    preload_market_history: bool = False,
    callback_deadline: Any = None,
) -> FreeStrategyEngine:
    from app.free_strategy.process import (
        MarketData,
        _instrument_records,
        _load_dividend_ratio_ranked,
        _load_market_data,
        _load_financial_snapshot,
        _load_smallcap_index_value,
        _load_valuation_market_caps,
        _load_scheduled_history_batch,
        _prepare_market_reference,
        _preload_tradable_dates,
        _read_rows,
    )
    from app.tickflow.repository import DataStore, KlineRepository

    raw = dict(state.get("config", {}))
    mode = state_market_mode(state)
    raw.pop("market_mode", None)
    timeframe = {"bar_1m": "1m", "bar_5m": "5m", "bar_30m": "30m"}.get(mode, "1m" if mode in QUOTE_MODES else "1d")
    asset_type = str(raw.pop("asset_type", "stock"))
    allowed = {field.name for field in fields(FreeStrategyConfig)}
    config = FreeStrategyConfig(asset_type=asset_type, **{key: value for key, value in raw.items() if key in allowed})
    risk = RiskConfig(**state.get("risk_config", {}))
    repo = KlineRepository(DataStore(data_dir))
    source = (account_root / "strategy.py").read_text(encoding="utf-8")
    checkpoint = state.get("checkpoint")
    compatible_checkpoint = (
        _compatible_checkpoint(source, checkpoint)
        if isinstance(checkpoint, dict) else None
    )
    initial_state = (
        compatible_checkpoint.get("state", state.get("state", {}))
        if compatible_checkpoint is not None else state.get("state", {})
    )
    engine = FreeStrategyEngine(
        source,
        timeframe=timeframe,
        config=config,
        state=initial_state,
        instrument_loader=lambda execution_mode: _instrument_records(
            repo,
            asset_type,
            "1d" if execution_mode == "scheduled" else timeframe,
        ),
        risk_config=risk,
        callback_deadline=callback_deadline,
    )
    runtime_timestamp = (
        state.get("last_bar")
        or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp")
    )
    run_start = (
        datetime.fromisoformat(str(runtime_timestamp)).date()
        if runtime_timestamp else cn_today()
    )
    engine.set_run_window(run_start, cn_today())
    engine.set_financial_snapshot_loader(
        lambda symbols, cutoff: _load_financial_snapshot(
            data_dir,
            symbols,
            cutoff,
        )
    )
    engine.set_dividend_ratio_loader(
        lambda symbols, cutoff: _load_dividend_ratio_ranked(
            repo,
            data_dir,
            symbols,
            cutoff,
        )
    )
    engine.set_valuation_market_cap_loader(
        lambda symbols, cutoff: _load_valuation_market_caps(
            data_dir,
            symbols,
            cutoff,
        )
    )
    engine.set_smallcap_index_loader(
        lambda symbols, cutoff: _load_smallcap_index_value(repo, data_dir, symbols, cutoff)
    )
    if config.allow_stale_fills:
        _preload_tradable_dates(
            engine,
            _load_market_data(repo, engine.universe, run_start, cn_today(), asset_type),
        )
    if "unit_net_value" in engine.extra_history_requirements:
        from app.free_strategy.fund_nav import prepare_fund_nav_data

        prepare_fund_nav_data(repo, engine, run_start, cn_today())

    history_asset_by_symbol = {
        str(item["symbol"]): requested_asset
        for requested_asset, _period in engine.market_history_requirements
        for item in _instrument_records(repo, requested_asset, "1d")
    }
    market_loaded_through: date | None = None
    scheduled_history_market = MarketData()

    requested_history_bars = engine.history_requirements.get("1d", 0)
    if requested_history_bars and engine.universe:
        history_start = run_start - timedelta(days=requested_history_bars * 2 + 14)
        scheduled_history_market = _load_market_data(
            repo,
            list(engine.universe),
            history_start,
            cn_today(),
            asset_type,
        )

    def load_market_history(cutoff: datetime) -> None:
        nonlocal market_loaded_through
        target = cutoff.date()
        if market_loaded_through is not None and market_loaded_through >= target:
            return
        market = MarketData()
        references = {
            requested_asset: _prepare_market_reference(
                repo, engine, target, target, requested_asset, market,
            )
            for requested_asset, period in engine.market_history_requirements
            if period == "1d"
        }
        primary = references.get(asset_type, {"enabled": False})
        engine.market_history_metadata = (
            {**primary, "assets": references}
            if len(references) > 1 else primary
        )
        market_loaded_through = target

    def load_history(symbol: str, count: int, period: str, cutoff: datetime) -> list[Bar]:
        if not symbol or count <= 0:
            return []
        start = cutoff.date() - timedelta(days=max(30, count * 3))
        try:
            rows = _read_rows(
                repo,
                [symbol],
                start,
                cutoff.date(),
                history_asset_by_symbol.get(symbol, asset_type),
                period,
            )
        except ValueError:
            return []
        return [bar for bar in rows if bar.timestamp <= cutoff][-count:]

    engine.set_history_loader(load_history)
    engine.set_history_batch_loader(
        lambda symbols, count, period, cutoff: _load_scheduled_history_batch(
            repo,
            scheduled_history_market,
            asset_type,
            symbols,
            count,
            period,
            cutoff,
        )
    )
    engine.set_market_history_loader(load_market_history)
    if preload_market_history and engine.market_history_requirements:
        load_market_history(cn_naive_now())
    if compatible_checkpoint is not None:
        engine.restore_checkpoint(compatible_checkpoint)
    else:
        engine.account.restore(state.get("account", {}))
        engine.restore_runtime(state.get("runtime"))
    return engine


def inspect_account_runtime(state: dict[str, Any], account_root: Path, data_dir: Path) -> tuple[set[str], str]:
    engine = _engine_from_state(state, account_root, data_dir)
    symbols = set(engine.universe)
    if not symbols:
        symbols.update(str(symbol).strip().upper() for symbol in state.get("config", {}).get("symbols", []) if str(symbol).strip())
    symbols.update(symbol for symbol, quantity in engine.account.positions.items() if quantity > 0)
    symbols.add(engine.config.benchmark_symbol)
    return symbols, engine.execution_mode


def _equity_snapshot(
    engine: FreeStrategyEngine,
    state: dict[str, Any],
    timestamp: datetime,
) -> dict[str, Any]:
    prices = dict(engine._current_close_prices)  # noqa: SLF001
    equity = engine.account.equity(prices)
    initial_capital = float(engine.config.initial_capital)
    peak = max(float(state.get("equity_peak", initial_capital)), equity)
    state["equity_peak"] = peak
    drawdown_pct = ((peak - equity) / peak * 100) if peak else 0.0
    state["max_drawdown_pct"] = max(
        float(state.get("max_drawdown_pct", 0.0)),
        drawdown_pct,
    )
    return {
        "timestamp": timestamp.isoformat(),
        "equity": equity,
        "cash": engine.account.cash,
        "nav": equity / initial_capital if initial_capital else 1.0,
        "drawdown_pct": drawdown_pct,
        "positions": {
            symbol: quantity
            for symbol, quantity in engine.account.positions.items()
            if quantity > 0
        },
        "avg_cost": {
            symbol: engine.account.avg_cost[symbol]
            for symbol, quantity in engine.account.positions.items()
            if quantity > 0 and symbol in engine.account.avg_cost
        },
        "source": "paper",
    }


def _closed_bar_cutoff(mode: str, now: datetime) -> datetime:
    if mode == "bar_1d":
        if now.time() >= clock_time(15, 1):
            return datetime.combine(now.date(), clock_time(15, 0))
        return datetime.combine(now.date(), clock_time.min) - timedelta(microseconds=1)
    if now.time() < clock_time(9, 31):
        return datetime.combine(now.date(), clock_time.min) - timedelta(microseconds=1)
    if now.time() <= clock_time(11, 31):
        return now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    if now.time() < clock_time(13, 1):
        return datetime.combine(now.date(), clock_time(11, 30))
    if now.time() <= clock_time(15, 1):
        return now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    return datetime.combine(now.date(), clock_time(15, 0))


def _fill_event_id(fill: Any) -> str:
    raw = ":".join(str(value) for value in (
        getattr(fill, "order_id", ""),
        getattr(fill, "timestamp", ""),
        getattr(fill, "symbol", ""),
        getattr(fill, "quantity", ""),
        getattr(fill, "price", ""),
    ))
    return f"fill:{sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _log_event_id(item: dict[str, Any]) -> str:
    raw = ":".join(str(item.get(key) or "") for key in ("timestamp", "level", "message"))
    return f"log:{sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _persist_engine_state(
    store: PaperAccountStore,
    account_id: str,
    current: dict[str, Any],
    engine: FreeStrategyEngine,
    snapshots: list[dict[str, Any]],
) -> dict[str, Any]:
    stored_curve_drawdown = store.max_drawdown_pct(account_id)
    if snapshots:
        store.upsert_equity_curve(account_id, snapshots)
    engine.account.equity_curve.clear()
    checkpoint = compact_paper_checkpoint(engine.checkpoint())
    prices = dict(engine._current_close_prices)  # noqa: SLF001
    equity = engine.account.equity(prices)
    peak = max(float(current.get("equity_peak", engine.config.initial_capital)), equity)
    drawdown_pct = ((peak - equity) / peak * 100) if peak else 0.0
    maximum_drawdown = max(
        float(current.get("max_drawdown_pct", 0.0)),
        stored_curve_drawdown,
        drawdown_pct,
        *(float(row.get("drawdown_pct", 0.0)) for row in snapshots),
    )
    runtime_fields = {
        "checkpoint": checkpoint,
        "universe": engine.universe,
        "risk_status": engine.risk_status,
        "cash": engine.account.cash,
        "equity": equity,
        "return_pct": (equity / engine.config.initial_capital - 1) * 100,
        "drawdown_pct": drawdown_pct,
        "max_drawdown_pct": maximum_drawdown,
        "positions": {
            symbol: quantity
            for symbol, quantity in engine.account.positions.items()
            if quantity > 0
        },
        "equity_peak": peak,
        "last_error": None,
    }
    for key in ("last_bar", "last_quote"):
        if key in current:
            runtime_fields[key] = current[key]

    def persist(latest: dict[str, Any]) -> dict[str, Any]:
        for key in ("account", "state", "runtime"):
            latest.pop(key, None)
        latest.update(runtime_fields)
        return latest

    return store.update(account_id, persist)


def _append_engine_events(
    store: PaperAccountStore,
    account_id: str,
    engine: FreeStrategyEngine,
    *,
    before_orders: int,
    before_fills: int,
    before_logs: int,
    before_risk: dict[str, Any],
    strategy_id: str | None = None,
    notify: Any = None,
) -> None:
    fills_by_order = {
        fill.order_id: fill
        for fill in engine.account.fills
    }
    for order in engine.account.orders[before_orders:]:
        event_type = "rejected" if order.status == "rejected" else "order"
        fill = fills_by_order.get(order.id)
        event = {
            "type": event_type,
            "timestamp": order.submitted_at,
            **asdict(order),
            "executed_side": fill.side if fill is not None else None,
        }
        if store.append_event_once(account_id, event) and notify is not None:
            notify(event)
    for fill in engine.account.fills[before_fills:]:
        event = {"id": _fill_event_id(fill), "type": "fill", **asdict(fill)}
        if store.append_event_once(account_id, event) and notify is not None:
            notify(event)
    for item in engine.logs[before_logs:]:
        store.append_event_once(account_id, {"id": _log_event_id(item), "type": "log", **item})
    for signal in engine.drain_signals():
        payload = dict(signal.pop("payload", {}))
        signal_id = str(signal.pop("id"))
        if strategy_id == "five_fortunes_v2" and signal_id.startswith("five_fortunes:"):
            signal_id = f"five_fortunes_v2:{signal_id.removeprefix('five_fortunes:')}"
            payload["strategy"] = "five_fortunes_v2"
        store.append_event_once(account_id, {
            "id": f"signal:{signal_id}",
            "type": "signal",
            **signal,
            **payload,
        })
    if engine.risk_status != before_risk and engine.risk_status.get("reason"):
        risk_id = sha256(
            f"{engine.risk_status.get('triggered_at')}:{engine.risk_status.get('reason')}".encode("utf-8")
        ).hexdigest()[:24]
        event = {"id": f"risk:{risk_id}", "type": "risk", **engine.risk_status}
        if store.append_event_once(account_id, event) and notify is not None:
            notify(event)


def _append_five_fortunes_decision(
    store: PaperAccountStore,
    account_id: str,
    engine: FreeStrategyEngine,
    timestamp: datetime,
) -> None:
    if timestamp.strftime("%H:%M") != "13:10":
        return
    state = engine.context.state.get("five_fortunes")
    if not isinstance(state, dict):
        return
    decision = dict(state.get("decision", {}))
    trading_date = str(decision.get("date") or timestamp.date().isoformat())
    target = list(decision.get("target") or state.get("target") or [])
    held_value = decision.get("held")
    holdings = [str(held_value)] if held_value else [
        symbol
        for symbol, quantity in engine.account.positions.items()
        if float(quantity) > 0
    ]
    decision_type = "empty" if not target and not holdings else "hold" if target == holdings else "rebalance"
    from app.free_strategy.five_fortunes import decision_reason_payload

    store.append_event_once(account_id, {
        "id": f"signal:five_fortunes:{trading_date}:decision",
        "type": "signal",
        "timestamp": timestamp.isoformat(),
        "signal_type": "daily_decision",
        "strategy": "five_fortunes",
        "trading_date": trading_date,
        "decision": decision_type,
        "regime": state.get("regime"),
        "raw_regime": state.get("raw_regime"),
        "target_symbols": target,
        "holding_symbols": holdings,
        "candidates": [
            {"symbol": row.get("symbol"), "score": row.get("score")}
            for row in list(state.get("candidate_rows", []))[:10]
            if row.get("symbol")
        ],
        **decision_reason_payload(decision),
    })


def _append_five_fortunes_v2_decision(
    store: PaperAccountStore,
    account_id: str,
    engine: FreeStrategyEngine,
    timestamp: datetime,
) -> None:
    if timestamp.strftime("%H:%M") != "13:10":
        return
    state = engine.context.state.get("five_fortunes_v2")
    if not isinstance(state, dict):
        return
    decision = dict(state.get("decision", {}))
    trading_date = str(decision.get("date") or timestamp.date().isoformat())
    target = list(decision.get("target") or state.get("target") or [])
    held_value = decision.get("held")
    holdings = [str(held_value)] if held_value else [
        symbol
        for symbol, quantity in engine.account.positions.items()
        if float(quantity) > 0
    ]
    decision_type = "empty" if not target and not holdings else "hold" if target == holdings else "rebalance"
    from app.free_strategy.five_fortunes_v2 import decision_reason_payload

    store.append_event_once(account_id, {
        "id": f"signal:five_fortunes_v2:{trading_date}:decision",
        "type": "signal",
        "timestamp": timestamp.isoformat(),
        "signal_type": "daily_decision",
        "strategy": "five_fortunes_v2",
        "trading_date": trading_date,
        "decision": decision_type,
        "regime": state.get("regime"),
        "raw_regime": state.get("raw_regime"),
        "target_symbols": target,
        "holding_symbols": holdings,
        "candidates": [
            {"symbol": row.get("symbol"), "score": row.get("score")}
            for row in list(state.get("candidate_rows", []))[:10]
            if row.get("symbol")
        ],
        **decision_reason_payload(decision),
    })


def _append_five_fortunes_decisions(
    store: PaperAccountStore,
    account_id: str,
    engine: FreeStrategyEngine,
    timestamp: datetime,
) -> None:
    _append_five_fortunes_decision(store, account_id, engine, timestamp)
    _append_five_fortunes_v2_decision(store, account_id, engine, timestamp)


def _process_bar_rows(
    store: PaperAccountStore,
    account_id: str,
    current: dict[str, Any],
    engine: FreeStrategyEngine,
    bars: list[Bar],
    *,
    notify: Any = None,
) -> dict[str, Any]:
    bars = list(bars)
    if engine.config.allow_stale_fills:
        engine.preload_tradable_dates(
            (bar.symbol, bar.timestamp.date())
            for bar in bars
            if bar.tradable and not bar.suspended and bar.open > 0 and bar.high > 0
        )
    before_orders = len(engine.account.orders)
    before_fills = len(engine.account.fills)
    before_logs = len(engine.logs)
    before_risk = engine.risk_status
    snapshots: list[dict[str, Any]] = []
    for timestamp, rows in groupby(bars, key=lambda bar: bar.timestamp):
        engine.run(list(rows), finalize_session=False, return_result=False)
        _append_five_fortunes_decisions(store, account_id, engine, timestamp)
        snapshots.append(_equity_snapshot(engine, current, timestamp))
    last_timestamp = max((bar.timestamp for bar in bars), default=None)
    if last_timestamp is not None and last_timestamp.time() >= clock_time(15, 0):
        engine.finish_session(persist_state=False)
    _append_engine_events(
        store,
        account_id,
        engine,
        before_orders=before_orders,
        before_fills=before_fills,
        before_logs=before_logs,
        before_risk=before_risk,
        strategy_id=str(current.get("strategy_id") or ""),
        notify=notify,
    )
    if last_timestamp is not None:
        current["last_bar"] = last_timestamp.isoformat()
    return _persist_engine_state(store, account_id, current, engine, snapshots)


def _scheduled_trading_dates(
    repo: Any,
    engine: FreeStrategyEngine,
    market: Any,
    start: date,
    cutoff: datetime,
    asset_type: str,
) -> list[date]:
    from app.free_strategy.process import _ensure_scheduled_market_data

    probes = list(dict.fromkeys([
        *(
            symbol
            for symbol, quantity in engine.account.positions.items()
            if float(quantity) > 0
        ),
        *engine.universe[:20],
    ]))
    if not probes:
        return []
    _ensure_scheduled_market_data(
        repo,
        market,
        probes,
        start,
        cutoff.date(),
        asset_type,
    )
    result = {
        day
        for symbol, day in market.daily
        if symbol in probes and start <= day <= cutoff.date()
    }
    if (
        cutoff.date() not in result
        and cutoff.date().weekday() < 5
        and cutoff.time() >= clock_time(9, 30)
    ):
        get_snapshot = getattr(repo, "get_minute_snapshot", None)
        if callable(get_snapshot):
            frame = get_snapshot(probes, cutoff, asset_type)
            if frame is not None and not frame.is_empty():
                result.add(cutoff.date())
    return sorted(result)


def _process_scheduled_day(
    store: PaperAccountStore,
    account_id: str,
    current: dict[str, Any],
    engine: FreeStrategyEngine,
    repo: Any,
    market: Any,
    day: date,
    cutoff: datetime,
    asset_type: str,
    timeframe: str,
    *,
    finalize: bool,
    allow_opening_data_retry: bool = False,
    notify: Any = None,
) -> dict[str, Any]:
    from app.free_strategy.process import advance_scheduled_session

    before_orders = len(engine.account.orders)
    before_fills = len(engine.account.fills)
    before_logs = len(engine.logs)
    before_risk = engine.risk_status
    advance_scheduled_session(
        repo,
        engine,
        market,
        day,
        cutoff,
        asset_type,
        timeframe,
        finalize=finalize,
        allow_opening_data_retry=allow_opening_data_retry,
    )
    timestamp = engine._last_timestamp  # noqa: SLF001
    if timestamp is None:
        return current
    _append_five_fortunes_decisions(store, account_id, engine, timestamp)
    _append_engine_events(
        store,
        account_id,
        engine,
        before_orders=before_orders,
        before_fills=before_fills,
        before_logs=before_logs,
        before_risk=before_risk,
        strategy_id=str(current.get("strategy_id") or ""),
        notify=notify,
    )
    current["last_bar"] = timestamp.isoformat()
    return _persist_engine_state(
        store,
        account_id,
        current,
        engine,
        [_equity_snapshot(engine, current, timestamp)],
    )


def _catch_up_scheduled(
    store: PaperAccountStore,
    account_id: str,
    current: dict[str, Any],
    engine: FreeStrategyEngine,
    repo: Any,
    market: Any,
    cutoff: datetime,
    asset_type: str,
    timeframe: str,
) -> dict[str, Any]:
    from app.free_strategy.process import ScheduledOpeningDataPending

    last_value = (
        current.get("last_bar")
        or current.get("checkpoint", {}).get("runtime", {}).get("last_timestamp")
    )
    last_timestamp = datetime.fromisoformat(str(last_value)) if last_value else None
    start_day = last_timestamp.date() if last_timestamp else cutoff.date()
    runtime = engine.runtime_snapshot()
    if (
        runtime.get("session_finished")
        and runtime.get("session_date") == start_day.isoformat()
    ):
        start_day += timedelta(days=1)
    trading_dates = (
        _scheduled_trading_dates(repo, engine, market, start_day, cutoff, asset_type)
        if start_day <= cutoff.date() else []
    )
    existing_sync = current.get("sync")
    if not trading_dates and isinstance(existing_sync, dict) and existing_sync.get("phase") == "live":
        return current

    target = None
    if trading_dates:
        last_day = trading_dates[-1]
        target = (
            datetime.combine(last_day, clock_time(15, 0))
            if last_day < cutoff.date() else cutoff
        )
    sync = {
        "phase": "catching_up",
        "from": last_timestamp.isoformat() if last_timestamp else None,
        "target": target.isoformat() if target else None,
        "through": last_timestamp.isoformat() if last_timestamp else None,
        "processed_days": 0,
        "total_days": len(trading_dates),
        "missing_symbols": [],
        "updated_at": now_iso(),
    }
    current["sync"] = sync
    current = store.update_fields(account_id, {"sync": sync})
    if trading_dates:
        store.append_event(account_id, {
            "type": "sync",
            "phase": "catching_up",
            "from": sync["from"],
            "target": sync["target"],
        })
    for index, trading_day in enumerate(trading_dates, start=1):
        latest = store.get(account_id)
        if latest.get("status") != "running":
            return latest
        day_cutoff = (
            datetime.combine(trading_day, clock_time(15, 0))
            if trading_day < cutoff.date() else cutoff
        )
        try:
            current = _process_scheduled_day(
                store,
                account_id,
                current,
                engine,
                repo,
                market,
                trading_day,
                day_cutoff,
                asset_type,
                timeframe,
                finalize=day_cutoff.time() >= clock_time(15, 0),
                allow_opening_data_retry=trading_day == cutoff.date(),
            )
        except ScheduledOpeningDataPending:
            sync.update({"phase": "live", "updated_at": now_iso()})
            current["sync"] = dict(sync)
            return store.update_fields(account_id, {
                "last_error": None,
                "sync": dict(sync),
            })
        sync.update({
            "through": current.get("last_bar"),
            "processed_days": index,
            "updated_at": now_iso(),
        })
        current["sync"] = dict(sync)
        current = store.update_fields(account_id, {"sync": dict(sync)})

    sync.update({"phase": "live", "updated_at": now_iso()})
    current["sync"] = sync
    current = store.update_fields(account_id, {"sync": sync})
    store.append_event(account_id, {
        "type": "sync",
        "phase": "live",
        "through": sync["through"],
        "target": sync["target"],
    })
    return current


def _catch_up_bars(
    store: PaperAccountStore,
    account_id: str,
    current: dict[str, Any],
    engine: FreeStrategyEngine,
    data_dir: Path,
    *,
    repo: Any = None,
    scheduled_market: Any = None,
) -> dict[str, Any]:
    mode = state_market_mode(current)
    if not mode.startswith("bar_"):
        current["sync"] = {
            "phase": "live",
            "from": None,
            "target": current.get("last_quote"),
            "through": current.get("last_quote"),
            "processed_days": 0,
            "total_days": 0,
            "missing_symbols": [],
            "updated_at": now_iso(),
        }
        return store.update_fields(account_id, {"sync": current["sync"]})

    from app.free_strategy.process import MarketData, _read_rows
    from app.tickflow.repository import DataStore, KlineRepository

    repo = repo or KlineRepository(DataStore(data_dir))
    held_symbols = {
        symbol
        for symbol, quantity in engine.account.positions.items()
        if float(quantity) > 0
    }
    symbols = sorted(set(engine.universe) | held_symbols | {str(engine.config.benchmark_symbol)})
    last_value = (
        current.get("last_bar")
        or current.get("checkpoint", {}).get("runtime", {}).get("last_timestamp")
    )
    last_timestamp = datetime.fromisoformat(str(last_value)) if last_value else None
    cutoff = _closed_bar_cutoff(mode, cn_naive_now())
    start_day = last_timestamp.date() if last_timestamp else cn_today()
    timeframe = {"bar_1m": "1m", "bar_5m": "5m", "bar_30m": "30m"}.get(mode, "1d")
    asset_type = str(current.get("config", {}).get("asset_type", "stock"))
    if engine.execution_mode == "scheduled":
        return _catch_up_scheduled(
            store,
            account_id,
            current,
            engine,
            repo,
            scheduled_market or MarketData(),
            cutoff,
            asset_type,
            timeframe,
        )
    rows = list(_read_rows(
        repo,
        symbols,
        start_day,
        cutoff.date(),
        asset_type,
        timeframe,
        require_all_symbols=False,
        allow_empty=True,
        after=last_timestamp,
        until=cutoff,
    ))
    rows = [bar for bar in rows if (last_timestamp is None or bar.timestamp > last_timestamp) and bar.timestamp <= cutoff]
    existing_sync = current.get("sync")
    if not rows and isinstance(existing_sync, dict) and existing_sync.get("phase") == "live":
        return current
    days = [day for day, _ in groupby(rows, key=lambda bar: bar.timestamp.date())]
    target = max((bar.timestamp for bar in rows), default=last_timestamp)
    sync = {
        "phase": "catching_up",
        "from": last_timestamp.isoformat() if last_timestamp else None,
        "target": target.isoformat() if target else None,
        "through": last_timestamp.isoformat() if last_timestamp else None,
        "processed_days": 0,
        "total_days": len(days),
        "missing_symbols": [],
        "updated_at": now_iso(),
    }
    current["sync"] = sync
    current = store.update_fields(account_id, {"sync": sync})
    store.append_event(account_id, {
        "type": "sync",
        "phase": "catching_up",
        "from": sync["from"],
        "target": sync["target"],
    })
    if not rows:
        sync.update({"phase": "live", "updated_at": now_iso()})
        current["sync"] = sync
        saved = store.update_fields(account_id, {"sync": sync})
        store.append_event(account_id, {"type": "sync", "phase": "live", "through": sync["through"]})
        return saved

    processed_days = 0
    for trade_day, day_rows_iter in groupby(rows, key=lambda bar: bar.timestamp.date()):
        latest = store.get(account_id)
        if latest.get("status") != "running":
            return latest
        day_rows = list(day_rows_iter)
        found = {bar.symbol for bar in day_rows}
        missing = sorted(set(symbols) - found)
        held = {
            symbol
            for symbol, quantity in engine.account.positions.items()
            if float(quantity) > 0
        }
        benchmark = str(engine.config.benchmark_symbol)
        critical = sorted((held | {benchmark}) - found)
        if critical:
            raise ValueError(f"{trade_day.isoformat()} 分钟K缺少持仓或基准标的: {', '.join(critical)}")
        if missing:
            store.append_event_once(account_id, {
                "id": f"market-gap:{trade_day.isoformat()}",
                "type": "market_gap",
                "trading_date": trade_day.isoformat(),
                "missing_symbols": missing,
                "reason": f"当日 {len(missing)} 只非关键标的无分钟K，按不可交易处理",
            })
        current = _process_bar_rows(store, account_id, current, engine, day_rows)
        processed_days += 1
        sync.update({
            "through": current.get("last_bar"),
            "processed_days": processed_days,
            "missing_symbols": missing,
            "updated_at": now_iso(),
        })
        current["sync"] = dict(sync)
        current = store.update_fields(account_id, {"sync": dict(sync)})

    sync.update({"phase": "live", "updated_at": now_iso()})
    current["sync"] = sync
    current = store.update_fields(account_id, {"sync": sync})
    store.append_event(account_id, {
        "type": "sync",
        "phase": "live",
        "through": sync["through"],
        "target": sync["target"],
    })
    return current


def _paper_worker(account_id: str, root: str, input_queue: Any, callback_deadline: Any = None) -> None:
    from app.free_strategy.process import MarketData, ScheduledOpeningDataPending
    from app.tickflow.repository import DataStore, KlineRepository

    account_root = Path(root) / account_id
    store = PaperAccountStore(Path(root).parent)
    state = store.get(account_id)
    repo = KlineRepository(DataStore(Path(root).parent))
    scheduled_market = MarketData()
    try:
        engine = _engine_from_state(
            state,
            account_root,
            Path(root).parent,
            preload_market_history=True,
            callback_deadline=callback_deadline,
        )
        mode = state_market_mode(state)
        if mode in QUOTE_MODES and engine.execution_mode == "full_bar":
            raise ValueError("3秒行情和 WebSocket 策略必须定义 on_quote(context, quotes) 或定时任务")
        if mode.startswith("bar_") and engine.execution_mode == "quote":
            raise ValueError("K线模式策略必须定义 on_bar(context, bars) 或定时任务")
    except Exception as exc:  # noqa: BLE001
        message = f"模拟账户初始化失败: {exc}"
        store.update_fields(account_id, {"status": "paused", "last_error": message})
        store.append_event(account_id, {"type": "error", "message": message})
        return
    state = store.update_fields(account_id, {
        "execution_mode": engine.execution_mode,
        "scheduled_times": engine.scheduled_times,
        "universe": engine.universe,
    })
    try:
        state = _catch_up_bars(
            store,
            account_id,
            state,
            engine,
            Path(root).parent,
            repo=repo,
            scheduled_market=scheduled_market,
        )
    except Exception as exc:  # noqa: BLE001
        latest = store.get(account_id)
        sync = dict(latest.get("sync", {}))
        sync.update({"phase": "error", "error": str(exc), "updated_at": now_iso()})
        store.update_fields(account_id, {
            "status": "paused",
            "last_error": str(exc),
            "sync": sync,
        })
        store.append_event(account_id, {"type": "error", "message": str(exc)})
        return
    notified: set[str] = set()
    notification_times: deque[float] = deque()

    def notify(event: dict[str, Any]) -> None:
        if event.get("type") not in {"order", "fill", "rejected", "risk"}:
            return
        key = ":".join(str(event.get(name) or "") for name in ("type", "id", "order_id", "symbol", "status", "reason"))
        if key in notified:
            return
        now = time.monotonic()
        while notification_times and notification_times[0] <= now - 60:
            notification_times.popleft()
        if len(notification_times) >= 20:
            return
        notified.add(key)
        notification_times.append(now)
        latest_state = store.get(account_id)
        channels = set(latest_state.get("notification_channels", []))
        if not channels:
            return
        from app.services import preferences, webhook_adapter

        symbol = str(event.get("symbol") or "")
        detail = str(event.get("reason") or event.get("message") or event.get("status") or "")
        body = f"{latest_state.get('name', account_id)} {symbol} {detail}".strip()
        if "feishu" in channels and preferences.get_feishu_webhook_url():
            _PAPER_WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_feishu,
                preferences.get_feishu_webhook_url(),
                "模拟",
                body,
                preferences.get_feishu_webhook_secret(),
            )
        if "wecom" in channels and preferences.get_wecom_webhook_url():
            _PAPER_WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_wecom,
                preferences.get_wecom_webhook_url(),
                "模拟",
                body,
            )
    while True:
        try:
            message = input_queue.get(timeout=2)
        except queue.Empty:
            continue
        if message.get("type") == "stop":
            return
        current = store.get(account_id)
        if current.get("status") != "running":
            continue
        if message.get("type") == "unlock_risk":
            engine.unlock_drawdown_risk()
            continue
        elif message.get("type") == "recovery":
            bars = rows_to_bars(message.get("bars", []))
            quotes = _quotes_from_records(message.get("quotes", []))
            engine.preload_history(bars, "1m")
            engine.preload_quote_snapshot(quotes)
            store.append_event(account_id, {
                "type": "market_recovered",
                "bars": len(bars),
                "snapshot_quotes": len(quotes),
            })
            continue
        elif message.get("type") == "gap":
            store.append_event(account_id, {"type": "market_gap", "reason": message.get("reason")})
            continue
        try:
            if message.get("type") == "scheduled_clock":
                if engine.execution_mode != "scheduled":
                    continue
                cutoff = datetime.fromisoformat(str(message["cutoff"]))
                asset_type = str(current.get("config", {}).get("asset_type", "stock"))
                trading_dates = _scheduled_trading_dates(
                    repo,
                    engine,
                    scheduled_market,
                    cutoff.date(),
                    cutoff,
                    asset_type,
                )
                if not trading_dates:
                    continue
                current = _process_scheduled_day(
                    store,
                    account_id,
                    current,
                    engine,
                    repo,
                    scheduled_market,
                    cutoff.date(),
                    cutoff,
                    asset_type,
                    {
                        "bar_1m": "1m",
                        "bar_5m": "5m",
                        "bar_30m": "30m",
                    }.get(state_market_mode(current), "1d"),
                    finalize=cutoff.time() >= clock_time(15, 0),
                    allow_opening_data_retry=True,
                    notify=notify,
                )
                sync = dict(current.get("sync", {}))
                sync.update({
                    "phase": "live",
                    "through": current.get("last_bar"),
                    "target": current.get("last_bar"),
                    "updated_at": now_iso(),
                })
                current = store.update_fields(account_id, {"sync": sync})
            elif message.get("type") == "quotes":
                before_orders = len(engine.account.orders)
                before_fills = len(engine.account.fills)
                before_logs = len(engine.logs)
                before_risk = engine.risk_status
                quotes = _quotes_from_records(message.get("quotes", []))
                if engine._last_timestamp is not None:  # noqa: SLF001
                    quotes = [quote for quote in quotes if quote.timestamp > engine._last_timestamp]  # noqa: SLF001
                if not quotes:
                    continue
                engine.process_quotes(quotes)
                current["last_quote"] = max((quote.timestamp.isoformat() for quote in quotes), default=current.get("last_quote"))
                _append_engine_events(
                    store,
                    account_id,
                    engine,
                    before_orders=before_orders,
                    before_fills=before_fills,
                    before_logs=before_logs,
                    before_risk=before_risk,
                    strategy_id=str(current.get("strategy_id") or ""),
                    notify=notify,
                )
                timestamp = engine._last_timestamp or cn_naive_now()  # noqa: SLF001
                current = _persist_engine_state(
                    store,
                    account_id,
                    current,
                    engine,
                    [_equity_snapshot(engine, current, timestamp)],
                )
            elif message.get("type") == "bars":
                bars = rows_to_bars(message.get("bars", []))
                if engine._last_timestamp is not None:  # noqa: SLF001
                    bars = [bar for bar in bars if bar.timestamp > engine._last_timestamp]  # noqa: SLF001
                if not bars:
                    continue
                current = _process_bar_rows(store, account_id, current, engine, bars, notify=notify)
                sync = dict(current.get("sync", {}))
                sync.update({
                    "phase": "live",
                    "through": current.get("last_bar"),
                    "target": current.get("last_bar"),
                    "updated_at": now_iso(),
                })
                current = store.update_fields(account_id, {"sync": sync})
            else:
                continue
        except ScheduledOpeningDataPending:
            # The first opening minute can arrive after the clock boundary. Keep the
            # account subscribed and let the next closed-minute clock retry it.
            sync = dict(current.get("sync", {}))
            sync.update({"phase": "live", "updated_at": now_iso()})
            current = store.update_fields(account_id, {
                "last_error": None,
                "sync": sync,
            })
            continue
        except Exception as exc:  # noqa: BLE001
            sync = dict(current.get("sync", {}))
            sync.update({"phase": "error", "error": str(exc), "updated_at": now_iso()})
            store.update_fields(account_id, {
                "status": "paused",
                "last_error": str(exc),
                "sync": sync,
            })
            store.append_event(account_id, {"type": "error", "message": str(exc)})
            continue


class PaperTradingSupervisor:
    def __init__(self, data_dir: Path, quote_service: Any, repo: Any) -> None:
        self.data_dir = Path(data_dir)
        self.store = PaperAccountStore(self.data_dir)
        self.hub = MarketDataHub(quote_service, repo)
        self._ctx = mp.get_context("spawn")
        self._processes: dict[str, mp.Process] = {}
        self._queues: dict[str, Any] = {}
        self._deadlines: dict[str, Any] = {}
        self._restart_attempts: dict[str, deque[float]] = {}
        self._lock = threading.RLock()
        self._monitor_stop = threading.Event()
        self._monitor_thread = threading.Thread(target=self._monitor_accounts, name="paper-supervisor", daemon=True)
        self._monitor_thread.start()

    def _monitor_accounts(self) -> None:
        while not self._monitor_stop.wait(1.0):
            self._monitor_once()

    def _monitor_once(self) -> None:
        with self._lock:
            account_ids = list(self._processes)
        for account_id in account_ids:
            with self._lock:
                try:
                    state = self.store.get(account_id)
                except FileNotFoundError:
                    self._detach_runtime(account_id)
                    continue
                process = self._processes.get(account_id)
                if state.get("status") != "running":
                    self._detach_runtime(account_id, expected_process=process)
                    continue
                if process is None or not process.is_alive():
                    if not self._detach_runtime(account_id, expected_process=process):
                        continue
                    exit_code = getattr(process, "exitcode", None) if process is not None else None
                    now = time.monotonic()
                    restart_attempts = getattr(self, "_restart_attempts", None)
                    if restart_attempts is None:
                        restart_attempts = self._restart_attempts = {}
                    attempts = restart_attempts.setdefault(account_id, deque())
                    while attempts and attempts[0] <= now - _WORKER_RESTART_WINDOW_SECONDS:
                        attempts.popleft()
                    if len(attempts) < _WORKER_RESTART_LIMIT:
                        attempts.append(now)
                        self.store.append_event(account_id, {
                            "type": "worker_restart",
                            "attempt": len(attempts),
                            "exit_code": exit_code,
                        })
                        try:
                            self.start(account_id, reset_restart_attempts=False)
                        except Exception as exc:  # noqa: BLE001
                            self._pause_with_error(
                                account_id,
                                state,
                                f"策略子进程自动恢复失败: {exc}",
                            )
                        continue
                    detail = f"，退出码 {exit_code}" if exit_code is not None else ""
                    self._pause_with_error(
                        account_id,
                        state,
                        f"策略子进程在 5 分钟内连续异常退出{detail}，已暂停",
                    )
                    continue
                deadline = getattr(self, "_deadlines", {}).get(account_id)
                if deadline is not None:
                    with deadline.get_lock() if hasattr(deadline, "get_lock") else nullcontext():
                        deadline_value = float(deadline.value)
                    if deadline_value > 0 and time.monotonic() >= deadline_value:
                        timeout = float(state.get("config", {}).get("callback_timeout_seconds", 30.0))
                        message = f"策略执行超过 {timeout:g} 秒，已终止子进程"
                        if not self._detach_runtime(account_id, expected_process=process):
                            continue
                        self._pause_with_error(account_id, state, message)
                        continue
                input_queue = self._queues.get(account_id)
            if input_queue is None:
                continue
            symbols = set(state.get("universe", []))
            if not symbols:
                symbols.update(state.get("config", {}).get("symbols", []))
            positions = state.get("positions", {}) or state.get("checkpoint", {}).get("account", {}).get("positions", {})
            held_symbols = {symbol for symbol, quantity in positions.items() if float(quantity) > 0}
            symbols.update(held_symbols)
            symbols.add(str(state.get("config", {}).get("benchmark_symbol", "510300.SH")))
            try:
                sync_phase = str(state.get("sync", {}).get("phase") or "live")
                if sync_phase == "live" and not self.hub.has_subscription(account_id):
                    self.hub.register(
                        account_id,
                        state_market_mode(state),
                        symbols,
                        str(state.get("config", {}).get("asset_type", "stock")),
                        input_queue,
                        str(state.get("last_bar") or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp") or ""),
                        held_symbols,
                        execution_mode=str(state.get("execution_mode") or "full_bar"),
                        scheduled_times=tuple(state.get("scheduled_times") or ()),
                    )
                elif sync_phase != "live" and self.hub.has_subscription(account_id):
                    self.hub.unregister(account_id)
                self.hub.update_symbols(
                    account_id,
                    symbols,
                    held_symbols,
                    str(state.get("last_bar") or ""),
                )
            except ValueError as exc:
                self.pause_or_stop(account_id, "paused")
                self.store.update_fields(account_id, {"last_error": str(exc)})
                self.store.append_event(account_id, {"type": "error", "message": str(exc)})

    def _pause_with_error(
        self,
        account_id: str,
        state: dict[str, Any],
        message: str,
    ) -> None:
        sync = dict(state.get("sync", {}))
        sync.update({"phase": "error", "error": message, "updated_at": now_iso()})
        self.store.update_fields(account_id, {
            "status": "paused",
            "last_error": message,
            "sync": sync,
        })
        self.store.append_event(account_id, {"type": "error", "message": message})

    def _detach_runtime(self, account_id: str, *, expected_process: Any = ...) -> bool:
        with self._lock:
            if expected_process is not ... and self._processes.get(account_id) is not expected_process:
                return False
            self.hub.unregister(account_id)
            process = self._processes.pop(account_id, None)
            input_queue = self._queues.pop(account_id, None)
            getattr(self, "_deadlines", {}).pop(account_id, None)
            if input_queue is not None:
                _put_latest(input_queue, {"type": "stop", "account_id": account_id})
            if process and process.is_alive():
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)
            return True

    def recover(self) -> None:
        for state in self.store.list():
            if state.get("status") == "running":
                try:
                    self.start(str(state["id"]))
                except Exception as exc:  # noqa: BLE001
                    self.store.update_fields(str(state["id"]), {
                        "status": "paused",
                        "last_error": f"自动恢复失败: {exc}",
                    })

    def start(
        self,
        account_id: str,
        *,
        reset_restart_attempts: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            if reset_restart_attempts:
                getattr(self, "_restart_attempts", {}).pop(account_id, None)
            state = self.store.get(account_id)
            mode = state_market_mode(state)
            if mode == "poll_3s" and self.hub.quote_service.get_min_interval() > 3:
                raise ValueError(f"当前套餐最小行情间隔为 {self.hub.quote_service.get_min_interval():g} 秒，不能启动 3 秒行情")
            previous_status = str(state.get("status", "stopped"))
            existing_sync = state.get("sync")
            if mode in QUOTE_MODES or not (
                isinstance(existing_sync, dict) and existing_sync.get("phase") == "live"
            ):
                phase = "catching_up"
                state["sync"] = {
                    "phase": phase,
                    "from": state.get("last_bar") or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp"),
                    "target": None,
                    "through": state.get("last_bar") or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp"),
                    "processed_days": 0,
                    "total_days": 0,
                    "missing_symbols": [],
                    "updated_at": now_iso(),
                }
            state = self.store.update_fields(account_id, {
                "status": "running",
                "last_error": None,
                "sync": state.get("sync"),
            })
            process = self._processes.get(account_id)
            if process is None or not process.is_alive():
                input_queue = self._ctx.Queue(maxsize=2)
                callback_deadline = self._ctx.Value("d", 0.0)
                process = self._ctx.Process(
                    target=_paper_worker,
                    args=(account_id, str(self.store.root), input_queue, callback_deadline),
                    daemon=True,
                )
                try:
                    process.start()
                except Exception:
                    self.store.update_fields(account_id, {"status": previous_status})
                    raise
                self._processes[account_id] = process
                self._queues[account_id] = input_queue
                if not hasattr(self, "_deadlines"):
                    self._deadlines = {}
                self._deadlines[account_id] = callback_deadline
            self.store.append_event(account_id, {"type": "start"})
            return self.store.get(account_id)

    def pause_or_stop(self, account_id: str, status: str) -> dict[str, Any]:
        with self._lock:
            self._detach_runtime(account_id)
            state = self.store.get(account_id)
            sync = dict(state.get("sync", {}))
            sync.update({"phase": "idle", "updated_at": now_iso()})
            self.store.append_event(account_id, {"type": "pause" if status == "paused" else "stop"})
            return self.store.update_fields(account_id, {"status": status, "sync": sync})

    def unlock_risk(self, account_id: str) -> dict[str, Any]:
        state = self.store.get(account_id)
        checkpoint = dict(state.get("checkpoint", {}))
        risk = dict(checkpoint.get("risk", {}))
        status = dict(risk.get("status", state.get("risk_status", {})))
        status["drawdown_locked"] = False
        if not status.get("daily_loss_locked"):
            status["reason"] = None
            status["triggered_at"] = None
        risk["status"] = status
        checkpoint["risk"] = risk
        target = self._queues.get(account_id)
        if target is not None:
            _put_latest(target, {"type": "unlock_risk", "account_id": account_id})
        self.store.append_event(account_id, {"type": "risk_unlocked"})
        return self.store.update_fields(account_id, {
            "checkpoint": checkpoint,
            "risk_status": status,
        })

    def is_alive(self, account_id: str) -> bool:
        process = self._processes.get(account_id)
        return bool(process and process.is_alive())

    def live_valuation(self, state: dict[str, Any]) -> dict[str, Any]:
        """按最新报价只读估值，不修改策略状态或收益曲线。"""
        positions = {
            str(symbol): float(quantity)
            for symbol, quantity in (
                state.get("positions", {})
                or state.get("checkpoint", {}).get("account", {}).get("positions", {})
            ).items()
            if float(quantity) > 0
        }
        if state.get("status") != "running":
            return {"live": False, "as_of": None, "date": None, "missing_symbols": sorted(positions)}
        snapshot = self.hub.quote_service.get_fresh_quotes(set(positions))
        if not snapshot["live"]:
            return {
                "live": False,
                "as_of": snapshot.get("as_of"),
                "date": snapshot.get("date"),
                "missing_symbols": snapshot.get("missing_symbols", []),
            }
        cash = float(state.get("cash") or state.get("checkpoint", {}).get("account", {}).get("cash") or 0)
        equity = cash + sum(positions[symbol] * float(snapshot["quotes"][symbol]["last_price"]) for symbol in positions)
        initial = float(state.get("config", {}).get("initial_capital") or 0)
        peak = max(float(state.get("equity_peak") or initial), equity)
        drawdown_pct = ((peak - equity) / peak * 100) if peak else 0.0
        return {
            "live": True,
            "as_of": snapshot.get("as_of"),
            "date": snapshot.get("date"),
            "missing_symbols": [],
            "equity": equity,
            "return_pct": (equity / initial - 1) * 100 if initial else 0.0,
            "drawdown_pct": drawdown_pct,
            "max_drawdown_pct": max(
                float(state.get("max_drawdown_pct") or 0.0),
                drawdown_pct,
            ),
        }

    def status(self) -> dict[str, Any]:
        return self.hub.status()

    def close(self) -> None:
        self._monitor_stop.set()
        if self._monitor_thread is not threading.current_thread():
            self._monitor_thread.join(timeout=2)
        for account_id in list(self._processes):
            self._detach_runtime(account_id)
        self.hub.close()
