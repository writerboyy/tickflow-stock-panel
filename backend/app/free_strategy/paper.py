"""模拟盘共享行情与独立策略进程运行时。"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, time as clock_time, timedelta
from itertools import groupby
from pathlib import Path
from typing import Any

from app.free_strategy.bars import Bar, rows_to_bars
from app.free_strategy.continuation import compact_paper_checkpoint
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine, Quote, RiskConfig
from app.free_strategy.store import PaperAccountStore, now_iso

logger = logging.getLogger(__name__)
_PAPER_WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="paper-webhook")

MARKET_MODES = {"bar_1m", "bar_1d", "poll_3s", "websocket"}
LEGACY_MARKET_MODES = {"bar_5m", "bar_30m"}
QUOTE_MODES = {"poll_3s", "websocket"}
WS_SYMBOL_LIMIT = 100


def state_market_mode(state: dict[str, Any]) -> str:
    explicit = state.get("market_mode") or state.get("config", {}).get("market_mode")
    if explicit:
        return str(explicit)
    timeframe = str(state.get("config", {}).get("timeframe", "1d"))
    return {"1m": "bar_1m", "5m": "bar_5m", "30m": "bar_30m"}.get(timeframe, "bar_1d")


@dataclass
class _Subscription:
    account_id: str
    mode: str
    symbols: set[str]
    asset_type: str
    input_queue: Any
    last_bar: str = ""


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


def _quote_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    symbol = str(raw.get("symbol") or "")
    price = raw.get("last_price", raw.get("close"))
    if not symbol or price is None:
        return None
    timestamp = raw.get("timestamp") or datetime.now().isoformat()
    if isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()
    elif isinstance(timestamp, (int, float)):
        seconds = float(timestamp) / 1000 if float(timestamp) > 10_000_000_000 else float(timestamp)
        timestamp = datetime.fromtimestamp(seconds).isoformat()
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
        values["timestamp"] = parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed
        quotes.append(Quote(**values))
    return quotes


class MarketDataHub:
    """跨账户共享全市场轮询、闭合 K 线读取和 WebSocket 连接。"""

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

    def register(
        self,
        account_id: str,
        mode: str,
        symbols: set[str],
        asset_type: str,
        input_queue: Any,
        last_bar: str = "",
    ) -> None:
        if mode not in MARKET_MODES | LEGACY_MARKET_MODES:
            raise ValueError(f"不支持的行情模式: {mode}")
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        with self._lock:
            if mode == "websocket":
                combined = cleaned | self._websocket_symbols(exclude=account_id)
                if len(combined) > WS_SYMBOL_LIMIT:
                    raise ValueError(f"WebSocket 去重订阅最多 {WS_SYMBOL_LIMIT} 只，当前需要 {len(combined)} 只")
            previous = self._subscriptions.get(account_id)
            self._subscriptions[account_id] = _Subscription(account_id, mode, cleaned, asset_type, input_queue, last_bar)
            try:
                if mode == "poll_3s" and not self._poll_leased:
                    self.quote_service.add_fetch_listener(self._on_poll_quotes)
                    self.quote_service.acquire_temporary_polling(3.0)
                    self._poll_leased = True
                if mode.startswith("bar_"):
                    self._ensure_bar_thread()
                if mode == "websocket":
                    self._sync_websocket()
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
                raise

    def unregister(self, account_id: str) -> None:
        with self._lock:
            removed = self._subscriptions.pop(account_id, None)
            if removed is None:
                return
            if removed.mode == "poll_3s" and not any(s.mode == "poll_3s" for s in self._subscriptions.values()):
                self.quote_service.remove_fetch_listener(self._on_poll_quotes)
                self.quote_service.release_temporary_polling()
                self._poll_leased = False
            if removed.mode == "websocket":
                self._sync_websocket()
            if removed.mode.startswith("bar_") and not any(s.mode.startswith("bar_") for s in self._subscriptions.values()):
                self._bar_stop.set()

    def update_symbols(self, account_id: str, symbols: set[str]) -> None:
        with self._lock:
            subscription = self._subscriptions.get(account_id)
            if subscription is None:
                return
            cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
            if subscription.mode == "websocket" and len(cleaned | self._websocket_symbols(exclude=account_id)) > WS_SYMBOL_LIMIT:
                raise ValueError("运行时股票池扩容超过 WebSocket 100 只上限")
            subscription.symbols = cleaned
            if subscription.mode == "websocket":
                self._sync_websocket()

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
            now = datetime.now()
            with self._lock:
                targets = [sub for sub in self._subscriptions.values() if sub.mode.startswith("bar_")]
            groups: dict[tuple[str, str], list[_Subscription]] = {}
            for sub in targets:
                groups.setdefault((sub.mode, sub.asset_type), []).append(sub)
            for (mode, asset_type), subscriptions in groups.items():
                if mode == "bar_1d" and now.time() < clock_time(15, 1):
                    continue
                symbols = sorted(set().union(*(sub.symbols for sub in subscriptions)))
                if not symbols:
                    continue
                timeframe = {"bar_1m": "1m", "bar_5m": "5m", "bar_30m": "30m"}.get(mode, "1d")
                starts = []
                for sub in subscriptions:
                    if sub.last_bar:
                        try:
                            starts.append(datetime.fromisoformat(sub.last_bar).date())
                        except ValueError:
                            pass
                start_day = min(starts, default=date.today())
                try:
                    rows = _read_rows(self.repo, symbols, start_day, date.today(), asset_type, timeframe)
                except ValueError:
                    continue
                except Exception:  # noqa: BLE001
                    logger.exception("模拟盘闭合 K 线读取失败")
                    continue
                if mode in {"bar_1m", "bar_5m", "bar_30m"}:
                    minutes = int(timeframe[:-1])
                    closed_before = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                    rows = [bar for bar in rows if bar.timestamp + timedelta(minutes=minutes - 1) <= closed_before]
                for sub in subscriptions:
                    fresh = [
                        bar for bar in rows
                        if bar.symbol in sub.symbols and bar.timestamp.isoformat() > sub.last_bar
                    ]
                    if not fresh:
                        continue
                    sub.last_bar = max(bar.timestamp.isoformat() for bar in fresh)
                    _put_latest(sub.input_queue, {
                        "type": "bars",
                        "account_id": sub.account_id,
                        "bars": [bar.as_dict() for bar in fresh],
                    })

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
        recovering = self._ws_state in {"reconnecting", "error"}
        self._ws_state = "connected"
        self._ws_error = None
        if recovering:
            self._dispatch_recovery_snapshot()
        self._dispatch_quotes("websocket", records)

    def _on_websocket_error(self, message: str) -> None:
        self._ws_state = "reconnecting"
        self._ws_error = str(message)
        self._ws_disconnected_at = datetime.now()
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
                bars = _read_rows(self.repo, sorted(sub.symbols), date.today(), date.today(), sub.asset_type, "1m")
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
) -> FreeStrategyEngine:
    from app.free_strategy.process import (
        MarketData,
        _instrument_records,
        _prepare_market_reference,
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
    engine = FreeStrategyEngine(
        source,
        timeframe=timeframe,
        config=config,
        state=state.get("state", {}),
        instruments=_instrument_records(repo, asset_type, timeframe),
        risk_config=risk,
    )
    runtime_timestamp = (
        state.get("last_bar")
        or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp")
    )
    run_start = (
        datetime.fromisoformat(str(runtime_timestamp)).date()
        if runtime_timestamp else date.today()
    )
    engine.set_run_window(run_start, date.today())
    if "unit_net_value" in engine.extra_history_requirements:
        from app.free_strategy.fund_nav import prepare_fund_nav_data

        prepare_fund_nav_data(repo, engine, run_start, date.today())

    history_asset_by_symbol = {
        str(item["symbol"]): requested_asset
        for requested_asset, _period in engine.market_history_requirements
        for item in _instrument_records(repo, requested_asset, "1d")
    }
    market_loaded_through: date | None = None

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
    engine.set_market_history_loader(load_market_history)
    if preload_market_history and engine.market_history_requirements:
        load_market_history(datetime.now())
    if state.get("checkpoint"):
        engine.restore_checkpoint(state["checkpoint"])
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
    return {
        "timestamp": timestamp.isoformat(),
        "equity": equity,
        "cash": engine.account.cash,
        "nav": equity / initial_capital if initial_capital else 1.0,
        "drawdown_pct": ((peak - equity) / peak * 100) if peak else 0.0,
        "positions": {
            symbol: quantity
            for symbol, quantity in engine.account.positions.items()
            if quantity > 0
        },
        "source": "paper",
    }


def _paper_worker(account_id: str, root: str, input_queue: Any) -> None:
    account_root = Path(root) / account_id
    store = PaperAccountStore(Path(root).parent)
    state = store.get(account_id)
    try:
        engine = _engine_from_state(
            state,
            account_root,
            Path(root).parent,
            preload_market_history=True,
        )
    except Exception as exc:  # noqa: BLE001
        state["status"] = "paused"
        state["last_error"] = f"模拟账户初始化失败: {exc}"
        store.save(state)
        store.append_event(account_id, {"type": "error", "message": state["last_error"]})
        return
    state["execution_mode"] = engine.execution_mode
    state["scheduled_times"] = engine.scheduled_times
    state["universe"] = engine.universe
    store.save(state)
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
        channels = set(state.get("notification_channels", []))
        if not channels:
            return
        from app.services import preferences, webhook_adapter

        symbol = str(event.get("symbol") or "")
        detail = str(event.get("reason") or event.get("message") or event.get("status") or "")
        body = f"{state.get('name', account_id)} {symbol} {detail}".strip()
        if "feishu" in channels and preferences.get_feishu_webhook_url():
            _PAPER_WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_feishu,
                preferences.get_feishu_webhook_url(),
                "模拟盘",
                body,
                preferences.get_feishu_webhook_secret(),
            )
        if "wecom" in channels and preferences.get_wecom_webhook_url():
            _PAPER_WEBHOOK_EXECUTOR.submit(
                webhook_adapter.send_wecom,
                preferences.get_wecom_webhook_url(),
                "模拟盘",
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
        before_orders = len(engine.account.orders)
        before_fills = len(engine.account.fills)
        before_logs = len(engine.logs)
        before_risk = engine.risk_status
        try:
            if message.get("type") == "quotes":
                quotes = _quotes_from_records(message.get("quotes", []))
                if engine._last_timestamp is not None:  # noqa: SLF001
                    quotes = [quote for quote in quotes if quote.timestamp > engine._last_timestamp]  # noqa: SLF001
                if not quotes:
                    continue
                engine.process_quotes(quotes)
                current["last_quote"] = max((quote.timestamp.isoformat() for quote in quotes), default=current.get("last_quote"))
            elif message.get("type") == "bars":
                bars = rows_to_bars(message.get("bars", []))
                if engine._last_timestamp is not None:  # noqa: SLF001
                    bars = [bar for bar in bars if bar.timestamp > engine._last_timestamp]  # noqa: SLF001
                if not bars:
                    continue
                snapshots = []
                for timestamp, rows in groupby(bars, key=lambda bar: bar.timestamp):
                    engine.run(list(rows), finalize_session=False, return_result=False)
                    snapshots.append(_equity_snapshot(engine, current, timestamp))
                store.upsert_equity_curve(account_id, snapshots)
                current["last_bar"] = max((bar.timestamp.isoformat() for bar in bars), default=current.get("last_bar"))
            else:
                continue
        except Exception as exc:  # noqa: BLE001
            current["status"] = "paused"
            current["last_error"] = str(exc)
            store.save(current)
            store.append_event(account_id, {"type": "error", "message": str(exc)})
            continue
        for order in engine.account.orders[before_orders:]:
            event_type = "rejected" if order.status == "rejected" else "order"
            event = {"type": event_type, **asdict(order)}
            store.append_event(account_id, event)
            notify(event)
        for fill in engine.account.fills[before_fills:]:
            event = {"type": "fill", **asdict(fill)}
            store.append_event(account_id, event)
            notify(event)
        for item in engine.logs[before_logs:]:
            store.append_event(account_id, {"type": "log", **item})
        if engine.risk_status != before_risk and engine.risk_status.get("reason"):
            event = {"type": "risk", **engine.risk_status}
            store.append_event(account_id, event)
            notify(event)
        timestamp = engine._last_timestamp or datetime.now()  # noqa: SLF001
        if message.get("type") == "quotes":
            store.upsert_equity_curve(account_id, [_equity_snapshot(engine, current, timestamp)])
        engine.account.equity_curve.clear()
        checkpoint = compact_paper_checkpoint(engine.checkpoint())
        prices = dict(engine._current_close_prices)  # noqa: SLF001
        equity = engine.account.equity(prices)
        peak = max(float(current.get("equity_peak", engine.config.initial_capital)), equity)
        for key in ("account", "state", "runtime"):
            current.pop(key, None)
        current.update({
            "checkpoint": checkpoint,
            "universe": engine.universe,
            "risk_status": engine.risk_status,
            "cash": engine.account.cash,
            "equity": equity,
            "return_pct": (equity / engine.config.initial_capital - 1) * 100,
            "drawdown_pct": ((peak - equity) / peak * 100) if peak else 0,
            "positions": {
                symbol: quantity
                for symbol, quantity in engine.account.positions.items()
                if quantity > 0
            },
            "equity_peak": peak,
            "last_error": None,
        })
        store.save(current)


class PaperTradingSupervisor:
    def __init__(self, data_dir: Path, quote_service: Any, repo: Any) -> None:
        self.data_dir = Path(data_dir)
        self.store = PaperAccountStore(self.data_dir)
        self.hub = MarketDataHub(quote_service, repo)
        self._ctx = mp.get_context("spawn")
        self._processes: dict[str, mp.Process] = {}
        self._queues: dict[str, Any] = {}
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
            try:
                state = self.store.get(account_id)
            except FileNotFoundError:
                self._detach_runtime(account_id)
                continue
            process = self._processes.get(account_id)
            if state.get("status") != "running":
                self._detach_runtime(account_id)
                continue
            if process is None or not process.is_alive():
                self._detach_runtime(account_id)
                state["status"] = "paused"
                state["last_error"] = "策略子进程已退出"
                self.store.save(state)
                self.store.append_event(account_id, {"type": "error", "message": state["last_error"]})
                continue
            account = state.get("account", {})
            symbols = set(state.get("universe", []))
            if not symbols:
                symbols.update(state.get("config", {}).get("symbols", []))
            symbols.update(symbol for symbol, quantity in account.get("positions", {}).items() if float(quantity) > 0)
            symbols.add(str(state.get("config", {}).get("benchmark_symbol", "510300.SH")))
            try:
                self.hub.update_symbols(account_id, symbols)
            except ValueError as exc:
                state = self.pause_or_stop(account_id, "paused")
                state["last_error"] = str(exc)
                self.store.save(state)
                self.store.append_event(account_id, {"type": "error", "message": str(exc)})

    def _detach_runtime(self, account_id: str) -> None:
        with self._lock:
            self.hub.unregister(account_id)
            process = self._processes.pop(account_id, None)
            input_queue = self._queues.pop(account_id, None)
            if input_queue is not None:
                _put_latest(input_queue, {"type": "stop", "account_id": account_id})
            if process and process.is_alive():
                process.join(timeout=2)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)

    def recover(self) -> None:
        for state in self.store.list():
            if state.get("status") == "running":
                try:
                    self.start(str(state["id"]))
                except Exception as exc:  # noqa: BLE001
                    state["status"] = "paused"
                    state["last_error"] = f"自动恢复失败: {exc}"
                    self.store.save(state)

    def start(self, account_id: str) -> dict[str, Any]:
        with self._lock:
            state = self.store.get(account_id)
            mode = state_market_mode(state)
            if mode == "poll_3s" and self.hub.quote_service.get_min_interval() > 3:
                raise ValueError(f"当前套餐最小行情间隔为 {self.hub.quote_service.get_min_interval():g} 秒，不能启动 3 秒行情")
            symbols, execution_mode = inspect_account_runtime(state, self.store._path(account_id), self.data_dir)
            if mode in QUOTE_MODES and execution_mode == "full_bar":
                raise ValueError("3秒行情和 WebSocket 策略必须定义 on_quote(context, quotes) 或定时任务")
            if mode.startswith("bar_") and execution_mode == "quote":
                raise ValueError("K线模式策略必须定义 on_bar(context, bars) 或定时任务")
            previous_status = str(state.get("status", "stopped"))
            state["status"] = "running"
            state["execution_mode"] = execution_mode
            state["universe"] = sorted(symbols)
            state["last_error"] = None
            self.store.save(state)
            process = self._processes.get(account_id)
            if process is None or not process.is_alive():
                input_queue = self._ctx.Queue(maxsize=2)
                process = self._ctx.Process(target=_paper_worker, args=(account_id, str(self.store.root), input_queue), daemon=True)
                process.start()
                self._processes[account_id] = process
                self._queues[account_id] = input_queue
            try:
                self.hub.register(
                    account_id,
                    mode,
                    symbols,
                    str(state.get("config", {}).get("asset_type", "stock")),
                    self._queues[account_id],
                    str(state.get("last_bar") or state.get("checkpoint", {}).get("runtime", {}).get("last_timestamp") or ""),
                )
            except Exception:
                self._detach_runtime(account_id)
                state["status"] = previous_status
                self.store.save(state)
                raise
            self.store.append_event(account_id, {"type": "start"})
            return self.store.get(account_id)

    def pause_or_stop(self, account_id: str, status: str) -> dict[str, Any]:
        with self._lock:
            state = self.store.get(account_id)
            self._detach_runtime(account_id)
            state = self.store.get(account_id)
            state["status"] = status
            self.store.append_event(account_id, {"type": "pause" if status == "paused" else "stop"})
            return self.store.save(state)

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
        state["checkpoint"] = checkpoint
        state["risk_status"] = status
        target = self._queues.get(account_id)
        if target is not None:
            _put_latest(target, {"type": "unlock_risk", "account_id": account_id})
        self.store.append_event(account_id, {"type": "risk_unlocked"})
        return self.store.save(state)

    def is_alive(self, account_id: str) -> bool:
        process = self._processes.get(account_id)
        return bool(process and process.is_alive())

    def status(self) -> dict[str, Any]:
        return self.hub.status()

    def close(self) -> None:
        self._monitor_stop.set()
        if self._monitor_thread is not threading.current_thread():
            self._monitor_thread.join(timeout=2)
        for account_id in list(self._processes):
            self._detach_runtime(account_id)
        self.hub.close()
