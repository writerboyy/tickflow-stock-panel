"""持仓风控实时编排、证据计算和建议生成。"""
from __future__ import annotations

import hashlib
import math
import queue
import re
import statistics
import threading
import time
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, time as clock_time
from typing import Any

import polars as pl

from app.indicators.pipeline import ENRICHED_COLUMNS
from app.market_time import cn_now, cn_today
from app.services import alert_store
from app.services.position_risk_store import PositionRiskStore
from app.strategy.intraday_signals import INTRADAY_SIGNAL_LABELS

_ACCOUNT_ID = "position-risk"
_RULE_WEIGHTS = {
    "cost": 30,
    "trend": 20,
    "momentum": 10,
    "limit": 15,
    "flow": 15,
    "signal": 10,
}

_SIGNAL_LABELS = {
    **{key: value for key, value in ENRICHED_COLUMNS.items() if key.startswith("signal_")},
    **INTRADAY_SIGNAL_LABELS,
}
_SIGNAL_TOKEN = re.compile(r"(?<![A-Za-z0-9_])(?:signal|csg)[._][A-Za-z0-9_]+")


def position_risk_signal_label(signal_id: str, custom_labels: dict[str, str] | None = None) -> str:
    """把持仓风控内部信号 ID 转为用户可读名称，兼容历史点号格式。"""
    normalized = signal_id.replace("signal.", "signal_").replace("csg.", "csg_")
    if custom_labels:
        label = custom_labels.get(signal_id) or custom_labels.get(normalized)
        if label and label not in {signal_id, normalized}:
            return label
    return _SIGNAL_LABELS.get(normalized, signal_id)


def localize_position_risk_text(text: str, custom_labels: dict[str, str] | None = None) -> str:
    """替换事件/建议文本中残留的内置信号 ID，不改写未知业务文本。"""
    return _SIGNAL_TOKEN.sub(
        lambda match: position_risk_signal_label(match.group(0), custom_labels),
        text,
    )


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _action_pct(config: dict[str, Any], default: int) -> int:
    value = _finite(config.get("action_pct"))
    if value is None:
        value = default
    return max(0, min(100, int(round(value))))


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone().replace(tzinfo=None) if value.tzinfo else value
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            return None
    return None


def _is_continuous_trading(now: datetime) -> bool:
    return now.weekday() < 5 and (
        clock_time(9, 30) <= now.time() <= clock_time(11, 30)
        or clock_time(13, 0) <= now.time() <= clock_time(15, 0)
    )


class PositionRiskService:
    """只生成提醒和建议；任何路径都不修改真实或模拟持仓。"""

    def __init__(self, data_dir, repo, quote_service, app_state) -> None:
        self.store = PositionRiskStore(data_dir)
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._queue: queue.Queue = queue.Queue(maxsize=8)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._latest_quotes: dict[str, dict[str, Any]] = {}
        self._history: dict[str, dict[str, Any]] = {}
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=20))
        self._flow: dict[str, deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=240))
        self._active_rules: set[tuple[str, str]] = set()
        self._runtime_status = "idle"
        self._runtime_reason = "尚未导入持仓"
        self._polling_lease = False
        self._last_processed_at: str | None = None
        self._quote_gap_active = False
        self._recovery_pending_symbols: set[str] = set()
        self._flow_anchor_pending: set[str] = set()
        self._custom_signal_directions: dict[str, str] = {}
        self._custom_signal_labels: dict[str, str] = {}
        self._recent_exit_signals: dict[str, deque[tuple[float, str]]] = defaultdict(
            lambda: deque(maxlen=20)
        )
        self._severe_events: deque[float] = deque(maxlen=20)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._worker, name="position-risk", daemon=True)
        self._thread.start()
        add_listener = getattr(self.quote_service, "add_alert_listener", None)
        if callable(add_listener):
            add_listener(self.enqueue_monitor_events)
        self.refresh_subscription()

    def stop(self) -> None:
        remove_listener = getattr(self.quote_service, "remove_alert_listener", None)
        if callable(remove_listener):
            remove_listener(self.enqueue_monitor_events)
        self._detach_market()
        self._stop.set()
        try:
            self._queue.put_nowait({"type": "stop"})
        except queue.Full:
            pass
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _put_latest(self, payload: dict[str, Any]) -> None:
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

    def enqueue_depth(self, records: list[dict[str, Any]]) -> None:
        self._put_latest({"type": "depth", "records": records})

    def enqueue_monitor_events(self, events: list[dict[str, Any]]) -> None:
        selected = [event for event in events if event.get("source") != "position_risk"]
        if selected:
            self._put_latest({"type": "monitor_events", "events": selected})

    def _mark_quote_gap(self, reason: str) -> None:
        """标记行情连续性已丢失，恢复后必须重新建立规则基线。"""
        portfolio = self.store.load()
        symbols = {str(item.get("symbol") or "").strip().upper() for item in portfolio["positions"]}
        symbols.discard("")
        if not symbols:
            return
        self._quote_gap_active = True
        self._recovery_pending_symbols.update(symbols)
        self._flow_anchor_pending.update(symbols)
        for symbol in symbols:
            self._flow[symbol].clear()
            self._depth[symbol].clear()
            self.store.set_runtime(f"depth:{symbol}", {})
        self._runtime_status = "reconnecting"
        self._runtime_reason = reason

    def _mark_quote_recovered(self, symbols: set[str]) -> None:
        if not self._quote_gap_active:
            return
        self._recovery_pending_symbols.update(symbols)
        self._quote_gap_active = False
        self._runtime_status = "websocket"
        self._runtime_reason = "行情已恢复，正在重新建立连续性基线"

    def _on_poll(self) -> None:
        portfolio = self.store.load()
        symbols = {str(item.get("symbol")) for item in portfolio["positions"]}
        rows = self.quote_service.get_latest_quotes(symbols)
        if rows:
            self._put_latest({"type": "quotes", "quotes": rows})

    def _detach_market(self) -> None:
        supervisor = getattr(self.app_state, "paper_supervisor", None)
        hub = getattr(supervisor, "hub", None)
        if hub is not None:
            try:
                hub.unregister(_ACCOUNT_ID)
            except Exception:  # noqa: BLE001
                pass
        self.quote_service.remove_fetch_listener(self._on_poll)
        self.quote_service.remove_symbol_consumer(_ACCOUNT_ID)
        remove_intraday = getattr(self.quote_service, "remove_intraday_consumer", None)
        if callable(remove_intraday):
            remove_intraday(_ACCOUNT_ID)
        if self._polling_lease:
            self.quote_service.release_temporary_polling()
            self._polling_lease = False

    def refresh_subscription(self) -> None:
        self._detach_market()
        portfolio = self.store.load()
        symbols = {str(item.get("symbol") or "").strip().upper() for item in portfolio["positions"]}
        symbols.discard("")
        self._preload_history(symbols)
        if not symbols:
            self._runtime_status = "idle"
            self._runtime_reason = "尚未导入持仓"
            self._notify_updated()
            return

        set_intraday = getattr(self.quote_service, "set_intraday_consumer", None)
        if callable(set_intraday):
            set_intraday(_ACCOUNT_ID, symbols, "stock")

        supervisor = getattr(self.app_state, "paper_supervisor", None)
        hub = getattr(supervisor, "hub", None)
        capset = getattr(self.app_state, "capabilities", None)
        websocket_allowed = False
        try:
            from app.tickflow.capabilities import Cap

            websocket_allowed = bool(capset and capset.has(Cap.WEBSOCKET))
        except Exception:  # noqa: BLE001
            websocket_allowed = False
        if websocket_allowed and hub is not None:
            try:
                hub.register(_ACCOUNT_ID, "websocket", symbols, "stock", self._queue)
                self._runtime_status = "websocket"
                self._runtime_reason = "持仓池已整体接入共享 TickFlow WS"
                self._notify_updated()
                return
            except (ValueError, RuntimeError) as exc:
                self._runtime_reason = str(exc)

        self.quote_service.set_symbol_consumer(_ACCOUNT_ID, symbols)
        self.quote_service.add_fetch_listener(self._on_poll)
        try:
            interval = max(3.0, float(self.quote_service.get_min_interval()))
            self.quote_service.acquire_temporary_polling(interval)
            self._polling_lease = True
            self._runtime_status = "polling_degraded"
            if not self._runtime_reason:
                self._runtime_reason = "WS 能力不可用，全部持仓已转行情轮询"
        except ValueError as exc:
            self._runtime_status = "data_unavailable"
            self._runtime_reason = str(exc)
        self._notify_updated()

    def _preload_history(self, symbols: set[str]) -> None:
        rows: dict[str, dict[str, Any]] = {}
        stock_symbols: list[str] = []
        etf_symbols: list[str] = []
        for symbol in symbols:
            try:
                asset_type = self.repo.resolve_asset_type(symbol)
            except Exception:  # noqa: BLE001
                asset_type = "stock"
            (etf_symbols if asset_type == "etf" else stock_symbols).append(symbol)
        try:
            frame, _ = self.repo.get_enriched_latest()
            if stock_symbols and not frame.is_empty():
                rows.update({str(row["symbol"]): row for row in frame.filter(
                    frame["symbol"].is_in(stock_symbols)
                ).to_dicts()})
        except Exception:  # noqa: BLE001
            pass
        try:
            frame, _ = self.repo.get_enriched_latest_asset("etf")
            if etf_symbols and not frame.is_empty():
                rows.update({str(row["symbol"]): row for row in frame.filter(
                    frame["symbol"].is_in(etf_symbols)
                ).to_dicts()})
        except Exception:  # noqa: BLE001
            pass
        try:
            from app.strategy import custom_signals

            custom = [
                item for item in custom_signals.load_all(self.store.root.parents[1])
                if item.get("enabled", True)
            ]
            self._custom_signal_directions = {
                f"csg_{item['id']}": str(item.get("kind") or "both") for item in custom
            }
            self._custom_signal_labels = {
                f"csg_{item['id']}": str(item.get("name") or item['id']) for item in custom
            }
        except Exception:  # noqa: BLE001
            self._custom_signal_directions = {}
            self._custom_signal_labels = {}
        with self._lock:
            self._history = rows

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                message = self._queue.get(timeout=1)
            except queue.Empty:
                self._check_quote_staleness()
                continue
            if message.get("type") == "stop":
                return
            if message.get("type") in {"gap", "recovery"}:
                if message.get("type") == "gap":
                    self._mark_quote_gap(str(message.get("reason") or "WS 连接中断"))
                    self._notify_updated()
                else:
                    recovery_symbols = {
                        str(quote.get("symbol") or "").strip().upper()
                        for quote in message.get("quotes", [])
                        if str(quote.get("symbol") or "").strip()
                    }
                    if recovery_symbols:
                        self._mark_quote_recovered(recovery_symbols)
                for quote in message.get("quotes", []):
                    self._ingest_quote(quote)
                continue
            if message.get("type") == "depth":
                self._ingest_depth(message.get("records", []))
                continue
            if message.get("type") == "monitor_events":
                self._ingest_monitor_events(message.get("events", []))
                continue
            quotes = list(message.get("quotes", []))
            deadline = time.monotonic() + 0.2
            while time.monotonic() < deadline:
                try:
                    next_message = self._queue.get_nowait()
                except queue.Empty:
                    break
                if next_message.get("type") == "quotes":
                    quotes.extend(next_message.get("quotes", []))
                else:
                    self._put_latest(next_message)
                    break
            for quote in quotes:
                self._ingest_quote(quote)
            if quotes:
                self._mark_quote_recovered({
                    str(quote.get("symbol") or "").strip().upper()
                    for quote in quotes
                    if str(quote.get("symbol") or "").strip()
                })
                self._last_processed_at = cn_now().isoformat()
                self._evaluate_current()
                self._notify_updated()

    def _ingest_quote(self, raw: dict[str, Any]) -> None:
        symbol = str(raw.get("symbol") or "").strip().upper()
        price = _finite(raw.get("last_price", raw.get("close")))
        if not symbol or price is None or price <= 0:
            return
        timestamp = _timestamp(raw.get("timestamp")) or cn_now().replace(tzinfo=None)
        previous = self._latest_quotes.get(symbol)
        current_amount = _finite(raw.get("amount"))
        if symbol in self._flow_anchor_pending:
            self._flow_anchor_pending.discard(symbol)
        elif previous is not None and current_amount is not None:
            previous_amount = _finite(previous.get("amount"))
            previous_price = _finite(previous.get("last_price", previous.get("close")))
            if previous_amount is not None and previous_price is not None and current_amount >= previous_amount:
                delta = current_amount - previous_amount
                if delta > 0:
                    direction = 1.0 if price > previous_price else -1.0 if price < previous_price else 0.0
                    current_volume = _finite(raw.get("volume"))
                    previous_volume = _finite(previous.get("volume"))
                    volume_delta = (
                        max(0.0, current_volume - previous_volume)
                        if current_volume is not None and previous_volume is not None
                        else 0.0
                    )
                    self._flow[symbol].append({
                        "ts": timestamp.timestamp(),
                        "amount": delta,
                        "volume": volume_delta,
                        "direction": direction,
                        "price": price,
                    })
        self._latest_quotes[symbol] = {**raw, "symbol": symbol, "last_price": price, "timestamp": timestamp.isoformat()}

    def _ingest_depth(self, records: list[dict[str, Any]]) -> None:
        portfolio_symbols = {item["symbol"] for item in self.store.load()["positions"]}
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            if symbol in portfolio_symbols:
                self._depth[symbol].append({**raw, "received_at": time.time()})

    def _ingest_monitor_events(self, events: list[dict[str, Any]]) -> None:
        portfolio = self.store.load()
        positions = {item["symbol"] for item in portfolio["positions"]}
        for event in events:
            symbol = str(event.get("symbol") or "")
            if symbol not in positions:
                continue
            configured = self._signal_config(
                portfolio,
                symbol,
                "monitor_rules",
                str(event.get("rule_id") or ""),
            )
            reduction = int(configured.get("action_pct") or 0)
            if reduction <= 0:
                continue
            score = max(50, 50 + (20 if event.get("severity") == "critical" else 0))
            self._create_recommendation(
                portfolio,
                symbol=symbol,
                rule_id=f"monitor:{event.get('rule_id')}",
                severity=str(event.get("severity") or "info"),
                risk_score=score,
                reduction_pct=reduction,
                reasons=[localize_position_risk_text(str(event.get("message") or event.get("rule_name") or "监控规则命中"), self._custom_signal_labels)],
                source_ids=[str(event.get("rule_id") or "")],
                fingerprint=str(event.get("fingerprint") or f"monitor:{event.get('rule_id')}:{symbol}:{event.get('ts')}")
            )
        self._notify_updated()

    def _quote_is_fresh(self, quote: dict[str, Any], now: datetime) -> bool:
        timestamp = _timestamp(quote.get("timestamp"))
        return bool(timestamp and timestamp.date() == now.date() and 0 <= (now.replace(tzinfo=None) - timestamp).total_seconds() <= 30)

    @staticmethod
    def _quote_age_in_session(timestamp: datetime | None, now: datetime) -> float:
        """只计算当前连续竞价时段的行情年龄，排除午休和跨日间隔。"""
        current = now.replace(tzinfo=None)
        current_time = current.time()
        if clock_time(9, 30) <= current_time <= clock_time(11, 30):
            session_start = datetime.combine(current.date(), clock_time(9, 30))
        elif clock_time(13, 0) <= current_time <= clock_time(15, 0):
            session_start = datetime.combine(current.date(), clock_time(13, 0))
        else:
            return 0.0
        if timestamp is None:
            return max(0.0, (current - session_start).total_seconds())
        point = timestamp.replace(tzinfo=None)
        if point.date() != current.date() or point < session_start:
            point = session_start
        return max(0.0, (current - point).total_seconds())

    def _rule_config(self, portfolio: dict[str, Any], symbol: str, rule_id: str) -> dict[str, Any]:
        result = deepcopy(portfolio["template"]["rules"].get(rule_id) or {})
        override = (portfolio.get("overrides") or {}).get(symbol) or {}
        result.update((override.get("rules") or {}).get(rule_id) or {})
        return result

    @staticmethod
    def _signal_config(
        portfolio: dict[str, Any],
        symbol: str,
        group: str,
        signal_id: str,
    ) -> dict[str, Any]:
        template_signals = portfolio["template"].get("signals", {})
        result = deepcopy((template_signals.get(group) or {}).get(signal_id) or {})
        override = (portfolio.get("overrides") or {}).get(symbol) or {}
        override_signals = override.get("signals") or {}
        result.update(((override_signals.get(group) or {}).get(signal_id)) or {})
        return result

    def _set_rule(self, symbol: str, rule_id: str, active: bool) -> bool:
        key = (symbol, rule_id)
        was_active = key in self._active_rules
        if active:
            self._active_rules.add(key)
        else:
            self._active_rules.discard(key)
        return active and not was_active

    @staticmethod
    def _sustained(
        runtime: dict[str, Any],
        key: str,
        active: bool,
        seconds: int,
        now: datetime,
    ) -> bool:
        state_key = f"{key}_since"
        if not active:
            runtime.pop(state_key, None)
            return False
        started = _finite(runtime.get(state_key))
        if started is None:
            runtime[state_key] = now.timestamp()
            return seconds <= 0
        return now.timestamp() - started >= seconds

    @staticmethod
    def _recovery_suppressed(runtime: dict[str, Any], key: str, active: bool) -> bool:
        """断线后不把已存在的状态误判为新触发，直到重新看到恢复条件。"""
        if not runtime.get("quote_recovery_pending"):
            return False
        guards = runtime.setdefault("quote_recovery_guards", {})
        if key in guards:
            if active:
                runtime.pop(f"{key}_since", None)
                return True
            guards.pop(key, None)
            runtime.pop(f"{key}_since", None)
            return False
        if active:
            guards[key] = True
            runtime.pop(f"{key}_since", None)
            return True
        return False

    def _evaluate_current(self, *, now: datetime | None = None, force: bool = False) -> None:
        current_time = (now or cn_now()).replace(tzinfo=None)
        if not force and not _is_continuous_trading(current_time):
            return
        portfolio = self.store.load()
        intraday_signals = self._intraday_signals(
            {item["symbol"] for item in portfolio["positions"]}, current_time,
        )
        for position in portfolio["positions"]:
            symbol = position["symbol"]
            quote = self._latest_quotes.get(symbol)
            if not quote or (not force and not self._quote_is_fresh(quote, current_time)):
                continue
            self._evaluate_position(
                portfolio,
                position,
                quote,
                current_time,
                intraday_signals.get(symbol, {}),
            )
        self._evaluate_account(portfolio)

    def _intraday_signals(
        self,
        symbols: set[str],
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        getter = getattr(self.quote_service, "get_intraday_signals", None)
        if not callable(getter):
            return {}
        prev_close = {
            symbol: raw_close
            for symbol in symbols
            if (raw_close := _finite((self._history.get(symbol) or {}).get("raw_close")))
        }
        return getter(
            symbols,
            prev_close=prev_close,
            asset_type="stock",
            now=now,
            consumer_id=_ACCOUNT_ID,
        )

    def _evaluate_position(
        self,
        portfolio: dict[str, Any],
        position: dict[str, Any],
        quote: dict[str, Any],
        now: datetime,
        intraday_signals: dict[str, Any] | None = None,
    ) -> None:
        symbol = position["symbol"]
        price = _finite(quote.get("last_price"))
        cost = _finite(position.get("cost_price"))
        if price is None:
            return
        if intraday_signals:
            limit_up = _finite(quote.get("limit_up"))
            if limit_up and price >= limit_up - max(0.001, limit_up * 1e-6):
                intraday_signals = {
                    **intraday_signals,
                    "signal_intraday_avg_cross_up": False,
                    "signal_intraday_avg_cross_down": False,
                }
        runtime_key = f"position:{symbol}"
        runtime = self.store.get_runtime(runtime_key, {}) or {}
        if symbol in self._recovery_pending_symbols:
            runtime["quote_recovery_pending"] = True
            self._recovery_pending_symbols.discard(symbol)
        high = max(price, _finite(runtime.get("high_price")) or price)
        runtime["high_price"] = high
        runtime["last_price"] = price
        runtime["last_quote_at"] = quote.get("timestamp")

        stop_cfg = self._rule_config(portfolio, symbol, "stop_loss")
        stop_threshold = _finite(stop_cfg.get("threshold"))
        stop_threshold = stop_threshold if stop_threshold is not None else -0.10
        stop_action = _action_pct(stop_cfg, 100)
        stop_active = bool(stop_cfg.get("enabled", True) and cost and price / cost - 1 <= stop_threshold)
        if self._set_rule(symbol, "stop_loss", stop_active):
            self._emit(portfolio, position, "stop_loss", "成本止损", "critical", 85, stop_action, [f"现价较成本亏损 {(price / cost - 1) * 100:.2f}%"])

        trailing_cfg = self._rule_config(portfolio, symbol, "trailing_drawdown")
        activation_gain = _finite(trailing_cfg.get("activation_gain"))
        activation_gain = activation_gain if activation_gain is not None else 0.05
        trailing_threshold = _finite(trailing_cfg.get("threshold"))
        trailing_threshold = trailing_threshold if trailing_threshold is not None else 0.08
        trailing_action = _action_pct(trailing_cfg, 50)
        trailing_active = bool(
            trailing_cfg.get("enabled", True) and cost and high / cost - 1 >= activation_gain
            and price / high - 1 <= -trailing_threshold
        )
        if self._set_rule(symbol, "trailing_drawdown", trailing_active):
            self._emit(portfolio, position, "trailing_drawdown", "盈利回撤", "warn", 60, trailing_action, [f"从持仓高点回撤 {(1 - price / high) * 100:.2f}%"])

        history = self._history.get(symbol) or {}
        raw_close = _finite(history.get("raw_close"))
        adjusted_close = _finite(history.get("close"))
        adjustment = adjusted_close / raw_close if raw_close and adjusted_close else None
        adjusted_price = price * adjustment if adjustment else None
        for days, action in ((5, 0), (10, 25), (20, 50)):
            rule_id = f"ma{days}_breakdown"
            cfg = self._rule_config(portfolio, symbol, rule_id)
            buffer = _finite(cfg.get("buffer"))
            sustain_seconds = _finite(cfg.get("sustain_seconds"))
            configured_action = _action_pct(cfg, action)
            ma = _finite(history.get(f"ma{days}"))
            below = bool(
                cfg.get("enabled", True)
                and adjusted_price
                and ma
                and adjusted_price <= ma * (1 - (buffer if buffer is not None else 0.002))
            )
            ma_suppressed = self._recovery_suppressed(runtime, rule_id, below)
            active = self._sustained(
                runtime, rule_id, below, int(sustain_seconds if sustain_seconds is not None else 5), now,
            ) if not ma_suppressed else False
            if self._set_rule(symbol, rule_id, active) and not ma_suppressed:
                self._emit(
                    portfolio, position, rule_id, f"跌破 MA{days}", "warn" if action else "info",
                    _RULE_WEIGHTS["trend"] + (20 if configured_action >= 50 else 10 if configured_action else 0), configured_action,
                    [f"前复权动态价 {adjusted_price:.3f} 低于 MA{days} {ma:.3f}"],
                )

        limit_down_cfg = self._rule_config(portfolio, symbol, "limit_down")
        limit_down = _finite(quote.get("limit_down"))
        limit_down_action = _action_pct(limit_down_cfg, 100)
        at_limit_down = bool(
            limit_down_cfg.get("enabled", True)
            and limit_down
            and price <= limit_down + 0.001
        )
        if self._set_rule(symbol, "limit_down", at_limit_down):
            self._emit(portfolio, position, "limit_down", "跌停", "critical", 85, limit_down_action, ["原始现价触及当日跌停价"])

        depth_state = self._depth_state(symbol, quote, now)
        sealed_cfg = self._rule_config(portfolio, symbol, "resealed_limit_up")
        sealed_action = _action_pct(sealed_cfg, 0)
        if self._set_rule(
            symbol, "sealed_limit_up", bool(sealed_cfg.get("enabled", True) and depth_state["sealed"]),
        ):
            self._emit(
                portfolio, position, "sealed_limit_up", "涨停封板", "info", 35, sealed_action,
                ["连续 3 个五档快照确认封板"],
            )
        broken_cfg = self._rule_config(portfolio, symbol, "broken_limit_up")
        broken_action = _action_pct(broken_cfg, 50)
        if self._set_rule(
            symbol, "broken_limit_up", bool(broken_cfg.get("enabled", True) and depth_state["broken"]),
        ):
            self._emit(portfolio, position, "broken_limit_up", "涨停炸板", "critical", 70, broken_action, ["连续封板状态中断"])
        shrink_80_cfg = self._rule_config(portfolio, symbol, "sealed_order_shrink_80")
        shrink_50_cfg = self._rule_config(portfolio, symbol, "sealed_order_shrink_50")
        shrink_80_action = _action_pct(shrink_80_cfg, 50)
        shrink_50_action = _action_pct(shrink_50_cfg, 25)
        shrink_80_threshold = _finite(shrink_80_cfg.get("threshold"))
        shrink_80_threshold = shrink_80_threshold if shrink_80_threshold is not None else 0.80
        shrink_50_threshold = _finite(shrink_50_cfg.get("threshold"))
        shrink_50_threshold = shrink_50_threshold if shrink_50_threshold is not None else 0.50
        shrink_80 = bool(
            not depth_state["broken"]
            and shrink_80_cfg.get("enabled", True)
            and depth_state["shrink_ratio"] >= shrink_80_threshold
        )
        shrink_50 = bool(
            not depth_state["broken"]
            and shrink_50_cfg.get("enabled", True)
            and shrink_50_threshold <= depth_state["shrink_ratio"] < shrink_80_threshold
        )
        if self._set_rule(symbol, "sealed_order_shrink_80", shrink_80):
            self._emit(
                portfolio, position, "sealed_order_shrink_80", f"封单减少 {shrink_80_threshold:.0%}", "critical",
                70, shrink_80_action, [f"买一封单较盘中峰值减少至少 {shrink_80_threshold:.0%}"],
            )
        if self._set_rule(symbol, "sealed_order_shrink_50", shrink_50):
            self._emit(
                portfolio, position, "sealed_order_shrink_50", f"封单减少 {shrink_50_threshold:.0%}", "warn",
                55, shrink_50_action, [f"买一封单较盘中峰值减少至少 {shrink_50_threshold:.0%}"],
            )
        imbalance_cfg = self._rule_config(portfolio, symbol, "orderbook_imbalance")
        imbalance_threshold = _finite(imbalance_cfg.get("threshold"))
        imbalance_threshold = imbalance_threshold if imbalance_threshold is not None else -0.35
        imbalance_sustain = _finite(imbalance_cfg.get("sustain_seconds"))
        imbalance_sustain = imbalance_sustain if imbalance_sustain is not None else 10
        imbalance_action = _action_pct(imbalance_cfg, 25)
        imbalance_below = bool(
            imbalance_cfg.get("enabled", True)
            and depth_state["imbalance"] is not None
            and depth_state["imbalance"] < imbalance_threshold
        )
        imbalance_suppressed = self._recovery_suppressed(runtime, "orderbook_imbalance", imbalance_below)
        imbalance_active = self._sustained(
            runtime,
            "orderbook_imbalance",
            imbalance_below,
            int(imbalance_sustain),
            now,
        ) if not imbalance_suppressed else False
        if self._set_rule(symbol, "orderbook_imbalance", imbalance_active) and not imbalance_suppressed:
            self._emit(
                portfolio, position, "orderbook_imbalance", "盘口失衡", "warn", 55, imbalance_action,
                [f"盘口失衡 {depth_state['imbalance']:.2f} 持续 {int(imbalance_sustain)} 秒"],
            )

        large_buy_cfg = self._rule_config(portfolio, symbol, "large_buy")
        large_sell_cfg = self._rule_config(portfolio, symbol, "large_sell")
        buy_flow = self._flow_state(symbol, now, large_buy_cfg)
        sell_flow = self._flow_state(symbol, now, large_sell_cfg)
        flow = self._flow_state(symbol, now)
        for rule_id, active, action, label, flow_state in (
            ("large_buy", buy_flow["large_buy"], _action_pct(large_buy_cfg, 0), "大单买入", buy_flow),
            ("large_sell", sell_flow["large_sell"], _action_pct(large_sell_cfg, 25), "大单卖出", sell_flow),
        ):
            flow_cfg = large_buy_cfg if rule_id == "large_buy" else large_sell_cfg
            if self._set_rule(symbol, rule_id, bool(flow_cfg.get("enabled", True) and active)):
                chosen_action = (
                    50
                    if active and action and depth_state["imbalance"] is not None
                    and depth_state["imbalance"] < -0.35
                    else action
                )
                self._emit(
                    portfolio, position, rule_id, label, "warn" if action else "info",
                    75 if chosen_action >= 50 else _RULE_WEIGHTS["flow"] + (35 if action else 15),
                    chosen_action,
                    [flow_state["summary"]],
                )
        outflow_cfg = self._rule_config(portfolio, symbol, "continuous_outflow")
        outflow_below = bool(
            outflow_cfg.get("enabled", True)
            and flow["samples"] >= 3
            and flow["sell_ratio"] is not None
            and flow["sell_ratio"] >= float(outflow_cfg.get("direction_ratio", 0.65))
        )
        outflow_suppressed = self._recovery_suppressed(runtime, "continuous_outflow", outflow_below)
        outflow_active = self._sustained(
            runtime,
            "continuous_outflow",
            outflow_below,
            int(outflow_cfg.get("sustain_seconds", 10)),
            now,
        ) if not outflow_suppressed else False
        if self._set_rule(symbol, "continuous_outflow", outflow_active) and not outflow_suppressed:
            self._emit(
                portfolio,
                position,
                "continuous_outflow",
                "连续净流出",
                "warn",
                50,
                int(outflow_cfg.get("action_pct", 25)),
                [flow["summary"]],
            )

        recent_five_minutes = [
            item for item in self._flow.get(symbol, ()) if item["ts"] >= now.timestamp() - 300
        ]
        if recent_five_minutes:
            five_minute_high = max(item["price"] for item in recent_five_minutes)
            drawdown_cfg = self._rule_config(portfolio, symbol, "five_minute_drawdown")
            drawdown_threshold = _finite(drawdown_cfg.get("threshold"))
            drawdown_threshold = drawdown_threshold if drawdown_threshold is not None else 0.03
            drawdown_action = _action_pct(drawdown_cfg, 25)
            drawdown_active = bool(
                drawdown_cfg.get("enabled", True)
                and price / five_minute_high - 1 <= -drawdown_threshold
            )
            drawdown_suppressed = self._recovery_suppressed(runtime, "five_minute_drawdown", drawdown_active)
            if self._set_rule(symbol, "five_minute_drawdown", False if drawdown_suppressed else drawdown_active) and not drawdown_suppressed:
                self._emit(
                    portfolio, position, "five_minute_drawdown", "5 分钟高点回撤", "warn",
                    45, drawdown_action, [f"从 5 分钟高点回撤 {(1 - price / five_minute_high):.2%}"],
                )
        snapshot_getter = getattr(self.quote_service, "get_intraday_snapshot", None)
        snapshot = snapshot_getter({symbol}, asset_type="stock", now=now) if callable(snapshot_getter) else {}
        vwap = (snapshot.get("vwap") or {}).get(symbol)
        vwap_cfg = self._rule_config(portfolio, symbol, "vwap_breakdown")
        vwap_buffer = _finite(vwap_cfg.get("buffer"))
        vwap_buffer = vwap_buffer if vwap_buffer is not None else 0.01
        vwap_sustain = _finite(vwap_cfg.get("sustain_seconds"))
        vwap_sustain = vwap_sustain if vwap_sustain is not None else 30
        vwap_action = _action_pct(vwap_cfg, 25)
        vwap_below = bool(
            vwap_cfg.get("enabled", True)
            and vwap
            and price <= vwap * (1 - vwap_buffer)
        )
        vwap_suppressed = self._recovery_suppressed(runtime, "vwap_breakdown", vwap_below)
        vwap_active = self._sustained(
            runtime, "vwap_breakdown", vwap_below,
            int(vwap_sustain), now,
        ) if not vwap_suppressed else False
        if self._set_rule(symbol, "vwap_breakdown", vwap_active) and not vwap_suppressed:
            self._emit(
                portfolio, position, "vwap_breakdown", "跌破分时均价", "warn", 45, vwap_action,
                [f"现价低于 VWAP {(1 - price / vwap):.2%}"],
            )

        combined_signals = {**history, **(intraday_signals or {})}
        overlap_signals = {
            "signal_ma5_breakdown",
            "signal_ma10_breakdown",
            "signal_ma20_breakdown",
            "signal_limit_down",
            "signal_broken_limit_up",
        }
        for signal_id, value in combined_signals.items():
            if signal_id.startswith(("signal_", "csg_")) and value is not True:
                self._set_rule(symbol, f"signal:{signal_id}", False)
        for signal_id, value in combined_signals.items():
            if (
                not signal_id.startswith(("signal_", "csg_"))
                or value is not True
                or signal_id in overlap_signals
            ):
                continue
            is_custom = signal_id.startswith("csg_")
            configured = self._signal_config(
                portfolio,
                symbol,
                "custom" if is_custom else "builtin",
                signal_id,
            )
            if configured.get("enabled", True) is False:
                continue
            signal_rule_id = f"signal:{signal_id}"
            if self._recovery_suppressed(runtime, signal_rule_id, True):
                self._set_rule(symbol, signal_rule_id, False)
                continue
            direction = (
                configured.get("direction")
                or self._custom_signal_directions.get(signal_id)
                or self._signal_direction(signal_id)
            ) if is_custom else self._signal_direction(signal_id)
            configured_action = _finite(configured.get("action_pct"))
            action = int(round(configured_action)) if configured_action is not None else (25 if direction == "exit" else 0)
            action = max(0, min(100, action))
            reasons = ["统一指标流水线命中系统信号"]
            if action:
                recent = self._recent_exit_signals[symbol]
                while recent and recent[0][0] < now.timestamp() - 300:
                    recent.popleft()
                independent = {item[1] for item in recent if item[1] != signal_id}
                recent.append((now.timestamp(), signal_id))
                if independent:
                    action = max(action, 50)
                    reasons.append("5 分钟内两个独立出场信号共振")
            if self._set_rule(symbol, signal_rule_id, True):
                self._emit(
                    portfolio, position, f"signal:{signal_id}", self._signal_label(signal_id, configured),
                    "warn" if action else "info",
                    60 if action >= 50 else _RULE_WEIGHTS["signal"] + (40 if action else 10),
                    action,
                    reasons,
                    source_ids=[signal_id],
                )
        if runtime.get("quote_recovery_pending") and not runtime.get("quote_recovery_guards"):
            runtime.pop("quote_recovery_pending", None)
            runtime.pop("quote_recovery_guards", None)
        self.store.set_runtime(runtime_key, runtime)

    @staticmethod
    def _signal_direction(signal_id: str) -> str:
        exit_tokens = ("dead", "breakdown", "low", "limit_down", "broken")
        return "exit" if any(token in signal_id for token in exit_tokens) else "entry"

    def _signal_label(self, signal_id: str, configured: dict[str, Any] | None = None) -> str:
        configured_label = str((configured or {}).get("label") or "")
        if configured_label and configured_label not in {signal_id, signal_id.replace("signal.", "signal_")}:
            return configured_label
        return position_risk_signal_label(signal_id, self._custom_signal_labels)

    def localize_text(self, text: str) -> str:
        return localize_position_risk_text(text, self._custom_signal_labels)

    def _depth_state(
        self,
        symbol: str,
        quote: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        snapshots = self._depth.get(symbol)
        if not snapshots or len(snapshots) < 3:
            return {
                "sealed": False,
                "broken": False,
                "shrink_ratio": 0.0,
                "imbalance": None,
            }
        limit_up = _finite(quote.get("limit_up"))
        recent = list(snapshots)[-3:]
        sealed_flags = []
        for item in recent:
            bid_price = _finite(item.get("bid1_price", item.get("bid_price1")))
            bid_volume = _finite(item.get("bid1_volume", item.get("bid_volume1")))
            ask_price = _finite(item.get("ask1_price", item.get("ask_price1")))
            ask_volume = _finite(item.get("ask1_volume", item.get("ask_volume1")))
            sealed_flags.append(bool(limit_up and bid_price and abs(bid_price - limit_up) < 0.001 and bid_volume and bid_volume > 0 and not (ask_price and ask_volume)))
        sealed = all(sealed_flags)
        runtime = self.store.get_runtime(f"depth:{symbol}", {}) or {}
        was_sealed = bool(runtime.get("sealed"))
        latest = recent[-1]
        latest_bid = _finite(latest.get("bid1_volume", latest.get("bid_volume1"))) or 0.0
        peak_bid = max(latest_bid, _finite(runtime.get("peak_bid_volume")) or 0.0)
        shrink_ratio = 1 - latest_bid / peak_bid if peak_bid > 0 else 0.0
        bid_volumes = latest.get("bid_volumes") or [latest_bid]
        ask_volumes = latest.get("ask_volumes") or [
            _finite(latest.get("ask1_volume", latest.get("ask_volume1"))) or 0.0
        ]
        bid_total = sum(_finite(value) or 0 for value in bid_volumes)
        ask_total = sum(_finite(value) or 0 for value in ask_volumes)
        imbalance = (
            (bid_total - ask_total) / (bid_total + ask_total)
            if bid_total + ask_total > 0 else None
        )
        runtime["sealed"] = sealed
        runtime["peak_bid_volume"] = peak_bid
        runtime["last_depth_at"] = now.timestamp()
        self.store.set_runtime(f"depth:{symbol}", runtime)
        return {
            "sealed": sealed,
            "broken": was_sealed and not sealed,
            "shrink_ratio": shrink_ratio,
            "imbalance": imbalance,
        }

    def _flow_state(
        self,
        symbol: str,
        now: datetime,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        config = config or {}
        configured_window = _finite(config.get("window_seconds"))
        window_seconds = max(1, int(configured_window if configured_window is not None else 60))
        cutoff = now.timestamp() - window_seconds
        values = [item for item in self._flow.get(symbol, ()) if item["ts"] >= cutoff]
        if not values:
            return {
                "large_buy": False,
                "large_sell": False,
                "buy_ratio": None,
                "sell_ratio": None,
                "samples": 0,
                "summary": f"最近 {window_seconds} 秒成交证据不足",
            }
        amounts = [item["amount"] for item in values]
        total = sum(amounts)
        buy = sum(item["amount"] for item in values if item["direction"] > 0)
        sell = sum(item["amount"] for item in values if item["direction"] < 0)
        buy_ratio = buy / total if total > 0 else 0.0
        sell_ratio = sell / total if total > 0 else 0.0
        configured_samples = _finite(config.get("min_samples"))
        minimum_samples = max(1, int(configured_samples if configured_samples is not None else 7))
        if len(values) < minimum_samples:
            return {
                "large_buy": False,
                "large_sell": False,
                "buy_ratio": buy_ratio,
                "sell_ratio": sell_ratio,
                "samples": len(values),
                "summary": f"最近 {window_seconds} 秒方向占比 买{buy_ratio:.0%}/卖{sell_ratio:.0%}，样本不足 {minimum_samples} 笔",
            }
        median = statistics.median(amounts)
        mad = statistics.median(abs(value - median) for value in amounts)
        def is_large(direction_ratio: float) -> bool:
            configured_amount = _finite(config.get("min_amount"))
            configured_mad = _finite(config.get("mad_multiplier"))
            configured_z_score = _finite(config.get("min_z_score"))
            configured_direction = _finite(config.get("direction_ratio"))
            min_amount = max(0.0, configured_amount if configured_amount is not None else 1_000_000.0)
            mad_multiplier = max(0.0, configured_mad if configured_mad is not None else 3.0)
            min_z_score = max(0.0, configured_z_score if configured_z_score is not None else 2.5)
            minimum_direction = min(1.0, max(0.0, configured_direction if configured_direction is not None else 0.65))
            threshold = max(min_amount, median + mad_multiplier * 1.4826 * mad)
            return largest >= threshold and z_score >= min_z_score and direction_ratio >= minimum_direction
        dispersion = max(1.0, 1.4826 * mad)
        largest = max(amounts)
        z_score = (largest - median) / dispersion
        return {
            "large_buy": total > 0 and is_large(buy_ratio),
            "large_sell": total > 0 and is_large(sell_ratio),
            "buy_ratio": buy_ratio,
            "sell_ratio": sell_ratio,
            "samples": len(values),
            "summary": f"最近 {window_seconds} 秒方向占比 买{buy_ratio:.0%}/卖{sell_ratio:.0%}，z={z_score:.2f}",
        }

    def _check_quote_staleness(self) -> None:
        now = cn_now().replace(tzinfo=None)
        if not _is_continuous_trading(now):
            return
        portfolio = self.store.load()
        if not portfolio["positions"]:
            return
        config = self._rule_config(portfolio, "__portfolio__", "quote_interruption")
        threshold = float(config.get("threshold_seconds", 30))
        stale_symbols = []
        for position in portfolio["positions"]:
            symbol = str(position.get("symbol") or "").strip().upper()
            quote = self._latest_quotes.get(symbol) or {}
            age = self._quote_age_in_session(_timestamp(quote.get("timestamp")), now)
            if age > threshold:
                stale_symbols.append(symbol)
        stale = bool(stale_symbols)
        if stale:
            self._mark_quote_gap(
                f"持仓行情超过 {int(threshold)} 秒未更新，等待恢复后重新建立基线"
            )
        if self._set_rule(
            "__portfolio__", "quote_interruption", bool(config.get("enabled", True) and stale),
        ):
            self._emit(
                portfolio,
                None,
                "quote_interruption",
                "行情中断",
                "critical",
                0,
                _action_pct(config, 0),
                [f"{', '.join(stale_symbols)} 行情超过 {int(threshold)} 秒未更新；恢复后其余盘中规则重新建立基线"],
            )
            self._notify_updated()

    def _evaluate_account(self, portfolio: dict[str, Any]) -> None:
        account = portfolio["account"]
        total_asset = _finite(account.get("total_asset"))
        cash = _finite(account.get("cash"))
        if not total_asset or cash is None:
            return
        market_value = 0.0
        unrealized = 0.0
        for position in portfolio["positions"]:
            quote = self._latest_quotes.get(position["symbol"], {})
            price = _finite(quote.get("last_price"))
            quantity = _finite(position.get("quantity"))
            cost = _finite(position.get("cost_price"))
            if price is None or quantity is None:
                return
            market_value += price * quantity
            if cost is not None:
                unrealized += (price - cost) * quantity
        current_equity = cash + market_value
        account_runtime = self.store.get_runtime("account", {}) or {}
        high = max(
            current_equity,
            _finite(account_runtime.get("high_watermark"))
            or _finite(account.get("high_watermark"))
            or current_equity,
        )
        account_runtime["high_watermark"] = high
        account_runtime["last_equity"] = current_equity
        self.store.set_runtime("account", account_runtime)
        previous = _finite(account.get("previous_close_total_asset"))
        checks = [
            ("daily_equity_loss", previous, lambda config: current_equity / previous - 1 <= -abs(_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.03), "当日权益亏损"),
            ("equity_drawdown", high, lambda config: current_equity / high - 1 <= -abs(_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.08), "账户从导入后高点回撤"),
            ("unrealized_loss", total_asset, lambda config: unrealized / total_asset <= -abs(_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.08), "持仓总浮亏超过权益"),
            ("total_exposure", current_equity, lambda config: market_value / current_equity > (_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.95), "总仓位超过"),
        ]
        for rule_id, denominator, condition, reason_label in checks:
            config = self._rule_config(portfolio, "__portfolio__", rule_id)
            active = bool(denominator and condition(config))
            threshold = _finite(config.get("threshold"))
            threshold = threshold if threshold is not None else (0.95 if rule_id == "total_exposure" else 0.08)
            reason = f"{reason_label} {threshold:.0%}"
            if self._set_rule(
                "__portfolio__", rule_id, bool(config.get("enabled", True) and active),
            ):
                action = _action_pct(config, 25 if rule_id == "total_exposure" else 50)
                self._emit(portfolio, None, rule_id, reason, "critical" if rule_id != "total_exposure" else "warn", 75, action, [reason])
        for position in portfolio["positions"]:
            quote = self._latest_quotes.get(position["symbol"], {})
            price = _finite(quote.get("last_price"))
            quantity = _finite(position.get("quantity"))
            weight = price * quantity / current_equity if price and quantity else 0.0
            config = self._rule_config(portfolio, position["symbol"], "symbol_concentration")
            threshold = _finite(config.get("threshold"))
            threshold = threshold if threshold is not None else 0.30
            target_pct = _finite(config.get("target_pct"))
            target_pct = target_pct if target_pct is not None else threshold * 100
            target_weight = max(0.0, min(1.0, target_pct / 100))
            concentration = weight > threshold
            if self._set_rule(
                position["symbol"],
                "symbol_concentration",
                bool(config.get("enabled", True) and concentration),
            ):
                reduction = _action_pct(config, max(1, round((weight - target_weight) / weight * 100)))
                self._emit(
                    portfolio,
                    position,
                    "symbol_concentration",
                    f"单票超过权益 {threshold:.0%}",
                    "warn",
                    55,
                    reduction,
                    [f"当前单票仓位 {weight:.2%}，建议降至 {target_pct:.0f}%"],
                )
        cluster_config = self._rule_config(portfolio, "__portfolio__", "clustered_severe_events")
        cluster_window = _finite(cluster_config.get("window_seconds"))
        cluster_window = cluster_window if cluster_window is not None else 300
        cluster_count = _finite(cluster_config.get("count"))
        cluster_count = cluster_count if cluster_count is not None else 3
        cutoff = time.time() - cluster_window
        while self._severe_events and self._severe_events[0] < cutoff:
            self._severe_events.popleft()
        config = cluster_config
        clustered = bool(config.get("enabled", True) and len(self._severe_events) >= cluster_count)
        if self._set_rule("__portfolio__", "clustered_severe_events", clustered):
            self._emit(
                portfolio,
                None,
                "clustered_severe_events",
                "严重事件聚集",
                "critical",
                80,
                _action_pct(config, 50),
                [f"{int(cluster_window / 60)} 分钟内出现至少 {int(cluster_count)} 个严重风险事件"],
            )

    def _emit(
        self,
        portfolio: dict[str, Any],
        position: dict[str, Any] | None,
        rule_id: str,
        label: str,
        severity: str,
        score: int,
        reduction_pct: int,
        reasons: list[str],
        *,
        source_ids: list[str] | None = None,
    ) -> None:
        symbol = position.get("symbol") if position else None
        config_symbol = symbol or "__portfolio__"
        if rule_id.startswith("signal:"):
            signal_id = rule_id.removeprefix("signal:")
            signal_group = "custom" if signal_id.startswith("csg_") else "builtin"
            notify = self._signal_config(portfolio, config_symbol, signal_group, signal_id).get("notify", False)
        elif rule_id.startswith("monitor:"):
            notify = self._signal_config(
                portfolio, config_symbol, "monitor_rules", rule_id.removeprefix("monitor:"),
            ).get("notify", False)
        else:
            notify = self._rule_config(portfolio, config_symbol, rule_id).get("notify", False)
        name = position.get("name") if position else "组合"
        fingerprint_raw = f"{cn_today()}:{symbol or '__portfolio__'}:{rule_id}"
        fingerprint = hashlib.sha256(fingerprint_raw.encode()).hexdigest()
        event = {
            "ts": int(time.time() * 1000),
            "fingerprint": fingerprint,
            "source": "position_risk",
            "type": rule_id,
            "rule_id": rule_id,
            "rule_name": label,
            "symbol": symbol or "",
            "name": name,
            "message": f"{name}：{label}",
            "severity": severity,
            "risk_score": min(100, max(0, int(score))),
            "suggestion_pct": reduction_pct,
            "reasons": reasons,
        }
        if severity == "critical" and rule_id != "clustered_severe_events":
            self._severe_events.append(time.time())
        alert_store.append(self.store.root.parents[1], event)
        if notify is True:
            publish = getattr(self.quote_service, "publish_external_alerts", None)
            if callable(publish):
                publish([event])
            else:
                self.quote_service.push_alerts([event])
        if reduction_pct > 0:
            self._create_recommendation(
                portfolio,
                symbol=symbol,
                rule_id=rule_id,
                severity=severity,
                risk_score=int(score),
                reduction_pct=reduction_pct,
                reasons=reasons,
                source_ids=source_ids or [rule_id],
                fingerprint=fingerprint,
            )

    def _create_recommendation(
        self,
        portfolio: dict[str, Any],
        *,
        symbol: str | None,
        rule_id: str,
        severity: str,
        risk_score: int,
        reduction_pct: int,
        reasons: list[str],
        source_ids: list[str],
        fingerprint: str,
    ) -> dict[str, Any]:
        return self.store.add_recommendation({
            "fingerprint": fingerprint,
            "symbol": symbol,
            "scope": "symbol" if symbol else "portfolio",
            "rule_id": rule_id,
            "severity": severity,
            "risk_score": risk_score,
            "action": "清仓建议" if reduction_pct >= 100 else "减仓建议",
            "reduction_pct": reduction_pct,
            "reasons": reasons,
            "source_ids": source_ids,
            "portfolio_revision": portfolio["revision"],
        })

    def _notify_updated(self) -> None:
        notify = getattr(self.quote_service, "notify_position_risk_updated", None)
        if callable(notify):
            notify()

    def preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.store.load()
        issues: list[dict[str, Any]] = []
        account = dict(payload.get("account") or {})
        for field, label in (
            ("cash", "可用资金"),
            ("total_asset", "总资产"),
            ("previous_close_total_asset", "上日收盘总资产"),
        ):
            if _finite(account.get(field)) is None:
                issues.append({"level": "error", "field": field, "message": f"请补齐{label}"})
        seen: set[str] = set()
        normalized: list[dict[str, Any]] = []
        names = self.repo.get_name_map([str(item.get("symbol") or "") for item in payload.get("positions", [])])
        for index, raw in enumerate(payload.get("positions", [])):
            symbol = str(raw.get("symbol") or "").strip().upper()
            quantity = _finite(raw.get("quantity"))
            available = _finite(raw.get("available"))
            cost = _finite(raw.get("cost_price"))
            if not symbol or symbol in seen:
                issues.append({"level": "error", "row": index, "message": "持仓代码为空或重复"})
                continue
            if raw.get("requires_review") is True:
                issues.append({"level": "error", "row": index, "message": "低置信度字段需要人工校正"})
            seen.add(symbol)
            try:
                asset_type = self.repo.resolve_asset_type(symbol)
            except Exception:  # noqa: BLE001
                asset_type = "unknown"
            if asset_type not in {"stock", "etf"} or symbol not in names:
                issues.append({"level": "error", "row": index, "message": "代码未匹配本地股票/ETF主数据"})
            if quantity is None or quantity < 0 or cost is None or cost <= 0:
                issues.append({"level": "error", "row": index, "message": "数量和成本价必须有效"})
                continue
            if available is None:
                available = quantity
            if available < 0 or available > quantity:
                issues.append({"level": "error", "row": index, "message": "可用数量不能超过持仓数量"})
            master_name = names.get(symbol)
            if raw.get("name") and master_name and str(raw["name"]) not in master_name and master_name not in str(raw["name"]):
                issues.append({"level": "error", "row": index, "message": "代码和证券名称冲突"})
            price, source = self._baseline_price(symbol, raw)
            if price is None:
                issues.append({"level": "error", "row": index, "message": "缺少截图现价、实时价和本地原始收盘价"})
            normalized.append({
                "symbol": symbol,
                "name": master_name or raw.get("name") or symbol,
                "asset_type": asset_type,
                "quantity": int(quantity),
                "available": int(available),
                "cost_price": round(cost, 4),
                "import_price": price,
                "price_source": source,
            })
        cash = _finite(account.get("cash")) or 0.0
        holding_value = sum(float(item["quantity"]) * float(item["import_price"] or 0) for item in normalized)
        computed_total = cash + holding_value
        total_asset = _finite(account.get("total_asset"))
        difference = computed_total - total_asset if total_asset is not None else None
        difference_pct = difference / total_asset if total_asset and difference is not None else None
        if difference_pct is not None and difference_pct < -0.01:
            issues.append({
                "level": "error", "field": "total_asset",
                "message": f"现金与持仓市值合计比总资产少 {abs(difference_pct):.2%}，超过 1%",
            })
        old = {item["symbol"]: item for item in current["positions"]}
        new = {item["symbol"]: item for item in normalized}
        changed = [symbol for symbol in old.keys() & new.keys() if any(
            old[symbol].get(field) != new[symbol].get(field) for field in ("quantity", "available", "cost_price")
        )]
        return {
            "revision": current["revision"],
            "account": account,
            "positions": normalized,
            "reconciliation": {
                "cash": cash,
                "holding_value": round(holding_value, 2),
                "computed_total": round(computed_total, 2),
                "reported_total": total_asset,
                "difference": round(difference, 2) if difference is not None else None,
                "difference_pct": difference_pct,
            },
            "replacement": {
                "added": sorted(new.keys() - old.keys()),
                "removed": sorted(old.keys() - new.keys()),
                "changed": sorted(changed),
                "unchanged": sorted((old.keys() & new.keys()) - set(changed)),
            },
            "issues": issues,
            "can_confirm": not any(item["level"] == "error" for item in issues),
        }

    def _baseline_price(self, symbol: str, raw: dict[str, Any]) -> tuple[float | None, str | None]:
        screenshot = _finite(raw.get("current_price", raw.get("import_price")))
        if screenshot and screenshot > 0:
            return screenshot, "screenshot"
        fresh = self.quote_service.get_fresh_quotes({symbol})
        quote = (fresh.get("quotes") or {}).get(symbol)
        live = _finite((quote or {}).get("last_price"))
        if live and live > 0:
            return live, "realtime"
        history = self._history.get(symbol) or {}
        close = _finite(history.get("raw_close"))
        return (close, "local_raw_close") if close and close > 0 else (None, None)

    def replace_portfolio(self, payload: dict[str, Any], revision: int) -> dict[str, Any]:
        preview = self.preview(payload)
        if int(revision) != int(preview["revision"]):
            from app.services.position_risk_store import RevisionConflict

            raise RevisionConflict("配置已更新，请刷新后重新预览")
        if not preview["can_confirm"]:
            raise ValueError("资产核对未通过，不能确认导入")
        current = self.store.load()
        account = dict(preview["account"])
        account["high_watermark"] = float(account["total_asset"])
        value = {
            **current,
            "account": account,
            "positions": preview["positions"],
            "imported_at": cn_now().isoformat(),
        }
        saved = self.store.replace(value, revision)
        self.store.stale_pending()
        self.store.set_runtime("account", {
            "high_watermark": account["high_watermark"],
            "last_equity": account["total_asset"],
            "reset_at": saved["imported_at"],
        })
        for position in saved["positions"]:
            self.store.set_runtime(f"position:{position['symbol']}", {
                "high_price": position.get("import_price"),
                "last_price": position.get("import_price"),
                "reset_at": saved["imported_at"],
            })
        self.refresh_subscription()
        return saved

    def view(self) -> dict[str, Any]:
        portfolio = self.store.load()
        rows = []
        total_asset = _finite(portfolio["account"].get("total_asset"))
        evidence_fields = ("cost", "history", "quote", "depth", "flow")
        for position in portfolio["positions"]:
            symbol = position["symbol"]
            quote = self._latest_quotes.get(symbol) or {}
            history = self._history.get(symbol) or {}
            price = _finite(quote.get("last_price")) or _finite(position.get("import_price"))
            quantity = float(position.get("quantity") or 0)
            cost = _finite(position.get("cost_price"))
            market_value = price * quantity if price is not None else None
            pnl = (price - cost) * quantity if price is not None and cost is not None else None
            evidence = {
                "cost": cost is not None,
                "history": bool(history),
                "quote": bool(quote),
                "depth": bool(self._depth.get(symbol)),
                "flow": len(self._flow.get(symbol, ())) >= 7,
            }
            rows.append({
                **position,
                "price": price,
                "market_value": market_value,
                "profit_loss": pnl,
                "profit_loss_pct": (price / cost - 1) if price is not None and cost else None,
                "weight": market_value / total_asset if market_value is not None and total_asset else None,
                "ma5": history.get("ma5"),
                "ma10": history.get("ma10"),
                "ma20": history.get("ma20"),
                "latest_signal": next((key for key, value in history.items() if key.startswith("signal_") and value is True), None),
                "evidence": evidence,
                "evidence_coverage": sum(bool(evidence[field]) for field in evidence_fields) / len(evidence_fields),
                "data_status": "ready" if evidence["quote"] and evidence["history"] else "insufficient",
            })
        pending = [self._localize_recommendation(item) for item in self.store.list_recommendations("pending")]
        by_symbol = {item["symbol"]: item for item in pending if item.get("symbol")}
        for row in rows:
            suggestion = by_symbol.get(row["symbol"])
            row["suggestion"] = suggestion
            row["risk_score"] = suggestion["risk_score"] if suggestion else 0
            row["risk_level"] = "high" if row["risk_score"] >= 70 else "medium" if row["risk_score"] >= 40 else "low"
        return {
            **portfolio,
            "positions": rows,
            "runtime": {
                "status": self._runtime_status,
                "reason": self._runtime_reason,
                "last_processed_at": self._last_processed_at,
                "pending_count": len(pending),
            },
        }

    def _localize_recommendation(self, item: dict[str, Any]) -> dict[str, Any]:
        localized = dict(item)
        localized["reasons"] = [
            localize_position_risk_text(str(reason), self._custom_signal_labels)
            for reason in item.get("reasons", [])
        ]
        return localized
