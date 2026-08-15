"""持仓风控实时编排、证据计算和建议生成。"""
from __future__ import annotations

import hashlib
import logging
import math
import queue
import re
import statistics
import threading
import time
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta, time as clock_time
from typing import Any

import polars as pl

from app.indicators.pipeline import ENRICHED_COLUMNS
from app.market_time import cn_now
from app.services import alert_store
from app.services.position_risk_store import PositionRiskStore
from app.strategy.intraday_signals import INTRADAY_SIGNAL_LABELS

_ACCOUNT_ID = "position-risk"
_EVENT_COOLDOWN_SECONDS = 300
_STOP_LOSS_RECOVERY_BUFFER = 0.005
_QUOTE_INTERRUPTION_RECOVERY_SECONDS = 60
_FUND_EVIDENCE_RULES = {
    "large_buy",
    "large_sell",
    "continuous_outflow",
    "orderbook_imbalance",
}
_NO_COOLDOWN_RULES = {
    "stop_loss",
    "broken_limit_up",
    "limit_down",
}
_BUILTIN_SIGNAL_DIRECTIONS = {
    "signal_ma_golden_5_20": "entry",
    "signal_ma_dead_5_20": "exit",
    "signal_ma_golden_20_60": "entry",
    "signal_macd_golden": "entry",
    "signal_macd_dead": "exit",
    "signal_ma20_breakout": "entry",
    "signal_ma20_breakdown": "exit",
    "signal_ma5_breakout": "entry",
    "signal_ma5_breakdown": "exit",
    "signal_ma10_breakout": "entry",
    "signal_ma10_breakdown": "exit",
    "signal_n_day_high": "entry",
    "signal_n_day_low": "exit",
    "signal_boll_breakout_upper": "entry",
    "signal_boll_breakdown_lower": "exit",
    "signal_volume_surge": "both",
    "signal_limit_up": "entry",
    "signal_limit_down": "exit",
    "signal_limit_down_recovery": "entry",
    "signal_broken_limit_up": "exit",
    "signal_intraday_avg_cross_up": "entry",
    "signal_intraday_avg_cross_down": "exit",
    "signal_intraday_zero_cross_up": "entry",
    "signal_intraday_zero_cross_down": "exit",
}
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
logger = logging.getLogger(__name__)


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


def _trade_pct(config: dict[str, Any], key: str, default: int) -> int:
    value = _finite(config.get(key))
    if value is None:
        value = default
    return max(0, min(100, int(round(value))))


def _advanced_rule_enabled(config: dict[str, Any]) -> bool:
    """新短线规则以 active 作为显式确认开关，旧配置合并后默认保持关闭。"""
    return config.get("enabled", True) is not False and config.get("active", False) is True


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
    """生成风险提醒；QMT同步可替换本地快照，建议确认仍不会直接交易。"""

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
        self._flow: dict[str, deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=1800))
        self._rule_states: dict[str, dict[str, Any]] = self.store.get_runtime("rule_states", {}) or {}
        self._runtime_status = "idle"
        self._runtime_reason = "尚未导入持仓"
        self._polling_lease = False
        self._last_processed_at: str | None = None
        self._quote_gap_symbols: set[str] = set()
        self._recovery_pending_symbols: set[str] = set()
        self._flow_anchor_pending: set[str] = set()
        self._asset_types: dict[str, str] = {}
        self._history_pending_symbols: set[str] = set()
        self._history_as_of: dict[str, str] = {}
        self._next_history_refresh = 0.0
        self._custom_signal_directions: dict[str, str] = {}
        self._custom_signal_labels: dict[str, str] = {}
        self._recent_exit_signals: dict[str, deque[tuple[float, str]]] = defaultdict(
            lambda: deque(maxlen=20),
        )
        recent_exit_signals = self.store.get_runtime("recent_exit_signals", {}) or {}
        for symbol, values in recent_exit_signals.items():
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, (list, tuple)) or len(item) != 2:
                    continue
                timestamp = _finite(item[0])
                if timestamp is not None:
                    self._recent_exit_signals[str(symbol)].append((timestamp, str(item[1])))
        severe_events = self.store.get_runtime("severe_events", []) or []
        self._severe_events: deque[float] = deque(
            (_finite(value) for value in severe_events if _finite(value) is not None),
            maxlen=20,
        )
        self._severe_event_date = str(self.store.get_runtime("severe_event_date", "") or "")
        self._severe_event_fingerprints = {
            str(value)
            for value in (self.store.get_runtime("severe_event_fingerprints", []) or [])
            if value
        }
        self._quote_interruption_recovery_started_at = _finite(
            self.store.get_runtime("quote_interruption_recovery_started_at"),
        )
        if not self.store.get_runtime("risk_event_noise_guard_v1", False):
            self._severe_events.clear()
            self._severe_event_date = ""
            self._severe_event_fingerprints.clear()
            self.store.set_runtime("severe_events", [])
            self.store.set_runtime("severe_event_date", "")
            self.store.set_runtime("severe_event_fingerprints", [])
            self.store.set_runtime("risk_event_noise_guard_v1", True)
        if not self.store.get_runtime("fund_pressure_noise_v1", False):
            self.store.stale_pending_rules(_FUND_EVIDENCE_RULES)
            self._rule_states = {
                key: value for key, value in self._rule_states.items()
                if key.rsplit(":", 1)[-1] not in _FUND_EVIDENCE_RULES
            }
            self.store.set_runtime("rule_states", self._rule_states)
            self.store.set_runtime("fund_pressure_noise_v1", True)

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

    def _mark_quote_gap(self, reason: str, symbols: set[str] | None = None) -> None:
        """标记行情连续性已丢失，恢复后必须重新建立规则基线。"""
        portfolio = self.store.load()
        portfolio_symbols = {
            str(item.get("symbol") or "").strip().upper()
            for item in portfolio["positions"]
        }
        affected = {
            str(symbol).strip().upper() for symbol in (symbols or portfolio_symbols)
            if str(symbol).strip().upper() in portfolio_symbols
        }
        if not affected:
            return
        self._quote_gap_symbols.update(affected)
        self._recovery_pending_symbols.update(affected)
        self._flow_anchor_pending.update(affected)
        mark_intraday_gap = getattr(self.quote_service, "mark_intraday_gap", None)
        if callable(mark_intraday_gap):
            mark_intraday_gap(affected)
        for symbol in affected:
            self._flow[symbol].clear()
            self._depth[symbol].clear()
            self.store.set_runtime(f"depth:{symbol}", {})
        if self._runtime_status != "idle":
            self._runtime_status = "reconnecting"
            self._runtime_reason = reason

    def _mark_quote_recovered(self, symbols: set[str]) -> None:
        recovered = self._quote_gap_symbols & symbols
        if not recovered:
            return
        self._recovery_pending_symbols.update(recovered)
        self._quote_gap_symbols.difference_update(recovered)
        if self._quote_gap_symbols:
            self._runtime_status = "reconnecting"
            self._runtime_reason = (
                f"仍有 {len(self._quote_gap_symbols)} 只持仓行情中断，等待全部恢复"
            )
        else:
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
            remove_intraday(f"{_ACCOUNT_ID}:stock")
            remove_intraday(f"{_ACCOUNT_ID}:etf")
        if self._polling_lease:
            self.quote_service.release_temporary_polling()
            self._polling_lease = False

    def refresh_subscription(self) -> None:
        self._detach_market()
        portfolio = self.store.load()
        symbols = {str(item.get("symbol") or "").strip().upper() for item in portfolio["positions"]}
        symbols.discard("")
        self._asset_types = {}
        self._preload_history(symbols)
        if not symbols:
            self._runtime_status = "idle"
            self._runtime_reason = "尚未导入持仓"
            self._notify_updated()
            return
        self._mark_quote_gap("正在建立持仓行情连续性基线", symbols)

        set_intraday = getattr(self.quote_service, "set_intraday_consumer", None)
        if callable(set_intraday):
            for asset_type in ("stock", "etf"):
                asset_symbols = {
                    symbol for symbol in symbols
                    if self._asset_types.get(symbol, "stock") == asset_type
                }
                set_intraday(f"{_ACCOUNT_ID}:{asset_type}", asset_symbols, asset_type)

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
                asset_types = {self._asset_types.get(symbol, "stock") for symbol in symbols}
                ws_asset_type = next(iter(asset_types)) if len(asset_types) == 1 else "mixed"
                hub.register(_ACCOUNT_ID, "websocket", symbols, ws_asset_type, self._queue)
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
        history_as_of: dict[str, str] = {}
        stock_symbols: list[str] = []
        etf_symbols: list[str] = []
        for symbol in symbols:
            try:
                asset_type = self.repo.resolve_asset_type(symbol)
            except Exception:  # noqa: BLE001
                asset_type = "stock"
            self._asset_types[symbol] = asset_type
            (etf_symbols if asset_type == "etf" else stock_symbols).append(symbol)
        try:
            frame, as_of = self.repo.get_enriched_latest()
            stock_as_of = str(as_of or "")
            if stock_symbols and not frame.is_empty():
                rows.update({
                    str(row["symbol"]): {
                        **row,
                        "_position_risk_as_of": str(row.get("date") or as_of or ""),
                    }
                    for row in frame.filter(frame["symbol"].is_in(stock_symbols)).to_dicts()
                })
                history_as_of.update({symbol: stock_as_of for symbol in stock_symbols if symbol in rows})
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控股票历史预载失败", exc_info=True)
        try:
            frame, as_of = self.repo.get_enriched_latest_asset("etf")
            etf_as_of = str(as_of or "")
            if etf_symbols and not frame.is_empty():
                rows.update({
                    str(row["symbol"]): {
                        **row,
                        "_position_risk_as_of": str(row.get("date") or as_of or ""),
                    }
                    for row in frame.filter(frame["symbol"].is_in(etf_symbols)).to_dicts()
                })
                history_as_of.update({symbol: etf_as_of for symbol in etf_symbols if symbol in rows})
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控 ETF 历史预载失败", exc_info=True)
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
            self._history_pending_symbols = set(symbols) - set(rows)
            self._history_as_of = history_as_of

    def _preload_history_if_missing(self) -> None:
        if not self._asset_types or time.monotonic() < self._next_history_refresh:
            return
        self._next_history_refresh = time.monotonic() + 5
        symbols = set(self._asset_types)
        needs_refresh = bool(self._history_pending_symbols)
        for asset_type in {self._asset_types.get(symbol, "stock") for symbol in symbols}:
            try:
                if asset_type == "stock":
                    _, as_of = self.repo.get_enriched_latest()
                else:
                    _, as_of = self.repo.get_enriched_latest_asset(asset_type, refresh=False)
            except Exception:  # noqa: BLE001
                continue
            current_as_of = str(as_of or "")
            needs_refresh = needs_refresh or any(
                self._asset_types.get(symbol) == asset_type
                and self._history_as_of.get(symbol, "") != current_as_of
                for symbol in symbols
            )
        if needs_refresh:
            self._preload_history(symbols)

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                message = self._queue.get(timeout=1)
            except queue.Empty:
                self._check_quote_staleness()
                self._preload_history_if_missing()
                continue
            if message.get("type") == "stop":
                return
            if message.get("type") in {"gap", "recovery"}:
                if message.get("type") == "gap":
                    affected = {
                        str(symbol).strip().upper()
                        for symbol in message.get("symbols", [])
                        if str(symbol).strip()
                    }
                    self._mark_quote_gap(
                        str(message.get("reason") or "WS 连接中断"),
                        affected or None,
                    )
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

    def _set_rule(
        self,
        symbol: str,
        rule_id: str,
        active: bool,
        now: datetime | None = None,
        event_token: str | None = None,
        cooldown_seconds: int | None = None,
    ) -> bool:
        key = f"{symbol}:{rule_id}"
        current = self._rule_states.get(key) or {}
        was_active = bool(current.get("active"))
        same_event = bool(event_token and current.get("last_event_token") == event_token)
        should_emit = active and not same_event and (not was_active or event_token is not None)
        changed = active != was_active
        if not changed and not should_emit:
            return False
        event_time = (now or cn_now()).replace(tzinfo=None).timestamp()
        next_state = {**current, "active": active, "changed_at": event_time}
        if should_emit and rule_id not in _NO_COOLDOWN_RULES and not rule_id.startswith("signal:"):
            last_emitted = _finite(current.get("last_emitted_at"))
            cooldown = _EVENT_COOLDOWN_SECONDS if cooldown_seconds is None else max(0, cooldown_seconds)
            if last_emitted is not None and event_time - last_emitted < cooldown:
                should_emit = False
        if should_emit:
            next_state["last_emitted_at"] = event_time
            if event_token:
                next_state["last_event_token"] = event_token
        self._rule_states[key] = next_state
        self.store.set_runtime("rule_states", self._rule_states)
        return should_emit

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
        self._preload_history_if_missing()
        intraday_signals = self._intraday_signals(
            {item["symbol"] for item in portfolio["positions"]}, current_time,
        )
        intraday_features = self._intraday_features(
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
                intraday_features.get(symbol, {}),
            )
        self._evaluate_account(portfolio, current_time)

    def _intraday_signals(
        self,
        symbols: set[str],
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        getter = getattr(self.quote_service, "get_intraday_signals", None)
        if not callable(getter):
            return {}
        result: dict[str, dict[str, Any]] = {}
        for asset_type in ("stock", "etf"):
            asset_symbols = {
                symbol for symbol in symbols
                if self._asset_types.get(symbol, "stock") == asset_type
            }
            if not asset_symbols:
                continue
            prev_close = {
                symbol: raw_close
                for symbol in asset_symbols
                if (raw_close := _finite((self._history.get(symbol) or {}).get("raw_close")))
            }
            result.update(getter(
                asset_symbols,
                prev_close=prev_close,
                asset_type=asset_type,
                now=now,
                consumer_id=f"{_ACCOUNT_ID}:{asset_type}",
            ))
        return result

    def _intraday_features(
        self,
        symbols: set[str],
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        getter = getattr(self.quote_service, "get_intraday_features", None)
        if not callable(getter):
            return {}
        result: dict[str, dict[str, Any]] = {}
        for asset_type in ("stock", "etf"):
            asset_symbols = {
                symbol for symbol in symbols
                if self._asset_types.get(symbol, "stock") == asset_type
            }
            if not asset_symbols:
                continue
            try:
                result.update(getter(asset_symbols, asset_type=asset_type, now=now))
            except (TypeError, ValueError, RuntimeError):
                logger.warning("持仓风控分时特征获取失败", exc_info=True)
            for symbol in asset_symbols:
                feature = result.get(symbol)
                if feature is None:
                    continue
                flow = self._flow_state(symbol, now)
                feature.update({
                    "buy_ratio": flow.get("buy_ratio"),
                    "sell_ratio": flow.get("sell_ratio"),
                    "flow_samples": flow.get("samples", 0),
                })
                snapshots = list(self._depth.get(symbol, ()))
                if snapshots:
                    latest = snapshots[-1]
                    bids = latest.get("bid_volumes") or [
                        _finite(latest.get("bid1_volume", latest.get("bid_volume1"))) or 0.0
                    ]
                    asks = latest.get("ask_volumes") or [
                        _finite(latest.get("ask1_volume", latest.get("ask_volume1"))) or 0.0
                    ]
                    bid_total = sum(_finite(value) or 0 for value in bids)
                    ask_total = sum(_finite(value) or 0 for value in asks)
                    feature["orderbook_imbalance"] = (
                        (bid_total - ask_total) / (bid_total + ask_total)
                        if bid_total + ask_total > 0 else None
                    )
        return result

    def feature_snapshot(
        self,
        symbols: set[str] | None = None,
        now: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """返回 API 使用的特征快照，并附带持仓状态和实际保护价。"""
        portfolio = self.store.load()
        selected = symbols or {
            str(item.get("symbol") or "").strip().upper()
            for item in portfolio.get("positions", [])
        }
        selected = {symbol for symbol in selected if symbol}
        self._preload_history_if_missing()
        current_time = (now or cn_now()).replace(tzinfo=None)
        features = self._intraday_features(selected, current_time)
        result: dict[str, dict[str, Any]] = {}
        positions = {str(item.get("symbol")): item for item in portfolio.get("positions", [])}
        for symbol in selected:
            position = positions.get(symbol) or {}
            runtime = self.store.get_runtime(f"position:{symbol}", {}) or {}
            cost = _finite(position.get("cost_price"))
            stop_cfg = self._rule_config(portfolio, symbol, "stop_loss")
            threshold = _finite(stop_cfg.get("threshold"))
            hard_stop = cost * (1 + threshold) if cost is not None and threshold is not None else None
            item = dict(features.get(symbol) or {
                "symbol": symbol, "available": False, "fresh": False,
                "reason": "分时特征不可用", "source": None, "as_of": None,
            })
            item.update({
                "stage": runtime.get("stage", "initial"),
                "r_multiple": runtime.get("r_multiple"),
                "effective_stop_price": runtime.get("effective_stop_price") or hard_stop,
                "hard_stop_price": hard_stop,
                "feature_snapshot_at": runtime.get("feature_snapshot_at") or item.get("as_of"),
                "position_started_at": runtime.get("position_started_at"),
                "t_trade_count": int((runtime.get("t_trade") or {}).get("count") or 0),
                "t_trade_date": (runtime.get("t_trade") or {}).get("date"),
            })
            result[symbol] = item
        return result

    def _evaluate_position(
        self,
        portfolio: dict[str, Any],
        position: dict[str, Any],
        quote: dict[str, Any],
        now: datetime,
        intraday_signals: dict[str, Any] | None = None,
        intraday_features: dict[str, Any] | None = None,
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
            if not self._quote_gap_symbols and not self._recovery_pending_symbols:
                self._runtime_status = "websocket"
                self._runtime_reason = "持仓池行情连续性已恢复"
        high = max(price, _finite(runtime.get("high_price")) or price)
        runtime["high_price"] = high
        runtime["last_price"] = price
        runtime["last_quote_at"] = quote.get("timestamp")
        if cost is not None:
            previous_cost = _finite(runtime.get("position_cost"))
            if previous_cost is None or abs(previous_cost - cost) > 1e-9:
                runtime.update({
                    "position_cost": cost,
                    "position_started_at": now.timestamp(),
                    "stage": "initial",
                    "triggered_stages": [],
                    "initial_stop_price": None,
                    "initial_r": None,
                    "effective_stop_price": None,
                })

        stop_cfg = self._rule_config(portfolio, symbol, "stop_loss")
        stop_threshold = _finite(stop_cfg.get("threshold"))
        stop_threshold = stop_threshold if stop_threshold is not None else -0.10
        stop_action = _action_pct(stop_cfg, 100)
        stop_return = price / cost - 1 if cost else None
        stop_was_active = bool(
            (self._rule_states.get(f"{symbol}:stop_loss") or {}).get("active"),
        )
        stop_limit = stop_threshold + (_STOP_LOSS_RECOVERY_BUFFER if stop_was_active else 0)
        stop_active = bool(
            stop_cfg.get("enabled", True)
            and stop_return is not None
            and stop_return <= stop_limit
        )
        if self._set_rule(symbol, "stop_loss", stop_active, now):
            self._emit(portfolio, position, "stop_loss", "成本止损", "critical", 85, stop_action, [f"现价较成本亏损 {stop_return * 100:.2f}%"])

        initial_stop_price = cost * (1 + stop_threshold) if cost and stop_threshold < 0 else None
        if initial_stop_price and _finite(runtime.get("initial_stop_price")) is None:
            runtime["initial_stop_price"] = initial_stop_price
            runtime["initial_r"] = max(cost - initial_stop_price, cost * 0.0001)
            runtime["effective_stop_price"] = initial_stop_price
        initial_r = _finite(runtime.get("initial_r"))
        r_multiple = (price - cost) / initial_r if cost and initial_r and initial_r > 0 else None
        if r_multiple is not None:
            runtime["r_multiple"] = r_multiple

        features = intraday_features or {}
        features_available = bool(features.get("available") and features.get("fresh"))
        if features.get("as_of"):
            runtime["feature_snapshot_at"] = features.get("as_of")

        structure_cfg = self._rule_config(portfolio, symbol, "structure_stop")
        if _advanced_rule_enabled(structure_cfg) and features_available:
            reference_name = str(structure_cfg.get("reference") or "vwap")
            reference_map = {
                "opening_range_low": features.get("opening_range_low"),
                "five_minute_low": features.get("five_minute_low"),
                "vwap": features.get("session_vwap"),
                "ema20": features.get("ema20_1m"),
            }
            reference = _finite(reference_map.get(reference_name))
            buffer = _finite(structure_cfg.get("buffer")) or 0.002
            confirm_bars = max(1, int(_finite(structure_cfg.get("confirm_bars")) or 2))
            closed_bars = features.get("closed_bars_5m") or []
            structure_broken = bool(
                reference and len(closed_bars) >= confirm_bars
                and all(
                    (_finite(item.get("close")) or price) <= reference * (1 - buffer)
                    for item in closed_bars[-confirm_bars:]
                )
            )
            if self._set_rule(symbol, "structure_stop", structure_broken, now):
                self._emit(
                    portfolio, position, "structure_stop", "分时结构止损", "critical", 75,
                    _action_pct(structure_cfg, 50),
                    [f"连续 {confirm_bars} 根闭合 5 分钟 K 跌破 {reference_name} {reference:.3f}"],
                    feature_snapshot_at=features.get("as_of"),
                    effective_stop_price=reference * (1 - buffer) if reference else None,
                )

        atr_cfg = self._rule_config(portfolio, symbol, "atr_protection")
        atr = _finite(features.get("atr14_5m")) if features_available else None
        atr_multiple = _finite(atr_cfg.get("atr_multiple")) or 2.0
        activation_gain = _finite(atr_cfg.get("activation_gain")) or 0.02
        atr_stop = high - atr * atr_multiple if atr and high > 0 else None
        if _advanced_rule_enabled(atr_cfg) and atr_stop and cost and stop_return is not None and stop_return >= activation_gain:
            effective_stop = max(_finite(runtime.get("effective_stop_price")) or 0, atr_stop)
            runtime["effective_stop_price"] = effective_stop
            atr_active = price <= effective_stop
            if self._set_rule(symbol, "atr_protection", atr_active, now):
                self._emit(
                    portfolio, position, "atr_protection", "ATR 移动保护", "warn", 65,
                    _action_pct(atr_cfg, 50),
                    [f"现价 {price:.3f} 低于动态保护价 {effective_stop:.3f}（5 分钟 ATR {atr:.3f}）"],
                    feature_snapshot_at=features.get("as_of"),
                    effective_stop_price=effective_stop,
                )

        time_cfg = self._rule_config(portfolio, symbol, "time_stop")
        started_at = _finite(runtime.get("position_started_at"))
        max_minutes = _finite(time_cfg.get("max_minutes")) or 120
        min_gain = _finite(time_cfg.get("min_gain")) or 0.0
        close_before = max(0.0, _finite(time_cfg.get("close_before_minutes")) or 15)
        close_cutoff = datetime.combine(now.date(), clock_time(15, 0)) - timedelta(minutes=close_before)
        near_close = close_cutoff <= now <= datetime.combine(now.date(), clock_time(15, 0))
        time_active = bool(
            _advanced_rule_enabled(time_cfg) and started_at is not None
            and (now.timestamp() - started_at >= max_minutes * 60 or near_close)
            and stop_return is not None and stop_return < min_gain
        )
        if _advanced_rule_enabled(time_cfg) and self._set_rule(symbol, "time_stop", time_active, now):
            time_reason = (
                f"距离收盘不足 {int(close_before)} 分钟且收益 {stop_return:.2%} 未达到 {min_gain:.2%}"
                if near_close and (now.timestamp() - (started_at or now.timestamp())) < max_minutes * 60
                else f"持仓超过 {int(max_minutes)} 分钟且收益 {stop_return:.2%} 未达到 {min_gain:.2%}"
            )
            self._emit(
                portfolio, position, "time_stop", "时间止损", "warn", 55,
                _action_pct(time_cfg, 25),
                [time_reason],
            )

        take_cfg = self._rule_config(portfolio, symbol, "take_profit")
        take_threshold = _finite(take_cfg.get("threshold"))
        take_threshold = take_threshold if take_threshold is not None else 0.10
        take_action = _action_pct(take_cfg, 100)
        take_active = bool(
            take_cfg.get("enabled", False)
            and stop_return is not None
            and stop_return >= take_threshold
        )
        if self._set_rule(symbol, "take_profit", take_active, now) and stop_return is not None:
            self._emit(
                portfolio,
                position,
                "take_profit",
                "固定止盈",
                "info",
                55,
                take_action,
                [f"现价较成本盈利 {stop_return * 100:.2f}%（目标 {take_threshold:.2%}）"],
            )

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
        if self._set_rule(symbol, "trailing_drawdown", trailing_active, now):
            self._emit(portfolio, position, "trailing_drawdown", "盈利回撤", "warn", 60, trailing_action, [f"从持仓高点回撤 {(1 - price / high) * 100:.2f}%"])

        ladder_cfg = self._rule_config(portfolio, symbol, "take_profit_ladder")
        if _advanced_rule_enabled(ladder_cfg) and r_multiple is not None:
            triggered = set(runtime.get("triggered_stages") or [])
            stage_specs = (
                ("tp_1", _finite(ladder_cfg.get("first_r")) or 1.0, _action_pct({"action_pct": ladder_cfg.get("first_action_pct")}, 30)),
                ("tp_2", _finite(ladder_cfg.get("second_r")) or 2.0, _action_pct({"action_pct": ladder_cfg.get("second_action_pct")}, 30)),
            )
            for stage, threshold_r, action_pct in stage_specs:
                if r_multiple < threshold_r or stage in triggered:
                    continue
                triggered.add(stage)
                runtime["stage"] = stage
                if stage == "tp_1":
                    fee_buffer = max(0.0, _finite(ladder_cfg.get("fees_buffer")) or 0.002)
                    runtime["effective_stop_price"] = max(
                        _finite(runtime.get("effective_stop_price")) or 0,
                        cost * (1 + fee_buffer) if cost else 0,
                    )
                else:
                    runtime["stage"] = "runner"
                    runner_atr = _finite(features.get("atr14_5m")) if features_available else None
                    runner_multiple = _finite(ladder_cfg.get("runner_atr_multiple")) or 2.0
                    if runner_atr:
                        runtime["effective_stop_price"] = max(
                            _finite(runtime.get("effective_stop_price")) or 0,
                            high - runner_atr * runner_multiple,
                        )
                if self._set_rule(
                    symbol, "take_profit_ladder", True, now,
                    event_token=f"stage:{stage}", cooldown_seconds=0,
                ):
                    self._emit(
                        portfolio, position, "take_profit_ladder", f"分批止盈 {stage}", "info", 58 if stage == "tp_1" else 68,
                        action_pct,
                        [f"收益达到 {r_multiple:.2f}R（阶段阈值 {threshold_r:.2f}R）"],
                        source_ids=[stage], stage=stage, r_multiple=r_multiple,
                        effective_stop_price=runtime.get("effective_stop_price"),
                        feature_snapshot_at=features.get("as_of"),
                    )
            runtime["triggered_stages"] = sorted(triggered)
            if runtime.get("stage") == "runner" and runtime.get("effective_stop_price"):
                runner_active = price <= float(runtime["effective_stop_price"])
                if self._set_rule(
                    symbol, "take_profit_runner", runner_active, now,
                    event_token="runner" if runner_active else None,
                    cooldown_seconds=0,
                ):
                    self._emit(
                        portfolio, position, "take_profit_runner", "剩余仓位移动保护", "warn", 65,
                        _action_pct({"action_pct": ladder_cfg.get("runner_pct")}, 40),
                        [f"剩余仓位触及移动保护价 {float(runtime['effective_stop_price']):.3f}"],
                        source_ids=["runner"], stage="runner", r_multiple=r_multiple,
                        effective_stop_price=runtime.get("effective_stop_price"),
                        feature_snapshot_at=features.get("as_of"),
                    )

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
            if self._set_rule(symbol, rule_id, active, now) and not ma_suppressed:
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
        if self._set_rule(symbol, "limit_down", at_limit_down, now):
            self._emit(portfolio, position, "limit_down", "跌停", "critical", 85, limit_down_action, ["原始现价触及当日跌停价"])

        depth_state = self._depth_state(symbol, quote, now)
        sealed_cfg = self._rule_config(portfolio, symbol, "resealed_limit_up")
        sealed_action = _action_pct(sealed_cfg, 0)
        if self._set_rule(
            symbol, "resealed_limit_up",
            bool(sealed_cfg.get("enabled", True) and depth_state["resealed"]),
            now,
        ):
            self._emit(
                portfolio, position, "resealed_limit_up", "涨停回封", "info", 35,
                sealed_action, ["炸板后连续 3 个五档快照确认重新封板"],
            )
        broken_cfg = self._rule_config(portfolio, symbol, "broken_limit_up")
        broken_action = _action_pct(broken_cfg, 50)
        if self._set_rule(
            symbol, "broken_limit_up", bool(broken_cfg.get("enabled", True) and depth_state["broken"]), now,
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
            depth_state["sealed"]
            and not depth_state["broken"]
            and shrink_80_cfg.get("enabled", True)
            and depth_state["shrink_ratio"] >= shrink_80_threshold
        )
        shrink_50 = bool(
            depth_state["sealed"]
            and not depth_state["broken"]
            and shrink_50_cfg.get("enabled", True)
            and shrink_50_threshold <= depth_state["shrink_ratio"] < shrink_80_threshold
        )
        if self._set_rule(symbol, "sealed_order_shrink_80", shrink_80, now):
            self._emit(
                portfolio, position, "sealed_order_shrink_80", f"封单减少 {shrink_80_threshold:.0%}", "critical",
                70, shrink_80_action, [f"买一封单较盘中峰值减少至少 {shrink_80_threshold:.0%}"],
            )
        if self._set_rule(symbol, "sealed_order_shrink_50", shrink_50, now):
            self._emit(
                portfolio, position, "sealed_order_shrink_50", f"封单减少 {shrink_50_threshold:.0%}", "warn",
                55, shrink_50_action, [f"买一封单较盘中峰值减少至少 {shrink_50_threshold:.0%}"],
            )
        snapshot_getter = getattr(self.quote_service, "get_intraday_snapshot", None)
        asset_type = str(position.get("asset_type") or self._asset_types.get(symbol) or "stock")
        snapshot = snapshot_getter(
            {symbol}, asset_type=asset_type, now=now,
        ) if callable(snapshot_getter) else {}
        vwap = (snapshot.get("vwap") or {}).get(symbol)

        imbalance_cfg = self._rule_config(portfolio, symbol, "orderbook_imbalance")
        imbalance_threshold = _finite(imbalance_cfg.get("threshold"))
        imbalance_threshold = imbalance_threshold if imbalance_threshold is not None else -0.35
        imbalance_sustain = _finite(imbalance_cfg.get("sustain_seconds"))
        imbalance_sustain = imbalance_sustain if imbalance_sustain is not None else 10
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

        large_buy_cfg = self._rule_config(portfolio, symbol, "large_buy")
        large_sell_cfg = self._rule_config(portfolio, symbol, "large_sell")
        buy_flow = self._flow_state(symbol, now, large_buy_cfg)
        sell_flow = self._flow_state(symbol, now, large_sell_cfg)
        flow = self._flow_state(symbol, now)
        large_sell_active = bool(
            large_sell_cfg.get("enabled", True) and sell_flow["large_sell"]
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

        recent_five_minutes = [
            item for item in self._flow.get(symbol, ())
            if item["ts"] >= now.timestamp() - 300 and item["ts"] <= now.timestamp()
        ]
        pressure_cfg = self._rule_config(portfolio, symbol, "fund_flow_pressure")
        pressure_state = self._rule_states.get(f"{symbol}:fund_flow_pressure") or {}
        price_buffer = _finite(pressure_cfg.get("price_buffer"))
        price_buffer = price_buffer if price_buffer is not None else 0.002
        minute_points = [
            item for item in recent_five_minutes
            if item["ts"] <= now.timestamp() - 60
        ]
        minute_reference = max(minute_points, key=lambda item: item["ts"])["price"] if minute_points else None
        one_minute_return = price / minute_reference - 1 if minute_reference else None
        earlier_prices = [
            item["price"] for item in recent_five_minutes
            if item["ts"] <= now.timestamp() - 60
        ]
        previous_low = min(earlier_prices) if earlier_prices else None
        price_reasons = []
        if vwap and price <= float(vwap) * (1 - price_buffer):
            price_reasons.append(f"现价低于分时均价 {(1 - price / float(vwap)):.2%}")
        if one_minute_return is not None and one_minute_return <= -price_buffer:
            price_reasons.append(f"最近一分钟价格下跌 {-one_minute_return:.2%}")
        if previous_low and price <= previous_low * (1 - price_buffer):
            price_reasons.append(f"现价跌破一分钟前的 5 分钟低点 {previous_low:.3f}")
        evidence = []
        if large_sell_active:
            evidence.append(("large_sell", sell_flow["sell_summary"]))
        if outflow_active and not outflow_suppressed:
            evidence.append((
                "continuous_outflow",
                f"最近 60 秒 {int(flow['samples'])} 笔报价增量中卖方占比 "
                f"{float(flow['sell_ratio'] or 0):.0%}",
            ))
        if imbalance_active and not imbalance_suppressed:
            evidence.append((
                "orderbook_imbalance",
                f"买五档挂单 {depth_state['bid_total']:,.0f}，卖五档挂单 "
                f"{depth_state['ask_total']:,.0f}，盘口失衡 {depth_state['imbalance']:.2f}",
            ))
        minimum_evidence = max(2, int(_finite(pressure_cfg.get("min_evidence")) or 2))
        pressure_raw = bool(
            pressure_cfg.get("enabled", True)
            and len(evidence) >= minimum_evidence
            and price_reasons
        )
        recovery_sell_ratio = _finite(pressure_cfg.get("recovery_sell_ratio"))
        recovery_sell_ratio = recovery_sell_ratio if recovery_sell_ratio is not None else 0.55
        recovery_imbalance = _finite(pressure_cfg.get("recovery_imbalance"))
        recovery_imbalance = recovery_imbalance if recovery_imbalance is not None else -0.15
        previous_sources = set(runtime.get("fund_flow_pressure_sources") or [])
        sell_recovered = flow["sell_ratio"] is None or float(flow["sell_ratio"]) < recovery_sell_ratio
        depth_recovered = (
            "orderbook_imbalance" not in previous_sources
            or depth_state["imbalance"] is None
            or depth_state["imbalance"] > recovery_imbalance
        )
        strong_drop = bool(
            one_minute_return is not None
            and one_minute_return <= -float(pressure_cfg.get("strong_price_drop", 0.01))
        )
        trend_broken = any(
            bool((self._rule_states.get(f"{symbol}:ma{days}_breakdown") or {}).get("active"))
            for days in (10, 20)
        )
        pressure_level = 3 if len(evidence) >= 3 and strong_drop else 2 if len(evidence) >= 3 or trend_broken else 1
        reduction = (
            _action_pct({"action_pct": pressure_cfg.get("strong_action_pct")}, 50)
            if pressure_level == 3 else _action_pct(pressure_cfg, 25) if pressure_level == 2 else 0
        )
        score = 80 if pressure_level == 3 else 70 if pressure_level == 2 else 50
        source_ids = [item[0] for item in evidence]

        def emit_pressure() -> None:
            self._emit(
                portfolio,
                position,
                "fund_flow_pressure",
                "资金卖压",
                "warn" if pressure_level >= 2 else "info",
                score,
                reduction,
                [*(item[1] for item in evidence), *price_reasons],
                source_ids=source_ids,
            )

        pressure_was_active = bool(pressure_state.get("active"))
        if pressure_was_active:
            previous_level = int(_finite(runtime.get("fund_flow_pressure_level")) or 1)
            if pressure_raw and pressure_level > previous_level:
                runtime["fund_flow_pressure_level"] = pressure_level
                runtime["fund_flow_pressure_sources"] = source_ids
                if self._set_rule(
                    symbol,
                    "fund_flow_pressure",
                    True,
                    now,
                    event_token=f"level:{pressure_level}",
                    cooldown_seconds=0,
                ):
                    emit_pressure()
            configured_recovery = _finite(pressure_cfg.get("recovery_seconds"))
            recovery_seconds = int(configured_recovery if configured_recovery is not None else 60)
            recovered = self._sustained(
                runtime,
                "fund_flow_pressure_recovery",
                sell_recovered and depth_recovered and not large_sell_active,
                recovery_seconds,
                now,
            )
            if recovered:
                self._set_rule(symbol, "fund_flow_pressure", False, now)
                runtime.pop("fund_flow_pressure_sources", None)
                runtime.pop("fund_flow_pressure_level", None)
        else:
            runtime.pop("fund_flow_pressure_recovery_since", None)
            configured_sustain = _finite(pressure_cfg.get("sustain_seconds"))
            pressure_sustain = int(configured_sustain if configured_sustain is not None else 30)
            pressure_active = self._sustained(
                runtime,
                "fund_flow_pressure",
                pressure_raw,
                pressure_sustain,
                now,
            )
            if pressure_active:
                runtime["fund_flow_pressure_sources"] = source_ids
                runtime["fund_flow_pressure_level"] = pressure_level
                configured_cooldown = _finite(pressure_cfg.get("cooldown_seconds"))
                pressure_cooldown = int(configured_cooldown if configured_cooldown is not None else 900)
                if self._set_rule(
                    symbol,
                    "fund_flow_pressure",
                    True,
                    now,
                    cooldown_seconds=pressure_cooldown,
                ):
                    emit_pressure()
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
            if self._set_rule(symbol, "five_minute_drawdown", False if drawdown_suppressed else drawdown_active, now) and not drawdown_suppressed:
                self._emit(
                    portfolio, position, "five_minute_drawdown", "5 分钟高点回撤", "warn",
                    45, drawdown_action, [f"从 5 分钟高点回撤 {(1 - price / five_minute_high):.2%}"],
                )
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
        if self._set_rule(symbol, "vwap_breakdown", vwap_active, now) and not vwap_suppressed:
            self._emit(
                portfolio, position, "vwap_breakdown", "分时均价负偏离超限", "warn", 45, vwap_action,
                [
                    f"现价 {price:.3f}，VWAP {vwap:.3f}，负偏离 {(1 - price / vwap):.2%}"
                    f"（阈值 {vwap_buffer:.2%}）持续 {int(vwap_sustain)} 秒"
                ],
            )

        combined_signals = {
            **history,
            **{signal_id: False for signal_id in INTRADAY_SIGNAL_LABELS},
            **(intraday_signals or {}),
        }
        current_signal_ids = {
            signal_id for signal_id in combined_signals
            if signal_id.startswith(("signal_", "csg_"))
        }
        prefix = f"{symbol}:signal:"
        for key in list(self._rule_states):
            if key.startswith(prefix):
                signal_id = key.removeprefix(prefix)
                if signal_id not in current_signal_ids:
                    self._set_rule(symbol, f"signal:{signal_id}", False, now)
        overlap_signals = {
            "signal_ma5_breakdown",
            "signal_ma10_breakdown",
            "signal_ma20_breakdown",
            "signal_limit_down",
            "signal_broken_limit_up",
        }
        for signal_id, value in combined_signals.items():
            if signal_id.startswith(("signal_", "csg_")) and value is not True:
                self._set_rule(symbol, f"signal:{signal_id}", False, now)
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
                self._set_rule(symbol, f"signal:{signal_id}", False, now)
                continue
            signal_rule_id = f"signal:{signal_id}"
            if self._recovery_suppressed(runtime, signal_rule_id, True):
                self._set_rule(symbol, signal_rule_id, False, now)
                continue
            direction = (
                configured.get("direction")
                or self._custom_signal_directions.get(signal_id)
                or self._signal_direction(signal_id)
            ) if is_custom else self._signal_direction(signal_id)
            t_config = self._rule_config(portfolio, symbol, "t_trading")
            if signal_id in INTRADAY_SIGNAL_LABELS and t_config.get("enabled", False):
                trade_action = "BUY" if direction == "entry" else "SELL"
                trade_pct = _trade_pct(t_config, "buy_pct" if trade_action == "BUY" else "sell_pct", 10 if trade_action == "BUY" else 25)
                t_allowed = bool(features and features.get("available") and features.get("fresh"))
                if t_allowed:
                    closed_bars = features.get("closed_bars") or []
                    confirm_bars = max(1, int(_finite(t_config.get("confirm_bars")) or 2))
                    vwap_value = _finite(features.get("session_vwap"))
                    ema9_1m = _finite(features.get("ema9_1m"))
                    ema20_1m = _finite(features.get("ema20_1m"))
                    ema9_5m = _finite(features.get("ema9_5m"))
                    ema20_5m = _finite(features.get("ema20_5m"))
                    if t_allowed and (
                        len(closed_bars) < confirm_bars or features.get("bars_5m", 0) < 2
                        or vwap_value is None or ema9_1m is None or ema20_1m is None
                        or ema9_5m is None or ema20_5m is None
                    ):
                        t_allowed = False
                    if t_allowed:
                        recent_closes = [
                            _finite(item.get("close")) for item in closed_bars[-confirm_bars:]
                        ]
                        recent_closes = [value for value in recent_closes if value is not None]
                        if trade_action == "BUY":
                            t_allowed = bool(
                                len(recent_closes) == confirm_bars
                                and all(value >= vwap_value for value in recent_closes)
                                and ema9_1m > ema20_1m and ema9_5m >= ema20_5m
                            )
                        else:
                            t_allowed = bool(
                                len(recent_closes) == confirm_bars
                                and all(value <= vwap_value for value in recent_closes)
                                and ema9_1m < ema20_1m and ema9_5m <= ema20_5m
                            )
                        expected_return = abs(price - vwap_value) / price if price else 0.0
                        minimum_return = _finite(t_config.get("min_expected_return")) or 0.0
                        t_allowed = t_allowed and expected_return >= minimum_return
                        flow_ratio = _finite(features.get("buy_ratio" if trade_action == "BUY" else "sell_ratio"))
                        relative_volume = _finite(features.get("relative_volume"))
                        t_allowed = t_allowed and flow_ratio is not None and flow_ratio >= 0.5
                        t_allowed = t_allowed and relative_volume is not None and relative_volume >= 0.5
                        if features.get("orderbook_imbalance") is not None:
                            imbalance = float(features["orderbook_imbalance"])
                            t_allowed = t_allowed and (imbalance >= -0.2 if trade_action == "BUY" else imbalance <= 0.2)
                    t_state = runtime.setdefault("t_trade", {})
                    trading_date = now.date().isoformat()
                    if t_state.get("date") != trading_date:
                        t_state.clear()
                        t_state["date"] = trading_date
                    daily_limit = max(0, int(_finite(t_config.get("max_daily_trades")) or 3))
                    cooldown = max(0, int(_finite(t_config.get("cooldown_minutes")) or 10))
                    last_trade = _finite(t_state.get("last_at"))
                    if int(t_state.get("count") or 0) >= daily_limit or (
                        last_trade is not None and now.timestamp() - last_trade < cooldown * 60
                    ):
                        t_allowed = False
                if not t_allowed:
                    self._set_rule(symbol, signal_rule_id, False, now)
                    continue
                suggested_volume = self._t_trade_volume(portfolio, position, price, trade_action, trade_pct)
                event_token = f"{signal_id}:{features.get('as_of')}" if features else None
                if suggested_volume and self._set_rule(
                    symbol, signal_rule_id, True, now, event_token=event_token,
                ):
                    if features:
                        t_state = runtime.setdefault("t_trade", {})
                        t_state["last_at"] = now.timestamp()
                        t_state["count"] = int(t_state.get("count") or 0) + 1
                    self._emit(
                        portfolio,
                        position,
                        f"t:{signal_id}",
                        f"做T{'买入' if trade_action == 'BUY' else '卖出'}",
                        "info",
                        45,
                        trade_pct,
                        [f"分时信号：{self._signal_label(signal_id, configured)}"],
                        source_ids=[signal_id],
                        trade_action=trade_action,
                        suggested_price=price,
                        suggested_volume=suggested_volume,
                        feature_snapshot_at=features.get("as_of") if features else None,
                    )
                continue
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
                self.store.set_runtime("recent_exit_signals", {
                    current_symbol: list(values)
                    for current_symbol, values in self._recent_exit_signals.items()
                    if values
                })
                if independent:
                    action = max(action, 50)
                    reasons.append("5 分钟内两个独立出场信号共振")
            event_token = None
            if signal_id not in INTRADAY_SIGNAL_LABELS:
                event_token = f"{signal_id}:{history.get('_position_risk_as_of') or now.date()}"
            if self._set_rule(symbol, signal_rule_id, True, now, event_token=event_token):
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
    def _t_trade_volume(
        portfolio: dict[str, Any],
        position: dict[str, Any],
        price: float,
        action: str,
        trade_pct: int,
    ) -> int:
        if price <= 0 or trade_pct <= 0:
            return 0
        if action == "SELL":
            base = int(_finite(position.get("available")) or 0)
            return max(0, math.floor(base * trade_pct / 100 / 100) * 100)
        cash = _finite((portfolio.get("account") or {}).get("cash"))
        quantity = _finite(position.get("quantity"))
        if cash is None or cash <= 0 or quantity is None or quantity <= 0:
            return 0
        notional = min(cash, price * quantity * trade_pct / 100)
        return max(0, math.floor(notional / price / 100) * 100)

    @staticmethod
    def _signal_direction(signal_id: str) -> str:
        if signal_id in _BUILTIN_SIGNAL_DIRECTIONS:
            return _BUILTIN_SIGNAL_DIRECTIONS[signal_id]
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
                "resealed": False,
                "shrink_ratio": 0.0,
                "imbalance": None,
                "bid_total": 0.0,
                "ask_total": 0.0,
            }
        if any(
            item.get("received_at") is not None
            and now.timestamp() - float(item["received_at"]) > 30
            for item in list(snapshots)[-3:]
        ):
            self.store.set_runtime(f"depth:{symbol}", {})
            return {
                "sealed": False,
                "broken": False,
                "resealed": False,
                "shrink_ratio": 0.0,
                "imbalance": None,
                "bid_total": 0.0,
                "ask_total": 0.0,
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
        trading_date = now.date().isoformat()
        if runtime.get("trading_date") != trading_date:
            runtime = {"trading_date": trading_date}
        was_sealed = bool(runtime.get("sealed"))
        had_broken = bool(runtime.get("had_broken"))
        broken = was_sealed and not sealed
        resealed = sealed and had_broken
        latest = recent[-1]
        latest_bid = _finite(latest.get("bid1_volume", latest.get("bid_volume1"))) or 0.0
        previous_peak = _finite(runtime.get("peak_bid_volume")) or 0.0
        peak_bid = max(latest_bid, previous_peak) if sealed else previous_peak
        shrink_ratio = 1 - latest_bid / peak_bid if sealed and peak_bid > 0 else 0.0
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
        runtime["trading_date"] = trading_date
        runtime["had_broken"] = (had_broken or broken) and not resealed
        runtime["peak_bid_volume"] = peak_bid
        runtime["last_depth_at"] = now.timestamp()
        self.store.set_runtime(f"depth:{symbol}", runtime)
        return {
            "sealed": sealed,
            "broken": broken,
            "resealed": resealed,
            "shrink_ratio": shrink_ratio,
            "imbalance": imbalance,
            "bid_total": bid_total,
            "ask_total": ask_total,
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
        def direction_evidence(direction_ratio: float, direction: int) -> tuple[bool, float]:
            configured_amount = _finite(config.get("min_amount"))
            configured_mad = _finite(config.get("mad_multiplier"))
            configured_z_score = _finite(config.get("min_z_score"))
            configured_direction = _finite(config.get("direction_ratio"))
            min_amount = max(0.0, configured_amount if configured_amount is not None else 1_000_000.0)
            mad_multiplier = max(0.0, configured_mad if configured_mad is not None else 3.0)
            min_z_score = max(0.0, configured_z_score if configured_z_score is not None else 2.5)
            minimum_direction = min(1.0, max(0.0, configured_direction if configured_direction is not None else 0.65))
            threshold = max(min_amount, median + mad_multiplier * 1.4826 * mad)
            direction_amounts = [
                item["amount"] for item in values if item["direction"] * direction > 0
            ]
            if not direction_amounts:
                return False, 0.0
            direction_largest = max(direction_amounts)
            direction_z_score = (direction_largest - median) / dispersion
            return (
                direction_largest >= threshold
                and direction_z_score >= min_z_score
                and direction_ratio >= minimum_direction,
                direction_z_score,
            )
        dispersion = max(1.0, 1.4826 * mad)
        large_buy, buy_z_score = direction_evidence(buy_ratio, 1)
        large_sell, sell_z_score = direction_evidence(sell_ratio, -1)
        return {
            "large_buy": total > 0 and large_buy,
            "large_sell": total > 0 and large_sell,
            "buy_ratio": buy_ratio,
            "sell_ratio": sell_ratio,
            "buy_z_score": buy_z_score,
            "sell_z_score": sell_z_score,
            "samples": len(values),
            "summary": f"最近 {window_seconds} 秒方向占比 买{buy_ratio:.0%}/卖{sell_ratio:.0%}",
            "buy_summary": (
                f"最近 {window_seconds} 秒买方占比 {buy_ratio:.0%}，买向异常单 Z={buy_z_score:.2f}"
            ),
            "sell_summary": (
                f"最近 {window_seconds} 秒卖方占比 {sell_ratio:.0%}，卖向异常单 Z={sell_z_score:.2f}"
            ),
        }

    def _check_quote_staleness(self, now: datetime | None = None) -> None:
        now = (now or cn_now()).replace(tzinfo=None)
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
                f"持仓行情超过 {int(threshold)} 秒未更新，等待恢复后重新建立基线",
                set(stale_symbols),
            )
        enabled = bool(config.get("enabled", True))
        was_active = bool(
            (self._rule_states.get("__portfolio__:quote_interruption") or {}).get("active"),
        )
        interruption_active = enabled and stale
        recovery_started_at = self._quote_interruption_recovery_started_at
        if not enabled or stale:
            recovery_started_at = None
        elif was_active:
            recovery_started_at = recovery_started_at or now.timestamp()
            interruption_active = (
                now.timestamp() - recovery_started_at
                < _QUOTE_INTERRUPTION_RECOVERY_SECONDS
            )
            if not interruption_active:
                recovery_started_at = None
        else:
            recovery_started_at = None
        if recovery_started_at != self._quote_interruption_recovery_started_at:
            self._quote_interruption_recovery_started_at = recovery_started_at
            self.store.set_runtime(
                "quote_interruption_recovery_started_at", recovery_started_at,
            )
        if self._set_rule(
            "__portfolio__", "quote_interruption", interruption_active,
            now,
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

    def _evaluate_account(
        self,
        portfolio: dict[str, Any],
        now: datetime | None = None,
    ) -> None:
        evaluation_time = (now or cn_now()).replace(tzinfo=None)
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
            ("unrealized_loss", current_equity, lambda config: unrealized / current_equity <= -abs(_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.08), "持仓总浮亏超过权益"),
            ("total_exposure", current_equity, lambda config: market_value / current_equity > (_finite(config.get("threshold")) if _finite(config.get("threshold")) is not None else 0.95), "总仓位超过"),
        ]
        for rule_id, denominator, condition, reason_label in checks:
            config = self._rule_config(portfolio, "__portfolio__", rule_id)
            active = bool(denominator and condition(config))
            threshold = _finite(config.get("threshold"))
            default_thresholds = {
                "daily_equity_loss": 0.03,
                "equity_drawdown": 0.08,
                "unrealized_loss": 0.08,
                "total_exposure": 0.95,
            }
            threshold = threshold if threshold is not None else default_thresholds[rule_id]
            reason = f"{reason_label} {threshold:.0%}"
            if self._set_rule(
                "__portfolio__", rule_id, bool(config.get("enabled", True) and active),
                evaluation_time,
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
                evaluation_time,
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
        cutoff = evaluation_time.timestamp() - cluster_window
        while self._severe_events and self._severe_events[0] < cutoff:
            self._severe_events.popleft()
        config = cluster_config
        clustered = bool(config.get("enabled", True) and len(self._severe_events) >= cluster_count)
        if self._set_rule(
            "__portfolio__", "clustered_severe_events", clustered, evaluation_time,
        ):
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
        occurred_at: datetime | None = None,
        trade_action: str | None = None,
        suggested_price: float | None = None,
        suggested_volume: int | None = None,
        stage: str | None = None,
        r_multiple: float | None = None,
        effective_stop_price: float | None = None,
        feature_snapshot_at: str | None = None,
    ) -> None:
        symbol = position.get("symbol") if position else None
        config_symbol = symbol or "__portfolio__"
        if rule_id.startswith("signal:"):
            signal_id = rule_id.removeprefix("signal:")
            signal_group = "custom" if signal_id.startswith("csg_") else "builtin"
            notify = self._signal_config(portfolio, config_symbol, signal_group, signal_id).get("notify", False)
        elif rule_id.startswith("t:"):
            notify = self._rule_config(portfolio, config_symbol, "t_trading").get("notify", False)
        elif rule_id.startswith("monitor:"):
            notify = self._signal_config(
                portfolio, config_symbol, "monitor_rules", rule_id.removeprefix("monitor:"),
            ).get("notify", False)
        else:
            notify = self._rule_config(portfolio, config_symbol, rule_id).get("notify", False)
        name = position.get("name") if position else "组合"
        state = self._rule_states.get(f"{config_symbol}:{rule_id}") or {}
        state_time = _finite(state.get("changed_at"))
        event_time = (
            occurred_at.replace(tzinfo=None)
            if occurred_at is not None
            else datetime.fromtimestamp(state_time) if state_time is not None
            else cn_now().replace(tzinfo=None)
        )
        event_token = str(state.get("last_event_token") or "")
        fingerprint_raw = (
            f"{event_time.date()}:{symbol or '__portfolio__'}:{rule_id}:{event_token}"
        )
        fingerprint = hashlib.sha256(fingerprint_raw.encode()).hexdigest()
        event = {
            "ts": int(event_time.timestamp() * 1000),
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
        if source_ids:
            event["source_ids"] = list(source_ids)
        if stage is not None:
            event["stage"] = stage
        if r_multiple is not None:
            event["r_multiple"] = r_multiple
        if effective_stop_price is not None:
            event["effective_stop_price"] = effective_stop_price
        if feature_snapshot_at:
            event["feature_snapshot_at"] = feature_snapshot_at
        if severity == "critical" and rule_id not in {
            "clustered_severe_events", "quote_interruption",
        }:
            event_date = event_time.date().isoformat()
            if self._severe_event_date != event_date:
                self._severe_events.clear()
                self._severe_event_date = event_date
                self._severe_event_fingerprints.clear()
            if fingerprint not in self._severe_event_fingerprints:
                self._severe_events.append(event_time.timestamp())
                self._severe_event_fingerprints.add(fingerprint)
                self.store.set_runtime("severe_events", list(self._severe_events))
                self.store.set_runtime("severe_event_date", self._severe_event_date)
                self.store.set_runtime(
                    "severe_event_fingerprints",
                    sorted(self._severe_event_fingerprints),
                )
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
                trade_action=trade_action,
                suggested_price=suggested_price,
                suggested_volume=suggested_volume,
                stage=stage,
                r_multiple=r_multiple,
                effective_stop_price=effective_stop_price,
                feature_snapshot_at=feature_snapshot_at,
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
        trade_action: str | None = None,
        suggested_price: float | None = None,
        suggested_volume: int | None = None,
        stage: str | None = None,
        r_multiple: float | None = None,
        effective_stop_price: float | None = None,
        feature_snapshot_at: str | None = None,
    ) -> dict[str, Any]:
        if trade_action == "BUY":
            action = "做T买入建议"
        elif trade_action == "SELL":
            action = "做T卖出建议"
        else:
            action = "清仓建议" if reduction_pct >= 100 else "减仓建议"
        return self.store.add_recommendation({
            "fingerprint": fingerprint,
            "symbol": symbol,
            "scope": "symbol" if symbol else "portfolio",
            "rule_id": rule_id,
            "severity": severity,
            "risk_score": risk_score,
            "action": action,
            "reduction_pct": reduction_pct,
            "reasons": reasons,
            "source_ids": source_ids,
            "portfolio_revision": portfolio["revision"],
            "trade_action": trade_action,
            "suggested_price": suggested_price,
            "suggested_volume": suggested_volume,
            "stage": stage,
            "r_multiple": r_multiple,
            "effective_stop_price": effective_stop_price,
            "feature_snapshot_at": feature_snapshot_at,
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
        self._rule_states = {}
        self.store.set_runtime("rule_states", {})
        self._severe_events.clear()
        self.store.set_runtime("severe_events", [])
        self._severe_event_date = ""
        self._severe_event_fingerprints.clear()
        self.store.set_runtime("severe_event_date", "")
        self.store.set_runtime("severe_event_fingerprints", [])
        self._quote_interruption_recovery_started_at = None
        self.store.set_runtime("quote_interruption_recovery_started_at", None)
        self._recent_exit_signals.clear()
        self.store.set_runtime("recent_exit_signals", {})
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

    def replace_from_qmt(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        """把已通过同一轮 QMT 探活的快照作为权威持仓写入本地。"""
        account = dict(snapshot.get("account") or {})
        total_asset = _finite(account.get("total_asset"))
        cash = _finite(account.get("cash"))
        if total_asset is None or total_asset <= 0 or cash is None or cash < 0:
            raise ValueError("QMT 账户资金字段无效，拒绝替换本地持仓")
        positions = []
        for row in snapshot.get("positions") or []:
            symbol = str(row.get("symbol") or "").strip().upper()
            quantity = _finite(row.get("quantity"))
            available = _finite(row.get("available"))
            cost = _finite(row.get("cost_price"))
            if (
                not symbol or quantity is None or available is None or cost is None
                or quantity < 0 or available < 0 or available > quantity or cost <= 0
            ):
                raise ValueError(f"QMT 持仓字段无效: {symbol or 'unknown'}")
            positions.append({
                "symbol": symbol,
                "name": str(row.get("name") or symbol),
                "asset_type": str(row.get("asset_type") or "stock"),
                "quantity": int(quantity),
                "available": int(available),
                "cost_price": round(cost, 4),
                "import_price": _finite(row.get("price")) or round(cost, 4),
                "price_source": "qmt_position",
            })
        current = self.store.load()
        account_id = str(snapshot.get("account_id") or account.get("name") or "").strip()
        account_value = {
            **current.get("account", {}),
            "name": account_id or str(current["account"].get("name") or "QMT账户"),
            "cash": round(cash, 2),
            "total_asset": round(total_asset, 2),
            "previous_close_total_asset": _finite(current["account"].get("previous_close_total_asset")) or total_asset,
        }
        position_fields = ("symbol", "name", "asset_type", "quantity", "available", "cost_price")

        def position_signature(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
            return sorted(tuple(row.get(field) for field in position_fields) for row in rows)

        positions_changed = position_signature(current.get("positions") or []) != position_signature(positions)
        account_changed = any(
            current.get("account", {}).get(field) != account_value.get(field)
            for field in ("name", "cash", "total_asset")
        )
        first_qmt_sync = self.store.get_runtime("qmt_account_id") != account_id
        sync_state = {"synced_at": snapshot.get("synced_at"), "account_id": account_id}
        self.store.set_runtime("qmt_sync", sync_state)
        if not first_qmt_sync and not positions_changed and not account_changed:
            return current

        if first_qmt_sync:
            account_value["high_watermark"] = round(total_asset, 2)
        value = {
            **current,
            "account": account_value,
            "positions": positions,
            "imported_at": cn_now().isoformat(),
        }
        if positions_changed:
            saved = self.store.replace(value, current["revision"])
        else:
            saved = self.store.update_system(lambda stored: stored.update({
                "account": account_value,
                "imported_at": value["imported_at"],
            }))
        self.store.set_runtime("qmt_account_id", account_id)
        if first_qmt_sync:
            self.store.set_runtime("account", {
                "high_watermark": round(total_asset, 2),
                "last_equity": round(total_asset, 2),
                "reset_at": saved["imported_at"],
            })
        if positions_changed:
            old_by_symbol = {row.get("symbol"): row for row in current.get("positions") or []}
            new_by_symbol = {row.get("symbol"): row for row in positions}

            def row_signature(row: dict[str, Any]) -> tuple[Any, ...]:
                return tuple(row.get(field) for field in position_fields)

            changed_symbols = {
                symbol for symbol in old_by_symbol.keys() | new_by_symbol.keys()
                if symbol not in old_by_symbol
                or symbol not in new_by_symbol
                or row_signature(old_by_symbol[symbol]) != row_signature(new_by_symbol[symbol])
            }
            self._rule_states = {
                key: state for key, state in self._rule_states.items()
                if key.split(":", 1)[0] not in changed_symbols
            }
            self.store.set_runtime("rule_states", self._rule_states)
            self.refresh_subscription()
        self._notify_updated()
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
