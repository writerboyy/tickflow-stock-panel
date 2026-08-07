"""自由策略脚本的可信执行核心与账户撮合。"""
from __future__ import annotations

import builtins
import copy
import io
import logging
import math
import re
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal
from itertools import groupby
from statistics import mean, pstdev
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Literal

from .bars import Bar
from .readiness import ReadinessRequirement, make_requirement
from .schedule import RegisteredSchedule, ScheduleRule, parse_time_expression
from app.market_time import cn_naive_now
from app.services.data_authority import normalize_reference_asset

logger = logging.getLogger(__name__)

EventType = Literal["bar", "quote", "scheduled", "fill", "market"]


@dataclass(slots=True)
class FreeStrategyConfig:
    initial_capital: float = 1_000_000.0
    fees_pct: float = 0.0002
    commission_pct: float | None = None
    sell_commission_pct: float | None = None
    min_commission: float = 0.0
    reserve_buy_fees: bool = True
    stamp_tax_pct: float = 0.001
    transfer_fee_pct: float = 0.0
    slippage_bps: float = 5.0
    price_tick: float | None = None
    lot_size: int = 100
    max_exposure_pct: float = 1.0
    settlement: str = "t1"
    t0_symbols: list[str] = field(default_factory=list)
    allow_stale_fills: bool = False
    fill_policy: str = "next_open"
    asset_type: str = "stock"
    benchmark_symbol: str = "510300.SH"
    callback_timeout_seconds: float = 30.0


@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    timestamp: datetime
    last_price: float
    prev_close: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float = 0.0
    amount: float = 0.0
    name: str | None = None
    limit_up: float | None = None
    limit_down: float | None = None
    suspended: bool = False


@dataclass(slots=True)
class RiskConfig:
    max_symbol_exposure_pct: float = 1.0
    daily_loss_pct: float = 0.10
    max_drawdown_pct: float = 0.30
    max_orders_per_minute: int = 60


@dataclass(slots=True)
class Order:
    id: str
    symbol: str
    side: str
    quantity: float | None = None
    value: float | None = None
    cash_weight: float | None = None
    target_quantity: float | None = None
    target_value: float | None = None
    target_percent: float | None = None
    submitted_at: str = ""
    status: str = "pending"
    reason: str = ""


@dataclass(slots=True)
class Fill:
    order_id: str
    symbol: str
    side: str
    quantity: float
    price: float
    value: float
    fee: float
    timestamp: str
    market_amount: float | None = None
    market_volume: float | None = None
    participation_pct: float | None = None
    commission: float = 0.0
    stamp_tax: float = 0.0
    transfer_fee: float = 0.0
    dividend_tax: float = 0.0
    total_fee: float = 0.0
    status: str = "filled"
    reason: str = ""
    submitted_at: str = ""
    fee_components_complete: bool = True


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    order_id: str
    submitted_at: str
    executed_at: str | None
    symbol: str
    side: str
    requested_quantity: float | None
    executed_quantity: float
    price: float | None
    amount: float
    commission: float
    stamp_tax: float
    transfer_fee: float
    dividend_tax: float
    fee: float
    total_fee: float
    fee_components_complete: bool
    status: str
    reason: str


class Account:
    def __init__(self, config: FreeStrategyConfig):
        self.config = config
        self.cash = float(config.initial_capital)
        self.positions: dict[str, float] = {}
        self.available: dict[str, float] = {}
        self.avg_cost: dict[str, float] = {}
        self.orders: list[Order] = []
        self.fills: list[Fill] = []
        self.corporate_actions: list[dict[str, Any]] = []
        self.equity_curve: list[dict[str, Any]] = []

    def snapshot(self) -> dict[str, Any]:
        return {"cash": self.cash, "positions": self.positions, "available": self.available,
                "avg_cost": self.avg_cost, "orders": [asdict(v) for v in self.orders],
                "fills": [asdict(v) for v in self.fills],
                "corporate_actions": self.corporate_actions, "equity_curve": self.equity_curve}

    def restore(self, raw: dict[str, Any]) -> None:
        self.cash = float(raw.get("cash", self.cash))
        self.positions = {k: float(v) for k, v in raw.get("positions", {}).items()}
        self.available = {k: float(v) for k, v in raw.get("available", {}).items()}
        self.avg_cost = {k: float(v) for k, v in raw.get("avg_cost", {}).items()}
        self.orders = [Order(**item) for item in raw.get("orders", [])]
        self.fills = []
        for item in raw.get("fills", []):
            migrated = dict(item)
            if "total_fee" not in migrated:
                migrated["total_fee"] = float(migrated.get("fee", 0.0))
            if "fee_components_complete" not in migrated:
                migrated["fee_components_complete"] = False
            self.fills.append(Fill(**migrated))
        self.corporate_actions = list(raw.get("corporate_actions", []))
        self.equity_curve = list(raw.get("equity_curve", []))

    def equity(self, prices: dict[str, float]) -> float:
        return self.cash + sum(qty * prices.get(symbol, self.avg_cost.get(symbol, 0.0))
                                for symbol, qty in self.positions.items())


class BarsView(dict):
    """策略脚本可用的只读友好 bar 映射。"""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


class QuotesView(dict):
    """只读的 ``symbol -> Quote`` 策略视图。"""

    def _readonly(self, *_args: Any, **_kwargs: Any) -> None:
        raise TypeError("quotes 是只读映射")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _readonly


class Context:
    def __init__(self, engine: "FreeStrategyEngine") -> None:
        self._engine = engine
        self.state: dict[str, Any] = copy.deepcopy(engine.state)
        self.now: datetime | None = None
        self.period = engine.timeframe
        self.portfolio = SimpleNamespace(cash=engine.account.cash, positions={}, available_positions={}, avg_cost={}, total_value=engine.account.cash)
        self.account = self.portfolio
        self._scheduled: list[RegisteredSchedule] = []
        self._universe: list[str] = []
        self._history_requirements: dict[str, int] = {}
        self._market_history_requirements: dict[tuple[str, str], int] = {}
        self._extra_history_requirements: set[str] = set()
        self._readiness_requirements: list[ReadinessRequirement] = []

    @property
    def universe(self) -> list[str]:
        return list(self._universe)

    def set_universe(self, symbols: Iterable[str]) -> None:
        if isinstance(symbols, (str, bytes)):
            raise ValueError("股票池必须是标的代码列表")
        suffixes = {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}
        normalized: list[str] = []
        for raw in symbols:
            symbol = str(raw).strip().upper()
            for source_suffix, target_suffix in suffixes.items():
                if symbol.endswith(source_suffix):
                    symbol = f"{symbol[:-len(source_suffix)]}{target_suffix}"
                    break
            if symbol and symbol not in normalized:
                normalized.append(symbol)
        if not normalized:
            raise ValueError("股票池不能为空")
        self._universe = normalized

    @property
    def history_requirements(self) -> dict[str, int]:
        return dict(self._history_requirements)

    def require_history(self, timeframe: str = "1d", bars: int = 1) -> None:
        period = str(timeframe).strip().lower()
        if period != "1d":
            raise ValueError("预热历史目前只支持 1d 日线")
        if isinstance(bars, bool) or not isinstance(bars, int) or bars <= 0:
            raise ValueError("预热 bar 数量必须是正整数")
        self._history_requirements[period] = max(
            bars, self._history_requirements.get(period, 0),
        )

    def require_market_history(
        self,
        asset_type: str = "etf",
        timeframe: str = "1d",
        bars: int = 1,
    ) -> None:
        asset = str(asset_type).strip().lower()
        period = str(timeframe).strip().lower()
        if asset not in {"stock", "etf", "index"}:
            raise ValueError("全市场历史的资产类型只支持 stock、etf 或 index")
        if period != "1d":
            raise ValueError("全市场历史目前只支持 1d 日线")
        if isinstance(bars, bool) or not isinstance(bars, int) or bars <= 0:
            raise ValueError("全市场预热 bar 数量必须是正整数")
        key = (asset, period)
        self._market_history_requirements[key] = max(
            bars, self._market_history_requirements.get(key, 0),
        )

    def reference_asset(
        self,
        symbol: str,
        *,
        asset_type: str = "etf",
        timeframe: str = "1d",
    ) -> dict[str, str]:
        """Return an explicit reference K-line handle without stock aliasing."""
        return normalize_reference_asset({
            "symbol": symbol,
            "asset_type": asset_type,
            "timeframe": timeframe,
        }).to_dict()

    @property
    def market_history_requirements(self) -> dict[tuple[str, str], int]:
        return dict(self._market_history_requirements)

    def require_extra_history(self, name: str) -> None:
        value = str(name).strip().lower()
        if re.fullmatch(r"[a-z][a-z0-9_]*", value) is None:
            raise ValueError("额外历史数据名称必须为小写英文标识符")
        self._extra_history_requirements.add(value)

    @property
    def extra_history_requirements(self) -> set[str]:
        return set(self._extra_history_requirements)

    @property
    def readiness_requirements(self) -> tuple[ReadinessRequirement, ...]:
        return tuple(self._readiness_requirements)

    def require_data_readiness(
        self,
        *,
        rebalance: str,
        financials: dict[str, dict[str, Any]] | None = None,
        valuation_fields: Iterable[str] = (),
        industry_standard: str | None = None,
        industry_level: str | int | None = None,
        lifecycle: bool = False,
        adjustment: str | None = None,
        corporate_actions: bool = False,
    ) -> None:
        requirement = make_requirement(
            rebalance=rebalance,
            financials=financials,
            valuation_fields=valuation_fields,
            industry_standard=industry_standard,
            industry_level=industry_level,
            lifecycle=lifecycle,
            adjustment=adjustment,
            corporate_actions=corporate_actions,
        )
        if requirement not in self._readiness_requirements:
            self._readiness_requirements.append(requirement)

    @staticmethod
    def _normalize_symbol(raw: Any) -> str:
        symbol = str(raw).strip().upper()
        for source_suffix, target_suffix in {
            ".XSHG": ".SH",
            ".XSHE": ".SZ",
            ".XBSE": ".BJ",
        }.items():
            if symbol.endswith(source_suffix):
                return f"{symbol[:-len(source_suffix)]}{target_suffix}"
        return symbol

    def instruments(self, asset_type: str | None = None) -> list[dict[str, Any]]:
        asset = str(asset_type).strip().lower() if asset_type else None
        return [
            dict(item)
            for item in self._engine._instruments
            if asset is None or item.get("asset_type") == asset
        ]

    def financial_snapshot(
        self,
        symbols: Iterable[str],
        end_date: date | datetime | str | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized = list(dict.fromkeys(
            self._normalize_symbol(symbol)
            for symbol in symbols
            if str(symbol).strip()
        ))
        if not normalized or self.now is None:
            return {}
        if isinstance(end_date, datetime):
            requested_end = end_date.date()
        elif isinstance(end_date, date):
            requested_end = end_date
        elif end_date is not None:
            requested_end = date.fromisoformat(str(end_date))
        else:
            requested_end = self.now.date() - timedelta(days=1)
        cutoff = min(requested_end, self.now.date() - timedelta(days=1))
        if self._engine._financial_snapshot_loader is None:
            return {}
        return self._engine._financial_snapshot_loader(normalized, cutoff)

    def get_industry(
        self,
        symbols: Iterable[str],
        as_of: date | datetime | str,
        standard: str,
        level: str | int | None = None,
    ) -> dict[str, dict[str, Any]]:
        normalized = list(dict.fromkeys(
            self._normalize_symbol(symbol)
            for symbol in symbols
            if str(symbol).strip()
        ))
        if isinstance(as_of, datetime):
            cutoff = as_of.date()
        elif isinstance(as_of, date):
            cutoff = as_of
        else:
            cutoff = date.fromisoformat(str(as_of)[:10])
        if self.now is not None and cutoff > self.now.date():
            raise ValueError("PIT 行业查询日期不能晚于当前策略时间")
        loader = self._engine._industry_history_loader
        if loader is None:
            raise ValueError("缺少 TickFlow PIT 行业历史加载器")
        return loader(normalized, cutoff, standard, level)

    def dividend_ratio_ranked(
        self,
        symbols: Iterable[str],
        previous_date: date,
    ) -> list[str] | None:
        if self._engine._dividend_ratio_loader is None:
            return None
        normalized = list(dict.fromkeys(
            self._normalize_symbol(symbol)
            for symbol in symbols
            if str(symbol).strip()
        ))
        return self._engine._dividend_ratio_loader(normalized, previous_date)

    def valuation_market_caps(
        self,
        symbols: Iterable[str],
        end_date: date | datetime | str | None = None,
    ) -> dict[str, float]:
        if self._engine._valuation_market_cap_loader is None or self.now is None:
            return {}
        normalized = list(dict.fromkeys(
            self._normalize_symbol(symbol)
            for symbol in symbols
            if str(symbol).strip()
        ))
        if isinstance(end_date, datetime):
            requested_end = end_date.date()
        elif isinstance(end_date, date):
            requested_end = end_date
        elif end_date is not None:
            requested_end = date.fromisoformat(str(end_date))
        else:
            requested_end = self.now.date() - timedelta(days=1)
        cutoff = min(requested_end, self.now.date() - timedelta(days=1))
        return self._engine._valuation_market_cap_loader(normalized, cutoff)

    def smallcap_index_value(
        self,
        symbols: Iterable[str],
        previous_date: date,
    ) -> float | None:
        if self._engine._smallcap_index_loader is None:
            return None
        normalized = list(dict.fromkeys(
            self._normalize_symbol(symbol)
            for symbol in symbols
            if str(symbol).strip()
        ))
        return self._engine._smallcap_index_loader(normalized, previous_date)

    def style_liquidity_signal(
        self,
        previous_date: date | datetime | str,
    ) -> dict[str, Any] | None:
        if self._engine._style_liquidity_loader is None or self.now is None:
            return None
        if isinstance(previous_date, datetime):
            requested_date = previous_date.date()
        elif isinstance(previous_date, date):
            requested_date = previous_date
        else:
            requested_date = date.fromisoformat(str(previous_date)[:10])
        cutoff = min(requested_date, self.now.date() - timedelta(days=1))
        return self._engine._style_liquidity_loader(cutoff)

    def market_history_bars(
        self,
        symbol: str,
        count: int = 20,
        timeframe: str = "1d",
    ) -> list[Bar]:
        if count <= 0:
            return []
        history = self._engine._market_history_by_period.get(timeframe, {}).get(symbol, [])
        cutoff = self.now
        if cutoff is None:
            return []
        if self._engine._market_history_loader is not None:
            self._engine._market_history_loader(cutoff)
            history = self._engine._market_history_by_period.get(timeframe, {}).get(symbol, [])
        visible = [bar for bar in history if bar.timestamp < cutoff]
        return list(visible[-count:])

    def market_history_batch(
        self,
        symbols: Iterable[str],
        count: int = 20,
        timeframe: str = "1d",
    ) -> dict[str, list[Bar]]:
        if count <= 0 or self.now is None:
            return {}
        if self._engine._market_history_loader is not None:
            self._engine._market_history_loader(self.now)
        history = self._engine._market_history_by_period.get(timeframe, {})
        result: dict[str, list[Bar]] = {}
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            visible = [bar for bar in history.get(symbol, []) if bar.timestamp < self.now]
            result[symbol] = list(visible[-count:])
        return result

    @property
    def market_history_metadata(self) -> dict[str, Any]:
        return dict(self._engine.market_history_metadata)

    def current_bars(self) -> BarsView:
        return BarsView(self._engine._session_bars)

    def history_batch(
        self,
        symbols: Iterable[str],
        count: int = 20,
        timeframe: str | None = None,
    ) -> dict[str, list[Bar]]:
        normalized = list(dict.fromkeys(
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        ))
        if count <= 0 or not normalized:
            return {}
        cutoff = self.now
        period = timeframe or self.period
        if self._engine._history_batch_loader is not None and cutoff is not None:
            result = self._engine._history_batch_loader(normalized, count, period, cutoff)
            visible = {
                symbol: [bar for bar in result.get(symbol, []) if bar.timestamp <= cutoff][-count:]
                for symbol in normalized
            }
            self._engine.market_rows_consumed += sum(len(values) for values in visible.values())
            return visible
        return {
            symbol: list(self._engine._history_by_period.get(period, {}).get(symbol, [])[-count:])
            for symbol in normalized
        }

    def _sync(self, prices: dict[str, float]) -> None:
        self.portfolio.cash = self._engine.account.cash
        self.portfolio.positions = dict(self._engine.account.positions)
        self.portfolio.available_positions = dict(self._engine.account.available)
        self.portfolio.avg_cost = dict(self._engine.account.avg_cost)
        self.portfolio.total_value = self._engine.account.equity(prices)

    def schedule(
        self,
        callback: Callable[..., Any],
        at: str | datetime_time,
        *,
        symbols: Iterable[str] | Callable[["Context", datetime], Iterable[str]] | None = None,
        optional_symbols: (
            Iterable[str] | Callable[["Context", datetime], Iterable[str]] | None
        ) = None,
        when: Callable[["Context", datetime], bool] | None = None,
    ) -> None:
        if not callable(callback):
            raise ValueError("定时任务 callback 必须可调用")
        if when is not None and not callable(when):
            raise ValueError("定时任务 when 必须可调用")
        self._register_schedule(
            callback,
            ScheduleRule("daily", parse_time_expression(at)),
            symbols=symbols,
            optional_symbols=optional_symbols,
            when=when,
        )

    def _register_schedule(
        self,
        callback: Callable[..., Any],
        rule: ScheduleRule,
        *,
        symbols: Iterable[str] | Callable[["Context", datetime], Iterable[str]] | None = None,
        optional_symbols: (
            Iterable[str] | Callable[["Context", datetime], Iterable[str]] | None
        ) = None,
        when: Callable[["Context", datetime], bool] | None = None,
    ) -> None:
        if any(task.callback is callback and task.rule == rule for task in self._scheduled):
            return
        self._scheduled.append(RegisteredSchedule(
            callback=callback,
            rule=rule,
            condition=when,
            symbols=symbols,
            optional_symbols=optional_symbols,
        ))

    def run_daily(
        self,
        callback: Callable[..., Any],
        time: str = "every_bar",
        reference_security: str | None = None,
    ) -> None:
        self._register_schedule(
            callback,
            ScheduleRule("daily", parse_time_expression(time), reference_security=reference_security),
        )

    def run_weekly(
        self,
        callback: Callable[..., Any],
        weekday: int,
        time: str = "09:30",
        reference_security: str | None = None,
    ) -> None:
        self._register_schedule(
            callback,
            ScheduleRule(
                "weekly",
                parse_time_expression(time),
                ordinal=weekday,
                reference_security=reference_security,
            ),
        )

    def run_monthly(
        self,
        callback: Callable[..., Any],
        monthday: int,
        time: str = "09:30",
        reference_security: str | None = None,
    ) -> None:
        self._register_schedule(
            callback,
            ScheduleRule(
                "monthly",
                parse_time_expression(time),
                ordinal=monthday,
                reference_security=reference_security,
            ),
        )

    def unschedule_all(self) -> None:
        self._scheduled.clear()
        self._engine._scheduled_condition_results.clear()

    schedule_function = schedule

    def buy(self, symbol: str, quantity: float | None = None, value: float | None = None, **kwargs: Any) -> None:
        self._engine.submit_order("buy", symbol, quantity=quantity, value=value, reason=kwargs.get("reason", ""))

    def sell(self, symbol: str, quantity: float | None = None, value: float | None = None, **kwargs: Any) -> None:
        self._engine.submit_order("sell", symbol, quantity=quantity, value=value, reason=kwargs.get("reason", ""))

    def order_cash_weight(self, symbol: str, weight: float) -> None:
        normalized = float(weight)
        if not math.isfinite(normalized) or normalized <= 0:
            raise ValueError("现金分配权重必须是正数")
        self._engine.submit_order("buy", symbol, cash_weight=normalized)

    def order_target(self, symbol: str, quantity: float) -> None:
        self._engine.submit_order("target", symbol, target_quantity=quantity)

    order_target_quantity = order_target

    def order_target_value(self, symbol: str, value: float) -> None:
        self._engine.submit_order("target", symbol, target_value=value)

    def order_target_percent(self, symbol: str, percent: float) -> None:
        self._engine.submit_order("target", symbol, target_percent=percent)

    def history(
        self,
        symbol: str | None = None,
        count: int = 20,
        field: str = "close",
        timeframe: str | None = None,
    ) -> Any:
        values = self.history_bars(symbol, count=count, timeframe=timeframe)
        if field == "close":
            return [item.close for item in values]
        return [getattr(item, field) for item in values]

    def history_bars(
        self,
        symbol: str | None = None,
        count: int = 20,
        timeframe: str | None = None,
    ) -> list[Bar]:
        cutoff = self.now
        if self._engine._history_loader is not None and cutoff is not None:
            values = self._engine._history_loader(
                symbol or "", count, timeframe or self.period, cutoff,
            )
            visible = [bar for bar in values if bar.timestamp <= cutoff]
            self._engine.market_rows_consumed += len(visible)
            return list(visible[-count:])
        history = self._engine._history_by_period.get(timeframe or self.period, {})
        return list(history.get(symbol or "", [])[-count:])

    def extra_history(
        self,
        name: str,
        symbol: str,
        count: int = 1,
        end_date: date | datetime | str | None = None,
    ) -> list[dict[str, Any]]:
        if count <= 0 or self.now is None:
            return []
        normalized = str(symbol).strip().upper()
        normalized = self._normalize_symbol(normalized)
        if isinstance(end_date, datetime):
            requested_end = end_date.date()
        elif isinstance(end_date, date):
            requested_end = end_date
        elif end_date is not None:
            requested_end = date.fromisoformat(str(end_date))
        else:
            requested_end = self.now.date() - timedelta(days=1)
        cutoff = min(requested_end, self.now.date() - timedelta(days=1))
        values = self._engine.extra_history.get(name, {})
        actual_date = max(values.get(normalized, {}), default=None)
        if (
            self._engine._extra_history_loader is not None
            and (actual_date is None or actual_date < cutoff)
        ):
            load_start = self._engine.run_start or (cutoff - timedelta(days=365))
            self._engine._extra_history_loader(name, [normalized], load_start, cutoff)
            values = self._engine.extra_history.get(name, {})
        rows = sorted(
            (day, value)
            for day, value in values.get(normalized, {}).items()
            if day <= cutoff
        )
        return [
            {"date": day.isoformat(), "value": float(value)}
            for day, value in rows[-count:]
        ]

    @staticmethod
    def _normalize_symbol(raw: Any) -> str:
        symbol = str(raw).strip().upper()
        for source, target in {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}.items():
            if symbol.endswith(source):
                return f"{symbol[:-len(source)]}{target}"
        return symbol

    def log(self, message: str, level: str = "INFO") -> None:
        timestamp = self.now or cn_naive_now()
        self._engine.logs.append({
            "timestamp": timestamp.isoformat(),
            "level": str(level).upper(),
            "message": str(message),
            "source": "strategy",
        })

    def emit_signal(
        self,
        signal_type: str,
        payload: dict[str, Any],
        *,
        event_id: str | None = None,
    ) -> None:
        normalized = str(signal_type).strip()
        if not normalized:
            raise ValueError("信号类型不能为空")
        if not isinstance(payload, dict):
            raise ValueError("信号内容必须是字典")
        self._engine._signals.append({
            "id": str(event_id or f"{normalized}:{len(self._engine._signals) + 1}"),
            "timestamp": self.now.isoformat() if self.now else "",
            "signal_type": normalized,
            "payload": copy.deepcopy(payload),
        })

    info = log


class FreeStrategyEngine:
    def __init__(self, source: str, timeframe: str = "1d", config: FreeStrategyConfig | None = None,
                 state: dict[str, Any] | None = None,
                 instruments: Iterable[dict[str, Any]] | None = None,
                 instrument_loader: Callable[[str], Iterable[dict[str, Any]]] | None = None,
                 risk_config: RiskConfig | None = None,
                 callback_deadline: Any = None,
                 callback_label: Any = None) -> None:
        self.source = source
        self.timeframe = timeframe
        self.config = config or FreeStrategyConfig()
        self.risk_config = risk_config or RiskConfig()
        self.account = Account(self.config)
        self.state = state or {}
        self.logs: list[dict[str, Any]] = []
        self._signals: list[dict[str, Any]] = []
        self.history: dict[str, list[Bar]] = {}
        self._history_by_period: dict[str, dict[str, list[Bar]]] = {timeframe: self.history}
        self._market_history_by_period: dict[str, dict[str, list[Bar]]] = {}
        self._tradable_dates: set[tuple[str, date]] = set()
        self._market_dates: set[date] = set()
        self.market_history_metadata: dict[str, Any] = {"enabled": False}
        self._instruments = [dict(item) for item in (instruments or [])]
        self.pending: list[tuple[Order, datetime]] = []
        self._immediate: list[Order] = []
        self._bought_dates: dict[str, date] = {}
        self._counter = 0
        self._callbacks: dict[str, Callable[..., Any]] = {}
        self._callback_deadline = callback_deadline
        self._callback_label = callback_label
        self._history_loader: Callable[[str, int, str, datetime], list[Bar]] | None = None
        self._history_batch_loader: Callable[
            [list[str], int, str, datetime], dict[str, list[Bar]]
        ] | None = None
        self._scheduled_condition_results: dict[
            tuple[date, int], bool
        ] = {}
        self._trading_dates: tuple[date, ...] = ()
        self._market_history_loader: Callable[[datetime], None] | None = None
        self._active_session_date: date | None = None
        self._session_finished = False
        self._last_bars = BarsView()
        self._last_timestamp: datetime | None = None
        self._current_prices: dict[str, float] = {}
        self._current_close_prices: dict[str, float] = {}
        self._session_bars: dict[str, Bar] = {}
        self._session_daily_bars: dict[str, dict[str, Any]] = {}
        self._applied_splits: dict[str, date] = {}
        self._position_lots: dict[str, list[dict[str, Any]]] = {}
        self._benchmark_curve: list[dict[str, Any]] = []
        self._session_equity_snapshot: dict[str, Any] | None = None
        self._session_benchmark_close: float | None = None
        self._next_timestamp = cn_naive_now()
        self._order_times: deque[datetime] = deque()
        self._risk_peak_equity = float(self.config.initial_capital)
        self._risk_day: date | None = None
        self._risk_day_start_equity = float(self.config.initial_capital)
        self._risk_status: dict[str, Any] = {
            "daily_loss_locked": False,
            "drawdown_locked": False,
            "reason": None,
            "triggered_at": None,
        }
        self.callbacks_executed = 0
        self.market_rows_consumed = 0
        self.run_start: date | None = None
        self.run_end: date | None = None
        self.extra_history: dict[str, dict[str, dict[date, float]]] = {}
        self._extra_history_loader: Callable[[str, list[str], date, date], None] | None = None
        self._financial_snapshot_loader: Callable[[list[str], date], dict[str, dict[str, Any]]] | None = None
        self._dividend_ratio_loader: Callable[[list[str], date], list[str]] | None = None
        self._valuation_market_cap_loader: Callable[[list[str], date], dict[str, float]] | None = None
        self._smallcap_index_loader: Callable[[list[str], date], float | None] | None = None
        self._style_liquidity_loader: Callable[[date], dict[str, Any] | None] | None = None
        self._industry_history_loader: Callable[
            [list[str], date, str, str | int | None],
            dict[str, dict[str, Any]],
        ] | None = None
        self.context = Context(self)
        namespace: dict[str, Any] = {
            "__name__": "free_strategy_snapshot",
            "print": self._strategy_print,
            "run_daily": self.context.run_daily,
            "run_weekly": self.context.run_weekly,
            "run_monthly": self.context.run_monthly,
            "unschedule_all": self.context.unschedule_all,
        }
        # Trusted local execution is intentional for this feature: user scripts may import
        # installed packages and local modules. They run in a worker process at the API edge.
        self._protected_call(
            "策略加载",
            lambda: exec(compile(source, "<free_strategy>", "exec"), namespace, namespace),
        )
        callback_names = ("initialize", "before_trading_start", "on_bar", "on_quote", "after_trading_end")
        self._callbacks = {
            name: namespace[name]
            for name in callback_names
            if callable(namespace.get(name))
        }
        self.execution_mode = (
            "quote"
            if "on_quote" in self._callbacks
            else "full_bar"
            if "on_bar" in self._callbacks
            else "scheduled"
        )
        if instrument_loader is not None:
            self._instruments = [dict(item) for item in instrument_loader(self.execution_mode)]
        if "initialize" in self._callbacks:
            self._protected_call("initialize 回调", self._callbacks["initialize"], self.context)
        every_bar = any(
            task.resolved_time == "every_bar" for task in self.context._scheduled
        )
        if every_bar and self.execution_mode == "scheduled":
            self.execution_mode = "full_bar"
        if every_bar and timeframe == "1d":
            raise ValueError("every_bar 必须使用分钟级回测周期")
        if "on_bar" not in self._callbacks and "on_quote" not in self._callbacks and not self.context._scheduled:
            raise ValueError("策略必须定义 on_bar(context, bars)、on_quote(context, quotes) 或通过 context.schedule 注册定时任务")

    @property
    def universe(self) -> list[str]:
        return self.context.universe

    def _strategy_print(
        self,
        *values: Any,
        sep: str | None = " ",
        end: str | None = "\n",
        file: Any = None,
        flush: bool = False,
    ) -> None:
        if file is not None:
            builtins.print(*values, sep=sep, end=end, file=file, flush=flush)
            return
        buffer = io.StringIO()
        builtins.print(*values, sep=sep, end=end, file=buffer, flush=flush)
        message = buffer.getvalue()
        if message.endswith("\n"):
            message = message[:-1]
        self.context.log(message)

    @property
    def history_requirements(self) -> dict[str, int]:
        return self.context.history_requirements

    @property
    def market_history_requirements(self) -> dict[tuple[str, str], int]:
        return self.context.market_history_requirements

    @property
    def extra_history_requirements(self) -> set[str]:
        return self.context.extra_history_requirements

    @property
    def readiness_requirements(self) -> tuple[ReadinessRequirement, ...]:
        return self.context.readiness_requirements

    @property
    def scheduled_times(self) -> list[str]:
        return sorted({task.resolved_time for task in self.context._scheduled})

    def set_trading_calendar(self, values: Iterable[date]) -> None:
        self._trading_dates = tuple(sorted(set(values)))

    def scheduled_snapshot_symbols(self, timestamp: datetime) -> list[str] | None:
        return self._scheduled_scope_symbols(timestamp, include_optional=True)

    def scheduled_required_snapshot_symbols(self, timestamp: datetime) -> list[str] | None:
        return self._scheduled_scope_symbols(timestamp, include_optional=False)

    def _scheduled_scope_symbols(
        self,
        timestamp: datetime,
        *,
        include_optional: bool,
    ) -> list[str] | None:
        current_time = timestamp.strftime("%H:%M")
        due = [
            task
            for task in self.context._scheduled
            if task.resolved_time == current_time
        ]
        if not due:
            return []
        result: list[str] = []
        for task in due:
            if task.done or not self._scheduled_condition_met(task, timestamp):
                continue
            scope = task.symbols
            if scope is None:
                return None
            scopes = [("定时任务必需标的范围", scope)]
            optional_scope = task.optional_symbols
            if include_optional and optional_scope is not None:
                scopes.append(("定时任务可选标的范围", optional_scope))
            for label, current_scope in scopes:
                values = (
                    self._protected_call(
                        label, current_scope, self.context, timestamp,
                    )
                    if callable(current_scope) else current_scope
                )
                for raw in values:
                    symbol = str(raw).strip().upper()
                    if symbol and symbol not in result:
                        result.append(symbol)
        return result

    def _scheduled_condition_met(
        self,
        task: RegisteredSchedule,
        timestamp: datetime,
    ) -> bool:
        if not task.rule.matches_date(timestamp.date(), self._trading_dates):
            return False
        condition = task.condition
        if condition is None:
            return True
        key = (timestamp.date(), id(task))
        if key not in self._scheduled_condition_results:
            resolved = task.resolved_time
            scheduled_timestamp = (
                timestamp
                if resolved == "every_bar"
                else datetime.combine(timestamp.date(), datetime_time.fromisoformat(resolved))
            )
            self._scheduled_condition_results[key] = bool(self._protected_call(
                f"定时任务 {resolved} 执行条件",
                condition,
                self.context,
                scheduled_timestamp,
            ))
        return self._scheduled_condition_results[key]

    def prepare_scheduled_event(self, timestamp: datetime) -> bool:
        """跳过当日不生效的定时回调，并在无任务时推进执行游标。"""
        current_time = timestamp.strftime("%H:%M")
        active = False
        for task in self.context._scheduled:
            if task.done or task.resolved_time != current_time:
                continue
            if self._scheduled_condition_met(task, timestamp):
                active = True
                continue
            task.done = True
        if not active:
            self._next_timestamp = timestamp
            self._last_timestamp = timestamp
            self.context.now = timestamp
        return active

    @property
    def pending_orders(self) -> list[tuple[Order, datetime]]:
        return list(self.pending)

    @property
    def signals(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self._signals)

    def drain_signals(self) -> list[dict[str, Any]]:
        rows = self.signals
        self._signals.clear()
        return rows

    def set_history_loader(
        self,
        loader: Callable[[str, int, str, datetime], list[Bar]] | None,
    ) -> None:
        self._history_loader = loader

    def set_history_batch_loader(
        self,
        loader: Callable[
            [list[str], int, str, datetime], dict[str, list[Bar]]
        ] | None,
    ) -> None:
        self._history_batch_loader = loader

    def set_market_history_loader(
        self,
        loader: Callable[[datetime], None] | None,
    ) -> None:
        self._market_history_loader = loader

    def set_run_window(self, start: date, end: date) -> None:
        self.run_start = start
        self.run_end = end

    def set_extra_history(
        self,
        name: str,
        values: dict[str, dict[date, float]],
    ) -> None:
        self.extra_history.setdefault(name, {}).update(values)

    def set_extra_history_loader(
        self,
        loader: Callable[[str, list[str], date, date], None] | None,
    ) -> None:
        self._extra_history_loader = loader

    def set_financial_snapshot_loader(
        self,
        loader: Callable[[list[str], date], dict[str, dict[str, Any]]] | None,
    ) -> None:
        self._financial_snapshot_loader = loader

    def set_dividend_ratio_loader(
        self,
        loader: Callable[[list[str], date], list[str]] | None,
    ) -> None:
        self._dividend_ratio_loader = loader

    def set_valuation_market_cap_loader(
        self,
        loader: Callable[[list[str], date], dict[str, float]] | None,
    ) -> None:
        self._valuation_market_cap_loader = loader

    def set_smallcap_index_loader(
        self,
        loader: Callable[[list[str], date], float | None] | None,
    ) -> None:
        self._smallcap_index_loader = loader

    def set_style_liquidity_loader(
        self,
        loader: Callable[[date], dict[str, Any] | None] | None,
    ) -> None:
        self._style_liquidity_loader = loader

    def set_industry_history_loader(
        self,
        loader: Callable[
            [list[str], date, str, str | int | None],
            dict[str, dict[str, Any]],
        ] | None,
    ) -> None:
        self._industry_history_loader = loader

    def preload_history(self, bars: Iterable[Bar], timeframe: str = "1d") -> int:
        """注入只读历史，不触发生命周期、下单或资金变动。"""
        history = self._history_by_period.setdefault(timeframe, {})
        count = 0
        for bar in sorted(bars, key=lambda item: (item.timestamp, item.symbol)):
            values = history.setdefault(bar.symbol, [])
            if values:
                if values[-1].timestamp == bar.timestamp:
                    values[-1] = bar
                    continue
                if values[-1].timestamp > bar.timestamp:
                    continue
            values.append(bar)
            if len(values) > 5_000:
                del values[:-5_000]
            count += 1
        return count

    def preload_quote_snapshot(self, quotes: Iterable[Quote]) -> int:
        """更新重连快照估值，不触发策略回调或成交。"""
        values = list(quotes)
        prices = {quote.symbol: float(quote.last_price) for quote in values}
        self._current_prices.update(prices)
        self._current_close_prices.update(prices)
        self.context._sync(self._current_close_prices)
        return len(values)

    def preload_market_history(self, bars: Iterable[Bar], timeframe: str = "1d") -> int:
        history = self._market_history_by_period.setdefault(timeframe, {})
        count = 0
        for bar in sorted(bars, key=lambda item: (item.timestamp, item.symbol)):
            values = history.setdefault(bar.symbol, [])
            if values:
                if values[-1].timestamp == bar.timestamp:
                    values[-1] = bar
                    continue
                if values[-1].timestamp > bar.timestamp:
                    continue
            values.append(bar)
            self._market_dates.add(bar.timestamp.date())
            count += 1
        return count

    def preload_tradable_dates(self, values: Iterable[tuple[str, date]]) -> None:
        self._tradable_dates.update(values)

    def has_market_date(self, day: date) -> bool:
        return day in self._market_dates

    def submit_order(self, side: str, symbol: str, **kwargs: Any) -> None:
        self._counter += 1
        order = Order(id=f"o{self._counter}", symbol=symbol, side=side, submitted_at=self.context.now.isoformat() if self.context.now else "", **kwargs)
        self.account.orders.append(order)
        if self._reject_for_risk(order):
            return
        if self.config.fill_policy in {"close", "current_close"}:
            self._immediate.append(order)
        else:
            self.pending.append((order, self._next_timestamp))

    def _order_increases_risk(self, order: Order) -> bool:
        if order.side == "buy":
            return True
        if order.side == "sell":
            return False
        current = self.account.positions.get(order.symbol, 0.0)
        price = max(self._current_close_prices.get(order.symbol, 0.0), 0.01)
        target = order.target_quantity
        if target is None and order.target_value is not None:
            target = order.target_value / price
        if target is None and order.target_percent is not None:
            target = self.account.equity(self._current_close_prices) * order.target_percent / price
        return (target or 0.0) > current

    def _reject_for_risk(self, order: Order) -> bool:
        now = self.context.now or cn_naive_now()
        cutoff = now.timestamp() - 60
        while self._order_times and self._order_times[0].timestamp() <= cutoff:
            self._order_times.popleft()
        increases_risk = self._order_increases_risk(order)
        if increases_risk and len(self._order_times) >= self.risk_config.max_orders_per_minute:
            order.status = "rejected"
            order.reason = "统一风控：每分钟委托数已达上限"
            return True
        self._order_times.append(now)
        if increases_risk and (
            self._risk_status["daily_loss_locked"] or self._risk_status["drawdown_locked"]
        ):
            order.status = "rejected"
            order.reason = f"统一风控：{self._risk_status['reason'] or '账户已锁定'}"
            return True
        return False

    @property
    def risk_status(self) -> dict[str, Any]:
        return dict(self._risk_status)

    def unlock_drawdown_risk(self) -> None:
        self._risk_status["drawdown_locked"] = False
        if not self._risk_status["daily_loss_locked"]:
            self._risk_status["reason"] = None
            self._risk_status["triggered_at"] = None

    def _update_risk(self, timestamp: datetime) -> None:
        equity = self.account.equity(self._current_close_prices)
        if self._risk_day != timestamp.date():
            self._risk_day = timestamp.date()
            self._risk_day_start_equity = equity
            self._risk_status["daily_loss_locked"] = False
            if not self._risk_status["drawdown_locked"]:
                self._risk_status["reason"] = None
                self._risk_status["triggered_at"] = None
        self._risk_peak_equity = max(self._risk_peak_equity, equity)
        daily_loss = (self._risk_day_start_equity - equity) / self._risk_day_start_equity if self._risk_day_start_equity else 0.0
        drawdown = (self._risk_peak_equity - equity) / self._risk_peak_equity if self._risk_peak_equity else 0.0
        reason = None
        if daily_loss >= self.risk_config.daily_loss_pct:
            self._risk_status["daily_loss_locked"] = True
            reason = "日亏损达到限制"
        if drawdown >= self.risk_config.max_drawdown_pct:
            self._risk_status["drawdown_locked"] = True
            reason = "最大回撤达到限制"
        if reason:
            self._risk_status["reason"] = reason
            self._risk_status["triggered_at"] = timestamp.isoformat()
            for order, _ in self.pending:
                if self._order_increases_risk(order):
                    order.status = "cancelled"
                    order.reason = f"统一风控：{reason}"
            self.pending = [item for item in self.pending if item[0].status == "pending"]

    def _sell_first(self, item: tuple[Order, datetime] | Order) -> int:
        order = item[0] if isinstance(item, tuple) else item
        if order.side == "sell":
            return 0
        if order.side == "target":
            current = self.account.positions.get(order.symbol, 0.0)
            target = order.target_quantity
            if target is None and order.target_value is not None:
                target = order.target_value / max(self._current_prices.get(order.symbol, 1.0), 0.01)
            if target is None and order.target_percent is not None:
                target = (
                    self.account.equity(self._current_close_prices)
                    * order.target_percent
                    / max(self._current_prices.get(order.symbol, 1.0), 0.01)
                )
            if target is not None and target < current:
                return 0
        return 2 if order.cash_weight is not None else 1

    def _fill_orders(
        self,
        orders: Iterable[tuple[Order, datetime] | Order],
        bars: BarsView,
        timestamp: datetime,
        field: str,
    ) -> None:
        values = [item[0] if isinstance(item, tuple) else item for item in orders]
        ordered = sorted(values, key=self._sell_first)
        weighted = [order for order in ordered if order.cash_weight is not None]
        for order in ordered:
            if order.cash_weight is None:
                self._fill_order(order, bars.get(order.symbol), timestamp, field)
        if not weighted:
            return
        cash_budget = max(0.0, self.account.cash)
        total_weight = sum(float(order.cash_weight or 0.0) for order in weighted)
        for order in weighted:
            order.value = cash_budget * float(order.cash_weight or 0.0) / total_weight
            self._fill_order(order, bars.get(order.symbol), timestamp, field)

    def _fill_immediate_orders(
        self,
        bars: BarsView,
        timestamp: datetime,
        field: str = "close",
    ) -> None:
        self._fill_orders(self._immediate, bars, timestamp, field)
        self._immediate.clear()

    def _fill_order(
        self,
        order: Order,
        bar: Bar | None,
        timestamp: datetime | None,
        field: str,
    ) -> None:
        if bar is None and self.config.allow_stale_fills and timestamp is not None:
            trades_today = (order.symbol, timestamp.date()) in self._tradable_dates
            price = self._current_close_prices.get(order.symbol, 0.0)
            if trades_today and price > 0:
                bar = Bar(order.symbol, timestamp, price, price, price, price)
        if bar is None:
            order.status = "rejected"
            order.reason = "证券停牌或无可交易行情"
            return
        raw_price = bar.execution_price(field)
        if raw_price <= 0:
            order.status = "rejected"
            order.reason = "缺少可成交价格"
            return
        current = self.account.positions.get(order.symbol, 0.0)
        target = None
        if order.side == "target":
            target = order.target_quantity
            if target is None and order.target_value is not None:
                target = order.target_value / raw_price
            if target is None and order.target_percent is not None:
                target = self.account.equity(self._current_close_prices) * order.target_percent / raw_price
            side = "buy" if (target or 0) > current else "sell"
            qty = abs((target or 0) - current)
        else:
            side = order.side
            qty = order.quantity
            if qty is None and order.value is not None:
                qty = order.value / raw_price
            qty = qty or 0
        if bar.suspended or not bar.tradable:
            order.status = "rejected"
            order.reason = "证券停牌或不可交易"
            return
        if side == "buy" and bar.limit_up is not None and raw_price >= bar.limit_up - 0.005:
            order.status = "rejected"
            order.reason = "涨停，买入未成交"
            return
        if side == "sell" and bar.limit_down is not None and raw_price <= bar.limit_down + 0.005:
            order.status = "rejected"
            order.reason = "跌停，卖出未成交"
            return
        price = raw_price * (1 + (self.config.slippage_bps / 10000) * (1 if side == "buy" else -1))
        if self.config.price_tick is not None:
            tick = self.config.price_tick
            price = math.floor(price / tick + 0.5 + 1e-10) * tick
        if side == "buy" and bar.limit_up is not None:
            price = min(price, bar.limit_up)
        elif side == "sell" and bar.limit_down is not None:
            price = max(price, bar.limit_down)
        buy_commission_rate = self.config.commission_pct if self.config.commission_pct is not None else self.config.fees_pct
        commission_rate = (
            self.config.sell_commission_pct
            if side == "sell" and self.config.sell_commission_pct is not None
            else buy_commission_rate
        )
        lot = max(1, self.config.lot_size)
        liquidates_position = (
            side == "sell"
            and order.side == "target"
            and target is not None
            and target <= 0
        )
        if not liquidates_position:
            qty = math.floor(qty / lot) * lot
        if side == "sell":
            bought_today = (
                self.config.settlement == "t1"
                and order.symbol not in self.config.t0_symbols
                and self._bought_dates.get(order.symbol) == (timestamp.date() if timestamp else self.context.now.date())
            )
            available = self.account.available.get(order.symbol, 0.0 if bought_today else current)
            qty = min(qty, available)
        else:
            max_gross = self.account.equity(self._current_close_prices) * self.config.max_exposure_pct
            symbol_gross = max(
                0.0,
                self.account.equity(self._current_close_prices) * self.risk_config.max_symbol_exposure_pct
                - current * raw_price,
            )
            available = max(0.0, self.account.cash)
            if order.cash_weight is not None and order.value is not None:
                available = min(available, max(0.0, order.value))
            cash_gross = min(
                max(0.0, available - self.config.min_commission),
                available / (1 + commission_rate),
            )
            qty = min(qty, math.floor(max(0.0, min(cash_gross, max_gross, symbol_gross)) / price / lot) * lot)
        if qty <= 0:
            if order.side == "target":
                order.status = "skipped"
                order.reason = "目标仓位无需调整或不足一手"
            else:
                order.status = "rejected"
                order.reason = "数量不足、现金不足或 T+1 未结算"
            return
        price_decimal = Decimal(str(price))
        if self.config.price_tick is not None:
            price_decimal = price_decimal.quantize(Decimal(str(self.config.price_tick)))
        while True:
            gross_decimal = Decimal(str(qty)) * price_decimal
            commission = max(
                Decimal(str(self.config.min_commission)),
                gross_decimal * Decimal(str(commission_rate)),
            )
            stamp_tax = (
                gross_decimal * Decimal(str(self.config.stamp_tax_pct))
                if side == "sell" and self.config.asset_type == "stock"
                else Decimal(0)
            )
            transfer_fee = (
                gross_decimal * Decimal(str(self.config.transfer_fee_pct))
                if self.config.asset_type == "stock"
                else Decimal(0)
            )
            fee_decimal = commission + stamp_tax + transfer_fee
            if side != "buy" or gross_decimal + fee_decimal <= Decimal(str(max(0.0, available))):
                break
            qty -= lot
            if qty <= 0:
                if order.side == "target":
                    order.status = "skipped"
                    order.reason = "目标仓位无需调整或不足一手"
                else:
                    order.status = "rejected"
                    order.reason = "数量不足、现金不足或 T+1 未结算"
                return
        gross = float(gross_decimal)
        fee = float(fee_decimal)
        dividend_tax = 0.0
        if side == "buy":
            self.account.cash = max(0.0, self.account.cash - gross - fee)
            old = self.account.positions.get(order.symbol, 0.0)
            self.account.avg_cost[order.symbol] = ((old * self.account.avg_cost.get(order.symbol, price)) + gross + fee) / (old + qty)
            if old <= 0 and order.symbol in self.account.positions:
                del self.account.positions[order.symbol]
            self.account.positions[order.symbol] = old + qty
            acquired = timestamp.date() if timestamp else self.context.now.date()
            self._position_lots.setdefault(order.symbol, []).append({
                "quantity": qty,
                "acquired": acquired,
                "dividend_per_share": 0.0,
            })
            if self.config.settlement == "t0" or order.symbol in self.config.t0_symbols:
                self.account.available[order.symbol] = self.account.positions[order.symbol]
            else:
                self.account.available.setdefault(order.symbol, 0.0)
                self._bought_dates[order.symbol] = (timestamp.date() if timestamp else self.context.now.date())
        else:
            dividend_tax = self._consume_position_lots(
                order.symbol,
                qty,
                timestamp.date() if timestamp else self.context.now.date(),
            )
            self.account.cash += gross - fee - dividend_tax
            self.account.positions[order.symbol] = max(0.0, current - qty)
            self.account.available[order.symbol] = max(0.0, self.account.available.get(order.symbol, current) - qty)
            if dividend_tax > 0:
                self.account.corporate_actions.append({
                    "timestamp": timestamp.isoformat() if timestamp else "",
                    "symbol": order.symbol,
                    "type": "dividend_tax",
                    "tax_withheld": dividend_tax,
                })
        order.status = "filled"
        market_volume = float(bar.volume) if bar.volume > 0 else None
        self.account.fills.append(Fill(
            order.id,
            order.symbol,
            side,
            qty,
            price,
            gross,
            fee,
            timestamp.isoformat() if timestamp else "",
            market_amount=float(bar.amount) if bar.amount > 0 else None,
            market_volume=market_volume,
            participation_pct=qty / market_volume * 100 if market_volume else None,
            commission=float(commission),
            stamp_tax=float(stamp_tax),
            transfer_fee=float(transfer_fee),
            dividend_tax=float(dividend_tax),
            total_fee=fee + float(dividend_tax),
            status=order.status,
            reason=order.reason,
            submitted_at=order.submitted_at,
        ))

    @staticmethod
    def _dividend_tax_rate(acquired: date, sold: date) -> float:
        held_days = (sold - acquired).days
        if held_days <= 30:
            return 0.2
        if held_days <= 365:
            return 0.1
        return 0.0

    def _consume_position_lots(self, symbol: str, quantity: float, sold: date) -> float:
        remaining = quantity
        tax = 0.0
        lots = self._position_lots.get(symbol, [])
        while remaining > 1e-9 and lots:
            lot = lots[0]
            consumed = min(remaining, float(lot["quantity"]))
            if self.config.asset_type == "stock":
                tax += (
                    consumed
                    * float(lot.get("dividend_per_share", 0.0))
                    * self._dividend_tax_rate(lot["acquired"], sold)
                )
            lot["quantity"] = float(lot["quantity"]) - consumed
            remaining -= consumed
            if float(lot["quantity"]) <= 1e-9:
                lots.pop(0)
        if not lots:
            self._position_lots.pop(symbol, None)
        return float(Decimal(str(tax)).quantize(Decimal("0.01")))

    def _restore_position_lots(self) -> None:
        self._position_lots = {}
        events = [
            (fill.timestamp, 1, "fill", fill)
            for fill in self.account.fills
        ] + [
            (str(action.get("timestamp", "")), 0, "action", action)
            for action in self.account.corporate_actions
        ]
        for timestamp, _, event_type, event in sorted(events, key=lambda item: (item[0], item[1])):
            if event_type == "fill":
                fill = event
                if fill.side == "buy":
                    self._position_lots.setdefault(fill.symbol, []).append({
                        "quantity": float(fill.quantity),
                        "acquired": datetime.fromisoformat(timestamp).date(),
                        "dividend_per_share": 0.0,
                    })
                else:
                    self._consume_position_lots(
                        fill.symbol,
                        float(fill.quantity),
                        datetime.fromisoformat(timestamp).date(),
                    )
                continue
            action = event
            symbol = str(action.get("symbol", ""))
            if action.get("type") == "cash_dividend":
                cash_per_share = float(action.get("cash_per_share", 0.0))
                for lot in self._position_lots.get(symbol, []):
                    lot["dividend_per_share"] = (
                        float(lot.get("dividend_per_share", 0.0)) + cash_per_share
                    )
            elif action.get("type") == "split":
                ratio = float(action.get("ratio", 1.0))
                if ratio <= 0:
                    continue
                for lot in self._position_lots.get(symbol, []):
                    lot["quantity"] = float(lot["quantity"]) * ratio
                    lot["dividend_per_share"] = (
                        float(lot.get("dividend_per_share", 0.0)) / ratio
                    )
        for symbol, position in self.account.positions.items():
            missing = float(position) - sum(
                float(lot["quantity"])
                for lot in self._position_lots.get(symbol, [])
            )
            if missing > 1e-9:
                self._position_lots.setdefault(symbol, []).append({
                    "quantity": missing,
                    "acquired": date.min,
                    "dividend_per_share": 0.0,
                })

    def _apply_splits(self, bars: BarsView, timestamp: datetime) -> None:
        for symbol, bar in bars.items():
            ratio = float(bar.split_ratio)
            cash_dividend = float(bar.cash_dividend)
            has_split = ratio > 0 and not math.isclose(
                ratio, 1.0, rel_tol=0.0, abs_tol=1e-9,
            )
            effective_ratio = ratio if has_split else 1.0
            if not has_split and cash_dividend <= 0:
                continue
            if self._applied_splits.get(symbol) == timestamp.date():
                continue
            self._applied_splits[symbol] = timestamp.date()
            quantity = self.account.positions.get(symbol, 0.0)
            if quantity <= 0:
                continue
            if cash_dividend > 0:
                cash_received = quantity * cash_dividend
                self.account.cash += cash_received
                for lot in self._position_lots.get(symbol, []):
                    lot["dividend_per_share"] = (
                        float(lot.get("dividend_per_share", 0.0)) + cash_dividend
                    )
                self.account.corporate_actions.append({
                    "timestamp": timestamp.isoformat(),
                    "symbol": symbol,
                    "type": "cash_dividend",
                    "cash_per_share": cash_dividend,
                    "cash_received": cash_received,
                })
            if has_split:
                self.account.positions[symbol] = quantity * ratio
                self.account.available[symbol] = self.account.available.get(symbol, 0.0) * ratio
                for lot in self._position_lots.get(symbol, []):
                    lot["quantity"] = float(lot["quantity"]) * ratio
                    lot["dividend_per_share"] = (
                        float(lot.get("dividend_per_share", 0.0)) / ratio
                    )
            if symbol in self.account.avg_cost:
                self.account.avg_cost[symbol] = (
                    self.account.avg_cost[symbol] - cash_dividend
                ) / effective_ratio
            if has_split:
                self.account.corporate_actions.append({
                    "timestamp": timestamp.isoformat(),
                    "symbol": symbol,
                    "type": "split",
                    "ratio": ratio,
                })
                self.logs.append({
                    "timestamp": timestamp.isoformat(),
                    "level": "INFO",
                    "message": f"{symbol} 拆分/送转生效：持仓数量按 {ratio:g} 倍调整",
                    "source": "engine",
                })

    def _run_callback(self, name: str, bars: BarsView) -> None:
        callback = self._callbacks.get(name)
        if callback is None:
            return
        if name == "on_bar":
            self._protected_call(f"{name} 回调", callback, self.context, bars)
        else:
            self._protected_call(f"{name} 回调", callback, self.context)

    def _run_scheduled_callback(self, callback: Callable[..., Any], at: str) -> None:
        self._protected_call(f"定时回调 {at}", callback, self.context)
        self.callbacks_executed += 1

    def _protected_call(self, label: str, callback: Callable[..., Any], *args: Any) -> Any:
        timeout = float(self.config.callback_timeout_seconds)
        started = time.monotonic()
        if self._callback_label is not None:
            with self._callback_label.get_lock():
                self._callback_label.get_obj().value = label
        if self._callback_deadline is not None:
            with self._callback_deadline.get_lock():
                self._callback_deadline.value = started + timeout
        try:
            return callback(*args)
        finally:
            elapsed = time.monotonic() - started
            if self._callback_deadline is not None:
                with self._callback_deadline.get_lock():
                    self._callback_deadline.value = 0.0
            if self._callback_label is not None:
                with self._callback_label.get_lock():
                    self._callback_label.get_obj().value = ""
            if elapsed > timeout:
                raise TimeoutError(f"{label}超过 {timeout:g} 秒（实际 {elapsed:.1f} 秒）")

    def runtime_snapshot(self) -> dict[str, Any]:
        """保存模拟盘恢复所需的会话边界，避免盘后任务重复执行。"""
        return {
            "session_date": self._active_session_date.isoformat() if self._active_session_date else None,
            "session_finished": self._session_finished,
            "last_timestamp": self._last_timestamp.isoformat() if self._last_timestamp else None,
            "context_time": self.context.now.isoformat() if self.context.now else None,
            "scheduled_completed": [task.done for task in self.context._scheduled],
            "callbacks_executed": self.callbacks_executed,
            "market_rows_consumed": self.market_rows_consumed,
        }

    def restore_runtime(self, raw: dict[str, Any] | None) -> None:
        raw = raw or {}
        session_date = raw.get("session_date")
        last_timestamp = raw.get("last_timestamp")
        self._active_session_date = date.fromisoformat(session_date) if session_date else None
        self._session_finished = bool(raw.get("session_finished", False))
        self._last_timestamp = datetime.fromisoformat(last_timestamp) if last_timestamp else None
        context_time = raw.get("context_time") or last_timestamp
        self.context.now = datetime.fromisoformat(context_time) if context_time else None
        completed = list(raw.get("scheduled_completed", []))
        if completed:
            for index, task in enumerate(self.context._scheduled):
                task.done = bool(completed[index]) if index < len(completed) else False
        self.callbacks_executed = int(raw.get("callbacks_executed", self.callbacks_executed))
        self.market_rows_consumed = int(raw.get("market_rows_consumed", self.market_rows_consumed))

    def checkpoint(self) -> dict[str, Any]:
        """返回可用于分段历史回测或模拟盘恢复的完整运行状态。"""
        return {
            "account": self.account.snapshot(),
            "state": self.context.state,
            "runtime": self.runtime_snapshot(),
            "universe": self.universe,
            "benchmark_curve": self._benchmark_curve,
            "bought_dates": {symbol: value.isoformat() for symbol, value in self._bought_dates.items()},
            "applied_splits": {symbol: value.isoformat() for symbol, value in self._applied_splits.items()},
            "position_lots": {
                symbol: [
                    {
                        **lot,
                        "acquired": lot["acquired"].isoformat(),
                    }
                    for lot in lots
                ]
                for symbol, lots in self._position_lots.items()
            },
            "pending_orders": [
                {"order_id": order.id, "due_at": due_at.isoformat()}
                for order, due_at in self.pending
            ],
            "order_counter": self._counter,
            "risk": {
                "status": self.risk_status,
                "peak_equity": self._risk_peak_equity,
                "day": self._risk_day.isoformat() if self._risk_day else None,
                "day_start_equity": self._risk_day_start_equity,
                "order_times": [value.isoformat() for value in self._order_times],
            },
        }

    def restore_checkpoint(self, raw: dict[str, Any]) -> None:
        self.account.restore(raw.get("account", {}))
        self.context.state = copy.deepcopy(raw.get("state", self.context.state))
        self.state = copy.deepcopy(self.context.state)
        if raw.get("universe"):
            self.context.set_universe(raw["universe"])
        self.restore_runtime(raw.get("runtime"))
        self._benchmark_curve = list(raw.get("benchmark_curve", []))
        self._bought_dates = {
            symbol: date.fromisoformat(value)
            for symbol, value in raw.get("bought_dates", {}).items()
        }
        self._applied_splits = {
            symbol: date.fromisoformat(value)
            for symbol, value in raw.get("applied_splits", {}).items()
        }
        if "position_lots" in raw:
            self._position_lots = {
                symbol: [
                    {
                        **lot,
                        "quantity": float(lot["quantity"]),
                        "acquired": date.fromisoformat(str(lot["acquired"])),
                        "dividend_per_share": float(lot.get("dividend_per_share", 0.0)),
                    }
                    for lot in lots
                ]
                for symbol, lots in raw.get("position_lots", {}).items()
            }
        else:
            self._restore_position_lots()
        orders_by_id = {order.id: order for order in self.account.orders}
        self.pending = [
            (orders_by_id[item["order_id"]], datetime.fromisoformat(item["due_at"]))
            for item in raw.get("pending_orders", [])
            if item.get("order_id") in orders_by_id
        ]
        self._counter = int(raw.get("order_counter", len(self.account.orders)))
        risk = raw.get("risk", {})
        self._risk_status.update(risk.get("status", {}))
        self._risk_peak_equity = float(risk.get("peak_equity", self._risk_peak_equity))
        self._risk_day = date.fromisoformat(risk["day"]) if risk.get("day") else None
        self._risk_day_start_equity = float(risk.get("day_start_equity", self._risk_day_start_equity))
        self._order_times = deque(datetime.fromisoformat(value) for value in risk.get("order_times", []))

    def _start_session(self, timestamp: datetime, bars_now: BarsView) -> None:
        self._active_session_date = timestamp.date()
        self._session_finished = False
        for task in self.context._scheduled:
            task.done = False
        self._scheduled_condition_results.clear()
        self.context.now = timestamp
        self._session_bars = dict(bars_now)
        self._session_daily_bars = {}
        self._session_equity_snapshot = None
        self._session_benchmark_close = None
        if self.config.settlement == "t1":
            for symbol, bought_date in list(self._bought_dates.items()):
                if bought_date < timestamp.date():
                    self.account.available[symbol] = self.account.positions.get(symbol, 0.0)
                    self._bought_dates.pop(symbol, None)
        self.context._sync(self._current_close_prices)
        self._run_callback("before_trading_start", bars_now)

    def _advance_scheduled_before(self, timestamp: datetime) -> None:
        current = timestamp.strftime("%H:%M")
        due_times = sorted({
            task.resolved_time
            for task in self.context._scheduled
            if not task.done
            and task.resolved_time != "every_bar"
            and task.resolved_time < current
        })
        for at in due_times:
            scheduled_timestamp = datetime.combine(
                timestamp.date(), datetime_time.fromisoformat(at),
            )
            self.advance_event(
                scheduled_timestamp,
                event_type="scheduled",
                scheduled_at=at,
                run_prior_schedules=False,
            )

    def _run_scheduled_at(
        self,
        timestamp: datetime,
        *,
        actual_bar: bool = False,
        scheduled_at: str | None = None,
    ) -> None:
        current = scheduled_at or timestamp.strftime("%H:%M")
        for task in self.context._scheduled:
            at = task.resolved_time
            if at == "every_bar":
                if actual_bar and self._scheduled_condition_met(task, timestamp):
                    self.context.now = timestamp
                    self._run_scheduled_callback(task.callback, at)
                continue
            if task.done or at != current:
                continue
            self.context.now = timestamp
            if self._scheduled_condition_met(task, timestamp):
                self._run_scheduled_callback(task.callback, at)
            task.done = True

    def _accumulate_session_daily_bars(self, bars_now: BarsView) -> None:
        if self.timeframe == "1d":
            return
        for symbol, bar in bars_now.items():
            previous = self._session_daily_bars.get(symbol)
            if previous is None:
                self._session_daily_bars[symbol] = {
                    "symbol": symbol,
                    "timestamp": bar.timestamp,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "amount": bar.amount,
                    "raw_open": bar.execution_price("open"),
                    "raw_high": bar.execution_price("high"),
                    "raw_low": bar.execution_price("low"),
                    "raw_close": bar.execution_price("close"),
                    "tradable": bar.tradable,
                    "suspended": bar.suspended,
                    "limit_up": bar.limit_up,
                    "limit_down": bar.limit_down,
                    "split_ratio": bar.split_ratio,
                    "cash_dividend": bar.cash_dividend,
                }
                continue
            previous["timestamp"] = bar.timestamp
            previous["high"] = max(previous["high"], bar.high)
            previous["low"] = min(previous["low"], bar.low)
            previous["close"] = bar.close
            previous["volume"] += bar.volume
            previous["amount"] += bar.amount
            previous["raw_high"] = max(previous["raw_high"], bar.execution_price("high"))
            previous["raw_low"] = min(previous["raw_low"], bar.execution_price("low"))
            previous["raw_close"] = bar.execution_price("close")
            previous["tradable"] = previous["tradable"] or bar.tradable
            previous["suspended"] = previous["suspended"] and bar.suspended
            previous["limit_up"] = bar.limit_up if bar.limit_up is not None else previous["limit_up"]
            previous["limit_down"] = bar.limit_down if bar.limit_down is not None else previous["limit_down"]
            previous["split_ratio"] = max(previous["split_ratio"], bar.split_ratio)
            previous["cash_dividend"] = max(previous["cash_dividend"], bar.cash_dividend)

    def begin_session(self, day: date) -> None:
        """在读取当日行情前执行盘前回调，供策略动态设置当日订阅标的。"""
        if self._active_session_date == day:
            return
        self.finish_session()
        timestamp = datetime.combine(day, datetime.min.time()).replace(hour=9, minute=29)
        self._start_session(timestamp, BarsView())

    def finish_session(self, *, persist_state: bool = True) -> bool:
        """执行当日盘后任务；同一会话重复调用不会重复执行。"""
        if self._active_session_date is None or self._session_finished:
            return False
        timestamp = self._last_timestamp or datetime.combine(
            self._active_session_date,
            datetime_time(15, 0),
        )
        if self.execution_mode != "scheduled":
            self._advance_scheduled_before(
                datetime.combine(self._active_session_date, datetime_time(23, 59)),
            )
            timestamp = self._last_timestamp or timestamp
        self.context.now = timestamp
        self._run_callback("after_trading_end", self._last_bars)
        self._fill_immediate_orders(self._session_bars, timestamp)
        if self.timeframe != "1d":
            self.preload_history((Bar(**values) for values in self._session_daily_bars.values()), "1d")
        if self._session_equity_snapshot is not None:
            position_values = {
                symbol: quantity * self._current_close_prices.get(symbol, self.account.avg_cost.get(symbol, 0.0))
                for symbol, quantity in self.account.positions.items()
                if quantity > 0
            }
            self.account.equity_curve.append({
                "timestamp": timestamp.isoformat(),
                "equity": self.account.equity(self._current_close_prices),
                "cash": self.account.cash,
                "positions": dict(self.account.positions),
                "avg_cost": dict(self.account.avg_cost),
                "position_values": position_values,
            })
        if self._session_benchmark_close is not None:
            self._benchmark_curve.append({"timestamp": timestamp.isoformat(), "close": self._session_benchmark_close})
        if persist_state:
            self.state = copy.deepcopy(self.context.state)
        self._session_finished = True
        return True

    def _publish_market(
        self,
        timestamp: datetime,
        bars_now: BarsView,
        *,
        record_history: bool,
    ) -> None:
        self._current_close_prices.update({symbol: bar.execution_price("close") for symbol, bar in bars_now.items()})
        self.context.now = timestamp
        self.context._sync(self._current_close_prices)
        current_bar = next(iter(bars_now.values()), None)
        if current_bar is not None:
            self._current_bar = current_bar
        benchmark = bars_now.get(self.config.benchmark_symbol)
        if benchmark is not None:
            self._session_benchmark_close = benchmark.close
        if bars_now:
            self._last_bars = bars_now
            self._session_bars.update(bars_now)
        self._last_timestamp = timestamp
        if record_history:
            self._accumulate_session_daily_bars(bars_now)
            for symbol, bar in bars_now.items():
                self.history.setdefault(symbol, []).append(bar)
                if len(self.history[symbol]) > 5_000:
                    del self.history[symbol][:-5_000]
        self._session_equity_snapshot = {
            "timestamp": timestamp.isoformat(),
            "equity": self.account.equity(self._current_close_prices),
            "cash": self.account.cash,
            "positions": dict(self.account.positions),
        }
        self._update_risk(timestamp)

    @staticmethod
    def _quote_bar(quote: Quote) -> Bar:
        price = float(quote.last_price)
        return Bar(
            symbol=quote.symbol,
            timestamp=quote.timestamp,
            open=float(quote.open if quote.open is not None else price),
            high=float(quote.high if quote.high is not None else price),
            low=float(quote.low if quote.low is not None else price),
            close=price,
            volume=float(quote.volume),
            amount=float(quote.amount),
            raw_open=price,
            raw_high=price,
            raw_low=price,
            raw_close=price,
            tradable=not quote.suspended and price > 0,
            suspended=quote.suspended,
            limit_up=quote.limit_up,
            limit_down=quote.limit_down,
        )

    def advance_event(
        self,
        timestamp: datetime,
        bars: Iterable[Bar] = (),
        *,
        event_type: EventType,
        quotes: Iterable[Quote] = (),
        scheduled_at: str | None = None,
        run_prior_schedules: bool = True,
    ) -> None:
        """Advance one deterministic market event through the shared state machine."""
        if event_type not in {"bar", "quote", "scheduled", "fill", "market"}:
            raise ValueError(f"不支持的事件类型: {event_type}")
        quote_values = list(quotes)
        values = list(bars)
        if event_type == "quote":
            if not quote_values:
                return
            values = [self._quote_bar(quote) for quote in quote_values]
        elif quote_values:
            raise ValueError("只有 quote 事件可以携带 quotes")
        bars_now = BarsView({bar.symbol: bar for bar in values})
        quote_view = QuotesView({quote.symbol: quote for quote in quote_values})
        if self._active_session_date != timestamp.date():
            self.finish_session()
            premarket = datetime.combine(timestamp.date(), datetime.min.time()).replace(
                hour=9,
                minute=29,
            )
            self._start_session(premarket, BarsView())
        if run_prior_schedules and event_type in {"bar", "quote"}:
            self._advance_scheduled_before(timestamp)
        self.market_rows_consumed += len(values)
        self._next_timestamp = timestamp
        self.context.now = timestamp

        # 1. Settlement is completed by _start_session; corporate actions are first.
        self._apply_splits(bars_now, timestamp)
        self._current_prices.update({symbol: bar.execution_price("open") for symbol, bar in bars_now.items()})

        # 2. Orders waiting for the next tradable open are matched before publication.
        due = [
            item
            for item in self.pending
            if item[1] < timestamp and item[0].symbol in bars_now
        ]
        self.pending = [
            item for item in self.pending
            if item[1] >= timestamp or item[0].symbol not in bars_now
        ]
        self._fill_orders(due, bars_now, timestamp, "open")

        # 3. Publish the market snapshot and history visible at this timestamp.
        self._publish_market(
            timestamp,
            bars_now,
            record_history=event_type == "bar",
        )

        # 4. Primary market callback.
        if event_type == "bar":
            self._run_callback("on_bar", bars_now)
        elif event_type == "quote":
            callback = self._callbacks.get("on_quote")
            if callback is not None:
                self._protected_call("on_quote 回调", callback, self.context, quote_view)
                self.callbacks_executed += 1

        # 5. Scheduled callbacks keep their registration order.
        if event_type in {"bar", "quote", "scheduled"}:
            self._run_scheduled_at(
                timestamp,
                actual_bar=event_type == "bar",
                scheduled_at=scheduled_at,
            )

        # 6. Current-price orders are matched after every callback at this timestamp.
        if bars_now:
            self._fill_immediate_orders(bars_now, timestamp)

        # 7. Record the complete post-event state used by checkpoints and paper mode.
        self.context._sync(self._current_close_prices)
        self._session_equity_snapshot = {
            "timestamp": timestamp.isoformat(),
            "equity": self.account.equity(self._current_close_prices),
            "cash": self.account.cash,
            "positions": dict(self.account.positions),
        }
        self.state = copy.deepcopy(self.context.state)

    def run(
        self,
        bars: Iterable[Bar],
        *,
        finalize_session: bool = True,
        return_result: bool = True,
    ) -> dict[str, Any] | None:
        """按时间回放 bar。

        分钟回测会一次处理数百万根 bar，不能为每个时间戳反复扫描全量数据。
        ``process._read_rows`` 对生产数据保证按 ``datetime, symbol`` 排序；直接传入
        的 list 则保留旧行为，发现乱序时才排序。
        """
        if isinstance(bars, list):
            ordered = bars
            if any(
                (ordered[index].timestamp, ordered[index].symbol)
                < (ordered[index - 1].timestamp, ordered[index - 1].symbol)
                for index in range(1, len(ordered))
            ):
                ordered = sorted(ordered, key=lambda bar: (bar.timestamp, bar.symbol))
            if not self._trading_dates:
                self.set_trading_calendar(bar.timestamp.date() for bar in ordered)
            stream: Iterable[Bar] = ordered
        else:
            stream = bars
        self._current_bar: Bar | None = None
        self._next_timestamp = cn_naive_now()
        handled = False
        for timestamp, rows_at_time in groupby(stream, key=lambda bar: bar.timestamp):
            handled = True
            self.advance_event(timestamp, rows_at_time, event_type="bar")
        if handled and finalize_session:
            self.finish_session(persist_state=return_result)
        if return_result:
            self.state = copy.deepcopy(self.context.state)
            return self.result()
        return None

    @staticmethod
    def _annual_ratio(values: list[float]) -> float:
        deviation = pstdev(values) if len(values) > 1 else 0.0
        return mean(values) / deviation * math.sqrt(250) if deviation else 0.0

    @staticmethod
    def _beta(strategy_returns: list[float], benchmark_returns: list[float]) -> float:
        size = min(len(strategy_returns), len(benchmark_returns))
        if size < 2:
            return 0.0
        strategy = strategy_returns[-size:]
        benchmark = benchmark_returns[-size:]
        benchmark_mean = mean(benchmark)
        variance = sum((value - benchmark_mean) ** 2 for value in benchmark)
        if not variance:
            return 0.0
        strategy_mean = mean(strategy)
        covariance = sum(
            (strategy_value - strategy_mean) * (benchmark_value - benchmark_mean)
            for strategy_value, benchmark_value in zip(strategy, benchmark)
        )
        return covariance / variance

    def _drawdown_period(self, rows: list[dict[str, Any]]) -> tuple[float, str, str]:
        if not rows:
            return 0.0, "", ""
        peak_value = self.config.initial_capital
        peak_date = str(rows[0]["date"])
        best = 0.0
        best_start = peak_date
        best_end = peak_date
        for row in rows:
            equity = float(row["equity"])
            if equity > peak_value:
                peak_value = equity
                peak_date = str(row["date"])
            current = (peak_value - equity) / peak_value if peak_value else 0.0
            if current > best:
                best = current
                best_start = peak_date
                best_end = str(row["date"])
        return best, best_start, best_end

    def _attribution_rows(self) -> tuple[list[dict[str, Any]], list[float]]:
        positions: dict[str, tuple[float, float]] = {}
        rows: list[dict[str, Any]] = []
        realized_trades: list[float] = []
        cumulative_realized = 0.0
        events = [
            (fill.timestamp, 1, "fill", fill)
            for fill in self.account.fills
        ] + [
            (str(action["timestamp"]), 0, "action", action)
            for action in self.account.corporate_actions
        ]
        for _, _, event_type, event in sorted(events, key=lambda item: (item[0], item[1])):
            if event_type == "action":
                action = event
                if action.get("type") == "split":
                    held, cost = positions.get(str(action["symbol"]), (0.0, 0.0))
                    positions[str(action["symbol"])] = (held * float(action["ratio"]), cost)
                continue
            fill = event
            held, cost = positions.get(fill.symbol, (0.0, 0.0))
            cost_basis = 0.0
            realized_pnl = 0.0
            if fill.side == "buy":
                positions[fill.symbol] = (
                    held + fill.quantity,
                    cost + fill.value + fill.total_fee,
                )
            elif held > 0:
                sold = min(fill.quantity, held)
                cost_basis = cost * sold / held
                realized_pnl = (
                    fill.value * sold / fill.quantity
                    - fill.total_fee * sold / fill.quantity
                    - cost_basis
                )
                remaining = held - sold
                positions[fill.symbol] = (remaining, max(0.0, cost - cost_basis))
                realized_trades.append(realized_pnl)
                cumulative_realized += realized_pnl
            rows.append({
                "order_id": fill.order_id,
                "timestamp": fill.timestamp,
                "symbol": fill.symbol,
                "side": fill.side,
                "quantity": fill.quantity,
                "price": fill.price,
                "value": fill.value,
                "fee": fill.fee,
                "total_fee": fill.total_fee,
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "cumulative_realized_pnl": cumulative_realized,
                "realized_return_pct": realized_pnl / cost_basis * 100 if cost_basis else 0.0,
            })
        return rows, realized_trades

    def _execution_records(self) -> list[ExecutionRecord]:
        fills_by_order: dict[str, list[Fill]] = {}
        for fill in self.account.fills:
            fills_by_order.setdefault(fill.order_id, []).append(fill)
        records = []
        for order in self.account.orders:
            fills = fills_by_order.get(order.id, [])
            executed_quantity = sum(fill.quantity for fill in fills)
            amount = sum(fill.value for fill in fills)
            requested_quantity = order.quantity
            if requested_quantity is None and order.target_quantity is not None:
                requested_quantity = order.target_quantity
            records.append(ExecutionRecord(
                order_id=order.id,
                submitted_at=order.submitted_at,
                executed_at=fills[-1].timestamp if fills else None,
                symbol=order.symbol,
                side=fills[-1].side if fills else order.side,
                requested_quantity=requested_quantity,
                executed_quantity=executed_quantity,
                price=amount / executed_quantity if executed_quantity else None,
                amount=amount,
                commission=sum(fill.commission for fill in fills),
                stamp_tax=sum(fill.stamp_tax for fill in fills),
                transfer_fee=sum(fill.transfer_fee for fill in fills),
                dividend_tax=sum(fill.dividend_tax for fill in fills),
                fee=sum(fill.fee for fill in fills),
                total_fee=sum(fill.total_fee for fill in fills),
                fee_components_complete=bool(fills) and all(
                    fill.fee_components_complete for fill in fills
                ),
                status=order.status,
                reason=order.reason,
            ))
        return records

    def _transaction_rows(self) -> list[dict[str, Any]]:
        fills_by_order: dict[str, list[Fill]] = {}
        for fill in self.account.fills:
            fills_by_order.setdefault(fill.order_id, []).append(fill)
        rows = []
        for order in self.account.orders:
            fills = fills_by_order.get(order.id, [])
            filled_quantity = sum(fill.quantity for fill in fills)
            fill_value = sum(fill.value for fill in fills)
            fee = sum(fill.fee for fill in fills)
            rows.append({
                "transaction_id": order.id,
                "order_id": order.id,
                "submitted_at": order.submitted_at,
                "filled_at": fills[-1].timestamp if fills else None,
                "symbol": order.symbol,
                "requested_side": order.side,
                "executed_side": fills[-1].side if fills else None,
                "quantity": order.quantity,
                "value": order.value,
                "cash_weight": order.cash_weight,
                "target_quantity": order.target_quantity,
                "target_value": order.target_value,
                "target_percent": order.target_percent,
                "filled_quantity": filled_quantity,
                "average_fill_price": fill_value / filled_quantity if filled_quantity else None,
                "fill_value": fill_value,
                "fee": fee,
                "commission": sum(fill.commission for fill in fills),
                "stamp_tax": sum(fill.stamp_tax for fill in fills),
                "transfer_fee": sum(fill.transfer_fee for fill in fills),
                "dividend_tax": sum(fill.dividend_tax for fill in fills),
                "total_fee": sum(fill.total_fee for fill in fills),
                "fee_components_complete": bool(fills) and all(
                    fill.fee_components_complete for fill in fills
                ),
                "status": order.status,
                "reason": order.reason,
            })
        return rows

    def _daily_performance(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        by_day: dict[str, dict[str, Any]] = {}
        for row in self.account.equity_curve:
            by_day[row["timestamp"][:10]] = row
        benchmark_by_day: dict[str, float] = {}
        for row in self._benchmark_curve:
            benchmark_by_day[row["timestamp"][:10]] = float(row["close"])

        rows: list[dict[str, Any]] = []
        base_benchmark = next(iter(benchmark_by_day.values()), None)
        previous_benchmark = base_benchmark
        previous_equity = self.config.initial_capital
        peak = self.config.initial_capital
        for day, item in sorted(by_day.items()):
            benchmark_daily_return = 0.0
            if day in benchmark_by_day:
                benchmark_daily_return = (
                    benchmark_by_day[day] / previous_benchmark - 1
                    if previous_benchmark else 0.0
                )
                previous_benchmark = benchmark_by_day[day]
            equity = float(item["equity"])
            peak = max(peak, equity)
            drawdown = (peak - equity) / peak if peak else 0.0
            strategy_nav = equity / self.config.initial_capital if self.config.initial_capital else 1.0
            daily_return = equity / previous_equity - 1 if previous_equity else 0.0
            previous_equity = equity
            benchmark_nav = (
                previous_benchmark / base_benchmark
                if base_benchmark and previous_benchmark is not None else None
            )
            rows.append({
                **item,
                "date": day,
                "strategy_nav": strategy_nav,
                "benchmark_nav": benchmark_nav,
                "excess_nav": strategy_nav / benchmark_nav if benchmark_nav else None,
                "drawdown_pct": drawdown * 100,
                "exposure_pct": (equity - float(item["cash"])) / equity * 100 if equity else 0.0,
                "daily_return_pct": daily_return * 100,
                "benchmark_daily_return_pct": benchmark_daily_return * 100 if benchmark_nav is not None else None,
                "excess_daily_return_pct": (
                    (daily_return - benchmark_daily_return) * 100 if benchmark_nav is not None else None
                ),
            })

        values = [float(row["equity"]) for row in rows]
        daily_returns = [float(row["daily_return_pct"]) / 100 for row in rows]
        benchmark_returns = [
            float(row["benchmark_daily_return_pct"] or 0.0) / 100 for row in rows
        ]
        excess_returns = [left - right for left, right in zip(daily_returns, benchmark_returns)]
        total_return = values[-1] / self.config.initial_capital - 1 if values and self.config.initial_capital else 0.0
        trading_days = max(len(rows), 1)
        annual_return = (1 + total_return) ** (250 / trading_days) - 1 if total_return > -1 else -1.0
        volatility = pstdev(daily_returns) * math.sqrt(250) if len(daily_returns) > 1 else 0.0
        benchmark_return = (
            rows[-1]["benchmark_nav"] - 1
            if rows and rows[-1]["benchmark_nav"] is not None else 0.0
        )
        benchmark_annual_return = (
            (1 + benchmark_return) ** (250 / trading_days) - 1 if benchmark_return > -1 else -1.0
        )
        benchmark_volatility = pstdev(benchmark_returns) * math.sqrt(250) if len(benchmark_returns) > 1 else 0.0
        beta = self._beta(daily_returns, benchmark_returns)
        downside = [min(value, 0.0) for value in daily_returns]
        downside_deviation = math.sqrt(mean(value * value for value in downside)) * math.sqrt(250) if downside else 0.0
        max_drawdown, drawdown_start, drawdown_end = self._drawdown_period(rows)
        _, realized_trades = self._attribution_rows()
        wins = [value for value in realized_trades if value > 0]
        losses = [value for value in realized_trades if value < 0]
        traded_value = sum(fill.value for fill in self.account.fills)
        return rows, {
            "total_return_pct": total_return * 100,
            "annual_return_pct": annual_return * 100,
            "benchmark_return_pct": benchmark_return * 100,
            "benchmark_annual_return_pct": benchmark_annual_return * 100,
            "excess_return_pct": (total_return - benchmark_return) * 100,
            "max_drawdown_pct": max_drawdown * 100,
            "max_drawdown_start": drawdown_start,
            "max_drawdown_end": drawdown_end,
            "volatility_pct": volatility * 100,
            "benchmark_volatility_pct": benchmark_volatility * 100,
            "alpha_pct": (annual_return - beta * benchmark_annual_return) * 100,
            "beta": beta,
            "sharpe_ratio": self._annual_ratio(daily_returns),
            "sortino_ratio": annual_return / downside_deviation if downside_deviation else 0.0,
            "information_ratio": self._annual_ratio(excess_returns),
            "positive_day_rate_pct": (
                sum(value > 0 for value in daily_returns) / len(daily_returns) * 100 if daily_returns else 0.0
            ),
            "trade_win_rate_pct": (
                len(wins) / len(realized_trades) * 100 if realized_trades else 0.0
            ),
            "profit_loss_ratio": (
                mean(wins) / abs(mean(losses)) if wins and losses else 0.0
            ),
            "trade_count": len(self.account.fills),
            "turnover": traded_value / mean(values) if values else 0.0,
            "turnover_pct": traded_value / mean(values) * 100 if values else 0.0,
        }

    def result(self) -> dict[str, Any]:
        curve = self.account.equity_curve
        values = [float(item["equity"]) for item in curve]
        peak = self.config.initial_capital
        drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            drawdown = max(drawdown, (peak - value) / peak if peak else 0.0)
        daily_curve, performance = self._daily_performance()
        attribution, _ = self._attribution_rows()
        orders = [asdict(value) for value in self.account.orders]
        participation = sorted(
            float(fill.participation_pct)
            for fill in self.account.fills
            if fill.participation_pct is not None
        )
        percentile_index = max(0, math.ceil(len(participation) * 0.95) - 1)
        capacity_analysis = {
            "model": "bar_volume_participation",
            "diagnostic_only": True,
            "total_fills": len(self.account.fills),
            "covered_fills": len(participation),
            "max_participation_pct": max(participation, default=None),
            "p95_participation_pct": participation[percentile_index] if participation else None,
            "fills_over_1_pct": sum(value > 1 for value in participation),
            "fills_over_5_pct": sum(value > 5 for value in participation),
            "fills_over_10_pct": sum(value > 10 for value in participation),
        }
        executions = [asdict(value) for value in self._execution_records()]
        return {"initial_capital": self.config.initial_capital, "final_equity": values[-1] if values else self.config.initial_capital,
                "return_pct": ((values[-1] / self.config.initial_capital) - 1) * 100 if values else 0.0,
                "max_drawdown_pct": drawdown * 100, "equity_curve": curve,
                "daily_equity_curve": daily_curve, "performance": performance,
                "benchmark_symbol": self.config.benchmark_symbol,
                "orders": orders, "signals": orders, "transactions": self._transaction_rows(),
                "executions": executions,
                "strategy_signals": self.signals,
                "fills": [asdict(v) for v in self.account.fills], "attribution": attribution,
                "capacity_analysis": capacity_analysis,
                "corporate_actions": self.account.corporate_actions,
                "positions": self.account.positions, "logs": self.logs, "state": self.state,
                "execution_mode": self.execution_mode,
                "scheduled_times": self.scheduled_times,
                "callbacks_executed": self.callbacks_executed,
                "market_rows_consumed": self.market_rows_consumed,
                "checkpoint": self.checkpoint()}
