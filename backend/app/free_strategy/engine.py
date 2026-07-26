"""自由策略脚本的可信执行核心与账户撮合。"""
from __future__ import annotations

import copy
import logging
import math
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Callable, Iterable

from .bars import Bar

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FreeStrategyConfig:
    initial_capital: float = 1_000_000.0
    fees_pct: float = 0.0002
    commission_pct: float | None = None
    stamp_tax_pct: float = 0.001
    slippage_bps: float = 5.0
    lot_size: int = 100
    max_exposure_pct: float = 1.0
    settlement: str = "t1"
    fill_policy: str = "next_open"
    asset_type: str = "stock"
    callback_timeout_seconds: float = 30.0


@dataclass(slots=True)
class Order:
    id: str
    symbol: str
    side: str
    quantity: float | None = None
    value: float | None = None
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


class Account:
    def __init__(self, config: FreeStrategyConfig):
        self.config = config
        self.cash = float(config.initial_capital)
        self.positions: dict[str, float] = {}
        self.available: dict[str, float] = {}
        self.avg_cost: dict[str, float] = {}
        self.orders: list[Order] = []
        self.fills: list[Fill] = []
        self.equity_curve: list[dict[str, Any]] = []

    def snapshot(self) -> dict[str, Any]:
        return {"cash": self.cash, "positions": self.positions, "available": self.available,
                "avg_cost": self.avg_cost, "orders": [asdict(v) for v in self.orders],
                "fills": [asdict(v) for v in self.fills], "equity_curve": self.equity_curve}

    def restore(self, raw: dict[str, Any]) -> None:
        self.cash = float(raw.get("cash", self.cash))
        self.positions = {k: float(v) for k, v in raw.get("positions", {}).items()}
        self.available = {k: float(v) for k, v in raw.get("available", {}).items()}
        self.avg_cost = {k: float(v) for k, v in raw.get("avg_cost", {}).items()}

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


class Context:
    def __init__(self, engine: "FreeStrategyEngine") -> None:
        self._engine = engine
        self.state: dict[str, Any] = copy.deepcopy(engine.state)
        self.now: datetime | None = None
        self.period = engine.timeframe
        self.portfolio = SimpleNamespace(cash=engine.account.cash, positions={}, available_positions={}, total_value=engine.account.cash)
        self.account = self.portfolio
        self._scheduled: list[tuple[str, Callable[..., Any], bool]] = []

    def _sync(self, prices: dict[str, float]) -> None:
        self.portfolio.cash = self._engine.account.cash
        self.portfolio.positions = dict(self._engine.account.positions)
        self.portfolio.available_positions = dict(self._engine.account.available)
        self.portfolio.total_value = self._engine.account.equity(prices)

    def schedule(self, callback: Callable[..., Any], at: str | time) -> None:
        value = at.strftime("%H:%M") if hasattr(at, "strftime") else str(at)[:5]
        self._scheduled.append((value, callback, False))

    schedule_function = schedule

    def buy(self, symbol: str, quantity: float | None = None, value: float | None = None, **kwargs: Any) -> None:
        self._engine.submit_order("buy", symbol, quantity=quantity, value=value, reason=kwargs.get("reason", ""))

    def sell(self, symbol: str, quantity: float | None = None, value: float | None = None, **kwargs: Any) -> None:
        self._engine.submit_order("sell", symbol, quantity=quantity, value=value, reason=kwargs.get("reason", ""))

    def order_target(self, symbol: str, quantity: float) -> None:
        self._engine.submit_order("target", symbol, target_quantity=quantity)

    order_target_quantity = order_target

    def order_target_value(self, symbol: str, value: float) -> None:
        self._engine.submit_order("target", symbol, target_value=value)

    def order_target_percent(self, symbol: str, percent: float) -> None:
        self._engine.submit_order("target", symbol, target_percent=percent)

    def history(self, symbol: str | None = None, count: int = 20, field: str = "close") -> Any:
        values = self._engine.history.get(symbol or "", [])[-count:]
        if field == "close":
            return [item.close for item in values]
        return [getattr(item, field) for item in values]

    def log(self, message: str, level: str = "INFO") -> None:
        self._engine.logs.append({"timestamp": self.now.isoformat() if self.now else "", "level": level, "message": str(message)})

    info = log


class FreeStrategyEngine:
    def __init__(self, source: str, timeframe: str = "1d", config: FreeStrategyConfig | None = None,
                 state: dict[str, Any] | None = None) -> None:
        self.source = source
        self.timeframe = timeframe
        self.config = config or FreeStrategyConfig()
        self.account = Account(self.config)
        self.state = state or {}
        self.logs: list[dict[str, Any]] = []
        self.history: dict[str, list[Bar]] = {}
        self.pending: list[tuple[Order, datetime]] = []
        self._immediate: list[Order] = []
        self._bought_dates: dict[str, date] = {}
        self._counter = 0
        self._callbacks: dict[str, Callable[..., Any]] = {}
        self.context = Context(self)
        namespace: dict[str, Any] = {"__name__": "free_strategy_snapshot"}
        # Trusted local execution is intentional for this feature: user scripts may import
        # installed packages and local modules. They run in a worker process at the API edge.
        exec(compile(source, "<free_strategy>", "exec"), namespace, namespace)
        self._callbacks = {name: namespace[name] for name in
                           ("initialize", "before_trading_start", "on_bar", "after_trading_end")
                           if callable(namespace.get(name))}
        if "on_bar" not in self._callbacks:
            raise ValueError("策略必须定义 on_bar(context, bars)")
        if "initialize" in self._callbacks:
            self._callbacks["initialize"](self.context)

    def submit_order(self, side: str, symbol: str, **kwargs: Any) -> None:
        self._counter += 1
        order = Order(id=f"o{self._counter}", symbol=symbol, side=side, submitted_at=self.context.now.isoformat() if self.context.now else "", **kwargs)
        self.account.orders.append(order)
        if self.config.fill_policy in {"close", "current_close"}:
            self._immediate.append(order)
        else:
            self.pending.append((order, self._next_timestamp))

    def _sell_first(self, item: tuple[Order, datetime] | Order) -> int:
        order = item[0] if isinstance(item, tuple) else item
        if order.side == "sell":
            return 0
        if order.side == "target":
            current = self.account.positions.get(order.symbol, 0.0)
            target = order.target_quantity
            if target is None and order.target_value is not None:
                target = order.target_value / max(self._current_prices.get(order.symbol, 1.0), 0.01)
            return 0 if target is not None and target < current else 1
        return 1

    def _fill_order(self, order: Order, raw_price: float | None, timestamp: datetime | None) -> None:
        if raw_price is None or raw_price <= 0:
            order.status = "rejected"
            order.reason = "缺少可成交价格"
            return
        price = raw_price * (1 + (self.config.slippage_bps / 10000) * (1 if order.side == "buy" else -1))
        current = self.account.positions.get(order.symbol, 0.0)
        target = None
        if order.side == "target":
            target = order.target_quantity
            if target is None and order.target_value is not None:
                target = order.target_value / price
            if target is None and order.target_percent is not None:
                target = self.account.equity(self._current_prices) * order.target_percent / price
            side = "buy" if (target or 0) > current else "sell"
            qty = abs((target or 0) - current)
        else:
            side = order.side
            qty = order.quantity
            if qty is None and order.value is not None:
                qty = order.value / price
            qty = qty or 0
        lot = max(1, self.config.lot_size)
        qty = math.floor(qty / lot) * lot
        if side == "sell":
            bought_today = self.config.settlement == "t1" and self._bought_dates.get(order.symbol) == (timestamp.date() if timestamp else self.context.now.date())
            available = self.account.available.get(order.symbol, 0.0 if bought_today else current)
            qty = min(qty, available)
        else:
            gross = qty * price
            max_gross = self.account.equity(self._current_prices) * self.config.max_exposure_pct
            qty = min(qty, math.floor(max(0.0, min(self.account.cash, max_gross)) / price / lot) * lot)
        if qty <= 0:
            order.status = "rejected"
            order.reason = "数量不足、现金不足或 T+1 未结算"
            return
        gross = qty * price
        commission_rate = self.config.commission_pct if self.config.commission_pct is not None else self.config.fees_pct
        fee = gross * commission_rate + (gross * self.config.stamp_tax_pct if side == "sell" and self.config.asset_type == "stock" else 0.0)
        if side == "buy":
            self.account.cash -= gross + fee
            old = self.account.positions.get(order.symbol, 0.0)
            self.account.avg_cost[order.symbol] = ((old * self.account.avg_cost.get(order.symbol, price)) + gross + fee) / (old + qty)
            self.account.positions[order.symbol] = old + qty
            if self.config.settlement == "t0":
                self.account.available[order.symbol] = self.account.positions[order.symbol]
            else:
                self._bought_dates[order.symbol] = (timestamp.date() if timestamp else self.context.now.date())
        else:
            self.account.cash += gross - fee
            self.account.positions[order.symbol] = max(0.0, current - qty)
            self.account.available[order.symbol] = max(0.0, self.account.available.get(order.symbol, current) - qty)
        order.status = "filled"
        order.status = "filled"
        self.account.fills.append(Fill(order.id, order.symbol, side, qty, price, gross, fee, timestamp.isoformat() if timestamp else ""))

    def _run_callback(self, name: str, bars: BarsView) -> None:
        callback = self._callbacks.get(name)
        if callback is None:
            return
        started = time.monotonic()
        if name == "on_bar":
            callback(self.context, bars)
        else:
            callback(self.context)
        elapsed = time.monotonic() - started
        if elapsed > self.config.callback_timeout_seconds:
            raise TimeoutError(f"{name} 回调超过 {self.config.callback_timeout_seconds:g} 秒")

    def run(self, bars: Iterable[Bar]) -> dict[str, Any]:
        grouped = sorted(list(bars), key=lambda b: (b.timestamp, b.symbol))
        timestamps = sorted({bar.timestamp for bar in grouped})
        by_time = {timestamp: BarsView({bar.symbol: bar for bar in grouped if bar.timestamp == timestamp}) for timestamp in timestamps}
        dates_seen: set[date] = set()
        self._current_bar: Bar | None = None
        self._current_prices: dict[str, float] = {}
        self._current_close_prices: dict[str, float] = {}
        self._next_timestamp = timestamps[0] if timestamps else datetime.now()
        for index, timestamp in enumerate(timestamps):
            self._next_timestamp = timestamp
            bars_now = by_time[timestamp]
            is_new_day = timestamp.date() not in dates_seen
            if is_new_day:
                dates_seen.add(timestamp.date())
                self.context._scheduled = [(at, callback, False) for at, callback, _ in self.context._scheduled]
                self.context.now = timestamp
                self.context._sync({s: b.close for s, b in bars_now.items()})
                self._run_callback("before_trading_start", bars_now)
            # next-open orders created on the previous callback are filled before this bar.
            self._current_prices = {s: b.open for s, b in bars_now.items()}
            if is_new_day and self.config.settlement == "t1":
                for symbol, bought_date in list(self._bought_dates.items()):
                    if bought_date < timestamp.date():
                        self.account.available[symbol] = self.account.positions.get(symbol, 0.0)
                        self._bought_dates.pop(symbol, None)
            due = [p for p in self.pending if p[1] <= timestamp]
            self.pending = [p for p in self.pending if p[1] > timestamp]
            for order, _ in sorted(due, key=self._sell_first):
                self._fill_order(order, self._current_prices.get(order.symbol), timestamp)
            self.context.now = timestamp
            self.context._sync({s: b.close for s, b in bars_now.items()})
            self._current_bar = next(iter(bars_now.values()), None)
            self._current_close_prices = {s: b.close for s, b in bars_now.items()}
            for symbol, bar in bars_now.items():
                self.history.setdefault(symbol, []).append(bar)
            for slot_index, (at, callback, done) in enumerate(self.context._scheduled):
                if not done and timestamp.strftime("%H:%M") >= at:
                    callback(self.context)
                    self.context._scheduled[slot_index] = (at, callback, True)
            self._run_callback("on_bar", bars_now)
            for order in sorted(self._immediate, key=self._sell_first):
                self._fill_order(order, self._current_close_prices.get(order.symbol), timestamp)
            self._immediate.clear()
            self.state = copy.deepcopy(self.context.state)
            prices = {s: b.close for s, b in bars_now.items()}
            self.account.equity_curve.append({"timestamp": timestamp.isoformat(), "equity": self.account.equity(prices), "cash": self.account.cash, "positions": dict(self.account.positions)})
            if index + 1 == len(timestamps) or timestamps[index + 1].date() != timestamp.date():
                self.context.now = timestamp
                self._run_callback("after_trading_end", bars_now)
                for order in sorted(self._immediate, key=self._sell_first):
                    self._fill_order(order, self._current_close_prices.get(order.symbol), timestamp)
                self._immediate.clear()
                self.state = copy.deepcopy(self.context.state)
        return self.result()

    def result(self) -> dict[str, Any]:
        curve = self.account.equity_curve
        values = [float(item["equity"]) for item in curve]
        peak = self.config.initial_capital
        drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            drawdown = max(drawdown, (peak - value) / peak if peak else 0.0)
        return {"initial_capital": self.config.initial_capital, "final_equity": values[-1] if values else self.config.initial_capital,
                "return_pct": ((values[-1] / self.config.initial_capital) - 1) * 100 if values else 0.0,
                "max_drawdown_pct": drawdown * 100, "equity_curve": curve,
                "orders": [asdict(v) for v in self.account.orders], "fills": [asdict(v) for v in self.account.fills],
                "positions": self.account.positions, "logs": self.logs, "state": self.state}
