"""自由策略脚本的可信执行核心与账户撮合。"""
from __future__ import annotations

import copy
import logging
import math
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from itertools import groupby
from statistics import mean, pstdev
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
    benchmark_symbol: str = "510300.SH"
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
        self.fills = [Fill(**item) for item in raw.get("fills", [])]
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


class Context:
    def __init__(self, engine: "FreeStrategyEngine") -> None:
        self._engine = engine
        self.state: dict[str, Any] = copy.deepcopy(engine.state)
        self.now: datetime | None = None
        self.period = engine.timeframe
        self.portfolio = SimpleNamespace(cash=engine.account.cash, positions={}, available_positions={}, avg_cost={}, total_value=engine.account.cash)
        self.account = self.portfolio
        self._scheduled: list[tuple[str, Callable[..., Any], bool]] = []
        self._universe: list[str] = []
        self._history_requirements: dict[str, int] = {}

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

    def _sync(self, prices: dict[str, float]) -> None:
        self.portfolio.cash = self._engine.account.cash
        self.portfolio.positions = dict(self._engine.account.positions)
        self.portfolio.available_positions = dict(self._engine.account.available)
        self.portfolio.avg_cost = dict(self._engine.account.avg_cost)
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
        history = self._engine._history_by_period.get(timeframe or self.period, {})
        return list(history.get(symbol or "", [])[-count:])

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
        self._history_by_period: dict[str, dict[str, list[Bar]]] = {timeframe: self.history}
        self.pending: list[tuple[Order, datetime]] = []
        self._immediate: list[Order] = []
        self._bought_dates: dict[str, date] = {}
        self._counter = 0
        self._callbacks: dict[str, Callable[..., Any]] = {}
        self._active_session_date: date | None = None
        self._session_finished = False
        self._last_bars = BarsView()
        self._last_timestamp: datetime | None = None
        self._current_prices: dict[str, float] = {}
        self._current_close_prices: dict[str, float] = {}
        self._session_bars: dict[str, Bar] = {}
        self._applied_splits: dict[str, date] = {}
        self._benchmark_curve: list[dict[str, Any]] = []
        self._session_equity_snapshot: dict[str, Any] | None = None
        self._session_benchmark_close: float | None = None
        self.context = Context(self)
        namespace: dict[str, Any] = {"__name__": "free_strategy_snapshot"}
        # Trusted local execution is intentional for this feature: user scripts may import
        # installed packages and local modules. They run in a worker process at the API edge.
        exec(compile(source, "<free_strategy>", "exec"), namespace, namespace)
        callback_names = {
            "initialize": ("initialize",),
            "before_trading_start": ("before_trading_start", "on_session_start"),
            "on_bar": ("on_bar",),
            "after_trading_end": ("after_trading_end", "on_session_end", "after_market_close"),
        }
        self._callbacks = {
            canonical: next((namespace[name] for name in names if callable(namespace.get(name))), None)
            for canonical, names in callback_names.items()
        }
        self._callbacks = {name: callback for name, callback in self._callbacks.items() if callback is not None}
        if "on_bar" not in self._callbacks:
            raise ValueError("策略必须定义 on_bar(context, bars)")
        if "initialize" in self._callbacks:
            self._callbacks["initialize"](self.context)

    @property
    def universe(self) -> list[str]:
        return self.context.universe

    @property
    def history_requirements(self) -> dict[str, int]:
        return self.context.history_requirements

    def preload_history(self, bars: Iterable[Bar], timeframe: str = "1d") -> int:
        """注入只读历史，不触发生命周期、下单或资金变动。"""
        history = self._history_by_period.setdefault(timeframe, {})
        count = 0
        for bar in sorted(bars, key=lambda item: (item.timestamp, item.symbol)):
            values = history.setdefault(bar.symbol, [])
            if values and values[-1].timestamp == bar.timestamp:
                values[-1] = bar
                continue
            values.append(bar)
            if len(values) > 5_000:
                del values[:-5_000]
            count += 1
        return count

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

    def _fill_order(self, order: Order, bar: Bar | None, timestamp: datetime | None, field: str) -> None:
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
        lot = max(1, self.config.lot_size)
        qty = math.floor(qty / lot) * lot
        if side == "sell":
            bought_today = self.config.settlement == "t1" and self._bought_dates.get(order.symbol) == (timestamp.date() if timestamp else self.context.now.date())
            available = self.account.available.get(order.symbol, 0.0 if bought_today else current)
            qty = min(qty, available)
        else:
            gross = qty * price
            max_gross = self.account.equity(self._current_close_prices) * self.config.max_exposure_pct
            qty = min(qty, math.floor(max(0.0, min(self.account.cash, max_gross)) / price / lot) * lot)
        if qty <= 0:
            if order.side == "target":
                order.status = "skipped"
                order.reason = "目标仓位无需调整或不足一手"
            else:
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
        self.account.fills.append(Fill(order.id, order.symbol, side, qty, price, gross, fee, timestamp.isoformat() if timestamp else ""))

    def _apply_splits(self, bars: BarsView, timestamp: datetime) -> None:
        for symbol, bar in bars.items():
            ratio = float(bar.split_ratio)
            if ratio <= 0 or math.isclose(ratio, 1.0, rel_tol=0.0, abs_tol=1e-9):
                continue
            if self._applied_splits.get(symbol) == timestamp.date():
                continue
            self._applied_splits[symbol] = timestamp.date()
            quantity = self.account.positions.get(symbol, 0.0)
            if quantity <= 0:
                continue
            self.account.positions[symbol] = quantity * ratio
            self.account.available[symbol] = self.account.available.get(symbol, 0.0) * ratio
            if symbol in self.account.avg_cost:
                self.account.avg_cost[symbol] /= ratio
            self.account.corporate_actions.append({
                "timestamp": timestamp.isoformat(),
                "symbol": symbol,
                "type": "split",
                "ratio": ratio,
            })
            self.logs.append({
                "timestamp": timestamp.isoformat(),
                "level": "INFO",
                "message": f"{symbol} ETF拆分生效：持仓数量按 {ratio:g} 倍调整",
            })

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

    def runtime_snapshot(self) -> dict[str, Any]:
        """保存模拟盘恢复所需的会话边界，避免盘后任务重复执行。"""
        return {
            "session_date": self._active_session_date.isoformat() if self._active_session_date else None,
            "session_finished": self._session_finished,
            "last_timestamp": self._last_timestamp.isoformat() if self._last_timestamp else None,
        }

    def restore_runtime(self, raw: dict[str, Any] | None) -> None:
        raw = raw or {}
        session_date = raw.get("session_date")
        last_timestamp = raw.get("last_timestamp")
        self._active_session_date = date.fromisoformat(session_date) if session_date else None
        self._session_finished = bool(raw.get("session_finished", False))
        self._last_timestamp = datetime.fromisoformat(last_timestamp) if last_timestamp else None

    def checkpoint(self) -> dict[str, Any]:
        """返回可用于分段历史回测或模拟盘恢复的完整运行状态。"""
        return {
            "account": self.account.snapshot(),
            "state": self.context.state,
            "runtime": self.runtime_snapshot(),
            "benchmark_curve": self._benchmark_curve,
            "bought_dates": {symbol: value.isoformat() for symbol, value in self._bought_dates.items()},
            "applied_splits": {symbol: value.isoformat() for symbol, value in self._applied_splits.items()},
            "pending_orders": [
                {"order_id": order.id, "due_at": due_at.isoformat()}
                for order, due_at in self.pending
            ],
            "order_counter": self._counter,
        }

    def restore_checkpoint(self, raw: dict[str, Any]) -> None:
        self.account.restore(raw.get("account", {}))
        self.context.state = copy.deepcopy(raw.get("state", self.context.state))
        self.state = copy.deepcopy(self.context.state)
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
        orders_by_id = {order.id: order for order in self.account.orders}
        self.pending = [
            (orders_by_id[item["order_id"]], datetime.fromisoformat(item["due_at"]))
            for item in raw.get("pending_orders", [])
            if item.get("order_id") in orders_by_id
        ]
        self._counter = int(raw.get("order_counter", len(self.account.orders)))

    def _start_session(self, timestamp: datetime, bars_now: BarsView) -> None:
        self._active_session_date = timestamp.date()
        self._session_finished = False
        self.context._scheduled = [(at, callback, False) for at, callback, _ in self.context._scheduled]
        self.context.now = timestamp
        self._session_bars = dict(bars_now)
        self._session_equity_snapshot = None
        self._session_benchmark_close = None
        if self.config.settlement == "t1":
            for symbol, bought_date in list(self._bought_dates.items()):
                if bought_date < timestamp.date():
                    self.account.available[symbol] = self.account.positions.get(symbol, 0.0)
                    self._bought_dates.pop(symbol, None)
        self.context._sync(self._current_close_prices)
        self._run_callback("before_trading_start", bars_now)

    def finish_session(self, *, persist_state: bool = True) -> bool:
        """执行当日盘后任务；同一会话重复调用不会重复执行。"""
        if self._active_session_date is None or self._session_finished:
            return False
        timestamp = self._last_timestamp or datetime.now()
        self.context.now = timestamp
        self._run_callback("after_trading_end", self._last_bars)
        for order in sorted(self._immediate, key=self._sell_first):
            self._fill_order(order, self._session_bars.get(order.symbol), timestamp, "close")
        self._immediate.clear()
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
                "position_values": position_values,
            })
        if self._session_benchmark_close is not None:
            self._benchmark_curve.append({"timestamp": timestamp.isoformat(), "close": self._session_benchmark_close})
        if persist_state:
            self.state = copy.deepcopy(self.context.state)
        self._session_finished = True
        return True

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
            stream: Iterable[Bar] = ordered
        else:
            stream = bars
        self._current_bar: Bar | None = None
        self._next_timestamp = datetime.now()
        handled = False
        for timestamp, rows_at_time in groupby(stream, key=lambda bar: bar.timestamp):
            handled = True
            self._next_timestamp = timestamp
            bars_now = BarsView({bar.symbol: bar for bar in rows_at_time})
            if self._active_session_date != timestamp.date():
                self.finish_session(persist_state=return_result)
                self._apply_splits(bars_now, timestamp)
                self._current_prices.update({s: b.execution_price("open") for s, b in bars_now.items()})
                self._current_close_prices.update({s: b.execution_price("close") for s, b in bars_now.items()})
                self._start_session(timestamp, bars_now)
            else:
                self._apply_splits(bars_now, timestamp)
            # next-open orders created on the previous callback are filled before this bar.
            self._current_prices.update({s: b.execution_price("open") for s, b in bars_now.items()})
            due = [p for p in self.pending if p[1] <= timestamp and p[0].symbol in bars_now]
            self.pending = [p for p in self.pending if p[1] > timestamp or p[0].symbol not in bars_now]
            for order, _ in sorted(due, key=self._sell_first):
                self._fill_order(order, bars_now.get(order.symbol), timestamp, "open")
            self.context.now = timestamp
            self._current_close_prices.update({s: b.execution_price("close") for s, b in bars_now.items()})
            self.context._sync(self._current_close_prices)
            self._current_bar = next(iter(bars_now.values()), None)
            benchmark = bars_now.get(self.config.benchmark_symbol)
            if benchmark is not None:
                self._session_benchmark_close = benchmark.close
            self._last_bars = bars_now
            self._session_bars.update(bars_now)
            self._last_timestamp = timestamp
            for symbol, bar in bars_now.items():
                self.history.setdefault(symbol, []).append(bar)
                if len(self.history[symbol]) > 5_000:
                    del self.history[symbol][:-5_000]
            for slot_index, (at, callback, done) in enumerate(self.context._scheduled):
                if not done and timestamp.strftime("%H:%M") >= at:
                    callback(self.context)
                    self.context._scheduled[slot_index] = (at, callback, True)
            self._run_callback("on_bar", bars_now)
            for order in sorted(self._immediate, key=self._sell_first):
                self._fill_order(order, bars_now.get(order.symbol), timestamp, "close")
            self._immediate.clear()
            self._session_equity_snapshot = {"timestamp": timestamp.isoformat(), "equity": self.account.equity(self._current_close_prices), "cash": self.account.cash, "positions": dict(self.account.positions)}
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
                positions[fill.symbol] = (held + fill.quantity, cost + fill.value + fill.fee)
            elif held > 0:
                sold = min(fill.quantity, held)
                cost_basis = cost * sold / held
                realized_pnl = fill.value * sold / fill.quantity - fill.fee * sold / fill.quantity - cost_basis
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
                "cost_basis": cost_basis,
                "realized_pnl": realized_pnl,
                "cumulative_realized_pnl": cumulative_realized,
                "realized_return_pct": realized_pnl / cost_basis * 100 if cost_basis else 0.0,
            })
        return rows, realized_trades

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
                "target_quantity": order.target_quantity,
                "target_value": order.target_value,
                "target_percent": order.target_percent,
                "filled_quantity": filled_quantity,
                "average_fill_price": fill_value / filled_quantity if filled_quantity else None,
                "fill_value": fill_value,
                "fee": fee,
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
        return {"initial_capital": self.config.initial_capital, "final_equity": values[-1] if values else self.config.initial_capital,
                "return_pct": ((values[-1] / self.config.initial_capital) - 1) * 100 if values else 0.0,
                "max_drawdown_pct": drawdown * 100, "equity_curve": curve,
                "daily_equity_curve": daily_curve, "performance": performance,
                "benchmark_symbol": self.config.benchmark_symbol,
                "orders": orders, "signals": orders, "transactions": self._transaction_rows(),
                "fills": [asdict(v) for v in self.account.fills], "attribution": attribution,
                "corporate_actions": self.account.corporate_actions,
                "positions": self.account.positions, "logs": self.logs, "state": self.state,
                "checkpoint": self.checkpoint()}
