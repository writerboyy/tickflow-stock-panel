from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from app.free_strategy import five_fortunes as five


class FakeContext:
    def __init__(self, equity: float = 1_000_000.0) -> None:
        self.state = {}
        self.now = datetime(2026, 7, 20, 9, 30)
        self.portfolio = SimpleNamespace(
            total_value=equity,
            cash=equity,
            positions={},
            avg_cost={},
        )
        self.scheduled = []
        self.orders = []
        self.logs = []
        self.universe = []
        self.history_requirements = {}
        self.market_history_requirements = {}
        self.market_rows = {}

    def set_universe(self, symbols) -> None:
        self.universe = list(symbols)

    def schedule(self, callback, at) -> None:
        self.scheduled.append((callback, at))

    def require_history(self, timeframe: str = "1d", bars: int = 1) -> None:
        self.history_requirements[timeframe] = bars

    def require_market_history(self, asset_type: str = "etf", timeframe: str = "1d", bars: int = 1) -> None:
        self.market_history_requirements[(asset_type, timeframe)] = bars

    def market_history_bars(self, symbol: str, count: int = 20, timeframe: str | None = None):
        return list(self.market_rows.get(symbol, []))[-count:]

    def log(self, message: str, level: str = "INFO") -> None:
        self.logs.append((level, message))

    def history_bars(self, _symbol: str, count: int = 20, timeframe: str | None = None):
        return []

    def order_target_percent(self, symbol: str, percent: float) -> None:
        self.orders.append(("percent", symbol, percent))

    def order_target(self, symbol: str, quantity: float) -> None:
        self.orders.append(("quantity", symbol, quantity))


def initialized_context(equity: float = 1_000_000.0) -> FakeContext:
    context = FakeContext(equity)
    five.initialize(context)
    five.before_trading_start(context)
    return context


def metric(symbol: str, score: float, history: list[float]) -> dict:
    return {"symbol": symbol, "score": score, "history": history}


def variable_prices(offset: float = 0.0) -> list[float]:
    changes = (0.01, -0.004, 0.007, -0.002, 0.012, -0.006)
    values = [100.0 + offset]
    for index in range(1, 61):
        values.append(values[-1] * (1 + changes[index % len(changes)]))
    return values


def test_standard_lifecycle_name_keeps_legacy_alias():
    assert five.on_session_start is five.before_trading_start


def test_initialize_registers_available_etf_universe():
    context = initialized_context()

    assert context.universe == sorted({*five.WUFU_MINUTE_POOL, five.DEFENSIVE_ETF})
    assert context.history_requirements == {"1d": 61}
    assert context.market_history_requirements == {("etf", "1d"): 61}


def test_laplace_smoothing_is_regime_specific():
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 10)
    closes = [100 * (1.001 ** index) for index in range(70)]
    volumes = [1_000_000.0] * 69

    normal = five._metric_for("510300.SH", closes, volumes, 550_000, context, "正常期")
    weak = five._metric_for("510300.SH", closes, volumes, 550_000, context, "走弱期")

    assert normal is not None and weak is not None
    assert normal["laplace_s"] == pytest.approx(0.06)
    assert weak["laplace_s"] == pytest.approx(0.12)
    assert weak["laplace_value"] > normal["laplace_value"]


def test_adjusted_correlation_matches_identical_price_paths():
    prices = variable_prices()
    assert five._adjusted_correlation(prices, prices) == pytest.approx(1.0)


def test_high_adjusted_correlation_guard_keeps_current_holding():
    context = initialized_context()
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    prices = variable_prices()
    state["all_metric_rows"] = [metric("A", 4.0, prices), metric("B", 4.5, prices)]

    target = five._apply_correlation_hold_guard(context, "A", "B")

    assert target == "A"
    assert state["correlation_decisions"][-1]["blocked"] is True
    assert state["correlation_decisions"][-1]["reason"] == "high_pair_overlay"


def test_regime_change_day_blocks_immediate_swap():
    context = initialized_context()
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    state["regime_changed_today"] = True
    state["all_metric_rows"] = [metric("A", -1.0, variable_prices()), metric("B", 4.0, variable_prices(5))]

    targets = five._choose_targets(context, [state["all_metric_rows"][1]], [state["all_metric_rows"][1]])

    assert targets == ["A"]
    assert state["decision"]["reason"] == "regime_change_hold"


def test_weak_regime_drawdown_uses_five_percent_half_position():
    context = initialized_context(equity=94.0)
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    state["regime"] = "走弱期"
    state["peak_equity"] = 100.0
    context.now = datetime(2026, 7, 20, 10, 31)

    five._risk_monitor(context)

    assert context.orders == [("quantity", "A", 50.0)]
    assert state["position_scale"] == 0.5
    assert state["risk_actions"][-1]["action"] == "half"
    assert state["risk_actions"][-1]["thresholds"] == {
        "half": 0.05,
        "defensive": 0.08,
        "flat": 0.12,
    }


def test_normal_regime_does_not_reduce_position_at_six_percent_drawdown():
    context = initialized_context(equity=94.0)
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    state["regime"] = "正常期"
    state["peak_equity"] = 100.0

    five._risk_monitor(context)

    assert context.orders == []
    assert state["risk_actions"] == []


def test_four_consecutive_filter_fail_days_force_defensive(monkeypatch):
    context = initialized_context()
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    state["intraday"]["close"][five.DEFENSIVE_ETF] = 1.0
    filtered = [metric("B", 4.0, variable_prices(5))]

    def rank(_context):
        state["all_metric_rows"] = [metric("A", -1.0, variable_prices()), *filtered]
        return filtered

    monkeypatch.setattr(five, "_rank_candidates", rank)
    monkeypatch.setattr(five, "_choose_targets", lambda *_: ["B"])

    for day in range(4):
        context.now = datetime(2026, 7, 20, 13, 10) + timedelta(days=day)
        state["decision"] = {"date": context.now.date().isoformat(), "reason": "ranked_target"}
        five._prepare_and_sell(context)

    assert state["target"] == [five.DEFENSIVE_ETF]
    assert state["decision"]["reason"] == "four_day_filter_fail_defensive"


def test_candidate_pool_uses_regime_threshold():
    rows = [metric("A", 4.0, []), metric("B", 3.7, []), metric("C", 3.5, [])]
    assert [row["symbol"] for row in five._candidate_pool(rows, "正常期")] == ["A", "B"]
    assert [row["symbol"] for row in five._candidate_pool(rows, "走弱期")] == ["A"]


def test_liquidity_pool_uses_stricter_weak_regime_divisor():
    context = initialized_context()
    state = context.state["five_fortunes"]
    high, low = "510300.SH", "159985.SZ"
    context.market_rows = {
        high: [SimpleNamespace(date=datetime(2026, 7, day).date(), close=1, volume=1, amount=100_000_000.0) for day in (17, 18, 19)],
        low: [SimpleNamespace(date=datetime(2026, 7, day).date(), close=1, volume=1, amount=1.0) for day in (17, 18, 19)],
    }

    five._refresh_liquidity_pools(context)
    pool = five._liquidity_pool(state, "走弱期")

    assert high in pool
    assert low not in pool
    assert state["weak_liquidity_threshold"] == pytest.approx((100_000_000.0 + 1.0) / 3_000)
