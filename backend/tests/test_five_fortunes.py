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
        self.extra_history_requirements = set()
        self.extra_rows = {}
        self.market_rows = {}

    def set_universe(self, symbols) -> None:
        self.universe = list(symbols)

    def schedule(self, callback, at) -> None:
        self.scheduled.append((callback, at))

    def require_history(self, timeframe: str = "1d", bars: int = 1) -> None:
        self.history_requirements[timeframe] = bars

    def require_market_history(self, asset_type: str = "etf", timeframe: str = "1d", bars: int = 1) -> None:
        self.market_history_requirements[(asset_type, timeframe)] = bars

    def require_extra_history(self, name: str) -> None:
        self.extra_history_requirements.add(name)

    def extra_history(self, name: str, symbol: str, count: int = 1, end_date=None):
        return list(self.extra_rows.get((name, symbol), []))[-count:]

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


def test_standard_lifecycle_does_not_export_legacy_alias():
    assert not hasattr(five, "on_session_start")


def test_initialize_registers_available_etf_universe():
    context = initialized_context()

    assert context.universe == sorted({*five.WUFU_MINUTE_POOL, five.DEFENSIVE_ETF})
    assert context.history_requirements == {"1d": 61}
    assert context.market_history_requirements == {
        ("etf", "1d"): 61,
        ("index", "1d"): 21,
    }
    assert context.extra_history_requirements == {"unit_net_value"}


def test_market_catalog_uses_minute_availability_instead_of_symbol_exclusions():
    context = FakeContext()
    context.instruments = lambda _asset=None: [
        {"symbol": "161226.SZ", "name": "国投白银LOF", "has_minute": True},
        {"symbol": "164824.SZ", "name": "印度基金LOF", "has_minute": False},
    ]

    _, _, _, minute_symbols = five._market_catalog(context)

    assert "161226.SZ" in minute_symbols
    assert "164824.SZ" not in minute_symbols


def test_historical_etf_names_preserve_original_dynamic_groups():
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["520500.SH"]) == "香港组:药"
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588020.SH"]) is None
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588790.SH"]) is None
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["516080.SH"]) == "普通组:创医"
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["589680.SH"]) == "科创组:综Z"
    assert five._dynamic_group("恒生创新药ETF华泰柏瑞") == "香港组:创药"
    assert five._dynamic_group("港股红利ETF") == "香港组:红利"
    assert five._dynamic_group("科创芯片ETF南方") == "科创组:芯片"
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588890.SH"]) == "科创组:芯"


def test_reference_dynamic_pool_excludes_unavailable_fund():
    context = FakeContext()
    context.instruments = lambda _asset=None: [
        {"symbol": "159814.SZ", "name": "创业大盘ETF西部利得", "has_minute": True},
        {"symbol": "588220.SH", "name": "科创100ETF鹏华", "has_minute": True},
    ]

    _, _, groups, _ = five._market_catalog(context)

    assert "159814.SZ" not in groups


def test_laplace_smoothing_is_regime_specific():
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 10)
    closes = [100 * (1.001 ** index) for index in range(70)]
    volumes = [1_000_000.0] * 69
    context.extra_rows[("unit_net_value", "510300.SH")] = [{"date": "2026-07-19", "value": closes[-2]}]

    normal = five._metric_for("510300.SH", closes, volumes, 550_000, context, "正常期", "2026-07-19")
    weak = five._metric_for("510300.SH", closes, volumes, 550_000, context, "走弱期", "2026-07-19")

    assert normal is not None and weak is not None
    assert normal["laplace_s"] == pytest.approx(0.06)
    assert weak["laplace_s"] == pytest.approx(0.12)
    assert weak["laplace_value"] > normal["laplace_value"]


def test_history_is_aligned_to_previous_raw_close():
    context = initialized_context()
    context.market_rows["159667.SZ"] = [
        SimpleNamespace(
            date=datetime(2026, 7, day).date(),
            close=close,
            raw_close=close * 3,
            volume=1_000,
            amount=1_000,
        )
        for day, close in ((17, 0.45), (18, 0.46), (19, 0.47))
    ]

    rows = five._history_rows(context, "159667.SZ", 3)

    assert [row["close"] for row in rows] == pytest.approx([1.35, 1.38, 1.41])


def test_history_uses_exact_etf_split_ratio():
    context = initialized_context()
    context.market_rows["515000.SH"] = [
        SimpleNamespace(
            date=datetime(2025, 9, 5).date(),
            close=1.835 / 1.994565,
            raw_close=1.835,
            volume=1_000,
            amount=1_000,
            split_ratio=1.0,
        ),
        SimpleNamespace(
            date=datetime(2025, 9, 8).date(),
            close=0.908,
            raw_close=0.908,
            volume=1_000,
            amount=1_000,
            split_ratio=2.0,
        ),
    ]

    rows = five._history_rows(context, "515000.SH", 2)

    assert [row["close"] for row in rows] == pytest.approx([1.835 / 2, 0.908])


def test_weighted_momentum_matches_original_r_squared_weighting():
    score, annualized, r2 = five._weighted_momentum(variable_prices(), 25)

    assert score == pytest.approx(1.0092122547173843)
    assert annualized == pytest.approx(1.038545691888901)
    assert r2 == pytest.approx(0.9717552752848408)


def test_momentum_entry_uses_upper_bound_only_for_stale_quote():
    context = initialized_context()
    rows = [
        {**metric("A", 4.997, variable_prices(1)), "entry_score": 5.001},
        {**metric("B", 4.9, variable_prices(2)), "entry_score": 4.95},
    ]
    state = context.state["five_fortunes"]
    state["all_metric_rows"] = rows

    targets = five._choose_targets(context, rows, rows)

    assert targets == ["B"]

    rows[0]["entry_score"] = rows[0]["score"]
    targets = five._choose_targets(context, rows, rows)

    assert targets == ["A"]


def test_momentum_filter_keeps_hard_upper_bound():
    row = {
        "score": 5.07,
        "r2": 1.0,
        "volume_ratio": 1.0,
        "day_ratios": [1.0, 1.0, 1.0],
        "passed_premium": True,
        "close": 1.0,
        "laplace_value": 0.9,
        "laplace_slope": 0.01,
    }

    assert five._passes_filters(row, "正常期") is False


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
    context.portfolio.positions = {"A": 218_300.0}
    state = context.state["five_fortunes"]
    state["regime"] = "走弱期"
    state["peak_equity"] = 100.0
    context.now = datetime(2026, 7, 20, 10, 31)

    five._risk_monitor(context)

    assert context.orders == [("quantity", "A", 109_100)]
    assert state["position_scale"] == 1.0
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


def test_weak_regime_noop_defensive_action_keeps_peak_equity():
    context = initialized_context(equity=91.0)
    context.portfolio.positions = {five.DEFENSIVE_ETF: 100.0}
    state = context.state["five_fortunes"]
    state["regime"] = "走弱期"
    state["peak_equity"] = 100.0
    context.now = datetime(2026, 7, 20, 10, 31)

    five._risk_monitor(context)

    assert context.orders == []
    assert state["peak_equity"] == 100.0
    assert state["risk_action_date"] is None
    assert state["risk_actions"] == []


def test_drawdown_clear_action_can_enter_a_different_target_same_day():
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 11)
    state = context.state["five_fortunes"]
    state["risk_action_date"] = context.now.date().isoformat()
    state["rebuy_cooldown"]["SOLD"] = 3
    state["target"] = ["NEW"]
    state["intraday"]["raw_close"]["NEW"] = 1.0

    five._buy_targets(context)

    assert context.orders == [("quantity", "NEW", 999_800)]


def test_buy_targets_reserves_reference_commission_and_slippage():
    context = initialized_context()
    state = context.state["five_fortunes"]
    state["target"] = ["A"]
    state["intraday"]["raw_close"]["A"] = 0.888

    five._buy_targets(context)

    assert context.orders == [("quantity", "A", 1_125_900)]


def test_rebuy_cooldown_keeps_selected_target_but_filters_when_buying():
    context = initialized_context()
    state = context.state["five_fortunes"]
    state["rebuy_cooldown"]["A"] = 1
    state["intraday"]["raw_close"]["A"] = 1.0
    rows = [metric("A", 4.0, variable_prices())]

    state["target"] = five._choose_targets(context, rows, rows)

    five._buy_targets(context)

    assert state["target"] == ["A"]
    assert context.orders == []


def test_limit_up_holding_is_not_sold_or_replaced(monkeypatch):
    context = initialized_context()
    context.portfolio.positions = {"A": 100.0}
    state = context.state["five_fortunes"]
    state["intraday"]["raw_close"].update({"A": 1.1, "B": 1.0})
    state["intraday"]["limit_up"]["A"] = 1.1
    target = metric("B", 4.0, variable_prices())

    monkeypatch.setattr(five, "_rank_candidates", lambda _context: [target])
    monkeypatch.setattr(five, "_candidate_pool", lambda *_args: [target])
    monkeypatch.setattr(five, "_choose_targets", lambda *_args: ["B"])

    five._prepare_and_sell(context)
    five._buy_targets(context)

    assert context.orders == []


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


def test_candidate_pool_uses_reference_ninety_percent_boundary():
    rows = [metric("A", 4.753851, []), metric("B", 4.278, [])]

    assert [row["symbol"] for row in five._candidate_pool(rows, "震荡期")] == ["A"]


def test_rank_candidate_correlation_history_excludes_current_snapshot(monkeypatch):
    context = FakeContext()
    five.initialize(context)
    state = context.state["five_fortunes"]
    state["regime"] = "震荡期"
    state["normal_liquidity_pool"] = ["A"]
    state["intraday"] = {
        "date": "2025-09-12",
        "close": {"A": 999.0},
        "volume": {"A": 1.0},
    }
    rows = [
        {"date": f"2025-01-{index + 1:02d}", "close": float(index + 1), "volume": 1.0, "amount": 1.0}
        for index in range(61)
    ]
    monkeypatch.setattr(five, "_history_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(
        five,
        "_metric_for",
        lambda *_args, **_kwargs: {
            "symbol": "A",
            "score": 1.0,
            "history": [],
        },
    )
    monkeypatch.setattr(five, "_passes_filters", lambda *_args, **_kwargs: True)

    ranked = five._rank_candidates(context)

    assert ranked[0]["history"] == [float(value) for value in range(2, 62)]
    assert 999.0 not in ranked[0]["history"]


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
