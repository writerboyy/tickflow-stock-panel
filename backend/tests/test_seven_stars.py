from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from app.free_strategy import seven_stars as seven


class FakeContext:
    def __init__(self, equity: float = 100_000.0) -> None:
        self.state = {}
        self.now = datetime(2026, 7, 20, 9, 30)
        self.portfolio = SimpleNamespace(
            total_value=equity,
            cash=equity,
            positions={},
            available_positions={},
            avg_cost={},
        )
        self.universe = []
        self.scheduled = []
        self.history_requirements = {}
        self.extra_history_requirements = set()
        self.daily_rows = {}
        self.minute_rows = {}
        self.nav_rows = {}
        self.orders = []
        self.logs = []
        self.signals = []

    def set_universe(self, symbols) -> None:
        self.universe = list(symbols)

    def schedule(self, callback, at) -> None:
        self.scheduled.append((callback, at))

    def require_history(self, timeframe: str = "1d", bars: int = 1) -> None:
        self.history_requirements[timeframe] = bars

    def require_extra_history(self, name: str) -> None:
        self.extra_history_requirements.add(name)

    def history_bars(self, symbol: str, count: int = 20, timeframe: str = "1d"):
        source = self.daily_rows if timeframe == "1d" else self.minute_rows
        return list(source.get(symbol, []))[-count:]

    def extra_history(self, name: str, symbol: str, count: int = 1, end_date=None):
        assert name == "unit_net_value"
        rows = list(self.nav_rows.get(symbol, []))
        if end_date is not None:
            rows = [row for row in rows if row["date"] <= str(end_date)]
        return rows[-count:]

    def order_target(self, symbol: str, quantity: float) -> None:
        self.orders.append((symbol, quantity))

    def emit_signal(self, signal_type: str, payload: dict, *, event_id: str | None = None) -> None:
        self.signals.append({"id": event_id, "signal_type": signal_type, **payload})

    def log(self, message: str, level: str = "INFO") -> None:
        self.logs.append((level, message))


def bar(
    close: float,
    *,
    high: float | None = None,
    volume: float = 1_000.0,
    suspended: bool = False,
):
    return SimpleNamespace(
        close=close,
        raw_close=close,
        high=high if high is not None else close,
        raw_high=high if high is not None else close,
        volume=volume,
        suspended=suspended,
        tradable=not suspended,
        limit_up=close * 1.1,
        limit_down=close * 0.9,
    )


def initialized_context() -> FakeContext:
    context = FakeContext()
    seven.initialize(context)
    seven.before_trading_start(context)
    return context


def test_initialize_registers_reference_universe_and_schedule():
    context = initialized_context()

    assert context.universe == seven.SEVEN_STARS_ETF_POOL
    assert context.history_requirements == {"1d": 45}
    assert context.extra_history_requirements == {"unit_net_value"}
    assert [at for _, at in context.scheduled] == [
        "09:10",
        "13:09",
        "13:10",
        *seven.PROFIT_PROTECTION_TIMES,
    ]
    assert callable(seven.on_bar)


def test_weighted_momentum_matches_joinquant_polyfit():
    prices = [100.0]
    changes = (0.01, -0.004, 0.007, -0.002, 0.012, -0.006)
    for index in range(1, 46):
        prices.append(prices[-1] * (1 + changes[index % len(changes)]))

    score, annualized, r_squared = seven._weighted_momentum(prices, 25)

    assert score == pytest.approx(0.9644483158678002)
    assert annualized == pytest.approx(0.9926817010197448)
    assert r_squared == pytest.approx(0.971558471237113)


def test_short_momentum_uses_endpoint_return_instead_of_weighted_slope():
    prices = [2.275, 2.289, 2.287, 2.297, 2.318, 2.342, 2.383, 2.345, 2.301, 2.303, 2.296]

    short_annualized = seven._short_annualized_return(prices, 10)

    assert short_annualized == pytest.approx((2.296 / 2.275) ** 25 - 1)
    assert short_annualized > 0


def test_profit_protection_sells_and_blocks_same_day_reentry():
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 9, 45)
    context.portfolio.positions = {"159915.SZ": 43_000.0}
    context.portfolio.available_positions = {"159915.SZ": 43_000.0}
    context.daily_rows["159915.SZ"] = [bar(2.5, high=2.5)]
    seven.on_bar(context, {"159915.SZ": bar(2.37)})

    seven._profit_protection_check(context)

    assert context.orders == [("159915.SZ", 0)]
    assert seven._state(context)["reentry_blocked_today"] == ["159915.SZ"]


def test_profit_protection_uses_previous_close_before_delayed_open():
    context = initialized_context()
    context.now = datetime(2026, 1, 30, 9, 45)
    context.portfolio.positions = {"513310.SH": 64_200.0}
    context.portfolio.available_positions = {"513310.SH": 64_200.0}
    context.daily_rows["513310.SH"] = [bar(3.652, high=3.858)]

    seven._profit_protection_check(context)

    assert context.orders == [("513310.SH", 0)]
    assert seven._state(context)["reentry_blocked_today"] == ["513310.SH"]


def test_sell_then_buy_uses_same_cached_ranking(monkeypatch):
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 9)
    context.portfolio.positions = {"159915.SZ": 43_000.0}
    context.portfolio.available_positions = {"159915.SZ": 43_000.0}
    seven.on_bar(context, {"159915.SZ": bar(2.3), "588080.SH": bar(1.1)})
    ranking = [{"symbol": "588080.SH", "score": 2.0}]
    monkeypatch.setattr(seven, "_rank_candidates", lambda _context: ranking)
    monkeypatch.setattr(seven, "_passes_premium_filter", lambda *_args: True)

    seven._sell_targets(context)
    context.portfolio.positions = {"159915.SZ": 0.0}
    context.portfolio.available_positions = {"159915.SZ": 0.0}
    context.now = datetime(2026, 7, 20, 13, 10)
    seven._buy_targets(context)

    assert context.orders == [("159915.SZ", 0), ("588080.SH", 90_800)]
    signal = context.signals[-1]
    assert signal["id"] == "seven_stars:2026-07-20:decision"
    assert signal["decision"] == "rebalance"
    assert signal["target_symbols"] == ["588080.SH"]


def test_strategy_capital_rolls_forward_after_sell_commission():
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 9)
    context.portfolio.positions = {"159915.SZ": 43_000.0}
    context.portfolio.available_positions = {"159915.SZ": 43_000.0}
    seven._state(context)["entry_prices"]["159915.SZ"] = 2.325
    seven.on_bar(context, {"159915.SZ": bar(2.425)})

    assert seven._exit_position(context, "159915.SZ") is True

    assert seven._state(context)["trade_capital"] == pytest.approx(104_279.145)
    assert context.orders == [("159915.SZ", 0)]


def test_sell_keeps_top_ranked_holding_when_previous_nav_is_missing(monkeypatch):
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 9)
    context.portfolio.positions = {"513690.SH": 80_000.0}
    context.portfolio.available_positions = {"513690.SH": 80_000.0}
    previous = bar(1.14)
    previous.timestamp = datetime(2026, 7, 17, 15)
    context.daily_rows["513690.SH"] = [previous]
    seven.on_bar(context, {"513690.SH": bar(1.15)})
    monkeypatch.setattr(
        seven,
        "_rank_candidates",
        lambda _context: [{"symbol": "513690.SH", "score": 2.0}],
    )

    seven._sell_targets(context)

    assert context.orders == []


def test_sell_exits_top_ranked_holding_when_premium_exceeds_limit(monkeypatch):
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 9)
    context.portfolio.positions = {"513690.SH": 80_000.0}
    context.portfolio.available_positions = {"513690.SH": 80_000.0}
    previous = bar(1.14)
    previous.timestamp = datetime(2026, 7, 17, 15)
    context.daily_rows["513690.SH"] = [previous]
    context.nav_rows["513690.SH"] = [{"date": "2026-07-17", "value": 0.90}]
    seven.on_bar(context, {"513690.SH": bar(1.15)})
    monkeypatch.setattr(
        seven,
        "_rank_candidates",
        lambda _context: [{"symbol": "513690.SH", "score": 2.0}],
    )

    seven._sell_targets(context)

    assert context.orders == [("513690.SH", 0)]


def test_buy_fails_closed_when_previous_nav_is_missing(monkeypatch):
    context = initialized_context()
    context.now = datetime(2026, 7, 20, 13, 10)
    seven.on_bar(context, {"588080.SH": bar(1.1)})
    monkeypatch.setattr(
        seven,
        "_rank_candidates",
        lambda _context: [{"symbol": "588080.SH", "score": 2.0}],
    )

    seven._buy_targets(context)

    assert context.orders == []
    assert context.signals[-1]["decision"] == "empty"
