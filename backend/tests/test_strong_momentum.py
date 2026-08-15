from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.strong_momentum_snapshot import _with_candidate_features
from app.free_strategy.templates import TEMPLATES


SYMBOL = "000001.SZ"


def _snapshot(day: date) -> dict:
    return {
        "date": day.isoformat(),
        "as_of": (day - timedelta(days=1)).isoformat(),
        "selection_mode": "strict",
        "candidates": [{
            "symbol": SYMBOL,
            "name": "平安银行",
            "previous_raw_close": 10.0,
            "limit_price": 11.0,
            "previous_change": 0.08,
            "previous_ret5": 0.20,
            "previous_ret20": 0.30,
            "previous_amplitude": 0.10,
            "previous_turnover_rate": 8.0,
            "previous_volume_growth": 1.3,
            "previous_limit_up": False,
            "previous_high_volume_limit": False,
            "tail_gain_d1": 0.01,
        }],
    }


def _config() -> FreeStrategyConfig:
    raw = dict(TEMPLATES["strong_momentum"]["config"])
    raw.pop("timeframe")
    raw.pop("asset_type")
    return FreeStrategyConfig(**raw)


def _engine() -> FreeStrategyEngine:
    engine = FreeStrategyEngine(
        TEMPLATES["strong_momentum"]["source"],
        timeframe="1m",
        config=_config(),
        instruments=[{
            "symbol": SYMBOL,
            "name": "平安银行",
            "asset_type": "stock",
            "has_minute": True,
        }],
    )
    engine.set_strong_momentum_snapshot_loader(_snapshot)
    return engine


def _bar(
    day: date,
    hour: int,
    minute: int,
    *,
    open_price: float,
    close: float,
    high: float | None = None,
    low: float | None = None,
    limit_up: float = 11.0,
    limit_down: float = 9.0,
) -> Bar:
    high = max(open_price, close) if high is None else high
    low = min(open_price, close) if low is None else low
    return Bar(
        SYMBOL,
        datetime(day.year, day.month, day.day, hour, minute),
        open_price,
        high,
        low,
        close,
        volume=10_000,
        amount=100_000,
        raw_open=open_price,
        raw_high=high,
        raw_low=low,
        raw_close=close,
        limit_up=limit_up,
        limit_down=limit_down,
    )


def test_strong_momentum_template_uses_minute_morning_contract():
    template = TEMPLATES["strong_momentum"]

    assert template["name"] == "强者恒强·项目适配"
    assert template["config"]["timeframe"] == "1m"
    assert template["config"]["asset_type"] == "stock"
    assert template["config"]["settlement"] == "t1"
    assert template["config"]["fill_policy"] == "close"
    assert template["config"]["benchmark_symbol"] == "000300.SH"
    assert "jqdata" not in template["source"]
    assert "context.require_strong_momentum_snapshot" in template["source"]


def test_candidate_features_shift_all_selection_inputs_to_d1():
    start = date(2025, 12, 1)
    rows = []
    for offset in range(22):
        close = 10.0 + offset * 0.1
        rows.append({
            "symbol": SYMBOL,
            "date": start + timedelta(days=offset),
            "open": close,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "volume": 1_000 + offset * 100,
            "amount": 1_000_000.0,
            "raw_close": close,
            "raw_high": close * 1.02,
            "raw_low": close * 0.98,
            "turnover_rate": 5.0 + offset * 0.1,
            "pit_name": "平安银行",
        })
    base = pl.DataFrame(rows)
    changed = base.with_columns(
        pl.when(pl.col("date") == rows[-1]["date"])
        .then(pl.lit(99.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )

    original = _with_candidate_features(base).row(-1, named=True)
    modified = _with_candidate_features(changed).row(-1, named=True)

    for field in (
        "previous_raw_close",
        "previous_change",
        "previous_ret5",
        "previous_ret20",
        "previous_turnover_rate",
        "previous_volume_growth",
        "recent_limit_down_count",
    ):
        assert modified[field] == original[field]


def test_entry_fills_at_first_supported_morning_minute_and_never_at_close():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.2, close=10.2),
        _bar(day, 9, 31, open_price=10.2, close=10.3),
        _bar(day, 15, 0, open_price=10.5, close=10.5),
    ])

    assert len(result["fills"]) == 1
    assert result["fills"][0]["side"] == "buy"
    assert result["fills"][0]["timestamp"] == "2026-01-05T09:31:00"
    assert all(not fill["timestamp"].endswith("T15:00:00") for fill in result["fills"] if fill["side"] == "buy")
    assert result["strategy_signals"][0]["signal_type"] == "strong_momentum_entry"


def test_entry_ignores_unsupported_minute_and_can_buy_at_0937():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.2, close=10.2),
        _bar(day, 9, 33, open_price=10.3, close=10.4),
        _bar(day, 9, 37, open_price=10.4, close=10.5),
    ])

    assert [fill["timestamp"] for fill in result["fills"]] == ["2026-01-05T09:37:00"]


def test_auction_gate_rejects_open_above_eight_percent():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.9, close=10.9),
        _bar(day, 9, 31, open_price=10.9, close=10.8),
        _bar(day, 9, 32, open_price=10.8, close=10.7),
        _bar(day, 9, 37, open_price=10.7, close=10.6),
        _bar(day, 10, 29, open_price=10.6, close=10.5),
    ])

    assert result["fills"] == []
    assert result["strategy_signals"] == []


def test_t1_blocks_same_day_stop_and_allows_next_day_exit():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.2, close=10.2),
        _bar(first, 9, 31, open_price=10.2, close=10.3),
        _bar(first, 10, 20, open_price=10.0, close=9.7),
        _bar(first, 15, 0, open_price=9.8, close=10.0),
        _bar(second, 9, 30, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(second, 9, 31, open_price=10.0, close=9.5, limit_up=11.0),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-06T09:31:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "成本止损-4%"


def test_limit_touch_then_drawdown_exits_after_1020():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.2, close=10.2),
        _bar(first, 9, 31, open_price=10.2, close=10.3),
        _bar(first, 15, 0, open_price=10.3, close=10.4),
        _bar(second, 9, 30, open_price=10.95, close=10.95),
        _bar(second, 10, 20, open_price=10.9, close=11.0, high=11.0),
        _bar(second, 10, 21, open_price=10.9, close=10.8, high=10.9),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-06T10:21:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "涨停后回落1.5%"


def test_take_profit_exits_on_a_later_trading_day():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.0, close=10.0),
        _bar(first, 9, 31, open_price=10.0, close=10.0),
        _bar(first, 15, 0, open_price=10.9, close=11.0),
        _bar(second, 9, 30, open_price=11.55, close=11.55, limit_up=12.1),
        _bar(second, 9, 31, open_price=11.7, close=12.0, limit_up=12.1),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "止盈19%"


def test_position_exits_after_three_future_sessions():
    days = [date(2026, 1, day) for day in (5, 6, 7, 8)]
    engine = _engine()
    engine.set_trading_calendar(days)
    result = engine.run([
        _bar(days[0], 9, 30, open_price=10.0, close=10.0),
        _bar(days[0], 9, 31, open_price=10.0, close=10.0),
        _bar(days[0], 15, 0, open_price=10.0, close=10.0),
        _bar(days[1], 9, 30, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(days[1], 15, 0, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(days[2], 9, 30, open_price=11.025, close=11.0, limit_up=11.55),
        _bar(days[2], 15, 0, open_price=11.0, close=11.0, limit_up=11.55),
        _bar(days[3], 9, 30, open_price=11.55, close=11.55, limit_up=12.1),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-08T09:30:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "持有满3个交易日"
