from __future__ import annotations

from datetime import date, datetime, timedelta

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.large_amount_first_board import _passes_daily_gate
from app.free_strategy.templates import TEMPLATES


SYMBOL = "000001.SZ"


def _config() -> FreeStrategyConfig:
    raw = dict(TEMPLATES["large_amount_first_board"]["config"])
    raw.pop("timeframe")
    raw.pop("asset_type")
    return FreeStrategyConfig(**raw)


def _snapshot(day: date, candidate_overrides: dict | None = None) -> dict:
    candidate = {
        "symbol": SYMBOL,
        "name": "平安银行",
        "limit_price": 11.0,
        "previous_raw_close": 10.0,
        "ret5_d1": -0.06,
        "ret20_d1": -0.05,
        "above_ma20_d1": False,
        "amount_expansion_d1": 1.2,
        "amount_median20_d1": 300_000_000.0,
        "market_cap_d1": 20_000_000_000.0,
        "prior_limit_close_5d": 0,
        "limit_up_count_d1": 4,
        "next_day_red_rate_d1": 0.80,
        "first_board_broken_rate_d1": 0.75,
    }
    candidate.update(candidate_overrides or {})
    return {
        "date": day.isoformat(),
        "as_of": (day - timedelta(days=1)).isoformat(),
        "scan_index_only": "daily_high_limit_touch",
        "candidates": [candidate],
    }


def _engine(
    snapshot_day: date,
    candidate_overrides: dict | None = None,
) -> FreeStrategyEngine:
    engine = FreeStrategyEngine(
        TEMPLATES["large_amount_first_board"]["source"],
        timeframe="1m",
        config=_config(),
        instruments=[{
            "symbol": SYMBOL,
            "name": "平安银行",
            "asset_type": "stock",
            "has_minute": True,
        }],
    )
    engine.set_limit_board_snapshot_loader(
        lambda day: _snapshot(day, candidate_overrides) if day == snapshot_day else {
            "date": day.isoformat(), "as_of": None, "candidates": [],
        }
    )
    return engine


def _bar(day: date, hour: int, minute: int, *, high: float, close: float, amount: float) -> Bar:
    timestamp = datetime.combine(day, datetime.min.time()).replace(hour=hour, minute=minute)
    return Bar(
        SYMBOL, timestamp, close, high, min(close, high), close,
        volume=max(amount / max(close, 0.01) / 100, 1), amount=amount,
        raw_open=close, raw_high=high, raw_low=min(close, high), raw_close=close,
        limit_up=11.0, limit_down=9.0,
    )


def test_template_uses_same_minute_exact_limit_fill():
    template = TEMPLATES["large_amount_first_board"]

    assert template["config"]["fill_policy"] == "close"
    assert template["config"]["limit_up_touch_fill"] is True
    assert template["config"]["slippage_bps"] == 0


def test_daily_gate_requires_five_day_pullback_of_at_least_five_percent():
    meta = _snapshot(date(2026, 8, 14))["candidates"][0]
    assert _passes_daily_gate(meta)
    assert not _passes_daily_gate({**meta, "ret5_d1": -0.0499})


def test_daily_gate_requires_premium_gene_thresholds_and_rejects_missing_data():
    meta = _snapshot(date(2026, 8, 14))["candidates"][0]

    assert _passes_daily_gate(meta)
    assert not _passes_daily_gate({**meta, "limit_up_count_d1": 3})
    assert not _passes_daily_gate({**meta, "next_day_red_rate_d1": 0.7999})
    assert not _passes_daily_gate({**meta, "first_board_broken_rate_d1": 0.7501})
    assert not _passes_daily_gate({**meta, "next_day_red_rate_d1": None})


def test_first_touch_fills_at_limit_and_later_break_does_not_undo_fill():
    day = date(2026, 8, 14)
    result = _engine(day).run([
        _bar(day, 10, 0, high=11.0, close=10.6, amount=1_100_000_000),
        _bar(day, 10, 1, high=10.8, close=10.4, amount=200_000_000),
    ])

    assert len(result["fills"]) == 1
    assert result["fills"][0]["price"] == 11.0
    assert result["fills"][0]["timestamp"] == "2026-08-14T10:00:00"
    assert result["positions"][SYMBOL] == 1_600
    assert len(result["strategy_signals"]) == 1


def test_first_touch_below_amount_threshold_is_not_reconsidered_later():
    day = date(2026, 8, 14)
    result = _engine(day).run([
        _bar(day, 10, 0, high=11.0, close=10.8, amount=900_000_000),
        _bar(day, 10, 1, high=11.0, close=11.0, amount=200_000_000),
    ])

    assert result["fills"] == []
    assert result["strategy_signals"] == []


def test_premium_gene_gate_blocks_order_after_large_first_touch():
    day = date(2026, 8, 14)
    result = _engine(day, {"limit_up_count_d1": 3}).run([
        _bar(day, 10, 0, high=11.0, close=11.0, amount=1_100_000_000),
    ])

    assert result["fills"] == []
    assert result["strategy_signals"] == []


def test_position_exits_on_fifth_future_trading_day():
    entry_day = date(2026, 8, 14)
    trading_days = [
        date(2026, 8, 17), date(2026, 8, 18), date(2026, 8, 19),
        date(2026, 8, 20), date(2026, 8, 21),
    ]
    bars = [_bar(entry_day, 10, 0, high=11.0, close=10.8, amount=1_100_000_000)]
    bars.extend(
        _bar(day, 14, 55, high=10.5, close=10.2, amount=100_000_000)
        for day in trading_days
    )

    result = _engine(entry_day).run(bars)

    assert len(result["fills"]) == 2
    assert result["fills"][1]["side"] == "sell"
    assert result["fills"][1]["timestamp"].startswith("2026-08-21T14:55")
    assert result["positions"][SYMBOL] == 0
