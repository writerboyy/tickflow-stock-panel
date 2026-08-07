from datetime import date, datetime

import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine, Quote
from app.free_strategy.schedule import ScheduleRule, parse_time_expression


def test_time_expression_supports_market_offsets_and_rejects_false_precision():
    assert parse_time_expression("open-30m") == "09:00"
    assert parse_time_expression("close+15m") == "15:15"
    assert parse_time_expression("every_bar") == "every_bar"
    with pytest.raises(ValueError, match="分钟精度"):
        parse_time_expression(datetime(2024, 1, 2, 9, 30, 1).time())
    with pytest.raises(ValueError, match="HH:MM"):
        parse_time_expression("open-30s")


def test_weekly_and_monthly_rules_use_actual_trade_day_ordinals():
    dates = [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    assert ScheduleRule("weekly", "09:30", 1).matches_date(date(2024, 1, 2), dates)
    assert ScheduleRule("weekly", "09:30", -1).matches_date(date(2024, 1, 5), dates)
    assert ScheduleRule("monthly", "09:30", 4).matches_date(date(2024, 1, 5), dates)
    assert not ScheduleRule("monthly", "09:30", 6).matches_date(date(2024, 1, 8), dates)


def test_joinquant_schedule_functions_run_in_registration_order():
    source = """
def initialize(context):
    run_daily(first, time='open-30m')
    run_daily(second, time='09:00')

def first(context):
    context.state.setdefault('events', []).append(('first', context.now.isoformat()))

def second(context):
    context.state.setdefault('events', []).append(('second', context.now.isoformat()))
"""
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.set_trading_calendar([date(2024, 1, 2)])
    engine.advance_event(
        datetime(2024, 1, 2, 9, 0),
        [],
        event_type="scheduled",
        scheduled_at="09:00",
    )

    assert engine.context.state["events"] == [
        ("first", "2024-01-02T09:00:00"),
        ("second", "2024-01-02T09:00:00"),
    ]


def test_every_bar_uses_only_actual_tickflow_bar_timestamps():
    source = """
def initialize(context):
    run_daily(record, time='every_bar')

def record(context):
    context.state.setdefault('events', []).append(context.now.isoformat())
"""
    bars = [
        Bar("X", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 11, 30), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 13, 1), 10, 10, 10, 10),
    ]
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.run(bars)

    assert engine.execution_mode == "full_bar"
    assert engine.context.state["events"] == [bar.timestamp.isoformat() for bar in bars]


def test_weekly_and_monthly_callbacks_share_the_engine_trade_calendar():
    source = """
def initialize(context):
    run_weekly(weekly, weekday=-1, time='09:31')
    run_monthly(monthly, monthday=2, time='09:31')

def weekly(context):
    context.state.setdefault('events', []).append(('weekly', context.now.date().isoformat()))

def monthly(context):
    context.state.setdefault('events', []).append(('monthly', context.now.date().isoformat()))
"""
    days = [date(2024, 1, day) for day in (2, 3, 4, 5, 8)]
    bars = [Bar("X", datetime.combine(day, datetime.min.time()).replace(hour=9, minute=31), 10, 10, 10, 10) for day in days]
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.run(bars)

    assert engine.context.state["events"] == [
        ("monthly", "2024-01-03"),
        ("weekly", "2024-01-05"),
        ("weekly", "2024-01-08"),
    ]


def test_advance_event_has_one_deterministic_callback_and_fill_order():
    source = """
def initialize(context):
    context.set_universe(['X', 'Y'])
    context.schedule(scheduled, '09:32')

def on_bar(context, bars):
    context.state.setdefault('events', []).append(
        ('bar', context.portfolio.positions.get('X', 0), context.portfolio.positions.get('Y', 0))
    )
    context.buy('Y', quantity=100)

def scheduled(context):
    context.state['events'].append(
        ('scheduled', context.portfolio.positions.get('X', 0), context.portfolio.positions.get('Y', 0))
    )
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(fill_policy="next_open", slippage_bps=0),
    )
    engine.context.now = datetime(2024, 1, 2, 9, 31)
    engine._next_timestamp = engine.context.now  # noqa: SLF001
    engine.context.buy("X", quantity=100)
    engine.config.fill_policy = "current_close"

    timestamp = datetime(2024, 1, 2, 9, 32)
    engine.advance_event(
        timestamp,
        [
            Bar("X", timestamp, 10, 10, 10, 10),
            Bar("Y", timestamp, 20, 20, 20, 20),
        ],
        event_type="bar",
    )

    assert engine.context.state["events"] == [
        ("bar", 100, 0),
        ("scheduled", 100, 0),
    ]
    assert [(fill.symbol, fill.timestamp) for fill in engine.account.fills] == [
        ("X", timestamp.isoformat()),
        ("Y", timestamp.isoformat()),
    ]


def test_bar_and_quote_paths_share_the_same_event_trajectory():
    bar_source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(scheduled, '09:31')

def on_bar(context, bars):
    context.state.setdefault('events', []).append(('primary', context.portfolio.positions.get('X', 0)))
    context.buy('X', quantity=100)

def scheduled(context):
    context.state['events'].append(('scheduled', context.portfolio.positions.get('X', 0)))
"""
    quote_source = bar_source.replace("def on_bar(context, bars):", "def on_quote(context, quotes):")
    config = FreeStrategyConfig(fill_policy="current_close", slippage_bps=0)
    bar_engine = FreeStrategyEngine(bar_source, timeframe="1m", config=config)
    quote_engine = FreeStrategyEngine(
        quote_source,
        timeframe="1m",
        config=FreeStrategyConfig(fill_policy="current_close", slippage_bps=0),
    )
    timestamp = datetime(2024, 1, 2, 9, 31)
    bar_engine.advance_event(
        timestamp,
        [Bar("X", timestamp, 10, 10, 10, 10)],
        event_type="bar",
    )
    quote_engine.advance_event(
        timestamp,
        event_type="quote",
        quotes=[Quote("X", timestamp, 10, open=10, high=10, low=10)],
    )

    assert quote_engine.context.state["events"] == bar_engine.context.state["events"]
    assert [
        (fill.symbol, fill.side, fill.quantity, fill.price, fill.value, fill.total_fee)
        for fill in quote_engine.account.fills
    ] == [
        (fill.symbol, fill.side, fill.quantity, fill.price, fill.value, fill.total_fee)
        for fill in bar_engine.account.fills
    ]
