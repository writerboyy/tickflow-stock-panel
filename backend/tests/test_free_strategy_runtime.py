from datetime import datetime

from app.free_strategy.bars import Bar, aggregate_minute_bars
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine


def test_minute_aggregation_respects_lunch_boundary():
    rows = [
        Bar("510300.SH", datetime(2024, 1, 2, 11, 28), 1, 2, 1, 2, 1, 1),
        Bar("510300.SH", datetime(2024, 1, 2, 11, 29), 2, 3, 2, 3, 2, 2),
        Bar("510300.SH", datetime(2024, 1, 2, 13, 0), 4, 5, 4, 5, 4, 4),
    ]
    result = aggregate_minute_bars(rows, 5)
    assert [(bar.timestamp.hour, bar.timestamp.minute, bar.open, bar.close) for bar in result] == [
        (11, 25, 1, 3), (13, 0, 4, 5)
    ]


def test_next_open_fill_and_t1_are_default():
    source = """
def on_bar(context, bars):
    if context.now.hour == 15 and context.now.day == 1:
        context.buy('X', quantity=100)
    if context.now.day == 2:
        context.sell('X', quantity=100)
"""
    bars = [Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10), Bar("X", datetime(2024, 1, 2, 15), 11, 11, 11, 11), Bar("X", datetime(2024, 1, 3, 15), 12, 12, 12, 12)]
    result = FreeStrategyEngine(source, config=FreeStrategyConfig(lot_size=100)).run(bars)
    assert [round(fill["price"], 4) for fill in result["fills"]] == [11.0055, 11.994]
    assert result["positions"] == {"X": 0.0}


def test_t0_can_sell_on_same_day():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)

def after_trading_end(context):
    if context.now.day == 1:
        context.sell('X', quantity=100)
"""
    bars = [Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10)]
    t1 = FreeStrategyEngine(source, config=FreeStrategyConfig(lot_size=100, fill_policy="close")).run(bars)
    t0 = FreeStrategyEngine(source, config=FreeStrategyConfig(lot_size=100, settlement="t0", fill_policy="close")).run(bars)
    assert len(t1["fills"]) == 1
    assert len(t0["fills"]) == 2


def test_lifecycle_and_scheduled_callback():
    source = """
def initialize(context):
    context.state['events'] = []
    def midday(ctx):
        ctx.state['events'].append('scheduled')
    context.schedule(midday, '13:00')

def before_trading_start(context):
    context.state['events'].append('before')

def on_bar(context, bars):
    context.state['events'].append('bar')

def after_trading_end(context):
    context.state['events'].append('after')
"""
    bars = [
        Bar("X", datetime(2024, 1, 1, 9, 30), 1, 1, 1, 1),
        Bar("X", datetime(2024, 1, 1, 13, 0), 1, 1, 1, 1),
    ]
    result = FreeStrategyEngine(source, timeframe="1m").run(bars)
    assert result["state"]["events"] == ["before", "bar", "scheduled", "bar", "after"]


def test_paper_session_end_runs_once_and_supports_session_aliases():
    source = """
def initialize(context):
    context.state['events'] = []

def on_session_start(context):
    context.state['events'].append('start')

def on_bar(context, bars):
    context.state['events'].append('bar')

def after_market_close(context):
    context.state['events'].append('after')
"""
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.run([Bar("X", datetime(2024, 1, 2, 14, 59), 1, 1, 1, 1)], finalize_session=False)
    assert engine.state["events"] == ["start", "bar"]
    assert engine.finish_session() is True
    assert engine.finish_session() is False
    assert engine.state["events"] == ["start", "bar", "after"]

    engine.run([Bar("X", datetime(2024, 1, 3, 9, 30), 1, 1, 1, 1)], finalize_session=False)
    assert engine.finish_session() is True
    assert engine.state["events"] == ["start", "bar", "after", "start", "bar", "after"]
