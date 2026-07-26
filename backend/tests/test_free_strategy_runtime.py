from datetime import datetime

import pytest

from app.free_strategy.bars import Bar, aggregate_minute_bars
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.five_fortunes import DEFENSIVE_ETF, REGIME_PROXIES


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


def test_five_fortunes_runs_on_minute_bars_and_records_daily_candidates():
    source = """
from app.free_strategy.five_fortunes import after_trading_end, initialize, on_bar, on_session_start
"""
    symbols = [*REGIME_PROXIES, "518880.SH", DEFENSIVE_ETF]
    bars = []
    for day in range(1, 67):
        for hour, minute in ((9, 30), (9, 40), (10, 31), (13, 10), (13, 11), (15, 0)):
            for index, symbol in enumerate(symbols):
                price = (1 + index * 0.01) * (1.0025 ** day)
                bars.append(Bar(symbol, datetime(2024, 1 + (day - 1) // 28, 1 + (day - 1) % 28, hour, minute), price, price, price, price, 1, price))
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(asset_type="etf", fill_policy="next_open", benchmark_symbol="510300.SH"),
    ).run(iter(bars))
    reports = result["state"]["five_fortunes"]["daily_reports"]
    assert reports
    assert reports[-1]["nav_filter"] == "skipped_no_data"
    assert result["daily_equity_curve"]
    assert result["performance"]["benchmark_return_pct"] > 0


def test_checkpoint_restores_account_curve_and_strategy_state():
    source = """
def initialize(context):
    context.state.setdefault('days', 0)

def on_bar(context, bars):
    context.state['days'] += 1
    if context.state['days'] == 1:
        context.buy('X', quantity=100)
"""
    config = FreeStrategyConfig(lot_size=100, fill_policy="close")
    first = FreeStrategyEngine(source, config=config)
    initial = first.run([Bar("X", datetime(2024, 1, 2, 15), 10, 10, 10, 10)])

    resumed = FreeStrategyEngine(source, config=config)
    resumed.restore_checkpoint(initial["checkpoint"])
    final = resumed.run([Bar("X", datetime(2024, 1, 3, 15), 11, 11, 11, 11)])

    assert final["state"]["days"] == 2
    assert len(final["daily_equity_curve"]) == 2
    assert final["positions"] == {"X": 100.0}


def test_result_contains_diagnostic_transactions_metrics_and_attribution():
    source = """
def on_bar(context, bars):
    day = context.now.day
    if day == 1:
        context.sell('X', quantity=100, reason='no-position')
        context.buy('X', quantity=100, reason='first-entry')
    elif day == 2:
        context.sell('X', quantity=100, reason='first-exit')
    elif day == 3:
        context.buy('X', quantity=100, reason='second-entry')
    elif day == 4:
        context.sell('X', quantity=100, reason='second-exit')
"""
    prices = [10.0, 12.0, 12.0, 11.0, 11.0]
    benchmark = [100.0, 101.0, 102.0, 101.0, 103.0]
    bars = []
    for day, (price, benchmark_price) in enumerate(zip(prices, benchmark), start=1):
        timestamp = datetime(2024, 1, day, 15)
        bars.extend([
            Bar("B", timestamp, benchmark_price, benchmark_price, benchmark_price, benchmark_price),
            Bar("X", timestamp, price, price, price, price),
        ])

    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=10_000,
            fees_pct=0,
            stamp_tax_pct=0,
            slippage_bps=0,
            lot_size=100,
            fill_policy="close",
            benchmark_symbol="B",
        ),
    ).run(bars)

    assert len(result["signals"]) == 5
    assert len(result["transactions"]) == 5
    rejected = result["transactions"][0]
    assert rejected["status"] == "rejected"
    assert rejected["filled_quantity"] == 0
    assert rejected["reason"] == "数量不足、现金不足或 T+1 未结算"

    realized = [row["realized_pnl"] for row in result["attribution"] if row["side"] == "sell"]
    assert realized == [200.0, -100.0]
    assert result["performance"]["trade_win_rate_pct"] == 50.0
    assert result["performance"]["profit_loss_ratio"] == 2.0

    daily = result["daily_equity_curve"]
    assert len(daily) == 5
    assert daily[0]["daily_return_pct"] == 0.0
    assert daily[1]["benchmark_daily_return_pct"] == pytest.approx(1.0)
    assert result["performance"]["max_drawdown_start"] == "2024-01-02"
    assert result["performance"]["max_drawdown_end"] == "2024-01-04"
    for key in ("alpha_pct", "beta", "sortino_ratio", "information_ratio", "benchmark_volatility_pct"):
        assert key in result["performance"]


def test_target_order_below_one_lot_is_skipped_instead_of_rejected():
    source = """
def on_bar(context, bars):
    context.order_target_percent('X', 0.0)
"""
    result = FreeStrategyEngine(source, config=FreeStrategyConfig(fill_policy="close")).run([
        Bar("X", datetime(2024, 1, 2, 15), 10, 10, 10, 10),
    ])

    assert result["orders"][0]["status"] == "skipped"
    assert result["orders"][0]["reason"] == "目标仓位无需调整或不足一手"
