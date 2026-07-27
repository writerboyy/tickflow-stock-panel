from datetime import datetime, timedelta

import pytest

from app.free_strategy.bars import Bar, aggregate_minute_bars
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.five_fortunes import DEFENSIVE_ETF, REGIME_PROXIES
from app.free_strategy.templates import TEMPLATES


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


def test_minute_aggregation_preserves_raw_prices_and_market_state():
    rows = [
        Bar("X", datetime(2024, 1, 2, 9, 30), 5, 6, 4, 5.5, raw_open=10, raw_high=12, raw_low=8, raw_close=11, limit_up=12, limit_down=8, split_ratio=2),
        Bar("X", datetime(2024, 1, 2, 9, 31), 5.5, 6.5, 5, 6, raw_open=11, raw_high=13, raw_low=10, raw_close=12, limit_up=12, limit_down=8, split_ratio=2),
    ]

    result = aggregate_minute_bars(rows, 5)[0]

    assert (result.raw_open, result.raw_high, result.raw_low, result.raw_close) == (10, 13, 8, 12)
    assert (result.limit_up, result.limit_down, result.split_ratio) == (12, 8, 2)


def test_daily_warmup_is_visible_without_running_callbacks_or_orders():
    source = """
def initialize(context):
    context.state['callbacks'] = 0

def on_bar(context, bars):
    context.state['callbacks'] += 1
    context.state['warmup'] = context.history('X', count=3, timeframe='1d')
"""
    engine = FreeStrategyEngine(source, timeframe="1m")
    warmup = [
        Bar("X", datetime(2024, 1, day, 15), day, day, day, day)
        for day in (1, 2, 3)
    ]

    assert engine.preload_history(warmup, "1d") == 3
    result = engine.run([Bar("X", datetime(2024, 1, 4, 9, 30), 4, 4, 4, 4)])

    assert result["state"] == {"callbacks": 1, "warmup": [1, 2, 3]}
    assert result["orders"] == []


def test_history_warmup_must_be_explicitly_declared_by_strategy():
    without_warmup = FreeStrategyEngine("def on_bar(context, bars):\n    pass\n")
    with_warmup = FreeStrategyEngine("""
def initialize(context):
    context.require_history(timeframe='1d', bars=61)
    context.require_history(timeframe='1d', bars=20)

def on_bar(context, bars):
    pass
""")

    assert without_warmup.history_requirements == {}
    assert with_warmup.history_requirements == {"1d": 61}


def test_market_history_is_explicit_and_hides_future_bars():
    source = """
def initialize(context):
    context.require_market_history(asset_type='etf', timeframe='1d', bars=61)

def before_trading_start(context):
    context.state['visible'] = [bar.close for bar in context.market_history_bars('X', count=10)]
    context.state['name'] = context.instruments('etf')[0]['name']

def on_bar(context, bars):
    pass
"""
    engine = FreeStrategyEngine(
        source,
        instruments=[{"symbol": "X", "name": "测试ETF", "asset_type": "etf", "has_minute": True}],
    )
    engine.preload_market_history([
        Bar("X", datetime(2024, 1, day, 15), day, day, day, day)
        for day in (1, 2, 3)
    ])

    assert engine.market_history_requirements == {("etf", "1d"): 61}
    assert engine.context.market_history_bars("X", count=10) == []

    engine.begin_session(datetime(2024, 1, 2).date())

    assert engine.context.state == {"visible": [1], "name": "测试ETF"}


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ("timeframe='5m', bars=10", "只支持 1d"),
        ("timeframe='1d', bars=0", "正整数"),
    ],
)
def test_history_warmup_declaration_validates_arguments(arguments, message):
    source = f"""
def initialize(context):
    context.require_history({arguments})

def on_bar(context, bars):
    pass
"""

    with pytest.raises(ValueError, match=message):
        FreeStrategyEngine(source)


def test_initialize_defines_universe_and_normalizes_joinquant_suffixes():
    source = """
def initialize(context):
    context.set_universe(['510300.XSHG', '159915.XSHE', '510300.SH'])

def on_bar(context, bars):
    pass
"""
    engine = FreeStrategyEngine(source)

    assert engine.universe == ["510300.SH", "159915.SZ"]


def test_set_universe_rejects_a_symbol_string():
    source = """
def initialize(context):
    context.set_universe('510300.SH')

def on_bar(context, bars):
    pass
"""
    with pytest.raises(ValueError, match="标的代码列表"):
        FreeStrategyEngine(source)


@pytest.mark.parametrize("template_id", ["dual_ma", "etf_rotation", "five_fortunes"])
def test_templates_define_universe_in_strategy_source(template_id):
    template = TEMPLATES[template_id]

    engine = FreeStrategyEngine(template["source"], timeframe=template.get("config", {}).get("timeframe", "1d"))

    assert engine.universe


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


def test_strategy_uses_adjusted_bar_but_fill_and_equity_use_raw_price():
    source = """
def on_bar(context, bars):
    context.state.setdefault('seen', []).append(bars['X'].close)
    if context.now.day == 1:
        context.buy('X', quantity=100)
"""
    bars = [
        Bar("X", datetime(2024, 1, 1, 15), 5, 5, 5, 5, raw_open=10, raw_high=10, raw_low=10, raw_close=10),
        Bar("X", datetime(2024, 1, 2, 9, 30), 6, 6, 6, 6, raw_open=12, raw_high=12, raw_low=12, raw_close=12),
    ]
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(initial_capital=10_000, fees_pct=0, slippage_bps=0),
    ).run(bars)

    assert result["state"]["seen"] == [5, 6]
    assert result["fills"][0]["price"] == 12
    assert result["final_equity"] == 10_000


def test_etf_split_adjusts_position_once_and_survives_checkpoint_restore():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
"""
    config = FreeStrategyConfig(
        initial_capital=10_000, asset_type="etf", fill_policy="close",
        fees_pct=0, slippage_bps=0,
    )
    engine = FreeStrategyEngine(source, timeframe="1m", config=config)
    engine.run([Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=20)], finalize_session=False)
    first_split_bar = Bar(
        "X", datetime(2024, 1, 2, 9, 30), 10, 10, 10, 10,
        raw_open=10, raw_high=10, raw_low=10, raw_close=10, split_ratio=2,
    )
    result = engine.run([first_split_bar])

    assert result["positions"] == {"X": 200.0}
    assert result["checkpoint"]["account"]["avg_cost"]["X"] == 10
    assert result["final_equity"] == 10_000

    resumed = FreeStrategyEngine(source, timeframe="1m", config=config)
    resumed.restore_checkpoint(result["checkpoint"])
    repeated = resumed.run([Bar(
        "X", datetime(2024, 1, 2, 10), 10, 10, 10, 10,
        raw_open=10, raw_high=10, raw_low=10, raw_close=10, split_ratio=2,
    )])
    assert repeated["positions"] == {"X": 200.0}


def test_etf_split_keeps_realized_attribution_cost_basis_continuous():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    elif context.now.day == 3:
        context.sell('X', quantity=context.portfolio.positions['X'])
"""
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=10_000, asset_type="etf", fill_policy="close",
            fees_pct=0, slippage_bps=0,
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=20),
        Bar("X", datetime(2024, 1, 2, 15), 10, 10, 10, 10, raw_close=10, split_ratio=2),
        Bar("X", datetime(2024, 1, 3, 15), 11, 11, 11, 11, raw_close=11),
    ])

    sell = [row for row in result["attribution"] if row["side"] == "sell"][0]
    assert sell["cost_basis"] == 2_000
    assert sell["realized_pnl"] == 200


@pytest.mark.parametrize(
    ("bar", "reason"),
    [
        (Bar("X", datetime(2024, 1, 1, 15), 11, 11, 11, 11, raw_close=11, limit_up=11), "涨停，买入未成交"),
        (Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10, tradable=False, suspended=True), "证券停牌或不可交易"),
    ],
)
def test_market_state_rejects_unfillable_buy_orders(bar, reason):
    source = """
def on_bar(context, bars):
    context.buy('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(fill_policy="close", slippage_bps=0),
    ).run([bar])

    assert result["orders"][0]["status"] == "rejected"
    assert result["orders"][0]["reason"] == reason


def test_limit_down_rejects_sell_order():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    else:
        context.sell('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(fill_policy="close", fees_pct=0, slippage_bps=0),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 15), 9, 9, 9, 9, raw_close=9, limit_down=9),
    ])

    assert result["orders"][-1]["status"] == "rejected"
    assert result["orders"][-1]["reason"] == "跌停，卖出未成交"


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
        ctx.state['scheduled_close'] = ctx.state['last_close']
    context.schedule(midday, '13:00')

def before_trading_start(context):
    context.state['events'].append('before')

def on_bar(context, bars):
    context.state['events'].append('bar')
    context.state['last_close'] = bars['X'].close

def after_trading_end(context):
    context.state['events'].append('after')
"""
    bars = [
        Bar("X", datetime(2024, 1, 1, 9, 30), 1, 1, 1, 1),
        Bar("X", datetime(2024, 1, 1, 13, 0), 2, 2, 2, 2),
    ]
    result = FreeStrategyEngine(source, timeframe="1m").run(bars)
    assert result["state"]["events"] == ["before", "bar", "bar", "scheduled", "after"]
    assert result["state"]["scheduled_close"] == 2


def test_paper_session_end_runs_once_with_standard_lifecycle():
    source = """
def initialize(context):
    context.state['events'] = []

def before_trading_start(context):
    context.state['events'].append('start')

def on_bar(context, bars):
    context.state['events'].append('bar')

def after_trading_end(context):
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


def test_legacy_lifecycle_aliases_are_not_executed():
    source = """
def initialize(context):
    context.state['events'] = []

def on_session_start(context):
    context.state['events'].append('legacy-start')

def on_bar(context, bars):
    context.state['events'].append('bar')

def on_session_end(context):
    context.state['events'].append('legacy-end')

def after_market_close(context):
    context.state['events'].append('legacy-after')
"""
    result = FreeStrategyEngine(source, timeframe="1m").run([
        Bar("X", datetime(2024, 1, 2, 14, 59), 1, 1, 1, 1),
    ])

    assert result["state"]["events"] == ["bar"]


def test_five_fortunes_runs_on_minute_bars_and_records_daily_candidates():
    source = """
from app.free_strategy.five_fortunes import after_trading_end, before_trading_start, initialize, on_bar
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


def test_five_fortunes_uses_daily_warmup_on_first_backtest_day():
    source = """
from app.free_strategy.five_fortunes import after_trading_end, before_trading_start, initialize, on_bar
"""
    symbols = [*REGIME_PROXIES, "518880.SH", DEFENSIVE_ETF]
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(asset_type="etf", benchmark_symbol="510300.SH"),
    )
    start = datetime(2024, 1, 1, 15)
    warmup = []
    for offset in range(65):
        timestamp = start + timedelta(days=offset)
        for index, symbol in enumerate(symbols):
            price = (1 + index * 0.01) * (1.002 ** offset)
            warmup.append(Bar(symbol, timestamp, price, price, price, price, 100, price * 100))
    engine.preload_history(warmup, "1d")
    current_day = start + timedelta(days=66)
    bars = []
    for hour, minute in ((9, 30), (9, 40), (10, 31), (13, 10), (13, 11), (15, 0)):
        for index, symbol in enumerate(symbols):
            price = (1 + index * 0.01) * (1.002 ** 66)
            bars.append(Bar(symbol, current_day.replace(hour=hour, minute=minute), price, price, price, price, 1, price))

    result = engine.run(bars)
    state = result["state"]["five_fortunes"]

    assert state["warmup_rows"] == 61 * len(symbols)
    assert state["all_metric_rows"]


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


def test_checkpoint_restores_next_open_pending_orders():
    source = """
def on_bar(context, bars):
    if context.now.day == 2:
        context.buy('X', quantity=100)
"""
    config = FreeStrategyConfig(lot_size=100)
    initial = FreeStrategyEngine(source, config=config)
    initial.run(
        [Bar("X", datetime(2024, 1, 2, 15), 10, 10, 10, 10)],
        finalize_session=False,
    )

    resumed = FreeStrategyEngine(source, config=config)
    resumed.restore_checkpoint(initial.checkpoint())
    result = resumed.run([Bar("X", datetime(2024, 1, 3, 9, 30), 11, 11, 11, 11)])

    assert result["fills"][0]["order_id"] == "o1"
    assert result["fills"][0]["timestamp"] == "2024-01-03T09:30:00"
    assert result["positions"] == {"X": 100.0}


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
