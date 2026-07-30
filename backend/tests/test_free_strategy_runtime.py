from datetime import datetime, timedelta
from pathlib import Path

import pytest

from app.free_strategy.bars import Bar, aggregate_minute_bars
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy import five_fortunes, performance_small_cap, seven_stars, small_cap_limitup
from app.free_strategy.five_fortunes import (
    DEFENSIVE_ETF,
    REGIME_FALLBACK_PROXIES,
    REGIME_PROXIES,
)
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
        Bar("X", datetime(2024, 1, 2, 9, 30), 5, 6, 4, 5.5, raw_open=10, raw_high=12, raw_low=8, raw_close=11, limit_up=12, limit_down=8, split_ratio=2, cash_dividend=0.2),
        Bar("X", datetime(2024, 1, 2, 9, 31), 5.5, 6.5, 5, 6, raw_open=11, raw_high=13, raw_low=10, raw_close=12, limit_up=12, limit_down=8, split_ratio=2, cash_dividend=0.2),
    ]

    result = aggregate_minute_bars(rows, 5)[0]

    assert (result.raw_open, result.raw_high, result.raw_low, result.raw_close) == (10, 13, 8, 12)
    assert (result.limit_up, result.limit_down, result.split_ratio) == (12, 8, 2)
    assert result.cash_dividend == 0.2


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


def test_before_trading_start_uses_previous_close_for_portfolio_value():
    source = """
def initialize(context):
    context.state['before'] = []

def before_trading_start(context):
    context.state['before'].append((context.now.strftime('%H:%M'), context.portfolio.total_value))

def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=10)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_000,
            lot_size=1,
            fees_pct=0,
            slippage_bps=0,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 15), 100, 100, 100, 100),
    ])

    assert result["state"]["before"] == [("09:29", 1_000), ("09:29", 1_000)]


def test_intraday_backtest_adds_completed_sessions_to_daily_history():
    source = """
def initialize(context):
    context.state['daily_seen'] = []

def on_bar(context, bars):
    context.state['daily_seen'].append(context.history('X', count=10, timeframe='1d'))
"""
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.preload_history([
        Bar("X", datetime(2023, 12, 29, 15), 5, 5, 5, 5),
    ], "1d")

    engine.run([
        Bar("X", datetime(2024, 1, 2, 9, 30), 8, 10, 8, 10),
    ], return_result=False)
    result = engine.run([
        Bar("X", datetime(2024, 1, 3, 9, 30), 20, 20, 20, 20),
    ])

    assert result["state"]["daily_seen"] == [[5], [5, 10]]


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


@pytest.mark.parametrize(
    ("template_id", "instruments"),
    [
        ("dual_ma", []),
        ("etf_rotation", []),
        ("five_fortunes", []),
        ("seven_stars", []),
        ("small_cap_limitup", [{
            "symbol": "000001.SZ",
            "asset_type": "stock",
            "has_minute": True,
        }]),
        ("performance_small_cap", [{
            "symbol": "000001.SZ",
            "asset_type": "stock",
            "has_minute": True,
        }]),
    ],
)
def test_templates_define_universe_in_strategy_source(template_id, instruments):
    template = TEMPLATES[template_id]

    engine = FreeStrategyEngine(
        template["source"],
        timeframe=template.get("config", {}).get("timeframe", "1d"),
        instruments=instruments,
    )

    assert engine.universe


def test_five_fortunes_template_is_a_self_contained_source_snapshot():
    source = TEMPLATES["five_fortunes"]["source"]

    assert source == Path(five_fortunes.__file__).read_text(encoding="utf-8")
    assert "from app.free_strategy.five_fortunes import" not in source
    assert "from jqdata import" not in source


def test_seven_stars_template_is_a_self_contained_source_snapshot():
    source = TEMPLATES["seven_stars"]["source"]

    assert source == Path(seven_stars.__file__).read_text(encoding="utf-8")
    assert "from app.free_strategy.seven_stars import" not in source
    assert "from jqdata import" not in source


def test_small_cap_template_is_a_self_contained_source_snapshot():
    source = TEMPLATES["small_cap_limitup"]["source"]

    assert source == Path(small_cap_limitup.__file__).read_text(encoding="utf-8")
    assert "from jqdata import" not in source


def test_performance_small_cap_template_is_a_self_contained_source_snapshot():
    source = TEMPLATES["performance_small_cap"]["source"]

    assert source == Path(performance_small_cap.__file__).read_text(encoding="utf-8")
    assert "from jqdata import" not in source


def test_native_extra_history_is_lazy_and_never_exposes_current_day():
    source = '''
def initialize(context):
    context.set_universe(["510300.SH"])
    context.require_extra_history("unit_net_value")

def on_bar(context, bars):
    context.state["nav"] = context.extra_history(
        "unit_net_value", "510300.SH", count=2,
    )
'''
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
    )
    engine.set_run_window(datetime(2025, 7, 1).date(), datetime(2025, 7, 24).date())
    calls = []

    def load(info, symbols, start, end):
        calls.append((info, symbols, start, end))
        engine.set_extra_history(info, {
            "510300.SH": {
                datetime(2025, 7, 23).date(): 0.7,
                datetime(2025, 7, 24).date(): 0.8,
            },
        })

    engine.set_extra_history_loader(load)
    result = engine.run([
        Bar("510300.SH", datetime(2025, 7, 24, 9, 30), 10, 10, 10, 10),
    ])

    assert calls == [(
        "unit_net_value",
        ["510300.SH"],
        datetime(2025, 7, 1).date(),
        datetime(2025, 7, 24).date(),
    )]
    assert engine.extra_history_requirements == {"unit_net_value"}
    assert result["state"]["nav"] == [{"date": "2025-07-23", "value": 0.7}]


def test_five_fortunes_template_uses_reference_backtest_parameters():
    config = TEMPLATES["five_fortunes"]["config"]

    assert config == {
        "timeframe": "1m",
        "asset_type": "etf",
        "initial_capital": 100_000,
        "fees_pct": 0.0001,
        "commission_pct": 0.0001,
        "min_commission": 5,
        "stamp_tax_pct": 0,
        "slippage_bps": 0.5,
        "price_tick": 0.001,
        "benchmark_symbol": "510300.SH",
        "settlement": "t1",
        "fill_policy": "close",
    }


def test_seven_stars_template_uses_reference_backtest_parameters():
    assert TEMPLATES["seven_stars"]["config"] == {
        "timeframe": "1m",
        "asset_type": "etf",
        "initial_capital": 100_000,
        "fees_pct": 0.0002,
        "commission_pct": 0.0002,
        "min_commission": 5,
        "reserve_buy_fees": False,
        "stamp_tax_pct": 0,
        "slippage_bps": 0.5,
        "price_tick": 0.001,
        "benchmark_symbol": "510300.SH",
        "settlement": "t1",
        "t0_symbols": seven_stars.T0_ETFS,
        "allow_stale_fills": True,
        "fill_policy": "close",
    }


def test_small_cap_template_uses_reference_capital_for_paper_continuation():
    assert TEMPLATES["small_cap_limitup"]["config"] == {
        "timeframe": "1m",
        "asset_type": "stock",
        "initial_capital": 130_000,
        "paper_initial_capital": 130_000,
        "fees_pct": 0.0001,
        "commission_pct": 0.0001,
        "sell_commission_pct": 0.0001,
        "min_commission": 1,
        "stamp_tax_pct": 0.0005,
        "slippage_bps": 10,
        "price_tick": 0.01,
        "benchmark_symbol": "399101.SZ",
        "settlement": "t1",
        "fill_policy": "close",
    }


def test_performance_small_cap_template_uses_reference_backtest_parameters():
    assert TEMPLATES["performance_small_cap"]["config"] == {
        "timeframe": "1m",
        "asset_type": "stock",
        "initial_capital": 100_000,
        "fees_pct": 0.0001,
        "commission_pct": 0.0001,
        "min_commission": 5,
        "stamp_tax_pct": 0.001,
        "slippage_bps": 0,
        "price_tick": 0.01,
        "benchmark_symbol": "399303.SZ",
        "settlement": "t1",
        "fill_policy": "close",
    }


def test_performance_small_cap_template_runs_as_scheduled_strategy():
    engine = FreeStrategyEngine(
        TEMPLATES["performance_small_cap"]["source"],
        timeframe="1m",
        instruments=[{
            "symbol": "000001.SZ",
            "asset_type": "stock",
            "has_minute": True,
        }],
    )

    assert engine.execution_mode == "scheduled"
    assert engine.scheduled_times == ["09:00", "09:30", "14:00"]
    assert engine.market_history_requirements == {("index", "1d"): 235}


def test_financial_snapshot_normalizes_symbols_and_uses_previous_day_cutoff():
    source = """
def initialize(context):
    context.set_universe(["600000.XSHG"])

def on_bar(context, bars):
    context.state["snapshot"] = context.financial_snapshot(["600000.XSHG"])
"""
    engine = FreeStrategyEngine(source)
    calls = []

    def load(symbols, cutoff):
        calls.append((symbols, cutoff))
        return {symbols[0]: {"revenue": 100}}

    engine.set_financial_snapshot_loader(load)
    result = engine.run([
        Bar("600000.SH", datetime(2024, 5, 6, 9, 30), 10, 10, 10, 10),
    ])

    assert calls == [(["600000.SH"], datetime(2024, 5, 5).date())]
    assert result["state"]["snapshot"] == {"600000.SH": {"revenue": 100}}


def test_five_fortunes_template_matches_reference_slippage_fills():
    source = """
def on_bar(context, bars):
    if context.now.day == 10:
        context.buy('511880.SH', quantity=1500)
    else:
        context.sell('511880.SH', quantity=1500)
"""
    template = TEMPLATES["five_fortunes"]["config"]
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=1_000_000,
            fees_pct=0,
            stamp_tax_pct=0,
            slippage_bps=template["slippage_bps"],
            price_tick=template["price_tick"],
            fill_policy="close",
        ),
    ).run([
        Bar("511880.SH", datetime(2025, 10, 10, 13, 11), 100.964, 100.964, 100.964, 100.964),
        Bar("511880.SH", datetime(2025, 10, 13, 13, 10), 100.967, 100.967, 100.967, 100.967),
    ])

    assert [fill["price"] for fill in result["fills"]] == pytest.approx([100.969, 100.962])


def test_stock_sell_fee_uses_decimal_trade_value():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=1500)
    else:
        context.sell('X', quantity=1500)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=100_000,
            commission_pct=0.0001,
            sell_commission_pct=0.0001,
            min_commission=1,
            stamp_tax_pct=0.0005,
            slippage_bps=0,
            price_tick=0.01,
            lot_size=100,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, day, 15), 18.65, 18.65, 18.65, 18.65)
        for day in (1, 2)
    ])

    assert result["fills"][1]["value"] == 27_975
    assert result["fills"][1]["fee"] == 16.785


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


def test_reopened_position_uses_current_entry_order():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
        context.buy('Y', quantity=100)
    elif context.now.day == 2:
        context.sell('X', quantity=100)
    elif context.now.day == 3:
        context.buy('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=10_000,
            lot_size=100,
            fees_pct=0,
            slippage_bps=0,
            fill_policy="close",
        ),
    ).run([
        Bar(symbol, datetime(2024, 1, day, 15), 10, 10, 10, 10)
        for day in (1, 2, 3)
        for symbol in ("X", "Y")
    ])

    assert list(result["positions"]) == ["Y", "X"]
    assert result["positions"] == {"Y": 100.0, "X": 100.0}


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


def test_stock_share_distribution_adjusts_quantity_and_average_cost():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=1500)
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=100_000, asset_type="stock", fill_policy="close",
            fees_pct=0, slippage_bps=0,
        ),
    )
    engine.run([
        Bar("X", datetime(2024, 1, 1, 15), 20, 20, 20, 20, raw_close=20),
    ], finalize_session=False)

    result = engine.run([
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 16, 16, 16, 16,
            raw_close=16, split_ratio=1.2,
        ),
    ])

    assert result["positions"] == {"X": 1800.0}
    assert result["checkpoint"]["account"]["avg_cost"]["X"] == pytest.approx(20 / 1.2)


def test_stock_cash_dividend_increases_cash_and_reduces_average_cost_once():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=10_000, asset_type="stock", fill_policy="close",
            fees_pct=0, min_commission=0, stamp_tax_pct=0, slippage_bps=0,
        ),
    )
    engine.run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10),
    ], finalize_session=False)

    dividend_bar = Bar(
        "X", datetime(2024, 1, 2, 9, 30), 9.8, 9.8, 9.8, 9.8,
        raw_close=9.8, cash_dividend=0.2,
    )
    engine.run([dividend_bar], finalize_session=False, return_result=False)
    result = engine.run([
        Bar(
            "X", datetime(2024, 1, 2, 10, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
    ])

    assert result["positions"] == {"X": 100.0}
    assert result["checkpoint"]["account"]["cash"] == pytest.approx(9_020)
    assert result["checkpoint"]["account"]["avg_cost"]["X"] == pytest.approx(9.8)
    assert result["corporate_actions"] == [{
        "timestamp": "2024-01-02T09:30:00",
        "symbol": "X",
        "type": "cash_dividend",
        "cash_per_share": 0.2,
        "cash_received": 20.0,
    }]


def test_stock_cash_dividend_does_not_repeat_after_checkpoint_restore():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
"""
    config = FreeStrategyConfig(
        initial_capital=10_000, asset_type="stock", fill_policy="close",
        fees_pct=0, min_commission=0, stamp_tax_pct=0, slippage_bps=0,
    )
    initial = FreeStrategyEngine(source, timeframe="1m", config=config)
    initial.run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10),
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
    ], finalize_session=False, return_result=False)
    checkpoint = initial.checkpoint()

    resumed = FreeStrategyEngine(source, timeframe="1m", config=config)
    resumed.restore_checkpoint(checkpoint)
    result = resumed.run([
        Bar(
            "X", datetime(2024, 1, 2, 10, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
    ])

    assert result["checkpoint"]["account"]["cash"] == pytest.approx(9_020)
    assert len(result["corporate_actions"]) == 1


def test_stock_sale_withholds_short_term_dividend_tax_outside_fill_fee():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    elif context.now.day == 20:
        context.sell('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=10_000, asset_type="stock", fill_policy="close",
            fees_pct=0, min_commission=0, stamp_tax_pct=0, slippage_bps=0,
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10),
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
        Bar("X", datetime(2024, 1, 20, 15), 9.8, 9.8, 9.8, 9.8, raw_close=9.8),
    ])

    assert result["fills"][-1]["fee"] == 0
    assert result["checkpoint"]["account"]["cash"] == pytest.approx(9_996)
    assert result["corporate_actions"][-1] == {
        "timestamp": "2024-01-20T15:00:00",
        "symbol": "X",
        "type": "dividend_tax",
        "tax_withheld": 4.0,
    }


def test_stock_dividend_tax_lot_survives_checkpoint_restore():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    elif context.now.day == 20:
        context.sell('X', quantity=100)
"""
    config = FreeStrategyConfig(
        initial_capital=10_000, asset_type="stock", fill_policy="close",
        fees_pct=0, min_commission=0, stamp_tax_pct=0, slippage_bps=0,
    )
    initial = FreeStrategyEngine(source, timeframe="1m", config=config)
    initial.run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10),
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
    ], finalize_session=False, return_result=False)

    resumed = FreeStrategyEngine(source, timeframe="1m", config=config)
    resumed.restore_checkpoint(initial.checkpoint())
    result = resumed.run([
        Bar("X", datetime(2024, 1, 20, 15), 9.8, 9.8, 9.8, 9.8, raw_close=9.8),
    ])

    assert result["checkpoint"]["account"]["cash"] == pytest.approx(9_996)
    assert result["corporate_actions"][-1]["tax_withheld"] == 4.0


def test_stock_sale_after_one_year_does_not_withhold_dividend_tax():
    source = """
def on_bar(context, bars):
    if context.now.year == 2024 and context.now.day == 1:
        context.buy('X', quantity=100)
    elif context.now.year == 2025:
        context.sell('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=10_000, asset_type="stock", fill_policy="close",
            fees_pct=0, min_commission=0, stamp_tax_pct=0, slippage_bps=0,
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10, raw_close=10),
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 9.8, 9.8, 9.8, 9.8,
            raw_close=9.8, cash_dividend=0.2,
        ),
        Bar("X", datetime(2025, 1, 2, 15), 9.8, 9.8, 9.8, 9.8, raw_close=9.8),
    ])

    assert result["checkpoint"]["account"]["cash"] == pytest.approx(10_000)
    assert [action["type"] for action in result["corporate_actions"]] == ["cash_dividend"]


def test_stock_cash_dividend_and_share_distribution_apply_together():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=1500)
"""
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=100_000, asset_type="stock", fill_policy="close",
            fees_pct=0, slippage_bps=0,
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 20, 20, 20, 20, raw_close=20),
        Bar(
            "X", datetime(2024, 1, 2, 9, 30), 16, 16, 16, 16,
            raw_close=16, split_ratio=1.2, cash_dividend=0.3,
        ),
    ])

    assert result["positions"] == {"X": 1800.0}
    assert result["checkpoint"]["account"]["cash"] == pytest.approx(70_450)
    assert result["checkpoint"]["account"]["avg_cost"]["X"] == pytest.approx((20 - 0.3) / 1.2)


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


def test_buy_quantity_reserves_commission_before_filling():
    source = """
def on_bar(context, bars):
    context.buy('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=10_000,
            lot_size=1,
            fees_pct=0.0002,
            slippage_bps=0,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 2, 15), 100, 100, 100, 100),
    ])

    assert result["fills"][0]["quantity"] == 99
    assert result["checkpoint"]["account"]["cash"] >= 0


def test_slippage_fill_price_does_not_cross_price_limit():
    source = """
def on_bar(context, bars):
    context.buy('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=100_000,
            fees_pct=0,
            slippage_bps=50,
            fill_policy="close",
        ),
    ).run([
        Bar(
            "X", datetime(2024, 1, 2, 15), 9.99, 9.99, 9.99, 9.99,
            raw_close=9.99, limit_up=10,
        ),
    ])

    assert result["fills"][0]["price"] == 10


def test_sell_slippage_fill_price_does_not_cross_price_limit():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    else:
        context.sell('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=100_000,
            fees_pct=0,
            stamp_tax_pct=0,
            slippage_bps=50,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar(
            "X", datetime(2024, 1, 2, 15), 9.01, 9.01, 9.01, 9.01,
            raw_close=9.01, limit_down=9,
        ),
    ])

    assert result["fills"][-1]["price"] == 9


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


def test_symbol_level_t0_can_coexist_with_t1():
    source = """
def on_bar(context, bars):
    context.buy('T0', quantity=100)
    context.buy('T1', quantity=100)

def after_trading_end(context):
    context.sell('T0', quantity=100)
    context.sell('T1', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            lot_size=100,
            settlement="t1",
            t0_symbols=["T0"],
            fill_policy="close",
        ),
    ).run([
        Bar("T0", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("T1", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
    ])

    assert [(fill["symbol"], fill["side"]) for fill in result["fills"]] == [
        ("T0", "buy"),
        ("T1", "buy"),
        ("T0", "sell"),
    ]


def test_scheduled_callback_sees_new_t1_position_as_unavailable():
    source = """
def initialize(context):
    context.schedule(buy, '10:30')
    context.schedule(check_available, '14:20')

def buy(context):
    context.order_target('X', 100)

def check_available(context):
    context.state['available'] = context.portfolio.available_positions.get('X')
"""
    result = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            initial_capital=10_000,
            lot_size=100,
            fees_pct=0,
            slippage_bps=0,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 2, 10, 30), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 14, 20), 10, 10, 10, 10),
    ])

    assert result["state"]["available"] == 0


def test_buy_fee_reservation_can_match_post_fill_commission_brokers():
    source = """
def on_bar(context, bars):
    context.buy('X', quantity=100)
"""
    bars = [Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10)]
    reserved = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_000,
            commission_pct=0.01,
            reserve_buy_fees=True,
            fill_policy="close",
            slippage_bps=0,
        ),
    ).run(bars)
    post_fill = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_000,
            commission_pct=0.01,
            reserve_buy_fees=False,
            fill_policy="close",
            slippage_bps=0,
        ),
    ).run(bars)

    assert reserved["fills"] == []
    assert post_fill["fills"][0]["quantity"] == 100
    assert post_fill["checkpoint"]["account"]["cash"] == pytest.approx(-10)


def test_target_buys_in_one_callback_share_the_pre_fill_cash_snapshot():
    source = """
def on_bar(context, bars):
    context.order_target('X', 100)
    context.order_target('Y', 100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=1_500,
            commission_pct=0,
            min_commission=0,
            slippage_bps=0,
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("Y", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
    ])

    assert [(fill["symbol"], fill["quantity"]) for fill in result["fills"]] == [
        ("X", 100),
        ("Y", 100),
    ]
    assert result["checkpoint"]["account"]["cash"] == pytest.approx(-500)


def test_sell_commission_can_differ_from_buy_commission():
    source = """
def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
    else:
        context.sell('X', quantity=100)
"""
    result = FreeStrategyEngine(
        source,
        config=FreeStrategyConfig(
            initial_capital=2_000,
            commission_pct=0.001,
            sell_commission_pct=0.002,
            min_commission=0,
            stamp_tax_pct=0,
            slippage_bps=0,
            settlement="t1",
            fill_policy="close",
        ),
    ).run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("X", datetime(2024, 1, 2, 15), 10, 10, 10, 10),
    ])

    assert [fill["fee"] for fill in result["fills"]] == pytest.approx([1, 2])


def test_stale_fill_requires_current_day_trading_evidence():
    source = """
def initialize(context):
    context.schedule(sell_x, '09:45')

def sell_x(context):
    context.sell('X', quantity=100)

def on_bar(context, bars):
    if context.now.day == 1:
        context.buy('X', quantity=100)
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            settlement="t0",
            allow_stale_fills=True,
            fill_policy="close",
            fees_pct=0,
            slippage_bps=0,
        ),
    )
    engine.preload_market_history([
        Bar("X", datetime(2024, 1, 2, 15), 11, 12, 10, 11),
    ])
    engine.preload_tradable_dates([("X", datetime(2024, 1, 2).date())])

    result = engine.run([
        Bar("X", datetime(2024, 1, 1, 15), 10, 10, 10, 10),
        Bar("Y", datetime(2024, 1, 2, 9, 45), 1, 1, 1, 1),
    ])

    assert [(fill["symbol"], fill["side"], fill["timestamp"]) for fill in result["fills"]] == [
        ("X", "buy", "2024-01-01T15:00:00"),
        ("X", "sell", "2024-01-02T09:45:00"),
    ]


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


def test_schedule_only_strategy_detects_mode_and_runs_each_callback_once_per_day():
    source = """
def initialize(context):
    context.state['events'] = []
    context.schedule(lambda ctx: ctx.state['events'].append(ctx.now.isoformat()), '13:10')
"""
    engine = FreeStrategyEngine(source, timeframe="1m")

    assert engine.execution_mode == "scheduled"
    assert engine.scheduled_times == ["13:10"]

    for day in (2, 3):
        timestamp = datetime(2024, 1, day, 13, 10)
        bar = Bar("X", timestamp, 10, 10, 10, 10)
        engine.run_scheduled_event(timestamp, [bar])
        engine.run_scheduled_event(timestamp, [bar])
        engine.finish_session()

    assert engine.context.state["events"] == [
        "2024-01-02T13:10:00",
        "2024-01-03T13:10:00",
    ]
    assert engine.callbacks_executed == 2
    assert engine.market_rows_consumed == 4


def test_execution_mode_validation_and_strict_schedule_time():
    with pytest.raises(ValueError, match="on_bar.*context.schedule"):
        FreeStrategyEngine("def initialize(context):\n    pass\n")

    with pytest.raises(ValueError, match="HH:MM"):
        FreeStrategyEngine("""
def initialize(context):
    context.schedule(lambda ctx: None, '9:30')
""")

    with pytest.raises(ValueError, match="HH:MM"):
        FreeStrategyEngine("""
from datetime import time

def initialize(context):
    context.schedule(lambda ctx: None, time(9, 30, 1))
""")

    deduplicated = FreeStrategyEngine("""
def task(context):
    pass

def initialize(context):
    context.schedule(task, '09:30')
    context.schedule(task, '09:30')
""")
    assert deduplicated.scheduled_times == ["09:30"]
    assert len(deduplicated.context._scheduled) == 1

    mixed = FreeStrategyEngine("""
def initialize(context):
    context.schedule(lambda ctx: None, '13:10')

def on_bar(context, bars):
    pass
""")
    assert mixed.execution_mode == "full_bar"
    assert mixed.scheduled_times == ["13:10"]


def test_scheduled_history_is_loaded_at_logical_time_without_future_bars():
    source = """
def initialize(context):
    context.schedule(run, '13:10')

def run(context):
    context.state['history'] = [bar.close for bar in context.history_bars('X', 10)]
"""
    engine = FreeStrategyEngine(source, timeframe="1m")
    calls = []

    def load_history(symbol, count, timeframe, cutoff):
        calls.append((symbol, count, timeframe, cutoff))
        return [
            Bar("X", datetime(2024, 1, 2, 13, 9), 9, 9, 9, 9),
            Bar("X", datetime(2024, 1, 2, 13, 11), 11, 11, 11, 11),
        ]

    engine.set_history_loader(load_history)
    engine.run_scheduled_event(
        datetime(2024, 1, 2, 13, 10),
        [Bar("X", datetime(2024, 1, 2, 13, 10), 10, 10, 10, 10)],
    )

    assert engine.context.state["history"] == [9]
    assert calls == [("X", 10, "1m", datetime(2024, 1, 2, 13, 10))]


def test_scheduled_close_and_next_open_use_event_market_without_on_bar():
    source = """
def initialize(context):
    context.schedule(run, '13:10')

def run(context):
    context.buy('X', quantity=100)
"""
    timestamp = datetime(2024, 1, 2, 13, 10)
    snapshot = Bar("X", timestamp, 10, 10, 10, 10)

    close_engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(fill_policy="close", slippage_bps=0),
    )
    close_engine.run_scheduled_event(timestamp, [snapshot])
    assert close_engine.account.fills[0].timestamp == timestamp.isoformat()
    assert close_engine.account.fills[0].price == 10

    next_engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(fill_policy="next_open", slippage_bps=0),
    )
    next_engine.run_scheduled_event(timestamp, [snapshot])
    assert next_engine.account.fills == []
    next_engine.process_fill_event(
        datetime(2024, 1, 2, 13, 11),
        [Bar("X", datetime(2024, 1, 2, 13, 11), 11, 11, 11, 11)],
    )
    assert next_engine.account.fills[0].timestamp == "2024-01-02T13:11:00"
    assert next_engine.account.fills[0].price == 11

    for blocked in (
        Bar("X", timestamp, 11, 11, 11, 11, limit_up=11),
        Bar("X", timestamp, 10, 10, 10, 10, tradable=False, suspended=True),
    ):
        blocked_engine = FreeStrategyEngine(
            source,
            timeframe="1m",
            config=FreeStrategyConfig(fill_policy="close", slippage_bps=0),
        )
        blocked_engine.run_scheduled_event(timestamp, [blocked])
        assert blocked_engine.account.fills == []
        assert blocked_engine.account.orders[0].status == "rejected"


def test_scheduled_checkpoint_restores_completed_callbacks():
    source = """
def initialize(context):
    context.state.setdefault('runs', 0)
    context.schedule(run, '13:10')

def run(context):
    context.state['runs'] += 1
"""
    timestamp = datetime(2024, 1, 2, 13, 10)
    bar = Bar("X", timestamp, 10, 10, 10, 10)
    initial = FreeStrategyEngine(source, timeframe="1m")
    initial.run_scheduled_event(timestamp, [bar])

    resumed = FreeStrategyEngine(source, timeframe="1m")
    resumed.restore_checkpoint(initial.checkpoint())
    resumed.run_scheduled_event(timestamp, [bar])

    assert resumed.context.state["runs"] == 1
    assert resumed.callbacks_executed == 1


def test_scheduled_checkpoint_restores_context_time_for_dynamic_scope():
    source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(run, '14:20', symbols=scope)

def scope(context, timestamp):
    return ['X'] if context.now.date() == timestamp.date() else []

def run(context):
    pass
"""
    timestamp = datetime(2026, 7, 30, 10, 30)
    initial = FreeStrategyEngine(source)
    initial.update_scheduled_market(timestamp, [])
    resumed = FreeStrategyEngine(source)

    resumed.restore_checkpoint(initial.checkpoint())

    assert resumed.context.now == timestamp
    assert resumed.scheduled_snapshot_symbols(datetime(2026, 7, 30, 14, 20)) == ["X"]


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
    symbols = [*REGIME_PROXIES, "510300.SH", "518880.SH", DEFENSIVE_ETF]
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
    assert reports[-1]["nav_filter"] == "unit_net_value"
    assert reports[-1]["holdings"] == [
        symbol for symbol, quantity in result["positions"].items() if quantity > 0
    ]
    assert result["daily_equity_curve"]
    assert result["performance"]["benchmark_return_pct"] > 0


def test_five_fortunes_uses_daily_warmup_on_first_backtest_day():
    source = """
from app.free_strategy.five_fortunes import after_trading_end, before_trading_start, initialize, on_bar
"""
    symbols = [*REGIME_FALLBACK_PROXIES, "518880.SH", DEFENSIVE_ETF]
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
    regime_warmup = []
    for offset in range(65):
        timestamp = start + timedelta(days=offset)
        for index, symbol in enumerate(REGIME_PROXIES):
            price = (1 + index * 0.01) * (1.002 ** offset)
            regime_warmup.append(
                Bar(symbol, timestamp, price, price, price, price, 100, price * 100)
            )
    engine.preload_market_history(regime_warmup, "1d")
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
