from datetime import date, datetime

import pytest

from app.free_strategy.bars import Bar as _Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.jq_compat.capabilities import JoinQuantCapabilityError, analyze_source


def Bar(*args, **kwargs):  # noqa: N802, ANN002, ANN003, ANN201
    if len(args) < 7:
        kwargs.setdefault("volume", 100.0)
    return _Bar(*args, **kwargs)


def joinquant_engine(source: str, *, instruments=None, **config):
    return FreeStrategyEngine(
        source,
        timeframe="1m",
        dialect="joinquant",
        instruments=instruments,
        config=FreeStrategyConfig(
            initial_capital=1_000,
            asset_type="stock",
            lot_size=1,
            fees_pct=0,
            stamp_tax_pct=0,
            slippage_bps=0,
            fill_policy="close",
            settlement="t0",
            **config,
        ),
    )


def test_joinquant_handle_data_uses_existing_matching_engine():
    source = """
from jqdata import *

def initialize(context):
    set_universe(['000001.XSHE'])
    g.seen = []
    set_order_cost(OrderCost(open_commission=0, close_commission=0, close_tax=0, min_commission=0))
    set_slippage(FixedSlippage(0))

def handle_data(context, data):
    g.seen.append([context.current_dt.strftime('%H:%M'), data['000001.XSHE'].close])
    if not context.portfolio.positions:
        order_target_value('000001.XSHE', 100)
"""

    result = joinquant_engine(source).run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
    ])

    assert result["dialect"] == "joinquant"
    assert result["compatibility_report"]["summary_status"] == "supported"
    assert result["positions"] == {"000001.SZ": 10}
    assert result["fills"][0]["price"] == 10
    assert result["state"]["__joinquant_g__"]["seen"] == [["09:31", 10.0]]


def test_joinquant_lifecycle_schedule_degradation_and_checkpoint_state():
    source = """
from datetime import date
from jqdata import *

def process_initialize(context):
    g.events = ['process']

def initialize(context):
    g.events.append('initialize')
    g.anchor = date(2024, 1, 2)
    set_universe(['000001.XSHE'])
    run_daily(mark, time='09:30:16')

def before_trading_start(context):
    g.events.append('before:' + context.current_dt.strftime('%H:%M'))

def mark(context):
    g.events.append('scheduled:' + context.current_dt.strftime('%H:%M'))

def handle_data(context, data):
    g.events.append('bar:' + context.current_dt.strftime('%H:%M'))

def after_trading_end(context):
    g.events.append('after:' + context.current_dt.strftime('%H:%M'))
"""

    engine = joinquant_engine(source)
    assert engine.scheduled_times == ["09:31"]
    result = engine.run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
    ])

    assert result["compatibility_report"]["summary_status"] == "degraded"
    assert result["state"]["__joinquant_g__"]["events"] == [
        "process", "initialize", "before:09:29", "bar:09:31", "scheduled:09:31", "after:09:31",
    ]
    checkpoint = result["checkpoint"]
    assert checkpoint["state"]["__joinquant_g__"]["anchor"] == {
        "__tickflow_joinquant_type__": "date", "value": "2024-01-02",
    }

    resumed = joinquant_engine(source)
    resumed.restore_checkpoint(checkpoint)

    assert resumed.context.state["__joinquant_g__"]["anchor"] == date(2024, 1, 2)


def test_joinquant_history_is_visible_at_current_bar_without_future_rows():
    source = """
from jqdata import *

def initialize(context):
    set_universe(['000001.XSHE'])
    g.prices = []

def handle_data(context, data):
    price_rows = get_price('000001.XSHE', count=10, frequency='1m', fields=['close'])
    attribute_rows = attribute_history('000001.XSHE', 10, unit='1m', fields=['close'])
    history_rows = history(10, unit='1m', field='close', security_list=['000001.XSHE'])
    bounded_rows = get_price(
        '000001.XSHE', count=10, frequency='1m', fields=['close'],
        end_date='2024-01-02 09:31:00',
    )
    g.prices.append([
        price_rows['close'].tolist(), history_rows['000001.XSHE'].tolist(),
        bounded_rows['close'].tolist(), attribute_rows['close'].tolist(),
    ])
"""

    result = joinquant_engine(source).run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 32), 11, 11, 11, 11),
    ])

    assert result["state"]["__joinquant_g__"]["prices"] == [
        [[10], [10], [10], [10]],
        [[10, 11], [10, 11], [10], [10, 11]],
    ]


def test_joinquant_current_data_and_security_catalog_use_jq_codes():
    source = """
from jqdata import *

def initialize(context):
    set_universe(['000001.XSHE'])

def handle_data(context, data):
    current = get_current_data()['000001.XSHE']
    info = get_security_info('000001.XSHE')
    catalog = get_all_securities(types='stock')
    g.snapshot = [
        current.code, current.last_price, current.paused, info.display_name,
        catalog.index.tolist(),
    ]
"""

    result = joinquant_engine(
        source,
        instruments=[{
            "symbol": "000001.SZ",
            "name": "平安银行",
            "asset_type": "stock",
        }],
    ).run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
    ])

    assert result["state"]["__joinquant_g__"]["snapshot"] == [
        "000001.XSHE", 10.0, False, "平安银行", ["000001.XSHE"],
    ]


def test_joinquant_order_api_variants_share_existing_order_and_fill_rules():
    source = """
from jqdata import *

def initialize(context):
    set_universe(['000001.XSHE'])
    g.step = 0

def handle_data(context, data):
    if g.step == 0:
        order('000001.XSHE', 10)
    elif g.step == 1:
        order_value('000001.XSHE', 100)
    elif g.step == 2:
        order_target('000001.XSHE', 30)
    elif g.step == 3:
        order_target_value('000001.XSHE', 200)
    else:
        order_target_percent('000001.XSHE', 0.1)
    g.step += 1
"""

    result = joinquant_engine(source).run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, minute), 10, 10, 10, 10)
        for minute in range(31, 36)
    ])

    assert len(result["orders"]) == 5
    assert len(result["fills"]) == 5
    assert result["positions"] == {"000001.SZ": 10}
    assert result["checkpoint"]["account"]["cash"] == 900


def test_joinquant_fixed_slippage_cannot_create_negative_sell_price():
    source = """
from jqdata import *

def initialize(context):
    set_universe(['000001.XSHE'])
    g.step = 0

def handle_data(context, data):
    if g.step == 0:
        set_slippage(FixedSlippage(0))
        order('000001.XSHE', 1)
    else:
        set_slippage(FixedSlippage(20))
        order('000001.XSHE', -1)
    g.step += 1
"""

    result = joinquant_engine(source).run([
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 31), 10, 10, 10, 10),
        Bar("000001.SZ", datetime(2024, 1, 2, 9, 32), 10, 10, 10, 10),
    ])

    assert len(result["fills"]) == 1
    assert result["orders"][-1]["status"] == "rejected"
    assert result["orders"][-1]["reason"] == "滑点后成交价格无效"


@pytest.mark.parametrize(
    ("source", "capability"),
    [
        ("from jqdata import *\n\ndef handle_data(context, data):\n    get_ticks('000001.XSHE')\n", "get_ticks"),
        ("import jqmt\n\ndef handle_data(context, data):\n    pass\n", "jqmt"),
        ("from jqdata import *\n\ndef handle_data(context, data):\n    get_factor_values('VOL5', ['000001.XSHE'])\n", "get_factor_values"),
        ("from jqdata import *\n\ndef handle_data(context, data):\n    get_valuation('000001.XSHE')\n", "get_valuation"),
        ("import jqdata as jq\n\ndef handle_data(context, data):\n    jq.get_ticks('000001.XSHE')\n", "get_ticks"),
        ("import jqlib.technical_analysis as ta\n\ndef handle_data(context, data):\n    pass\n", "jqlib.technical_analysis"),
    ],
)
def test_joinquant_unavailable_capabilities_fail_before_execution(source, capability):
    with pytest.raises(JoinQuantCapabilityError, match=capability):
        joinquant_engine(source)


def test_joinquant_capability_scan_does_not_treat_regular_attributes_as_jq_apis():
    report = analyze_source("""
import pandas as pd
from jqdata import *

def handle_data(context, data):
    pd.DataFrame({'value': [1]}).query('value > 0')
""")

    assert not any(item["name"] == "query" for item in report["apis"])
