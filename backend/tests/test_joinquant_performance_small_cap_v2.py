from __future__ import annotations

import ast
from pathlib import Path


STRATEGY_PATH = (
    Path(__file__).parents[2] / "docs" / "聚宽策略" / "绩优小市值2.0.py"
)


def test_joinquant_performance_small_cap_v2_is_standalone_and_parseable():
    source = STRATEGY_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(STRATEGY_PATH))
    functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}

    assert "initialize" in functions
    assert "monthly_adjustment" in functions
    assert "check_smallcap_timing" in functions
    assert "from jqdata import *" in source
    assert "from app." not in source
    assert "context.order_cash_weight" not in source
    compile(source, str(STRATEGY_PATH), "exec")


def test_joinquant_strategy_keeps_v2_core_parameters_in_source():
    source = STRATEGY_PATH.read_text(encoding="utf-8")

    assert "g.stock_num = 5" in source
    assert "g.max_stock_price = 6.0" in source
    assert "g.smallcap_index_size = 400" in source
    assert "g.smallcap_index_threshold = 18.72" in source
    assert "g.style_entry_quantile = 0.97" in source
    assert "g.style_recovery_quantile = 0.70" in source
    assert 'fields=["money"]' in source
    assert 'fields=["amount"]' not in source
