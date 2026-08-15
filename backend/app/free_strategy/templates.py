"""开箱即用的自由策略模板。"""
from __future__ import annotations

from pathlib import Path

from .seven_stars import T0_ETFS


LEGACY_FIVE_FORTUNES_SOURCE = '''from app.free_strategy.five_fortunes import (
    DEFENSIVE_ETF,
    WUFU_MINUTE_POOL,
    after_trading_end,
    before_trading_start,
    initialize as initialize_five_fortunes,
    on_bar,
)

ETF_POOL = [*WUFU_MINUTE_POOL, DEFENSIVE_ETF]

def initialize(context):
    context.set_universe(ETF_POOL)
    initialize_five_fortunes(context)
'''

LEGACY_FIVE_FORTUNES_SHA256 = "622c46f2ffcb0919de6cf7e986caa9f95da6cabe3e985721829a5f3d9d2a0022"
MANAGED_FIVE_FORTUNES_SHA256 = frozenset({
    LEGACY_FIVE_FORTUNES_SHA256,
    "6804de0afc67510410d8ae7c149d3278fb3890cbe606148788e5b6f7fb4c27fa",
    "a281121c543f923f7bccbe4869821ee114f8cfc4f3093631fc6a4af22a946d38",
    "ea7370773f12ed6e19fc707c0065271557aa9c64a4f9db208206838a3894f99a",
    "098dd281478b43527ecf706f2f140a19b21f4e7b9e2c6744d8120fcf64279380",
})
MANAGED_ETF_NAV_ALIGNMENT_SHA256 = {
    "five_fortunes": frozenset({
        "df96a2526a8ae27a464893add4ee22f5134fdac04f0e7c69f9a2e8d7864a48cb",
    }),
    "five_fortunes_v2": frozenset({
        "30cb1562438fe75b1e9ec214d748cbdfb88807a78f6c2c6650c2177e67dbfec5",
    }),
    "seven_stars": frozenset({
        "128072cf2734eedd7f86214f8369f26769e80e3c879dfa8b16899a43cc34fe96",
    }),
}
MANAGED_LARGE_AMOUNT_FIRST_BOARD_SHA256 = frozenset({
    "04a7fe514b041e99f76943d96e5e25e8413205fb212ed02d011f42a8cd22e577",
    "7f68c888943f0349362e4551d13fa7cf348d59b40bbfb50e7531f49a4c3230b2",
})
FIVE_FORTUNES_SOURCE = Path(__file__).with_name("five_fortunes.py").read_text(encoding="utf-8")
FIVE_FORTUNES_V2_SOURCE = Path(__file__).with_name("five_fortunes_v2.py").read_text(encoding="utf-8")
SEVEN_STARS_SOURCE = Path(__file__).with_name("seven_stars.py").read_text(encoding="utf-8")
SMALL_CAP_LIMITUP_SOURCE = Path(__file__).with_name("small_cap_limitup.py").read_text(encoding="utf-8")
PERFORMANCE_SMALL_CAP_SOURCE = Path(__file__).with_name("performance_small_cap.py").read_text(encoding="utf-8")
MAINLINE_MOMENTUM_SOURCE = Path(__file__).with_name("mainline_momentum.py").read_text(encoding="utf-8")
LARGE_AMOUNT_FIRST_BOARD_SOURCE = Path(__file__).with_name("large_amount_first_board.py").read_text(encoding="utf-8")


def _mainline_momentum_template(model: str, name: str) -> dict:
    return {
        "name": name,
        "config": {
            "timeframe": "1m",
            "asset_type": "stock",
            "initial_capital": 100_000,
            "fees_pct": 0.0002,
            "commission_pct": 0.0002,
            "sell_commission_pct": 0.0002,
            "min_commission": 5,
            "stamp_tax_pct": 0.0005,
            "transfer_fee_pct": 0.00001,
            "slippage_bps": 10,
            "price_tick": 0.01,
            "lot_size": 100,
            "max_exposure_pct": 0.9,
            "benchmark_symbol": "000905.SH",
            "settlement": "t1",
            "fill_policy": "next_open",
        },
        "source": f'ENTRY_MODEL = "{model}"\n{MAINLINE_MOMENTUM_SOURCE}',
    }


TEMPLATES = {
    "dual_ma": {
        "name": "双均线模板",
        "source": '''ETF_POOL = ["510300.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
    context.state.setdefault("closes", {})
    context.log("双均线策略初始化")

def on_bar(context, bars):
    for symbol, bar in bars.items():
        closes = context.state["closes"].setdefault(symbol, [])
        closes.append(bar.close)
        if len(closes) < 20:
            continue
        fast = sum(closes[-5:]) / 5
        slow = sum(closes[-20:]) / 20
        context.order_target_percent(symbol, 0.95 if fast > slow else 0.0)

def after_trading_end(context):
    context.log("双均线日终完成")
''',
    },
    "etf_rotation": {
        "name": "状态化 ETF 轮动模板",
        "source": '''ETF_POOL = ["510300.SH", "510500.SH", "159915.SZ", "518880.SH", "511880.SH"]

def initialize(context):
    context.set_universe(ETF_POOL)
    context.state.setdefault("history", {})
    context.state.setdefault("cooldown", 0)
    context.state.setdefault("regime", "neutral")
    context.log("ETF 轮动初始化：默认 T+1，可在账户设置切换 T+0")

def on_bar(context, bars):
    ranked = []
    for symbol, bar in bars.items():
        values = context.state["history"].setdefault(symbol, [])
        values.append(bar.close)
        if len(values) >= 21:
            momentum = values[-1] / values[-21] - 1
            ranked.append((momentum, symbol))
    if not ranked:
        return
    ranked.sort(reverse=True)
    winner = ranked[0][1]
    for _, symbol in ranked:
        context.order_target_percent(symbol, 0.95 if symbol == winner else 0.0)
    context.log(f"候选 {winner}，相关性/NAV 数据缺失时跳过对应过滤")
''',
    },
    "mainline_momentum_breakout": _mainline_momentum_template(
        "breakout", "主线动量·突破买点",
    ),
    "mainline_momentum_pullback": _mainline_momentum_template(
        "pullback", "主线动量·VWAP回踩",
    ),
    "mainline_momentum_resonance": _mainline_momentum_template(
        "resonance", "主线动量·行业共振",
    ),
    "mainline_momentum_combined": _mainline_momentum_template(
        "combined", "主线动量·组合门禁",
    ),
    "large_amount_first_board": {
        "name": "大成交首板·上午打板",
        "config": {
            "timeframe": "1m",
            "asset_type": "stock",
            "initial_capital": 100_000,
            "fees_pct": 0.0002,
            "commission_pct": 0.0002,
            "sell_commission_pct": 0.0002,
            "min_commission": 5,
            "stamp_tax_pct": 0.0005,
            "transfer_fee_pct": 0.00001,
            "slippage_bps": 0,
            "price_tick": 0.01,
            "lot_size": 100,
            "max_exposure_pct": 0.9,
            "benchmark_symbol": "000905.SH",
            "settlement": "t1",
            "fill_policy": "close",
            "limit_up_touch_fill": True,
        },
        "source": LARGE_AMOUNT_FIRST_BOARD_SOURCE,
    },
    "five_fortunes": {
        "name": "五福策略",
        "config": {
            "timeframe": "1m",
            "asset_type": "etf",
            "initial_capital": 100_000,
            "fees_pct": 0.0001,
            "commission_pct": 0.0001,
            "min_commission": 5,
            "stamp_tax_pct": 0,
            # PriceRelatedSlippage(0.0001) is a full spread; each side pays half.
            "slippage_bps": 0.5,
            "price_tick": 0.001,
            "benchmark_symbol": "510300.SH",
            "settlement": "t1",
            "fill_policy": "close",
        },
        "source": FIVE_FORTUNES_SOURCE,
    },
    "five_fortunes_v2": {
        "name": "五福2.0",
        "config": {
            "timeframe": "1m",
            "asset_type": "etf",
            "initial_capital": 100_000,
            "paper_initial_capital": 100_000,
            "fees_pct": 0.0001,
            "commission_pct": 0.0001,
            "min_commission": 5,
            "stamp_tax_pct": 0,
            # PriceRelatedSlippage(0.0001) is a full spread; each side pays half.
            "slippage_bps": 0.5,
            "price_tick": 0.001,
            "benchmark_symbol": "510300.SH",
            "settlement": "t1",
            "fill_policy": "close",
        },
        "source": FIVE_FORTUNES_V2_SOURCE,
    },
    "seven_stars": {
        "name": "七星策略",
        "config": {
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
            "t0_symbols": T0_ETFS,
            "allow_stale_fills": True,
            "fill_policy": "close",
        },
        "source": SEVEN_STARS_SOURCE,
    },
    "small_cap_limitup": {
        "name": "涨停基因小市值",
        "config": {
            "timeframe": "1m",
            "asset_type": "stock",
            "initial_capital": 130_000,
            "paper_initial_capital": 130_000,
            "fees_pct": 0.0001,
            "commission_pct": 0.0001,
            "sell_commission_pct": 0.0001,
            "min_commission": 1,
            "stamp_tax_pct": 0.0005,
            # PriceRelatedSlippage(0.002) is a full spread; each side pays half.
            "slippage_bps": 10,
            "price_tick": 0.01,
            "benchmark_symbol": "399101.SZ",
            "settlement": "t1",
            "fill_policy": "close",
        },
        "source": SMALL_CAP_LIMITUP_SOURCE,
    },
    "performance_small_cap": {
        "name": "绩优小市值",
        "config": {
            "timeframe": "1m",
            "asset_type": "stock",
            "initial_capital": 100_000,
            "fees_pct": 0.0001,
            "commission_pct": 0.0001,
            "min_commission": 5,
            "reserve_buy_fees": False,
            "stamp_tax_pct": 0.001,
            "slippage_bps": 0,
            "price_tick": 0.01,
            # Full-market one-year dividend screening exceeds the generic script limit.
            "callback_timeout_seconds": 120,
            "benchmark_symbol": "399303.SZ",
            "settlement": "t1",
            "fill_policy": "close",
        },
        "source": PERFORMANCE_SMALL_CAP_SOURCE,
    },
}
