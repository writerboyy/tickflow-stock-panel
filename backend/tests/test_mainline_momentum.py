from datetime import date, datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyEngine
from app.free_strategy.entry_analysis import build_mainline_entry_analysis
from app.free_strategy.mainline_momentum import (
    _append_minute,
    _combined_model_hit,
    _model_hits,
)
from app.free_strategy.mainline_snapshot import MainlineSnapshotCache, _industry_key
from app.free_strategy.templates import TEMPLATES


@pytest.mark.parametrize(
    ("template_id", "model"),
    [
        ("mainline_momentum_breakout", "breakout"),
        ("mainline_momentum_pullback", "pullback"),
        ("mainline_momentum_resonance", "resonance"),
        ("mainline_momentum_combined", "combined"),
    ],
)
def test_mainline_templates_share_execution_contract(template_id, model):
    template = TEMPLATES[template_id]

    assert f'ENTRY_MODEL = "{model}"' in template["source"]
    assert template["config"] == {
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
    }


def test_sparse_minutes_create_one_closed_bucket_and_raw_vwap():
    state = {"minute_bucket": {}, "five_bars": {}, "session": {}}
    first = Bar(
        "X", datetime(2026, 8, 13, 9, 31), 5, 5.5, 4.5, 5,
        volume=100, amount=100_000, raw_open=10, raw_high=11, raw_low=9, raw_close=10,
    )
    last = Bar(
        "X", datetime(2026, 8, 13, 9, 35), 6, 6.5, 5.5, 6,
        volume=200, amount=240_000, raw_open=12, raw_high=13, raw_low=11, raw_close=12,
    )

    assert _append_minute(state, "X", first) is False
    assert _append_minute(state, "X", last) is True
    assert len(state["five_bars"]["X"]) == 1
    bucket = state["five_bars"]["X"][0]
    assert (bucket["open"], bucket["high"], bucket["low"], bucket["close"]) == (10, 13, 9, 12)
    assert bucket["vwap"] == pytest.approx(340_000 / (300 * 100))


def _signal_state(rows):
    return {
        "five_bars": {"X": rows},
        "candidate_meta": {"X": {"previous_raw_close": 10}},
        "session": {"X": {"open": 10, "high": 10.4}},
    }


def test_breakout_pullback_and_resonance_rules_are_independent():
    base = [
        {"open": 10, "high": 10.05, "low": 9.95, "close": 10, "amount": 100, "vwap": 9.9},
        {"open": 10, "high": 10.08, "low": 9.98, "close": 10.02, "amount": 100, "vwap": 9.92},
        {"open": 10.02, "high": 10.09, "low": 10, "close": 10.04, "amount": 100, "vwap": 9.94},
        {"open": 10.04, "high": 10.10, "low": 10.01, "close": 10.05, "amount": 100, "vwap": 9.96},
        {"open": 10.05, "high": 10.16, "low": 10.04, "close": 10.15, "amount": 180, "vwap": 9.98},
    ]
    metrics = {"l1_breadth": 0.7, "l2_breadth": 0.7, "excess15": 0.012}

    hits = _model_hits(_signal_state(base), "X", metrics)

    assert hits["breakout"] is True
    assert hits["resonance"] is True

    pullback_rows = [
        {"open": 10, "high": 10.3, "low": 10, "close": 10.25, "amount": 300, "vwap": 10.05},
        {"open": 10.25, "high": 10.27, "low": 10.08, "close": 10.12, "amount": 120, "vwap": 10.10},
        {"open": 10.12, "high": 10.15, "low": 10.08, "close": 10.11, "amount": 100, "vwap": 10.10},
        {"open": 10.11, "high": 10.2, "low": 10.09, "close": 10.18, "amount": 100, "vwap": 10.11},
    ]
    assert _model_hits(_signal_state(pullback_rows), "X", metrics)["pullback"] is True


def test_combined_model_requires_two_hits_within_ten_minutes():
    state = {"model_hit_times": {}}
    now = datetime(2026, 8, 13, 10, 0)

    assert _combined_model_hit(
        state, "X", {"breakout": True, "pullback": False, "resonance": False}, now,
    ) is False
    assert _combined_model_hit(
        state, "X", {"breakout": False, "pullback": True, "resonance": False},
        now + timedelta(minutes=10),
    ) is True
    assert _combined_model_hit(
        state, "X", {"breakout": False, "pullback": False, "resonance": True},
        now + timedelta(minutes=21),
    ) is False


def test_blank_industry_code_uses_stable_standard_level_name_key():
    frame = pl.DataFrame({
        "l1_code": ["", "801010"],
        "l1_name": ["农林牧渔", "农林牧渔"],
    }).with_columns(
        _industry_key("l1", "申银万国行业分类标准", 1).alias("key")
    )

    assert frame["key"].to_list() == [
        "申银万国行业分类标准|1|农林牧渔",
        "申银万国行业分类标准|1|801010",
    ]


def test_context_industry_allow_missing_uses_partial_loader():
    engine = FreeStrategyEngine("def on_bar(context, bars):\n    pass\n", timeframe="1m")
    engine.set_industry_history_loader(
        lambda symbols, *_args: {symbol: {"strict": True} for symbol in symbols},
        partial_loader=lambda symbols, *_args: {symbols[0]: {"strict": False}},
    )
    engine.context.now = datetime(2026, 8, 13, 9, 31)

    assert engine.context.get_industry(
        ["X", "Y"], date(2026, 8, 12), "SW", allow_missing=True,
    ) == {"X": {"strict": False}}


def test_mainline_empty_boundary_snapshot_keeps_session_flat():
    engine = FreeStrategyEngine(
        TEMPLATES["mainline_momentum_breakout"]["source"],
        timeframe="1m",
        instruments=[{"symbol": "X", "asset_type": "stock", "has_minute": True}],
    )
    engine.set_mainline_snapshot_loader(lambda day: {
        "date": day.isoformat(), "as_of": "2021-07-29", "coverage": 0.3,
        "industries": [], "subindustries": [], "candidates": [],
    })

    engine.begin_session(date(2021, 7, 30))

    assert engine.universe == ["X"]
    assert engine.context.state["mainline_momentum"]["candidate_meta"] == {}


def test_historical_st_name_is_filtered_at_the_historical_date(tmp_path):
    history_path = tmp_path / "instrument_name_history/part.parquet"
    history_path.parent.mkdir(parents=True)
    pl.DataFrame([{
        "symbol": "X",
        "change_date": date(2024, 2, 1),
        "before_name": "ST测试",
        "after_name": "测试股份",
    }]).write_parquet(history_path)
    cache = object.__new__(MainlineSnapshotCache)
    cache.repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    instruments = pl.DataFrame([{
        "symbol": "X", "name": "测试股份", "listing_date": date(2020, 1, 1),
    }])
    frame = pl.DataFrame([
        {"symbol": "X", "date": date(2024, 1, 31)},
        {"symbol": "X", "date": date(2024, 2, 2)},
    ])

    eligible = cache._eligible_rows(frame, instruments)

    assert eligible["eligible"].to_list() == [False, True]


class _SnapshotRepo:
    def __init__(self, data_dir, missing_industry=False):
        self.store = SimpleNamespace(data_dir=data_dir)
        self.symbols = [f"{index:06d}.SZ" for index in range(1, 14)]
        self.missing_industry = missing_industry
        dates = [date(2023, 12, 1) + timedelta(days=index) for index in range(90)]
        dates = [day for day in dates if day.weekday() < 5]
        self.daily = pl.DataFrame([
            {
                "symbol": symbol,
                "date": day,
                "open": 10 + offset * 0.1,
                "high": 10.2 + offset * 0.1,
                "low": 9.8 + offset * 0.1,
                "close": 10.1 + offset * 0.1,
                "volume": 20_000_000,
                "amount": 200_000_000 + index * 1_000_000,
                "raw_close": 10.1 + offset * 0.1,
                "raw_high": 10.2 + offset * 0.1,
                "raw_low": 9.8 + offset * 0.1,
            }
            for index, symbol in enumerate(self.symbols)
            for offset, day in enumerate(dates)
        ])
        self.benchmark = pl.DataFrame([
            {"symbol": "000905.SH", "date": day, "close": 100 + offset * 0.01}
            for offset, day in enumerate(dates)
        ])
        history = []
        for index, symbol in enumerate(self.symbols):
            if missing_industry and index == 0:
                continue
            for level, name in ((1, "一级主线"), (2, f"二级{index // 7}")):
                history.append({
                    "member_symbol": symbol,
                    "industry_standard": "申银万国行业分类标准",
                    "industry_level": level,
                    "industry_code": "",
                    "industry_name": name,
                    "effective_from": date(2020, 1, 1),
                    "effective_to": None,
                })
        path = data_dir / "pit_reference/history/industry_membership_history/part.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame(history).write_parquet(path)

    def get_instruments_asset(self, _asset):
        return pl.DataFrame([
            {
                "symbol": symbol,
                "name": "ST噪音" if index == 12 else f"股票{index}",
                "exchange": "SZ",
                "listing_date": date(2020, 1, 1),
                "delist_date": None,
            }
            for index, symbol in enumerate(self.symbols)
        ])

    def get_minute_symbols(self, _asset, _start, _end):
        return self.symbols

    def get_daily_asset_batch(self, _asset, symbols, start, end, columns):
        return self.daily.filter(
            pl.col("symbol").is_in(symbols)
            & pl.col("date").is_between(start, end)
        ).select(columns)

    def get_daily_asset(self, _asset, _symbol, start, end, columns):
        return self.benchmark.filter(pl.col("date").is_between(start, end)).select(columns)


def test_snapshot_is_d_minus_one_and_caps_each_subindustry_at_five(tmp_path):
    repo = _SnapshotRepo(tmp_path)
    cache = MainlineSnapshotCache(
        repo, date(2024, 2, 26), date(2024, 2, 27),
        {"industry_levels": (1, 2), "min_coverage": 0.95, "lookback_days": 60},
    )

    snapshot = cache.snapshot(date(2024, 2, 27))

    assert snapshot["as_of"] == "2024-02-26"
    assert len(snapshot["candidates"]) <= 30
    counts: dict[str, int] = {}
    for row in snapshot["candidates"]:
        counts[row["l2_key"]] = counts.get(row["l2_key"], 0) + 1
        assert row["name"] != "ST噪音"
        assert row["l1_key"] == "申银万国行业分类标准|1|一级主线"
    assert counts and max(counts.values()) <= 5


def test_snapshot_fails_when_industry_coverage_is_below_threshold(tmp_path):
    repo = _SnapshotRepo(tmp_path, missing_industry=True)

    with pytest.raises(ValueError, match="行业最低覆盖率"):
        MainlineSnapshotCache(
            repo, date(2024, 2, 26), date(2024, 2, 27),
            {"industry_levels": (1, 2), "min_coverage": 0.95, "lookback_days": 60},
        )


class _AnalysisRepo:
    def get_daily_asset_batch(self, _asset, _symbols, _start, _end, _columns):
        return pl.DataFrame([
            {"symbol": "X", "date": date(2026, 8, 13), "close": 10.0, "high": 10.5, "low": 9.8, "raw_close": 20.0},
            {"symbol": "X", "date": date(2026, 8, 14), "close": 10.5, "high": 10.8, "low": 10.0, "raw_close": 21.0},
            {"symbol": "X", "date": date(2026, 8, 17), "close": 11.0, "high": 11.2, "low": 10.4, "raw_close": 22.0},
            {"symbol": "X", "date": date(2026, 8, 18), "close": 11.5, "high": 11.8, "low": 10.8, "raw_close": 23.0},
            {"symbol": "X", "date": date(2026, 8, 19), "close": 12.0, "high": 12.2, "low": 11.3, "raw_close": 24.0},
            {"symbol": "X", "date": date(2026, 8, 20), "close": 12.5, "high": 12.8, "low": 11.8, "raw_close": 25.0},
        ])

    def get_daily_asset(self, _asset, _symbol, _start, _end, _columns):
        return pl.DataFrame([
            {"date": date(2026, 8, 12), "close": 100},
            {"date": date(2026, 8, 13), "close": 101},
            {"date": date(2026, 8, 14), "close": 102},
            {"date": date(2026, 8, 17), "close": 103},
            {"date": date(2026, 8, 18), "close": 104},
            {"date": date(2026, 8, 19), "close": 105},
            {"date": date(2026, 8, 20), "close": 106},
        ])

    def get_minute_range(self, symbols, day, _end, asset_type):
        if asset_type == "index":
            return pl.DataFrame()
        return pl.DataFrame([
            {"symbol": symbols[0], "datetime": datetime.combine(day, datetime.min.time()).replace(hour=10), "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.0},
            {"symbol": symbols[0], "datetime": datetime.combine(day, datetime.min.time()).replace(hour=10, minute=30), "open": 10.2, "high": 10.6, "low": 10.1, "close": 10.5},
            {"symbol": symbols[0], "datetime": datetime.combine(day, datetime.min.time()).replace(hour=15), "open": 10.8, "high": 11.1, "low": 10.7, "close": 11.0},
        ])


def test_entry_analysis_keeps_missing_intraday_benchmark_explicit(tmp_path):
    for day, net_amount in ((date(2026, 8, 12), -100.0), (date(2026, 8, 13), 1_000.0)):
        path = tmp_path / f"ext_data/ext_tushare_moneyflow/timeseries/date={day}/part.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame([{
            "symbol": "X", "trade_date": day.isoformat(), "net_mf_amount": net_amount,
        }]).write_parquet(path)
    result = {"strategy_signals": [{
        "id": "entry-1",
        "timestamp": "2026-08-13T10:00:00",
        "signal_type": "mainline_momentum_entry",
        "payload": {"symbol": "X", "price": 20.0, "model": "breakout"},
    }]}

    analysis = build_mainline_entry_analysis(
        _AnalysisRepo(), result, date(2026, 8, 13), date(2026, 8, 20),
        tmp_path, "000905.SH",
    )

    assert analysis is not None
    assert analysis["intraday_benchmark_available"] is False
    event = analysis["events"][0]
    assert event["returns"]["30m"] == pytest.approx(5.0)
    assert event["returns"]["close"] == pytest.approx(10.0)
    assert event["excess"]["30m"] is None
    assert event["segment"] == "out_of_sample"
    assert analysis["money_flow"]["excluded_sources"] == ["ext_money_flow"]
    tushare = next(
        source for source in analysis["money_flow"]["sources"]
        if source["source"] == "Tushare资金流"
    )
    assert tushare["matched_signals"] == 1
    assert next(group for group in tushare["groups"] if not group["confirmed"])["signal_count"] == 1
