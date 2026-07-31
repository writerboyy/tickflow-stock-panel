from __future__ import annotations

import queue
from datetime import datetime, time, timedelta
from hashlib import sha256

import polars as pl
import pytest

from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
from app.free_strategy.process import (
    MarketData,
    _assert_performance_small_cap_financial_coverage,
    _aligned_warmup_bars,
    _is_performance_small_cap_source,
    _load_financial_snapshot,
    _load_scheduled_history,
    _load_scheduled_history_batch,
    _prepare_market_data,
    _preload_tradable_dates,
    _read_rows,
    _scheduled_daily_bar,
    _scheduled_snapshot,
    _set_daily_row,
    advance_scheduled_session,
    execute_backtest,
)
from app.services.stock_dividends import import_xdxr_cash_dividends, load_cash_dividends
from app.free_strategy.templates import TEMPLATES


class DailyRepository:
    def __init__(self, rows: dict[str, list[dict]]) -> None:
        self.rows = rows

    def get_daily_asset(self, _asset_type, symbol, start, end, _columns):
        return pl.DataFrame([
            row for row in self.rows[symbol] if start <= row["date"] <= end
        ])


def daily_row(day, price: float) -> dict:
    return {
        "date": day,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "raw_close": price,
        "raw_high": price,
        "raw_low": price,
        "volume": 100.0,
        "amount": price * 100,
    }


def _write_financial_table(tmp_path, table: str, rows: dict) -> None:
    path = tmp_path / "financials" / table / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_axdata_xdxr_cash_dividend_import_overrides_price_inference(tmp_path):
    source = tmp_path / "xdxr.parquet"
    pl.DataFrame({
        "instrument_id": ["X", "X", "X"],
        "event_date": ["20240102", "20240102", "20240103"],
        "c1": [2.5, 0.0, 1.0],
        "record_hex": ["a", "b", "c"],
    }).write_parquet(source)

    assert import_xdxr_cash_dividends(source, tmp_path) == 2
    market = MarketData(cash_dividends=load_cash_dividends(tmp_path))
    first = datetime(2024, 1, 1).date()
    second = datetime(2024, 1, 2).date()
    _set_daily_row(market, "X", first, daily_row(first, 10.0))
    _set_daily_row(market, "X", second, daily_row(second, 9.0))

    bar = _scheduled_daily_bar(market, "X", second, "stock")

    assert bar is not None
    assert bar.cash_dividend == 0.25


def test_financial_snapshot_uses_latest_announced_records_without_future_data(tmp_path):
    income_path = tmp_path / "financials" / "income" / "part.parquet"
    income_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["X", "X"],
        "period_end": ["2024-03-31", "2024-06-30"],
        "announce_date": ["2024-04-30", "2024-08-30"],
        "revenue": [120_000_000.0, 80_000_000.0],
        "net_income": [10_000_000.0, -1.0],
        "net_income_attributable": [9_000_000.0, -1.0],
    }).write_parquet(income_path)
    metrics_path = tmp_path / "financials" / "metrics" / "part.parquet"
    metrics_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["X", "X"],
        "period_end": ["2024-03-31", "2024-06-30"],
        "announce_date": ["2024-04-30", "2024-08-30"],
        "roe": [6.0, -1.0],
    }).write_parquet(metrics_path)
    balance_path = tmp_path / "financials" / "balance_sheet" / "part.parquet"
    balance_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["X", "X"],
        "period_end": ["2024-03-31", "2024-06-30"],
        "announce_date": ["2024-04-30", "2024-08-30"],
        "total_assets": [200_000_000.0, 1.0],
    }).write_parquet(balance_path)

    snapshot = _load_financial_snapshot(
        tmp_path,
        ["X"],
        datetime(2024, 7, 1).date(),
    )

    assert snapshot["X"]["revenue"] == 120_000_000.0
    assert snapshot["X"]["net_income_attributable"] == 9_000_000.0
    assert snapshot["X"]["roe"] == 6.0
    assert snapshot["X"]["roa"] == 5.0


def test_performance_small_cap_financial_coverage_allows_historical_records(tmp_path):
    rows = {
        "symbol": ["X"],
        "period_end": ["2024-03-31"],
        "announce_date": ["2024-04-30"],
    }
    for table in ("income", "metrics", "balance_sheet"):
        _write_financial_table(tmp_path, table, rows)

    _assert_performance_small_cap_financial_coverage(
        tmp_path,
        datetime(2024, 7, 1).date(),
    )


def test_performance_small_cap_financial_coverage_rejects_future_only_data(tmp_path):
    rows = {
        "symbol": ["X"],
        "period_end": ["2024-06-30"],
        "announce_date": ["2024-08-30"],
    }
    for table in ("income", "metrics", "balance_sheet"):
        _write_financial_table(tmp_path, table, rows)

    with pytest.raises(ValueError, match="绩优小市值回测需要.*income.*metrics.*balance_sheet"):
        _assert_performance_small_cap_financial_coverage(
            tmp_path,
            datetime(2024, 7, 1).date(),
        )


def test_performance_small_cap_financial_preflight_uses_template_source(tmp_path):
    source = TEMPLATES["performance_small_cap"]["source"]

    assert _is_performance_small_cap_source(source) is True
    assert _is_performance_small_cap_source("def on_bar(context, bars):\n    pass\n") is False

    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": source,
        "strategy_id": "saved-template-copy",
        "timeframe": "1m",
        "asset_type": "stock",
        "start": "2025-07-24",
        "end": "2025-07-24",
        "config": {},
    }, output)

    assert output.get_nowait()["type"] == "progress"
    error = output.get_nowait()
    assert error["type"] == "error"
    assert "绩优小市值回测需要首个回测日前已公告的历史财务数据" in error["error"]


def test_scheduled_daily_bar_cache_invalidates_when_row_changes():
    first_day = datetime(2024, 1, 2).date()
    second_day = datetime(2024, 1, 3).date()
    market = MarketData()
    _set_daily_row(market, "X", first_day, daily_row(first_day, 10))
    _set_daily_row(market, "X", second_day, daily_row(second_day, 11))

    first = _scheduled_daily_bar(market, "X", second_day, "stock")
    cached = _scheduled_daily_bar(market, "X", second_day, "stock")
    _set_daily_row(market, "X", second_day, daily_row(second_day, 12))
    refreshed = _scheduled_daily_bar(market, "X", second_day, "stock")

    assert cached is first
    assert refreshed is not first
    assert refreshed is not None and refreshed.close == 12


def test_scheduled_limits_use_name_valid_on_historical_date():
    previous_day = datetime(2025, 8, 15).date()
    historical_day = datetime(2025, 8, 18).date()
    risk_warning_day = datetime(2026, 4, 29).date()
    market = MarketData(
        names={"002207.SZ": "*ST准油"},
        name_changes={
            "002207.SZ": ((risk_warning_day, "准油股份", "*ST准油"),),
        },
    )
    _set_daily_row(market, "002207.SZ", previous_day, daily_row(previous_day, 10))
    _set_daily_row(market, "002207.SZ", historical_day, daily_row(historical_day, 10))
    _set_daily_row(market, "002207.SZ", risk_warning_day, daily_row(risk_warning_day, 10))

    historical = _scheduled_daily_bar(market, "002207.SZ", historical_day, "stock")
    risk_warning = _scheduled_daily_bar(market, "002207.SZ", risk_warning_day, "stock")

    assert historical is not None and historical.limit_up == 11
    assert risk_warning is not None and risk_warning.limit_up == 10.5


def test_scheduled_stock_split_uses_share_change_instead_of_adjustment_scale():
    previous_day = datetime(2026, 5, 28).date()
    split_day = datetime(2026, 5, 29).date()
    market = MarketData()
    previous = {
        **daily_row(previous_day, 16.1697146989273),
        "raw_close": 19.7,
        "total_shares": 190_182_182.0,
    }
    current = {
        **daily_row(split_day, 16.5),
        "raw_close": 16.5,
        "total_shares": 228_218_618.0,
    }
    _set_daily_row(market, "003020.SZ", previous_day, previous)
    _set_daily_row(market, "003020.SZ", split_day, current)

    bar = _scheduled_daily_bar(market, "003020.SZ", split_day, "stock")

    assert bar is not None and bar.split_ratio == 1.2
    assert bar.cash_dividend == 0.3


def test_scheduled_stock_cash_dividend_does_not_change_position_quantity():
    previous_day = datetime(2026, 5, 28).date()
    dividend_day = datetime(2026, 5, 29).date()
    market = MarketData()
    previous = {
        **daily_row(previous_day, 9.8),
        "raw_close": 10.0,
        "total_shares": 100_000_000.0,
    }
    current = {
        **daily_row(dividend_day, 9.8),
        "raw_close": 9.8,
        "total_shares": 100_000_000.0,
    }
    _set_daily_row(market, "X", previous_day, previous)
    _set_daily_row(market, "X", dividend_day, current)

    bar = _scheduled_daily_bar(market, "X", dividend_day, "stock")

    assert bar is not None and bar.split_ratio == 1.0
    assert bar.cash_dividend == 0.2


def test_scheduled_stock_small_share_increase_is_not_a_distribution():
    previous_day = datetime(2026, 5, 28).date()
    issuance_day = datetime(2026, 5, 29).date()
    market = MarketData()
    previous = {
        **daily_row(previous_day, 10.0),
        "raw_close": 10.0,
        "total_shares": 100_000_000.0,
    }
    current = {
        **daily_row(issuance_day, 10.0),
        "raw_close": 10.0,
        "total_shares": 100_500_000.0,
    }
    _set_daily_row(market, "X", previous_day, previous)
    _set_daily_row(market, "X", issuance_day, current)

    bar = _scheduled_daily_bar(market, "X", issuance_day, "stock")

    assert bar is not None and bar.split_ratio == 1.0
    assert bar.cash_dividend == 0.0


def test_minute_warmup_daily_prices_align_to_minute_adjustment_scale():
    market = MarketData(
        daily={
            ("X", datetime(2024, 1, 1).date()): {
                "open": 19.0, "high": 21.0, "low": 18.0, "close": 10.0,
                "raw_close": 20.0, "raw_high": 21.0, "raw_low": 18.0,
                "volume": 100.0, "amount": 2_000.0,
            },
            ("X", datetime(2024, 1, 2).date()): {
                "open": 10.5, "high": 11.5, "low": 10.0, "close": 11.0,
                "raw_close": 22.0, "raw_high": 23.0, "raw_low": 20.0,
                "volume": 100.0, "amount": 2_200.0,
            },
        },
        previous_scale={"X": 4.0},
    )

    bars = _aligned_warmup_bars(
        ["X"], datetime(2024, 1, 1).date(), datetime(2024, 1, 2).date(), market,
    )

    assert [bar.close for bar in bars] == [5.0, 5.5]


def test_minute_rows_derive_raw_prices_split_and_limits_from_daily_data():
    class FakeRepository:
        def get_minute_range(self, _symbols, _start, _end, _asset_type):
            return pl.DataFrame({
                "symbol": ["X", "X"],
                "datetime": [datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 15)],
                "open": [5.0, 5.5], "high": [5.5, 6.0], "low": [5.0, 5.5],
                "close": [5.5, 6.0], "volume": [100.0, 100.0], "amount": [550.0, 600.0],
            })

    market = MarketData(
        daily={("X", datetime(2024, 1, 2).date()): {"raw_close": 12.0}},
        previous_scale={"X": 4.0},
        previous_adjusted_close={"X": 5.0},
    )

    bars = list(_read_rows(
        FakeRepository(), ["X"], datetime(2024, 1, 2).date(), datetime(2024, 1, 2).date(),
        "etf", "1m", market_data=market,
    ))

    assert bars[0].raw_open == 10
    assert bars[-1].raw_close == 12
    assert bars[0].split_ratio == 2
    assert (bars[0].limit_up, bars[0].limit_down) == (11, 9)


def test_market_preparation_does_not_inject_undeclared_history():
    start = datetime(2024, 2, 1).date()
    rows = {"X": [daily_row(start - timedelta(days=day), float(day)) for day in range(1, 11)]}
    engine = FreeStrategyEngine("def on_bar(context, bars):\n    pass\n")

    market, warmup = _prepare_market_data(
        DailyRepository(rows), engine, ["X"], start, start, "etf", "1d",
    )

    assert warmup == {
        "enabled": False,
        "timeframe": None,
        "requested_bars": 0,
        "rows": 0,
        "symbols": 0,
        "start": None,
        "end": None,
    }
    assert engine.context.history_bars("X", count=100, timeframe="1d") == []
    assert market.previous_adjusted_close["X"] == 1.0


def test_stale_fill_dates_include_only_days_with_trading_evidence():
    active = datetime(2024, 2, 1).date()
    suspended = datetime(2024, 2, 2).date()
    engine = FreeStrategyEngine(
        "def on_bar(context, bars):\n    pass\n",
        config=FreeStrategyConfig(allow_stale_fills=True),
    )
    market = MarketData(daily={
        ("X", active): daily_row(active, 10),
        ("X", suspended): {**daily_row(suspended, 0), "open": 0, "high": 0},
    })

    _preload_tradable_dates(engine, market)

    assert engine._tradable_dates == {("X", active)}  # noqa: SLF001


def test_market_preparation_injects_exact_declared_bars_per_symbol():
    start = datetime(2024, 4, 1).date()
    symbols = ["X", "Y"]
    rows = {
        symbol: [
            daily_row(start - timedelta(days=offset), float(100 - offset))
            for offset in range(70, 0, -1)
        ] + [daily_row(start, 100.0)]
        for symbol in symbols
    }
    engine = FreeStrategyEngine("""
def initialize(context):
    context.require_history(timeframe='1d', bars=61)

def on_bar(context, bars):
    pass
""")

    _, warmup = _prepare_market_data(
        DailyRepository(rows), engine, symbols, start, start, "etf", "1d",
    )

    assert warmup == {
        "enabled": True,
        "timeframe": "1d",
        "requested_bars": 61,
        "rows": 122,
        "symbols": 2,
        "start": (start - timedelta(days=61)).isoformat(),
        "end": (start - timedelta(days=1)).isoformat(),
    }
    assert all(
        len(engine.context.history_bars(symbol, count=100, timeframe="1d")) == 61
        for symbol in symbols
    )


def test_market_preparation_loads_declared_market_reference_in_one_batch():
    class ReferenceRepository(DailyRepository):
        def get_daily_asset_batch(self, _asset_type, symbols, start, end, _columns):
            return pl.DataFrame([
                {"symbol": symbol, **row}
                for symbol in symbols
                for row in self.rows[symbol]
                if start <= row["date"] <= end
            ])

    start = datetime(2024, 4, 1).date()
    rows = {
        symbol: [daily_row(start - timedelta(days=1), price), daily_row(start, price + 1)]
        for symbol, price in (("X", 10.0), ("Y", 20.0))
    }
    engine = FreeStrategyEngine("""
def initialize(context):
    context.set_universe(['X'])
    context.require_market_history(asset_type='etf', timeframe='1d', bars=1)

def on_bar(context, bars):
    pass
""", instruments=[
        {"symbol": "X", "name": "X", "asset_type": "etf"},
        {"symbol": "Y", "name": "Y", "asset_type": "etf"},
    ])

    _prepare_market_data(
        ReferenceRepository(rows), engine, ["X"], start, start, "etf", "1d",
    )
    engine.begin_session(start)

    assert engine.market_history_metadata == {
        "enabled": True,
        "asset_type": "etf",
        "timeframe": "1d",
        "requested_bars": 1,
        "rows": 4,
        "symbols": 2,
        "start": (start - timedelta(days=1)).isoformat(),
        "end": start.isoformat(),
    }
    assert [bar.close for bar in engine.context.market_history_bars("Y")] == [20.0]


def test_minute_backtest_records_exact_data_coverage(monkeypatch, tmp_path):
    class FakeRepository:
        def __init__(self, _store):
            pass

        def get_minute_range(self, symbols, start, _end, _asset_type):
            timestamps = [datetime.combine(start, datetime.min.time()).replace(hour=9, minute=30)] * len(symbols)
            return pl.DataFrame({
                "symbol": symbols,
                "datetime": timestamps,
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100.0] * len(symbols),
                "amount": [100.0] * len(symbols),
            })

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": "def on_bar(context, bars):\n    pass\n",
        "strategy_id": "coverage-test",
        "strategy_name": "覆盖测试",
        "source_revision": 1,
        "symbols": ["510300.SH", "511880.SH"],
        "timeframe": "1m",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-03",
        "config": {"asset_type": "etf"},
        "data_provider": "tickflow",
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()

    assert event["type"] == "result"
    coverage = event["result"]["metadata"]["data_coverage"]
    assert coverage == {
        "rows": 4,
        "first_bar": "2024-01-02T09:30:00",
        "last_bar": "2024-01-03T09:30:00",
        "trading_days": 2,
        "requested_symbols": ["510300.SH", "511880.SH"],
        "seen_symbols": ["510300.SH", "511880.SH"],
        "missing_symbols": [],
        "configured_provider": "tickflow",
        "storage": "kline_etf_minute",
    }
    assert event["result"]["metadata"]["warmup"] == {
        "enabled": False,
        "timeframe": None,
        "requested_bars": 0,
        "rows": 0,
        "symbols": 0,
        "start": None,
        "end": None,
    }
    assert event["result"]["metadata"]["strategy_source_sha256"] == sha256(
        b"def on_bar(context, bars):\n    pass\n"
    ).hexdigest()


def test_backtest_reads_universe_from_strategy_source(monkeypatch, tmp_path):
    requested: list[list[str]] = []

    class FakeRepository:
        def __init__(self, _store):
            pass

        def get_daily_asset(self, _asset_type, symbol, start, _end, _columns):
            requested.append([symbol])
            return pl.DataFrame({
                "date": [start], "open": [1.0], "high": [1.0], "low": [1.0],
                "close": [1.0], "volume": [100.0], "amount": [100.0],
            })

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": "def initialize(context):\n    context.set_universe(['510300.XSHG'])\n\ndef on_bar(context, bars):\n    pass\n",
        "strategy_id": "source-universe",
        "strategy_name": "源码股票池",
        "source_revision": 1,
        "symbols": [],
        "timeframe": "1d",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-02",
        "config": {"asset_type": "etf"},
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()

    assert event["type"] == "result"
    assert requested == [["510300.SH"], ["510300.SH"]]
    assert event["result"]["metadata"]["universe_source"] == "strategy_source"
    assert event["result"]["metadata"]["symbols"] == ["510300.SH"]


def test_backtest_requires_universe_in_source_or_legacy_config(tmp_path):
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": "def on_bar(context, bars):\n    pass\n",
        "symbols": [],
        "timeframe": "1d",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-02",
        "config": {"asset_type": "etf"},
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()

    assert event["type"] == "error"
    assert "context.set_universe" in event["error"]


def test_failed_backtest_removes_incomplete_run_directory(monkeypatch, tmp_path):
    class FakeRepository:
        def __init__(self, _store):
            pass

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    run_dir = tmp_path / "free_strategy_runs" / "failed-run"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")
    output: queue.SimpleQueue = queue.SimpleQueue()

    execute_backtest({
        "data_dir": str(tmp_path),
        "run_dir": str(run_dir),
        "source": "this is not valid Python!",
        "symbols": ["X"],
        "timeframe": "1d",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-02",
        "config": {"asset_type": "etf"},
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()
    assert event["type"] == "error"
    assert not run_dir.exists()


def test_backtest_rejects_a_tampered_source_snapshot(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "tampered-run"
    run_dir.mkdir(parents=True)
    (run_dir / "strategy.py").write_text(
        "def on_bar(context, bars):\n    context.log('tampered')\n",
        encoding="utf-8",
    )
    source = "def on_bar(context, bars):\n    pass\n"
    output: queue.SimpleQueue = queue.SimpleQueue()

    execute_backtest({
        "data_dir": str(tmp_path),
        "run_dir": str(run_dir),
        "source": source,
        "strategy_source_sha256": sha256(source.encode("utf-8")).hexdigest(),
        "symbols": ["X"],
        "timeframe": "1d",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-02",
        "config": {"asset_type": "etf"},
    }, output)

    assert output.get()["type"] == "progress"
    event = output.get()
    assert event["type"] == "error"
    assert "源码快照与任务源码不一致" in event["error"]
    assert not run_dir.exists()


def test_scheduled_backtest_queries_events_without_full_minute_replay(monkeypatch, tmp_path):
    calls = {"range": 0, "snapshot": 0, "next": 0}

    class FakeRepository:
        def __init__(self, _store):
            pass

        def get_instruments_asset(self, _asset_type):
            return pl.DataFrame({"symbol": ["X"]})

        def get_minute_symbols(self, *_args):
            raise AssertionError("scheduled mode must not scan interval minute coverage")

        def get_daily_asset(self, _asset_type, symbol, start, end, _columns):
            if symbol != "X":
                return pl.DataFrame()
            days = [datetime(2024, 1, 2).date(), datetime(2024, 1, 3).date()]
            return pl.DataFrame([
                daily_row(day, 10.0 + index)
                for index, day in enumerate(days)
                if start <= day <= end
            ])

        def get_minute_range(self, *_args):
            calls["range"] += 1
            raise AssertionError("scheduled mode must not read the full minute range")

        def get_minute_snapshot(self, symbols, at, _asset_type):
            calls["snapshot"] += 1
            return pl.DataFrame({
                "symbol": symbols,
                "datetime": [at] * len(symbols),
                "open": [10.0] * len(symbols),
                "high": [10.0] * len(symbols),
                "low": [10.0] * len(symbols),
                "close": [10.0] * len(symbols),
                "volume": [100.0] * len(symbols),
                "amount": [1_000.0] * len(symbols),
            })

        def get_minute_next(self, _symbols, _after, _until, _asset_type):
            calls["next"] += 1
            return pl.DataFrame()

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(run, '13:10')

def run(context):
    context.state['runs'] = context.state.get('runs', 0) + 1
""",
        "strategy_id": "scheduled-test",
        "strategy_name": "定时测试",
        "source_revision": 1,
        "symbols": [],
        "timeframe": "1m",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-03",
        "config": {"asset_type": "etf"},
        "data_provider": "tickflow",
    }, output)

    event = output.get()
    progress_messages = []
    while event["type"] == "progress":
        progress_messages.append(event["message"])
        event = output.get()

    assert event["type"] == "result"
    assert event["result"]["state"]["runs"] == 2
    metadata = event["result"]["metadata"]
    assert metadata["execution_mode"] == "scheduled"
    assert metadata["scheduled_times"] == ["13:10"]
    assert metadata["callbacks_executed"] == 2
    assert metadata["market_rows_consumed"] < 20
    assert calls["range"] == 0
    assert calls["snapshot"] > 0
    assert any("执行定时任务" in message for message in progress_messages)


def test_scheduled_daily_intraday_requires_minute_data(monkeypatch, tmp_path):
    class FakeRepository:
        def __init__(self, _store):
            pass

        def get_daily_asset(self, _asset_type, symbol, start, end, _columns):
            if symbol != "X":
                return pl.DataFrame()
            day = datetime(2024, 1, 2).date()
            return pl.DataFrame([daily_row(day, 10.0)]) if start <= day <= end else pl.DataFrame()

        def get_minute_snapshot(self, _symbols, _at, _asset_type):
            return pl.DataFrame()

        def get_minute_next(self, _symbols, _after, _until, _asset_type):
            return pl.DataFrame()

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(lambda ctx: None, '13:10')
""",
        "symbols": [],
        "timeframe": "1d",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-02",
        "config": {"asset_type": "etf"},
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()

    assert event["type"] == "error"
    assert "分钟K" in event["error"]


class ScheduledRepository:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def get_daily_asset(self, _asset_type, _symbol, _start, _end, _columns):
        return pl.DataFrame()

    def get_minute_snapshot(self, symbols, at, _asset_type):
        visible = [row for row in self.rows if row["symbol"] in symbols and row["datetime"] <= at]
        latest = {}
        for row in visible:
            latest[row["symbol"]] = row
        return pl.DataFrame(list(latest.values())) if latest else pl.DataFrame()

    def get_minute_next(self, symbols, after, until, _asset_type):
        result = []
        for symbol in symbols:
            values = [
                row for row in self.rows
                if row["symbol"] == symbol and after < row["datetime"] <= until
            ]
            if values:
                result.append(min(values, key=lambda row: row["datetime"]))
        return pl.DataFrame(result) if result else pl.DataFrame()

    def get_minute_range(self, symbols, start, end, _asset_type):
        values = [
            row for row in self.rows
            if row["symbol"] in symbols and start <= row["datetime"].date() <= end
        ]
        return pl.DataFrame(values) if values else pl.DataFrame()


def minute_row(symbol: str, timestamp: datetime, price: float) -> dict:
    return {
        "symbol": symbol,
        "datetime": timestamp,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": 100.0,
        "amount": price * 100,
    }


def scheduled_market(*symbols: str) -> MarketData:
    previous = datetime(2024, 1, 1).date()
    current = datetime(2024, 1, 2).date()
    return MarketData(daily={
        (symbol, day): daily_row(day, 10.0)
        for symbol in symbols
        for day in (previous, current)
    })


def test_scheduled_lunch_event_uses_latest_visible_bar_and_dynamic_universe():
    source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(run, '12:00')

def before_trading_start(context):
    context.set_universe(['Y'])

def run(context):
    context.state['visible'] = context.history('Y', count=1)[0]
"""
    rows = [
        minute_row("X", datetime(2024, 1, 2, 11, 30), 10.0),
        minute_row("Y", datetime(2024, 1, 2, 11, 29), 20.0),
        minute_row("Y", datetime(2024, 1, 2, 13, 0), 30.0),
    ]
    repo = ScheduledRepository(rows)
    market = scheduled_market("X", "Y")
    engine = FreeStrategyEngine(source, timeframe="1m")
    engine.set_history_loader(lambda symbol, count, timeframe, cutoff: _load_scheduled_history(
        repo, market, "etf", symbol, count, timeframe, cutoff,
    ))

    advance_scheduled_session(
        repo,
        engine,
        market,
        datetime(2024, 1, 2).date(),
        datetime(2024, 1, 2, 15, 0),
        "etf",
        "1m",
        finalize=True,
    )

    assert engine.universe == ["Y"]
    assert engine.context.state["visible"] == 20.0


def test_scheduled_aggregated_history_never_reads_minutes_after_cutoff():
    rows = [
        minute_row("X", datetime(2024, 1, 2, 13, 10), 10.0),
        minute_row("X", datetime(2024, 1, 2, 13, 11), 11.0),
        minute_row("X", datetime(2024, 1, 2, 13, 12), 99.0),
    ]
    repo = ScheduledRepository(rows)
    market = scheduled_market("X")
    cutoff = datetime(2024, 1, 2, 13, 11)

    for timeframe in ("5m", "30m"):
        history = _load_scheduled_history(repo, market, "etf", "X", 10, timeframe, cutoff)
        assert history[-1].close == 11.0
        assert all(bar.timestamp <= cutoff for bar in history)


def test_scheduled_batch_history_reuses_loaded_daily_range():
    class BatchRepository:
        def __init__(self):
            self.ranges = []

        def get_daily_asset_batch(self, _asset_type, symbols, start, end, _columns):
            self.ranges.append((start, end))
            return pl.DataFrame([
                {"symbol": symbol, **daily_row(day, float(day.day))}
                for symbol in symbols
                for day in (
                    datetime(2024, 1, 2).date(),
                    datetime(2024, 1, 3).date(),
                    datetime(2024, 1, 4).date(),
                    datetime(2024, 1, 5).date(),
                )
                if start <= day <= end
            ])

        def get_daily_asset(self, *_args):
            raise AssertionError("batch history must use the batch repository API")

    repo = BatchRepository()
    market = MarketData()
    cutoff = datetime(2024, 1, 4, 10, 30)

    first = _load_scheduled_history_batch(repo, market, "stock", ["X", "Y"], 2, "1d", cutoff)
    second = _load_scheduled_history_batch(repo, market, "stock", ["X", "Y"], 2, "1d", cutoff)
    shifted = _load_scheduled_history_batch(
        repo,
        market,
        "stock",
        ["X", "Y"],
        2,
        "1d",
        datetime(2024, 1, 5, 10, 30),
    )

    assert len(repo.ranges) == 2
    assert repo.ranges[1] == (
        datetime(2024, 1, 5).date(),
        datetime(2024, 1, 5).date(),
    )
    assert [bar.close for bar in first["X"]] == [2.0, 3.0]
    assert [bar.close for bar in second["Y"]] == [2.0, 3.0]
    assert [bar.close for bar in shifted["X"]] == [3.0, 4.0]


def test_scheduled_snapshot_scope_avoids_full_universe_minute_reads():
    calls = {"range": 0, "snapshot": []}

    class CachedDayRepository:
        def get_minute_range(self, *_args):
            calls["range"] += 1
            raise AssertionError("scheduled snapshots must use the snapshot repository API")

        def get_minute_snapshot(self, symbols, at, _asset_type):
            calls["snapshot"].append((list(symbols), at))
            return pl.DataFrame([
                minute_row(symbol, at, 10.0)
                for symbol in symbols
            ])

    source = """
def initialize(context):
    context.set_universe(['X', 'Y'])
    context.schedule(run, '10:30', symbols=['X'])

def run(context):
    pass
"""
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(asset_type="stock", benchmark_symbol="X"),
    )
    market = scheduled_market("X", "Y")
    repo = CachedDayRepository()

    morning = _scheduled_snapshot(
        repo, engine, market, datetime(2024, 1, 2, 10, 30), "stock", "1m",
    )
    closing = _scheduled_snapshot(
        repo, engine, market, datetime(2024, 1, 2, 15, 0), "stock", "1m",
    )

    assert calls == {
        "range": 0,
        "snapshot": [
            (["X"], datetime(2024, 1, 2, 10, 30)),
            (["X"], datetime(2024, 1, 2, 15, 0)),
        ],
    }
    assert {bar.symbol for bar in morning} == {"X"}
    assert {bar.symbol for bar in closing} == {"X"}


def test_scheduled_next_open_preserves_t1_settlement():
    source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(buy, '13:10')
    context.schedule(sell, '13:12')

def buy(context):
    context.buy('X', quantity=100)

def sell(context):
    context.sell('X', quantity=100)
"""
    rows = [
        minute_row("X", datetime(2024, 1, 2, 13, 10), 10.0),
        minute_row("X", datetime(2024, 1, 2, 13, 11), 10.0),
        minute_row("X", datetime(2024, 1, 2, 13, 12), 10.0),
        minute_row("X", datetime(2024, 1, 2, 13, 13), 10.0),
        minute_row("X", datetime(2024, 1, 2, 15, 0), 10.0),
    ]
    repo = ScheduledRepository(rows)
    market = scheduled_market("X")
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(
            asset_type="etf",
            fill_policy="next_open",
            slippage_bps=0,
        ),
    )

    advance_scheduled_session(
        repo,
        engine,
        market,
        datetime(2024, 1, 2).date(),
        datetime(2024, 1, 2, 15, 0),
        "etf",
        "1m",
        finalize=True,
    )

    assert len(engine.account.fills) == 1
    assert engine.account.fills[0].timestamp == "2024-01-02T13:11:00"
    assert engine.account.orders[-1].status == "rejected"
    assert "T+1" in engine.account.orders[-1].reason


def test_scheduled_next_open_loads_order_symbol_outside_universe():
    source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(run, '13:10')

def run(context):
    context.buy('Y', quantity=100)
"""
    rows = [
        minute_row("X", datetime(2024, 1, 2, 13, 10), 10.0),
        minute_row("Y", datetime(2024, 1, 2, 13, 11), 20.0),
    ]
    class DynamicRepository(ScheduledRepository):
        requested_daily: list[str] = []

        def get_daily_asset(self, _asset_type, symbol, start, end, _columns):
            self.requested_daily.append(symbol)
            if symbol != "Y":
                return pl.DataFrame()
            days = [datetime(2024, 1, 1).date(), datetime(2024, 1, 2).date()]
            return pl.DataFrame([
                daily_row(day, 20.0) for day in days if start <= day <= end
            ])

    repo = DynamicRepository(rows)
    market = scheduled_market("X")
    engine = FreeStrategyEngine(
        source,
        timeframe="1m",
        config=FreeStrategyConfig(asset_type="etf", slippage_bps=0),
    )

    advance_scheduled_session(
        repo,
        engine,
        market,
        datetime(2024, 1, 2).date(),
        datetime(2024, 1, 2, 15, 0),
        "etf",
        "1m",
        finalize=True,
    )

    assert engine.account.fills[0].symbol == "Y"
    assert engine.account.fills[0].timestamp == "2024-01-02T13:11:00"
    assert "Y" in repo.requested_daily
    assert ("Y", datetime(2024, 1, 2).date()) in market.daily


def test_scheduled_daily_intraday_snapshot_fills_at_next_trading_day_open():
    source = """
def initialize(context):
    context.set_universe(['X'])
    context.schedule(run, '13:10')

def run(context):
    context.buy('X', quantity=100)
"""
    rows = [
        minute_row("X", datetime(2024, 1, 2, 13, 10), 10.0),
        minute_row("X", datetime(2024, 1, 3, 13, 10), 11.0),
    ]
    repo = ScheduledRepository(rows)
    days = [datetime(2024, 1, day).date() for day in (1, 2, 3)]
    market = MarketData(daily={
        ("X", day): daily_row(day, price)
        for day, price in zip(days, (10.0, 10.0, 10.5))
    })
    engine = FreeStrategyEngine(
        source,
        timeframe="1d",
        config=FreeStrategyConfig(asset_type="etf", slippage_bps=0),
    )

    for day in days[1:]:
        advance_scheduled_session(
            repo,
            engine,
            market,
            day,
            datetime.combine(day, time(15, 0)),
            "etf",
            "1d",
            finalize=True,
        )

    assert engine.callbacks_executed == 2
    assert engine.account.fills[0].timestamp == "2024-01-03T09:30:00"
    assert engine.account.fills[0].price == 10.5
