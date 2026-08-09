from datetime import date
from pathlib import Path

import polars as pl
import pytest

from app.free_strategy.readiness import (
    ReadinessUnavailable,
    build_readiness_manifest,
    make_requirement,
)


def write_table(tmp_path, relative, rows):
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def prepare_complete_data(tmp_path):
    write_table(tmp_path, "kline_daily/date=2024-01-01/part.parquet", {
        "symbol": ["X"],
        "date": [date(2024, 1, 1)],
        "close": [10.0],
        "raw_close": [10.0],
    })
    write_table(tmp_path, "financials/income/part.parquet", {
        "symbol": ["X"],
        "period_end": ["2023-09-30"],
        "announce_date": ["2023-10-30"],
        "revenue": [100.0],
    })
    write_table(tmp_path, "valuation_daily/date=2024-01-01/part.parquet", {
        "symbol": ["X"],
        "date": [date(2024, 1, 1)],
        "market_cap": [1_000.0],
    })
    write_table(
        tmp_path,
        "pit_reference/history/instrument_lifecycle_events/part.parquet",
        {
            "symbol": ["X"],
            "event_date": [date(2020, 1, 1)],
            "event_type": ["listed"],
            "source": ["tickflow-test"],
        },
    )
    write_table(tmp_path, "corporate_actions/stock_dividends.parquet", {
        "symbol": ["X"],
        "event_date": [date(2023, 6, 1)],
        "cash_per_share": [0.1],
    })


def requirement():
    return make_requirement(
        rebalance="monthly",
        financials={"income": {"fields": ["revenue"], "periods": 1}},
        valuation_fields=["market_cap"],
        lifecycle=True,
        adjustment="pre",
        corporate_actions=True,
    )


def test_readiness_manifest_checks_full_declared_universe(tmp_path):
    prepare_complete_data(tmp_path)
    write_table(tmp_path, "kline_daily/.part.pre-repair.parquet", {
        "symbol": ["X"],
        "date": [date(2024, 1, 1)],
    })

    report = build_readiness_manifest(
        tmp_path,
        [requirement()],
        strategy_sha256="strategy-hash",
        universe=["X"],
        trading_dates=[date(2024, 1, 2)],
        calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
        benchmark_symbol="BENCH",
        benchmark_dates=[date(2024, 1, 1)],
        dataset_roots=[Path("kline_daily")],
    )

    assert report["status"] == "passed"
    assert report["checks"] == [{
        "rebalance_date": "2024-01-02",
        "as_of": "2024-01-01",
        "universe_size": 1,
    }]
    assert len(report["tickflow_data_manifest_sha256"]) == 64
    assert len(report["trading_calendar_sha256"]) == 64
    assert all(
        ".part.pre-repair.parquet" not in item["path"]
        for item in report["source_proof"]["files"]
    )


def test_readiness_has_no_percentage_threshold_for_missing_symbol(tmp_path):
    prepare_complete_data(tmp_path)

    with pytest.raises(ReadinessUnavailable) as raised:
        build_readiness_manifest(
            tmp_path,
            [requirement()],
            strategy_sha256="strategy-hash",
            universe=["X", "Y"],
            trading_dates=[date(2024, 1, 2)],
            calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
            benchmark_symbol="BENCH",
            benchmark_dates=[date(2024, 1, 1)],
            dataset_roots=[Path("kline_daily")],
        )

    report = raised.value.report
    assert report["status"] == "failed"
    lifecycle = next(item for item in report["gaps"] if item["kind"] == "lifecycle")
    assert lifecycle["symbols"] == ["Y"]
    assert "85%" not in str(report)


def test_readiness_excludes_symbol_listed_after_as_of(tmp_path):
    write_table(
        tmp_path,
        "pit_reference/history/instrument_lifecycle_events/part.parquet",
        {
            "symbol": ["FUTURE"],
            "event_date": [date(2024, 1, 3)],
            "event_type": ["listed"],
        },
    )

    report = build_readiness_manifest(
        tmp_path,
        [make_requirement(rebalance="daily", lifecycle=True)],
        strategy_sha256="strategy-hash",
        universe=["FUTURE"],
        trading_dates=[date(2024, 1, 2)],
        calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
        benchmark_symbol="BENCH",
        benchmark_dates=[date(2024, 1, 1)],
    )

    assert report["status"] == "passed"
    assert report["checks"][0]["universe_size"] == 0


def test_readiness_loads_lifecycle_events_once_for_multiple_rebalances(
    tmp_path, monkeypatch,
):
    prepare_complete_data(tmp_path)
    real_read_parquet = pl.read_parquet
    lifecycle_reads = 0

    def tracked_read_parquet(path, *args, **kwargs):
        nonlocal lifecycle_reads
        if str(path).endswith("instrument_lifecycle_events/part.parquet"):
            lifecycle_reads += 1
        return real_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", tracked_read_parquet)

    report = build_readiness_manifest(
        tmp_path,
        [make_requirement(rebalance="daily", lifecycle=True)],
        strategy_sha256="strategy-hash",
        universe=["X"],
        trading_dates=[date(2024, 1, 2), date(2024, 1, 3)],
        calendar_dates=[
            date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3),
        ],
        benchmark_symbol="BENCH",
        benchmark_dates=[date(2024, 1, 1), date(2024, 1, 2)],
    )

    assert report["status"] == "passed"
    assert len(report["checks"]) == 2
    assert lifecycle_reads == 1


def test_readiness_blocks_future_only_financial_period(tmp_path):
    prepare_complete_data(tmp_path)
    write_table(tmp_path, "financials/income/part.parquet", {
        "symbol": ["X"],
        "period_end": ["2023-09-30"],
        "announce_date": ["2024-01-03"],
        "revenue": [100.0],
    })

    with pytest.raises(ReadinessUnavailable) as raised:
        build_readiness_manifest(
            tmp_path,
            [requirement()],
            strategy_sha256="strategy-hash",
            universe=["X"],
            trading_dates=[date(2024, 1, 2)],
            calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
            benchmark_symbol="BENCH",
            benchmark_dates=[date(2024, 1, 1)],
            dataset_roots=[Path("kline_daily")],
        )

    financial = next(item for item in raised.value.report["gaps"] if item["kind"] == "financial")
    assert financial["symbols"] == ["X"]


def test_readiness_classifies_null_required_field_as_financial_gap(tmp_path):
    prepare_complete_data(tmp_path)
    write_table(tmp_path, "financials/income/part.parquet", {
        "symbol": ["X"],
        "period_end": ["2023-09-30"],
        "announce_date": ["2023-10-30"],
        "revenue": [None],
    })

    with pytest.raises(ReadinessUnavailable) as raised:
        build_readiness_manifest(
            tmp_path,
            [requirement()],
            strategy_sha256="strategy-hash",
            universe=["X"],
            trading_dates=[date(2024, 1, 2)],
            calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
            benchmark_symbol="BENCH",
            benchmark_dates=[date(2024, 1, 1)],
            dataset_roots=[Path("kline_daily")],
        )

    financial = next(item for item in raised.value.report["gaps"] if item["kind"] == "financial")
    assert financial["symbols"] == ["X"]
    assert all(item["kind"] != "financial_conflict" for item in raised.value.report["gaps"])


def test_readiness_validates_declared_comparison_periods(tmp_path):
    prepare_complete_data(tmp_path)
    write_table(tmp_path, "financials/income/part.parquet", {
        "symbol": ["X", "X"],
        "period_end": ["2023-09-30", "2022-12-31"],
        "announce_date": ["2023-10-30", "2023-03-30"],
        "revenue": [100.0, 90.0],
    })
    compared = make_requirement(
        rebalance="monthly",
        financials={
            "income": {
                "fields": ["revenue"],
                "periods": 2,
                "comparison": "consecutive",
            },
        },
        lifecycle=True,
    )

    with pytest.raises(ReadinessUnavailable) as raised:
        build_readiness_manifest(
            tmp_path,
            [compared],
            strategy_sha256="strategy-hash",
            universe=["X"],
            trading_dates=[date(2024, 1, 2)],
            calendar_dates=[date(2024, 1, 1), date(2024, 1, 2)],
            benchmark_symbol="BENCH",
            benchmark_dates=[date(2024, 1, 1)],
        )

    assert any(item["kind"] == "financial" for item in raised.value.report["gaps"])
