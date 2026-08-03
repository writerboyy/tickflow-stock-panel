from __future__ import annotations

from datetime import date
import json

import polars as pl
import pytest

from app.services.daily_valuation import (
    assert_daily_valuation_coverage,
    build_daily_valuation,
    build_ttm_events,
    load_latest_market_caps,
)
from app.services.source_snapshot import capture_source_snapshot


def _write_enriched(data_dir, day: date, *, close: float = 10.0) -> None:
    path = data_dir / "kline_daily_enriched" / f"date={day.isoformat()}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [day],
        "raw_close": [close],
        "total_shares": [100.0],
        "float_shares": [60.0],
    }).write_parquet(path)


def _write_financial(data_dir, table: str, rows: list[dict]) -> None:
    path = data_dir / "financials" / table / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_build_ttm_events_recomputes_when_prior_annual_is_revised() -> None:
    income = pl.DataFrame({
        "symbol": ["600000.SH"] * 4,
        "period_end": ["2023-03-31", "2023-12-31", "2024-03-31", "2023-12-31"],
        "announce_date": ["2023-04-20", "2024-03-30", "2024-04-22", "2024-05-10"],
        "net_income_attributable": [10.0, 50.0, 15.0, 52.0],
        "revenue": [100.0, 500.0, 120.0, 510.0],
    })

    events = build_ttm_events(
        income,
        {
            "net_income_attributable": "net_income_ttm",
            "revenue": "revenue_ttm",
        },
        prefix="income",
    )

    q1 = events.filter(pl.col("income_announce_date") == date(2024, 4, 22)).row(0, named=True)
    assert q1["income_period_end"] == date(2024, 3, 31)
    assert q1["net_income_ttm"] == pytest.approx(55.0)
    assert q1["revenue_ttm"] == pytest.approx(520.0)

    revised = events.filter(pl.col("income_announce_date") == date(2024, 5, 10)).row(0, named=True)
    assert revised["income_period_end"] == date(2024, 3, 31)
    assert revised["net_income_ttm"] == pytest.approx(57.0)
    assert revised["revenue_ttm"] == pytest.approx(530.0)


def test_financial_events_drop_exact_duplicates_independent_of_row_order() -> None:
    rows = [
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": 50.0},
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": 50.0},
    ]

    forward = build_ttm_events(
        pl.DataFrame(rows),
        {"net_income_attributable": "net_income_ttm"},
        prefix="income",
    )
    reverse = build_ttm_events(
        pl.DataFrame(list(reversed(rows))),
        {"net_income_attributable": "net_income_ttm"},
        prefix="income",
    )

    assert forward.equals(reverse)
    assert forward.height == 1


def test_financial_events_fail_closed_on_same_key_conflicting_values() -> None:
    rows = pl.DataFrame([
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": 50.0},
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": 52.0},
    ])

    with pytest.raises(ValueError, match="缺少可验证的 revision/update"):
        build_ttm_events(
            rows,
            {"net_income_attributable": "net_income_ttm"},
            prefix="income",
        )


def test_build_daily_valuation_persists_pit_ratios(tmp_path) -> None:
    before_q1 = date(2024, 4, 19)
    after_q1 = date(2024, 4, 22)
    _write_enriched(tmp_path, before_q1)
    _write_enriched(tmp_path, after_q1)
    _write_financial(tmp_path, "income", [
        {"symbol": "600000.SH", "period_end": "2023-03-31", "announce_date": "2023-04-20", "net_income_attributable": 10.0, "revenue": 100.0},
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": 50.0, "revenue": 500.0},
        {"symbol": "600000.SH", "period_end": "2024-03-31", "announce_date": "2024-04-22", "net_income_attributable": 15.0, "revenue": 120.0},
    ])
    _write_financial(tmp_path, "balance_sheet", [
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "equity_attributable": 100.0},
        {"symbol": "600000.SH", "period_end": "2024-03-31", "announce_date": "2024-04-22", "equity_attributable": 110.0},
    ])
    _write_financial(tmp_path, "cash_flow", [
        {"symbol": "600000.SH", "period_end": "2023-03-31", "announce_date": "2023-04-20", "net_operating_cash_flow": 20.0},
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_operating_cash_flow": 80.0},
        {"symbol": "600000.SH", "period_end": "2024-03-31", "announce_date": "2024-04-22", "net_operating_cash_flow": 25.0},
    ])

    result = build_daily_valuation(tmp_path)

    assert result == {"rows": 2, "trading_days": 2}
    before = pl.read_parquet(
        tmp_path / "valuation_daily" / "date=2024-04-19" / "part.parquet"
    ).row(0, named=True)
    assert before["market_cap"] == pytest.approx(1_000.0)
    assert before["float_market_cap"] == pytest.approx(600.0)
    assert before["float_share_ratio"] == pytest.approx(0.6)
    assert before["net_income_ttm"] == pytest.approx(50.0)
    assert before["pe_ttm"] == pytest.approx(20.0)
    assert before["pb"] == pytest.approx(10.0)
    assert before["ps_ttm"] == pytest.approx(2.0)
    assert before["pcf_ttm"] == pytest.approx(12.5)

    after = pl.read_parquet(
        tmp_path / "valuation_daily" / "date=2024-04-22" / "part.parquet"
    ).row(0, named=True)
    assert after["income_announce_date"] == after_q1
    assert after["net_income_ttm"] == pytest.approx(55.0)
    assert after["pe_ttm"] == pytest.approx(1_000 / 55)
    assert after["pb"] == pytest.approx(1_000 / 110)
    assert after["ps_ttm"] == pytest.approx(1_000 / 520)
    assert after["pcf_ttm"] == pytest.approx(1_000 / 85)
    metadata = json.loads(
        (tmp_path / "valuation_daily" / "metadata.json").read_text(encoding="utf-8")
    )
    assert metadata["source_snapshots"]["kline_daily_enriched"]["files"] == 2
    assert len(metadata["source_snapshots"]["financials/income"]["sha256"]) == 64


def test_source_snapshot_is_content_addressed_and_order_independent(tmp_path) -> None:
    first = tmp_path / "financials" / "income" / "a.parquet"
    second = tmp_path / "financials" / "income" / "b.parquet"
    first.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(first)
    pl.DataFrame({"value": [2]}).write_parquet(second)

    before = capture_source_snapshot(tmp_path, ["financials/income"])
    second.touch()
    after_touch = capture_source_snapshot(tmp_path, ["financials/income"])
    pl.DataFrame({"value": [3]}).write_parquet(second)
    after_change = capture_source_snapshot(tmp_path, ["financials/income"])

    assert before["financials/income"]["sha256"] == after_touch["financials/income"]["sha256"]
    assert before["financials/income"]["sha256"] != after_change["financials/income"]["sha256"]


def test_source_snapshot_reuses_unchanged_file_hashes(tmp_path, monkeypatch) -> None:
    path = tmp_path / "financials" / "income" / "part.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"value": [1]}).write_parquet(path)
    before = capture_source_snapshot(tmp_path, ["financials/income"])

    def unexpected_open(*_args, **_kwargs):
        raise AssertionError("unchanged source file should use cached content hash")

    monkeypatch.setattr(path.__class__, "open", unexpected_open)
    after = capture_source_snapshot(
        tmp_path,
        ["financials/income"],
        previous=before,
    )

    assert before == after


def test_daily_valuation_does_not_emit_pe_for_loss(tmp_path) -> None:
    day = date(2024, 4, 22)
    _write_enriched(tmp_path, day)
    _write_financial(tmp_path, "income", [
        {"symbol": "600000.SH", "period_end": "2023-12-31", "announce_date": "2024-03-30", "net_income_attributable": -10.0, "revenue": 500.0},
    ])

    build_daily_valuation(tmp_path)

    row = pl.read_parquet(
        tmp_path / "valuation_daily" / "date=2024-04-22" / "part.parquet"
    ).row(0, named=True)
    assert row["pe_ttm"] is None
    assert row["ps_ttm"] == pytest.approx(2.0)


def test_daily_valuation_fails_on_corrupt_financial_table(tmp_path) -> None:
    _write_enriched(tmp_path, date(2024, 4, 22))
    path = tmp_path / "financials" / "income" / "part.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not parquet")

    with pytest.raises(pl.exceptions.ComputeError):
        build_daily_valuation(tmp_path)


def test_load_latest_market_caps_reads_persisted_daily_values(tmp_path) -> None:
    _write_enriched(tmp_path, date(2024, 4, 19), close=10.0)
    _write_enriched(tmp_path, date(2024, 4, 22), close=12.0)
    build_daily_valuation(tmp_path)

    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH"],
        date(2024, 4, 21),
    ) == {"600000.SH": 1_000.0}
    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH"],
        date(2024, 4, 22),
    ) == {"600000.SH": 1_200.0}


def test_load_latest_market_caps_falls_back_for_symbol_missing_from_latest_partition(
    tmp_path,
) -> None:
    directory = tmp_path / "valuation_daily"
    earlier = directory / "date=2024-04-19" / "part.parquet"
    latest = directory / "date=2024-04-22" / "part.parquet"
    earlier.parent.mkdir(parents=True)
    latest.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH", "600001.SH"],
        "date": [date(2024, 4, 19), date(2024, 4, 19)],
        "market_cap": [1_000.0, 2_000.0],
    }).write_parquet(earlier)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2024, 4, 22)],
        "market_cap": [1_200.0],
    }).write_parquet(latest)

    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH", "600001.SH"],
        date(2024, 4, 22),
    ) == {"600000.SH": 1_200.0, "600001.SH": 2_000.0}


def test_load_latest_market_caps_fast_path_scans_only_latest_partition(
    tmp_path,
    monkeypatch,
) -> None:
    directory = tmp_path / "valuation_daily" / "date=2024-04-22"
    directory.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2024, 4, 22)],
        "market_cap": [1_200.0],
    }).write_parquet(directory / "part.parquet")
    scanned: list[str] = []
    original = pl.scan_parquet

    def recording_scan(source, *args, **kwargs):
        scanned.append(str(source))
        return original(source, *args, **kwargs)

    monkeypatch.setattr("app.services.daily_valuation.pl.scan_parquet", recording_scan)

    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH"],
        date(2024, 4, 22),
    ) == {"600000.SH": 1_200.0}
    assert scanned == [str(directory / "*.parquet")]


def test_load_latest_market_caps_advances_cached_snapshot_by_partition(tmp_path) -> None:
    directory = tmp_path / "valuation_daily"
    earlier = directory / "date=2024-04-19" / "part.parquet"
    latest = directory / "date=2024-04-22" / "part.parquet"
    earlier.parent.mkdir(parents=True)
    latest.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH", "600001.SH"],
        "date": [date(2024, 4, 19), date(2024, 4, 19)],
        "market_cap": [1_000.0, 2_000.0],
    }).write_parquet(earlier)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2024, 4, 22)],
        "market_cap": [1_200.0],
    }).write_parquet(latest)

    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH", "600001.SH"],
        date(2024, 4, 19),
    ) == {"600000.SH": 1_000.0, "600001.SH": 2_000.0}
    assert load_latest_market_caps(
        tmp_path,
        ["600000.SH", "600001.SH"],
        date(2024, 4, 22),
    ) == {"600000.SH": 1_200.0, "600001.SH": 2_000.0}


def test_daily_valuation_coverage_requires_previous_and_backtest_dates(tmp_path) -> None:
    for day in (date(2024, 4, 19), date(2024, 4, 22), date(2024, 4, 23)):
        _write_enriched(tmp_path, day)
    build_daily_valuation(tmp_path, [date(2024, 4, 19), date(2024, 4, 22)])

    with pytest.raises(ValueError, match="2024-04-23"):
        assert_daily_valuation_coverage(
            tmp_path,
            date(2024, 4, 22),
            date(2024, 4, 23),
        )

    build_daily_valuation(tmp_path, [date(2024, 4, 23)])
    assert_daily_valuation_coverage(
        tmp_path,
        date(2024, 4, 22),
        date(2024, 4, 23),
    )


def test_full_valuation_rebuild_can_keep_rollback_directory(tmp_path) -> None:
    day = date(2024, 4, 22)
    _write_enriched(tmp_path, day)
    build_daily_valuation(tmp_path)
    original = (
        tmp_path / "valuation_daily" / "date=2024-04-22" / "part.parquet"
    ).read_bytes()
    _write_enriched(tmp_path, day, close=12.0)

    assert build_daily_valuation(tmp_path, keep_backup=True) == {
        "rows": 1,
        "trading_days": 1,
    }

    backups = list(tmp_path.glob(".valuation_daily.pre-rebuild-*"))
    assert len(backups) == 1
    assert (backups[0] / "date=2024-04-22" / "part.parquet").read_bytes() == original
    rebuilt = pl.read_parquet(
        tmp_path / "valuation_daily" / "date=2024-04-22" / "part.parquet"
    )
    assert rebuilt["market_cap"].to_list() == [1_200.0]
