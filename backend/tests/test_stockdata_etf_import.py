from __future__ import annotations

from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
import shutil

import polars as pl

from app.services.stockdata_etf_import import (
    StockDataEtfImportConfig,
    _collapse_staged_partition,
    run_stockdata_etf_import,
)


def _minute_times(day: date) -> list[datetime]:
    values = [datetime.combine(day, time(9, 30))]
    current = datetime.combine(day, time(9, 31))
    finish = datetime.combine(day, time(11, 30))
    while current <= finish:
        values.append(current)
        current += timedelta(minutes=1)
    current = datetime.combine(day, time(13, 1))
    finish = datetime.combine(day, time(15, 0))
    while current <= finish:
        values.append(current)
        current += timedelta(minutes=1)
    assert len(values) == 241
    return values


def _write_fixture(
    root: Path,
    *,
    source_daily_close: float = 10.0,
    source_factor: float | None = 1.0,
) -> tuple[Path, Path]:
    source_dir = root / "source"
    data_dir = root / "data"
    minute_root = source_dir / "etf_1min"
    minute_root.mkdir(parents=True)
    import_day = date(2019, 1, 2)
    times = _minute_times(import_day)
    pl.DataFrame({
        "ts_code": ["510300.SH"] * len(times),
        "open": [10.0] * len(times),
        "high": [10.0] * len(times),
        "low": [10.0] * len(times),
        "close": [10.0] * len(times),
        "vol": [0.0, *([1000.0] * (len(times) - 1))],
        "amount": [0.0, *([10_000.0] * (len(times) - 1))],
        "adj_factor": [source_factor] * len(times),
        "trade_date": [datetime.combine(import_day, time())] * len(times),
        "trade_time": times,
    }).write_parquet(minute_root / "510300.SH.parquet")
    shutil.copy2(
        minute_root / "510300.SH.parquet",
        minute_root / "510300.SH(1).parquet",
    )
    pl.DataFrame({
        "open": [10.0, 20.0],
        "high": [10.0, 20.0],
        "low": [10.0, 20.0],
        "close": [source_daily_close, 20.0],
        "pre_close": [10.0, 20.0],
        "change": [0.0, 0.0],
        "pct_chg": [0.0, 0.0],
        "vol": [2400.0, 10.0],
        "amount": [2400.0, 0.2],
        "adj_factor": [source_factor, 2.0],
        "etf_name": ["sample", "sample"],
        "total_share": [None, None],
        "total_size": [None, None],
        "nav": [None, None],
        "exchange": ["SH", "SH"],
        "trade_date": [datetime(2019, 1, 2), datetime(2020, 1, 3)],
        "ts_code": ["510300.SH", "510300.SH"],
    }).write_parquet(source_dir / "etf_daily.parquet")

    instrument = data_dir / "instruments_etf" / "instruments_etf.parquet"
    instrument.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "name": ["sample"],
        "code": ["510300"],
        "asset_type": ["etf"],
    }).write_parquet(instrument)
    current_daily = data_dir / "kline_etf_daily" / "date=2020-01-03" / "part.parquet"
    current_daily.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "date": [date(2020, 1, 3)],
        "open": [20.0],
        "high": [20.0],
        "low": [20.0],
        "close": [20.0],
        "volume": [10.0],
        "amount": [200.0],
    }).write_parquet(current_daily)
    current_minute = data_dir / "kline_etf_minute" / "date=2020-01-03" / "part.parquet"
    current_minute.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "datetime": [datetime(2020, 1, 3, 9, 30)],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "close": [10.0],
        "volume": [1.0],
        "amount": [1000.0],
    }).write_parquet(current_minute)
    factor = data_dir / "adj_factor_etf" / "all.parquet"
    factor.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "trade_date": [date(2020, 1, 4)],
        "ex_factor": [2.0],
    }).write_parquet(factor)
    enriched = data_dir / "kline_etf_enriched" / "date=2020-01-03" / "part.parquet"
    enriched.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "date": [date(2020, 1, 3)],
        "close": [10.0],
    }).write_parquet(enriched)
    return source_dir, data_dir


def _config(source_dir: Path, data_dir: Path, *, publish: bool = False):
    return StockDataEtfImportConfig(
        source_dir=source_dir,
        data_dir=data_dir,
        start=date(2019, 1, 1),
        end=date(2020, 1, 4),
        publish=publish,
        run_id="test-run",
    )


def test_dry_run_scopes_before_current_history_and_ignores_copy(tmp_path):
    source_dir, data_dir = _write_fixture(tmp_path)

    result = run_stockdata_etf_import(_config(source_dir, data_dir))

    audit = result["audit"]
    assert audit["status"] == "ready"
    assert audit["source"]["ignored_duplicate_files"] == ["510300.SH(1).parquet"]
    assert audit["minute"]["source_rows"] == 241
    assert audit["minute"]["publish_rows"] == 241
    assert audit["minute"]["zero_volume_rows"] == 1
    assert audit["daily"]["missing_rows"] == 1
    assert audit["scope"]["minute_end_exclusive"] == date(2020, 1, 3)
    assert result["publish"] == {"status": "dry_run"}
    assert not (data_dir / "backfill_state").exists()


def test_close_mismatch_quarantines_complete_symbol_day(tmp_path):
    source_dir, data_dir = _write_fixture(tmp_path, source_daily_close=9.0)

    audit = run_stockdata_etf_import(_config(source_dir, data_dir))["audit"]

    assert audit["status"] == "ready"
    assert audit["minute"]["quarantined_symbol_days"] == 1
    assert audit["minute"]["quarantined_rows"] == 241
    assert audit["minute"]["publish_rows"] == 0


def test_unresolved_adjustment_factor_blocks_publish(tmp_path):
    source_dir, data_dir = _write_fixture(tmp_path, source_factor=None)

    audit = run_stockdata_etf_import(_config(source_dir, data_dir))["audit"]

    assert audit["status"] == "blocked"
    assert "unresolved adjustment factors" in audit["blockers"][0]


def test_missing_intraday_factor_uses_prior_daily_factor(tmp_path):
    source_dir, data_dir = _write_fixture(tmp_path, source_factor=None)
    daily_path = source_dir / "etf_daily.parquet"
    daily = pl.read_parquet(daily_path)
    prior = daily.head(1).with_columns(
        pl.lit(datetime(2018, 12, 28)).alias("trade_date"),
        pl.lit(1.0).alias("adj_factor"),
    )
    pl.concat([prior, daily], how="vertical_relaxed").write_parquet(daily_path)

    audit = run_stockdata_etf_import(_config(source_dir, data_dir))["audit"]

    assert audit["status"] == "ready"
    assert audit["minute"]["publish_rows"] == 241


def test_publish_adjusts_prices_units_and_rebuilds_enriched(tmp_path):
    source_dir, data_dir = _write_fixture(tmp_path)

    result = run_stockdata_etf_import(_config(source_dir, data_dir, publish=True))

    assert result["publish"]["status"] == "published"
    assert result["publish"]["minute_rows"] == 241
    minute = pl.read_parquet(
        data_dir / "kline_etf_minute" / "date=2019-01-02" / "part.parquet"
    )
    assert minute["close"][0] == 2.5
    assert minute["volume"].to_list()[:2] == [0.0, 10.0]
    assert minute.schema["datetime"] == pl.Datetime("us")
    daily = pl.read_parquet(
        data_dir / "kline_etf_daily" / "date=2019-01-02" / "part.parquet"
    )
    assert daily.select("symbol", "amount").row(0) == ("510300.SH", 2_400_000.0)
    enriched = pl.read_parquet(
        data_dir / "kline_etf_enriched" / "date=2019-01-02" / "part.parquet"
    )
    assert enriched["symbol"].to_list() == ["510300.SH"]
    coverage = json.loads(
        (
            data_dir
            / "kline_etf_minute"
            / "_coverage"
            / "date=2019-01-02.json"
        ).read_text(encoding="utf-8")
    )
    assert coverage["representation"] == "fixed_clock_grid"
    assert coverage["zero_volume_rows"] == 1
    rollback = Path(result["publish"]["rollback_dir"])
    assert (rollback / "kline_etf_enriched" / "date=2020-01-03" / "part.parquet").exists()
    assert (data_dir / ".matrix_generation_etf.json").exists()


def test_parallel_staged_parts_collapse_to_sorted_canonical_file(tmp_path):
    partition = tmp_path / "date=2019-01-02"
    partition.mkdir()
    for index, symbols in enumerate((["B", "A"], ["D", "C"])):
        pl.DataFrame({
            "symbol": symbols,
            "datetime": [datetime(2019, 1, 2, 9, 31)] * 2,
            "close": [1.0, 1.0],
        }).write_parquet(partition / f"part{index}.parquet")

    _collapse_staged_partition(partition)

    assert [path.name for path in partition.glob("*.parquet")] == ["part.parquet"]
    assert pl.read_parquet(partition / "part.parquet")["symbol"].to_list() == [
        "A", "B", "C", "D",
    ]
