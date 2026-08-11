from __future__ import annotations

from datetime import date, datetime, time, timedelta
import json
from pathlib import Path

import polars as pl

from app.services.stockdata_stock_import import (
    StockDataStockImportConfig,
    run_stockdata_stock_import,
)


def _minute_times(day: date) -> list[datetime]:
    values = [datetime.combine(day, time(9, 30))]
    current = datetime.combine(day, time(9, 31))
    while current <= datetime.combine(day, time(11, 30)):
        values.append(current)
        current += timedelta(minutes=1)
    current = datetime.combine(day, time(13, 1))
    while current <= datetime.combine(day, time(15, 0)):
        values.append(current)
        current += timedelta(minutes=1)
    assert len(values) == 241
    return values


def _write_fixture(root: Path, *, daily_close: float = 10.0, incomplete: bool = False) -> tuple[Path, Path]:
    source_dir = root / "source"
    data_dir = root / "data"
    day = date(2025, 1, 2)
    times = _minute_times(day)
    if incomplete:
        times.pop(10)
    times.append(datetime.combine(day, time(15, 30)))
    source_path = source_dir / "2025" / "20250102.parquet"
    source_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "code": ["000001.SZ"] * len(times),
        "trade_time": [value.strftime("%Y-%m-%d %H:%M:%S") for value in times],
        "close": [10.0] * len(times),
        "open": [10.0] * len(times),
        "high": [10.0] * len(times),
        "low": [10.0] * len(times),
        "vol": [0.0, *([1000.0] * (len(times) - 1))],
        "amount": [0.0, *([10_000.0] * (len(times) - 1))],
        "date": ["20250102"] * len(times),
    }).write_parquet(source_path)

    for name, symbols in (
        ("instruments", ["000001.SZ"]),
        ("instruments_etf", ["510300.SH"]),
        ("instruments_index", ["000001.SH"]),
    ):
        path = data_dir / name / f"{name}.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame({"symbol": symbols}).write_parquet(path)
    factor = data_dir / "adj_factor" / "all.parquet"
    factor.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "trade_date": [date(2025, 1, 3)],
        "ex_factor": [2.0],
    }).write_parquet(factor)
    daily = data_dir / "kline_daily" / "date=2025-01-02" / "part.parquet"
    daily.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"], "date": [day], "close": [daily_close]}).write_parquet(daily)
    enriched = data_dir / "kline_daily_enriched" / "date=2025-01-02" / "part.parquet"
    enriched.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"], "date": [day], "close": [daily_close / 2]}).write_parquet(enriched)
    existing = data_dir / "kline_minute" / "date=2025-01-02" / "part.parquet"
    existing.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "datetime": [datetime(2025, 1, 2, 9, 30)],
        "open": [4.5], "high": [4.5], "low": [4.5], "close": [4.5],
        "volume": [7.0], "amount": [7000.0],
    }).write_parquet(existing)
    coverage = data_dir / "kline_minute" / "_coverage" / "date=2025-01-02.json"
    coverage.parent.mkdir(parents=True)
    coverage.write_text("{}", encoding="utf-8")
    return source_dir, data_dir


def _config(source_dir: Path, data_dir: Path, *, publish: bool = False) -> StockDataStockImportConfig:
    return StockDataStockImportConfig(
        source_dir=source_dir,
        data_dir=data_dir,
        start=date(2025, 1, 1),
        end=date(2025, 12, 31),
        publish=publish,
        run_id="test-run",
    )


def test_dry_run_audits_units_session_and_existing_priority(tmp_path: Path) -> None:
    source_dir, data_dir = _write_fixture(tmp_path)

    result = run_stockdata_stock_import(_config(source_dir, data_dir))

    year = result["audit"]["years"][0]
    assert year["status"] == "ready"
    assert year["source"]["stock_symbols"] == 1
    assert year["source"]["etf_symbols"] == 0
    assert year["source"]["index_symbols"] == 0
    assert year["minute"]["eligible_rows"] == 241
    assert year["minute"]["rejected_out_of_session_rows"] == 1
    assert year["minute"]["publish_rows"] == 240
    assert year["minute"]["overlap_rows_preserved"] == 1
    assert result["publish"] == {"status": "dry_run"}
    assert not (data_dir / "backfill_state").exists()


def test_publish_adjusts_prices_converts_volume_and_preserves_existing(tmp_path: Path) -> None:
    source_dir, data_dir = _write_fixture(tmp_path)

    result = run_stockdata_stock_import(_config(source_dir, data_dir, publish=True))

    assert result["publish"]["status"] == "published"
    assert result["publish"]["minute_rows"] == 240
    minute = pl.read_parquet(data_dir / "kline_minute" / "date=2025-01-02" / "part.parquet")
    assert minute.height == 241
    assert minute.row(0, named=True)["close"] == 4.5
    assert minute.row(0, named=True)["volume"] == 7.0
    assert minute.row(1, named=True)["close"] == 5.0
    assert minute.row(1, named=True)["volume"] == 10.0
    assert minute.row(1, named=True)["amount"] == 10_000.0
    coverage = json.loads(
        (data_dir / "kline_minute" / "_coverage" / "date=2025-01-02.json").read_text()
    )
    assert coverage["complete_symbols"] == 1
    assert coverage["zero_volume_tradeable"] is False
    year_result = result["publish"]["years"][0]
    rollback = Path(year_result["rollback_dir"])
    assert (rollback / "minute" / "date=2025-01-02" / "part.parquet").exists()
    assert (data_dir / ".matrix_generation_stock.json").exists()


def test_daily_close_mismatch_quarantines_symbol_day(tmp_path: Path) -> None:
    source_dir, data_dir = _write_fixture(tmp_path, daily_close=9.0)

    year = run_stockdata_stock_import(_config(source_dir, data_dir))["audit"]["years"][0]

    assert year["status"] == "ready"
    assert year["minute"]["quarantined_symbol_days"] == 1
    assert year["minute"]["publish_rows"] == 0


def test_incomplete_clock_quarantines_symbol_day(tmp_path: Path) -> None:
    source_dir, data_dir = _write_fixture(tmp_path, incomplete=True)

    year = run_stockdata_stock_import(_config(source_dir, data_dir))["audit"]["years"][0]

    assert year["status"] == "ready"
    assert year["minute"]["quarantine_reasons"]["incomplete_clock"] == 1
    assert year["minute"]["publish_rows"] == 0


def test_invalid_adjustment_symbol_is_quarantined(tmp_path: Path) -> None:
    source_dir, data_dir = _write_fixture(tmp_path)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "trade_date": [date(2025, 1, 3)],
        "ex_factor": [-1.0],
    }).write_parquet(data_dir / "adj_factor" / "all.parquet")

    year = run_stockdata_stock_import(_config(source_dir, data_dir))["audit"]["years"][0]

    assert year["status"] == "ready"
    assert year["adjustment"]["invalid_rows"] == 1
    assert year["minute"]["quarantine_reasons"]["invalid_adjustment_factor"] == 1
    assert year["minute"]["publish_rows"] == 0
