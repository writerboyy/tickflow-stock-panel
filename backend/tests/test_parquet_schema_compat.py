from datetime import date

import polars as pl

from app.parquet import scan_daily_parquet, scan_enriched_parquet
from app.tickflow.repository import DataStore, KlineRepository


def test_partitioned_daily_scan_tolerates_added_quote_ts(tmp_path):
    old_part = tmp_path / "kline_daily" / "date=2026-07-08" / "part.parquet"
    new_part = tmp_path / "kline_daily" / "date=2026-07-09" / "part.parquet"
    old_part.parent.mkdir(parents=True)
    new_part.parent.mkdir(parents=True)

    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2026, 7, 8)],
        "open": [10.0],
        "high": [10.5],
        "low": [9.8],
        "close": [10.2],
        "volume": [1000.0],
        "amount": [10200.0],
    }).write_parquet(old_part)

    pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2026, 7, 9)],
        "open": [10.2],
        "high": [10.8],
        "low": [10.1],
        "close": [10.6],
        "volume": [1200],
        "amount": [12720.0],
        "quote_ts": [1783560600000],
    }).write_parquet(new_part)

    df = scan_daily_parquet(str(tmp_path / "kline_daily" / "**" / "*.parquet")).sort("date").collect()

    assert df.height == 2
    assert df.schema["volume"] == pl.Float64
    assert df.schema["quote_ts"] == pl.Int64
    assert df["quote_ts"].to_list() == [None, 1783560600000]


def test_partitioned_enriched_scan_tolerates_added_quote_ts(tmp_path):
    base = tmp_path / "kline_daily_enriched"
    old_part = base / "date=2026-07-08" / "part.parquet"
    new_part = base / "date=2026-07-09" / "part.parquet"
    old_part.parent.mkdir(parents=True)
    new_part.parent.mkdir(parents=True)

    common_old = {
        "symbol": ["600000.SH"],
        "date": [date(2026, 7, 8)],
        "open": [10.0],
        "high": [10.5],
        "low": [9.8],
        "close": [10.2],
        "volume": [1000.0],
        "amount": [10200.0],
        "raw_close": [10.2],
        "raw_high": [10.5],
        "raw_low": [9.8],
        "turnover_rate": [1.1],
        "consecutive_limit_ups": pl.Series([0], dtype=pl.UInt32),
        "consecutive_limit_downs": pl.Series([0], dtype=pl.UInt32),
    }
    pl.DataFrame(common_old).write_parquet(old_part)

    common_new = dict(common_old)
    common_new["date"] = [date(2026, 7, 9)]
    common_new["volume"] = [1200]
    common_new["quote_ts"] = [1783560600000]
    pl.DataFrame(common_new).write_parquet(new_part)

    df = scan_enriched_parquet(str(base / "**" / "*.parquet")).sort("date").collect()

    assert df.height == 2
    assert df.schema["volume"] == pl.Float64
    assert df.schema["quote_ts"] == pl.Int64
    assert df["quote_ts"].to_list() == [None, 1783560600000]


def test_etf_daily_batch_tolerates_added_quote_ts(tmp_path):
    store = DataStore(tmp_path)
    base = tmp_path / "kline_etf_enriched"
    old_part = base / "date=2026-07-28" / "part.parquet"
    new_part = base / "date=2026-07-29" / "part.parquet"
    old_part.parent.mkdir(parents=True)
    new_part.parent.mkdir(parents=True)

    common = {
        "symbol": ["159920.SZ"],
        "open": [1.50],
        "high": [1.53],
        "low": [1.49],
        "close": [1.52],
        "volume": [1000.0],
        "amount": [1520.0],
        "raw_close": [1.52],
        "raw_high": [1.53],
        "raw_low": [1.49],
    }
    pl.DataFrame({**common, "date": [date(2026, 7, 28)]}).write_parquet(old_part)
    pl.DataFrame({
        **common,
        "date": [date(2026, 7, 29)],
        "quote_ts": [1785295800000],
    }).write_parquet(new_part)

    frame = KlineRepository(store).get_daily_asset_batch(
        "etf",
        ["159920.SZ"],
        date(2026, 7, 28),
        date(2026, 7, 29),
        ["symbol", "date", "close", "quote_ts"],
    )

    assert frame.height == 2
    assert frame["date"].to_list() == [date(2026, 7, 28), date(2026, 7, 29)]
    assert frame["quote_ts"].to_list() == [None, 1785295800000]


def test_etf_minute_range_tolerates_added_quote_ts(tmp_path):
    store = DataStore(tmp_path)
    base = tmp_path / "kline_etf_minute"
    old_part = base / "date=2026-07-28" / "part.parquet"
    new_part = base / "date=2026-07-29" / "part.parquet"
    old_part.parent.mkdir(parents=True)
    new_part.parent.mkdir(parents=True)

    common = {
        "symbol": ["159920.SZ"],
        "datetime": [date(2026, 7, 28)],
        "open": [1.50],
        "high": [1.53],
        "low": [1.49],
        "close": [1.52],
        "volume": [1000.0],
        "amount": [1520.0],
    }
    pl.DataFrame(common).with_columns(
        pl.col("datetime").cast(pl.Datetime),
    ).write_parquet(old_part)
    pl.DataFrame({
        **common,
        "datetime": [date(2026, 7, 29)],
        "quote_ts": [1785295800000],
    }).with_columns(
        pl.col("datetime").cast(pl.Datetime),
    ).write_parquet(new_part)

    frame = KlineRepository(store).get_minute_range(
        ["159920.SZ"],
        date(2026, 7, 28),
        date(2026, 7, 29),
        asset_type="etf",
    )

    assert frame.height == 2
    assert frame["datetime"].dt.date().to_list() == [date(2026, 7, 28), date(2026, 7, 29)]


def test_stock_daily_queries_read_only_requested_date_partitions(tmp_path, monkeypatch):
    base = tmp_path / "kline_daily_enriched"
    first = base / "date=2026-07-28" / "part.parquet"
    second = base / "date=2026-07-29" / "part.parquet"
    unrelated = base / "date=2026-07-30" / "part.parquet"
    for path in (first, second, unrelated):
        path.parent.mkdir(parents=True)

    common = {
        "symbol": ["600000.SH"],
        "open": [10.0],
        "high": [10.5],
        "low": [9.8],
        "close": [10.2],
        "volume": [1000.0],
        "amount": [10200.0],
        "raw_close": [10.2],
        "raw_high": [10.5],
        "raw_low": [9.8],
    }
    pl.DataFrame({**common, "date": [date(2026, 7, 28)]}).write_parquet(first)
    pl.DataFrame({**common, "date": [date(2026, 7, 29)]}).write_parquet(second)
    unrelated.write_bytes(b"")

    scanned = []
    original_scan = scan_enriched_parquet

    def recording_scan(source, **kwargs):
        scanned.append([str(path) for path in source])
        return original_scan(source, **kwargs)

    monkeypatch.setattr("app.tickflow.repository.scan_enriched_parquet", recording_scan)
    repo = KlineRepository(DataStore(tmp_path))

    symbol_frame = repo._scan_daily_symbol(
        "600000.SH", date(2026, 7, 28), date(2026, 7, 29), None,
    )
    batch_frame = repo._scan_daily_batch(
        ["600000.SH"], date(2026, 7, 28), date(2026, 7, 29), None,
    )

    expected = [str(first), str(second)]
    assert symbol_frame["date"].to_list() == [date(2026, 7, 28), date(2026, 7, 29)]
    assert batch_frame["date"].to_list() == [date(2026, 7, 28), date(2026, 7, 29)]
    assert scanned == [expected, expected]
