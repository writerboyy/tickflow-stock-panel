from datetime import date, datetime

import polars as pl
import pytest

from scripts.repair_minute_clock import _resolve_duplicate_rows
from app.services import kline_sync

from app.services.minute_quality import (
    minute_clock_basis,
    minute_frame_is_canonical,
    normalize_minute_clock,
)


def _frame(times: list[datetime]) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": ["600000.SH"] * len(times),
        "datetime": times,
        "open": [10.0] * len(times),
        "high": [10.0] * len(times),
        "low": [10.0] * len(times),
        "close": [10.0] * len(times),
        "volume": [1.0] * len(times),
        "amount": [10.0] * len(times),
    })


def test_minute_clock_basis_distinguishes_legacy_and_canonical_rows():
    beijing = _frame([datetime(2026, 8, 26, 9, 30)])
    utc = _frame([datetime(2026, 8, 26, 1, 30)])
    mixed = pl.concat([beijing, utc])

    assert minute_clock_basis(beijing) == "beijing_naive"
    assert minute_clock_basis(utc) == "utc_naive"
    assert minute_clock_basis(mixed) == "mixed_or_invalid"


def test_normalize_minute_clock_shifts_legacy_rows_and_dedupes_at_call_site():
    frame = _frame([
        datetime(2026, 8, 26, 9, 30),
        datetime(2026, 8, 26, 1, 30),
    ])

    normalized, basis, shifted = normalize_minute_clock(frame)

    assert basis == "mixed_or_invalid"
    assert shifted == 1
    assert normalized["datetime"].to_list() == [
        datetime(2026, 8, 26, 1, 30),
        datetime(2026, 8, 26, 1, 30),
    ]


def test_minute_frame_is_canonical_accepts_only_utc_naive_clock():
    assert minute_frame_is_canonical(_frame([datetime(2026, 8, 26, 1, 30)]))
    assert not minute_frame_is_canonical(_frame([datetime(2026, 8, 26, 9, 30)]))
    assert not minute_frame_is_canonical(pl.DataFrame())


def test_duplicate_repair_prefers_daily_enriched_close(tmp_path):
    enriched = tmp_path / "kline_daily_enriched/date=2026-08-24/part.parquet"
    enriched.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"], "close": [9.0]}).write_parquet(enriched)
    frame = _frame([datetime(2026, 8, 24, 7, 0)] * 2).with_columns(
        pl.when(pl.arange(0, pl.len()) == 1)
        .then(pl.lit(10.0))
        .otherwise(pl.lit(9.0))
        .alias("close"),
    )

    repaired, removed, conflicts = _resolve_duplicate_rows(
        frame,
        tmp_path,
        "kline_minute",
        date(2026, 8, 24),
    )

    assert repaired.height == 1
    assert repaired["close"].item() == 9.0
    assert removed == 1
    assert conflicts == 1


def test_duplicate_repair_rejects_unexplained_price_conflict(tmp_path):
    frame = _frame([datetime(2026, 8, 24, 7, 0)] * 2).with_columns(
        pl.when(pl.arange(0, pl.len()) == 1)
        .then(pl.lit(10.0))
        .otherwise(pl.lit(9.0))
        .alias("close"),
    )

    with pytest.raises(RuntimeError, match="unexplained conflicting duplicate"):
        _resolve_duplicate_rows(frame, tmp_path, "kline_minute", date(2026, 8, 24))


def test_minute_partition_write_normalizes_legacy_beijing_clock(tmp_path):
    frame = _frame([datetime(2026, 8, 24, 9, 31)])

    assert kline_sync._write_minute_partition(frame, tmp_path / "kline_minute") == 1

    stored = pl.read_parquet(
        tmp_path / "kline_minute/date=2026-08-24/part.parquet"
    )
    assert stored["datetime"].item() == datetime(2026, 8, 24, 1, 31)
    assert minute_frame_is_canonical(stored)
