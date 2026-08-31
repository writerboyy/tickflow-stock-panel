"""Minute-bar session completeness checks shared by ingestion and backtests."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Iterable

import polars as pl


class MinuteCoverageError(ValueError):
    """Raised when exact minute execution is requested on incomplete data."""


_PRICE_TOLERANCE = 1e-7
_AMOUNT_TOLERANCE = 1e-6


def _minute_range(start: time, end: time) -> set[time]:
    current = datetime.combine(date.min, start)
    finish = datetime.combine(date.min, end)
    values: set[time] = set()
    while current <= finish:
        values.add(current.time())
        current += timedelta(minutes=1)
    return values


_BEIJING_CONTINUOUS_TIMES = _minute_range(time(9, 31), time(11, 30)) | _minute_range(
    time(13, 1), time(15, 0)
)
_BEIJING_AUCTION_TIME = time(9, 30)
_BEIJING_ALLOWED_TIMES = _BEIJING_CONTINUOUS_TIMES | {_BEIJING_AUCTION_TIME}

# TickFlow timestamps are UTC epochs. The repository stores Beijing-naive
# wall-clock timestamps; legacy partitions may still contain UTC-naive values.
_UTC_REFERENCE_DATE = date(2000, 1, 1)
_UTC_CONTINUOUS_TIMES = {
    (datetime.combine(_UTC_REFERENCE_DATE, value) - timedelta(hours=8)).time()
    for value in _BEIJING_CONTINUOUS_TIMES
}
_UTC_AUCTION_TIME = (datetime.combine(_UTC_REFERENCE_DATE, _BEIJING_AUCTION_TIME) - timedelta(hours=8)).time()
_UTC_ALLOWED_TIMES = _UTC_CONTINUOUS_TIMES | {_UTC_AUCTION_TIME}
_BEIJING_TIME_STRINGS = [value.strftime("%H:%M") for value in _BEIJING_ALLOWED_TIMES]
_UTC_TIME_STRINGS = {value.strftime("%H:%M") for value in _UTC_ALLOWED_TIMES}
_BEIJING_CONTINUOUS_STRINGS = {
    value.strftime("%H:%M") for value in _BEIJING_CONTINUOUS_TIMES
}
_UTC_CONTINUOUS_STRINGS = {
    value.strftime("%H:%M") for value in _UTC_CONTINUOUS_TIMES
}


def minute_clock_basis(frame: pl.DataFrame) -> str:
    """Classify naive minute timestamps without silently accepting mixtures."""
    if frame.is_empty() or "datetime" not in frame.columns:
        return "invalid"
    if frame["datetime"].null_count():
        return "invalid"
    times = set(frame.select(pl.col("datetime").dt.strftime("%H:%M").unique()).to_series().to_list())
    if times <= _UTC_TIME_STRINGS:
        return "utc_naive"
    if times <= set(_BEIJING_TIME_STRINGS):
        return "beijing_naive"
    return "mixed_or_invalid"


def minute_frame_is_canonical(frame: pl.DataFrame) -> bool:
    """Return whether a frame uses the repository's Beijing-naive minute clock."""
    return minute_clock_basis(frame) == "beijing_naive"


def sanitize_minute_rows(frame: pl.DataFrame) -> pl.DataFrame:
    """Drop unusable bars and remove only numerical boundary noise."""
    required = ("symbol", "datetime", "open", "high", "low", "close")
    if any(column not in frame.columns for column in required):
        return pl.DataFrame(schema=frame.schema)
    clean = frame.filter(
        pl.all_horizontal(pl.col(column).is_not_null() for column in required)
        & pl.all_horizontal(pl.col(column).is_finite() for column in required[2:])
    )
    if "amount" in clean.columns:
        clean = clean.filter(
            pl.col("amount").is_null()
            | (pl.col("amount").is_finite() & (pl.col("amount") >= -_AMOUNT_TOLERANCE))
        ).with_columns(
            pl.when(pl.col("amount") < 0)
            .then(pl.lit(0.0))
            .otherwise(pl.col("amount"))
            .alias("amount")
        )
    if clean.is_empty():
        return clean
    high_bound = pl.max_horizontal("open", "close")
    low_bound = pl.min_horizontal("open", "close")
    return clean.with_columns(
        pl.when((pl.col("high") - high_bound).abs() <= _PRICE_TOLERANCE)
        .then(high_bound)
        .otherwise(pl.col("high"))
        .alias("high"),
        pl.when((pl.col("low") - low_bound).abs() <= _PRICE_TOLERANCE)
        .then(low_bound)
        .otherwise(pl.col("low"))
        .alias("low"),
    )


def normalize_minute_clock(frame: pl.DataFrame) -> tuple[pl.DataFrame, str, int]:
    """Convert legacy UTC-naive rows to Beijing-naive, including mixed partitions."""
    basis = minute_clock_basis(frame)
    if basis == "beijing_naive":
        return frame, basis, 0
    if basis not in {"utc_naive", "mixed_or_invalid"}:
        return frame, basis, 0
    utc_rows = frame.filter(
        pl.col("datetime").dt.strftime("%H:%M").is_in(_UTC_TIME_STRINGS)
    ).height
    if not utc_rows:
        return frame, basis, 0
    normalized = frame.with_columns(
        pl.when(pl.col("datetime").dt.strftime("%H:%M").is_in(_UTC_TIME_STRINGS))
        .then(pl.col("datetime").dt.offset_by("8h"))
        .otherwise(pl.col("datetime"))
        .alias("datetime")
    )
    return normalized, basis, utc_rows


def minute_group_complete(frame: pl.DataFrame) -> bool:
    required = {"datetime", "open", "high", "low", "close"}
    if frame.is_empty() or not required <= set(frame.columns):
        return False
    valid = frame.filter(
        pl.col("datetime").is_not_null()
        & pl.all_horizontal(
            pl.col(column).is_not_null() & pl.col(column).is_finite() & (pl.col(column) > 0)
            for column in ("open", "high", "low", "close")
        )
        & (pl.col("high") + _PRICE_TOLERANCE >= pl.max_horizontal("open", "close"))
        & (pl.col("low") - _PRICE_TOLERANCE <= pl.min_horizontal("open", "close"))
    )
    if valid.height != frame.height or valid["datetime"].n_unique() != frame.height:
        return False
    valid, _basis, _shifted = normalize_minute_clock(valid)
    if minute_clock_basis(valid) != "beijing_naive":
        return False
    required, allowed = _BEIJING_CONTINUOUS_TIMES, _BEIJING_ALLOWED_TIMES
    time_strings = set(
        valid.select(pl.col("datetime").dt.strftime("%H:%M").unique()).to_series().to_list()
    )
    required_strings = {value.strftime("%H:%M") for value in required}
    allowed_strings = {value.strftime("%H:%M") for value in allowed}
    return required_strings <= time_strings and time_strings <= allowed_strings


def minute_coverage_manifest(frame: pl.DataFrame) -> dict[str, object]:
    groups: list[dict[str, object]] = []
    if not frame.is_empty() and {"symbol", "datetime"} <= set(frame.columns):
        # Keep this aggregation in Polars. The old partition loop converted every
        # symbol's full timestamp column to Python objects, which was prohibitively
        # slow for full-market minute partitions.
        available = set(frame.columns)
        valid_expr = pl.col("datetime").is_not_null()
        for column in ("open", "high", "low", "close"):
            if column not in available:
                valid_expr &= pl.lit(False)
            else:
                valid_expr &= (
                    pl.col(column).is_not_null()
                    & pl.col(column).is_finite()
                    & (pl.col(column) > 0)
                )
        if {"open", "high", "low", "close"} <= available:
            valid_expr &= (
                pl.col("high") + _PRICE_TOLERANCE >= pl.max_horizontal("open", "close")
            ) & (
                pl.col("low") - _PRICE_TOLERANCE <= pl.min_horizontal("open", "close")
            )
        work = frame.with_columns([
            pl.col("datetime").dt.strftime("%H:%M").alias("_time"),
            valid_expr.alias("_valid"),
        ])
        stats = work.group_by("symbol", maintain_order=True).agg([
            pl.len().alias("bars"),
            pl.col("datetime").n_unique().alias("_unique_datetimes"),
            pl.col("_time").n_unique().alias("_unique_times"),
            pl.col("_valid").sum().alias("_valid_rows"),
            pl.col("_time").filter(pl.col("_time").is_in(list(_UTC_CONTINUOUS_STRINGS))).n_unique().alias("_utc_required"),
            pl.col("_time").filter(pl.col("_time").is_in(list(_BEIJING_CONTINUOUS_STRINGS))).n_unique().alias("_beijing_required"),
            pl.col("_time").filter(~pl.col("_time").is_in(list(_UTC_TIME_STRINGS))).n_unique().alias("_utc_bad"),
            pl.col("_time").filter(~pl.col("_time").is_in(set(_BEIJING_TIME_STRINGS))).n_unique().alias("_beijing_bad"),
        ])
        complete = (
            (pl.col("_valid_rows") == pl.col("bars"))
            & (pl.col("_unique_datetimes") == pl.col("bars"))
            & (
                (
                    (pl.col("_utc_bad") == 0)
                    & (pl.col("_utc_required") == len(_UTC_CONTINUOUS_STRINGS))
                )
                | (
                    (pl.col("_beijing_bad") == 0)
                    & (pl.col("_beijing_required") == len(_BEIJING_CONTINUOUS_STRINGS))
                )
            )
            & (pl.col("_unique_times") <= 241)
        )
        groups = [
            {"symbol": str(row["symbol"]), "bars": int(row["bars"]), "complete": bool(row["complete"])}
            for row in stats.with_columns(complete.alias("complete")).select("symbol", "bars", "complete").to_dicts()
        ]
    complete = sum(bool(group["complete"]) for group in groups)
    return {
        "schema_version": 1,
        "expected_continuous_bars": 240,
        "optional_auction_bar": "09:30",
        "symbols": len(groups),
        "complete_symbols": complete,
        "incomplete_symbols": len(groups) - complete,
        "groups": groups,
    }


def assert_required_minute_coverage(
    frame: pl.DataFrame,
    required_keys: Iterable[tuple[str, str]],
) -> None:
    required = set(required_keys)
    by_key: dict[tuple[str, str], pl.DataFrame] = {}
    if not frame.is_empty() and {"symbol", "datetime"} <= set(frame.columns):
        dated = frame.with_columns(pl.col("datetime").dt.strftime("%Y-%m-%d").alias("_date"))
        for group in dated.partition_by(["symbol", "_date"], maintain_order=True):
            by_key[(str(group["symbol"][0]), str(group["_date"][0]))] = group.drop("_date")
    incomplete = sorted(
        key for key in required
        if key not in by_key or not minute_group_complete(by_key[key])
    )
    if incomplete:
        sample = ", ".join(f"{symbol}/{day}" for symbol, day in incomplete[:8])
        raise MinuteCoverageError(
            "分钟精确回测需要完整的 240 根连续竞价数据（可另含 09:30 竞价根）；"
            f"缺失或不完整: {sample}"
        )
