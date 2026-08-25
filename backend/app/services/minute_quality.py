"""Minute-bar session completeness checks shared by ingestion and backtests."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Iterable

import polars as pl


class MinuteCoverageError(ValueError):
    """Raised when exact minute execution is requested on incomplete data."""


def _minute_range(start: time, end: time) -> set[time]:
    current = datetime.combine(date.min, start)
    finish = datetime.combine(date.min, end)
    values: set[time] = set()
    while current <= finish:
        values.add(current.time())
        current += timedelta(minutes=1)
    return values


_CONTINUOUS_TIMES = _minute_range(time(9, 31), time(11, 30)) | _minute_range(
    time(13, 1), time(15, 0)
)
_AUCTION_TIME = time(9, 30)
_ALLOWED_TIMES = _CONTINUOUS_TIMES | {_AUCTION_TIME}


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
        & (pl.col("high") >= pl.max_horizontal("open", "close"))
        & (pl.col("low") <= pl.min_horizontal("open", "close"))
    )
    if valid.height != frame.height or valid["datetime"].n_unique() != frame.height:
        return False
    times = {value.time() for value in valid["datetime"].to_list()}
    return _CONTINUOUS_TIMES <= times and times <= _ALLOWED_TIMES


def minute_coverage_manifest(frame: pl.DataFrame) -> dict[str, object]:
    groups: list[dict[str, object]] = []
    if not frame.is_empty() and {"symbol", "datetime"} <= set(frame.columns):
        for group in frame.partition_by("symbol", maintain_order=True):
            symbol = str(group["symbol"][0])
            groups.append({
                "symbol": symbol,
                "bars": group.height,
                "complete": minute_group_complete(group),
            })
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
