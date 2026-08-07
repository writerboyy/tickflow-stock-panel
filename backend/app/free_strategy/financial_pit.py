"""Fail-closed point-in-time financial record selection."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterable

import polars as pl


class FinancialPitUnavailable(ValueError):
    """Financial PIT records cannot be selected without ambiguity."""


def _date_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    dtype = frame.schema.get(column)
    if dtype == pl.Date:
        return pl.col(column)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date()
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, strict=False)


def select_financial_periods(
    frame: pl.DataFrame,
    *,
    table: str,
    symbols: Iterable[str],
    as_of: date,
    period_count: int = 1,
    required_fields: Iterable[str] = (),
) -> dict[str, list[dict[str, Any]]]:
    normalized = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ))
    if not normalized:
        return {}
    if isinstance(period_count, bool) or period_count <= 0:
        raise ValueError("财务比较报告期数量必须是正整数")
    required_columns = {"symbol", "period_end", "announce_date", *required_fields}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise FinancialPitUnavailable(
            f"{table} 缺少 PIT 字段: {', '.join(missing_columns)}"
        )
    visible = (
        frame
        .filter(pl.col("symbol").is_in(normalized))
        .with_columns([
            _date_expr(frame, "period_end").alias("_period_date"),
            _date_expr(frame, "announce_date").alias("_announce_date"),
        ])
        .filter(
            pl.col("_period_date").is_not_null()
            & pl.col("_announce_date").is_not_null()
            & (pl.col("_announce_date") <= as_of)
        )
    )
    if visible.is_empty():
        return {}
    unique = visible.unique(maintain_order=True)
    keys = ["symbol", "_period_date", "_announce_date"]
    conflicts = unique.group_by(keys).len().filter(pl.col("len") > 1)
    if not conflicts.is_empty():
        samples = [
            f"{row['symbol']}:{row['_period_date']}:{row['_announce_date']}"
            for row in conflicts.head(8).iter_rows(named=True)
        ]
        raise FinancialPitUnavailable(
            f"{table} 同键修订冲突: {', '.join(samples)}"
        )
    latest_revisions = (
        unique
        .sort(["symbol", "_period_date", "_announce_date"])
        .group_by(["symbol", "_period_date"], maintain_order=True)
        .tail(1)
    )
    selected = (
        latest_revisions
        .sort(["symbol", "_period_date"])
        .group_by("symbol", maintain_order=True)
        .tail(period_count)
        .sort(["symbol", "_period_date"], descending=[False, True])
    )
    fields = tuple(required_fields)
    if fields:
        invalid = selected.filter(
            pl.any_horizontal(pl.col(field).is_null() for field in fields)
        )
        if not invalid.is_empty():
            samples = [
                f"{row['symbol']}:{row['_period_date']}"
                for row in invalid.head(8).iter_rows(named=True)
            ]
            raise FinancialPitUnavailable(
                f"{table} 必需字段为空: {', '.join(samples)}"
            )
    result: dict[str, list[dict[str, Any]]] = {}
    for row in selected.iter_rows(named=True):
        result.setdefault(str(row["symbol"]), []).append({
            key: value
            for key, value in row.items()
            if key not in {"_period_date", "_announce_date"}
        })
    return result


def load_financial_periods(
    data_dir: Path,
    table: str,
    symbols: Iterable[str],
    as_of: date,
    *,
    period_count: int = 1,
    required_fields: Iterable[str] = (),
) -> dict[str, list[dict[str, Any]]]:
    from app.services.financial_sync import get_financial_df

    return select_financial_periods(
        get_financial_df(Path(data_dir), table),
        table=table,
        symbols=symbols,
        as_of=as_of,
        period_count=period_count,
        required_fields=required_fields,
    )
