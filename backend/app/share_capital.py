"""按时点解析历史股本。"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl


def load_share_history(data_dir: Path) -> pl.DataFrame:
    """读取本地财务股本表；未同步或损坏时返回空表。"""
    path = data_dir / "financials" / "shares" / "part.parquet"
    if not path.exists():
        return pl.DataFrame()
    try:
        shares = pl.read_parquet(path)
        if not {"symbol", "period_end", "float_shares"} <= set(shares.columns):
            return pl.DataFrame()
        return shares
    except Exception:
        return pl.DataFrame()


def apply_point_in_time_shares(
    rows: pl.DataFrame,
    shares: pl.DataFrame | None,
    *,
    today: date,
) -> pl.DataFrame:
    """为行情行解析当时已公告且已生效的总股本和流通股本。

    ``rows`` 中的股本视为 instruments 快照。快照仅能用于其 ``as_of`` 当日
    及以后；没有 ``as_of`` 时仅允许用于今天。更早日期找不到历史记录时返回
    null，禁止回退当前股本造成未来数据污染。
    """
    share_columns = [
        column for column in ("total_shares", "float_shares")
        if column in rows.columns
    ]
    if rows.is_empty() or not {"symbol", "date"} <= set(rows.columns) or not share_columns:
        return rows

    def as_date_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
        dtype = frame.schema[column]
        if dtype == pl.Utf8:
            return pl.col(column).str.to_date(strict=False)
        return pl.col(column).cast(pl.Date, strict=False)

    history = pl.DataFrame()
    if (
        shares is not None
        and not shares.is_empty()
        and {"symbol", "period_end"} <= set(shares.columns)
    ):
        available_date = as_date_expr(shares, "period_end")
        if "announce_date" in shares.columns:
            available_date = pl.max_horizontal(
                available_date,
                as_date_expr(shares, "announce_date"),
            )
        historical_columns = [
            column for column in share_columns if column in shares.columns
        ]
        history = (
            shares
            .select(
                pl.col("symbol").cast(pl.Utf8),
                available_date.alias("_share_available_date"),
                as_date_expr(shares, "period_end").alias("_share_period_end"),
                *[
                    pl.when(pl.col(column).cast(pl.Float64, strict=False) > 0)
                    .then(pl.col(column).cast(pl.Float64, strict=False))
                    .otherwise(None)
                    .alias(f"_historical_{column}")
                    for column in historical_columns
                ],
            )
            .filter(
                pl.col("symbol").is_not_null()
                & pl.col("_share_available_date").is_not_null()
            )
            .sort(["symbol", "_share_available_date", "_share_period_end"])
            .with_columns(
                pl.col("_share_period_end")
                .cum_max()
                .over("symbol")
                .alias("_latest_effective_period")
            )
            .filter(pl.col("_share_period_end") == pl.col("_latest_effective_period"))
            .drop("_latest_effective_period")
            .unique(subset=["symbol", "_share_available_date"], keep="last")
            .sort(["symbol", "_share_available_date"])
        )

    resolved = (
        rows
        .with_row_index("_share_row_order")
        .with_columns(
            pl.col("symbol").cast(pl.Utf8),
            pl.col("date").cast(pl.Date, strict=False).alias("_share_trade_date"),
        )
    )
    if not history.is_empty():
        resolved = (
            resolved
            .sort(["symbol", "_share_trade_date"])
            .join_asof(
                history,
                left_on="_share_trade_date",
                right_on="_share_available_date",
                by="symbol",
                strategy="backward",
                check_sortedness=False,
            )
        )

    if "_instrument_as_of" in resolved.columns:
        snapshot_safe = (
            pl.col("_instrument_as_of").cast(pl.Date, strict=False).is_not_null()
            & (
                pl.col("_instrument_as_of").cast(pl.Date, strict=False)
                <= pl.col("_share_trade_date")
            )
        )
    else:
        snapshot_safe = pl.col("_share_trade_date") == pl.lit(today)

    expressions: list[pl.Expr] = []
    for column in share_columns:
        historical = f"_historical_{column}"
        history_value = pl.col(historical) if historical in resolved.columns else pl.lit(None)
        expressions.append(
            pl.when(snapshot_safe & (pl.col(column) > 0))
            .then(pl.col(column))
            .otherwise(history_value)
            .cast(pl.Float64, strict=False)
            .alias(column)
        )
    resolved = resolved.with_columns(expressions).sort("_share_row_order")
    cleanup = [
        column for column in resolved.columns
        if column.startswith("_historical_")
        or column in {
            "_share_row_order",
            "_share_trade_date",
            "_share_available_date",
            "_share_period_end",
        }
    ]
    return resolved.drop(cleanup)
