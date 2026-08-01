"""Normalize authoritative stock XDXR cash-dividend events for strategy replay."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl


DIVIDEND_PATH = Path("corporate_actions") / "stock_dividends.parquet"
RECORD_DATE_DIVIDEND_PATH = Path("ext_data") / "ext_tdx_dividend_history" / "timeseries"
_REQUIRED_COLUMNS = {"instrument_id", "event_date", "c1", "record_hex"}


def dividend_path(data_dir: Path) -> Path:
    return data_dir / DIVIDEND_PATH


def normalize_xdxr_cash_dividends(events: pl.DataFrame) -> pl.DataFrame:
    """Map TDX category-1 XDXR records to per-share cash events.

    TDX ``c1`` is the cash distribution per ten shares.  An event may also
    carry rights/split values in the remaining slots, which are intentionally
    left to the existing split handling path.
    """
    if events.is_empty() or not _REQUIRED_COLUMNS <= set(events.columns):
        return pl.DataFrame(schema={
            "symbol": pl.String,
            "event_date": pl.Date,
            "cash_per_share": pl.Float64,
            "source_record": pl.String,
        })
    return (
        events
        .select(
            pl.col("instrument_id").cast(pl.String).alias("symbol"),
            pl.col("event_date").cast(pl.String).str.to_date("%Y%m%d", strict=False),
            (pl.col("c1").cast(pl.Float64, strict=False) / 10).alias("cash_per_share"),
            pl.col("record_hex").cast(pl.String).alias("source_record"),
        )
        .filter(
            pl.col("symbol").is_not_null()
            & pl.col("event_date").is_not_null()
            & pl.col("cash_per_share").is_finite()
            & (pl.col("cash_per_share") > 0)
        )
        .unique(subset=["symbol", "event_date", "source_record"])
        .sort(["symbol", "event_date", "source_record"])
    )


def import_xdxr_cash_dividends(source: Path, data_dir: Path) -> int:
    events = normalize_xdxr_cash_dividends(pl.read_parquet(source))
    target = dividend_path(data_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{target.name}.tmp")
    events.write_parquet(temporary)
    temporary.replace(target)
    return events.height


def load_cash_dividends(data_dir: Path) -> dict[tuple[str, date], float]:
    target = dividend_path(data_dir)
    if not target.exists():
        return {}
    try:
        frame = pl.read_parquet(target)
    except Exception:
        return {}
    required = {"symbol", "event_date", "cash_per_share"}
    if frame.is_empty() or not required <= set(frame.columns):
        return {}
    result: dict[tuple[str, date], float] = {}
    for symbol, day, cash in frame.select("symbol", "event_date", "cash_per_share").iter_rows():
        if isinstance(day, date) and cash is not None:
            result[(str(symbol), day)] = result.get((str(symbol), day), 0.0) + float(cash)
    return result


def load_record_date_cash_dividends(
    data_dir: Path,
    as_of: date | None = None,
) -> dict[tuple[str, date], float]:
    """Load implemented TDX F10 cash dividends keyed by record date.

    ext_tdx_dividend_history is reference context, not the event-date
    corporate-action replay table. as_of prevents backtests from seeing
    announced future registration dates.
    """
    directory = data_dir / RECORD_DATE_DIVIDEND_PATH
    if not directory.exists():
        return {}
    try:
        frame = pl.read_parquet(str(directory / "**" / "*.parquet"), glob=True)
    except Exception:
        return {}
    required = {"symbol", "record_date", "cash_per_share", "progress_code"}
    if frame.is_empty() or not required <= set(frame.columns):
        return {}
    result: dict[tuple[str, date], float] = {}
    for symbol, day, cash, progress_code in frame.select(
        "symbol", "record_date", "cash_per_share", "progress_code"
    ).iter_rows():
        if isinstance(day, str):
            try:
                day = date.fromisoformat(day)
            except ValueError:
                continue
        if (
            isinstance(day, date)
            and cash is not None
            and str(progress_code) == "036003"
            and (as_of is None or day <= as_of)
        ):
            result[(str(symbol), day)] = result.get((str(symbol), day), 0.0) + float(cash)
    return result
