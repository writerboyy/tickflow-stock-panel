"""Fail-closed QMT Tick import into canonical date partitions."""
from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterable
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl

from app.data_providers.normalizer import TICK_COLS

_REQUIRED = set(TICK_COLS) - {"sequence", "trade_id"}


def _validate_frame(frame: pl.DataFrame, symbol: str, day: date) -> pl.DataFrame:
    if frame is None or frame.is_empty():
        raise ValueError(f"{symbol} {day.isoformat()} QMT Tick 为空")
    missing = sorted(_REQUIRED - set(frame.columns))
    if missing:
        raise ValueError(f"{symbol} {day.isoformat()} Tick 缺少字段: {', '.join(missing)}")
    try:
        selected = frame.filter(
            (pl.col("symbol") == symbol) & (pl.col("datetime").dt.date() == day),
        )
    except Exception as exc:  # noqa: BLE001
        raise ValueError(
            f"{symbol} {day.isoformat()} Tick schema 无法校验: {exc.__class__.__name__}",
        ) from exc
    if selected.is_empty():
        raise ValueError(f"{symbol} {day.isoformat()} QMT Tick 为空")
    try:
        invalid_expression = (
            pl.col("datetime").is_null()
            | pl.col("last_price").is_null()
            | ~pl.col("last_price").is_finite()
            | (pl.col("last_price") <= 0)
            | pl.col("open").is_null()
            | ~pl.col("open").is_finite()
            | (pl.col("open") <= 0)
            | pl.col("high").is_null()
            | ~pl.col("high").is_finite()
            | (pl.col("high") <= 0)
            | pl.col("low").is_null()
            | ~pl.col("low").is_finite()
            | (pl.col("low") <= 0)
            | pl.col("volume").is_null()
            | ~pl.col("volume").is_finite()
            | (pl.col("volume") < 0)
            | pl.col("amount").is_null()
            | ~pl.col("amount").is_finite()
            | (pl.col("amount") < 0)
            | pl.col("source").is_null()
            | (pl.col("source").str.strip_chars() == "")
            | pl.col("source_order").is_null()
            | (pl.col("source_order") < 0)
        )
        for column in ("prev_close", "limit_up", "limit_down"):
            invalid_expression |= pl.col(column).is_not_null() & (
                ~pl.col(column).is_finite() | (pl.col(column) <= 0)
            )
        invalid = selected.filter(invalid_expression)
    except Exception as exc:  # noqa: BLE001
        raise ValueError(
            f"{symbol} {day.isoformat()} Tick schema 无法校验: {exc.__class__.__name__}",
        ) from exc
    if not invalid.is_empty():
        raise ValueError(
            f"{symbol} {day.isoformat()} QMT Tick 包含 {invalid.height} 条无效记录",
        )
    sort_columns = [column for column in (
        "datetime", "symbol", "source_order", "sequence", "trade_id",
    ) if column in selected.columns]
    return selected.sort(sort_columns, maintain_order=True)


def _write_day_partition(
    data_dir: Path,
    symbols: list[str],
    day: date,
    frames: list[pl.DataFrame],
) -> None:
    part = data_dir / "tick" / f"date={day.isoformat()}" / "part.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    if part.exists():
        existing = pl.read_parquet(part)
        if "symbol" not in existing.columns:
            raise ValueError(f"{part} 缺少 symbol 字段，拒绝覆盖")
        existing = existing.filter(~pl.col("symbol").is_in(symbols))
        combined = pl.concat([existing, *frames], how="diagonal_relaxed")
    else:
        combined = pl.concat(frames, how="diagonal_relaxed")
    columns = [column for column in TICK_COLS if column in combined.columns]
    extras = [column for column in combined.columns if column not in columns]
    combined = combined.select([*columns, *extras])
    sort_columns = [column for column in (
        "datetime", "symbol", "source_order", "sequence", "trade_id",
    ) if column in combined.columns]
    combined = combined.sort(sort_columns, maintain_order=True)
    temporary = part.with_name(f".{part.name}.{uuid.uuid4().hex}.tmp")
    try:
        combined.write_parquet(temporary)
        os.replace(temporary, part)
    finally:
        temporary.unlink(missing_ok=True)


def import_qmt_ticks(
    provider: Any,
    data_dir: Path,
    symbols: Iterable[str],
    start: date,
    end: date,
    *,
    on_progress: Callable[[str, date, int], None] | None = None,
) -> dict[str, Any]:
    if end < start:
        raise ValueError("结束日期不能早于开始日期")
    normalized_symbols = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ))
    if not normalized_symbols:
        raise ValueError("至少需要一个股票代码")
    if not bool(getattr(getattr(provider, "capabilities", None), "tick", False)):
        raise ValueError("当前 provider 未声明 Tick 能力")
    trading_dates = provider.get_trading_dates(start, end)
    if not trading_dates:
        raise ValueError(f"{start.isoformat()} 至 {end.isoformat()} 没有 QMT 交易日")
    imported_rows = 0
    partitions: set[date] = set()
    for day in trading_dates:
        begin = datetime.combine(day, time.min)
        finish = datetime.combine(day, time.max)
        day_frames: list[pl.DataFrame] = []
        day_counts: list[tuple[str, int]] = []
        for symbol in normalized_symbols:
            frame = provider.get_tick([symbol], begin, finish, "stock")
            selected = _validate_frame(frame, symbol, day)
            day_frames.append(selected)
            day_counts.append((symbol, selected.height))
            imported_rows += selected.height
        _write_day_partition(
            Path(data_dir), normalized_symbols, day, day_frames,
        )
        partitions.add(day)
        if on_progress is not None:
            for symbol, row_count in day_counts:
                on_progress(symbol, day, row_count)
    return {
        "source": str(getattr(provider, "name", "qmt")),
        "symbols": normalized_symbols,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "trading_dates": [day.isoformat() for day in trading_dates],
        "partitions": len(partitions),
        "rows": imported_rows,
    }
