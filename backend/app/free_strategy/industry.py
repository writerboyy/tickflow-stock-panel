"""Point-in-time industry membership access for free strategies."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterable

import polars as pl


class IndustryHistoryUnavailable(ValueError):
    """The requested PIT industry classification cannot be proven."""


def load_industry_history(
    data_dir: Path,
    symbols: Iterable[str],
    as_of: date,
    standard: str,
    level: str | int | None = None,
    *,
    allow_missing: bool = False,
) -> dict[str, dict[str, Any]]:
    normalized = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ))
    if not normalized:
        return {}
    classification = str(standard).strip()
    if not classification:
        raise IndustryHistoryUnavailable("PIT 行业分类标准不能为空")
    path = (
        Path(data_dir)
        / "pit_reference"
        / "history"
        / "industry_membership_history"
        / "part.parquet"
    )
    if not path.exists():
        raise IndustryHistoryUnavailable(f"TickFlow PIT 行业历史不存在: {path}")
    frame = pl.read_parquet(path)
    required = {
        "member_symbol",
        "industry_standard",
        "industry_code",
        "industry_name",
        "effective_from",
        "effective_to",
    }
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise IndustryHistoryUnavailable(
            f"TickFlow PIT 行业历史缺少字段: {', '.join(missing_columns)}"
        )
    if level is not None and "industry_level" not in frame.columns:
        raise IndustryHistoryUnavailable("TickFlow PIT 行业历史未声明 industry_level，不能推断层级")
    selected = frame.filter(
        pl.col("member_symbol").is_in(normalized)
        & (pl.col("industry_standard") == classification)
        & (pl.col("effective_from") <= as_of)
        & (pl.col("effective_to").is_null() | (pl.col("effective_to") > as_of))
    )
    if level is not None:
        selected = selected.filter(pl.col("industry_level").cast(pl.Utf8) == str(level))
    counts = selected.group_by("member_symbol").len()
    overlaps = sorted(
        str(value)
        for value in counts.filter(pl.col("len") > 1)["member_symbol"].to_list()
    )
    if overlaps:
        raise IndustryHistoryUnavailable(
            f"PIT 行业区间重叠 ({classification}, {as_of.isoformat()}): "
            f"{', '.join(overlaps[:8])}"
        )
    found = set(selected["member_symbol"].to_list())
    gaps = [symbol for symbol in normalized if symbol not in found]
    if gaps and not allow_missing:
        raise IndustryHistoryUnavailable(
            f"PIT 行业区间缺口 ({classification}, {as_of.isoformat()}): "
            f"{', '.join(gaps[:8])}"
        )
    return {
        str(row["member_symbol"]): dict(row)
        for row in selected.iter_rows(named=True)
    }
