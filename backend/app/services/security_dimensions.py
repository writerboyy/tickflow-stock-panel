"""策略共用的证券维度快照读取边界。"""

from __future__ import annotations

import logging
from datetime import date
from functools import lru_cache
from typing import Any

import polars as pl

from app.plugins.easy_tdx.storage import INDUSTRY_TABLE


logger = logging.getLogger(__name__)


@lru_cache(maxsize=4)
def _read_instrument_name_changes(
    path: str,
    modified_ns: int,
) -> dict[str, tuple[tuple[date, str, str], ...]]:
    del modified_ns
    frame = pl.read_parquet(
        path,
        columns=["symbol", "change_date", "before_name", "after_name"],
    ).sort(["symbol", "change_date"])
    result: dict[str, list[tuple[date, str, str]]] = {}
    for symbol, change_date, before_name, after_name in frame.iter_rows():
        result.setdefault(str(symbol), []).append((
            change_date,
            str(before_name or ""),
            str(after_name or ""),
        ))
    return {symbol: tuple(values) for symbol, values in result.items()}


def load_instrument_name_changes(
    repo: Any,
) -> dict[str, tuple[tuple[date, str, str], ...]]:
    data_dir = getattr(getattr(repo, "store", None), "data_dir", None)
    if data_dir is None:
        return {}
    path = data_dir / "instrument_name_history" / "part.parquet"
    if not path.exists():
        return {}
    try:
        return _read_instrument_name_changes(str(path), path.stat().st_mtime_ns)
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.debug("股票简称变更快照读取跳过: %s", exc)
        return {}


@lru_cache(maxsize=4)
def _read_industry_dimensions(path: str, modified_ns: int) -> dict[str, dict[str, str]]:
    del modified_ns
    frame = pl.read_parquet(
        path,
        columns=["symbol", "industry_sw", "industry_tdx"],
    )
    return {
        str(symbol): {
            "industry_sw": str(industry_sw or ""),
            "industry_tdx": str(industry_tdx or ""),
        }
        for symbol, industry_sw, industry_tdx in frame.iter_rows()
        if symbol
    }


def load_industry_dimensions(repo: Any) -> dict[str, dict[str, str]]:
    data_dir = getattr(getattr(repo, "store", None), "data_dir", None)
    if data_dir is None:
        return {}
    path = data_dir / "ext_data" / INDUSTRY_TABLE / "part.parquet"
    if not path.exists():
        return {}
    try:
        return _read_industry_dimensions(str(path), path.stat().st_mtime_ns)
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.debug("EasyTDX 行业快照读取跳过: %s", exc)
        return {}
