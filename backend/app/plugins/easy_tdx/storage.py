"""EasyTDX 行业快照的扩展表契约与原子替换。"""

from __future__ import annotations

import os
import threading
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl

from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField


INDUSTRY_TABLE = "ext_industry_tdx"
_LOCK = threading.Lock()


def _config() -> ExtConfig:
    return ExtConfig(
        id=INDUSTRY_TABLE,
        label="EasyTDX 行业维度",
        mode="snapshot",
        fields=[
            ExtField("symbol", "string", "标的代码"),
            ExtField("code", "string", "股票代码"),
            ExtField("industry_sw", "string", "申万行业代码"),
            ExtField("industry_tdx", "string", "通达信行业代码"),
            ExtField("source", "string", "数据来源"),
            ExtField("collected_at", "string", "采集时间"),
        ],
        description="EasyTDX tdxhy.cfg 行业代码快照（不含行情、股本、财务与题材）",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def ensure_config(data_dir: Path) -> None:
    store = ExtConfigStore(Path(data_dir))
    if store.get(INDUSTRY_TABLE) is None:
        store.upsert(_config())


def snapshot_path(data_dir: Path) -> Path:
    return Path(data_dir) / "ext_data" / INDUSTRY_TABLE / "part.parquet"


def snapshot_is_fresh(data_dir: Path, max_age: timedelta = timedelta(hours=24)) -> bool:
    path = snapshot_path(data_dir)
    try:
        return path.exists() and time.time() - path.stat().st_mtime <= max_age.total_seconds()
    except OSError:
        return False


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _normalized_rows(rows: list[dict]) -> list[dict[str, str]]:
    normalized: dict[str, dict[str, str]] = {}
    for row in rows:
        code = _text(row.get("code"))
        symbol = _text(row.get("symbol"))
        industry_sw = _text(row.get("industry_sw"))
        industry_tdx = _text(row.get("industry_tdx"))
        if not code or not symbol or not (industry_sw or industry_tdx):
            continue
        normalized[symbol] = {
            "symbol": symbol,
            "code": code,
            "industry_sw": industry_sw,
            "industry_tdx": industry_tdx,
            "source": "easy_tdx",
            "collected_at": _text(row.get("collected_at")),
        }
    return [normalized[symbol] for symbol in sorted(normalized)]


def replace_industry_snapshot(data_dir: Path, rows: list[dict]) -> int:
    """Replace the complete snapshot; an empty result never erases valid data."""
    normalized = _normalized_rows(rows)
    if not normalized:
        return 0
    ensure_config(data_dir)
    path = snapshot_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame({
        key: pl.Series(key, [row[key] for row in normalized], dtype=pl.String)
        for key in (
            "symbol",
            "code",
            "industry_sw",
            "industry_tdx",
            "source",
            "collected_at",
        )
    })

    with _LOCK:
        tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            frame.write_parquet(tmp)
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()
    return len(frame)
