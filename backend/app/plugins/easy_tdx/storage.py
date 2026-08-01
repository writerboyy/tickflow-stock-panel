"""EasyTDX 行业快照的扩展表契约与原子替换。"""

from __future__ import annotations

import os
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from uuid import uuid4

import polars as pl

from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField


INDUSTRY_TABLE = "ext_industry_tdx"
MARGIN_TABLE = "ext_tdx_margin"
FORECAST_TABLE = "ext_tdx_forecast"
EXPRESS_TABLE = "ext_tdx_express"
DIVIDEND_HISTORY_TABLE = "ext_tdx_dividend_history"
_LOCK = threading.Lock()
_LOCKS: dict[Path, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()
_DTYPES = {"string": pl.String, "float": pl.Float64}


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


def _reference_configs() -> list[ExtConfig]:
    base = [
        ExtField("symbol", "string", "标的代码"),
        ExtField("code", "string", "股票代码"),
        ExtField("name", "string", "股票简称"),
        ExtField("report_date", "string", "数据日期"),
        ExtField("collected_at", "string", "采集时间"),
    ]
    return [
        ExtConfig(
            id=MARGIN_TABLE,
            label="EasyTDX 融资融券",
            mode="timeseries",
            fields=base + [
                ExtField("margin_balance_10k", "float", "融资余额（万元）"),
                ExtField("margin_purchase_10k", "float", "融资买入额（万元）"),
                ExtField("short_balance_10k", "float", "融券余额（万元）"),
                ExtField("short_sell_10k_shares", "float", "融券卖出量（万股）"),
                ExtField("margin_short_balance_10k", "float", "两融余额（万元）"),
            ],
            description="EasyTDX F10 融资融券日度表",
            symbol_map={"type": "mapped", "col": "symbol"},
            code_map={"type": "mapped", "col": "code"},
        ),
        ExtConfig(
            id=FORECAST_TABLE,
            label="EasyTDX 业绩预告",
            mode="timeseries",
            fields=base + [
                ExtField("announcement_date", "string", "公告日"),
                ExtField("report_period", "string", "报告期"),
                ExtField("forecast_type", "string", "预告类型"),
                ExtField("net_profit_low_10k", "float", "归母净利润下限（万元）"),
                ExtField("net_profit_high_10k", "float", "归母净利润上限（万元）"),
                ExtField("net_profit_yoy_low_pct", "float", "归母净利润同比下限（%）"),
                ExtField("net_profit_yoy_high_pct", "float", "归母净利润同比上限（%）"),
                ExtField("summary", "string", "原始预告摘要"),
            ],
            description="EasyTDX F10 正式业绩预告栏目",
            symbol_map={"type": "mapped", "col": "symbol"},
            code_map={"type": "mapped", "col": "code"},
        ),
        ExtConfig(
            id=EXPRESS_TABLE,
            label="EasyTDX 业绩快报",
            mode="timeseries",
            fields=base + [
                ExtField("announcement_date", "string", "公告日"),
                ExtField("summary", "string", "原始快报摘要"),
            ],
            description="EasyTDX F10 正式业绩快报栏目；未匹配数值格式时只保留原始摘要",
            symbol_map={"type": "mapped", "col": "symbol"},
            code_map={"type": "mapped", "col": "code"},
        ),
        ExtConfig(
            id=DIVIDEND_HISTORY_TABLE,
            label="EasyTDX 分红历史",
            mode="timeseries",
            fields=base + [
                ExtField("record_date", "string", "股权登记日"),
                ExtField("ex_dividend_date", "string", "除权派息日"),
                ExtField("board_date", "string", "董事会日期"),
                ExtField("plan", "string", "通达信原始分红方案"),
                ExtField("cash_per_share", "float", "每股税前现金分红（元）"),
                ExtField("progress", "string", "方案进度"),
                ExtField("progress_code", "string", "通达信方案进度码"),
                ExtField("source", "string", "数据来源"),
            ],
            description="EasyTDX 通达信 7615 F10 已实施现金分红历史，按股权登记日分区",
            symbol_map={"type": "mapped", "col": "symbol"},
            code_map={"type": "mapped", "col": "code"},
        ),
    ]


def ensure_config(data_dir: Path) -> None:
    store = ExtConfigStore(Path(data_dir))
    for config in [_config(), *_reference_configs()]:
        if store.get(config.id) is None:
            store.upsert(config)


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


def _partition_path(data_dir: Path, table_id: str, value: date) -> Path:
    return Path(data_dir) / "ext_data" / table_id / "timeseries" / f"date={value}" / "part.parquet"


def _path_lock(path: Path) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(path, threading.Lock())


def upsert_records(data_dir: Path, table_id: str, rows: list[dict], key_fields: tuple[str, ...]) -> int:
    if not rows:
        return 0
    ensure_config(data_dir)
    config = ExtConfigStore(Path(data_dir)).get(table_id)
    if config is None:
        raise ValueError(f"未知 EasyTDX 扩展表: {table_id}")
    buckets: dict[date, list[dict]] = {}
    for row in rows:
        value = date.fromisoformat(str(row["report_date"]))
        if any(row.get(field) in (None, "") for field in key_fields):
            raise ValueError(f"{table_id} 缺少主键字段")
        buckets.setdefault(value, []).append(row)
    count = 0
    for value, incoming in buckets.items():
        path = _partition_path(data_dir, table_id, value)
        path.parent.mkdir(parents=True, exist_ok=True)
        with _path_lock(path):
            existing = pl.read_parquet(path).to_dicts() if path.exists() else []
            key = lambda row: tuple(str(row[field]) for field in key_fields)
            merged = {key(row): row for row in existing if all(row.get(field) not in (None, "") for field in key_fields)}
            for row in incoming:
                current = dict(merged.get(key(row), {}))
                current.update({field: field_value for field, field_value in row.items() if field_value is not None})
                merged[key(row)] = current
            frame = pl.DataFrame({
                field.name: pl.Series(field.name, [row.get(field.name) for row in merged.values()], dtype=_DTYPES[field.dtype], strict=False)
                for field in config.fields
            })
            tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
            try:
                frame.write_parquet(tmp)
                os.replace(tmp, path)
            finally:
                if tmp.exists():
                    tmp.unlink()
        count += len(incoming)
    return count
