"""扶摇集合竞价扩展表：配置、标准化和原子发布。"""

from __future__ import annotations

import os
from datetime import date
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import polars as pl

from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField
from app.services.ingestion_manifest import archive_source_payload, update_ingestion_manifest


TABLE_ID = "ext_fuyao_auction"
SOURCE = "fuyao"
PARSER_VERSION = "fuyao_auction_v1"

_FIELDS = [
    ExtField("symbol", "string", "标的代码"),
    ExtField("code", "string", "股票代码"),
    ExtField("name", "string", "股票简称"),
    ExtField("checkpoint", "string", "采集时点"),
    ExtField("stage", "string", "接口阶段"),
    ExtField("collected_at", "string", "采集时间"),
    ExtField("server_timestamp", "int", "服务端时间戳"),
    ExtField("auction_phase", "string", "竞价阶段"),
    ExtField("data_status", "string", "数据状态"),
    ExtField("auction_price", "float", "竞价价格"),
    ExtField("auction_pct", "float", "竞价涨幅（%）"),
    ExtField("auction_volume", "float", "竞价成交量"),
    ExtField("auction_amount", "float", "竞价成交额（元）"),
    ExtField("auction_unmatched", "float", "竞价未匹配量"),
    ExtField("auction_turnover_pct", "float", "竞价换手率（%）"),
    ExtField("auction_yesterday_ratio_pct", "float", "较昨日竞价涨幅（%）"),
    ExtField("auction_volume_ratio", "float", "竞价量比"),
    ExtField("pre_close_price", "float", "昨收价"),
    ExtField("open_price", "float", "开盘价"),
    ExtField("last_price", "float", "最新价"),
    ExtField("float_market_cap", "float", "流通市值（元）"),
    ExtField("source", "string", "数据来源"),
]


def config() -> ExtConfig:
    return ExtConfig(
        id=TABLE_ID,
        label="扶摇集合竞价",
        mode="timeseries",
        fields=_FIELDS,
        description="扶摇/同花顺集合竞价批量快照（09:15/09:20/09:24:57/09:25 及收盘竞价）",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
        primary_key=["symbol", "checkpoint"],
        logical_date="trade_date",
        units={
            "auction_pct": "percent_points",
            "auction_turnover_pct": "percent_points",
            "auction_yesterday_ratio_pct": "percent_points",
            "auction_volume": "shares",
            "auction_amount": "CNY",
            "float_market_cap": "CNY",
        },
    )


def ensure_config(data_dir: Path) -> ExtConfig:
    store = ExtConfigStore(Path(data_dir))
    expected = config()
    current = store.get(TABLE_ID)
    if current is None:
        store.upsert(expected)
        return expected
    return current


def partition_path(data_dir: Path, trade_date: date) -> Path:
    return Path(data_dir) / "ext_data" / TABLE_ID / "timeseries" / f"date={trade_date.isoformat()}" / "part.parquet"


def _as_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_rows(
    rows: list[dict],
    *,
    checkpoint: str,
    stage: str,
    collected_at: str,
    server_timestamp: int | None,
    auction_phase: str | None,
    data_status: str | None,
) -> list[dict]:
    normalized: list[dict] = []
    for row in rows:
        symbol = str(row.get("thscode") or row.get("symbol") or "").strip().upper()
        if not symbol or "." not in symbol:
            continue
        code = str(row.get("ticker") or symbol.split(".", 1)[0]).strip()
        normalized.append({
            "symbol": symbol,
            "code": code,
            "name": row.get("name"),
            "checkpoint": checkpoint,
            "stage": stage,
            "collected_at": collected_at,
            "server_timestamp": server_timestamp,
            "auction_phase": auction_phase,
            "data_status": data_status or "ready",
            "auction_price": _as_float(row.get("auction_price")),
            "auction_pct": _as_float(row.get("auction_pct")),
            "auction_volume": _as_float(row.get("auction_volume")),
            "auction_amount": _as_float(row.get("auction_amount")),
            "auction_unmatched": _as_float(row.get("auction_unmatched")),
            "auction_turnover_pct": _as_float(row.get("auction_turnover_pct")),
            "auction_yesterday_ratio_pct": _as_float(row.get("auction_yesterday_ratio_pct")),
            "auction_volume_ratio": _as_float(row.get("auction_volume_ratio")),
            "pre_close_price": _as_float(row.get("pre_close_price")),
            "open_price": _as_float(row.get("open_price")),
            "last_price": _as_float(row.get("last_price")),
            "float_market_cap": _as_float(row.get("float_market_cap")),
            "source": SOURCE,
        })
    return normalized


def publish(
    data_dir: Path,
    trade_date: date,
    rows: list[dict],
    *,
    checkpoint: str,
    stage: str,
    payload: dict,
    status: str = "published",
    empty_reason: str | None = None,
) -> int:
    """按 symbol+checkpoint 幂等合并，并用临时文件原子替换。"""
    ensure_config(data_dir)
    path = partition_path(data_dir, trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    incoming = normalize_rows(
        rows,
        checkpoint=checkpoint,
        stage=stage,
        collected_at=str(payload.get("collected_at") or ""),
        server_timestamp=payload.get("timestamp"),
        auction_phase=payload.get("auction_phase"),
        data_status=payload.get("data_status"),
    )
    existing = pl.read_parquet(path).to_dicts() if path.exists() else []
    merged = {
        (str(item.get("symbol")), str(item.get("checkpoint"))): item
        for item in existing
        if item.get("symbol") and item.get("checkpoint")
    }
    for item in incoming:
        key = (item["symbol"], item["checkpoint"])
        current = dict(merged.get(key, {}))
        current.update({key: value for key, value in item.items() if value is not None})
        merged[key] = current
    frame = pl.DataFrame(
        [{field.name: item.get(field.name) for field in _FIELDS} for item in merged.values()],
        schema={
            field.name: {
                "string": pl.String,
                "int": pl.Int64,
                "float": pl.Float64,
            }[field.dtype]
            for field in _FIELDS
        },
    ).sort(["checkpoint", "symbol"]) if merged else pl.DataFrame(schema={field.name: pl.String for field in _FIELDS})
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()

    logical = trade_date.isoformat()
    _, source_hash = archive_source_payload(
        data_dir,
        SOURCE,
        TABLE_ID,
        logical,
        checkpoint,
        payload,
        parser_version=PARSER_VERSION,
    )
    update_ingestion_manifest(
        data_dir,
        SOURCE,
        TABLE_ID,
        logical,
        status=status,
        parser_version=PARSER_VERSION,
        source_content_hash=source_hash,
        published_rows=frame.height,
        incoming_rows=len(incoming),
        empty_reason=empty_reason,
        checkpoint=checkpoint,
        stage=stage,
        data_status=payload.get("data_status"),
        auction_phase=payload.get("auction_phase"),
        server_timestamp=payload.get("timestamp"),
        published_hash=sha256(path.read_bytes()).hexdigest(),
    )
    return len(incoming)


def read_status(data_dir: Path, trade_date: date) -> dict:
    path = partition_path(data_dir, trade_date)
    if not path.exists():
        return {"rows": 0, "symbols": 0, "checkpoints": [], "latest_collected_at": None}
    try:
        frame = pl.read_parquet(path, columns=["symbol", "checkpoint", "collected_at"])
    except Exception:
        return {"rows": 0, "symbols": 0, "checkpoints": [], "latest_collected_at": None}
    return {
        "rows": frame.height,
        "symbols": frame["symbol"].n_unique() if "symbol" in frame.columns else 0,
        "checkpoints": sorted(frame["checkpoint"].drop_nulls().unique().to_list()) if "checkpoint" in frame.columns else [],
        "latest_collected_at": frame["collected_at"].max() if "collected_at" in frame.columns else None,
    }
