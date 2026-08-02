"""BaoStock index constituent candidate snapshots.

BaoStock exposes dated CSI index constituent snapshots such as
query_hs300_stocks(date=...). Those snapshots are useful candidate evidence,
but they are not membership adjustment events. They must stay out of the strict
PIT interval table until another source supplies auditable in/out dates or an
explicit reconciliation step promotes them.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from app.plugins.pit_history.storage import normalize_symbol
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)

SOURCE = "baostock"
INDEX_CONSTITUENT_CANDIDATES_TABLE = "index_constituent_candidates"
PARSER_VERSION = "baostock_index_candidates_v1"
DEFAULT_INDEX_SYMBOL = "000300.SH"
DEFAULT_INDEX_NAME = "沪深300"

_INDEX_QUERY_METHODS = {
    "000300.SH": "query_hs300_stocks",
}


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null", "<na>"} else text


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _member_code(symbol: str) -> str:
    return symbol.split(".", 1)[0] if "." in symbol else symbol


def _table_root(data_dir: Path, table: str) -> Path:
    return Path(data_dir) / "pit_reference" / SOURCE / table


def partition_path(data_dir: Path, table: str, snapshot_date: date) -> Path:
    return _table_root(data_dir, table) / f"snapshot_date={snapshot_date.isoformat()}" / "part.parquet"


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def publish_candidate_snapshot(data_dir: Path, snapshot_date: date, frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    stored = (
        frame.unique(subset=["index_symbol", "member_symbol", "snapshot_date"], keep="last")
        .sort(["index_symbol", "member_symbol"])
    )
    _atomic_write_parquet(
        stored,
        partition_path(data_dir, INDEX_CONSTITUENT_CANDIDATES_TABLE, snapshot_date),
    )
    return stored.height


def normalize_index_constituent_candidates(
    rows: Iterable[dict[str, Any]],
    *,
    index_symbol: str,
    index_name: str,
    snapshot_date: date,
) -> pl.DataFrame:
    raw_rows = list(rows)
    output: list[dict[str, Any]] = []
    normalized_index = normalize_symbol(index_symbol).upper()
    snapshot_hash = stable_content_hash({
        "dataset": INDEX_CONSTITUENT_CANDIDATES_TABLE,
        "index_symbol": normalized_index,
        "snapshot_date": snapshot_date.isoformat(),
        "rows": raw_rows,
    })
    for row in raw_rows:
        member_symbol = normalize_symbol(
            row.get("member_symbol")
            or row.get("code")
            or row.get("证券代码")
            or row.get("股票代码")
        )
        if not member_symbol:
            continue
        output.append({
            "index_symbol": normalized_index,
            "index_name": _text(index_name),
            "member_symbol": member_symbol,
            "member_code": _member_code(member_symbol),
            "member_name": _text(
                row.get("member_name")
                or row.get("code_name")
                or row.get("证券简称")
                or row.get("股票简称")
                or row.get("name")
            ),
            "snapshot_date": snapshot_date,
            "source_update_date": _parse_date(row.get("date") or row.get("updateDate")),
            "source": SOURCE,
            "provenance": "candidate_snapshot",
            "snapshot_hash": snapshot_hash,
        })
    if not output:
        return pl.DataFrame()
    return pl.DataFrame(output).select([
        pl.col("index_symbol").cast(pl.String),
        pl.col("index_name").cast(pl.String),
        pl.col("member_symbol").cast(pl.String),
        pl.col("member_code").cast(pl.String),
        pl.col("member_name").cast(pl.String),
        pl.col("snapshot_date").cast(pl.Date),
        pl.col("source_update_date").cast(pl.Date),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("snapshot_hash").cast(pl.String),
    ]).unique(
        subset=["index_symbol", "member_symbol", "snapshot_date"],
        keep="last",
    ).sort(["index_symbol", "member_symbol"])


class BaoStockIndexCandidateCollector:
    def __init__(self, data_dir: Path, bs_module: Any | None = None) -> None:
        self.data_dir = Path(data_dir)
        self._bs_module = bs_module

    def collect_hs300_snapshots(
        self,
        snapshot_dates: Iterable[date],
        *,
        index_name: str = DEFAULT_INDEX_NAME,
    ) -> int:
        return self.collect_index_snapshots(
            DEFAULT_INDEX_SYMBOL,
            snapshot_dates=snapshot_dates,
            index_name=index_name,
        )

    def collect_index_snapshots(
        self,
        index_symbol: str,
        *,
        snapshot_dates: Iterable[date],
        index_name: str = "",
    ) -> int:
        normalized_index = normalize_symbol(index_symbol).upper()
        method_name = _INDEX_QUERY_METHODS.get(normalized_index)
        if method_name is None:
            raise ValueError("BaoStock candidate snapshots currently support only 000300.SH")
        dates = sorted(set(snapshot_dates))
        if not dates:
            return 0

        bs = self._baostock()
        login = bs.login()
        if getattr(login, "error_code", "0") != "0":
            raise RuntimeError(f"BaoStock login failed: {getattr(login, 'error_msg', '')}")

        frames: list[pl.DataFrame] = []
        raw_payloads: dict[str, Any] = {}
        try:
            query = getattr(bs, method_name)
            for snapshot_date in dates:
                result = query(date=snapshot_date.isoformat())
                rows = _result_rows(result)
                raw_payloads[snapshot_date.isoformat()] = {
                    "index_symbol": normalized_index,
                    "fields": list(getattr(result, "fields", [])),
                    "rows": rows,
                }
                frame = normalize_index_constituent_candidates(
                    rows,
                    index_symbol=normalized_index,
                    index_name=index_name,
                    snapshot_date=snapshot_date,
                )
                if not frame.is_empty():
                    publish_candidate_snapshot(self.data_dir, snapshot_date, frame)
                    frames.append(frame)
        finally:
            bs.logout()

        logical_snapshot = (
            f"{normalized_index}_{dates[0].isoformat()}"
            if len(dates) == 1
            else f"{normalized_index}_{dates[0].isoformat()}_{dates[-1].isoformat()}"
        )
        _, source_hash = archive_source_payload(
            self.data_dir,
            SOURCE,
            INDEX_CONSTITUENT_CANDIDATES_TABLE,
            logical_snapshot,
            normalized_index,
            raw_payloads,
            parser_version=PARSER_VERSION,
        )
        published_rows = sum(frame.height for frame in frames)
        update_ingestion_manifest(
            self.data_dir,
            SOURCE,
            INDEX_CONSTITUENT_CANDIDATES_TABLE,
            logical_snapshot,
            status="published" if published_rows else "valid_empty",
            parser_version=PARSER_VERSION,
            schema_version=1,
            source_content_hash=source_hash,
            content_hash=stable_content_hash([frame.to_dicts() for frame in frames])
            if published_rows else None,
            published_rows=published_rows,
            empty_reason=None if published_rows else "source_empty",
            provenance="candidate_snapshot",
        )
        return published_rows

    def _baostock(self):
        if self._bs_module is not None:
            return self._bs_module
        import baostock as bs  # noqa: PLC0415

        return bs


def _result_rows(result: Any) -> list[dict[str, str]]:
    if getattr(result, "error_code", "0") != "0":
        raise RuntimeError(
            f"BaoStock index constituents query failed: {getattr(result, 'error_msg', '')}"
        )
    fields = list(getattr(result, "fields", []))
    rows: list[dict[str, str]] = []
    while result.next():
        values = result.get_row_data()
        rows.append({field: values[pos] for pos, field in enumerate(fields)})
    return rows
