"""Shadow rebuild for Kaipanla dragon-tiger seat details."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
from typing import Any
from uuid import uuid4

import polars as pl

from app.plugins.kaipanla.parsers import parse_dragon_tiger_details
from app.plugins.kaipanla.replay import (
    _archive_date,
    _archive_files,
    _code_context,
    _read_json,
    _unwrap_archive,
)
from app.plugins.kaipanla.storage import (
    LHB_DETAIL_TABLE,
    _lhb_detail_config,
    _normalize_rows,
    _to_frame,
)
from app.services.ext_data import ExtConfig, ExtConfigStore
from app.services.ingestion_manifest import stable_content_hash


_KEY = ("symbol", "side", "log_id")


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _target_config(data_dir: Path) -> ExtConfig:
    expected = _lhb_detail_config()
    current = ExtConfigStore(data_dir).get(LHB_DETAIL_TABLE)
    if current is None:
        return expected
    current.fields = expected.fields
    current.schema_version = expected.schema_version
    current.description = expected.description
    return current


def _source_rows(data_dir: Path) -> tuple[dict[tuple[str, ...], dict[str, Any]], int, int]:
    root = data_dir / "ext_data" / "_kaipanla_raw"
    files = sorted(
        path
        for date_root in root.glob("date=*")
        for path in _archive_files(date_root / "dragon_tiger_details")
    )
    if not files:
        raise FileNotFoundError(f"Kaipanla dragon-tiger archives not found: {root}")
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    parsed_rows = 0
    revisions = 0
    for path in files:
        envelope = _read_json(path)
        payload, _parser_version = _unwrap_archive(envelope)
        partition = _archive_date(path)
        rows = _normalize_rows(
            parse_dragon_tiger_details(payload, _code_context(path)),
            data_dir,
        )
        archive_keys: set[tuple[str, ...]] = set()
        for row in rows:
            parsed_rows += 1
            row["collected_at"] = envelope.get("captured_at")
            key = (partition.isoformat(), *(str(row[field]) for field in _KEY))
            if key in archive_keys:
                raise ValueError(f"dragon-tiger archive contains duplicate log_id: {key}")
            archive_keys.add(key)
            if key in merged:
                revisions += 1
            merged[key] = row
    return merged, len(files), revisions


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def repair_lhb_details(data_dir: Path, *, apply: bool = False) -> dict[str, Any]:
    data_dir = Path(data_dir)
    table_root = data_dir / "ext_data" / LHB_DETAIL_TABLE
    source_root = table_root / "timeseries"
    source_files = sorted(source_root.glob("date=*/part.parquet"))
    source_rows = sum(pl.scan_parquet(path).select(pl.len()).collect().item() for path in source_files)
    rows_by_key, archive_files, revisions = _source_rows(data_dir)
    config = _target_config(data_dir)

    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    shadow_root = table_root / ".repair" / repair_id
    shadow_timeseries = shadow_root / "timeseries"
    buckets: dict[str, list[dict[str, Any]]] = {}
    for key, row in rows_by_key.items():
        buckets.setdefault(key[0], []).append(row)
    for partition, rows in sorted(buckets.items()):
        target = shadow_timeseries / f"date={partition}" / "part.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        _to_frame(sorted(rows, key=lambda row: tuple(str(row[field]) for field in _KEY)), config).write_parquet(target)

    shadow_files = sorted(shadow_timeseries.glob("date=*/part.parquet"))
    rebuilt = pl.read_parquet(shadow_files) if shadow_files else pl.DataFrame()
    if rebuilt.height != len(rows_by_key):
        raise RuntimeError("Kaipanla LHB detail shadow rebuild lost rows")
    if rebuilt.select(_KEY).n_unique() != rebuilt.height:
        raise RuntimeError("Kaipanla LHB detail shadow rebuild contains duplicate keys")
    if set(rebuilt.columns) != {field.name for field in config.fields}:
        raise RuntimeError("Kaipanla LHB detail shadow schema differs from config")

    manifest: dict[str, Any] = {
        "schema_version": 2,
        "repair_id": repair_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "validated",
        "primary_key": list(_KEY),
        "source_files": len(source_files),
        "source_rows": source_rows,
        "archive_files": archive_files,
        "archive_rows": len(rows_by_key),
        "source_revisions": revisions,
        "recovered_rows": len(rows_by_key) - source_rows,
        "published_files": len(shadow_files),
        "published_rows": rebuilt.height,
        "field_contract_hash": stable_content_hash(
            [field.to_dict() for field in config.fields]
        ),
        "published_hashes": {
            path.relative_to(shadow_timeseries).as_posix(): _file_hash(path)
            for path in shadow_files
        },
    }
    _atomic_json(shadow_root / "manifest.json", manifest)
    if not apply:
        return {**manifest, "shadow_path": str(shadow_root)}

    config_path = table_root / "config.json"
    config_backup = table_root / f"config.pre-repair-{repair_id}.json"
    if config_path.exists():
        shutil.copy2(config_path, config_backup)
    backup = table_root / f"timeseries.pre-repair-{repair_id}"
    if source_root.exists():
        os.replace(source_root, backup)
    try:
        os.replace(shadow_timeseries, source_root)
        _atomic_json(config_path, config.to_dict())
    except Exception:
        if source_root.exists():
            os.replace(source_root, shadow_timeseries)
        if backup.exists():
            os.replace(backup, source_root)
        if config_backup.exists():
            _atomic_json(config_path, json.loads(config_backup.read_text(encoding="utf-8")))
        raise

    manifest.update({
        "status": "published",
        "backup_path": str(backup),
        "config_backup_path": str(config_backup) if config_backup.exists() else None,
    })
    _atomic_json(table_root / "repair-manifest.json", manifest)
    shutil.rmtree(shadow_root, ignore_errors=True)
    return manifest
