"""Atomic ingestion manifests, checkpoints, and immutable staging payloads."""

from __future__ import annotations

import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Any
from uuid import uuid4


_SAFE_PART = re.compile(r"[^A-Za-z0-9_.=-]+")


def _safe_part(value: object) -> str:
    result = _SAFE_PART.sub("-", str(value)).strip("-.")
    if not result:
        raise ValueError("ingestion manifest path component is empty")
    return result[:120]


def stable_content_hash(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


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


def manifest_path(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
) -> Path:
    return (
        Path(data_dir)
        / "ext_data"
        / "_ingestion"
        / _safe_part(source)
        / _safe_part(dataset)
        / f"{_safe_part(logical_snapshot)}.json"
    )


def load_ingestion_manifest(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
) -> dict[str, Any]:
    path = manifest_path(data_dir, source, dataset, logical_snapshot)
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid ingestion manifest: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"invalid ingestion manifest: {path}")
    return value


def update_ingestion_manifest(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
    **updates: Any,
) -> dict[str, Any]:
    state = load_ingestion_manifest(data_dir, source, dataset, logical_snapshot)
    state.update({
        "source": source,
        "dataset": dataset,
        "logical_snapshot": logical_snapshot,
        **updates,
    })
    _atomic_json(manifest_path(data_dir, source, dataset, logical_snapshot), state)
    return state


def record_ingestion_batch(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
    batch_id: str,
    *,
    status: str,
    row_count: int = 0,
    content_hash: str | None = None,
    source_content_hash: str | None = None,
    empty_reason: str | None = None,
    error_code: str | None = None,
    retry_count: int = 0,
    **run_metadata: Any,
) -> dict[str, Any]:
    state = load_ingestion_manifest(data_dir, source, dataset, logical_snapshot)
    batches = dict(state.get("batches") or {})
    batches[str(batch_id)] = {
        "status": status,
        "row_count": int(row_count),
        "content_hash": content_hash,
        "source_content_hash": source_content_hash,
        "empty_reason": empty_reason,
        "error_code": error_code,
        "retry_count": int(retry_count),
    }
    return update_ingestion_manifest(
        data_dir,
        source,
        dataset,
        logical_snapshot,
        **run_metadata,
        batches=batches,
    )


def write_staging_rows(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
    batch_id: str,
    rows: list[dict[str, Any]],
) -> Path:
    path = (
        Path(data_dir)
        / "ext_data"
        / "_staging"
        / _safe_part(source)
        / _safe_part(dataset)
        / _safe_part(logical_snapshot)
        / f"{_safe_part(batch_id)}.json.gz"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(rows, handle, ensure_ascii=False, sort_keys=True, default=str)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return path


def read_staging_rows(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
) -> list[dict[str, Any]]:
    root = (
        Path(data_dir)
        / "ext_data"
        / "_staging"
        / _safe_part(source)
        / _safe_part(dataset)
        / _safe_part(logical_snapshot)
    )
    rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.json.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
            raise ValueError(f"invalid staging payload: {path.name}")
        rows.extend(payload)
    return rows


def archive_source_payload(
    data_dir: Path,
    source: str,
    dataset: str,
    logical_snapshot: str,
    batch_id: str,
    payload: Any,
    *,
    parser_version: str,
) -> tuple[Path, str]:
    content_hash = stable_content_hash(payload)
    path = (
        Path(data_dir)
        / "ext_data"
        / f"_{_safe_part(source)}_raw"
        / f"snapshot={_safe_part(logical_snapshot)}"
        / _safe_part(dataset)
        / f"{_safe_part(batch_id)}-{content_hash[:16]}.json.gz"
    )
    if path.exists():
        return path, content_hash
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    envelope = {
        "source": source,
        "dataset": dataset,
        "logical_snapshot": logical_snapshot,
        "batch_id": batch_id,
        "parser_version": parser_version,
        "content_hash": content_hash,
        "payload": payload,
    }
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(envelope, handle, ensure_ascii=False, sort_keys=True, default=str)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return path, content_hash
