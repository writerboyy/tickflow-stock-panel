"""Repair local financial parquet tables without inventing PIT revisions."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl


FINANCIAL_REPAIR_TABLES = (
    "metrics",
    "income",
    "balance_sheet",
    "cash_flow",
    "shares",
)
_KEYS = ("symbol", "period_end", "announce_date")


def _read_table(data_dir: Path, table: str) -> tuple[Path, pl.DataFrame | None]:
    path = data_dir / "financials" / table / "part.parquet"
    if not path.exists():
        return path, None
    return path, pl.read_parquet(path)


def _validate_columns(frame: pl.DataFrame, table: str) -> None:
    missing = sorted(set(_KEYS) - set(frame.columns))
    if missing:
        raise ValueError(f"financials/{table} 缺少 PIT 键列: {missing}")


def _conflict_report(frame: pl.DataFrame, *, limit: int = 8) -> tuple[int, list[dict[str, str]]]:
    if frame.is_empty():
        return 0, []
    deduped = frame.unique(maintain_order=True)
    groups = (
        deduped
        .group_by(list(_KEYS))
        .agg(pl.len().alias("_unique_rows"))
        .filter(pl.col("_unique_rows") > 1)
        .sort(list(_KEYS))
    )
    samples: list[dict[str, str]] = []
    for row in groups.head(limit).iter_rows(named=True):
        samples.append({column: str(row[column]) for column in _KEYS})
    return groups.height, samples


def _duplicate_key_groups(frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    return (
        frame
        .group_by(list(_KEYS))
        .agg(pl.len().alias("_rows"))
        .filter(pl.col("_rows") > 1)
        .height
    )


def _plan_table(data_dir: Path, table: str) -> dict[str, Any]:
    path, frame = _read_table(data_dir, table)
    if frame is None:
        return {
            "table": table,
            "path": str(path),
            "status": "missing",
            "original_rows": 0,
            "repaired_rows": 0,
            "removed_exact_duplicate_rows": 0,
            "duplicate_key_groups": 0,
            "conflicting_key_groups": 0,
            "conflict_samples": [],
        }

    _validate_columns(frame, table)
    repaired = frame.unique(maintain_order=True)
    conflict_groups, conflict_samples = _conflict_report(frame)
    removed = frame.height - repaired.height
    if conflict_groups:
        status = "repairable_with_unresolved_conflicts" if removed else "blocked"
    else:
        status = "repairable" if removed else "clean"
    return {
        "table": table,
        "path": str(path),
        "status": status,
        "original_rows": frame.height,
        "repaired_rows": repaired.height,
        "removed_exact_duplicate_rows": removed,
        "duplicate_key_groups": _duplicate_key_groups(frame),
        "conflicting_key_groups": conflict_groups,
        "conflict_samples": conflict_samples,
        "_frame": repaired,
    }


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_repaired_table(path: Path, frame: pl.DataFrame, repair_id: str) -> Path:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    backup = path.with_name(f".{path.stem}.pre-financial-repair-{repair_id}{path.suffix}")
    try:
        frame.write_parquet(temporary)
        os.replace(path, backup)
        os.replace(temporary, path)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        if backup.exists() and not path.exists():
            os.replace(backup, path)
        raise
    return backup


def repair_financial_tables(
    data_dir: Path,
    *,
    tables: tuple[str, ...] = FINANCIAL_REPAIR_TABLES,
    apply: bool = False,
) -> dict[str, Any]:
    """Drop exact duplicate financial rows without choosing PIT revisions.

    Financial statement revisions are point-in-time data. This repair only removes
    rows that are byte-for-byte equivalent after Parquet decoding. If the same
    symbol/period/announcement key contains different values, the function reports
    those conflicts and leaves the conflicting rows unresolved.
    """
    data_dir = Path(data_dir)
    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    planned = [_plan_table(data_dir, table) for table in tables]
    total_removed = sum(int(item["removed_exact_duplicate_rows"]) for item in planned)
    unresolved_conflicts = sum(int(item["conflicting_key_groups"]) for item in planned)
    result_tables = []
    frames: dict[str, pl.DataFrame] = {}
    for item in planned:
        public = {key: value for key, value in item.items() if key != "_frame"}
        result_tables.append(public)
        frame = item.get("_frame")
        if isinstance(frame, pl.DataFrame):
            frames[item["table"]] = frame

    if apply and total_removed:
        status = "published_with_unresolved_conflicts" if unresolved_conflicts else "published"
    elif total_removed:
        status = "validated_with_unresolved_conflicts" if unresolved_conflicts else "validated"
    elif unresolved_conflicts:
        status = "blocked"
    else:
        status = "noop"
    result: dict[str, Any] = {
        "schema_version": 1,
        "repair_id": repair_id,
        "status": status,
        "apply": apply,
        "total_removed_exact_duplicate_rows": total_removed,
        "unresolved_conflicting_key_groups": unresolved_conflicts,
        "tables": result_tables,
    }
    if not apply or total_removed == 0:
        return result

    applied: list[tuple[Path, Path]] = []
    try:
        for item in planned:
            if int(item["removed_exact_duplicate_rows"]) <= 0:
                continue
            path = Path(str(item["path"]))
            backup = _write_repaired_table(path, frames[item["table"]], repair_id)
            applied.append((path, backup))
            for public in result_tables:
                if public["table"] == item["table"]:
                    public["backup_path"] = str(backup)
                    break
        manifest = data_dir / "financials" / f"repair-manifest-{repair_id}.json"
        result["manifest_path"] = str(manifest)
        _atomic_json(manifest, result)
    except Exception:
        for path, backup in reversed(applied):
            if backup.exists():
                if path.exists():
                    path.unlink()
                os.replace(backup, path)
        raise
    return result
