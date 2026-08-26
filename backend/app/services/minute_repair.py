"""Shadow cleanup and coverage manifests for persisted minute bars."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

import polars as pl

from app.services.minute_quality import minute_coverage_manifest, sanitize_minute_rows


def _valid_minute_rows(frame: pl.DataFrame) -> pl.DataFrame:
    return sanitize_minute_rows(frame)


def repair_minute_table(
    data_dir: Path,
    table: str,
    *,
    apply: bool = False,
) -> dict[str, object]:
    if table not in {"kline_minute", "kline_etf_minute"}:
        raise ValueError(f"unsupported minute table: {table}")
    data_dir = Path(data_dir)
    source_root = data_dir / table
    source_files = sorted(source_root.glob("date=*/part.parquet"))
    if not source_files:
        raise FileNotFoundError(f"minute table not found: {source_root}")

    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    shadow_root = data_dir / f".{table}.repair-{repair_id}"
    shadow_root.mkdir(parents=True)
    source_rows = 0
    published_rows = 0
    rejected_rows = 0
    rewritten_files = 0
    hardlinked_files = 0
    for source_path in source_files:
        frame = pl.read_parquet(source_path)
        clean = _valid_minute_rows(frame)
        rejected = frame.height - clean.height
        source_rows += frame.height
        published_rows += clean.height
        rejected_rows += rejected
        relative = source_path.relative_to(source_root)
        target = shadow_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if rejected:
            if not clean.is_empty():
                clean.sort(["symbol", "datetime"]).write_parquet(target)
            rewritten_files += 1
        else:
            os.link(source_path, target)
            hardlinked_files += 1
        if clean.select("symbol", "datetime").n_unique() != clean.height:
            raise RuntimeError(f"minute repair found duplicate keys: {relative}")
        coverage = minute_coverage_manifest(clean)
        coverage.update({
            "trade_date": source_path.parent.name.removeprefix("date="),
            "incoming_rows": frame.height,
            "rejected_rows": rejected,
        })
        coverage_path = shadow_root / "_coverage" / f"{source_path.parent.name}.json"
        coverage_path.parent.mkdir(parents=True, exist_ok=True)
        coverage_path.write_text(
            json.dumps(coverage, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    if published_rows != source_rows - rejected_rows:
        raise RuntimeError("minute repair row parity failed")
    manifest: dict[str, object] = {
        "schema_version": 1,
        "repair_id": repair_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "validated",
        "table": table,
        "source_files": len(source_files),
        "source_rows": source_rows,
        "published_rows": published_rows,
        "rejected_rows": rejected_rows,
        "rewritten_files": rewritten_files,
        "hardlinked_files": hardlinked_files,
        "coverage_files": len(source_files),
    }
    (shadow_root / "repair-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    if not apply:
        manifest["shadow_path"] = str(shadow_root)
        return manifest

    backup = data_dir / f".{table}.pre-repair-{repair_id}"
    os.replace(source_root, backup)
    try:
        os.replace(shadow_root, source_root)
    except Exception:
        os.replace(backup, source_root)
        raise
    manifest.update({"status": "published", "backup_path": str(backup)})
    target_manifest = source_root / "repair-manifest.json"
    temporary = target_manifest.with_name(f".{target_manifest.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, target_manifest)
    finally:
        if temporary.exists():
            temporary.unlink()
    return manifest


def remove_shadow(path: Path) -> None:
    """Remove a validated, unpublished shadow directory."""
    if ".repair-" not in path.name:
        raise ValueError(f"not a minute repair shadow: {path}")
    shutil.rmtree(path)
