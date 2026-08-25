"""Shadow repair and yearly compaction for EasyTDX dividend history."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

import polars as pl

from app.plugins.easy_tdx.storage import DIVIDEND_HISTORY_TABLE
from app.services.stock_dividends import cash_per_share_from_plan


_KEY = ["symbol", "record_date", "plan"]


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def repair_dividend_history(data_dir: Path, *, apply: bool = False) -> dict[str, object]:
    data_dir = Path(data_dir)
    table_root = data_dir / "ext_data" / DIVIDEND_HISTORY_TABLE
    source_root = table_root / "timeseries"
    source_files = sorted(source_root.rglob("*.parquet"))
    if not source_files:
        raise FileNotFoundError(f"EasyTDX dividend history not found: {source_root}")

    source = pl.read_parquet(source_files)
    missing = sorted(set(_KEY + ["cash_per_share"]) - set(source.columns))
    if missing:
        raise ValueError(f"EasyTDX dividend history missing fields: {', '.join(missing)}")
    exact = source.unique(maintain_order=False)
    conflicts = exact.group_by(_KEY).len().filter(pl.col("len") > 1)
    if not conflicts.is_empty():
        raise ValueError("EasyTDX dividend history contains conflicting duplicate keys")
    source = exact.sort(_KEY)
    corrected = source.with_columns(
        pl.col("plan").map_elements(
            cash_per_share_from_plan,
            return_dtype=pl.Float64,
        ).alias("cash_per_share")
    )
    if corrected["cash_per_share"].null_count():
        raise ValueError("EasyTDX dividend repair produced null cash_per_share")
    differences = source.join(
        corrected.select(*_KEY, pl.col("cash_per_share").alias("corrected_cash_per_share")),
        on=_KEY,
        how="inner",
    ).filter(
        (pl.col("cash_per_share") - pl.col("corrected_cash_per_share")).abs() > 1e-12
    )

    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    shadow_root = table_root / ".repair" / repair_id
    shadow_timeseries = shadow_root / "timeseries"
    for year_frame in corrected.with_columns(
        pl.col("record_date").cast(pl.String).str.slice(0, 4).alias("_year")
    ).partition_by("_year"):
        year = str(year_frame["_year"][0])
        path = shadow_timeseries / f"year={year}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        year_frame.drop("_year").sort(_KEY).write_parquet(path)

    shadow_files = sorted(shadow_timeseries.rglob("*.parquet"))
    rebuilt = pl.read_parquet(shadow_files).sort(_KEY)
    if rebuilt.height != source.height or rebuilt.select(_KEY).equals(source.select(_KEY)) is False:
        raise RuntimeError("EasyTDX dividend shadow rebuild failed row/key parity")
    if rebuilt.select(_KEY).n_unique() != rebuilt.height:
        raise RuntimeError("EasyTDX dividend shadow rebuild contains duplicate keys")
    remaining = rebuilt.join(
        corrected.select(*_KEY, pl.col("cash_per_share").alias("expected")),
        on=_KEY,
    ).filter((pl.col("cash_per_share") - pl.col("expected")).abs() > 1e-12)
    if not remaining.is_empty():
        raise RuntimeError("EasyTDX dividend shadow rebuild differs from frozen parser")

    manifest: dict[str, object] = {
        "schema_version": 1,
        "repair_id": repair_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "validated",
        "primary_key": _KEY,
        "source_files": len(source_files),
        "source_rows": source.height,
        "corrected_rows": differences.height,
        "published_files": len(shadow_files),
        "published_rows": rebuilt.height,
        "published_hashes": {
            path.relative_to(shadow_timeseries).as_posix(): _file_hash(path)
            for path in shadow_files
        },
        "authoritative_corporate_actions_changed": False,
    }
    manifest_path = shadow_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if not apply:
        manifest["shadow_path"] = str(shadow_root)
        return manifest

    backup = table_root / f"timeseries.pre-repair-{repair_id}"
    os.replace(source_root, backup)
    try:
        os.replace(shadow_timeseries, source_root)
    except Exception:
        os.replace(backup, source_root)
        raise
    manifest.update({
        "status": "published",
        "backup_path": str(backup),
    })
    target_manifest = table_root / "repair-manifest.json"
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
    shutil.rmtree(shadow_root, ignore_errors=True)
    return manifest
