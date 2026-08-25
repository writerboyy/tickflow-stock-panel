"""Physical schema registry for the backward-compatible fund NAV cache."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from uuid import uuid4

import polars as pl


def write_fund_nav_schema_registry(data_dir: Path) -> dict[str, object]:
    root = Path(data_dir) / "fund_nav"
    versions = {
        "1": {
            "columns": ["symbol", "date", "unit_net_value"],
            "date_semantics": "legacy_timestamp_date_shift_plus_one_on_read",
            "files": 0,
        },
        "2": {
            "columns": ["symbol", "date", "unit_net_value", "date_timezone"],
            "date_semantics": "Asia/Shanghai_calendar_date",
            "files": 0,
        },
    }
    unknown: list[str] = []
    for path in sorted(root.glob("symbol=*/part.parquet")):
        try:
            columns = set(pl.read_parquet_schema(path))
        except (OSError, pl.exceptions.PolarsError):
            unknown.append(path.relative_to(root).as_posix())
            continue
        version = "2" if "date_timezone" in columns else "1"
        versions[version]["files"] = int(versions[version]["files"]) + 1
    metadata = {
        "registry_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "logical_schema": ["symbol", "date", "unit_net_value"],
        "physical_versions": versions,
        "unknown_files": unknown,
    }
    root.mkdir(parents=True, exist_ok=True)
    target = root / "metadata.json"
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return metadata
