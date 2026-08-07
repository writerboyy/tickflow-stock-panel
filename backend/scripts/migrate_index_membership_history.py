"""Publish the canonical daily index-membership history from dated snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import polars as pl

from app.config import settings
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    normalize_index_membership_history,
    publish_history_table,
    read_history_table,
    validate_index_membership_history,
)


DEFAULT_JOINQUANT_ROOT = Path("pit_reference/joinquant/joinquant_index_constituent_candidates")


def migrate_index_membership_history(
    data_dir: Path,
    *,
    source_root: Path | None = None,
    source: str = "joinquant",
    replace: bool = False,
) -> dict[str, Any]:
    data_dir = Path(data_dir).expanduser().resolve()
    root = source_root or DEFAULT_JOINQUANT_ROOT
    root = root if root.is_absolute() else data_dir / root
    files = sorted(root.glob("snapshot_date=*/part.parquet"))
    if not files:
        raise FileNotFoundError(f"no dated index snapshots found under {root}")

    target = (
        data_dir / "pit_reference" / "history" / INDEX_MEMBERSHIP_HISTORY_TABLE / "part.parquet"
    )
    if target.exists() and not replace:
        raise FileExistsError(f"canonical index membership history already exists: {target}")

    raw = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
    frame = normalize_index_membership_history(raw.to_dicts(), source=source)
    validation = validate_index_membership_history(frame)
    if not validation["usable"]:
        raise ValueError(f"index membership history failed strict validation: {validation}")

    published_rows = publish_history_table(
        data_dir,
        INDEX_MEMBERSHIP_HISTORY_TABLE,
        frame,
    )
    published = read_history_table(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
    if published.height != frame.height:
        raise RuntimeError(
            "canonical index membership history verification failed: "
            f"expected {frame.height} rows, found {published.height}"
        )

    dates = frame["snapshot_date"]
    return {
        "table": INDEX_MEMBERSHIP_HISTORY_TABLE,
        "source": source,
        "source_root": str(root),
        "target": str(target),
        "published_rows": published_rows,
        "snapshot_dates": dates.n_unique(),
        "date_start": str(dates.min()),
        "date_end": str(dates.max()),
        "indices": sorted(frame["index_symbol"].unique().to_list()),
        "validation": validation,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migrate JoinQuant dated constituents into one canonical history table"
    )
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--source", default="joinquant")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    result = migrate_index_membership_history(
        args.data_dir,
        source_root=args.source_root,
        source=args.source,
        replace=args.replace,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
