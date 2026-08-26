"""Normalize persisted minute timestamps to the repository UTC-naive contract."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

import polars as pl

from app.services.minute_quality import (
    minute_coverage_manifest,
    minute_frame_is_canonical,
    normalize_minute_clock,
)


TABLES = ("kline_minute", "kline_etf_minute")
MINUTE_COLUMNS = ("open", "high", "low", "close", "volume", "amount")
PRICE_COLUMNS = ("open", "high", "low", "close")
PRICE_TOLERANCE = 1e-7
AMOUNT_RELATIVE_TOLERANCE = 1e-6
CLOSE_MATCH_TOLERANCE = 1e-6


def _daily_close_path(data_dir: Path, table: str, partition_date: date) -> Path:
    enriched_table = "kline_etf_enriched" if table == "kline_etf_minute" else "kline_daily_enriched"
    return data_dir / enriched_table / f"date={partition_date.isoformat()}" / "part.parquet"


def _load_daily_closes(data_dir: Path, table: str, partition_date: date) -> pl.DataFrame:
    path = _daily_close_path(data_dir, table, partition_date)
    if not path.exists():
        return pl.DataFrame()
    try:
        return (
            pl.read_parquet(path)
            .select("symbol", "close")
            .rename({"close": "_daily_close"})
            .unique(subset=["symbol"], keep="last")
        )
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _duplicate_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """Summarize duplicate keys and retain spreads for conflict classification."""
    return (
        frame.group_by(["symbol", "datetime"])
        .agg(
            pl.len().alias("_rows"),
            *[
                (pl.col(column).max() - pl.col(column).min()).abs().fill_null(0).alias(
                    f"_{column}_spread"
                )
                for column in MINUTE_COLUMNS
            ],
            pl.col("amount").abs().max().fill_null(0).alias("_amount_scale"),
        )
        .filter(pl.col("_rows") > 1)
    )


def _substantive_duplicate_groups(duplicates: pl.DataFrame) -> pl.DataFrame:
    if duplicates.is_empty():
        return duplicates
    price_conflict = pl.any_horizontal(
        pl.col(f"_{column}_spread") > PRICE_TOLERANCE for column in PRICE_COLUMNS
    )
    amount_conflict = pl.col("_amount_spread") > pl.max_horizontal(
        pl.lit(1.0), pl.col("_amount_scale")
    ) * AMOUNT_RELATIVE_TOLERANCE
    return duplicates.filter(
        price_conflict
        | (pl.col("_volume_spread") > PRICE_TOLERANCE)
        | amount_conflict
    )


def _resolve_duplicate_rows(
    frame: pl.DataFrame,
    data_dir: Path,
    table: str,
    partition_date: date,
) -> tuple[pl.DataFrame, int, int]:
    """Deduplicate keys, failing closed on unexplained substantive conflicts."""
    duplicates = _duplicate_summary(frame)
    if duplicates.is_empty():
        return frame, 0, 0

    substantive = _substantive_duplicate_groups(duplicates)
    with_daily = frame.with_row_index("_row_nr")
    daily = _load_daily_closes(data_dir, table, partition_date)
    if not daily.is_empty():
        with_daily = with_daily.join(daily, on="symbol", how="left")
    else:
        with_daily = with_daily.with_columns(pl.lit(None, dtype=pl.Float64).alias("_daily_close"))
    with_daily = with_daily.with_columns(
        pl.when(pl.col("_daily_close").is_not_null() & pl.col("close").is_not_null())
        .then((pl.col("close") - pl.col("_daily_close")).abs())
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("_close_error"),
    )

    if not substantive.is_empty():
        match_stats = (
            with_daily.join(
                substantive.select("symbol", "datetime"),
                on=["symbol", "datetime"],
                how="semi",
            )
            .group_by(["symbol", "datetime"])
            .agg(pl.col("_close_error").min().alias("_min_close_error"))
        )
        unresolved = substantive.join(match_stats, on=["symbol", "datetime"], how="left").filter(
            pl.col("_min_close_error").is_null()
            | (pl.col("_min_close_error") > CLOSE_MATCH_TOLERANCE)
        )
        if not unresolved.is_empty():
            sample = unresolved.select("symbol", "datetime").head(8).to_dicts()
            raise RuntimeError(
                "unexplained conflicting duplicate minute keys after clock normalization: "
                f"{sample}"
            )

    selected = (
        with_daily.with_columns(
            pl.col("_close_error").fill_null(float("inf")).alias("_close_error_sort"),
        )
        .sort(["symbol", "datetime", "_close_error_sort", "_row_nr"])
        .unique(subset=["symbol", "datetime"], keep="first")
        .drop(["_row_nr", "_daily_close", "_close_error", "_close_error_sort"])
        .sort(["symbol", "datetime"])
    )
    return selected, frame.height - selected.height, substantive.height


def _build_shadow(
    data_dir: Path,
    table: str,
    repair_id: str,
    start_date: date | None,
    end_date: date | None,
) -> tuple[Path, dict[str, object]]:
    source_root = data_dir / table
    source_files = sorted(source_root.glob("date=*/part.parquet"))
    if not source_files:
        raise FileNotFoundError(f"minute table not found: {source_root}")

    shadow_root = data_dir / f".{table}.utc-normalize-{repair_id}"
    shadow_root.mkdir()
    for child in source_root.iterdir():
        if child.is_file() and child.name != "repair-manifest.json":
            os.link(child, shadow_root / child.name)

    source_rows = 0
    published_rows = 0
    shifted_rows = 0
    deduplicated_rows = 0
    conflict_groups = 0
    basis_counts: dict[str, int] = {}
    for source_path in source_files:
        partition_date = date.fromisoformat(source_path.parent.name.removeprefix("date="))
        selected = (
            (start_date is None or partition_date >= start_date)
            and (end_date is None or partition_date <= end_date)
        )
        if not selected:
            target = shadow_root / source_path.relative_to(source_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(source_path, target)
            coverage_source = source_root / "_coverage" / f"date={partition_date}.json"
            if coverage_source.exists():
                coverage_target = shadow_root / "_coverage" / coverage_source.name
                coverage_target.parent.mkdir(parents=True, exist_ok=True)
                os.link(coverage_source, coverage_target)
            continue
        frame = pl.read_parquet(source_path)
        normalized, basis, shifted = normalize_minute_clock(frame)
        if basis == "invalid":
            raise RuntimeError(f"invalid minute timestamp column: {source_path}")
        if not minute_frame_is_canonical(normalized):
            raise RuntimeError(
                f"minute timestamp remains mixed or invalid after normalization: {source_path}"
            )
        normalized, deduplicated, partition_conflict_groups = _resolve_duplicate_rows(
            normalized,
            data_dir,
            table,
            partition_date,
        )
        dates = normalized.select(pl.col("datetime").dt.date().unique()).to_series().to_list()
        expected_date = source_path.parent.name.removeprefix("date=")
        if any(str(value) != expected_date for value in dates):
            raise RuntimeError(f"clock normalization changed partition date: {source_path}")

        relative = source_path.relative_to(source_root)
        target = shadow_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        normalized.write_parquet(target, compression="zstd")
        coverage = minute_coverage_manifest(normalized)
        coverage.update({
            "trade_date": expected_date,
            "incoming_rows": frame.height,
            "rejected_rows": 0,
            "source": "utc_clock_repair",
            "datetime_basis": "utc_naive",
            "basis_before": basis,
            "shifted_rows": shifted,
            "deduplicated_rows": deduplicated,
            "conflicting_duplicate_groups": partition_conflict_groups,
        })
        coverage_path = shadow_root / "_coverage" / f"date={expected_date}.json"
        coverage_path.parent.mkdir(parents=True, exist_ok=True)
        coverage_path.write_text(
            json.dumps(coverage, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        source_rows += frame.height
        published_rows += normalized.height
        shifted_rows += shifted
        deduplicated_rows += deduplicated
        conflict_groups += partition_conflict_groups
        basis_counts[basis] = basis_counts.get(basis, 0) + 1

    manifest: dict[str, object] = {
        "schema_version": 1,
        "repair_id": repair_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "validated",
        "table": table,
        "source_files": len(source_files),
        "source_rows": source_rows,
        "published_rows": published_rows,
        "shifted_rows": shifted_rows,
        "deduplicated_rows": deduplicated_rows,
        "conflicting_duplicate_groups": conflict_groups,
        "basis_counts": basis_counts,
        "datetime_basis": "utc_naive",
        "selected_start": start_date.isoformat() if start_date else None,
        "selected_end": end_date.isoformat() if end_date else None,
    }
    (shadow_root / "repair-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return shadow_root, manifest


def repair(
    data_dir: Path,
    tables: tuple[str, ...],
    *,
    apply: bool,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, object]]:
    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    built: list[tuple[str, Path, dict[str, object]]] = []
    try:
        for table in tables:
            shadow, manifest = _build_shadow(data_dir, table, repair_id, start_date, end_date)
            built.append((table, shadow, manifest))
    except Exception:
        for _table, shadow, _manifest in built:
            shutil.rmtree(shadow, ignore_errors=True)
        raise

    if not apply:
        return [{**manifest, "shadow_path": str(shadow)} for _table, shadow, manifest in built]

    published: list[tuple[Path, Path, Path]] = []
    try:
        for table, shadow, manifest in built:
            source = data_dir / table
            backup = data_dir / f".{table}.pre-utc-normalize-{repair_id}"
            if backup.exists():
                raise FileExistsError(f"backup already exists: {backup}")
            os.replace(source, backup)
            try:
                os.replace(shadow, source)
            except Exception:
                os.replace(backup, source)
                raise
            published.append((source, backup, shadow))
            manifest.update({"status": "published", "backup_path": str(backup)})
            (source / "repair-manifest.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
    except Exception:
        for source, backup, _shadow in reversed(published):
            if source.exists():
                shutil.rmtree(source)
            if backup.exists():
                os.replace(backup, source)
        for table, shadow, _manifest in built:
            if shadow.exists():
                shutil.rmtree(shadow, ignore_errors=True)
        raise
    return [manifest for _table, _shadow, manifest in built]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data",
    )
    parser.add_argument("--table", choices=TABLES, action="append", dest="tables")
    parser.add_argument("--start-date", type=date.fromisoformat)
    parser.add_argument("--end-date", type=date.fromisoformat)
    parser.add_argument("--apply", action="store_true", help="atomically publish the normalized tables")
    args = parser.parse_args()
    tables = tuple(args.tables or TABLES)
    if (args.start_date is None) != (args.end_date is None):
        parser.error("--start-date and --end-date must be provided together")
    if args.start_date and args.start_date > args.end_date:
        parser.error("--start-date must be no later than --end-date")
    result = repair(
        args.data_dir.resolve(),
        tables,
        apply=args.apply,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
