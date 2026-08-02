"""Evidence-bound shadow repair for confirmed index daily anomalies."""

from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
from typing import Any
from uuid import uuid4

import polars as pl

from app.services.index_sync import IndexDailyQualityError, _validate_index_daily


_TABLES = ("kline_index_daily", "kline_index_enriched")
_FIELDS = ("open", "high", "low", "close", "volume", "amount")
_ANOMALY_FIELDS = {
    "volume_negative": "volume",
    "amount_overflow": "amount",
}


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_hash(root: Path, files: list[Path]) -> str:
    digest = sha256()
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(_file_hash(path)))
    return digest.hexdigest()


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


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"repair evidence must be a JSON object: {path.name}")
    return value


def _same(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    return float(left) == float(right)


def _load_replacements(
    evidence_path: Path,
    corroboration_path: Path | None,
    *,
    expected_rows: int,
    expected_dual_source_rows: int,
) -> tuple[dict[tuple[str, date], dict[str, Any]], dict[str, Any]]:
    evidence = _read_json(evidence_path)
    rows = evidence.get("rows")
    if not isinstance(rows, list):
        raise ValueError("EasyTDX repair evidence has no rows list")

    replacements: dict[tuple[str, date], dict[str, Any]] = {}
    for item in rows:
        if item.get("status") != "tickflow_anomaly_confirmed":
            continue
        if item.get("tdx_anomalies") or item.get("differing_valid_fields"):
            raise ValueError("confirmed EasyTDX evidence contains unresolved differences")
        symbol = str(item.get("symbol") or "")
        trade_date = date.fromisoformat(str(item.get("date")))
        key = symbol, trade_date
        if not symbol or key in replacements:
            raise ValueError("EasyTDX repair evidence contains an invalid or duplicate key")
        anomalies = item.get("tickflow_anomalies") or []
        unknown = sorted(set(anomalies) - set(_ANOMALY_FIELDS))
        if unknown or not anomalies:
            raise ValueError(f"unsupported confirmed anomaly for {symbol}/{trade_date}: {unknown}")
        tickflow = item.get("tickflow")
        easy_tdx = item.get("easy_tdx")
        if not isinstance(tickflow, dict) or not isinstance(easy_tdx, dict):
            raise ValueError(f"repair evidence missing source values for {symbol}/{trade_date}")
        changes = {_ANOMALY_FIELDS[anomaly] for anomaly in anomalies}
        for field in _FIELDS:
            if tickflow.get(field) is None or easy_tdx.get(field) is None:
                raise ValueError(f"repair evidence missing {field} for {symbol}/{trade_date}")
        replacements[key] = {
            "source": {field: float(tickflow[field]) for field in _FIELDS},
            "replacement": {
                field: float(easy_tdx[field]) if field in changes else None
                for field in ("volume", "amount")
            },
            "changed_fields": sorted(changes),
        }

    if len(replacements) != expected_rows:
        raise ValueError(
            f"confirmed repair row count changed: expected {expected_rows}, got {len(replacements)}"
        )

    dual_source_keys: set[tuple[str, date]] = set()
    corroboration_hash = None
    if corroboration_path is not None:
        corroboration = _read_json(corroboration_path)
        corroboration_rows = corroboration.get("rows")
        if not isinstance(corroboration_rows, list):
            raise ValueError("BaoStock corroboration evidence has no rows list")
        for item in corroboration_rows:
            if item.get("status") != "tdx_baostock_confirm_tickflow_anomaly":
                continue
            if item.get("easy_tdx_baostock_differing_fields"):
                raise ValueError("BaoStock corroboration contains EasyTDX differences")
            key = str(item.get("symbol") or ""), date.fromisoformat(str(item.get("date")))
            if key not in replacements:
                raise ValueError("BaoStock corroboration contains an unexpected repair key")
            baostock = item.get("baostock_normalized")
            if not isinstance(baostock, dict):
                raise ValueError("BaoStock corroboration is missing normalized values")
            replacements[key]["baostock"] = {
                field: float(baostock[field]) for field in _FIELDS
            }
            dual_source_keys.add(key)
        corroboration_hash = _file_hash(corroboration_path)
    if len(dual_source_keys) != expected_dual_source_rows:
        raise ValueError(
            "dual-source repair row count changed: "
            f"expected {expected_dual_source_rows}, got {len(dual_source_keys)}"
        )

    for key, replacement in replacements.items():
        replacement["evidence_level"] = (
            "easy_tdx_baostock" if key in dual_source_keys else "easy_tdx"
        )
    return replacements, {
        "easy_tdx_evidence_hash": _file_hash(evidence_path),
        "baostock_evidence_hash": corroboration_hash,
        "dual_source_rows": len(dual_source_keys),
        "single_source_rows": len(replacements) - len(dual_source_keys),
    }


def _partition_date(path: Path) -> date:
    return date.fromisoformat(path.parent.name.removeprefix("date="))


def _repair_partition(
    frame: pl.DataFrame,
    partition_replacements: dict[tuple[str, date], dict[str, Any]],
    *,
    table: str,
) -> pl.DataFrame:
    by_key = {
        (str(row["symbol"]), row["date"]): row
        for row in frame.select("symbol", "date", *_FIELDS).iter_rows(named=True)
    }
    missing = sorted(set(partition_replacements) - set(by_key))
    if missing:
        raise ValueError(f"{table} is missing repair keys: {missing[:3]}")
    for key, replacement in partition_replacements.items():
        current = by_key[key]
        for field in _FIELDS:
            if not _same(current[field], replacement["source"][field]):
                raise ValueError(f"{table} source drift for {key[0]}/{key[1]} field {field}")

    replacement_rows = [
        {
            "symbol": key[0],
            "date": key[1],
            "_replacement_volume": value["replacement"]["volume"],
            "_replacement_amount": value["replacement"]["amount"],
        }
        for key, value in partition_replacements.items()
    ]
    replacement_frame = pl.DataFrame(
        replacement_rows,
        schema={
            "symbol": pl.String,
            "date": pl.Date,
            "_replacement_volume": pl.Float64,
            "_replacement_amount": pl.Float64,
        },
    )
    repaired = (
        frame.join(replacement_frame, on=["symbol", "date"], how="left")
        .with_columns(
            pl.coalesce("_replacement_volume", "volume").alias("volume"),
            pl.coalesce("_replacement_amount", "amount").alias("amount"),
        )
        .drop("_replacement_volume", "_replacement_amount")
        .select(frame.columns)
    )
    if repaired.schema != frame.schema:
        raise RuntimeError(f"{table} repair changed partition schema")
    return repaired.sort(["symbol", "date"])


def _validate_shadow(
    shadow_daily: Path,
    shadow_enriched: Path,
    replacements: dict[tuple[str, date], dict[str, Any]],
    *,
    expected_remaining_rows: int,
) -> None:
    daily_files = sorted(shadow_daily.glob("date=*/part.parquet"))
    enriched_files = sorted(shadow_enriched.glob("date=*/part.parquet"))
    daily = pl.read_parquet(daily_files)
    enriched = pl.read_parquet(enriched_files)
    if daily.select("symbol", "date").n_unique() != daily.height:
        raise RuntimeError("repaired index daily contains duplicate keys")
    if enriched.select("symbol", "date").n_unique() != enriched.height:
        raise RuntimeError("repaired index enriched contains duplicate keys")
    parity = daily.select("symbol", "date", *_FIELDS).join(
        enriched.select("symbol", "date", *_FIELDS),
        on=["symbol", "date"],
        how="full",
        suffix="_enriched",
    )
    if parity.height != daily.height or daily.height != enriched.height:
        raise RuntimeError("index daily/enriched repair lost key parity")
    mismatches = parity.filter(
        pl.any_horizontal(
            (pl.col(field) - pl.col(f"{field}_enriched")).abs() > 1e-9
            for field in _FIELDS
        )
    )
    if not mismatches.is_empty():
        raise RuntimeError("index daily/enriched repair differs on OHLCVA")
    repaired_keys = pl.DataFrame(
        [{"symbol": key[0], "date": key[1]} for key in replacements],
        schema={"symbol": pl.String, "date": pl.Date},
    )
    repaired_rows = daily.join(repaired_keys, on=["symbol", "date"], how="inner")
    if repaired_rows.height != len(replacements):
        raise RuntimeError("repaired index daily does not contain every evidence key")
    _validate_index_daily(repaired_rows)
    try:
        _validate_index_daily(daily)
        remaining = 0
    except IndexDailyQualityError as exc:
        remaining = exc.invalid_rows.select("symbol", "date").n_unique()
    if remaining != expected_remaining_rows:
        raise RuntimeError(
            f"remaining index anomaly count changed: expected {expected_remaining_rows}, got {remaining}"
        )


def repair_confirmed_index_daily(
    data_dir: Path,
    evidence_path: Path,
    *,
    corroboration_path: Path | None = None,
    expected_rows: int,
    expected_dual_source_rows: int,
    expected_remaining_rows: int,
    apply: bool = False,
) -> dict[str, Any]:
    """Validate or publish replacements for evidence-confirmed index fields."""
    data_dir = Path(data_dir)
    evidence_path = Path(evidence_path)
    corroboration_path = Path(corroboration_path) if corroboration_path else None
    replacements, evidence_meta = _load_replacements(
        evidence_path,
        corroboration_path,
        expected_rows=expected_rows,
        expected_dual_source_rows=expected_dual_source_rows,
    )
    replacement_dates = {key[1] for key in replacements}
    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    shadow_parent = data_dir / f".index-confirmed-repair-{repair_id}"
    shadow_roots = {table: shadow_parent / table for table in _TABLES}
    source_roots = {table: data_dir / table for table in _TABLES}
    source_files: dict[str, list[Path]] = {}
    source_rows: dict[str, int] = {}
    rewritten_files: dict[str, int] = {}
    hardlinked_files: dict[str, int] = {}

    shadow_parent.mkdir(parents=True)
    try:
        for table in _TABLES:
            root = source_roots[table]
            files = sorted(root.glob("date=*/part.parquet"))
            if not files:
                raise FileNotFoundError(f"index table not found: {root}")
            source_files[table] = files
            source_rows[table] = 0
            rewritten_files[table] = 0
            hardlinked_files[table] = 0
            for source_path in files:
                trade_date = _partition_date(source_path)
                target = shadow_roots[table] / source_path.relative_to(root)
                target.parent.mkdir(parents=True, exist_ok=True)
                frame = pl.read_parquet(source_path)
                source_rows[table] += frame.height
                partition_replacements = {
                    key: value for key, value in replacements.items() if key[1] == trade_date
                }
                if trade_date in replacement_dates:
                    repaired = _repair_partition(
                        frame,
                        partition_replacements,
                        table=table,
                    )
                    repaired.write_parquet(target)
                    rewritten_files[table] += 1
                else:
                    os.link(source_path, target)
                    hardlinked_files[table] += 1

        _validate_shadow(
            shadow_roots["kline_index_daily"],
            shadow_roots["kline_index_enriched"],
            replacements,
            expected_remaining_rows=expected_remaining_rows,
        )
        published_files = {
            table: sorted(shadow_roots[table].glob("date=*/part.parquet"))
            for table in _TABLES
        }
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "repair_id": repair_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "validated",
            "primary_key": ["symbol", "date"],
            "replacement_source": "easy_tdx",
            "replacement_policy": "replace_only_quality_gate_invalid_fields",
            "confirmed_rows": len(replacements),
            "changed_field_values_per_table": sum(
                len(value["changed_fields"]) for value in replacements.values()
            ),
            "affected_dates": len(replacement_dates),
            "first_affected_date": min(replacement_dates).isoformat(),
            "last_affected_date": max(replacement_dates).isoformat(),
            "remaining_anomaly_rows": expected_remaining_rows,
            "source_rows": source_rows,
            "source_files": {table: len(files) for table, files in source_files.items()},
            "rewritten_files": rewritten_files,
            "hardlinked_files": hardlinked_files,
            "source_hashes": {
                table: _tree_hash(source_roots[table], files)
                for table, files in source_files.items()
            },
            "published_hashes": {
                table: _tree_hash(shadow_roots[table], files)
                for table, files in published_files.items()
            },
            "replacement_records": [
                {
                    "symbol": key[0],
                    "date": key[1].isoformat(),
                    **value,
                }
                for key, value in sorted(replacements.items())
            ],
            **evidence_meta,
        }
        _atomic_json(shadow_parent / "repair-manifest.json", manifest)
        if not apply:
            return {**manifest, "shadow_path": str(shadow_parent)}

        backups = {
            table: data_dir / f".{table}.pre-repair-{repair_id}"
            for table in _TABLES
        }
        manifest.update({
            "status": "published",
            "backup_paths": {table: str(path) for table, path in backups.items()},
        })
        moved_backups: list[str] = []
        published: list[str] = []
        try:
            for table in _TABLES:
                os.replace(source_roots[table], backups[table])
                moved_backups.append(table)
            for table in _TABLES:
                os.replace(shadow_roots[table], source_roots[table])
                published.append(table)
            for table in _TABLES:
                _atomic_json(source_roots[table] / "repair-manifest.json", manifest)
        except Exception:
            for table in reversed(published):
                if source_roots[table].exists():
                    os.replace(source_roots[table], shadow_roots[table])
            for table in reversed(moved_backups):
                if backups[table].exists() and not source_roots[table].exists():
                    os.replace(backups[table], source_roots[table])
            raise

        shutil.rmtree(shadow_parent)
        return manifest
    except Exception:
        if all(root.exists() for root in source_roots.values()):
            shutil.rmtree(shadow_parent, ignore_errors=True)
        raise


def remove_index_repair_shadow(path: Path) -> None:
    path = Path(path)
    if not path.name.startswith(".index-confirmed-repair-"):
        raise ValueError(f"not an index repair shadow: {path}")
    shutil.rmtree(path)
