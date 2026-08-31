"""Read-only replay and reconciliation for archived Kaipanla responses."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import gzip
import json
import math
from pathlib import Path
from typing import Any

import polars as pl

from app.plugins.kaipanla.parsers import (
    parse_auction,
    parse_bid_detail,
    parse_dragon_tiger_details,
    parse_dragon_tiger_movement,
    parse_lhb_detail,
    parse_lhb_list,
    parse_northbound_sector,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
    parse_sector_strength,
    parse_shareholder_changes,
    parse_shareholder_count_changes,
)
from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    LHB_DETAIL_TABLE,
    LHB_MOVEMENT_TABLE,
    LHB_TABLE,
    NORTHBOUND_SECTOR_TABLE,
    REGULATORY_TABLE,
    SHAREHOLDER_COUNT_TABLE,
    SHAREHOLDER_TABLE,
    TABLE_IDS,
    _normalize_rows,
)
from app.services.ext_data import ExtConfigStore
from app.services.ingestion_manifest import stable_content_hash


_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    AUCTION_TABLE: ("symbol",),
    LHB_TABLE: ("symbol",),
    REGULATORY_TABLE: ("symbol",),
    NORTHBOUND_SECTOR_TABLE: ("plate_id",),
    SHAREHOLDER_TABLE: ("symbol", "snapshot_kind", "shareholder_id"),
    SHAREHOLDER_COUNT_TABLE: ("symbol",),
    LHB_MOVEMENT_TABLE: ("participant_id", "side", "symbol"),
    LHB_DETAIL_TABLE: ("symbol", "side", "log_id"),
}
_IGNORED_COMPARE_FIELDS = {
    "collected_at",
    "bid_collected_at",
    "detail_collected_at",
    "pre_collected_at",
    "post_collected_at",
}
_VIRTUAL_PARTITION_FIELDS = {"report_date"}
_EXPECTED_DTYPES = {
    "string": pl.String,
    "int": pl.Int64,
    "float": pl.Float64,
    "bool": pl.Boolean,
}


def _archive_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and (path.name.endswith(".json") or path.name.endswith(".json.gz"))
    )


def _read_json(path: Path) -> dict[str, Any]:
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("archive root is not an object")
    return value


def _unwrap_archive(value: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    if "response" in value:
        payload = value["response"]
    elif "payload" in value:
        payload = value["payload"]
    else:
        payload = value
    if not isinstance(payload, dict):
        raise ValueError("archive response is not an object")
    expected_hash = value.get("content_hash")
    if expected_hash is not None and expected_hash != stable_content_hash(payload):
        raise ValueError("archive content hash mismatch")
    parser_version = value.get("parser_version")
    return payload, str(parser_version) if parser_version is not None else None


def _archive_date(path: Path) -> date:
    for parent in path.parents:
        if parent.name.startswith("date="):
            return date.fromisoformat(parent.name.removeprefix("date="))
    raise ValueError("archive path does not contain date partition")


def _context(path: Path) -> str:
    name = path.name.removesuffix(".gz").removesuffix(".json")
    parts = name.split("-", 2)
    return parts[2] if len(parts) == 3 else ""


def _code_context(path: Path) -> str:
    value = _context(path).split("-", 1)[0]
    if len(value) != 6 or not value.isdigit():
        raise ValueError("archive filename does not contain a six-digit code")
    return value


def _stamp_report_date(rows: list[dict[str, Any]], value: date) -> list[dict[str, Any]]:
    return [{**row, "report_date": value.isoformat()} for row in rows]


def _auction_rows(
    endpoint: str,
    payload: dict[str, Any],
    path: Path,
) -> list[dict[str, Any]]:
    rows = parse_auction(payload)
    if not rows:
        return []
    context = _context(path)
    checkpoint = context.split("-", 1)[0]
    if endpoint == "30" and checkpoint not in {"0915", "0920", "0925"}:
        checkpoint = "0925"
    if checkpoint not in {"0915", "0920", "0925"}:
        raise ValueError("auction archive is missing checkpoint metadata")
    result = []
    for row in rows:
        item = {key: row.get(key) for key in ("symbol", "code", "name")}
        item[f"source_{checkpoint}"] = f"/{endpoint}"
        for field, value in row.items():
            if field not in {"symbol", "code", "name"}:
                item[f"{field}_{checkpoint}"] = value
        result.append(item)
    return result


def _parse_archive(
    endpoint: str,
    payload: dict[str, Any],
    archive_date: date,
    path: Path,
) -> list[tuple[str, date, dict[str, Any]]]:
    table: str | None = None
    partition = archive_date
    rows: list[dict[str, Any]]
    if endpoint in {"30", "115"}:
        table, rows = AUCTION_TABLE, _auction_rows(endpoint, payload, path)
    elif endpoint == "31":
        table, rows = AUCTION_TABLE, [parse_bid_detail(payload)]
    elif endpoint == "100":
        parsed_date, rows = parse_lhb_list(payload)
        table, partition = LHB_TABLE, parsed_date or archive_date
    elif endpoint == "101":
        table, rows = LHB_TABLE, [parse_lhb_detail(payload, _code_context(path))]
    elif endpoint == "108":
        rows = parse_regulatory_monitor(payload)
        prefix = _context(path).split("-", 1)[0]
        table = REGULATORY_TABLE
        rows = [
            {
                **{key: row.get(key) for key in ("symbol", "code", "name")},
                **{
                    f"{prefix}_{key}": value
                    for key, value in row.items()
                    if key not in {"symbol", "code", "name"}
                },
            }
            for row in rows
        ]
    elif endpoint == "109":
        rows = parse_regulatory_anomaly(payload)
        prefix = _context(path).split("-", 1)[0]
        table = REGULATORY_TABLE
        rows = [
            {
                **{key: row.get(key) for key in ("symbol", "code", "name")},
                **{
                    f"{prefix}_{key}": value
                    for key, value in row.items()
                    if key not in {"symbol", "code", "name"}
                },
            }
            for row in rows
        ]
    elif endpoint.startswith("northbound_sector_"):
        partition, rows = parse_northbound_sector(payload)
        table = NORTHBOUND_SECTOR_TABLE
    elif endpoint == "shareholder_changes":
        table = SHAREHOLDER_TABLE
        rows = parse_shareholder_changes(payload, _code_context(path), archive_date)
    elif endpoint == "shareholder_count_changes":
        table, rows = SHAREHOLDER_COUNT_TABLE, parse_shareholder_count_changes(payload)
    elif endpoint == "dragon_tiger_movement":
        table = LHB_MOVEMENT_TABLE
        rows = _stamp_report_date(parse_dragon_tiger_movement(payload, archive_date), archive_date)
    elif endpoint == "dragon_tiger_details":
        table = LHB_DETAIL_TABLE
        rows = _stamp_report_date(
            parse_dragon_tiger_details(payload, _code_context(path)), archive_date
        )
    elif endpoint == "sector_strength":
        parse_sector_strength(payload)
        return []
    else:
        raise ValueError(f"unsupported archived endpoint: {endpoint}")

    normalized = (
        _normalize_rows(rows, path.parents[4])
        if any("symbol" in row or "code" in row for row in rows)
        else rows
    )
    result = []
    for row in normalized:
        row_partition = partition
        if table == SHAREHOLDER_COUNT_TABLE:
            row_partition = date.fromisoformat(str(row["report_date"]))
        result.append((table, row_partition, row))
    return result


def _row_key(table: str, partition: date, row: dict[str, Any]) -> tuple[str, ...]:
    fields = _PRIMARY_KEYS[table]
    values = []
    for field in fields:
        value = row.get(field)
        if value in (None, ""):
            raise ValueError(f"{table} row is missing primary key field {field}")
        values.append(str(value))
    return (partition.isoformat(), *values)


def _same_value(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
        return math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-12)
    return left == right


def _compare_table(
    data_dir: Path,
    table: str,
    replay_rows: dict[tuple[str, ...], dict[str, Any]],
) -> dict[str, Any]:
    config = ExtConfigStore(data_dir).get(table)
    contract_fields = {field.name: field for field in config.fields} if config else {}
    contract_hash = stable_content_hash(
        [field.to_dict() for field in config.fields]
    ) if config else None
    parquet_rows: dict[tuple[str, ...], dict[str, Any]] = {}
    duplicate_keys = 0
    partition_date_mismatches = 0
    schema_missing_fields: set[str] = set()
    schema_unexpected_fields: set[str] = set()
    dtype_mismatches: set[str] = set()
    root = data_dir / "ext_data" / table / "timeseries"
    parquet_files = sorted(root.glob("date=*/part.parquet"))
    for path in parquet_files:
        partition = date.fromisoformat(path.parent.name.removeprefix("date="))
        schema = pl.read_parquet_schema(path)
        actual_fields = set(schema)
        expected_fields = set(contract_fields)
        schema_missing_fields.update(expected_fields - actual_fields)
        schema_unexpected_fields.update(actual_fields - expected_fields)
        for field in sorted(expected_fields & actual_fields):
            expected_dtype = _EXPECTED_DTYPES.get(contract_fields[field].dtype)
            if expected_dtype is None or schema[field] != expected_dtype:
                dtype_mismatches.add(
                    f"{field}: expected={contract_fields[field].dtype}, actual={schema[field]}"
                )
        for row in pl.read_parquet(path).to_dicts():
            if "report_date" in row and row["report_date"] not in (
                None,
                partition.isoformat(),
            ):
                partition_date_mismatches += 1
            key = _row_key(table, partition, row)
            if key in parquet_rows:
                duplicate_keys += 1
            parquet_rows[key] = row

    replay_keys = set(replay_rows)
    parquet_keys = set(parquet_rows)
    mismatches = 0
    missing_value_fields = 0
    unexpected_replay_fields: set[str] = set()
    for key in sorted(replay_keys & parquet_keys):
        expected = replay_rows[key]
        actual = parquet_rows[key]
        for field, value in expected.items():
            if field in _VIRTUAL_PARTITION_FIELDS:
                if value not in (None, key[0]):
                    partition_date_mismatches += 1
                continue
            if field in _IGNORED_COMPARE_FIELDS:
                continue
            if field not in contract_fields:
                unexpected_replay_fields.add(field)
                continue
            if field not in actual:
                missing_value_fields += 1
                continue
            if not _same_value(value, actual[field]):
                mismatches += 1

    contract_valid = config is not None and config.schema_version >= 1
    passed = (
        contract_valid
        and replay_keys == parquet_keys
        and mismatches == 0
        and missing_value_fields == 0
        and duplicate_keys == 0
        and partition_date_mismatches == 0
        and not schema_missing_fields
        and not schema_unexpected_fields
        and not dtype_mismatches
        and not unexpected_replay_fields
    )

    return {
        "status": "passed" if passed else "failed",
        "replay_rows": len(replay_rows),
        "parquet_rows": len(parquet_rows),
        "parquet_files": len(parquet_files),
        "missing_in_parquet": len(replay_keys - parquet_keys),
        "missing_in_replay": len(parquet_keys - replay_keys),
        "field_mismatches": mismatches,
        "missing_value_fields": missing_value_fields,
        "duplicate_parquet_keys": duplicate_keys,
        "partition_date_mismatches": partition_date_mismatches,
        "schema_missing_fields": sorted(schema_missing_fields),
        "schema_unexpected_fields": sorted(schema_unexpected_fields),
        "dtype_mismatches": sorted(dtype_mismatches),
        "unexpected_replay_fields": sorted(unexpected_replay_fields),
        "schema_version": config.schema_version if config else None,
        "field_contract_hash": contract_hash,
        "primary_key": ["partition_date", *_PRIMARY_KEYS[table]],
        "replay_hash": stable_content_hash(
            [{"key": key, "row": replay_rows[key]} for key in sorted(replay_rows)]
        ),
    }


def replay_archives(data_dir: Path, *, compare_parquet: bool = True) -> dict[str, Any]:
    """Replay every Kaipanla archive without calling a source or writing lake data."""
    data_dir = Path(data_dir).resolve()
    root = data_dir / "ext_data" / "_kaipanla_raw"
    files = _archive_files(root)
    endpoint_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"archives": 0, "parsed_rows": 0, "valid_empty": 0, "errors": 0}
    )
    replay_rows: dict[str, dict[tuple[str, ...], dict[str, Any]]] = defaultdict(dict)
    replay_revisions: dict[str, int] = defaultdict(int)
    conflicting_field_updates: dict[str, int] = defaultdict(int)
    errors: list[dict[str, str]] = []
    parser_versions: set[str] = set()

    for path in files:
        endpoint = path.parent.name
        stats = endpoint_stats[endpoint]
        stats["archives"] += 1
        try:
            archive_date = _archive_date(path)
            payload, parser_version = _unwrap_archive(_read_json(path))
            if parser_version:
                parser_versions.add(parser_version)
            records = _parse_archive(endpoint, payload, archive_date, path)
            stats["parsed_rows"] += len(records)
            if not records:
                stats["valid_empty"] += 1
            for table, partition, row in records:
                key = _row_key(table, partition, row)
                current = dict(replay_rows[table].get(key, {}))
                if current:
                    replay_revisions[table] += 1
                    conflicting_field_updates[table] += sum(
                        1
                        for field, value in row.items()
                        if field not in _IGNORED_COMPARE_FIELDS
                        and value is not None
                        and field in current
                        and current[field] is not None
                        and not _same_value(current[field], value)
                    )
                current.update({field: value for field, value in row.items() if value is not None})
                replay_rows[table][key] = current
        except Exception as exc:  # noqa: BLE001
            stats["errors"] += 1
            errors.append({
                "path": path.relative_to(data_dir).as_posix(),
                "error": f"{type(exc).__name__}: {exc}",
            })

    tables = (
        {
            table: {
                **_compare_table(data_dir, table, replay_rows[table]),
                "replay_revisions": replay_revisions[table],
                "conflicting_field_updates": conflicting_field_updates[table],
            }
            for table in sorted(TABLE_IDS)
        }
        if compare_parquet
        else {}
    )
    return {
        "status": (
            "passed"
            if not errors and all(value["status"] == "passed" for value in tables.values())
            else "failed"
        ),
        "data_dir": str(data_dir),
        "archive_root": str(root),
        "archives": len(files),
        "parsed_archives": len(files) - len(errors),
        "errors": errors,
        "parser_versions": sorted(parser_versions),
        "endpoints": dict(sorted(endpoint_stats.items())),
        "tables": tables,
    }
