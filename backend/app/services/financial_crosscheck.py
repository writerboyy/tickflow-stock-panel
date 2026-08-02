"""Read-only third-party corroboration for conflicting TickFlow financial rows."""

from __future__ import annotations

from collections.abc import Callable
import math
from pathlib import Path
from typing import Any

import polars as pl

from app.services.backup_data_sources import (
    astock_data_source_metadata,
    fetch_astock_data_sina_financial_reference,
)


_KEYS = ("symbol", "period_end", "announce_date")
_TABLES = ("metrics", "income", "balance_sheet", "cash_flow")
_FetchFinancialReference = Callable[
    [list[tuple[str, str]]], dict[tuple[str, str], dict[str, float]]
]


def _same(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-5, abs_tol=1e-6)
    except (TypeError, ValueError):
        return left == right


def _conflicting_groups(frame: pl.DataFrame) -> list[pl.DataFrame]:
    if frame.is_empty() or not set(_KEYS) <= set(frame.columns):
        return []
    groups = frame.group_by(_KEYS).agg(pl.len().alias("_rows")).filter(pl.col("_rows") > 1)
    conflicts: list[pl.DataFrame] = []
    for key in groups.iter_rows(named=True):
        rows = frame.filter(pl.all_horizontal(pl.col(field) == key[field] for field in _KEYS))
        if rows.unique().height > 1:
            conflicts.append(rows)
    return conflicts


def _different_fields(rows: pl.DataFrame) -> list[str]:
    fields: list[str] = []
    for column in rows.columns:
        if column in _KEYS:
            continue
        values = rows[column].to_list()
        if any(not _same(values[0], value) for value in values[1:]):
            fields.append(column)
    return sorted(fields)


def crosscheck_financial_conflicts(
    data_dir: Path,
    *,
    fetcher: _FetchFinancialReference = fetch_astock_data_sina_financial_reference,
    frames: dict[str, pl.DataFrame] | None = None,
) -> dict[str, Any]:
    """Corroborate values but never select a PIT revision or write financial data."""
    data_dir = Path(data_dir)
    table_groups: list[tuple[str, pl.DataFrame]] = []
    for table in _TABLES:
        frame = (frames or {}).get(table)
        if frame is None:
            path = data_dir / "financials" / table / "part.parquet"
            frame = pl.read_parquet(path) if path.exists() else pl.DataFrame()
        table_groups.extend((table, group) for group in _conflicting_groups(frame))

    request_keys = [
        (str(group["symbol"][0]), str(group["period_end"][0]))
        for _, group in table_groups
    ]
    try:
        references = fetcher(request_keys)
        source_status = "available"
        error_code = None
    except Exception as exc:  # noqa: BLE001
        references = {}
        source_status = "unavailable"
        error_code = type(exc).__name__

    results: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for table, rows in table_groups:
        symbol = str(rows["symbol"][0])
        period_end = str(rows["period_end"][0])
        announce_date = str(rows["announce_date"][0])
        differing_fields = _different_fields(rows)
        reference = references.get((symbol, period_end), {})
        field_matches: dict[str, dict[str, Any]] = {}
        for field in differing_fields:
            if field not in reference:
                continue
            matches = [
                index
                for index, value in enumerate(rows[field].to_list())
                if _same(value, reference[field])
            ]
            field_matches[field] = {
                "reference_value": reference[field],
                "matching_row_indexes": matches,
            }
        covered = set(field_matches)
        if not reference or not covered:
            status = "reference_unavailable"
        elif covered == set(differing_fields) and all(
            len(value["matching_row_indexes"]) == 1 for value in field_matches.values()
        ):
            status = "reference_corroborated_revision_unverified"
        else:
            status = "partial_reference_corroboration"
        counts[status] = counts.get(status, 0) + 1
        results.append({
            "table": table,
            "symbol": symbol,
            "period_end": period_end,
            "announce_date": announce_date,
            "status": status,
            "differing_fields": differing_fields,
            "candidate_values": {
                field: rows[field].to_list() for field in differing_fields
            },
            "field_matches": field_matches,
            "missing_reference_fields": sorted(set(differing_fields) - covered),
            "can_repair": False,
            "blocked_reason": "reference_has_no_announce_revision_metadata",
        })
    source = {
        "status": source_status,
        "provider": "A-Stock-Data",
        "backend": "sina_finance",
        "supports_announce_revision": False,
    }
    if error_code:
        source["error_code"] = error_code
    return {
        "schema_version": 1,
        "status": "blocked" if results else "no_conflicts",
        "conflict_groups": len(results),
        "repairable_groups": 0,
        "status_counts": dict(sorted(counts.items())),
        "source": source,
        "astock_data": astock_data_source_metadata(),
        "rows": results,
    }
