"""Multi-source evidence for canonical index bars rejected by quality gates."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
import math
from typing import Any

import polars as pl

from app.services.backup_data_sources import fetch_baostock_index_daily
from app.services.index_crosscheck import (
    _FIELDS,
    _PRICE_FIELDS,
    _anomalies,
    _same_value,
    fetch_easy_tdx_index_daily,
)


_FetchIndexDaily = Callable[[list[str], date, date], pl.DataFrame]
_REPAIRABLE_FIELDS = {"volume", "amount"}
_UINT32_MODULUS = 2**32


@dataclass(frozen=True)
class IndexReferenceSource:
    name: str
    fields: tuple[str, ...]
    fetcher: _FetchIndexDaily
    provider: str
    backend: str


def default_index_reference_sources() -> tuple[IndexReferenceSource, ...]:
    return (
        IndexReferenceSource("easy_tdx", _FIELDS, fetch_easy_tdx_index_daily, "EasyTDX", "tdx"),
        IndexReferenceSource(
            "baostock", _FIELDS, fetch_baostock_index_daily, "BaoStock", "baostock"
        ),
    )


def _validate_source_frame(frame: pl.DataFrame, source: IndexReferenceSource) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    required = {"symbol", "date", *source.fields}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{source.name} index bars missing fields: {', '.join(missing)}")
    return (
        frame.select(
            pl.col("symbol").cast(pl.String),
            pl.col("date").cast(pl.Date),
            *(pl.col(field).cast(pl.Float64, strict=True) for field in source.fields),
        )
        .unique(["symbol", "date"], keep="last")
        .sort(["symbol", "date"])
    )


def _source_row_valid(row: dict[str, Any], fields: tuple[str, ...]) -> bool:
    for field in fields:
        value = row.get(field)
        if value is None or not math.isfinite(float(value)):
            return False
        if field in _PRICE_FIELDS and float(value) <= 0:
            return False
        if field in {"volume", "amount"} and float(value) < 0:
            return False
    if all(field in fields for field in _PRICE_FIELDS):
        values = {field: float(row[field]) for field in _PRICE_FIELDS}
        if values["high"] < max(values.values()) or values["low"] > min(values.values()):
            return False
    return True


def _field_consensus(
    field: str,
    values: list[tuple[str, float]],
) -> tuple[float, list[str]] | None:
    clusters: list[list[tuple[str, float]]] = []
    for item in values:
        for cluster in clusters:
            if _same_value(field, cluster[0][1], item[1]):
                cluster.append(item)
                break
        else:
            clusters.append([item])
    if not clusters:
        return None
    clusters.sort(key=lambda cluster: (-len(cluster), [source for source, _ in cluster]))
    if len(clusters[0]) < 2:
        return None
    if len(clusters) > 1 and len(clusters[0]) == len(clusters[1]):
        return None
    # Source order is stable and selects an observed value instead of synthesizing an average.
    return clusters[0][0][1], [source for source, _ in clusters[0]]


def _anomaly_fields(anomalies: list[str]) -> set[str]:
    return {value.split("_", 1)[0] for value in anomalies}


def crosscheck_index_daily_consensus(
    tickflow_rows: pl.DataFrame,
    *,
    sources: tuple[IndexReferenceSource, ...] | None = None,
) -> dict[str, Any]:
    """Build field-level consensus without publishing or mutating canonical data."""
    sources = sources or default_index_reference_sources()
    if tickflow_rows.is_empty():
        return {
            "schema_version": 2,
            "status": "no_anomalies",
            "requested_rows": 0,
            "confirmed_rows": 0,
            "status_counts": {},
            "sources": {},
            "rows": [],
        }
    required = {"symbol", "date", *_FIELDS}
    missing = sorted(required - set(tickflow_rows.columns))
    if missing:
        raise ValueError(f"TickFlow index bars missing fields: {', '.join(missing)}")
    source_rows = tickflow_rows.select("symbol", "date", *_FIELDS).sort(["symbol", "date"])
    symbols = source_rows["symbol"].unique().sort().to_list()
    start_date = source_rows["date"].min()
    end_date = source_rows["date"].max()

    source_maps: dict[str, dict[tuple[str, date], dict[str, Any]]] = {}
    source_meta: dict[str, dict[str, Any]] = {}
    for source in sources:
        try:
            frame = _validate_source_frame(source.fetcher(symbols, start_date, end_date), source)
            mapping = {
                (str(row["symbol"]), row["date"]): row
                for row in frame.iter_rows(named=True)
                if _source_row_valid(row, source.fields)
            }
            source_maps[source.name] = mapping
            source_meta[source.name] = {
                "status": "available" if mapping else "no_coverage",
                "provider": source.provider,
                "backend": source.backend,
                "fields": list(source.fields),
                "fetched_rows": sum(
                    1
                    for row in source_rows.iter_rows(named=True)
                    if (str(row["symbol"]), row["date"]) in mapping
                ),
                "accepted_rows": 0,
            }
        except Exception as exc:  # noqa: BLE001
            source_maps[source.name] = {}
            source_meta[source.name] = {
                "status": "unavailable",
                "provider": source.provider,
                "backend": source.backend,
                "fields": list(source.fields),
                "fetched_rows": 0,
                "accepted_rows": 0,
                "error_code": type(exc).__name__,
            }

    results: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for tickflow in source_rows.iter_rows(named=True):
        key = str(tickflow["symbol"]), tickflow["date"]
        anomalies = _anomalies(tickflow)
        anomaly_fields = _anomaly_fields(anomalies)
        references: dict[str, dict[str, float]] = {}
        rejected_references: dict[str, list[str]] = {}
        for source in sources:
            row = source_maps[source.name].get(key)
            if row is not None:
                differing_valid_prices = [
                    field
                    for field in sorted(_PRICE_FIELDS)
                    if field not in anomaly_fields
                    and field in source.fields
                    and not _same_value(field, tickflow[field], row[field])
                ]
                if differing_valid_prices:
                    rejected_references[source.name] = differing_valid_prices
                    continue
                references[source.name] = {
                    field: float(row[field]) for field in source.fields
                }
                source_meta[source.name]["accepted_rows"] += 1

        consensus: dict[str, dict[str, Any]] = {}
        for field in _FIELDS:
            values = [
                (source.name, references[source.name][field])
                for source in sources
                if source.name in references and field in references[source.name]
            ]
            agreed = _field_consensus(field, values)
            if agreed is not None:
                value, agreeing_sources = agreed
                consensus[field] = {"value": value, "sources": agreeing_sources}

        volume = float(tickflow["volume"])
        if "volume" not in consensus and "volume" in anomaly_fields and volume < 0:
            recovered = volume + _UINT32_MODULUS
            if volume.is_integer() and recovered >= 0:
                confirming_sources = [
                    source.name
                    for source in sources
                    if source.name in references
                    and "volume" in references[source.name]
                    and _same_value("volume", references[source.name]["volume"], recovered)
                ]
                if confirming_sources:
                    consensus["volume"] = {
                        "value": recovered,
                        "sources": [confirming_sources[0], "tickflow_uint32_recovery"],
                        "evidence_kind": "external_source_plus_exact_uint32_recovery",
                    }

        missing_consensus = sorted(
            field for field in anomaly_fields if field not in consensus
        )
        related_fields = sorted(
            field
            for field in _REPAIRABLE_FIELDS - anomaly_fields
            if field in consensus and not _same_value(field, tickflow[field], consensus[field]["value"])
        )
        unsupported_anomalies = sorted(anomaly_fields - _REPAIRABLE_FIELDS)
        unresolved_valid_fields = sorted(
            field
            for field in _PRICE_FIELDS
            if field not in anomaly_fields
            and field in consensus
            and not _same_value(field, tickflow[field], consensus[field]["value"])
        )
        changed_fields = sorted((anomaly_fields & _REPAIRABLE_FIELDS) | set(related_fields))
        if not references:
            status = "reference_unavailable"
        elif missing_consensus:
            status = "insufficient_consensus"
        elif unsupported_anomalies or unresolved_valid_fields:
            status = "source_conflict"
        elif changed_fields:
            status = "replacement_confirmed"
        else:
            status = "no_replacement"
        counts[status] = counts.get(status, 0) + 1
        results.append({
            "symbol": key[0],
            "date": key[1].isoformat(),
            "status": status,
            "tickflow_anomalies": anomalies,
            "missing_consensus_fields": missing_consensus,
            "unsupported_anomaly_fields": unsupported_anomalies,
            "unresolved_valid_fields": unresolved_valid_fields,
            "related_corrupt_fields": related_fields,
            "changed_fields": changed_fields,
            "tickflow": {field: tickflow[field] for field in _FIELDS},
            "references": references,
            "rejected_references": rejected_references,
            "field_consensus": consensus,
            "replacement": {
                field: consensus[field]["value"]
                for field in changed_fields
                if field in consensus
            },
        })

    confirmed = counts.get("replacement_confirmed", 0)
    return {
        "schema_version": 2,
        "status": "complete" if confirmed == source_rows.height else "partial",
        "requested_rows": source_rows.height,
        "confirmed_rows": confirmed,
        "status_counts": dict(sorted(counts.items())),
        "sources": source_meta,
        "derived_evidence": {
            "tickflow_uint32_recovery": "negative_volume_plus_2^32_exact_match"
        },
        "rows": results,
    }


def consensus_summary(result: dict[str, Any]) -> str:
    counts = result.get("status_counts") or {}
    detail = ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
    sources = result.get("sources") or {}
    available = ",".join(
        name for name, value in sources.items() if value.get("status") == "available"
    )
    return (
        f"status={result.get('status', 'unknown')}, "
        f"confirmed={result.get('confirmed_rows', 0)}/{result.get('requested_rows', 0)}, "
        f"available={available or 'none'}"
        + (f", {detail}" if detail else "")
    )
