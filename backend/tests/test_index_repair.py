from __future__ import annotations

from datetime import date
import json
import os

import polars as pl
import pytest

from app.services import index_repair
from app.services.index_repair import (
    remove_index_repair_shadow,
    repair_confirmed_index_daily,
    repair_consensus_index_daily,
)


def _daily_row(trade_date: date, **updates) -> dict:
    return {
        "symbol": "399379.SZ",
        "date": trade_date,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": -10.0,
        "amount": 2e18,
        **updates,
    }


def _write_tables(tmp_path, rows_by_date: dict[date, list[dict]]) -> None:
    for trade_date, rows in rows_by_date.items():
        daily_path = tmp_path / "kline_index_daily" / f"date={trade_date}" / "part.parquet"
        enriched_path = tmp_path / "kline_index_enriched" / f"date={trade_date}" / "part.parquet"
        daily_path.parent.mkdir(parents=True)
        enriched_path.parent.mkdir(parents=True)
        daily = pl.DataFrame(rows)
        daily.write_parquet(daily_path)
        daily.with_columns(
            pl.col("close").alias("raw_close"),
            pl.col("high").alias("raw_high"),
            pl.col("low").alias("raw_low"),
        ).write_parquet(enriched_path)


def _write_evidence(tmp_path, row: dict, *, source_close: float | None = None):
    source = {field: row[field] for field in ("open", "high", "low", "close", "volume", "amount")}
    if source_close is not None:
        source["close"] = source_close
    evidence = {
        "rows": [{
            "symbol": row["symbol"],
            "date": row["date"].isoformat(),
            "status": "tickflow_anomaly_confirmed",
            "tickflow_anomalies": ["amount_overflow", "volume_negative"],
            "tdx_anomalies": [],
            "differing_valid_fields": [],
            "tickflow": source,
            "easy_tdx": {**source, "volume": 20.0, "amount": 2000.0},
        }],
    }
    corroboration = {
        "rows": [{
            "symbol": row["symbol"],
            "date": row["date"].isoformat(),
            "status": "tdx_baostock_confirm_tickflow_anomaly",
            "easy_tdx_baostock_differing_fields": [],
            "baostock_normalized": {**source, "volume": 20.0, "amount": 2000.0},
        }],
    }
    evidence_path = tmp_path / "easytdx.json"
    corroboration_path = tmp_path / "baostock.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    corroboration_path.write_text(json.dumps(corroboration), encoding="utf-8")
    return evidence_path, corroboration_path


def _repair(tmp_path, evidence_path, corroboration_path, *, apply=False, remaining=0):
    return repair_confirmed_index_daily(
        tmp_path,
        evidence_path,
        corroboration_path=corroboration_path,
        expected_rows=1,
        expected_dual_source_rows=1,
        expected_remaining_rows=remaining,
        apply=apply,
    )


def _write_consensus_evidence(tmp_path, row: dict):
    source = {field: row[field] for field in ("open", "high", "low", "close", "volume", "amount")}
    evidence = {
        "schema_version": 2,
        "sources": {
            "easy_tdx": {"status": "available"},
            "astock_data_eastmoney": {"status": "available"},
        },
        "astock_data": {"recipe_revision": "test"},
        "rows": [{
            "symbol": row["symbol"],
            "date": row["date"].isoformat(),
            "status": "replacement_confirmed",
            "changed_fields": ["amount", "volume"],
            "related_corrupt_fields": ["volume"],
            "tickflow": source,
            "replacement": {"volume": 20.0, "amount": 2000.0},
            "references": {
                "easy_tdx": {**source, "volume": 20.0, "amount": 2000.0},
                "astock_data_eastmoney": {**source, "volume": 20.0, "amount": 2000.0},
            },
            "field_consensus": {
                "volume": {"value": 20.0, "sources": ["easy_tdx", "astock_data_eastmoney"]},
                "amount": {"value": 2000.0, "sources": ["easy_tdx", "astock_data_eastmoney"]},
            },
        }],
    }
    path = tmp_path / "consensus.json"
    path.write_text(json.dumps(evidence), encoding="utf-8")
    return path


def test_consensus_index_repair_publishes_related_corrupt_fields(tmp_path):
    trade_date = date(2026, 7, 30)
    row = _daily_row(trade_date, volume=10.0)
    _write_tables(tmp_path, {trade_date: [row]})
    evidence_path = _write_consensus_evidence(tmp_path, row)

    published = repair_consensus_index_daily(
        tmp_path,
        evidence_path,
        expected_rows=1,
        expected_remaining_rows=0,
        apply=True,
    )

    repaired = pl.read_parquet(
        tmp_path / "kline_index_daily" / f"date={trade_date}" / "part.parquet"
    )
    assert repaired["volume"].to_list() == [20.0]
    assert repaired["amount"].to_list() == [2000.0]
    assert published["replacement_source"] == "multi_source_consensus"
    assert published["replacement_records"][0]["related_corrupt_fields"] == ["volume"]


def test_index_repair_validates_then_switches_both_tables_with_rollback(tmp_path):
    affected_date = date(2026, 7, 30)
    clean_date = date(2026, 7, 29)
    affected = _daily_row(affected_date)
    clean = _daily_row(clean_date, symbol="000001.SH", volume=10.0, amount=1000.0)
    _write_tables(tmp_path, {clean_date: [clean], affected_date: [affected]})
    evidence_path, corroboration_path = _write_evidence(tmp_path, affected)
    clean_path = tmp_path / "kline_index_daily" / f"date={clean_date}" / "part.parquet"

    validated = _repair(tmp_path, evidence_path, corroboration_path)

    assert validated["status"] == "validated"
    assert validated["confirmed_rows"] == 1
    assert validated["changed_field_values_per_table"] == 2
    assert validated["dual_source_rows"] == 1
    assert validated["replacement_records"][0]["evidence_level"] == "easy_tdx_baostock"
    assert pl.read_parquet(
        tmp_path / "kline_index_daily" / f"date={affected_date}" / "part.parquet"
    )["volume"].to_list() == [-10.0]
    remove_index_repair_shadow(validated["shadow_path"])

    published = _repair(tmp_path, evidence_path, corroboration_path, apply=True)

    assert published["status"] == "published"
    for table in ("kline_index_daily", "kline_index_enriched"):
        repaired = pl.read_parquet(tmp_path / table / f"date={affected_date}" / "part.parquet")
        assert repaired["volume"].to_list() == [20.0]
        assert repaired["amount"].to_list() == [2000.0]
        backup = tmp_path / f".{table}.pre-repair-{published['repair_id']}"
        original = pl.read_parquet(backup / f"date={affected_date}" / "part.parquet")
        assert original["volume"].to_list() == [-10.0]
        assert json.loads((tmp_path / table / "repair-manifest.json").read_text())["status"] == "published"
    daily_backup = tmp_path / f".kline_index_daily.pre-repair-{published['repair_id']}"
    assert os.stat(clean_path).st_ino == os.stat(
        daily_backup / f"date={clean_date}" / "part.parquet"
    ).st_ino


def test_index_repair_rejects_source_drift_without_publishing(tmp_path):
    trade_date = date(2026, 7, 30)
    row = _daily_row(trade_date)
    _write_tables(tmp_path, {trade_date: [row]})
    evidence_path, corroboration_path = _write_evidence(tmp_path, row, source_close=99.0)

    with pytest.raises(ValueError, match="source drift"):
        _repair(tmp_path, evidence_path, corroboration_path, apply=True)

    assert (tmp_path / "kline_index_daily").exists()
    assert (tmp_path / "kline_index_enriched").exists()
    assert not list(tmp_path.glob(".kline_index_daily.pre-repair-*"))


def test_index_repair_preserves_unconfirmed_anomalies(tmp_path):
    trade_date = date(2026, 7, 30)
    confirmed = _daily_row(trade_date)
    unresolved = _daily_row(
        trade_date,
        symbol="000902.SH",
        volume=-30.0,
        amount=3000.0,
    )
    _write_tables(tmp_path, {trade_date: [confirmed, unresolved]})
    evidence_path, corroboration_path = _write_evidence(tmp_path, confirmed)

    published = _repair(
        tmp_path,
        evidence_path,
        corroboration_path,
        apply=True,
        remaining=1,
    )

    repaired = pl.read_parquet(
        tmp_path / "kline_index_daily" / f"date={trade_date}" / "part.parquet"
    ).sort("symbol")
    assert published["remaining_anomaly_rows"] == 1
    assert repaired.filter(pl.col("symbol") == "000902.SH")["volume"].item() == -30.0


def test_index_repair_restores_both_tables_when_second_publish_fails(tmp_path, monkeypatch):
    trade_date = date(2026, 7, 30)
    row = _daily_row(trade_date)
    _write_tables(tmp_path, {trade_date: [row]})
    evidence_path, corroboration_path = _write_evidence(tmp_path, row)
    real_replace = index_repair.os.replace

    def fail_enriched_publish(source, target):
        source_path = index_repair.Path(source)
        target_path = index_repair.Path(target)
        if (
            source_path.name == "kline_index_enriched"
            and source_path.parent.name.startswith(".index-confirmed-repair-")
            and target_path == tmp_path / "kline_index_enriched"
        ):
            raise OSError("injected enriched publish failure")
        return real_replace(source, target)

    monkeypatch.setattr(index_repair.os, "replace", fail_enriched_publish)

    with pytest.raises(OSError, match="injected enriched publish failure"):
        _repair(tmp_path, evidence_path, corroboration_path, apply=True)

    for table in ("kline_index_daily", "kline_index_enriched"):
        restored = pl.read_parquet(tmp_path / table / f"date={trade_date}" / "part.parquet")
        assert restored["volume"].to_list() == [-10.0]
        assert restored["amount"].to_list() == [2e18]
    assert not list(tmp_path.glob(".kline_index_daily.pre-repair-*"))
    assert not list(tmp_path.glob(".kline_index_enriched.pre-repair-*"))
    assert not list(tmp_path.glob(".index-confirmed-repair-*"))


def test_index_repair_restores_both_tables_when_manifest_publish_fails(tmp_path, monkeypatch):
    trade_date = date(2026, 7, 30)
    row = _daily_row(trade_date)
    _write_tables(tmp_path, {trade_date: [row]})
    evidence_path, corroboration_path = _write_evidence(tmp_path, row)
    real_atomic_json = index_repair._atomic_json

    def fail_enriched_manifest(path, value):
        if path == tmp_path / "kline_index_enriched" / "repair-manifest.json":
            raise OSError("injected manifest publish failure")
        return real_atomic_json(path, value)

    monkeypatch.setattr(index_repair, "_atomic_json", fail_enriched_manifest)

    with pytest.raises(OSError, match="injected manifest publish failure"):
        _repair(tmp_path, evidence_path, corroboration_path, apply=True)

    for table in ("kline_index_daily", "kline_index_enriched"):
        restored = pl.read_parquet(tmp_path / table / f"date={trade_date}" / "part.parquet")
        assert restored["volume"].to_list() == [-10.0]
        assert restored["amount"].to_list() == [2e18]
        assert not (tmp_path / table / "repair-manifest.json").exists()
    assert not list(tmp_path.glob(".kline_index_daily.pre-repair-*"))
    assert not list(tmp_path.glob(".kline_index_enriched.pre-repair-*"))
    assert not list(tmp_path.glob(".index-confirmed-repair-*"))
