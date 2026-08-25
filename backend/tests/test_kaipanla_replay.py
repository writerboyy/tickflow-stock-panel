from __future__ import annotations

import gzip
import json
from datetime import date

import polars as pl

from app.plugins.kaipanla.parsers import (
    parse_large_order_statistics,
    parse_regulatory_anomaly,
    parse_shareholder_count_changes,
)
from app.plugins.kaipanla.replay import replay_archives
from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    FUNDS_TABLE,
    REGULATORY_TABLE,
    SHAREHOLDER_COUNT_TABLE,
    archive_raw,
    atomic_upsert,
    atomic_upsert_records,
    ensure_configs,
)
from app.services.ingestion_manifest import stable_content_hash, update_ingestion_manifest


def test_replay_accepts_legacy_response_and_matches_parquet(tmp_path):
    payload = {
        "DateList": [],
        "List": [{
            "Day": "20260731",
            "StockID": "600126",
            "Name": "杭钢股份",
            "LTZB": 1,
            "CMJZ": 2,
            "JSQBH": 3,
            "UpdateDay": "20260801",
            "IsNew": 1,
        }],
    }
    raw = (
        tmp_path / "ext_data" / "_kaipanla_raw" / "date=2026-07-31"
        / "shareholder_count_changes"
    )
    raw.mkdir(parents=True)
    (raw / "120000-000000-offset-0.json").write_text(
        json.dumps({"endpoint": "/shareholder_count_changes", "response": payload}),
        encoding="utf-8",
    )
    rows = parse_shareholder_count_changes(payload)
    atomic_upsert_records(
        tmp_path,
        SHAREHOLDER_COUNT_TABLE,
        date.fromisoformat(rows[0]["report_date"]),
        rows,
        ("symbol",),
    )

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
    assert result["archives"] == 1
    assert result["tables"][SHAREHOLDER_COUNT_TABLE]["field_mismatches"] == 0


def test_replay_accepts_current_envelope_and_classifies_endpoint_30_empty(tmp_path):
    ensure_configs(tmp_path)
    archive_raw(tmp_path, 30, date(2026, 7, 31), {"info": []})

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
    assert result["tables"][AUCTION_TABLE]["status"] == "passed"
    assert result["tables"][AUCTION_TABLE]["replay_rows"] == 0
    assert result["endpoints"]["30"] == {
        "archives": 1,
        "parsed_rows": 0,
        "valid_empty": 1,
        "errors": 0,
    }


def test_replay_rejects_tampered_content_hash(tmp_path):
    raw = tmp_path / "ext_data" / "_kaipanla_raw" / "date=2026-07-31" / "30"
    raw.mkdir(parents=True)
    payload = {"info": []}
    with gzip.open(raw / "120000-000000-page-0.json.gz", "wt", encoding="utf-8") as handle:
        json.dump(
            {
                "response": payload,
                "content_hash": stable_content_hash({"info": [["tampered"]]}),
                "parser_version": "kaipanla_v1",
            },
            handle,
        )

    result = replay_archives(tmp_path)

    assert result["status"] == "failed"
    assert result["parsed_archives"] == 0
    assert "content hash mismatch" in result["errors"][0]["error"]


def test_replay_fails_when_parquet_schema_drops_a_declared_field(tmp_path):
    payload = {
        "DateList": [],
        "List": [{
            "Day": "20260731",
            "StockID": "600126",
            "Name": "杭钢股份",
            "LTZB": 1,
            "CMJZ": 2,
            "JSQBH": 3,
            "UpdateDay": "20260801",
            "IsNew": 1,
        }],
    }
    archive_raw(tmp_path, "shareholder_count_changes", date(2026, 7, 31), payload)
    rows = parse_shareholder_count_changes(payload)
    atomic_upsert_records(
        tmp_path,
        SHAREHOLDER_COUNT_TABLE,
        date(2026, 7, 31),
        rows,
        ("symbol",),
    )
    parquet = (
        tmp_path / "ext_data" / SHAREHOLDER_COUNT_TABLE
        / "timeseries" / "date=2026-07-31" / "part.parquet"
    )
    pl.read_parquet(parquet).drop("chip_concentration").write_parquet(parquet)

    result = replay_archives(tmp_path)

    table = result["tables"][SHAREHOLDER_COUNT_TABLE]
    assert result["status"] == "failed"
    assert table["status"] == "failed"
    assert table["schema_missing_fields"] == ["chip_concentration"]
    assert table["missing_value_fields"] == 1


def test_replay_fails_on_duplicate_parquet_primary_key(tmp_path):
    payload = {
        "DateList": [],
        "List": [{
            "Day": "20260731",
            "StockID": "600126",
            "Name": "杭钢股份",
            "LTZB": 1,
            "CMJZ": 2,
            "JSQBH": 3,
            "UpdateDay": "20260801",
            "IsNew": 1,
        }],
    }
    archive_raw(tmp_path, "shareholder_count_changes", date(2026, 7, 31), payload)
    rows = parse_shareholder_count_changes(payload)
    atomic_upsert_records(
        tmp_path,
        SHAREHOLDER_COUNT_TABLE,
        date(2026, 7, 31),
        rows,
        ("symbol",),
    )
    parquet = (
        tmp_path / "ext_data" / SHAREHOLDER_COUNT_TABLE
        / "timeseries" / "date=2026-07-31" / "part.parquet"
    )
    frame = pl.read_parquet(parquet)
    pl.concat([frame, frame]).write_parquet(parquet)

    result = replay_archives(tmp_path)

    table = result["tables"][SHAREHOLDER_COUNT_TABLE]
    assert result["status"] == "failed"
    assert table["duplicate_parquet_keys"] == 1


def test_replay_excludes_archives_from_incomplete_fail_closed_fund_component(tmp_path):
    ensure_configs(tmp_path)
    payload = {
        "Date": ["20260731"],
        "TDJL": [30],
        "DDJL": [20],
        "ZDJL": [10],
        "XDJL": [-5],
    }
    archive_raw(
        tmp_path,
        "fund_large_order_statistics",
        date(2026, 7, 31),
        payload,
        "600126",
    )
    update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "fund_large_order_statistics",
        "2026-07-31",
        status="incomplete",
        expected_batches=["600126", "600127"],
        failed_batches=["600127"],
        published_rows=0,
    )

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
    assert result["tables"][FUNDS_TABLE]["replay_rows"] == 0
    assert result["endpoints"]["fund_large_order_statistics"] == {
        "archives": 1,
        "parsed_rows": 1,
        "valid_empty": 0,
        "errors": 0,
        "excluded_from_reconciliation": 1,
    }


def test_replay_excludes_fund_archives_outside_completed_expected_batches(tmp_path):
    ensure_configs(tmp_path)
    trade_date = date(2026, 7, 31)
    payload = {
        "Date": ["20260731"],
        "TDJL": [30],
        "DDJL": [20],
        "ZDJL": [10],
        "XDJL": [-5],
    }
    for code in ("600126", "000003"):
        archive_raw(
            tmp_path,
            "fund_large_order_statistics",
            trade_date,
            payload,
            code,
        )
    atomic_upsert(
        tmp_path,
        FUNDS_TABLE,
        trade_date,
        [parse_large_order_statistics(payload, "600126", trade_date)],
    )
    update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "fund_large_order_statistics",
        trade_date.isoformat(),
        status="complete",
        expected_batches=["600126"],
        failed_batches=[],
        published_rows=1,
    )

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
    assert result["tables"][FUNDS_TABLE]["replay_rows"] == 1
    assert result["endpoints"]["fund_large_order_statistics"][
        "excluded_from_reconciliation"
    ] == 1


def test_replay_merges_duplicate_regulatory_symbols_like_atomic_upsert(tmp_path):
    ensure_configs(tmp_path)
    payload = {
        "List": [
            ["003032", "传智教育", 1, "10日内2次异动个股", 3, 5, 9.77, 0.83, 9.69, 0, 18.98],
            ["003032", "传智教育", 1, "10日内偏离值临近100%", 4, 8, 10.59, 9.29, 9.69, 0, 83.1],
        ]
    }
    trade_date = date(2026, 8, 3)
    archive_raw(tmp_path, 109, trade_date, payload, "post")
    rows = []
    for parsed in parse_regulatory_anomaly(payload):
        rows.append({
            **{key: parsed.get(key) for key in ("symbol", "code", "name")},
            **{
                f"post_{key}": value
                for key, value in parsed.items()
                if key not in {"symbol", "code", "name"}
            },
        })
    atomic_upsert(tmp_path, REGULATORY_TABLE, trade_date, rows)

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
    table = result["tables"][REGULATORY_TABLE]
    assert table["replay_rows"] == 1
    assert table["replay_revisions"] == 1
    assert table["field_mismatches"] == 0
