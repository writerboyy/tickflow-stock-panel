from __future__ import annotations

import gzip
import json
from datetime import date

from app.plugins.kaipanla.parsers import parse_shareholder_count_changes
from app.plugins.kaipanla.replay import replay_archives
from app.plugins.kaipanla.storage import (
    SHAREHOLDER_COUNT_TABLE,
    archive_raw,
    atomic_upsert_records,
)
from app.services.ingestion_manifest import stable_content_hash


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
    archive_raw(tmp_path, 30, date(2026, 7, 31), {"info": []})

    result = replay_archives(tmp_path)

    assert result["status"] == "passed"
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
