from __future__ import annotations

import json
import os
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.data import router


def _client(data_dir) -> TestClient:
    app = FastAPI()
    app.state.repo = SimpleNamespace(store=SimpleNamespace(data_dir=data_dir))
    app.include_router(router)
    return TestClient(app)


def _write_matrix(data_dir, run_id: str, payload: dict, modified_at: int) -> None:
    path = data_dir / "backfill_state" / "tushare_proxy" / run_id / "capability_matrix.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (modified_at, modified_at))


def test_tushare_capability_matrix_returns_explicit_empty_state(tmp_path):
    response = _client(tmp_path).get("/api/data/tushare-capability-matrix")

    assert response.status_code == 200
    assert response.json() == {
        "available": False,
        "generated_at": None,
        "schema_version": 1,
        "run_id": None,
        "run_ids": [],
        "run_count": 0,
        "source": "tushare_proxy",
        "runtime_source": "local_parquet_only",
        "history_start": None,
        "history_end": None,
        "datasets": {},
        "formal_publish": {},
        "legacy_phases": {},
    }


def test_tushare_capability_matrix_returns_latest_run_without_file_path(tmp_path):
    base = {
        "schema_version": 1,
        "source": "tushare_proxy",
        "runtime_source": "local_parquet_only",
        "datasets": {},
    }
    _write_matrix(tmp_path, "older", {**base, "run_id": "older"}, 1_700_000_000)
    _write_matrix(
        tmp_path,
        "latest",
        {
            **base,
            "run_id": "latest",
            "history_start": "2010-01-01",
            "history_end": "2026-08-04",
            "datasets": {
                "daily_basic": {
                    "status": "published",
                    "staged_rows": 10,
                    "published_rows": 3,
                    "symbols": 2,
                    "factor_input": True,
                    "field_non_null_rate": {"pe_ttm": 0.8},
                }
            },
            "token": "must-not-be-returned",
        },
        1_800_000_000,
    )

    response = _client(tmp_path).get("/api/data/tushare-capability-matrix")

    assert response.status_code == 200
    payload = response.json()
    assert payload["available"] is True
    assert payload["run_id"] == "latest"
    assert payload["history_start"] == "2010-01-01"
    assert payload["datasets"]["daily_basic"]["factor_input"] is True
    assert "token" not in payload
    assert str(tmp_path) not in response.text


def test_tushare_capability_matrix_prefers_latest_populated_run(tmp_path):
    populated = {
        "schema_version": 1,
        "run_id": "published",
        "datasets": {"daily": {"status": "published", "staged_rows": 1}},
    }
    audit_only = {
        "schema_version": 1,
        "run_id": "weekly-audit",
        "datasets": {},
    }
    _write_matrix(tmp_path, "published", populated, 1_700_000_000)
    _write_matrix(tmp_path, "weekly-audit", audit_only, 1_800_000_000)

    response = _client(tmp_path).get("/api/data/tushare-capability-matrix")

    assert response.status_code == 200
    assert response.json()["run_id"] == "published"
    assert list(response.json()["datasets"]) == ["daily"]


def test_tushare_capability_matrix_merges_latest_dataset_state_across_runs(tmp_path):
    _write_matrix(
        tmp_path,
        "full",
        {
            "run_id": "full",
            "history_start": "2010-01-01",
            "history_end": "2025-12-31",
            "datasets": {
                "daily": {"status": "published", "staged_rows": 10},
                "income": {"status": "failed", "staged_rows": 0},
            },
        },
        1_700_000_000,
    )
    _write_matrix(
        tmp_path,
        "income-retry",
        {
            "run_id": "income-retry",
            "history_start": "2025-01-01",
            "history_end": "2026-08-04",
            "datasets": {"income": {"status": "published", "staged_rows": 3}},
        },
        1_800_000_000,
    )

    response = _client(tmp_path).get("/api/data/tushare-capability-matrix")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == "income-retry"
    assert payload["run_ids"] == ["full", "income-retry"]
    assert payload["run_count"] == 2
    assert payload["history_start"] == "2010-01-01"
    assert payload["history_end"] == "2026-08-04"
    assert payload["datasets"]["daily"]["staged_rows"] == 10
    assert payload["datasets"]["income"]["status"] == "published"


def test_tushare_capability_matrix_rejects_malformed_latest_run(tmp_path):
    _write_matrix(tmp_path, "broken", {"run_id": "broken", "datasets": []}, 1_800_000_000)

    response = _client(tmp_path).get("/api/data/tushare-capability-matrix")

    assert response.status_code == 500
    assert response.json()["detail"] == "latest Tushare capability matrix has an invalid schema"
