from __future__ import annotations

import json

from app.services.ingestion_health import summarize_ingestion_health
from app.services.ingestion_manifest import (
    record_ingestion_batch,
    update_ingestion_manifest,
)


def test_ingestion_health_reports_no_data_without_manifests(tmp_path):
    result = summarize_ingestion_health(tmp_path)

    assert result["status"] == "no_data"
    assert result["latest_datasets"] == 0


def test_ingestion_health_uses_latest_snapshot_and_aggregates_batches(tmp_path):
    update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "dataset_a",
        "2026-08-01",
        status="incomplete",
        failed_batches=["old"],
    )
    record_ingestion_batch(
        tmp_path,
        "kaipanla",
        "dataset_a",
        "2026-08-02",
        "page-000",
        status="completed",
        row_count=10,
    )
    update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "dataset_a",
        "2026-08-02",
        status="published",
        published_rows=10,
    )

    result = summarize_ingestion_health(tmp_path)

    assert result["status"] == "healthy"
    assert result["latest_datasets"] == 1
    assert result["published_rows"] == 10
    assert result["datasets"][0]["logical_snapshot"] == "2026-08-02"
    assert result["batch_status_counts"] == {"completed": 1}


def test_ingestion_health_fails_closed_on_latest_failed_batch(tmp_path):
    record_ingestion_batch(
        tmp_path,
        "easy_tdx",
        "ext_tdx_margin",
        "2026-08-02",
        "00000",
        status="source_error",
        error_code="TimeoutError",
    )
    update_ingestion_manifest(
        tmp_path,
        "easy_tdx",
        "ext_tdx_margin",
        "2026-08-02",
        status="incomplete",
        failed_batches=["00000"],
    )

    result = summarize_ingestion_health(tmp_path, sources={"easy_tdx"})

    assert result["status"] == "unhealthy"
    assert result["issues"][0]["failed_batches"] == ["00000"]
    assert result["batch_status_counts"] == {"source_error": 1}


def test_ingestion_health_fails_closed_on_corrupt_manifest(tmp_path):
    path = (
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "dataset_a" / "2026-08-02.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text("{not-json", encoding="utf-8")

    result = summarize_ingestion_health(tmp_path)

    assert result["status"] == "unhealthy"
    assert result["invalid_manifests"][0]["path"] == (
        "kaipanla/dataset_a/2026-08-02.json"
    )


def test_ingestion_health_ignores_corrupt_superseded_snapshot(tmp_path):
    old_path = (
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "dataset_a" / "2026-08-01.json"
    )
    old_path.parent.mkdir(parents=True)
    old_path.write_text("{not-json", encoding="utf-8")
    update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "dataset_a",
        "2026-08-02",
        status="complete",
    )

    result = summarize_ingestion_health(tmp_path)

    assert result["status"] == "healthy"
    assert result["invalid_manifests"] == []
    assert result["manifest_files"] == 2


def test_ingestion_health_fails_closed_on_manifest_path_mismatch(tmp_path):
    manifest = update_ingestion_manifest(
        tmp_path,
        "kaipanla",
        "dataset_a",
        "2026-08-02",
        status="complete",
    )
    manifest["dataset"] = "dataset_b"
    path = (
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "dataset_a" / "2026-08-02.json"
    )
    path.write_text(json.dumps(manifest), encoding="utf-8")

    result = summarize_ingestion_health(tmp_path)

    assert result["status"] == "unhealthy"
    assert "path mismatch: dataset" in result["invalid_manifests"][0]["error"]
