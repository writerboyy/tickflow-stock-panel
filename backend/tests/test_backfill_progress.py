from __future__ import annotations

from datetime import datetime, timezone
import json

from scripts.serve_backfill_progress import INDEX_HTML, build_snapshot


def _write_manifest(tmp_path, *, updated_at="2026-08-07T10:00:00+00:00"):
    manifest = {
        "run_id": "minute-2020",
        "status": "staging",
        "history_start": "2020-01-01",
        "history_end": "2026-08-07",
        "updated_at": updated_at,
        "phases_state": {
            "universe": {"stock_count": 5, "etf_count": 3},
            "adjustment": {
                "status": "completed",
                "items": {
                    "000001.SZ": {"status": "completed"},
                    "000002.SZ": {"status": "completed"},
                },
            },
            "stock_minute": {
                "status": "running",
                "items": {
                    "000001.SZ": {"status": "completed"},
                    "000002.SZ": {"status": "completed"},
                    "000003.SZ": {"status": "running"},
                    "000004.SZ": {"status": "pending"},
                },
            },
            "etf_minute": {
                "status": "pending",
                "items": {"510300.SH": {"status": "failed"}},
            },
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_build_snapshot_counts_unseen_items_as_pending(tmp_path):
    snapshot = build_snapshot(
        _write_manifest(tmp_path),
        now=datetime(2026, 8, 7, 10, 0, 30, tzinfo=timezone.utc),
    )

    assert snapshot["minute"] == {
        "completed": 2,
        "failed": 1,
        "percent": 25.0,
        "running": 1,
        "total": 8,
    }
    assert snapshot["phases"]["stock_minute"]["pending"] == 2
    assert snapshot["phases"]["etf_minute"]["pending"] == 2
    assert snapshot["activity"] == "failed"
    assert snapshot["age_seconds"] == 30


def test_build_snapshot_marks_old_running_batch_as_waiting(tmp_path):
    path = _write_manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["phases_state"]["etf_minute"]["items"] = {}
    path.write_text(json.dumps(manifest))

    snapshot = build_snapshot(
        path,
        now=datetime(2026, 8, 7, 10, 3, tzinfo=timezone.utc),
    )

    assert snapshot["activity"] == "waiting"
    assert snapshot["activity_label"] == "等待接口响应"


def test_dashboard_has_live_progress_landmarks():
    assert 'role="progressbar"' in INDEX_HTML
    assert "setInterval(refresh, 2000)" in INDEX_HTML
    assert "分钟数据总进度" in INDEX_HTML
