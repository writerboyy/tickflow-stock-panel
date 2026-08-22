from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from app.api import xiaoshi as xiaoshi_api
from app.services.xiaoshi import (
    XiaoshiClient,
    XiaoshiError,
    XiaoshiHistory,
    XiaoshiProtectionError,
    XiaoshiResourceManager,
    _filter_financial_rows,
    _sha256,
    _skill_package_sha256,
)


def _manifest(tmp_path: Path, *, skill_file: dict | None = None) -> dict:
    skill_file = skill_file or {
        "path": "SKILL.md",
        "size": 4,
        "sha256": _sha256(b"test"),
        "url": "https://api.shizixi.com/skill",
    }
    manifest = {
        "manifest_version": "test-1",
        "prompt_version": "test-1",
        "skill_version": "test-1",
        "prompt_url": "https://api.shizixi.com/prompt",
        "skill_url": "https://api.shizixi.com/skill",
        "api_schema_url": "https://api.shizixi.com/schema",
        "checksums": {
            "prompt_sha256": _sha256(b"test"),
            "skill_sha256": skill_file["sha256"],
            "api_schema_sha256": _sha256(b"test"),
            "skill_package_sha256": _skill_package_sha256([skill_file]),
        },
        "skill_files": [skill_file],
        "min_compatibility": {"agent_protocol": "1", "api_schema": "v3"},
    }
    return manifest


def test_resource_manager_verifies_and_publishes_atomically(tmp_path, monkeypatch):
    payload = b"test"
    manifest = _manifest(tmp_path)

    class Client:
        timeout = 1

        def get_manifest(self):
            return manifest

    manager = XiaoshiResourceManager(client=Client(), root=tmp_path)
    monkeypatch.setattr(manager, "_download_public_resource", lambda _url: payload)

    state = manager.refresh()

    assert state.active is True
    assert state.manifest_version == "test-1"
    assert (tmp_path / "active.json").exists()
    release = Path(state.resource_dir)
    assert (release / "prompt.txt").read_bytes() == payload
    assert (release / "skill" / "SKILL.md").read_bytes() == payload


def test_resource_manager_keeps_last_good_release_on_failed_update(tmp_path, monkeypatch):
    first = _manifest(tmp_path)
    second = _manifest(tmp_path)
    second["manifest_version"] = "test-2"
    second["prompt_version"] = "test-2"

    class Client:
        timeout = 1
        current = first

        def get_manifest(self):
            return self.current

    client = Client()
    manager = XiaoshiResourceManager(client=client, root=tmp_path)
    monkeypatch.setattr(manager, "_download_public_resource", lambda _url: b"test")
    assert manager.refresh().manifest_version == "test-1"

    client.current = second
    monkeypatch.setattr(manager, "_download_public_resource", lambda _url: b"bad")
    with pytest.raises(XiaoshiError, match="last verified release retained"):
        manager.refresh()
    assert manager.state().manifest_version == "test-1"


def test_resource_manager_does_not_download_unchanged_publication(tmp_path, monkeypatch):
    manifest = _manifest(tmp_path)

    class Client:
        timeout = 1

        def get_manifest(self):
            return manifest

    manager = XiaoshiResourceManager(client=Client(), root=tmp_path)
    downloads = []
    monkeypatch.setattr(
        manager,
        "_download_public_resource",
        lambda url: downloads.append(url) or b"test",
    )
    manager.refresh()
    first_count = len(downloads)
    manager.refresh()
    assert first_count == 3
    assert len(downloads) == first_count


def test_client_stops_on_controlled_429_without_retry():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={
                "detail": {
                    "error": "bulk_download_required",
                    "retry_after_seconds": 60,
                    "alternative": {"dataset": "cn-minute"},
                }
            },
            request=request,
        )

    client = XiaoshiClient(
        api_key="test-key",
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(XiaoshiProtectionError) as error:
        client.request_json("/api/v3/data/kline/600519")
    assert error.value.error_code == "bulk_download_required"
    assert error.value.retry_after_seconds == 60
    assert "test-key" not in str(error.value.context)


def test_client_error_context_redacts_secret_parameters():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "bad_request"}, request=request)

    client = XiaoshiClient(api_key="test-key", transport=httpx.MockTransport(handler))
    with pytest.raises(XiaoshiError) as error:
        client.request_json(
            "/api/v3/test",
            params={"api_key": "test-key", "code": "600519"},
        )
    assert error.value.context["parameters"]["api_key"] == "[REDACTED]"
    assert "test-key" not in json.dumps(error.value.context)
    assert error.value.context["error_fingerprint"]


def test_financial_as_reported_filters_available_at():
    as_of = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        {"symbol": "600519", "available_at": "2025-12-31T23:59:59+00:00"},
        {"symbol": "600519", "available_at": "2026-01-02T00:00:00+00:00"},
    ]
    assert _filter_financial_rows(rows, as_of) == [rows[0]]


def test_financial_as_reported_requires_timezone_aware_as_of():
    with pytest.raises(XiaoshiError, match="timezone-aware"):
        _filter_financial_rows([], datetime(2026, 1, 1))


def test_history_query_runs_catalog_schema_coverage_before_query():
    calls: list[list[str]] = []

    class Client:
        def get_history_manifest(self):
            return {
                "datasets": [
                    {
                        "dataset": "cn-daily",
                        "publication_scope": {"market": ["CN"]},
                        "coverage": {
                            "markets": ["CN"],
                            "first_date": "2020-01-01",
                            "last_date": "2026-12-31",
                        },
                    }
                ]
            }

    def runner(command, **_kwargs):
        calls.append(command)

        class Result:
            returncode = 0
            stdout = "[]"

        return Result()

    history = XiaoshiHistory(client=Client(), runner=runner)
    assert history.query(
        dataset="cn-daily",
        market="CN",
        since="2025-01-01",
        to="2025-01-02",
    ) == []
    assert [call[1] for call in calls] == ["catalog", "schema", "coverage", "query"]


def test_history_query_blocks_missing_manifest_coverage_before_cli():
    class Client:
        def get_history_manifest(self):
            return {"datasets": [{"dataset": "cn-daily"}]}

    history = XiaoshiHistory(client=Client(), runner=lambda *_args, **_kwargs: None)
    with pytest.raises(XiaoshiError, match="not covered"):
        history.query(dataset="cn-daily", market="CN", since="2025-01-01", to="2025-01-02")


def test_history_query_cannot_override_manifest_controlled_arguments():
    class Client:
        def get_history_manifest(self):
            return {
                "datasets": [
                    {
                        "dataset": "cn-daily",
                        "publication_scope": {"market": ["CN"]},
                        "coverage": {
                            "markets": ["CN"],
                            "first_date": "2020-01-01",
                            "last_date": "2026-12-31",
                        },
                    }
                ]
            }

    history = XiaoshiHistory(client=Client(), runner=lambda *_args, **_kwargs: None)
    with pytest.raises(XiaoshiError, match="override"):
        history.query(
            dataset="cn-daily",
            market="CN",
            since="2025-01-01",
            to="2025-01-02",
            extra_args=("--dataset", "global-daily"),
        )


def test_quote_route_url_encodes_symbol_and_forwards_identity(monkeypatch):
    captured: dict[str, object] = {}

    class Client:
        def request_json(self, path, *, params):
            captured["path"] = path
            captured["params"] = params
            return {"market": "US", "instrument": "stock", "symbol": "BRK/B"}

    monkeypatch.setattr(xiaoshi_api, "XiaoshiClient", Client)
    result = xiaoshi_api.quote("BRK/B", market="US", instrument="stock")

    assert captured == {
        "path": "/api/v3/market/quote/BRK%2FB",
        "params": {"market": "US", "instrument": "stock"},
    }
    assert result["market"] == "US"
