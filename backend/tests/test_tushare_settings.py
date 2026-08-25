from __future__ import annotations

import os
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.api.settings import router
from app.services import tushare_history as th


def _app_client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def test_tushare_card_lists_plugin_and_key_lifecycle_is_redacted(tmp_path, monkeypatch):
    seen_keys: list[str] = []

    class Client:
        def __init__(self, key, **_kwargs):
            seen_keys.append(key)

        def request(self, api_name, _params):
            return SimpleNamespace(api_name=api_name, items=((1,),), fields=("sample",))

    monkeypatch.setattr(th, "TushareProxyClient", Client)
    client = _app_client(tmp_path, monkeypatch)
    secret = "secret-tushare-token"

    listed = client.get("/api/settings/data-sources")
    assert listed.status_code == 200
    tushare = next(item for item in listed.json()["plugins"] if item["name"] == "tushare")
    assert tushare["datasets"] == ["daily", "adj_factor", "minute"]
    assert tushare["runtime"] == "none"

    saved = client.put("/api/settings/tushare", json={"api_key": secret})
    assert saved.status_code == 200
    assert saved.json()["configured"] is True
    assert secret not in saved.text
    assert seen_keys == [secret]
    secret_path = tmp_path / "user_data" / "secrets.json"
    assert oct(os.stat(secret_path).st_mode & 0o777) == "0o600"

    switched = client.put(
        "/api/settings/preferences/data-providers",
        json={
            "daily_data_provider": "tushare",
            "adj_factor_provider": "same_as_daily",
            "minute_data_provider": "tushare",
        },
    )
    assert switched.status_code == 200
    assert switched.json()["daily_data_provider"] == "tushare"
    assert switched.json()["minute_data_provider"] == "tushare"

    probes = client.post("/api/settings/tushare/test")
    assert probes.status_code == 200
    assert all(item["status"] == "ok" for item in probes.json()["probes"].values())
    assert secret not in probes.text

    cleared = client.delete("/api/settings/tushare")
    assert cleared.status_code == 200
    assert cleared.json()["configured"] is False
    preferences = client.get("/api/settings/preferences").json()
    assert preferences["daily_data_provider"] == "tickflow"
    assert preferences["minute_data_provider"] == "tickflow"
    assert secret not in secret_path.read_text(encoding="utf-8")


def test_tushare_cannot_be_selected_without_key(tmp_path, monkeypatch):
    client = _app_client(tmp_path, monkeypatch)
    response = client.put(
        "/api/settings/preferences/data-providers",
        json={"daily_data_provider": "tushare"},
    )
    assert response.status_code == 400
    assert "配置 Tushare" in response.text


def test_invalid_tushare_key_is_not_persisted_or_exposed(tmp_path, monkeypatch):
    class RejectingClient:
        def __init__(self, _key, **_kwargs):
            pass

        def request(self, _api_name, _params):
            raise th.TusharePermissionError("provider rejected token")

    monkeypatch.setattr(th, "TushareProxyClient", RejectingClient)
    client = _app_client(tmp_path, monkeypatch)
    secret = "invalid-secret-token"
    response = client.put("/api/settings/tushare", json={"api_key": secret})

    assert response.status_code == 400
    assert secret not in response.text
    secret_path = tmp_path / "user_data" / "secrets.json"
    assert not secret_path.exists() or secret not in secret_path.read_text(encoding="utf-8")
