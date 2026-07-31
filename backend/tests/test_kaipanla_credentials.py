from __future__ import annotations

from urllib.parse import parse_qs

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.plugins.kaipanla.client import KaipanlaClient
from app.plugins.kaipanla.credentials import (
    KaipanlaCredentials,
    load_credentials,
    parse_authorized_url,
)
from app.plugins.kaipanla.router import router


AUTHORIZED_URL = (
    "https://apphwhq.longhuvip.com/w1/api/index.php?"
    "Token=test-token-123456&UserID=2675923&DeviceID=device-id-123456&"
    "PhoneOSNew=1&VerSion=5.23.0.4&apiv=w44&Type=4"
)


def test_authorized_url_requires_exact_whitelist_and_connection_fields():
    credentials = parse_authorized_url(AUTHORIZED_URL)
    assert credentials.token == "test-token-123456"
    assert credentials.userid == "2675923"

    with pytest.raises(ValueError, match="白名单"):
        parse_authorized_url(AUTHORIZED_URL.replace("apphwhq.longhuvip.com", "example.com"))
    with pytest.raises(ValueError, match="缺少连接字段: apiv"):
        parse_authorized_url(AUTHORIZED_URL.replace("&apiv=w44", ""))


def test_connection_api_persists_fields_only_and_returns_masked_status(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "data_dir", tmp_path)
    app = FastAPI()
    app.include_router(router)

    class Collector:
        triggered = 0
        stopped = 0

        def trigger_catch_up(self):
            self.triggered += 1

        def stop(self):
            self.stopped += 1

    collector = Collector()
    app.state.kaipanla_collector = collector
    client = TestClient(app)

    rejected = client.put(
        "/api/settings/kaipanla",
        json={"source_url": AUTHORIZED_URL + "&padding=" + "x" * 4096},
    )
    assert rejected.status_code == 400
    assert "test-token-123456" not in rejected.text

    response = client.put("/api/settings/kaipanla", json={"source_url": AUTHORIZED_URL})
    assert response.status_code == 200
    body = response.json()
    assert body["configured"] is True
    assert AUTHORIZED_URL not in response.text
    assert "test-token-123456" not in response.text
    assert collector.triggered == 1

    stored = load_credentials()
    assert stored is not None
    assert stored.deviceid == "device-id-123456"
    raw = (tmp_path / "user_data" / "secrets.json").read_text(encoding="utf-8")
    assert "source_url" not in raw
    assert "Type" not in raw

    cleared = client.delete("/api/settings/kaipanla")
    assert cleared.json()["configured"] is False
    assert load_credentials() is None
    assert collector.stopped == 1


@pytest.mark.parametrize(
    ("endpoint", "host", "method"),
    [
        (115, "apphwhq.longhuvip.com", "POST"),
        (30, "apphis.longhuvip.com", "POST"),
        (31, "apphwhq.longhuvip.com", "POST"),
        (15, "apphwshhq.longhuvip.com", "POST"),
        (100, "applhb.longhuvip.com", "POST"),
        (101, "applhb.longhuvip.com", "POST"),
        (108, "apphwshhq.longhuvip.com", "POST"),
        (109, "apphwshhq.longhuvip.com", "POST"),
        ("fund_interval", "apphis.longhuvip.com", "POST"),
        ("fund_capital_net", "apphis.longhuvip.com", "POST"),
        ("fund_large_order_statistics", "apphis.longhuvip.com", "POST"),
        ("northbound_sector_latest", "apphis.longhuvip.com", "GET"),
        ("northbound_sector_history", "apphis.longhuvip.com", "GET"),
        ("northbound_stocks_latest", "apphis.longhuvip.com", "GET"),
        ("northbound_stocks_history", "apphis.longhuvip.com", "GET"),
        ("shareholder_changes", "apphis.longhuvip.com", "POST"),
        ("shareholder_count_changes", "applhb.longhuvip.com", "GET"),
        ("dragon_tiger_movement", "apphis.longhuvip.com", "POST"),
        ("dragon_tiger_details", "applhb.longhuvip.com", "POST"),
        ("sector_strength", "apphis.longhuvip.com", "POST"),
        ("sector_constituents", "apphis.longhuvip.com", "POST"),
    ],
)
@pytest.mark.asyncio
async def test_client_uses_fixed_endpoint_and_redacts_credentials(endpoint, host, method):
    credentials = KaipanlaCredentials(
        token="test-token-123456",
        userid="2675923",
        deviceid="device-id-123456",
        phoneosnew="1",
        version="5.23.0.4",
        apiv="w44",
    )
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "errcode": "0",
                "Token": "test-token-123456",
                "message": "device-id-123456",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
        client = KaipanlaClient(credentials, http_client=http_client, attempts=1)
        payload = await client.request(endpoint, {"StockID": "000785"})

    request = seen[0]
    assert request.method == method
    assert request.url.host == host
    assert request.url.path == "/w1/api/index.php"
    if method == "POST":
        assert "test-token-123456" not in str(request.url)
        form = parse_qs(request.content.decode())
        assert form["Token"] == ["test-token-123456"]
    else:
        assert request.url.params["Token"] == "test-token-123456"
    assert request.url.params["StockID"] == "000785"
    assert payload["Token"] == "[REDACTED]"
    assert payload["message"] == "[REDACTED]"
