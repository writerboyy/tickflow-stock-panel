from datetime import date, datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.large_orders import router
from app.services.large_order_service import LargeOrderService
from app.services.large_order_store import LargeOrderStore


def _client(tmp_path):
    storage = LargeOrderStore(tmp_path, flush_interval=0.01)
    storage.start()
    service = LargeOrderService()
    service._storage = storage
    app = FastAPI()
    app.state.large_order_service = service
    app.include_router(router)
    return TestClient(app), storage


def _ts(second: int) -> int:
    return int(datetime(2026, 8, 4, 9, 31, second).timestamp() * 1000)


def test_large_order_history_filters_and_reports_truncation(tmp_path):
    client, storage = _client(tmp_path)
    storage.submit("proxy_flow", [
        {
            "trade_date": date(2026, 8, 4),
            "event_ts_ms": _ts(second),
            "symbol": "000001.SZ" if second < 3 else "600000.SH",
            "event_id": f"flow-{second}",
            "amount": 1000 + second,
            "volume": 100,
        }
        for second in range(1, 4)
    ])

    response = client.get("/api/large-orders/history", params={
        "date": "2026-08-04",
        "kind": "proxy_flow",
        "symbol": "000001.sz",
        "from_ms": _ts(1),
        "to_ms": _ts(2),
        "limit": 1,
        "order": "desc",
    })

    assert response.status_code == 200
    payload = response.json()
    assert payload["kind"] == "proxy_flow"
    assert payload["date"] == "2026-08-04"
    assert payload["count"] == 1
    assert payload["truncated"] is True
    assert payload["rows"][0]["event_id"] == "flow-2"
    storage.stop()


def test_large_order_history_validates_kind_and_limit(tmp_path):
    client, storage = _client(tmp_path)

    assert client.get(
        "/api/large-orders/history",
        params={"date": "2026-08-04", "kind": "unknown"},
    ).status_code == 422
    assert client.get(
        "/api/large-orders/history",
        params={"date": "2026-08-04", "limit": 10001},
    ).status_code == 422
    storage.stop()
