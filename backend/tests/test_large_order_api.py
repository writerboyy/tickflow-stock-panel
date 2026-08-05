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


def test_status_and_ranking_expose_v2_published_snapshot_fields(tmp_path):
    client, storage = _client(tmp_path)
    service = client.app.state.large_order_service
    service._rankings[60] = ({
        "symbol": "000001.SZ",
        "source": "tick_proxy",
        "change_pct": 0.04,
        "limit_up_price": 11.0,
        "limit_up_gap_pct": 0.057692,
    },)
    service._filtered_near_limit_count = 3
    service._unassessable_count = 2
    service._last_update_ms = 1_785_911_566_296
    service._last_calculation_ms = 214.25

    status = client.get("/api/large-orders/status").json()
    ranking = client.get("/api/large-orders/ranking", params={"window": 60}).json()

    assert status["candidate_count"] == 1
    assert status["filtered_near_limit_count"] == 3
    assert status["unassessable_count"] == 2
    assert status["last_calculation_ms"] == 214.25
    assert ranking["last_updated_ms"] == 1_785_911_566_296
    assert ranking["rows"][0]["change_pct"] == 0.04
    assert ranking["rows"][0]["limit_up_price"] == 11.0
    assert ranking["rows"][0]["limit_up_gap_pct"] == 0.057692
    storage.stop()
