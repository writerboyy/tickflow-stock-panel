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
    assert client.get(
        "/api/large-orders/history",
        params={"date": "2026-08-04", "cursor": "not-a-cursor"},
    ).status_code == 422
    assert client.get(
        "/api/large-orders/reconciliation",
        params={"date": "2026-08-04", "from_ms": 2, "to_ms": 1},
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


def test_large_order_history_combines_execution_events_and_pages(tmp_path):
    client, storage = _client(tmp_path)
    day = date(2026, 8, 4)
    timestamp = _ts(1)
    storage.submit(
        "proxy_flow",
        [{
            "trade_date": day,
            "event_ts_ms": timestamp,
            "symbol": "000001.SZ",
            "event_id": "proxy-1",
            "amount": 1000,
        }],
    )
    storage.submit(
        "kaipanla_trade",
        [{
            "trade_date": day,
            "event_ts_ms": timestamp,
            "symbol": "000001.SZ",
            "event_id": "trade-1",
            "amount": 900,
            "direction": "active_buy",
        }],
    )

    first = client.get(
        "/api/large-orders/history",
        params={"date": day.isoformat(), "mode": "execution", "limit": 1, "order": "desc"},
    ).json()
    second = client.get(
        "/api/large-orders/history",
        params={
            "date": day.isoformat(),
            "mode": "execution",
            "limit": 1,
            "order": "desc",
            "cursor": first["next_cursor"],
        },
    ).json()

    assert {first["rows"][0]["event_kind"], second["rows"][0]["event_kind"]} == {
        "proxy_flow",
        "kaipanla_trade",
    }
    assert first["has_more"] is True
    assert second["has_more"] is False
    storage.stop()


def test_large_order_analysis_returns_evidence_and_snapshot_history(tmp_path):
    client, storage = _client(tmp_path)
    day = client.app.state.large_order_service._trade_date
    storage.submit("orderbook_snapshot", [{
        "trade_date": day,
        "event_ts_ms": _ts(1),
        "symbol": "000001.SZ",
        "event_id": "depth-1",
        "bid_prices": [10.0],
        "bid_volumes": [1000],
        "ask_prices": [10.01],
        "ask_volumes": [500],
        "book_imbalance": 0.3333,
        "ofi": 500,
    }])

    response = client.get("/api/large-orders/000001.sz/analysis")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "000001.SZ"
    assert payload["evidence"]["orderbook"] is False
    assert payload["orderbook_history"][0]["event_id"] == "depth-1"
    storage.stop()


def test_large_order_reconciliation_aggregates_minute_events(tmp_path):
    client, storage = _client(tmp_path)
    day = date(2026, 8, 4)
    storage.submit(
        "proxy_flow",
        [
            {
                "trade_date": day,
                "event_ts_ms": _ts(1),
                "symbol": "000001.SZ",
                "name": "代理名称",
                "event_id": "proxy-buy",
                "amount": 1_000,
                "buy_amount": 1_000,
                "sell_amount": 0,
            },
            {
                "trade_date": day,
                "event_ts_ms": _ts(2),
                "symbol": "000001.SZ",
                "event_id": "proxy-sell",
                "amount": 200,
                "buy_amount": 0,
                "sell_amount": 200,
            },
        ],
    )
    storage.submit(
        "kaipanla_trade",
        [{
            "trade_date": day,
                "event_ts_ms": _ts(3),
                "symbol": "000001.SZ",
                "name": "精确名称",
            "event_id": "trade-buy",
            "amount": 700,
            "direction": "active_buy",
        }],
    )
    storage.submit(
        "kaipanla_intent",
        [
            {
                "trade_date": day,
                "event_ts_ms": _ts(4),
                "symbol": "000001.SZ",
                "event_id": "intent-1",
                "amount": 100,
                "cancel_flag": True,
            },
            {
                "trade_date": day,
                "event_ts_ms": _ts(5),
                "symbol": "000001.SZ",
                "event_id": "intent-2",
                "amount": 100,
                "cancel_flag": False,
            },
        ],
    )
    # ext_kpl_funds 参考表已随 deprecated-overlap 清理移除，
    # 对账行恒报 reference_missing，日线参考净额恒为空。
    response = client.get(
        "/api/large-orders/reconciliation",
        params={"date": day.isoformat(), "symbol": "000001.SZ"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["rows"][0]["proxy_net_amount"] == 800
    assert payload["rows"][0]["precise_net_amount"] == 700
    assert payload["rows"][0]["net_difference"] == -100
    assert payload["rows"][0]["cancel_rate"] == 0.5
    assert payload["rows"][0]["status"] == "reference_missing"
    assert payload["summary"]["daily_reference_net"] is None
    storage.stop()


def test_large_order_reconciliation_marks_intent_only_and_missing_reference(tmp_path):
    client, storage = _client(tmp_path)
    day = date(2026, 8, 4)
    matched_ts = int(datetime(2026, 8, 4, 9, 31, 1).timestamp() * 1000)
    intent_ts = int(datetime(2026, 8, 4, 9, 32, 1).timestamp() * 1000)
    storage.submit("proxy_flow", [{
        "trade_date": day,
        "event_ts_ms": matched_ts,
        "symbol": "000001.SZ",
        "event_id": "proxy",
        "buy_amount": 1_000,
        "sell_amount": 0,
    }])
    storage.submit("kaipanla_trade", [{
        "trade_date": day,
        "event_ts_ms": matched_ts,
        "symbol": "000001.SZ",
        "event_id": "trade",
        "amount": 900,
        "direction": "active_buy",
    }])
    storage.submit("kaipanla_intent", [{
        "trade_date": day,
        "event_ts_ms": intent_ts,
        "symbol": "000001.SZ",
        "event_id": "intent",
        "cancel_flag": True,
    }])

    payload = client.get(
        "/api/large-orders/reconciliation",
        params={"date": day.isoformat()},
    ).json()

    assert [row["status"] for row in payload["rows"]] == [
        "intent_only",
        "reference_missing",
    ]
    storage.stop()
