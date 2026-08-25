from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import json
import sqlite3
import time
from threading import Event, Thread
from types import SimpleNamespace

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.position_risk import _collapse_timeline_events, router as position_risk_router
from app.services import alert_store
from app.services.position_risk_ocr import import_position_image, parse_position_tokens
from app.services.position_risk_service import PositionRiskService, localize_position_risk_text
from app.services.position_risk_store import PositionRiskStore, RevisionConflict, default_rule_options
from app.services.qmt_trading import QmtRpcError, QmtTradingService, QmtZmqRpcClient
from app.services.quote_service import QuoteService
from app.services.watchlist_ocr.provider import OcrProvider
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet


def _token(text: str, x: int, y: int, line: int, confidence: float = 95) -> dict:
    return {
        "text": text,
        "left": x,
        "top": y,
        "width": 70,
        "height": 20,
        "confidence": confidence,
        "block_num": 1,
        "par_num": 1,
        "line_num": line,
    }


def _instruments(data_dir: Path) -> None:
    target = data_dir / "instruments"
    target.mkdir(parents=True)
    pl.DataFrame({
        "code": ["600036"],
        "symbol": ["600036.SH"],
        "name": ["招商银行"],
    }).write_parquet(target / "instruments.parquet")


def _margin_instruments(data_dir: Path) -> None:
    target = data_dir / "instruments"
    target.mkdir(parents=True)
    pl.DataFrame({
        "code": ["002432", "001258"],
        "symbol": ["002432.SZ", "001258.SZ"],
        "name": ["九安医疗", "立新能源"],
    }).write_parquet(target / "instruments.parquet")


def _position_events(service: PositionRiskService, rule_id: str | None = None) -> list[dict]:
    events = alert_store.list_recent(service.store.root.parents[1], days=30, source="position_risk")
    return [event for event in events if rule_id is None or event.get("rule_id") == rule_id]


_REMOVED_PUBLIC_EVENT_FIELDS = {
    "risk_score", "risk_level", "suggestion_pct", "reasons", "source_ids",
    "signals", "conditions", "logic", "evidence", "evidence_coverage",
}


def test_ths_position_ocr_parses_headers_numbers_and_confidence(tmp_path: Path):
    _instruments(tmp_path)
    tokens = [
        _token("证券名称", 10, 100, 1),
        _token("持仓", 210, 100, 1),
        _token("可用", 310, 100, 1),
        _token("成本价", 410, 100, 1),
        _token("现价", 510, 100, 1),
        _token("市值", 610, 100, 1),
        _token("招商银行", 10, 140, 2),
        _token("600036", 90, 140, 2),
        _token("1,200", 210, 140, 2),
        _token("1,000", 310, 140, 2),
        _token("34.567", 410, 140, 2),
        _token("36.10", 510, 140, 2),
        _token("43,320", 610, 140, 2),
        _token("总资产", 10, 40, 3),
        _token("100,000.00", 100, 40, 3),
    ]
    result = parse_position_tokens(tokens, tmp_path)
    assert result["template_version"] == "ths_mobile_position_v1"
    assert result["account_candidates"]["total_asset"]["value"] == 100_000
    assert result["positions"] == [{
        "code": "600036",
        "symbol": "600036.SH",
        "name": "招商银行",
        "quantity": 1200.0,
        "available": 1000.0,
        "cost_price": 34.567,
        "current_price": 36.1,
        "market_value": 43320.0,
        "profit_loss": None,
        "field_confidence": {
            "name_code": 95.0,
            "quantity": 95.0,
            "available": 95.0,
            "cost_price": 95.0,
            "current_price": 95.0,
            "market_value": 95.0,
        },
        "requires_review": False,
        "issues": [],
    }]


def test_ths_position_ocr_requires_review_for_low_confidence_cost(tmp_path: Path):
    _instruments(tmp_path)
    tokens = [
        _token("证券名称", 10, 100, 1), _token("持仓", 210, 100, 1),
        _token("可用", 310, 100, 1), _token("成本价", 410, 100, 1),
        _token("招商银行600036", 10, 140, 2), _token("100", 210, 140, 2),
        _token("100", 310, 140, 2), _token("34.5", 410, 140, 2, confidence=49),
    ]
    row = parse_position_tokens(tokens, tmp_path)["positions"][0]
    assert row["requires_review"] is True
    assert any("cost_price" in issue for issue in row["issues"])


def test_ths_position_ocr_accepts_split_headers_and_code(tmp_path: Path):
    _instruments(tmp_path)
    tokens = [
        _token("证券", 10, 100, 1), _token("名称", 80, 100, 1),
        _token("持仓", 210, 100, 1), _token("可用", 310, 100, 1),
        _token("成本", 410, 100, 1), _token("价", 480, 100, 1),
        _token("招商银行", 10, 140, 2), _token("600", 50, 140, 2),
        _token("036", 100, 140, 2), _token("100", 210, 140, 2),
        _token("100", 310, 140, 2), _token("34.5", 410, 140, 2),
    ]

    result = parse_position_tokens(tokens, tmp_path)

    assert result["issues"] == []
    assert result["positions"][0]["symbol"] == "600036.SH"
    assert result["positions"][0]["cost_price"] == 34.5


def test_ths_margin_position_ocr_parses_name_only_two_line_rows(tmp_path: Path):
    _margin_instruments(tmp_path)
    tokens = [
        _token("市", 10, 100, 1), _token("值", 80, 100, 1),
        _token("盈亏", 310, 100, 1), _token("持仓", 520, 100, 1),
        _token("/可", 590, 100, 1), _token("用", 650, 100, 1),
        _token("成", 760, 100, 1), _token("本", 820, 100, 1),
        _token("/现价", 880, 100, 1),
        _token("九安医疗", 10, 140, 2, 84), _token("-3,984.97", 310, 140, 2, 79),
        _token("3900", 520, 140, 2), _token("73.642", 840, 140, 2),
        _token("283,218.00", 10, 170, 3), _token("-1.388%", 310, 170, 3),
        _token("3900", 520, 170, 3), _token("72.620", 840, 170, 3),
        _token("立新", 10, 220, 4), _token("能", 80, 220, 4), _token("源", 140, 220, 4),
        _token("959.87", 310, 220, 4), _token("9700", 520, 220, 4),
        _token("13.061", 840, 220, 4),
        _token("127,652.00", 10, 250, 5), _token("0.758%", 310, 250, 5),
        _token("9700", 520, 250, 5), _token("13.160", 840, 250, 5),
    ]

    result = parse_position_tokens(tokens, tmp_path)

    assert result["issues"] == []
    assert result["positions"] == [
        {
            "code": "002432", "symbol": "002432.SZ", "name": "九安医疗",
            "quantity": 3900.0, "available": 3900.0,
            "cost_price": 73.642, "current_price": 72.62,
            "market_value": 283218.0, "profit_loss": -3984.97,
            "field_confidence": {
                "name_code": 84.0, "quantity": 95.0, "available": 95.0,
                "cost_price": 95.0, "current_price": 95.0,
                "market_value": 95.0, "profit_loss": 79.0,
            },
            "requires_review": False, "issues": [],
        },
        {
            "code": "001258", "symbol": "001258.SZ", "name": "立新能源",
            "quantity": 9700.0, "available": 9700.0,
            "cost_price": 13.061, "current_price": 13.16,
            "market_value": 127652.0, "profit_loss": 959.87,
            "field_confidence": {
                "name_code": 95.0, "quantity": 95.0, "available": 95.0,
                "cost_price": 95.0, "current_price": 95.0,
                "market_value": 95.0, "profit_loss": 95.0,
            },
            "requires_review": False, "issues": [],
        },
    ]


def test_ths_margin_position_ocr_reads_account_values_from_next_line(tmp_path: Path):
    _margin_instruments(tmp_path)
    tokens = [
        _token("总", 10, 10, 1, 90), _token("资产", 70, 10, 1, 80),
        _token("493,171.85", 10, 40, 2),
        _token("总", 10, 70, 3), _token("市", 70, 70, 3), _token("值", 130, 70, 3),
        _token("可", 310, 70, 3), _token("用", 370, 70, 3),
        _token("可用", 610, 70, 3), _token("保证金", 680, 70, 3),
        _token("410,870.00", 10, 100, 4), _token("82,301.85", 310, 100, 4),
        _token("184,920.21", 610, 100, 4),
        _token("市", 10, 140, 5), _token("值", 80, 140, 5),
        _token("盈亏", 310, 140, 5), _token("持仓", 520, 140, 5),
        _token("成本", 800, 140, 5),
    ]

    candidates = parse_position_tokens(tokens, tmp_path)["account_candidates"]

    assert candidates["total_asset"] == {"value": 493171.85, "confidence": 80.0}
    assert candidates["cash"] == {"value": 82301.85, "confidence": 95.0}


class _NoChineseOcr(OcrProvider):
    name = "fake"

    def available(self) -> bool:
        return True

    def supports_language(self, language: str) -> bool:
        return language != "chi_sim"

    def extract_text(self, image_bytes: bytes) -> str:
        return ""


def test_position_ocr_requires_simplified_chinese_language(tmp_path: Path):
    with pytest.raises(RuntimeError, match="chi_sim|简体中文"):
        import_position_image(b"image", tmp_path, provider=_NoChineseOcr())


def test_position_risk_localizes_current_and_historical_signal_ids():
    text = "九安医疗：signal_intraday_avg_cross_down / signal.n_day_high"
    assert localize_position_risk_text(text) == "九安医疗：分时价格下穿均价 / 创60日新高"


def test_position_risk_uses_custom_signal_name_for_new_events(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service._custom_signal_labels = {"csg_take_profit": "自定义止盈"}
    assert service.localize_text("csg.take_profit") == "自定义止盈"


def test_portfolio_store_revision_and_recommendation_table_is_removed(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    saved = store.replace({
        "account": {"name": "账户", "cash": 1000, "total_asset": 2000, "previous_close_total_asset": 2100},
        "positions": [],
    }, 0)
    assert saved["revision"] == 1
    with pytest.raises(RevisionConflict):
        store.replace(saved, 0)
    store.set_runtime("keep", {"value": 1})
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("CREATE TABLE recommendations (id TEXT)")

    reopened = PositionRiskStore(tmp_path)
    tables = {
        row[0]
        for row in sqlite3.connect(reopened.db_path).execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        )
    }
    assert "recommendations" not in tables
    assert reopened.get_runtime("keep") == {"value": 1}


def test_position_risk_recommendation_routes_are_removed():
    assert not any(route.path.startswith("/api/position-risk/recommendations") for route in position_risk_router.routes)
    assert not any(route.path == "/api/position-risk/template" for route in position_risk_router.routes)
    assert any(
        route.path == "/api/position-risk/qmt/orders/confirm-action"
        for route in position_risk_router.routes
    )


def test_position_risk_context_gate_blocks_ordinary_action_but_not_hard_guard(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service._context_service = object()
    service._contexts["600036.SH"] = {
        "state": "unavailable",
        "gate_open": False,
        "emotion_phase": "数据不足",
    }
    portfolio = service.store.load()
    portfolio["overrides"]["600036.SH"] = {"rules": {"market_context": {"enabled": True}}}
    position = {
        "symbol": "600036.SH", "name": "招商银行",
        "quantity": 1000, "available": 1000, "cost_price": 10,
    }
    event_time = datetime.now().replace(microsecond=0)

    service._emit(
        portfolio, position, "take_profit", "固定止盈", "warn", 50, [],
        occurred_at=event_time,
    )
    ordinary = _position_events(service, "take_profit")[0]
    assert ordinary["action_pct"] == 0
    assert ordinary["trade_action"] == "SELL"
    assert ordinary["action_eligible"] is False
    assert ordinary["context_state"] == "unavailable"

    service._emit(
        portfolio, position, "stop_loss", "成本止损", "critical", 100, [],
        occurred_at=event_time,
    )
    hard_guard = _position_events(service, "stop_loss")[0]
    assert hard_guard["action_pct"] == 100
    assert hard_guard["trade_action"] == "SELL"
    assert hard_guard["action_eligible"] is True


def test_position_risk_context_gate_registers_short_lived_sell_action(tmp_path: Path):
    class FreshQuotes(_Quotes):
        def get_fresh_quotes(self, _symbols):
            return {
                "quotes": {
                    "600036.SH": {
                        "symbol": "600036.SH", "last_price": 10.25,
                        "limit_down": 9.0, "limit_up": 11.0,
                        "timestamp": datetime.now().isoformat(),
                    },
                },
            }

    service = PositionRiskService(tmp_path, _Repo(), FreshQuotes(), SimpleNamespace(paper_supervisor=None))
    service._context_service = object()
    service._contexts["600036.SH"] = {
        "state": "supportive", "gate_open": True, "emotion_phase": "发酵",
    }
    service.store.replace({
        "account": {"name": "账户", "cash": 10_000, "total_asset": 20_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行",
            "quantity": 1000, "available": 1000, "cost_price": 10,
        }],
    }, 0)
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    event_time = datetime.now().replace(microsecond=0)
    service._emit(
        portfolio, position, "take_profit", "固定止盈", "warn", 50, [],
        occurred_at=event_time,
    )
    event = _position_events(service, "take_profit")[0]

    order = service.confirmed_action_order(
        event["fingerprint"], "600036.SH", "SELL", 500,
        now=event_time + timedelta(seconds=10),
    )
    assert order == {
        "action": "SELL",
        "symbol": "600036.SH",
        "volume": 500,
        "price": 10.25,
        "price_type": "LIMIT",
        "idempotency_key": f"risk-{event['fingerprint']}",
        "strategy_name": "position_risk",
    }
    with pytest.raises(ValueError, match="120 秒"):
        service.confirmed_action_order(
            event["fingerprint"], "600036.SH", "SELL", 500,
            now=event_time + timedelta(seconds=121),
        )
    with pytest.raises(ValueError, match="只允许卖出|不一致"):
        service.confirmed_action_order(
            event["fingerprint"], "600036.SH", "BUY", 500,
            now=event_time + timedelta(seconds=10),
        )
    expanded = service.confirmed_action_order(
        event["fingerprint"], "600036.SH", "SELL", 600,
        now=event_time + timedelta(seconds=10),
    )
    assert expanded["volume"] == 600



def test_position_risk_history_migration_cleans_public_fields_and_preserves_other_alerts(tmp_path: Path):
    alerts_path = tmp_path / "user_data" / "alerts.jsonl"
    alerts_path.parent.mkdir(parents=True)
    alerts_path.write_text(
        "\n".join([
            '{"ts": 4102444800000, "source": "position_risk", "rule_id": "stop_loss", "risk_score": 85, "risk_level": "high", "suggestion_pct": 50, "reasons": ["x"], "source_ids": ["stop_loss"], "evidence": {"quote": true}, "evidence_coverage": 1}',
            '{"ts": 4102444800000, "source": "monitor", "rule_id": "custom", "reasons": ["keep"], "signals": ["s"]}',
        ]) + "\n",
        encoding="utf-8",
    )

    assert alert_store.sanitize_position_risk_events(tmp_path) == 6
    position_event = alert_store.list_recent(tmp_path, days=30, source="position_risk")[0]
    assert position_event["action_pct"] == 50
    assert not _REMOVED_PUBLIC_EVENT_FIELDS & position_event.keys()
    monitor_event = alert_store.list_recent(tmp_path, days=30, source="monitor")[0]
    assert monitor_event["reasons"] == ["keep"]
    assert monitor_event["signals"] == ["s"]


def test_legacy_position_risk_config_gets_short_term_defaults_without_activation(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    store.portfolio_path.write_text(
        '{"schema_version": 1, "revision": 0, "account": {}, "positions": [], "template": {"rules": {"stop_loss": {"enabled": true}}}}',
        encoding="utf-8",
    )
    portfolio = store.load()
    assert "template" not in portfolio
    assert portfolio["overrides"] == {}
    assert portfolio["schema_version"] == 2
    assert "template" not in json.loads(store.portfolio_path.read_text(encoding="utf-8"))
    assert store.load()["positions"] == []


def test_legacy_global_template_is_ignored_for_existing_positions(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    store.portfolio_path.write_text(json.dumps({
        "schema_version": 1,
        "revision": 0,
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 1000,
            "available": 1000, "cost_price": 40,
        }],
        "template": {"rules": {"stop_loss": {"enabled": True, "threshold": -0.05}}},
    }), encoding="utf-8")

    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    portfolio = service.store.load()
    service._preload_history({"600036.SH"})
    service._evaluate_position(
        portfolio,
        portfolio["positions"][0],
        {"symbol": "600036.SH", "last_price": 35, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
    )

    assert "template" not in portfolio
    assert service._rule_config(portfolio, "600036.SH", "stop_loss") == {}
    assert not _position_events(service, "stop_loss")


def test_portfolio_store_adds_new_large_order_defaults_to_existing_rule(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    saved = store.replace({
        "overrides": {"600036.SH": {"rules": {"large_buy": {"enabled": False, "action_pct": 0}}}},
    }, 0)

    assert "template" not in saved
    assert saved["overrides"]["600036.SH"]["rules"]["large_buy"] == {
        "enabled": False,
        "action_pct": 0,
    }


def test_position_risk_modules_default_to_off_and_notifications_default_to_off(tmp_path: Path):
    portfolio = PositionRiskStore(tmp_path).load()
    assert "template" not in portfolio
    assert portfolio["overrides"] == {}
    assert all("enabled" in rule for rule in default_rule_options()["rules"].values())
    assert all(rule.get("notify") is False for rule in default_rule_options()["rules"].values())


def _qmt_settings(**overrides):
    values = {
        "qmt_enabled": True,
        "qmt_zmq_connect_address": "tcp://127.0.0.1:15648",
        "qmt_account_id": "account-1",
        "qmt_rpc_timeout_seconds": 1,
        "qmt_trade_enabled": False,
        "qmt_account_type": "STOCK",
        "qmt_auto_sync": True,
        "qmt_auto_sync_interval_seconds": 30,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _start_fake_qmt_zmq(handler):
    import zmq

    context = zmq.Context()
    router = context.socket(zmq.ROUTER)
    router.bind("tcp://127.0.0.1:*")
    address = router.getsockopt(zmq.LAST_ENDPOINT).decode("utf-8")
    stopped = Event()

    def run():
        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        try:
            while not stopped.is_set():
                if not dict(poller.poll(50)).get(router):
                    continue
                identity, payload = router.recv_multipart()
                request = _decode_zmq_payload(payload)
                responses = handler(request)
                if isinstance(responses, tuple):
                    responses = list(responses)
                else:
                    responses = [responses]
                for response in responses:
                    router.send_multipart([identity, _encode_zmq_payload(response)])
        finally:
            router.close(linger=0)
            context.term()

    thread = Thread(target=run, name="fake-qmt-zmq", daemon=True)
    thread.start()
    return address, stopped, thread


def _encode_zmq_payload(value):
    from app.services.qmt_trading import _encode_zmq_payload as encode

    return encode(value)


def _decode_zmq_payload(value):
    from app.services.qmt_trading import _decode_zmq_payload as decode

    return decode(value)


def test_qmt_client_requires_explicit_enable_and_complete_credentials():
    disabled = QmtZmqRpcClient(_qmt_settings(qmt_enabled=False))
    assert disabled.configured is False
    assert "QMT_ENABLED" in disabled.configuration_reason

    incomplete = QmtZmqRpcClient(_qmt_settings(qmt_zmq_connect_address=""))
    assert incomplete.configured is False
    assert "QMT_ZMQ_CONNECT_ADDRESS" in incomplete.configuration_reason


def test_qmt_zmq_client_round_trip_uses_server_protocol():
    seen = []

    def handler(request):
        seen.append(request)
        response = {
            "schema_version": 1,
            "request_id": request["request_id"],
            "account_id": request["account_id"],
            "ok": True,
            "data": {"account_id": request["account_id"], "server_time": "now"},
        }
        wrong = {**response, "request_id": "wrong-request-id"}
        return (wrong, response)

    address, stopped, thread = _start_fake_qmt_zmq(handler)
    client = QmtZmqRpcClient(_qmt_settings(qmt_zmq_connect_address=address))
    try:
        assert client.call("ping") == {"account_id": "account-1", "server_time": "now"}
    finally:
        client.close()
        stopped.set()
        thread.join(timeout=2)
    assert seen[0]["schema_version"] == 1
    assert seen[0]["account_id"] == "account-1"
    assert seen[0]["method"] == "ping"
    assert seen[0]["params"] == {}


def test_qmt_zmq_client_times_out_without_server():
    client = QmtZmqRpcClient(
        _qmt_settings(
            qmt_zmq_connect_address="tcp://127.0.0.1:1",
            qmt_rpc_timeout_seconds=0.05,
        ),
    )
    with pytest.raises(QmtRpcError, match="超时"):
        client.call("ping")


def test_qmt_trading_service_rejects_order_when_trade_switch_is_off(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings())
    with pytest.raises(QmtRpcError, match="交易开关"):
        service._validate_order({"action": "BUY", "symbol": "600036.SH", "volume": 100, "price": 35}, {"positions": []})


def test_qmt_trading_service_allows_multiple_lots_and_enforces_sell_available_volume(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    snapshot = {"positions": [{"symbol": "600036.SH", "available": 300}]}
    assert service._validate_order({"action": "SELL", "symbol": "600036.SH", "volume": 200, "price": 35}, snapshot)["volume"] == 200
    with pytest.raises(ValueError, match="可用持仓不足"):
        service._validate_order({"action": "SELL", "symbol": "600036.SH", "volume": 400, "price": 35}, snapshot)


def test_qmt_order_preview_allocates_fraction_and_fixed_amount(tmp_path: Path):
    service = QmtTradingService(
        tmp_path,
        _qmt_settings(),
    )

    def fake_call(method, _params):
        if method == "get_asset":
            return {"cash": 120_000}
        if method == "get_positions":
            return {"600036.SH": {"stock_code": "600036.SH", "available": 1_000}}
        raise AssertionError(method)

    service.client.call = fake_call

    buy = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "quarter",
    })
    assert buy["basis_amount"] == 120_000
    assert buy["target_amount"] == 30_000
    assert buy["volume"] == 800
    assert buy["actual_amount"] == 28_000

    available = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "available",
    })
    assert available["basis_amount"] == 120_000
    assert available["target_amount"] == 120_000
    assert available["volume"] == 3_400
    assert available["actual_amount"] == 119_000

    sixth = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "sixth",
    })
    assert sixth["target_amount"] == 20_000
    assert sixth["volume"] == 500
    assert sixth["actual_amount"] == 17_500

    fifth = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "fifth",
    })
    assert fifth["target_amount"] == 24_000
    assert fifth["volume"] == 600
    assert fifth["actual_amount"] == 21_000

    one_lot = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "lot",
    })
    assert one_lot["target_amount"] == 3_500
    assert one_lot["volume"] == 100
    assert one_lot["actual_amount"] == 3_500

    sell = service.preview_order({
        "action": "SELL",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "fixed",
        "allocation_value": 5_000,
    })
    assert sell["basis_label"] == "可用持仓市值"
    assert sell["basis_amount"] == 35_000
    assert sell["volume"] == 100
    assert sell["actual_amount"] == 3_500


def test_credit_order_preview_uses_credit_buying_power_not_cash(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_account_type="CREDIT"))

    def fake_call(method, _params):
        assert method == "get_asset"
        return {
            "cash": 3_800.52,
            "m_dAssureEnbuyBalance": 18_000,
            "m_dFinEnbuyBalance": 25_000,
            "m_dFinEnableBalance": 30_000,
        }

    service.client.call = fake_call

    preview = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 10,
        "price_type": "LIMIT",
        "allocation_mode": "available",
    })

    assert preview["basis_label"] == "可买担保品资金"
    assert preview["cash_amount"] == 3_800.52
    assert preview["financing_available_amount"] == 30_000
    assert preview["buying_power_amount"] == 18_000
    assert preview["volume"] == 1_800


def test_credit_order_preview_can_use_financing_buying_power(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_account_type="CREDIT"))

    service.client.call = lambda method, _params: {
        "cash": 3_800.52,
        "m_dAssureEnbuyBalance": 18_000,
        "m_dFinEnbuyBalance": 25_000,
        "m_dFinEnableBalance": 30_000,
    } if method == "get_asset" else None

    preview = service.preview_order({
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 10,
        "price_type": "LIMIT",
        "allocation_mode": "available",
        "credit_buy_mode": "financing",
    })

    assert preview["credit_buy_mode"] == "financing"
    assert preview["basis_label"] == "可买融资标的资金"
    assert preview["buying_power_amount"] == 25_000
    assert preview["volume"] == 2_500


def test_credit_order_preview_rejects_cash_only_response(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_account_type="CREDIT"))
    service.client.call = lambda method, _params: {"cash": 100_000} if method == "get_asset" else None

    with pytest.raises(QmtRpcError, match="未返回信用账户可买额度"):
        service.preview_order({
            "action": "BUY",
            "symbol": "600036.SH",
            "price": 10,
            "price_type": "LIMIT",
            "allocation_mode": "available",
        })


def test_credit_order_preview_rejects_financing_only_response(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_account_type="CREDIT"))
    service.client.call = lambda method, _params: {
        "cash": 100_000,
        "m_dFinEnbuyBalance": 50_000,
        "m_dFinEnableBalance": 80_000,
    } if method == "get_asset" else None

    with pytest.raises(QmtRpcError, match="未返回信用账户可买额度"):
        service.preview_order({
            "action": "BUY",
            "symbol": "600036.SH",
            "price": 10,
            "price_type": "LIMIT",
            "allocation_mode": "available",
        })


def test_qmt_order_preview_uses_recent_sync_snapshot(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings())
    service._last_snapshot = {
        "account": {"cash": 120_000},
        "positions": [{"symbol": "600036.SH", "available": 1_000}],
    }
    service._last_snapshot_monotonic = time.monotonic()

    def fail_call(_method, _params):
        raise AssertionError("preview should use the recent sync snapshot")

    service.client.call = fail_call
    preview = service.preview_order({
        "action": "SELL",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "third",
    })
    assert preview["volume"] == 300


def test_qmt_submit_recomputes_allocation_before_sending(tmp_path: Path):
    service = QmtTradingService(
        tmp_path,
        _qmt_settings(qmt_trade_enabled=True),
    )
    service.trade_enabled = True
    calls = []

    def fake_call(method, params):
        calls.append(method)
        if method == "get_asset":
            return {"cash": 120_000}
        if method == "submit_orders_batch":
            assert params["orders"][0]["volume"] == 800
            return [{"success": True, "accepted": True, "order_sys_id": "allocated-1"}]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "allocated-request-1",
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "quarter",
    })

    assert result["volume"] == 800
    assert result["estimated_amount"] == 28_000
    assert result["allocation_basis_amount"] == 120_000
    assert calls == ["get_asset", "submit_orders_batch"]


def test_qmt_order_preview_api_maps_success_and_service_errors():
    class FakeQmt:
        def preview_order(self, payload):
            if payload["symbol"] == "ERROR.SH":
                raise ValueError("固定金额必须大于 0")
            if payload["symbol"] == "OFFLINE.SH":
                raise QmtRpcError("QMT 资产响应不可用")
            return {
                "action": payload["action"],
                "symbol": payload["symbol"],
                "price": payload["price"],
                "price_type": payload["price_type"],
                "allocation_mode": payload["allocation_mode"],
                "allocation_value": None,
                "basis_label": "可用资金",
                "basis_amount": 120_000,
                "target_amount": 30_000,
                "actual_amount": 28_000,
                "volume": 800,
                "available_volume": None,
                "capped": True,
                "reason": None,
            }

    app = FastAPI()
    app.include_router(position_risk_router)
    app.state.qmt_trading_service = FakeQmt()
    client = TestClient(app)

    response = client.post("/api/position-risk/qmt/orders/preview", json={
        "action": "BUY",
        "symbol": "600036.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "quarter",
    })
    assert response.status_code == 200
    assert response.json()["preview"]["volume"] == 800

    invalid = client.post("/api/position-risk/qmt/orders/preview", json={
        "action": "BUY",
        "symbol": "ERROR.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "fixed",
        "allocation_value": 100,
    })
    assert invalid.status_code == 400
    assert invalid.json()["detail"] == "固定金额必须大于 0"

    unavailable = client.post("/api/position-risk/qmt/orders/preview", json={
        "action": "BUY",
        "symbol": "OFFLINE.SH",
        "price": 35,
        "price_type": "LIMIT",
        "allocation_mode": "quarter",
    })
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"] == "QMT 资产响应不可用"


def test_qmt_runtime_trade_switch_defaults_to_authorized_state_and_requires_sync_to_reenable(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    assert service.status()["trade_authorized"] is True
    assert service.status()["trade_enabled"] is True
    assert service.set_trade_enabled(False)["trade_enabled"] is False
    with pytest.raises(QmtRpcError, match="先成功同步"):
        service.set_trade_enabled(True)
    service._last_snapshot = {"synced_at": "2026-08-14T00:00:00+00:00"}
    service._last_status = {"state": "ready"}
    assert service.set_trade_enabled(True)["trade_enabled"] is True

    unauthorized = QmtTradingService(tmp_path / "unauthorized", _qmt_settings(qmt_trade_enabled=False))
    assert unauthorized.status()["trade_enabled"] is False


def test_qmt_auto_sync_starts_immediately_and_stops(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings())
    called = Event()
    service.sync_into = lambda _position_risk: called.set()
    assert service.start_auto_sync(object()) is True
    assert called.wait(1) is True
    assert service.status()["auto_sync_running"] is True
    service.stop()
    assert service.status()["auto_sync_running"] is False
    assert service.status()["trade_enabled"] is False


def test_qmt_submit_persists_unknown_before_timeout_and_does_not_retry(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    calls = []

    def fail_call(method, params):
        calls.append((method, params))
        if method == "get_asset":
            return {"cash": 100_000}
        raise QmtRpcError("QMT RPC 超时")

    service.client.call = fail_call
    request = {
        "idempotency_key": "same-request-1", "action": "BUY", "symbol": "600036.SH",
        "volume": 100, "price": 35, "price_type": "LIMIT",
    }
    with pytest.raises(QmtRpcError, match="超时"):
        service.submit_order(request)
    assert service._known_order("same-request-1")["status"] == "unknown"
    assert service.submit_order(request)["status"] == "unknown"
    assert [method for method, _params in calls] == ["get_asset", "submit_orders_batch"]


def test_qmt_submit_uses_buy_preflight_and_returns_after_acceptance(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    calls = []

    def fake_call(method, params):
        calls.append(method)
        if method == "get_asset":
            return {"cash": 100_000}
        if method == "submit_orders_batch":
            assert params["orders"][0]["require_idempotency_check"] is True
            return [{"success": True, "accepted": True, "order_sys_id": "sys-2", "user_order_id": "position-risk:request-2"}]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "request-2", "action": "BUY", "symbol": "600036.SH",
        "volume": 100, "price": 35, "price_type": "LIMIT",
    })
    assert result["status"] == "accepted_pending"
    assert result["order_sys_id"] == "sys-2"
    assert result["symbol"] == "600036.SH"
    assert calls == ["get_asset", "submit_orders_batch"]


def test_qmt_submit_accepts_wrapped_batch_result(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True

    def fake_call(method, _params):
        if method == "get_asset":
            return {"cash": 100_000}
        if method == "submit_orders_batch":
            return {"results": [{"success": True, "accepted": True, "order_sys_id": "wrapped-1"}]}
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "wrapped-request-1", "action": "BUY", "symbol": "600036.SH",
        "volume": 100, "price": 35, "price_type": "LIMIT",
    })

    assert result["status"] == "accepted_pending"
    assert result["order_sys_id"] == "wrapped-1"


def test_qmt_submit_reconciles_remote_order_when_batch_response_shape_is_invalid(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    calls = []

    def fake_call(method, _params):
        calls.append(method)
        if method == "get_asset":
            return {"cash": 100_000}
        if method == "submit_orders_batch":
            return {"unexpected": "shape"}
        if method == "query_orders":
            return [{
                "stock_code": "600036.SH",
                "status": "50",
                "order_sys_id": "remote-1",
                "remark": "position_risk:remote-shape-request",
            }]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "remote-shape-request", "action": "BUY", "symbol": "600036.SH",
        "volume": 100, "price": 35, "price_type": "LIMIT",
    })

    assert result["status"] == "50"
    assert result["order_sys_id"] == "remote-1"
    assert result["error"] is None
    assert calls == ["get_asset", "submit_orders_batch", "query_orders"]


def test_qmt_submit_preserves_limit_board_order_source(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True

    def fake_call(method, params):
        if method == "get_asset":
            return {"cash": 100_000}
        if method == "submit_orders_batch":
            order = params["orders"][0]
            assert params["strategy_name"] == "limit_board"
            assert order["strategy_name"] == "limit_board"
            assert order["remark"].startswith("limit_board:")
            return [{"success": True, "accepted": True, "order_sys_id": "board-1"}]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "limit-board-20260814-600036.SH",
        "strategy_name": "limit_board",
        "action": "BUY",
        "symbol": "600036.SH",
        "volume": 100,
        "price": 35,
        "price_type": "LIMIT",
    })

    assert result["strategy_name"] == "limit_board"
    assert result["status"] == "accepted_pending"


def test_qmt_submit_records_order_timeline_and_real_broker_time(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True

    def fake_call(method, _params):
        if method == "get_asset":
            return {"cash": 100_000}
        if method == "submit_orders_batch":
            return [{
                "success": True,
                "accepted": True,
                "order_sys_id": "board-time-1",
                "order_time": "2026-08-18 10:05:00.250",
            }]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "limit-board-20260818-600036.SH",
        "strategy_name": "limit_board",
        "action": "BUY",
        "symbol": "600036.SH",
        "volume": 100,
        "price": 35,
        "price_type": "LIMIT",
        "trigger_at": "2026-08-18T10:03:20.100+08:00",
        "system_order_at": "2026-08-18T10:03:20.150+08:00",
    })

    assert result["trigger_at"] == "2026-08-18T10:03:20.100+08:00"
    assert result["system_order_at"] == "2026-08-18T10:03:20.150+08:00"
    assert result["qmt_submit_at"]
    assert result["qmt_accepted_at"]
    assert result["broker_order_at"] == "2026-08-18T10:05:00.250+08:00"
    assert result["broker_order_time_raw"] == "2026-08-18 10:05:00.250"
    assert result["broker_order_time_field"] == "order_time"


def test_qmt_remote_order_sync_adds_broker_time_to_local_order(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service._remember_order({
        "idempotency_key": "limit-board-20260818-600036.SH",
        "strategy_name": "limit_board",
        "order_sys_id": "board-sync-1",
        "user_order_id": "limit_board:limit-board-20260818-600036.SH",
        "status": "accepted_pending",
        "created_at": "2026-08-18T02:03:20.150+00:00",
        "updated_at": "2026-08-18T02:03:20.150+00:00",
        "system_order_at": "2026-08-18T10:03:20.150+08:00",
    })

    service._merge_remote_orders([{
        "order_sys_id": "board-sync-1",
        "status": "queued",
        "entrust_time": 100500,
    }])
    saved = service.get_orders({"limit-board-20260818-600036.SH"})[
        "limit-board-20260818-600036.SH"
    ]

    assert saved["status"] == "queued"
    assert saved["broker_order_at"] == "2026-08-18T10:05:00.000+08:00"
    assert saved["broker_order_time_raw"] == 100500


def test_qmt_remote_order_sync_clears_stale_local_error_and_keeps_broker_reason(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service._remember_order({
        "idempotency_key": "request-with-stale-error",
        "strategy_name": "position_risk",
        "order_sys_id": "remote-2",
        "user_order_id": "position_risk:request-with-stale-error",
        "status": "unknown",
        "error": "QMT 委托响应格式无效",
        "created_at": "2026-08-18T02:03:20.150+00:00",
        "updated_at": "2026-08-18T02:03:20.150+00:00",
    })
    service._merge_remote_orders([{
        "order_sys_id": "remote-2",
        "user_order_id": "position_risk:request-with-stale-error",
        "status": "50",
    }])
    saved = service.get_orders({"request-with-stale-error"})["request-with-stale-error"]
    assert saved["status"] == "50"
    assert saved["error"] is None

    service._merge_remote_orders([{
        "order_sys_id": "remote-2",
        "user_order_id": "position_risk:request-with-stale-error",
        "status": "57",
        "status_msg": "资金冻结不足",
    }])
    saved = service.get_orders({"request-with-stale-error"})["request-with-stale-error"]
    assert saved["error"] == "资金冻结不足"


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ("54", "已撤"),
        ("56", "已成"),
        ("57", "废单"),
    ],
)
def test_qmt_cancel_order_rejects_terminal_remote_status(tmp_path: Path, status: str, message: str):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    service.client.probe = lambda: {}
    calls = []

    def fake_call(method, _params):
        calls.append(method)
        if method == "query_orders":
            return [{"order_sys_id": "terminal-1", "status": status}]
        raise AssertionError(method)

    service.client.call = fake_call
    with pytest.raises(QmtRpcError, match=message):
        service.cancel_order({"order_sys_id": "terminal-1"})
    assert calls == ["query_orders"]


def test_qmt_cancel_order_rejects_cancel_pending_remote_status(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    service.client.probe = lambda: {}
    calls = []

    def fake_call(method, _params):
        calls.append(method)
        if method == "query_orders":
            return [{"order_sys_id": "pending-cancel-1", "status": "51"}]
        raise AssertionError(method)

    service.client.call = fake_call
    with pytest.raises(QmtRpcError, match="处理中"):
        service.cancel_order({"order_sys_id": "pending-cancel-1"})
    assert calls == ["query_orders"]


def test_qmt_cancel_order_checks_remote_status_before_request(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    service.client.probe = lambda: {}
    calls = []

    def fake_call(method, params):
        calls.append(method)
        if method == "query_orders":
            return [{"order_sys_id": "active-1", "status": "50"}]
        if method == "cancel_order":
            assert params["order_sys_id"] == "active-1"
            return {"accepted": True}
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.cancel_order({"order_sys_id": "active-1"})
    assert result["status"] == "cancel_requested"
    assert calls == ["query_orders", "cancel_order"]


def test_qmt_submit_uses_sell_preflight(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    calls = []

    def fake_call(method, _params):
        calls.append(method)
        if method == "get_positions":
            return {"600036.SH": {"stock_code": "600036.SH", "available": 100}}
        if method == "submit_orders_batch":
            return [{"success": True, "accepted": True, "user_order_id": "position-risk:request-3"}]
        raise AssertionError(method)

    service.client.call = fake_call
    result = service.submit_order({
        "idempotency_key": "request-3", "action": "SELL", "symbol": "600036.SH",
        "volume": 100, "price": 35, "price_type": "LIMIT",
    })

    assert result["status"] == "accepted_pending"
    assert calls == ["get_positions", "submit_orders_batch"]


def test_qmt_snapshot_rejects_available_above_volume():
    client = QmtZmqRpcClient(_qmt_settings())
    responses = iter([
        {"account_id": "account-1"},
        {"cash": 1_000, "total_asset": 2_000, "market_value": 1_000},
        {"600036.SH": {"stock_code": "600036.SH", "volume": 100, "available": 200, "cost": 10}},
        [],
        [],
    ])
    client.call = lambda _method, _params=None: next(responses)
    with pytest.raises(QmtRpcError, match="可用数量大于持仓数量"):
        client.snapshot()


def test_qmt_snapshot_derives_latest_buy_entry_date():
    client = QmtZmqRpcClient(_qmt_settings())
    responses = iter([
        {"account_id": "account-1"},
        {"cash": 1_000, "total_asset": 2_000, "market_value": 1_000},
        {"600036.SH": {"stock_code": "600036.SH", "volume": 100, "available": 100, "cost": 10}},
        [],
        [
            {"stock_code": "600036.SH", "action": "BUY", "trade_time": "2026-08-20 10:00:00"},
            {"stock_code": "600036.SH", "action": "BUY", "trade_time": "2026-08-21 10:00:00"},
        ],
    ])
    client.call = lambda _method, _params=None: next(responses)
    snapshot = client.snapshot()
    assert snapshot["positions"][0]["entry_date"] == "2026-08-21"


class _Repo:
    def __init__(self) -> None:
        self.rows = pl.DataFrame({
            "symbol": ["600036.SH"],
            "close": [18.0],
            "raw_close": [36.0],
            "ma5": [17.5],
            "ma10": [17.0],
            "ma20": [16.0],
            "signal_macd_dead": [False],
        })

    def resolve_asset_type(self, symbol: str) -> str:
        return "stock" if symbol == "600036.SH" else "unknown"

    def get_name_map(self, symbols):
        return {symbol: "招商银行" for symbol in symbols if symbol == "600036.SH"}

    def get_enriched_latest(self):
        return self.rows, None

    def get_enriched_latest_asset(self, _asset_type: str):
        return pl.DataFrame(), None


class _Quotes:
    def __init__(self) -> None:
        self.alerts = []

    def get_fresh_quotes(self, _symbols):
        return {"quotes": {}}

    def push_alerts(self, alerts):
        self.alerts.extend(alerts)

    def remove_fetch_listener(self, _callback):
        pass

    def remove_symbol_consumer(self, _consumer):
        pass

    def release_temporary_polling(self):
        pass

    def add_fetch_listener(self, _callback):
        pass

    def set_symbol_consumer(self, _consumer, _symbols):
        pass

    def get_min_interval(self):
        return 3

    def acquire_temporary_polling(self, _interval):
        pass


class _AssetAwareQuotes(_Quotes):
    def __init__(self) -> None:
        super().__init__()
        self.intraday_consumers = {}
        self.signal_assets = []
        self.snapshot_assets = []

    def set_intraday_consumer(self, consumer, symbols, asset_type="stock"):
        self.intraday_consumers[consumer] = (set(symbols), asset_type)

    def remove_intraday_consumer(self, consumer):
        self.intraday_consumers.pop(consumer, None)

    def get_intraday_signals(self, symbols, *, prev_close, asset_type, now, consumer_id):
        self.signal_assets.append((set(symbols), asset_type, consumer_id))
        return {}

    def get_intraday_snapshot(self, symbols, *, asset_type, now):
        self.snapshot_assets.append((set(symbols), asset_type))
        return {"vwap": {}, "rows": [], "available": False}


class _IntradayQuotes(_Quotes):
    def get_intraday_snapshot(self, _symbols, *, asset_type="stock", now=None):
        return {"vwap": {"600036.SH": 100.0}, "rows": [], "available": True}


class _FeatureQuotes(_Quotes):
    def get_intraday_features(self, symbols, *, asset_type="stock", now=None):
        return {
            symbol: {
                "symbol": symbol, "available": True, "fresh": True,
                "as_of": "2026-08-07T10:00:00", "bars_1m": 20, "bars_5m": 4,
                "session_vwap": 9.5, "ema9_1m": 10.6, "ema20_1m": 10.4,
                "ema9_5m": 10.5, "ema20_5m": 10.3, "atr14_5m": 0.2,
                "relative_volume": 1.2, "buy_ratio": 0.7, "sell_ratio": 0.3,
                "closed_bars": [{"close": 10.8}] * 4,
                "closed_bars_5m": [{"close": 10.8}] * 4,
            }
            for symbol in symbols
        }


def test_feature_snapshot_reads_latest_trading_day_intraday_data_on_weekend(tmp_path: Path):
    class HistoricalRepo(_Repo):
        def get_enriched_latest(self):
            return self.rows, date(2026, 8, 21)

    class TrackingFeatureQuotes(_FeatureQuotes):
        def __init__(self):
            self.feature_times = []

        def get_intraday_features(self, symbols, *, asset_type="stock", now=None):
            self.feature_times.append(now)
            return super().get_intraday_features(symbols, asset_type=asset_type, now=now)

    quotes = TrackingFeatureQuotes()
    service = PositionRiskService(
        tmp_path, HistoricalRepo(), quotes, SimpleNamespace(paper_supervisor=None),
    )
    service.store.replace({
        "account": {"name": "账户", "cash": 10_000, "total_asset": 20_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
    }, 0)
    service._preload_history({"600036.SH"})

    features = service.feature_snapshot(
        {"600036.SH"}, datetime(2026, 8, 23, 10, 0),
    )

    assert quotes.feature_times == [datetime(2026, 8, 21, 15, 0)]
    assert features["600036.SH"]["data_as_of"] == "2026-08-21"
    assert features["600036.SH"]["data_status"] == "historical"


def test_preview_rejects_negative_asset_gap_and_confirm_does_not_trade(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service._preload_history({"600036.SH"})
    result = service.preview({
        "revision": 0,
        "account": {"name": "账户", "cash": 1000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 35, "current_price": 36}],
    })
    assert result["can_confirm"] is False
    assert result["reconciliation"]["difference_pct"] < -0.01


def test_preview_rejects_unconfirmed_low_confidence_row(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    result = service.preview({
        "revision": 0,
        "account": {"name": "账户", "cash": 64_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH",
            "name": "招商银行",
            "quantity": 1000,
            "available": 1000,
            "cost_price": 35,
            "current_price": 36,
            "requires_review": True,
        }],
    })

    assert result["can_confirm"] is False
    assert any("低置信度" in issue["message"] for issue in result["issues"])


def test_stop_loss_uses_raw_live_price_and_has_risk_floor(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000, "previous_close_total_asset": 100_000, "high_watermark": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40, "import_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True, "notify": True}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 35.9, "timestamp": "2026-08-07T10:00:00"}
    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    stop = _position_events(service, "stop_loss")[0]
    assert stop["action_pct"] == 100
    assert not _REMOVED_PUBLIC_EVENT_FIELDS & stop.keys()
    assert any(alert["source"] == "position_risk" for alert in quotes.alerts)


def test_disabled_stop_loss_does_not_create_hidden_hard_stop(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": False}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]

    service._evaluate_position(
        portfolio,
        position,
        {"symbol": "600036.SH", "last_price": 35, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
    )

    feature = service.feature_snapshot({"600036.SH"}, datetime(2026, 8, 7, 10, 0))["600036.SH"]
    assert feature["hard_stop_enabled"] is False
    assert feature["hard_stop_price"] is None
    assert not _position_events(service, "stop_loss")
    assert service.store.get_runtime("position:600036.SH")["initial_stop_price"] is None


def test_stop_loss_hysteresis_ignores_one_tick_threshold_noise(tmp_path: Path):
    service = PositionRiskService(
        tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None),
    )
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 1000,
            "available": 1000, "cost_price": 9.7808,
        }],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]

    def evaluate(price: float, second: int) -> None:
        service._evaluate_position(
            portfolio,
            position,
            {
                "symbol": "600036.SH", "last_price": price,
                "timestamp": f"2026-08-07T14:00:{second:02d}",
            },
            datetime(2026, 8, 7, 14, 0, second),
        )

    evaluate(8.80, 0)
    evaluate(8.81, 1)
    evaluate(8.80, 2)
    stop_events = [
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "stop_loss"
    ]
    assert len(stop_events) == 1
    assert service._rule_states["600036.SH:stop_loss"]["active"] is True

    evaluate(8.86, 3)
    assert service._rule_states["600036.SH:stop_loss"]["active"] is False
    evaluate(8.80, 4)
    stop_events = [
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "stop_loss"
    ]
    assert len(stop_events) == 2
    assert len(service._severe_events) == 1


def test_position_rule_uses_private_threshold_and_action(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True, "notify": True, "threshold": -0.05, "action_pct": 25}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 37.9, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    stop = _position_events(service, "stop_loss")[0]
    assert stop["action_pct"] == 25


def test_atr_initial_stop_and_r_use_actual_protection_price(tmp_path: Path):
    alert_store._write_count = 0
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True, "mode": "atr", "atr_multiple": 2.0}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._history["600036.SH"]["atr_14"] = 1.5
    portfolio = service.store.load()
    service._evaluate_position(
        portfolio, portfolio["positions"][0],
        {"symbol": "600036.SH", "last_price": 36.9, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
    )
    runtime = service.store.get_runtime("position:600036.SH")
    assert runtime["initial_stop_price"] == pytest.approx(37.0)
    assert runtime["initial_r"] == pytest.approx(3.0)
    assert runtime["r_multiple"] == pytest.approx(-1.0333333333)
    alert_store._write_count = 0


def test_effective_protection_price_only_moves_up(tmp_path: Path):
    alert_store._write_count = 0
    service = PositionRiskService(tmp_path, _Repo(), _FeatureQuotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {
            "stop_loss": {"enabled": True},
            "take_profit_ladder": {"enabled": True, "active": True, "first_r": 1, "second_r": 1.5},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    features = {"available": True, "fresh": True, "as_of": "2026-08-07T10:00:00", "atr14_5m": 0.2, "session_bars": []}
    service._evaluate_position(portfolio, position, {"last_price": 11, "timestamp": "2026-08-07T10:00:00"}, datetime(2026, 8, 7, 10, 0), intraday_features=features)
    first = service.store.get_runtime("position:600036.SH")["effective_stop_price"]
    service._evaluate_position(portfolio, position, {"last_price": 10.4, "timestamp": "2026-08-07T10:01:00"}, datetime(2026, 8, 7, 10, 1), intraday_features=features)
    assert service.store.get_runtime("position:600036.SH")["effective_stop_price"] >= first
    alert_store._write_count = 0


def test_t_plus_one_requires_entry_date_and_uses_trading_day(tmp_path: Path):
    alert_store._write_count = 0
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10, "entry_date": "2026-08-21"}],
        "overrides": {"600036.SH": {"rules": {"t_plus_one_exit": {"enabled": True, "active": True, "close_before_minutes": 15}}}},
    }, 0)
    portfolio = service.store.load()
    service._evaluate_position(
        portfolio, portfolio["positions"][0],
        {"last_price": 10, "timestamp": "2026-08-24T14:46:00"}, datetime(2026, 8, 24, 14, 46),
    )
    event = _position_events(service, "t_plus_one_exit")
    assert event and event[0]["holding_day"] == 1

    service2 = PositionRiskService(tmp_path / "missing", _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service2.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {"t_plus_one_exit": {"enabled": True, "active": True}}}},
    }, 0)
    portfolio2 = service2.store.load()
    service2._evaluate_position(portfolio2, portfolio2["positions"][0], {"last_price": 9, "timestamp": "2026-08-24T14:46:00"}, datetime(2026, 8, 24, 14, 46))
    insufficient = _position_events(service2, "t_plus_one_exit")
    assert insufficient and insufficient[0]["action_pct"] == 0 and insufficient[0]["risk_stage"] == "data_insufficient"
    alert_store._write_count = 0


def test_auto_sell_is_blocked_on_entry_day_or_without_entry_date(tmp_path: Path):
    class FakeQmt:
        trade_enabled = True

        def __init__(self):
            self.calls = []

        def submit_order(self, payload):
            self.calls.append(payload)
            return {"status": "accepted_pending"}

    qmt = FakeQmt()
    service = PositionRiskService(
        tmp_path,
        _Repo(),
        _Quotes(),
        SimpleNamespace(qmt_trading_service=qmt),
    )
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 1000,
            "available": 1000, "cost_price": 10, "entry_date": "2026-08-24",
        }],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True, "auto_execute": True}}}},
    }, 0)
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    service._latest_quotes["600036.SH"] = {
        "last_price": 9, "timestamp": "2026-08-24T10:00:00",
    }

    status = service._submit_auto_order(
        portfolio, position, "stop_loss", 100, True, "entry-day", datetime(2026, 8, 24, 10, 0),
    )

    assert status[0] == "blocked"
    assert "买入日" in (status[2] or "")
    assert qmt.calls == []

    position.pop("entry_date")
    status = service._submit_auto_order(
        portfolio, position, "stop_loss", 100, True, "unknown-entry", datetime(2026, 8, 25, 10, 0),
    )
    assert status[0] == "blocked"
    assert "entry_date" in (status[2] or "")
    assert qmt.calls == []


def test_t_plus_one_uses_repository_trading_dates(tmp_path: Path):
    class CalendarRepo(_Repo):
        def get_daily_asset(self, _asset_type, _symbol, _start, _end, columns=None):
            return pl.DataFrame({"date": [date(2026, 10, 2)]}).select(columns or ["date"])

    service = PositionRiskService(tmp_path, CalendarRepo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service._asset_types["600036.SH"] = "stock"
    assert service._holding_days_for_position(
        "600036.SH", date(2026, 9, 30), date(2026, 10, 2), True,
    ) == 1


def test_ladder_effective_protection_triggers_after_break_even(tmp_path: Path):
    alert_store._write_count = 0
    service = PositionRiskService(tmp_path, _Repo(), _FeatureQuotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {
            "stop_loss": {"enabled": True},
            "take_profit_ladder": {"enabled": True, "active": True, "first_r": 1, "second_r": 1.5},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    features = {"available": True, "fresh": True, "as_of": "2026-08-07T10:00:00", "atr14_5m": 0.2, "session_bars": []}
    service._evaluate_position(portfolio, position, {"last_price": 11, "timestamp": "2026-08-07T10:00:00"}, datetime(2026, 8, 7, 10, 0), intraday_features=features)
    service._evaluate_position(portfolio, position, {"last_price": 10.02, "timestamp": "2026-08-07T10:01:00"}, datetime(2026, 8, 7, 10, 1), intraday_features=features)
    events = _position_events(service, "take_profit_runner")
    assert len(events) == 1
    assert events[0]["risk_stage"] == "tp_1"
    assert events[0]["action_pct"] == 70
    alert_store._write_count = 0


def test_take_profit_uses_private_threshold_and_action(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {"take_profit": {"enabled": True, "threshold": 0.10, "action_pct": 50}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]

    service._evaluate_position(
        portfolio,
        position,
        {"symbol": "600036.SH", "last_price": 11.2, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
    )

    take_profit = _position_events(service, "take_profit")[0]
    assert take_profit["action_pct"] == 50


def test_take_profit_includes_fees_buffer(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {"take_profit": {"enabled": True, "threshold": 0.10, "fees_buffer": 0.002}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 11.01, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
    )
    assert not _position_events(service, "take_profit")

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 11.03, "timestamp": "2026-08-07T10:01:00"},
        datetime(2026, 8, 7, 10, 1),
    )
    assert _position_events(service, "take_profit")


def test_t_trade_signal_listener_is_disabled(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _FeatureQuotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 10_000, "total_asset": 20_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 500, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {"t_trading": {"enabled": True, "buy_pct": 10, "sell_pct": 25}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    service._flow["600036.SH"].extend(
        {"ts": datetime(2026, 8, 7, 9, 59, index).timestamp(), "amount": 1000.0, "volume": 100.0, "direction": 1, "price": 10.0}
        for index in range(7)
    )

    features = service._intraday_features({"600036.SH"}, datetime(2026, 8, 7, 10, 0))["600036.SH"]
    service._evaluate_position(
        portfolio,
        position,
        {"symbol": "600036.SH", "last_price": 10, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
        {"signal_intraday_avg_cross_up": True},
        features,
    )

    assert not _position_events(service, "t:signal_intraday_avg_cross_up")


def test_t_trade_signal_fails_closed_without_fresh_features(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 10_000, "total_asset": 20_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 500, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {"t_trading": {"enabled": True}}}},
    }, 0)
    portfolio = service.store.load()
    service._evaluate_position(
        portfolio,
        portfolio["positions"][0],
        {"symbol": "600036.SH", "last_price": 10, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0),
        {"signal_intraday_avg_cross_up": True},
    )

    assert not any(item["rule_id"].startswith("t:") for item in _position_events(service))


def test_take_profit_ladder_persists_r_stages_and_protection(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _FeatureQuotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 10_000, "total_asset": 20_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 10}],
        "overrides": {"600036.SH": {"rules": {
            "stop_loss": {"enabled": True},
            "take_profit_ladder": {"enabled": True, "active": True, "first_action_pct": 30, "second_action_pct": 30},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 11, "timestamp": "2026-08-07T10:00:00"},
        datetime(2026, 8, 7, 10, 0), {},
        service._intraday_features({"600036.SH"}, datetime(2026, 8, 7, 10, 0))["600036.SH"],
    )
    first = _position_events(service, "take_profit_ladder")[0]
    assert first["stage"] == "tp_1"
    assert first["r_multiple"] == pytest.approx(1.0)

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 12, "timestamp": "2026-08-07T10:01:00"},
        datetime(2026, 8, 7, 10, 1), {},
        service._intraday_features({"600036.SH"}, datetime(2026, 8, 7, 10, 1))["600036.SH"],
    )
    runtime = service.store.get_runtime("position:600036.SH")
    assert runtime["stage"] == "runner"
    assert set(runtime["triggered_stages"]) == {"tp_1", "tp_2"}


def test_t_trade_volume_is_lot_rounded_and_fail_closed():
    portfolio = {"account": {"cash": 500}}
    position = {"quantity": 1000, "available": 550}
    assert PositionRiskService._t_trade_volume(portfolio, position, 10, "SELL", 25) == 100
    assert PositionRiskService._t_trade_volume(portfolio, position, 10, "BUY", 10) == 0
    assert PositionRiskService._t_trade_volume({"account": {"cash": 1_500}}, position, 10, "BUY", 25) == 100


def test_position_signal_action_does_not_modify_public_signal_value(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {"600036.SH": {"signals": {"builtin": {"signal_macd_dead": {"enabled": True, "notify": True, "direction": "exit", "action_pct": 100}}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    signal = _position_events(service, "signal:signal_macd_dead")[0]
    assert signal["action_pct"] == 100
    assert repo.rows["signal_macd_dead"].to_list() == [True]


def test_position_signal_is_recorded_without_sending_notification(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, repo, quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {"600036.SH": {"signals": {"builtin": {"signal_macd_dead": {"enabled": True, "action_pct": 100}}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    pending = _position_events(service)
    assert [item["rule_id"] for item in pending] == ["signal:signal_macd_dead"]
    assert not any(item["rule_id"] == "signal:signal_macd_dead" for item in quotes.alerts)


def test_default_rule_notification_off_still_records_event(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    current_time = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
    service._latest_quotes["600036.SH"] = {
        "symbol": "600036.SH", "last_price": 35.9,
        "timestamp": current_time.isoformat(),
    }

    service._evaluate_current(now=current_time, force=True)

    assert [item["rule_id"] for item in _position_events(service, "stop_loss")] == ["stop_loss"]
    events = alert_store.list_recent(tmp_path, days=30, source="position_risk")
    assert any(item["rule_id"] == "stop_loss" for item in events)
    assert quotes.alerts == []


def test_builtin_signal_direction_is_read_only_to_position_config(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {"600036.SH": {"signals": {"builtin": {"signal_macd_dead": {"enabled": True, "notify": True, "direction": "entry"}}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    signal = _position_events(service, "signal:signal_macd_dead")[0]
    assert signal["action_pct"] == 25


def test_quote_recovery_does_not_replay_existing_vwap_breakdown(tmp_path: Path):
    quotes = _IntradayQuotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 100_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1, "available": 1, "cost_price": 100}],
        "overrides": {"600036.SH": {"rules": {"vwap_breakdown": {"enabled": True, "notify": True}}}},
    }, 0)
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    service._preload_history({"600036.SH"})
    service._recovery_pending_symbols.add("600036.SH")

    test_day = datetime.now().replace(hour=13, minute=0, second=0, microsecond=0)
    below = {"symbol": "600036.SH", "last_price": 98, "timestamp": test_day.replace(second=1).isoformat()}
    service._evaluate_position(portfolio, position, below, test_day.replace(second=1))
    service._evaluate_position(portfolio, position, below, test_day.replace(second=31))
    assert not _position_events(service)

    above = {**below, "last_price": 102, "timestamp": test_day.replace(minute=1).isoformat()}
    service._evaluate_position(portfolio, position, above, test_day.replace(minute=1))
    service._evaluate_position(portfolio, position, below, test_day.replace(minute=1, second=1))
    service._evaluate_position(portfolio, position, below, test_day.replace(minute=1, second=31))

    events = _position_events(service, "vwap_breakdown")
    assert len(events) == 1
    assert "reasons" not in events[0]
    event = next(
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "vwap_breakdown"
    )
    assert event["rule_name"] == "分时均价负偏离超限"


def test_active_rule_state_survives_service_restart(tmp_path: Path):
    quotes = _Quotes()
    first = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    first.store.replace({
        "account": {"name": "账户", "cash": 64_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "overrides": {"600036.SH": {"rules": {"stop_loss": {"enabled": True, "notify": True}}}},
    }, 0)
    portfolio = first.store.load()
    position = portfolio["positions"][0]
    quote = {"symbol": "600036.SH", "last_price": 35.9, "timestamp": "2026-08-07T10:00:00"}

    first._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0))
    assert [item["rule_id"] for item in quotes.alerts] == ["stop_loss"]

    restarted_quotes = _Quotes()
    restarted = PositionRiskService(
        tmp_path, _Repo(), restarted_quotes, SimpleNamespace(paper_supervisor=None),
    )
    restarted._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 1))
    assert restarted_quotes.alerts == []


def test_account_rule_state_survives_service_restart(tmp_path: Path):
    quotes = _Quotes()
    first = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    first.store.replace({
        "account": {
            "name": "账户", "cash": 1_000, "total_asset": 100_000,
            "previous_close_total_asset": 100_000,
        },
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 2_600,
            "available": 2_600, "cost_price": 38,
        }],
        "overrides": {"600036.SH": {"rules": {"total_exposure": {"enabled": True}}}},
    }, 0)
    first._latest_quotes["600036.SH"] = {"last_price": 36}
    portfolio = first.store.load()

    first._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 0))
    assert any(
        item["rule_id"] == "total_exposure"
        for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
    )

    restarted = PositionRiskService(
        tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None),
    )
    restarted._latest_quotes["600036.SH"] = {"last_price": 36}
    restarted._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 1))
    assert len([
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "total_exposure"
    ]) == 1


def test_normal_event_cooldown_survives_recovery_within_five_minutes(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    assert service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 0))
    assert not service._set_rule("600036.SH", "large_buy", False, datetime(2026, 8, 7, 10, 0, 1))
    assert not service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 4, 59))
    assert not service._set_rule("600036.SH", "large_buy", False, datetime(2026, 8, 7, 10, 5))
    assert service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 5, 1))


def test_quote_interruption_requires_stable_recovery_and_obeys_cooldown(tmp_path: Path):
    service = PositionRiskService(
        tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None),
    )
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 1000,
            "available": 1000, "cost_price": 36,
        }],
        "overrides": {"600036.SH": {"rules": {"quote_interruption": {"enabled": True}}}},
    }, 0)
    service._latest_quotes["600036.SH"] = {
        "symbol": "600036.SH", "last_price": 36,
        "timestamp": "2026-08-07T09:59:00",
    }

    service._check_quote_staleness(datetime(2026, 8, 7, 10, 0))
    service._latest_quotes["600036.SH"]["timestamp"] = "2026-08-07T10:00:01"
    service._check_quote_staleness(datetime(2026, 8, 7, 10, 0, 1))
    service._latest_quotes["600036.SH"]["timestamp"] = "2026-08-07T10:00:30"
    service._check_quote_staleness(datetime(2026, 8, 7, 10, 0, 30))
    assert service._rule_states["600036.SH:quote_interruption"]["active"] is True

    service._latest_quotes["600036.SH"]["timestamp"] = "2026-08-07T10:01:02"
    service._check_quote_staleness(datetime(2026, 8, 7, 10, 1, 2))
    assert service._rule_states["600036.SH:quote_interruption"]["active"] is False

    service._check_quote_staleness(datetime(2026, 8, 7, 10, 1, 40))
    interruptions = [
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "quote_interruption"
    ]
    assert len(interruptions) == 1
    assert service._rule_states["600036.SH:quote_interruption"]["active"] is True


def test_position_risk_timeline_collapses_duplicate_fingerprints():
    rows = _collapse_timeline_events([
        {"ts": 200, "fingerprint": "same", "rule_id": "stop_loss"},
        {"ts": 100, "fingerprint": "same", "rule_id": "stop_loss"},
        {"ts": 150, "rule_id": "ma5_breakdown"},
    ])

    assert len(rows) == 2
    grouped = next(item for item in rows if item.get("fingerprint") == "same")
    assert grouped["ts"] == 200
    assert grouped["first_ts"] == 100
    assert grouped["last_ts"] == 200
    assert grouped["occurrence_count"] == 2


def test_quote_gap_is_scoped_and_waits_for_every_symbol_to_recover(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 80_000, "total_asset": 100_000},
        "positions": [
            {"symbol": "600036.SH", "name": "招商银行", "quantity": 100, "available": 100, "cost_price": 35},
            {"symbol": "000001.SZ", "name": "平安银行", "quantity": 100, "available": 100, "cost_price": 10},
        ],
    }, 0)
    service._flow["600036.SH"].append({"ts": 1, "amount": 1, "volume": 1, "direction": 1, "price": 36})
    service._flow["000001.SZ"].append({"ts": 1, "amount": 1, "volume": 1, "direction": 1, "price": 10})

    service._mark_quote_gap("单股中断", {"600036.SH"})
    assert not service._flow["600036.SH"]
    assert service._flow["000001.SZ"]
    assert service._quote_gap_symbols == {"600036.SH"}

    service._mark_quote_gap("另一标的中断", {"000001.SZ"})
    service._mark_quote_recovered({"600036.SH"})
    assert service._quote_gap_symbols == {"000001.SZ"}
    assert service._runtime_status == "reconnecting"
    service._mark_quote_recovered({"000001.SZ"})
    assert not service._quote_gap_symbols
    assert service._runtime_status == "websocket"
    portfolio = service.store.load()
    for position in portfolio["positions"]:
        service._evaluate_position(
            portfolio,
            position,
            {"symbol": position["symbol"], "last_price": position["cost_price"], "timestamp": "2026-08-07T10:00:00"},
            datetime(2026, 8, 7, 10, 0),
        )
    assert not service._recovery_pending_symbols
    assert service._runtime_reason == "持仓池行情连续性已恢复"


def test_unrealized_loss_uses_current_equity_denominator(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {
            "name": "账户", "cash": 50_000, "total_asset": 200_000,
            "previous_close_total_asset": 200_000,
        },
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 5_000,
            "available": 5_000, "cost_price": 40,
        }],
        "overrides": {"600036.SH": {"rules": {"unrealized_loss": {"enabled": True}}}},
    }, 0)
    portfolio = service.store.load()
    service._latest_quotes["600036.SH"] = {"last_price": 36}

    service._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 0))

    assert any(
        item["rule_id"] == "unrealized_loss"
        for item in _position_events(service)
    )


def test_clustered_severe_events_use_each_position_window(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 80_000, "total_asset": 100_000},
        "positions": [
            {"symbol": "600036.SH", "name": "招商银行", "quantity": 100, "available": 100, "cost_price": 40},
            {"symbol": "000001.SZ", "name": "平安银行", "quantity": 100, "available": 100, "cost_price": 10},
        ],
        "overrides": {
            "600036.SH": {"rules": {"clustered_severe_events": {"enabled": True, "count": 3, "window_seconds": 60}}},
            "000001.SZ": {"rules": {"clustered_severe_events": {"enabled": True, "count": 3, "window_seconds": 300}}},
        },
    }, 0)
    portfolio = service.store.load()
    now = datetime(2026, 8, 7, 10, 0)
    service._latest_quotes.update({
        "600036.SH": {"last_price": 40},
        "000001.SZ": {"last_price": 10},
    })
    service._severe_events.extend([
        (now - timedelta(seconds=120)).timestamp(),
        (now - timedelta(seconds=30)).timestamp(),
        (now - timedelta(seconds=10)).timestamp(),
    ])

    service._evaluate_account(portfolio, now)

    states = service._rule_states
    assert not states.get("600036.SH:clustered_severe_events", {}).get("active")
    assert states["000001.SZ:clustered_severe_events"]["active"] is True
    events = _position_events(service, "clustered_severe_events")
    assert [event["symbol"] for event in events] == ["000001.SZ"]


def test_depth_state_isolated_between_trading_dates(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.set_runtime("depth:600036.SH", {
        "trading_date": "2026-08-06", "sealed": True,
        "had_broken": True, "peak_bid_volume": 10_000,
    })
    sealed = {
        "symbol": "600036.SH", "bid1_price": 40, "bid1_volume": 10_000,
        "ask1_price": None, "ask1_volume": 0,
    }
    service._depth["600036.SH"].extend([sealed] * 3)

    state = service._depth_state(
        "600036.SH", {"limit_up": 40}, datetime(2026, 8, 7, 10, 0),
    )

    assert state["broken"] is False
    assert state["resealed"] is False


def test_stock_and_etf_use_separate_intraday_routes(tmp_path: Path):
    class _MixedRepo(_Repo):
        def resolve_asset_type(self, symbol: str) -> str:
            return "etf" if symbol == "510300.SH" else "stock"

    quotes = _AssetAwareQuotes()
    service = PositionRiskService(tmp_path, _MixedRepo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 90_000, "total_asset": 100_000},
        "positions": [
            {"symbol": "600036.SH", "name": "招商银行", "asset_type": "stock", "quantity": 100, "available": 100, "cost_price": 35},
            {"symbol": "510300.SH", "name": "沪深300ETF", "asset_type": "etf", "quantity": 100, "available": 100, "cost_price": 4},
        ],
    }, 0)

    service.refresh_subscription()
    assert quotes.intraday_consumers["position-risk:stock"] == ({"600036.SH"}, "stock")
    assert quotes.intraday_consumers["position-risk:etf"] == ({"510300.SH"}, "etf")

    now = datetime(2026, 8, 7, 10, 0)
    service._history = {
        "600036.SH": {"raw_close": 36},
        "510300.SH": {"raw_close": 4},
    }
    service._intraday_signals({"600036.SH", "510300.SH"}, now)
    assert {(next(iter(symbols)), asset) for symbols, asset, _ in quotes.signal_assets} == {
        ("600036.SH", "stock"), ("510300.SH", "etf"),
    }
    portfolio = service.store.load()
    etf = next(item for item in portfolio["positions"] if item["asset_type"] == "etf")
    service._evaluate_position(
        portfolio, etf,
        {"symbol": "510300.SH", "last_price": 4, "timestamp": now.isoformat()},
        now,
    )
    assert quotes.snapshot_assets[-1] == ({"510300.SH"}, "etf")


def test_history_preload_retries_after_repository_warmup(tmp_path: Path):
    repo = _Repo()
    calls = 0

    def get_latest():
        nonlocal calls
        calls += 1
        return (pl.DataFrame(), None) if calls == 1 else (repo.rows, None)

    repo.get_enriched_latest = get_latest
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service._preload_history({"600036.SH"})
    assert service._history == {}
    service._preload_history_if_missing()
    assert "600036.SH" in service._history


def test_limit_down_recovery_is_an_entry_signal():
    assert PositionRiskService._signal_direction("signal_limit_down_recovery") == "entry"


def test_builtin_signal_directions_match_the_shared_catalog():
    expected = {
        "signal_ma_golden_5_20": "entry",
        "signal_ma_dead_5_20": "exit",
        "signal_boll_breakout_upper": "entry",
        "signal_boll_breakdown_lower": "exit",
        "signal_volume_surge": "both",
        "signal_limit_up": "entry",
        "signal_limit_down": "exit",
        "signal_limit_down_recovery": "entry",
        "signal_broken_limit_up": "exit",
        "signal_intraday_avg_cross_up": "entry",
        "signal_intraday_avg_cross_down": "exit",
        "signal_intraday_zero_cross_up": "entry",
        "signal_intraday_zero_cross_down": "exit",
    }
    assert {
        signal_id: PositionRiskService._signal_direction(signal_id)
        for signal_id in expected
    } == expected


def test_removed_custom_signal_state_is_cleared(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("csg_take_profit"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    service._set_rule("600036.SH", "signal:csg_take_profit", True, datetime(2026, 8, 7, 10, 0))
    service._history["600036.SH"].pop("csg_take_profit", None)

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 36},
        datetime(2026, 8, 7, 10, 1),
    )

    assert service._rule_states["600036.SH:signal:csg_take_profit"]["active"] is False


def test_quote_age_excludes_lunch_break():
    timestamp = datetime(2026, 8, 7, 11, 30)
    assert PositionRiskService._quote_age_in_session(timestamp, datetime(2026, 8, 7, 13, 0)) == 0
    assert PositionRiskService._quote_age_in_session(timestamp, datetime(2026, 8, 7, 13, 0, 31)) == 31
    assert PositionRiskService._quote_age_in_session(None, datetime(2026, 8, 7, 13, 0)) == 0


def test_depth_requires_three_snapshots_and_detects_break(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    quote = {"limit_up": 40.0}
    sealed = {"symbol": "600036.SH", "bid1_price": 40.0, "bid1_volume": 10000, "ask1_price": None, "ask1_volume": 0}
    service._depth["600036.SH"].extend([sealed, sealed])
    assert service._depth_state("600036.SH", quote, datetime(2026, 8, 7, 10, 0))["sealed"] is False
    service._depth["600036.SH"].append(sealed)
    state = service._depth_state("600036.SH", quote, datetime(2026, 8, 7, 10, 0, 1))
    assert state["sealed"] is True
    assert state["bid_total"] == 10_000
    assert state["ask_total"] == 0
    service._depth["600036.SH"].append({**sealed, "bid1_price": 39.9, "ask1_price": 40.0, "ask1_volume": 100})
    service._depth["600036.SH"].extend([{**sealed, "bid1_price": 39.9}, {**sealed, "bid1_price": 39.9}])
    assert service._depth_state("600036.SH", quote, datetime(2026, 8, 7, 10, 0, 2))["broken"] is True


def test_resealed_limit_up_only_fires_after_a_confirmed_break(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 35}],
        "overrides": {"600036.SH": {"rules": {"resealed_limit_up": {"enabled": True, "notify": True}}}},
    }, 0)
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    quote = {"symbol": "600036.SH", "last_price": 40, "limit_up": 40}
    sealed = {
        "symbol": "600036.SH", "bid1_price": 40, "bid1_volume": 10_000,
        "ask1_price": None, "ask1_volume": 0,
    }
    open_depth = {**sealed, "bid1_price": 39.9, "ask1_price": 40, "ask1_volume": 100}

    service._depth["600036.SH"].extend([sealed] * 3)
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0))
    assert not any(item["rule_id"] == "resealed_limit_up" for item in quotes.alerts)

    service._depth["600036.SH"].append(open_depth)
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0, 1))
    service._depth["600036.SH"].extend([sealed] * 3)
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0, 2))

    event = next(item for item in quotes.alerts if item["rule_id"] == "resealed_limit_up")
    assert event["rule_name"] == "涨停回封"


def test_stale_depth_does_not_trigger_orderbook_rules(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    old = datetime(2026, 8, 7, 9, 59).timestamp()
    depth = {
        "symbol": "600036.SH", "bid_volumes": [10], "ask_volumes": [100],
        "bid1_price": 35.9, "bid1_volume": 10, "ask1_price": 36, "ask1_volume": 100,
        "received_at": old,
    }
    service._depth["600036.SH"].extend([depth] * 3)
    state = service._depth_state(
        "600036.SH", {"limit_up": 40}, datetime(2026, 8, 7, 10, 0),
    )
    assert state["imbalance"] is None


def test_breaking_limit_up_does_not_also_emit_seal_shrink(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 35}],
        "overrides": {"600036.SH": {"rules": {
            "broken_limit_up": {"enabled": True, "notify": True},
            "sealed_order_shrink_50": {"enabled": True, "notify": True},
            "sealed_order_shrink_80": {"enabled": True, "notify": True},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    quote = {"symbol": "600036.SH", "last_price": 39.9, "limit_up": 40.0, "timestamp": "2026-08-07T10:00:00"}
    sealed = {"symbol": "600036.SH", "bid1_price": 40.0, "bid1_volume": 10_000, "ask1_price": None, "ask1_volume": 0}
    service._depth["600036.SH"].extend([sealed, sealed, sealed])
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0))
    quotes.alerts.clear()
    broken = {**sealed, "bid1_price": 39.9, "bid1_volume": 0, "ask1_price": 40.0, "ask1_volume": 100}
    service._depth["600036.SH"].extend([broken, broken, broken])

    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 10, 0, 1))

    emitted = {item["rule_id"] for item in quotes.alerts}
    assert "broken_limit_up" in emitted
    assert "sealed_order_shrink_50" not in emitted
    assert "sealed_order_shrink_80" not in emitted


def test_symbol_override_controls_builtin_signal_and_monitor_action(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {
            "600036.SH": {
                "rules": {},
                "signals": {
                    "builtin": {"signal_macd_dead": {"enabled": False}},
                    "custom": {},
                    "monitor_rules": {"rule-one": {"action_pct": 50}},
                },
            },
        },
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    assert _position_events(service) == []

    service._ingest_monitor_events([{
        "symbol": "600036.SH",
        "rule_id": "rule-one",
        "severity": "warn",
        "message": "监控规则命中",
        "fingerprint": "monitor-rule-one",
    }])
    assert _position_events(service) == []


def test_raw_fund_evidence_does_not_emit_independent_events(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {"600036.SH": {"rules": {
            "large_sell": {
                "notify": True, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"notify": True},
            "fund_flow_pressure": {"sustain_seconds": 0},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    started = datetime(2026, 8, 7, 10, 0)
    for offset in range(3):
        service._flow["600036.SH"].append({
            "ts": started.timestamp() - offset,
            "amount": 100 if offset else 1_000,
            "volume": 50,
            "direction": -1,
            "price": 36,
        })
    quote = {"symbol": "600036.SH", "last_price": 36, "timestamp": started.isoformat()}

    service._evaluate_position(portfolio, position, quote, started)
    assert quotes.alerts == []
    service._evaluate_position(portfolio, position, quote, started.replace(second=10))

    assert quotes.alerts == []


def test_large_order_uses_configured_thresholds(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    now = datetime(2026, 8, 7, 10, 0)
    for offset, amount in enumerate([100, 100, 100, 100, 100, 100, 1_000]):
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - offset,
            "amount": amount,
            "volume": 1,
            "direction": 1,
            "price": 36,
        })

    assert service._flow_state("600036.SH", now)["large_buy"] is False
    configured = service._flow_state("600036.SH", now, {
        "window_seconds": 60,
        "min_samples": 7,
        "min_amount": 500,
        "mad_multiplier": 0,
        "min_z_score": 2.5,
        "direction_ratio": 0.65,
    })
    assert configured["large_buy"] is True
    assert configured["large_sell"] is False


def test_large_order_outlier_must_match_the_dominant_direction(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    now = datetime(2026, 8, 7, 10, 0)
    amounts = [100, 100, 100, 100, 100, 100, 1_000]
    directions = [-1, -1, -1, -1, -1, -1, 1]
    for offset, (amount, direction) in enumerate(zip(amounts, directions, strict=True)):
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - offset,
            "amount": amount,
            "volume": 1,
            "direction": direction,
            "price": 36,
        })
    configured = {
        "window_seconds": 60,
        "min_samples": 7,
        "min_amount": 500,
        "mad_multiplier": 0,
        "min_z_score": 2.5,
        "direction_ratio": 0.65,
    }

    state = service._flow_state("600036.SH", now, configured)
    assert state["large_buy"] is False
    assert state["large_sell"] is False


def test_large_order_summary_uses_the_matching_direction_z_score(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    now = datetime(2026, 8, 7, 10, 0)
    for offset, (amount, direction) in enumerate([
        (100, 1), (100, 1), (100, 1), (100, 1), (100, 1), (100, 1), (1_000, -1),
    ]):
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - offset,
            "amount": amount,
            "volume": 1,
            "direction": direction,
            "price": 36,
        })
    configured = {
        "min_amount": 500, "mad_multiplier": 0,
        "min_z_score": 2.5, "direction_ratio": 0.10,
    }

    state = service._flow_state("600036.SH", now, configured)
    assert state["large_sell"] is True
    assert "卖向异常单" in state["sell_summary"]
    assert state["sell_z_score"] > state["buy_z_score"]


def test_large_buy_is_only_internal_fund_evidence(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "overrides": {"600036.SH": {"rules": {"large_buy": {
            "notify": True, "min_amount": 500, "mad_multiplier": 0,
            "min_z_score": 2.5, "direction_ratio": 0.65,
        }}}},
    }, 0)
    now = datetime(2026, 8, 7, 10, 0)
    for offset, amount in enumerate([100, 100, 100, 100, 100, 100, 1_000]):
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - offset, "amount": amount, "volume": 1,
            "direction": 1, "price": 36,
        })
    service._depth["600036.SH"].extend([{
        "symbol": "600036.SH", "bid_volumes": [10], "ask_volumes": [100],
        "bid1_price": 35.9, "bid1_volume": 10, "ask1_price": 36, "ask1_volume": 100,
    }] * 3)
    portfolio = service.store.load()
    service._evaluate_position(
        portfolio, portfolio["positions"][0],
        {"symbol": "600036.SH", "last_price": 36, "timestamp": now.isoformat()},
        now,
    )
    assert quotes.alerts == []


def test_fund_pressure_requires_two_evidence_price_confirmation_and_sustain(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "overrides": {"600036.SH": {"rules": {
            "large_sell": {
                "enabled": True,
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"enabled": True, "direction_ratio": 0.65, "sustain_seconds": 0},
            "fund_flow_pressure": {
                "enabled": True,
                "notify": True, "sustain_seconds": 30, "price_buffer": 0.002,
            },
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    started = datetime(2026, 8, 7, 10, 0)
    for offset, amount in enumerate([100, 100, 100, 100, 100, 100, 1_000]):
        service._flow["600036.SH"].append({
            "ts": started.timestamp() - offset,
            "amount": amount,
            "volume": 1,
            "direction": -1,
            "price": 36.0 if offset else 35.8,
        })
    service._flow["600036.SH"].append({
        "ts": started.timestamp() - 61,
        "amount": 100,
        "volume": 1,
        "direction": -1,
        "price": 36.0,
    })
    quote = {"symbol": "600036.SH", "last_price": 35.8, "timestamp": started.isoformat()}

    service._evaluate_position(portfolio, position, quote, started)
    service._evaluate_position(portfolio, position, quote, started.replace(second=29))
    assert quotes.alerts == []

    service._evaluate_position(portfolio, position, quote, started.replace(second=30))

    assert len(quotes.alerts) == 1
    event = quotes.alerts[0]
    assert event["rule_id"] == "fund_flow_pressure"
    assert event["action_pct"] == 0
    assert not _REMOVED_PUBLIC_EVENT_FIELDS & event.keys()


def test_fund_pressure_three_evidence_and_sharp_drop_upgrades_action(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "overrides": {"600036.SH": {"rules": {
            "large_sell": {
                "enabled": True,
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"enabled": True, "sustain_seconds": 0},
            "orderbook_imbalance": {"enabled": True, "sustain_seconds": 0},
            "fund_flow_pressure": {
                "enabled": True,
                "notify": True, "sustain_seconds": 0, "strong_price_drop": 0.01,
            },
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    now = datetime(2026, 8, 7, 10, 1)
    for offset, amount in enumerate([100, 100, 100, 100, 100, 100, 1_000]):
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - offset,
            "amount": amount, "volume": 1, "direction": -1, "price": 35.0,
        })
    service._flow["600036.SH"].append({
        "ts": now.timestamp() - 61, "amount": 100, "volume": 1,
        "direction": -1, "price": 36.0,
    })
    depth = {
        "symbol": "600036.SH", "bid_volumes": [10], "ask_volumes": [100],
        "bid1_price": 35, "bid1_volume": 10, "ask1_price": 35.01,
        "ask1_volume": 100, "received_at": now.timestamp(),
    }
    service._depth["600036.SH"].extend([depth] * 3)

    service._evaluate_position(
        portfolio, position,
        {"symbol": "600036.SH", "last_price": 35, "timestamp": now.isoformat()},
        now,
    )

    event = quotes.alerts[0]
    assert event["rule_id"] == "fund_flow_pressure"
    assert event["action_pct"] == 50
    assert not _REMOVED_PUBLIC_EVENT_FIELDS & event.keys()


def test_fund_pressure_requires_recovery_and_respects_group_cooldown(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "overrides": {"600036.SH": {"rules": {
            "large_sell": {
                "enabled": True,
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"enabled": True, "sustain_seconds": 0},
            "fund_flow_pressure": {
                "enabled": True,
                "notify": True, "sustain_seconds": 0, "recovery_seconds": 60,
                "cooldown_seconds": 900,
            },
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    started = datetime(2026, 8, 7, 10, 0)

    def set_sell_pressure(now: datetime) -> None:
        service._flow["600036.SH"].clear()
        for offset, amount in enumerate([100, 100, 100, 100, 100, 100, 1_000]):
            service._flow["600036.SH"].append({
                "ts": now.timestamp() - offset,
                "amount": amount, "volume": 1, "direction": -1, "price": 35.8,
            })
        service._flow["600036.SH"].append({
            "ts": now.timestamp() - 61,
            "amount": 100, "volume": 1, "direction": -1, "price": 36,
        })

    def evaluate(now: datetime) -> None:
        service._evaluate_position(
            portfolio, position,
            {"symbol": "600036.SH", "last_price": 35.8, "timestamp": now.isoformat()},
            now,
        )

    set_sell_pressure(started)
    evaluate(started)
    assert len(quotes.alerts) == 1

    service._flow["600036.SH"].clear()
    recovery_started = started.replace(minute=1)
    evaluate(recovery_started)
    evaluate(recovery_started.replace(second=59))
    assert service._rule_states["600036.SH:fund_flow_pressure"]["active"] is True
    evaluate(recovery_started.replace(minute=2, second=0))
    assert service._rule_states["600036.SH:fund_flow_pressure"]["active"] is False

    within_cooldown = started.replace(minute=10)
    set_sell_pressure(within_cooldown)
    evaluate(within_cooldown)
    assert len(quotes.alerts) == 1

    service._flow["600036.SH"].clear()
    evaluate(started.replace(minute=11))
    evaluate(started.replace(minute=12))
    after_cooldown = started.replace(minute=16)
    set_sell_pressure(after_cooldown)
    evaluate(after_cooldown)
    assert len(quotes.alerts) == 2


def test_sealed_order_shrink_requires_current_seal(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.set_runtime("depth:600036.SH", {
        "sealed": False,
        "peak_bid_volume": 10_000,
    })
    open_depth = {
        "symbol": "600036.SH", "bid1_price": 39.9, "bid1_volume": 100,
        "ask1_price": 40, "ask1_volume": 100,
    }
    service._depth["600036.SH"].extend([open_depth] * 3)
    state = service._depth_state(
        "600036.SH", {"limit_up": 40}, datetime(2026, 8, 7, 10, 0),
    )
    assert state["sealed"] is False
    assert state["shrink_ratio"] == 0


def test_two_independent_exit_signals_upgrade_to_half_reduction(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns([
        pl.lit(True).alias("signal_macd_dead"),
        pl.lit(True).alias("signal_n_day_low"),
    ])
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000, "high_watermark": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35, "import_price": 36}],
        "overrides": {"600036.SH": {"signals": {"builtin": {
            "signal_macd_dead": {"enabled": True, "notify": True},
            "signal_n_day_low": {"enabled": True, "notify": True},
        }}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}
    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    events = _position_events(service)
    assert max(event["action_pct"] for event in events) == 50
    assert all("reasons" not in event for event in events)


def test_exit_signal_resonance_survives_service_restart(tmp_path: Path):
    first_repo = _Repo()
    first_repo.rows = first_repo.rows.with_columns([
        pl.lit(False).alias("signal_macd_dead"),
        pl.lit(True).alias("signal_n_day_low"),
    ])
    first = PositionRiskService(tmp_path, first_repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    first.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "overrides": {"600036.SH": {"signals": {"builtin": {
            "signal_macd_dead": {"enabled": True},
            "signal_n_day_low": {"enabled": True},
        }}}},
    }, 0)
    first._preload_history({"600036.SH"})
    first._latest_quotes["600036.SH"] = {
        "symbol": "600036.SH", "last_price": 36,
        "timestamp": "2026-08-07T10:00:00",
    }
    first._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    second_repo = _Repo()
    second_repo.rows = second_repo.rows.with_columns([
        pl.lit(True).alias("signal_macd_dead"),
        pl.lit(False).alias("signal_n_day_low"),
    ])
    restarted = PositionRiskService(
        tmp_path, second_repo, _Quotes(), SimpleNamespace(paper_supervisor=None),
    )
    restarted._preload_history({"600036.SH"})
    restarted._latest_quotes["600036.SH"] = {
        "symbol": "600036.SH", "last_price": 36,
        "timestamp": "2026-08-07T10:04:00",
    }
    restarted._evaluate_current(now=datetime(2026, 8, 7, 10, 4), force=True)

    macd = _position_events(restarted, "signal:signal_macd_dead")[0]
    assert macd["action_pct"] == 50
    assert "reasons" not in macd


def test_persistent_daily_signal_emits_once_for_each_trading_date(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns([
        pl.lit(datetime(2026, 8, 13).date()).alias("date"),
        pl.lit(True).alias("signal_volume_surge"),
    ])
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "overrides": {"600036.SH": {"signals": {"builtin": {"signal_volume_surge": {"enabled": True}}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    quote = {"symbol": "600036.SH", "last_price": 36}

    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 13, 14, 59))
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 13, 15, 0))
    repo.rows = repo.rows.with_columns(pl.lit(datetime(2026, 8, 14).date()).alias("date"))
    service._preload_history({"600036.SH"})
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 14, 9, 31))

    events = [
        item for item in alert_store.list_recent(tmp_path, days=30, source="position_risk")
        if item["rule_id"] == "signal:signal_volume_surge"
    ]
    assert len(events) == 2


def test_position_risk_sse_channel_is_independent():
    service = QuoteService()
    subscriber = service.subscribe()
    service.notify_position_risk_updated()
    payload = subscriber.pop()
    assert payload["position_risk_updated"] is True
    assert payload["large_orders_updated"] is False
    service.unsubscribe(subscriber)


def test_ws_capacity_failure_falls_back_with_entire_portfolio(tmp_path: Path):
    class _Hub:
        def __init__(self):
            self.registered = None

        def unregister(self, _account_id):
            pass

        def register(self, _account_id, _mode, symbols, _asset_type, _queue):
            self.registered = set(symbols)
            raise ValueError("WebSocket 去重订阅最多 1 只")

    class _PollingQuotes(_Quotes):
        def __init__(self):
            super().__init__()
            self.consumer_symbols = set()

        def set_symbol_consumer(self, _consumer, symbols):
            self.consumer_symbols = set(symbols)

    hub = _Hub()
    quotes = _PollingQuotes()
    state = SimpleNamespace(
        paper_supervisor=SimpleNamespace(hub=hub),
        capabilities=CapabilitySet({Cap.WEBSOCKET: CapabilityLimits(subscribe=1)}),
    )
    service = PositionRiskService(tmp_path, _Repo(), quotes, state)
    service.store.replace({
        "account": {"name": "账户", "cash": 1000, "total_asset": 2000, "previous_close_total_asset": 2000},
        "positions": [
            {"symbol": "600036.SH", "name": "招商银行", "quantity": 10, "available": 10, "cost_price": 35},
            {"symbol": "000001.SZ", "name": "平安银行", "quantity": 10, "available": 10, "cost_price": 10},
        ],
    }, 0)
    service.refresh_subscription()
    assert hub.registered == {"600036.SH", "000001.SZ"}
    assert quotes.consumer_symbols == hub.registered
    assert service.view()["runtime"]["status"] == "polling_degraded"
