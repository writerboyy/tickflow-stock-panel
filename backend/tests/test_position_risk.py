from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import polars as pl
import pytest

from app.services import alert_store
from app.services.position_risk_ocr import import_position_image, parse_position_tokens
from app.services.position_risk_service import PositionRiskService, localize_position_risk_text
from app.services.position_risk_store import PositionRiskStore, RevisionConflict
from app.services.qmt_trading import QmtRedisRpcClient, QmtRpcError, QmtTradingService
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


def test_portfolio_store_revision_and_recommendation_lifecycle(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    saved = store.replace({
        "account": {"name": "账户", "cash": 1000, "total_asset": 2000, "previous_close_total_asset": 2100},
        "positions": [],
    }, 0)
    assert saved["revision"] == 1
    with pytest.raises(RevisionConflict):
        store.replace(saved, 0)

    recommendation = store.add_recommendation({
        "fingerprint": "one",
        "symbol": "600036.SH",
        "scope": "symbol",
        "rule_id": "stop_loss",
        "severity": "critical",
        "risk_score": 85,
        "action": "清仓建议",
        "reduction_pct": 100,
        "reasons": ["测试"],
        "source_ids": ["stop_loss"],
        "portfolio_revision": 1,
    })
    assert recommendation["status"] == "pending"
    assert store.set_recommendation_status(recommendation["id"], "confirmed")["status"] == "confirmed"
    assert store.load()["positions"] == []


def test_portfolio_store_adds_new_large_order_defaults_to_existing_rule(tmp_path: Path):
    store = PositionRiskStore(tmp_path)
    saved = store.replace({
        "template": {"rules": {"large_buy": {"enabled": False, "action_pct": 0}}},
    }, 0)

    large_buy = saved["template"]["rules"]["large_buy"]
    assert large_buy["enabled"] is False
    assert large_buy["notify"] is False
    assert large_buy["min_amount"] == 1_000_000
    assert large_buy["direction_ratio"] == 0.65


def test_position_risk_notifications_default_to_off(tmp_path: Path):
    portfolio = PositionRiskStore(tmp_path).load()

    assert all(rule["enabled"] is True for rule in portfolio["template"]["rules"].values())
    assert all(rule["notify"] is False for rule in portfolio["template"]["rules"].values())


def _qmt_settings(**overrides):
    values = {
        "qmt_enabled": True,
        "qmt_redis_host": "127.0.0.1",
        "qmt_redis_port": 6379,
        "qmt_redis_db": 5,
        "qmt_redis_username": "",
        "qmt_redis_password": "secret",
        "qmt_account_id": "account-1",
        "qmt_rpc_timeout_seconds": 1,
        "qmt_trade_enabled": False,
        "qmt_max_order_lots": 1,
        "qmt_account_type": "CREDIT",
        "qmt_auto_sync": True,
        "qmt_auto_sync_interval_seconds": 30,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_qmt_client_requires_explicit_enable_and_complete_credentials():
    disabled = QmtRedisRpcClient(_qmt_settings(qmt_enabled=False))
    assert disabled.configured is False
    assert "QMT_ENABLED" in disabled.configuration_reason

    incomplete = QmtRedisRpcClient(_qmt_settings(qmt_redis_password=""))
    assert incomplete.configured is False
    assert "QMT_REDIS_PASSWORD" in incomplete.configuration_reason


def test_qmt_client_forces_resp2_for_legacy_cloud_redis(monkeypatch):
    captured = {}

    class FakeRedis:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr("app.services.qmt_trading.redis.Redis", FakeRedis)
    client = QmtRedisRpcClient(_qmt_settings())
    client._redis()
    assert captured["protocol"] == 2


def test_qmt_trading_service_rejects_order_when_trade_switch_is_off(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings())
    with pytest.raises(QmtRpcError, match="交易开关"):
        service._validate_order({"action": "BUY", "symbol": "600036.SH", "volume": 100, "price": 35}, {"positions": []})


def test_qmt_trading_service_enforces_one_lot_and_sell_available_volume(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    service.trade_enabled = True
    snapshot = {"positions": [{"symbol": "600036.SH", "available": 100}]}
    with pytest.raises(ValueError, match="不超过 100 股"):
        service._validate_order({"action": "SELL", "symbol": "600036.SH", "volume": 200, "price": 35}, snapshot)
    with pytest.raises(ValueError, match="可用持仓不足"):
        service._validate_order({"action": "SELL", "symbol": "600036.SH", "volume": 100, "price": 35}, {"positions": [{"symbol": "600036.SH", "available": 0}]})
    assert service._validate_order({"action": "SELL", "symbol": "600036.SH", "volume": 100, "price": 35}, snapshot)["volume"] == 100


def test_qmt_runtime_trade_switch_starts_off_and_requires_sync(tmp_path: Path):
    service = QmtTradingService(tmp_path, _qmt_settings(qmt_trade_enabled=True))
    assert service.status()["trade_authorized"] is True
    assert service.status()["trade_enabled"] is False
    with pytest.raises(QmtRpcError, match="先成功同步"):
        service.set_trade_enabled(True)
    service._last_snapshot = {"synced_at": "2026-08-14T00:00:00+00:00"}
    service._last_status = {"state": "ready"}
    assert service.set_trade_enabled(True)["trade_enabled"] is True


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
    client = QmtRedisRpcClient(_qmt_settings())
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
        "template": {"rules": {"stop_loss": {"notify": True}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 35.9, "timestamp": "2026-08-07T10:00:00"}
    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    pending = service.store.list_recommendations("pending")
    stop = next(item for item in pending if item["rule_id"] == "stop_loss")
    assert stop["risk_score"] >= 85
    assert stop["reduction_pct"] == 100
    assert any(alert["source"] == "position_risk" for alert in quotes.alerts)


def test_position_rule_uses_private_threshold_and_action(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "template": {"rules": {"stop_loss": {"notify": True, "threshold": -0.05, "action_pct": 25}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 37.9, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    stop = next(item for item in service.store.list_recommendations("pending") if item["rule_id"] == "stop_loss")
    assert stop["reduction_pct"] == 25


def test_position_signal_action_does_not_modify_public_signal_value(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "template": {"signals": {"builtin": {"signal_macd_dead": {"enabled": True, "notify": True, "direction": "exit", "action_pct": 100}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    signal = next(item for item in service.store.list_recommendations("pending") if item["rule_id"] == "signal:signal_macd_dead")
    assert signal["reduction_pct"] == 100
    assert repo.rows["signal_macd_dead"].to_list() == [True]


def test_position_signal_is_recorded_without_sending_notification(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, repo, quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "template": {"signals": {"builtin": {"signal_macd_dead": {"action_pct": 100}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    pending = service.store.list_recommendations("pending")
    assert [item["rule_id"] for item in pending] == ["signal:signal_macd_dead"]
    assert not any(item["rule_id"] == "signal:signal_macd_dead" for item in quotes.alerts)


def test_default_rule_notification_off_still_records_event_and_recommendation(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 35.9, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    assert [item["rule_id"] for item in service.store.list_recommendations("pending")] == ["stop_loss"]
    events = alert_store.list_recent(tmp_path, source="position_risk")
    assert any(item["rule_id"] == "stop_loss" for item in events)
    assert quotes.alerts == []


def test_builtin_signal_direction_is_read_only_to_position_config(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns(pl.lit(True).alias("signal_macd_dead"))
    service = PositionRiskService(tmp_path, repo, _Quotes(), SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "template": {"signals": {"builtin": {"signal_macd_dead": {"notify": True, "direction": "entry"}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    signal = next(item for item in service.store.list_recommendations("pending") if item["rule_id"] == "signal:signal_macd_dead")
    assert signal["reduction_pct"] == 25


def test_quote_recovery_does_not_replay_existing_vwap_breakdown(tmp_path: Path):
    quotes = _IntradayQuotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 100_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1, "available": 1, "cost_price": 100}],
        "template": {"rules": {"vwap_breakdown": {"notify": True}}},
    }, 0)
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    service._preload_history({"600036.SH"})
    service._recovery_pending_symbols.add("600036.SH")

    below = {"symbol": "600036.SH", "last_price": 98, "timestamp": "2026-08-07T13:00:01"}
    service._evaluate_position(portfolio, position, below, datetime(2026, 8, 7, 13, 0, 1))
    service._evaluate_position(portfolio, position, below, datetime(2026, 8, 7, 13, 0, 31))
    assert not service.store.list_recommendations("pending")

    above = {**below, "last_price": 102, "timestamp": "2026-08-07T13:01:00"}
    service._evaluate_position(portfolio, position, above, datetime(2026, 8, 7, 13, 1))
    service._evaluate_position(portfolio, position, below, datetime(2026, 8, 7, 13, 1, 1))
    service._evaluate_position(portfolio, position, below, datetime(2026, 8, 7, 13, 1, 31))

    recommendation = service.store.list_recommendations("pending")
    assert [item["rule_id"] for item in recommendation] == ["vwap_breakdown"]
    assert recommendation[0]["reasons"] == [
        "现价 98.000，VWAP 100.000，负偏离 2.00%（阈值 1.00%）持续 30 秒"
    ]
    event = next(
        item for item in alert_store.list_recent(tmp_path, source="position_risk")
        if item["rule_id"] == "vwap_breakdown"
    )
    assert event["rule_name"] == "分时均价负偏离超限"


def test_active_rule_state_survives_service_restart(tmp_path: Path):
    quotes = _Quotes()
    first = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    first.store.replace({
        "account": {"name": "账户", "cash": 64_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 40}],
        "template": {"rules": {"stop_loss": {"notify": True}}},
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
    }, 0)
    first._latest_quotes["600036.SH"] = {"last_price": 36}
    portfolio = first.store.load()

    first._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 0))
    assert any(
        item["rule_id"] == "total_exposure"
        for item in alert_store.list_recent(tmp_path, source="position_risk")
    )

    restarted = PositionRiskService(
        tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None),
    )
    restarted._latest_quotes["600036.SH"] = {"last_price": 36}
    restarted._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 1))
    assert len([
        item for item in alert_store.list_recent(tmp_path, source="position_risk")
        if item["rule_id"] == "total_exposure"
    ]) == 1


def test_normal_event_cooldown_survives_recovery_within_five_minutes(tmp_path: Path):
    service = PositionRiskService(tmp_path, _Repo(), _Quotes(), SimpleNamespace(paper_supervisor=None))
    assert service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 0))
    assert not service._set_rule("600036.SH", "large_buy", False, datetime(2026, 8, 7, 10, 0, 1))
    assert not service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 4, 59))
    assert not service._set_rule("600036.SH", "large_buy", False, datetime(2026, 8, 7, 10, 5))
    assert service._set_rule("600036.SH", "large_buy", True, datetime(2026, 8, 7, 10, 5, 1))


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
    }, 0)
    portfolio = service.store.load()
    service._latest_quotes["600036.SH"] = {"last_price": 36}

    service._evaluate_account(portfolio, datetime(2026, 8, 7, 10, 0))

    assert any(
        item["rule_id"] == "unrealized_loss"
        for item in alert_store.list_recent(tmp_path, source="position_risk")
    )


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
        "template": {"rules": {"resealed_limit_up": {"notify": True}}},
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
        "template": {"rules": {
            "broken_limit_up": {"notify": True},
            "sealed_order_shrink_50": {"notify": True},
            "sealed_order_shrink_80": {"notify": True},
        }},
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
        "template": {
            "rules": {},
            "signals": {"builtin": {}, "custom": {}, "monitor_rules": {"rule-one": {"notify": True, "action_pct": 25}}},
        },
        "overrides": {
            "600036.SH": {
                "signals": {
                    "builtin": {"signal_macd_dead": {"enabled": False}},
                    "monitor_rules": {"rule-one": {"action_pct": 50}},
                },
            },
        },
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    assert service.store.list_recommendations("pending") == []

    service._ingest_monitor_events([{
        "symbol": "600036.SH",
        "rule_id": "rule-one",
        "severity": "warn",
        "message": "监控规则命中",
        "fingerprint": "monitor-rule-one",
    }])
    pending = service.store.list_recommendations("pending")
    assert pending[0]["rule_id"] == "monitor:rule-one"
    assert pending[0]["reduction_pct"] == 50


def test_raw_fund_evidence_does_not_emit_independent_events(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
        "template": {"rules": {
            "large_sell": {
                "notify": True, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"notify": True},
            "fund_flow_pressure": {"sustain_seconds": 0},
        }},
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
        "template": {"rules": {"large_buy": {
            "notify": True, "min_amount": 500, "mad_multiplier": 0,
            "min_z_score": 2.5, "direction_ratio": 0.65,
        }}},
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
        "template": {"rules": {
            "large_sell": {
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"direction_ratio": 0.65, "sustain_seconds": 0},
            "fund_flow_pressure": {
                "notify": True, "sustain_seconds": 30, "price_buffer": 0.002,
            },
        }},
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
    assert set(event["source_ids"]) == {"large_sell", "continuous_outflow"}
    assert event["suggestion_pct"] == 0
    assert event["risk_score"] == 50


def test_fund_pressure_three_evidence_and_sharp_drop_upgrades_action(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "template": {"rules": {
            "large_sell": {
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"sustain_seconds": 0},
            "orderbook_imbalance": {"sustain_seconds": 0},
            "fund_flow_pressure": {
                "notify": True, "sustain_seconds": 0, "strong_price_drop": 0.01,
            },
        }},
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
    assert event["suggestion_pct"] == 50
    assert event["risk_score"] == 80


def test_fund_pressure_requires_recovery_and_respects_group_cooldown(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000},
        "positions": [{
            "symbol": "600036.SH", "name": "招商银行", "quantity": 500,
            "available": 500, "cost_price": 35,
        }],
        "template": {"rules": {
            "large_sell": {
                "min_samples": 7, "min_amount": 500, "mad_multiplier": 0,
                "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"sustain_seconds": 0},
            "fund_flow_pressure": {
                "notify": True, "sustain_seconds": 0, "recovery_seconds": 60,
                "cooldown_seconds": 900,
            },
        }},
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
        "template": {"signals": {"builtin": {
            "signal_macd_dead": {"notify": True},
            "signal_n_day_low": {"notify": True},
        }}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}
    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    pending = service.store.list_recommendations("pending")
    assert pending[0]["reduction_pct"] == 50
    assert any("共振" in reason for reason in pending[0]["reasons"])


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

    macd = next(
        item for item in restarted.store.list_recommendations("pending")
        if item["rule_id"] == "signal:signal_macd_dead"
    )
    assert macd["reduction_pct"] == 50
    assert any("共振" in reason for reason in macd["reasons"])


def test_persistent_daily_signal_emits_once_for_each_trading_date(tmp_path: Path):
    repo = _Repo()
    repo.rows = repo.rows.with_columns([
        pl.lit(datetime(2026, 8, 7).date()).alias("date"),
        pl.lit(True).alias("signal_volume_surge"),
    ])
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
    quote = {"symbol": "600036.SH", "last_price": 36}

    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 14, 59))
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 7, 15, 0))
    repo.rows = repo.rows.with_columns(pl.lit(datetime(2026, 8, 10).date()).alias("date"))
    service._preload_history({"600036.SH"})
    service._evaluate_position(portfolio, position, quote, datetime(2026, 8, 10, 9, 31))

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
