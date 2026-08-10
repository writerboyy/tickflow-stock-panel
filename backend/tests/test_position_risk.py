from __future__ import annotations

from pathlib import Path
from datetime import datetime
from types import SimpleNamespace

import polars as pl
import pytest

from app.services.position_risk_ocr import import_position_image, parse_position_tokens
from app.services.position_risk_service import PositionRiskService, localize_position_risk_text
from app.services.position_risk_store import PositionRiskStore, RevisionConflict
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
    assert large_buy["min_amount"] == 1_000_000
    assert large_buy["direction_ratio"] == 0.65


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
        "template": {"rules": {"stop_loss": {"threshold": -0.05, "action_pct": 25}}},
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
        "template": {"signals": {"builtin": {"signal_macd_dead": {"enabled": True, "direction": "exit", "action_pct": 100}}}},
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}

    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)

    signal = next(item for item in service.store.list_recommendations("pending") if item["rule_id"] == "signal:signal_macd_dead")
    assert signal["reduction_pct"] == 100
    assert repo.rows["signal_macd_dead"].to_list() == [True]


def test_quote_recovery_does_not_replay_existing_vwap_breakdown(tmp_path: Path):
    quotes = _IntradayQuotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 100_000, "total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1, "available": 1, "cost_price": 100}],
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
    assert service._depth_state("600036.SH", quote, datetime(2026, 8, 7, 10, 0, 1))["sealed"] is True
    service._depth["600036.SH"].append({**sealed, "bid1_price": 39.9, "ask1_price": 40.0, "ask1_volume": 100})
    service._depth["600036.SH"].extend([{**sealed, "bid1_price": 39.9}, {**sealed, "bid1_price": 39.9}])
    assert service._depth_state("600036.SH", quote, datetime(2026, 8, 7, 10, 0, 2))["broken"] is True


def test_breaking_limit_up_does_not_also_emit_seal_shrink(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 60_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 1000, "available": 1000, "cost_price": 35}],
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
            "signals": {"builtin": {}, "custom": {}, "monitor_rules": {"rule-one": {"action_pct": 25}}},
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


def test_continuous_outflow_requires_sustained_direction_ratio(tmp_path: Path):
    quotes = _Quotes()
    service = PositionRiskService(tmp_path, _Repo(), quotes, SimpleNamespace(paper_supervisor=None))
    service.store.replace({
        "account": {"name": "账户", "cash": 82_000, "total_asset": 100_000, "previous_close_total_asset": 100_000},
        "positions": [{"symbol": "600036.SH", "name": "招商银行", "quantity": 500, "available": 500, "cost_price": 35}],
    }, 0)
    service._preload_history({"600036.SH"})
    portfolio = service.store.load()
    position = portfolio["positions"][0]
    started = datetime(2026, 8, 7, 10, 0)
    for offset in range(3):
        service._flow["600036.SH"].append({
            "ts": started.timestamp() - offset,
            "amount": 200_000,
            "volume": 50,
            "direction": -1,
            "price": 36,
        })
    quote = {"symbol": "600036.SH", "last_price": 36, "timestamp": started.isoformat()}

    service._evaluate_position(portfolio, position, quote, started)
    assert not any(item["rule_id"] == "continuous_outflow" for item in quotes.alerts)
    service._evaluate_position(portfolio, position, quote, started.replace(second=10))

    outflow = next(item for item in quotes.alerts if item["rule_id"] == "continuous_outflow")
    assert outflow["suggestion_pct"] == 25


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
    }, 0)
    service._preload_history({"600036.SH"})
    service._latest_quotes["600036.SH"] = {"symbol": "600036.SH", "last_price": 36, "timestamp": "2026-08-07T10:00:00"}
    service._evaluate_current(now=datetime(2026, 8, 7, 10, 0), force=True)
    pending = service.store.list_recommendations("pending")
    assert pending[0]["reduction_pct"] == 50
    assert any("共振" in reason for reason in pending[0]["reasons"])


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
