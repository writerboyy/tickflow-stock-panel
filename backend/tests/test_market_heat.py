from __future__ import annotations

from datetime import date
from io import BytesIO
from typing import Any
from urllib.error import HTTPError

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import market_heat
from app.plugins.hithink.client import HiThinkAuthError
from app.services.market_heat import build_market_heat_radar, build_market_heat_rank_trend


def _item(thscode: str, name: str, rank: int, heat: float, change: int, trend: str) -> dict[str, Any]:
    return {
        "thscode": thscode,
        "ticker": thscode.split(".", 1)[0],
        "name": name,
        "rank": rank,
        "heat": heat,
        "rank_change": change,
        "rank_trend": trend,
    }


class FakeHiThinkClient:
    def __init__(self) -> None:
        self.rank_trend_calls: list[tuple[str, str, str]] = []

    def get_hot_stock_list(self, period: str = "day") -> dict[str, Any]:
        if period == "day":
            return {
                "timestamp": 1785681600000,
                "item": [
                    _item("600001.SH", "热股一", 1, 99.5, 8, "up"),
                    _item("600002.SH", "热股二", 2, 88.0, -2, "down"),
                    _item("600003.SH", "热股三", 3, 80.0, 0, "flat"),
                    _item("600004.SH", "热股四", 4, 70.0, 1, "up"),
                ],
            }
        return {
            "timestamp": 1785681660000,
            "item": [
                _item("600002.SH", "热股二", 1, 91.0, 5, "up"),
                _item("600005.SH", "热股五", 2, 82.0, 2, "up"),
            ],
        }

    def get_skyrocket_list(self, period: str = "day") -> dict[str, Any]:
        if period == "day":
            return {
                "timestamp": 1785681720000,
                "item": [
                    _item("600004.SH", "热股四", 1, 96.0, 9, "up"),
                    _item("600008.SH", "飙升二", 2, 83.0, 3, "up"),
                ],
            }
        return {
            "timestamp": 1785681780000,
            "item": [
                _item("600002.SH", "热股二", 1, 93.0, 4, "up"),
                _item("600009.SH", "飙升九", 2, 79.0, -1, "down"),
            ],
        }

    def get_hot_stock_rank_trend(
        self,
        thscode: str,
        *,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        self.rank_trend_calls.append((thscode, start_date, end_date))
        ranks = {
            "600001.SH": [30, 18, 6],
            "600002.SH": [7, 9, 12],
            "600003.SH": [3, 3, 3],
            "600004.SH": [26, 11, 4],
        }[thscode]
        return {
            "timestamp": 1783180800000,
            "item": [
                {"thscode": thscode, "ticker": thscode[:6], "date": "2026-07-04", "rank": ranks[0]},
                {"thscode": thscode, "ticker": thscode[:6], "date": "2026-07-18", "rank": ranks[1]},
                {"thscode": thscode, "ticker": thscode[:6], "date": "2026-08-02", "rank": ranks[2]},
            ],
        }


def _assert_no_score_keys(value: Any) -> None:
    if isinstance(value, dict):
        assert "score" not in value
        for child in value.values():
            _assert_no_score_keys(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_score_keys(child)


def test_market_heat_radar_fetches_four_lists_and_top_hot_trends() -> None:
    client = FakeHiThinkClient()

    result = build_market_heat_radar(
        client=client,
        today=date(2026, 8, 2),
        trend_days=30,
    )

    assert set(result["lists"]) == {"hot_day", "hot_hour", "skyrocket_day", "skyrocket_hour"}
    assert result["trend_window"] == {
        "start_date": "2026-07-04",
        "end_date": "2026-08-02",
        "natural_days": 30,
    }
    assert client.rank_trend_calls == [
        ("600001.SH", "2026-07-04", "2026-08-02"),
        ("600002.SH", "2026-07-04", "2026-08-02"),
        ("600003.SH", "2026-07-04", "2026-08-02"),
    ]
    assert result["trends"]["600001.SH"]["analysis"]["direction"] == "improving"
    assert result["trends"]["600002.SH"]["analysis"]["direction"] == "weakening"
    assert result["trends"]["600003.SH"]["analysis"]["direction"] == "flat"
    assert result["lists"]["hot_day"]["summary"]["positive_rank_change_count"] == 2
    assert result["overlaps"][0]["key"] == "hot_vs_skyrocket_day"
    assert result["overlaps"][0]["count"] == 1
    assert result["overlaps"][0]["items"][0]["thscode"] == "600004.SH"
    _assert_no_score_keys(result)


def test_market_heat_rank_trend_fetches_single_requested_stock() -> None:
    client = FakeHiThinkClient()

    result = build_market_heat_rank_trend(
        client=client,
        thscode="600004.SH",
        ticker="600004",
        name="热股四",
        today=date(2026, 8, 2),
        trend_days=30,
    )

    assert client.rank_trend_calls == [("600004.SH", "2026-07-04", "2026-08-02")]
    assert result["thscode"] == "600004.SH"
    assert result["ticker"] == "600004"
    assert result["name"] == "热股四"
    assert result["analysis"]["direction"] == "improving"
    assert result["analysis"]["latest_rank"] == 4


def test_market_heat_trend_api_uses_requested_stock(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_rank_trend(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "thscode": "600004.SH",
            "ticker": "600004",
            "name": "热股四",
            "timestamp": None,
            "timestamp_iso": None,
            "points": [],
            "analysis": {
                "direction": "insufficient",
                "first_rank": None,
                "latest_rank": None,
                "rank_delta": None,
                "points": 0,
            },
        }

    monkeypatch.setattr(market_heat, "build_market_heat_rank_trend", _fake_rank_trend)
    app = FastAPI()
    app.include_router(market_heat.router)

    response = TestClient(app).get(
        "/api/market-heat/trend?thscode=600004.SH&ticker=600004&name=热股四&trend_days=30"
    )

    assert response.status_code == 200
    assert response.json()["thscode"] == "600004.SH"
    assert captured == {
        "thscode": "600004.SH",
        "ticker": "600004",
        "name": "热股四",
        "trend_days": 30,
    }


def test_market_heat_api_reports_missing_hithink_key(monkeypatch) -> None:
    def _raise_missing_key(*_args, **_kwargs):
        raise HiThinkAuthError("missing")

    monkeypatch.setattr(market_heat, "build_market_heat_radar", _raise_missing_key)
    app = FastAPI()
    app.include_router(market_heat.router)

    response = TestClient(app).get("/api/market-heat/radar")

    assert response.status_code == 503
    assert "未配置同花顺/Fuyao API Key" in response.json()["detail"]


def test_market_heat_api_maps_upstream_http_error_to_bad_gateway(monkeypatch) -> None:
    def _raise_upstream_error(*_args, **_kwargs):
        raise HTTPError(
            url="https://fuyao.aicubes.cn/api/a-share/special-data/hot-stock-list",
            code=500,
            msg="upstream error",
            hdrs=None,
            fp=BytesIO(b"upstream body"),
        )

    monkeypatch.setattr(market_heat, "build_market_heat_radar", _raise_upstream_error)
    app = FastAPI()
    app.include_router(market_heat.router)

    response = TestClient(app).get("/api/market-heat/radar")

    assert response.status_code == 502
    assert response.json()["detail"] == "同花顺/Fuyao 热度服务暂时不可用，请稍后重试。"


def test_market_heat_router_is_registered_on_main_app() -> None:
    from app.main import app

    paths = {route.path for route in app.routes}
    assert "/api/market-heat/radar" in paths
    assert "/api/market-heat/trend" in paths
