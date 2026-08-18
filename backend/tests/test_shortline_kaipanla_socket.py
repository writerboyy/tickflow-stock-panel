import asyncio
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.market_time import CN_TZ
from app.plugins.kaipanla.collector import KaipanlaCollector
from app.services.limit_board_service import LimitBoardService

from tests.test_limit_board import FakeQuotes, FakeRepo


def test_shortline_socket_snapshot_uses_current_top_boards(tmp_path, monkeypatch):
    collector = KaipanlaCollector(tmp_path)
    today = date(2026, 8, 18)
    collector._sector_strength = {
        "state": "live",
        "as_of": today.isoformat(),
        "refreshed_at": "2026-08-18T10:00:00+08:00",
        "rows": [{"plate_id": "801248", "rank": 1, "strength": 100}],
    }

    class SocketClient:
        def __init__(self, _packet):
            pass

        @staticmethod
        def fetch_blocks(plate_ids):
            assert list(plate_ids) == ["801248"]
            return {"801248": [{
                "plate_id": "801248", "code": "600000", "symbol": "600000",
                "name": "浦发银行", "last_price": 10.2, "change_pct": 0.02,
                "amount": 1000.0, "turnover_rate": 0.03, "main_net": 100.0,
            }]}

    monkeypatch.setattr("app.plugins.kaipanla.collector.load_socket_login_packet", lambda: b"packet")
    monkeypatch.setattr("app.plugins.kaipanla.collector.KaipanlaSocketClient", SocketClient)

    assert asyncio.run(collector.refresh_shortline_constituents(today)) == 1
    snapshot = collector.shortline_constituents_snapshot()
    assert snapshot["provider"] == "kaipanla_socket"
    assert snapshot["state"] == "live"
    assert snapshot["rows"][0]["main_net"] == 100.0


@pytest.mark.asyncio
async def test_shortline_bootstrap_refreshes_close_snapshot_after_close(
    tmp_path,
    monkeypatch,
):
    collector = KaipanlaCollector(tmp_path)
    today = date(2026, 8, 18)
    close_refreshed = []
    refreshed = []

    async def refresh_strength(trade_date, close_snapshot):
        close_refreshed.append((trade_date, close_snapshot))
        collector._sector_strength = {
            "state": "live",
            "as_of": trade_date.isoformat(),
            "refreshed_at": "2026-08-18T15:00:00+08:00",
            "history_state": "closed",
            "rows": [{"plate_id": "801248", "rank": 1, "strength": 120}],
        }
        return 1

    async def refresh_shortline(trade_date):
        refreshed.append(trade_date)
        return 1

    async def run_safely(_name, func, *args):
        return await func(*args)

    monkeypatch.setattr(
        "app.plugins.kaipanla.collector.cn_now",
        lambda: datetime(2026, 8, 18, 20, 30, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(collector, "refresh_sector_strength", refresh_strength)
    monkeypatch.setattr(collector, "refresh_shortline_constituents", refresh_shortline)
    monkeypatch.setattr(collector, "_run_safely", run_safely)

    assert await collector._bootstrap_sector_strength() == 1
    assert close_refreshed == [(today, True)]
    assert refreshed == [today]
    assert collector.sector_strength_snapshot()["refreshed_at"] == "2026-08-18T15:00:00+08:00"


def test_shortline_service_uses_socket_quotes_without_tickflow(tmp_path, monkeypatch):
    today = date(2026, 8, 18)
    quotes = FakeQuotes()
    service = LimitBoardService(
        Path(tmp_path), FakeRepo(), quotes,
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=None),
    )
    strength = {
        "state": "live", "as_of": today.isoformat(),
        "refreshed_at": "2026-08-18T10:00:00+08:00",
        "rows": [{"plate_id": "801248", "plate_name": "汽车零部件", "rank": 1, "strength": 100}],
    }
    constituent = {
        "state": "live", "as_of": today.isoformat(),
        "refreshed_at": "2026-08-18T10:00:01+08:00",
        "rows": [{
            "plate_id": "801248", "code": "600000", "name": "浦发银行",
            "last_price": 10.2, "change_pct": 0.02, "amount": 1000.0,
            "turnover_rate": 0.03, "main_net": 100.0,
        }],
    }
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: strength,
        shortline_constituents_snapshot=lambda: constituent,
    )
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)

    assert service._refresh_sector_candidate_universe(today) == {"600000.SH"}
    snapshot = service.quote_snapshot(["600000.SH"])
    assert snapshot["state"] == "live"
    assert snapshot["quotes"]["600000.SH"]["source"] == "kaipanla_socket"
    assert quotes.consumers == {}


def test_shortline_scope_keeps_top_ten_target_when_upstream_returns_nine(
    tmp_path,
):
    today = date(2026, 8, 18)
    strength_rows = [
        {
            "plate_id": f"P{rank}", "plate_name": f"板块{rank}",
            "rank": rank, "strength": 100 - rank,
        }
        for rank in range(1, 10)
    ]
    constituent_rows = [
        {
            "plate_id": f"P{rank}", "code": f"600{rank:03d}",
            "name": f"股票{rank}", "last_price": 10 + rank,
        }
        for rank in range(1, 10)
    ]
    service = LimitBoardService(
        Path(tmp_path), FakeRepo(), FakeQuotes(),
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=None),
    )
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: {
            "provider": "kaipanla", "state": "live", "as_of": today.isoformat(),
            "refreshed_at": "2026-08-18T10:00:00+08:00", "rows": strength_rows,
        },
        shortline_constituents_snapshot=lambda: {
            "provider": "kaipanla_socket", "state": "live", "as_of": today.isoformat(),
            "refreshed_at": "2026-08-18T10:00:01+08:00", "rows": constituent_rows,
        },
    )

    symbols = service._refresh_sector_candidate_universe(today)

    assert len(symbols) == 9
    assert service._sector_candidate_scope["state"] == "live"
    assert service._sector_candidate_scope["reason"].startswith(
        "仅扫描开盘啦实时板块强度前 10 名范围（当前返回 9 个有效板块）"
    )


@pytest.mark.asyncio
async def test_shortline_constituents_accept_current_point_and_reject_history(
    tmp_path,
    monkeypatch,
):
    today = date(2026, 8, 18)
    service = LimitBoardService(
        Path(tmp_path), FakeRepo(), FakeQuotes(),
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=None),
    )
    current_at = "2026-08-18T14:59:55+08:00"
    strength = {
        "state": "live", "as_of": today.isoformat(),
        "refreshed_at": current_at,
        "rows": [{
            "plate_id": "801248", "plate_name": "汽车零部件",
            "rank": 1, "strength": 100,
        }],
    }
    constituent = {
        "state": "live", "as_of": today.isoformat(),
        "refreshed_at": "2026-08-18T20:30:01+08:00",
        "rows": [{
            "plate_id": "801248", "code": "600000", "name": "浦发银行",
            "last_price": 10.2, "change_pct": 0.02, "amount": 1000.0,
            "turnover_rate": 0.03, "main_net": 100.0,
        }],
    }
    service.app_state.kaipanla_collector = SimpleNamespace(
        sector_strength_snapshot=lambda: strength,
        shortline_constituents_snapshot=lambda: constituent,
    )
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)

    result = await service.sector_constituents_view("801248", current_at)

    assert result["provider"] == "kaipanla_socket"
    assert result["captured_at"] == constituent["refreshed_at"]
    assert result["rows"][0]["last_price"] == 10.2
    with pytest.raises(ValueError, match="不提供历史时点回看"):
        await service.sector_constituents_view(
            "801248",
            "2026-08-18T10:00:00+08:00",
        )


@pytest.mark.asyncio
async def test_shortline_constituents_fetches_plate_outside_preloaded_top_boards(
    tmp_path,
    monkeypatch,
):
    today = date(2026, 8, 18)
    current_at = "2026-08-18T14:59:55+08:00"
    collector = KaipanlaCollector(tmp_path)
    collector._shortline_constituents = {
        "provider": "kaipanla_socket",
        "state": "live",
        "as_of": today.isoformat(),
        "refreshed_at": current_at,
        "plate_ids": ["801248"],
        "missing_plate_ids": [],
        "rows": [{
            "plate_id": "801248", "code": "600000", "name": "浦发银行",
            "last_price": 10.2, "change_pct": 0.02,
        }],
    }
    calls = []

    class SocketClient:
        def __init__(self, _packet):
            pass

        @staticmethod
        def fetch_blocks(plate_ids):
            calls.append(list(plate_ids))
            return {"801999": [{
                "plate_id": "801999", "code": "000001", "name": "平安银行",
                "last_price": 11.2, "change_pct": -0.01,
            }]}

    monkeypatch.setattr("app.plugins.kaipanla.collector.load_socket_login_packet", lambda: b"packet")
    monkeypatch.setattr("app.plugins.kaipanla.collector.KaipanlaSocketClient", SocketClient)
    monkeypatch.setattr("app.services.limit_board_service.cn_today", lambda: today)
    service = LimitBoardService(
        Path(tmp_path), FakeRepo(), FakeQuotes(),
        SimpleNamespace(paper_supervisor=None, qmt_trading_service=None),
    )
    service.app_state.kaipanla_collector = collector
    collector._sector_strength = {
        "state": "live", "as_of": today.isoformat(),
        "refreshed_at": current_at,
        "rows": [
            {"plate_id": "801248", "plate_name": "汽车零部件", "rank": 1, "strength": 100},
            {"plate_id": "801999", "plate_name": "区间涨速板块", "rank": 18, "strength": 40},
        ],
    }

    result = await service.sector_constituents_view("801999", current_at)
    cached = collector.shortline_constituents_snapshot()

    assert calls == [["801999"]]
    assert result["plate_name"] == "区间涨速板块"
    assert result["rows"][0]["symbol"] == "000001.SZ"
    assert {row["plate_id"] for row in cached["rows"]} == {"801248", "801999"}
