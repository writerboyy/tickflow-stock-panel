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
async def test_shortline_bootstrap_restores_current_day_socket_snapshot_after_close(
    tmp_path,
    monkeypatch,
):
    collector = KaipanlaCollector(tmp_path)
    today = date(2026, 8, 18)
    strength = {
        "state": "live",
        "as_of": today.isoformat(),
        "refreshed_at": "2026-08-18T14:59:55+08:00",
        "rows": [{"plate_id": "801248", "rank": 1, "strength": 100}],
    }
    refreshed = []

    async def refresh_shortline(trade_date):
        refreshed.append(trade_date)
        return 1

    async def run_safely(_name, func, *args):
        return await func(*args)

    monkeypatch.setattr("app.plugins.kaipanla.collector.cn_today", lambda: today)
    monkeypatch.setattr(
        "app.plugins.kaipanla.collector.cn_now",
        lambda: datetime(2026, 8, 18, 20, 30, tzinfo=CN_TZ),
    )
    monkeypatch.setattr(
        "app.plugins.kaipanla.collector.read_sector_strength_snapshot",
        lambda _data_dir, trade_date: strength if trade_date == today else None,
    )
    monkeypatch.setattr(collector, "refresh_shortline_constituents", refresh_shortline)
    monkeypatch.setattr(collector, "_run_safely", run_safely)

    assert await collector._bootstrap_sector_strength() == 1
    assert refreshed == [today]
    assert collector.sector_strength_snapshot()["refreshed_at"] == strength["refreshed_at"]


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
