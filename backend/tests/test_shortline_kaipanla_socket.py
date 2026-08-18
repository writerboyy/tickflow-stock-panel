import asyncio
from datetime import date
from pathlib import Path
from types import SimpleNamespace

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
