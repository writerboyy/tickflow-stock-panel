from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.fuyao import client as fuyao_client
from app.plugins.fuyao_auction import collector as collector_module
from app.plugins.fuyao_auction.collector import FuyaoAuctionCollector
from app.plugins.fuyao_auction.storage import TABLE_ID, partition_path, publish, read_status


def _row(symbol: str = "600519.SH", **over) -> dict:
    row = {
        "thscode": symbol,
        "ticker": symbol.split(".")[0],
        "name": "贵州茅台",
        "auction_price": 1500,
        "auction_pct": 2.1,
        "auction_volume": 1000,
        "auction_amount": 1_500_000,
        "auction_unmatched": 10,
        "auction_turnover_pct": 0.01,
        "auction_yesterday_ratio_pct": 1.2,
        "auction_volume_ratio": 3.0,
        "pre_close_price": 1470,
        "open_price": None,
        "last_price": 1500,
        "float_market_cap": 1e12,
    }
    row.update(over)
    return row


class _FakeClient:
    def __init__(self, data: dict):
        self.data = data
        self.calls: list[tuple[list[str], str]] = []

    def auction_snapshot(self, thscodes, stage):
        self.calls.append((list(thscodes), stage))
        if self.data.get("item") and len(self.data["item"]) == 1:
            return {**self.data, "item": [_row(symbol) for symbol in thscodes]}
        return self.data

    def close(self):
        pass


@pytest.mark.asyncio
async def test_collect_batches_symbols_and_publishes(tmp_path, monkeypatch):
    symbols = ["600519.SH", "000001.SZ"]
    inst = tmp_path / "instruments"
    inst.mkdir()
    pl.DataFrame({"symbol": symbols}).write_parquet(inst / "instruments.parquet")
    fake = _FakeClient({"timestamp": 123, "auction_phase": "open", "data_status": "ready", "item": [_row()]})
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = fake
    rows = await collector.collect("0915", date(2026, 8, 27))
    assert rows == 2
    assert len(fake.calls) == 1
    assert fake.calls[0][1] == "live"
    frame = pl.read_parquet(partition_path(tmp_path, date(2026, 8, 27)))
    assert frame.height == 2
    assert set(frame["checkpoint"].to_list()) == {"0915"}
    assert collector.status()["state"] == "completed"


@pytest.mark.asyncio
async def test_not_ready_does_not_publish_fake_rows(tmp_path, monkeypatch):
    inst = tmp_path / "instruments"
    inst.mkdir()
    pl.DataFrame({"symbol": ["600519.SH"]}).write_parquet(inst / "instruments.parquet")
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = _FakeClient({"data_status": "not_ready", "item": []})
    assert await collector.collect("0920", date(2026, 8, 27)) == 0
    assert collector.status()["state"] == "not_ready"
    assert not partition_path(tmp_path, date(2026, 8, 27)).exists()


@pytest.mark.asyncio
async def test_missing_key_is_non_blocking(tmp_path, monkeypatch):
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "")
    collector = FuyaoAuctionCollector(tmp_path)
    assert await collector.collect("0925", date(2026, 8, 27)) == 0
    assert collector.status()["state"] == "unconfigured"


def test_publish_is_idempotent_per_checkpoint(tmp_path):
    day = date(2026, 8, 27)
    payload = {"timestamp": 1, "auction_phase": "open", "data_status": "ready", "collected_at": "2026-08-27T09:15:00+08:00"}
    publish(tmp_path, day, [_row()], checkpoint="0915", stage="live", payload={**payload, "item": [_row()]})
    publish(tmp_path, day, [_row(auction_price=1510)], checkpoint="0915", stage="live", payload={**payload, "item": [_row(auction_price=1510)]})
    publish(tmp_path, day, [_row(auction_price=1520)], checkpoint="0925", stage="final", payload={**payload, "item": [_row(auction_price=1520)]})
    frame = pl.read_parquet(partition_path(tmp_path, day))
    assert frame.height == 2
    assert set(frame["checkpoint"].to_list()) == {"0915", "0925"}
    assert read_status(tmp_path, day)["rows"] == 2


def test_client_auction_snapshot_builds_documented_query(monkeypatch):
    seen = {}

    class Response:
        status_code = 200

        def json(self):
            return {"code": 0, "data": {"data_status": "ready", "item": []}}

    class Http:
        def get(self, path, params=None):
            seen.update(path=path, params=params)
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(fuyao_client.httpx, "Client", lambda **_: Http())
    client = fuyao_client.FuyaoClient("key")
    result = client.auction_snapshot(["600519.SH", "000001.SZ"], "live")
    assert result["data_status"] == "ready"
    assert seen["path"] == "/api/a-share/auction/snapshot"
    assert seen["params"] == {"thscodes": "600519.SH,000001.SZ", "stage": "live"}
