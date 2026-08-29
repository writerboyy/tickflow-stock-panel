from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import polars as pl
import pytest

from app.plugins.fuyao import client as fuyao_client
from app.plugins.fuyao_auction import collector as collector_module
from app.plugins.fuyao_auction.collector import FuyaoAuctionCollector
from app.plugins.fuyao_auction.storage import TABLE_ID, partition_path, publish, read_status
from app.services.ingestion_manifest import load_ingestion_manifest
import app.plugins.fuyao_auction.router as router_module


def _request_for(collector):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(fuyao_auction_collector=collector)))


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


class _UnknownSymbolClient(_FakeClient):
    def auction_snapshot(self, thscodes, stage):
        self.calls.append((list(thscodes), stage))
        if "000003.SZ" in thscodes:
            raise fuyao_client.FuyaoError("unknown", code=1002)
        return {"timestamp": 123, "auction_phase": "open", "data_status": "ready", "item": [_row(thscodes[0])]}


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
async def test_collect_respects_fuyao_symbol_batch_limit(tmp_path, monkeypatch):
    symbols = [f"{600000 + i:06d}.SH" for i in range(101)]
    inst = tmp_path / "instruments"
    inst.mkdir()
    pl.DataFrame({"symbol": symbols}).write_parquet(inst / "instruments.parquet")
    fake = _FakeClient({"timestamp": 123, "auction_phase": "open", "data_status": "ready", "item": [_row()]})
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = fake

    assert await collector.collect("0915", date(2026, 8, 27)) == 101
    assert [len(batch) for batch, _stage in fake.calls] == [100, 1]


@pytest.mark.asyncio
async def test_collect_skips_unknown_symbol_without_discarding_batch(tmp_path, monkeypatch):
    inst = tmp_path / "instruments"
    inst.mkdir()
    pl.DataFrame({"symbol": ["000001.SZ", "000003.SZ", "600519.SH"]}).write_parquet(inst / "instruments.parquet")
    fake = _UnknownSymbolClient({})
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = fake

    assert await collector.collect("0915", date(2026, 8, 27)) == 2
    assert collector.status()["state"] == "completed"
    frame = pl.read_parquet(partition_path(tmp_path, date(2026, 8, 27)))
    assert set(frame["symbol"].to_list()) == {"000001.SZ", "600519.SH"}


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


def test_status_normalizes_initial_state_when_key_is_configured(tmp_path, monkeypatch):
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)

    status = collector.status()

    assert status["configured"] is True
    assert status["state"] == "not_ready"
    assert status["message"] == "今日暂无竞价数据"


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


def test_status_does_not_reuse_previous_trade_date_state(tmp_path, monkeypatch):
    today = date(2026, 8, 28)
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    monkeypatch.setattr(collector_module, "cn_today", lambda: today)
    collector = FuyaoAuctionCollector(tmp_path)
    collector._status = {
        "state": "completed",
        "checkpoint": "0925",
        "stage": "final",
        "rows": 10,
        "symbols": 10,
        "message": "采集完成",
        "error_code": None,
        "collected_at": "2026-08-27T09:25:00+08:00",
    }

    status = collector.status()

    assert status["trade_date"] == today.isoformat()
    assert status["state"] == "not_ready"
    assert status["checkpoint"] is None
    assert status["rows"] == 0
    assert status["message"] == "今日暂无竞价数据"


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


def test_save_api_key_rejects_invalid_key_without_persisting(monkeypatch):
    collector = SimpleNamespace(status=lambda: {"state": "unconfigured"}, stop=lambda: None)
    request = _request_for(collector)
    saved = {}
    monkeypatch.setattr(router_module, "probe_api_key", lambda key: (False, "Key 无效"))
    monkeypatch.setattr(router_module.secrets_store, "save", lambda updates: saved.update(updates))
    monkeypatch.setattr(router_module, "get_api_key", lambda: "")

    result = router_module.save_api_key(router_module.FuyaoApiKeyIn(api_key="bad"), request)

    assert result["ok"] is False
    assert saved == {}


def test_save_api_key_persists_and_refreshes_collector(monkeypatch):
    stopped = []
    collector = SimpleNamespace(status=lambda: {"state": "completed"}, stop=lambda: stopped.append(True))
    request = _request_for(collector)
    saved = {}
    monkeypatch.setattr(router_module, "probe_api_key", lambda key: (True, "ok"))
    monkeypatch.setattr(router_module.secrets_store, "save", lambda updates: saved.update(updates))
    monkeypatch.setattr(router_module.secrets_store, "mask", lambda value: "sk-f••••••••key")
    monkeypatch.setattr(router_module, "get_api_key", lambda: "new-key")

    result = router_module.save_api_key(router_module.FuyaoApiKeyIn(api_key="new-key"), request)

    assert result["ok"] is True
    assert result["api_key_masked"] == "sk-f••••••••key"
    assert saved == {"fuyao_api_key": "new-key"}
    assert stopped == [True]


def test_clear_api_key_removes_local_secret_and_refreshes_collector(monkeypatch):
    stopped = []
    collector = SimpleNamespace(status=lambda: {"state": "unconfigured"}, stop=lambda: stopped.append(True))
    request = _request_for(collector)
    cleared = []
    monkeypatch.setattr(router_module.secrets_store, "clear", lambda *keys: cleared.extend(keys))
    monkeypatch.setattr(router_module.secrets_store, "mask", lambda value: "")
    monkeypatch.setattr(router_module, "get_api_key", lambda: "")

    result = router_module.clear_api_key(request)

    assert result["ok"] is True
    assert cleared == ["fuyao_api_key"]
    assert stopped == [True]


def _write_symbols(tmp_path, filename: str, symbols: list[str]) -> None:
    inst = tmp_path / "instruments"
    inst.mkdir(exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(inst / filename)


@pytest.mark.asyncio
async def test_collect_092457_uses_focus_pool_instead_of_full_universe(tmp_path, monkeypatch):
    """最后 3 秒窗口拉不完全市场，092457 必须只采 auction_focus 重点池。"""
    _write_symbols(tmp_path, "instruments.parquet", [f"{600000 + i:06d}.SH" for i in range(300)])
    focus = ["600519.SH", "000001.SZ"]
    _write_symbols(tmp_path, "auction_focus.parquet", focus)

    fake = _FakeClient({"timestamp": 123, "auction_phase": "open", "data_status": "ready", "item": [_row()]})
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = fake

    rows = await collector.collect("092457", date(2026, 8, 27))

    assert rows == 2
    assert fake.calls[0][0] == sorted(focus)  # 重点池按 symbol 排序后分批
    assert fake.calls[0][1] == "live"
    frame = pl.read_parquet(partition_path(tmp_path, date(2026, 8, 27)))
    assert set(frame["checkpoint"].to_list()) == {"092457"}


@pytest.mark.asyncio
async def test_collect_092457_skips_when_focus_pool_missing(tmp_path, monkeypatch):
    """无重点池时不得回退全市场：54 批跨越 09:25:00 会造成 live/final 数据混杂。"""
    _write_symbols(tmp_path, "instruments.parquet", [f"{600000 + i:06d}.SH" for i in range(10)])

    fake = _FakeClient({"timestamp": 123, "auction_phase": "open", "data_status": "ready", "item": [_row()]})
    monkeypatch.setattr(collector_module, "get_api_key", lambda: "key")
    collector = FuyaoAuctionCollector(tmp_path)
    collector._client = fake

    rows = await collector.collect("092457", date(2026, 8, 27))

    assert rows == 0
    assert fake.calls == []
    assert not partition_path(tmp_path, date(2026, 8, 27)).exists()
    manifest = load_ingestion_manifest(tmp_path, "fuyao", TABLE_ID, "2026-08-27")
    assert manifest["status"] == "missing_instruments"


def test_default_checkpoint_is_092457_within_last_three_seconds(tmp_path, monkeypatch):
    monkeypatch.setattr(collector_module, "cn_now", lambda: datetime(2026, 8, 28, 9, 24, 58))
    assert FuyaoAuctionCollector(tmp_path).default_checkpoint() == "092457"


def test_default_checkpoint_advances_to_0925_after_auction_closes(tmp_path, monkeypatch):
    monkeypatch.setattr(collector_module, "cn_now", lambda: datetime(2026, 8, 28, 9, 25, 30))
    assert FuyaoAuctionCollector(tmp_path).default_checkpoint() == "0925"


def test_start_registers_092457_at_second_57(tmp_path):
    jobs: list[dict] = []

    class _FakeScheduler:
        def add_job(self, func, args=None, trigger=None, id=None, **kwargs):  # noqa: ARG002
            jobs.append({"id": id, "args": args, "trigger": trigger})

    FuyaoAuctionCollector(tmp_path).start(_FakeScheduler(), bootstrap=False)

    job = next(j for j in jobs if j["id"] == "fuyao_auction_092457")
    assert job["args"] == ["092457"]
    assert "second='57'" in str(job["trigger"])
    legacy = next(j for j in jobs if j["id"] == "fuyao_auction_0925")
    assert "second='5'" in str(legacy["trigger"])


def test_start_uses_tight_misfire_grace_for_every_checkpoint(tmp_path):
    """竞价是时点快照：任何时点延迟补跑都会拿到别的口径，必须统一收紧。"""
    jobs: list[dict] = []

    class _FakeScheduler:
        def add_job(self, func, args=None, trigger=None, id=None, **kwargs):  # noqa: ARG002
            jobs.append({"id": id, "misfire": kwargs.get("misfire_grace_time")})

    FuyaoAuctionCollector(tmp_path).start(_FakeScheduler(), bootstrap=False)

    assert {j["id"] for j in jobs} == {
        f"fuyao_auction_{cp}" for cp in ("0915", "0920", "092457", "0925", "1457", "1500")
    }
    assert {j["misfire"] for j in jobs} == {3}
