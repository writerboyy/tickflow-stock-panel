from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.kaipanla import collector as collector_module
from app.plugins.kaipanla.collector import KaipanlaCollector
from app.plugins.kaipanla.credentials import KaipanlaCredentials
from app.plugins.kaipanla.storage import AUCTION_TABLE, LHB_TABLE, REGULATORY_TABLE


AUCTION_ROW = [
    "002969",
    "嘉美包装",
    6.07,
    9.96,
    1_381_972_304,
    9.96,
    2_222_228,
    0.16,
    4_058_402,
    37_199_550,
    4_058_402,
    "实控人变更、酿酒",
    2_547_574_181,
    12_878_114,
    17_454_281,
    -4_576_167,
    "6天4板",
]
DUMMY_CREDENTIALS = KaipanlaCredentials("token", "user", "device", "1", "5.0", "w44")


class FakeClient:
    def __init__(self, responses, calls):
        self.responses = responses
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def request(self, endpoint, params=None):
        self.calls.append((endpoint, params or {}))
        value = self.responses[endpoint]
        if isinstance(value, Exception):
            raise value
        return value


def _configured(monkeypatch):
    monkeypatch.setattr(collector_module, "load_credentials", lambda: DUMMY_CREDENTIALS)


@pytest.mark.asyncio
async def test_0925_collection_automatically_fans_out_bid_details(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    responses = {
        115: {"info": [AUCTION_ROW]},
        31: {
            "code": "002969",
            "bid": [["09:15", 3.03, 1, 134]],
            "preclose_px": 3.0,
            "hprice": 3.03,
            "lprice": 3.03,
            "openpx": 3.03,
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, calls))

    rows = await collector.collect_auction("0925", date(2026, 5, 15))

    assert rows == 1
    assert [endpoint for endpoint, _ in calls] == [115, 31]
    path = tmp_path / "ext_data" / AUCTION_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["source_0925"] == "/115"
    assert stored["bid_points"] == 1


@pytest.mark.asyncio
async def test_lhb_collection_automatically_fans_out_seat_details(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    responses = {
        100: {
            "Time": "2026-05-15",
            "list": [
                {
                    "ID": "002208",
                    "Name": "合肥城建",
                    "IncreaseAmount": "4.85%",
                    "BuyIn": "100",
                    "JoinNum": 1,
                    "Turnover": "500",
                    "CircPrice": 1000,
                    "Amplitude": "5",
                    "TurnoverRatio": "2",
                    "Capitalization": 2000,
                }
            ],
        },
        101: {
            "BuyList": [{"Name": "席位甲", "Buy": "100", "Sell": "20"}],
            "SellList": [{"Name": "席位乙", "Buy": "10", "Sell": "80"}],
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, calls))

    rows = await collector.collect_lhb()

    assert rows == 1
    assert [endpoint for endpoint, _ in calls] == [100, 101]
    path = tmp_path / "ext_data" / LHB_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["buy_seat_count"] == 1
    assert stored["sell_list_sell_amount"] == 80


@pytest.mark.asyncio
async def test_regulatory_endpoints_fail_independently(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    responses = {
        108: RuntimeError("unavailable"),
        109: {
            "List": [
                ["002208", "合肥城建", 1, "10日内2次异动", 3, 7, 3.76, 27.67, 6.92, 23.96, -7.42]
            ]
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, calls))

    rows = await collector.collect_regulatory("pre", date(2026, 5, 15))

    assert rows == 1
    path = (
        tmp_path / "ext_data" / REGULATORY_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    )
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["pre_anomaly_type"] == 3
    assert stored["pre_monitor_category"] is None


def test_start_without_credentials_registers_jobs_but_does_not_start_backfill(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(collector_module, "load_credentials", lambda: None)

    class Scheduler:
        def __init__(self):
            self.jobs = []

        def add_job(self, _func, **kwargs):
            self.jobs.append(kwargs["id"])

    scheduler = Scheduler()
    collector = KaipanlaCollector(tmp_path)
    collector.start(scheduler)

    assert len(scheduler.jobs) == 8
    assert collector._bootstrap_task is None
