from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from app.plugins.kaipanla import collector as collector_module
from app.plugins.kaipanla.collector import KaipanlaCollector
from app.plugins.kaipanla.credentials import KaipanlaCredentials
from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    FUNDS_TABLE,
    LHB_DETAIL_TABLE,
    LHB_MOVEMENT_TABLE,
    LHB_TABLE,
    NORTHBOUND_SECTOR_TABLE,
    NORTHBOUND_STOCK_TABLE,
    REGULATORY_TABLE,
    SECTOR_CONSTITUENT_TABLE,
    SHAREHOLDER_COUNT_TABLE,
    SHAREHOLDER_TABLE,
)


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
        if callable(value):
            value = value(params or {})
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
async def test_auction_manifest_requires_all_live_checkpoints_and_bid_details(tmp_path, monkeypatch):
    _configured(monkeypatch)
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
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, []))
    trade_date = date(2026, 5, 15)

    await collector.collect_auction("0915", trade_date)
    manifest_path = (
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "auction_completion" / "2026-05-15.json"
    )
    assert json.loads(manifest_path.read_text())["status"] == "incomplete"

    await collector.collect_auction("0920", trade_date)
    await collector.collect_auction("0925", trade_date)

    manifest = json.loads(manifest_path.read_text())
    assert manifest["status"] == "complete"
    assert set(manifest["components"]) == {"0915", "0920", "0925", "bid_detail"}
    assert manifest["components"]["bid_detail"]["status"] == "complete"


@pytest.mark.asyncio
async def test_historical_auction_records_explicit_valid_empty_without_fake_rows(tmp_path, monkeypatch):
    _configured(monkeypatch)
    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({30: {"info": []}}, []),
    )

    assert await collector.collect_auction("0925", date(2026, 5, 15), True) == 0
    assert not (
        tmp_path / "ext_data" / AUCTION_TABLE
        / "timeseries" / "date=2026-05-15" / "part.parquet"
    ).exists()
    endpoint_manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "endpoint_30" / "2026-05-15.json"
    ).read_text())
    completion_manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "auction_completion" / "2026-05-15.json"
    ).read_text())
    assert endpoint_manifest["status"] == "valid_empty"
    assert endpoint_manifest["empty_reason"] == "valid_empty"
    assert completion_manifest["status"] == "complete"
    assert completion_manifest["components"]["0925"]["status"] == "valid_empty"


@pytest.mark.asyncio
async def test_incomplete_auction_pages_do_not_overwrite_last_valid_partition(tmp_path, monkeypatch):
    _configured(monkeypatch)
    trade_date = date(2026, 5, 15)
    good = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({115: {"info": [AUCTION_ROW]}}, []),
    )
    await good.collect_auction("0915", trade_date)
    partition = (
        tmp_path / "ext_data" / AUCTION_TABLE
        / "timeseries" / "date=2026-05-15" / "part.parquet"
    )
    before = partition.read_bytes()

    def interrupted(params):
        if params["Index"] == 0:
            return {"info": [AUCTION_ROW] * 200}
        raise RuntimeError("page unavailable")

    failing = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({115: interrupted}, []),
    )
    with pytest.raises(RuntimeError, match="page unavailable"):
        await failing.collect_auction("0920", trade_date)

    assert partition.read_bytes() == before
    endpoint_manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "endpoint_115" / "2026-05-15.json"
    ).read_text())
    assert endpoint_manifest["batches"]["page-001"]["status"] == "source_error"


@pytest.mark.asyncio
async def test_partial_bid_details_do_not_publish(tmp_path, monkeypatch):
    _configured(monkeypatch)
    trade_date = date(2026, 5, 15)

    def bid_response(params):
        if params["StockID"] == "002970":
            raise RuntimeError("detail unavailable")
        return {
            "code": params["StockID"],
            "bid": [["09:15", 3.03, 1, 134]],
            "preclose_px": 3.0,
            "hprice": 3.03,
            "lprice": 3.03,
            "openpx": 3.03,
        }

    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({31: bid_response}, []),
    )
    auction_rows = [{"code": "002969"}, {"code": "002970"}]

    async with collector._client_factory() as client:
        assert await collector._collect_bid_details(client, trade_date, auction_rows) == 0

    assert not (
        tmp_path / "ext_data" / AUCTION_TABLE
        / "timeseries" / "date=2026-05-15" / "part.parquet"
    ).exists()
    manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "auction_bid_detail" / "2026-05-15.json"
    ).read_text())
    assert manifest["status"] == "incomplete"
    assert manifest["failed_batches"] == ["002970"]


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
        "dragon_tiger_movement": {
            "List": [{"BID": "P1", "BName": "席位甲", "Buy": [], "Sell": []}]
        },
        "dragon_tiger_details": {
            "List": [
                {
                    "BuyList": [{"ID": "D1", "Name": "席位甲", "Buy": 100, "Sell": 20, "PX": 1, "GroupIcon": []}],
                    "SellList": [],
                }
            ]
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, calls))

    rows = await collector.collect_lhb()

    assert rows == 1
    assert [endpoint for endpoint, _ in calls] == [100, 101, "dragon_tiger_movement", "dragon_tiger_details"]
    path = tmp_path / "ext_data" / LHB_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["buy_seat_count"] == 1
    assert stored["sell_list_sell_amount"] == 80
    movement_path = tmp_path / "ext_data" / LHB_MOVEMENT_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    assert not movement_path.exists()
    detail_path = tmp_path / "ext_data" / LHB_DETAIL_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    assert pl.read_parquet(detail_path).to_dicts()[0]["department_id"] == "D1"


@pytest.mark.asyncio
async def test_lhb_reference_collects_department_details_when_movement_fails(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient(
            {
                "dragon_tiger_movement": RuntimeError("unavailable"),
                "dragon_tiger_details": {
                    "List": [
                        {
                            "BuyList": [
                                {"ID": "D1", "Name": "席位甲", "Buy": 100, "Sell": 20, "PX": 1, "GroupIcon": []}
                            ],
                            "SellList": [],
                        }
                    ]
                },
            },
            calls,
        ),
    )

    rows = await collector.collect_lhb_reference(date(2026, 5, 15), ["002208"])

    assert rows == 1
    assert [endpoint for endpoint, _ in calls] == ["dragon_tiger_movement", "dragon_tiger_details"]
    detail_path = tmp_path / "ext_data" / LHB_DETAIL_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    assert pl.read_parquet(detail_path).to_dicts()[0]["department_id"] == "D1"


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


@pytest.mark.asyncio
async def test_fund_collection_pages_market_flow_and_fans_out_all_stock_codes(tmp_path, monkeypatch):
    _configured(monkeypatch)
    (tmp_path / "instruments").mkdir()
    pl.DataFrame({"code": ["600126"], "type": ["stock"]}).write_parquet(
        tmp_path / "instruments" / "instruments.parquet"
    )
    calls = []
    responses = {
        "fund_interval": {
            "List": [["600126", "杭钢股份", 9.2, 1.5, 100, 40, 60, 3.2, 1000, 2000, "算力", "", "流入", 3]]
        },
        "fund_capital_net": {"trend": [["15:00", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]},
        "fund_large_order_statistics": {
            "Date": ["20260710"], "TDJL": [30], "DDJL": [20], "ZDJL": [10], "XDJL": [-5]
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, calls))

    rows = await collector.collect_funds(date(2026, 7, 10))

    assert rows == 2
    assert [endpoint for endpoint, _ in calls] == [
        "fund_interval",
        "fund_capital_net",
        "fund_large_order_statistics",
    ]
    path = tmp_path / "ext_data" / FUNDS_TABLE / "timeseries" / "date=2026-07-10" / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["main_net"] == 60
    assert stored["capital_net_close"] == 2
    assert stored["main_net_amount_over_300k"] == 50


@pytest.mark.asyncio
async def test_northbound_collection_writes_report_period_sector_and_stock_records(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient(
            {
                "northbound_sector_latest": {
                    "Date": "20260630",
                    "Sum_ZCJE": 10,
                    "Sum_ZCC": 20,
                    "List": [["P1", "板块", 1, 2, 3, 4, 5, 6, 7]],
                },
                "northbound_stocks_latest": {
                    "Date": "20260630",
                    "List": [["600126", "杭钢股份", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
                },
            },
            calls,
        ),
    )

    rows = await collector.collect_northbound()

    assert rows == 2
    assert [endpoint for endpoint, _ in calls] == ["northbound_sector_latest", "northbound_stocks_latest"]
    sector_path = tmp_path / "ext_data" / NORTHBOUND_SECTOR_TABLE / "timeseries" / "date=2026-06-30" / "part.parquet"
    stock_path = tmp_path / "ext_data" / NORTHBOUND_STOCK_TABLE / "timeseries" / "date=2026-06-30" / "part.parquet"
    assert pl.read_parquet(sector_path).to_dicts()[0]["holding_amount"] == 3
    assert pl.read_parquet(stock_path).to_dicts()[0]["symbol"] == "600126.SH"


@pytest.mark.asyncio
async def test_shareholder_counts_expands_documented_date_windows(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []

    def response(params):
        if params["StratDate"] == "2026-07-31":
            return {"DateList": [{"StratDate": "2026-07-16", "EndDate": "2026-07-31"}]}
        return {
            "DateList": [],
            "List": [{"Day": "20260731", "StockID": "600126", "Name": "杭钢股份", "LTZB": 1, "CMJZ": 2, "JSQBH": 3, "UpdateDay": "20260801", "IsNew": 1}],
        }

    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({"shareholder_count_changes": response}, calls),
    )

    rows = await collector.collect_shareholder_counts(date(2026, 7, 31), date(2026, 7, 31))

    assert rows == 1
    assert [params["StratDate"] for _, params in calls] == ["2026-07-31", "2026-07-16"]
    path = tmp_path / "ext_data" / SHAREHOLDER_COUNT_TABLE / "timeseries" / "date=2026-07-31" / "part.parquet"
    assert pl.read_parquet(path).to_dicts()[0]["chip_concentration"] == 2


@pytest.mark.asyncio
async def test_fund_interval_uses_offsets_and_stops_on_a_duplicate_page(tmp_path, monkeypatch):
    _configured(monkeypatch)
    calls = []
    rows = [[f"{600000 + index:06d}", "测试", 1, 1, 1, 1, 0, 1, 1, 1, "", "", "", 0] for index in range(1000)]
    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({"fund_interval": lambda _params: {"List": rows}}, calls),
    )

    count = await collector.collect_funds(date(2026, 7, 10))

    assert count == 1000
    assert [(endpoint, params["Index"]) for endpoint, params in calls] == [
        ("fund_interval", 0),
        ("fund_interval", 1000),
    ]


@pytest.mark.asyncio
async def test_fund_detail_endpoints_fail_independently(tmp_path, monkeypatch):
    _configured(monkeypatch)
    (tmp_path / "instruments").mkdir()
    pl.DataFrame({"code": ["600126"], "type": ["stock"]}).write_parquet(
        tmp_path / "instruments" / "instruments.parquet"
    )
    responses = {
        "fund_interval": {
            "List": [["600126", "杭钢股份", 9.2, 1.5, 100, 40, 60, 3.2, 1000, 2000, "算力", "", "流入", 3]]
        },
        "fund_capital_net": RuntimeError("unavailable"),
        "fund_large_order_statistics": {
            "Date": ["20260710"], "TDJL": [30], "DDJL": [20], "ZDJL": [10], "XDJL": [-5]
        },
    }
    collector = KaipanlaCollector(tmp_path, lambda: FakeClient(responses, []))

    await collector.collect_funds(date(2026, 7, 10))

    path = tmp_path / "ext_data" / FUNDS_TABLE / "timeseries" / "date=2026-07-10" / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["capital_net_close"] is None
    assert stored["main_net_amount_over_300k"] == 50
    capital_manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "fund_capital_net" / "2026-07-10.json"
    ).read_text())
    statistics_manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / "fund_large_order_statistics" / "2026-07-10.json"
    ).read_text())
    assert capital_manifest["status"] == "incomplete"
    assert capital_manifest["failed_batches"] == ["600126"]
    assert statistics_manifest["status"] == "complete"


@pytest.mark.asyncio
async def test_northbound_partial_stock_batch_preserves_sector_only(tmp_path, monkeypatch):
    _configured(monkeypatch)

    def stock_response(params):
        if params["IndexID"] == "P2":
            raise RuntimeError("unavailable")
        return {
            "Date": "20260630",
            "List": [["600126", "杭钢股份", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
        }

    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient(
            {
                "northbound_sector_latest": {
                    "Date": "20260630",
                    "Sum_ZCJE": 10,
                    "Sum_ZCC": 20,
                    "List": [
                        ["P1", "板块一", 1, 2, 3, 4, 5, 6, 7],
                        ["P2", "板块二", 1, 2, 3, 4, 5, 6, 7],
                    ],
                },
                "northbound_stocks_latest": stock_response,
            },
            [],
        ),
    )

    assert await collector.collect_northbound() == 2
    stock_path = (
        tmp_path / "ext_data" / NORTHBOUND_STOCK_TABLE
        / "timeseries" / "date=2026-06-30" / "part.parquet"
    )
    assert not stock_path.exists()
    manifest = json.loads(next((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla" / NORTHBOUND_STOCK_TABLE
    ).glob("*.json")).read_text())
    assert manifest["status"] == "incomplete"
    assert manifest["failed_batches"] == ["P2"]


@pytest.mark.asyncio
async def test_shareholder_partial_symbol_batch_does_not_publish(tmp_path, monkeypatch):
    _configured(monkeypatch)
    (tmp_path / "instruments").mkdir()
    pl.DataFrame({"code": ["600126", "600127"], "type": ["stock", "stock"]}).write_parquet(
        tmp_path / "instruments" / "instruments.parquet"
    )

    def response(params):
        if params["StockID"] == "600127":
            raise RuntimeError("unavailable")
        return {
            "LTGDData": [{
                "JGID": "holder-1",
                "JG": "股东甲",
                "CYSL": 10,
                "ZLTBL": 1,
                "SJJZC": "新进",
                "NiuSan": 0,
                "Color": 0,
            }],
            "LTGDData_SQ": [],
        }

    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({"shareholder_changes": response}, []),
    )

    assert await collector.collect_shareholder_changes(date(2026, 6, 30)) == 0
    partition = (
        tmp_path / "ext_data" / SHAREHOLDER_TABLE
        / "timeseries" / "date=2026-06-30" / "part.parquet"
    )
    assert not partition.exists()
    manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / SHAREHOLDER_TABLE / "2026-06-30.json"
    ).read_text())
    assert manifest["status"] == "incomplete"
    assert manifest["failed_batches"] == ["600127"]


@pytest.mark.asyncio
async def test_sector_partial_plate_batch_does_not_publish(tmp_path, monkeypatch):
    _configured(monkeypatch)

    def response(params):
        if params["PlateID"] == "801002":
            raise RuntimeError("unavailable")
        row = [None] * 41
        row[0], row[1] = "600126", "杭钢股份"
        return {"list": [row]}

    collector = KaipanlaCollector(
        tmp_path,
        lambda: FakeClient({"sector_constituents": response}, []),
    )

    assert await collector.collect_sector_constituents(
        date(2026, 7, 31), ["801001", "801002"]
    ) == 0
    partition = (
        tmp_path / "ext_data" / SECTOR_CONSTITUENT_TABLE
        / "timeseries" / "date=2026-07-31" / "part.parquet"
    )
    assert not partition.exists()
    manifest = json.loads((
        tmp_path / "ext_data" / "_ingestion" / "kaipanla"
        / SECTOR_CONSTITUENT_TABLE / "2026-07-31.json"
    ).read_text())
    assert manifest["status"] == "incomplete"
    assert manifest["failed_batches"] == ["801002"]


def test_fund_stock_pool_requires_current_code_and_type_schema(tmp_path):
    (tmp_path / "instruments").mkdir()
    pl.DataFrame({"code": ["600126", "510300"], "type": ["stock", "etf"]}).write_parquet(
        tmp_path / "instruments" / "instruments.parquet"
    )
    collector = KaipanlaCollector(tmp_path)

    assert collector._stock_codes() == ["600126"]

    pl.DataFrame({"code": ["600126"]}).write_parquet(
        tmp_path / "instruments" / "instruments.parquet"
    )
    assert collector._stock_codes() == []


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

    assert len(scheduler.jobs) == 12
    assert "kaipanla_funds" in scheduler.jobs
    assert "kaipanla_northbound" in scheduler.jobs
    assert "kaipanla_shareholder_counts" in scheduler.jobs
    assert "kaipanla_sector_constituents" in scheduler.jobs
    assert collector._bootstrap_task is None


def test_start_can_register_jobs_without_running_catch_up(tmp_path, monkeypatch):
    _configured(monkeypatch)

    class Scheduler:
        def __init__(self):
            self.jobs = []

        def add_job(self, _func, **kwargs):
            self.jobs.append(kwargs["id"])

    scheduler = Scheduler()
    collector = KaipanlaCollector(tmp_path)

    collector.start(scheduler, bootstrap=False)

    assert len(scheduler.jobs) == 12
    assert collector._bootstrap_task is None
