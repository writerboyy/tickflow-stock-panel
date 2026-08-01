from __future__ import annotations

import sys
from types import SimpleNamespace

import polars as pl
import pytest

from app.plugins.easy_tdx.client import (
    _cash_per_share,
    fetch_dividend_history_rows,
    fetch_industry_rows,
    normalize_industry_rows,
    parse_f10_reference,
)
from app.plugins.easy_tdx.collector import EasyTdxCollector
from app.plugins.easy_tdx.storage import (
    DIVIDEND_HISTORY_TABLE,
    INDUSTRY_TABLE,
    EXPRESS_TABLE,
    FORECAST_TABLE,
    MARGIN_TABLE,
    ensure_config,
    replace_industry_snapshot,
)
from app.services.ext_data import ExtConfigStore


def _row(code: str, industry_sw: str, industry_tdx: str = "T01") -> dict[str, str]:
    return {
        "code": code,
        "industry_sw": industry_sw,
        "industry_tdx": industry_tdx,
        "source": "easy_tdx",
        "collected_at": "2026-07-30T08:30:00+08:00",
    }


def test_normalize_industry_rows_keeps_only_missing_industry_dimensions():
    rows = normalize_industry_rows([
        {
            "code": "600000",
            "name": "浦发银行",
            "pre_close": 12.42,
            "industry_tdx": "T01",
            "industry_sw": "X480101",
        },
        {
            "code": "000001",
            "name": "平安银行",
            "industry_tdx": "T1001",
            "industry_sw": "X500102",
        },
        {"code": "830000", "industry_tdx": "T03", "industry_sw": "X03"},
    ])

    assert rows == [
        {
            "symbol": "600000.SH",
            "code": "600000",
            "industry_sw": "X480101",
            "industry_tdx": "T01",
        },
        {
            "symbol": "000001.SZ",
            "code": "000001",
            "industry_sw": "X500102",
            "industry_tdx": "T1001",
        },
    ]


def test_fetch_industry_rows_uses_easy_tdx_public_api(monkeypatch):
    calls = []

    class FakeFrame:
        def to_dict(self, *, orient):
            assert orient == "records"
            return [_row("600000", "X480101")]

    class FakeClient:
        @classmethod
        def from_best_host(cls, *, timeout, heartbeat_interval):
            calls.append((timeout, heartbeat_interval))
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get_security_list_all(self):
            return FakeFrame()

    monkeypatch.setitem(sys.modules, "easy_tdx", SimpleNamespace(TdxClient=FakeClient))

    assert fetch_industry_rows() == [{
        "symbol": "600000.SH",
        "code": "600000",
        "industry_sw": "X480101",
        "industry_tdx": "T01",
    }]
    assert calls == [(5.0, 0)]


def test_fetch_dividend_history_keeps_only_implemented_cash_records(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            return br'''{"ErrorCode":0,"ResultSets":[{"ColName":["rq","T003","T004","T021","T023","T036","aT036"],"Content":[["2024-12-31","2025-04-23","10\u6d3e0.9\u5143(\u542b\u7a0e)","2025-05-26","2025-05-27","\u5b9e\u65bd\u65b9\u6848","036003"],["2025-06-30","2025-08-29","10\u6d3e0.3\u5143(\u542b\u7a0e)",null,null,"\u8463\u4e8b\u4f1a\u9884\u6848","036001"],["2025-12-31","2026-04-22","\u4e0d\u5206\u914d\u4e0d\u8f6c\u589e","2026-06-16","2026-06-17","\u5b9e\u65bd\u65b9\u6848","036003"]]}]}'''

    monkeypatch.setattr("app.plugins.easy_tdx.client.urlopen", lambda *_args, **_kwargs: Response())

    assert fetch_dividend_history_rows(["300187"]) == [{
        "symbol": "300187.SZ",
        "code": "300187",
        "report_date": "2025-05-26",
        "record_date": "2025-05-26",
        "ex_dividend_date": "2025-05-27",
        "board_date": "2025-04-23",
        "plan": "10派0.9元(含税)",
        "cash_per_share": 0.09,
        "progress": "实施方案",
        "progress_code": "036003",
        "source": "tdx_7615_f10",
    }]


def test_cash_per_share_uses_original_share_base_for_transfer_plans():
    assert _cash_per_share("10转增3股派1元(含税)") == 0.1
    assert _cash_per_share("10送5股派2元(含税)") == 0.2


def test_parse_f10_reference_requires_explicit_sections():
    text = """最新提示☆ ◇000858 五 粮 液 更新日期：2026-08-01◇
【7.融资融券】
│交易日期        │ 融资余额(万元)│ 融资买入额(万元)│ 融券余额(万元)│ 融券卖出量(万股)│融资融券余额(万元)│
│2026-07-30      │       474212.95│          21533.51│          100.00│             2.00│       474312.95│
【8.风险提示】
│●业绩预告:
│2026-07-15 预告业绩:业绩大幅上升
│预计公司2026年01-06月归属于上市公司股东的净利润为873000万元至920000万元，与上年同期相比变动幅度为88.8%至98.97%。
├────────────────────
│问：什么时候发布业绩快报
│答：请关注公告
"""

    margins, forecasts, expresses = parse_f10_reference(text, "000858")

    assert margins == [{
        "symbol": "000858.SZ", "code": "000858", "name": "五 粮 液", "report_date": "2026-07-30",
        "margin_balance_10k": 474212.95, "margin_purchase_10k": 21533.51, "short_balance_10k": 100.0,
        "short_sell_10k_shares": 2.0, "margin_short_balance_10k": 474312.95,
    }]
    assert forecasts[0]["announcement_date"] == "2026-07-15"
    assert forecasts[0]["report_period"] == "2026-06-30"
    assert forecasts[0]["net_profit_low_10k"] == 873000
    assert forecasts[0]["net_profit_yoy_high_pct"] == 98.97
    assert expresses == []


def test_parse_f10_reference_keeps_only_explicit_express_section():
    text = """◇300750 宁德时代 更新日期：2026-08-01◇
│●业绩快报:
│2026-02-20 公司披露 2025 年度业绩快报
│营业收入和净利润以正式公告为准
├────────────────────
│问：什么时候发布业绩快报
"""

    _, _, expresses = parse_f10_reference(text, "300750")

    assert expresses == [{
        "symbol": "300750.SZ", "code": "300750", "name": "宁德时代", "report_date": "2026-02-20",
        "announcement_date": "2026-02-20", "summary": "●业绩快报:\n2026-02-20 公司披露 2025 年度业绩快报\n营业收入和净利润以正式公告为准",
    }]


def test_snapshot_replace_is_atomic_and_empty_rows_preserve_last_valid_data(tmp_path):
    first = [{**_row("600000", "X480101"), "symbol": "600000.SH"}]
    second = [{**_row("000001", "X500102"), "symbol": "000001.SZ"}]

    assert replace_industry_snapshot(tmp_path, first) == 1
    path = tmp_path / "ext_data" / INDUSTRY_TABLE / "part.parquet"
    assert pl.read_parquet(path)["symbol"].to_list() == ["600000.SH"]

    assert replace_industry_snapshot(tmp_path, second) == 1
    assert pl.read_parquet(path)["symbol"].to_list() == ["000001.SZ"]
    last_valid = path.read_bytes()

    assert replace_industry_snapshot(tmp_path, []) == 0
    assert path.read_bytes() == last_valid


def test_config_has_no_generic_pull_and_does_not_register_as_market_provider(tmp_path):
    ensure_config(tmp_path)

    config = ExtConfigStore(tmp_path).get(INDUSTRY_TABLE)

    assert config is not None
    assert config.mode == "snapshot"
    assert config.pull is None
    assert config.authority == "extension"
    assert config.canonical_dataset == "tdx_industry_dimension"
    assert [field.name for field in config.fields] == [
        "symbol",
        "code",
        "industry_sw",
        "industry_tdx",
        "source",
        "collected_at",
    ]


def test_start_without_dependency_registers_config_and_job_without_bootstrap(tmp_path):
    class Scheduler:
        def __init__(self):
            self.jobs = []

        def add_job(self, _func, **kwargs):
            self.jobs.append(kwargs["id"])

    scheduler = Scheduler()
    collector = EasyTdxCollector(
        tmp_path,
        availability_check=lambda: (False, "not installed"),
    )

    collector.start(scheduler)

    assert scheduler.jobs == ["easy_tdx_industry", "easy_tdx_f10_reference"]
    assert collector._bootstrap_task is None
    assert ExtConfigStore(tmp_path).get(INDUSTRY_TABLE) is not None


@pytest.mark.asyncio
async def test_collection_writes_source_and_collection_time(tmp_path):
    collector = EasyTdxCollector(
        tmp_path,
        fetcher=lambda: [{
            "symbol": "600000.SH",
            "code": "600000",
            "industry_sw": "X480101",
            "industry_tdx": "T01",
        }],
    )

    assert await collector.collect() == 1

    path = tmp_path / "ext_data" / INDUSTRY_TABLE / "part.parquet"
    stored = pl.read_parquet(path).to_dicts()[0]
    assert stored["source"] == "easy_tdx"
    assert stored["collected_at"]


@pytest.mark.asyncio
async def test_f10_collection_writes_separate_reference_tables(tmp_path):
    text = """◇000858 五 粮 液 更新日期：2026-08-01◇
    【7.融资融券】
    │交易日期        │ 融资余额(万元)│ 融资买入额(万元)│ 融券余额(万元)│ 融券卖出量(万股)│融资融券余额(万元)│
    │2026-07-30      │       474212.95│          21533.51│          100.00│             2.00│       474312.95│
│●业绩预告:
│2026-07-15 预告业绩:业绩大幅上升
│预计公司2026年01-06月归属于上市公司股东的净利润为873000万元至920000万元，与上年同期相比变动幅度为88.8%至98.97%。
├────────────────────
"""
    dividends = [{
        "symbol": "000858.SZ", "code": "000858", "report_date": "2026-06-16",
        "record_date": "2026-06-16", "ex_dividend_date": "2026-06-17", "board_date": "2026-04-22",
        "plan": "10派0.3元(含税)", "cash_per_share": 0.03, "progress": "实施方案",
        "progress_code": "036003", "source": "tdx_7615_f10",
    }]
    collector = EasyTdxCollector(
        tmp_path,
        f10_fetcher=lambda _codes: [("000858", text)],
        dividend_fetcher=lambda _codes: dividends,
    )

    assert await collector.collect_f10(["000858"]) == 3
    margin = pl.read_parquet(tmp_path / "ext_data" / MARGIN_TABLE / "timeseries" / "date=2026-07-30" / "part.parquet")
    forecast = pl.read_parquet(tmp_path / "ext_data" / FORECAST_TABLE / "timeseries" / "date=2026-07-15" / "part.parquet")
    assert margin.to_dicts()[0]["margin_balance_10k"] == 474212.95
    assert forecast.to_dicts()[0]["report_period"] == "2026-06-30"
    dividend = pl.read_parquet(tmp_path / "ext_data" / DIVIDEND_HISTORY_TABLE / "timeseries" / "date=2026-06-16" / "part.parquet")
    assert dividend.to_dicts()[0]["cash_per_share"] == 0.03
    assert not (tmp_path / "ext_data" / EXPRESS_TABLE / "timeseries").exists()


@pytest.mark.asyncio
async def test_empty_collection_fails_without_overwriting_last_valid_snapshot(tmp_path):
    previous = [{**_row("600000", "X480101"), "symbol": "600000.SH"}]
    replace_industry_snapshot(tmp_path, previous)
    path = tmp_path / "ext_data" / INDUSTRY_TABLE / "part.parquet"
    last_valid = path.read_bytes()
    collector = EasyTdxCollector(tmp_path, fetcher=lambda: [])

    with pytest.raises(RuntimeError, match="行业快照为空"):
        await collector.collect()

    assert path.read_bytes() == last_valid
