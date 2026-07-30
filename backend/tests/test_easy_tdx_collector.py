from __future__ import annotations

import sys
from types import SimpleNamespace

import polars as pl
import pytest

from app.plugins.easy_tdx.client import fetch_industry_rows, normalize_industry_rows
from app.plugins.easy_tdx.collector import EasyTdxCollector
from app.plugins.easy_tdx.storage import (
    INDUSTRY_TABLE,
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

    assert fetch_industry_rows(timeout=15.0) == [{
        "symbol": "600000.SH",
        "code": "600000",
        "industry_sw": "X480101",
        "industry_tdx": "T01",
    }]
    assert calls == [(15.0, 0)]


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

    assert scheduler.jobs == ["easy_tdx_industry"]
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
async def test_empty_collection_fails_without_overwriting_last_valid_snapshot(tmp_path):
    previous = [{**_row("600000", "X480101"), "symbol": "600000.SH"}]
    replace_industry_snapshot(tmp_path, previous)
    path = tmp_path / "ext_data" / INDUSTRY_TABLE / "part.parquet"
    last_valid = path.read_bytes()
    collector = EasyTdxCollector(tmp_path, fetcher=lambda: [])

    with pytest.raises(RuntimeError, match="行业快照为空"):
        await collector.collect()

    assert path.read_bytes() == last_valid
