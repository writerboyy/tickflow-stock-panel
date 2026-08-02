from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from app.plugins.hithink.client import HiThinkAuthError, HiThinkClient
from app.plugins.hithink.collector import HiThinkSnapshotCollector
from app.plugins.hithink import client as hithink_client_module
from app.plugins.hithink.storage import (
    INDEX_CONSTITUENTS_TABLE,
    INSTRUMENT_LIFECYCLE_TABLE,
    THS_SECTOR_CONSTITUENTS_TABLE,
    normalize_lifecycle_observed,
    read_latest_snapshot,
)


class FakeHiThinkClient:
    def get_index_constituents(self, thscode: str) -> dict:
        payloads = {
            "000300.SH": {
                "timestamp": 1817203200000,
                "item": [
                    {"thscode": "600519.SH", "ticker": "600519", "name": "贵州茅台"},
                    {"thscode": "000001.SZ", "ticker": "000001", "name": "平安银行"},
                    {"thscode": "600519.SH", "ticker": "600519", "name": "重复行"},
                ],
            },
            "881001.TI": {
                "timestamp": 1817203200000,
                "item": [
                    {"thscode": "300750.SZ", "ticker": "300750", "name": "宁德时代"},
                ],
            },
        }
        return payloads.get(thscode, {"timestamp": 1817203200000, "item": []})

    def get_ths_index_list(self, tag: str) -> dict:
        assert tag == "industry"
        return {
            "timestamp": 1817203200000,
            "item": [
                {"thscode": "881001.TI", "name": "电力设备"},
            ],
        }

    def list_tickers(self, **_kwargs) -> list[dict]:
        return [
            {
                "thscode": "600519.SH",
                "ticker": "600519",
                "name": "贵州茅台",
                "exchange": "SH",
                "asset_type": "a-share",
            }
        ]


def test_hithink_index_constituents_are_frozen_as_snapshot(tmp_path):
    collector = HiThinkSnapshotCollector(tmp_path, client=FakeHiThinkClient())

    count = collector.collect_index_constituents(
        ["000300.SH"],
        snapshot_date=date(2027, 8, 1),
        index_names={"000300.SH": "沪深300"},
    )

    assert count == 2
    frame = read_latest_snapshot(tmp_path, INDEX_CONSTITUENTS_TABLE)
    assert frame.select("index_symbol", "member_symbol", "provenance").to_dicts() == [
        {
            "index_symbol": "000300.SH",
            "member_symbol": "000001.SZ",
            "provenance": "snapshot_frozen",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600519.SH",
            "provenance": "snapshot_frozen",
        },
    ]
    manifest = json.loads(
        (
            tmp_path / "ext_data" / "_ingestion" / "hithink"
            / INDEX_CONSTITUENTS_TABLE / "2027-08-01.json"
        ).read_text(encoding="utf-8")
    )
    assert manifest["status"] == "published"
    assert manifest["published_rows"] == 2
    assert manifest["provenance"] == "snapshot_frozen"


def test_hithink_sector_constituents_use_separate_snapshot_table(tmp_path):
    collector = HiThinkSnapshotCollector(tmp_path, client=FakeHiThinkClient())

    count = collector.collect_sector_constituents(
        ["industry"],
        snapshot_date=date(2027, 8, 1),
    )

    assert count == 1
    frame = read_latest_snapshot(tmp_path, THS_SECTOR_CONSTITUENTS_TABLE)
    row = frame.to_dicts()[0]
    assert row["sector_symbol"] == "881001.TI"
    assert row["sector_name"] == "电力设备"
    assert row["member_symbol"] == "300750.SZ"
    assert row["provenance"] == "snapshot_frozen"


def test_observed_lifecycle_marks_history_only_symbols_as_observed_delisted():
    frame = normalize_lifecycle_observed(
        current_tickers=[
            {
                "thscode": "600519.SH",
                "name": "贵州茅台",
                "exchange": "SH",
                "asset_type": "a-share",
            }
        ],
        daily_rows=pl.DataFrame(
            {
                "symbol": ["600519.SH", "600519.SH", "000003.SZ"],
                "date": [date(2024, 1, 2), date(2024, 1, 3), date(2020, 5, 1)],
            }
        ),
        observed_as_of=date(2027, 8, 1),
    )

    rows = {row["symbol"]: row for row in frame.to_dicts()}
    assert rows["600519.SH"]["is_currently_listed"] is True
    assert rows["600519.SH"]["status_confidence"] == "current_snapshot_with_history"
    assert rows["000003.SZ"]["is_currently_listed"] is False
    assert rows["000003.SZ"]["observed_delisted"] is True
    assert rows["000003.SZ"]["status_confidence"] == "observed_history_only"


def test_lifecycle_collector_publishes_observed_table(tmp_path):
    collector = HiThinkSnapshotCollector(tmp_path, client=FakeHiThinkClient())

    count = collector.collect_lifecycle_observed(
        observed_as_of=date(2027, 8, 1),
        daily_rows=pl.DataFrame(
            {
                "symbol": ["600519.SH", "000003.SZ"],
                "date": [date(2024, 1, 2), date(2020, 5, 1)],
            }
        ),
    )

    assert count == 2
    frame = read_latest_snapshot(tmp_path, INSTRUMENT_LIFECYCLE_TABLE)
    assert set(frame["status_confidence"].to_list()) == {
        "current_snapshot_with_history",
        "observed_history_only",
    }


def test_hithink_client_requires_explicit_api_key(monkeypatch):
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    monkeypatch.delenv("FUYAO_TOKEN", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr(hithink_client_module.settings, "hithink_finance_api_key", "")
    monkeypatch.setattr(hithink_client_module, "_read_credentials_env_api_key", lambda: "")

    with pytest.raises(HiThinkAuthError):
        HiThinkClient(api_key="")._api_key()
