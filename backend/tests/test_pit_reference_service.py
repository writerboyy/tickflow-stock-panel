from __future__ import annotations

from datetime import date

import polars as pl

from app.plugins.hithink.storage import (
    INDEX_CONSTITUENTS_TABLE,
    INSTRUMENT_LIFECYCLE_TABLE,
    publish_snapshot,
)
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_EVENTS_TABLE,
    INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
    publish_history_table,
)
from app.services import pit_reference
from app.plugins.hithink import client as hithink_client_module


def test_pit_reference_status_summarizes_history_and_snapshots(monkeypatch, tmp_path):
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    monkeypatch.delenv("FUYAO_TOKEN", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr(hithink_client_module.settings, "hithink_finance_api_key", "")

    publish_history_table(
        tmp_path,
        INDEX_MEMBERSHIP_EVENTS_TABLE,
        pl.DataFrame(
            {
                "index_symbol": ["000300.SH", "000300.SH"],
                "member_symbol": ["600519.SH", "000001.SZ"],
                "member_code": ["600519", "000001"],
                "member_name": ["贵州茅台", "平安银行"],
                "effective_from": [date(2005, 4, 8), date(2006, 1, 1)],
                "effective_to": [date(2010, 7, 1), None],
                "source": ["fixture", "fixture"],
                "provenance": ["historical_event", "historical_event"],
                "raw_hash": ["a", "b"],
            }
        ),
    )
    publish_history_table(
        tmp_path,
        INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
        pl.DataFrame(
            {
                "member_symbol": ["600519.SH"],
                "member_code": ["600519"],
                "member_name": ["贵州茅台"],
                "industry_standard": ["cninfo"],
                "industry_code": ["C15"],
                "industry_name": ["白酒"],
                "effective_from": [date(2001, 8, 27)],
                "effective_to": [None],
                "source": ["fixture"],
                "provenance": ["historical_event"],
                "raw_hash": ["c"],
            }
        ),
    )
    publish_snapshot(
        tmp_path,
        INDEX_CONSTITUENTS_TABLE,
        date(2026, 8, 2),
        pl.DataFrame(
            {
                "index_symbol": ["000300.SH"],
                "index_name": ["沪深300"],
                "member_symbol": ["600519.SH"],
                "member_code": ["600519"],
                "member_name": ["贵州茅台"],
                "snapshot_date": [date(2026, 8, 2)],
                "source_timestamp": [None],
                "source": ["hithink"],
                "provenance": ["snapshot_frozen"],
                "snapshot_hash": ["x"],
            }
        ),
    )

    status = pit_reference.get_status(tmp_path)

    assert status["summary"]["history_rows"] == 3
    assert status["summary"]["snapshot_rows"] == 1
    assert status["summary"]["earliest_date"] == "2001-08-27"
    assert status["summary"]["latest_snapshot_date"] == "2026-08-02"
    assert status["summary"]["hithink_configured"] is False
    assert status["history"][INDEX_MEMBERSHIP_EVENTS_TABLE]["symbols_covered"] == 2
    assert status["snapshots"][INDEX_CONSTITUENTS_TABLE]["provenance_counts"] == {
        "snapshot_frozen": 1
    }


def test_pit_reference_sync_skips_without_hithink_key(monkeypatch, tmp_path):
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    monkeypatch.delenv("FUYAO_TOKEN", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr(hithink_client_module.settings, "hithink_finance_api_key", "")

    result = pit_reference.sync_hithink_snapshots(
        tmp_path,
        snapshot_date=date(2026, 8, 2),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "missing_hithink_api_key"
    assert result["published_rows"] == 0


def test_pit_reference_sync_uses_injected_collector(tmp_path):
    calls = []

    class Collector:
        def collect_index_constituents(self, indices, *, snapshot_date, index_names):
            calls.append(("index", tuple(indices), snapshot_date, index_names))
            return 2

        def collect_sector_constituents(self, tags, *, snapshot_date, sector_limit):
            calls.append(("sector", tuple(tags), snapshot_date, sector_limit))
            return 3

        def collect_lifecycle_observed(self, *, observed_as_of, daily_rows):
            calls.append(("lifecycle", observed_as_of, daily_rows.height))
            return 4

    result = pit_reference.sync_hithink_snapshots(
        tmp_path,
        snapshot_date=date(2026, 8, 2),
        collector=Collector(),
    )

    assert result == {
        "status": "published",
        "snapshot_date": "2026-08-02",
        "tables": {
            INDEX_CONSTITUENTS_TABLE: 2,
            "ths_sector_constituents_snapshots": 3,
            INSTRUMENT_LIFECYCLE_TABLE: 4,
        },
        "published_rows": 9,
        "errors": [],
    }
    assert calls[0][0] == "index"
    assert calls[1][0] == "sector"
    assert calls[2] == ("lifecycle", date(2026, 8, 2), 0)
