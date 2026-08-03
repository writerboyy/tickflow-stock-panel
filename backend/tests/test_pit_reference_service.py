from __future__ import annotations

from datetime import date

import polars as pl

from app.plugins.baostock.index_candidates import (
    INDEX_CONSTITUENT_CANDIDATES_TABLE,
    normalize_index_constituent_candidates,
    publish_candidate_snapshot,
)
from app.plugins.hithink.storage import (
    INDEX_CONSTITUENTS_TABLE,
    INSTRUMENT_LIFECYCLE_TABLE,
    publish_snapshot,
)
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_EVENTS_TABLE,
    INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    publish_history_table,
)
from app.services import pit_reference
from app.plugins.hithink import client as hithink_client_module


class MissingHiThinkClient:
    def _api_key(self):
        raise pit_reference.HiThinkAuthError("missing")


def test_pit_reference_status_summarizes_only_baostock(monkeypatch, tmp_path):
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    monkeypatch.delenv("FUYAO_TOKEN", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr(hithink_client_module.settings, "hithink_finance_api_key", "")
    monkeypatch.setattr(pit_reference, "HiThinkClient", MissingHiThinkClient)

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
                "industry_standard": ["证监会行业"],
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
    publish_history_table(
        tmp_path,
        INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "name": ["贵州茅台", "平安银行"],
                "exchange": ["SH", "SZ"],
                "event_date": [date(2001, 8, 27), date(1991, 4, 3)],
                "event_type": ["listed", "listed"],
                "event_status": ["listed", "listed"],
                "reason": ["", ""],
                "source": ["baostock", "akshare_exchange"],
                "provenance": ["historical_event", "historical_event"],
                "raw_hash": ["d", "e"],
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
    candidate_date = date(2026, 8, 2)
    publish_candidate_snapshot(
        tmp_path,
        candidate_date,
        normalize_index_constituent_candidates(
            [{"code": "sh.600519", "code_name": "贵州茅台"}],
            index_symbol="000300.SH",
            index_name="沪深300",
            snapshot_date=candidate_date,
        ),
    )

    status = pit_reference.get_status(tmp_path)

    assert status["summary"]["source"] == "baostock"
    assert status["summary"]["history_rows"] == 1
    assert status["summary"]["snapshot_rows"] == 1
    assert status["summary"]["earliest_date"] == "2001-08-27"
    assert status["summary"]["latest_snapshot_date"] == "2026-08-02"
    assert status["summary"]["strict_index_membership_usable"] is False
    assert set(status["history"]) == {INSTRUMENT_LIFECYCLE_EVENTS_TABLE}
    assert set(status["snapshots"]) == {INDEX_CONSTITUENT_CANDIDATES_TABLE}
    assert status["history"][INSTRUMENT_LIFECYCLE_EVENTS_TABLE]["sources"] == ["baostock"]
    assert status["snapshots"][INDEX_CONSTITUENT_CANDIDATES_TABLE]["provenance_counts"] == {
        "candidate_snapshot": 1
    }


def test_pit_reference_sync_skips_without_hithink_key(monkeypatch, tmp_path):
    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    monkeypatch.delenv("FUYAO_TOKEN", raising=False)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setattr(hithink_client_module.settings, "hithink_finance_api_key", "")
    monkeypatch.setattr(pit_reference, "HiThinkClient", MissingHiThinkClient)

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


def test_pit_reference_syncs_baostock_only(monkeypatch, tmp_path):
    calls = []

    def sync_candidates(data_dir, *, snapshot_dates):
        calls.append(("candidates", data_dir, tuple(snapshot_dates)))
        return {
            "status": "published",
            "tables": {INDEX_CONSTITUENT_CANDIDATES_TABLE: 300},
            "published_rows": 300,
            "errors": [],
        }

    def sync_lifecycle(data_dir, *, end_date, years):
        calls.append(("lifecycle", data_dir, end_date, years))
        return {
            "status": "published",
            "tables": {INSTRUMENT_LIFECYCLE_EVENTS_TABLE: 5587},
            "published_rows": 5587,
            "instrument_appended_symbols": 2,
            "errors": [],
        }

    monkeypatch.setattr(pit_reference, "sync_baostock_index_candidates", sync_candidates)
    monkeypatch.setattr(pit_reference, "sync_baostock_lifecycle", sync_lifecycle)

    result = pit_reference.sync_baostock_reference(
        tmp_path,
        snapshot_date=date(2026, 8, 3),
    )

    assert result == {
        "status": "published",
        "source": "baostock",
        "snapshot_date": "2026-08-03",
        "tables": {
            INDEX_CONSTITUENT_CANDIDATES_TABLE: 300,
            INSTRUMENT_LIFECYCLE_EVENTS_TABLE: 5587,
        },
        "published_rows": 5887,
        "index_candidate_rows": 300,
        "lifecycle_rows": 5587,
        "instrument_appended_symbols": 2,
        "errors": [],
    }
    assert calls == [
        ("candidates", tmp_path, (date(2026, 8, 3),)),
        ("lifecycle", tmp_path, date(2026, 8, 3), 5),
    ]


def test_pit_reference_sync_baostock_lifecycle_updates_instruments(tmp_path):
    inst_path = tmp_path / "instruments" / "instruments.parquet"
    inst_path.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "name": ["浦发银行"],
        "code": ["600000"],
        "exchange": ["SH"],
    }).write_parquet(inst_path)

    class Collector:
        def collect_stock_lifecycle(self, *, start_date, end_date, years):
            assert start_date is None
            assert end_date == date(2026, 8, 3)
            assert years == 5
            frame = pl.DataFrame({
                "symbol": ["600000.SH", "600001.SH", "600001.SH", "600003.SH"],
                "name": ["浦发银行", "邯郸钢铁", "邯郸钢铁", "仅上市事件"],
                "exchange": ["SH", "SH", "SH", "SH"],
                "event_date": [
                    date(1999, 11, 10),
                    date(1998, 1, 22),
                    date(2024, 1, 3),
                    date(2022, 1, 4),
                ],
                "event_type": ["listed", "listed", "delisted", "listed"],
                "event_status": ["listed", "listed", "delisted", "listed"],
                "reason": ["", "", "", ""],
                "source": ["baostock", "baostock", "baostock", "baostock"],
                "provenance": [
                    "historical_event",
                    "historical_event",
                    "historical_event",
                    "historical_event",
                ],
                "raw_hash": ["a", "b", "c", "d"],
            })
            publish_history_table(tmp_path, INSTRUMENT_LIFECYCLE_EVENTS_TABLE, frame)
            return {
                "source_rows": 2,
                "candidate_rows": 2,
                "published_rows": 4,
                "total_table_rows": 4,
                "start_date": date(2021, 8, 3),
                "end_date": date(2026, 8, 3),
            }

    result = pit_reference.sync_baostock_lifecycle(
        tmp_path,
        end_date=date(2026, 8, 3),
        collector=Collector(),
    )

    assert result["status"] == "published"
    assert result["tables"] == {INSTRUMENT_LIFECYCLE_EVENTS_TABLE: 4}
    assert result["instrument_matched_symbols"] == 1
    assert result["instrument_appended_symbols"] == 1
    stored = pl.read_parquet(inst_path).sort("symbol")
    assert stored["symbol"].to_list() == ["600000.SH", "600001.SH"]
    assert stored.select(["symbol", "listing_date", "list_date", "delist_date", "status"]).to_dicts() == [
        {
            "symbol": "600000.SH",
            "listing_date": date(1999, 11, 10),
            "list_date": date(1999, 11, 10),
            "delist_date": None,
            "status": "active",
        },
        {
            "symbol": "600001.SH",
            "listing_date": date(1998, 1, 22),
            "list_date": date(1998, 1, 22),
            "delist_date": date(2024, 1, 3),
            "status": "delisted",
        },
    ]


def test_pit_reference_sync_baostock_lifecycle_reports_failure(tmp_path):
    class Collector:
        def collect_stock_lifecycle(self, *, start_date, end_date, years):
            raise RuntimeError("offline")

    result = pit_reference.sync_baostock_lifecycle(tmp_path, collector=Collector())

    assert result["status"] == "failed"
    assert result["published_rows"] == 0
    assert result["errors"] == ["instrument_lifecycle_events: offline"]
