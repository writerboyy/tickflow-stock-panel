from __future__ import annotations

from datetime import date

import polars as pl

from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    publish_history_table,
)
from app.services import pit_reference


def test_pit_reference_status_uses_only_canonical_membership_table(tmp_path):

    publish_history_table(
        tmp_path,
        INDEX_MEMBERSHIP_HISTORY_TABLE,
        pl.DataFrame(
            {
                "index_symbol": ["000300.SH"] * 300,
                "index_name": ["沪深300"] * 300,
                "member_symbol": [f"{600000 + index}.SH" for index in range(300)],
                "member_code": [str(600000 + index) for index in range(300)],
                "member_name": [f"member-{index}" for index in range(300)],
                "snapshot_date": [date(2026, 8, 1)] * 300,
                "source_update_date": [date(2026, 8, 1)] * 300,
                "source": ["fixture"] * 300,
                "provenance": ["dated_snapshot"] * 300,
                "snapshot_hash": [f"hash-{index}" for index in range(300)],
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
    status = pit_reference.get_status(tmp_path)

    assert status["summary"]["source"] == "canonical"
    assert status["summary"]["historical_default_source"] == "baostock"
    assert status["summary"]["daily_snapshot_primary_source"] == "hithink"
    assert status["summary"]["history_rows"] == 301
    assert status["summary"]["snapshot_rows"] == 0
    assert status["summary"]["earliest_date"] == "2001-08-27"
    assert status["summary"]["latest_snapshot_date"] == "2026-08-01"
    assert status["summary"]["strict_index_membership_usable"] is True
    assert set(status["history"]) == {
        INDEX_MEMBERSHIP_HISTORY_TABLE,
        INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    }
    assert status["snapshots"] == {}
    assert status["history"][INSTRUMENT_LIFECYCLE_EVENTS_TABLE]["sources"] == ["baostock"]
    assert status["history"][INDEX_MEMBERSHIP_HISTORY_TABLE]["sources"] == ["fixture"]


def _membership_frame(snapshot_date: date, *, source: str = "hithink") -> pl.DataFrame:
    csi300 = [f"{600000 + index}.SH" for index in range(300)]
    csi500 = [f"{index + 1:06d}.SZ" for index in range(500)]
    rows = []
    for index_symbol, members in (
        ("000300.SH", csi300),
        ("000905.SH", csi500),
        ("000906.SH", [*csi300, *csi500]),
        ("000852.SH", [f"{2000 + index:06d}.SZ" for index in range(1000)]),
    ):
        rows.extend(
            {
                "index_symbol": index_symbol,
                "index_name": "",
                "member_symbol": member,
                "member_code": member.split(".")[0],
                "member_name": "",
                "snapshot_date": snapshot_date,
                "source_update_date": None,
                "source": source,
                "provenance": "snapshot_frozen",
                "snapshot_hash": f"{source}-{index_symbol}",
            }
            for member in members
        )
    return pl.DataFrame(rows).with_columns(
        pl.col("snapshot_date").cast(pl.Date),
        pl.col("source_update_date").cast(pl.Date),
    )


def test_daily_membership_uses_hithink_and_crosschecks_baostock(tmp_path):
    snapshot_date = date(2026, 8, 3)
    primary = _membership_frame(snapshot_date)
    crosscheck = primary.filter(
        pl.col("index_symbol").is_in(["000300.SH", "000905.SH"])
    ).with_columns(pl.lit("baostock").alias("source"))

    class HiThinkCollector:
        def fetch_index_constituents(self, indices, *, snapshot_date, index_names):
            assert set(indices) == set(pit_reference.DEFAULT_INDEX_NAMES)
            return primary

    class BaoStockCollector:
        def fetch_index_snapshots(self, indices, *, snapshot_dates, index_names):
            assert tuple(indices) == pit_reference.BAOSTOCK_CROSSCHECK_INDICES
            return crosscheck

    result = pit_reference.sync_index_membership_snapshots(
        tmp_path,
        snapshot_date=snapshot_date,
        hithink_collector=HiThinkCollector(),
        baostock_collector=BaoStockCollector(),
    )

    assert result["status"] == "published"
    assert result["source"] == "hithink"
    assert result["published_rows"] == 2600
    assert result["crosschecked_snapshots"] == 2
    canonical = pl.read_parquet(
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    )
    assert canonical.height == 2600
    assert canonical["source"].unique().to_list() == ["hithink"]


def test_daily_membership_rejects_provider_conflict_before_publish(tmp_path):
    snapshot_date = date(2026, 8, 3)
    primary = _membership_frame(snapshot_date)
    crosscheck = primary.filter(pl.col("index_symbol") == "000300.SH")
    crosscheck = crosscheck.with_row_index().with_columns(
        pl.when(pl.col("index") == 0)
        .then(pl.lit("688999.SH"))
        .otherwise(pl.col("member_symbol"))
        .alias("member_symbol")
    ).drop("index")

    class Collector:
        def __init__(self, frame):
            self.frame = frame

        def fetch_index_constituents(self, *_args, **_kwargs):
            return self.frame

        def fetch_index_snapshots(self, *_args, **_kwargs):
            return self.frame

    result = pit_reference.sync_index_membership_snapshots(
        tmp_path,
        snapshot_date=snapshot_date,
        hithink_collector=Collector(primary),
        baostock_collector=Collector(crosscheck),
    )

    assert result["status"] == "failed"
    assert "provider conflict" in result["errors"][0]
    assert not (tmp_path / "pit_reference/history/index_membership_history/part.parquet").exists()


def test_pit_reference_sync_combines_membership_and_lifecycle(monkeypatch, tmp_path):
    calls = []

    def sync_membership(data_dir, *, snapshot_date):
        calls.append(("membership", data_dir, snapshot_date))
        return {
            "status": "published",
            "source": "hithink",
            "tables": {INDEX_MEMBERSHIP_HISTORY_TABLE: 2600},
            "published_rows": 2600,
            "crosschecked_snapshots": 2,
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

    monkeypatch.setattr(pit_reference, "sync_index_membership_snapshots", sync_membership)
    monkeypatch.setattr(pit_reference, "sync_baostock_lifecycle", sync_lifecycle)

    result = pit_reference.sync_pit_reference(
        tmp_path,
        snapshot_date=date(2026, 8, 3),
    )

    assert result == {
        "status": "published",
        "source": "hithink",
        "snapshot_date": "2026-08-03",
        "tables": {
            INDEX_MEMBERSHIP_HISTORY_TABLE: 2600,
            INSTRUMENT_LIFECYCLE_EVENTS_TABLE: 5587,
        },
        "published_rows": 8187,
        "index_membership_rows": 2600,
        "crosschecked_snapshots": 2,
        "lifecycle_rows": 5587,
        "instrument_appended_symbols": 2,
        "warnings": [],
        "errors": [],
    }
    assert calls == [
        ("membership", tmp_path, date(2026, 8, 3)),
        ("lifecycle", tmp_path, date(2026, 8, 3), 5),
    ]


def test_pit_reference_sync_baostock_lifecycle_updates_instruments(tmp_path):
    inst_path = tmp_path / "instruments" / "instruments.parquet"
    inst_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "name": ["浦发银行"],
            "code": ["600000"],
            "exchange": ["SH"],
        }
    ).write_parquet(inst_path)

    class Collector:
        def collect_stock_lifecycle(self, *, start_date, end_date, years):
            assert start_date is None
            assert end_date == date(2026, 8, 3)
            assert years == 5
            frame = pl.DataFrame(
                {
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
                }
            )
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
    assert stored.select(
        ["symbol", "listing_date", "list_date", "delist_date", "status"]
    ).to_dicts() == [
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
