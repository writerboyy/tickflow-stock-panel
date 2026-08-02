from __future__ import annotations

from datetime import date

import polars as pl

from app.plugins.baostock.index_candidates import (
    INDEX_CONSTITUENT_CANDIDATES_TABLE,
    BaoStockIndexCandidateCollector,
    normalize_index_constituent_candidates,
    partition_path,
)
from app.plugins.pit_history.storage import INDEX_MEMBERSHIP_EVENTS_TABLE, table_path
from app.services import pit_reference


class _LoginResult:
    error_code = "0"
    error_msg = ""


class _BaoStockResult:
    error_code = "0"
    error_msg = ""
    fields = ["date", "code", "code_name"]

    def __init__(self, rows: list[list[str]]) -> None:
        self._rows = rows
        self._idx = -1

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._idx]


class _FakeBaoStock:
    def __init__(self, by_date: dict[str, list[list[str]]] | None = None) -> None:
        self.by_date = by_date or {}
        self.queries: list[str] = []
        self.logout_count = 0

    def login(self) -> _LoginResult:
        return _LoginResult()

    def logout(self) -> None:
        self.logout_count += 1

    def query_hs300_stocks(self, date: str = "") -> _BaoStockResult:
        self.queries.append(date)
        return _BaoStockResult(self.by_date.get(date, []))


class _FailingCollector:
    def collect_hs300_snapshots(self, snapshot_dates):
        raise RuntimeError("offline")


def test_normalize_baostock_hs300_candidates_keeps_snapshot_provenance():
    frame = normalize_index_constituent_candidates(
        [
            {"date": "2020-01-03", "code": "sh.600000", "code_name": "浦发银行"},
            {"date": "2020-01-03", "code": "sz.000001", "code_name": "平安银行"},
            {"date": "2020-01-03", "code": "sz.000001", "code_name": "平安银行"},
        ],
        index_symbol="000300.SH",
        index_name="沪深300",
        snapshot_date=date(2020, 1, 3),
    )

    assert frame.select(
        "index_symbol",
        "member_symbol",
        "member_code",
        "member_name",
        "snapshot_date",
        "source_update_date",
        "source",
        "provenance",
    ).to_dicts() == [
        {
            "index_symbol": "000300.SH",
            "member_symbol": "000001.SZ",
            "member_code": "000001",
            "member_name": "平安银行",
            "snapshot_date": date(2020, 1, 3),
            "source_update_date": date(2020, 1, 3),
            "source": "baostock",
            "provenance": "candidate_snapshot",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600000.SH",
            "member_code": "600000",
            "member_name": "浦发银行",
            "snapshot_date": date(2020, 1, 3),
            "source_update_date": date(2020, 1, 3),
            "source": "baostock",
            "provenance": "candidate_snapshot",
        },
    ]


def test_baostock_collector_publishes_candidate_snapshots_and_manifest(tmp_path):
    fake = _FakeBaoStock({
        "2020-01-03": [["2020-01-03", "sh.600000", "浦发银行"]],
        "2020-01-06": [["2020-01-06", "sz.000001", "平安银行"]],
    })
    collector = BaoStockIndexCandidateCollector(tmp_path, bs_module=fake)

    rows = collector.collect_hs300_snapshots([date(2020, 1, 6), date(2020, 1, 3)])

    assert rows == 2
    assert fake.queries == ["2020-01-03", "2020-01-06"]
    assert fake.logout_count == 1
    first = pl.read_parquet(
        partition_path(tmp_path, INDEX_CONSTITUENT_CANDIDATES_TABLE, date(2020, 1, 3))
    )
    assert first.select("member_symbol", "provenance").to_dicts() == [
        {"member_symbol": "600000.SH", "provenance": "candidate_snapshot"}
    ]
    manifest = (
        tmp_path
        / "ext_data"
        / "_ingestion"
        / "baostock"
        / INDEX_CONSTITUENT_CANDIDATES_TABLE
        / "000300.SH_2020-01-03_2020-01-06.json"
    )
    assert manifest.exists()
    assert not table_path(tmp_path, INDEX_MEMBERSHIP_EVENTS_TABLE).exists()


def test_pit_reference_status_reports_baostock_candidate_as_non_strict(tmp_path):
    collector = BaoStockIndexCandidateCollector(
        tmp_path,
        bs_module=_FakeBaoStock({
            "2020-01-03": [["2020-01-03", "sh.600000", "浦发银行"]],
        }),
    )
    collector.collect_hs300_snapshots([date(2020, 1, 3)])

    status = pit_reference.get_status(tmp_path)
    candidate = status["snapshots"][INDEX_CONSTITUENT_CANDIDATES_TABLE]

    assert candidate["source"] == "baostock"
    assert candidate["rows"] == 1
    assert candidate["latest_snapshot_date"] == "2020-01-03"
    assert candidate["provenance_counts"] == {"candidate_snapshot": 1}
    assert candidate["candidate_source"]["strict_backtest_usable"] is False
    assert status["summary"]["strict_index_membership_usable"] is False


def test_sync_baostock_candidates_returns_failed_without_partial_claim(tmp_path):
    result = pit_reference.sync_baostock_index_candidates(
        tmp_path,
        snapshot_dates=[date(2020, 1, 3)],
        collector=_FailingCollector(),
    )

    assert result["status"] == "failed"
    assert result["published_rows"] == 0
    assert result["errors"] == ["index_constituent_candidates: offline"]
