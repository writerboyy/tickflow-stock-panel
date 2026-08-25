from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.pit_history.storage import (
    merge_index_membership_history,
    normalize_index_membership_history,
)
from scripts.supplement_tushare_index_membership import (
    fetch_monthly_snapshots,
    half_year_windows,
    supplement_tushare_index_membership,
)


class _Response:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.raw = {"code": 0, "data": {"rows": len(rows)}}


class _Client:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls = []

    def request(self, api_name: str, params: dict) -> _Response:
        self.calls.append((api_name, params))
        return _Response([
            row
            for row in self.rows
            if params["start_date"] <= str(row["trade_date"]) <= params["end_date"]
        ])


def _csi1000_rows(count: int = 1000) -> list[dict]:
    return [
        {
            "index_code": "000852.SH",
            "con_code": f"{2000 + index:06d}.SZ",
            "trade_date": "20250127",
            "weight": 0.1,
        }
        for index in range(count)
    ]


def _index_rows(index_code: str, count: int, *, start: int) -> list[dict]:
    return [
        {
            "index_code": index_code,
            "con_code": f"{start + index:06d}.{'SH' if start >= 600000 else 'SZ'}",
            "trade_date": "20210930",
            "weight": 100 / count,
        }
        for index in range(count)
    ]


def test_half_year_windows_are_bounded():
    assert half_year_windows(date(2025, 3, 1), date(2026, 2, 1)) == [
        (date(2025, 3, 1), date(2025, 6, 30)),
        (date(2025, 7, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]


def test_tushare_monthly_supplement_publishes_exact_dates_only(tmp_path):
    client = _Client(_csi1000_rows())

    result = supplement_tushare_index_membership(
        tmp_path,
        indices=["000852.SH"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30),
        client=client,
    )

    assert result["added_rows"] == 1000
    frame = pl.read_parquet(
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    )
    assert frame["snapshot_date"].unique().to_list() == [date(2025, 1, 27)]
    assert frame["source"].unique().to_list() == ["tushare_proxy"]
    assert frame["provenance"].unique().to_list() == ["monthly_weight_snapshot"]
    assert client.calls == [
        (
            "index_weight",
            {
                "index_code": "000852.SH",
                "start_date": "20250101",
                "end_date": "20250630",
            },
        )
    ]


def test_tushare_monthly_supplement_rejects_incomplete_snapshot(tmp_path):
    with pytest.raises(ValueError, match="failed strict validation"):
        fetch_monthly_snapshots(
            tmp_path,
            index_symbol="000852.SH",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 6, 30),
            client=_Client(_csi1000_rows(999)),
        )

    assert not (
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    ).exists()


def test_tushare_monthly_supplement_keeps_valid_dates_and_reports_invalid(tmp_path):
    valid = _csi1000_rows()
    invalid = [
        {**row, "trade_date": "20250228"}
        for row in _csi1000_rows(999)
    ]

    frame, skipped = fetch_monthly_snapshots(
        tmp_path,
        index_symbol="000852.SH",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30),
        client=_Client(valid + invalid),
    )

    assert frame.height == 1000
    assert frame["snapshot_date"].unique().to_list() == [date(2025, 1, 27)]
    assert skipped == [
        {
            "index_symbol": "000852.SH",
            "snapshot_date": "2025-02-28",
            "members": 999,
            "expected_members": 1000,
        }
    ]


def test_tushare_csi500_supplement_derives_same_date_csi800(tmp_path):
    hs300 = normalize_index_membership_history(
        [
            {
                "index_code": "000300.SH",
                "stock_code": f"{600000 + index}.SH",
                "trade_date": "20210930",
            }
            for index in range(300)
        ],
        source="baostock",
    )
    merge_index_membership_history(tmp_path, hs300)

    result = supplement_tushare_index_membership(
        tmp_path,
        indices=["000905.SH"],
        start_date=date(2021, 9, 30),
        end_date=date(2021, 9, 30),
        client=_Client(_index_rows("000905.SH", 500, start=1)),
    )

    assert result["added_rows"] == 1300
    assert result["derived_csi800_rows"] == 800
    frame = pl.read_parquet(
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    )
    counts = frame.group_by("index_symbol").len().sort("index_symbol").to_dicts()
    assert counts == [
        {"index_symbol": "000300.SH", "len": 300},
        {"index_symbol": "000905.SH", "len": 500},
        {"index_symbol": "000906.SH", "len": 800},
    ]
