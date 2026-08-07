from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    merge_index_membership_history,
    normalize_index_membership_history,
    read_history_table,
    validate_index_membership_history,
)


def _snapshot_rows(snapshot_date: date, count: int = 300) -> list[dict]:
    return [
        {
            "index_symbol": "000300.SH",
            "index_name": "沪深300",
            "member_symbol": f"{600000 + index}.SH",
            "member_name": f"member-{index}",
            "snapshot_date": snapshot_date,
            "source_update_date": snapshot_date,
            "source": "fixture",
            "provenance": "dated_snapshot",
            "snapshot_hash": f"hash-{snapshot_date}-{index}",
        }
        for index in range(count)
    ]


def test_normalize_index_membership_history_uses_daily_snapshot_key() -> None:
    frame = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25), count=2),
        source="fixture",
    )

    assert frame.select("index_symbol", "snapshot_date", "member_symbol", "source").to_dicts() == [
        {
            "index_symbol": "000300.SH",
            "snapshot_date": date(2025, 4, 25),
            "member_symbol": "600000.SH",
            "source": "fixture",
        },
        {
            "index_symbol": "000300.SH",
            "snapshot_date": date(2025, 4, 25),
            "member_symbol": "600001.SH",
            "source": "fixture",
        },
    ]


def test_validate_index_membership_history_requires_exact_member_count() -> None:
    complete = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25)),
        source="fixture",
    )
    partial = complete.head(299)

    assert validate_index_membership_history(complete)["usable"] is True
    result = validate_index_membership_history(partial)
    assert result["usable"] is False
    assert result["invalid_snapshot_dates"] == [
        {
            "index_symbol": "000300.SH",
            "snapshot_date": "2025-04-25",
            "members": 299,
            "expected_members": 300,
        }
    ]


def test_merge_index_membership_appends_complete_dates_to_one_canonical_table(tmp_path) -> None:
    first = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25)), source="fixture"
    )
    second = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 28)), source="fixture"
    )

    first_result = merge_index_membership_history(tmp_path, first)
    second_result = merge_index_membership_history(tmp_path, second)

    assert first_result["added_rows"] == 300
    assert second_result["added_rows"] == 300
    canonical = read_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE)
    assert canonical.height == 600
    assert canonical["snapshot_date"].n_unique() == 2


def test_merge_index_membership_rejects_same_date_conflict_without_overwrite(tmp_path) -> None:
    existing = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25)), source="fixture"
    )
    merge_index_membership_history(tmp_path, existing)
    conflicting_rows = _snapshot_rows(date(2025, 4, 25))
    conflicting_rows[-1]["member_symbol"] = "000001.SZ"
    conflicting = normalize_index_membership_history(conflicting_rows, source="other")

    with pytest.raises(ValueError, match="same-date index membership conflict"):
        merge_index_membership_history(tmp_path, conflicting)

    stored = read_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE)
    assert stored.select("member_symbol").to_series().to_list() == existing.select(
        "member_symbol"
    ).to_series().to_list()
