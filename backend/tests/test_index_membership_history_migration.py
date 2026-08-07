from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    normalize_index_membership_history,
    read_history_table,
    validate_index_membership_history,
)
from scripts.migrate_index_membership_history import migrate_index_membership_history


def _snapshot_rows(snapshot_date: date, count: int = 300) -> list[dict]:
    return [
        {
            "index_symbol": "000300.SH",
            "index_name": "沪深300",
            "member_symbol": f"{600000 + index}.SH",
            "member_name": f"member-{index}",
            "snapshot_date": snapshot_date,
            "source_update_date": snapshot_date,
            "source": "joinquant",
            "provenance": "candidate_snapshot",
            "snapshot_hash": f"hash-{snapshot_date}-{index}",
        }
        for index in range(count)
    ]


def test_normalize_index_membership_history_uses_daily_snapshot_key() -> None:
    frame = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25), count=2),
        source="joinquant",
    )

    assert frame.select("index_symbol", "snapshot_date", "member_symbol", "source").to_dicts() == [
        {
            "index_symbol": "000300.SH",
            "snapshot_date": date(2025, 4, 25),
            "member_symbol": "600000.SH",
            "source": "joinquant",
        },
        {
            "index_symbol": "000300.SH",
            "snapshot_date": date(2025, 4, 25),
            "member_symbol": "600001.SH",
            "source": "joinquant",
        },
    ]


def test_validate_index_membership_history_requires_exact_member_count() -> None:
    complete = normalize_index_membership_history(
        _snapshot_rows(date(2025, 4, 25)),
        source="joinquant",
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


def test_migrate_joinquant_snapshots_publishes_one_canonical_table(tmp_path) -> None:
    source = (
        tmp_path
        / "pit_reference"
        / "joinquant"
        / "joinquant_index_constituent_candidates"
        / "snapshot_date=2025-04-25"
    )
    source.mkdir(parents=True)
    pl.DataFrame(_snapshot_rows(date(2025, 4, 25))).write_parquet(source / "part.parquet")

    result = migrate_index_membership_history(tmp_path)

    assert result["published_rows"] == 300
    assert result["snapshot_dates"] == 1
    canonical = read_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE)
    assert canonical.height == 300
    assert canonical["source"].unique().to_list() == ["joinquant"]
    assert not (tmp_path / "pit_reference/history/index_membership_events").exists()

    with pytest.raises(FileExistsError, match="already exists"):
        migrate_index_membership_history(tmp_path)
