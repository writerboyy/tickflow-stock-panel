from __future__ import annotations

import json

import polars as pl

from app.services.financial_repair import repair_financial_tables


def _write_financial_table(tmp_path, table: str, rows: list[dict]) -> None:
    path = tmp_path / "financials" / table / "part.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path)


def test_financial_repair_dry_run_reports_exact_duplicates_without_writing(tmp_path) -> None:
    rows = [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 100.0,
        },
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 100.0,
        },
    ]
    _write_financial_table(tmp_path, "income", rows)

    result = repair_financial_tables(tmp_path, tables=("income",))

    assert result["status"] == "validated"
    assert result["total_removed_exact_duplicate_rows"] == 1
    assert result["tables"][0]["duplicate_key_groups"] == 1
    assert pl.read_parquet(tmp_path / "financials" / "income" / "part.parquet").height == 2
    assert not list((tmp_path / "financials" / "income").glob(".part.pre-financial-repair-*"))


def test_financial_repair_apply_rewrites_with_backup_and_manifest(tmp_path) -> None:
    rows = [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 100.0,
        },
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 100.0,
        },
        {
            "symbol": "000001.SZ",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-28",
            "revenue": 50.0,
        },
    ]
    _write_financial_table(tmp_path, "income", rows)

    result = repair_financial_tables(tmp_path, tables=("income",), apply=True)

    repaired = pl.read_parquet(tmp_path / "financials" / "income" / "part.parquet")
    assert result["status"] == "published"
    assert repaired.height == 2
    assert repaired["revenue"].to_list() == [100.0, 50.0]
    backup_path = result["tables"][0]["backup_path"]
    assert pl.read_parquet(backup_path).height == 3
    manifest = json.loads((tmp_path / result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["total_removed_exact_duplicate_rows"] == 1


def test_financial_repair_reports_same_key_different_values_without_selecting_revision(
    tmp_path,
) -> None:
    _write_financial_table(tmp_path, "income", [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 100.0,
        },
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "revenue": 101.0,
        },
    ])

    result = repair_financial_tables(tmp_path, tables=("income",), apply=True)

    assert result["status"] == "blocked"
    assert result["unresolved_conflicting_key_groups"] == 1
    assert result["tables"][0]["conflicting_key_groups"] == 1
    assert pl.read_parquet(tmp_path / "financials" / "income" / "part.parquet").height == 2
    assert not list((tmp_path / "financials" / "income").glob(".part.pre-financial-repair-*"))
