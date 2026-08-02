from __future__ import annotations

import polars as pl

from app.services.financial_crosscheck import crosscheck_financial_conflicts


def test_financial_reference_is_evidence_but_not_pit_revision(tmp_path):
    metrics = pl.DataFrame([
        {
            "symbol": "920249.BJ",
            "period_end": "2021-12-31",
            "announce_date": "2026-01-30",
            "eps_basic": 0.46,
            "eps_diluted": 0.46,
        },
        {
            "symbol": "920249.BJ",
            "period_end": "2021-12-31",
            "announce_date": "2026-01-30",
            "eps_basic": 0.48,
            "eps_diluted": 0.48,
        },
    ])

    result = crosscheck_financial_conflicts(
        tmp_path,
        frames={"metrics": metrics},
        fetcher=lambda _keys: {
            ("920249.BJ", "2021-12-31"): {"eps_basic": 0.48, "eps_diluted": 0.48}
        },
    )

    row = result["rows"][0]
    assert row["status"] == "reference_corroborated_revision_unverified"
    assert row["can_repair"] is False
    assert row["blocked_reason"] == "reference_has_no_announce_revision_metadata"
    assert result["repairable_groups"] == 0


def test_financial_reference_reports_unmapped_conflicting_fields(tmp_path):
    metrics = pl.DataFrame([
        {
            "symbol": "300205.SZ",
            "period_end": "2024-12-31",
            "announce_date": "2026-04-23",
            "ocfps": 0.098,
            "operating_cash_to_revenue": 1.4673,
        },
        {
            "symbol": "300205.SZ",
            "period_end": "2024-12-31",
            "announce_date": "2026-04-23",
            "ocfps": -0.187,
            "operating_cash_to_revenue": 0.3884,
        },
    ])

    result = crosscheck_financial_conflicts(
        tmp_path,
        frames={"metrics": metrics},
        fetcher=lambda _keys: {("300205.SZ", "2024-12-31"): {"ocfps": 0.098}},
    )

    row = result["rows"][0]
    assert row["status"] == "partial_reference_corroboration"
    assert row["missing_reference_fields"] == ["operating_cash_to_revenue"]
    assert row["can_repair"] is False
