from __future__ import annotations

import json

import polars as pl

from app.services.financial_cninfo_evidence import (
    collect_cninfo_financial_conflict_evidence,
)


def _write_financial_table(tmp_path, table: str, rows: list[dict]) -> None:
    path = tmp_path / "financials" / table / "part.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path)


class FakeCninfoAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def request(self, interface_name, params):
        self.calls.append((interface_name, dict(params)))
        if interface_name == "stock_zh_a_disclosure_report_cninfo":
            if params.get("category") == "年报":
                return [
                    {
                        "announcement_id": "122",
                        "title": "独立董事2024年度述职报告",
                        "publish_date": "20250330",
                        "file_type": "PDF",
                        "file_size_kb": 10.0,
                        "download_url": "https://static.cninfo.com.cn/finalpage/2025-03-30/122.PDF",
                    },
                    {
                        "announcement_id": "123",
                        "title": "2024年年度报告",
                        "publish_date": "20250330",
                        "file_type": "PDF",
                        "file_size_kb": 100.0,
                        "download_url": "https://static.cninfo.com.cn/finalpage/2025-03-30/123.PDF",
                    },
                ]
            if params.get("category") == "补充更正":
                return [{
                    "announcement_id": "124",
                    "title": "关于前期会计差错更正后的财务报表及附注",
                    "publish_date": "20250331",
                    "file_type": "PDF",
                    "file_size_kb": 110.0,
                    "download_url": "https://static.cninfo.com.cn/finalpage/2025-03-31/124.PDF",
                }]
            return []
        if interface_name == "cninfo_announcement_detail":
            return [{
                "announcement_id": params["announcement_id"],
                "title": params.get("title"),
                "content_type": "application/pdf",
                "file_size_bytes": 1024,
                "download_url": params["url"],
            }]
        raise AssertionError(interface_name)


def test_cninfo_evidence_collects_announcements_without_mutating_financials(tmp_path) -> None:
    _write_financial_table(tmp_path, "income", [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "net_income_deducted": 100.0,
        },
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "net_income_deducted": 101.0,
        },
    ])
    adapter = FakeCninfoAdapter()
    output = tmp_path / "financials" / "cninfo-evidence.json"

    result = collect_cninfo_financial_conflict_evidence(
        tmp_path,
        output=output,
        tables=("income",),
        adapter_factory=lambda: adapter,
    )

    assert result["status"] == "candidate_announcements_found"
    assert result["conflict_groups"] == 1
    assert result["groups_with_candidate_announcements"] == 1
    assert result["rows"][0]["can_repair"] is False
    assert result["rows"][0]["blocked_reason"] == "cninfo_pdf_not_parsed_to_financial_fields"
    assert result["rows"][0]["announcement_candidates"][0]["announcement_id"] == "124"
    assert result["rows"][0]["announcement_candidates"][0]["detail"]["file_size_bytes"] == 1024
    assert json.loads(output.read_text(encoding="utf-8"))["conflict_groups"] == 1
    assert pl.read_parquet(tmp_path / "financials" / "income" / "part.parquet").height == 2
    assert adapter.calls[0] == (
        "stock_zh_a_disclosure_report_cninfo",
        {
            "code": "600000.SH",
            "start_date": "20250327",
            "end_date": "20250402",
            "limit": 100,
            "category": "年报",
        },
    )


def test_cninfo_evidence_can_cache_pdf_with_sha256(tmp_path) -> None:
    _write_financial_table(tmp_path, "income", [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "net_income_deducted": 100.0,
        },
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "net_income_deducted": 101.0,
        },
    ])

    result = collect_cninfo_financial_conflict_evidence(
        tmp_path,
        tables=("income",),
        adapter_factory=FakeCninfoAdapter,
        download_pdfs=True,
        pdf_fetcher=lambda _url: b"%PDF-test",
    )

    cached = result["rows"][0]["announcement_candidates"][0]["pdf_cache"]
    assert cached["status"] == "cached"
    assert cached["sha256"] == "3c87d37f1dbea6909f917ce437c390fb8e655a774387d9e69301c0b2283d5b63"
    assert (tmp_path / cached["path"]).read_bytes() == b"%PDF-test"


def test_cninfo_evidence_noops_when_no_conflicts(tmp_path) -> None:
    _write_financial_table(tmp_path, "income", [
        {
            "symbol": "600000.SH",
            "period_end": "2024-12-31",
            "announce_date": "2025-03-30",
            "net_income_deducted": 100.0,
        },
    ])

    result = collect_cninfo_financial_conflict_evidence(
        tmp_path,
        tables=("income",),
        adapter_factory=FakeCninfoAdapter,
    )

    assert result["status"] == "no_conflicts"
    assert result["rows"] == []
