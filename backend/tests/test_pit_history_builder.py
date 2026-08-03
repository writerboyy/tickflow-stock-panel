from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_EVENTS_TABLE,
    normalize_index_membership_events,
    normalize_industry_membership_history,
    normalize_instrument_lifecycle_events,
    publish_history_table,
    read_history_table,
    summarize_industry_standards,
    validate_industry_history_coverage,
    summarize_lifecycle_completeness,
    validate_index_membership_coverage,
)
from scripts.build_pit_history_from_raw import build_index_history, read_raw_rows


def test_index_history_rows_become_pit_intervals():
    frame = normalize_index_membership_events(
        [
            {
                "品种代码": "600519",
                "品种名称": "贵州茅台",
                "纳入日期": "2005-04-08",
                "剔除日期": "",
            },
            {
                "品种代码": "000001",
                "品种名称": "平安银行",
                "纳入日期": "2012/01/04",
                "剔除日期": "2014/06/16",
            },
        ],
        index_symbol="000300.SH",
        source="fixture",
    )

    rows = frame.select([
        "index_symbol",
        "member_symbol",
        "effective_from",
        "effective_to",
        "provenance",
    ]).to_dicts()
    assert rows == [
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600519.SH",
            "effective_from": date(2005, 4, 8),
            "effective_to": None,
            "provenance": "historical_event",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "000001.SZ",
            "effective_from": date(2012, 1, 4),
            "effective_to": date(2014, 6, 16),
            "provenance": "historical_event",
        },
    ]


def test_index_history_strict_coverage_fails_incomplete_hs300():
    frame = normalize_index_membership_events(
        [
            {"品种代码": "600519", "纳入日期": "2005-04-08"},
            {"品种代码": "000001", "纳入日期": "2005-04-08"},
        ],
        index_symbol="000300.SH",
        source="fixture",
    )

    coverage = validate_index_membership_coverage(frame, index_symbol="000300.SH")

    assert coverage["usable"] is False
    assert coverage["status"] == "incomplete"
    assert coverage["coverage_checks"][0] == {
        "date": "2021-08-02",
        "members": 2,
        "expected_min_members": 250,
        "ok": False,
    }


def test_index_history_strict_coverage_passes_complete_hs300_intervals():
    raw_rows = [
        {"品种代码": f"{600000 + index}", "纳入日期": "2020-01-01"}
        for index in range(300)
    ]
    frame = normalize_index_membership_events(
        raw_rows,
        index_symbol="000300.SH",
        source="fixture",
    )

    coverage = validate_index_membership_coverage(frame, index_symbol="000300.SH")

    assert coverage["usable"] is True
    assert [item["members"] for item in coverage["coverage_checks"]] == [300, 300, 300]


def test_build_index_history_rejects_incomplete_strict_hs300(tmp_path):
    with pytest.raises(ValueError, match="incomplete strict index history"):
        build_index_history(
            data_dir=tmp_path,
            raw_rows=[{"品种代码": "600519", "纳入日期": "2005-04-08"}],
            index_symbol="000300.SH",
            source="fixture",
            logical_snapshot="2026-08-02",
            raw_label="sample",
            validate_strict=True,
        )


def test_industry_history_derives_effective_to_from_next_change():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "600519",
                "证券简称": "贵州茅台",
                "分类标准": "证监会行业",
                "行业编码": "C15",
                "行业名称": "酒、饮料和精制茶制造业",
                "变更日期": "2020-01-01",
            },
            {
                "证券代码": "600519",
                "证券简称": "贵州茅台",
                "分类标准": "证监会行业",
                "行业编码": "C16",
                "行业名称": "食品制造业",
                "变更日期": "2021-06-30",
            },
        ],
        source="fixture",
    )

    rows = frame.select([
        "member_symbol",
        "industry_code",
        "effective_from",
        "effective_to",
    ]).to_dicts()
    assert rows == [
        {
            "member_symbol": "600519.SH",
            "industry_code": "C15",
            "effective_from": date(2020, 1, 1),
            "effective_to": date(2021, 6, 30),
        },
        {
            "member_symbol": "600519.SH",
            "industry_code": "C16",
            "effective_from": date(2021, 6, 30),
            "effective_to": None,
        },
    ]


def test_industry_summary_requires_one_standard_before_joining():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "600519",
                "分类标准": "证监会行业",
                "行业编码": "C15",
                "行业名称": "白酒",
                "变更日期": "2020-01-01",
            },
            {
                "证券代码": "600519",
                "分类标准": "申万行业",
                "行业编码": "801125",
                "行业名称": "白酒",
                "变更日期": "2020-01-01",
            },
        ],
        source="fixture",
    )

    summary = summarize_industry_standards(frame)

    assert summary["requires_industry_standard"] is True
    assert [item["industry_standard"] for item in summary["standards"]] == ["申万行业", "证监会行业"]


def test_industry_history_accepts_public_raw_columns():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "600519",
                "新证券简称": "贵州茅台",
                "分类标准": "证监会行业分类标准（2012）",
                "行业编码": "C15",
                "行业中类": "",
                "行业大类": "酒、饮料和精制茶制造业",
                "变更日期": "2001-08-27",
            }
        ],
        source="fixture",
    )

    assert frame.select([
        "member_symbol",
        "member_name",
        "industry_name",
        "effective_from",
    ]).to_dicts() == [
        {
            "member_symbol": "600519.SH",
            "member_name": "贵州茅台",
            "industry_name": "酒、饮料和精制茶制造业",
            "effective_from": date(2001, 8, 27),
        }
    ]


def test_industry_history_coverage_fails_closed_on_observed_daily_gap():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "600519",
                "分类标准": "申万行业",
                "行业编码": "801120",
                "行业名称": "食品饮料",
                "变更日期": "2020-01-01",
            }
        ],
        source="fixture",
    )
    daily = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "date": [date(2021, 8, 2), date(2021, 8, 2)],
        }
    )

    coverage = validate_industry_history_coverage(
        frame,
        industry_standard="申万行业",
        sample_dates=[date(2021, 8, 2)],
        daily_frame=daily,
    )

    assert coverage["usable"] is False
    assert coverage["status"] == "incomplete"
    assert coverage["sample_checks"] == [
        {
            "date": "2021-08-02",
            "active_members": 1,
            "expected_members": 2,
            "covered_members": 1,
            "coverage": 0.5,
            "ok": False,
        }
    ]


def test_industry_history_coverage_passes_single_standard_without_overlap():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "600519",
                "分类标准": "申万行业",
                "行业编码": "801120",
                "行业名称": "食品饮料",
                "变更日期": "2020-01-01",
            },
            {
                "证券代码": "000001",
                "分类标准": "申万行业",
                "行业编码": "801120",
                "行业名称": "食品饮料",
                "变更日期": "2020-01-01",
            },
        ],
        source="fixture",
    )
    daily = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "date": [date(2021, 8, 2), date(2021, 8, 2)],
        }
    )

    coverage = validate_industry_history_coverage(
        frame,
        industry_standard="申万行业",
        sample_dates=[date(2021, 8, 2)],
        daily_frame=daily,
    )

    assert coverage["usable"] is True
    assert coverage["invalid_intervals"] == 0
    assert coverage["duplicate_keys"] == 0
    assert coverage["overlap_intervals"] == 0
    assert coverage["sample_checks"][0]["coverage"] == 1.0


def test_strict_index_thresholds_are_specific_to_index_family():
    frame = normalize_index_membership_events(
        [
            {"品种代码": f"600{index:03d}", "纳入日期": "2020-01-01"}
            for index in range(300)
        ],
        index_symbol="000905.SH",
        source="fixture",
    )

    coverage = validate_index_membership_coverage(frame, index_symbol="000905.SH")

    assert coverage["usable"] is False
    assert coverage["coverage_checks"][0]["expected_min_members"] == 450
    assert coverage["coverage_checks"][0]["members"] == 300


def test_lifecycle_summary_marks_partial_without_decision_period():
    frame = normalize_instrument_lifecycle_events(
        [
            {
                "证券代码": "000003",
                "上市日期": "1991-07-03",
                "终止上市日期": "2002-06-14",
                "终止上市原因": "连续亏损",
            }
        ],
        source="fixture",
    )

    summary = summarize_lifecycle_completeness(frame)

    assert summary["complete_lifecycle"] is False
    assert summary["status"] == "partial"
    assert summary["missing_event_types"] == [
        "delist_decision",
        "delist_period_end",
        "delist_period_start",
    ]


def test_lifecycle_rows_become_ordered_events():
    frame = normalize_instrument_lifecycle_events(
        [
            {
                "证券代码": "000003",
                "证券简称": "PT金田A",
                "上市日期": "1991-07-03",
                "暂停上市日期": "2002-06-14",
                "终止上市日期": "2002-06-14",
                "终止上市原因": "连续亏损",
            }
        ],
        source="exchange",
    )

    rows = frame.select(["symbol", "event_date", "event_type", "event_status", "reason"]).to_dicts()
    assert rows == [
        {
            "symbol": "000003.SZ",
            "event_date": date(1991, 7, 3),
            "event_type": "listed",
            "event_status": "listed",
            "reason": "连续亏损",
        },
        {
            "symbol": "000003.SZ",
            "event_date": date(2002, 6, 14),
            "event_type": "delisted",
            "event_status": "delisted",
            "reason": "连续亏损",
        },
        {
            "symbol": "000003.SZ",
            "event_date": date(2002, 6, 14),
            "event_type": "suspended",
            "event_status": "suspended",
            "reason": "连续亏损",
        },
    ]


def test_lifecycle_accepts_exchange_delist_columns():
    frame = normalize_instrument_lifecycle_events(
        [
            {
                "公司代码": "600001",
                "公司简称": "邯郸钢铁",
                "上市日期": "1998-01-22",
                "暂停上市日期": "2009-12-29",
            }
        ],
        source="exchange",
    )

    assert frame.select(["symbol", "name", "event_type", "event_date"]).to_dicts() == [
        {
            "symbol": "600001.SH",
            "name": "邯郸钢铁",
            "event_type": "listed",
            "event_date": date(1998, 1, 22),
        },
        {
            "symbol": "600001.SH",
            "name": "邯郸钢铁",
            "event_type": "suspended",
            "event_date": date(2009, 12, 29),
        },
    ]


def test_publish_history_table_round_trips(tmp_path):
    frame = normalize_index_membership_events(
        [{"品种代码": "600519", "纳入日期": "2005-04-08"}],
        index_symbol="000300.SH",
        source="fixture",
    )

    count = publish_history_table(tmp_path, INDEX_MEMBERSHIP_EVENTS_TABLE, frame)

    assert count == 1
    assert read_history_table(tmp_path, INDEX_MEMBERSHIP_EVENTS_TABLE).to_dicts() == frame.to_dicts()


def test_build_index_history_records_manifest_and_published_table(tmp_path):
    count = build_index_history(
        data_dir=tmp_path,
        raw_rows=[{"品种代码": "600519", "纳入日期": "2005-04-08"}],
        index_symbol="000300.SH",
        source="fixture",
        logical_snapshot="2026-08-02",
        raw_label="sample",
    )

    assert count == 1
    frame = read_history_table(tmp_path, INDEX_MEMBERSHIP_EVENTS_TABLE)
    assert frame.select("member_symbol").to_series().to_list() == ["600519.SH"]
    manifest = (
        tmp_path
        / "ext_data"
        / "_ingestion"
        / "pit_history"
        / INDEX_MEMBERSHIP_EVENTS_TABLE
        / "2026-08-02.json"
    )
    assert manifest.exists()


def test_read_raw_rows_supports_csv(tmp_path):
    path = tmp_path / "index.csv"
    path.write_text("品种代码,纳入日期\n600519,2005-04-08\n", encoding="utf-8")

    assert read_raw_rows(path) == [{"品种代码": "600519", "纳入日期": "2005-04-08"}]


def test_read_raw_rows_supports_html_history_table(tmp_path):
    path = tmp_path / "index.html"
    path.write_text(
        """
        <table><tr><td>layout</td></tr><tr><td>ignored</td></tr></table>
        <table>
          <tr><th>品种代码</th><th>品种名称</th><th>纳入日期</th><th>剔除日期</th></tr>
          <tr><td>600519</td><td>贵州茅台</td><td>2005-04-08</td><td></td></tr>
        </table>
        """,
        encoding="utf-8",
    )

    assert read_raw_rows(path) == [
        {
            "品种代码": "600519",
            "品种名称": "贵州茅台",
            "纳入日期": "2005-04-08",
            "剔除日期": "",
        }
    ]


def test_read_raw_rows_supports_html_history_table_with_title_row(tmp_path):
    path = tmp_path / "history.html"
    path.write_text(
        """
        <table>
          <tr><th colspan="4">历史成分</th></tr>
          <tr><td><strong>品种代码</strong></td><td><strong>品种名称</strong></td><td><strong>纳入日期</strong></td><td><strong>剔除日期</strong></td></tr>
          <tr><td>000016</td><td>深康佳A</td><td>2005-04-08</td><td>2006-06-30</td></tr>
        </table>
        """,
        encoding="utf-8",
    )

    assert read_raw_rows(path) == [
        {
            "品种代码": "000016",
            "品种名称": "深康佳A",
            "纳入日期": "2005-04-08",
            "剔除日期": "2006-06-30",
        }
    ]
