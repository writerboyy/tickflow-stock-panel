from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    normalize_index_membership_history,
    normalize_industry_membership_history,
    normalize_instrument_lifecycle_events,
    publish_history_table,
    read_history_table,
    summarize_industry_standards,
    validate_industry_history_coverage,
    summarize_lifecycle_completeness,
    validate_index_membership_history,
)
from scripts.build_pit_history_from_raw import build_index_history, read_raw_rows


def test_index_history_rows_become_daily_snapshots():
    frame = normalize_index_membership_history(
        [
            {
                "品种代码": "600519",
                "品种名称": "贵州茅台",
                "快照日期": "2025-04-25",
            },
            {
                "品种代码": "000001",
                "品种名称": "平安银行",
                "快照日期": "2025/04/25",
            },
        ],
        index_symbol="000300.SH",
        source="fixture",
    )

    rows = frame.select(
        [
            "index_symbol",
            "member_symbol",
            "snapshot_date",
            "provenance",
        ]
    ).to_dicts()
    assert rows == [
        {
            "index_symbol": "000300.SH",
            "member_symbol": "000001.SZ",
            "snapshot_date": date(2025, 4, 25),
            "provenance": "dated_snapshot",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600519.SH",
            "snapshot_date": date(2025, 4, 25),
            "provenance": "dated_snapshot",
        },
    ]


def test_index_history_strict_coverage_fails_incomplete_hs300():
    frame = normalize_index_membership_history(
        [
            {"品种代码": "600519", "快照日期": "2025-04-25"},
            {"品种代码": "000001", "快照日期": "2025-04-25"},
        ],
        index_symbol="000300.SH",
        source="fixture",
    )

    coverage = validate_index_membership_history(frame, index_symbol="000300.SH")

    assert coverage["usable"] is False
    assert coverage["status"] == "incomplete"
    assert coverage["invalid_snapshot_dates"][0] == {
        "index_symbol": "000300.SH",
        "snapshot_date": "2025-04-25",
        "members": 2,
        "expected_members": 300,
    }


def test_index_history_strict_coverage_passes_complete_hs300_snapshot():
    raw_rows = [{"品种代码": f"{600000 + index}", "快照日期": "2025-04-25"} for index in range(300)]
    frame = normalize_index_membership_history(
        raw_rows,
        index_symbol="000300.SH",
        source="fixture",
    )

    coverage = validate_index_membership_history(frame, index_symbol="000300.SH")

    assert coverage["usable"] is True
    assert coverage["snapshot_dates"] == 1
    assert coverage["invalid_snapshot_dates"] == []


def test_build_index_history_rejects_incomplete_strict_hs300(tmp_path):
    with pytest.raises(ValueError, match="incomplete strict index history"):
        build_index_history(
            data_dir=tmp_path,
            raw_rows=[{"品种代码": "600519", "快照日期": "2025-04-25"}],
            index_symbol="000300.SH",
            source="fixture",
            logical_snapshot="2026-08-02",
            raw_label="sample",
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

    rows = frame.select(
        [
            "member_symbol",
            "industry_code",
            "effective_from",
            "effective_to",
        ]
    ).to_dicts()
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
    assert [item["industry_standard"] for item in summary["standards"]] == [
        "申万行业",
        "证监会行业",
    ]


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

    assert frame.select(
        [
            "member_symbol",
            "member_name",
            "industry_name",
            "effective_from",
        ]
    ).to_dicts() == [
        {
            "member_symbol": "600519.SH",
            "member_name": "贵州茅台",
            "industry_name": "酒、饮料和精制茶制造业",
            "effective_from": date(2001, 8, 27),
        }
    ]


def test_cninfo_sw_history_normalizes_to_level_one_without_reusing_leaf_code():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "000016",
                "新证券简称": "*ST康佳A",
                "分类标准": "申银万国行业分类标准",
                "分类标准编码": "008003",
                "行业门类": "家用电器",
                "行业次类": "白色家电",
                "行业中类": "冰洗",
                "行业大类": "冰洗",
                "行业编码": "S330106",
                "变更日期": "2024-07-30",
            }
        ],
        source="akshare_cninfo",
    )

    assert frame.select(
        "member_symbol",
        "industry_standard_code",
        "industry_level",
        "industry_code",
        "industry_name",
    ).to_dicts() == [
        {
            "member_symbol": "000016.SZ",
            "industry_standard_code": "008003",
            "industry_level": 1,
            "industry_code": "",
            "industry_name": "家用电器",
        }
    ]


def test_cninfo_sw_name_alone_normalizes_standard_code_and_level():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "000001",
                "分类标准": "申银万国行业分类标准",
                "行业门类": "金融业",
                "行业编码": "S480000",
                "变更日期": "2024-01-02",
            }
        ],
        source="akshare_cninfo",
    )

    assert frame.select(
        "industry_standard", "industry_standard_code", "industry_level"
    ).row(0) == ("申银万国行业分类标准", "008003", 1)


def test_cninfo_industry_normalizes_302_chinext_symbol():
    frame = normalize_industry_membership_history(
        [
            {
                "证券代码": "302132",
                "分类标准": "申银万国行业分类标准",
                "分类标准编码": "008003",
                "行业门类": "国防军工",
                "变更日期": "2021-07-30",
            }
        ],
        source="akshare_cninfo",
    )

    assert frame["member_symbol"].to_list() == ["302132.SZ"]


def test_industry_history_preserves_provider_effective_to():
    frame = normalize_industry_membership_history(
        [
            {
                "member_symbol": "000001.SZ",
                "industry_standard": "sw",
                "industry_level": 1,
                "industry_name": "银行",
                "effective_from": "2024-01-01",
                "effective_to": "2024-06-01",
            },
            {
                "member_symbol": "000001.SZ",
                "industry_standard": "sw",
                "industry_level": 1,
                "industry_name": "非银金融",
                "effective_from": "2024-07-01",
            },
        ],
        source="fixture",
    )

    assert frame.select("industry_name", "effective_from", "effective_to").to_dicts() == [
        {
            "industry_name": "银行",
            "effective_from": date(2024, 1, 1),
            "effective_to": date(2024, 6, 1),
        },
        {
            "industry_name": "非银金融",
            "effective_from": date(2024, 7, 1),
            "effective_to": None,
        },
    ]


def test_read_raw_rows_accepts_archived_json_gzip_envelope(tmp_path):
    import gzip
    import json

    path = tmp_path / "industry.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"payload": [{"证券代码": "000001"}]}, handle, ensure_ascii=False)

    assert read_raw_rows(path) == [{"证券代码": "000001"}]


def test_industry_history_coverage_checks_levels_independently():
    frame = pl.DataFrame(
        {
            "member_symbol": ["000001.SZ", "000001.SZ"],
            "industry_standard": ["sw", "sw"],
            "industry_standard_code": ["008003", "008003"],
            "industry_level": [1, 2],
            "effective_from": [date(2024, 1, 1), date(2024, 1, 1)],
            "effective_to": [None, None],
        }
    )
    daily = pl.DataFrame(
        {"symbol": ["000001.SZ"], "date": [date(2024, 1, 2)]}
    )

    report = validate_industry_history_coverage(
        frame,
        industry_standard="sw",
        industry_standard_code="008003",
        industry_level=1,
        sample_dates=[date(2024, 1, 2)],
        daily_frame=daily,
        min_coverage=1.0,
    )

    assert report["usable"] is True
    assert report["overlap_intervals"] == 0


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
    frame = normalize_index_membership_history(
        [{"品种代码": f"600{index:03d}", "快照日期": "2025-04-25"} for index in range(300)],
        index_symbol="000905.SH",
        source="fixture",
    )

    coverage = validate_index_membership_history(frame, index_symbol="000905.SH")

    assert coverage["usable"] is False
    assert coverage["invalid_snapshot_dates"][0]["expected_members"] == 500
    assert coverage["invalid_snapshot_dates"][0]["members"] == 300


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
    frame = normalize_index_membership_history(
        [{"品种代码": "600519", "快照日期": "2025-04-25"}],
        index_symbol="000300.SH",
        source="fixture",
    )

    count = publish_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE, frame)

    assert count == 1
    assert (
        read_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE).to_dicts() == frame.to_dicts()
    )


def test_build_index_history_records_manifest_and_published_table(tmp_path):
    count = build_index_history(
        data_dir=tmp_path,
        raw_rows=[
            {"品种代码": f"{600000 + index}", "快照日期": "2025-04-25"}
            for index in range(300)
        ],
        index_symbol="000300.SH",
        source="fixture",
        logical_snapshot="2026-08-02",
        raw_label="sample",
    )

    assert count == 300
    frame = read_history_table(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE)
    assert frame.height == 300
    manifest = (
        tmp_path
        / "ext_data"
        / "_ingestion"
        / "pit_history"
        / INDEX_MEMBERSHIP_HISTORY_TABLE
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
