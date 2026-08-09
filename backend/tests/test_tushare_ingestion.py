from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import polars as pl
import pytest

from app.services.data_authority import FACTOR_USAGE, assert_extension_field_usage
from app.services.ext_data import ExtConfig
from app.services.tushare_datasets import DATASET_SPECS
from app.services.tushare_ingestion import (
    IngestionConfig,
    TushareDatasetIngestion,
    TushareIngestionBlocked,
    _merge_existing_wins,
    extension_config,
    normalize_dataset_rows,
)
from app.services.tushare_history import TushareResponse


def _response(api_name: str, rows: list[dict]) -> TushareResponse:
    fields = tuple(rows[0]) if rows else ()
    items = tuple(tuple(row.get(field) for field in fields) for row in rows)
    raw = {
        "code": 0,
        "msg": "",
        "data": {"fields": list(fields), "items": [list(item) for item in items]},
    }
    return TushareResponse(api_name, 0, "", fields, items, raw)


class _Client:
    def __init__(self, responses: dict[str, list[dict]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def request(self, api_name: str, params: dict) -> TushareResponse:
        self.calls.append((api_name, dict(params)))
        return _response(api_name, self.responses.get(api_name, []))


def test_extension_config_persists_factor_contract_metadata():
    spec = DATASET_SPECS["moneyflow"]
    payload = extension_config(spec).to_dict()

    assert payload["primary_key"] == ["symbol", "trade_date"]
    assert payload["logical_date"] == "trade_date"
    assert payload["units"]["net_mf_amount"] == "万元"
    assert payload["allowed_usage"] == ["display", "filter", "event-context"]
    assert "factor-input" in extension_config(spec, factor_ready=True).allowed_usage
    assert ExtConfig.from_dict(payload).to_dict()["primary_key"] == payload["primary_key"]


def test_overlapping_market_fields_are_not_factor_inputs():
    assert_extension_field_usage("ext_tushare_top_list", "net_amount", FACTOR_USAGE)
    with pytest.raises(ValueError, match="canonical_market_data"):
        assert_extension_field_usage("ext_tushare_top_list", "close", FACTOR_USAGE)


def test_daily_normalization_keeps_lots_and_converts_thousand_yuan():
    frame, audit = normalize_dataset_rows(
        DATASET_SPECS["daily"],
        [{
            "ts_code": "000001.SZ",
            "trade_date": "20250102",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "vol": 12,
            "amount": 34,
        }],
        collected_at="2026-08-03T00:00:00+00:00",
    )

    assert frame["date"].to_list() == ["2025-01-02"]
    assert frame["volume"].to_list() == [12.0]
    assert frame["amount"].to_list() == [34000.0]
    assert audit["rejected_rows"] == 0


def test_financial_normalization_uses_announcement_date_and_revision_hash():
    frame, _ = normalize_dataset_rows(
        DATASET_SPECS["income"],
        [{
            "ts_code": "000001.SZ",
            "ann_date": "20250420",
            "f_ann_date": "20250422",
            "end_date": "20250331",
            "revenue": 100,
            "n_income_attr_p": 10,
        }],
    )

    assert frame.select("symbol", "announce_date", "period_end").row(0) == (
        "000001.SZ",
        "2025-04-22",
        "2025-03-31",
    )
    assert frame["net_income_attributable"].to_list() == [10.0]
    assert len(frame["source_revision_hash"][0]) == 64


def test_existing_wins_and_conflicting_overlap_is_visible():
    existing = pl.DataFrame({"symbol": ["000001.SZ"], "trade_date": ["2025-01-02"], "value": [1.0]})
    incoming = pl.DataFrame({
        "symbol": ["000001.SZ", "000002.SZ"],
        "trade_date": ["2025-01-02", "2025-01-02"],
        "value": [2.0, 3.0],
    })

    merged, report = _merge_existing_wins(
        existing,
        incoming,
        key=["symbol", "trade_date"],
        compare_columns=["value"],
        label="sample",
    )

    assert report["added_rows"] == 1
    assert report["conflicts"] == [
        {"symbol": "000001.SZ", "trade_date": "2025-01-02", "columns": ["value"]}
    ]
    assert merged.filter(pl.col("symbol") == "000001.SZ")["value"].to_list() == [1.0]


def test_extension_collection_is_resumable_and_publishes_full_primary_key(tmp_path):
    client = _Client({
        "moneyflow": [{
            "ts_code": "000001.SZ",
            "trade_date": "20250102",
            "net_mf_vol": 10,
            "net_mf_amount": 20,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "factor-run",
            start=date(2025, 1, 1),
            end=date(2025, 12, 31),
            publish=True,
        ),
        client,
    )
    spec = DATASET_SPECS["moneyflow"]

    first = engine.collect((spec,), symbols=["000001.SZ"])
    second = engine.collect((spec,), symbols=["000001.SZ"])
    published = engine.publish((spec,))

    assert first["moneyflow"]["rows"] == 1
    assert second["moneyflow"]["rows"] == 1
    assert len(client.calls) == 1
    assert published["moneyflow"]["published_rows"] == 1
    path = tmp_path / "ext_data/ext_tushare_moneyflow/timeseries/date=2025-01-02/part.parquet"
    assert pl.read_parquet(path).select("symbol", "trade_date", "net_mf_amount").row(0) == (
        "000001.SZ",
        "2025-01-02",
        20.0,
    )
    config = json.loads((tmp_path / "ext_data/ext_tushare_moneyflow/config.json").read_text())
    assert config["primary_key"] == ["symbol", "trade_date"]


def test_parser_version_change_rebuilds_completed_staging(tmp_path):
    client = _Client({
        "moneyflow": [{
            "ts_code": "000001.SZ",
            "trade_date": "20250102",
            "net_mf_vol": 10,
            "net_mf_amount": 20,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "parser-upgrade",
            start=date(2025, 1, 1),
            end=date(2025, 12, 31),
        ),
        client,
    )
    spec = DATASET_SPECS["moneyflow"]
    engine.collect((spec,), symbols=["000001.SZ"])
    manifest_path = tmp_path / "ext_data/_ingestion/tushare_proxy/moneyflow/parser-upgrade.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["parser_version"] = "tushare_ingestion_v1"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    engine.collect((spec,), symbols=["000001.SZ"])

    assert len(client.calls) == 2


def test_daily_publish_blocks_conflict_before_replacing_partition(tmp_path):
    target = tmp_path / "kline_daily/date=2025-01-02/part.parquet"
    target.parent.mkdir(parents=True)
    original = pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2025, 1, 2)],
        "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.0],
        "volume": [100.0], "amount": [1000.0],
    })
    original.write_parquet(target)
    client = _Client({
        "daily": [{
            "ts_code": "000001.SZ",
            "trade_date": "20250102",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 12,
            "vol": 1,
            "amount": 1,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "daily-conflict",
            start=date(2025, 1, 1),
            end=date(2025, 12, 31),
            publish=True,
        ),
        client,
    )
    spec = DATASET_SPECS["daily"]
    engine.collect((spec,), symbols=["000001.SZ"])

    with pytest.raises(TushareIngestionBlocked, match="canonical overlap conflicts"):
        engine.publish((spec,))

    assert pl.read_parquet(target)["close"].to_list() == [10.0]


def test_wholly_empty_dataset_is_blocked_unless_preflight_contract_allows_it(tmp_path):
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "empty-run", start=date(2025, 1, 1), end=date(2025, 12, 31)),
        _Client({}),
    )

    result = engine.collect((DATASET_SPECS["moneyflow"],), symbols=["000001.SZ"])
    assert result["moneyflow"]["status"] == "blocked"
    manifest = json.loads(Path(result["moneyflow"]["manifest"]).read_text())
    assert manifest["empty_unconfirmed_batches"] == ["000001.SZ-2025"]

    valid = engine.collect((DATASET_SPECS["express"],), symbols=["000001.SZ"])
    assert valid["express"]["status"] == "completed"


def test_weekly_audit_accepts_previously_published_valid_empty_dataset(tmp_path):
    class EmptyWithSchemaClient:
        def request(self, api_name, _params):
            fields = ("ts_code", "name", "start_date")
            return TushareResponse(
                api_name,
                0,
                "",
                fields,
                (),
                {"code": 0, "msg": "", "data": {"fields": list(fields), "items": []}},
            )

    spec = DATASET_SPECS["namechange"]
    collector = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "empty-source", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        EmptyWithSchemaClient(),
    )
    collector.collect((spec,), symbols=["000001.SZ"])
    collector.publish((spec,))
    auditor = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "later-audit", start=date(2025, 1, 1), end=date(2025, 12, 31)),
        _Client({}),
    )

    report = auditor.audit((spec,))

    assert report["status"] == "healthy"
    assert report["datasets"]["namechange"]["status"] == "valid_empty"


def test_incremental_collection_uses_market_dates_instead_of_symbol_loops(tmp_path):
    client = _Client({
        "daily": [{
            "ts_code": "000001.SZ", "trade_date": "20250102",
            "open": 10, "high": 11, "low": 9, "close": 10,
            "vol": 1, "amount": 1,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "incremental",
            start=date(2025, 1, 2),
            end=date(2025, 1, 3),
            incremental=True,
        ),
        client,
    )

    engine.collect(
        (DATASET_SPECS["daily"],),
        symbols=["000001.SZ", "000002.SZ"],
        trade_dates=[date(2025, 1, 2), date(2025, 1, 3)],
    )

    assert [params for api, params in client.calls if api == "daily"] == [
        {"trade_date": "20250102"},
        {"trade_date": "20250103"},
    ]


def test_reference_publish_keeps_existing_instrument_and_adds_gap(tmp_path):
    target = tmp_path / "instruments/instruments.parquet"
    target.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "name": ["平安银行"],
        "code": ["000001"],
        "exchange": ["SZ"],
        "listing_date": [date(1991, 4, 3)],
    }).write_parquet(target)
    class ReferenceClient(_Client):
        def request(self, api_name: str, params: dict) -> TushareResponse:
            self.calls.append((api_name, dict(params)))
            rows = [
            {"ts_code": "000001.SZ", "name": "平安银行", "list_status": "L", "list_date": "19910403"},
            {"ts_code": "000002.SZ", "name": "万科A", "list_status": "L", "list_date": "19910129"},
            ] if params.get("list_status") == "L" else []
            return _response(api_name, rows)

    client = ReferenceClient({})
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "reference", start=date(2025, 1, 1), end=date(2025, 1, 2), publish=True),
        client,
    )
    spec = DATASET_SPECS["stock_basic"]

    engine.collect((spec,))
    published = engine.publish((spec,))["stock_basic"]

    assert published["published_rows"] == 1
    assert pl.read_parquet(target)["symbol"].sort().to_list() == ["000001.SZ", "000002.SZ"]
    lifecycle = tmp_path / "pit_reference/history/instrument_lifecycle_events/part.parquet"
    assert lifecycle.exists()


def _index_weight_rows(snapshot: str, *, start: int = 600000) -> list[dict]:
    return [
        {
            "index_code": "000300.SH",
            "con_code": f"{start + index}.SH",
            "trade_date": snapshot,
            "weight": 100 / 300,
        }
        for index in range(300)
    ]


def test_pit_index_publish_creates_daily_snapshot_table(tmp_path):
    client = _Client({
        "index_weight": _index_weight_rows("20250102")
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "pit", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["index_weight"]

    engine.collect((spec,), indexes=["000300.SH"])
    engine.publish((spec,))

    target = tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    row = pl.read_parquet(target).row(0, named=True)
    assert row["index_symbol"] == "000300.SH"
    assert row["snapshot_date"] == date(2025, 1, 2)
    assert row["source"] == "tushare_proxy"


def test_index_weight_rejects_incomplete_daily_snapshot(tmp_path):
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "incomplete-index",
            start=date(2025, 1, 2),
            end=date(2025, 1, 2),
            publish=True,
        ),
        _Client({"index_weight": _index_weight_rows("20250102")[:299]}),
    )
    spec = DATASET_SPECS["index_weight"]

    engine.collect((spec,), indexes=["000300.SH"])

    with pytest.raises(TushareIngestionBlocked, match="incomplete daily membership"):
        engine.publish((spec,))
    assert not (
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    ).exists()


def test_index_weight_incremental_retains_each_complete_daily_snapshot(tmp_path):
    first = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "weights-1", start=date(2025, 1, 2), end=date(2025, 1, 2), publish=True),
        _Client({"index_weight": _index_weight_rows("20250102")}),
    )
    spec = DATASET_SPECS["index_weight"]
    first.collect((spec,), indexes=["000300.SH"])
    first.publish((spec,))
    second = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "weights-2", start=date(2025, 2, 3), end=date(2025, 2, 3), publish=True),
        _Client({"index_weight": _index_weight_rows("20250203", start=600001)}),
    )
    second.collect((spec,), indexes=["000300.SH"])
    second.publish((spec,))

    memberships = pl.read_parquet(
        tmp_path / "pit_reference/history/index_membership_history/part.parquet"
    ).sort(["snapshot_date", "member_symbol"])
    assert memberships.height == 600
    assert memberships.group_by("snapshot_date").len().sort("snapshot_date").to_dicts() == [
        {"snapshot_date": date(2025, 1, 2), "len": 300},
        {"snapshot_date": date(2025, 2, 3), "len": 300},
    ]
    assert memberships.filter(pl.col("member_symbol") == "600000.SH")[
        "snapshot_date"
    ].to_list() == [date(2025, 1, 2)]
    assert memberships.filter(pl.col("member_symbol") == "600300.SH")[
        "snapshot_date"
    ].to_list() == [date(2025, 2, 3)]
    audit = second.audit((spec,))
    assert audit["status"] == "healthy"
    assert audit["datasets"]["index_weight"]["pit_membership_validation"]["usable"] is True


def test_weekly_audit_detects_financial_and_valuation_lookahead(tmp_path):
    income = tmp_path / "financials/income/part.parquet"
    income.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "period_end": ["2025-03-31"],
        "announce_date": ["2025-03-30"],
    }).write_parquet(income)
    valuation = tmp_path / "valuation_daily/date=2025-03-31/part.parquet"
    valuation.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2025, 3, 31)],
        "income_announce_date": [date(2025, 4, 1)],
    }).write_parquet(valuation)
    auditor = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "pit-audit", start=date(2025, 1, 1), end=date(2025, 12, 31)),
        _Client({}),
    )

    report = auditor.audit((DATASET_SPECS["income"],))

    assert report["status"] == "unhealthy"
    assert report["datasets"]["income"]["pit_front_look_rows"] == 1
    assert report["pit_checks"]["valuation_lookahead_rows"] == 1


def _sw_industry_row() -> dict:
    return {
        "l1_code": "801130.SI",
        "l1_name": "纺织服饰",
        "l2_code": "801131.SI",
        "l2_name": "纺织制造",
        "l3_code": "851316.SI",
        "l3_name": "其他纺织",
        "ts_code": "001312.SZ",
        "name": "某公司",
        "in_date": "20260428",
        "out_date": None,
    }


def test_index_member_all_expands_every_sw_industry_level():
    frame, audit = normalize_dataset_rows(
        DATASET_SPECS["index_member_all"],
        [_sw_industry_row()],
    )

    assert audit["rejected_rows"] == 0
    assert frame.select(
        "member_symbol",
        "industry_standard",
        "industry_standard_code",
        "industry_level",
        "industry_code",
        "industry_name",
        "effective_from",
    ).to_dicts() == [
        {
            "member_symbol": "001312.SZ",
            "industry_standard": "申银万国行业分类标准",
            "industry_standard_code": "008003",
            "industry_level": 1,
            "industry_code": "801130.SI",
            "industry_name": "纺织服饰",
            "effective_from": "2026-04-28",
        },
        {
            "member_symbol": "001312.SZ",
            "industry_standard": "申银万国行业分类标准",
            "industry_standard_code": "008003",
            "industry_level": 2,
            "industry_code": "801131.SI",
            "industry_name": "纺织制造",
            "effective_from": "2026-04-28",
        },
        {
            "member_symbol": "001312.SZ",
            "industry_standard": "申银万国行业分类标准",
            "industry_standard_code": "008003",
            "industry_level": 3,
            "industry_code": "851316.SI",
            "industry_name": "其他纺织",
            "effective_from": "2026-04-28",
        },
    ]


def test_index_member_all_publishes_levels_as_independent_pit_rows(tmp_path):
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "sw-levels",
            start=date(2026, 4, 28),
            end=date(2026, 8, 7),
            publish=True,
        ),
        _Client({"index_member_all": [_sw_industry_row()]}),
    )
    spec = DATASET_SPECS["index_member_all"]

    engine.collect((spec,))
    published = engine.publish((spec,))["index_member_all"]

    target = tmp_path / "pit_reference/history/industry_membership_history/part.parquet"
    frame = pl.read_parquet(target)
    assert published["published_rows"] == 3
    assert frame.select("industry_level", "industry_code").to_dicts() == [
        {"industry_level": 1, "industry_code": "801130.SI"},
        {"industry_level": 2, "industry_code": "801131.SI"},
        {"industry_level": 3, "industry_code": "851316.SI"},
    ]


def test_ci_index_member_without_declared_level_still_publishes(tmp_path):
    engine = TushareDatasetIngestion(
        IngestionConfig(
            tmp_path,
            "citics-no-level",
            start=date(2026, 4, 28),
            end=date(2026, 8, 7),
            publish=True,
        ),
        _Client({"ci_index_member": [{
            "l1_code": "CI005001.WI",
            "l1_name": "银行",
            "con_code": "000001.SZ",
            "con_name": "平安银行",
            "in_date": "20260428",
            "out_date": None,
        }]}),
    )
    spec = DATASET_SPECS["ci_index_member"]

    engine.collect((spec,))
    published = engine.publish((spec,))["ci_index_member"]

    assert published["published_rows"] == 1


def test_financial_publish_blocks_existing_key_conflict(tmp_path):
    target = tmp_path / "financials/income/part.parquet"
    target.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "period_end": ["2025-03-31"],
        "announce_date": ["2025-04-20"],
        "revenue": [100.0],
    }).write_parquet(target)
    client = _Client({
        "income": [{
            "ts_code": "000001.SZ", "end_date": "20250331",
            "ann_date": "20250420", "revenue": 200,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "financial", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["income"]
    engine.collect((spec,), symbols=["000001.SZ"])

    with pytest.raises(TushareIngestionBlocked, match="canonical overlap conflicts"):
        engine.publish((spec,))

    assert pl.read_parquet(target)["revenue"].to_list() == [100.0]


def test_value_equivalent_financial_revisions_publish_once_and_archive_all(tmp_path):
    client = _Client({
        "income": [
            {
                "ts_code": "000001.SZ", "end_date": "20250331",
                "ann_date": "20250420", "revenue": 100, "update_flag": "0",
            },
            {
                "ts_code": "000001.SZ", "end_date": "20250331",
                "ann_date": "20250420", "revenue": 100, "update_flag": "1",
            },
        ]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "revisions", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["income"]
    engine.collect((spec,), symbols=["000001.SZ"])

    result = engine.publish((spec,))["income"]

    assert result["published_rows"] == 1
    assert result["revision_rows"] == 2
    assert pl.read_parquet(tmp_path / "financials/income/part.parquet").height == 1
    revisions = pl.read_parquet(tmp_path / "financials/_revisions/income/part.parquet")
    assert revisions.height == 2
    assert revisions["source_revision_hash"].n_unique() == 2


def test_latest_provider_financial_revision_is_canonical_and_all_versions_archive(tmp_path):
    client = _Client({
        "income": [
            {
                "ts_code": "300750.SZ", "end_date": "20250331",
                "ann_date": "20250415", "revenue": 100,
                "rd_exp": None, "update_flag": "0",
            },
            {
                "ts_code": "300750.SZ", "end_date": "20250331",
                "ann_date": "20250415", "revenue": 100,
                "rd_exp": 12, "update_flag": "1",
            },
        ]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "revision-rank", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["income"]
    engine.collect((spec,), symbols=["300750.SZ"])

    result = engine.publish((spec,))["income"]

    canonical = pl.read_parquet(tmp_path / "financials/income/part.parquet")
    revisions = pl.read_parquet(tmp_path / "financials/_revisions/income/part.parquet")
    assert result["published_rows"] == 1
    assert canonical.select("rd_expense").row(0) == (12.0,)
    assert revisions.height == 2
    assert revisions["provider_revision_flag"].sort().to_list() == ["0", "1"]


def test_daily_basic_publishes_share_and_valuation_gaps_with_units(tmp_path):
    client = _Client({
        "daily_basic": [{
            "ts_code": "000001.SZ", "trade_date": "20250102", "close": 10,
            "total_share": 100, "float_share": 80,
            "total_mv": 1000, "circ_mv": 800,
            "pe_ttm": 10, "pb": 2, "ps_ttm": 3,
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "daily-basic", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["daily_basic"]
    engine.collect((spec,), symbols=["000001.SZ"])

    published = engine.publish((spec,))["daily_basic"]

    assert published["canonical_supplements"] == {
        "shares_added_rows": 1,
        "valuation_added_rows": 1,
    }
    shares = pl.read_parquet(tmp_path / "financials/shares/part.parquet")
    assert shares.select("total_shares", "float_shares").row(0) == (1_000_000.0, 800_000.0)
    valuation = pl.read_parquet(tmp_path / "valuation_daily/date=2025-01-02/part.parquet")
    assert valuation.select("market_cap", "float_market_cap").row(0) == (
        10_000_000.0,
        8_000_000.0,
    )
    config = json.loads((tmp_path / "ext_data/ext_tushare_daily_basic/config.json").read_text())
    assert "factor-input" in config["allowed_usage"]

    instruments = tmp_path / "instruments/instruments.parquet"
    instruments.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000001.SZ"]}).write_parquet(instruments)
    bars = tmp_path / "kline_daily/date=2025-01-02/part.parquet"
    bars.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2025, 1, 2)],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "close": [10.0],
        "volume": [100.0],
        "amount": [1_000.0],
    }).write_parquet(bars)

    from app.indicators.pipeline import run_pipeline
    from app.services.daily_valuation import build_daily_valuation

    assert run_pipeline(tmp_path) == 1
    enriched = pl.read_parquet(
        tmp_path / "kline_daily_enriched/date=2025-01-02/part.parquet"
    )
    assert enriched.select("total_shares", "float_shares").row(0) == (
        1_000_000.0,
        800_000.0,
    )
    assert build_daily_valuation(tmp_path)["rows"] == 1
    rebuilt_valuation = pl.read_parquet(
        tmp_path / "valuation_daily/date=2025-01-02/part.parquet"
    )
    assert rebuilt_valuation.select("market_cap", "float_market_cap").row(0) == (
        10_000_000.0,
        8_000_000.0,
    )


def test_dividend_publish_requires_ex_date_and_uses_per_share_cash(tmp_path):
    client = _Client({
        "dividend": [{
            "ts_code": "000001.SZ", "ann_date": "20250420", "end_date": "20241231",
            "div_proc": "实施", "cash_div": 0.3, "ex_date": "20250510",
        }]
    })
    engine = TushareDatasetIngestion(
        IngestionConfig(tmp_path, "dividend", start=date(2025, 1, 1), end=date(2025, 12, 31), publish=True),
        client,
    )
    spec = DATASET_SPECS["dividend"]
    engine.collect((spec,), symbols=["000001.SZ"])
    engine.publish((spec,))

    target = tmp_path / "corporate_actions/stock_dividends.parquet"
    assert pl.read_parquet(target).select("symbol", "event_date", "cash_per_share").row(0) == (
        "000001.SZ",
        date(2025, 5, 10),
        0.3,
    )
