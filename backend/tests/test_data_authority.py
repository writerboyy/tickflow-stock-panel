from __future__ import annotations

import pytest

from app.services.data_authority import (
    CANONICAL_USAGE,
    assert_extension_field_usage,
    dataset_authority,
    deprecated_overlap_fields,
    extension_config_metadata,
    normalize_reference_asset,
)
from app.services.ext_data import ExtConfig, ExtField


def test_tickflow_primary_datasets_cover_etf_daily_and_minute_tables():
    etf_daily = dataset_authority("etf_daily")
    etf_minute = dataset_authority("etf_minute")

    assert etf_daily is not None
    assert etf_daily.authority == "primary"
    assert etf_daily.owner == "tickflow"
    assert etf_daily.storage == ("kline_etf_daily",)
    assert etf_minute is not None
    assert etf_minute.storage == ("kline_etf_minute",)


def test_ext_config_backfills_authority_metadata_without_rewriting_legacy_config():
    config = ExtConfig(
        id="ext_money_flow",
        label="资金流向",
        mode="snapshot",
        fields=[ExtField("change_pct", "float", "涨跌幅")],
    )

    payload = config.to_dict()
    assert payload["authority"] == "deprecated-overlap"
    assert payload["canonical_dataset"] == "tickflow.realtime_quotes"
    assert payload["overlap_policy"] == "overlapping_quote_fields_are_display_context_only"
    assert payload["allowed_usage"] == ["display", "filter"]


def test_unknown_user_extension_has_no_forced_authority_metadata():
    config = ExtConfig(
        id="user_custom_factor",
        label="用户自定义",
        mode="snapshot",
        fields=[ExtField("symbol")],
    )

    assert "authority" not in config.to_dict()
    assert extension_config_metadata("user_custom_factor") == {}


def test_extension_overlap_fields_cannot_be_requested_as_canonical_inputs():
    assert "change_pct" in deprecated_overlap_fields("ext_money_flow")
    assert "market_cap" in deprecated_overlap_fields("ext_kpl_funds")

    with pytest.raises(ValueError, match="TickFlow canonical"):
        assert_extension_field_usage("ext_money_flow", "change_pct", CANONICAL_USAGE)

    # Non-overlap fields remain valid display/filter context.
    assert_extension_field_usage("ext_money_flow", "net", "display")


def test_reference_asset_handles_keep_etf_daily_and_minute_separate():
    daily = normalize_reference_asset({"symbol": "510300.XSHG", "asset_type": "etf", "timeframe": "1d"})
    minute = normalize_reference_asset({"symbol": "510300.SH", "asset_type": "etf", "timeframe": "1m"})

    assert daily.to_dict() == {
        "symbol": "510300.SH",
        "asset_type": "etf",
        "timeframe": "1d",
        "canonical_dataset": "kline_etf_daily",
    }
    assert minute.to_dict() == {
        "symbol": "510300.SH",
        "asset_type": "etf",
        "timeframe": "1m",
        "canonical_dataset": "kline_etf_minute",
    }


def test_index_minute_reference_fails_closed_because_no_local_table_exists():
    with pytest.raises(ValueError, match="does not support timeframe"):
        normalize_reference_asset({"symbol": "000001.SH", "asset_type": "index", "timeframe": "1m"})
