"""Authoritative data ownership rules for TickFlow persisted datasets.

This module is intentionally static: callers can inspect one place to decide
whether a dataset is TickFlow-primary, TickFlow-derived, or auxiliary extension
context. It does not read parquet files or external providers.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast


Authority = Literal["primary", "derived", "extension", "deprecated-overlap"]
ReferenceAssetType = Literal["stock", "etf", "index"]
ReferenceTimeframe = Literal["1d", "1m"]

CANONICAL_USAGE = "canonical"
DISPLAY_USAGE = "display"
FILTER_USAGE = "filter"
DIMENSION_USAGE = "dimension"
EVENT_USAGE = "event-context"


@dataclass(frozen=True, slots=True)
class DatasetAuthority:
    dataset: str
    authority: Authority
    owner: str
    storage: tuple[str, ...]
    fields: tuple[str, ...]
    note: str


@dataclass(frozen=True, slots=True)
class ExtensionAuthority:
    config_id: str
    authority: Authority
    canonical_dataset: str
    overlap_policy: str
    allowed_usage: tuple[str, ...]
    deprecated_overlap_fields: tuple[str, ...] = ()

    def metadata(self) -> dict[str, Any]:
        return {
            "authority": self.authority,
            "canonical_dataset": self.canonical_dataset,
            "overlap_policy": self.overlap_policy,
            "allowed_usage": list(self.allowed_usage),
        }


@dataclass(frozen=True, slots=True)
class ReferenceAsset:
    symbol: str
    asset_type: ReferenceAssetType
    timeframe: ReferenceTimeframe
    canonical_dataset: str

    def to_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "asset_type": self.asset_type,
            "timeframe": self.timeframe,
            "canonical_dataset": self.canonical_dataset,
        }


PRIMARY_DATASETS: dict[str, DatasetAuthority] = {
    "stock_daily": DatasetAuthority(
        dataset="stock_daily",
        authority="primary",
        owner="tickflow",
        storage=("kline_daily",),
        fields=("open", "high", "low", "close", "volume", "amount", "quote_ts"),
        note="A-share daily OHLCV must come from TickFlow-normalized daily K data.",
    ),
    "etf_daily": DatasetAuthority(
        dataset="etf_daily",
        authority="primary",
        owner="tickflow",
        storage=("kline_etf_daily",),
        fields=("open", "high", "low", "close", "volume", "amount", "quote_ts"),
        note="ETF daily OHLCV is a first-class TickFlow asset dataset, not stock data.",
    ),
    "index_daily": DatasetAuthority(
        dataset="index_daily",
        authority="primary",
        owner="tickflow",
        storage=("kline_index_daily",),
        fields=("open", "high", "low", "close", "volume", "amount"),
        note="Index daily OHLCV is routed separately from stocks and ETFs.",
    ),
    "stock_minute": DatasetAuthority(
        dataset="stock_minute",
        authority="primary",
        owner="tickflow",
        storage=("kline_minute",),
        fields=("datetime", "open", "high", "low", "close", "volume", "amount"),
        note="A-share minute bars stay in the stock minute table.",
    ),
    "etf_minute": DatasetAuthority(
        dataset="etf_minute",
        authority="primary",
        owner="tickflow",
        storage=("kline_etf_minute",),
        fields=("datetime", "open", "high", "low", "close", "volume", "amount"),
        note="ETF minute bars stay in the ETF minute table.",
    ),
    "realtime_quotes": DatasetAuthority(
        dataset="realtime_quotes",
        authority="primary",
        owner="tickflow",
        storage=("quote_service",),
        fields=("price", "change_pct", "amount", "volume"),
        note="Realtime price context must be read from TickFlow quote paths.",
    ),
    "financials": DatasetAuthority(
        dataset="financials",
        authority="primary",
        owner="tickflow",
        storage=(
            "financials/income",
            "financials/balance_sheet",
            "financials/cash_flow",
            "financials/metrics",
            "financials/shares",
        ),
        fields=("announce_date", "period_end", "total_shares", "float_shares"),
        note="PIT financial and share-capital fields use TickFlow financial tables.",
    ),
    "corporate_actions": DatasetAuthority(
        dataset="corporate_actions",
        authority="primary",
        owner="tickflow",
        storage=("corporate_actions/stock_dividends.parquet",),
        fields=("event_date", "cash_per_share"),
        note="Corporate-action event-date dividends are the canonical replay source.",
    ),
}


DERIVED_DATASETS: dict[str, DatasetAuthority] = {
    "valuation_daily": DatasetAuthority(
        dataset="valuation_daily",
        authority="derived",
        owner="tickflow",
        storage=("valuation_daily",),
        fields=(
            "market_cap",
            "float_market_cap",
            "float_share_ratio",
            "pe_ttm",
            "pb",
            "ps_ttm",
            "pcf_ttm",
        ),
        note="Derived PIT values from raw close, historical shares, and announced statements.",
    ),
    "stock_enriched": DatasetAuthority(
        dataset="stock_enriched",
        authority="derived",
        owner="tickflow",
        storage=("kline_daily_enriched",),
        fields=(
            "raw_close",
            "raw_high",
            "raw_low",
            "turnover_rate",
            "consecutive_limit_ups",
        ),
        note="Derived from TickFlow daily, adjustment, financial shares, and limit rules.",
    ),
    "etf_enriched": DatasetAuthority(
        dataset="etf_enriched",
        authority="derived",
        owner="tickflow",
        storage=("kline_etf_enriched",),
        fields=("raw_close", "raw_high", "raw_low"),
        note="ETF derived daily data remains separate from stock enriched data.",
    ),
}


EXTENSION_POLICIES: dict[str, ExtensionAuthority] = {
    "ext_gn_ths": ExtensionAuthority(
        "ext_gn_ths",
        "extension",
        "ths_concept_membership",
        "external_dimension_only",
        (DISPLAY_USAGE, FILTER_USAGE, DIMENSION_USAGE),
    ),
    "ext_hy_ths": ExtensionAuthority(
        "ext_hy_ths",
        "extension",
        "ths_industry_membership",
        "external_dimension_only",
        (DISPLAY_USAGE, FILTER_USAGE, DIMENSION_USAGE),
    ),
    "ext_industry_tdx": ExtensionAuthority(
        "ext_industry_tdx",
        "extension",
        "tdx_industry_dimension",
        "external_dimension_only",
        (DISPLAY_USAGE, FILTER_USAGE, DIMENSION_USAGE),
    ),
    "ext_money_flow": ExtensionAuthority(
        "ext_money_flow",
        "deprecated-overlap",
        "tickflow.realtime_quotes",
        "overlapping_quote_fields_are_display_context_only",
        (DISPLAY_USAGE, FILTER_USAGE),
        ("change_pct",),
    ),
    "ext_kpl_funds": ExtensionAuthority(
        "ext_kpl_funds",
        "deprecated-overlap",
        "tickflow.realtime_quotes",
        "flow_structure_is_extension_but_quote_like_fields_are_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
        ("price", "change_pct", "amount", "turnover_pct", "market_cap"),
    ),
    "ext_kpl_auction": ExtensionAuthority(
        "ext_kpl_auction",
        "extension",
        "kaipanla_auction_event",
        "auction_event_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
        (
            "realtime_change_pct_0915",
            "realtime_change_pct_0920",
            "realtime_change_pct_0925",
        ),
    ),
    "ext_kpl_limitup": ExtensionAuthority(
        "ext_kpl_limitup",
        "deprecated-overlap",
        "tickflow.enriched_limit_state",
        "limit_reason_is_extension_but_limit_metrics_are_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
        ("turnover_pct", "float_market_cap", "consecutive_limitups"),
    ),
    "ext_kpl_lhb": ExtensionAuthority(
        "ext_kpl_lhb",
        "extension",
        "kaipanla_dragon_tiger",
        "seat_and_listing_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_lhb_detail": ExtensionAuthority(
        "ext_kpl_lhb_detail",
        "extension",
        "kaipanla_dragon_tiger_seats",
        "seat_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_lhb_movement": ExtensionAuthority(
        "ext_kpl_lhb_movement",
        "extension",
        "kaipanla_dragon_tiger_participants",
        "participant_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_regulatory": ExtensionAuthority(
        "ext_kpl_regulatory",
        "extension",
        "kaipanla_regulatory_events",
        "regulatory_event_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_northbound_sector": ExtensionAuthority(
        "ext_kpl_northbound_sector",
        "extension",
        "kaipanla_northbound_sector_holding",
        "quarterly_holding_context_not_daily_flow",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_northbound_stock": ExtensionAuthority(
        "ext_kpl_northbound_stock",
        "deprecated-overlap",
        "tickflow.financials",
        "northbound_holding_is_extension_but_share_and_market_cap_fields_are_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
        ("total_shares", "market_cap", "float_market_cap"),
    ),
    "ext_kpl_sector_constituents": ExtensionAuthority(
        "ext_kpl_sector_constituents",
        "deprecated-overlap",
        "tickflow.daily",
        "sector_membership_is_extension_but_quote_like_fields_are_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, DIMENSION_USAGE),
        ("last_price", "change_pct", "amount", "turnover_rate", "float_market_value"),
    ),
    "ext_kpl_shareholder_changes": ExtensionAuthority(
        "ext_kpl_shareholder_changes",
        "extension",
        "kaipanla_top_float_shareholders",
        "shareholder_structure_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_kpl_shareholder_counts": ExtensionAuthority(
        "ext_kpl_shareholder_counts",
        "extension",
        "kaipanla_shareholder_count",
        "shareholder_count_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_tdx_margin": ExtensionAuthority(
        "ext_tdx_margin",
        "extension",
        "tdx_margin_f10",
        "financing_balance_context_only",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_tdx_forecast": ExtensionAuthority(
        "ext_tdx_forecast",
        "extension",
        "tdx_earnings_forecast_f10",
        "f10_summary_context_not_tickflow_financial_statement",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_tdx_express": ExtensionAuthority(
        "ext_tdx_express",
        "extension",
        "tdx_earnings_express_f10",
        "f10_summary_context_not_tickflow_financial_statement",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
    ),
    "ext_tdx_dividend_history": ExtensionAuthority(
        "ext_tdx_dividend_history",
        "deprecated-overlap",
        "tickflow.corporate_actions",
        "record_date_context_only; event_date_replay_uses_corporate_actions",
        (DISPLAY_USAGE, FILTER_USAGE, EVENT_USAGE),
        ("cash_per_share", "record_date", "ex_dividend_date"),
    ),
}


REFERENCE_DATASETS: dict[tuple[ReferenceAssetType, ReferenceTimeframe], str] = {
    ("stock", "1d"): "kline_daily",
    ("stock", "1m"): "kline_minute",
    ("etf", "1d"): "kline_etf_daily",
    ("etf", "1m"): "kline_etf_minute",
    ("index", "1d"): "kline_index_daily",
}


def dataset_authority(dataset: str) -> DatasetAuthority | None:
    return PRIMARY_DATASETS.get(dataset) or DERIVED_DATASETS.get(dataset)


def extension_policy(config_id: str) -> ExtensionAuthority | None:
    return EXTENSION_POLICIES.get(str(config_id).strip())


def extension_config_metadata(config_id: str) -> dict[str, Any]:
    policy = extension_policy(config_id)
    return policy.metadata() if policy else {}


def deprecated_overlap_fields(config_id: str) -> set[str]:
    policy = extension_policy(config_id)
    return set(policy.deprecated_overlap_fields) if policy else set()


def is_deprecated_overlap_field(config_id: str, field_name: str) -> bool:
    return field_name in deprecated_overlap_fields(config_id)


def assert_extension_field_usage(config_id: str, field_name: str, usage: str) -> None:
    """Fail closed if an extension overlap is requested as canonical data."""
    policy = extension_policy(config_id)
    if policy is None:
        return
    normalized_usage = str(usage).strip().lower()
    if normalized_usage == CANONICAL_USAGE and field_name in policy.deprecated_overlap_fields:
        raise ValueError(
            f"{config_id}.{field_name} overlaps {policy.canonical_dataset}; "
            "read TickFlow canonical data instead"
        )
    if (
        normalized_usage
        and normalized_usage not in policy.allowed_usage
        and normalized_usage != CANONICAL_USAGE
    ):
        raise ValueError(
            f"{config_id} only supports usage {list(policy.allowed_usage)}, "
            f"not {normalized_usage!r}"
        )


def normalize_reference_asset(
    raw: Mapping[str, Any] | str,
    *,
    default_asset_type: ReferenceAssetType = "etf",
    default_timeframe: ReferenceTimeframe = "1d",
) -> ReferenceAsset:
    """Normalize upper-level reference K-line handles without creating fake stocks."""
    if isinstance(raw, str):
        symbol = raw
        asset_type = default_asset_type
        timeframe = default_timeframe
    else:
        symbol = str(raw.get("symbol") or "").strip()
        asset_type = str(raw.get("asset_type") or default_asset_type).strip().lower()
        timeframe = str(raw.get("timeframe") or default_timeframe).strip().lower()
    symbol = symbol.upper()
    for source, target in {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}.items():
        if symbol.endswith(source):
            symbol = f"{symbol[:-len(source)]}{target}"
            break
    if not symbol:
        raise ValueError("reference asset symbol is required")
    if asset_type not in {"stock", "etf", "index"}:
        raise ValueError("reference asset_type must be stock, etf, or index")
    if timeframe not in {"1d", "1m"}:
        raise ValueError("reference timeframe must be 1d or 1m")
    key = (cast(ReferenceAssetType, asset_type), cast(ReferenceTimeframe, timeframe))
    canonical_dataset = REFERENCE_DATASETS.get(key)
    if canonical_dataset is None:
        raise ValueError(f"{asset_type} reference data does not support timeframe {timeframe}")
    return ReferenceAsset(
        symbol=symbol,
        asset_type=cast(ReferenceAssetType, asset_type),
        timeframe=cast(ReferenceTimeframe, timeframe),
        canonical_dataset=canonical_dataset,
    )
