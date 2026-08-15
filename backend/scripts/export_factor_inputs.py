#!/usr/bin/env python3
"""Export a PIT factor-input table from the local canonical data only.

The exporter deliberately has no network/provider calls.  It joins the local
TickFlow-derived daily tables with the audited EasyTDX and Kaipanla extension
tables, keeping source names and units explicit.  Unknown factor definitions
are represented by null columns and a manifest entry instead of an estimate.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import polars as pl

from app.services.data_authority import FACTOR_USAGE
from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField
from app.services.ingestion_manifest import update_ingestion_manifest


TABLE_ID = "ext_factor_inputs"
OUTPUT_ROOT = Path("ext_data") / TABLE_ID / "timeseries"
SUPPORTED_FLOAT_FIELDS = (
    "pe_ttm",
    "pb",
    "ps_ttm",
    "dv_ttm",
    "turnover_rate_f",
    "roa",
    "assets_yoy",
    "margin_balance",
    "short_balance",
    "main_net",
    "main_buy",
    "main_sell",
    "holding_amount",
    "holding_shares",
    "holding_ratio",
    "market_ratio",
    "shareholder_change_pct",
    "float_holding_ratio",
    "chip_concentration",
    "buy_in_amount",
    "listings_count",
    "turnover",
    "turnover_pct",
    "net_profit_low_10k",
    "net_profit_high_10k",
    "net_profit_yoy_low_pct",
    "net_profit_yoy_high_pct",
)
SUPPORTED_STRING_FIELDS = (
    "stock_basic",
    "name",
    "namechange",
    "forecast_type",
    "summary",
)
SUPPORTED_DATE_FIELDS = ("listing_date",)
SUPPORTED_BOOL_FIELDS = (
    "top_list_flag",
    "csi300_member",
    "csi500_member",
    "sse50_member",
)
UNSUPPORTED_FIELDS = {
    "net_mf_amount": "没有与开盘啦 main_net 等价的统一资金流口径",
    "north_ratio": "北向数据只有季度原始持仓，不生成日度比例别名",
    "holder_num": "股东人数原始表未转成统一的 PIT 股东人数",
    "margin_ratio": "两融余额单位和公式未确认",
    "margin_buy_ratio": "两融买入额单位和公式未确认",
    "top_list_net_buy": "开盘啦 buy_in_amount 保留自身龙虎榜口径，不冒充统一净买额",
    "fc_surprise": "没有可靠的预期值来源",
    "express_yoy": "EasyTDX 业绩快报没有可靠的同比数值字段",
    "i_*": "本阶段不推断分钟因子公式，只输出分钟覆盖元数据",
}


def _parquet_files(root: Path) -> list[Path]:
    if root.is_file() and root.suffix == ".parquet":
        return [root]
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.parquet")
        if path.is_file() and not any(part.startswith(".") for part in path.relative_to(root).parts)
    )


def _scan(root: Path) -> pl.LazyFrame | None:
    files = _parquet_files(root)
    return pl.scan_parquet(files, missing_columns="insert", extra_columns="ignore") if files else None


def _scan_files(files: list[Path]) -> pl.LazyFrame | None:
    return pl.scan_parquet(files, missing_columns="insert", extra_columns="ignore") if files else None


def _date_partition_files(root: Path, start: date, end: date) -> list[Path]:
    files = _parquet_files(root)
    selected: list[Path] = []
    for path in files:
        partition_dates = [
            _parse_date(part.removeprefix("date="))
            for part in path.relative_to(root).parts[:-1]
            if part.startswith("date=")
        ]
        if not partition_dates or any(start <= value <= end for value in partition_dates if value):
            selected.append(path)
    return selected


def _date_expr(frame: pl.DataFrame | pl.LazyFrame, column: str) -> pl.Expr:
    dtype = frame.collect_schema().get(column) if isinstance(frame, pl.LazyFrame) else frame.schema.get(column)
    if dtype == pl.Date:
        return pl.col(column)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date()
    return pl.col(column).cast(pl.String).str.to_date(strict=False)


def _parse_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text or text.casefold() in {"none", "null", "nan", "nat", "-"}:
        return None
    text = text.replace("/", "-").replace(".", "-")
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y%m%d", "%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _with_date(frame: pl.DataFrame, column: str, alias: str) -> pl.DataFrame:
    if column not in frame.columns:
        return frame.with_columns(pl.lit(None, dtype=pl.Date).alias(alias))
    return frame.with_columns(_date_expr(frame, column).alias(alias))


def _read_daily(data_dir: Path, start: date, end: date, symbols: set[str] | None) -> pl.DataFrame:
    source = _scan(data_dir / "kline_daily_enriched")
    if source is None:
        raise FileNotFoundError("missing canonical kline_daily_enriched parquet")
    schema = source.collect_schema()
    required = [
        column
        for column in (
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "raw_close",
            "turnover_rate",
        )
        if column in schema
    ]
    if "symbol" not in required or "date" not in required:
        raise ValueError("kline_daily_enriched must contain symbol and date")
    frame = (
        source.select(required)
        .with_columns(_date_expr(source, "date").alias("date"))
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .collect(engine="streaming")
    )
    frame = frame.with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase())
    if symbols:
        frame = frame.filter(pl.col("symbol").is_in(sorted(symbols)))
    return frame.sort(["symbol", "date"])


def _read_valuation(
    data_dir: Path,
    start: date,
    end: date,
    symbols: set[str] | None,
) -> pl.DataFrame:
    source = _scan(data_dir / "valuation_daily")
    if source is None:
        return pl.DataFrame()
    schema = source.collect_schema()
    selected = [column for column in ("symbol", "date", "pe_ttm", "pb", "ps_ttm", "net_income_ttm") if column in schema]
    if not {"symbol", "date"}.issubset(selected):
        return pl.DataFrame()
    frame = (
        source.select(selected)
        .with_columns(_date_expr(source, "date").alias("date"))
        .filter((pl.col("date") >= start) & (pl.col("date") <= end))
        .collect(engine="streaming")
        .with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase())
    )
    if symbols:
        frame = frame.filter(pl.col("symbol").is_in(sorted(symbols)))
    return frame.unique(subset=["symbol", "date"], keep="last")


def _read_table(data_dir: Path, relative: str, columns: Iterable[str] | None = None) -> pl.DataFrame:
    source = _scan(data_dir / relative)
    if source is None:
        return pl.DataFrame()
    schema = source.collect_schema()
    selected = [column for column in (columns or schema.names()) if column in schema]
    return source.select(selected).collect(engine="streaming") if selected else pl.DataFrame()


def _read_partitioned(data_dir: Path, table_id: str) -> pl.DataFrame:
    root = data_dir / "ext_data" / table_id / "timeseries"
    files = _parquet_files(root)
    frames: list[pl.DataFrame] = []
    for path in files:
        frame = pl.read_parquet(path)
        partition = _parse_date(path.parent.name.removeprefix("date="))
        frame = frame.with_columns(pl.lit(partition, dtype=pl.Date).alias("_partition_date"))
        frames.append(frame)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _filter_symbols(frame: pl.DataFrame, symbols: set[str] | None) -> pl.DataFrame:
    if frame.is_empty() or not symbols or "symbol" not in frame.columns:
        return frame
    return frame.with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase()).filter(
        pl.col("symbol").is_in(sorted(symbols))
    )


def _prepare_partitioned(
    frame: pl.DataFrame,
    *,
    date_column: str,
    start: date,
    end: date,
    symbols: set[str] | None,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    frame = _filter_symbols(frame, symbols)
    if frame.is_empty():
        return frame
    frame = _with_date(frame, date_column, "_factor_date")
    if date_column not in frame.columns:
        frame = frame.with_columns(pl.col("_partition_date").alias("_factor_date"))
    frame = frame.with_columns(
        pl.when(pl.col("_factor_date").is_null())
        .then(pl.col("_partition_date"))
        .otherwise(pl.col("_factor_date"))
        .alias("_factor_date")
    ).filter((pl.col("_factor_date") >= start) & (pl.col("_factor_date") <= end))
    if frame.is_empty() or "symbol" not in frame.columns:
        return frame
    return frame.sort(["symbol", "_factor_date"]).unique(
        subset=["symbol", "_factor_date"], keep="last"
    )


def _join_by_date(base: pl.DataFrame, source: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    if source.is_empty() or "symbol" not in source.columns or "_factor_date" not in source.columns:
        return base
    available = [column for column in columns if column in source.columns]
    if not available:
        return base
    source = source.select(["symbol", "_factor_date", *available]).rename({"_factor_date": "date"})
    source = source.unique(subset=["symbol", "date"], keep="last")
    overlap = [column for column in available if column in base.columns]
    if overlap:
        source = source.rename({column: f"{column}__source" for column in overlap})
    joined = base.join(source, on=["symbol", "date"], how="left")
    for column in overlap:
        joined = joined.with_columns(
            pl.coalesce([pl.col(f"{column}__source"), pl.col(column)]).alias(column)
        ).drop(f"{column}__source")
    return joined


def _dividend_yield(base: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    events = _read_table(data_dir, "corporate_actions/stock_dividends.parquet")
    if (
        "raw_close" not in base.columns
        or events.is_empty()
        or not {"symbol", "event_date", "cash_per_share"}.issubset(events.columns)
    ):
        return base.with_columns(pl.lit(None, dtype=pl.Float64).alias("dv_ttm"))
    events = (
        _with_date(events, "event_date", "event_date")
        .with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase())
        .filter(pl.col("event_date").is_not_null())
        .group_by(["symbol", "event_date"])
        .agg(pl.col("cash_per_share").cast(pl.Float64, strict=False).sum().alias("_cash"))
        .sort(["symbol", "event_date"])
        .with_columns(pl.col("_cash").cum_sum().over("symbol").alias("_cum_cash"))
    )
    if events.is_empty():
        return base.with_columns(pl.lit(None, dtype=pl.Float64).alias("dv_ttm"))
    left = base.select("symbol", "date", "raw_close").sort(["symbol", "date"])
    current = left.join_asof(
        events.select("symbol", pl.col("event_date").alias("_asof_date"), "_cum_cash"),
        left_on="date",
        right_on="_asof_date",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    ).rename({"_cum_cash": "_current_cash"})
    prior = left.select(
        "symbol",
        pl.col("date").sub(timedelta(days=365)).alias("_cutoff"),
    ).sort(["symbol", "_cutoff"]).join_asof(
        events.select("symbol", pl.col("event_date").alias("_asof_date"), "_cum_cash"),
        left_on="_cutoff",
        right_on="_asof_date",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    ).select("symbol", pl.col("_cutoff").alias("date"), pl.col("_cum_cash").alias("_prior_cash"))
    result = current.join(prior, on=["symbol", "date"], how="left").with_columns(
        pl.when(
            pl.col("raw_close").is_not_null()
            & (pl.col("raw_close") > 0)
            & pl.col("_current_cash").is_not_null()
        )
        .then((pl.col("_current_cash") - pl.col("_prior_cash").fill_null(0)) / pl.col("raw_close") * 100)
        .otherwise(None)
        .alias("dv_ttm")
    )
    return base.join(result.select("symbol", "date", "dv_ttm"), on=["symbol", "date"], how="left")


def _year_before(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        return value.replace(year=value.year - 1, day=28)


def _balance_events(data_dir: Path, symbols: set[str] | None = None) -> pl.DataFrame:
    frame = _read_table(data_dir, "financials/balance_sheet")
    required = {"symbol", "period_end", "announce_date", "total_assets"}
    if frame.is_empty() or not required.issubset(frame.columns):
        return pl.DataFrame()
    frame = (
        _with_date(frame, "period_end", "_period_date")
        .pipe(_with_date, "announce_date", "_announce_date")
        .with_columns([
            pl.col("symbol").cast(pl.String).str.to_uppercase(),
            pl.col("total_assets").cast(pl.Float64, strict=False),
        ])
        .filter(pl.col("_period_date").is_not_null() & pl.col("_announce_date").is_not_null())
    )
    if symbols:
        frame = frame.filter(pl.col("symbol").is_in(sorted(symbols)))
    if frame.is_empty():
        return frame
    rows_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in frame.sort(["symbol", "_period_date", "_announce_date"]).iter_rows(named=True):
        rows_by_symbol[str(row["symbol"])].append(row)
    events: list[dict[str, Any]] = []
    for symbol, rows in rows_by_symbol.items():
        by_key: dict[tuple[date, date], dict[str, Any]] = {}
        for row in rows:
            key = (row["_period_date"], row["_announce_date"])
            by_key[key] = row
        clean = list(by_key.values())
        for row in clean:
            current_period = row["_period_date"]
            visible_prior = [
                item
                for item in clean
                if item["_period_date"] == _year_before(current_period)
                and item["_announce_date"] <= row["_announce_date"]
                and item.get("total_assets") is not None
            ]
            prior = max(visible_prior, key=lambda item: item["_announce_date"], default=None)
            current_assets = row.get("total_assets")
            yoy = None
            if prior and prior.get("total_assets") not in (None, 0) and current_assets is not None:
                yoy = (float(current_assets) / float(prior["total_assets"]) - 1) * 100
            events.append({
                "symbol": symbol,
                "_effective_date": row["_announce_date"],
                "_period_date": current_period,
                "total_assets": current_assets,
                "assets_yoy": yoy,
            })
    if not events:
        return pl.DataFrame()
    event_frame = pl.DataFrame(events).sort(["symbol", "_effective_date", "_period_date"])
    return event_frame.group_by(["symbol", "_effective_date"], maintain_order=True).tail(1).sort(
        ["symbol", "_effective_date"]
    )


def _join_financials(base: pl.DataFrame, data_dir: Path, symbols: set[str] | None = None) -> pl.DataFrame:
    events = _balance_events(data_dir, symbols)
    if events.is_empty():
        return base.with_columns([
            pl.lit(None, dtype=pl.Float64).alias("roa"),
            pl.lit(None, dtype=pl.Float64).alias("assets_yoy"),
        ])
    selected = base.select("symbol", "date").sort(["symbol", "date"]).join_asof(
        events.select("symbol", pl.col("_effective_date").alias("_asof_date"), "total_assets", "assets_yoy"),
        left_on="date",
        right_on="_asof_date",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    )
    result = base.join(selected.select("symbol", "date", "total_assets", "assets_yoy"), on=["symbol", "date"], how="left")
    if "net_income_ttm" in result.columns:
        result = result.with_columns(
            pl.when((pl.col("total_assets") > 0) & pl.col("net_income_ttm").is_not_null())
            .then(pl.col("net_income_ttm") / pl.col("total_assets") * 100)
            .otherwise(None)
            .alias("roa")
        )
    else:
        result = result.with_columns(pl.lit(None, dtype=pl.Float64).alias("roa"))
    return result.drop("total_assets")


def _join_instruments(base: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    instruments = _read_table(data_dir, "instruments/instruments.parquet")
    if instruments.is_empty() or "symbol" not in instruments.columns:
        return base.with_columns([
            pl.lit(None, dtype=pl.String).alias("stock_basic"),
            pl.lit(None, dtype=pl.String).alias("name"),
            pl.lit(None, dtype=pl.Date).alias("listing_date"),
        ])
    instruments = instruments.with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase())
    listing_column = "listing_date" if "listing_date" in instruments.columns else "list_date"
    if listing_column in instruments.columns:
        instruments = _with_date(instruments, listing_column, "listing_date")
    else:
        instruments = instruments.with_columns(pl.lit(None, dtype=pl.Date).alias("listing_date"))
    fields = [column for column in ["symbol", "name", "listing_date"] if column in instruments.columns]
    metadata = instruments.select(fields).unique("symbol", keep="last")
    result = base.join(metadata, on="symbol", how="left")
    if "name" in result.columns:
        result = result.with_columns(
            pl.when(pl.col("listing_date").is_null() | (pl.col("date") >= pl.col("listing_date")))
            .then(pl.col("name"))
            .otherwise(None)
            .alias("stock_basic")
        )
    else:
        result = result.with_columns(pl.lit(None, dtype=pl.String).alias("stock_basic"))
    return result


def _join_name_history(base: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    history = _read_table(data_dir, "instrument_name_history/part.parquet")
    required = {"symbol", "change_date", "after_name"}
    if history.is_empty() or not required.issubset(history.columns):
        return base.with_columns(pl.lit(None, dtype=pl.String).alias("namechange"))
    history = (
        _with_date(history, "change_date", "_change_date")
        .with_columns(pl.col("symbol").cast(pl.String).str.to_uppercase())
        .filter(pl.col("_change_date").is_not_null())
        .sort(["symbol", "_change_date"])
    )
    selected = base.select("symbol", "date").sort(["symbol", "date"]).join_asof(
        history.select("symbol", pl.col("_change_date").alias("_asof_date"), "after_name"),
        left_on="date",
        right_on="_asof_date",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    )
    return base.join(
        selected.select("symbol", "date", pl.col("after_name").alias("namechange")),
        on=["symbol", "date"],
        how="left",
    )


def _join_index_flags(
    base: pl.DataFrame,
    data_dir: Path,
    start: date,
    end: date,
    symbols: set[str] | None,
) -> pl.DataFrame:
    source = _scan(data_dir / "pit_reference/history/index_membership_history")
    if source is None:
        history = pl.DataFrame()
    else:
        schema = source.collect_schema()
        required = [
            column
            for column in ("index_symbol", "member_symbol", "snapshot_date")
            if column in schema
        ]
        if len(required) < 3:
            history = pl.DataFrame()
        else:
            history = source.select(required).with_columns(
                _date_expr(source, "snapshot_date").alias("snapshot_date")
            ).filter(
                pl.col("index_symbol").is_in(["000300.SH", "000905.SH"])
                & (pl.col("snapshot_date") >= start)
                & (pl.col("snapshot_date") <= end)
            )
            if symbols:
                history = history.filter(pl.col("member_symbol").is_in(sorted(symbols)))
            history = history.collect(engine="streaming")
    output: dict[str, pl.Series] = {}
    for field, index_symbol in (("csi300_member", "000300.SH"), ("csi500_member", "000905.SH")):
        if history.is_empty() or not {"index_symbol", "member_symbol", "snapshot_date"}.issubset(history.columns):
            output[field] = pl.Series(field, [None] * base.height, dtype=pl.Boolean)
            continue
        selected = _with_date(history, "snapshot_date", "snapshot_date").filter(
            pl.col("index_symbol") == index_symbol
        )
        dates = set(selected["snapshot_date"].drop_nulls().to_list())
        members = selected.select(
            pl.col("member_symbol").cast(pl.String).str.to_uppercase().alias("symbol"),
            "snapshot_date",
        ).unique().rename({"snapshot_date": "date"})
        true_rows = base.select("symbol", "date").join(members, on=["symbol", "date"], how="inner").with_columns(pl.lit(True).alias(field))
        defaults = base.select("symbol", "date").with_columns(
            pl.when(pl.col("date").is_in(sorted(dates))).then(False).otherwise(None).alias(field)
        )
        merged = defaults.join(true_rows, on=["symbol", "date"], how="left", suffix="__true").with_columns(
            pl.coalesce([pl.col(f"{field}__true"), pl.col(field)]).alias(field)
        )
        output[field] = merged[field]
    output["sse50_member"] = pl.Series("sse50_member", [None] * base.height, dtype=pl.Boolean)
    return base.with_columns(output.values())


def _join_extensions(base: pl.DataFrame, data_dir: Path, start: date, end: date, symbols: set[str] | None) -> pl.DataFrame:
    margin = _prepare_partitioned(_read_partitioned(data_dir, "ext_tdx_margin"), date_column="report_date", start=start, end=end, symbols=symbols)
    if not margin.is_empty():
        margin = margin.with_columns([
            (pl.col("margin_balance_10k") * 10000).alias("margin_balance") if "margin_balance_10k" in margin.columns else pl.lit(None, dtype=pl.Float64).alias("margin_balance"),
            (pl.col("short_balance_10k") * 10000).alias("short_balance") if "short_balance_10k" in margin.columns else pl.lit(None, dtype=pl.Float64).alias("short_balance"),
        ])
    base = _join_by_date(base, margin, ["margin_balance", "short_balance"])

    forecast = _prepare_partitioned(_read_partitioned(data_dir, "ext_tdx_forecast"), date_column="announcement_date", start=start, end=end, symbols=symbols)
    base = _join_by_date(base, forecast, ["forecast_type", "net_profit_low_10k", "net_profit_high_10k", "net_profit_yoy_low_pct", "net_profit_yoy_high_pct", "summary"])

    funds = _prepare_partitioned(_read_partitioned(data_dir, "ext_kpl_funds"), date_column="_partition_date", start=start, end=end, symbols=symbols)
    base = _join_by_date(base, funds, ["main_net", "main_buy", "main_sell"])
    northbound = _prepare_partitioned(_read_partitioned(data_dir, "ext_kpl_northbound_stock"), date_column="report_date", start=start, end=end, symbols=symbols)
    base = _join_by_date(base, northbound, ["holding_amount", "holding_shares", "holding_ratio", "market_ratio"])
    shareholders = _prepare_partitioned(_read_partitioned(data_dir, "ext_kpl_shareholder_counts"), date_column="report_date", start=start, end=end, symbols=symbols)
    base = _join_by_date(base, shareholders, ["shareholder_change_pct", "float_holding_ratio", "chip_concentration"])
    lhb = _prepare_partitioned(_read_partitioned(data_dir, "ext_kpl_lhb"), date_column="_partition_date", start=start, end=end, symbols=symbols)
    if not lhb.is_empty():
        lhb = lhb.with_columns(pl.lit(True).alias("top_list_flag"))
    return _join_by_date(base, lhb, ["buy_in_amount", "listings_count", "turnover", "turnover_pct", "top_list_flag"])


def _factor_config() -> ExtConfig:
    fields = [ExtField("symbol", "string", "标的代码"), ExtField("date", "date", "交易日 (Date)")]
    fields.extend(ExtField(field, "float", field) for field in SUPPORTED_FLOAT_FIELDS)
    fields.extend(ExtField(field, "string", field) for field in SUPPORTED_STRING_FIELDS)
    fields.extend(ExtField(field, "date", field) for field in SUPPORTED_DATE_FIELDS)
    fields.extend(ExtField(field, "bool", field) for field in SUPPORTED_BOOL_FIELDS)
    return ExtConfig(
        id=TABLE_ID,
        label="本地因子输入",
        mode="timeseries",
        fields=fields,
        description="由本地 TickFlow、EasyTDX、开盘啦和 PIT 表导出的因子输入；未确认口径保持空值",
        symbol_map={"type": "mapped", "col": "symbol"},
        primary_key=["symbol", "date"],
        logical_date="date",
        units={
            "dv_ttm": "percent",
            "turnover_rate_f": "percent",
            "roa": "percent",
            "assets_yoy": "percent",
            "margin_balance": "CNY",
            "short_balance": "CNY",
            "main_net": "source_defined",
            "holding_ratio": "source_defined",
            "shareholder_change_pct": "source_defined",
            "buy_in_amount": "source_defined",
        },
    )


def _ensure_output_columns(frame: pl.DataFrame) -> pl.DataFrame:
    specs: list[tuple[str, pl.DataType]] = [
        *((field, pl.Float64) for field in SUPPORTED_FLOAT_FIELDS),
        *((field, pl.String) for field in SUPPORTED_STRING_FIELDS),
        *((field, pl.Date) for field in SUPPORTED_DATE_FIELDS),
        *((field, pl.Boolean) for field in SUPPORTED_BOOL_FIELDS),
    ]
    for field, dtype in specs:
        if field not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(field))
    return frame.select(
        [
            "symbol",
            "date",
            *SUPPORTED_FLOAT_FIELDS,
            *SUPPORTED_STRING_FIELDS,
            *SUPPORTED_DATE_FIELDS,
            *SUPPORTED_BOOL_FIELDS,
        ]
    )


def _field_manifest(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    total = frame.height
    source_by_field = {
        "pe_ttm": ("valuation_daily", "ratio"),
        "pb": ("valuation_daily", "ratio"),
        "ps_ttm": ("valuation_daily", "ratio"),
        "dv_ttm": ("corporate_actions/stock_dividends.parquet", "percent"),
        "turnover_rate_f": ("kline_daily_enriched.turnover_rate", "percent"),
        "roa": ("financials/income + financials/balance_sheet", "percent"),
        "assets_yoy": ("financials/balance_sheet", "percent"),
        "listing_date": ("instruments.instruments.parquet", "date"),
        "margin_balance": ("ext_tdx_margin.margin_balance_10k", "CNY"),
        "short_balance": ("ext_tdx_margin.short_balance_10k", "CNY"),
        "top_list_flag": ("ext_kpl_lhb", "bool"),
        "csi300_member": ("index_membership_history", "bool"),
        "csi500_member": ("index_membership_history", "bool"),
        "sse50_member": ("unavailable", "bool"),
        "stock_basic": ("instruments.instruments.parquet", "name"),
        "name": ("instruments.instruments.parquet", "name"),
        "listing_date": ("instruments.instruments.parquet", "date"),
        "namechange": ("instrument_name_history/part.parquet", "name"),
        "main_net": ("ext_kpl_funds", "source_defined"),
        "main_buy": ("ext_kpl_funds", "source_defined"),
        "main_sell": ("ext_kpl_funds", "source_defined"),
        "holding_amount": ("ext_kpl_northbound_stock", "source_defined"),
        "holding_shares": ("ext_kpl_northbound_stock", "source_defined"),
        "holding_ratio": ("ext_kpl_northbound_stock", "source_defined"),
        "market_ratio": ("ext_kpl_northbound_stock", "source_defined"),
        "shareholder_change_pct": ("ext_kpl_shareholder_counts", "source_defined"),
        "float_holding_ratio": ("ext_kpl_shareholder_counts", "source_defined"),
        "chip_concentration": ("ext_kpl_shareholder_counts", "source_defined"),
        "buy_in_amount": ("ext_kpl_lhb", "source_defined"),
        "listings_count": ("ext_kpl_lhb", "source_defined"),
        "turnover": ("ext_kpl_lhb", "source_defined"),
        "turnover_pct": ("ext_kpl_lhb", "source_defined"),
        "forecast_type": ("ext_tdx_forecast", "source_defined"),
        "summary": ("ext_tdx_forecast", "source_defined"),
        "net_profit_low_10k": ("ext_tdx_forecast", "10k CNY"),
        "net_profit_high_10k": ("ext_tdx_forecast", "10k CNY"),
        "net_profit_yoy_low_pct": ("ext_tdx_forecast", "percent"),
        "net_profit_yoy_high_pct": ("ext_tdx_forecast", "percent"),
    }
    for field in frame.columns:
        non_null = frame.filter(pl.col(field).is_not_null()).height
        dates = frame.filter(pl.col(field).is_not_null()).get_column("date") if non_null else pl.Series("date", [], dtype=pl.Date)
        source, unit = source_by_field.get(field, ("local_extension", "source_defined"))
        result[field] = {
            "source": source,
            "unit": unit,
            "row_count": non_null,
            "total_rows": total,
            "null_rate": (1 - non_null / total) if total else 1.0,
            "earliest_date": str(dates.min()) if non_null else None,
            "latest_date": str(dates.max()) if non_null else None,
        }
        if field == "sse50_member":
            result[field]["unavailable_reason"] = "当前没有可靠的 SSE50 历史成员表"
        elif non_null == 0:
            result[field]["unavailable_reason"] = "请求日期范围内没有可用原始行"
    for field, reason in UNSUPPORTED_FIELDS.items():
        result[field] = {
            "source": "unavailable",
            "unit": "unknown",
            "row_count": 0,
            "total_rows": total,
            "null_rate": 1.0,
            "earliest_date": None,
            "latest_date": None,
            "unavailable_reason": reason,
        }
    return result


def _minute_coverage(data_dir: Path, start: date, end: date, symbols: set[str] | None) -> dict[str, Any]:
    for name in ("kline_minute", "kline_minute_enriched"):
        root = data_dir / name
        all_files = _parquet_files(root)
        source = _scan_files(_date_partition_files(root, start, end))
        if source is None:
            continue
        schema = source.collect_schema()
        date_column = "datetime" if "datetime" in schema else "date" if "date" in schema else None
        if date_column is None or "symbol" not in schema:
            continue
        selected = source.select(["symbol", date_column]).collect(engine="streaming")
        if date_column == "datetime":
            selected = selected.with_columns(pl.col(date_column).cast(pl.Datetime, strict=False))
            date_values = selected[date_column].dt.date()
        else:
            selected = selected.with_columns(_date_expr(selected, date_column).alias(date_column))
            date_values = selected[date_column]
        selected = selected.with_columns(date_values.alias("_date")).filter((pl.col("_date") >= start) & (pl.col("_date") <= end))
        if symbols:
            selected = selected.filter(pl.col("symbol").is_in(sorted(symbols)))
        return {
            "available": True,
            "dataset": name,
            "rows": selected.height,
            "symbols": selected["symbol"].n_unique() if not selected.is_empty() else 0,
            "earliest_date": str(selected["_date"].min()) if not selected.is_empty() else None,
            "latest_date": str(selected["_date"].max()) if not selected.is_empty() else None,
            "complete_symbol_count": None,
            "i_fields_generated": False,
            "dataset_files": len(all_files),
        }
    return {
        "available": False,
        "dataset": None,
        "rows": 0,
        "symbols": 0,
        "earliest_date": None,
        "latest_date": None,
        "complete_symbol_count": 0,
        "i_fields_generated": False,
        "unavailable_reason": "没有本地分钟表；不使用日线或零成交行伪造 i_*",
    }


def export_factor_inputs(
    data_dir: Path,
    start: date,
    end: date,
    *,
    symbols: Iterable[str] | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    if end < start:
        raise ValueError("end must be on or after start")
    normalized_symbols = {str(symbol).strip().upper() for symbol in symbols or () if str(symbol).strip()}
    base = _read_daily(data_dir, start, end, normalized_symbols or None)
    if base.is_empty():
        base = pl.DataFrame({"symbol": pl.Series([], dtype=pl.String), "date": pl.Series([], dtype=pl.Date)})
    base = base.join(
        _read_valuation(data_dir, start, end, normalized_symbols or None),
        on=["symbol", "date"],
        how="left",
    ) if not base.is_empty() else base
    if "turnover_rate" in base.columns:
        base = base.with_columns(pl.col("turnover_rate").alias("turnover_rate_f"))
    base = _dividend_yield(base, data_dir)
    base = _join_financials(base, data_dir, normalized_symbols or None)
    base = _join_instruments(base, data_dir)
    base = _join_name_history(base, data_dir)
    base = _join_extensions(base, data_dir, start, end, normalized_symbols or None)
    base = _join_index_flags(base, data_dir, start, end, normalized_symbols or None)
    output = _ensure_output_columns(base).sort(["symbol", "date"])

    store = ExtConfigStore(data_dir)
    store.upsert(_factor_config())
    output_root = data_dir / OUTPUT_ROOT
    output_root.mkdir(parents=True, exist_ok=True)
    partitions: list[str] = []
    for day in output["date"].unique().sort().to_list():
        partition = output_root / f"date={day}" / "part.parquet"
        partition.parent.mkdir(parents=True, exist_ok=True)
        temporary = partition.with_name(f".{partition.name}.{uuid4().hex}.tmp")
        try:
            output.filter(pl.col("date") == day).write_parquet(temporary)
            os.replace(temporary, partition)
        finally:
            if temporary.exists():
                temporary.unlink()
        partitions.append(str(partition.relative_to(data_dir)))

    logical_snapshot = run_id or f"{start.isoformat()}_{end.isoformat()}"
    manifest = {
        "source": "factor_inputs_export",
        "dataset": TABLE_ID,
        "logical_snapshot": logical_snapshot,
        "status": "published",
        "schema_version": 1,
        "date_range": {"start": start.isoformat(), "end": end.isoformat()},
        "symbols_requested": sorted(normalized_symbols),
        "rows": output.height,
        "symbols": output["symbol"].n_unique() if not output.is_empty() else 0,
        "partitions": partitions,
        "fields": _field_manifest(output),
        "unsupported_fields": UNSUPPORTED_FIELDS,
        "minute_coverage": _minute_coverage(data_dir, start, end, normalized_symbols or None),
        "factor_usage": FACTOR_USAGE,
    }
    update_ingestion_manifest(
        data_dir,
        "factor_inputs_export",
        TABLE_ID,
        logical_snapshot,
        **{
            key: value
            for key, value in manifest.items()
            if key not in {"source", "dataset", "logical_snapshot"}
        },
    )
    return manifest


def _date_argument(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _symbol_arguments(values: list[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    return tuple(symbol.strip() for value in values for symbol in value.split(",") if symbol.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="从本地 canonical/extension 数据导出 PIT 因子输入")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--start", type=_date_argument, required=True)
    parser.add_argument("--end", type=_date_argument, required=True)
    parser.add_argument("--symbols", action="append", help="逗号分隔的标的，可重复传入")
    parser.add_argument("--run-id")
    args = parser.parse_args()
    try:
        result = export_factor_inputs(
            args.data_dir.resolve(),
            args.start,
            args.end,
            symbols=_symbol_arguments(args.symbols),
            run_id=args.run_id,
        )
    except (OSError, ValueError) as exc:
        parser.exit(1, f"factor input export blocked: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
