"""自由策略历史回测/模拟盘的受管子进程入口。"""
from __future__ import annotations

import json
import logging
import math
import multiprocessing as mp
import queue
import shutil
import threading
import time as time_module
from bisect import bisect_left, insort
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from app.services.security_dimensions import (
    load_industry_dimensions,
    load_instrument_name_changes,
)

from .bars import Bar, group_bars
from .engine import FreeStrategyConfig, FreeStrategyEngine

logger = logging.getLogger(__name__)

MARKET_METADATA_CALENDAR_DAYS = 30
PERFORMANCE_SMALL_CAP_SOURCE_MARKER = 'STRATEGY_KIND = "performance_small_cap"'
PERFORMANCE_SMALL_CAP_REQUIRED_FINANCIAL_TABLES = (
    "income",
    "metrics",
    "balance_sheet",
)


@dataclass
class MarketData:
    daily: dict[tuple[str, date], dict[str, Any]] = field(default_factory=dict)
    daily_dates: dict[str, list[date]] = field(default_factory=dict)
    daily_bar_cache: dict[tuple[str, date], Bar] = field(default_factory=dict)
    loaded_daily_ranges: dict[str, list[tuple[date, date]]] = field(default_factory=dict)
    names: dict[str, str] = field(default_factory=dict)
    name_changes: dict[str, tuple[tuple[date, str, str], ...]] = field(default_factory=dict)
    previous_scale: dict[str, float] = field(default_factory=dict)
    previous_adjusted_close: dict[str, float] = field(default_factory=dict)
    cash_dividends: dict[tuple[str, date], float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.daily_dates and self.daily:
            for symbol, day in sorted(self.daily):
                self.daily_dates.setdefault(symbol, []).append(day)


def _set_daily_row(
    market: MarketData,
    symbol: str,
    day: date,
    row: dict[str, Any],
) -> None:
    key = (symbol, day)
    market.daily_bar_cache.pop(key, None)
    if key not in market.daily:
        dates = market.daily_dates.setdefault(symbol, [])
        if not dates or day > dates[-1]:
            dates.append(day)
        elif day not in dates:
            insort(dates, day)
    market.daily[key] = row


def _record_loaded_daily_range(
    market: MarketData,
    symbol: str,
    start: date,
    end: date,
) -> None:
    ranges = sorted([*market.loaded_daily_ranges.get(symbol, []), (start, end)])
    merged: list[tuple[date, date]] = []
    for range_start, range_end in ranges:
        if merged and range_start <= merged[-1][1] + timedelta(days=1):
            merged[-1] = (merged[-1][0], max(merged[-1][1], range_end))
        else:
            merged.append((range_start, range_end))
    market.loaded_daily_ranges[symbol] = merged


def _missing_daily_ranges(
    market: MarketData,
    symbol: str,
    start: date,
    end: date,
) -> list[tuple[date, date]]:
    cursor = start
    missing: list[tuple[date, date]] = []
    for range_start, range_end in market.loaded_daily_ranges.get(symbol, []):
        if range_end < cursor:
            continue
        if range_start > end:
            break
        if range_start > cursor:
            missing.append((cursor, min(end, range_start - timedelta(days=1))))
        cursor = max(cursor, range_end + timedelta(days=1))
        if cursor > end:
            break
    if cursor <= end:
        missing.append((cursor, end))
    return missing


def _merge_market_data(target: MarketData, source: MarketData) -> None:
    for (symbol, day), row in source.daily.items():
        _set_daily_row(target, symbol, day, row)
    target.names.update(source.names)
    target.name_changes.update(source.name_changes)
    target.cash_dividends.update(source.cash_dividends)
    for symbol, ranges in source.loaded_daily_ranges.items():
        for start, end in ranges:
            _record_loaded_daily_range(target, symbol, start, end)


def _previous_daily_row(
    market: MarketData,
    symbol: str,
    day: date,
) -> dict[str, Any]:
    dates = market.daily_dates.get(symbol, [])
    index = bisect_left(dates, day) - 1
    return market.daily.get((symbol, dates[index]), {}) if index >= 0 else {}


def _name_on(
    current_name: str,
    changes: tuple[tuple[date, str, str], ...],
    day: date,
) -> str:
    for change_date, before_name, after_name in changes:
        if day < change_date:
            return before_name or current_name
        current_name = after_name or current_name
    return current_name


def _instrument_records(
    repo: Any,
    asset_type: str,
    timeframe: str,
    start: date | None = None,
    end: date | None = None,
) -> list[dict[str, Any]]:
    get_instruments = getattr(repo, "get_instruments_asset", None)
    if not callable(get_instruments):
        return []
    frame = get_instruments(asset_type)
    if frame.is_empty() or "symbol" not in frame.columns:
        return []
    minute_symbols: set[str] | None = None
    get_minute_symbols = getattr(repo, "get_minute_symbols", None)
    if timeframe != "1d" and callable(get_minute_symbols):
        minute_symbols = get_minute_symbols(asset_type, start, end)
    name_changes = load_instrument_name_changes(repo) if asset_type == "stock" else {}
    industries = load_industry_dimensions(repo) if asset_type == "stock" else {}
    records = []
    for raw in frame.iter_rows(named=True):
        item = dict(raw)
        item["symbol"] = str(item["symbol"])
        item["asset_type"] = str(item.get("asset_type") or asset_type).lower()
        item["has_minute"] = minute_symbols is None or item["symbol"] in minute_symbols
        item["name_changes"] = [
            {
                "date": change_date.isoformat(),
                "before": before_name,
                "after": after_name,
            }
            for change_date, before_name, after_name in name_changes.get(item["symbol"], ())
        ]
        if asset_type == "stock":
            industry = industries.get(item["symbol"], {})
            item["industry_sw"] = industry.get("industry_sw", "")
            item["industry_tdx"] = industry.get("industry_tdx", "")
        records.append(item)
    return records


def _date_column_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    dtype = frame.schema.get(column)
    if dtype == pl.Date:
        return pl.col(column)
    if dtype == pl.Datetime:
        return pl.col(column).dt.date()
    return pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, strict=False)


def _latest_announced_records(
    data_dir: Path,
    table: str,
    symbols: list[str],
    cutoff: date,
) -> dict[str, dict[str, Any]]:
    from app.services.financial_sync import get_financial_df

    frame = get_financial_df(data_dir, table)
    if frame.is_empty() or "symbol" not in frame.columns or not symbols:
        return {}
    date_column = "announce_date" if "announce_date" in frame.columns else "period_end"
    if date_column not in frame.columns:
        return {}
    period_expr = (
        _date_column_expr(frame, "period_end")
        if "period_end" in frame.columns else pl.lit(None, dtype=pl.Date)
    )
    frame = (
        frame
        .filter(pl.col("symbol").is_in(symbols))
        .with_columns([
            _date_column_expr(frame, date_column).alias("_available_date"),
            period_expr.alias("_period_date"),
        ])
        .filter(
            pl.col("_available_date").is_not_null()
            & (pl.col("_available_date") <= cutoff)
        )
        .sort(["symbol", "_available_date", "_period_date"], nulls_last=True)
    )
    if frame.is_empty():
        return {}
    return {
        str(row["symbol"]): {
            key: value
            for key, value in row.items()
            if key not in {"_available_date", "_period_date"}
        }
        for row in frame.group_by("symbol", maintain_order=True).tail(1).iter_rows(named=True)
    }


def _as_float(row: dict[str, Any], *columns: str) -> float | None:
    for column in columns:
        value = row.get(column)
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric):
            return numeric
    return None


def _load_financial_snapshot(
    data_dir: Path,
    symbols: list[str],
    cutoff: date,
) -> dict[str, dict[str, Any]]:
    symbols = list(dict.fromkeys(symbols))
    income = _latest_announced_records(data_dir, "income", symbols, cutoff)
    metrics = _latest_announced_records(data_dir, "metrics", symbols, cutoff)
    balance = _latest_announced_records(data_dir, "balance_sheet", symbols, cutoff)
    result: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        income_row = income.get(symbol, {})
        metrics_row = metrics.get(symbol, {})
        balance_row = balance.get(symbol, {})
        revenue = _as_float(income_row, "revenue", "operating_revenue")
        net_income = _as_float(income_row, "net_income")
        attributable = _as_float(
            income_row,
            "net_income_attributable",
            "np_parent_company_owners",
        )
        roe = _as_float(metrics_row, "roe", "roe_diluted")
        roa = _as_float(metrics_row, "roa")
        if roa is None:
            assets = _as_float(balance_row, "total_assets")
            if net_income is not None and assets and assets > 0:
                roa = net_income / assets * 100
        rows = [row for row in (income_row, metrics_row, balance_row) if row]
        if not rows:
            continue
        result[symbol] = {
            "revenue": revenue,
            "net_income": net_income,
            "net_income_attributable": attributable,
            "roe": roe,
            "roa": roa,
            "income_period_end": income_row.get("period_end"),
            "income_announce_date": income_row.get("announce_date"),
            "metrics_period_end": metrics_row.get("period_end"),
            "metrics_announce_date": metrics_row.get("announce_date"),
        }
    return result


def _load_dividend_ratio_ranked(
    repo: Any,
    data_dir: Path,
    symbols: list[str],
    previous_date: date,
) -> list[str]:
    """Return the original 260-session dividend-yield top quartile without Bar expansion."""
    if not symbols:
        return []
    get_batch = getattr(repo, "get_daily_asset_batch", None)
    if not callable(get_batch):
        return []
    start = previous_date - timedelta(days=260 * 2 + 14)
    columns = ["symbol", "date", "close", "raw_close", "total_shares"]
    frame = get_batch("stock", symbols, start, previous_date, columns)
    required = {"symbol", "date", "close", "total_shares"}
    if frame.is_empty() or not required.issubset(frame.columns):
        return []
    frame = (
        frame
        .filter(pl.col("date") <= previous_date)
        .sort(["symbol", "date"])
        .group_by("symbol", maintain_order=True)
        .tail(260)
        .filter(pl.col("date") >= previous_date - timedelta(days=366))
    )
    if frame.is_empty():
        return []
    from app.services.stock_dividends import load_record_date_cash_dividends

    dividends = [
        {"symbol": symbol, "date": day, "cash_dividend": cash}
        for (symbol, day), cash in load_record_date_cash_dividends(data_dir).items()
        if symbol in symbols and start <= day <= previous_date
    ]
    dividend_frame = pl.DataFrame(
        dividends,
        schema={"symbol": pl.String, "date": pl.Date, "cash_dividend": pl.Float64},
    )
    values = frame.join(dividend_frame, on=["symbol", "date"], how="left").with_columns(
        pl.col("cash_dividend").fill_null(0.0),
        pl.coalesce([pl.col("raw_close"), pl.col("close")]).alias("_price"),
        pl.col("total_shares").shift(1).over("symbol").alias("_shares"),
    )
    dividend = values.group_by("symbol").agg(
        (pl.col("cash_dividend") * pl.col("_shares")).sum().alias("_dividend")
    )
    latest = values.group_by("symbol", maintain_order=True).tail(1).select(
        "symbol",
        (pl.col("_price") * pl.col("_shares")).alias("_market_cap"),
    )
    ranked = (
        dividend
        .join(latest, on="symbol", how="inner")
        .filter(
            pl.col("_dividend").is_finite()
            & (pl.col("_dividend") > 0)
            & pl.col("_market_cap").is_finite()
            & (pl.col("_market_cap") > 0)
        )
        .with_columns((pl.col("_dividend") / pl.col("_market_cap")).alias("_ratio"))
        .sort(["_ratio", "symbol"], descending=[True, False])
    )
    return ranked["symbol"].to_list()[: int(ranked.height * 0.25)]


def _load_smallcap_index_value(
    repo: Any,
    symbols: list[str],
    previous_date: date,
) -> float | None:
    if not symbols:
        return None
    get_batch = getattr(repo, "get_daily_asset_batch", None)
    if not callable(get_batch):
        return None
    frame = get_batch(
        "stock",
        symbols,
        previous_date - timedelta(days=16),
        previous_date,
        ["symbol", "date", "close", "raw_close", "total_shares"],
    )
    required = {"symbol", "date", "close", "total_shares"}
    if frame.is_empty() or not required.issubset(frame.columns):
        return None
    latest = (
        frame
        .filter(pl.col("date") <= previous_date)
        .sort(["symbol", "date"])
        .with_columns(pl.col("total_shares").shift(1).over("symbol").alias("_shares"))
        .group_by("symbol", maintain_order=True)
        .tail(1)
        .with_columns(
            (pl.coalesce([pl.col("raw_close"), pl.col("close")]) * pl.col("_shares")).alias("_market_cap")
        )
        .filter(pl.col("_market_cap").is_finite() & (pl.col("_market_cap") > 0))
        .sort(["_market_cap", "symbol"])
        .head(400)
    )
    if latest.is_empty():
        return None
    closes = latest.filter(pl.col("close") > 0)["close"]
    return round(float(closes.mean()), 4) if len(closes) else None


def _financial_coverage(
    data_dir: Path,
    table: str,
    cutoff: date,
) -> dict[str, Any]:
    from app.services.financial_sync import get_financial_df

    frame = get_financial_df(data_dir, table)
    if frame.is_empty() or "symbol" not in frame.columns:
        return {
            "table": table,
            "rows": 0,
            "symbols": 0,
            "earliest_available": None,
            "latest_available": None,
            "rows_before_cutoff": 0,
            "symbols_before_cutoff": 0,
            "latest_before_cutoff": None,
        }
    date_column = "announce_date" if "announce_date" in frame.columns else "period_end"
    if date_column not in frame.columns:
        return {
            "table": table,
            "rows": frame.height,
            "symbols": frame["symbol"].n_unique(),
            "earliest_available": None,
            "latest_available": None,
            "rows_before_cutoff": 0,
            "symbols_before_cutoff": 0,
            "latest_before_cutoff": None,
        }
    dated = (
        frame
        .with_columns(_date_column_expr(frame, date_column).alias("_available_date"))
        .filter(pl.col("_available_date").is_not_null())
    )
    if dated.is_empty():
        return {
            "table": table,
            "rows": frame.height,
            "symbols": frame["symbol"].n_unique(),
            "earliest_available": None,
            "latest_available": None,
            "rows_before_cutoff": 0,
            "symbols_before_cutoff": 0,
            "latest_before_cutoff": None,
        }
    available = dated.filter(pl.col("_available_date") <= cutoff)
    return {
        "table": table,
        "rows": frame.height,
        "symbols": frame["symbol"].n_unique(),
        "earliest_available": dated["_available_date"].min(),
        "latest_available": dated["_available_date"].max(),
        "rows_before_cutoff": available.height,
        "symbols_before_cutoff": (
            available["symbol"].n_unique() if not available.is_empty() else 0
        ),
        "latest_before_cutoff": (
            available["_available_date"].max() if not available.is_empty() else None
        ),
    }


def _assert_performance_small_cap_financial_coverage(
    data_dir: Path,
    start: date,
) -> None:
    coverage = [
        _financial_coverage(data_dir, table, start)
        for table in PERFORMANCE_SMALL_CAP_REQUIRED_FINANCIAL_TABLES
    ]
    missing = [item for item in coverage if int(item["rows_before_cutoff"]) <= 0]
    if not missing:
        return
    details = []
    for item in missing:
        earliest = item["earliest_available"]
        latest = item["latest_available"]
        details.append(
            f"{item['table']}(rows={item['rows']}, "
            f"earliest={earliest.isoformat() if earliest else 'none'}, "
            f"latest={latest.isoformat() if latest else 'none'})"
        )
    raise ValueError(
        "绩优小市值回测需要首个回测日前已公告的历史财务数据；"
        f"当前 start={start.isoformat()} 前缺少可用表: {', '.join(details)}。"
        "请先同步完整历史 financial 数据或配置支持 latest=false 的自定义 financial provider。"
    )


def _is_performance_small_cap_source(source: str) -> bool:
    return PERFORMANCE_SMALL_CAP_SOURCE_MARKER in source


def _load_market_data(
    repo: Any,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
) -> MarketData:
    market = MarketData()
    if asset_type == "stock":
        market.name_changes.update(load_instrument_name_changes(repo))
        data_dir = getattr(getattr(repo, "store", None), "data_dir", None)
        if data_dir is not None:
            from app.services.stock_dividends import load_cash_dividends
            market.cash_dividends = load_cash_dividends(data_dir)
    get_daily = getattr(repo, "get_daily_asset", None)
    if callable(get_daily):
        columns = [
            "date", "open", "high", "low", "close", "volume", "amount",
            "raw_close", "raw_high", "raw_low", "turnover_rate",
            "total_shares", "float_shares",
        ]
        get_batch = getattr(repo, "get_daily_asset_batch", None)
        frame = (
            get_batch(asset_type, symbols, start, end, ["symbol", *columns])
            if len(symbols) > 1 and callable(get_batch)
            else pl.DataFrame()
        )
        if not frame.is_empty() and "symbol" in frame.columns:
            for row in frame.iter_rows(named=True):
                symbol = str(row["symbol"])
                _set_daily_row(market, symbol, row["date"], dict(row))
        else:
            for symbol in symbols:
                frame = get_daily(asset_type, symbol, start, end, columns)
                for row in frame.iter_rows(named=True):
                    _set_daily_row(market, symbol, row["date"], dict(row))
        for symbol in symbols:
            _record_loaded_daily_range(market, symbol, start, end)
    get_instruments = getattr(repo, "get_instruments_asset", None)
    if callable(get_instruments):
        instruments = get_instruments(asset_type)
        if not instruments.is_empty() and {"symbol", "name"}.issubset(instruments.columns):
            market.names = {
                str(symbol): str(name or "")
                for symbol, name in instruments.select("symbol", "name").iter_rows()
            }
    return market


def _prepare_market_reference(
    repo: Any,
    engine: FreeStrategyEngine,
    start: date,
    end: date,
    asset_type: str,
    market: MarketData,
) -> dict[str, Any]:
    requested_bars = engine.market_history_requirements.get((asset_type, "1d"), 0)
    if not requested_bars:
        return {"enabled": False, "asset_type": None, "timeframe": None, "requested_bars": 0,
                "rows": 0, "symbols": 0, "start": None, "end": None}
    records = engine.context.instruments(asset_type)
    if not records:
        records = _instrument_records(repo, asset_type, "1d", start, end)
    symbols = [item["symbol"] for item in records]
    get_batch = getattr(repo, "get_daily_asset_batch", None)
    if not symbols or not callable(get_batch):
        raise ValueError("策略已声明全市场日线预热，但当前数据源不支持批量日K")
    load_start = start - timedelta(days=requested_bars * 2 + 14)
    columns = [
        "symbol", "date", "open", "high", "low", "close", "volume", "amount",
        "raw_close", "raw_high", "raw_low", "turnover_rate",
        "total_shares", "float_shares",
    ]
    frame = get_batch(asset_type, symbols, load_start, end, columns)
    if frame.is_empty():
        existing = engine._market_history_by_period.get("1d", {})
        existing_bars = [bar for symbol in symbols for bar in existing.get(symbol, [])]
        if existing_bars:
            dates = [bar.timestamp.date() for bar in existing_bars]
            return {
                "enabled": True,
                "asset_type": asset_type,
                "timeframe": "1d",
                "requested_bars": requested_bars,
                "rows": len(existing_bars),
                "symbols": len({bar.symbol for bar in existing_bars}),
                "start": min(dates).isoformat(),
                "end": max(dates).isoformat(),
            }
        raise ValueError("全市场日线预热没有可用数据")
    bars = []
    for row in frame.iter_rows(named=True):
        symbol = str(row["symbol"])
        day = row["date"]
        close = float(row["close"])
        raw_close = float(row.get("raw_close") or close)
        scale = raw_close / close if close > 0 else 1.0
        _set_daily_row(market, symbol, day, dict(row))
        bars.append(Bar(
            symbol=symbol,
            timestamp=datetime.combine(day, time(15, 0)),
            open=float(row["open"]), high=float(row["high"]),
            low=float(row["low"]), close=close,
            volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
            raw_open=float(row["open"]) * scale,
            raw_high=float(row.get("raw_high") or float(row["high"]) * scale),
            raw_low=float(row.get("raw_low") or float(row["low"]) * scale),
            raw_close=raw_close,
            turnover_rate=(
                float(row["turnover_rate"]) if row.get("turnover_rate") is not None else None
            ),
            total_shares=(
                float(row["total_shares"]) if row.get("total_shares") is not None else None
            ),
            float_shares=(
                float(row["float_shares"]) if row.get("float_shares") is not None else None
            ),
        ))
    market.names.update({item["symbol"]: str(item.get("name") or "") for item in records})
    engine.preload_market_history(bars, "1d")
    dates = [bar.timestamp.date() for bar in bars]
    return {
        "enabled": True,
        "asset_type": asset_type,
        "timeframe": "1d",
        "requested_bars": requested_bars,
        "rows": len(bars),
        "symbols": len({bar.symbol for bar in bars}),
        "start": min(dates).isoformat() if dates else None,
        "end": max(dates).isoformat() if dates else None,
    }


def _limit_pct(symbol: str, asset_type: str, name: str) -> float:
    if asset_type == "etf":
        return 0.10
    code = symbol.split(".", 1)[0]
    if "ST" in name.upper():
        return 0.05
    if symbol.endswith(".BJ"):
        return 0.30
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def _round_limit(value: float, asset_type: str) -> float:
    tick = Decimal("0.001") if asset_type == "etf" else Decimal("0.01")
    return float(Decimal(str(value)).quantize(tick, rounding=ROUND_HALF_UP))


def _split_ratio(
    previous_scale: float | None,
    current_scale: float,
    asset_type: str,
    previous_shares: float | None = None,
    current_shares: float | None = None,
) -> float:
    if previous_scale is None or current_scale <= 0:
        return 1.0
    observed = previous_scale / current_scale
    if asset_type == "stock" and previous_shares and current_shares:
        share_ratio = current_shares / previous_shares
        if share_ratio > 1.01 and abs(observed - share_ratio) / share_ratio <= 0.02:
            return round(share_ratio, 6)
    if asset_type == "etf":
        nearest = round(observed)
        if nearest >= 2 and abs(observed - nearest) / nearest <= 0.02:
            return float(nearest)
    return 1.0


def _cash_dividend(
    previous_scale: float | None,
    current_scale: float,
    previous_raw_close: float,
    previous_adjusted_close: float,
    split_ratio: float,
) -> float:
    if (
        previous_scale is None
        or current_scale <= 0
        or math.isclose(previous_scale, current_scale, rel_tol=0.0, abs_tol=1e-6)
    ):
        return 0.0
    inferred = previous_raw_close - previous_adjusted_close * current_scale * split_ratio
    if inferred <= 0:
        return 0.0
    return float(Decimal(str(inferred)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _observe_daily_price(
    market: MarketData,
    symbol: str,
    adjusted_close: float,
    raw_close: float,
    asset_type: str,
    day: date,
) -> dict[str, float | None]:
    scale = raw_close / adjusted_close if adjusted_close > 0 and raw_close > 0 else 1.0
    split_ratio = _split_ratio(market.previous_scale.get(symbol), scale, asset_type)
    previous_close = market.previous_adjusted_close.get(symbol)
    reference = previous_close * scale if previous_close is not None else None
    market.previous_scale[symbol] = scale
    market.previous_adjusted_close[symbol] = adjusted_close
    pct = _limit_pct(
        symbol,
        asset_type,
        _name_on(
            market.names.get(symbol, ""),
            market.name_changes.get(symbol, ()),
            day,
        ),
    )
    return {
        "scale": scale,
        "split_ratio": split_ratio,
        "previous_close": reference,
        "limit_up": _round_limit(reference * (1 + pct), asset_type) if reference is not None else None,
        "limit_down": _round_limit(reference * (1 - pct), asset_type) if reference is not None else None,
    }


def _daily_bars(
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    market: MarketData,
) -> list[Bar]:
    rows: list[Bar] = []
    for symbol in symbols:
        for day in market.daily_dates.get(symbol, []):
            if not start <= day <= end:
                continue
            row = market.daily[(symbol, day)]
            close = float(row["close"])
            raw_close = float(row.get("raw_close") or close)
            observed = _observe_daily_price(market, symbol, close, raw_close, asset_type, day)
            scale = observed["scale"]
            rows.append(Bar(
                symbol=symbol,
                timestamp=datetime.combine(day, time(15, 0)),
                open=float(row["open"]), high=float(row["high"]),
                low=float(row["low"]), close=close,
                volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
                raw_open=float(row["open"]) * scale,
                raw_high=float(row.get("raw_high") or float(row["high"]) * scale),
                raw_low=float(row.get("raw_low") or float(row["low"]) * scale),
                raw_close=raw_close,
                tradable=float(row["open"]) > 0 and float(row["high"]) > 0,
                suspended=float(row["open"]) == 0 and float(row["high"]) == 0,
                limit_up=observed["limit_up"], limit_down=observed["limit_down"],
                split_ratio=observed["split_ratio"],
                previous_close=observed["previous_close"],
                turnover_rate=(
                    float(row["turnover_rate"])
                    if row.get("turnover_rate") is not None else None
                ),
                total_shares=(
                    float(row["total_shares"])
                    if row.get("total_shares") is not None else None
                ),
                float_shares=(
                    float(row["float_shares"])
                    if row.get("float_shares") is not None else None
                ),
            ))
    return sorted(rows, key=lambda bar: (bar.timestamp, bar.symbol))


def _minute_metadata(frame: Any, market: MarketData, asset_type: str) -> dict[tuple[str, date], dict[str, float]]:
    metadata: dict[tuple[str, date], dict[str, float]] = {}
    if frame.is_empty():
        return metadata
    closes = (
        frame.with_columns(pl.col("datetime").dt.date().alias("_date"))
        .sort(["symbol", "datetime"])
        .group_by(["symbol", "_date"], maintain_order=True)
        .agg(pl.col("close").last().alias("close"))
        .sort(["_date", "symbol"])
    )
    for row in closes.iter_rows(named=True):
        symbol = str(row["symbol"])
        day = row["_date"]
        adjusted_close = float(row["close"])
        daily = market.daily.get((symbol, day), {})
        raw_close = float(daily.get("raw_close") or daily.get("close") or adjusted_close)
        metadata[(symbol, day)] = _observe_daily_price(
            market, symbol, adjusted_close, raw_close, asset_type, day,
        )
    return metadata


def _inferred_historical_split(previous_raw_close: float, current_raw_close: float) -> float:
    if previous_raw_close <= 0 or current_raw_close <= 0:
        return 1.0
    observed = previous_raw_close / current_raw_close
    nearest = round(observed)
    if nearest >= 2 and abs(observed - nearest) / nearest <= 0.15:
        return float(nearest)
    return 1.0


def _aligned_warmup_bars(
    symbols: list[str],
    start: date,
    end: date,
    market: MarketData,
) -> list[Bar]:
    """按分钟K在回测起点的复权倍率，向前还原连续的日线预热序列。"""
    result: list[Bar] = []
    for symbol in symbols:
        rows = [
            (day, market.daily[(symbol, day)])
            for day in market.daily_dates.get(symbol, [])
            if start <= day <= end
        ]
        rows.sort(key=lambda item: item[0])
        if not rows:
            continue
        last_row = rows[-1][1]
        last_close = float(last_row["close"])
        last_raw_close = float(last_row.get("raw_close") or last_close)
        scale = market.previous_scale.get(symbol, last_raw_close / last_close if last_close > 0 else 1.0)
        scales: dict[date, float] = {}
        for index in range(len(rows) - 1, -1, -1):
            day, row = rows[index]
            scales[day] = scale
            if index > 0:
                previous = rows[index - 1][1]
                previous_raw = float(previous.get("raw_close") or previous["close"])
                current_raw = float(row.get("raw_close") or row["close"])
                scale *= _inferred_historical_split(previous_raw, current_raw)
        for day, row in rows:
            raw_close = float(row.get("raw_close") or row["close"])
            local_close = float(row["close"])
            local_scale = raw_close / local_close if local_close > 0 else 1.0
            aligned_scale = scales[day]
            raw_open = float(row["open"]) * local_scale
            raw_high = float(row.get("raw_high") or float(row["high"]) * local_scale)
            raw_low = float(row.get("raw_low") or float(row["low"]) * local_scale)
            result.append(Bar(
                symbol=symbol, timestamp=datetime.combine(day, time(15, 0)),
                open=raw_open / aligned_scale, high=raw_high / aligned_scale,
                low=raw_low / aligned_scale, close=raw_close / aligned_scale,
                volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
                raw_open=raw_open, raw_high=raw_high, raw_low=raw_low, raw_close=raw_close,
                turnover_rate=(
                    float(row["turnover_rate"])
                    if row.get("turnover_rate") is not None else None
                ),
                total_shares=(
                    float(row["total_shares"])
                    if row.get("total_shares") is not None else None
                ),
                float_shares=(
                    float(row["float_shares"])
                    if row.get("float_shares") is not None else None
                ),
            ))
    return sorted(result, key=lambda bar: (bar.timestamp, bar.symbol))


def _prime_minute_market_data(
    repo: Any,
    symbols: list[str],
    start: date,
    asset_type: str,
    market: MarketData,
) -> None:
    frame = repo.get_minute_range(symbols, start - timedelta(days=30), start - timedelta(days=1), asset_type)
    _minute_metadata(frame, market, asset_type)


def _resolve_symbols(engine: FreeStrategyEngine, payload: dict[str, Any]) -> tuple[list[str], str]:
    source_symbols = engine.universe
    if source_symbols:
        return source_symbols, "strategy_source"
    legacy_symbols = [str(symbol).strip() for symbol in payload.get("symbols", []) if str(symbol).strip()]
    if legacy_symbols:
        return legacy_symbols, "legacy_config"
    raise ValueError("策略源码未定义股票池，请在 initialize(context) 中调用 context.set_universe([...])")


def _read_rows(
    repo: Any,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    timeframe: str,
    *,
    require_all_symbols: bool = True,
    allow_empty: bool = False,
    market_data: MarketData | None = None,
    after: datetime | None = None,
    until: datetime | None = None,
) -> Iterable[Bar]:
    if timeframe == "1d":
        if market_data is not None:
            rows = _daily_bars(symbols, start, end, asset_type, market_data)
            if not rows and not allow_empty:
                raise ValueError("没有可用的日K数据，请先同步历史行情")
            return rows
        rows: list[Bar] = []
        for symbol in symbols:
            frame = repo.get_daily_asset(asset_type, symbol, start, end, [
                "date", "open", "high", "low", "close", "volume", "amount",
                "raw_close", "raw_high", "raw_low", "turnover_rate",
                "total_shares", "float_shares",
            ])
            for row in frame.iter_rows(named=True):
                close = float(row["close"])
                raw_close = float(row.get("raw_close") or close)
                scale = raw_close / close if close > 0 else 1.0
                rows.append(Bar(
                    symbol=symbol,
                    timestamp=datetime.combine(row["date"], time(15, 0)),
                    open=float(row["open"]), high=float(row["high"]),
                    low=float(row["low"]), close=close,
                    volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
                    raw_open=float(row["open"]) * scale,
                    raw_high=float(row.get("raw_high") or float(row["high"]) * scale),
                    raw_low=float(row.get("raw_low") or float(row["low"]) * scale),
                    raw_close=raw_close,
                    turnover_rate=(
                        float(row["turnover_rate"])
                        if row.get("turnover_rate") is not None else None
                    ),
                    total_shares=(
                        float(row["total_shares"])
                        if row.get("total_shares") is not None else None
                    ),
                    float_shares=(
                        float(row["float_shares"])
                        if row.get("float_shares") is not None else None
                    ),
                ))
        if not rows:
            raise ValueError("没有可用的日K数据，请先同步历史行情")
        return rows
    window = {}
    if after is not None:
        window["after"] = after
    if until is not None:
        window["until"] = until
    frame = repo.get_minute_range(symbols, start, end, asset_type, **window)
    if not frame.is_empty():
        frame = frame.drop_nulls(["open", "high", "low", "close"])
    if frame.is_empty():
        if allow_empty:
            return []
        asset_label = "ETF" if asset_type == "etf" else "股票"
        raise ValueError(
            f"没有可用的{asset_label}分钟K历史数据。请先同步{asset_label}分钟K，"
            "或将周期切换为 1d 后重新运行。"
        )
    found = set(frame["symbol"].unique().to_list())
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing and require_all_symbols:
        raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
    bar_metadata = _minute_metadata(frame, market_data, asset_type) if market_data is not None else {}

    def minute_rows() -> Iterable[Bar]:
        for row in frame.iter_rows(named=True):
            symbol = str(row["symbol"])
            day = row["datetime"].date()
            observed = bar_metadata.get((symbol, day), {})
            scale = float(observed.get("scale", 1.0))
            yield Bar(
                symbol=symbol, timestamp=row["datetime"],
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]),
                close=float(row["close"]), volume=float(row.get("volume") or 0),
                amount=float(row.get("amount") or 0),
                raw_open=float(row["open"]) * scale, raw_high=float(row["high"]) * scale,
                raw_low=float(row["low"]) * scale, raw_close=float(row["close"]) * scale,
                limit_up=observed.get("limit_up"), limit_down=observed.get("limit_down"),
                split_ratio=float(observed.get("split_ratio", 1.0)),
            )

    if timeframe == "1m":
        return minute_rows()
    return group_bars(minute_rows(), timeframe)


def _prepare_market_data(
    repo: Any,
    engine: FreeStrategyEngine,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    timeframe: str,
) -> tuple[MarketData, dict[str, Any]]:
    requested_bars = engine.history_requirements.get("1d", 0)
    lookback_days = max(
        MARKET_METADATA_CALENDAR_DAYS,
        requested_bars * 2 + 14 if requested_bars else 0,
    )
    load_start = start - timedelta(days=lookback_days)
    market_data = _load_market_data(repo, symbols, load_start, end, asset_type)
    _preload_tradable_dates(engine, market_data)
    references = {
        requested_asset: _prepare_market_reference(
            repo, engine, start, end, requested_asset, market_data,
        )
        for requested_asset, period in engine.market_history_requirements
        if period == "1d"
    }
    primary_reference = references.get(asset_type, {
        "enabled": False, "asset_type": None, "timeframe": None,
        "requested_bars": 0, "rows": 0, "symbols": 0, "start": None, "end": None,
    })
    engine.market_history_metadata = (
        {**primary_reference, "assets": references}
        if len(references) > 1 else primary_reference
    )
    if not any(start <= day <= end for _, day in market_data.daily):
        formal_market_data = _load_market_data(repo, symbols, start, end, asset_type)
        _merge_market_data(market_data, formal_market_data)

    warmup_end = start - timedelta(days=1)
    if timeframe == "1d":
        prior_bars = _daily_bars(symbols, load_start, warmup_end, asset_type, market_data)
    else:
        if engine.execution_mode == "full_bar":
            _prime_minute_market_data(repo, symbols, start, asset_type, market_data)
        prior_bars = (
            _aligned_warmup_bars(symbols, load_start, warmup_end, market_data)
            if requested_bars else []
        )

    selected: list[Bar] = []
    if requested_bars:
        by_symbol: dict[str, list[Bar]] = {}
        for bar in prior_bars:
            by_symbol.setdefault(bar.symbol, []).append(bar)
        selected = sorted(
            (
                bar
                for symbol_bars in by_symbol.values()
                for bar in symbol_bars[-requested_bars:]
            ),
            key=lambda bar: (bar.timestamp, bar.symbol),
        )
        engine.preload_history(selected, "1d")

    dates = [bar.timestamp.date() for bar in selected]
    return market_data, {
        "enabled": bool(requested_bars),
        "timeframe": "1d" if requested_bars else None,
        "requested_bars": requested_bars,
        "rows": len(selected),
        "symbols": len({bar.symbol for bar in selected}),
        "start": min(dates).isoformat() if dates else None,
        "end": max(dates).isoformat() if dates else None,
    }


def _preload_tradable_dates(engine: FreeStrategyEngine, market: MarketData) -> None:
    if not engine.config.allow_stale_fills:
        return
    engine.preload_tradable_dates(
        (symbol, day)
        for (symbol, day), row in market.daily.items()
        if float(row.get("open") or 0) > 0 and float(row.get("high") or 0) > 0
    )


def _scheduled_price_metadata(
    market: MarketData,
    symbol: str,
    day: date,
    asset_type: str,
) -> dict[str, float | None]:
    current = market.daily.get((symbol, day), {})
    close = float(current.get("close") or 0)
    raw_close = float(current.get("raw_close") or close or 0)
    scale = raw_close / close if close > 0 and raw_close > 0 else 1.0
    previous = _previous_daily_row(market, symbol, day)
    previous_close = float(previous.get("close") or 0)
    previous_raw_close = float(previous.get("raw_close") or previous_close or 0)
    previous_local_scale = (
        previous_raw_close / previous_close
        if previous_close > 0 and previous_raw_close > 0 else 1.0
    )
    previous_open = (
        float(previous.get("open") or 0) * previous_local_scale
        if previous else None
    )
    previous_scale = (
        previous_raw_close / previous_close
        if previous_close > 0 and previous_raw_close > 0 else None
    )
    previous_shares = float(previous.get("total_shares") or 0) or None
    current_shares = float(current.get("total_shares") or 0) or None
    reference = previous_close * scale if previous_close > 0 else None
    split_ratio = _split_ratio(
        previous_scale,
        scale,
        asset_type,
        previous_shares,
        current_shares,
    )
    pct = _limit_pct(
        symbol,
        asset_type,
        _name_on(
            market.names.get(symbol, ""),
            market.name_changes.get(symbol, ()),
            day,
        ),
    )
    return {
        "scale": scale,
        "split_ratio": split_ratio,
        "cash_dividend": market.cash_dividends.get((symbol, day), _cash_dividend(
            previous_scale,
            scale,
            previous_raw_close,
            previous_close,
            split_ratio,
        )),
        "previous_open": previous_open,
        "previous_close": reference,
        "turnover_rate": previous.get("turnover_rate"),
        "total_shares": previous.get("total_shares"),
        "float_shares": previous.get("float_shares"),
        "limit_up": _round_limit(reference * (1 + pct), asset_type) if reference is not None else None,
        "limit_down": _round_limit(reference * (1 - pct), asset_type) if reference is not None else None,
    }


def _scheduled_minute_bars(
    frame: Any,
    market: MarketData,
    asset_type: str,
) -> list[Bar]:
    if frame.is_empty():
        return []
    rows: list[Bar] = []
    for row in frame.sort(["datetime", "symbol"]).iter_rows(named=True):
        symbol = str(row["symbol"])
        timestamp = row["datetime"]
        metadata = _scheduled_price_metadata(market, symbol, timestamp.date(), asset_type)
        scale = float(metadata["scale"] or 1.0)
        rows.append(Bar(
            symbol=symbol,
            timestamp=timestamp,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row.get("volume") or 0),
            amount=float(row.get("amount") or 0),
            session_volume=(
                float(row["session_volume"])
                if row.get("session_volume") is not None else None
            ),
            raw_open=float(row["open"]) * scale,
            raw_high=float(row["high"]) * scale,
            raw_low=float(row["low"]) * scale,
            raw_close=float(row["close"]) * scale,
            limit_up=metadata["limit_up"],
            limit_down=metadata["limit_down"],
            split_ratio=float(metadata["split_ratio"] or 1.0),
            cash_dividend=float(metadata["cash_dividend"] or 0.0),
            previous_open=(
                float(metadata["previous_open"])
                if metadata["previous_open"] is not None else None
            ),
            previous_close=(
                float(metadata["previous_close"])
                if metadata["previous_close"] is not None else None
            ),
            turnover_rate=(
                float(metadata["turnover_rate"])
                if metadata["turnover_rate"] is not None else None
            ),
            total_shares=(
                float(metadata["total_shares"])
                if metadata["total_shares"] is not None else None
            ),
            float_shares=(
                float(metadata["float_shares"])
                if metadata["float_shares"] is not None else None
            ),
        ))
    return rows


def _scheduled_daily_bar(
    market: MarketData,
    symbol: str,
    day: date,
    asset_type: str,
) -> Bar | None:
    key = (symbol, day)
    cached = market.daily_bar_cache.get(key)
    if cached is not None:
        return cached
    row = market.daily.get(key)
    if row is None:
        return None
    metadata = _scheduled_price_metadata(market, symbol, day, asset_type)
    scale = float(metadata["scale"] or 1.0)
    bar = Bar(
        symbol=symbol,
        timestamp=datetime.combine(day, time(15, 0)),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row.get("volume") or 0),
        amount=float(row.get("amount") or 0),
        session_volume=float(row.get("volume") or 0),
        raw_open=float(row["open"]) * scale,
        raw_high=float(row.get("raw_high") or float(row["high"]) * scale),
        raw_low=float(row.get("raw_low") or float(row["low"]) * scale),
        raw_close=float(row.get("raw_close") or float(row["close"]) * scale),
        tradable=float(row["open"]) > 0 and float(row["high"]) > 0,
        suspended=float(row["open"]) == 0 and float(row["high"]) == 0,
        limit_up=metadata["limit_up"],
        limit_down=metadata["limit_down"],
        split_ratio=float(metadata["split_ratio"] or 1.0),
        cash_dividend=float(metadata["cash_dividend"] or 0.0),
        previous_open=(
            float(metadata["previous_open"])
            if metadata["previous_open"] is not None else None
        ),
        previous_close=(
            float(metadata["previous_close"])
            if metadata["previous_close"] is not None else None
        ),
        turnover_rate=(
            float(row["turnover_rate"])
            if row.get("turnover_rate") is not None else None
        ),
        total_shares=(
            float(row["total_shares"])
            if row.get("total_shares") is not None else None
        ),
        float_shares=(
            float(row["float_shares"])
            if row.get("float_shares") is not None else None
        ),
    )
    market.daily_bar_cache[key] = bar
    return bar


def _ensure_scheduled_market_data(
    repo: Any,
    market: MarketData,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
) -> None:
    requests: dict[tuple[date, date], list[str]] = {}
    for symbol in symbols:
        if (
            symbol not in market.loaded_daily_ranges
            and any(start <= day <= end for day in market.daily_dates.get(symbol, []))
        ):
            continue
        for missing_range in _missing_daily_ranges(market, symbol, start, end):
            requests.setdefault(missing_range, []).append(symbol)
    for (range_start, range_end), missing_symbols in sorted(requests.items()):
        loaded = _load_market_data(
            repo,
            missing_symbols,
            range_start,
            range_end,
            asset_type,
        )
        _merge_market_data(market, loaded)


def _scheduled_symbols(engine: FreeStrategyEngine, timestamp: datetime) -> list[str]:
    result: list[str] = []
    scoped = engine.scheduled_snapshot_symbols(timestamp)
    for symbol in [
        *(engine.universe if scoped is None else scoped),
        *engine.account.positions,
        engine.config.benchmark_symbol,
    ]:
        if symbol and symbol not in result:
            result.append(symbol)
    return result


def _scheduled_snapshot(
    repo: Any,
    engine: FreeStrategyEngine,
    market: MarketData,
    timestamp: datetime,
    asset_type: str,
    timeframe: str,
) -> list[Bar]:
    symbols = _scheduled_symbols(engine, timestamp)
    _ensure_scheduled_market_data(
        repo, market, symbols, timestamp.date() - timedelta(days=45), timestamp.date(), asset_type,
    )
    if timeframe == "1d" and timestamp.time() >= time(15, 0):
        return [
            bar for symbol in symbols
            if (bar := _scheduled_daily_bar(market, symbol, timestamp.date(), asset_type)) is not None
        ]

    get_range = getattr(repo, "get_minute_range", None)
    get_snapshot = getattr(repo, "get_minute_snapshot", None)
    if callable(get_snapshot):
        frame = get_snapshot(symbols, timestamp, asset_type)
    else:
        frame = get_range(symbols, timestamp.date(), timestamp.date(), asset_type)
    if not frame.is_empty():
        frame = frame.filter(pl.all_horizontal([
            pl.col(column).is_not_null() & pl.col(column).is_finite()
            for column in ("open", "high", "low", "close")
        ]))
    if not frame.is_empty() and not callable(get_snapshot):
        frame = (
            frame.filter(pl.col("datetime") <= timestamp)
            .with_columns(
                pl.col("volume").sum().over("symbol").alias("session_volume"),
            )
            .sort(["symbol", "datetime"])
            .group_by("symbol", maintain_order=True)
            .tail(1)
        )
    bars = _scheduled_minute_bars(frame, market, asset_type)
    if timeframe == "1d" and timestamp.time() < time(15, 0) and not bars:
        raise ValueError("1d 定时策略的盘中任务需要分钟K能力和对应历史数据")

    found = {bar.symbol for bar in bars}
    for symbol in symbols:
        if symbol in found:
            continue
        dates = market.daily_dates.get(symbol, [])
        previous_index = bisect_left(dates, timestamp.date()) - 1
        if previous_index < 0:
            continue
        previous = _scheduled_daily_bar(market, symbol, dates[previous_index], asset_type)
        if previous is not None:
            bars.append(replace(
                previous,
                timestamp=timestamp,
                tradable=False,
                suspended=True,
                split_ratio=1.0,
            ))
    return sorted(bars, key=lambda bar: (bar.timestamp, bar.symbol))


def _load_scheduled_history(
    repo: Any,
    market: MarketData,
    asset_type: str,
    symbol: str,
    count: int,
    timeframe: str,
    cutoff: datetime,
) -> list[Bar]:
    if count <= 0:
        return []
    if timeframe == "1d":
        start = cutoff.date() - timedelta(days=count * 2 + 14)
        _ensure_scheduled_market_data(repo, market, [symbol], start, cutoff.date(), asset_type)
        include_today = cutoff.time() >= time(15, 0)
        days = [
            row_day for row_day in market.daily_dates.get(symbol, [])
            if start <= row_day <= cutoff.date()
            and (row_day < cutoff.date() or include_today)
        ]
        return [
            bar for day in days[-count:]
            if (bar := _scheduled_daily_bar(market, symbol, day, asset_type)) is not None
        ]
    minutes = int(timeframe[:-1])
    start = cutoff.date() - timedelta(days=max(7, count * minutes // 120 + 7))
    frame = repo.get_minute_range([symbol], start, cutoff.date(), asset_type)
    if frame.is_empty():
        return []
    frame = frame.filter(pl.col("datetime") <= cutoff)
    bars = _scheduled_minute_bars(frame, market, asset_type)
    return group_bars(bars, timeframe)[-count:]


def _load_scheduled_history_batch(
    repo: Any,
    market: MarketData,
    asset_type: str,
    symbols: list[str],
    count: int,
    timeframe: str,
    cutoff: datetime,
) -> dict[str, list[Bar]]:
    if count <= 0 or not symbols:
        return {}
    if timeframe != "1d":
        return {
            symbol: _load_scheduled_history(
                repo, market, asset_type, symbol, count, timeframe, cutoff,
            )
            for symbol in symbols
        }
    start = cutoff.date() - timedelta(days=count * 2 + 14)
    _ensure_scheduled_market_data(
        repo, market, symbols, start, cutoff.date(), asset_type,
    )
    include_today = cutoff.time() >= time(15, 0)
    result: dict[str, list[Bar]] = {}
    for symbol in symbols:
        days = [
            day for day in market.daily_dates.get(symbol, [])
            if start <= day <= cutoff.date()
            and (day < cutoff.date() or include_today)
        ][-count:]
        result[symbol] = [
            bar for day in days
            if (bar := _scheduled_daily_bar(market, symbol, day, asset_type)) is not None
        ]
    return result


def _next_period_after(timestamp: datetime, timeframe: str) -> datetime:
    if timeframe == "1m" or timeframe == "1d":
        return timestamp
    minutes = int(timeframe[:-1])
    current = timestamp.time()
    if current < time(9, 30):
        boundary = timestamp.replace(hour=9, minute=30, second=0, microsecond=0)
    elif time(11, 30) <= current < time(13, 0):
        boundary = timestamp.replace(hour=13, minute=0, second=0, microsecond=0)
    elif current >= time(15, 0):
        boundary = timestamp
    else:
        session_hour, session_minute = (13, 0) if current >= time(13, 0) else (9, 30)
        session = timestamp.replace(
            hour=session_hour, minute=session_minute, second=0, microsecond=0,
        )
        elapsed = int((timestamp - session).total_seconds() // 60)
        boundary = session + timedelta(minutes=(elapsed // minutes + 1) * minutes)
    return boundary - timedelta(microseconds=1)


def _process_scheduled_fills(
    repo: Any,
    engine: FreeStrategyEngine,
    market: MarketData,
    until: datetime,
    asset_type: str,
    timeframe: str,
) -> None:
    due_groups: dict[datetime, list[str]] = {}
    for order, due_at in engine.pending_orders:
        due_groups.setdefault(due_at, [])
        if order.symbol not in due_groups[due_at]:
            due_groups[due_at].append(order.symbol)
    pending_symbols = list(dict.fromkeys(
        symbol for symbols in due_groups.values() for symbol in symbols
    ))
    _ensure_scheduled_market_data(
        repo,
        market,
        pending_symbols,
        min(due_groups, default=until).date() - timedelta(days=45),
        until.date(),
        asset_type,
    )
    for due_at, symbols in sorted(due_groups.items()):
        if timeframe == "1d":
            candidates: list[Bar] = []
            for symbol in symbols:
                days = [
                    row_day for row_day in market.daily_dates.get(symbol, [])
                    if due_at.date() < row_day <= until.date()
                ]
                if not days or until < datetime.combine(days[0], time(9, 30)):
                    continue
                bar = _scheduled_daily_bar(market, symbol, days[0], asset_type)
                if bar is not None:
                    candidates.append(replace(bar, timestamp=datetime.combine(days[0], time(9, 30))))
        else:
            get_next = getattr(repo, "get_minute_next", None)
            if not callable(get_next):
                continue
            frame = get_next(symbols, _next_period_after(due_at, timeframe), until, asset_type)
            candidates = _scheduled_minute_bars(frame, market, asset_type)
        by_time: dict[datetime, list[Bar]] = {}
        for bar in candidates:
            by_time.setdefault(bar.timestamp, []).append(bar)
        for timestamp, bars in sorted(by_time.items()):
            engine.process_fill_event(timestamp, bars)


def advance_scheduled_session(
    repo: Any,
    engine: FreeStrategyEngine,
    market: MarketData,
    day: date,
    cutoff: datetime,
    asset_type: str,
    timeframe: str,
    *,
    finalize: bool = False,
) -> None:
    engine.begin_session(day)
    due_times = sorted({
        at for at, _, done in engine.context._scheduled
        if not done and datetime.combine(day, time.fromisoformat(at)) <= cutoff
    })
    for at in due_times:
        timestamp = datetime.combine(day, time.fromisoformat(at))
        _process_scheduled_fills(repo, engine, market, timestamp, asset_type, timeframe)
        snapshot = _scheduled_snapshot(repo, engine, market, timestamp, asset_type, timeframe)
        engine.run_scheduled_event(timestamp, snapshot)
    _process_scheduled_fills(repo, engine, market, cutoff, asset_type, timeframe)
    if finalize:
        closing_time = max(cutoff, datetime.combine(day, time(15, 0)))
        snapshot = _scheduled_snapshot(
            repo, engine, market, closing_time, asset_type, timeframe,
        )
        engine.update_scheduled_market(closing_time, snapshot)
        engine.finish_session()


def execute_backtest(payload: dict[str, Any], output: Any, callback_deadline: Any = None) -> None:
    try:
        from app.tickflow.repository import DataStore, KlineRepository
        output.put({"type": "progress", "message": "初始化策略并读取行情数据", "progress": 0.1})
        source = str(payload["source"])
        source_digest = sha256(source.encode("utf-8")).hexdigest()
        expected_digest = payload.get("strategy_source_sha256")
        if expected_digest is not None and expected_digest != source_digest:
            raise ValueError("回测源码指纹与任务声明不一致")
        if payload.get("run_dir"):
            snapshot_path = Path(payload["run_dir"]) / "strategy.py"
            if not snapshot_path.exists():
                raise ValueError("回测源码快照不存在")
            snapshot = snapshot_path.read_text(encoding="utf-8")
            if sha256(snapshot.encode("utf-8")).hexdigest() != source_digest:
                raise ValueError("回测源码快照与任务源码不一致")
            source = snapshot
        repo = KlineRepository(DataStore(Path(payload["data_dir"])))
        start, end = date.fromisoformat(payload["start"]), date.fromisoformat(payload["end"])
        if _is_performance_small_cap_source(source):
            _assert_performance_small_cap_financial_coverage(repo.store.data_dir, start)
        config = FreeStrategyConfig(**payload["config"])
        engine = FreeStrategyEngine(
            source,
            payload["timeframe"],
            config,
            instrument_loader=lambda mode: _instrument_records(
                repo,
                payload["asset_type"],
                "1d" if mode == "scheduled" else payload["timeframe"],
                start,
                end,
            ),
            callback_deadline=callback_deadline,
        )
        engine.set_run_window(start, end)
        engine.set_financial_snapshot_loader(
            lambda symbols, cutoff: _load_financial_snapshot(
                repo.store.data_dir,
                symbols,
                cutoff,
            )
        )
        engine.set_dividend_ratio_loader(
            lambda symbols, cutoff: _load_dividend_ratio_ranked(
                repo,
                repo.store.data_dir,
                symbols,
                cutoff,
            )
        )
        engine.set_smallcap_index_loader(
            lambda symbols, cutoff: _load_smallcap_index_value(repo, symbols, cutoff)
        )
        fund_nav_data: dict[str, Any] = {}
        if "unit_net_value" in engine.extra_history_requirements:
            from .fund_nav import prepare_fund_nav_data

            output.put({"type": "progress", "message": "准备 ETF 单位净值", "progress": 0.12})
            fund_nav_data = prepare_fund_nav_data(repo, engine, start, end)
        output.put({
            "type": "progress",
            "message": "已识别定时执行模式" if engine.execution_mode == "scheduled" else "已识别完整回放模式",
            "progress": 0.15,
            "execution_mode": engine.execution_mode,
        })
        symbols, universe_source = _resolve_symbols(engine, payload)
        if payload.get("checkpoint"):
            engine.restore_checkpoint(payload["checkpoint"])
        market_data, warmup_metadata = _prepare_market_data(
            repo, engine, symbols, start, end, payload["asset_type"], payload["timeframe"],
        )
        replayed_rows = 0
        first_bar: datetime | None = None
        last_bar: datetime | None = None
        symbols_seen: set[str] = set()
        requested_symbols = list(symbols)
        trading_days = 0
        if engine.execution_mode == "scheduled":
            output.put({
                "type": "progress",
                "message": f"按交易日执行定时任务（{', '.join(engine.scheduled_times)}）",
                "progress": 0.35,
                "execution_mode": engine.execution_mode,
            })
            engine.set_history_loader(lambda symbol, count, timeframe, cutoff: _load_scheduled_history(
                repo, market_data, payload["asset_type"], symbol, count, timeframe, cutoff,
            ))
            engine.set_history_batch_loader(
                lambda symbols, count, timeframe, cutoff: _load_scheduled_history_batch(
                    repo,
                    market_data,
                    payload["asset_type"],
                    symbols,
                    count,
                    timeframe,
                    cutoff,
                )
            )
            trading_dates = sorted({
                day for symbol, day in market_data.daily
                if symbol in symbols and start <= day <= end
            })
            if not trading_dates:
                raise ValueError("回测区间没有可用的交易日行情")
            last_schedule = max(time.fromisoformat(value) for value in engine.scheduled_times)
            session_cutoff = max(last_schedule, time(15, 0))
            for index, trading_day in enumerate(trading_dates, start=1):
                advance_scheduled_session(
                    repo,
                    engine,
                    market_data,
                    trading_day,
                    datetime.combine(trading_day, session_cutoff),
                    payload["asset_type"],
                    payload["timeframe"],
                    finalize=True,
                )
                for symbol in engine.universe:
                    if symbol not in requested_symbols:
                        requested_symbols.append(symbol)
                symbols_seen.update(engine._current_close_prices)
                event_start = datetime.combine(trading_day, time.fromisoformat(engine.scheduled_times[0]))
                event_end = datetime.combine(trading_day, session_cutoff)
                first_bar = event_start if first_bar is None else min(first_bar, event_start)
                last_bar = event_end
                if index % 20 == 0:
                    progress = min(0.9, 0.35 + 0.55 * index / len(trading_dates))
                    output.put({
                        "type": "progress",
                        "message": f"已执行 {index} 个交易日的定时任务",
                        "progress": progress,
                        "execution_mode": engine.execution_mode,
                    })
            trading_days = len(trading_dates)
            replayed_rows = engine.market_rows_consumed
            engine.state = engine.context.state.copy()
            result = engine.result()
        elif payload["timeframe"] == "1d":
            bars = _read_rows(repo, symbols, start, end, payload["asset_type"], payload["timeframe"], market_data=market_data)
            output.put({"type": "progress", "message": f"回放 {len(bars)} 根日K", "progress": 0.35})
            replayed_rows = len(bars)
            symbols_seen.update(bar.symbol for bar in bars)
            trading_days = len({bar.timestamp.date() for bar in bars})
            first_bar = min(bar.timestamp for bar in bars)
            last_bar = max(bar.timestamp for bar in bars)
            result = engine.run(bars)
        else:
            output.put({"type": "progress", "message": "按交易日读取并回放分钟K", "progress": 0.35})
            cursor = start
            days_seen = 0
            days_with_bars = 0
            while cursor <= end:
                if cursor.weekday() < 5:
                    session_symbols = symbols
                    if engine.market_history_requirements:
                        if not engine.has_market_date(cursor):
                            cursor += timedelta(days=1)
                            days_seen += 1
                            continue
                        engine.begin_session(cursor)
                        session_symbols = engine.universe
                        for symbol in session_symbols:
                            if symbol not in requested_symbols:
                                requested_symbols.append(symbol)
                    bars = _read_rows(
                        repo, session_symbols, cursor, cursor,
                        payload["asset_type"], payload["timeframe"], require_all_symbols=False, allow_empty=True,
                        market_data=market_data,
                    )
                    rows = list(bars)
                    if rows:
                        replayed_rows += len(rows)
                        for bar in rows:
                            symbols_seen.add(bar.symbol)
                            first_bar = bar.timestamp if first_bar is None else min(first_bar, bar.timestamp)
                            last_bar = bar.timestamp if last_bar is None else max(last_bar, bar.timestamp)
                        engine.run(rows, return_result=False)
                        days_with_bars += 1
                days_seen += 1
                if days_seen % 20 == 0:
                    progress = min(0.9, 0.35 + 0.55 * days_seen / max((end - start).days + 1, 1))
                    output.put({"type": "progress", "message": f"已回放 {days_with_bars} 个交易日", "progress": progress})
                cursor += timedelta(days=1)
            if not days_with_bars:
                raise ValueError("没有可用的分钟K历史数据，请先同步后重试")
            get_minute_symbols = getattr(repo, "get_minute_symbols", None)
            stored_symbols = (
                set(get_minute_symbols(payload["asset_type"], start, end))
                if callable(get_minute_symbols) else symbols_seen
            )
            missing = [symbol for symbol in requested_symbols if symbol not in stored_symbols]
            if missing:
                raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
            engine.state = engine.context.state.copy()
            result = engine.result()
            trading_days = days_with_bars
        five_fortunes = result.get("state", {}).get("five_fortunes", {})
        five_fortunes_v2 = result.get("state", {}).get("five_fortunes_v2", {})
        strategy_metadata = five_fortunes or five_fortunes_v2
        if payload["timeframe"] == "1d" or engine.execution_mode == "scheduled":
            available_symbols = symbols_seen
        else:
            get_minute_symbols = getattr(repo, "get_minute_symbols", None)
            available_symbols = (
                set(get_minute_symbols(payload["asset_type"], start, end))
                if callable(get_minute_symbols) else symbols_seen
            )
        missing_symbols = [symbol for symbol in requested_symbols if symbol not in available_symbols]
        minute_table = "kline_etf_minute" if payload["asset_type"] == "etf" else "kline_minute"
        result["metadata"] = {
            "strategy_id": payload.get("strategy_id"), "strategy_name": payload.get("strategy_name"),
            "timeframe": payload["timeframe"], "asset_type": payload["asset_type"],
            "start": payload["start"], "end": payload["end"],
            "symbols": requested_symbols, "symbol_count": len(requested_symbols), "universe_source": universe_source,
            "data_days": len(result.get("daily_equity_curve", [])),
            "source_revision": payload.get("source_revision"),
            "strategy_source_sha256": source_digest,
            "resumed_from_checkpoint": bool(payload.get("checkpoint")),
            "warmup": warmup_metadata,
            "market_history": engine.market_history_metadata,
            "fund_nav": fund_nav_data,
            "execution_mode": engine.execution_mode,
            "scheduled_times": engine.scheduled_times,
            "callbacks_executed": engine.callbacks_executed,
            "market_rows_consumed": engine.market_rows_consumed,
            "nav_filter": strategy_metadata.get("nav_filter"),
            "excluded_no_minute_symbols": strategy_metadata.get("excluded_no_minute_symbols", []),
            "liquidity_scope": strategy_metadata.get("liquidity_scope"),
            "data_coverage": {
                "rows": replayed_rows,
                "first_bar": first_bar.isoformat() if first_bar else None,
                "last_bar": last_bar.isoformat() if last_bar else None,
                "trading_days": trading_days,
                "requested_symbols": requested_symbols,
                "seen_symbols": sorted(symbols_seen),
                "missing_symbols": missing_symbols,
                "configured_provider": payload.get("data_provider", "tickflow"),
                "storage": (
                    "event_snapshots"
                    if engine.execution_mode == "scheduled"
                    else "kline_daily" if payload["timeframe"] == "1d" else minute_table
                ),
            },
        }
        if payload.get("run_dir"):
            Path(payload["run_dir"]).mkdir(parents=True, exist_ok=True)
            (Path(payload["run_dir"]) / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        output.put({"type": "result", "result": result})
    except BaseException as exc:  # noqa: BLE001 - worker must report all script errors
        logger.exception("free strategy backtest failed")
        if payload.get("run_dir"):
            shutil.rmtree(Path(payload["run_dir"]), ignore_errors=True)
        output.put({"type": "error", "error": str(exc)})


def start_process(payload: dict[str, Any]) -> tuple[mp.Process, Any]:
    ctx = mp.get_context("spawn")
    output = ctx.Queue()
    callback_deadline = ctx.Value("d", 0.0)
    process = ctx.Process(
        target=execute_backtest,
        args=(payload, output, callback_deadline),
        daemon=True,
    )
    process.start()
    timeout = float(payload.get("config", {}).get("callback_timeout_seconds", 30.0))

    def watch_deadline() -> None:
        while process.is_alive():
            with callback_deadline.get_lock():
                deadline = float(callback_deadline.value)
            if deadline > 0 and time_module.monotonic() >= deadline:
                process.terminate()
                process.join(timeout=2)
                if payload.get("run_dir"):
                    shutil.rmtree(Path(payload["run_dir"]), ignore_errors=True)
                output.put({
                    "type": "error",
                    "error": f"策略执行超过 {timeout:g} 秒，已终止子进程",
                })
                return
            time_module.sleep(0.02)

    threading.Thread(
        target=watch_deadline,
        name=f"free-strategy-watchdog-{process.pid}",
        daemon=True,
    ).start()
    return process, output
