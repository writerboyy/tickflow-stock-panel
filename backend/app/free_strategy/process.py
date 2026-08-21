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
from bisect import bisect_left, bisect_right, insort
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from itertools import groupby
from pathlib import Path
from typing import Any, Iterable

import duckdb
import polars as pl

from app.services.security_dimensions import (
    load_industry_dimensions,
    load_instrument_name_changes,
)

from .bars import Bar, group_bars
from .entry_analysis import build_mainline_entry_analysis
from .engine import FreeStrategyConfig, FreeStrategyEngine, Quote
from .financial_pit import load_financial_periods
from .first_board_snapshot import configure_first_board_snapshot
from .industry import load_industry_history
from .mainline_snapshot import configure_mainline_snapshot
from .strong_momentum_snapshot import configure_strong_momentum_snapshot
from .four_mode_snapshot import configure_four_mode_snapshot
from .readiness import (
    ReadinessUnavailable,
    build_readiness_manifest,
    persist_readiness_report,
)
from .research_periods import build_research_periods

logger = logging.getLogger(__name__)

MARKET_METADATA_CALENDAR_DAYS = 30
PERFORMANCE_SMALL_CAP_SOURCE_MARKER = 'STRATEGY_KIND = "performance_small_cap"'
STYLE_LIQUIDITY_ENTRY_QUANTILE = 0.97
STYLE_LIQUIDITY_RECOVERY_QUANTILE = 0.70


class StyleLiquiditySignalCache:
    def __init__(
        self,
        repo: Any,
        start: date,
        end: date,
        *,
        entry_quantile: float = STYLE_LIQUIDITY_ENTRY_QUANTILE,
        recovery_quantile: float = STYLE_LIQUIDITY_RECOVERY_QUANTILE,
    ) -> None:
        self._repo = repo
        self._data_dir = Path(repo.store.data_dir)
        self._start = start
        self._end = end
        self._entry_quantile = entry_quantile
        self._recovery_quantile = recovery_quantile
        self._lock = threading.RLock()
        self._loaded = False
        self._loaded_through: date | None = None
        self._error: str | None = None
        self._entry_threshold: float | None = None
        self._recovery_threshold: float | None = None
        self._signals: dict[date, dict[str, Any]] = {}

    @staticmethod
    def _years_before(day: date, years: int) -> date:
        try:
            return day.replace(year=day.year - years)
        except ValueError:
            return day.replace(year=day.year - years, day=28)

    def _valid_symbols(self) -> pl.DataFrame:
        instruments = self._repo.get_instruments_asset("stock")
        required = {"symbol", "name"}
        if instruments.is_empty() or not required.issubset(instruments.columns):
            raise ValueError("大小盘成交占比择时缺少股票标的目录")
        symbol = pl.col("symbol")
        valid = (
            instruments
            .filter(~(
                symbol.str.starts_with("4")
                | symbol.str.starts_with("8")
                | symbol.str.starts_with("68")
            ))
            .filter(~pl.col("name").fill_null("").str.to_uppercase().str.contains("ST"))
            .filter(~pl.col("name").fill_null("").str.contains(r"\*|退"))
            .select("symbol")
            .unique()
        )
        if valid.is_empty():
            raise ValueError("大小盘成交占比择时没有有效股票标的")
        return valid

    def _load_metrics(
        self,
        load_start: date,
        output_start: date,
        end: date,
    ) -> pl.DataFrame:
        valid = self._valid_symbols()
        daily_glob = str(
            self._data_dir / "kline_daily" / "**" / "*.parquet"
        ).replace("'", "''")
        valuation_glob = str(
            self._data_dir / "valuation_daily" / "**" / "*.parquet"
        ).replace("'", "''")
        connection = duckdb.connect()
        connection.register("valid_symbols", valid.to_arrow())
        query = f"""
            WITH bars AS (
                SELECT
                    k.symbol,
                    k.date,
                    sum(k.amount) OVER (
                        PARTITION BY k.symbol ORDER BY k.date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS amount_5d,
                    count(*) OVER (
                        PARTITION BY k.symbol ORDER BY k.date
                        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                    ) AS bars_5d
                FROM read_parquet('{daily_glob}', hive_partitioning=false) AS k
                INNER JOIN valid_symbols AS s USING (symbol)
                WHERE k.date BETWEEN DATE '{load_start.isoformat()}'
                    AND DATE '{end.isoformat()}'
            ), ranked AS (
                SELECT
                    b.symbol,
                    b.date,
                    b.amount_5d,
                    row_number() OVER (
                        PARTITION BY b.date ORDER BY v.market_cap DESC, b.symbol
                    ) AS cap_rank,
                    count(*) OVER (PARTITION BY b.date) AS symbol_count
                FROM bars AS b
                INNER JOIN read_parquet(
                    '{valuation_glob}', hive_partitioning=false
                ) AS v USING (symbol, date)
                WHERE b.bars_5d = 5 AND b.amount_5d > 0 AND v.market_cap > 0
            ), daily AS (
                SELECT
                    date,
                    symbol_count,
                    sum(amount_5d) AS total_amount_5d,
                    sum(CASE
                        WHEN cap_rank <= floor(symbol_count * 0.1) THEN amount_5d
                        ELSE 0
                    END) AS large_amount_5d,
                    sum(CASE
                        WHEN cap_rank > symbol_count - floor(symbol_count * 0.1)
                            THEN amount_5d
                        ELSE 0
                    END) AS small_amount_5d
                FROM ranked
                GROUP BY date, symbol_count
            )
            SELECT
                date,
                100 * large_amount_5d / total_amount_5d AS large_ratio,
                100 * small_amount_5d / total_amount_5d AS small_ratio,
                large_amount_5d / small_amount_5d AS cap_ratio
            FROM daily
            WHERE date >= DATE '{output_start.isoformat()}'
            ORDER BY date
        """
        try:
            return connection.sql(query).pl()
        finally:
            connection.close()

    def _append_signals(self, metrics: pl.DataFrame, risk_off: bool) -> None:
        if self._entry_threshold is None or self._recovery_threshold is None:
            raise ValueError("大小盘成交占比择时阈值尚未初始化")
        for row in metrics.iter_rows(named=True):
            ratio = float(row["cap_ratio"])
            if not risk_off and ratio >= self._entry_threshold:
                risk_off = True
            elif risk_off and ratio <= self._recovery_threshold:
                risk_off = False
            self._signals[row["date"]] = {
                "available": True,
                "date": row["date"].isoformat(),
                "risk_off": risk_off,
                "large_ratio": float(row["large_ratio"]),
                "small_ratio": float(row["small_ratio"]),
                "cap_ratio": ratio,
                "entry_quantile": self._entry_quantile,
                "recovery_quantile": self._recovery_quantile,
                "entry_threshold": self._entry_threshold,
                "recovery_threshold": self._recovery_threshold,
            }

    def _load(self) -> None:
        history_start = self._years_before(self._start, 10)
        metrics = self._load_metrics(
            history_start - timedelta(days=MARKET_METADATA_CALENDAR_DAYS),
            history_start,
            self._end,
        )
        history = metrics.filter(pl.col("date") < self._start)
        period = metrics.filter(pl.col("date").is_between(self._start, self._end))
        if history.is_empty() or period.is_empty():
            raise ValueError("大小盘成交占比择时缺少十年历史或回测期数据")
        entry = history.select(
            pl.col("cap_ratio").quantile(self._entry_quantile)
        ).item()
        recovery = history.select(
            pl.col("cap_ratio").quantile(self._recovery_quantile)
        ).item()
        if entry is None or recovery is None:
            raise ValueError("大小盘成交占比择时无法计算历史分位")
        self._entry_threshold = float(entry)
        self._recovery_threshold = float(recovery)
        self._append_signals(period, False)
        self._loaded_through = period["date"].max()

    def _extend(self, cutoff: date) -> None:
        if self._loaded_through is None or cutoff <= self._loaded_through:
            return
        metrics = self._load_metrics(
            self._loaded_through - timedelta(days=MARKET_METADATA_CALENDAR_DAYS),
            self._loaded_through + timedelta(days=1),
            cutoff,
        )
        if metrics.is_empty():
            return
        previous = self._signals[max(self._signals)]
        self._append_signals(metrics, bool(previous["risk_off"]))
        self._loaded_through = metrics["date"].max()
        self._error = None

    def signal(self, cutoff: date) -> dict[str, Any] | None:
        if cutoff < self._start:
            return None
        with self._lock:
            if not self._loaded:
                try:
                    self._load()
                except Exception as exc:
                    self._error = str(exc)
                self._loaded = True
            if self._error is None and (
                self._loaded_through is None or cutoff > self._loaded_through
            ):
                try:
                    self._extend(cutoff)
                except Exception as exc:
                    self._error = str(exc)
            signal = self._signals.get(cutoff)
            if signal is not None:
                return dict(signal)
            return {
                "available": False,
                "date": cutoff.isoformat(),
                "risk_off": True,
                "reason": self._error or "大小盘成交占比择时缺少当日数据",
                "entry_quantile": self._entry_quantile,
                "recovery_quantile": self._recovery_quantile,
            }
SCHEDULED_OPENING_RETRY_MINUTES = 5


class ScheduledOpeningDataPending(RuntimeError):
    """实时纸盘缺少当前任务需要的实时行情，等待下一次时钟重试。"""


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
        listing_date = item.get("list_date") or item.get("listing_date")
        if isinstance(listing_date, date) and listing_date <= date(1970, 1, 1):
            continue
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


def _one_year_before(day: date) -> date:
    try:
        return day.replace(year=day.year - 1)
    except ValueError:
        return day.replace(year=day.year - 1, day=28)


def _latest_announced_records(
    data_dir: Path,
    table: str,
    symbols: list[str],
    cutoff: date,
) -> dict[str, dict[str, Any]]:
    return {
        symbol: rows[0]
        for symbol, rows in load_financial_periods(
            data_dir,
            table,
            symbols,
            cutoff,
        ).items()
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


def _load_valuation_market_caps(
    data_dir: Path,
    symbols: list[str],
    cutoff: date,
) -> dict[str, float]:
    from app.services.daily_valuation import load_latest_market_caps

    return load_latest_market_caps(data_dir, symbols, cutoff)


def _load_dividend_ratio_ranked(
    repo: Any,
    data_dir: Path,
    symbols: list[str],
    previous_date: date,
) -> list[str]:
    """Return the original one-calendar-year dividend-yield top quartile."""
    if not symbols:
        return []
    get_batch = getattr(repo, "get_daily_asset_batch", None)
    if not callable(get_batch):
        return []
    time0 = _one_year_before(previous_date)
    start = time0 - timedelta(days=45)
    columns = ["symbol", "date", "close", "raw_close", "total_shares"]
    frame = get_batch("stock", symbols, start, previous_date, columns)
    required = {"symbol", "date", "close", "total_shares"}
    if frame.is_empty() or not required.issubset(frame.columns):
        return []
    values = (
        frame
        .filter(pl.col("date") <= previous_date)
        .sort(["symbol", "date"])
        .with_columns(pl.col("total_shares").shift(1).over("symbol").alias("_shares"))
    )
    if values.is_empty():
        return []
    from app.services.stock_dividends import load_record_date_cash_dividends

    share_rows: dict[str, list[tuple[date, float]]] = {}
    for symbol, day, shares, same_day_shares in values.select(
        "symbol",
        "date",
        "_shares",
        "total_shares",
    ).iter_rows():
        usable_shares = shares if shares is not None else same_day_shares
        if isinstance(day, date) and usable_shares is not None and float(usable_shares) > 0:
            share_rows.setdefault(str(symbol), []).append((day, float(usable_shares)))
    symbol_set = set(symbols)
    dividend_totals: dict[str, float] = {}
    for (symbol, day), cash in load_record_date_cash_dividends(
        data_dir,
        as_of=previous_date,
    ).items():
        if symbol not in symbol_set or not (time0 <= day <= previous_date):
            continue
        rows = share_rows.get(symbol)
        if not rows:
            continue
        index = bisect_right(rows, (day, math.inf)) - 1
        if index < 0:
            continue
        dividend_totals[symbol] = dividend_totals.get(symbol, 0.0) + float(cash) * rows[index][1]
    dividend = pl.DataFrame(
        [{"symbol": symbol, "_dividend": value} for symbol, value in dividend_totals.items()],
        schema={"symbol": pl.String, "_dividend": pl.Float64},
    )
    if dividend.is_empty():
        return []
    valuation_caps = _load_valuation_market_caps(data_dir, symbols, previous_date)
    if not valuation_caps:
        return []
    valuation = pl.DataFrame(
        [{"symbol": symbol, "_market_cap": value} for symbol, value in valuation_caps.items()],
        schema={"symbol": pl.String, "_market_cap": pl.Float64},
    )
    ranked = (
        dividend
        .join(valuation, on="symbol", how="inner")
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
    data_dir: Path,
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
        ["symbol", "date", "close"],
    )
    required = {"symbol", "date", "close"}
    if frame.is_empty() or not required.issubset(frame.columns):
        return None
    latest = (
        frame
        .filter(pl.col("date") <= previous_date)
        .sort(["symbol", "date"])
        .group_by("symbol", maintain_order=True)
        .tail(1)
    )
    valuation_caps = _load_valuation_market_caps(data_dir, symbols, previous_date)
    if not valuation_caps:
        return None
    valuation = pl.DataFrame(
        [{"symbol": symbol, "_market_cap": value} for symbol, value in valuation_caps.items()],
        schema={"symbol": pl.String, "_market_cap": pl.Float64},
    )
    latest = (
        latest.join(valuation, on="symbol", how="inner")
        .filter(pl.col("_market_cap").is_finite() & (pl.col("_market_cap") > 0))
        .sort(["_market_cap", "symbol"])
        .head(400)
    )
    if latest.is_empty():
        return None
    closes = latest.filter(pl.col("close") > 0)["close"]
    return round(float(closes.mean()), 4) if len(closes) else None


def _is_performance_small_cap_source(source: str) -> bool:
    return PERFORMANCE_SMALL_CAP_SOURCE_MARKER in source


def configure_strategy_data_loaders(
    engine: FreeStrategyEngine,
    repo: Any,
    data_dir: Path,
    source: str,
    start: date,
    end: date,
) -> None:
    """Attach the point-in-time data capabilities shared by backtest and paper."""
    data_dir = Path(data_dir)
    engine.set_financial_snapshot_loader(
        lambda symbols, cutoff: _load_financial_snapshot(data_dir, symbols, cutoff)
    )
    engine.set_industry_history_loader(
        lambda symbols, cutoff, standard, level: load_industry_history(
            data_dir,
            symbols,
            cutoff,
            standard,
            level,
        ),
        partial_loader=lambda symbols, cutoff, standard, level: load_industry_history(
            data_dir,
            symbols,
            cutoff,
            standard,
            level,
            allow_missing=True,
        ),
    )
    engine.set_dividend_ratio_loader(
        lambda symbols, cutoff: _load_dividend_ratio_ranked(
            repo,
            data_dir,
            symbols,
            cutoff,
        )
    )
    engine.set_valuation_market_cap_loader(
        lambda symbols, cutoff: _load_valuation_market_caps(
            data_dir,
            symbols,
            cutoff,
        )
    )
    engine.set_smallcap_index_loader(
        lambda symbols, cutoff: _load_smallcap_index_value(
            repo,
            data_dir,
            symbols,
            cutoff,
        )
    )
    if _is_performance_small_cap_source(source):
        style_liquidity = StyleLiquiditySignalCache(repo, start, end)
        engine.set_style_liquidity_loader(style_liquidity.signal)


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


def _market_asset_type(repo: Any, symbol: str, default: str) -> str:
    resolver = getattr(repo, "resolve_asset_type", None)
    if callable(resolver):
        try:
            resolved = str(resolver(symbol) or default).lower()
        except Exception:  # noqa: BLE001
            return default
        if resolved in {"stock", "etf", "index"}:
            return resolved
    return default


def _market_symbols(engine: FreeStrategyEngine, symbols: list[str]) -> list[str]:
    benchmark = str(engine.config.benchmark_symbol or "").strip()
    return list(dict.fromkeys([*symbols, benchmark] if benchmark else symbols))


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
    second_precision: bool = False,
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
    if second_precision:
        get_tick_range = getattr(repo, "get_tick_range", None)
        legacy_second_range = getattr(repo, "get_second_range", None)
        if not callable(get_tick_range) and not callable(legacy_second_range):
            raise ValueError(
                "回测包含秒级定时点，但当前历史 provider 没有 tick/逐笔行情能力；"
                "请接入 tick 数据后再回测"
            )
        # Tick is the source of truth for explicit-second callbacks.  The
        # legacy method is only retained for custom repositories and old test
        # doubles; it never falls back to minute K.
        getter = get_tick_range if callable(get_tick_range) else legacy_second_range
        frame = getter(symbols, start, end, asset_type, **window)
        interval_label = "tick"
    else:
        frame = repo.get_minute_range(symbols, start, end, asset_type, **window)
        interval_label = "分钟"
    if second_precision and not frame.is_empty():
        columns = set(frame.columns)
        if "close" not in columns and "last_price" in columns:
            frame = frame.with_columns(pl.col("last_price").alias("close"))
            columns.add("close")
        missing_price_fields = [
            field for field in ("open", "high", "low") if field not in columns
        ]
        if missing_price_fields and "close" in columns:
            frame = frame.with_columns([
                pl.col("close").alias(field) for field in missing_price_fields
            ])
    if not frame.is_empty():
        frame = frame.drop_nulls(["open", "high", "low", "close"])
    if frame.is_empty():
        if allow_empty and not second_precision:
            return []
        asset_label = "ETF" if asset_type == "etf" else "股票"
        source_label = "秒级K历史/tick" if second_precision else f"{interval_label}K"
        raise ValueError(
            f"没有可用的{asset_label}{source_label}历史数据。"
            f"请先同步{asset_label}{source_label}，"
            "或将周期切换为 1d 后重新运行。"
        )
    found = set(frame["symbol"].unique().to_list())
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing and require_all_symbols:
        raise ValueError(f"{interval_label}历史缺少标的: {', '.join(missing[:8])}")
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
    *,
    include_benchmark: bool = False,
) -> tuple[MarketData, dict[str, Any]]:
    requested_bars = engine.history_requirements.get("1d", 0)
    lookback_days = max(
        MARKET_METADATA_CALENDAR_DAYS,
        requested_bars * 2 + 14 if requested_bars else 0,
    )
    load_start = start - timedelta(days=lookback_days)
    market_symbols = _market_symbols(engine, symbols) if include_benchmark else symbols
    market_data = MarketData()
    symbols_by_asset: dict[str, list[str]] = {}
    for symbol in market_symbols:
        symbols_by_asset.setdefault(_market_asset_type(repo, symbol, asset_type), []).append(symbol)
    for requested_asset, requested_symbols in symbols_by_asset.items():
        _merge_market_data(
            market_data,
            _load_market_data(repo, requested_symbols, load_start, end, requested_asset),
        )
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
        for requested_asset, requested_symbols in symbols_by_asset.items():
            _merge_market_data(
                market_data,
                _load_market_data(repo, requested_symbols, start, end, requested_asset),
            )

    warmup_end = start - timedelta(days=1)
    if timeframe == "1d":
        prior_bars = _daily_bars(market_symbols, load_start, warmup_end, asset_type, market_data)
    else:
        if engine.execution_mode == "full_bar":
            _prime_minute_market_data(repo, market_symbols, start, asset_type, market_data)
        prior_bars = (
            _aligned_warmup_bars(market_symbols, load_start, warmup_end, market_data)
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


def _prepare_dynamic_market_data(
    repo: Any,
    engine: FreeStrategyEngine,
    start: date,
    end: date,
    dynamic_cache: Any | None = None,
) -> tuple[MarketData, dict[str, Any]]:
    """动态股票池策略只预载基准，股票日线元数据按当日候选滚动加载。"""
    benchmark = str(engine.config.benchmark_symbol or "").strip()
    if not benchmark:
        raise ValueError("动态股票池策略必须配置基准指数")
    market = _load_market_data(
        repo,
        [benchmark],
        start - timedelta(days=45),
        end,
        _market_asset_type(repo, benchmark, "index"),
    )
    if engine.timeframe == "1d" and dynamic_cache is not None:
        candidate_symbols = list(dynamic_cache.all_symbols)
        if candidate_symbols:
            _merge_market_data(
                market,
                _load_market_data(
                    repo,
                    candidate_symbols,
                    start - timedelta(days=45),
                    end,
                    "stock",
                ),
            )
    dates = [
        day for (symbol, day) in market.daily
        if symbol == benchmark and start <= day <= end
    ]
    if not dates:
        raise ValueError(f"动态股票池策略基准 {benchmark} 在回测区间没有日线")
    engine.preload_market_history(
        _daily_bars([benchmark], min(dates), max(dates), "index", market),
        "1d",
    )
    return market, {
        "enabled": True,
        "timeframe": "1d",
        "mode": (
            "pit_mainline_snapshot"
            if engine.mainline_snapshot_requirement is not None
            else "pit_first_board_snapshot"
            if engine.limit_board_snapshot_requirement is not None
            else "pit_strong_momentum_snapshot"
            if engine.strong_momentum_snapshot_requirement is not None
            else "four_mode_pit_static_and_auction"
        ),
        "requested_bars": int(
            (
                engine.mainline_snapshot_requirement
                or engine.limit_board_snapshot_requirement
                or engine.strong_momentum_snapshot_requirement
                or engine.four_mode_snapshot_requirement
                or {}
            ).get("lookback_days", 60)
        ),
        "rows": len(dates),
        "symbols": 1,
        "start": min(dates).isoformat(),
        "end": max(dates).isoformat(),
    }


def _prime_dynamic_minute_metadata(
    market: MarketData,
    symbols: Iterable[str],
    day: date,
) -> None:
    """为首次进入候选池的股票建立前收盘和复权基线。"""
    for symbol in symbols:
        previous = _previous_daily_row(market, symbol, day)
        close = float(previous.get("close") or 0)
        raw_close = float(previous.get("raw_close") or close or 0)
        if close <= 0 or raw_close <= 0:
            continue
        market.previous_scale[symbol] = raw_close / close
        market.previous_adjusted_close[symbol] = close


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
    requests: dict[tuple[str, date, date], list[str]] = {}
    for symbol in symbols:
        if (
            symbol not in market.loaded_daily_ranges
            and any(start <= day <= end for day in market.daily_dates.get(symbol, []))
        ):
            continue
        for missing_range in _missing_daily_ranges(market, symbol, start, end):
            requested_asset = _market_asset_type(repo, symbol, asset_type)
            requests.setdefault((requested_asset, *missing_range), []).append(symbol)
    for (requested_asset, range_start, range_end), missing_symbols in sorted(requests.items()):
        loaded = _load_market_data(
            repo,
            missing_symbols,
            range_start,
            range_end,
            requested_asset,
        )
        _merge_market_data(market, loaded)


def _scheduled_symbols(engine: FreeStrategyEngine, timestamp: datetime) -> list[str]:
    result: list[str] = []
    scoped = engine.scheduled_snapshot_symbols(timestamp)
    held_symbols = [
        symbol
        for symbol, quantity in engine.account.positions.items()
        if float(quantity) > 0
    ]
    for symbol in [
        *(engine.universe if scoped is None else scoped),
        *held_symbols,
        engine.config.benchmark_symbol,
    ]:
        if symbol and symbol not in result:
            result.append(symbol)
    return result


def _scheduled_required_symbols(
    engine: FreeStrategyEngine,
    timestamp: datetime,
) -> list[str]:
    result: list[str] = []
    scoped = engine.scheduled_required_snapshot_symbols(timestamp)
    held_symbols = [
        symbol
        for symbol, quantity in engine.account.positions.items()
        if float(quantity) > 0
    ]
    # The benchmark is included in the visible snapshot for valuation, but it
    # is not a strategy input and therefore must not block a second-precision
    # callback when the provider has no benchmark tick history.
    for symbol in [*(engine.universe if scoped is None else scoped), *held_symbols]:
        if symbol and symbol not in result:
            result.append(symbol)
    return result


def _missing_snapshot_symbols(snapshot: Iterable[Bar], symbols: Iterable[str]) -> list[str]:
    found = {bar.symbol for bar in snapshot}
    return [symbol for symbol in symbols if symbol not in found]


def _missing_snapshot_text(symbols: list[str]) -> str:
    visible = ", ".join(symbols[:12])
    remainder = len(symbols) - 12
    return f"{visible}{f' 等 {len(symbols)} 只' if remainder > 0 else ''}"


def _bar_as_quote(bar: Bar) -> Quote:
    return Quote(
        symbol=bar.symbol,
        timestamp=bar.timestamp,
        last_price=bar.execution_price("close"),
        prev_close=bar.previous_close,
        open=bar.execution_price("open"),
        high=bar.execution_price("high"),
        low=bar.execution_price("low"),
        volume=float(bar.volume or 0),
        amount=float(bar.amount or 0),
        limit_up=bar.limit_up,
        limit_down=bar.limit_down,
        suspended=bar.suspended,
    )


def _live_daily_snapshot(rows: list[Bar], timestamp: datetime) -> list[Bar]:
    grouped: dict[str, list[Bar]] = {}
    for bar in rows:
        grouped.setdefault(bar.symbol, []).append(bar)
    result: list[Bar] = []
    for symbol, values in grouped.items():
        values.sort(key=lambda bar: bar.timestamp)
        result.append(replace(
            values[-1],
            timestamp=timestamp,
            open=values[0].open,
            high=max(bar.high for bar in values),
            low=min(bar.low for bar in values),
            close=values[-1].close,
            volume=sum(bar.volume for bar in values),
            amount=sum(bar.amount for bar in values),
            raw_open=values[0].raw_open,
            raw_high=max(bar.raw_high or bar.high for bar in values),
            raw_low=min(bar.raw_low or bar.low for bar in values),
            raw_close=values[-1].raw_close,
            session_volume=values[-1].session_volume,
            tradable=any(bar.tradable for bar in values),
            suspended=all(bar.suspended for bar in values),
        ))
    return sorted(result, key=lambda bar: (bar.timestamp, bar.symbol))


def _live_scheduled_snapshot(
    live_bars: Iterable[Bar],
    symbols: list[str],
    timestamp: datetime,
    timeframe: str,
    *,
    second_precision: bool = False,
) -> list[Bar]:
    visible = [
        bar for bar in live_bars
        if bar.date == timestamp.date()
        and bar.timestamp <= timestamp
        and bar.symbol in symbols
    ]
    freshness_floor = timestamp
    if second_precision:
        # Explicit-second callbacks see the last quote that had arrived at the
        # boundary.  The quote must still belong to the current continuous
        # trading session; a morning quote cannot satisfy an afternoon task.
        session_start = time(9, 30) if timestamp.time() < time(12, 0) else time(13, 0)
        freshness_floor = datetime.combine(timestamp.date(), session_start)
    elif time(11, 30) < timestamp.time() < time(13, 0):
        freshness_floor = datetime.combine(timestamp.date(), time(11, 30))
    latest_timestamp: dict[str, datetime] = {}
    for bar in visible:
        latest_timestamp[bar.symbol] = max(
            latest_timestamp.get(bar.symbol, bar.timestamp), bar.timestamp,
        )
    fresh_symbols = {
        symbol for symbol, latest in latest_timestamp.items()
        if latest >= freshness_floor
    }
    visible = [bar for bar in visible if bar.symbol in fresh_symbols]
    if timeframe == "1d":
        return _live_daily_snapshot(visible, timestamp)
    if timeframe != "1m":
        visible = group_bars(visible, timeframe)
    latest: dict[str, Bar] = {}
    for bar in visible:
        if bar.symbol not in latest or bar.timestamp > latest[bar.symbol].timestamp:
            latest[bar.symbol] = bar
    return sorted(latest.values(), key=lambda bar: (bar.timestamp, bar.symbol))


def _scheduled_snapshot(
    repo: Any,
    engine: FreeStrategyEngine,
    market: MarketData,
    timestamp: datetime,
    asset_type: str,
    timeframe: str,
    *,
    symbols: list[str] | None = None,
    live_bars: Iterable[Bar] | None = None,
    live_only: bool = False,
    second_precision: bool = False,
) -> list[Bar]:
    symbols = _scheduled_symbols(engine, timestamp) if symbols is None else symbols
    _ensure_scheduled_market_data(
        repo, market, symbols, timestamp.date() - timedelta(days=45), timestamp.date(), asset_type,
    )
    if live_only:
        # Historical rows remain available to history()/indicators, but they can
        # never become today's execution snapshot.
        return _live_scheduled_snapshot(
            live_bars or (),
            symbols,
            timestamp,
            timeframe,
            second_precision=second_precision,
        )
    intraday_second = time(9, 30) <= timestamp.time() < time(15, 0)
    if (second_precision and intraday_second) or timestamp.time().second or timestamp.time().microsecond:
        # A minute bar cannot prove what was tradable at 09:30:16.  Only a
        # provider-owned tick snapshot may satisfy a second-precision task.
        get_tick_snapshot = getattr(repo, "get_tick_snapshot", None)
        legacy_second_snapshot = getattr(repo, "get_second_snapshot", None)
        if not callable(get_tick_snapshot) and not callable(legacy_second_snapshot):
            get_second_snapshot = getattr(repo, "get_quote_snapshot", None)
        else:
            get_second_snapshot = get_tick_snapshot if callable(get_tick_snapshot) else legacy_second_snapshot
        if not callable(get_second_snapshot):
            raise ValueError(
                f"{timestamp.isoformat()} 定时任务需要 tick/逐笔历史行情，当前 provider 只有分钟K"
            )
        frame = get_second_snapshot(symbols, timestamp, asset_type)
        if frame is None or frame.is_empty():
            raise ValueError(f"{timestamp.isoformat()} tick历史行情为空，无法执行定时任务")
        if not frame.is_empty():
            frame = frame.filter(
                (pl.col("datetime") <= timestamp)
                & pl.col("datetime").is_not_null()
            )
        bars = _scheduled_minute_bars(frame, market, asset_type)
        latest: dict[str, Bar] = {}
        for bar in bars:
            if bar.symbol in symbols and bar.timestamp <= timestamp:
                latest[bar.symbol] = bar
        return sorted(latest.values(), key=lambda bar: (bar.timestamp, bar.symbol))
    if timeframe == "1d" and timestamp.time() >= time(15, 0):
        return [
            bar for symbol in symbols
            if (
                bar := _scheduled_daily_bar(
                    market,
                    symbol,
                    timestamp.date(),
                    _market_asset_type(repo, symbol, asset_type),
                )
            ) is not None
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
        symbol_asset_type = _market_asset_type(repo, symbol, asset_type)
        if timestamp.time() >= time(15, 0):
            closing = _scheduled_daily_bar(
                market,
                symbol,
                timestamp.date(),
                symbol_asset_type,
            )
            if closing is not None:
                bars.append(closing)
                continue
        dates = market.daily_dates.get(symbol, [])
        previous_index = bisect_left(dates, timestamp.date()) - 1
        if previous_index < 0:
            continue
        previous = _scheduled_daily_bar(
            market,
            symbol,
            dates[previous_index],
            symbol_asset_type,
        )
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
    *,
    live_bars: Iterable[Bar] | None = None,
    live_only: bool = False,
    second_precision: bool = False,
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
        if live_only:
            lower = _next_period_after(due_at, timeframe)
            candidates = [
                bar for bar in live_bars or ()
                if bar.symbol in symbols
                and lower <= bar.timestamp <= until
                and (timeframe != "1d" or bar.date > due_at.date())
            ]
            if timeframe == "1d":
                first_by_symbol: dict[str, Bar] = {}
                for bar in sorted(candidates, key=lambda item: (item.timestamp, item.symbol)):
                    first_by_symbol.setdefault(bar.symbol, bar)
                candidates = list(first_by_symbol.values())
        elif timeframe == "1d":
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
            get_next = getattr(
                repo,
                "get_tick_next" if second_precision else "get_minute_next",
                None,
            )
            if second_precision and not callable(get_next):
                get_next = getattr(repo, "get_second_next", None)
            if not callable(get_next):
                continue
            frame = get_next(
                symbols,
                _next_period_after(due_at, timeframe),
                until,
                asset_type,
            )
            candidates = _scheduled_minute_bars(frame, market, asset_type)
        by_time: dict[datetime, list[Bar]] = {}
        for bar in candidates:
            by_time.setdefault(bar.timestamp, []).append(bar)
        for timestamp, bars in sorted(by_time.items()):
            engine.advance_event(timestamp, bars, event_type="fill")


def replay_second_precision_session(
    engine: FreeStrategyEngine,
    day: date,
    rows: Iterable[Bar],
    cutoff: datetime,
    *,
    finalize: bool = True,
) -> int:
    """Replay a second-level session without moving a callback past its target.

    A strategy may combine ``on_quote`` with explicit schedules.  Each target
    consumes rows only through its own boundary, then runs the scheduled
    callback against the latest visible row.  Market callbacks and scheduled
    callbacks are replayed in two ordered phases so a later row can never be
    exposed to an earlier callback.
    """
    grouped = [
        (timestamp, list(values))
        for timestamp, values in groupby(
            sorted(
                (row for row in rows if row.date == day and row.timestamp <= cutoff),
                key=lambda row: (row.timestamp, row.symbol),
            ),
            key=lambda row: row.timestamp,
        )
    ]
    engine.begin_session(day)
    latest: dict[str, Bar] = {}
    offset = 0
    schedules = [
        at for at in engine.scheduled_times
        if at != "every_bar"
        and datetime.combine(day, time.fromisoformat(at)) <= cutoff
    ]

    def consume_until(target: datetime) -> None:
        nonlocal offset
        while offset < len(grouped) and grouped[offset][0] <= target:
            timestamp, values = grouped[offset]
            if engine.execution_mode == "quote":
                engine.advance_event(
                    timestamp,
                    event_type="quote",
                    quotes=[_bar_as_quote(bar) for bar in values],
                    run_schedules=False,
                )
            else:
                engine.advance_event(
                    timestamp,
                    values,
                    event_type="bar",
                    run_schedules=False,
                )
            latest.update({bar.symbol: bar for bar in values})
            offset += 1

    for at in schedules:
        target = datetime.combine(day, time.fromisoformat(at))
        consume_until(target)
        if latest:
            engine.advance_event(
                target,
                sorted(latest.values(), key=lambda bar: bar.symbol),
                event_type="scheduled",
                scheduled_at=at,
            )
        else:
            # Do not let finish_session catch up an unavailable callback with
            # a later row.  Missing target-time data is a no-trade outcome.
            for task in engine.context._scheduled:
                if task.resolved_time == at:
                    task.done = True

    consume_until(cutoff)
    if finalize:
        engine.finish_session()
    return len(grouped)


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
    allow_opening_data_retry: bool = False,
    live_bars: Iterable[Bar] | None = None,
    live_only: bool = False,
) -> None:
    engine.begin_session(day)
    second_precision = bool(engine.second_precision_schedules)
    live_values = tuple(live_bars or ())
    replayed_live_keys: set[tuple[datetime, str]] = set()

    def replay_live_quotes_through(target: datetime) -> None:
        """Replay received quotes once, without crossing a scheduled boundary."""
        grouped: dict[datetime, dict[str, Bar]] = {}
        last_timestamp = engine._last_timestamp  # noqa: SLF001
        for bar in live_values:
            if bar.date != day or bar.timestamp > target:
                continue
            key = (bar.timestamp, bar.symbol)
            if key not in replayed_live_keys and (
                last_timestamp is None or bar.timestamp > last_timestamp
            ):
                grouped.setdefault(bar.timestamp, {})[bar.symbol] = bar
        for timestamp, by_symbol in sorted(grouped.items()):
            values = [
                bar for symbol, bar in sorted(by_symbol.items())
                if (bar.timestamp, symbol) not in replayed_live_keys
            ]
            if not values:
                continue
            engine.advance_event(
                timestamp,
                event_type="quote",
                quotes=[_bar_as_quote(bar) for bar in values],
                run_schedules=False,
            )
            replayed_live_keys.update((bar.timestamp, bar.symbol) for bar in values)

    due_times = sorted({
        task.resolved_time
        for task in engine.context._scheduled
        if (
            not task.done
            and task.resolved_time != "every_bar"
            and datetime.combine(day, time.fromisoformat(task.resolved_time)) <= cutoff
        )
    })
    for at in due_times:
        timestamp = datetime.combine(day, time.fromisoformat(at))
        if not engine.prepare_scheduled_event(timestamp):
            continue
        _process_scheduled_fills(
            repo, engine, market, timestamp, asset_type, timeframe,
            live_bars=live_values, live_only=live_only,
            second_precision=second_precision,
        )
        symbols = _scheduled_symbols(engine, timestamp)
        required_symbols = _scheduled_required_symbols(engine, timestamp)
        # The callback boundary is a logical clock.  Its snapshot may only use
        # quotes already visible at that instant; a later poll must not be
        # backdated into this callback.
        event_timestamp = timestamp
        snapshot = _scheduled_snapshot(
            repo,
            engine,
            market,
            event_timestamp,
            asset_type,
            timeframe,
            symbols=symbols,
            live_bars=live_values,
            live_only=live_only,
            second_precision=second_precision,
        )
        market_time = event_timestamp.time()
        needs_live = live_only and market_time >= time(9, 30)
        missing_required = _missing_snapshot_symbols(snapshot, required_symbols)
        if needs_live and missing_required:
            retry_times: list[datetime] = []
            first_continuous_minute = timestamp + timedelta(minutes=1)
            if timestamp.time() == time(9, 30) and first_continuous_minute <= cutoff:
                retry_times.append(first_continuous_minute)
            if cutoff > timestamp and cutoff not in retry_times:
                retry_times.append(cutoff)
            if not (live_only and second_precision):
                for retry_timestamp in retry_times:
                    retry_snapshot = _scheduled_snapshot(
                        repo,
                        engine,
                        market,
                        retry_timestamp,
                        asset_type,
                        timeframe,
                        symbols=symbols,
                        live_bars=live_values,
                        live_only=live_only,
                        second_precision=second_precision,
                    )
                    retry_missing = _missing_snapshot_symbols(
                        retry_snapshot, required_symbols,
                    )
                    if not retry_missing:
                        event_timestamp = retry_timestamp
                        snapshot = retry_snapshot
                        missing_required = []
                        break
                    missing_required = retry_missing
            if event_timestamp == timestamp:
                if live_only:
                    missing_text = _missing_snapshot_text(missing_required)
                    raise ScheduledOpeningDataPending(
                        f"{day.isoformat()} {at} 定时任务缺少实时行情: "
                        f"{missing_text}，等待下一次行情同步"
                    )
                if (
                    allow_opening_data_retry
                    and cutoff < timestamp + timedelta(minutes=SCHEDULED_OPENING_RETRY_MINUTES)
                ):
                    raise ScheduledOpeningDataPending(
                        f"{day.isoformat()} 09:30 定时任务等待可交易分钟K，下一次行情同步将重试"
                    )
                raise ValueError(
                    f"{day.isoformat()} 09:30 定时任务缺少可交易分钟K，已停止执行以避免错误调仓"
                )
        elif timestamp.time() == time(9, 30) and not any(
            bar.tradable and not bar.suspended for bar in snapshot
        ):
            first_continuous_minute = timestamp + timedelta(minutes=1)
            if first_continuous_minute <= cutoff:
                opening_snapshot = _scheduled_snapshot(
                    repo,
                    engine,
                    market,
                    first_continuous_minute,
                    asset_type,
                    timeframe,
                    symbols=symbols,
                    second_precision=second_precision,
                )
                if any(bar.tradable and not bar.suspended for bar in opening_snapshot):
                    event_timestamp = first_continuous_minute
                    snapshot = opening_snapshot
            if event_timestamp == timestamp:
                if (
                    allow_opening_data_retry
                    and cutoff < timestamp + timedelta(minutes=SCHEDULED_OPENING_RETRY_MINUTES)
                ):
                    raise ScheduledOpeningDataPending(
                        f"{day.isoformat()} 09:30 定时任务等待可交易分钟K，下一次行情同步将重试"
                    )
                raise ValueError(
                    f"{day.isoformat()} 09:30 定时任务缺少可交易分钟K，已停止执行以避免错误调仓"
                )
        if event_timestamp > timestamp and not (live_only and second_precision):
            _process_scheduled_fills(
                repo,
                engine,
                market,
                event_timestamp,
                asset_type,
                timeframe,
                live_bars=live_values,
                live_only=live_only,
                second_precision=second_precision,
            )
        if live_only and second_precision:
            # Consume only actual quotes at or before this boundary. Quotes
            # after it remain for a later boundary or the final replay.
            replay_live_quotes_through(timestamp)
        engine.advance_event(
            timestamp if live_only and second_precision else event_timestamp,
            snapshot,
            event_type="scheduled",
            scheduled_at=at,
        )
    if live_only and second_precision:
        replay_live_quotes_through(cutoff)
    _process_scheduled_fills(
        repo, engine, market, cutoff, asset_type, timeframe,
        live_bars=live_values, live_only=live_only, second_precision=second_precision,
    )
    if finalize:
        closing_time = max(cutoff, datetime.combine(day, time(15, 0)))
        closing_symbols = _scheduled_symbols(engine, closing_time)
        snapshot = _scheduled_snapshot(
            repo, engine, market, closing_time, asset_type, timeframe,
            symbols=closing_symbols,
            live_bars=live_values,
            live_only=live_only,
            second_precision=second_precision,
        )
        closing_found = {bar.symbol for bar in snapshot}
        required_closing_symbols = closing_symbols if live_only else [
            symbol
            for symbol, quantity in engine.account.positions.items()
            if float(quantity) > 0
        ]
        closing_missing = [
            symbol for symbol in required_closing_symbols if symbol not in closing_found
        ]
        if closing_missing:
            missing_text = ", ".join(closing_missing)
            if live_only:
                raise ScheduledOpeningDataPending(
                    f"{day.isoformat()} 15:00 收盘任务缺少实时行情: {missing_text}，"
                    "等待下一次行情同步"
                )
            raise ValueError(
                f"{day.isoformat()} 15:00 收盘任务缺少行情: {missing_text}"
            )
        engine.advance_event(closing_time, snapshot, event_type="market")
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
        config = FreeStrategyConfig(**payload["config"])
        engine = FreeStrategyEngine(
            source,
            payload["timeframe"],
            config,
            instrument_loader=lambda _mode: _instrument_records(
                repo,
                payload["asset_type"],
                payload["timeframe"],
                start,
                end,
            ),
            callback_deadline=callback_deadline,
            dialect=str(payload.get("dialect") or "native"),
        )
        engine.set_run_window(start, end)
        configure_strategy_data_loaders(
            engine,
            repo,
            Path(payload["data_dir"]),
            source,
            start,
            end,
        )
        snapshot_caches = [
            configure_mainline_snapshot(engine, repo, start, end),
            configure_first_board_snapshot(engine, repo, start, end),
            configure_strong_momentum_snapshot(engine, repo, start, end),
            configure_four_mode_snapshot(engine, repo, start, end),
        ]
        active_snapshot_caches = [cache for cache in snapshot_caches if cache is not None]
        if len(active_snapshot_caches) > 1:
            raise ValueError("同一策略不能同时启用多个动态候选快照")
        mainline_cache, first_board_cache, strong_momentum_cache, four_mode_cache = snapshot_caches
        dynamic_cache = active_snapshot_caches[0] if active_snapshot_caches else None
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
        market_symbols = _market_symbols(engine, symbols)
        if payload.get("checkpoint"):
            engine.restore_checkpoint(payload["checkpoint"])
        if dynamic_cache is not None:
            market_data, warmup_metadata = _prepare_dynamic_market_data(
                repo, engine, start, end, dynamic_cache,
            )
        else:
            market_data, warmup_metadata = _prepare_market_data(
                repo, engine, symbols, start, end, payload["asset_type"], payload["timeframe"],
                include_benchmark=True,
            )
        engine.set_trading_calendar(
            day
            for symbol, day in market_data.daily
            if symbol in market_symbols and start <= day <= end
        )
        trading_dates = sorted({
            day
            for symbol, day in market_data.daily
            if symbol in market_symbols and start <= day <= end
        })
        calendar_dates = sorted({
            day
            for symbol, day in market_data.daily
            if symbol in market_symbols and day <= end
        })
        benchmark_dates = {
            day
            for symbol, day in market_data.daily
            if symbol == config.benchmark_symbol and day <= end
        }
        daily_root = {
            "stock": "kline_daily_enriched",
            "etf": "kline_etf_enriched",
            "index": "kline_index_enriched",
        }.get(payload["asset_type"], "kline_daily_enriched")
        minute_root = (
            "kline_etf_minute"
            if payload["asset_type"] == "etf"
            else "kline_minute"
        )
        try:
            readiness_manifest = build_readiness_manifest(
                Path(payload["data_dir"]),
                engine.readiness_requirements,
                strategy_sha256=source_digest,
                universe=engine.universe,
                trading_dates=trading_dates,
                calendar_dates=calendar_dates,
                benchmark_symbol=config.benchmark_symbol,
                benchmark_dates=benchmark_dates,
                dataset_roots=[
                    Path(daily_root),
                    Path(minute_root) if payload["timeframe"] != "1d" else Path(daily_root),
                ],
            )
            readiness_manifest["strategy_runtime"] = {
                "dialect": engine.dialect,
                "compatibility_version": engine.runtime.runtime_snapshot().get("compatibility_version"),
                "compatibility_report": engine.compatibility_report,
            }
        except ReadinessUnavailable as exc:
            if payload.get("run_dir"):
                persist_readiness_report(Path(payload["run_dir"]), exc.report)
            raise
        if payload.get("run_dir"):
            readiness_path = Path(payload["run_dir"]) / "readiness-manifest.json"
            readiness_path.write_text(
                json.dumps(readiness_manifest, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        replayed_rows = 0
        first_bar: datetime | None = None
        last_bar: datetime | None = None
        symbols_seen: set[str] = set()
        # Dynamic PIT pools bootstrap the engine with static candidates so the
        # first session can resolve instruments.  Those symbols are not actual
        # replay requirements when the current-day auction gate is waiting for
        # data; only the benchmark and candidates observed during replay are.
        requested_symbols = (
            [config.benchmark_symbol]
            if dynamic_cache is not None and config.benchmark_symbol
            else list(symbols)
        )
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
                if symbol in market_symbols and start <= day <= end
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
            if dynamic_cache is None:
                bars = _read_rows(repo, market_symbols, start, end, payload["asset_type"], payload["timeframe"], market_data=market_data)
                output.put({"type": "progress", "message": f"回放 {len(bars)} 根日K", "progress": 0.35})
                replayed_rows = len(bars)
                symbols_seen.update(bar.symbol for bar in bars)
                trading_days = len({bar.timestamp.date() for bar in bars})
                first_bar = min(bar.timestamp for bar in bars)
                last_bar = max(bar.timestamp for bar in bars)
                result = engine.run(bars)
            else:
                output.put({
                    "type": "progress",
                    "message": "按交易日回放 PIT 强势候选日K",
                    "progress": 0.35,
                })
                dynamic_dates = sorted({
                    day for symbol, day in market_data.daily
                    if symbol == config.benchmark_symbol and start <= day <= end
                })
                for index, trading_day in enumerate(dynamic_dates, start=1):
                    held_symbols = [
                        symbol for symbol, quantity in engine.account.positions.items()
                        if float(quantity) > 0
                    ]
                    candidate_symbols = [
                        str(row["symbol"])
                        for row in dynamic_cache.snapshot(trading_day).get("candidates", [])
                    ]
                    session_symbols = _market_symbols(
                        engine,
                        list(dict.fromkeys([*candidate_symbols, *held_symbols])),
                    )
                    rows = _daily_bars(
                        session_symbols,
                        trading_day,
                        trading_day,
                        payload["asset_type"],
                        market_data,
                    )
                    if not rows:
                        continue
                    replayed_rows += len(rows)
                    trading_days += 1
                    symbols_seen.update(bar.symbol for bar in rows)
                    for symbol in candidate_symbols:
                        if symbol not in requested_symbols:
                            requested_symbols.append(symbol)
                    first_bar = rows[0].timestamp if first_bar is None else min(first_bar, rows[0].timestamp)
                    last_bar = rows[-1].timestamp if last_bar is None else max(last_bar, rows[-1].timestamp)
                    engine.run(rows, return_result=False)
                    if index % 20 == 0:
                        output.put({
                            "type": "progress",
                            "message": f"已回放 {index} 个交易日的强势候选",
                            "progress": min(0.9, 0.35 + 0.55 * index / len(dynamic_dates)),
                        })
                if not trading_days:
                    raise ValueError("回测区间没有可用的 PIT 强势候选日K")
                engine.state = engine.context.state.copy()
                result = engine.result()
        else:
            output.put({
                "type": "progress",
                "message": (
                    "按交易日读取并回放 tick/逐笔行情"
                    if engine.second_precision_schedules else "按交易日读取并回放分钟K"
                ),
                "progress": 0.35,
            })
            cursor = start
            days_seen = 0
            days_with_bars = 0
            while cursor <= end:
                if cursor.weekday() < 5:
                    session_symbols = market_symbols
                    if engine.market_history_requirements or dynamic_cache is not None:
                        if not engine.has_market_date(cursor):
                            cursor += timedelta(days=1)
                            days_seen += 1
                            continue
                        engine.begin_session(cursor)
                        held_symbols = [
                            symbol for symbol, quantity in engine.account.positions.items()
                            if float(quantity) > 0
                        ]
                        dynamic_symbols = engine.universe
                        if dynamic_cache is not None:
                            dynamic_symbols = [
                                str(row["symbol"])
                                for row in dynamic_cache.snapshot(cursor).get("candidates", [])
                            ]
                        session_symbols = _market_symbols(
                            engine,
                            list(dict.fromkeys([*dynamic_symbols, *held_symbols])),
                        )
                        if dynamic_cache is not None:
                            _ensure_scheduled_market_data(
                                repo,
                                market_data,
                                session_symbols,
                                cursor - timedelta(days=45),
                                cursor,
                                payload["asset_type"],
                            )
                            _prime_dynamic_minute_metadata(
                                market_data,
                                (
                                    symbol for symbol in session_symbols
                                    if symbol != engine.config.benchmark_symbol
                                ),
                                cursor,
                            )
                        for symbol in session_symbols:
                            if symbol != engine.config.benchmark_symbol and symbol not in requested_symbols:
                                requested_symbols.append(symbol)
                    second_symbols = (
                        [
                            symbol for symbol in session_symbols
                            if symbol != engine.config.benchmark_symbol
                        ]
                        if engine.second_precision_schedules else session_symbols
                    )
                    bars = _read_rows(
                        repo, second_symbols, cursor, cursor,
                        payload["asset_type"], payload["timeframe"],
                        require_all_symbols=bool(engine.second_precision_schedules),
                        allow_empty=not bool(engine.second_precision_schedules),
                        market_data=market_data,
                        second_precision=bool(engine.second_precision_schedules),
                    )
                    rows = list(bars)
                    benchmark_symbol = engine.config.benchmark_symbol
                    if benchmark_symbol and not any(bar.symbol == benchmark_symbol for bar in rows):
                        benchmark_bar = _scheduled_daily_bar(
                            market_data,
                            benchmark_symbol,
                            cursor,
                            _market_asset_type(repo, benchmark_symbol, payload["asset_type"]),
                        )
                        if benchmark_bar is not None:
                            rows.append(benchmark_bar)
                            rows.sort(key=lambda bar: (bar.timestamp, bar.symbol))
                    if rows:
                        replayed_rows += len(rows)
                        for bar in rows:
                            symbols_seen.add(bar.symbol)
                            first_bar = bar.timestamp if first_bar is None else min(first_bar, bar.timestamp)
                            last_bar = bar.timestamp if last_bar is None else max(last_bar, bar.timestamp)
                        if engine.second_precision_schedules:
                            replay_second_precision_session(
                                engine,
                                cursor,
                                rows,
                                datetime.combine(cursor, time(15, 0)),
                            )
                        else:
                            engine.run(rows, return_result=False)
                        days_with_bars += 1
                days_seen += 1
                if days_seen % 20 == 0:
                    progress = min(0.9, 0.35 + 0.55 * days_seen / max((end - start).days + 1, 1))
                    output.put({"type": "progress", "message": f"已回放 {days_with_bars} 个交易日", "progress": progress})
                cursor += timedelta(days=1)
            if not days_with_bars:
                raise ValueError("没有可用的 tick/分钟K历史数据，请先同步后重试")
            get_minute_symbols = getattr(repo, "get_minute_symbols", None)
            stored_symbols = (
                set(dynamic_cache.all_symbols) | symbols_seen
                if dynamic_cache is not None
                else set(get_minute_symbols(payload["asset_type"], start, end))
                if callable(get_minute_symbols) else symbols_seen
            )
            missing = [symbol for symbol in requested_symbols if symbol not in stored_symbols]
            if missing:
                raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
            engine.state = engine.context.state.copy()
            result = engine.result()
            trading_days = days_with_bars
        if mainline_cache is not None:
            output.put({
                "type": "progress",
                "message": "计算买点评估与 D-1 资金流匹配样本",
                "progress": 0.92,
            })
            result["entry_analysis"] = build_mainline_entry_analysis(
                repo,
                result,
                start,
                end,
                Path(payload["data_dir"]),
                config.benchmark_symbol,
            )
        if dynamic_cache is not None:
            result["research_performance"] = build_research_periods(result, start, end)
        five_fortunes = result.get("state", {}).get("five_fortunes", {})
        five_fortunes_v2 = result.get("state", {}).get("five_fortunes_v2", {})
        strategy_metadata = five_fortunes or five_fortunes_v2
        if payload["timeframe"] == "1d" or engine.execution_mode == "scheduled":
            available_symbols = symbols_seen
        else:
            get_minute_symbols = getattr(repo, "get_minute_symbols", None)
            available_symbols = (
                set(dynamic_cache.all_symbols) | symbols_seen
                if dynamic_cache is not None
                else set(get_minute_symbols(payload["asset_type"], start, end))
                if callable(get_minute_symbols) else symbols_seen
            )
        missing_symbols = [symbol for symbol in requested_symbols if symbol not in available_symbols]
        requested_symbol_set = set(requested_symbols)
        coverage_seen_symbols = sorted(
            symbol for symbol in symbols_seen if symbol in requested_symbol_set
        )
        minute_table = "kline_etf_minute" if payload["asset_type"] == "etf" else "kline_minute"
        result["metadata"] = {
            "strategy_id": payload.get("strategy_id"), "strategy_name": payload.get("strategy_name"),
            "timeframe": payload["timeframe"], "asset_type": payload["asset_type"],
            "start": payload["start"], "end": payload["end"],
            "symbols": requested_symbols, "symbol_count": len(requested_symbols), "universe_source": universe_source,
            "data_days": len(result.get("daily_equity_curve", [])),
            "source_revision": payload.get("source_revision"),
            "strategy_source_sha256": source_digest,
            "dialect": engine.dialect,
            "compatibility_version": engine.runtime.runtime_snapshot().get("compatibility_version"),
            "compatibility_report": engine.compatibility_report,
            "resumed_from_checkpoint": bool(payload.get("checkpoint")),
            "warmup": warmup_metadata,
            "market_history": engine.market_history_metadata,
            "mainline_snapshot": (
                {
                    "enabled": True,
                    "candidate_symbols": len(mainline_cache.all_symbols),
                    "mode": "pit_daily_dynamic_universe",
                }
                if mainline_cache is not None else {"enabled": False}
            ),
            "four_mode_snapshot": (
                {
                    "enabled": True,
                    "candidate_symbols": len(four_mode_cache.all_symbols),
                    "mode": "four_mode_pit_static_and_auction",
                    "modes": ["yje", "rzq", "qs", "sb"],
                }
                if four_mode_cache is not None else {"enabled": False}
            ),
            "first_board_snapshot": (
                {
                    "enabled": True,
                    "candidate_symbols": len(first_board_cache.all_symbols),
                    "mode": "daily_high_limit_touch_io_index",
                }
                if first_board_cache is not None else {"enabled": False}
            ),
            "strong_momentum_snapshot": (
                {
                    "enabled": True,
                    "candidate_symbols": len(strong_momentum_cache.all_symbols),
                    "mode": "pit_d1_strong_stock_dynamic_universe",
                }
                if strong_momentum_cache is not None else {"enabled": False}
            ),
            "readiness": readiness_manifest,
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
                "seen_symbols": coverage_seen_symbols,
                "missing_symbols": missing_symbols,
                "configured_provider": payload.get("data_provider", "tickflow"),
                "storage": (
                    "tick"
                    if engine.second_precision_schedules
                    else "event_snapshots"
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
