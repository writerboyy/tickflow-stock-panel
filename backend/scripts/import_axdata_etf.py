#!/usr/bin/env python3
"""Import ETF daily and historical intraday data from a local AxData service."""
from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from app.indicators.pipeline import compute_enriched
from app.services.kline_sync import _atomic_write_parquet, _write_minute_partition
from app.tickflow.repository import DataStore, KlineRepository


logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_FIELDS = [
    "instrument_id", "trade_time", "open", "high", "low", "close", "volume", "amount",
]
INTRADAY_FIELDS = [
    "instrument_id", "trade_date", "trade_time", "minute_index", "price", "volume", "prev_close",
]
DIVIDEND_FIELDS = [
    "instrument_id", "dividend_date", "accumulated_dividend",
]


def _request(
    base_url: str,
    interface: str,
    params: dict[str, Any],
    fields: list[str],
    *,
    retries: int,
) -> list[dict[str, Any]]:
    error: Exception | None = None
    url = f"{base_url.rstrip('/')}/v1/request/{interface}"
    for attempt in range(retries + 1):
        try:
            response = httpx.post(
                url,
                json={"params": params, "fields": fields},
                timeout=30.0,
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("success"):
                detail = payload.get("error") or "unknown AxData error"
                raise RuntimeError(f"{interface} failed: {detail}")
            return list(payload.get("data") or [])
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            error = exc
            if attempt < retries:
                time.sleep(0.5 * (2**attempt))
    assert error is not None
    raise error


def _daily_frame(
    symbol: str,
    rows: list[dict[str, Any]],
    start: date,
    end: date,
) -> pl.DataFrame:
    normalized = []
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["trade_time"]))
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(SHANGHAI)
        day = timestamp.date()
        if day < start or day > end:
            continue
        normalized.append({
            "symbol": symbol,
            "date": day,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row.get("volume") or 0.0),
            "amount": float(row.get("amount") or 0.0),
        })
    daily = pl.DataFrame(normalized)
    if daily.is_empty():
        return daily
    return daily.sort("date")


def _dividend_factors(
    symbol: str,
    daily: pl.DataFrame,
    dividend_rows: list[dict[str, Any]],
    existing: pl.DataFrame,
) -> pl.DataFrame:
    factors = existing.filter(pl.col("symbol") == symbol) if not existing.is_empty() else existing
    replacements: list[dict[str, Any]] = []
    previous_accumulated = 0.0
    for row in sorted(dividend_rows, key=lambda item: str(item["dividend_date"])):
        day = datetime.strptime(str(row["dividend_date"]), "%Y%m%d").date()
        accumulated = float(row["accumulated_dividend"])
        cash_dividend = accumulated - previous_accumulated
        previous_accumulated = accumulated
        if cash_dividend <= 0:
            continue
        previous = daily.filter(pl.col("date") < day).tail(1)
        if previous.is_empty():
            continue
        previous_close = float(previous["close"][0])
        if previous_close <= cash_dividend:
            raise ValueError(f"invalid dividend for {symbol} on {day}: {cash_dividend}")
        split_ratio = 1.0
        if not factors.is_empty():
            current = factors.filter(pl.col("trade_date") == day)
            if not current.is_empty():
                observed = float(current["ex_factor"][0])
                nearest = round(observed)
                if nearest >= 2 and abs(observed - nearest) / nearest <= 0.02:
                    split_ratio = float(nearest)
        replacements.append({
            "symbol": symbol,
            "trade_date": day,
            "ex_factor": split_ratio * previous_close / (previous_close - cash_dividend),
        })
    if not replacements:
        return factors
    replacement_frame = pl.DataFrame(replacements)
    if factors.is_empty():
        return replacement_frame.sort("trade_date")
    replaced_dates = replacement_frame["trade_date"].to_list()
    return (
        pl.concat([factors.filter(~pl.col("trade_date").is_in(replaced_dates)), replacement_frame])
        .sort("trade_date")
    )


def _load_factors(data_dir: Path) -> pl.DataFrame:
    path = data_dir / "adj_factor_etf" / "all.parquet"
    return pl.read_parquet(path) if path.exists() else pl.DataFrame()


def _save_factors(data_dir: Path, symbol: str, factors: pl.DataFrame, existing: pl.DataFrame) -> None:
    if factors.is_empty():
        return
    other = existing.filter(pl.col("symbol") != symbol) if not existing.is_empty() else existing
    merged = factors if other.is_empty() else pl.concat([other, factors])
    path = data_dir / "adj_factor_etf" / "all.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_parquet(merged.sort(["symbol", "trade_date"]), path)


def _minute_frame(
    symbol: str,
    rows: list[dict[str, Any]],
    adjustment_ratio: float,
) -> pl.DataFrame:
    normalized = []
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["trade_time"]))
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(SHANGHAI).replace(tzinfo=None)
        raw_price = float(row["price"])
        volume = float(row.get("volume") or 0.0)
        adjusted_price = raw_price * adjustment_ratio
        normalized.append({
            "symbol": symbol,
            "datetime": timestamp,
            "open": adjusted_price,
            "high": adjusted_price,
            "low": adjusted_price,
            "close": adjusted_price,
            "volume": volume,
            "amount": raw_price * volume * 100.0,
        })
    return pl.DataFrame(normalized)


def _existing_minute_dates(data_dir: Path, symbol: str, dates: list[date]) -> set[date]:
    existing: set[date] = set()
    for day in dates:
        path = data_dir / "kline_etf_minute" / f"date={day.isoformat()}" / "part.parquet"
        if not path.exists():
            continue
        rows = pl.read_parquet(path, columns=["symbol"]).filter(pl.col("symbol") == symbol)
        if not rows.is_empty():
            existing.add(day)
    return existing


def _merge_instrument(repo: KlineRepository, symbol: str, name: str) -> None:
    instruments = repo.get_etf_instruments()
    incoming = pl.DataFrame({
        "symbol": [symbol],
        "name": [name],
        "code": [symbol.split(".", 1)[0]],
        "asset_type": ["etf"],
    })
    if not instruments.is_empty():
        incoming = pl.concat([instruments, incoming], how="diagonal_relaxed")
    repo.save_etf_instruments(incoming)


def import_symbol(
    *,
    symbol: str,
    name: str,
    start: date,
    end: date,
    data_dir: Path,
    axdata_url: str,
    workers: int,
    retries: int,
    replace_minute: bool = False,
) -> tuple[int, int]:
    daily_rows = _request(
        axdata_url,
        "etf_kline_tdx",
        {"code": symbol, "period": "day", "count": 800},
        DAILY_FIELDS,
        retries=retries,
    )
    daily = _daily_frame(symbol, daily_rows, start - timedelta(days=120), end)
    if daily.is_empty():
        raise RuntimeError(f"AxData returned no daily data for {symbol}")

    dividend_rows = _request(
        axdata_url,
        "fund_etf_dividend_sina",
        {"symbol": symbol, "limit": 5000},
        DIVIDEND_FIELDS,
        retries=retries,
    )
    existing_factors = _load_factors(data_dir)
    factors = _dividend_factors(symbol, daily, dividend_rows, existing_factors)
    enriched = compute_enriched(daily, factors=factors, instruments=None)

    trading_dates = daily.filter(pl.col("date") >= start)["date"].to_list()
    existing_dates = _existing_minute_dates(data_dir, symbol, trading_dates)
    requested_dates = trading_dates if replace_minute else [
        day for day in trading_dates if day not in existing_dates
    ]
    logger.info(
        "%s daily=%d, minute dates requested=%d, existing=%d, dividends=%d",
        symbol,
        daily.height,
        len(requested_dates),
        len(existing_dates),
        len(dividend_rows),
    )

    intraday_by_date: dict[date, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _request,
                axdata_url,
                "etf_intraday_history_tdx",
                {"code": symbol, "trade_date": day.strftime("%Y%m%d")},
                INTRADAY_FIELDS,
                retries=retries,
            ): day
            for day in requested_dates
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            day = futures[future]
            rows = future.result()
            if not rows:
                raise RuntimeError(f"AxData returned no intraday data for {symbol} on {day}")
            intraday_by_date[day] = rows
            if completed % 20 == 0 or completed == len(futures):
                logger.info("%s minute fetch %d/%d", symbol, completed, len(futures))

    repo = KlineRepository(DataStore(data_dir))
    repo.append_etf_daily(daily)
    repo.append_etf_enriched(enriched)
    _save_factors(data_dir, symbol, factors, existing_factors)
    _merge_instrument(repo, symbol, name)

    ratios = {
        row["date"]: float(row["close"]) / float(row["raw_close"])
        for row in enriched.select("date", "close", "raw_close").iter_rows(named=True)
    }
    minute_frames = [
        _minute_frame(symbol, intraday_by_date[day], ratios.get(day, 1.0))
        for day in sorted(intraday_by_date)
    ]
    minute_rows = sum(frame.height for frame in minute_frames)
    if minute_frames:
        _write_minute_partition(
            pl.concat(minute_frames),
            data_dir / "kline_etf_minute",
        )
    return daily.height, minute_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Import missing ETF history from a local AxData service")
    parser.add_argument("symbol", help="TickFlow symbol, for example 161226.SZ")
    parser.add_argument("--name", default=None, help="Instrument display name")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data")
    parser.add_argument("--axdata-url", default="http://127.0.0.1:8666")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--replace-minute",
        action="store_true",
        help="Replace existing minute rows with AxData raw prices adjusted by imported dividends",
    )
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must not be after --end")
    if args.workers < 1:
        parser.error("--workers must be at least 1")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    daily_count, minute_count = import_symbol(
        symbol=args.symbol.upper(),
        name=args.name or args.symbol.upper(),
        start=args.start,
        end=args.end,
        data_dir=args.data_dir.resolve(),
        axdata_url=args.axdata_url,
        workers=args.workers,
        retries=args.retries,
        replace_minute=args.replace_minute,
    )
    logger.info("import complete: daily=%d minute=%d", daily_count, minute_count)


if __name__ == "__main__":
    main()
