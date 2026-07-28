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

from app.services.kline_sync import _write_minute_partition
from app.tickflow.repository import DataStore, KlineRepository


logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_FIELDS = [
    "instrument_id", "trade_time", "open", "high", "low", "close", "volume", "amount",
]
INTRADAY_FIELDS = [
    "instrument_id", "trade_date", "trade_time", "minute_index", "price", "volume", "prev_close",
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


def _daily_frames(
    symbol: str,
    rows: list[dict[str, Any]],
    start: date,
    end: date,
) -> tuple[pl.DataFrame, pl.DataFrame]:
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
        return daily, daily
    daily = daily.sort("date")
    enriched = daily.with_columns(
        pl.col("close").alias("raw_close"),
        pl.col("high").alias("raw_high"),
        pl.col("low").alias("raw_low"),
    )
    return daily, enriched


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
) -> tuple[int, int]:
    daily_rows = _request(
        axdata_url,
        "etf_kline_tdx",
        {"code": symbol, "period": "day", "count": 800},
        DAILY_FIELDS,
        retries=retries,
    )
    daily, enriched = _daily_frames(symbol, daily_rows, start - timedelta(days=120), end)
    if daily.is_empty():
        raise RuntimeError(f"AxData returned no daily data for {symbol}")

    trading_dates = daily.filter(pl.col("date") >= start)["date"].to_list()
    existing_dates = _existing_minute_dates(data_dir, symbol, trading_dates)
    missing_dates = [day for day in trading_dates if day not in existing_dates]
    logger.info(
        "%s daily=%d, minute dates missing=%d, existing=%d",
        symbol,
        daily.height,
        len(missing_dates),
        len(existing_dates),
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
            for day in missing_dates
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
    )
    logger.info("import complete: daily=%d minute=%d", daily_count, minute_count)


if __name__ == "__main__":
    main()
