"""Selectable Tushare history provider backed by the fixed proxy endpoint."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import logging
from typing import Any

import polars as pl

from app.data_providers.base import AssetType
from app.data_providers.normalizer import normalize_daily
from app.services.tushare_history import (
    MAX_MINUTE_ROWS,
    BackfillBlocked,
    GlobalRateLimiter,
    TusharePermissionError,
    TushareProxyClient,
    canonicalize_minute_units,
    forward_adjust_minutes,
    load_tushare_key,
    normalize_adjustment_rows,
    normalize_rows,
    validate_minute_frame,
)

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "adj_factor", "minute")
_MINUTE_FIELDS = ("symbol", "datetime", "open", "high", "low", "close", "volume", "amount")
_SHARED_LIMITER = GlobalRateLimiter()


@dataclass
class _TushareConfig:
    name: str = "tushare"
    display_name: str = "Tushare（历史行情）"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def availability() -> tuple[bool, str]:
    """No optional runtime is needed; the card handles key configuration."""
    return True, "已配置 Key" if load_tushare_key() else "需要配置 API Key"


def _api_for(asset_type: AssetType, *, kind: str) -> str:
    routes = {
        ("stock", "daily"): "daily",
        ("etf", "daily"): "fund_daily",
        ("index", "daily"): "index_daily",
        ("stock", "adjustment"): "adj_factor",
        ("etf", "adjustment"): "fund_adj",
        ("stock", "minute"): "stk_mins",
        ("etf", "minute"): "etf_mins",
    }
    api_name = routes.get((asset_type, kind))
    if api_name is None:
        raise ValueError(f"Tushare {kind} route does not support asset_type={asset_type}")
    return api_name


def _date_param(value: datetime | None, fallback: str) -> str:
    return value.strftime("%Y%m%d") if value else fallback


def _datetime_param(value: datetime | None, fallback: str) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else fallback


def _parse_date_column(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    if column not in frame.columns or frame.schema[column] == pl.Date:
        return frame
    if frame.schema[column] == pl.String:
        return frame.with_columns(
            pl.coalesce(
                pl.col(column).str.strptime(pl.Date, "%Y%m%d", strict=False),
                pl.col(column).str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            ).alias(column)
        )
    return frame.with_columns(pl.col(column).cast(pl.Date, strict=False).alias(column))


def _instrument_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text or None


class TushareProvider:
    """Historical provider. The API key is read lazily for every pull."""

    name = "tushare"
    builtin = True

    def __init__(self, client_factory: Callable[[str], Any] | None = None) -> None:
        self.config = _TushareConfig()
        self._client_factory = client_factory or (
            lambda key: TushareProxyClient(key, limiter=_SHARED_LIMITER)
        )

    def close(self) -> None:
        pass

    def _client(self):
        key = load_tushare_key()
        if not key:
            raise TusharePermissionError("Tushare API key is not configured")
        return self._client_factory(key)

    @staticmethod
    def supports_minute_freq(freq: str = "1m") -> bool:
        return str(freq).strip().lower() in {"1", "1m", "1min"}

    def get_instruments(self, asset_type: AssetType = "stock") -> list[dict]:
        client = self._client()
        if asset_type == "stock":
            requests = [
                ("stock_basic", {"exchange": "", "list_status": status})
                for status in ("L", "D", "P")
            ]
        elif asset_type == "etf":
            requests = [("etf_basic", {"exchange": ""})]
        elif asset_type == "index":
            requests = [("index_basic", {"market": "SSE,SZSE"})]
        else:
            return []
        out: list[dict] = []
        for api_name, params in requests:
            response = client.request(api_name, params)
            for row in response.rows:
                symbol = str(row.get("ts_code") or "")
                if not symbol:
                    continue
                exchange = symbol.rsplit(".", 1)[-1] if "." in symbol else ""
                out.append(
                    {
                        "symbol": symbol,
                        "name": row.get("name") or symbol,
                        "code": symbol.split(".", 1)[0],
                        "exchange": exchange,
                        "region": "CN",
                        "type": asset_type,
                        "ext": {
                            "listing_date": _instrument_date(row.get("list_date")),
                        },
                    }
                )
        return out

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        client = self._client()
        api_name = _api_for(asset_type, kind="daily")
        frames: list[pl.DataFrame] = []
        total = len(symbols)
        for index, symbol in enumerate(symbols, start=1):
            response = client.request(
                api_name,
                {
                    "ts_code": symbol,
                    "start_date": _date_param(start_time, "19900101"),
                    "end_date": _date_param(end_time, date.today().strftime("%Y%m%d")),
                },
            )
            frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
            if not frame.is_empty():
                frame = _parse_date_column(frame, "trade_date")
                # Tushare daily-family endpoints use lots and thousand yuan.
                if "amount" in frame.columns:
                    frame = frame.with_columns((pl.col("amount").cast(pl.Float64) * 1000).alias("amount"))
                normalized = normalize_daily(frame, source=self.name)
                if not normalized.is_empty():
                    frames.append(normalized)
            if on_chunk_done:
                on_chunk_done(index, total)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        client = self._client()
        api_name = _api_for(asset_type, kind="adjustment")
        frames: list[pl.DataFrame] = []
        total = len(symbols)
        requested_start = start_time.date() if start_time else None
        requested_end = end_time.date() if end_time else None
        query_start = start_time - timedelta(days=14) if start_time else None
        for index, symbol in enumerate(symbols, start=1):
            response = client.request(
                api_name,
                {
                    "ts_code": symbol,
                    "start_date": _date_param(query_start, "19900101"),
                    "end_date": _date_param(end_time, date.today().strftime("%Y%m%d")),
                },
            )
            frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
            if not frame.is_empty():
                frame = _parse_date_column(frame, "trade_date")
                normalized = normalize_adjustment_rows(frame).filter(
                    (pl.col("ex_factor") - 1.0).abs() > 1e-10
                )
                if requested_start is not None:
                    normalized = normalized.filter(pl.col("trade_date") >= requested_start)
                if requested_end is not None:
                    normalized = normalized.filter(pl.col("trade_date") <= requested_end)
                if not normalized.is_empty():
                    frames.append(normalized)
            if on_chunk_done:
                on_chunk_done(index, total)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        if not self.supports_minute_freq(freq):
            raise ValueError("Tushare provider only supports 1-minute bars")
        api_name = _api_for(asset_type, kind="minute")
        client = self._client()
        frames: list[pl.DataFrame] = []
        total = len(symbols)
        start_text = _datetime_param(start_time, "1990-01-01 00:00:00")
        initial_end = _datetime_param(end_time, f"{date.today().isoformat()} 23:59:59")
        for index, symbol in enumerate(symbols, start=1):
            cursor = initial_end
            pages: list[pl.DataFrame] = []
            while True:
                response = client.request(
                    api_name,
                    {
                        "ts_code": symbol,
                        "freq": "1min",
                        "start_date": start_text,
                        "end_date": cursor,
                        "limit": MAX_MINUTE_ROWS,
                    },
                )
                if not response.items:
                    break
                frame = normalize_rows(response.rows, asset_type=asset_type)
                valid, _audit = validate_minute_frame(frame)
                if valid.is_empty():
                    raise BackfillBlocked(f"Tushare minute page has no valid rows for {symbol}")
                pages.append(valid.select(_MINUTE_FIELDS))
                oldest = valid["datetime"].min()
                previous = datetime.fromisoformat(cursor)
                if oldest is None or oldest > previous:
                    raise BackfillBlocked(f"Tushare minute cursor did not decrease for {symbol}")
                next_cursor = oldest - timedelta(minutes=1)
                if next_cursor >= previous:
                    raise BackfillBlocked(f"Tushare minute cursor did not decrease for {symbol}")
                cursor = next_cursor.strftime("%Y-%m-%d %H:%M:%S")
                if len(response.items) < MAX_MINUTE_ROWS:
                    break
                if start_time is not None and datetime.fromisoformat(cursor) < start_time:
                    break
            if pages:
                raw = pl.concat(pages, how="vertical_relaxed").unique(
                    subset=["symbol", "datetime"], keep="last"
                ).sort(["symbol", "datetime"])
                if start_time is not None:
                    raw = raw.filter(pl.col("datetime") >= start_time)
                if end_time is not None:
                    raw = raw.filter(pl.col("datetime") <= end_time)
                if raw.is_empty():
                    if on_chunk_done:
                        on_chunk_done(index, total)
                    continue
                factors = self._minute_factors(client, symbol, asset_type, start_time)
                adjusted = canonicalize_minute_units(forward_adjust_minutes(raw, factors))
                adjusted, _audit = validate_minute_frame(adjusted)
                if not adjusted.is_empty():
                    frames.append(adjusted.select(_MINUTE_FIELDS))
            if on_chunk_done:
                on_chunk_done(index, total)
        return pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()

    @staticmethod
    def _minute_factors(client, symbol: str, asset_type: AssetType, start_time: datetime | None) -> pl.DataFrame:
        api_name = _api_for(asset_type, kind="adjustment")
        query_start = start_time - timedelta(days=14) if start_time else None
        response = client.request(
            api_name,
            {
                "ts_code": symbol,
                "start_date": _date_param(query_start, "19900101"),
                "end_date": date.today().strftime("%Y%m%d"),
            },
        )
        frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
        return _parse_date_column(frame, "trade_date") if not frame.is_empty() else frame

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        sample_symbols = symbols or ["000001.SZ"]
        end_time = datetime(2025, 1, 2, 15, 0)
        start_time = datetime(2025, 1, 2, 9, 30)
        if dataset == "daily":
            frame = self.get_daily(sample_symbols, start_time, end_time)
        elif dataset == "adj_factor":
            frame = self.get_adj_factors(sample_symbols, start_time, end_time)
        elif dataset == "minute":
            frame = self.get_minute(sample_symbols, start_time, end_time)
        else:
            raise ValueError(f"Tushare does not support dataset={dataset}")
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": frame.height,
            "columns": frame.columns,
            "preview": frame.head(5).to_dicts() if not frame.is_empty() else [],
        }
