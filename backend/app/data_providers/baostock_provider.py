"""BaoStock provider.

BaoStock is used as an auxiliary free route for coarse historical stock
minute bars. It intentionally does not advertise 1-minute support: the
canonical TickFlow minute table is 1m, while BaoStock documents 5/15/30/60m
stock K-lines.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

import polars as pl

from app.data_providers.base import AssetType, ProviderCapabilities

logger = logging.getLogger(__name__)

_BAOSTOCK_MINUTE_FIELDS = "date,time,code,open,high,low,close,volume,amount,adjustflag"
_SUPPORTED_FREQS = {
    "5": "5",
    "5m": "5",
    "15": "15",
    "15m": "15",
    "30": "30",
    "30m": "30",
    "60": "60",
    "60m": "60",
}
_EXCHANGE_SUFFIX_TO_PREFIX = {
    "SH": "sh",
    "SZ": "sz",
}


class BaoStockProvider:
    name = "baostock"
    capabilities = ProviderCapabilities(minute=True)

    def __init__(self, bs_module: Any | None = None) -> None:
        self._bs_module = bs_module

    @staticmethod
    def supports_minute_freq(freq: str = "1m") -> bool:
        return _normalize_freq(freq, strict=False) is not None

    def get_instruments(self, asset_type: AssetType) -> pl.DataFrame:  # noqa: ARG002
        return pl.DataFrame()

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
    ) -> pl.DataFrame:
        raise NotImplementedError("BaoStockProvider only supports coarse historical minute data")

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
    ) -> pl.DataFrame:
        raise NotImplementedError("BaoStockProvider does not support adjustment factors")

    def get_realtime(
        self,
        universes: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        return pl.DataFrame()

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
        freq: str = "5m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """Fetch normalized BaoStock coarse minute rows.

        BaoStock minute rows are stock-only and only documented for
        5/15/30/60-minute frequencies. Returning a DataFrame here does not mean
        the rows are safe to persist into the existing 1m TickFlow table.
        """
        if not symbols:
            return pl.DataFrame()
        if asset_type != "stock":
            raise ValueError("BaoStock minute route supports stock symbols only")
        frequency = _normalize_freq(freq, strict=True)
        start_date = _format_date(start_time)
        end_date = _format_date(end_time)
        bs = self._baostock()
        login = bs.login()
        if getattr(login, "error_code", "0") != "0":
            raise RuntimeError(f"BaoStock login failed: {getattr(login, 'error_msg', '')}")
        frames: list[pl.DataFrame] = []
        try:
            total = len(symbols)
            for idx, symbol in enumerate(symbols, start=1):
                code = _to_baostock_code(symbol)
                if code is None:
                    logger.warning("BaoStock minute route skipped unsupported symbol: %s", symbol)
                    if on_chunk_done:
                        on_chunk_done(idx, total)
                    continue
                rows = self._query_symbol(bs, code, start_date, end_date, frequency)
                frame = _normalize_baostock_minute_rows(rows, default_symbol=symbol)
                if not frame.is_empty():
                    frames.append(frame)
                if on_chunk_done:
                    on_chunk_done(idx, total)
        finally:
            bs.logout()
        return (
            pl.concat(frames, how="diagonal_relaxed").sort(["symbol", "datetime"])
            if frames else pl.DataFrame()
        )

    def _baostock(self):
        if self._bs_module is not None:
            return self._bs_module
        import baostock as bs  # noqa: PLC0415

        return bs

    @staticmethod
    def _query_symbol(
        bs,
        code: str,
        start_date: str | None,
        end_date: str | None,
        frequency: str,
    ) -> list[dict[str, str]]:
        rs = bs.query_history_k_data_plus(
            code,
            _BAOSTOCK_MINUTE_FIELDS,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag="2",
        )
        if getattr(rs, "error_code", "0") != "0":
            raise RuntimeError(
                f"BaoStock query_history_k_data_plus failed for {code}: "
                f"{getattr(rs, 'error_msg', '')}"
            )
        rows: list[dict[str, str]] = []
        fields = list(getattr(rs, "fields", []))
        while rs.next():
            values = rs.get_row_data()
            rows.append({field: values[pos] for pos, field in enumerate(fields)})
        return rows


def _normalize_freq(freq: str, *, strict: bool) -> str | None:
    normalized = _SUPPORTED_FREQS.get(str(freq or "").strip().lower())
    if normalized is None and strict:
        raise ValueError("BaoStock minute route supports only 5m/15m/30m/60m frequencies")
    return normalized


def _format_date(value: datetime | None) -> str | None:
    return value.strftime("%Y-%m-%d") if value is not None else None


def _to_baostock_code(symbol: str) -> str | None:
    parts = str(symbol or "").strip().upper().split(".")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    prefix = _EXCHANGE_SUFFIX_TO_PREFIX.get(parts[1])
    if prefix is None:
        return None
    return f"{prefix}.{parts[0]}"


def _from_baostock_code(code: str) -> str | None:
    parts = str(code or "").strip().split(".")
    if len(parts) != 2:
        return None
    prefix, raw_code = parts[0].lower(), parts[1]
    exchange = {"sh": "SH", "sz": "SZ"}.get(prefix)
    if exchange is None or not raw_code:
        return None
    return f"{raw_code}.{exchange}"


def _parse_baostock_datetime(date_value: Any, time_value: Any) -> datetime | None:
    date_text = str(date_value or "").strip()
    time_text = str(time_value or "").strip()
    if not date_text and not time_text:
        return None
    if time_text:
        compact = "".join(ch for ch in time_text if ch.isdigit())
        if len(compact) >= 14:
            try:
                return datetime.strptime(compact[:14], "%Y%m%d%H%M%S")
            except ValueError:
                return None
        if len(compact) >= 6 and date_text:
            normalized_date = date_text.replace("-", "")
            try:
                return datetime.strptime(normalized_date + compact[:6], "%Y%m%d%H%M%S")
            except ValueError:
                return None
        try:
            return datetime.fromisoformat(time_text)
        except ValueError:
            pass
    if date_text:
        try:
            return datetime.fromisoformat(date_text)
        except ValueError:
            return None
    return None


def _normalize_baostock_minute_rows(
    rows: list[dict[str, str]],
    *,
    default_symbol: str,
) -> pl.DataFrame:
    normalized: list[dict[str, object]] = []
    for row in rows:
        symbol = _from_baostock_code(row.get("code", "")) or default_symbol
        timestamp = _parse_baostock_datetime(row.get("date"), row.get("time"))
        normalized.append({
            "symbol": symbol,
            "datetime": timestamp,
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
        })
    if not normalized:
        return pl.DataFrame()
    df = pl.DataFrame(normalized)
    df = df.with_columns(
        pl.col("datetime").cast(pl.Datetime("us"), strict=False),
        pl.col("open").cast(pl.Float64, strict=False),
        pl.col("high").cast(pl.Float64, strict=False),
        pl.col("low").cast(pl.Float64, strict=False),
        pl.col("close").cast(pl.Float64, strict=False),
        pl.col("volume").cast(pl.Float64, strict=False),
        pl.col("amount").cast(pl.Float64, strict=False),
    )
    return df.select(["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"])
