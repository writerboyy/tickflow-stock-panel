"""Read-only backup market-data sources used after canonical quality failures."""

from __future__ import annotations

from datetime import date
import importlib
import socket
import threading
from typing import Any

import polars as pl


_FIELDS = ("open", "high", "low", "close", "volume", "amount")
_BAOSTOCK_LOCK = threading.Lock()


class _SocketModuleWithTimeout:
    def __init__(self, original: Any, timeout: float) -> None:
        self._original = original
        self._timeout = timeout

    def socket(self, *args: Any, **kwargs: Any):
        value = self._original.socket(*args, **kwargs)
        value.settimeout(self._timeout)
        return value

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


def _empty_index_frame(fields: tuple[str, ...] = _FIELDS) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {"symbol": pl.String, "date": pl.Date}
    schema.update({field: pl.Float64 for field in fields})
    return pl.DataFrame(schema=schema)


def _index_symbol(symbol: str, *, dotted: bool = False) -> str:
    code, exchange = symbol.split(".", 1)
    prefix = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(exchange)
    if prefix is None or len(code) != 6 or not code.isdigit():
        raise ValueError(f"unsupported index symbol: {symbol}")
    return f"{prefix}.{code}" if dotted else f"{prefix}{code}"


def fetch_baostock_index_daily(
    symbols: list[str],
    start_date: date,
    end_date: date,
    *,
    timeout: float = 5.0,
) -> pl.DataFrame:
    """Fetch unadjusted index bars from BaoStock and normalize shares to lots."""
    import baostock as bs

    rows: list[dict[str, Any]] = []
    with _BAOSTOCK_LOCK:
        socket_util = None
        original_socket_module = None
        try:
            socket_util = importlib.import_module("baostock.util.socketutil")
            original_socket_module = socket_util.socket
            socket_util.socket = _SocketModuleWithTimeout(socket, timeout)
        except (AttributeError, ModuleNotFoundError):
            pass
        try:
            login = bs.login()
            if str(login.error_code) != "0":
                raise RuntimeError(f"BaoStock login failed: {login.error_code}")
            for symbol in sorted(set(symbols)):
                query = bs.query_history_k_data_plus(
                    _index_symbol(symbol, dotted=True),
                    "date,open,high,low,close,volume,amount",
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    frequency="d",
                    adjustflag="3",
                )
                if str(query.error_code) != "0":
                    continue
                while query.next():
                    values = query.get_row_data()
                    if len(values) != 7 or any(value == "" for value in values):
                        continue
                    rows.append({
                        "symbol": symbol,
                        "date": date.fromisoformat(values[0]),
                        "open": float(values[1]),
                        "high": float(values[2]),
                        "low": float(values[3]),
                        "close": float(values[4]),
                        # BaoStock index volume is shares; canonical index volume is lots.
                        "volume": float(values[5]) / 100.0,
                        "amount": float(values[6]),
                    })
        finally:
            try:
                bs.logout()
            finally:
                if socket_util is not None and original_socket_module is not None:
                    socket_util.socket = original_socket_module
    if not rows:
        return _empty_index_frame()
    return pl.DataFrame(rows).unique(["symbol", "date"], keep="last").sort(["symbol", "date"])
