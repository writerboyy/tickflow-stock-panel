"""Read-only backup market-data sources used after canonical quality failures."""

from __future__ import annotations

from datetime import date
import importlib
import json
import socket
import threading
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import httpx
import polars as pl


ASTOCK_DATA_RECIPE_REVISION = "715b9f47f098aef393713a03f1f37cb3c7eef93b"
_FIELDS = ("open", "high", "low", "close", "volume", "amount")
_BAOSTOCK_LOCK = threading.Lock()
_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}


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


def _eastmoney_secid(symbol: str) -> str:
    code, exchange = symbol.split(".", 1)
    market = {"SH": "1", "SZ": "0", "BJ": "0"}.get(exchange)
    if market is None:
        raise ValueError(f"unsupported index symbol: {symbol}")
    return f"{market}.{code}"


def _request_json(
    client: httpx.Client,
    url: str,
    params: dict[str, str],
) -> Any:
    last_error: Exception | None = None
    for _ in range(2):
        try:
            response = client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            last_error = exc
    try:
        headers = {key: value for key, value in client.headers.items()}
        request = Request(f"{url}?{urlencode(params)}", headers=headers)
        with urlopen(request, timeout=15.0) as response:
            return json.load(response)
    except (OSError, ValueError) as exc:
        last_error = exc
    assert last_error is not None
    raise last_error


def fetch_astock_data_eastmoney_index_daily(
    symbols: list[str],
    start_date: date,
    end_date: date,
    *,
    timeout: float = 15.0,
) -> pl.DataFrame:
    """Use A-Stock-Data's Eastmoney recipe for unadjusted index OHLCVA."""
    rows: list[dict[str, Any]] = []
    with httpx.Client(timeout=timeout, headers=_HTTP_HEADERS, trust_env=False) as client:
        for symbol in sorted(set(symbols)):
            payload = _request_json(
                client,
                "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                {
                    "secid": _eastmoney_secid(symbol),
                    "fields1": "f1,f2,f3,f4,f5,f6",
                    "fields2": "f51,f52,f53,f54,f55,f56,f57",
                    "klt": "101",
                    "fqt": "0",
                    "beg": start_date.strftime("%Y%m%d"),
                    "end": end_date.strftime("%Y%m%d"),
                    "lmt": "5000",
                },
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            klines = data.get("klines") if isinstance(data, dict) else None
            for raw in klines or []:
                values = str(raw).split(",")
                if len(values) < 7:
                    raise ValueError(f"Eastmoney index row has {len(values)} fields")
                rows.append({
                    "symbol": symbol,
                    "date": date.fromisoformat(values[0]),
                    "open": float(values[1]),
                    "close": float(values[2]),
                    "high": float(values[3]),
                    "low": float(values[4]),
                    "volume": float(values[5]),
                    "amount": float(values[6]),
                })
    if not rows:
        return _empty_index_frame()
    return pl.DataFrame(rows).unique(["symbol", "date"], keep="last").sort(["symbol", "date"])


def _year_ranges(start_date: date, end_date: date) -> list[tuple[date, date]]:
    return [
        (max(start_date, date(year, 1, 1)), min(end_date, date(year, 12, 31)))
        for year in range(start_date.year, end_date.year + 1)
    ]


def fetch_astock_data_tencent_index_daily(
    symbols: list[str],
    start_date: date,
    end_date: date,
    *,
    timeout: float = 15.0,
) -> pl.DataFrame:
    """Use A-Stock-Data's Tencent recipe for unadjusted index OHLCV."""
    fields = ("open", "high", "low", "close", "volume")
    rows: list[dict[str, Any]] = []
    headers = {**_HTTP_HEADERS, "Referer": "https://gu.qq.com/"}
    with httpx.Client(timeout=timeout, headers=headers, trust_env=False) as client:
        for symbol in sorted(set(symbols)):
            ticker = _index_symbol(symbol)
            for range_start, range_end in _year_ranges(start_date, end_date):
                payload = _request_json(
                    client,
                    "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
                    {
                        "param": (
                            f"{ticker},day,{range_start.isoformat()},"
                            f"{range_end.isoformat()},640,"
                        )
                    },
                )
                data = payload.get("data") if isinstance(payload, dict) else None
                item = data.get(ticker) if isinstance(data, dict) else None
                values_list = item.get("day") if isinstance(item, dict) else None
                if not values_list and isinstance(item, dict):
                    values_list = item.get("qfqday")
                for values in values_list or []:
                    if not isinstance(values, list) or len(values) < 6:
                        raise ValueError("Tencent index row is missing OHLCV fields")
                    rows.append({
                        "symbol": symbol,
                        "date": date.fromisoformat(str(values[0])),
                        "open": float(values[1]),
                        "close": float(values[2]),
                        "high": float(values[3]),
                        "low": float(values[4]),
                        "volume": float(values[5]),
                    })
    if not rows:
        return _empty_index_frame(fields)
    return pl.DataFrame(rows).unique(["symbol", "date"], keep="last").sort(["symbol", "date"])


def fetch_astock_data_baidu_index_daily(
    symbols: list[str],
    start_date: date,
    end_date: date,
    *,
    timeout: float = 15.0,
) -> pl.DataFrame:
    """Use A-Stock-Data's Baidu recipe with the index-specific request group."""
    rows: list[dict[str, Any]] = []
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/vnd.finance-web.v1+json",
        "Origin": "https://gushitong.baidu.com",
        "Referer": "https://gushitong.baidu.com/",
    }
    with httpx.Client(timeout=timeout, headers=headers, trust_env=False) as client:
        for symbol in sorted(set(symbols)):
            payload = _request_json(
                client,
                "https://finance.pae.baidu.com/selfselect/getstockquotation",
                {
                    "all": "1",
                    "isIndex": "true",
                    "isBk": "false",
                    "isBlock": "false",
                    "isFutures": "false",
                    "isStock": "false",
                    "newFormat": "1",
                    "group": "quotation_index_kline",
                    "market_type": "ab",
                    "finClientType": "pc",
                    "code": symbol.split(".", 1)[0],
                    "start_time": "",
                    "ktype": "1",
                },
            )
            result = payload.get("Result") if isinstance(payload, dict) else None
            market = result.get("newMarketData") if isinstance(result, dict) else None
            keys = market.get("keys") if isinstance(market, dict) else None
            raw_rows = market.get("marketData") if isinstance(market, dict) else None
            if not isinstance(keys, list) or not isinstance(raw_rows, str):
                continue
            for raw in raw_rows.split(";"):
                values = raw.split(",")
                if not raw or len(values) != len(keys):
                    continue
                item = dict(zip(keys, values, strict=True))
                trade_date = date.fromisoformat(item["time"])
                if not start_date <= trade_date <= end_date:
                    continue
                rows.append({
                    "symbol": symbol,
                    "date": trade_date,
                    "open": float(item["open"]),
                    "close": float(item["close"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    # Baidu index volume is shares; canonical index volume is lots.
                    "volume": float(item["volume"]) / 100.0,
                    "amount": float(item["amount"]),
                })
    if not rows:
        return _empty_index_frame()
    return pl.DataFrame(rows).unique(["symbol", "date"], keep="last").sort(["symbol", "date"])


def astock_data_source_metadata() -> dict[str, str]:
    return {
        "recipe": "simonlin1212/a-stock-data",
        "recipe_revision": ASTOCK_DATA_RECIPE_REVISION,
        "eastmoney_endpoint": "push2his.eastmoney.com/api/qt/stock/kline/get",
        "tencent_endpoint": "web.ifzq.gtimg.cn/appstock/app/fqkline/get",
        "baidu_endpoint": "finance.pae.baidu.com/selfselect/getstockquotation",
        "sina_financial_endpoint": (
            "quotes.sina.cn/cn/api/openapi.php/"
            "CompanyFinanceService.getFinanceReport2022"
        ),
    }


_SINA_FINANCIAL_FIELDS = {
    "基本每股收益": "eps_basic",
    "稀释每股收益": "eps_diluted",
    "每股净资产": "bps",
    "净资产收益率_平均": "roe",
    "归属母公司净利润增长率": "net_income_yoy",
    "每股经营现金流": "ocfps",
    "扣非净利润": "net_income_deducted",
}


def fetch_astock_data_sina_financial_reference(
    keys: list[tuple[str, str]],
    *,
    timeout: float = 15.0,
) -> dict[tuple[str, str], dict[str, float]]:
    """Fetch current statement values; Sina does not expose PIT revision metadata."""
    result: dict[tuple[str, str], dict[str, float]] = {}
    headers = {**_HTTP_HEADERS, "Referer": "https://finance.sina.com.cn/"}
    with httpx.Client(timeout=timeout, headers=headers, trust_env=False) as client:
        for symbol, period_end in sorted(set(keys)):
            payload = _request_json(
                client,
                (
                    "https://quotes.sina.cn/cn/api/openapi.php/"
                    "CompanyFinanceService.getFinanceReport2022"
                ),
                {
                    "paperCode": _index_symbol(symbol),
                    "source": "gjzb",
                    "type": "0",
                    "page": "1",
                    "num": "100",
                },
            )
            report_list = (
                ((payload.get("result") or {}).get("data") or {}).get("report_list") or {}
                if isinstance(payload, dict)
                else {}
            )
            period = period_end.replace("-", "")
            report = report_list.get(period) if isinstance(report_list, dict) else None
            values: dict[str, float] = {}
            for item in report.get("data") or [] if isinstance(report, dict) else []:
                field = _SINA_FINANCIAL_FIELDS.get(str(item.get("item_title") or ""))
                value = item.get("item_value")
                if field and value not in (None, ""):
                    values[field] = float(value)
            if values:
                result[(symbol, period_end)] = values
    return result
