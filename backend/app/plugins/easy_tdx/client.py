"""EasyTDX 行业数据边界。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import json
import os
import re
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.services.stock_dividends import cash_per_share_from_plan


def _text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _code(value: Any) -> str:
    text = _text(value)
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text.zfill(6) if text.isdigit() and len(text) <= 6 else text


def _symbol(code: str) -> str | None:
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    return None


def _f10_symbol(code: str) -> str | None:
    symbol = _symbol(code)
    if symbol:
        return symbol
    if code.startswith(("4", "8", "9")):
        return f"{code}.BJ"
    return None


def normalize_industry_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    """Only retain the two dimensions that TickFlow and Kaipanla do not provide."""
    result = []
    for row in rows:
        code = _code(row.get("code"))
        symbol = _symbol(code)
        if symbol is None:
            continue
        result.append({
            "symbol": symbol,
            "code": code,
            "industry_sw": _text(row.get("industry_sw")),
            "industry_tdx": _text(row.get("industry_tdx")),
        })
    return result


def fetch_industry_rows(timeout: float = 5.0) -> list[dict[str, str]]:
    """Fetch the public EasyTDX security list without importing it at app startup."""
    from easy_tdx import TdxClient

    with TdxClient.from_best_host(timeout=timeout, heartbeat_interval=0) as client:
        frame = client.get_security_list_all()
    rows = frame.to_dict(orient="records")
    return normalize_industry_rows(rows)


_HEADER = re.compile(r"◇(?P<code>\d{6})\s+(?P<name>.*?)\s+更新日期：(?P<updated>\d{4}-\d{2}-\d{2})◇")
_MARGIN_ROW = re.compile(
    r"│(?P<trade_date>20\d{2}-\d{2}-\d{2})\s*│\s*(?P<margin_balance>-?[\d,.]+)\s*│\s*"
    r"(?P<margin_purchase>-?[\d,.]+)\s*│\s*(?P<short_balance>-?[\d,.]+)\s*│\s*"
    r"(?P<short_sell>-?[\d,.]+)\s*│\s*(?P<total_balance>-?[\d,.]+)",
)
_FORECAST = re.compile(
    r"(?P<announcement_date>20\d{2}-\d{2}-\d{2})\s+预告业绩:(?P<forecast_type>[^\n]+)\n"
    r".*?(?P<year>20\d{2})年(?P<start_month>\d{2})-(?P<end_month>\d{2})月.*?净利润为"
    r"(?P<low>-?[\d.]+)万元(?:至(?P<high>-?[\d.]+)万元)?，与上年同期相比变动幅度为"
    r"(?P<yoy_low>-?[\d.]+)%(?:至(?P<yoy_high>-?[\d.]+)%)?",
    re.S,
)
_TQLEX_URL = "http://static.tdx.com.cn:7615/TQLEX"


def _number(value: str) -> float:
    return float(value.replace(",", ""))


def _date_text(value: Any) -> str | None:
    text = _text(value)
    if not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", text):
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _cash_per_share(plan: Any) -> float | None:
    return cash_per_share_from_plan(_text(plan))


def _section(text: str, marker: str) -> str | None:
    start = text.find(marker)
    if start < 0:
        return None
    end = text.find("├", start)
    return text[start:] if end < 0 else text[start:end]


def _plain(section: str) -> str:
    return "\n".join(line.replace("│", "").strip() for line in section.splitlines() if line.strip())


def _identity(text: str, fallback_code: str) -> dict[str, str] | None:
    match = _HEADER.search(text)
    code = _code(match.group("code") if match else fallback_code)
    symbol = _f10_symbol(code)
    if symbol is None:
        return None
    return {"symbol": symbol, "code": code, "name": _text(match.group("name")) if match else ""}


def parse_f10_reference(text: str, fallback_code: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse only explicit F10 reference sections; other text is not financial data."""
    identity = _identity(text, fallback_code)
    if identity is None:
        return [], [], []
    # The F10 response has only one financing heading in its contents list;
    # the actual table is embedded later in the same prompt text.
    margins = [
        {
            **identity,
            "report_date": match.group("trade_date"),
            "margin_balance_10k": _number(match.group("margin_balance")),
            "margin_purchase_10k": _number(match.group("margin_purchase")),
            "short_balance_10k": _number(match.group("short_balance")),
            "short_sell_10k_shares": _number(match.group("short_sell")),
            "margin_short_balance_10k": _number(match.group("total_balance")),
        }
        for match in _MARGIN_ROW.finditer(text)
    ] if "融资余额(万元)" in text else []
    forecasts: list[dict] = []
    forecast_section = _section(text, "●业绩预告:")
    if forecast_section:
        plain = _plain(forecast_section)
        match = _FORECAST.search(plain)
        if match:
            year, month = int(match.group("year")), int(match.group("end_month"))
            forecasts.append(
                {
                    **identity,
                    "report_date": match.group("announcement_date"),
                    "announcement_date": match.group("announcement_date"),
                    "report_period": date(year, month, monthrange(year, month)[1]).isoformat(),
                    "forecast_type": _text(match.group("forecast_type")),
                    "net_profit_low_10k": _number(match.group("low")),
                    "net_profit_high_10k": _number(match.group("high") or match.group("low")),
                    "net_profit_yoy_low_pct": _number(match.group("yoy_low")),
                    "net_profit_yoy_high_pct": _number(match.group("yoy_high") or match.group("yoy_low")),
                    "summary": plain,
                }
            )
    expresses: list[dict] = []
    express_section = _section(text, "●业绩快报:")
    if express_section:
        plain = _plain(express_section)
        dates = re.findall(r"20\d{2}-\d{2}-\d{2}", plain)
        if dates:
            expresses.append(
                {
                    **identity,
                    "report_date": dates[0],
                    "announcement_date": dates[0],
                    "summary": plain,
                }
            )
    return margins, forecasts, expresses


def fetch_f10_texts(codes: Iterable[str], timeout: float = 8.0) -> list[tuple[str, str]]:
    """Fetch the current F10 prompt text for A-share codes through EasyTDX only."""
    from easy_tdx import TdxClient
    from easy_tdx.models.enums import Market

    records = []
    with TdxClient.from_best_host(timeout=timeout, heartbeat_interval=0) as client:
        for raw_code in codes:
            code = _code(raw_code)
            symbol = _f10_symbol(code)
            if symbol is None:
                continue
            market = Market.SH if symbol.endswith(".SH") else Market.SZ if symbol.endswith(".SZ") else Market.BJ
            try:
                category = client.get_company_info_category(market, code).iloc[0]
                content = client.get_company_info_content(
                    market,
                    code,
                    str(category["filename"]),
                    int(category["start"]),
                    int(category["length"]),
                )
            except Exception:
                continue
            records.append((code, content))
    return records


def _fetch_dividend_history_for_code(
    code: str,
    symbol: str,
    url: str,
    timeout: float,
) -> tuple[list[dict], str | None]:
    request = Request(
        url,
        data=json.dumps({"Params": [code, "fh"]}, separators=(",", ":")).encode(),
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json",
            "User-Agent": "TickFlow EasyTDX",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8-sig"))
    except Exception as exc:  # noqa: BLE001
        return [], type(exc).__name__
    if not isinstance(payload, dict) or payload.get("ErrorCode", 0) not in (0, "0", None):
        return [], "invalid_response"
    result_sets = payload.get("ResultSets") or []
    if not result_sets or not isinstance(result_sets[0], dict):
        return [], "missing_result_set"
    table = result_sets[0]
    columns = table.get("ColName") or []
    if not isinstance(columns, list):
        return [], "invalid_columns"
    records: list[dict] = []
    for source_row in table.get("Content") or []:
        if not isinstance(source_row, list):
            continue
        row = dict(zip((str(column) for column in columns), source_row, strict=False))
        record_date = _date_text(row.get("T021"))
        plan = _text(row.get("T004"))
        cash_per_share = _cash_per_share(plan)
        if (
            record_date is None
            or cash_per_share is None
            or _text(row.get("aT036")) != "036003"
        ):
            continue
        records.append({
            "symbol": symbol,
            "code": code,
            "report_date": record_date,
            "record_date": record_date,
            "ex_dividend_date": _date_text(row.get("T023")),
            "board_date": _date_text(row.get("T003")),
            "plan": plan,
            "cash_per_share": cash_per_share,
            "progress": _text(row.get("T036")),
            "progress_code": _text(row.get("aT036")),
            "source": "tdx_7615_f10",
        })
    return records, None


def fetch_dividend_history_batch(
    codes: Iterable[str],
    timeout: float = 10.0,
    workers: int = 4,
) -> tuple[list[dict], dict[str, str]]:
    """Fetch a bounded code batch and retain per-code failures for retry."""
    normalized = []
    for raw_code in codes:
        code = _code(raw_code)
        symbol = _f10_symbol(code)
        if symbol is not None:
            normalized.append((code, symbol))
    if not normalized:
        return [], {}
    base_url = os.getenv("EASY_TDX_TQLEX_URL", _TQLEX_URL).rstrip("?")
    entry = "CWServ.tdxf10_gg_fhrz"
    url = f"{base_url}{'&' if '?' in base_url else '?'}{urlencode({'Entry': entry})}"
    records: list[dict] = []
    failures: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=min(max(1, workers), len(normalized))) as executor:
        futures = {
            executor.submit(_fetch_dividend_history_for_code, code, symbol, url, timeout): code
            for code, symbol in normalized
        }
        for future in as_completed(futures):
            code = futures[future]
            try:
                rows, error = future.result()
            except Exception as exc:  # noqa: BLE001
                failures[code] = type(exc).__name__
                continue
            if error is not None:
                failures[code] = error
            else:
                records.extend(rows)
    return records, failures


def fetch_dividend_history_rows(codes: Iterable[str], timeout: float = 10.0) -> list[dict]:
    """Fetch implemented cash dividends from the TDX 7615 history page.

    EasyTDX's company-info API exposes only the current prompt text, while this
    public TDX F10 page retains historical registration dates.  Rows without a
    concrete record date or cash amount are deliberately discarded.
    """
    records, _failures = fetch_dividend_history_batch(codes, timeout=timeout)
    return records
