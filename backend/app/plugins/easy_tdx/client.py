"""EasyTDX 行业数据边界。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from calendar import monthrange
from datetime import date
import re
from typing import Any


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


def _number(value: str) -> float:
    return float(value.replace(",", ""))


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
