"""EasyTDX 行业数据边界。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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


def fetch_industry_rows(timeout: float = 30.0) -> list[dict[str, str]]:
    """Fetch the public EasyTDX security list without importing it at app startup."""
    from easy_tdx import TdxClient

    with TdxClient.from_best_host(timeout=timeout) as client:
        frame = client.get_security_list_all()
    rows = frame.to_dict(orient="records")
    return normalize_industry_rows(rows)
