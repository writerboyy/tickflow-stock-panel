"""同花顺手机持仓截图的结构化 OCR 解析。"""
from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from app.services.watchlist_ocr.pipeline import build_instrument_lookups
from app.services.watchlist_ocr.provider import OcrProvider, get_ocr_provider

TEMPLATE_VERSION = "ths_mobile_position_v1"

_HEADERS = {
    "name_code": ("证券名称", "名称代码", "证券/代码", "股票名称"),
    "quantity": ("持仓", "持仓数量", "股票余额"),
    "available": ("可用", "可用数量", "可卖"),
    "cost_price": ("成本价", "成本"),
    "current_price": ("现价", "最新价", "市价"),
    "market_value": ("市值", "参考市值"),
    "profit_loss": ("盈亏", "持仓盈亏", "浮动盈亏"),
}
_ACCOUNT_LABELS = {
    "cash": ("可用资金", "可取资金"),
    "total_asset": ("总资产", "总市值"),
    "previous_close_total_asset": ("上日资产", "昨日资产", "上日收盘总资产"),
    "account_name": ("资金账号", "账户"),
}
_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
_NUMBER_RE = re.compile(r"[-+]?\d[\d,，]*(?:\.\d+)?")


def _normalized(text: object) -> str:
    return re.sub(r"\s+", "", str(text or "")).replace("：", ":")


def _number(text: object) -> float | None:
    match = _NUMBER_RE.search(str(text or "").replace("，", ","))
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _group_lines(tokens: list[dict]) -> list[list[dict]]:
    keyed: dict[tuple[int, int, int], list[dict]] = defaultdict(list)
    loose: list[dict] = []
    for token in tokens:
        key = (
            int(token.get("block_num") or 0),
            int(token.get("par_num") or 0),
            int(token.get("line_num") or 0),
        )
        if key != (0, 0, 0):
            keyed[key].append(token)
        else:
            loose.append(token)
    lines = list(keyed.values())
    for token in sorted(loose, key=lambda item: (int(item.get("top") or 0), int(item.get("left") or 0))):
        center = int(token.get("top") or 0) + int(token.get("height") or 0) / 2
        target = next((line for line in lines if abs(
            center - mean(int(item.get("top") or 0) + int(item.get("height") or 0) / 2 for item in line)
        ) <= max(8, int(token.get("height") or 0) * 0.65)), None)
        if target is None:
            lines.append([token])
        else:
            target.append(token)
    for line in lines:
        line.sort(key=lambda item: int(item.get("left") or 0))
    return sorted(lines, key=lambda line: min(int(item.get("top") or 0) for item in line))


def _find_headers(lines: list[list[dict]]) -> tuple[int, dict[str, float]]:
    best: tuple[int, dict[str, float]] = (-1, {})
    for idx, line in enumerate(lines):
        found: dict[str, float] = {}
        for start in range(len(line)):
            text = ""
            for end in range(start, min(len(line), start + 3)):
                token = line[end]
                text += _normalized(token.get("text"))
                left = int(line[start].get("left") or 0)
                right = int(token.get("left") or 0) + int(token.get("width") or 0)
                matched = False
                for field, labels in _HEADERS.items():
                    if text in labels or (start == end and any(label in text for label in labels)):
                        found.setdefault(field, (left + right) / 2)
                        matched = True
                if matched:
                    break
        if len(found) > len(best[1]):
            best = idx, found
    if len(best[1]) < 3:
        return -1, {}
    return best


def _account_candidates(lines: list[list[dict]]) -> dict[str, dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    for line in lines:
        text = " ".join(str(token.get("text") or "") for token in line)
        normalized = _normalized(text)
        confidence = min((float(token.get("confidence") or 0) for token in line), default=0.0)
        for field, labels in _ACCOUNT_LABELS.items():
            label = next((item for item in labels if item in normalized), None)
            if not label:
                continue
            value_text = normalized.split(label, 1)[-1].lstrip(":")
            value: Any = value_text if field == "account_name" else _number(value_text)
            if value not in (None, ""):
                candidates[field] = {"value": value, "confidence": confidence}
    return candidates


def _line_code(line: list[dict]) -> str | None:
    line_text = " ".join(str(token.get("text") or "") for token in line)
    if match := _CODE_RE.search(line_text):
        return match.group(1)
    for start in range(len(line)):
        candidate = ""
        for token in line[start:start + 3]:
            text = _normalized(token.get("text"))
            if not text.isdigit():
                break
            candidate += text
            if len(candidate) == 6:
                return candidate
            if len(candidate) > 6:
                break
    return None


def parse_position_tokens(tokens: list[dict], data_dir: Path) -> dict[str, Any]:
    """解析结构化 token；不返回 OCR 全文。"""
    lines = _group_lines(tokens)
    header_idx, headers = _find_headers(lines)
    issues: list[dict[str, Any]] = []
    if header_idx < 0:
        return {
            "template_version": TEMPLATE_VERSION,
            "account_candidates": _account_candidates(lines),
            "positions": [],
            "issues": [{"level": "error", "code": "headers_not_found", "message": "未识别到同花顺持仓表头"}],
        }

    ordered = sorted(headers.items(), key=lambda item: item[1])
    code_to_symbol, symbol_to_name = build_instrument_lookups(data_dir)
    positions: list[dict[str, Any]] = []
    for line in lines[header_idx + 1:]:
        code = _line_code(line)
        if not code:
            continue
        buckets: dict[str, list[dict]] = defaultdict(list)
        for token in line:
            center = int(token.get("left") or 0) + int(token.get("width") or 0) / 2
            field = min(ordered, key=lambda item: abs(center - item[1]))[0]
            buckets[field].append(token)

        def field_text(field: str) -> str:
            return "".join(str(token.get("text") or "") for token in buckets.get(field, []))

        confidences = {
            field: min((float(token.get("confidence") or 0) for token in values), default=0.0)
            for field, values in buckets.items()
        }
        name_text = field_text("name_code").replace(code, "").strip(" /-")
        symbol = code_to_symbol.get(code)
        master_name = symbol_to_name.get(symbol or "")
        row = {
            "code": code,
            "symbol": symbol,
            "name": master_name or name_text or None,
            "quantity": _number(field_text("quantity")),
            "available": _number(field_text("available")),
            "cost_price": _number(field_text("cost_price")),
            "current_price": _number(field_text("current_price")),
            "market_value": _number(field_text("market_value")),
            "profit_loss": _number(field_text("profit_loss")),
            "field_confidence": confidences,
            "requires_review": False,
        }
        row_issues: list[str] = []
        if symbol is None:
            row_issues.append("代码未匹配本地股票/ETF主数据")
        if name_text and master_name and name_text not in master_name and master_name not in name_text:
            row_issues.append("代码与名称不一致")
        for field in ("quantity", "cost_price"):
            if row[field] is None or confidences.get(field, 0) < 70:
                row_issues.append(f"{field} 需要人工校正")
        row["issues"] = row_issues
        row["requires_review"] = bool(row_issues)
        positions.append(row)

    if not positions:
        issues.append({"level": "error", "code": "positions_not_found", "message": "未识别到持仓行"})
    return {
        "template_version": TEMPLATE_VERSION,
        "account_candidates": _account_candidates(lines),
        "positions": positions,
        "issues": issues,
    }


def import_position_image(
    image_bytes: bytes,
    data_dir: Path,
    *,
    provider: OcrProvider | None = None,
) -> dict[str, Any]:
    ocr = provider or get_ocr_provider()
    if not ocr.available():
        raise RuntimeError("OCR 引擎不可用，请先安装 Tesseract 及简体中文语言包")
    return {"provider": ocr.name, **parse_position_tokens(ocr.extract_tokens(image_bytes), data_dir)}
