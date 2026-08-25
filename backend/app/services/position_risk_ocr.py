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
    "cash": ("可用资金", "可取资金", "可用"),
    "total_asset": ("总资产",),
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


def _token_center(token: dict) -> float:
    return int(token.get("left") or 0) + int(token.get("width") or 0) / 2


def _account_label_spans(line: list[dict], labels: tuple[str, ...]) -> list[tuple[int, int, str]]:
    spans: list[tuple[int, int, str]] = []
    for start in range(len(line)):
        text = ""
        for end in range(start, min(len(line), start + 5)):
            text += _normalized(line[end].get("text"))
            if text not in labels:
                continue
            if text == "可用":
                tail = "".join(
                    _normalized(token.get("text")) for token in line[end + 1:end + 3]
                )
                if tail.startswith("保证金"):
                    continue
            spans.append((start, end, text))
    return spans


def _account_candidates(
    lines: list[list[dict]],
    *,
    stop_idx: int | None = None,
) -> dict[str, dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    limit = len(lines) if stop_idx is None or stop_idx < 0 else stop_idx
    for line_idx, line in enumerate(lines[:limit]):
        for field, labels in _ACCOUNT_LABELS.items():
            spans = _account_label_spans(line, labels)
            if not spans:
                continue
            for start, end, _label in spans:
                label_tokens = line[start:end + 1]
                label_confidence = min(
                    (float(token.get("confidence") or 0) for token in label_tokens),
                    default=0.0,
                )
                if field == "account_name":
                    value_text = "".join(
                        str(token.get("text") or "") for token in line[end + 1:]
                    ).lstrip(":：")
                    if value_text:
                        value_confidence = min(
                            (float(token.get("confidence") or 0) for token in line[end + 1:]),
                            default=label_confidence,
                        )
                        candidates.setdefault(field, {
                            "value": value_text,
                            "confidence": min(label_confidence, value_confidence),
                        })
                    continue

                label_center = mean(_token_center(token) for token in label_tokens)
                numeric: list[tuple[float, dict, float]] = []
                for candidate_idx in range(line_idx, min(limit, line_idx + 2)):
                    candidate_line = lines[candidate_idx]
                    for token_idx, token in enumerate(candidate_line):
                        if candidate_idx == line_idx and start <= token_idx <= end:
                            continue
                        value = _number(token.get("text"))
                        if value is None:
                            continue
                        vertical = abs(
                            int(token.get("top") or 0)
                            - mean(int(item.get("top") or 0) for item in label_tokens)
                        )
                        score = abs(_token_center(token) - label_center) + vertical * 2
                        numeric.append((score, token, value))
                if numeric:
                    _, value_token, value = min(numeric, key=lambda item: item[0])
                    candidates.setdefault(field, {
                        "value": value,
                        "confidence": min(
                            label_confidence,
                            float(value_token.get("confidence") or 0),
                        ),
                    })
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


def _nearest_numeric_token(
    line: list[dict],
    anchor: float,
    *,
    minimum_x: float | None = None,
    maximum_x: float | None = None,
) -> dict | None:
    candidates = []
    for token in line:
        if _number(token.get("text")) is None:
            continue
        center = _token_center(token)
        if minimum_x is not None and center < minimum_x:
            continue
        if maximum_x is not None and center >= maximum_x:
            continue
        candidates.append(token)
    return min(candidates, key=lambda token: abs(_token_center(token) - anchor), default=None)


def _parse_margin_rows(
    lines: list[list[dict]],
    header_idx: int,
    headers: dict[str, float],
    symbol_to_name: dict[str, str],
) -> list[dict[str, Any]]:
    quantity_x = headers["quantity"]
    price_anchors = [headers[field] for field in ("cost_price", "current_price") if field in headers]
    price_x = mean(price_anchors)
    quantity_price_split = (quantity_x + price_x) / 2
    names: dict[str, list[str]] = defaultdict(list)
    for symbol, name in symbol_to_name.items():
        names[_normalized(name)].append(symbol)

    positions: list[dict[str, Any]] = []
    for row_idx in range(header_idx + 1, len(lines)):
        primary = lines[row_idx]
        secondary = lines[row_idx + 1] if row_idx + 1 < len(lines) else []
        quantity_token = _nearest_numeric_token(
            primary,
            quantity_x,
            maximum_x=quantity_price_split,
        )
        cost_token = _nearest_numeric_token(
            primary,
            price_x,
            minimum_x=quantity_price_split,
        )
        if quantity_token is None or cost_token is None:
            continue

        name_tokens: list[dict] = []
        for token in primary:
            if _number(token.get("text")) is not None:
                break
            text = _normalized(token.get("text"))
            if text and text not in {"/", "-"}:
                name_tokens.append(token)
        name_text = "".join(str(token.get("text") or "") for token in name_tokens).strip()
        if not name_text or not re.search(r"[\u4e00-\u9fff]", name_text):
            continue

        available_token = _nearest_numeric_token(
            secondary,
            quantity_x,
            maximum_x=quantity_price_split,
        )
        current_token = _nearest_numeric_token(
            secondary,
            price_x,
            minimum_x=quantity_price_split,
        )
        used_primary = {id(quantity_token), id(cost_token)}
        profit_token = min(
            (
                token for token in primary
                if id(token) not in used_primary and _number(token.get("text")) is not None
            ),
            key=_token_center,
            default=None,
        )
        used_secondary = {id(token) for token in (available_token, current_token) if token is not None}
        market_value_token = min(
            (
                token for token in secondary
                if id(token) not in used_secondary and _number(token.get("text")) is not None
            ),
            key=_token_center,
            default=None,
        )

        matches = names.get(_normalized(name_text), [])
        symbol = matches[0] if len(matches) == 1 else None
        master_name = symbol_to_name.get(symbol or "")
        code = (symbol or "").split(".", 1)[0]
        confidence_tokens = {
            "name_code": name_tokens,
            "quantity": [quantity_token],
            "available": [available_token] if available_token else [],
            "cost_price": [cost_token],
            "current_price": [current_token] if current_token else [],
            "market_value": [market_value_token] if market_value_token else [],
            "profit_loss": [profit_token] if profit_token else [],
        }
        confidences = {
            field: min(
                (float(token.get("confidence") or 0) for token in tokens),
                default=0.0,
            )
            for field, tokens in confidence_tokens.items()
        }
        row_issues: list[str] = []
        if symbol is None:
            row_issues.append(
                "证券名称在本地股票/ETF主数据中未唯一匹配"
                if not matches else "证券名称对应多个本地代码"
            )
        if confidences["name_code"] < 70:
            row_issues.append("证券名称需要人工校正")
        for field in ("quantity", "cost_price"):
            token = quantity_token if field == "quantity" else cost_token
            if _number(token.get("text")) is None or confidences[field] < 70:
                row_issues.append(f"{field} 需要人工校正")
        positions.append({
            "code": code,
            "symbol": symbol,
            "name": master_name or name_text,
            "quantity": _number(quantity_token.get("text")),
            "available": _number(available_token.get("text")) if available_token else None,
            "cost_price": _number(cost_token.get("text")),
            "current_price": _number(current_token.get("text")) if current_token else None,
            "market_value": _number(market_value_token.get("text")) if market_value_token else None,
            "profit_loss": _number(profit_token.get("text")) if profit_token else None,
            "field_confidence": confidences,
            "requires_review": bool(row_issues),
            "issues": row_issues,
        })
    return positions


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

    code_to_symbol, symbol_to_name = build_instrument_lookups(data_dir)
    if (
        "name_code" not in headers
        and "market_value" in headers
        and "quantity" in headers
        and ({"cost_price", "current_price"} & headers.keys())
    ):
        positions = _parse_margin_rows(lines, header_idx, headers, symbol_to_name)
        issues = [] if positions else [
            {"level": "error", "code": "positions_not_found", "message": "未识别到持仓行"}
        ]
        return {
            "template_version": TEMPLATE_VERSION,
            "account_candidates": _account_candidates(lines, stop_idx=header_idx),
            "positions": positions,
            "issues": issues,
        }

    ordered = sorted(headers.items(), key=lambda item: item[1])
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
        "account_candidates": _account_candidates(lines, stop_idx=header_idx),
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
    if not ocr.supports_language("chi_sim"):
        raise RuntimeError(
            "持仓 OCR 缺少简体中文语言包 chi_sim；macOS 安装 tesseract-lang，"
            "Debian/Ubuntu 安装 tesseract-ocr-chi-sim，然后重启项目"
        )
    return {"provider": ocr.name, **parse_position_tokens(ocr.extract_tokens(image_bytes), data_dir)}
