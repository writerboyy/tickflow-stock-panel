"""把持仓风控快照聚合为可解释的人工决策提示。"""
from __future__ import annotations

import math
from typing import Any


ACTION_LABELS = {
    "hold": "持有",
    "observe": "观察",
    "reduce_25": "减仓 25%",
    "reduce_50": "减仓 50%",
    "exit": "退出",
}


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _block(status: str, reason: str, **values: Any) -> dict[str, Any]:
    return {"status": status, "reason": reason, **values}


def _quality_blocks(
    feature: dict[str, Any],
    context: dict[str, Any],
    daily: dict[str, Any],
    price: float | None,
) -> dict[str, dict[str, Any]]:
    context_missing = set(str(value) for value in context.get("missing") or [])
    quote_available = price is not None
    technical_available = bool(
        feature.get("available")
        and feature.get("fresh")
        and any(
            _finite(feature.get(key)) is not None
            for key in ("session_vwap", "ema9_1m", "ema20_1m", "atr14_5m", "momentum_5m")
        )
    )
    flow_available = (
        int(_finite(feature.get("flow_samples")) or 0) > 0
        and (
            _finite(feature.get("buy_ratio")) is not None
            or _finite(feature.get("sell_ratio")) is not None
        )
    )
    market_available = context.get("state") != "unavailable" and "market" not in context_missing
    context_status = "available" if not context_missing and context.get("state") != "unavailable" else (
        "partial" if market_available else "missing"
    )
    return {
        "quote": _block("available" if quote_available else "missing", "实时价格已获取" if quote_available else "缺少实时价格"),
        "daily": _block(
            "available" if daily.get("available") else "missing",
            str(daily.get("reason") or ("日线数据已获取" if daily.get("available") else "缺少日线数据")),
            as_of=daily.get("as_of"),
        ),
        "technical": _block(
            "available" if technical_available else "missing",
            "技术指标已计算" if technical_available else str(feature.get("reason") or "技术指标不可用"),
            as_of=feature.get("as_of"),
        ),
        "fund_flow": _block(
            "available" if flow_available else "missing",
            "资金流样本已达到计算要求" if flow_available else "资金流样本不足或未接入",
            samples=int(_finite(feature.get("flow_samples")) or 0),
        ),
        "market_context": _block(
            context_status,
            "市场、板块上下文已完整" if context_status == "available" else (
                "；".join(context_missing) if context_missing else "市场上下文不可用"
            ),
            missing=sorted(context_missing),
        ),
        "news": _block("not_supported", "当前未接入个股新闻源，新闻不参与本次判断"),
    }


def build_position_decision(
    feature: dict[str, Any],
    *,
    position: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """以快照证据生成统一动作；缺少核心数据时只允许观察。"""
    position = position or {}
    quote = quote or {}
    context = dict(feature.get("context") or {})
    daily = dict(feature.get("daily") or {})
    price = _finite(feature.get("last_price")) or _finite(quote.get("last_price")) or _finite(position.get("import_price"))
    cost = _finite(position.get("cost_price"))
    quality = _quality_blocks(feature, context, daily, price)
    missing = [
        block["reason"] for block in quality.values()
        if block["status"] in {"missing", "not_supported"}
    ]
    core_ready = all(quality[name]["status"] == "available" for name in ("quote", "daily"))

    evidence: list[dict[str, str]] = []
    watch_conditions: list[str] = []
    negative_count = 0
    positive_count = 0

    context_state = str(context.get("state") or "unavailable")
    if context_state in {"weakening", "divergent"}:
        negative_count += 1
        evidence.append({"source": "market_context", "label": "市场上下文走弱", "detail": context_state})
    elif context_state == "supportive":
        positive_count += 1
        evidence.append({"source": "market_context", "label": "市场上下文支持", "detail": "板块和市场环境偏支持"})
    elif context_state == "unavailable":
        watch_conditions.append("等待市场、板块或相关性数据恢复")

    sell_ratio = _finite(feature.get("sell_ratio"))
    buy_ratio = _finite(feature.get("buy_ratio"))
    imbalance = _finite(feature.get("orderbook_imbalance"))
    if sell_ratio is not None and sell_ratio >= 0.60:
        negative_count += 1
        evidence.append({"source": "fund_flow", "label": "卖方资金占优", "detail": f"卖方占比 {sell_ratio:.0%}"})
    elif buy_ratio is not None and buy_ratio >= 0.55:
        positive_count += 1
        evidence.append({"source": "fund_flow", "label": "买方资金占优", "detail": f"买方占比 {buy_ratio:.0%}"})
    if imbalance is not None and imbalance <= -0.35:
        negative_count += 1
        evidence.append({"source": "fund_flow", "label": "盘口偏空", "detail": f"盘口失衡 {imbalance:.2f}"})

    vwap = _finite(feature.get("session_vwap"))
    ema20 = _finite(feature.get("ema20_1m")) or _finite(feature.get("ema20_5m"))
    momentum = _finite(feature.get("momentum_5m"))
    if price is not None and vwap is not None and price < vwap:
        negative_count += 1
        evidence.append({"source": "technical", "label": "价格低于分时均价", "detail": f"现价 {price:.3f} < VWAP {vwap:.3f}"})
    elif price is not None and vwap is not None and price >= vwap:
        positive_count += 1
        evidence.append({"source": "technical", "label": "价格站在分时均价上方", "detail": f"现价 {price:.3f} >= VWAP {vwap:.3f}"})
    if price is not None and ema20 is not None and price < ema20:
        negative_count += 1
        evidence.append({"source": "technical", "label": "价格低于 EMA20", "detail": f"现价 {price:.3f} < EMA20 {ema20:.3f}"})
    if momentum is not None and momentum < 0:
        negative_count += 1
        evidence.append({"source": "technical", "label": "短线动能为负", "detail": f"5m 动能 {momentum:.2%}"})
    elif momentum is not None and momentum > 0:
        positive_count += 1
        evidence.append({"source": "technical", "label": "短线动能为正", "detail": f"5m 动能 {momentum:.2%}"})

    return_pct = _finite(position.get("profit_loss_pct"))
    if return_pct is None and price is not None and cost:
        return_pct = price / cost - 1
    limit_up = _finite(feature.get("limit_up")) or _finite(quote.get("limit_up"))
    limit_down = _finite(feature.get("limit_down")) or _finite(quote.get("limit_down"))
    hard_stop = _finite(feature.get("hard_stop_price"))
    hard_guard = bool(
        price is not None and (
            (hard_stop is not None and price <= hard_stop)
            or (limit_down is not None and price <= limit_down + 0.001)
        )
    )
    at_limit_up = bool(limit_up is not None and price is not None and price >= limit_up - max(0.001, limit_up * 1e-6))
    conflicting = bool(
        positive_count > 0
        and negative_count > 0
        and context_state not in {"weakening", "divergent"}
    )
    if hard_guard:
        reason = "触发硬止损或跌停保护，退出建议需要人工确认委托"
        action = "exit"
        suggested_pct = 100
        risk_level = "high"
        event = None
        watch_conditions = ["确认可用数量、涨跌停可成交状态和实际成交回报"]
    elif not core_ready:
        action = "observe"
        suggested_pct = 0
        risk_level = "unknown"
        reason = "核心行情或日线数据不足，暂不生成减仓或退出建议"
        event = None
        watch_conditions.extend(["补齐实时价格和日线指标后重新评估"])
    elif at_limit_up and (return_pct is None or return_pct >= 0):
        action = "hold"
        suggested_pct = 25
        risk_level = "low"
        reason = "涨停兑现属于盈利保护，可选减仓 25%，不等同于止损"
        event = {"kind": "limit_up_realization", "label": "涨停兑现", "optional_action_pct": 25}
        watch_conditions.append("关注是否炸板；实际盈亏以成交回报为准")
    elif conflicting:
        action = "observe"
        suggested_pct = 0
        risk_level = "medium"
        reason = "技术面与资金流方向不一致，暂不把单一证据升级为卖出动作"
        event = None
        watch_conditions.append("等待技术面与资金流形成一致方向")
    elif negative_count >= 3:
        action = "reduce_50"
        suggested_pct = 50
        risk_level = "high"
        reason = "技术面、资金流或市场上下文出现多项转弱证据"
        event = None
    elif negative_count >= 2:
        action = "reduce_25"
        suggested_pct = 25
        risk_level = "medium"
        reason = "出现至少两项转弱证据，先降低风险暴露"
        event = None
    else:
        action = "hold"
        suggested_pct = 0
        risk_level = "low" if positive_count >= 2 else "medium"
        reason = "暂未形成足够的退出证据，继续观察已有仓位"
        event = None
        watch_conditions.append("继续观察技术面、资金流和市场上下文是否同步转弱")

    available_count = sum(block["status"] == "available" for block in quality.values())
    confidence = 0.25 if not core_ready else min(0.85, 0.35 + available_count * 0.08)
    if quality["news"]["status"] == "not_supported":
        confidence = max(0.0, confidence - 0.05)
    if hard_guard:
        confidence = 0.95
    return {
        "action": action,
        "action_label": ACTION_LABELS[action],
        "suggested_pct": suggested_pct,
        "risk_level": risk_level,
        "confidence": round(confidence, 2),
        "reason": reason,
        "evidence": evidence,
        "watch_conditions": watch_conditions,
        "data_quality": quality,
        "missing": missing,
        "event": event,
        "manual_confirmation": True,
    }
