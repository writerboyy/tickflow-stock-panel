"""纯函数形式的动态持仓退出判定。

这些函数只消费已经标准化、且明确标记为闭合的分钟 K 线，便于服务层
复用并在没有实时数据时保持 fail-closed。
"""
from __future__ import annotations

import math
from datetime import datetime, time
from typing import Any, Iterable


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _int_config(config: dict[str, Any], key: str, default: int, minimum: int = 1) -> int:
    value = _finite(config.get(key))
    return max(minimum, int(value)) if value is not None else default


def evaluate_peak_pullback(
    *,
    price: float | None,
    intraday_high: float | None,
    cost: float | None,
    initial_r: float | None,
    atr14_5m: float | None,
    closed_bars_5m: Iterable[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    """以 R + ATR 判断冲高回落，返回 active/reason/token。"""
    if (
        config.get("enabled") is not True
        or config.get("active", True) is not True
        or price is None
        or intraday_high is None
        or intraday_high <= 0
        or cost is None
        or cost <= 0
    ):
        return {"active": False, "available": False, "reason": "规则未启用或缺少价格"}
    legacy_mode = config.get("_legacy_mode") is True
    activation_r = _finite(config.get("activation_r"))
    if legacy_mode:
        legacy_activation = _finite(config.get("activation_gain"))
        activated = intraday_high / cost - 1 >= (legacy_activation if legacy_activation is not None else 0.05)
    elif activation_r is None:
        # 旧配置只在显式提供旧字段时沿用百分比口径。
        legacy_activation = _finite(config.get("activation_gain"))
        if legacy_activation is not None and "activation_r" not in config:
            activated = intraday_high / cost - 1 >= legacy_activation
        else:
            activated = initial_r is not None and initial_r > 0 and (intraday_high - cost) / initial_r >= 1.0
    else:
        activated = initial_r is not None and initial_r > 0 and (intraday_high - cost) / initial_r >= activation_r
    atr = _finite(atr14_5m)
    if atr is None or atr <= 0:
        return {"active": False, "available": False, "reason": "缺少 5 分钟 ATR"}
    multiple = _finite(config.get("pullback_atr_multiple"))
    if multiple is None:
        multiple = _finite(config.get("threshold"))
        # 旧 threshold 是百分比，不能误当 ATR 倍数。
        if "pullback_atr_multiple" not in config:
            multiple = None
    if multiple is None:
        multiple = 1.5
    legacy_threshold = _finite(config.get("threshold"))
    threshold = (
        intraday_high * max(0.0, legacy_threshold if legacy_threshold is not None else 0.03)
        if legacy_mode else atr * max(0.1, multiple)
    )
    pullback = activated and intraday_high - price >= threshold
    confirm_bars = _int_config(config, "confirm_bars", 2)
    bars = [bar for bar in closed_bars_5m if bar.get("closed", True) is not False]
    confirmed = len(bars) >= confirm_bars and all(
        (_finite(bar.get("close")) is not None and intraday_high - float(bar["close"]) >= threshold)
        for bar in bars[-confirm_bars:]
    )
    active = bool(pullback and confirmed)
    token = None
    if active:
        token = ":".join(str(bar.get("datetime")) for bar in bars[-confirm_bars:])
    return {
        "active": active,
        "available": True,
        "threshold": threshold,
        "confirm_bars": confirm_bars,
        "token": token,
        "reason": (
            f"高点 {intraday_high:.3f} 回撤达到 {threshold / intraday_high:.2%}"
            if legacy_mode else f"高点 {intraday_high:.3f} 回撤 {intraday_high - price:.3f}，达到 {multiple:.2f}×ATR"
        ),
    }


def evaluate_volume_price_divergence(
    closed_bars_5m: Iterable[dict[str, Any]],
    atr14_5m: float | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    """识别价格双峰创新高但量能不确认，并要求后续闭合 K 线确认。"""
    if config.get("enabled", True) is not True:
        return {"active": False, "available": False, "reason": "规则未启用"}
    bars = [bar for bar in closed_bars_5m if bar.get("closed", True) is not False]
    bars = bars[-_int_config(config, "lookback_bars", 24):]
    atr = _finite(atr14_5m)
    if len(bars) < 5 or atr is None or atr <= 0:
        return {"active": False, "available": False, "reason": "双峰或 ATR 数据不足"}
    if any((_finite(bar.get("volume")) is not None and float(bar["volume"]) < 0) for bar in bars):
        return {"active": False, "available": False, "reason": "成交量数据无效"}
    separation = _int_config(config, "min_peak_separation", 2)
    prominence = _finite(config.get("min_peak_prominence_atr"))
    prominence = prominence if prominence is not None else 0.5
    max_volume_ratio = _finite(config.get("max_peak_volume_ratio"))
    max_volume_ratio = max_volume_ratio if max_volume_ratio is not None else 0.8
    peaks: list[int] = []
    for index in range(1, len(bars) - 1):
        high = _finite(bars[index].get("high"))
        left = _finite(bars[index - 1].get("high"))
        right = _finite(bars[index + 1].get("high"))
        if high is not None and left is not None and right is not None and high >= left and high >= right:
            peaks.append(index)
    for second in reversed(peaks):
        first = next((item for item in reversed(peaks) if second - item >= separation), None)
        if first is None:
            continue
        first_high = _finite(bars[first].get("high"))
        second_high = _finite(bars[second].get("high"))
        first_close = _finite(bars[first].get("close"))
        second_close = _finite(bars[second].get("close"))
        first_volume = _finite(bars[first].get("volume"))
        second_volume = _finite(bars[second].get("volume"))
        if None in {first_high, second_high, first_close, second_close, first_volume, second_volume}:
            continue
        if second_high < first_high + prominence * atr or first_volume <= 0:
            continue
        volume_ratio = second_volume / first_volume
        obv_first = _finite(bars[first].get("obv"))
        obv_second = _finite(bars[second].get("obv"))
        volume_divergent = volume_ratio <= max_volume_ratio
        obv_divergent = obv_first is not None and obv_second is not None and obv_second <= obv_first
        if not (volume_divergent or obv_divergent):
            continue
        confirm_bars = _int_config(config, "confirm_bars", 2)
        confirmations = bars[second + 1:]
        if len(confirmations) < confirm_bars:
            continue
        confirmations = confirmations[:confirm_bars]
        closes = [_finite(item.get("close")) for item in confirmations]
        if any(value is None for value in closes) or not all(value < second_close for value in closes):
            continue
        if len(closes) >= 2 and closes[-1] >= closes[-2]:
            continue
        token = ":".join(str(bars[index].get("datetime")) for index in (first, second))
        token += ":" + ":".join(str(item.get("datetime")) for item in confirmations)
        return {
            "active": True,
            "available": True,
            "token": token,
            "first_peak": first_high,
            "second_peak": second_high,
            "volume_ratio": volume_ratio,
            "reason": f"第二峰价格创新高，峰值量比 {volume_ratio:.2f}，后续 {confirm_bars} 根走弱确认",
        }
    return {"active": False, "available": True, "reason": "未形成确认的双峰背离"}


def evaluate_opening_volume_selloff(
    feature: dict[str, Any],
    now: datetime,
    config: dict[str, Any],
) -> dict[str, Any]:
    """早盘同窗量能基准 + 三项价格确认，数据不足严格不触发。"""
    if config.get("enabled") is not True or not (time(9, 36) <= now.time() <= time(10, 0)):
        return {"active": False, "available": False, "reason": "不在早盘评估窗口"}
    opening = feature.get("opening_five_minute") or {}
    if opening.get("available") is False or opening.get("volume_valid") is False:
        return {"active": False, "available": False, "reason": "早盘五根分钟 K 线不完整或成交量无效"}
    baseline = _finite(opening.get("baseline_median_volume"))
    samples = _int_config(opening, "baseline_samples", 0, minimum=0)
    volume = _finite(opening.get("volume"))
    if baseline is None or baseline <= 0 or volume is None or volume < 0 or samples < _int_config(config, "baseline_sessions", 20):
        return {"active": False, "available": False, "reason": "过去 20 个完整交易日同窗基准不足"}
    multiple = _finite(config.get("volume_multiple"))
    multiple = multiple if multiple is not None else 2.0
    latest = _finite(feature.get("last_price"))
    opening_price = _finite(opening.get("open"))
    vwap = _finite(feature.get("session_vwap"))
    range_low = _finite(opening.get("low"))
    checks = [
        latest is not None and opening_price is not None and latest < opening_price,
        latest is not None and vwap is not None and latest < vwap,
        latest is not None and range_low is not None and latest < range_low,
    ]
    required = _int_config(config, "price_confirmations", 2)
    active = volume >= baseline * multiple and sum(checks) >= required
    return {
        "active": active,
        "available": True,
        "token": str(opening.get("as_of") or ""),
        "volume_ratio": volume / baseline if baseline > 0 else None,
        "price_confirmations": sum(checks),
        "reason": f"早盘量比 {volume / baseline:.2f}，价格确认 {sum(checks)}/3" if baseline > 0 else "早盘量能基准为零",
    }


def evaluate_sector_leader_weakening(
    context: dict[str, Any],
    feature: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    """相关性窗口下降 + 最近闭合 5 分钟相对跑输。"""
    if config.get("enabled") is not True:
        return {"active": False, "available": False, "reason": "规则未启用"}
    correlation = _finite(context.get("sector_correlation_current"))
    baseline = _finite(context.get("sector_correlation_baseline"))
    samples = int(_finite(context.get("sector_correlation_samples")) or 0)
    if correlation is None or baseline is None:
        correlation = _finite(context.get("leader_correlation_current"))
        baseline = _finite(context.get("leader_correlation_baseline"))
        samples = int(_finite(context.get("leader_correlation_samples")) or samples)
    minimum = _int_config(config, "min_correlation_samples", 20)
    if correlation is None or baseline is None or samples < minimum:
        return {"active": False, "available": False, "reason": "缺少板块/龙头分钟相关性基准"}
    min_corr = _finite(config.get("min_correlation"))
    min_corr = min_corr if min_corr is not None else 0.5
    decline = _finite(config.get("decline_delta"))
    decline = decline if decline is not None else 0.2
    relative = feature.get("sector_relative_returns") or context.get("sector_relative_returns") or []
    minute_proxy_available = bool(
        feature.get("minute_proxy_available") is True
        or context.get("minute_proxy_available") is True
    )
    if not minute_proxy_available:
        return {"active": False, "available": False, "reason": "缺少可用板块/龙头闭合分钟代理"}
    gap = _finite(config.get("underperformance_gap"))
    gap = gap if gap is not None else -0.003
    confirmed = (
        minute_proxy_available
        and len(relative) >= 2
        and all((_finite(item) is not None and float(item) <= gap) for item in relative[-2:])
    )
    active = (baseline - correlation >= decline or (baseline >= min_corr and correlation < min_corr)) and confirmed
    return {
        "active": active,
        "available": True,
        "token": str(feature.get("latest_closed_5m_token") or ""),
        "reason": (
            f"相关性 {baseline:.2f}→{correlation:.2f}，最近两根相对跑输"
            if minute_proxy_available else "缺少可用板块/龙头闭合分钟代理"
        ),
    }
