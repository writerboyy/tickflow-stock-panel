"""Deterministic candidate scoring for the limit-board approval pool."""
from __future__ import annotations

from datetime import date
from math import prod
from typing import Any

from app.price_limits import is_risk_warning_name, price_limit_pct


def finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _linear(value: float, low: float, high: float, points: float) -> float:
    if high <= low:
        return 0.0
    return _clamp((value - low) / (high - low)) * points


_PREMIUM_GENE_MAX_SCORE = 10.0
_PREMIUM_GENE_CRITERION_SCORE = _PREMIUM_GENE_MAX_SCORE / 3.0
SCORE_MODEL_VERSION = "limit-board-v6-60-20-20"


def premium_gene_detail(values: dict[str, Any]) -> dict[str, Any] | None:
    required = {
        "limit_up_count",
        "next_day_red_rate",
        "first_board_broken_rate",
    }
    if not required.issubset(values):
        return None
    numbers = {key: finite(values.get(key)) for key in required}
    if any(value is None for value in numbers.values()):
        return None

    limit_count = max(0.0, numbers["limit_up_count"] or 0.0)
    observations = max(0.0, finite(values.get("next_day_observation_count")) or 0.0)
    attempts = max(0.0, finite(values.get("first_board_attempt_count")) or 0.0)
    sealed = max(0.0, finite(values.get("first_board_sealed_count")) or 0.0)
    premium_count = max(0.0, finite(values.get("premium_5_count")) or 0.0)
    premium_rate = (
        premium_count / observations
        if observations > 0
        else _clamp(finite(values.get("premium_5_rate")) or 0.0)
    )
    components = {
        "limit_frequency": min(limit_count / 12.0, 1.0) * _PREMIUM_GENE_CRITERION_SCORE,
        "next_day_red": _clamp(numbers["next_day_red_rate"] or 0.0) * _PREMIUM_GENE_CRITERION_SCORE,
        "first_board_broken": (
            1.0 - _clamp(numbers["first_board_broken_rate"] or 0.0)
        ) * _PREMIUM_GENE_CRITERION_SCORE,
    }
    criteria = {
        "limit_up_count": {
            "value": int(limit_count),
            "threshold": 4,
            "operator": ">=",
            "passed": limit_count >= 4,
            "score": round(components["limit_frequency"], 2),
            "max_score": round(_PREMIUM_GENE_CRITERION_SCORE, 2),
        },
        "next_day_red_rate": {
            "value": numbers["next_day_red_rate"],
            "threshold": 0.80,
            "operator": ">=",
            "passed": numbers["next_day_red_rate"] >= 0.80,
            "score": round(components["next_day_red"], 2),
            "max_score": round(_PREMIUM_GENE_CRITERION_SCORE, 2),
        },
        "first_board_broken_rate": {
            "value": numbers["first_board_broken_rate"],
            "threshold": 0.75,
            "operator": "<=",
            "passed": numbers["first_board_broken_rate"] <= 0.75,
            "score": round(components["first_board_broken"], 2),
            "max_score": round(_PREMIUM_GENE_CRITERION_SCORE, 2),
        },
    }
    return {
        "score": round(sum(components.values()), 2),
        "max_score": _PREMIUM_GENE_MAX_SCORE,
        "passed": all(item["passed"] for item in criteria.values()),
        "components": {key: round(value, 2) for key, value in components.items()},
        "criteria": criteria,
        "as_of": str(values.get("as_of") or "") or None,
        "window_days": int(finite(values.get("window_days")) or 200),
        "limit_up_count": int(limit_count),
        "premium_5_count": int(premium_count),
        "next_day_observation_count": int(observations),
        "next_day_red_rate": numbers["next_day_red_rate"],
        "premium_5_rate": premium_rate,
        "first_board_attempt_count": int(attempts),
        "first_board_sealed_count": int(sealed),
        "first_board_seal_rate": finite(values.get("first_board_seal_rate")),
        "first_board_broken_rate": numbers["first_board_broken_rate"],
        "consecutive_rate": finite(values.get("consecutive_rate")),
    }


def technical_detail(values: dict[str, Any], *, as_of: str | None = None) -> dict[str, Any] | None:
    raw = {
        "price": finite(values.get("last_price", values.get("close"))),
        "ma5": finite(values.get("ma5")),
        "ma10": finite(values.get("ma10")),
        "ma20": finite(values.get("ma20")),
        "ma60": finite(values.get("ma60")),
        "momentum_5d": finite(values.get("momentum_5d")),
        "momentum_20d": finite(values.get("momentum_20d")),
        "vol_ratio_5d": finite(values.get("vol_ratio_5d")),
        "macd_dif": finite(values.get("macd_dif")),
        "macd_dea": finite(values.get("macd_dea")),
        "macd_hist": finite(values.get("macd_hist")),
        "rsi_14": finite(values.get("rsi_14")),
    }
    if any(value is None for value in raw.values()):
        return None
    price = raw["price"] or 0.0
    ma5 = raw["ma5"] or 0.0
    ma10 = raw["ma10"] or 0.0
    ma20 = raw["ma20"] or 0.0
    ma60 = raw["ma60"] or 0.0
    trend = (
        (1.0 if price > ma5 else 0.0)
        + (2.0 if ma5 > ma10 else 0.0)
        + (2.0 if ma10 > ma20 else 0.0)
        + (2.0 if ma20 > ma60 else 0.0)
    )
    momentum = (
        _linear(raw["momentum_5d"] or 0.0, -0.02, 0.10, 2.5)
        + _linear(raw["momentum_20d"] or 0.0, 0.0, 0.30, 2.5)
    )
    volume = _linear(raw["vol_ratio_5d"] or 0.0, 0.8, 2.5, 3.0)
    macd = (
        (2.0 if (raw["macd_dif"] or 0.0) > (raw["macd_dea"] or 0.0) else 0.0)
        + (1.0 if (raw["macd_hist"] or 0.0) > 0 else 0.0)
    )
    rsi = raw["rsi_14"] or 0.0
    if 40.0 <= rsi < 50.0:
        rsi_score = _linear(rsi, 40.0, 50.0, 2.0)
    elif 50.0 <= rsi <= 85.0:
        rsi_score = 2.0
    elif 85.0 < rsi < 95.0:
        rsi_score = 2.0 - _linear(rsi, 85.0, 95.0, 2.0)
    else:
        rsi_score = 0.0
    components = {
        "trend": trend,
        "momentum": momentum,
        "volume": volume,
        "macd": macd,
        "rsi": rsi_score,
    }
    return {
        "score": round(sum(components.values()), 2),
        "max_score": 20.0,
        "components": {key: round(value, 2) for key, value in components.items()},
        "as_of": as_of,
        **raw,
        # KDJ 只做明细展示，不进 raw 门控：缺失时不能把整个技术面明细打没。
        "kdj_k": finite(values.get("kdj_k")),
        "kdj_d": finite(values.get("kdj_d")),
        "kdj_j": finite(values.get("kdj_j")),
    }


def intraday_flow_detail(
    intraday: dict[str, Any] | None,
    *,
    previous_close: object = None,
    external_flow: dict[str, Any] | None = None,
    as_of: str | None = None,
    limit_up: object = None,
) -> dict[str, Any] | None:
    """Score live intraday trend and capital-flow direction.

    Minute bars are the required live input.  Active buy/sell ratios remain
    distinct from the Kaipanla cumulative main-net-flow speed contract.

    ``limit_up`` enables decision-time pull-up metrics (启动点、拉升用时、
    拉升回撤、封板前量能、触板/封板状态)。没有涨停价时这些字段输出 None，
    由综合评分的数据门控显式标记为数据不足。
    """
    if not isinstance(intraday, dict) or not intraday.get("available"):
        return None
    bars = [
        row for row in (intraday.get("session_bars") or intraday.get("closed_bars") or [])
        if isinstance(row, dict)
        and finite(row.get("close")) is not None
        and finite(row.get("amount")) is not None
    ]
    if not bars:
        return None

    closes = [float(finite(row.get("close")) or 0.0) for row in bars]
    base = finite(previous_close)
    if base is None or base <= 0:
        base = finite(bars[0].get("open"))
    if base is None or base <= 0:
        return None
    last_price = closes[-1]
    trend_pct = last_price / base - 1.0
    underwater_ratio = sum(value < base for value in closes) / len(closes)
    vwap = finite(intraday.get("session_vwap"))
    vwap_gap_pct = last_price / vwap - 1.0 if vwap and vwap > 0 else None

    buy_ratio = finite((external_flow or {}).get("buy_ratio"))
    sell_ratio = finite((external_flow or {}).get("sell_ratio"))
    net_flow_speed = finite((external_flow or {}).get("net_flow_speed"))
    net_flow_delta = finite((external_flow or {}).get("net_flow_delta"))
    net_flow_amount = finite((external_flow or {}).get("net_flow_amount"))
    external_source = str((external_flow or {}).get("source") or "large_order")
    if external_source == "kaipanla_net_flow":
        buy_ratio = sell_ratio = None
    capital_available = (
        (buy_ratio is not None or sell_ratio is not None or net_flow_speed is not None)
        and str((external_flow or {}).get("data_quality") or "") != "proxy_only"
    )
    flow_source = external_source if capital_available else "unavailable"
    if buy_ratio is None and sell_ratio is not None:
        buy_ratio = 1.0 - sell_ratio
    elif sell_ratio is None and buy_ratio is not None:
        sell_ratio = 1.0 - buy_ratio
    if buy_ratio is not None and sell_ratio is not None:
        buy_ratio = _clamp(buy_ratio)
        sell_ratio = _clamp(sell_ratio)
        net_flow_ratio = buy_ratio - sell_ratio
    else:
        buy_ratio = sell_ratio = net_flow_ratio = None

    outflow_streak = 0
    latest = closes[-1]
    for previous in reversed(closes[:-1]):
        if latest < previous:
            outflow_streak += 1
            latest = previous
            continue
        break

    amounts = [max(0.0, finite(row.get("amount")) or 0.0) for row in bars]
    recent_amount = sum(amounts[-3:]) / max(1, len(amounts[-3:]))

    # ---- 打板决策时点指标（只用封板前已发生的数据，不含未来函数）----
    limit_price = finite(limit_up)
    touch_index: int | None = None
    if limit_price is not None and limit_price > 0:
        for index, row in enumerate(bars):
            bar_high = finite(row.get("high")) or closes[index]
            if bar_high >= limit_price * 0.998:
                touch_index = index
                break
    sealed_now = bool(
        limit_price is not None
        and limit_price > 0
        and closes[-1] >= limit_price * 0.998
    )
    # 启动点：第一根收盘价达到昨收 +3% 的分钟 bar
    start_index = next(
        (index for index, close in enumerate(closes) if close >= base * 1.03),
        None,
    )
    pull_up_minutes: int | None = None
    pull_up_max_drawdown: float | None = None
    pull_up_gain: float | None = None
    if start_index is not None:
        leg_end = touch_index if touch_index is not None else len(closes) - 1
        if leg_end >= start_index:
            leg = closes[start_index:leg_end + 1]
            pull_up_minutes = len(leg)
            pull_up_gain = closes[leg_end] / closes[start_index] - 1.0
            peak = leg[0]
            drawdown = 0.0
            for price in leg:
                peak = max(peak, price)
                if peak > 0:
                    drawdown = max(drawdown, (peak - price) / peak)
            pull_up_max_drawdown = drawdown
    # 封板前量能：只统计触板前（含触板那根）的 bar，
    # 封死后缩量是强势特征，不能按「量价背离」惩罚。
    pre_seal_bars = bars[: touch_index + 1] if touch_index is not None else bars
    pre_seal_amount_growth: float | None = None
    if len(pre_seal_bars) >= 5:
        pre_amounts = [
            max(0.0, finite(row.get("amount")) or 0.0) for row in pre_seal_bars
        ]
        pre_recent = sum(pre_amounts[-3:]) / 3.0
        pre_earlier = pre_amounts[:-3]
        pre_earlier_avg = sum(pre_earlier) / len(pre_earlier) if pre_earlier else None
        if pre_earlier_avg and pre_earlier_avg > 0:
            pre_seal_amount_growth = pre_recent / pre_earlier_avg - 1.0
    day_open = finite(bars[0].get("open")) or closes[0]
    day_high = max(
        (finite(row.get("high")) or closes[index]) for index, row in enumerate(bars)
    )
    day_low = min(
        (finite(row.get("low")) or closes[index]) for index, row in enumerate(bars)
    )

    net_flow_speed_ratio = (
        _clamp(net_flow_speed / recent_amount, -1.0, 1.0)
        if net_flow_speed is not None and recent_amount > 0
        else None
    )
    if net_flow_ratio is None:
        net_flow_ratio = net_flow_speed_ratio
    earlier_amounts = amounts[:-3]
    earlier_amount = sum(earlier_amounts) / len(earlier_amounts) if earlier_amounts else None
    amount_growth = (
        recent_amount / earlier_amount - 1.0
        if earlier_amount and earlier_amount > 0 else None
    )
    price_volume = (
        _linear(amount_growth, -0.20, 0.50, 5.0)
        if trend_pct > 0 and amount_growth is not None else 0.0
    )
    components = {
        "trend": _linear(trend_pct, -0.03, 0.05, 8.0),
        "vwap": _linear(vwap_gap_pct, -0.02, 0.03, 6.0) if vwap_gap_pct is not None else 0.0,
        "underwater": (1.0 - underwater_ratio) * 6.0,
        "price_volume": price_volume,
        "net_flow": _linear(net_flow_ratio, -0.60, 0.60, 18.0)
        if capital_available and net_flow_ratio is not None else 0.0,
        "outflow_continuity": (
            (1.0 - min(outflow_streak / 5.0, 1.0)) * 7.0
            if capital_available else 0.0
        ),
    }
    trend_score = sum(components[key] for key in ("trend", "vwap", "underwater", "price_volume"))
    capital_score = sum(components[key] for key in ("net_flow", "outflow_continuity"))
    trend_state = "strong" if trend_score >= 18.0 else "weak" if trend_score < 10.0 else "neutral"
    if not capital_available:
        flow_state = "unavailable"
        capital_source_label = "暂无实时主动资金"
    elif external_source == "kaipanla_net_flow":
        flow_state = (
            "inflow" if (net_flow_ratio or 0.0) >= 0.10
            else "outflow" if (net_flow_ratio or 0.0) <= -0.10
            else "balanced"
        )
        capital_source_label = "开盘啦主力净额涨速"
    elif net_flow_ratio is not None and net_flow_ratio >= 0.10:
        flow_state = "inflow"
        capital_source_label = "实时主动大单"
    elif net_flow_ratio is not None and net_flow_ratio <= -0.10:
        flow_state = "outflow"
        capital_source_label = "实时主动大单"
    else:
        flow_state = "balanced"
        capital_source_label = "实时主动大单"
    return {
        "score": round(sum(components.values()), 2),
        "max_score": 50.0,
        "components": {key: round(value, 2) for key, value in components.items()},
        "trend_score": round(trend_score, 2),
        "trend_max_score": 25.0,
        "trend_state": trend_state,
        "price_volume_rising": bool(trend_pct > 0 and amount_growth is not None and amount_growth > 0),
        "capital_score": round(capital_score, 2),
        "capital_max_score": 25.0,
        "flow_state": flow_state,
        "capital_source_label": capital_source_label,
        "trend_pct": trend_pct,
        "underwater_ratio": underwater_ratio,
        "vwap_gap_pct": vwap_gap_pct,
        "buy_ratio": buy_ratio,
        "sell_ratio": sell_ratio,
        "net_flow_ratio": net_flow_ratio,
        "net_flow_amount": net_flow_amount,
        "net_flow_delta": net_flow_delta,
        "net_flow_speed": net_flow_speed,
        "net_flow_speed_ratio": net_flow_speed_ratio,
        "net_flow_window_minutes": finite((external_flow or {}).get("net_flow_window_minutes")),
        "net_flow_as_of": (external_flow or {}).get("net_flow_as_of"),
        "flow_metric": "main_net_speed" if external_source == "kaipanla_net_flow" else "active_ratio",
        "outflow_streak": outflow_streak,
        "flow_source": flow_source,
        "capital_available": capital_available,
        "amount_growth": amount_growth,
        "bars": len(bars),
        "last_price": last_price,
        "limit_up": limit_price,
        "touch_index": touch_index,
        "sealed_now": sealed_now,
        "pull_up_start_index": start_index,
        "pull_up_minutes": pull_up_minutes,
        "pull_up_max_drawdown": pull_up_max_drawdown,
        "pull_up_gain": pull_up_gain,
        "pre_seal_amount_growth": pre_seal_amount_growth,
        "day_open": day_open,
        "day_high": day_high,
        "day_low": day_low,
        "as_of": as_of,
    }


def rotation_detail(
    rotation: dict[str, Any], sector_name: str, today: date,
) -> dict[str, Any] | None:
    """Build an institutional-style cross-sectional sector signal.

    The matrix contains sector returns ranked within each day.  We use the
    latest 1/3/5/20 completed sessions, convert each window return to a
    cross-sectional percentile, and keep the component scores unnormalised
    when a window is missing.  This avoids treating a short history as a
    fully observed signal.
    """
    trading_dates: list[date] = []
    for raw_date in rotation.get("dates") or []:
        try:
            trading_date = date.fromisoformat(str(raw_date))
        except ValueError:
            continue
        if trading_date < today:
            trading_dates.append(trading_date)
    unique_dates = sorted(set(trading_dates))
    columns = rotation.get("columns") or {}
    daily_maps: dict[str, dict[str, float]] = {}
    daily_rank_info: dict[str, tuple[int, int]] = {}
    for trading_date in unique_dates:
        rows = columns.get(trading_date.isoformat()) or []
        values: dict[str, float] = {}
        for index, item in enumerate(rows):
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            value = finite(item[1])
            if value is not None:
                name = str(item[0])
                values[name] = value
                if name == sector_name:
                    daily_rank_info[trading_date.isoformat()] = (index, len(rows))
        if values:
            daily_maps[trading_date.isoformat()] = values

    target_days: list[dict[str, Any]] = []
    for trading_date in unique_dates:
        values = daily_maps.get(trading_date.isoformat()) or {}
        change = values.get(sector_name)
        if change is None:
            continue
        rank_index, count = daily_rank_info.get(
            trading_date.isoformat(), (0, len(values)),
        )
        percentile = 1.0 if count <= 1 else 1.0 - rank_index / (count - 1)
        target_days.append({
            "date": trading_date.isoformat(),
            "change_pct": change,
            "rank": rank_index + 1,
            "rank_count": count,
            "rank_percentile": percentile,
        })
    if len(target_days) < 3:
        return None

    def compound(values: list[float]) -> float:
        return prod(1.0 + value for value in values) - 1.0

    def percentile(values: list[float], target: float) -> float:
        if len(values) <= 1:
            return 1.0
        below = sum(value < target for value in values)
        equal = sum(value == target for value in values)
        rank = below + max(equal - 1, 0) / 2.0
        return _clamp(rank / (len(values) - 1))

    def window_percentile(window: int) -> tuple[float, float] | None:
        if len(target_days) < window:
            return None
        window_days = target_days[-window:]
        date_keys = [str(item["date"]) for item in window_days]
        common_names = set(daily_maps.get(date_keys[0], {}))
        for date_key in date_keys[1:]:
            common_names.intersection_update(daily_maps.get(date_key, {}))
        returns: list[float] = []
        target_return: float | None = None
        for name in common_names:
            values = [daily_maps[date_key][name] for date_key in date_keys]
            value = compound(values)
            returns.append(value)
            if name == sector_name:
                target_return = value
        if target_return is None or not returns:
            return None
        return target_return, percentile(returns, target_return)

    windows = {window: window_percentile(window) for window in (1, 3, 5, 20)}
    window_weights = {1: 0.15, 3: 0.20, 5: 0.30, 20: 0.35}
    momentum_score = sum(
        result[1] * 20.0 * window_weights[window]
        for window, result in windows.items()
        if result is not None
    )
    momentum_max = sum(
        20.0 * window_weights[window]
        for window, result in windows.items()
        if result is not None
    )

    returns = [float(item["change_pct"]) for item in target_days]
    percentiles = [float(item["rank_percentile"]) for item in target_days]
    latest_returns = returns[-5:]
    center = (len(latest_returns) - 1) / 2.0
    mean = sum(latest_returns) / len(latest_returns)
    denominator = sum((index - center) ** 2 for index in range(len(latest_returns)))
    return_slope = (
        sum((index - center) * (value - mean) for index, value in enumerate(latest_returns)) / denominator
        if denominator > 0 else None
    )
    rank_change = percentiles[-1] - percentiles[0]
    trend_score = (
        (_clamp((return_slope + 0.002) / 0.004) * 0.5
         + _clamp((rank_change + 0.30) / 0.60) * 0.5) * 10.0
        if return_slope is not None else 0.0
    )
    trend_max = 10.0 if return_slope is not None else 0.0
    top_days = sum(value >= 0.8 for value in percentiles)
    persistence_max = 10.0 if len(percentiles) >= 5 else 0.0
    persistence_score = top_days / len(percentiles) * persistence_max if persistence_max else 0.0
    positive_ratio = sum(value > 0 for value in returns) / len(returns)
    volatility = (
        sum((value - mean) ** 2 for value in latest_returns) / len(latest_returns)
    ) ** 0.5
    stability = _clamp(positive_ratio * 0.7 + (1.0 - _clamp(volatility / 0.03)) * 0.3)
    stability_max = 10.0 if len(latest_returns) >= 3 else 0.0
    stability_score = stability * stability_max if stability_max else 0.0
    institutional_score = momentum_score + trend_score + persistence_score + stability_score
    institutional_max = momentum_max + trend_max + persistence_max + stability_max
    five_day = windows.get(5)
    one_day = windows.get(1)
    three_day = windows.get(3)
    twenty_day = windows.get(20)
    five_day_return = five_day[0] if five_day else None
    legacy_components = {
        "compound": _linear(five_day_return, -0.05, 0.10, 6.0)
        if five_day_return is not None else 0.0,
        "slope": _linear(return_slope, -0.01, 0.01, 4.0)
        if return_slope is not None else 0.0,
        "rank_change": _linear(rank_change, -0.30, 0.30, 4.0),
        "persistence": top_days / min(len(percentiles), 5) * 3.0,
        "yesterday": _linear(returns[-1], -0.02, 0.03, 3.0),
    }
    if top_days >= 3 and percentiles[-1] >= 0.8 and (five_day_return or 0.0) > 0:
        legacy_label = "主线"
    elif rank_change >= 0.15 and (return_slope or 0.0) > 0:
        legacy_label = "上升"
    elif rank_change <= -0.15 and (return_slope or 0.0) < 0:
        legacy_label = "退潮"
    else:
        legacy_label = "震荡"
    return {
        # Keep the legacy 20-point field for existing candidate consumers. The
        # institutional score is exposed separately and drives board scoring.
        "score": round(sum(legacy_components.values()), 2),
        "max_score": 20.0,
        "components": {key: round(value, 2) for key, value in legacy_components.items()},
        "institutional_score": round(institutional_score, 2),
        "institutional_max_score": round(institutional_max, 2),
        "institutional_components": {
            "relative_momentum": round(momentum_score, 2),
            "trend": round(trend_score, 2),
            "persistence": round(persistence_score, 2),
            "stability": round(stability_score, 2),
        },
        "institutional_component_max": {
            "relative_momentum": round(momentum_max, 2),
            "trend": round(trend_max, 2),
            "persistence": round(persistence_max, 2),
            "stability": round(stability_max, 2),
        },
        "days": target_days[-20:],
        "one_day_change_pct": one_day[0] if one_day else None,
        "three_day_change_pct": three_day[0] if three_day else None,
        "five_day_change_pct": five_day[0] if five_day else None,
        "twenty_day_change_pct": twenty_day[0] if twenty_day else None,
        "momentum_1d_percentile": one_day[1] if one_day else None,
        "momentum_3d_percentile": three_day[1] if three_day else None,
        "momentum_5d_percentile": five_day[1] if five_day else None,
        "momentum_20d_percentile": twenty_day[1] if twenty_day else None,
        "trend_slope": return_slope,
        "rank_change": rank_change,
        "top_20_days": top_days,
        "positive_days": sum(value > 0 for value in returns),
        "volatility": volatility,
        "yesterday_change_pct": returns[-1],
        # Deprecated compatibility field; new scoring/UI do not use it.
        "rotation_label": legacy_label,
    }


def _rotation_session_count(
    rotation: dict[str, Any], sector_name: str, today: date,
) -> int:
    """Count completed rotation sessions containing the target sector."""
    columns = rotation.get("columns") or {}
    sessions: set[date] = set()
    for raw_date in rotation.get("dates") or []:
        try:
            trading_date = date.fromisoformat(str(raw_date))
        except ValueError:
            continue
        if trading_date >= today:
            continue
        rows = columns.get(trading_date.isoformat()) or []
        if any(
            isinstance(item, (list, tuple))
            and len(item) >= 2
            and str(item[0]) == sector_name
            and finite(item[1]) is not None
            for item in rows
        ):
            sessions.add(trading_date)
    return len(sessions)


def sector_detail(
    *,
    symbol: str,
    target: dict[str, Any],
    snapshot: dict[str, Any],
    rotation: dict[str, Any],
    stock_rows: dict[str, dict[str, Any]],
    member_symbols: set[str],
    today: date,
    realtime: dict[str, Any] | None = None,
    realtime_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    effective_snapshot = {
        **snapshot,
        **(realtime_snapshot or {}),
    }
    if not effective_snapshot.get("valid") or (
        finite(effective_snapshot.get("coverage_ratio")) or 0.0
    ) < 0.8:
        return None
    history = rotation_detail(rotation, _rotation_name(target), today)
    rotation_matrix_available = bool(rotation.get("dates") and rotation.get("columns"))
    rotation_history_sessions = (
        len(history.get("days") or [])
        if history is not None
        else _rotation_session_count(rotation, _rotation_name(target), today)
    )
    stock = stock_rows.get(symbol)
    if stock is None:
        return None
    rotation_available = history is not None
    if history is None:
        history = {
            "score": 0.0,
            "components": {},
            "days": [],
            "five_day_change_pct": None,
            "trend_slope": None,
            "rank_change": None,
            "top_20_days": None,
            "yesterday_change_pct": None,
            "rotation_label": None,
        }
    candidate_change = finite(stock.get("change_pct"))
    local_sector_change = finite(effective_snapshot.get("change_pct"))
    realtime_sector_change = finite((realtime or {}).get("change_pct"))
    sector_change = (
        realtime_sector_change
        if realtime_sector_change is not None
        else local_sector_change
    )
    if candidate_change is None or sector_change is None:
        return None
    ranked = []
    for member_symbol in member_symbols:
        member = stock_rows.get(member_symbol)
        change = finite((member or {}).get("change_pct"))
        if member is None or change is None:
            continue
        # 排名按「涨停进度」归一：日内涨幅 ÷ 该票涨停幅度。
        # 主板 10%、创业板/科创板 20%、北交所 30%、主板 ST 按规则调整——
        # 否则 20cm 票 +15% 会排在主板涨停票前面，龙头判定失真。
        limit_pct = price_limit_pct(
            member_symbol,
            today,
            is_risk_warning=is_risk_warning_name(str(member.get("name") or "")),
        )
        progress = change / limit_pct if limit_pct and limit_pct > 0 else change
        ranked.append({
            "symbol": member_symbol,
            "name": member.get("name") or member_symbol,
            "change_pct": change,
            "limit_pct": limit_pct,
            "progress_pct": progress,
            "amount": max(0.0, finite(member.get("amount")) or 0.0),
        })
    ranked.sort(key=lambda row: (-row["progress_pct"], -row["amount"], row["symbol"]))
    member_count = int(effective_snapshot.get("total_count") or len(member_symbols))
    if member_count < 5 or not ranked:
        return None
    candidate_index = next(
        (index for index, row in enumerate(ranked) if row["symbol"] == symbol),
        None,
    )
    if candidate_index is None:
        return None
    top_progress = float(ranked[0]["progress_pct"])
    candidate_progress = float(ranked[candidate_index]["progress_pct"])
    co_leaders = [
        row for row in ranked
        if float(row["progress_pct"]) > 0
        and top_progress - float(row["progress_pct"]) <= 0.01
    ]
    leader = (
        min(
            co_leaders,
            key=lambda row: (-float(row["amount"]), str(row["symbol"])),
        )
        if co_leaders else ranked[0]
    )
    leader_gap = top_progress - candidate_progress
    is_leader = candidate_progress > 0 and leader_gap <= 0.01
    is_front = not is_leader and (candidate_index < 3 or leader_gap <= 0.10)
    leadership = "leader" if is_leader else "front" if is_front else "follower"
    position_available = bool(ranked)
    up_count = int(effective_snapshot.get("up_count") or 0)
    valid_count = int(effective_snapshot.get("valid_count") or len(ranked))
    up_ratio = up_count / valid_count if valid_count else 0.0
    current_components = {
        "sector_change": _linear(sector_change, -0.01, 0.04, 8.0),
        "breadth": _clamp(up_ratio) * 5.0,
        "leader_change": _linear(float(leader["change_pct"]), 0.0, 0.10, 3.0),
        "relative_strength": _linear(candidate_change - sector_change, -0.02, 0.04, 4.0),
        "leadership": 10.0 if is_leader else 5.0 if is_front else 0.0,
    }
    current_score = sum(current_components.values())
    # 机构式当日确认：广度、资金和流动性各自独立计分，缺失时
    # 只减少可得满分，不把缺失数据当成中性分数。
    institutional_components = dict(history.get("institutional_components") or {})
    institutional_component_max = dict(history.get("institutional_component_max") or {})
    institutional_score = float(history.get("institutional_score") or 0.0)
    institutional_max = float(history.get("institutional_max_score") or 0.0)
    breadth_available = up_ratio is not None
    if breadth_available:
        institutional_components["breadth"] = _clamp(up_ratio) * 20.0
        institutional_component_max["breadth"] = 20.0
        institutional_score += institutional_components["breadth"]
        institutional_max += 20.0
    flow_amount = finite((realtime or {}).get("amount"))
    flow_net = finite((realtime or {}).get("main_net"))
    flow_ratio = flow_net / flow_amount if flow_net is not None and flow_amount and flow_amount > 0 else None
    if flow_ratio is not None:
        institutional_components["money_flow"] = _linear(flow_ratio, -0.20, 0.40, 15.0)
        institutional_component_max["money_flow"] = 15.0
        institutional_score += institutional_components["money_flow"]
        institutional_max += 15.0
    volume_ratio = finite((realtime or {}).get("volume_ratio"))
    if volume_ratio is not None:
        institutional_components["liquidity"] = _linear(volume_ratio, 0.8, 2.0, 5.0)
        institutional_component_max["liquidity"] = 5.0
        institutional_score += institutional_components["liquidity"]
        institutional_max += 5.0
    rotation_fields = {
        key: value for key, value in history.items()
        if key not in {"score", "max_score", "components"}
    }
    return {
        "score": round(current_score + float(history["score"]), 2),
        "max_score": 50.0,
        "current_score": round(current_score, 2),
        "rotation_score": history["score"],
        "current_components": {
            key: round(value, 2) for key, value in current_components.items()
        },
        "kind": target.get("kind"),
        "name": target.get("name") or _rotation_name(target),
        "change_pct": sector_change,
        "up_ratio": up_ratio,
        "coverage_ratio": finite(effective_snapshot.get("coverage_ratio")),
        "valid_count": valid_count,
        "member_count": member_count,
        "leader": leader,
        "stock_rank": candidate_index + 1,
        "stock_change_pct": candidate_change,
        "stock_progress_pct": candidate_progress,
        "leader_gap_pct": leader_gap,
        "rank_method": "intraday_progress_normalized",
        "leadership": leadership,
        "is_sector_leader": is_leader,
        "rotation_available": rotation_available,
        "rotation_matrix_available": rotation_matrix_available,
        "rotation_history_sessions": rotation_history_sessions,
        "realtime_available": realtime_sector_change is not None,
        "realtime_rank": (realtime or {}).get("rank"),
        "realtime_rank_count": (realtime or {}).get("rank_count"),
        "realtime_strength": finite((realtime or {}).get("strength")),
        "realtime_change_pct": realtime_sector_change,
        "realtime_speed_pct": finite((realtime or {}).get("speed_pct")),
        "realtime_amount": finite((realtime or {}).get("amount")),
        "realtime_main_net": finite((realtime or {}).get("main_net")),
        "realtime_main_buy": finite((realtime or {}).get("main_buy")),
        "realtime_main_sell": finite((realtime or {}).get("main_sell")),
        "realtime_volume_ratio": finite((realtime or {}).get("volume_ratio")),
        "rotation_components": history["components"],
        **rotation_fields,
        "institutional_score": round(institutional_score, 2),
        "institutional_max_score": round(institutional_max, 2),
        "institutional_components": {
            key: round(float(value), 2) for key, value in institutional_components.items()
        },
        "institutional_component_max": {
            key: round(float(value), 2) for key, value in institutional_component_max.items()
        },
    }


def _rotation_name(target: dict[str, Any]) -> str:
    name = str(target.get("name") or target.get("value") or "")
    return name.split(" / ")[-1].strip()


def rotation_only_detail(
    target: dict[str, Any],
    rotation: dict[str, Any],
    today: date,
) -> dict[str, Any] | None:
    """仅依赖日频轮动数据的板块评分（实时板块行情不可用时的降级路径）。

    板块K线形态/过热风险是日频数据，周末、盘后或实时 socket 断开时仍然
    有效；实时类组件（当日表现、板块地位、宽度）输出 None 字段，由综合
    评分的数据门控显式标记为数据不足，不参与打分。
    """
    history = rotation_detail(rotation, _rotation_name(target), today)
    if history is None:
        return None
    rotation_fields = {
        key: value for key, value in history.items()
        if key not in {"score", "max_score", "components"}
    }
    return {
        "score": history["score"],
        "max_score": 50.0,
        "current_score": 0.0,
        "rotation_score": history["score"],
        "current_components": {},
        "kind": target.get("kind"),
        "name": target.get("name") or _rotation_name(target),
        "change_pct": None,
        "up_ratio": None,
        "coverage_ratio": None,
        "leadership": None,
        "stock_rank": None,
        "is_sector_leader": False,
        "rotation_available": True,
        "rotation_matrix_available": bool(rotation.get("dates") and rotation.get("columns")),
        "rotation_history_sessions": len(history.get("days") or []),
        "realtime_available": False,
        "rotation_components": history["components"],
        **rotation_fields,
    }


_GRADE_ORDER = ("D", "C", "B", "B+", "A", "A+", "S")
_GRADE_LABELS = {
    "S": "完美",
    "A+": "优秀",
    "A": "良好",
    "B+": "中上",
    "B": "中等",
    "C": "一般",
    "D": "较差",
}


def _dimension_result(
    label: str,
    full_max: float,
    components: list[tuple[str, float, float, bool]],
    unavailable_reasons: dict[str, str] | None = None,
) -> dict[str, Any]:
    """装配一个评分维度。

    components 为 (key, score, available_max, available) 四元组，score 和
    available_max 统一使用 100 分内部刻度；full_max 是该卡片在综合评分中的
    外层满分，最终按比例缩放到 full_max：
    - available=False 的组件不计分、不出现在 components 里，列入
      unavailable_components，并通过 unavailable_reasons 解释缺失原因；
    - max_score 是「可得满分」，缺项时小于 full_max_score，总分按它折算。
    """
    internal_max = 100.0
    scale = full_max / internal_max
    score = sum(item[1] for item in components if item[3]) * scale
    available_max = sum(item[2] for item in components if item[3]) * scale
    unavailable = [item[0] for item in components if not item[3]]
    reason_map = unavailable_reasons or {}
    return {
        "score": round(score, 1),
        "max_score": round(available_max, 1),
        "full_max_score": full_max,
        "percentage": (
            round(score / available_max * 100, 1) if available_max > 0 else 0.0
        ),
        "components": {
            item[0]: round(item[1] * scale, 1) for item in components if item[3]
        },
        "unavailable_components": unavailable,
        "unavailable_reasons": {
            key: reason_map.get(key, "评分所需数据未返回") for key in unavailable
        },
        "data_complete": available_max >= full_max - 1e-9,
        "label": label,
    }


def comprehensive_score(
    candidate_score_detail: dict[str, Any] | None,
    *,
    board_quality: dict[str, Any] | None = None,
    four_mode_score: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """计算打板池综合评分（100分制）：历史涨停基因(60分) + 板块强度(20分) + 拉升健康度(20分)

    适用于打板池，评估标的打板价值。买入池不使用此评分。

    数据门控：缺失的输入不会被当成「安全/中性」打分——对应组件显式标记
    数据不足、不计分、不产生警示/优势；维度按可得满分折算，数据不完整时
    评级封顶 B；全部缺失时显式返回「数据不足」而不是一个伪装的低分。

    Args:
        candidate_score_detail: 包含 premium_gene, intraday_flow, technical, sector 等维度的评分
        board_quality: 封板质量数据（仅供参考，不用于打板前评分）
        four_mode_score: 四合一策略评分数据（仅供参考）

    Returns:
        包含综合评分、各维度得分、评级等信息的字典
    """
    if not isinstance(candidate_score_detail, dict):
        candidate_score_detail = {}

    gene = candidate_score_detail.get("premium_gene") or {}
    flow = candidate_score_detail.get("intraday_flow") or {}
    tech = candidate_score_detail.get("technical") or {}
    sector = candidate_score_detail.get("sector") or {}

    # ========================================
    # 一、历史涨停基因（60分）；内部按 100 分制计算
    # ========================================
    next_day_red_rate = finite(gene.get("next_day_red_rate"))
    first_board_seal_rate = finite(gene.get("first_board_seal_rate"))
    consecutive_rate = finite(gene.get("consecutive_rate"))
    history = _dimension_result("历史涨停基因", 60.0, [
        ("next_day_red", _clamp(next_day_red_rate or 0.0) * 40.0, 40.0, next_day_red_rate is not None),
        ("seal_success", _clamp(first_board_seal_rate or 0.0) * 40.0, 40.0, first_board_seal_rate is not None),
        ("consecutive_ability", _clamp(consecutive_rate or 0.0) * 20.0, 20.0, consecutive_rate is not None),
    ], {
        "next_day_red": "无足够历史涨停后的次日表现样本",
        "seal_success": "无首板封板或炸板历史样本",
        "consecutive_ability": "无连板历史样本",
    })

    # ========================================
    # 二、板块强度（20分）；内部按 100 分制计算
    # ========================================
    rotation_available = bool(sector.get("rotation_available", False))
    rotation_score = finite(sector.get("rotation_score"))
    institutional_score = finite(sector.get("institutional_score"))
    five_day_change = finite(sector.get("five_day_change_pct"))
    days = [day for day in (sector.get("days") or []) if isinstance(day, dict)]
    realtime_rank = finite(sector.get("realtime_rank"))
    realtime_rank_count = finite(sector.get("realtime_rank_count"))

    # 机构量化板块强度（20分）：原始机构分项合计 90 分，先归一到内部 100 分。
    # component_max 同时决定可得满分；缺项不计分并在前端显示「数据不足」。
    institutional_component_values = sector.get("institutional_components")
    institutional_component_max_values = sector.get("institutional_component_max")
    institutional_component_values = (
        institutional_component_values
        if isinstance(institutional_component_values, dict)
        else {}
    )
    institutional_component_max_values = (
        institutional_component_max_values
        if isinstance(institutional_component_max_values, dict)
        else {}
    )
    institutional_component_defaults = {
        "relative_momentum": 20.0,
        "trend": 10.0,
        "persistence": 10.0,
        "stability": 10.0,
        "breadth": 20.0,
        "money_flow": 15.0,
        "liquidity": 5.0,
    }
    institutional_keys = tuple(institutional_component_defaults)
    has_institutional_components = any(
        finite(institutional_component_values.get(key)) is not None
        for key in institutional_keys
    )
    rotation_history_sessions = int(
        finite(sector.get("rotation_history_sessions")) or len(days)
    )
    rotation_matrix_available = bool(
        sector.get("rotation_matrix_available", rotation_available)
    )

    def rotation_reason(required_sessions: int) -> str:
        if not rotation_matrix_available:
            return "无板块历史轮动矩阵"
        if rotation_history_sessions <= 0:
            return "当前板块未匹配到历史轮动数据"
        if rotation_history_sessions < required_sessions:
            return (
                f"板块历史仅 {rotation_history_sessions} 个交易日，"
                f"至少需要 {required_sessions} 个"
            )
        return "板块历史轮动分项未生成"

    institutional_unavailable_reasons = {
        "relative_momentum": rotation_reason(3),
        "trend": rotation_reason(3),
        "persistence": rotation_reason(5),
        "stability": rotation_reason(3),
        "breadth": "实时板块成分涨跌数据未返回",
        "money_flow": "实时板块主力净额或成交额未返回",
        "liquidity": "实时板块量比未返回",
    }

    if has_institutional_components:
        institutional_raw_total_max = sum(institutional_component_defaults.values())
        institutional_dimension_components = []
        for key in institutional_keys:
            raw_score = finite(institutional_component_values.get(key))
            raw_max = finite(institutional_component_max_values.get(key))
            if raw_max is None and raw_score is not None:
                # 兼容机构分项已存在、但尚未持久化 component_max 的旧快照。
                raw_max = institutional_component_defaults[key]
            available = raw_score is not None and raw_max is not None and raw_max > 0
            internal_component_max = (
                raw_max / institutional_raw_total_max * 100.0
                if available and raw_max is not None
                else 0.0
            )
            internal_component_score = (
                min(max(raw_score, 0.0), raw_max)
                / institutional_raw_total_max
                * 100.0
                if available and raw_score is not None and raw_max is not None
                else 0.0
            )
            institutional_dimension_components.append(
                (key, internal_component_score, internal_component_max, available)
            )
        sentiment = _dimension_result(
            "板块强度", 20.0, institutional_dimension_components,
            institutional_unavailable_reasons,
        )
    else:
        # 兼容没有机构分项的历史缓存，继续使用旧的日频/实时三项评分。
        # 内部 100 分制：板块形态 50 分、过热风险 33.2 分、当日表现 16.8 分。
        sector_pattern_available = rotation_available and rotation_score is not None
        sector_pattern = (rotation_score / 20.0) * 50.0 if rotation_score is not None else 0.0

        # 2. 板块过热风险 (内部 33.2 分)：三个子项各自门控，缺哪项哪项不计分
        overheat_gain_available = five_day_change is not None
        if not overheat_gain_available:
            overheat_gain = 0.0
        elif five_day_change < 0.05:
            overheat_gain = 16.8
        elif five_day_change < 0.10:
            overheat_gain = 13.2
        elif five_day_change < 0.15:
            overheat_gain = 6.8
        elif five_day_change < 0.20:
            overheat_gain = 3.2
        else:
            overheat_gain = 0.0

        overheat_streak_available = bool(days)
        consecutive_up = 0
        for day in reversed(days):
            if (finite(day.get("change_pct")) or 0.0) > 0:
                consecutive_up += 1
            else:
                break
        if not overheat_streak_available:
            overheat_streak = 0.0
        elif consecutive_up <= 2:
            overheat_streak = 10.0
        elif consecutive_up <= 4:
            overheat_streak = 6.8
        elif consecutive_up <= 6:
            overheat_streak = 3.2
        else:
            overheat_streak = 0.0

        overheat_rank_available = (
            realtime_rank is not None
            and realtime_rank_count is not None
            and realtime_rank_count > 0
        )
        if not overheat_rank_available:
            overheat_rank = 0.0
        else:
            rank_percentile = realtime_rank / realtime_rank_count
            if rank_percentile < 0.20:
                overheat_rank = 0.0
            elif rank_percentile < 0.40:
                overheat_rank = 3.2
            else:
                overheat_rank = 6.4

        overheat_available = (
            overheat_gain_available or overheat_streak_available or overheat_rank_available
        )
        overheat_score = overheat_gain + overheat_streak + overheat_rank
        overheat_max = (
            (16.8 if overheat_gain_available else 0.0)
            + (10.0 if overheat_streak_available else 0.0)
            + (6.4 if overheat_rank_available else 0.0)
        )

        # 3. 板块当日表现 (16.8内部点)
        sector_change = finite(sector.get("change_pct"))
        if sector_change is None:
            sector_change = finite(sector.get("realtime_change_pct"))
        up_ratio = finite(sector.get("up_ratio"))

        current_change_available = sector_change is not None
        if not current_change_available:
            current_change = 0.0
        elif sector_change >= 0.04:
            current_change = 10.0
        elif sector_change >= 0.02:
            current_change = _linear(sector_change, 0.02, 0.04, 3.2) + 6.8
        elif sector_change >= 0.0:
            current_change = _linear(sector_change, 0.0, 0.02, 3.2) + 3.2
        else:
            current_change = 0.0

        current_breadth_available = up_ratio is not None
        if not current_breadth_available:
            current_breadth = 0.0
        elif up_ratio >= 0.80:
            current_breadth = 6.8
        elif up_ratio >= 0.60:
            current_breadth = _linear(up_ratio, 0.60, 0.80, 3.4) + 3.4
        else:
            current_breadth = _linear(up_ratio, 0.0, 0.60, 3.4)

        sector_current_available = current_change_available or current_breadth_available
        sector_current_score = current_change + current_breadth
        sector_current_max = (
            (10.0 if current_change_available else 0.0)
            + (6.8 if current_breadth_available else 0.0)
        )

        sentiment = _dimension_result("板块强度", 20.0, [
            ("sector_pattern", sector_pattern, 50.0, sector_pattern_available),
            ("overheat_risk", overheat_score, overheat_max, overheat_available),
            ("sector_current", sector_current_score, sector_current_max, sector_current_available),
        ], {
            "sector_pattern": rotation_reason(3),
            "overheat_risk": "缺少板块历史涨幅、连涨天数或实时排名",
            "sector_current": "实时板块涨幅和上涨广度未返回",
        })

    # ========================================
    # 三、拉升健康度（20分）；内部按 100 分制计算
    # ========================================
    # 1. 板块内地位 (内部 38.4 分) - 拉升健康度内权重最高
    leadership = sector.get("leadership")
    stock_rank = finite(sector.get("stock_rank"))
    leader_gap_pct = finite(sector.get("leader_gap_pct")) or 0.0
    is_leader = bool(sector.get("is_sector_leader", False))
    position_available = bool(sector) and (
        leadership is not None or stock_rank is not None
    )

    if not position_available:
        sector_position = 0.0
    elif is_leader and leader_gap_pct >= 0.10:
        # 绝对龙头：排名第1且领先≥10%涨停进度（主板≈1%、20cm≈2%）
        sector_position = 38.4
    elif is_leader:
        # 并列龙头：排名第1但领先不足10%涨停进度
        sector_position = 30.8
    elif leadership == "front" and (stock_rank is None or stock_rank <= 3 or leader_gap_pct <= 0.10):
        # 前排强势：排名2-3或与龙头差距≤10%涨停进度
        sector_position = 23.2
    elif leadership == "front":
        # 前排跟随：排名4-5
        sector_position = 15.2
    elif stock_rank is not None and stock_rank <= 10:
        # 中游位置：排名6-10
        sector_position = 7.6
    else:
        # 跟风位置：排名10以后
        sector_position = 0.0

    # 2. 拉升形态 (内部 28.4 分)：启动点之后的拉升用时、流畅度、封板前量能。
    #    封死后缩量是强势特征，只统计触板前的量能，不按「量价背离」惩罚。
    pull_up_minutes = flow.get("pull_up_minutes")
    pull_up_max_drawdown = finite(flow.get("pull_up_max_drawdown"))
    pre_seal_growth = finite(flow.get("pre_seal_amount_growth"))
    pullup_available = pull_up_minutes is not None

    # 2.1 拉升用时 (内部 10.4 分)：一波流最强，墨迹拉升减分
    if not pullup_available:
        pullup_time = 0.0
    elif pull_up_minutes <= 15:
        pullup_time = 10.4
    elif pull_up_minutes <= 30:
        pullup_time = 7.6
    elif pull_up_minutes <= 60:
        pullup_time = 5.2
    elif pull_up_minutes <= 120:
        pullup_time = 2.4
    else:
        pullup_time = 1.2

    # 2.2 拉升流畅度 (内部 7.6 分)：拉升段最大回撤越小越强
    pullup_smooth_available = pullup_available and pull_up_max_drawdown is not None
    pullup_smooth = (
        7.6 - _linear(pull_up_max_drawdown, 0.01, 0.05, 7.6)
        if pullup_smooth_available and pull_up_max_drawdown is not None
        else 0.0
    )

    # 2.3 封板前量能 (内部 10.4 分)：放量上攻最强
    pre_seal_available = pullup_available and pre_seal_growth is not None
    if not pre_seal_available or pre_seal_growth is None:
        pullup_volume = 0.0
    elif pre_seal_growth > 0.50:
        pullup_volume = 10.4  # 放量突破
    elif pre_seal_growth > 0.0:
        pullup_volume = _linear(pre_seal_growth, 0.0, 0.50, 4.0) + 6.4  # 温和放量
    elif pre_seal_growth > -0.20:
        pullup_volume = _linear(pre_seal_growth, -0.20, 0.0, 4.0) + 2.4  # 缩量上涨
    else:
        pullup_volume = 0.0  # 无量空拉

    pullup_score = pullup_time + pullup_smooth + pullup_volume
    pullup_max = (
        (10.4 if pullup_available else 0.0)
        + (7.6 if pullup_smooth_available else 0.0)
        + (10.4 if pre_seal_available else 0.0)
    )

    # 3. 资金强度 (内部 25.6 分)：封板/贴板后主动成交只剩卖单，净流入指标失真，
    #    此时显式标记数据不足，不再报「主力资金流出」假警。
    sealed_now = bool(flow.get("sealed_now"))
    net_flow_ratio = finite(flow.get("net_flow_ratio"))
    capital_score_raw = finite(flow.get("capital_score"))
    capital_max = finite(flow.get("capital_max_score")) or 25.0
    capital_available = bool(flow) and not sealed_now and (
        net_flow_ratio is not None or capital_score_raw is not None
    )

    if not capital_available:
        capital_flow = 0.0
    elif net_flow_ratio is not None:
        # 基于净流入比例打分
        if net_flow_ratio >= 0.40:
            capital_flow = 25.6
        elif net_flow_ratio >= 0.20:
            capital_flow = _linear(net_flow_ratio, 0.20, 0.40, 7.6) + 18.0
        elif net_flow_ratio >= 0.0:
            capital_flow = _linear(net_flow_ratio, 0.0, 0.20, 7.6) + 10.4
        elif net_flow_ratio >= -0.20:
            capital_flow = _linear(net_flow_ratio, -0.20, 0.0, 7.6) + 2.4
        else:
            capital_flow = 0.0
    elif capital_max > 0:
        # 备选：使用 capital_score 转换
        capital_flow = ((capital_score_raw or 0.0) / capital_max) * 25.6
    else:
        capital_flow = 0.0

    # 4. 均线形态 (内部 7.6 分)：与前端「均线形态」展示口径一致，五线严格递减。
    price = finite(tech.get("price"))
    ma5 = finite(tech.get("ma5"))
    ma10 = finite(tech.get("ma10"))
    ma20 = finite(tech.get("ma20"))
    ma60 = finite(tech.get("ma60"))
    ma_available = all(
        value is not None and value > 0 for value in (price, ma5, ma10, ma20, ma60)
    )
    if not ma_available or price is None or ma5 is None or ma10 is None or ma20 is None or ma60 is None:
        ma_alignment = 0.0
    elif price > ma5 > ma10 > ma20 > ma60:
        ma_alignment = 7.6
    elif price > ma5 > ma10 > ma20:
        ma_alignment = 6.4
    elif price > ma5 > ma10:
        ma_alignment = 5.2
    elif price > ma5:
        ma_alignment = 2.4
    else:
        ma_alignment = 0.0

    health = _dimension_result("拉升健康度", 20.0, [
        ("sector_position", sector_position, 38.4, position_available),
        ("pullup_form", pullup_score, pullup_max, pullup_available),
        ("capital_flow", capital_flow, 25.6, capital_available),
        ("ma_alignment", ma_alignment, 7.6, ma_available),
    ], {
        "sector_position": "未取得个股在板块内的实时排名",
        "pullup_form": "分钟行情不足，无法识别拉升区间",
        "capital_flow": (
            "已封板，主动资金流指标失真，不参与评分"
            if sealed_now else "实时主动资金流数据未返回"
        ),
        "ma_alignment": "日线数据不足，需收盘价及 MA5/MA10/MA20/MA60",
    })

    # ========================================
    # 综合评分与评级：按可得满分折算，数据不完整时评级封顶 B
    # ========================================
    dimensions = {"history": history, "sentiment": sentiment, "health": health}
    available_total_max = sum(item["max_score"] for item in dimensions.values())
    raw_total = sum(item["score"] for item in dimensions.values())
    # 分母取各维度 full_max_score 之和，维度权重调整时无需同步改这里的常数，
    # 否则 data_completeness 永远到不了 1.0，评级会被静默封顶 B。
    full_total_max = sum(item["full_max_score"] for item in dimensions.values())
    data_completeness = (
        round(available_total_max / full_total_max, 2) if full_total_max > 0 else 0.0
    )
    total_score = (
        round(raw_total / available_total_max * 100.0, 1)
        if available_total_max > 0
        else 0.0
    )

    # 评级判定
    if total_score >= 90:
        grade = "S"
    elif total_score >= 85:
        grade = "A+"
    elif total_score >= 75:
        grade = "A"
    elif total_score >= 65:
        grade = "B+"
    elif total_score >= 55:
        grade = "B"
    elif total_score >= 45:
        grade = "C"
    else:
        grade = "D"
    if data_completeness < 1.0 and _GRADE_ORDER.index(grade) > _GRADE_ORDER.index("B"):
        grade = "B"
    grade_label = _GRADE_LABELS[grade]
    if available_total_max <= 0:
        grade = "D"
        grade_label = "数据不足"

    # ========================================
    # 智能警示和优势：全部由真实数据驱动，缺数据的维度不出警
    # ========================================
    warnings: list[str] = []
    strengths: list[str] = []
    # 历史基因
    if next_day_red_rate is not None:
        if next_day_red_rate < 0.60:
            warnings.append("次日收红率偏低")
        elif next_day_red_rate >= 0.80:
            strengths.append("次日收红率高")

    if consecutive_rate is not None and consecutive_rate >= 0.70:
        strengths.append("连板能力强")

    # 板块强度：机构式横截面分数替代离散状态标签。
    institutional_score = finite(sector.get("institutional_score"))
    institutional_max = finite(sector.get("institutional_max_score"))
    if institutional_score is not None and institutional_max and institutional_max > 0:
        institutional_pct = institutional_score / institutional_max
        if institutional_pct >= 0.75:
            strengths.append("板块相对强度高")
        elif institutional_pct < 0.35:
            warnings.append("板块相对强度偏弱")
    else:
        # 兼容历史缓存中尚未带机构分数的快照；新快照不会走这里。
        rotation_label = sector.get("rotation_label")
        if rotation_available and rotation_label == "主线":
            strengths.append("主线板块")
        elif rotation_available and rotation_label == "上升":
            strengths.append("板块快速走强")
        elif rotation_available and rotation_label == "退潮":
            warnings.append("板块退潮中")

    if not has_institutional_components:
        if five_day_change is not None:
            if five_day_change > 0.15:
                warnings.append("板块涨幅过大，注意回调风险")
            elif five_day_change < 0.05:
                strengths.append("板块涨幅不大，安全")

        if overheat_streak_available and consecutive_up > 5:
            warnings.append("板块连续上涨多日，过热")

    # 拉升健康度
    # 阈值使用内部 100 分制，避免随外层卡片权重变化而改变业务含义。
    if position_available:
        if sector_position >= 38.4:
            strengths.append("板块绝对龙头")
        elif sector_position >= 23.2:
            strengths.append("板块前排")
        elif sector_position < 7.6:
            warnings.append("非板块龙头")

    if capital_available:
        if capital_flow >= 18.0:
            strengths.append("主力大幅流入")
        elif capital_flow < 10.4:
            warnings.append("主力资金流出")

    if pullup_available:
        one_wave = (
            pull_up_minutes is not None
            and pull_up_minutes <= 15
            and pull_up_max_drawdown is not None
            and pull_up_max_drawdown <= 0.01
        )
        if one_wave:
            strengths.append("一波流拉升")
        elif pull_up_max_drawdown is not None and pull_up_max_drawdown >= 0.05:
            warnings.append("拉升反复，分歧大")

    return {
        "comprehensive_score": total_score,
        "score_model_version": SCORE_MODEL_VERSION,
        "max_score": 100.0,
        "data_completeness": data_completeness,
        "grade": grade,
        "grade_label": grade_label,
        "dimensions": dimensions,
        "warnings": warnings,
        "strengths": strengths,
        "detail_available": {
            "premium_gene": bool(gene),
            "intraday_flow": bool(flow),
            "technical": bool(tech),
            "sector": bool(sector),
            "four_mode": bool(four_mode_score),
            "board_quality": bool(board_quality),
        },
    }
