"""Deterministic candidate scoring for the limit-board approval pool."""
from __future__ import annotations

from datetime import date
from math import prod
from typing import Any


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
    }


def intraday_flow_detail(
    intraday: dict[str, Any] | None,
    *,
    previous_close: object = None,
    external_flow: dict[str, Any] | None = None,
    as_of: str | None = None,
) -> dict[str, Any] | None:
    """Score live intraday trend and capital-flow direction.

    Minute bars are the required live input.  Active buy/sell ratios remain
    distinct from the Kaipanla cumulative main-net-flow speed contract.
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
        "as_of": as_of,
    }


def rotation_detail(
    rotation: dict[str, Any], sector_name: str, today: date,
) -> dict[str, Any] | None:
    days: list[dict[str, Any]] = []
    trading_dates = []
    for raw_date in rotation.get("dates") or []:
        try:
            trading_date = date.fromisoformat(str(raw_date))
        except ValueError:
            continue
        if trading_date >= today:
            continue
        trading_dates.append(trading_date)
    for trading_date in sorted(set(trading_dates), reverse=True):
        raw_date = trading_date.isoformat()
        rows = rotation.get("columns", {}).get(str(raw_date)) or []
        match_index = next(
            (index for index, item in enumerate(rows) if str(item[0]) == sector_name),
            None,
        )
        if match_index is None:
            continue
        change = finite(rows[match_index][1])
        if change is None:
            continue
        count = len(rows)
        percentile = 1.0 if count <= 1 else 1.0 - match_index / (count - 1)
        days.append({
            "date": trading_date.isoformat(),
            "change_pct": change,
            "rank": match_index + 1,
            "rank_count": count,
            "rank_percentile": percentile,
        })
        if len(days) == 5:
            break
    if len(days) < 5:
        return None
    days.reverse()
    returns = [float(item["change_pct"]) for item in days]
    percentiles = [float(item["rank_percentile"]) for item in days]
    compound = prod(1.0 + value for value in returns) - 1.0
    mean = sum(returns) / 5.0
    slope = sum((index - 2.0) * (value - mean) for index, value in enumerate(returns)) / 10.0
    rank_change = percentiles[-1] - percentiles[0]
    top_days = sum(value >= 0.8 for value in percentiles)
    yesterday = returns[-1]
    components = {
        "compound": _linear(compound, -0.05, 0.10, 6.0),
        "slope": _linear(slope, -0.01, 0.01, 4.0),
        "rank_change": _linear(rank_change, -0.30, 0.30, 4.0),
        "persistence": top_days / 5.0 * 3.0,
        "yesterday": _linear(yesterday, -0.02, 0.03, 3.0),
    }
    if top_days >= 3 and percentiles[-1] >= 0.8 and compound > 0:
        label = "主线"
    elif rank_change >= 0.15 and slope > 0:
        label = "上升"
    elif rank_change <= -0.15 and slope < 0:
        label = "退潮"
    else:
        label = "震荡"
    return {
        "score": round(sum(components.values()), 2),
        "max_score": 20.0,
        "components": {key: round(value, 2) for key, value in components.items()},
        "days": days,
        "five_day_change_pct": compound,
        "trend_slope": slope,
        "rank_change": rank_change,
        "top_20_days": top_days,
        "yesterday_change_pct": yesterday,
        "rotation_label": label,
    }


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
        ranked.append({
            "symbol": member_symbol,
            "name": member.get("name") or member_symbol,
            "change_pct": change,
            "amount": max(0.0, finite(member.get("amount")) or 0.0),
        })
    ranked.sort(key=lambda row: (-row["change_pct"], -row["amount"], row["symbol"]))
    member_count = int(effective_snapshot.get("total_count") or len(member_symbols))
    if member_count < 5 or not ranked:
        return None
    candidate_index = next(
        (index for index, row in enumerate(ranked) if row["symbol"] == symbol),
        None,
    )
    if candidate_index is None:
        return None
    top_change = float(ranked[0]["change_pct"])
    co_leaders = [
        row for row in ranked
        if float(row["change_pct"]) > 0
        and top_change - float(row["change_pct"]) <= 0.001
    ]
    leader = (
        min(
            co_leaders,
            key=lambda row: (-float(row["amount"]), str(row["symbol"])),
        )
        if co_leaders else ranked[0]
    )
    leader_gap = top_change - candidate_change
    is_leader = candidate_change > 0 and leader_gap <= 0.001
    is_front = not is_leader and (candidate_index < 3 or leader_gap <= 0.01)
    leadership = "leader" if is_leader else "front" if is_front else "follower"
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
        "leader_gap_pct": leader_gap,
        "leadership": leadership,
        "is_sector_leader": is_leader,
        "rotation_available": rotation_available,
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
    }


def _rotation_name(target: dict[str, Any]) -> str:
    name = str(target.get("name") or target.get("value") or "")
    return name.split(" / ")[-1].strip()


def comprehensive_score(
    candidate_score_detail: dict[str, Any] | None,
    *,
    board_quality: dict[str, Any] | None = None,
    four_mode_score: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """计算打板池综合评分（100分制）：历史涨停基因(30分) + 板块情绪周期(30分) + 拉升健康度(40分)

    适用于打板池，评估标的打板价值。买入池不使用此评分。

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
    # 一、历史涨停基因（30分）
    # ========================================
    history_components = {}

    # 1. 次日收红率 (12分)
    next_day_red_rate = finite(gene.get("next_day_red_rate"))
    if next_day_red_rate is not None:
        history_components["next_day_red"] = _clamp(next_day_red_rate) * 12.0
    else:
        history_components["next_day_red"] = 0.0

    # 2. 封板成功率 (12分)
    first_board_seal_rate = finite(gene.get("first_board_seal_rate"))
    if first_board_seal_rate is not None:
        history_components["seal_success"] = _clamp(first_board_seal_rate) * 12.0
    else:
        history_components["seal_success"] = 0.0

    # 3. 连板能力 (6分)
    consecutive_rate = finite(gene.get("consecutive_rate"))
    if consecutive_rate is not None:
        history_components["consecutive_ability"] = _clamp(consecutive_rate) * 6.0
    else:
        history_components["consecutive_ability"] = 0.0

    history_score = sum(history_components.values())

    # ========================================
    # 二、板块情绪周期（30分）
    # ========================================
    sentiment_components = {}

    # 1. 板块K线形态 (15分)
    rotation_score = finite(sector.get("rotation_score")) or 0.0
    rotation_max = 20.0  # rotation_detail 的 max_score
    # 将 rotation_score 转换到15分制
    sentiment_components["sector_pattern"] = (rotation_score / rotation_max) * 15.0

    # 2. 板块过热风险 (10分)
    five_day_change = finite(sector.get("five_day_change_pct")) or 0.0
    days = sector.get("days") or []

    # 2.1 5日涨幅风险 (5分)
    if five_day_change < 0.05:
        overheat_gain = 5.0
    elif five_day_change < 0.10:
        overheat_gain = 4.0
    elif five_day_change < 0.15:
        overheat_gain = 2.0
    elif five_day_change < 0.20:
        overheat_gain = 1.0
    else:
        overheat_gain = 0.0

    # 2.2 连续上涨天数 (3分)
    consecutive_up = 0
    for day in reversed(days):
        if finite(day.get("change_pct")) or 0.0 > 0:
            consecutive_up += 1
        else:
            break
    if consecutive_up <= 2:
        overheat_streak = 3.0
    elif consecutive_up <= 4:
        overheat_streak = 2.0
    elif consecutive_up <= 6:
        overheat_streak = 1.0
    else:
        overheat_streak = 0.0

    # 2.3 排名位置风险 (2分)
    realtime_rank = finite(sector.get("realtime_rank"))
    realtime_rank_count = finite(sector.get("realtime_rank_count"))
    if realtime_rank is not None and realtime_rank_count is not None and realtime_rank_count > 0:
        rank_percentile = realtime_rank / realtime_rank_count
        if rank_percentile < 0.20:
            overheat_rank = 0.0  # 顶部区域
        elif rank_percentile < 0.40:
            overheat_rank = 1.0  # 高位
        else:
            overheat_rank = 2.0  # 安全
    else:
        overheat_rank = 2.0  # 无数据默认安全

    sentiment_components["overheat_risk"] = overheat_gain + overheat_streak + overheat_rank

    # 3. 板块当日表现 (5分)
    sector_change = finite(sector.get("change_pct")) or finite(sector.get("realtime_change_pct")) or 0.0
    up_ratio = finite(sector.get("up_ratio")) or 0.0

    # 3.1 当日涨跌幅 (3分)
    if sector_change >= 0.04:
        current_change = 3.0
    elif sector_change >= 0.02:
        current_change = _linear(sector_change, 0.02, 0.04, 1.0) + 2.0
    elif sector_change >= 0.0:
        current_change = _linear(sector_change, 0.0, 0.02, 1.0) + 1.0
    else:
        current_change = 0.0

    # 3.2 上涨家数占比 (2分)
    if up_ratio >= 0.80:
        current_breadth = 2.0
    elif up_ratio >= 0.60:
        current_breadth = _linear(up_ratio, 0.60, 0.80, 1.0) + 1.0
    else:
        current_breadth = _linear(up_ratio, 0.0, 0.60, 1.0)

    sentiment_components["sector_current"] = current_change + current_breadth

    sentiment_score = sum(sentiment_components.values())

    # ========================================
    # 三、拉升健康度（40分）
    # ========================================
    health_components = {}

    # 1. 板块内地位 (15分) - 权重最高
    leadership = sector.get("leadership")
    stock_rank = finite(sector.get("stock_rank"))
    leader_gap_pct = finite(sector.get("leader_gap_pct")) or 0.0
    is_leader = sector.get("is_sector_leader", False)

    if is_leader and leader_gap_pct >= 0.01:
        # 绝对龙头：排名第1且领先第2名≥1%
        health_components["sector_position"] = 15.0
    elif is_leader:
        # 并列龙头：排名第1但领先不足1%
        health_components["sector_position"] = 12.0
    elif leadership == "front" and (stock_rank is None or stock_rank <= 3 or leader_gap_pct <= 0.01):
        # 前排强势：排名2-3或与龙头差距≤1%
        health_components["sector_position"] = 9.0
    elif leadership == "front":
        # 前排跟随：排名4-5
        health_components["sector_position"] = 6.0
    elif stock_rank is not None and stock_rank <= 10:
        # 中游位置：排名6-10
        health_components["sector_position"] = 3.0
    else:
        # 跟风位置：排名10以后
        health_components["sector_position"] = 0.0

    # 2. 分钟级量价 (10分)
    trend_pct = finite(flow.get("trend_pct")) or 0.0
    underwater_ratio = finite(flow.get("underwater_ratio")) or 0.0
    amount_growth = finite(flow.get("amount_growth"))

    # 2.1 价格走势 + 水上占比 (5分)
    if trend_pct >= 0.05:
        price_strength = 4.0
    elif trend_pct >= 0.02:
        price_strength = _linear(trend_pct, 0.02, 0.05, 2.0) + 2.0
    elif trend_pct >= 0.0:
        price_strength = _linear(trend_pct, 0.0, 0.02, 2.0)
    else:
        price_strength = 0.0

    water_above_ratio = 1.0 - underwater_ratio
    water_score = water_above_ratio * 1.0

    # 2.2 量价配合 (5分)
    if amount_growth is not None and trend_pct > 0:
        if amount_growth > 0.50:
            volume_coord = 5.0  # 放量突破
        elif amount_growth > 0.0:
            volume_coord = _linear(amount_growth, 0.0, 0.50, 2.0) + 3.0  # 温和放量
        elif amount_growth > -0.20:
            volume_coord = _linear(amount_growth, -0.20, 0.0, 2.0) + 1.0  # 缩量上涨
        else:
            volume_coord = 0.0  # 量价背离
    else:
        volume_coord = 0.0

    health_components["intraday_volume_price"] = price_strength + water_score + volume_coord

    # 3. 资金流向 (10分)
    net_flow_ratio = finite(flow.get("net_flow_ratio"))
    capital_score_raw = finite(flow.get("capital_score")) or 0.0
    capital_max = finite(flow.get("capital_max_score")) or 25.0

    if net_flow_ratio is not None:
        # 基于净流入比例打分
        if net_flow_ratio >= 0.40:
            capital_flow = 10.0
        elif net_flow_ratio >= 0.20:
            capital_flow = _linear(net_flow_ratio, 0.20, 0.40, 3.0) + 7.0
        elif net_flow_ratio >= 0.0:
            capital_flow = _linear(net_flow_ratio, 0.0, 0.20, 3.0) + 4.0
        elif net_flow_ratio >= -0.20:
            capital_flow = _linear(net_flow_ratio, -0.20, 0.0, 3.0) + 1.0
        else:
            capital_flow = 0.0
    elif capital_max > 0:
        # 备选：使用 capital_score 转换
        capital_flow = (capital_score_raw / capital_max) * 10.0
    else:
        capital_flow = 0.0

    health_components["capital_flow"] = capital_flow

    # 4. 日K形态 (5分)
    price = finite(tech.get("price"))
    ma5 = finite(tech.get("ma5"))
    ma10 = finite(tech.get("ma10"))
    ma20 = finite(tech.get("ma20"))
    ma60 = finite(tech.get("ma60"))

    # 4.1 均线排列 (3分)
    if all(x is not None and x > 0 for x in [price, ma5, ma10, ma20, ma60]):
        if price > ma5 > ma10 > ma20 > ma60:
            ma_align = 3.0  # 完美多头
        elif price > ma5 > ma10 > ma20:
            ma_align = 2.5  # 强多头
        elif price > ma5 > ma10:
            ma_align = 2.0  # 中多头
        elif price > ma5:
            ma_align = 1.0  # 弱多头
        else:
            ma_align = 0.0  # 均线纠缠或空头
    else:
        ma_align = 0.0

    # 4.2 K线实体和影线 (2分) - 需要日K数据，这里简化处理
    # 如果有当日开高低收数据可以计算，否则给中间分
    health_components["daily_k_pattern"] = ma_align + 1.0  # 暂时简化，均线3分+形态2分默认1分

    health_score = sum(health_components.values())

    # ========================================
    # 综合评分与评级
    # ========================================
    total_score = history_score + sentiment_score + health_score

    # 评级判定
    if total_score >= 90:
        grade = "S"
        grade_label = "完美"
    elif total_score >= 85:
        grade = "A+"
        grade_label = "优秀"
    elif total_score >= 75:
        grade = "A"
        grade_label = "良好"
    elif total_score >= 65:
        grade = "B+"
        grade_label = "中上"
    elif total_score >= 55:
        grade = "B"
        grade_label = "中等"
    elif total_score >= 45:
        grade = "C"
        grade_label = "一般"
    else:
        grade = "D"
        grade_label = "较差"

    # ========================================
    # 智能警示和优势
    # ========================================
    warnings = []
    strengths = []

    # 历史基因
    if history_components.get("next_day_red", 0) < 7.2:  # <60%
        warnings.append("次日收红率偏低")
    elif history_components.get("next_day_red", 0) >= 9.6:  # ≥80%
        strengths.append("次日收红率高")

    if history_components.get("consecutive_ability", 0) >= 4.2:  # ≥70%
        strengths.append("连板能力强")

    # 板块情绪
    rotation_label = sector.get("rotation_label")
    if rotation_label == "主线":
        strengths.append("主线板块")
    elif rotation_label == "上升":
        strengths.append("板块快速走强")
    elif rotation_label == "退潮":
        warnings.append("板块退潮中")

    if five_day_change > 0.15:
        warnings.append("板块涨幅过大，注意回调风险")
    elif five_day_change < 0.05:
        strengths.append("板块涨幅不大，安全")

    if consecutive_up > 5:
        warnings.append("板块连续上涨多日，过热")

    # 拉升健康度
    if health_components.get("sector_position", 0) >= 15.0:
        strengths.append("板块绝对龙头")
    elif health_components.get("sector_position", 0) >= 9.0:
        strengths.append("板块前排")
    elif health_components.get("sector_position", 0) < 3.0:
        warnings.append("非板块龙头")

    if health_components.get("capital_flow", 0) >= 7.0:
        strengths.append("主力大幅流入")
    elif health_components.get("capital_flow", 0) < 4.0:
        warnings.append("主力资金流出")

    if ma_align >= 3.0:
        strengths.append("完美多头排列")
    elif ma_align < 1.0:
        warnings.append("均线压制")

    return {
        "comprehensive_score": round(total_score, 1),
        "max_score": 100.0,
        "grade": grade,
        "grade_label": grade_label,
        "dimensions": {
            "history": {
                "score": round(history_score, 1),
                "max_score": 30.0,
                "percentage": round(history_score / 30.0 * 100, 1) if history_score > 0 else 0.0,
                "components": {k: round(v, 1) for k, v in history_components.items()},
                "label": "历史涨停基因",
            },
            "sentiment": {
                "score": round(sentiment_score, 1),
                "max_score": 30.0,
                "percentage": round(sentiment_score / 30.0 * 100, 1) if sentiment_score > 0 else 0.0,
                "components": {k: round(v, 1) for k, v in sentiment_components.items()},
                "label": "板块情绪周期",
            },
            "health": {
                "score": round(health_score, 1),
                "max_score": 40.0,
                "percentage": round(health_score / 40.0 * 100, 1) if health_score > 0 else 0.0,
                "components": {k: round(v, 1) for k, v in health_components.items()},
                "label": "拉升健康度",
            },
        },
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
