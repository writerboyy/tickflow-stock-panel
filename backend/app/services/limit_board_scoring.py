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


def premium_gene_detail(values: dict[str, Any]) -> dict[str, Any] | None:
    required = {
        "limit_up_count",
        "premium_5_count",
        "next_day_observation_count",
        "next_day_red_rate",
        "first_board_attempt_count",
        "first_board_sealed_count",
        "first_board_seal_rate",
        "first_board_broken_rate",
        "consecutive_rate",
    }
    if not required.issubset(values):
        return None
    numbers = {key: finite(values.get(key)) for key in required}
    if any(value is None for value in numbers.values()):
        return None

    limit_count = max(0.0, numbers["limit_up_count"] or 0.0)
    observations = max(0.0, numbers["next_day_observation_count"] or 0.0)
    attempts = max(0.0, numbers["first_board_attempt_count"] or 0.0)
    sealed = max(0.0, numbers["first_board_sealed_count"] or 0.0)
    premium_rate = (
        max(0.0, numbers["premium_5_count"] or 0.0) / observations
        if observations > 0 else 0.0
    )
    observation_confidence = min(observations / 10.0, 1.0)
    attempt_confidence = min(attempts / 10.0, 1.0)
    sealed_confidence = min(sealed / 10.0, 1.0)
    components = {
        "limit_frequency": min(limit_count / 12.0, 1.0) * 7.0,
        "next_day_red": _clamp(numbers["next_day_red_rate"] or 0.0) * observation_confidence * 7.0,
        "premium_5": _clamp(premium_rate) * observation_confidence * 5.0,
        "first_board_seal": _clamp(numbers["first_board_seal_rate"] or 0.0) * attempt_confidence * 6.0,
        "consecutive": _clamp(numbers["consecutive_rate"] or 0.0) * sealed_confidence * 5.0,
    }
    return {
        "score": round(sum(components.values()), 2),
        "max_score": 30.0,
        "components": {key: round(value, 2) for key, value in components.items()},
        "as_of": str(values.get("as_of") or "") or None,
        "window_days": int(finite(values.get("window_days")) or 200),
        "limit_up_count": int(limit_count),
        "premium_5_count": int(numbers["premium_5_count"] or 0),
        "next_day_observation_count": int(observations),
        "next_day_red_rate": numbers["next_day_red_rate"],
        "premium_5_rate": premium_rate,
        "first_board_attempt_count": int(attempts),
        "first_board_sealed_count": int(sealed),
        "first_board_seal_rate": numbers["first_board_seal_rate"],
        "first_board_broken_rate": numbers["first_board_broken_rate"],
        "consecutive_rate": numbers["consecutive_rate"],
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
) -> dict[str, Any] | None:
    if not snapshot.get("valid") or (finite(snapshot.get("coverage_ratio")) or 0.0) < 0.8:
        return None
    history = rotation_detail(rotation, _rotation_name(target), today)
    stock = stock_rows.get(symbol)
    if history is None or stock is None:
        return None
    candidate_change = finite(stock.get("change_pct"))
    sector_change = finite(snapshot.get("change_pct"))
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
    member_count = int(snapshot.get("total_count") or len(member_symbols))
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
    up_count = int(snapshot.get("up_count") or 0)
    valid_count = int(snapshot.get("valid_count") or len(ranked))
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
        "coverage_ratio": finite(snapshot.get("coverage_ratio")),
        "valid_count": valid_count,
        "member_count": member_count,
        "leader": leader,
        "stock_rank": candidate_index + 1,
        "stock_change_pct": candidate_change,
        "leader_gap_pct": leader_gap,
        "leadership": leadership,
        "is_sector_leader": is_leader,
        "rotation_components": history["components"],
        **rotation_fields,
    }


def _rotation_name(target: dict[str, Any]) -> str:
    name = str(target.get("name") or target.get("value") or "")
    return name.split(" / ")[-1].strip()
