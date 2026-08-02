"""HiThink market heat and skyrocket radar service."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.plugins.hithink.client import HiThinkClient

SH_TZ = ZoneInfo("Asia/Shanghai")
SOURCE = "hithink"
SOURCE_LABEL = "HiThink/Fuyao 同花顺金融数据服务"

LIST_SPECS = (
    ("hot_day", "hot", "day", "热股榜 · 24小时"),
    ("hot_hour", "hot", "hour", "热股榜 · 小时"),
    ("skyrocket_day", "skyrocket", "day", "飙升榜 · 24小时"),
    ("skyrocket_hour", "skyrocket", "hour", "飙升榜 · 小时"),
)

OVERLAP_SPECS = (
    ("hot_vs_skyrocket_day", "热股榜 vs 飙升榜 · 24小时", "hot_day", "skyrocket_day"),
    ("hot_vs_skyrocket_hour", "热股榜 vs 飙升榜 · 小时", "hot_hour", "skyrocket_hour"),
    ("hot_period_overlap", "热股榜 24小时 vs 小时", "hot_day", "hot_hour"),
    ("skyrocket_period_overlap", "飙升榜 24小时 vs 小时", "skyrocket_day", "skyrocket_hour"),
)


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp_iso(value: Any) -> str | None:
    if value is None or value == "":
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return str(value)
    if ts > 10_000_000_000:
        ts /= 1000
    return datetime.fromtimestamp(ts, tz=SH_TZ).isoformat()


def _normalize_rank_item(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "thscode": str(raw.get("thscode") or ""),
        "ticker": str(raw.get("ticker") or ""),
        "name": str(raw.get("name") or ""),
        "rank": _to_int(raw.get("rank")),
        "heat": _to_float(raw.get("heat")),
        "rank_change": _to_int(raw.get("rank_change")),
        "rank_trend": str(raw.get("rank_trend") or ""),
    }


def _normalize_list(
    *,
    key: str,
    list_type: str,
    period: str,
    title: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    items = [
        _normalize_rank_item(item)
        for item in (payload.get("item") or [])
        if isinstance(item, dict)
    ]
    return {
        "key": key,
        "list_type": list_type,
        "period": period,
        "title": title,
        "timestamp": payload.get("timestamp"),
        "timestamp_iso": _timestamp_iso(payload.get("timestamp")),
        "items": items,
        "summary": _summarize_items(items),
    }


def _summarize_items(items: list[dict[str, Any]]) -> dict[str, Any]:
    heats = [item["heat"] for item in items if item.get("heat") is not None]
    changes = [item["rank_change"] for item in items if item.get("rank_change") is not None]
    trends = Counter(str(item.get("rank_trend") or "未知") for item in items)
    return {
        "count": len(items),
        "top_heat": max(heats) if heats else None,
        "avg_heat": (sum(heats) / len(heats)) if heats else None,
        "positive_rank_change_count": sum(1 for value in changes if value > 0),
        "negative_rank_change_count": sum(1 for value in changes if value < 0),
        "flat_rank_change_count": sum(1 for value in changes if value == 0),
        "trend_counts": dict(trends),
    }


def _item_projection(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "rank": item.get("rank"),
        "heat": item.get("heat"),
        "rank_change": item.get("rank_change"),
        "rank_trend": item.get("rank_trend"),
    }


def _build_overlap(
    key: str,
    label: str,
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    left_by_symbol = {item["thscode"]: item for item in left["items"] if item.get("thscode")}
    right_by_symbol = {item["thscode"]: item for item in right["items"] if item.get("thscode")}
    symbols = sorted(
        set(left_by_symbol) & set(right_by_symbol),
        key=lambda symbol: min(
            left_by_symbol[symbol].get("rank") or 9999,
            right_by_symbol[symbol].get("rank") or 9999,
        ),
    )
    items = []
    for symbol in symbols:
        left_item = left_by_symbol[symbol]
        right_item = right_by_symbol[symbol]
        items.append(
            {
                "thscode": symbol,
                "ticker": left_item.get("ticker") or right_item.get("ticker") or "",
                "name": left_item.get("name") or right_item.get("name") or "",
                "left": _item_projection(left_item),
                "right": _item_projection(right_item),
            }
        )
    denominator = min(len(left_by_symbol), len(right_by_symbol)) or 1
    return {
        "key": key,
        "label": label,
        "left_key": left["key"],
        "right_key": right["key"],
        "count": len(items),
        "ratio": len(items) / denominator,
        "items": items,
    }


def _normalize_trend_point(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "thscode": str(raw.get("thscode") or ""),
        "ticker": str(raw.get("ticker") or ""),
        "date": str(raw.get("date") or ""),
        "date_ms": raw.get("date_ms"),
        "rank": _to_int(raw.get("rank")),
    }


def _trend_analysis(points: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [point for point in points if point.get("date") and point.get("rank") is not None]
    if len(valid) < 2:
        return {
            "direction": "insufficient",
            "first_rank": valid[0]["rank"] if valid else None,
            "latest_rank": valid[-1]["rank"] if valid else None,
            "rank_delta": None,
            "points": len(valid),
        }
    first_rank = int(valid[0]["rank"])
    latest_rank = int(valid[-1]["rank"])
    if latest_rank < first_rank:
        direction = "improving"
    elif latest_rank > first_rank:
        direction = "weakening"
    else:
        direction = "flat"
    return {
        "direction": direction,
        "first_rank": first_rank,
        "latest_rank": latest_rank,
        "rank_delta": first_rank - latest_rank,
        "points": len(valid),
    }


def _fetch_rank_trends(
    client: HiThinkClient,
    targets: list[dict[str, Any]],
    *,
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, Any]]:
    trends: dict[str, dict[str, Any]] = {}
    for target in targets:
        thscode = str(target.get("thscode") or "")
        if not thscode:
            continue
        payload = client.get_hot_stock_rank_trend(
            thscode,
            start_date=start_date,
            end_date=end_date,
        )
        points = [
            _normalize_trend_point(item)
            for item in (payload.get("item") or [])
            if isinstance(item, dict)
        ]
        trends[thscode] = {
            "thscode": thscode,
            "ticker": target.get("ticker") or "",
            "name": target.get("name") or "",
            "timestamp": payload.get("timestamp"),
            "timestamp_iso": _timestamp_iso(payload.get("timestamp")),
            "points": points,
            "analysis": _trend_analysis(points),
        }
    return trends


def build_market_heat_radar(
    *,
    client: HiThinkClient | None = None,
    today: date | None = None,
    trend_days: int = 30,
) -> dict[str, Any]:
    client = client or HiThinkClient()
    end = today or datetime.now(SH_TZ).date()
    window_days = max(1, trend_days)
    start = end - timedelta(days=window_days - 1)
    start_text = start.isoformat()
    end_text = end.isoformat()

    lists: dict[str, dict[str, Any]] = {}
    for key, list_type, period, title in LIST_SPECS:
        if list_type == "hot":
            payload = client.get_hot_stock_list(period=period)
        else:
            payload = client.get_skyrocket_list(period=period)
        lists[key] = _normalize_list(
            key=key,
            list_type=list_type,
            period=period,
            title=title,
            payload=payload,
        )

    trend_targets = lists["hot_day"]["items"][:3] or lists["hot_hour"]["items"][:3]
    trends = _fetch_rank_trends(
        client,
        trend_targets,
        start_date=start_text,
        end_date=end_text,
    )
    overlaps = [
        _build_overlap(key, label, lists[left_key], lists[right_key])
        for key, label, left_key, right_key in OVERLAP_SPECS
    ]

    return {
        "source": SOURCE,
        "source_label": SOURCE_LABEL,
        "generated_at": datetime.now(SH_TZ).isoformat(),
        "delay_boundary": "按同花顺/Fuyao 接口返回 timestamp 展示；榜单为当前快照，可能存在服务端数据延迟。",
        "disclaimer": "榜单热度、排名变化和趋势方向仅用于市场观察，不构成投资建议或确定性买卖信号。",
        "trend_window": {
            "start_date": start_text,
            "end_date": end_text,
            "natural_days": window_days,
        },
        "lists": lists,
        "overlaps": overlaps,
        "trend_targets": trend_targets,
        "trends": trends,
    }
