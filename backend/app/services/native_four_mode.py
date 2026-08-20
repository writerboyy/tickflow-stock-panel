"""Native four-mode projections for the short-line hunter.

This module is intentionally independent from the archived external strategy.
It only projects the rows already produced by the short-line hunter's own
history, sector, scoring, quote and tradability pipeline.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable


_MODE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "id": "yje",
        "name": "一进二",
        "summary": "用系统强势股机会分观察从近期强势状态继续走强的标的。",
        "logic": "机会分 + 评分上升速度 + 分时强度 + 成交空间",
        "source": "system_opportunity",
        "filters": ("进入系统机会榜", "实时行情新鲜", "距涨停保留成交空间"),
        "score_components": ("强势确认分", "评分速度", "分时/资金", "距涨停空间"),
    },
    {
        "id": "rzq",
        "name": "弱转强",
        "summary": "使用系统涨停历史识别的反包候选，并按统一强势股评分确认。",
        "logic": "反包历史 + 板块强度 + 溢价基因 + 分时资金",
        "source": "system_rebound_board",
        "filters": ("系统反包历史候选", "板块强度可用", "强势评分可用"),
        "score_components": ("反包来源", "板块强度", "涨停基因", "分时/资金"),
    },
    {
        "id": "qs",
        "name": "趋势股",
        "summary": "按系统技术面和分时趋势状态观察持续性强、未失去成交空间的标的。",
        "logic": "技术趋势 + 20 日动量 + 均线状态 + 分时趋势",
        "source": "system_trend_score",
        "filters": ("技术指标可用", "分时趋势非弱", "未触及涨停"),
        "score_components": ("技术面", "动量", "均线", "分时趋势"),
    },
    {
        "id": "sb",
        "name": "首板",
        "summary": "使用系统首板历史池和实时板块强度，按统一评分查看首板候选。",
        "logic": "首板历史 + 板块强度 + 溢价基因 + 技术面",
        "source": "system_first_board",
        "filters": ("系统首板历史池", "风险名称已排除", "统一强势评分"),
        "score_components": ("首板来源", "板块强度", "涨停基因", "技术面"),
    },
)


def _number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _preview(row: dict[str, Any]) -> dict[str, Any]:
    score_detail = row.get("candidate_score_detail") or {}
    sector = score_detail.get("sector") or {}
    technical = score_detail.get("technical") or {}
    flow = score_detail.get("intraday_flow") or {}
    return {
        "symbol": str(row.get("symbol") or "").strip().upper(),
        "name": str(row.get("name") or row.get("symbol") or "").strip(),
        "candidate_score": _number(row.get("candidate_score")),
        "entry_score": _number(row.get("entry_score")),
        "candidate_score_velocity": _number(row.get("candidate_score_velocity")),
        "candidate_rank": row.get("candidate_rank"),
        "entry_rank": row.get("entry_rank"),
        "change_pct": _number(row.get("change_pct")),
        "limit_gap_pct": _number(row.get("limit_gap_pct")),
        "tradability_state": str(row.get("tradability_state") or "unavailable"),
        "tradability_reason": str(row.get("tradability_reason") or ""),
        "source_modes": [str(value) for value in row.get("source_modes") or []],
        "candidate_reasons": [str(value) for value in row.get("candidate_reasons") or []],
        "entry_reasons": [str(value) for value in row.get("entry_reasons") or []],
        "sector_name": str(sector.get("name") or "").strip() or None,
        "sector_score": _number(sector.get("score")),
        "technical_score": _number(technical.get("score")),
        "trend_state": str(flow.get("trend_state") or "unavailable"),
        "flow_score": _number(flow.get("score")),
    }


def _has_mode(row: dict[str, Any], mode: str) -> bool:
    return mode in {str(value) for value in row.get("source_modes") or []}


def _is_trend(row: dict[str, Any]) -> bool:
    detail = row.get("candidate_score_detail") or {}
    flow = detail.get("intraday_flow") or {}
    technical = detail.get("technical") or {}
    trend_state = str(flow.get("trend_state") or "").lower()
    momentum = _number(technical.get("momentum_20d"))
    return trend_state in {"strong", "neutral"} or (
        momentum is not None and momentum > 0
    )


def _rank(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    values = [_preview(row) for row in rows if str(row.get("symbol") or "").strip()]
    values.sort(key=lambda row: (
        -(_number(row.get("entry_score")) or -1.0),
        -(_number(row.get("candidate_score")) or -1.0),
        str(row.get("symbol") or ""),
    ))
    for rank, row in enumerate(values, start=1):
        row["mode_rank"] = rank
    return values


def build_native_four_mode_report(
    *,
    first_board: list[dict[str, Any]],
    rebound_board: list[dict[str, Any]],
    candidate_pool: list[dict[str, Any]],
    opportunity_pool: list[dict[str, Any]],
    runtime: dict[str, Any],
    market_sentiment: dict[str, Any] | None,
    sector_strength: dict[str, Any] | None,
    as_of: datetime,
) -> dict[str, Any]:
    """Build a read-only report from existing short-line hunter projections."""
    all_candidates = list(candidate_pool)
    trend_rows = [row for row in all_candidates if _is_trend(row)]
    mode_rows = {
        "yje": list(opportunity_pool) or all_candidates,
        "rzq": [row for row in all_candidates if _has_mode(row, "rebound_board")],
        "qs": trend_rows,
        "sb": [row for row in all_candidates if _has_mode(row, "first_board")],
    }
    modes: list[dict[str, Any]] = []
    for definition in _MODE_DEFINITIONS:
        rows = mode_rows[definition["id"]]
        previews = _rank(rows)[:20]
        tradable_count = sum(
            1 for row in previews if row.get("tradability_state") == "tradable"
        )
        modes.append({
            **definition,
            "filters": list(definition["filters"]),
            "score_components": list(definition["score_components"]),
            "candidate_count": len(rows),
            "tradable_count": tradable_count,
            "candidates": previews,
        })

    history_ready = bool(runtime.get("history_ready"))
    scope = runtime.get("candidate_scope") or {}
    state = "live" if history_ready and scope.get("state") in {"live", "partial"} else "partial"
    return {
        "state": state,
        "reason": "四个模式均使用短线猎手现有系统逻辑；不读取、不执行聚宽源码，不使用聚宽数据或因子。",
        "logic_version": "native-four-mode-v1",
        "execution_state": "read_only",
        "source": {
            "provider": "tickflow_native",
            "label": "短线猎手原生数据与评分链路",
            "as_of": as_of.isoformat(),
            "data_paths": [
                "TickFlow 实时行情",
                "本地涨停历史与反包识别",
                "系统溢价基因",
                "板块强度与轮动",
                "技术面、分时强度与实时资金",
                "评分速度与距涨停成交空间",
            ],
        },
        "modes": modes,
        "runtime": {
            "trading_date": runtime.get("trading_date"),
            "history_ready": history_ready,
            "history_reason": runtime.get("history_reason"),
            "candidate_scope": dict(scope),
            "market_sentiment_state": (market_sentiment or {}).get("state") if market_sentiment else "unavailable",
            "sector_strength_state": "live" if sector_strength and sector_strength.get("rows") else "unavailable",
            "first_board_rows": len(first_board),
            "rebound_board_rows": len(rebound_board),
            "candidate_rows": len(candidate_pool),
            "opportunity_rows": len(opportunity_pool),
        },
    }
