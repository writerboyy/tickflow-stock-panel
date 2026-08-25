"""实时大单榜单 API。"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from typing import Literal

router = APIRouter(prefix="/api/large-orders", tags=["large-orders"])


class LargeOrderPreferencesIn(BaseModel):
    enabled: bool | None = None
    score_threshold: int | None = Field(default=None, ge=50, le=100)
    cooldown_seconds: int | None = Field(default=None, ge=30, le=3600)
    deep_dive_interval_seconds: int | None = Field(default=None, ge=15, le=600)
    max_deep_dive_symbols: int | None = Field(default=None, ge=0, le=10)
    candidate_limit: int | None = Field(default=None, ge=10, le=200)
    min_limit_up_gap_pct: float | None = Field(default=None, ge=0.0, le=0.10)
    market_segments: list[Literal["main", "star", "chinext", "bse", "st"]] | None = None
    exclude_bse: bool | None = None
    exclude_st: bool | None = None


def _service(request: Request):
    return getattr(request.app.state, "large_order_service", None)


def _validate_time_range(from_ms: int | None, to_ms: int | None) -> None:
    if from_ms is not None and to_ms is not None and from_ms > to_ms:
        raise HTTPException(status_code=422, detail="from_ms must not exceed to_ms")


@router.post("/preferences")
def save_preferences(body: LargeOrderPreferencesIn, request: Request) -> dict:
    updates = body.model_dump(exclude_none=True)
    service = _service(request)
    if service is not None:
        return {"large_orders": service.update_preferences(updates)}
    from app.services import preferences

    return {"large_orders": preferences.set_large_orders_preferences(updates)}


@router.get("/status")
def status(request: Request) -> dict:
    service = _service(request)
    if service is None:
        return {
            "enabled": False,
            "running": False,
            "data_source": "proxy_only",
            "mode": "stale",
            "stale": True,
            "coverage_count": 0,
            "candidate_count": 0,
            "precise_count": 0,
            "filtered_near_limit_count": 0,
            "unassessable_count": 0,
            "last_updated_ms": None,
            "last_calculation_ms": 0.0,
            "market_phase": None,
            "storage": {
                "enabled": False,
                "queued_rows": 0,
                "written_rows": 0,
                "dropped_rows": 0,
                "invalid_rows": 0,
                "last_flush_ms": None,
                "last_error": None,
                "storage_root": None,
            },
        }
    return service.status()


@router.get("/ranking")
def ranking(
    request: Request,
    window: int = Query(60, description="15/60/300 秒窗口"),
    scope: str = Query("all", pattern="^(all|watchlist)$"),
    mode: str = Query("combined", pattern="^(combined|execution|intent)$"),
) -> dict:
    service = _service(request)
    if service is None:
        return {
            "rows": [],
            "count": 0,
            "window": window,
            "scope": scope,
            "stale": True,
            "last_updated_ms": None,
        }
    return service.ranking(window, scope, mode)


@router.get("/dates")
def dates(request: Request, limit: int = Query(30, ge=1, le=250)) -> dict:
    service = _service(request)
    values = service.available_history_dates(limit) if service is not None else []
    return {"dates": values, "count": len(values)}


@router.get("/history")
def history(
    request: Request,
    trade_date: date = Query(..., alias="date", description="交易日 YYYY-MM-DD"),
    kind: str | None = Query(None, pattern="^(proxy_flow|kaipanla_trade|kaipanla_intent|orderbook_snapshot)$"),
    mode: str = Query("combined", pattern="^(combined|execution|intent)$"),
    symbol: str | None = Query(None, min_length=1),
    from_ms: int | None = Query(None, ge=0),
    to_ms: int | None = Query(None, ge=0),
    cursor: str | None = Query(None, min_length=1),
    limit: int = Query(1000, ge=1, le=10000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
) -> dict:
    _validate_time_range(from_ms, to_ms)
    service = _service(request)
    if service is None:
        return {
            "rows": [],
            "count": 0,
            "has_more": False,
            "truncated": False,
            "next_cursor": None,
            "kind": kind,
            "date": trade_date.isoformat(),
        }
    try:
        return service.history(
            trade_date,
            kind=kind,
            mode=mode,
            symbol=symbol,
            from_ms=from_ms,
            to_ms=to_ms,
            cursor=cursor,
            limit=limit,
            order=order,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/reconciliation")
def reconciliation(
    request: Request,
    trade_date: date = Query(..., alias="date", description="交易日 YYYY-MM-DD"),
    symbol: str | None = Query(None, min_length=1),
    from_ms: int | None = Query(None, ge=0),
    to_ms: int | None = Query(None, ge=0),
    limit: int = Query(1000, ge=1, le=2000),
    order: str = Query("desc", pattern="^(asc|desc)$"),
) -> dict:
    _validate_time_range(from_ms, to_ms)
    service = _service(request)
    if service is None:
        return {
            "rows": [],
            "count": 0,
            "truncated": False,
            "date": trade_date.isoformat(),
            "summary": {},
        }
    return service.reconciliation(
        trade_date,
        symbol=symbol,
        from_ms=from_ms,
        to_ms=to_ms,
        limit=limit,
        order=order,
    )


@router.get("/{symbol}/tape")
def tape(symbol: str, request: Request) -> dict:
    service = _service(request)
    if service is None:
        return {"symbol": symbol.upper(), "trades": [], "intents": [], "timeline": [], "source": "proxy_only"}
    return service.tape(symbol)


@router.get("/{symbol}/analysis")
def analysis(symbol: str, request: Request, limit: int = Query(120, ge=1, le=500)) -> dict:
    service = _service(request)
    if service is None:
        return {
            "symbol": symbol.upper(),
            "name": symbol.upper(),
            "ranking": None,
            "orderbook": None,
            "orderbook_history": [],
            "tape": {"symbol": symbol.upper(), "trades": [], "intents": [], "timeline": []},
            "evidence": {"proxy": False, "execution": False, "intent": False, "orderbook": False},
            "degraded_reason": "实时大单服务未启用",
        }
    return service.analysis(symbol, limit=limit)
