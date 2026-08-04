"""实时大单榜单 API。"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request

router = APIRouter(prefix="/api/large-orders", tags=["large-orders"])


def _service(request: Request):
    return getattr(request.app.state, "large_order_service", None)


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
            "last_updated_ms": None,
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
) -> dict:
    service = _service(request)
    if service is None:
        return {"rows": [], "count": 0, "window": window, "scope": scope, "stale": True}
    return service.ranking(window, scope)


@router.get("/history")
def history(
    request: Request,
    trade_date: date = Query(..., alias="date", description="交易日 YYYY-MM-DD"),
    kind: str = Query("proxy_flow", pattern="^(proxy_flow|kaipanla_trade|kaipanla_intent)$"),
    symbol: str | None = Query(None, min_length=1),
    from_ms: int | None = Query(None, ge=0),
    to_ms: int | None = Query(None, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
) -> dict:
    service = _service(request)
    if service is None:
        return {"rows": [], "count": 0, "truncated": False, "kind": kind, "date": trade_date.isoformat()}
    return service.history(
        trade_date,
        kind=kind,
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
