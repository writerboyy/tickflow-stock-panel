"""实时大单榜单 API。"""

from __future__ import annotations

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


@router.get("/{symbol}/tape")
def tape(symbol: str, request: Request) -> dict:
    service = _service(request)
    if service is None:
        return {"symbol": symbol.upper(), "trades": [], "intents": [], "timeline": [], "source": "proxy_only"}
    return service.tape(symbol)
