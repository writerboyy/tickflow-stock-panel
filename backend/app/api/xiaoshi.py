"""Explicit read-only Xiaoshi endpoints."""
from __future__ import annotations

from typing import Literal
from urllib.parse import quote as url_quote

from fastapi import APIRouter, HTTPException, Query

from app.services.xiaoshi import (
    XiaoshiError,
    XiaoshiProtectionError,
    get_resource_manager,
    get_status,
    XiaoshiClient,
)

router = APIRouter(prefix="/api/xiaoshi", tags=["xiaoshi"])
settings_router = APIRouter(prefix="/api/settings/xiaoshi", tags=["settings"])


@settings_router.get("")
def status() -> dict:
    return get_status()


@settings_router.post("/refresh")
def refresh_resources() -> dict:
    try:
        get_resource_manager().refresh()
    except XiaoshiError as exc:
        raise HTTPException(status_code=502, detail={"message": str(exc), **exc.context}) from exc
    return get_status()


@settings_router.post("/update-prompt")
def update_prompt() -> dict:
    try:
        get_resource_manager().refresh(explicit_update=True)
    except XiaoshiError as exc:
        raise HTTPException(status_code=502, detail={"message": str(exc), **exc.context}) from exc
    return get_status()


@router.get("/quote/{symbol}")
def quote(
    symbol: str,
    market: Literal["CN", "HK", "US"] = Query("CN"),
    instrument: Literal["stock", "index", "etf"] = Query("stock"),
) -> dict:
    client = XiaoshiClient()
    try:
        return client.request_json(
            f"/api/v3/market/quote/{url_quote(symbol, safe='')}",
            params={"market": market, "instrument": instrument},
        )
    except XiaoshiProtectionError as exc:
        raise HTTPException(
            status_code=429,
            detail={
                "message": str(exc),
                "error": exc.error_code,
                "retry_after_seconds": exc.retry_after_seconds,
                "alternative": exc.alternative,
                **exc.context,
            },
        ) from exc
    except XiaoshiError as exc:
        raise HTTPException(status_code=502, detail={"message": str(exc), **exc.context}) from exc
