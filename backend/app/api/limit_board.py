"""Limit-board workspace API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.services.limit_board_store import RevisionConflict


router = APIRouter(prefix="/api/limit-board", tags=["limit-board"])


class SelectedWrite(BaseModel):
    revision: int = Field(ge=0)
    symbol: str = Field(min_length=1, max_length=20)


class PoolWrite(SelectedWrite):
    source: str = Field(default="manual", pattern="^(first_board|rebound_board|selected|manual)$")
    allocation_mode: str = Field(default="global", pattern="^(global|lot|fixed|volume)$")
    allocation_value: float | None = Field(default=None, gt=0)


class BuyPoolWrite(SelectedWrite):
    source: str = Field(default="manual", pattern="^(first_board|rebound_board|selected|manual)$")
    allocation_mode: str = Field(default="lot", pattern="^(lot|fixed|volume)$")
    allocation_value: float | None = Field(default=None, gt=0)


class PoolUpdate(BaseModel):
    revision: int = Field(ge=0)
    auto_trade: bool
    order_mode: str = Field(default="sweep", pattern="^(sweep|queue)$")
    allocation_mode: str | None = Field(default=None, pattern="^(global|lot|fixed|volume)$")
    allocation_value: float | None = Field(default=None, gt=0)


class AdvancedSettings(BaseModel):
    sweep_price_levels: int = Field(ge=1, le=10)
    queue_wait_seconds: int = Field(default=0, ge=0, le=300)
    queue_confirm_snapshots: int = Field(default=0, ge=0, le=10)
    order_allocation_mode: str = Field(default="fixed", pattern="^(quarter|third|half|fixed)$")
    order_amount_per_board: float = Field(default=0, ge=0, le=10_000_000)
    max_auto_board_count: int = Field(default=0, ge=0, le=100)
    max_market_broken_rate_pct: float = Field(default=40.0, ge=0, le=100)
    main_board_only: bool = False
    near_limit_pct: float = Field(ge=0.001, le=0.10)
    exit_limit_pct: float = Field(ge=0.001, le=0.20)
    exit_sustain_seconds: int = Field(ge=1, le=300)
    first_board_lookback_days: int = Field(ge=1, le=60)
    blacklist_after_breaks: int = Field(ge=0, le=20)


class AdvancedSettingsWrite(BaseModel):
    revision: int = Field(ge=0)
    settings: AdvancedSettings


class QuoteSnapshotRequest(BaseModel):
    symbols: list[str] = Field(min_length=1, max_length=30)


def _service(request: Request):
    service = getattr(request.app.state, "limit_board_service", None)
    if service is None:
        raise HTTPException(503, "打板专区服务尚未初始化")
    return service


@router.get("")
def view(request: Request):
    return _service(request).view()


@router.get("/sector-strength")
def sector_strength(request: Request, captured_at: str | None = None):
    try:
        return _service(request).sector_strength_view(captured_at)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/quotes")
def quote_snapshot(payload: QuoteSnapshotRequest, request: Request):
    return _service(request).quote_snapshot(payload.symbols)


@router.get("/sector-strength/{plate_id}/constituents")
async def sector_constituents(
    plate_id: str,
    request: Request,
    captured_at: str | None = None,
):
    try:
        return await _service(request).sector_constituents_view(plate_id, captured_at)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(502, str(exc)) from exc


@router.put("/settings/advanced")
def update_advanced_settings(payload: AdvancedSettingsWrite, request: Request):
    try:
        config = _service(request).update_advanced_settings(
            payload.settings.model_dump(), payload.revision,
        )
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "config": config}


@router.post("/selected")
def add_selected(payload: SelectedWrite, request: Request):
    try:
        config = _service(request).add_selected(payload.symbol, payload.revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "config": config}


@router.post("/candidate")
def add_candidate(payload: SelectedWrite, request: Request):
    try:
        config = _service(request).add_candidate(payload.symbol, payload.revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "config": config}


@router.delete("/selected/{symbol}")
def remove_selected(symbol: str, revision: int, request: Request):
    try:
        config = _service(request).remove_selected(symbol, revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True, "config": config}


@router.delete("/candidate/{symbol}")
def remove_candidate(symbol: str, revision: int, request: Request):
    try:
        config = _service(request).remove_candidate(symbol, revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True, "config": config}


@router.post("/pool")
def add_pool(payload: PoolWrite, request: Request):
    try:
        config = _service(request).add_pool(
            payload.symbol,
            payload.source,
            payload.revision,
            payload.allocation_mode,
            payload.allocation_value,
        )
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "config": config}


@router.put("/pool/{symbol}")
def update_pool(symbol: str, payload: PoolUpdate, request: Request):
    try:
        config = _service(request).update_pool(
            symbol,
            payload.auto_trade,
            payload.order_mode,
            payload.revision,
            payload.allocation_mode,
            payload.allocation_value,
        )
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "config": config}


@router.delete("/pool/{symbol}")
def remove_pool(symbol: str, revision: int, request: Request):
    try:
        config = _service(request).remove_pool(symbol, revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True, "config": config}


@router.post("/buy-pool")
def add_buy_pool(payload: BuyPoolWrite, request: Request):
    try:
        result = _service(request).add_buy_pool(
            payload.symbol,
            payload.source,
            payload.revision,
            payload.allocation_mode,
            payload.allocation_value,
        )
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, **result}


@router.delete("/buy-pool/{symbol}")
def remove_buy_pool(symbol: str, revision: int, request: Request):
    try:
        config = _service(request).remove_buy_pool(symbol, revision)
    except RevisionConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    return {"ok": True, "config": config}
