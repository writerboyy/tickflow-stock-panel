"""Limit-board workspace API."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.services.limit_board_store import RevisionConflict


router = APIRouter(prefix="/api/limit-board", tags=["limit-board"])


class SelectedWrite(BaseModel):
    revision: int = Field(ge=0)
    symbol: str = Field(min_length=1, max_length=20)


def _service(request: Request):
    service = getattr(request.app.state, "limit_board_service", None)
    if service is None:
        raise HTTPException(503, "打板专区服务尚未初始化")
    return service


@router.get("")
def view(request: Request):
    return _service(request).view()


@router.post("/selected")
def add_selected(payload: SelectedWrite, request: Request):
    try:
        config = _service(request).add_selected(payload.symbol, payload.revision)
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
