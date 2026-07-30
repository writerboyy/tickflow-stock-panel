"""设置页使用的开盘啦连接 API。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.plugins.kaipanla.credentials import (
    clear_credentials,
    credential_status,
    save_authorized_url,
)
from app.plugins.kaipanla.storage import TABLE_IDS

router = APIRouter(prefix="/api/settings/kaipanla", tags=["settings"])


class KaipanlaConnectionIn(BaseModel):
    source_url: str


def _status() -> dict:
    return {**credential_status(), "tables": list(TABLE_IDS), "automatic": True}


@router.get("")
def get_connection_status() -> dict:
    return _status()


@router.put("")
async def save_connection(body: KaipanlaConnectionIn, request: Request) -> dict:
    try:
        save_authorized_url(body.source_url)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    collector = getattr(request.app.state, "kaipanla_collector", None)
    if collector is not None:
        collector.trigger_catch_up()
    return _status()


@router.delete("")
def delete_connection(request: Request) -> dict:
    clear_credentials()
    collector = getattr(request.app.state, "kaipanla_collector", None)
    if collector is not None:
        collector.stop()
    return _status()
