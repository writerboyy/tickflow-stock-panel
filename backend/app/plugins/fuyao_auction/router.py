"""扶摇集合竞价状态和手动采集接口。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/api/settings/fuyao-auction", tags=["fuyao-auction"])


class CollectRequest(BaseModel):
    checkpoint: str | None = None


def _collector(request: Request):
    collector = getattr(request.app.state, "fuyao_auction_collector", None)
    if collector is None:
        raise HTTPException(status_code=503, detail="扶摇集合竞价采集器未启动")
    return collector


@router.get("/status")
def status(request: Request) -> dict:
    return _collector(request).status()


@router.post("/collect")
async def collect(request: Request, body: CollectRequest | None = None) -> dict:
    collector = _collector(request)
    checkpoint = body.checkpoint if body else None
    try:
        rows = await collector.collect(checkpoint)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "rows": rows, "status": collector.status()}
