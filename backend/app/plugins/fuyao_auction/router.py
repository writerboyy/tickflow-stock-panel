"""扶摇集合竞价状态和手动采集接口。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app import secrets_store
from app.plugins.fuyao.provider import API_KEY_ENV, SECRETS_FIELD, get_api_key, probe_api_key

router = APIRouter(prefix="/api/settings/fuyao-auction", tags=["fuyao-auction"])


class CollectRequest(BaseModel):
    checkpoint: str | None = None


class FuyaoApiKeyIn(BaseModel):
    api_key: str


def _collector(request: Request):
    collector = getattr(request.app.state, "fuyao_auction_collector", None)
    if collector is None:
        raise HTTPException(status_code=503, detail="扶摇集合竞价采集器未启动")
    return collector


def _status(request: Request) -> dict:
    result = _collector(request).status()
    result["api_key_masked"] = secrets_store.mask(get_api_key())
    return result


@router.get("/status")
def status(request: Request) -> dict:
    return _status(request)


@router.put("")
def save_api_key(body: FuyaoApiKeyIn, request: Request) -> dict:
    """先探测候选 Key，成功后写入本地凭据并让采集器刷新客户端。"""
    key = body.api_key.strip()
    if not key:
        return {"ok": False, "error": f"{API_KEY_ENV} 不能为空", **_status(request)}

    ok, message = probe_api_key(key)
    if not ok:
        return {"ok": False, "error": message, **_status(request)}

    secrets_store.save({SECRETS_FIELD: key})
    _collector(request).stop()
    return {"ok": True, **_status(request)}


@router.delete("")
def clear_api_key(request: Request) -> dict:
    """清除界面保存的 Key；若 .env 仍有同名变量，环境 Key 继续生效。"""
    secrets_store.clear(SECRETS_FIELD)
    _collector(request).stop()
    return {"ok": True, **_status(request)}


@router.post("/collect")
async def collect(request: Request, body: CollectRequest | None = None) -> dict:
    collector = _collector(request)
    checkpoint = body.checkpoint if body else None
    try:
        rows = await collector.collect(checkpoint)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "rows": rows, "status": collector.status()}
