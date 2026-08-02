"""PIT reference data maintenance API."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request

from app.services import pit_reference

router = APIRouter(prefix="/api/pit-reference", tags=["pit-reference"])


def _data_dir(request: Request):
    repo = request.app.state.repo
    return repo.store.data_dir


@router.get("/status")
def status(request: Request) -> dict:
    return pit_reference.get_status(_data_dir(request))


@router.post("/sync-snapshots")
async def sync_snapshots(request: Request) -> dict:
    data_dir = _data_dir(request)
    return await asyncio.to_thread(pit_reference.sync_hithink_snapshots, data_dir)
