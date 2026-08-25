"""PIT reference data maintenance API."""
from __future__ import annotations

import asyncio
from datetime import date

from fastapi import APIRouter, Query, Request

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
    return await asyncio.to_thread(pit_reference.sync_pit_reference, data_dir)


@router.post("/sync-baostock-lifecycle")
async def sync_baostock_lifecycle(
    request: Request,
    years: int = Query(5, ge=1, le=30),
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    data_dir = _data_dir(request)
    return await asyncio.to_thread(
        pit_reference.sync_baostock_lifecycle,
        data_dir,
        years=years,
        start_date=start_date,
        end_date=end_date,
    )
