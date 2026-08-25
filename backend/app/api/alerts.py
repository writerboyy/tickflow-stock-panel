"""监控规则触发记录 API。"""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

from app.services import alert_store

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


def _data_dir(request: Request) -> Path:
    return request.app.state.repo.store.data_dir


@router.get("")
def list_alerts(
    request: Request,
    days: int = 7,
    limit: int = 5000,
    source: str | None = None,
    type: str | None = None,
    ext_columns: str | None = None,
):
    """查询触发记录 (时间倒序)。

    ext_columns: 逗号分隔的 "configId.fieldName", 传入后按 symbol 富化行业/概念等 ext 字段,
    每条记录附带 {configId}__{fieldName} 键 (与 watchlist/screener 一致)。
    """
    events = alert_store.list_monitor_events(
        _data_dir(request), days=days, limit=limit, source=source, type=type,
    )
    if ext_columns and events:
        try:
            from app.api.screener import _load_ext_value_maps, _rows_with_ext
            repo = request.app.state.repo
            value_maps = _load_ext_value_maps(repo, ext_columns)
            if value_maps:
                events = _rows_with_ext(events, value_maps)
        except Exception:  # noqa: BLE001
            pass
    total = alert_store.count_monitor(_data_dir(request))
    return {"alerts": events, "total": total}


@router.delete("")
def clear_alerts(request: Request):
    """清空监控规则触发记录，保留其它领域服务的独立事件。"""
    n = alert_store.clear_monitor(_data_dir(request))
    return {"ok": True, "cleared": n}


@router.delete("/{ts}")
def delete_alert(ts: int, request: Request):
    """删除单条触发记录 (按 ts 毫秒时间戳)。"""
    deleted = alert_store.delete_monitor_one(_data_dir(request), ts)
    if not deleted:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"ok": True}
