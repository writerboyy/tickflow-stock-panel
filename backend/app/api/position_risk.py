"""持仓风控 API。"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from pydantic import BaseModel, Field

from app.services import alert_store
from app.services.position_risk_ocr import import_position_image
from app.services.position_risk_store import RevisionConflict, default_rule_options
from app.services.watchlist_ocr.runtime import OCR_LIMITER

router = APIRouter(prefix="/api/position-risk", tags=["position-risk"])

_MAX_IMAGE_BYTES = 12 * 1024 * 1024
_IMAGE_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp", "image/bmp", "image/gif"}


class PortfolioPayload(BaseModel):
    revision: int
    account: dict[str, Any] = Field(default_factory=dict)
    positions: list[dict[str, Any]] = Field(default_factory=list)


class OverridePayload(BaseModel):
    revision: int
    override: dict[str, Any]


class QmtOrderPayload(BaseModel):
    action: str
    symbol: str
    volume: int | None = Field(default=None, ge=100)
    price: float | None = None
    price_type: str = "LIMIT"
    reference_price: float | None = Field(default=None, gt=0)
    allocation_mode: str | None = None
    allocation_value: float | None = Field(default=None, gt=0)
    idempotency_key: str = Field(min_length=8, max_length=120)


class QmtOrderPreviewPayload(BaseModel):
    action: str
    symbol: str
    price: float | None = None
    price_type: str = "LIMIT"
    reference_price: float | None = Field(default=None, gt=0)
    allocation_mode: str = "quarter"
    allocation_value: float | None = Field(default=None, gt=0)


class QmtTradeTogglePayload(BaseModel):
    enabled: bool


class QmtRiskActionPayload(BaseModel):
    fingerprint: str = Field(min_length=32, max_length=128)
    symbol: str
    action: str
    volume: int = Field(gt=0)


def _service(request: Request):
    service = getattr(request.app.state, "position_risk_service", None)
    if service is None:
        raise HTTPException(503, "持仓风控服务尚未初始化")
    return service


def _qmt(request: Request):
    service = getattr(request.app.state, "qmt_trading_service", None)
    if service is None:
        raise HTTPException(503, "QMT交易网关尚未初始化")
    return service


def _map_error(exc: Exception) -> HTTPException:
    if isinstance(exc, RevisionConflict):
        return HTTPException(409, str(exc))
    return HTTPException(400, str(exc))


@router.post("/import-image")
async def import_image(request: Request, file: UploadFile = File(...)):
    """识别一张同花顺手机持仓截图，不持久化原图或 OCR 全文。"""
    content_type = (file.content_type or "").split(";")[0].strip().lower()
    filename = (file.filename or "").lower()
    valid_extension = filename.endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"))
    if content_type not in _IMAGE_TYPES and not valid_extension:
        raise HTTPException(400, "仅支持 JPG / PNG / WebP / BMP / GIF 图片")
    data = await file.read()
    if not data:
        raise HTTPException(400, "空文件")
    if len(data) > _MAX_IMAGE_BYTES:
        raise HTTPException(400, "图片过大（上限 12MB）")
    data_dir: Path = request.app.state.repo.store.data_dir
    try:
        return await anyio.to_thread.run_sync(
            lambda: import_position_image(data, data_dir),
            limiter=OCR_LIMITER,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, "持仓截图识别失败") from exc


@router.post("/portfolio/preview")
def preview_portfolio(payload: PortfolioPayload, request: Request):
    return _service(request).preview(payload.model_dump())


@router.get("/portfolio")
def get_portfolio(request: Request):
    return _service(request).view()


@router.get("/features")
def get_features(request: Request, symbols: str | None = Query(None)):
    service = _service(request)
    selected = {
        value.strip().upper()
        for value in (symbols or "").split(",")
        if value.strip()
    } or None
    features = service.feature_snapshot(selected)
    return {"features": features, "count": len(features)}


@router.put("/portfolio")
def replace_portfolio(payload: PortfolioPayload, request: Request):
    service = _service(request)
    try:
        saved = service.replace_portfolio(payload.model_dump(), payload.revision)
    except (RevisionConflict, ValueError) as exc:
        raise _map_error(exc) from exc
    return {"ok": True, "portfolio": saved, "message": "持仓快照已替换；风险高水位已重置"}


@router.put("/overrides/{symbol}")
def update_override(symbol: str, payload: OverridePayload, request: Request):
    service = _service(request)
    cleaned = symbol.strip().upper()

    def update(value: dict[str, Any]) -> None:
        if cleaned not in {item["symbol"] for item in value["positions"]}:
            raise ValueError("只能覆盖当前持仓标的")
        value.setdefault("overrides", {})[cleaned] = payload.override

    try:
        saved = service.store.update(payload.revision, update)
    except (RevisionConflict, ValueError) as exc:
        raise _map_error(exc) from exc
    service._notify_updated()  # noqa: SLF001
    return {"ok": True, "portfolio": saved}


@router.delete("/overrides/{symbol}")
def delete_override(symbol: str, revision: int, request: Request):
    service = _service(request)
    try:
        saved = service.store.update(
            revision,
            lambda value: value.setdefault("overrides", {}).pop(symbol.strip().upper(), None),
        )
    except RevisionConflict as exc:
        raise _map_error(exc) from exc
    service._notify_updated()  # noqa: SLF001
    return {"ok": True, "portfolio": saved}


def _collapse_timeline_events(rows: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for index, item in enumerate(rows):
        timestamp = int(item.get("ts") or 0)
        fingerprint = str(item.get("fingerprint") or "")
        key = f"{item.get('source')}:{fingerprint}" if fingerprint else f"row:{index}"
        current = grouped.get(key)
        if current is None:
            grouped[key] = {
                **item,
                "occurrence_count": 1,
                "first_ts": timestamp,
                "last_ts": timestamp,
            }
            continue
        occurrence_count = int(current["occurrence_count"]) + 1
        first_ts = min(int(current["first_ts"]), timestamp)
        last_ts = max(int(current["last_ts"]), timestamp)
        latest = item if timestamp > int(current["last_ts"]) else current
        grouped[key] = {
            **latest,
            "occurrence_count": occurrence_count,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "ts": last_ts,
        }
    return sorted(grouped.values(), key=lambda item: int(item.get("last_ts") or 0), reverse=True)


@router.get("/events")
def list_events(request: Request, days: int = Query(7, ge=1, le=30), limit: int = Query(500, ge=1, le=5000)):
    service = _service(request)
    positions = {item["symbol"] for item in service.store.load()["positions"]}
    rows = alert_store.list_recent(service.store.root.parents[1], days=days, limit=5000)
    removed_fields = {
        "risk_score", "risk_level", "suggestion_pct", "reasons", "source_ids",
        "signals", "conditions", "logic", "evidence", "evidence_coverage",
    }
    cleaned_rows = []
    for item in rows:
        if item.get("source") == "position_risk":
            cleaned = {key: value for key, value in item.items() if key not in removed_fields}
            if "suggestion_pct" in item:
                cleaned["action_pct"] = item["suggestion_pct"]
        else:
            cleaned = dict(item)
        cleaned["message"] = service.localize_text(str(item.get("message") or ""))
        cleaned["rule_name"] = service.localize_text(str(item.get("rule_name") or ""))
        cleaned["timeline_origin"] = "position_risk" if item.get("source") == "position_risk" else "monitor_rule"
        cleaned_rows.append(cleaned)
    rows = [
        item
        for item in cleaned_rows
        if (
            item.get("source") == "position_risk"
            and item.get("rule_id") not in {
                "large_buy", "large_sell", "continuous_outflow", "orderbook_imbalance",
            }
        ) or (item.get("source") != "position_risk" and item.get("symbol") in positions)
    ]
    rows = _collapse_timeline_events(rows)[:limit]
    return {"events": rows, "count": len(rows)}


@router.get("/options")
def get_options(request: Request):
    from app.indicators.pipeline import ENRICHED_COLUMNS
    from app.services.kline_sync import intraday_monitor_support
    from app.strategy import custom_signals, monitor_rules
    from app.strategy.intraday_signals import INTRADAY_SIGNAL_LABELS
    from app.tickflow.capabilities import Cap

    service = _service(request)
    builtin = []
    for signal_id, label in ENRICHED_COLUMNS.items():
        if not signal_id.startswith("signal_"):
            continue
        builtin.append({
            "id": signal_id,
            "label": label,
            "direction": service._signal_direction(signal_id),  # noqa: SLF001
            "enabled": True,
            "group": "builtin",
        })
    builtin.extend({
        "id": signal_id,
        "label": label,
        "direction": service._signal_direction(signal_id),  # noqa: SLF001
        "enabled": True,
        "group": "intraday",
    } for signal_id, label in INTRADAY_SIGNAL_LABELS.items())
    try:
        custom = [{
            "id": f"csg_{item['id']}",
            "label": item.get("name", item["id"]),
            "direction": item.get("kind", "both"),
            "enabled": item.get("enabled", True),
            "available": item.get("enabled", True),
            "group": "custom",
        } for item in custom_signals.load_all(service.store.root.parents[1]) if item.get("enabled", True)]
    except Exception:  # noqa: BLE001
        custom = []
    portfolio = service.store.load()
    configured_custom: dict[str, dict[str, Any]] = {}
    for override in (portfolio.get("overrides") or {}).values():
        for signal_id, config in ((override.get("signals") or {}).get("custom") or {}).items():
            configured_custom.setdefault(signal_id, config)
    live_custom_ids = {item["id"] for item in custom}
    custom.extend({
        "id": signal_id,
        "label": str(config.get("label") or signal_id),
        "direction": str(config.get("direction") or "both"),
        "enabled": bool(config.get("enabled", True)),
        "available": False,
        "group": "custom",
    } for signal_id, config in configured_custom.items() if signal_id not in live_custom_ids)
    rules = monitor_rules.load_all(service.store.root.parents[1])
    capset = getattr(request.app.state, "capabilities", None)
    websocket_limits = capset.limits(Cap.WEBSOCKET) if capset and capset.has(Cap.WEBSOCKET) else None
    return {
        "rules": default_rule_options()["rules"],
        "builtin_signals": builtin,
        "custom_signals": custom,
        "monitor_rules": [{
            "id": item.get("id"),
            "name": item.get("name"),
            "enabled": item.get("enabled", True),
            "conditions": item.get("conditions", []),
            "severity": item.get("severity", "info"),
            "default_action_pct": 0,
        } for item in rules],
        "capabilities": {
            "websocket": bool(capset and capset.has(Cap.WEBSOCKET)),
            "websocket_capacity": int(websocket_limits.subscribe or 200) if websocket_limits else 200,
            "depth": bool(capset and (capset.has(Cap.DEPTH5) or capset.has(Cap.DEPTH5_BATCH))),
            "intraday": intraday_monitor_support(capset),
        },
    }


@router.get("/qmt/status")
def qmt_status(request: Request):
    return _qmt(request).status()


@router.post("/qmt/probe")
def qmt_probe(request: Request):
    try:
        return _qmt(request).probe()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc


@router.post("/qmt/sync")
def qmt_sync(request: Request):
    qmt = _qmt(request)
    try:
        result = qmt.sync_into(_service(request))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, **result, "message": "QMT权威持仓已同步"}


@router.post("/qmt/trading-toggle")
def qmt_trading_toggle(payload: QmtTradeTogglePayload, request: Request):
    try:
        status = _qmt(request).set_trade_enabled(payload.enabled)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "status": status}


@router.get("/qmt/orders")
def qmt_orders(request: Request):
    try:
        return {"orders": _qmt(request).list_orders()}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc


@router.post("/qmt/orders/preview")
def qmt_preview_order(payload: QmtOrderPreviewPayload, request: Request):
    try:
        preview = _qmt(request).preview_order(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "preview": preview}


@router.post("/qmt/orders")
def qmt_submit_order(payload: QmtOrderPayload, request: Request):
    try:
        result = _qmt(request).submit_order(payload.model_dump())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "order": result}


@router.post("/qmt/orders/confirm-action")
def qmt_confirm_risk_action(payload: QmtRiskActionPayload, request: Request):
    try:
        order_request = _service(request).confirmed_action_order(
            payload.fingerprint,
            payload.symbol,
            payload.action,
            payload.volume,
        )
        result = _qmt(request).submit_order(order_request)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "order": result}


@router.post("/qmt/orders/cancel")
def qmt_cancel_order(payload: dict[str, Any], request: Request):
    try:
        result = _qmt(request).cancel_order(payload)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(503, str(exc)) from exc
    return {"ok": True, "order": result}
