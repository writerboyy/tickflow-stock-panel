"""扶摇集合竞价批量采集与调度。"""

from __future__ import annotations

import asyncio
import logging
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from apscheduler.triggers.cron import CronTrigger

from app.market_time import cn_now, cn_today
from app.plugins.fuyao.client import FuyaoClient, FuyaoError
from app.plugins.fuyao.provider import get_api_key
from app.plugins.fuyao_auction.storage import (
    TABLE_ID,
    ensure_config,
    partition_path,
    publish,
    read_status,
)
from app.services.ingestion_manifest import load_ingestion_manifest, update_ingestion_manifest

logger = logging.getLogger(__name__)

_CHECKPOINTS = {
    "0915": ("live", 9, 15),
    "0920": ("live", 9, 20),
    "0925": ("final", 9, 25),
    "1457": ("live", 14, 57),
    "1500": ("final", 15, 0),
}
# 扶摇接口约束: 单次 thscodes 不得超过 100 个。
_BATCH_SIZE = 100


def _symbols(data_dir: Path) -> list[str]:
    path = Path(data_dir) / "instruments" / "instruments.parquet"
    if not path.exists():
        return []
    try:
        frame = pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("读取集合竞价标的列表失败: %s", exc)
        return []
    # 维表同时保留历史退市标的；扶摇集合竞价只接受当前在市股票。
    if "status" in frame.columns:
        frame = frame.filter(pl.col("status").cast(pl.String).str.to_lowercase() == "active")
    if "asset_type" in frame.columns:
        frame = frame.filter(pl.col("asset_type").cast(pl.String).str.to_lowercase() == "stock")
    if "symbol" not in frame.columns:
        return []
    return sorted({str(value).strip().upper() for value in frame["symbol"].drop_nulls().to_list() if "." in str(value)})


def _status_from_error(exc: FuyaoError) -> str:
    code = str(getattr(exc, "code", ""))
    if code == "2001":
        return "unauthenticated"
    if code == "2003":
        return "forbidden"
    if code == "3002":
        return "not_ready"
    if code == "4001":
        return "rate_limited"
    if code in {"5002", "5003"}:
        return "upstream_error"
    return "error"


class FuyaoAuctionCollector:
    """使用扶摇批量接口冻结竞价时点，不改写主行情表。"""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self._client: FuyaoClient | None = None
        self._lock = asyncio.Lock()
        self._status: dict[str, Any] = {
            "state": "unconfigured",
            "checkpoint": None,
            "stage": None,
            "rows": 0,
            "symbols": 0,
            "message": "未配置 FUYAO_API_KEY",
            "error_code": None,
            "collected_at": None,
        }

    @property
    def configured(self) -> bool:
        return bool(get_api_key())

    def _get_client(self) -> FuyaoClient:
        if self._client is None:
            self._client = FuyaoClient(api_key=get_api_key())
        return self._client

    def start(self, scheduler, *, bootstrap: bool = True) -> None:
        ensure_config(self.data_dir)
        if scheduler is None:
            return
        for checkpoint, (_stage, hour, minute) in _CHECKPOINTS.items():
            scheduler.add_job(
                self._scheduled_collect,
                args=[checkpoint],
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=hour,
                    minute=minute,
                    second=5,
                    timezone="Asia/Shanghai",
                ),
                id=f"fuyao_auction_{checkpoint}",
                misfire_grace_time=120,
                replace_existing=True,
            )
        if bootstrap:
            now = cn_now().time()
            due = [key for key, (_stage, hour, minute) in _CHECKPOINTS.items() if (hour, minute) <= (now.hour, now.minute)]
            if due:
                asyncio.create_task(self._scheduled_collect(due[-1]), name="fuyao-auction-bootstrap")
        logger.info("扶摇集合竞价采集器已启动")

    def stop(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # noqa: BLE001
                pass
            self._client = None

    async def _scheduled_collect(self, checkpoint: str) -> int:
        return await self.collect(checkpoint)

    async def _fetch_batch(
        self, batch: list[str], stage: str,
    ) -> tuple[list[dict], list[str], int | None, str | None]:
        """拉取一批标的；未知标的会递归拆分，避免整批被扶摇拒绝。"""
        try:
            data = await asyncio.to_thread(self._get_client().auction_snapshot, batch, stage)
        except FuyaoError as exc:
            if str(getattr(exc, "code", "")) != "1002":
                raise
            if len(batch) == 1:
                logger.warning("扶摇集合竞价跳过未知标的: %s", batch[0])
                return [], [], None, None
            middle = len(batch) // 2
            left = await self._fetch_batch(batch[:middle], stage)
            right = await self._fetch_batch(batch[middle:], stage)
            return (
                [*left[0], *right[0]],
                [*left[1], *right[1]],
                left[2] or right[2],
                left[3] or right[3],
            )

        data = data if isinstance(data, dict) else {}
        rows = data.get("item") if isinstance(data.get("item"), list) else data.get("data")
        if not isinstance(rows, list):
            rows = []
        status = str(data.get("data_status") or "ready")
        return (
            [row for row in rows if isinstance(row, dict)],
            [status],
            _int_or_none(data.get("timestamp")),
            _text_or_none(data.get("auction_phase")),
        )

    async def collect(self, checkpoint: str | None = None, trade_date: date | None = None) -> int:
        checkpoint = checkpoint or self.default_checkpoint()
        if checkpoint not in _CHECKPOINTS:
            raise ValueError(f"不支持的集合竞价采集时点: {checkpoint}")
        stage = _CHECKPOINTS[checkpoint][0]
        day = trade_date or cn_today()
        async with self._lock:
            if not self.configured:
                self._status = {
                    **self._status,
                    "state": "unconfigured",
                    "checkpoint": checkpoint,
                    "stage": stage,
                    "message": "未配置 FUYAO_API_KEY",
                    "error_code": None,
                }
                return 0
            symbols = _symbols(self.data_dir)
            if not symbols:
                return self._fail(day, checkpoint, stage, "instruments 维表为空", None, "missing_instruments")
            started = cn_now().isoformat(timespec="seconds")
            self._status = {
                "state": "running", "checkpoint": checkpoint, "stage": stage,
                "rows": 0, "symbols": len(symbols), "message": "批量拉取中",
                "error_code": None, "collected_at": started,
            }
            all_rows: list[dict] = []
            statuses: list[str] = []
            payload_items: list[dict] = []
            server_timestamp: int | None = None
            auction_phase: str | None = None
            try:
                for start in range(0, len(symbols), _BATCH_SIZE):
                    batch = symbols[start : start + _BATCH_SIZE]
                    rows, batch_statuses, batch_timestamp, batch_phase = await self._fetch_batch(batch, stage)
                    all_rows.extend(rows)
                    payload_items.extend(rows)
                    statuses.extend(batch_statuses)
                    server_timestamp = server_timestamp or batch_timestamp
                    auction_phase = auction_phase or batch_phase
                    if "not_ready" in batch_statuses:
                        break
            except FuyaoError as exc:
                return self._fail(day, checkpoint, stage, str(exc), getattr(exc, "code", None), _status_from_error(exc))
            except Exception as exc:  # noqa: BLE001
                logger.warning("扶摇集合竞价 %s 失败: %s", checkpoint, exc)
                return self._fail(day, checkpoint, stage, str(exc), None, "error")

            data_status = "not_ready" if "not_ready" in statuses else (statuses[0] if statuses else "empty")
            payload = {
                "timestamp": server_timestamp,
                "auction_phase": auction_phase,
                "data_status": data_status,
                "item": payload_items,
                "collected_at": started,
            }
            if not all_rows:
                state = "not_ready" if data_status == "not_ready" else "empty"
                update_ingestion_manifest(
                    self.data_dir, "fuyao", TABLE_ID, day.isoformat(),
                    status="not_ready" if state == "not_ready" else "valid_empty",
                    checkpoint=checkpoint, stage=stage, data_status=data_status,
                    server_timestamp=server_timestamp, empty_reason=state,
                )
                self._status = {
                    "state": state, "checkpoint": checkpoint, "stage": stage,
                    "rows": 0, "symbols": len(symbols), "message": "接口暂无可用竞价数据",
                    "error_code": None, "collected_at": started,
                }
                return 0
            rows_written = publish(
                self.data_dir, day, all_rows, checkpoint=checkpoint,
                stage=stage, payload=payload,
            )
            self._status = {
                "state": "completed", "checkpoint": checkpoint, "stage": stage,
                "rows": rows_written, "symbols": len({str(r.get("thscode")) for r in all_rows if r.get("thscode")}),
                "message": "采集完成", "error_code": None, "collected_at": started,
                "server_timestamp": server_timestamp, "auction_phase": auction_phase,
            }
            return rows_written

    def _fail(self, day: date, checkpoint: str, stage: str, message: str, code: Any, state: str) -> int:
        update_ingestion_manifest(
            self.data_dir, "fuyao", TABLE_ID, day.isoformat(),
            status=state, checkpoint=checkpoint, stage=stage, error_code=code, error_message=message,
        )
        self._status = {
            "state": state, "checkpoint": checkpoint, "stage": stage,
            "rows": 0, "symbols": 0, "message": message, "error_code": code,
            "collected_at": cn_now().isoformat(timespec="seconds"),
        }
        return 0

    def default_checkpoint(self) -> str:
        now = cn_now().time()
        due = [key for key, (_stage, hour, minute) in _CHECKPOINTS.items() if (hour, minute) <= (now.hour, now.minute)]
        return due[-1] if due else "0915"

    def status(self) -> dict:
        day = cn_today()
        result = dict(self._status)
        collected_at = result.get("collected_at")
        if (
            isinstance(collected_at, str)
            and collected_at[:10]
            and collected_at[:10] != day.isoformat()
        ):
            result.update({
                "state": "not_ready" if self.configured else "unconfigured",
                "checkpoint": None,
                "stage": None,
                "rows": 0,
                "symbols": 0,
                "message": "今日暂无竞价数据" if self.configured else "未配置 FUYAO_API_KEY",
                "error_code": None,
                "collected_at": None,
            })
        if not self.configured and result.get("state") != "running":
            result.update({"state": "unconfigured", "message": "未配置 FUYAO_API_KEY"})
        result.update({"configured": self.configured, "trade_date": day.isoformat()})
        result.update(read_status(self.data_dir, day))
        manifest = load_ingestion_manifest(self.data_dir, "fuyao", TABLE_ID, day.isoformat())
        if manifest and result.get("state") not in {"running", "completed"}:
            manifest_state = str(manifest.get("status") or result.get("state"))
            if manifest_state == "published":
                manifest_state = "completed"
            result.update({
                "state": manifest_state,
                "checkpoint": manifest.get("checkpoint", result.get("checkpoint")),
                "stage": manifest.get("stage", result.get("stage")),
                "error_code": manifest.get("error_code", result.get("error_code")),
                "message": manifest.get("error_message", result.get("message")),
                "server_timestamp": manifest.get("server_timestamp"),
                "auction_phase": manifest.get("auction_phase"),
            })
        result["table_id"] = TABLE_ID
        result["partition_exists"] = partition_path(self.data_dir, day).exists()
        return result


def _int_or_none(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _text_or_none(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
