"""EasyTDX 行业快照采集与调度。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger

from app.market_time import cn_now
from app.plugins.easy_tdx.bridge import availability
from app.plugins.easy_tdx.client import fetch_industry_rows
from app.plugins.easy_tdx.storage import (
    ensure_config,
    replace_industry_snapshot,
    snapshot_is_fresh,
)


logger = logging.getLogger(__name__)


class EasyTdxCollector:
    def __init__(
        self,
        data_dir: Path,
        fetcher: Callable[[], list[dict]] = fetch_industry_rows,
        availability_check: Callable[[], tuple[bool, str]] = availability,
    ) -> None:
        self.data_dir = Path(data_dir)
        self._fetcher = fetcher
        self._availability_check = availability_check
        self._bootstrap_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def start(self, scheduler) -> None:
        ensure_config(self.data_dir)
        if scheduler is not None:
            scheduler.add_job(
                self._scheduled_collect,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=8,
                    minute=30,
                    timezone="Asia/Shanghai",
                ),
                id="easy_tdx_industry",
                misfire_grace_time=14400,
                replace_existing=True,
            )
        self.trigger_bootstrap()

    def trigger_bootstrap(self) -> None:
        available, reason = self._availability_check()
        if not available:
            logger.info("EasyTDX 行业采集未启用: %s", reason)
            return
        if snapshot_is_fresh(self.data_dir):
            return
        if self._bootstrap_task and not self._bootstrap_task.done():
            return
        self._bootstrap_task = asyncio.create_task(
            self._run_safely(),
            name="easy-tdx-industry-bootstrap",
        )

    def stop(self) -> None:
        if self._bootstrap_task and not self._bootstrap_task.done():
            self._bootstrap_task.cancel()
        self._bootstrap_task = None

    async def _run_safely(self) -> int:
        available, reason = self._availability_check()
        if not available:
            logger.info("EasyTDX 行业采集跳过: %s", reason)
            return 0
        if self._lock.locked():
            logger.info("EasyTDX 行业采集已在运行，跳过重复触发")
            return 0
        async with self._lock:
            try:
                return await self.collect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("EasyTDX 行业采集失败 (%s)", type(exc).__name__)
                return 0

    async def _scheduled_collect(self) -> int:
        return await self._run_safely()

    async def collect(self) -> int:
        rows = await asyncio.to_thread(self._fetcher)
        if not rows:
            raise RuntimeError("EasyTDX 行业快照为空")
        collected_at = cn_now().isoformat()
        count = replace_industry_snapshot(
            self.data_dir,
            [
                {**row, "source": "easy_tdx", "collected_at": collected_at}
                for row in rows
            ],
        )
        if count == 0:
            raise RuntimeError("EasyTDX 行业快照为空")
        logger.info("EasyTDX 行业快照已更新: %d 行", count)
        return count
