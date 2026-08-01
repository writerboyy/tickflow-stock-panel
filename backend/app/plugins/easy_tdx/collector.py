"""EasyTDX 行业快照采集与调度。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

import polars as pl

from apscheduler.triggers.cron import CronTrigger

from app.market_time import cn_now
from app.plugins.easy_tdx.bridge import availability
from app.plugins.easy_tdx.client import fetch_f10_texts, fetch_industry_rows, parse_f10_reference
from app.plugins.easy_tdx.storage import (
    EXPRESS_TABLE,
    FORECAST_TABLE,
    MARGIN_TABLE,
    ensure_config,
    replace_industry_snapshot,
    snapshot_is_fresh,
    upsert_records,
)


logger = logging.getLogger(__name__)


class EasyTdxCollector:
    def __init__(
        self,
        data_dir: Path,
        fetcher: Callable[[], list[dict]] = fetch_industry_rows,
        f10_fetcher: Callable[[list[str]], list[tuple[str, str]]] = fetch_f10_texts,
        availability_check: Callable[[], tuple[bool, str]] = availability,
    ) -> None:
        self.data_dir = Path(data_dir)
        self._fetcher = fetcher
        self._f10_fetcher = f10_fetcher
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
            scheduler.add_job(
                self._scheduled_f10,
                trigger=CronTrigger(day_of_week="mon-fri", hour=18, minute=40, timezone="Asia/Shanghai"),
                id="easy_tdx_f10_reference",
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

    async def _scheduled_f10(self) -> int:
        return await self._run_f10_safely()

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

    def _stock_codes(self) -> list[str]:
        path = self.data_dir / "instruments" / "instruments.parquet"
        if not path.exists():
            return []
        frame = pl.read_parquet(path, columns=["code", "type"])
        return sorted({str(code).zfill(6) for code, kind in frame.iter_rows() if kind == "stock"})

    async def _run_f10_safely(self) -> int:
        available, reason = self._availability_check()
        if not available:
            logger.info("EasyTDX F10 采集跳过: %s", reason)
            return 0
        if self._lock.locked():
            logger.info("EasyTDX 采集已在运行，跳过 F10")
            return 0
        async with self._lock:
            try:
                return await self.collect_f10()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("EasyTDX F10 采集失败 (%s)", type(exc).__name__)
                return 0

    async def collect_f10(self, codes: list[str] | None = None) -> int:
        texts = await asyncio.to_thread(self._f10_fetcher, codes or self._stock_codes())
        margins: list[dict] = []
        forecasts: list[dict] = []
        expresses: list[dict] = []
        collected_at = cn_now().isoformat()
        for code, text in texts:
            parsed_margin, parsed_forecast, parsed_express = parse_f10_reference(text, code)
            margins.extend({**row, "collected_at": collected_at} for row in parsed_margin)
            forecasts.extend({**row, "collected_at": collected_at} for row in parsed_forecast)
            expresses.extend({**row, "collected_at": collected_at} for row in parsed_express)
        return (
            upsert_records(self.data_dir, MARGIN_TABLE, margins, ("symbol",))
            + upsert_records(self.data_dir, FORECAST_TABLE, forecasts, ("symbol", "announcement_date"))
            + upsert_records(self.data_dir, EXPRESS_TABLE, expresses, ("symbol", "announcement_date"))
        )
