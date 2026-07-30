"""开盘啦自动采集编排与调度。"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import date
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger

from app.market_time import cn_now, cn_today
from app.plugins.kaipanla.client import KaipanlaClient
from app.plugins.kaipanla.credentials import load_credentials
from app.plugins.kaipanla.parsers import (
    parse_auction,
    parse_bid_detail,
    parse_lhb_detail,
    parse_lhb_list,
    parse_limitup,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
)
from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    LHB_TABLE,
    LIMITUP_TABLE,
    REGULATORY_TABLE,
    archive_raw,
    atomic_upsert,
    ensure_configs,
    has_auction_0925,
    recent_trading_dates,
)

logger = logging.getLogger(__name__)

_PAGE_SIZE = {115: 200, 30: 200, 100: 500}
_ROW_KEY = {115: "info", 30: "info", 100: "list"}
_MAX_PAGES = 100


class KaipanlaCollector:
    def __init__(
        self,
        data_dir: Path,
        client_factory: Callable[[], KaipanlaClient] = KaipanlaClient,
    ) -> None:
        self.data_dir = Path(data_dir)
        self._client_factory = client_factory
        self._bootstrap_task: asyncio.Task | None = None
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def configured(self) -> bool:
        return load_credentials() is not None

    def start(self, scheduler) -> None:
        ensure_configs(self.data_dir)
        if scheduler is not None:
            for checkpoint, hour, minute in (
                ("0915", 9, 15),
                ("0920", 9, 20),
                ("0925", 9, 25),
            ):
                scheduler.add_job(
                    self._scheduled_auction,
                    args=[checkpoint],
                    trigger=CronTrigger(
                        day_of_week="mon-fri",
                        hour=hour,
                        minute=minute,
                        second=5,
                        timezone="Asia/Shanghai",
                    ),
                    id=f"kaipanla_auction_{checkpoint}",
                    misfire_grace_time=20,
                    replace_existing=True,
                )
            scheduler.add_job(
                self._scheduled_limitup,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=15,
                    minute=30,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_limitup",
                misfire_grace_time=7200,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_catch_up,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=15,
                    minute=40,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_auction_catch_up",
                misfire_grace_time=7200,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_lhb,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=18,
                    minute=0,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_lhb",
                misfire_grace_time=14400,
                replace_existing=True,
            )
            for snapshot, hour, minute in (("pre", 8, 50), ("post", 15, 20)):
                scheduler.add_job(
                    self._scheduled_regulatory,
                    args=[snapshot],
                    trigger=CronTrigger(
                        day_of_week="mon-fri",
                        hour=hour,
                        minute=minute,
                        timezone="Asia/Shanghai",
                    ),
                    id=f"kaipanla_regulatory_{snapshot}",
                    misfire_grace_time=3600,
                    replace_existing=True,
                )
        self.trigger_catch_up()

    def trigger_catch_up(self) -> None:
        if not self.configured:
            return
        if self._bootstrap_task and not self._bootstrap_task.done():
            return
        self._bootstrap_task = asyncio.create_task(
            self._run_safely("auction_catch_up", self.catch_up_auction),
            name="kaipanla-auction-catch-up",
        )

    def stop(self) -> None:
        if self._bootstrap_task and not self._bootstrap_task.done():
            self._bootstrap_task.cancel()
        self._bootstrap_task = None

    async def _run_safely(self, name: str, func, *args) -> int:
        if not self.configured:
            return 0
        lock = self._locks.setdefault(name, asyncio.Lock())
        if lock.locked():
            logger.info("开盘啦任务 %s 已在运行，跳过重复触发", name)
            return 0
        async with lock:
            try:
                return await func(*args)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("开盘啦任务 %s 失败 (%s)", name, type(exc).__name__)
                return 0

    async def _scheduled_auction(self, checkpoint: str) -> int:
        return await self._run_safely(
            f"auction_{checkpoint}",
            self.collect_auction,
            checkpoint,
            cn_today(),
            False,
        )

    async def _scheduled_limitup(self) -> int:
        return await self._run_safely("limitup", self.collect_limitup, cn_today())

    async def _scheduled_lhb(self) -> int:
        return await self._run_safely("lhb", self.collect_lhb)

    async def _scheduled_regulatory(self, snapshot: str) -> int:
        return await self._run_safely(
            f"regulatory_{snapshot}",
            self.collect_regulatory,
            snapshot,
            cn_today(),
        )

    async def _scheduled_catch_up(self) -> int:
        return await self._run_safely("auction_catch_up", self.catch_up_auction)

    async def _fetch_pages(
        self,
        client: KaipanlaClient,
        endpoint: int,
        trade_date: date,
        params: dict[str, object] | None = None,
    ) -> list[dict]:
        page_size = _PAGE_SIZE[endpoint]
        pages = []
        for index in range(_MAX_PAGES):
            payload = await client.request(
                endpoint,
                {**(params or {}), "Index": index, "st": page_size},
            )
            archive_raw(self.data_dir, endpoint, trade_date, payload, f"page-{index}")
            pages.append(payload)
            rows = payload.get(_ROW_KEY[endpoint])
            if not isinstance(rows, list):
                break
            if len(rows) < page_size:
                return pages
        if len(pages) >= _MAX_PAGES:
            raise RuntimeError(f"开盘啦 /{endpoint} 分页超过安全上限")
        return pages

    async def collect_auction(
        self,
        checkpoint: str,
        trade_date: date,
        historical: bool = False,
    ) -> int:
        if checkpoint not in {"0915", "0920", "0925"}:
            raise ValueError("竞价检查点无效")
        endpoint = 30 if historical else 115
        params = {"Date": trade_date.isoformat()} if historical else {}
        collected_at = cn_now().isoformat()
        async with self._client_factory() as client:
            pages = await self._fetch_pages(client, endpoint, trade_date, params)
            base_rows = [row for payload in pages for row in parse_auction(payload)]
            rows = []
            for row in base_rows:
                item = {key: row.get(key) for key in ("symbol", "code", "name")}
                item[f"collected_at_{checkpoint}"] = collected_at
                item[f"source_{checkpoint}"] = f"/{endpoint}"
                for key, value in row.items():
                    if key not in {"symbol", "code", "name"}:
                        item[f"{key}_{checkpoint}"] = value
                rows.append(item)
            count = atomic_upsert(self.data_dir, AUCTION_TABLE, trade_date, rows)
            if not historical and checkpoint == "0925" and base_rows:
                await self._collect_bid_details(client, trade_date, base_rows)
            return count

    async def _collect_bid_details(
        self,
        client: KaipanlaClient,
        trade_date: date,
        auction_rows: list[dict],
    ) -> int:
        semaphore = asyncio.Semaphore(4)
        unique_codes = sorted({str(row["code"]) for row in auction_rows if row.get("code")})

        async def collect_one(code: str) -> dict | None:
            async with semaphore:
                try:
                    payload = await client.request(31, {"StockID": code})
                    archive_raw(self.data_dir, 31, trade_date, payload, code)
                    return {
                        **parse_bid_detail(payload),
                        "bid_collected_at": cn_now().isoformat(),
                    }
                except Exception as exc:  # noqa: BLE001
                    logger.warning("开盘啦 /31 个股 %s 采集失败 (%s)", code, type(exc).__name__)
                    return None

        details = [
            row for row in await asyncio.gather(*(collect_one(c) for c in unique_codes)) if row
        ]
        return atomic_upsert(self.data_dir, AUCTION_TABLE, trade_date, details)

    async def collect_limitup(self, trade_date: date) -> int:
        async with self._client_factory() as client:
            payload = await client.request(15, {"Index": 0, "st": 1000})
        archive_raw(self.data_dir, 15, trade_date, payload)
        collected_at = cn_now().isoformat()
        rows = [{**row, "collected_at": collected_at} for row in parse_limitup(payload)]
        return atomic_upsert(self.data_dir, LIMITUP_TABLE, trade_date, rows)

    async def collect_lhb(self) -> int:
        requested_date = cn_today()
        async with self._client_factory() as client:
            pages = await self._fetch_pages(client, 100, requested_date, {"Time": ""})
            trade_date: date | None = None
            rows: list[dict] = []
            for payload in pages:
                payload_date, payload_rows = parse_lhb_list(payload)
                trade_date = trade_date or payload_date
                rows.extend(payload_rows)
            trade_date = trade_date or requested_date
            collected_at = cn_now().isoformat()
            count = atomic_upsert(
                self.data_dir,
                LHB_TABLE,
                trade_date,
                [{**row, "collected_at": collected_at} for row in rows],
            )
            if rows:
                await self._collect_lhb_details(client, trade_date, rows)
            return count

    async def _collect_lhb_details(
        self,
        client: KaipanlaClient,
        trade_date: date,
        lhb_rows: list[dict],
    ) -> int:
        semaphore = asyncio.Semaphore(4)
        unique_codes = sorted({str(row["code"]) for row in lhb_rows if row.get("code")})

        async def collect_one(code: str) -> dict | None:
            async with semaphore:
                try:
                    payload = await client.request(
                        101,
                        {
                            "StockID": code,
                            "Time": trade_date.isoformat(),
                        },
                    )
                    archive_raw(self.data_dir, 101, trade_date, payload, code)
                    return {
                        **parse_lhb_detail(payload, code),
                        "detail_collected_at": cn_now().isoformat(),
                    }
                except Exception as exc:  # noqa: BLE001
                    logger.warning("开盘啦 /101 个股 %s 采集失败 (%s)", code, type(exc).__name__)
                    return None

        details = [
            row for row in await asyncio.gather(*(collect_one(c) for c in unique_codes)) if row
        ]
        return atomic_upsert(self.data_dir, LHB_TABLE, trade_date, details)

    async def collect_regulatory(self, snapshot: str, trade_date: date) -> int:
        if snapshot not in {"pre", "post"}:
            raise ValueError("监管快照类型无效")
        collected_at = cn_now().isoformat()
        rows: list[dict] = []
        successes = 0
        async with self._client_factory() as client:
            for endpoint, parser in (
                (108, parse_regulatory_monitor),
                (109, parse_regulatory_anomaly),
            ):
                try:
                    payload = await client.request(endpoint)
                    archive_raw(self.data_dir, endpoint, trade_date, payload, snapshot)
                    parsed = parser(payload)
                    for row in parsed:
                        base = {key: row.get(key) for key in ("symbol", "code", "name")}
                        base[f"{snapshot}_collected_at"] = collected_at
                        base.update(
                            {
                                f"{snapshot}_{key}": value
                                for key, value in row.items()
                                if key not in {"symbol", "code", "name"}
                            }
                        )
                        rows.append(base)
                    successes += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("开盘啦 /%d 采集失败 (%s)", endpoint, type(exc).__name__)
        if successes == 0:
            raise RuntimeError("开盘啦监管接口均采集失败")
        return atomic_upsert(self.data_dir, REGULATORY_TABLE, trade_date, rows)

    async def catch_up_auction(self) -> int:
        if not self.configured:
            return 0
        today = cn_today()
        candidates = [day for day in recent_trading_dates(self.data_dir, 60) if day < today]
        missing = [day for day in candidates if not has_auction_0925(self.data_dir, day)]
        total = 0
        for trade_date in missing:
            try:
                total += await self.collect_auction("0925", trade_date, True)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "开盘啦 /30 历史日期 %s 回补停止 (%s)",
                    trade_date,
                    type(exc).__name__,
                )
                break
        return total
