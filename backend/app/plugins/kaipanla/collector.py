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
    parse_capital_net,
    parse_auction,
    parse_bid_detail,
    parse_dragon_tiger_details,
    parse_dragon_tiger_movement,
    parse_lhb_detail,
    parse_lhb_list,
    parse_interval_stock,
    parse_large_order_statistics,
    parse_limitup,
    parse_northbound_sector,
    parse_northbound_stocks,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
    parse_sector_constituents,
    parse_sector_strength,
    parse_shareholder_changes,
    parse_shareholder_count_changes,
)
from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    LHB_TABLE,
    LIMITUP_TABLE,
    FUNDS_TABLE,
    LHB_DETAIL_TABLE,
    LHB_MOVEMENT_TABLE,
    NORTHBOUND_SECTOR_TABLE,
    NORTHBOUND_STOCK_TABLE,
    REGULATORY_TABLE,
    SECTOR_CONSTITUENT_TABLE,
    SHAREHOLDER_COUNT_TABLE,
    SHAREHOLDER_TABLE,
    archive_raw,
    atomic_upsert,
    atomic_upsert_records,
    ensure_configs,
    has_auction_0925,
    recent_trading_dates,
)
from app.services.ingestion_manifest import (
    load_ingestion_manifest,
    record_ingestion_batch,
    stable_content_hash,
    update_ingestion_manifest,
)

logger = logging.getLogger(__name__)

_PAGE_SIZE = {115: 200, 30: 200, 100: 500}
_ROW_KEY = {115: "info", 30: "info", 100: "list"}
_MAX_PAGES = 100
_FUND_INTERVAL_PAGE_SIZE = 1000
_REFERENCE_PAGE_SIZE = 1000


def _shareholder_count_windows(payload: dict) -> list[tuple[date, date]]:
    values = payload.get("DateList")
    if not isinstance(values, list):
        raise ValueError("开盘啦股东人数窗口不是数组")
    windows = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise ValueError(f"开盘啦股东人数窗口 {index} 不是对象")
        start = value.get("StratDate")
        end = value.get("EndDate")
        if not isinstance(start, str) or not isinstance(end, str):
            raise ValueError(f"开盘啦股东人数窗口 {index} 缺少日期")
        windows.append((date.fromisoformat(start), date.fromisoformat(end)))
    return windows


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

    def start(self, scheduler, *, bootstrap: bool = True) -> None:
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
                self._scheduled_funds,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=15,
                    minute=6,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_funds",
                misfire_grace_time=14400,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_northbound,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=18,
                    minute=10,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_northbound",
                misfire_grace_time=14400,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_shareholder_counts,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=18,
                    minute=20,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_shareholder_counts",
                misfire_grace_time=14400,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_sector_constituents,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=18,
                    minute=30,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_sector_constituents",
                misfire_grace_time=14400,
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
        if bootstrap:
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

    async def _scheduled_funds(self) -> int:
        return await self._run_safely("funds", self.collect_funds, cn_today())

    async def _scheduled_northbound(self) -> int:
        return await self._run_safely("northbound", self.collect_northbound)

    async def _scheduled_shareholder_counts(self) -> int:
        return await self._run_safely(
            "shareholder_counts", self.collect_shareholder_counts, cn_today(), cn_today()
        )

    async def _scheduled_sector_constituents(self) -> int:
        return await self._run_safely(
            "sector_constituents", self.collect_sector_constituents, cn_today(), self._northbound_plate_ids()
        )

    def _stock_codes(self) -> list[str]:
        path = self.data_dir / "instruments" / "instruments.parquet"
        if not path.exists():
            return []
        try:
            import polars as pl

            available = set(pl.read_parquet_schema(path))
            if not {"code", "type"}.issubset(available):
                logger.warning("开盘啦资金池缺少 code/type 列")
                return []
            frame = pl.read_parquet(path, columns=["code", "type"])
            frame = frame.filter(pl.col("type") == "stock")
            return sorted({str(code) for code in frame["code"].to_list() if code})
        except Exception as exc:  # noqa: BLE001
            logger.warning("开盘啦资金池读取失败 (%s)", type(exc).__name__)
            return []

    def _northbound_plate_ids(self) -> list[str]:
        root = self.data_dir / "ext_data" / NORTHBOUND_SECTOR_TABLE / "timeseries"
        partitions = sorted(root.glob("date=*/part.parquet"))
        if not partitions:
            return []
        try:
            import polars as pl

            frame = pl.read_parquet(partitions[-1], columns=["plate_id"])
            return sorted({str(value) for value in frame["plate_id"].to_list() if value})
        except Exception as exc:  # noqa: BLE001
            logger.warning("开盘啦北向板块池读取失败 (%s)", type(exc).__name__)
            return []

    async def collect_funds(self, trade_date: date) -> int:
        """盘后采集全市场资金排名，并补全逐股大单日频快照。"""
        collected_at = cn_now().isoformat()
        interval_rows: list[dict] = []
        interval_codes: set[str] = set()
        async with self._client_factory() as client:
            for offset in range(0, _MAX_PAGES * _FUND_INTERVAL_PAGE_SIZE, _FUND_INTERVAL_PAGE_SIZE):
                payload = await client.request(
                    "fund_interval",
                    {
                        "DStart": trade_date.strftime("%Y-%m-%d"),
                        "DEnd": trade_date.strftime("%Y-%m-%d"),
                        "Index": offset,
                        "st": _FUND_INTERVAL_PAGE_SIZE,
                    },
                )
                archive_raw(self.data_dir, "fund_interval", trade_date, payload, f"offset-{offset}")
                parsed = parse_interval_stock(payload)
                fresh = []
                for row in parsed:
                    code = row["code"]
                    if code not in interval_codes:
                        interval_codes.add(code)
                        fresh.append(row)
                if not fresh:
                    break
                interval_rows.extend(fresh)
                if len(parsed) < _FUND_INTERVAL_PAGE_SIZE:
                    break
            else:
                raise RuntimeError("开盘啦资金排名分页超过安全上限")

            codes = self._stock_codes()
            semaphore = asyncio.Semaphore(16)

            async def collect_one(code: str) -> dict | None:
                async with semaphore:
                    row: dict = {}
                    try:
                        capital = await client.request(
                            "fund_capital_net",
                            {"StockID": code, "Date": trade_date.strftime("%Y-%m-%d")},
                        )
                        archive_raw(self.data_dir, "fund_capital_net", trade_date, capital, code)
                        row.update(parse_capital_net(capital, code))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦分时大单采集失败 (%s)", type(exc).__name__)
                    try:
                        stats = await client.request(
                            "fund_large_order_statistics",
                            {"StockID": code, "Index": 0, "st": 120},
                        )
                        archive_raw(
                            self.data_dir,
                            "fund_large_order_statistics",
                            trade_date,
                            stats,
                            code,
                        )
                        row.update(parse_large_order_statistics(stats, code, trade_date) or {})
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦日度大单采集失败 (%s)", type(exc).__name__)
                    return row or None

            details = [row for row in await asyncio.gather(*(collect_one(code) for code in codes)) if row]

        count = atomic_upsert(
            self.data_dir,
            FUNDS_TABLE,
            trade_date,
            [{**row, "collected_at": collected_at} for row in interval_rows + details],
        )
        return count

    def _write_records(
        self,
        table_id: str,
        rows: list[dict],
        key_fields: tuple[str, ...],
    ) -> int:
        buckets: dict[date, list[dict]] = {}
        for row in rows:
            report_date = row.get("report_date")
            if not isinstance(report_date, str):
                raise ValueError(f"{table_id} 记录缺少报告期")
            value = date.fromisoformat(report_date)
            buckets.setdefault(value, []).append({**row, "collected_at": cn_now().isoformat()})
        return sum(
            atomic_upsert_records(self.data_dir, table_id, value, values, key_fields)
            for value, values in buckets.items()
        )

    async def collect_northbound(self, report_date: date | None = None) -> int:
        """采集北向季度板块及个股持仓，不混作每日资金流。"""
        sector_endpoint = "northbound_sector_history" if report_date else "northbound_sector_latest"
        stock_endpoint = "northbound_stocks_history" if report_date else "northbound_stocks_latest"
        sector_rows: list[dict] = []
        seen_plates: set[str] = set()
        async with self._client_factory() as client:
            for offset in range(0, _MAX_PAGES * 20, 20):
                params = {"Index": offset, "st": 20}
                if report_date:
                    params["Date"] = report_date.isoformat()
                payload = await client.request(sector_endpoint, params)
                archive_raw(self.data_dir, sector_endpoint, report_date or cn_today(), payload, f"offset-{offset}")
                _, parsed = parse_northbound_sector(payload)
                fresh = [row for row in parsed if row["plate_id"] not in seen_plates]
                if not fresh:
                    break
                seen_plates.update(row["plate_id"] for row in fresh)
                sector_rows.extend(fresh)
                if len(parsed) < 20:
                    break
            else:
                raise RuntimeError("开盘啦北向板块分页超过安全上限")

            semaphore = asyncio.Semaphore(8)

            async def collect_plate(plate_id: str) -> list[dict]:
                async with semaphore:
                    try:
                        params = {"IndexID": plate_id, "Index": 0, "st": _REFERENCE_PAGE_SIZE}
                        if report_date:
                            params["Date"] = report_date.isoformat()
                        payload = await client.request(stock_endpoint, params)
                        archive_raw(self.data_dir, stock_endpoint, report_date or cn_today(), payload, plate_id)
                        _, parsed = parse_northbound_stocks(payload, plate_id)
                        return parsed
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦北向个股采集失败 (%s)", type(exc).__name__)
                        return []

            stock_rows = [
                row
                for rows in await asyncio.gather(*(collect_plate(code) for code in sorted(seen_plates)))
                for row in rows
            ]
        return self._write_records(NORTHBOUND_SECTOR_TABLE, sector_rows, ("plate_id",)) + self._write_records(
            NORTHBOUND_STOCK_TABLE, stock_rows, ("plate_id", "symbol")
        )

    async def collect_shareholder_counts(self, start_date: date, end_date: date) -> int:
        """采集指定统计区间的股东人数变更，日期取上游每行 Day。"""
        rows: list[dict] = []
        async with self._client_factory() as client:
            window_payload = await client.request(
                "shareholder_count_changes",
                {
                    "StratDate": start_date.isoformat(),
                    "EndDate": end_date.isoformat(),
                    "Index": 0,
                    "st": _REFERENCE_PAGE_SIZE,
                },
            )
            archive_raw(self.data_dir, "shareholder_count_changes", end_date, window_payload, "windows")
            windows = _shareholder_count_windows(window_payload)
            for window_start, window_end in windows:
                for offset in range(0, _MAX_PAGES * _REFERENCE_PAGE_SIZE, _REFERENCE_PAGE_SIZE):
                    payload = await client.request(
                        "shareholder_count_changes",
                        {
                            "StratDate": window_start.isoformat(),
                            "EndDate": window_end.isoformat(),
                            "Index": offset,
                            "st": _REFERENCE_PAGE_SIZE,
                        },
                    )
                    archive_raw(
                        self.data_dir,
                        "shareholder_count_changes",
                        window_end,
                        payload,
                        f"offset-{offset}",
                    )
                    parsed = parse_shareholder_count_changes(payload)
                    rows.extend(parsed)
                    if len(parsed) < _REFERENCE_PAGE_SIZE:
                        break
                else:
                    raise RuntimeError("开盘啦股东人数分页超过安全上限")
        return self._write_records(SHAREHOLDER_COUNT_TABLE, rows, ("symbol",))

    async def collect_shareholder_changes(self, report_date: date) -> int:
        """按报告期采集全 A 股十大流通股东。"""
        semaphore = asyncio.Semaphore(8)

        async with self._client_factory() as client:
            async def collect_one(code: str) -> list[dict]:
                async with semaphore:
                    try:
                        payload = await client.request(
                            "shareholder_changes", {"StockID": code, "Day": report_date.isoformat()}
                        )
                        archive_raw(self.data_dir, "shareholder_changes", report_date, payload, code)
                        return parse_shareholder_changes(payload, code, report_date)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦十大流通股东采集失败 (%s)", type(exc).__name__)
                        return []

            rows = [
                row
                for values in await asyncio.gather(*(collect_one(code) for code in self._stock_codes()))
                for row in values
            ]
        return self._write_records(
            SHAREHOLDER_TABLE, rows, ("symbol", "snapshot_kind", "shareholder_id")
        )

    async def collect_sector_constituents(
        self, trade_date: date, plate_ids: list[str] | None = None
    ) -> int:
        """由板块强度榜发现板块，再抓取目标日完整历史成分。"""
        async with self._client_factory() as client:
            if plate_ids is None:
                payload = await client.request(
                    "sector_strength", {"Day": trade_date.isoformat(), "Index": 0, "st": _REFERENCE_PAGE_SIZE}
                )
                archive_raw(self.data_dir, "sector_strength", trade_date, payload)
                plate_ids = [row["plate_id"] for row in parse_sector_strength(payload)]
            semaphore = asyncio.Semaphore(8)

            async def collect_one(plate_id: str) -> list[dict]:
                async with semaphore:
                    try:
                        rows = []
                        seen_codes: set[str] = set()
                        for offset in range(
                            0, _MAX_PAGES * _REFERENCE_PAGE_SIZE, _REFERENCE_PAGE_SIZE
                        ):
                            payload = await client.request(
                                "sector_constituents",
                                {
                                    "PlateID": plate_id,
                                    "Date": trade_date.isoformat(),
                                    "Index": offset,
                                    "st": _REFERENCE_PAGE_SIZE,
                                },
                            )
                            archive_raw(
                                self.data_dir,
                                "sector_constituents",
                                trade_date,
                                payload,
                                f"{plate_id}-{offset}",
                            )
                            parsed = parse_sector_constituents(payload, plate_id)
                            fresh = [row for row in parsed if row["code"] not in seen_codes]
                            if not fresh:
                                break
                            seen_codes.update(row["code"] for row in fresh)
                            rows.extend(fresh)
                            if len(parsed) < _REFERENCE_PAGE_SIZE:
                                break
                        return rows
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦板块成分采集失败 (%s)", type(exc).__name__)
                        return []

            rows = [
                row
                for values in await asyncio.gather(*(collect_one(plate_id) for plate_id in sorted(set(plate_ids))))
                for row in values
            ]
        stamped = [{**row, "report_date": trade_date.isoformat()} for row in rows]
        return self._write_records(SECTOR_CONSTITUENT_TABLE, stamped, ("plate_id", "symbol"))

    async def collect_lhb_reference(self, trade_date: date, codes: list[str]) -> int:
        """补充龙虎榜游资动向和股票席位明细。"""
        async with self._client_factory() as client:
            movement_rows: list[dict] = []
            try:
                movement_payload = await client.request(
                    "dragon_tiger_movement", {"Date": trade_date.isoformat()}
                )
                archive_raw(self.data_dir, "dragon_tiger_movement", trade_date, movement_payload)
                movement_rows = parse_dragon_tiger_movement(movement_payload, trade_date)
            except Exception as exc:  # noqa: BLE001
                logger.warning("开盘啦龙虎榜游资动向采集失败 (%s)", type(exc).__name__)
            semaphore = asyncio.Semaphore(8)

            async def collect_one(code: str) -> list[dict]:
                async with semaphore:
                    try:
                        payload = await client.request(
                            "dragon_tiger_details", {"StockID": code, "Time": trade_date.isoformat()}
                        )
                        archive_raw(self.data_dir, "dragon_tiger_details", trade_date, payload, code)
                        return parse_dragon_tiger_details(payload, code)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("开盘啦龙虎榜席位采集失败 (%s)", type(exc).__name__)
                        return []

            detail_rows = [
                row for values in await asyncio.gather(*(collect_one(code) for code in sorted(set(codes)))) for row in values
            ]
        movement = [{**row, "report_date": trade_date.isoformat()} for row in movement_rows]
        details = [{**row, "report_date": trade_date.isoformat()} for row in detail_rows]
        return self._write_records(
            LHB_MOVEMENT_TABLE, movement, ("participant_id", "side", "symbol")
        ) + self._write_records(LHB_DETAIL_TABLE, details, ("symbol", "side", "rank"))

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
            batch_id = f"page-{index:03d}"
            try:
                payload = await client.request(
                    endpoint,
                    {**(params or {}), "Index": index, "st": page_size},
                )
            except Exception as exc:  # noqa: BLE001
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    f"endpoint_{endpoint}",
                    trade_date.isoformat(),
                    batch_id,
                    status="source_error",
                    error_code=type(exc).__name__,
                    parser_version="kaipanla_v1",
                    schema_version=1,
                    page_size=page_size,
                )
                raise
            archive_raw(self.data_dir, endpoint, trade_date, payload, f"page-{index}")
            pages.append(payload)
            rows = payload.get(_ROW_KEY[endpoint])
            if not isinstance(rows, list):
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    f"endpoint_{endpoint}",
                    trade_date.isoformat(),
                    batch_id,
                    status="parse_rejected",
                    error_code="rows_not_list",
                    source_content_hash=stable_content_hash(payload),
                    parser_version="kaipanla_v1",
                    schema_version=1,
                    page_size=page_size,
                )
                raise ValueError(f"开盘啦 /{endpoint} 分页记录不是数组")
            record_ingestion_batch(
                self.data_dir,
                "kaipanla",
                f"endpoint_{endpoint}",
                trade_date.isoformat(),
                batch_id,
                status="valid_empty" if not rows else "completed",
                row_count=len(rows),
                content_hash=stable_content_hash(rows),
                source_content_hash=stable_content_hash(payload),
                empty_reason="valid_empty" if not rows else None,
                parser_version="kaipanla_v1",
                schema_version=1,
                page_size=page_size,
            )
            if len(rows) < page_size:
                update_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    f"endpoint_{endpoint}",
                    trade_date.isoformat(),
                    status="valid_empty" if not any(
                        payload.get(_ROW_KEY[endpoint]) for payload in pages
                    ) else "complete",
                    completed_pages=len(pages),
                    empty_reason="valid_empty" if not any(
                        payload.get(_ROW_KEY[endpoint]) for payload in pages
                    ) else None,
                )
                return pages
        if len(pages) >= _MAX_PAGES:
            update_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                f"endpoint_{endpoint}",
                trade_date.isoformat(),
                status="page_limit_reached",
                completed_pages=len(pages),
                error_code="page_limit_reached",
            )
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
            components = {
                checkpoint: {
                    "status": "valid_empty" if not rows else "published",
                    "endpoint": f"/{endpoint}",
                    "rows": len(rows),
                }
            }
            if not historical and checkpoint == "0925" and base_rows:
                await self._collect_bid_details(client, trade_date, base_rows)
                bid_state = load_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    "auction_bid_detail",
                    trade_date.isoformat(),
                )
                components["bid_detail"] = {
                    "status": bid_state.get("status", "incomplete"),
                    "rows": bid_state.get("published_rows", 0),
                }
            elif checkpoint == "0925":
                components["bid_detail"] = {"status": "not_applicable", "rows": 0}
            existing = load_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                "auction_completion",
                trade_date.isoformat(),
            )
            all_components = {**(existing.get("components") or {}), **components}
            required_components = (
                {"0925", "bid_detail"}
                if historical
                else {"0915", "0920", "0925", "bid_detail"}
            )
            update_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                "auction_completion",
                trade_date.isoformat(),
                status=(
                    "complete"
                    if required_components <= set(all_components)
                    and all(
                        all_components[name].get("status")
                        in {"published", "valid_empty", "complete", "not_applicable"}
                        for name in required_components
                    )
                    else "incomplete"
                ),
                parser_version="kaipanla_v1",
                schema_version=1,
                expected_components=sorted(required_components),
                components=all_components,
            )
            return count

    async def _collect_bid_details(
        self,
        client: KaipanlaClient,
        trade_date: date,
        auction_rows: list[dict],
    ) -> int:
        semaphore = asyncio.Semaphore(4)
        unique_codes = sorted({str(row["code"]) for row in auction_rows if row.get("code")})
        failed_codes: set[str] = set()

        async def collect_one(code: str) -> dict | None:
            async with semaphore:
                try:
                    payload = await client.request(31, {"StockID": code})
                    archive_raw(self.data_dir, 31, trade_date, payload, code)
                    row = {
                        **parse_bid_detail(payload),
                        "bid_collected_at": cn_now().isoformat(),
                    }
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "auction_bid_detail",
                        trade_date.isoformat(),
                        code,
                        status="completed",
                        row_count=1,
                        content_hash=stable_content_hash(row),
                        source_content_hash=stable_content_hash(payload),
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        expected_batches=unique_codes,
                    )
                    return row
                except Exception as exc:  # noqa: BLE001
                    failed_codes.add(code)
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "auction_bid_detail",
                        trade_date.isoformat(),
                        code,
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        expected_batches=unique_codes,
                    )
                    logger.warning("开盘啦 /31 个股 %s 采集失败 (%s)", code, type(exc).__name__)
                    return None

        details = [
            row for row in await asyncio.gather(*(collect_one(c) for c in unique_codes)) if row
        ]
        count = 0
        if not failed_codes:
            count = atomic_upsert(self.data_dir, AUCTION_TABLE, trade_date, details)
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            "auction_bid_detail",
            trade_date.isoformat(),
            status="incomplete" if failed_codes else "complete",
            expected_batches=unique_codes,
            failed_batches=sorted(failed_codes),
            published_rows=count,
        )
        return count

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
            try:
                await self.collect_lhb_reference(
                    trade_date,
                    [str(row["code"]) for row in rows if row.get("code")],
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("开盘啦扩展龙虎榜采集失败 (%s)", type(exc).__name__)
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
