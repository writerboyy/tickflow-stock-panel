"""开盘啦自动采集编排与调度。"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import date, time as clock_time
from pathlib import Path
from typing import Any

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
    parse_limit_up_expression,
    parse_limit_up_ladder_height,
    parse_limitup,
    parse_northbound_sector,
    parse_northbound_stocks,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
    parse_trade_date,
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
    append_sector_strength_snapshot,
    archive_raw,
    atomic_upsert,
    atomic_upsert_records,
    ensure_configs,
    has_auction_0925,
    recent_trading_dates,
    read_sector_constituents,
    read_sector_strength_snapshot,
    read_sector_strength_timeline,
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


def _in_sector_strength_window(value: clock_time) -> bool:
    return (
        clock_time(9, 25) <= value <= clock_time(11, 30)
        or clock_time(13, 0) <= value <= clock_time(15, 0)
    )


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
        self._sentiment_task: asyncio.Task | None = None
        self._sector_strength_task: asyncio.Task | None = None
        self._sector_constituents_task: asyncio.Task | None = None
        self._locks: dict[str, asyncio.Lock] = {}
        self._sentiment_lock = threading.Lock()
        self._market_sentiment: dict | None = None
        self._sector_strength_lock = threading.Lock()
        self._sector_strength: dict | None = None
        self._sector_constituents_lock = threading.Lock()
        self._sector_constituents_cache: dict[tuple[date, str], list[dict]] = {}

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
                self._scheduled_market_sentiment,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour="9-15",
                    minute="*",
                    second="*/15",
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_market_sentiment",
                misfire_grace_time=30,
                replace_existing=True,
            )
            scheduler.add_job(
                self._scheduled_sector_strength,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour="9-15",
                    minute="*",
                    second="*/5",
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_sector_strength",
                misfire_grace_time=30,
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
                    hour=8,
                    minute=45,
                    timezone="Asia/Shanghai",
                ),
                id="kaipanla_sector_constituents",
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
        if self._sentiment_task is None or self._sentiment_task.done():
            self._sentiment_task = asyncio.create_task(
                self._run_safely(
                    "market_sentiment",
                    self.refresh_market_sentiment,
                    cn_today(),
                ),
                name="kaipanla-market-sentiment",
            )
        if self._sector_strength_task is None or self._sector_strength_task.done():
            self._sector_strength_task = asyncio.create_task(
                self._run_safely(
                    "sector_strength",
                    self.refresh_sector_strength,
                    cn_today(),
                ),
                name="kaipanla-sector-strength",
            )
        if self._sector_constituents_task is None or self._sector_constituents_task.done():
            self._sector_constituents_task = asyncio.create_task(
                self._scheduled_sector_constituents(),
                name="kaipanla-sector-constituents",
            )

    def stop(self) -> None:
        if self._bootstrap_task and not self._bootstrap_task.done():
            self._bootstrap_task.cancel()
        if self._sentiment_task and not self._sentiment_task.done():
            self._sentiment_task.cancel()
        if self._sector_strength_task and not self._sector_strength_task.done():
            self._sector_strength_task.cancel()
        if self._sector_constituents_task and not self._sector_constituents_task.done():
            self._sector_constituents_task.cancel()
        self._bootstrap_task = None
        self._sentiment_task = None
        self._sector_strength_task = None
        self._sector_constituents_task = None

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

    async def _scheduled_market_sentiment(self) -> int:
        now = cn_now()
        current = now.timetz().replace(tzinfo=None)
        if not (
            clock_time(9, 15) <= current <= clock_time(11, 30)
            or clock_time(13, 0) <= current <= clock_time(15, 5)
        ):
            return 0
        return await self._run_safely(
            "market_sentiment",
            self.refresh_market_sentiment,
            now.date(),
        )

    async def _scheduled_sector_strength(self) -> int:
        now = cn_now()
        current = now.timetz().replace(tzinfo=None)
        if not _in_sector_strength_window(current):
            return 0
        return await self._run_safely(
            "sector_strength",
            self.refresh_sector_strength,
            now.date(),
        )

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
        today = cn_today()
        completed_dates = [
            trade_date
            for trade_date in recent_trading_dates(self.data_dir, 60)
            if trade_date < today
        ]
        if not completed_dates:
            logger.warning("开盘啦资金采集缺少已完成交易日")
            return 0
        return await self._run_safely("funds", self.collect_funds, max(completed_dates))

    def market_sentiment_snapshot(self) -> dict | None:
        """Return the latest in-memory Kaipanla sentiment snapshot."""
        with self._sentiment_lock:
            return dict(self._market_sentiment) if self._market_sentiment else None

    def sector_strength_snapshot(self) -> dict | None:
        """Return today's live ranking, restoring the latest persisted point after restart."""
        with self._sector_strength_lock:
            current = self._sector_strength
        if not current:
            current = read_sector_strength_snapshot(self.data_dir, cn_today())
            if current:
                with self._sector_strength_lock:
                    self._sector_strength = current
        if not current:
            return None
        return {
            **current,
            "rows": [dict(row) for row in current.get("rows") or []],
        }

    def sector_strength_timeline(self, trade_date: date) -> list[str]:
        return read_sector_strength_timeline(self.data_dir, trade_date)

    def sector_strength_snapshot_at(self, trade_date: date, captured_at: str) -> dict | None:
        return read_sector_strength_snapshot(self.data_dir, trade_date, captured_at)

    def latest_completed_trading_date(self, trade_date: date) -> date | None:
        return next(
            (value for value in reversed(recent_trading_dates(self.data_dir)) if value < trade_date),
            None,
        )

    async def refresh_sector_strength(self, trade_date: date) -> int:
        """Poll today's board ranking without carrying an old trading day forward."""
        institution_label = None
        try:
            async with self._client_factory() as client:
                payload = await client.request(
                    "sector_strength",
                    {
                        "Day": trade_date.isoformat(),
                        "Index": 0,
                        "st": _REFERENCE_PAGE_SIZE,
                    },
                )
            reported = payload.get("Day") if isinstance(payload, dict) else None
            if isinstance(reported, list):
                reported = reported[0] if reported else None
            if reported not in (None, "") and parse_trade_date(reported) != trade_date:
                raise ValueError("实时板块强度返回的交易日不是当天")
            rows = parse_sector_strength(payload)
            titles = payload.get("Title") if isinstance(payload, dict) else None
            if isinstance(titles, list) and titles:
                institution_label = str(titles[0] or "").strip() or None
        except Exception:  # noqa: BLE001
            logger.debug("实时板块强度接口暂不可用", exc_info=True)
            rows = []
        captured_now = cn_now()
        in_capture_window = captured_now.date() == trade_date and _in_sector_strength_window(
            captured_now.timetz().replace(tzinfo=None),
        )
        snapshot = {
            "provider": "kaipanla",
            "state": "live" if rows else "unavailable",
            "as_of": trade_date.isoformat(),
            "refreshed_at": captured_now.isoformat(),
            "institution_label": institution_label,
            "history_state": "closed" if rows and not in_capture_window else "unavailable",
            "rows": rows,
        }
        if rows and in_capture_window:
            try:
                await asyncio.to_thread(
                    append_sector_strength_snapshot,
                    self.data_dir,
                    trade_date,
                    snapshot,
                )
                snapshot["history_state"] = "live"
            except Exception:  # noqa: BLE001
                logger.warning("实时板块强度快照落库失败", exc_info=True)
        with self._sector_strength_lock:
            self._sector_strength = snapshot
        return len(rows)

    async def sector_constituents_at(
        self,
        trade_date: date,
        plate_id: str,
        end_hhmm: str | None = None,
    ) -> list[dict]:
        """Fetch one board's historical members, optionally at an intraday minute."""
        cache_key = (trade_date, plate_id)
        if end_hhmm is None:
            with self._sector_constituents_lock:
                cached = self._sector_constituents_cache.get(cache_key)
            if cached is not None:
                return [dict(row) for row in cached]
            persisted = await asyncio.to_thread(
                read_sector_constituents,
                self.data_dir,
                trade_date,
                plate_id,
            )
            if persisted:
                with self._sector_constituents_lock:
                    self._sector_constituents_cache[cache_key] = [dict(row) for row in persisted]
                return persisted
        rows = []
        seen_codes: set[str] = set()
        async with self._client_factory() as client:
            for offset in range(0, _MAX_PAGES * _REFERENCE_PAGE_SIZE, _REFERENCE_PAGE_SIZE):
                params: dict[str, object] = {
                    "PlateID": plate_id,
                    "Date": trade_date.isoformat(),
                    "Index": offset,
                    "st": _REFERENCE_PAGE_SIZE,
                }
                if end_hhmm is not None:
                    params.update({"RStart": "0925", "REnd": end_hhmm, "Type": "1"})
                payload = await client.request(
                    "sector_constituents",
                    params,
                )
                parsed = parse_sector_constituents(payload, plate_id)
                fresh = [row for row in parsed if row["code"] not in seen_codes]
                if not fresh:
                    break
                seen_codes.update(row["code"] for row in fresh)
                rows.extend(fresh)
                if len(parsed) < _REFERENCE_PAGE_SIZE:
                    break
            else:
                raise RuntimeError("开盘啦板块成分分页超过安全上限")
        if end_hhmm is None:
            if rows:
                self._write_records(
                    SECTOR_CONSTITUENT_TABLE,
                    [
                        {**row, "report_date": trade_date.isoformat()}
                        for row in rows
                    ],
                    ("plate_id", "symbol"),
                )
            with self._sector_constituents_lock:
                if len(self._sector_constituents_cache) >= 128:
                    self._sector_constituents_cache.pop(next(iter(self._sector_constituents_cache)))
                self._sector_constituents_cache[cache_key] = [dict(row) for row in rows]
        return rows

    async def refresh_market_sentiment(self, trade_date: date) -> int:
        """Poll today's live sentiment endpoints without substituting an old close."""
        selected = None
        ladder: dict[str, Any] = {}
        async with self._client_factory() as client:
            try:
                payload = await client.request(
                    "limit_up_expression",
                    {"Day": trade_date.isoformat()},
                )
                selected = parse_limit_up_expression(payload, trade_date)
            except Exception:  # noqa: BLE001
                logger.debug("实时情绪表达接口暂不可用", exc_info=True)
            try:
                ladder_payload = await client.request("limit_up_ladder")
                ladder = parse_limit_up_ladder_height(ladder_payload)
            except Exception:  # noqa: BLE001
                logger.debug("实时连板高度接口暂不可用", exc_info=True)

            if selected is None:
                with self._sentiment_lock:
                    self._market_sentiment = {
                        "provider": "kaipanla",
                        "state": "unavailable",
                        "as_of": trade_date.isoformat(),
                        "max_consecutive": None,
                        "refreshed_at": cn_now().isoformat(),
                    }
                return 0

        selected["max_consecutive"] = (
            ladder.get("max_consecutive")
            if ladder.get("as_of") == selected["as_of"]
            else None
        )
        selected.update({
            "provider": "kaipanla",
            "state": "live",
            "refreshed_at": cn_now().isoformat(),
        })
        with self._sentiment_lock:
            self._market_sentiment = selected
        return 1

    async def _scheduled_northbound(self) -> int:
        return await self._run_safely("northbound", self.collect_northbound)

    async def _scheduled_shareholder_counts(self) -> int:
        return await self._run_safely(
            "shareholder_counts", self.collect_shareholder_counts, cn_today(), cn_today()
        )

    async def _scheduled_sector_constituents(self) -> int:
        today = cn_today()
        trade_date = self.latest_completed_trading_date(today)
        if trade_date is None:
            logger.warning("开盘啦板块成分采集缺少上一完整交易日")
            return 0
        manifest = load_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            SECTOR_CONSTITUENT_TABLE,
            trade_date.isoformat(),
        )
        expected = [str(value) for value in manifest.get("expected_batches") or []]
        batches = manifest.get("batches") or {}
        if expected and not manifest.get("failed_batches") and all(
            isinstance(batches.get(plate_id), dict)
            and batches[plate_id].get("status") in {"completed", "valid_empty"}
            for plate_id in expected
        ):
            return int(manifest.get("published_rows") or 0)
        return await self._run_safely(
            "sector_constituents",
            self.collect_sector_constituents,
            trade_date,
            None,
        )

    def _stock_codes(self, trade_date: date) -> list[str]:
        path = self.data_dir / "instruments" / "instruments.parquet"
        if not path.exists():
            return []
        try:
            import polars as pl

            available = set(pl.read_parquet_schema(path))
            if not {"code", "type"}.issubset(available):
                logger.warning("开盘啦资金池缺少 code/type 列")
                return []
            lifecycle_columns = [
                column for column in ("list_date", "delist_date") if column in available
            ]
            frame = pl.read_parquet(path, columns=["code", "type", *lifecycle_columns])
            frame = frame.filter(pl.col("type") == "stock")
            if "list_date" in lifecycle_columns:
                frame = frame.filter(
                    pl.col("list_date").is_null() | (pl.col("list_date") <= trade_date)
                )
            if "delist_date" in lifecycle_columns:
                frame = frame.filter(
                    pl.col("delist_date").is_null() | (pl.col("delist_date") > trade_date)
                )
            return sorted({str(code) for code in frame["code"].to_list() if code})
        except Exception as exc:  # noqa: BLE001
            logger.warning("开盘啦资金池读取失败 (%s)", type(exc).__name__)
            return []

    async def collect_funds(self, trade_date: date) -> int:
        """盘后采集全市场资金排名，并补全逐股大单日频快照。"""
        collected_at = cn_now().isoformat()
        interval_rows: list[dict] = []
        interval_codes: set[str] = set()
        completed_pages = 0
        async with self._client_factory() as client:
            for offset in range(0, _MAX_PAGES * _FUND_INTERVAL_PAGE_SIZE, _FUND_INTERVAL_PAGE_SIZE):
                batch_id = f"offset-{offset:06d}"
                try:
                    payload = await client.request(
                        "fund_interval",
                        {
                            "DStart": trade_date.strftime("%Y-%m-%d"),
                            "DEnd": trade_date.strftime("%Y-%m-%d"),
                            "Index": offset,
                            "st": _FUND_INTERVAL_PAGE_SIZE,
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "fund_interval",
                        trade_date.isoformat(),
                        batch_id,
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        page_size=_FUND_INTERVAL_PAGE_SIZE,
                    )
                    raise
                archive_raw(self.data_dir, "fund_interval", trade_date, payload, f"offset-{offset}")
                try:
                    parsed = parse_interval_stock(payload)
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "fund_interval",
                        trade_date.isoformat(),
                        batch_id,
                        status="parse_rejected",
                        error_code=type(exc).__name__,
                        source_content_hash=stable_content_hash(payload),
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        page_size=_FUND_INTERVAL_PAGE_SIZE,
                    )
                    raise
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    "fund_interval",
                    trade_date.isoformat(),
                    batch_id,
                    status="completed" if parsed else "valid_empty",
                    row_count=len(parsed),
                    content_hash=stable_content_hash(parsed),
                    source_content_hash=stable_content_hash(payload),
                    empty_reason=None if parsed else "valid_empty",
                    parser_version="kaipanla_v1",
                    schema_version=1,
                    page_size=_FUND_INTERVAL_PAGE_SIZE,
                )
                completed_pages += 1
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
                update_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    "fund_interval",
                    trade_date.isoformat(),
                    status="page_limit_reached",
                    error_code="page_limit_reached",
                    published_rows=0,
                )
                raise RuntimeError("开盘啦资金排名分页超过安全上限")
            update_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                "fund_interval",
                trade_date.isoformat(),
                status="complete" if interval_rows else "valid_empty",
                completed_pages=completed_pages,
                published_rows=len(interval_rows),
                empty_reason=None if interval_rows else "valid_empty",
                error_code=None,
                parser_version="kaipanla_v1",
                schema_version=1,
            )

            codes = self._stock_codes(trade_date)
            semaphore = asyncio.Semaphore(16)
            capital_failures: set[str] = set()
            statistics_failures: set[str] = set()

            async def collect_one(
                code: str,
                *,
                collect_capital: bool = True,
                collect_statistics: bool = True,
            ) -> tuple[dict | None, dict | None]:
                async with semaphore:
                    capital_row: dict | None = None
                    statistics_row: dict | None = None
                    if collect_capital:
                        try:
                            capital = await client.request(
                                "fund_capital_net",
                                {"StockID": code, "Date": trade_date.strftime("%Y-%m-%d")},
                            )
                            archive_raw(
                                self.data_dir, "fund_capital_net", trade_date, capital, code
                            )
                            capital_row = parse_capital_net(capital, code)
                            capital_failures.discard(code)
                            record_ingestion_batch(
                                self.data_dir,
                                "kaipanla",
                                "fund_capital_net",
                                trade_date.isoformat(),
                                code,
                                status="completed",
                                row_count=1,
                                content_hash=stable_content_hash(capital_row),
                                source_content_hash=stable_content_hash(capital),
                                parser_version="kaipanla_v1",
                                schema_version=1,
                                expected_batches=codes,
                            )
                        except Exception as exc:  # noqa: BLE001
                            capital_failures.add(code)
                            record_ingestion_batch(
                                self.data_dir,
                                "kaipanla",
                                "fund_capital_net",
                                trade_date.isoformat(),
                                code,
                                status="source_error",
                                error_code=type(exc).__name__,
                                parser_version="kaipanla_v1",
                                schema_version=1,
                                expected_batches=codes,
                            )
                            logger.warning("开盘啦分时大单采集失败 (%s)", type(exc).__name__)
                    if collect_statistics:
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
                            statistics_row = parse_large_order_statistics(
                                stats, code, trade_date
                            )
                            statistics_failures.discard(code)
                            record_ingestion_batch(
                                self.data_dir,
                                "kaipanla",
                                "fund_large_order_statistics",
                                trade_date.isoformat(),
                                code,
                                status="completed" if statistics_row else "valid_empty",
                                row_count=1 if statistics_row else 0,
                                content_hash=(
                                    stable_content_hash(statistics_row)
                                    if statistics_row
                                    else None
                                ),
                                source_content_hash=stable_content_hash(stats),
                                empty_reason=None if statistics_row else "valid_empty",
                                parser_version="kaipanla_v1",
                                schema_version=1,
                                expected_batches=codes,
                            )
                        except Exception as exc:  # noqa: BLE001
                            statistics_failures.add(code)
                            record_ingestion_batch(
                                self.data_dir,
                                "kaipanla",
                                "fund_large_order_statistics",
                                trade_date.isoformat(),
                                code,
                                status="source_error",
                                error_code=type(exc).__name__,
                                parser_version="kaipanla_v1",
                                schema_version=1,
                                expected_batches=codes,
                            )
                            logger.warning("开盘啦日度大单采集失败 (%s)", type(exc).__name__)
                    return capital_row, statistics_row

            details = list(await asyncio.gather(*(collect_one(code) for code in codes)))
            retry_codes = sorted(capital_failures | statistics_failures)
            if retry_codes:
                semaphore = asyncio.Semaphore(4)
                details.extend(await asyncio.gather(*(
                    collect_one(
                        code,
                        collect_capital=code in capital_failures,
                        collect_statistics=code in statistics_failures,
                    )
                    for code in retry_codes
                )))

        capital_rows = [row for row, _ in details if row] if not capital_failures else []
        statistics_rows = [row for _, row in details if row] if not statistics_failures else []
        for dataset, failures, rows in (
            ("fund_capital_net", capital_failures, capital_rows),
            ("fund_large_order_statistics", statistics_failures, statistics_rows),
        ):
            current_batches = (
                load_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    dataset,
                    trade_date.isoformat(),
                ).get("batches")
                or {}
            )
            update_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                dataset,
                trade_date.isoformat(),
                status="incomplete" if failures else "complete",
                expected_batches=codes,
                failed_batches=sorted(failures),
                published_rows=len(rows),
                batches={code: current_batches[code] for code in codes},
                parser_version="kaipanla_v1",
                schema_version=1,
            )
        merged_details: dict[str, dict] = {}
        for row in capital_rows + statistics_rows:
            code = str(row.get("code") or row.get("symbol") or "")
            if code:
                merged_details.setdefault(code, {}).update(row)
        count = atomic_upsert(
            self.data_dir,
            FUNDS_TABLE,
            trade_date,
            [
                {**row, "collected_at": collected_at}
                for row in interval_rows + list(merged_details.values())
            ],
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
        logical_snapshot = (report_date or cn_today()).isoformat()
        sector_rows: list[dict] = []
        seen_plates: set[str] = set()
        async with self._client_factory() as client:
            for offset in range(0, _MAX_PAGES * 20, 20):
                batch_id = f"offset-{offset:06d}"
                params = {"Index": offset, "st": 20}
                if report_date:
                    params["Date"] = report_date.isoformat()
                try:
                    payload = await client.request(sector_endpoint, params)
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        NORTHBOUND_SECTOR_TABLE,
                        logical_snapshot,
                        batch_id,
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        endpoint=sector_endpoint,
                        page_size=20,
                    )
                    raise
                archive_raw(self.data_dir, sector_endpoint, report_date or cn_today(), payload, f"offset-{offset}")
                try:
                    _, parsed = parse_northbound_sector(payload)
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        NORTHBOUND_SECTOR_TABLE,
                        logical_snapshot,
                        batch_id,
                        status="parse_rejected",
                        error_code=type(exc).__name__,
                        source_content_hash=stable_content_hash(payload),
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        endpoint=sector_endpoint,
                        page_size=20,
                    )
                    raise
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    NORTHBOUND_SECTOR_TABLE,
                    logical_snapshot,
                    batch_id,
                    status="completed" if parsed else "valid_empty",
                    row_count=len(parsed),
                    content_hash=stable_content_hash(parsed),
                    source_content_hash=stable_content_hash(payload),
                    empty_reason=None if parsed else "valid_empty",
                    parser_version="kaipanla_v1",
                    schema_version=1,
                    endpoint=sector_endpoint,
                    page_size=20,
                )
                fresh = [row for row in parsed if row["plate_id"] not in seen_plates]
                if not fresh:
                    break
                seen_plates.update(row["plate_id"] for row in fresh)
                sector_rows.extend(fresh)
                if len(parsed) < 20:
                    break
            else:
                update_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    NORTHBOUND_SECTOR_TABLE,
                    logical_snapshot,
                    status="page_limit_reached",
                    error_code="page_limit_reached",
                    published_rows=0,
                )
                raise RuntimeError("开盘啦北向板块分页超过安全上限")
            update_ingestion_manifest(
                self.data_dir,
                "kaipanla",
                NORTHBOUND_SECTOR_TABLE,
                logical_snapshot,
                status="complete" if sector_rows else "valid_empty",
                published_rows=len(sector_rows),
                empty_reason=None if sector_rows else "valid_empty",
                error_code=None,
                parser_version="kaipanla_v1",
                schema_version=1,
                endpoint=sector_endpoint,
            )

            semaphore = asyncio.Semaphore(8)
            failed_plates: set[str] = set()

            async def collect_plate(plate_id: str) -> list[dict]:
                async with semaphore:
                    try:
                        params = {"IndexID": plate_id, "Index": 0, "st": _REFERENCE_PAGE_SIZE}
                        if report_date:
                            params["Date"] = report_date.isoformat()
                        payload = await client.request(stock_endpoint, params)
                        archive_raw(self.data_dir, stock_endpoint, report_date or cn_today(), payload, plate_id)
                        _, parsed = parse_northbound_stocks(payload, plate_id)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            NORTHBOUND_STOCK_TABLE,
                            (report_date or cn_today()).isoformat(),
                            plate_id,
                            status="completed" if parsed else "valid_empty",
                            row_count=len(parsed),
                            content_hash=stable_content_hash(parsed),
                            source_content_hash=stable_content_hash(payload),
                            empty_reason=None if parsed else "valid_empty",
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=sorted(seen_plates),
                        )
                        return parsed
                    except Exception as exc:  # noqa: BLE001
                        failed_plates.add(plate_id)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            NORTHBOUND_STOCK_TABLE,
                            (report_date or cn_today()).isoformat(),
                            plate_id,
                            status="source_error",
                            error_code=type(exc).__name__,
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=sorted(seen_plates),
                        )
                        logger.warning("开盘啦北向个股采集失败 (%s)", type(exc).__name__)
                        return []

            stock_rows = [
                row
                for rows in await asyncio.gather(*(collect_plate(code) for code in sorted(seen_plates)))
                for row in rows
            ]
        stock_rows_to_publish = [] if failed_plates else stock_rows
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            NORTHBOUND_STOCK_TABLE,
            (report_date or cn_today()).isoformat(),
            status="incomplete" if failed_plates else "complete",
            expected_batches=sorted(seen_plates),
            failed_batches=sorted(failed_plates),
            published_rows=len(stock_rows_to_publish),
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return self._write_records(NORTHBOUND_SECTOR_TABLE, sector_rows, ("plate_id",)) + self._write_records(
            NORTHBOUND_STOCK_TABLE, stock_rows_to_publish, ("plate_id", "symbol")
        )

    async def collect_shareholder_counts(self, start_date: date, end_date: date) -> int:
        """采集指定统计区间的股东人数变更，日期取上游每行 Day。"""
        rows: list[dict] = []
        logical_snapshot = end_date.isoformat()
        async with self._client_factory() as client:
            try:
                window_payload = await client.request(
                    "shareholder_count_changes",
                    {
                        "StratDate": start_date.isoformat(),
                        "EndDate": end_date.isoformat(),
                        "Index": 0,
                        "st": _REFERENCE_PAGE_SIZE,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    "shareholder_count_windows",
                    logical_snapshot,
                    "windows",
                    status="source_error",
                    error_code=type(exc).__name__,
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
                raise
            archive_raw(self.data_dir, "shareholder_count_changes", end_date, window_payload, "windows")
            try:
                windows = _shareholder_count_windows(window_payload)
            except Exception as exc:  # noqa: BLE001
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    "shareholder_count_windows",
                    logical_snapshot,
                    "windows",
                    status="parse_rejected",
                    error_code=type(exc).__name__,
                    source_content_hash=stable_content_hash(window_payload),
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
                raise
            record_ingestion_batch(
                self.data_dir,
                "kaipanla",
                "shareholder_count_windows",
                logical_snapshot,
                "windows",
                status="completed" if windows else "valid_empty",
                row_count=len(windows),
                content_hash=stable_content_hash(windows),
                source_content_hash=stable_content_hash(window_payload),
                empty_reason=None if windows else "valid_empty",
                parser_version="kaipanla_v1",
                schema_version=1,
            )
            for window_start, window_end in windows:
                for offset in range(0, _MAX_PAGES * _REFERENCE_PAGE_SIZE, _REFERENCE_PAGE_SIZE):
                    batch_id = (
                        f"{window_start.isoformat()}_{window_end.isoformat()}_"
                        f"offset-{offset:06d}"
                    )
                    try:
                        payload = await client.request(
                            "shareholder_count_changes",
                            {
                                "StratDate": window_start.isoformat(),
                                "EndDate": window_end.isoformat(),
                                "Index": offset,
                                "st": _REFERENCE_PAGE_SIZE,
                            },
                        )
                    except Exception as exc:  # noqa: BLE001
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SHAREHOLDER_COUNT_TABLE,
                            logical_snapshot,
                            batch_id,
                            status="source_error",
                            error_code=type(exc).__name__,
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            page_size=_REFERENCE_PAGE_SIZE,
                        )
                        raise
                    archive_raw(
                        self.data_dir,
                        "shareholder_count_changes",
                        window_end,
                        payload,
                        f"offset-{offset}",
                    )
                    try:
                        parsed = parse_shareholder_count_changes(payload)
                    except Exception as exc:  # noqa: BLE001
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SHAREHOLDER_COUNT_TABLE,
                            logical_snapshot,
                            batch_id,
                            status="parse_rejected",
                            error_code=type(exc).__name__,
                            source_content_hash=stable_content_hash(payload),
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            page_size=_REFERENCE_PAGE_SIZE,
                        )
                        raise
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        SHAREHOLDER_COUNT_TABLE,
                        logical_snapshot,
                        batch_id,
                        status="completed" if parsed else "valid_empty",
                        row_count=len(parsed),
                        content_hash=stable_content_hash(parsed),
                        source_content_hash=stable_content_hash(payload),
                        empty_reason=None if parsed else "valid_empty",
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        page_size=_REFERENCE_PAGE_SIZE,
                    )
                    rows.extend(parsed)
                    if len(parsed) < _REFERENCE_PAGE_SIZE:
                        break
                else:
                    update_ingestion_manifest(
                        self.data_dir,
                        "kaipanla",
                        SHAREHOLDER_COUNT_TABLE,
                        logical_snapshot,
                        status="page_limit_reached",
                        error_code="page_limit_reached",
                        published_rows=0,
                    )
                    raise RuntimeError("开盘啦股东人数分页超过安全上限")
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            SHAREHOLDER_COUNT_TABLE,
            logical_snapshot,
            status="complete" if rows else "valid_empty",
            published_rows=len(rows),
            empty_reason=None if rows else "valid_empty",
            error_code=None,
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return self._write_records(SHAREHOLDER_COUNT_TABLE, rows, ("symbol",))

    async def collect_shareholder_changes(self, report_date: date) -> int:
        """按报告期采集全 A 股十大流通股东。"""
        semaphore = asyncio.Semaphore(8)
        codes = self._stock_codes(report_date)
        failed_codes: set[str] = set()

        async with self._client_factory() as client:
            async def collect_one(code: str) -> list[dict]:
                async with semaphore:
                    try:
                        payload = await client.request(
                            "shareholder_changes", {"StockID": code, "Day": report_date.isoformat()}
                        )
                        archive_raw(self.data_dir, "shareholder_changes", report_date, payload, code)
                        rows = parse_shareholder_changes(payload, code, report_date)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SHAREHOLDER_TABLE,
                            report_date.isoformat(),
                            code,
                            status="completed" if rows else "valid_empty",
                            row_count=len(rows),
                            content_hash=stable_content_hash(rows),
                            source_content_hash=stable_content_hash(payload),
                            empty_reason=None if rows else "valid_empty",
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=codes,
                        )
                        return rows
                    except Exception as exc:  # noqa: BLE001
                        failed_codes.add(code)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SHAREHOLDER_TABLE,
                            report_date.isoformat(),
                            code,
                            status="source_error",
                            error_code=type(exc).__name__,
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=codes,
                        )
                        logger.warning("开盘啦十大流通股东采集失败 (%s)", type(exc).__name__)
                        return []

            rows = [
                row
                for values in await asyncio.gather(*(collect_one(code) for code in codes))
                for row in values
            ]
        rows_to_publish = [] if failed_codes else rows
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            SHAREHOLDER_TABLE,
            report_date.isoformat(),
            status="incomplete" if failed_codes else "complete",
            expected_batches=codes,
            failed_batches=sorted(failed_codes),
            published_rows=len(rows_to_publish),
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return self._write_records(
            SHAREHOLDER_TABLE,
            rows_to_publish,
            ("symbol", "snapshot_kind", "shareholder_id"),
        )

    async def collect_sector_constituents(
        self, trade_date: date, plate_ids: list[str] | None = None
    ) -> int:
        """由板块强度榜发现板块，再抓取目标日完整历史成分。"""
        async with self._client_factory() as client:
            if plate_ids is None:
                try:
                    payload = await client.request(
                        "sector_strength",
                        {
                            "Day": trade_date.isoformat(),
                            "Index": 0,
                            "st": _REFERENCE_PAGE_SIZE,
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "sector_strength_discovery",
                        trade_date.isoformat(),
                        "page-000",
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                    )
                    raise
                archive_raw(self.data_dir, "sector_strength", trade_date, payload)
                try:
                    strength_rows = parse_sector_strength(payload)
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "sector_strength_discovery",
                        trade_date.isoformat(),
                        "page-000",
                        status="parse_rejected",
                        error_code=type(exc).__name__,
                        source_content_hash=stable_content_hash(payload),
                        parser_version="kaipanla_v1",
                        schema_version=1,
                    )
                    raise
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    "sector_strength_discovery",
                    trade_date.isoformat(),
                    "page-000",
                    status="completed" if strength_rows else "valid_empty",
                    row_count=len(strength_rows),
                    content_hash=stable_content_hash(strength_rows),
                    source_content_hash=stable_content_hash(payload),
                    empty_reason=None if strength_rows else "valid_empty",
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
                update_ingestion_manifest(
                    self.data_dir,
                    "kaipanla",
                    "sector_strength_discovery",
                    trade_date.isoformat(),
                    status="complete" if strength_rows else "valid_empty",
                    published_rows=len(strength_rows),
                    empty_reason=None if strength_rows else "valid_empty",
                    error_code=None,
                )
                plate_ids = [row["plate_id"] for row in strength_rows]
            requested_plates = sorted(set(plate_ids))
            failed_plates: set[str] = set()
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
                        else:
                            raise RuntimeError("开盘啦板块成分分页超过安全上限")
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SECTOR_CONSTITUENT_TABLE,
                            trade_date.isoformat(),
                            plate_id,
                            status="completed" if rows else "valid_empty",
                            row_count=len(rows),
                            content_hash=stable_content_hash(rows),
                            empty_reason=None if rows else "valid_empty",
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=requested_plates,
                        )
                        return rows
                    except Exception as exc:  # noqa: BLE001
                        failed_plates.add(plate_id)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            SECTOR_CONSTITUENT_TABLE,
                            trade_date.isoformat(),
                            plate_id,
                            status="source_error",
                            error_code=type(exc).__name__,
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=requested_plates,
                        )
                        logger.warning("开盘啦板块成分采集失败 (%s)", type(exc).__name__)
                        return []

            rows = [
                row
                for values in await asyncio.gather(*(collect_one(plate_id) for plate_id in requested_plates))
                for row in values
            ]
        stamped = (
            []
            if failed_plates
            else [{**row, "report_date": trade_date.isoformat()} for row in rows]
        )
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            SECTOR_CONSTITUENT_TABLE,
            trade_date.isoformat(),
            status="incomplete" if failed_plates else "complete",
            expected_batches=requested_plates,
            failed_batches=sorted(failed_plates),
            published_rows=len(stamped),
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return self._write_records(SECTOR_CONSTITUENT_TABLE, stamped, ("plate_id", "symbol"))

    async def collect_lhb_reference(self, trade_date: date, codes: list[str]) -> int:
        """补充龙虎榜游资动向和股票席位明细。"""
        requested_codes = sorted(set(codes))
        movement_complete = False
        failed_codes: set[str] = set()
        async with self._client_factory() as client:
            movement_rows: list[dict] = []
            try:
                movement_payload = await client.request(
                    "dragon_tiger_movement", {"Date": trade_date.isoformat()}
                )
                archive_raw(self.data_dir, "dragon_tiger_movement", trade_date, movement_payload)
                movement_rows = parse_dragon_tiger_movement(movement_payload, trade_date)
                movement_complete = True
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    LHB_MOVEMENT_TABLE,
                    trade_date.isoformat(),
                    "movement",
                    status="completed" if movement_rows else "valid_empty",
                    row_count=len(movement_rows),
                    content_hash=stable_content_hash(movement_rows),
                    source_content_hash=stable_content_hash(movement_payload),
                    empty_reason=None if movement_rows else "valid_empty",
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
            except Exception as exc:  # noqa: BLE001
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    LHB_MOVEMENT_TABLE,
                    trade_date.isoformat(),
                    "movement",
                    status="source_error",
                    error_code=type(exc).__name__,
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
                logger.warning("开盘啦龙虎榜游资动向采集失败 (%s)", type(exc).__name__)
            semaphore = asyncio.Semaphore(8)

            async def collect_one(code: str) -> list[dict]:
                async with semaphore:
                    try:
                        payload = await client.request(
                            "dragon_tiger_details", {"StockID": code, "Time": trade_date.isoformat()}
                        )
                        archive_raw(self.data_dir, "dragon_tiger_details", trade_date, payload, code)
                        rows = parse_dragon_tiger_details(payload, code)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            LHB_DETAIL_TABLE,
                            trade_date.isoformat(),
                            code,
                            status="completed" if rows else "valid_empty",
                            row_count=len(rows),
                            content_hash=stable_content_hash(rows),
                            source_content_hash=stable_content_hash(payload),
                            empty_reason=None if rows else "valid_empty",
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=requested_codes,
                        )
                        return rows
                    except Exception as exc:  # noqa: BLE001
                        failed_codes.add(code)
                        record_ingestion_batch(
                            self.data_dir,
                            "kaipanla",
                            LHB_DETAIL_TABLE,
                            trade_date.isoformat(),
                            code,
                            status="source_error",
                            error_code=type(exc).__name__,
                            parser_version="kaipanla_v1",
                            schema_version=1,
                            expected_batches=requested_codes,
                        )
                        logger.warning("开盘啦龙虎榜席位采集失败 (%s)", type(exc).__name__)
                        return []

            detail_rows = [
                row
                for values in await asyncio.gather(*(collect_one(code) for code in requested_codes))
                for row in values
            ]
        movement = (
            [{**row, "report_date": trade_date.isoformat()} for row in movement_rows]
            if movement_complete
            else []
        )
        details = (
            []
            if failed_codes
            else [{**row, "report_date": trade_date.isoformat()} for row in detail_rows]
        )
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            LHB_MOVEMENT_TABLE,
            trade_date.isoformat(),
            status="complete" if movement_complete else "incomplete",
            failed_batches=[] if movement_complete else ["movement"],
            published_rows=len(movement),
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            LHB_DETAIL_TABLE,
            trade_date.isoformat(),
            status="incomplete" if failed_codes else "complete",
            expected_batches=requested_codes,
            failed_batches=sorted(failed_codes),
            published_rows=len(details),
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return self._write_records(
            LHB_MOVEMENT_TABLE, movement, ("participant_id", "side", "symbol")
        ) + self._write_records(LHB_DETAIL_TABLE, details, ("symbol", "side", "log_id"))

    async def _fetch_pages(
        self,
        client: KaipanlaClient,
        endpoint: int,
        trade_date: date,
        params: dict[str, object] | None = None,
        *,
        archive_context: str | None = None,
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
            context = f"{archive_context}-page-{index}" if archive_context else f"page-{index}"
            archive_raw(self.data_dir, endpoint, trade_date, payload, context)
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
                    error_code=None,
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
            pages = await self._fetch_pages(
                client,
                endpoint,
                trade_date,
                params,
                archive_context=checkpoint,
            )
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
            try:
                payload = await client.request(15, {"Index": 0, "st": 1000})
            except Exception as exc:  # noqa: BLE001
                record_ingestion_batch(
                    self.data_dir,
                    "kaipanla",
                    "endpoint_15",
                    trade_date.isoformat(),
                    "page-000",
                    status="source_error",
                    error_code=type(exc).__name__,
                    parser_version="kaipanla_v1",
                    schema_version=1,
                )
                raise
        archive_raw(self.data_dir, 15, trade_date, payload)
        collected_at = cn_now().isoformat()
        try:
            parsed = parse_limitup(payload)
        except Exception as exc:  # noqa: BLE001
            record_ingestion_batch(
                self.data_dir,
                "kaipanla",
                "endpoint_15",
                trade_date.isoformat(),
                "page-000",
                status="parse_rejected",
                error_code=type(exc).__name__,
                source_content_hash=stable_content_hash(payload),
                parser_version="kaipanla_v1",
                schema_version=1,
            )
            raise
        record_ingestion_batch(
            self.data_dir,
            "kaipanla",
            "endpoint_15",
            trade_date.isoformat(),
            "page-000",
            status="completed" if parsed else "valid_empty",
            row_count=len(parsed),
            content_hash=stable_content_hash(parsed),
            source_content_hash=stable_content_hash(payload),
            empty_reason=None if parsed else "valid_empty",
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            "endpoint_15",
            trade_date.isoformat(),
            status="complete" if parsed else "valid_empty",
            published_rows=len(parsed),
            empty_reason=None if parsed else "valid_empty",
            error_code=None,
        )
        rows = [{**row, "collected_at": collected_at} for row in parsed]
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
        failed_codes: set[str] = set()

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
                    row = {
                        **parse_lhb_detail(payload, code),
                        "detail_collected_at": cn_now().isoformat(),
                    }
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        "lhb_seat_detail",
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
                        "lhb_seat_detail",
                        trade_date.isoformat(),
                        code,
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                        expected_batches=unique_codes,
                    )
                    logger.warning("开盘啦 /101 个股 %s 采集失败 (%s)", code, type(exc).__name__)
                    return None

        details = [
            row for row in await asyncio.gather(*(collect_one(c) for c in unique_codes)) if row
        ]
        details_to_publish = [] if failed_codes else details
        count = atomic_upsert(self.data_dir, LHB_TABLE, trade_date, details_to_publish)
        update_ingestion_manifest(
            self.data_dir,
            "kaipanla",
            "lhb_seat_detail",
            trade_date.isoformat(),
            status="incomplete" if failed_codes else "complete",
            expected_batches=unique_codes,
            failed_batches=sorted(failed_codes),
            published_rows=count,
            parser_version="kaipanla_v1",
            schema_version=1,
        )
        return count

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
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        f"regulatory_{endpoint}_{snapshot}",
                        trade_date.isoformat(),
                        str(endpoint),
                        status="completed" if parsed else "valid_empty",
                        row_count=len(parsed),
                        content_hash=stable_content_hash(parsed),
                        source_content_hash=stable_content_hash(payload),
                        empty_reason=None if parsed else "valid_empty",
                        parser_version="kaipanla_v1",
                        schema_version=1,
                    )
                    successes += 1
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "kaipanla",
                        f"regulatory_{endpoint}_{snapshot}",
                        trade_date.isoformat(),
                        str(endpoint),
                        status="source_error",
                        error_code=type(exc).__name__,
                        parser_version="kaipanla_v1",
                        schema_version=1,
                    )
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
