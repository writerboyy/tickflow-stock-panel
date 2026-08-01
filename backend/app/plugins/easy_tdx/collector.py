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
from app.plugins.easy_tdx.client import (
    fetch_dividend_history_rows,
    fetch_f10_texts,
    fetch_industry_rows,
    parse_f10_reference,
)
from app.plugins.easy_tdx.storage import (
    DIVIDEND_HISTORY_TABLE,
    EXPRESS_TABLE,
    FORECAST_TABLE,
    INDUSTRY_TABLE,
    MARGIN_TABLE,
    ensure_config,
    replace_industry_snapshot,
    snapshot_is_fresh,
    upsert_records,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    load_ingestion_manifest,
    read_staging_rows,
    record_ingestion_batch,
    stable_content_hash,
    update_ingestion_manifest,
    write_staging_rows,
)


logger = logging.getLogger(__name__)

_F10_BATCH_SIZE = 50
_F10_PARSER_VERSION = "easy_tdx_f10_v2"
_F10_DATASETS = {
    MARGIN_TABLE: ("symbol",),
    FORECAST_TABLE: ("symbol", "announcement_date"),
    EXPRESS_TABLE: ("symbol", "announcement_date"),
    DIVIDEND_HISTORY_TABLE: ("symbol", "record_date", "plan"),
}


class EasyTdxCollector:
    def __init__(
        self,
        data_dir: Path,
        fetcher: Callable[[], list[dict]] = fetch_industry_rows,
        f10_fetcher: Callable[[list[str]], list[tuple[str, str]]] = fetch_f10_texts,
        dividend_fetcher: Callable[[list[str]], list[dict]] = fetch_dividend_history_rows,
        availability_check: Callable[[], tuple[bool, str]] = availability,
    ) -> None:
        self.data_dir = Path(data_dir)
        self._fetcher = fetcher
        self._f10_fetcher = f10_fetcher
        self._dividend_fetcher = dividend_fetcher
        self._availability_check = availability_check
        self._bootstrap_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    def start(self, scheduler, *, bootstrap: bool = True) -> None:
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
        if bootstrap:
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
        logical_snapshot = cn_now().date().isoformat()
        _, source_hash = archive_source_payload(
            self.data_dir,
            "easy_tdx",
            INDUSTRY_TABLE,
            logical_snapshot,
            "full",
            rows,
            parser_version="easy_tdx_industry_v1",
        )
        count = replace_industry_snapshot(
            self.data_dir,
            [
                {**row, "source": "easy_tdx", "collected_at": collected_at}
                for row in rows
            ],
        )
        if count == 0:
            raise RuntimeError("EasyTDX 行业快照为空")
        update_ingestion_manifest(
            self.data_dir,
            "easy_tdx",
            INDUSTRY_TABLE,
            logical_snapshot,
            status="published",
            parser_version="easy_tdx_industry_v1",
            schema_version=1,
            source_content_hash=source_hash,
            published_rows=count,
            empty_reason=None,
        )
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

    async def _fetch_with_retry(self, fetcher, codes: list[str]) -> tuple[list, int]:
        for retry_count in range(3):
            try:
                return await asyncio.to_thread(fetcher, codes), retry_count
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                if retry_count == 2:
                    raise
                await asyncio.sleep(0.05 * (retry_count + 1))
        raise RuntimeError("unreachable EasyTDX retry state")

    @staticmethod
    def _batch_completed(state: dict, batch_id: str) -> bool:
        batch = (state.get("batches") or {}).get(batch_id) or {}
        return batch.get("status") in {"completed", "valid_empty"}

    async def collect_f10(self, codes: list[str] | None = None) -> int:
        requested = sorted({str(code).zfill(6) for code in (codes if codes is not None else self._stock_codes())})
        if not requested:
            raise RuntimeError("EasyTDX F10 标的列表为空")

        logical_snapshot = cn_now().date().isoformat()
        batches = [
            requested[offset:offset + _F10_BATCH_SIZE]
            for offset in range(0, len(requested), _F10_BATCH_SIZE)
        ]
        batch_ids = [f"{index:05d}" for index in range(len(batches))]
        input_hash = stable_content_hash(requested)
        staging_snapshot = f"{logical_snapshot}-{input_hash[:16]}"
        run_metadata = {
            "parser_version": _F10_PARSER_VERSION,
            "schema_version": 1,
            "input_hash": input_hash,
            "staging_snapshot": staging_snapshot,
            "expected_batches": batch_ids,
            "expected_symbols": len(requested),
        }
        for dataset in _F10_DATASETS:
            existing = load_ingestion_manifest(
                self.data_dir, "easy_tdx", dataset, logical_snapshot
            )
            same_input = existing.get("input_hash") == input_hash
            update_ingestion_manifest(
                self.data_dir,
                "easy_tdx",
                dataset,
                logical_snapshot,
                status=(
                    "published"
                    if same_input and existing.get("status") == "published"
                    else "collecting"
                ),
                batches=existing.get("batches", {}) if same_input else {},
                **run_metadata,
            )

        collected_at = cn_now().isoformat()
        for batch_id, batch_codes in zip(batch_ids, batches, strict=True):
            f10_states = {
                dataset: load_ingestion_manifest(
                    self.data_dir, "easy_tdx", dataset, logical_snapshot
                )
                for dataset in (MARGIN_TABLE, FORECAST_TABLE, EXPRESS_TABLE)
            }
            if not all(self._batch_completed(state, batch_id) for state in f10_states.values()):
                try:
                    texts, retry_count = await self._fetch_with_retry(self._f10_fetcher, batch_codes)
                    returned_codes = {str(code).zfill(6) for code, _text in texts}
                    missing_codes = sorted(set(batch_codes) - returned_codes)
                    if missing_codes:
                        sample = ", ".join(missing_codes[:8])
                        raise RuntimeError(
                            "EasyTDX F10 批次缺少标的响应，不能判为章节为空: " + sample
                        )
                    _, source_hash = archive_source_payload(
                        self.data_dir,
                        "easy_tdx",
                        "f10_text",
                        logical_snapshot,
                        batch_id,
                        [{"code": code, "text": text} for code, text in texts],
                        parser_version=_F10_PARSER_VERSION,
                    )
                    parsed_by_dataset: dict[str, list[dict]] = {
                        MARGIN_TABLE: [],
                        FORECAST_TABLE: [],
                        EXPRESS_TABLE: [],
                    }
                    for code, text in texts:
                        margin, forecast, express = parse_f10_reference(text, code)
                        parsed_by_dataset[MARGIN_TABLE].extend(margin)
                        parsed_by_dataset[FORECAST_TABLE].extend(forecast)
                        parsed_by_dataset[EXPRESS_TABLE].extend(express)
                    for dataset, rows in parsed_by_dataset.items():
                        staged = [{**row, "collected_at": collected_at} for row in rows]
                        write_staging_rows(
                            self.data_dir, "easy_tdx", dataset, staging_snapshot, batch_id, staged
                        )
                        empty_reason = "section_absent" if not staged else None
                        record_ingestion_batch(
                            self.data_dir,
                            "easy_tdx",
                            dataset,
                            logical_snapshot,
                            batch_id,
                            status="valid_empty" if not staged else "completed",
                            row_count=len(staged),
                            content_hash=stable_content_hash(staged),
                            source_content_hash=source_hash,
                            empty_reason=empty_reason,
                            retry_count=retry_count,
                            **run_metadata,
                        )
                except Exception as exc:  # noqa: BLE001
                    for dataset in (MARGIN_TABLE, FORECAST_TABLE, EXPRESS_TABLE):
                        record_ingestion_batch(
                            self.data_dir,
                            "easy_tdx",
                            dataset,
                            logical_snapshot,
                            batch_id,
                            status="source_error",
                            error_code=type(exc).__name__,
                            retry_count=2,
                            **run_metadata,
                        )

            dividend_state = load_ingestion_manifest(
                self.data_dir, "easy_tdx", DIVIDEND_HISTORY_TABLE, logical_snapshot
            )
            if not self._batch_completed(dividend_state, batch_id):
                try:
                    dividends, retry_count = await self._fetch_with_retry(
                        self._dividend_fetcher, batch_codes
                    )
                    _, source_hash = archive_source_payload(
                        self.data_dir,
                        "easy_tdx",
                        "dividend_rows",
                        logical_snapshot,
                        batch_id,
                        dividends,
                        parser_version=_F10_PARSER_VERSION,
                    )
                    staged = [{**row, "collected_at": collected_at} for row in dividends]
                    write_staging_rows(
                        self.data_dir,
                        "easy_tdx",
                        DIVIDEND_HISTORY_TABLE,
                        staging_snapshot,
                        batch_id,
                        staged,
                    )
                    record_ingestion_batch(
                        self.data_dir,
                        "easy_tdx",
                        DIVIDEND_HISTORY_TABLE,
                        logical_snapshot,
                        batch_id,
                        status="valid_empty" if not staged else "completed",
                        row_count=len(staged),
                        content_hash=stable_content_hash(staged),
                        source_content_hash=source_hash,
                        empty_reason="valid_empty" if not staged else None,
                        retry_count=retry_count,
                        **run_metadata,
                    )
                except Exception as exc:  # noqa: BLE001
                    record_ingestion_batch(
                        self.data_dir,
                        "easy_tdx",
                        DIVIDEND_HISTORY_TABLE,
                        logical_snapshot,
                        batch_id,
                        status="source_error",
                        error_code=type(exc).__name__,
                        retry_count=2,
                        **run_metadata,
                    )

        published = 0
        for dataset, key_fields in _F10_DATASETS.items():
            state = load_ingestion_manifest(
                self.data_dir, "easy_tdx", dataset, logical_snapshot
            )
            complete = all(self._batch_completed(state, batch_id) for batch_id in batch_ids)
            if not complete:
                update_ingestion_manifest(
                    self.data_dir,
                    "easy_tdx",
                    dataset,
                    logical_snapshot,
                    status="incomplete",
                )
                continue
            if state.get("status") == "published":
                continue
            rows = read_staging_rows(
                self.data_dir, "easy_tdx", dataset, staging_snapshot
            )
            count = upsert_records(self.data_dir, dataset, rows, key_fields)
            published += count
            update_ingestion_manifest(
                self.data_dir,
                "easy_tdx",
                dataset,
                logical_snapshot,
                status="published",
                published_rows=count,
                published_hash=stable_content_hash(rows),
                empty_reason=(
                    "section_absent" if dataset == EXPRESS_TABLE and not rows
                    else "valid_empty" if not rows
                    else None
                ),
            )
        return published
