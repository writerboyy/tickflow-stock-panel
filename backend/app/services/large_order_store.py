"""Persistent storage for realtime large-order events.

The realtime quote callback only enqueues normalized events. A dedicated writer
thread batches immutable Parquet fragments so disk I/O never runs on the quote
or ranking hot path.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import queue
import shutil
import threading
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import polars as pl

from app.market_time import CN_TZ, cn_today
from app.services.ingestion_manifest import archive_source_payload, stable_content_hash

logger = logging.getLogger(__name__)

EVENT_KINDS = ("proxy_flow", "kaipanla_trade", "kaipanla_intent", "orderbook_snapshot")
SCHEMA_VERSION = "large_orders_v1"
PARSER_VERSION = "large_orders_v1"
FLUSH_INTERVAL_SECONDS = 30.0
MAX_BATCH_ROWS = 50_000
MAX_QUEUE_BATCHES = 128
RAW_RETENTION_DAYS = 90
ORDERBOOK_RETENTION_DAYS = 20

_COMMON_SCHEMA: dict[str, pl.DataType] = {
    "trade_date": pl.Date,
    "event_ts_ms": pl.Int64,
    "symbol": pl.String,
    "name": pl.String,
    "price": pl.Float64,
    "amount": pl.Float64,
    "volume": pl.Float64,
    "source": pl.String,
    "event_id": pl.String,
    "received_at_ms": pl.Int64,
    "schema_version": pl.String,
    "parser_version": pl.String,
}

_KIND_SCHEMA: dict[str, dict[str, pl.DataType]] = {
    "proxy_flow": {
        **_COMMON_SCHEMA,
        "delta_amount": pl.Float64,
        "delta_volume": pl.Float64,
        "buy_amount": pl.Float64,
        "sell_amount": pl.Float64,
        "side": pl.Int8,
    },
    "kaipanla_trade": {
        **_COMMON_SCHEMA,
        "direction": pl.String,
        "direction_code": pl.Int8,
        "event_time": pl.String,
    },
    "kaipanla_intent": {
        **_COMMON_SCHEMA,
        "order_id": pl.String,
        "side": pl.String,
        "side_code": pl.Int8,
        "limit_flag": pl.Boolean,
        "limit_flag_code": pl.Int8,
        "cancel_flag": pl.Boolean,
        "cancel_flag_code": pl.Int8,
        "event_time": pl.String,
        "raw_tail": pl.String,
    },
    "orderbook_snapshot": {
        **_COMMON_SCHEMA,
        "bid_prices": pl.List(pl.Float64),
        "bid_volumes": pl.List(pl.Float64),
        "ask_prices": pl.List(pl.Float64),
        "ask_volumes": pl.List(pl.Float64),
        "book_imbalance": pl.Float64,
        "ofi": pl.Float64,
        "freshness_ms": pl.Int64,
        "target_kind": pl.String,
    },
}

_RAW_DATASETS = {
    "kaipanla_net_flow": "large_order_net_flow",
    "kaipanla_trade": "large_order_trades",
    "kaipanla_intent": "large_order_intents",
}


def _as_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _as_int(value: object) -> int | None:
    number = _as_float(value)
    return int(number) if number is not None else None


def _as_bool(value: object) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _event_date(value: object, event_ts_ms: int) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            pass
    return datetime.fromtimestamp(event_ts_ms / 1000, tz=CN_TZ).date()


def _event_hour(event_ts_ms: int) -> int:
    return datetime.fromtimestamp(event_ts_ms / 1000, tz=CN_TZ).hour


class LargeOrderStore:
    """Bounded asynchronous event writer and Parquet query facade."""

    def __init__(
        self,
        data_dir: Path,
        *,
        flush_interval: float = FLUSH_INTERVAL_SECONDS,
        max_batch_rows: int = MAX_BATCH_ROWS,
        max_queue_batches: int = MAX_QUEUE_BATCHES,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.root = self.data_dir / "large_orders"
        self.root.mkdir(parents=True, exist_ok=True)
        self.flush_interval = max(0.1, float(flush_interval))
        self.max_batch_rows = max(1, int(max_batch_rows))
        self._queue: queue.Queue[tuple[str, tuple[dict[str, Any], ...]]] = queue.Queue(
            maxsize=max(1, int(max_queue_batches)),
        )
        self._stop_event = threading.Event()
        self._flush_request = threading.Event()
        self._flush_ack = threading.Event()
        self._started = False
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._queued_rows = 0
        self._written_rows = 0
        self._dropped_rows = 0
        self._invalid_rows = 0
        self._last_flush_ms: int | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._writer_loop,
                name="large-order-store",
                daemon=True,
            )
            self._thread.start()
        self.cleanup_raw_archives()
        self.cleanup_orderbook_history()

    def stop(self, *, timeout: float = 15.0, compact_date: date | None = None) -> None:
        with self._lock:
            if not self._started:
                return
            self._stop_event.set()
            thread = self._thread
        if thread is not None:
            thread.join(max(0.1, timeout))
            if thread.is_alive():
                with self._lock:
                    self._last_error = "large-order storage writer did not stop before timeout"
                logger.error("实时大单存储 writer 未在超时时间内退出")
                return
        with self._lock:
            self._started = False
            self._thread = None
        if compact_date is not None:
            try:
                self.compact(compact_date)
            except Exception:  # noqa: BLE001
                logger.exception("实时大单分片压实失败: %s", compact_date)

    def submit(self, kind: str, rows: Iterable[dict[str, Any]]) -> int:
        if kind not in EVENT_KINDS:
            raise ValueError(f"unsupported large-order event kind: {kind}")
        payload = tuple(dict(row) for row in rows)
        if not payload:
            return 0
        with self._lock:
            try:
                self._queue.put_nowait((kind, payload))
            except queue.Full:
                self._dropped_rows += len(payload)
                self._last_error = "large-order storage queue full"
                return 0
            self._queued_rows += len(payload)
        return len(payload)

    def flush_now(self, timeout: float = 10.0) -> bool:
        with self._lock:
            if not self._started or self._thread is None:
                return False
            self._flush_ack.clear()
            self._flush_request.set()
        return self._flush_ack.wait(max(0.1, timeout))

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "enabled": True,
                "queued_rows": self._queued_rows,
                "written_rows": self._written_rows,
                "dropped_rows": self._dropped_rows,
                "invalid_rows": self._invalid_rows,
                "last_flush_ms": self._last_flush_ms,
                "last_error": self._last_error,
                "storage_root": str(self.root),
            }

    def archive_payload(
        self,
        kind: str,
        trade_date: date,
        batch_id: str,
        payload: Any,
    ) -> tuple[Path, str]:
        dataset = _RAW_DATASETS.get(kind)
        if dataset is None:
            raise ValueError(f"raw archive is not supported for {kind}")
        return archive_source_payload(
            self.data_dir,
            "kaipanla",
            dataset,
            trade_date.isoformat(),
            batch_id,
            payload,
            parser_version=PARSER_VERSION,
        )

    def cleanup_raw_archives(self, *, today: date | None = None) -> int:
        """Delete only this feature's raw archives older than the 90-day policy."""
        root = self.data_dir / "ext_data" / "_kaipanla_raw"
        if not root.exists():
            return 0
        cutoff = today or cn_today()
        removed = 0
        for snapshot_dir in root.glob("snapshot=*"):
            try:
                snapshot_date = date.fromisoformat(snapshot_dir.name.removeprefix("snapshot="))
            except ValueError:
                continue
            if (cutoff - snapshot_date).days <= RAW_RETENTION_DAYS:
                continue
            for dataset in _RAW_DATASETS.values():
                target = snapshot_dir / dataset
                if target.exists():
                    shutil.rmtree(target)
                    removed += 1
            try:
                if snapshot_dir.exists() and not any(snapshot_dir.iterdir()):
                    snapshot_dir.rmdir()
            except OSError:
                pass
        return removed

    def cleanup_orderbook_history(self, *, today: date | None = None) -> int:
        """Remove only monitored depth snapshots older than the 20-trading-day retention window."""
        cutoff = today or cn_today()
        root = self.root / "orderbook_snapshot"
        removed = 0
        if not root.exists():
            return removed
        dates = []
        for day_root in root.glob("date=*"):
            try:
                dates.append((date.fromisoformat(day_root.name.removeprefix("date=")), day_root))
            except ValueError:
                continue
        for _value, day_root in sorted(dates, reverse=True)[ORDERBOOK_RETENTION_DAYS:]:
            shutil.rmtree(day_root)
            removed += 1
        return removed

    def query(
        self,
        kind: str,
        trade_date: date,
        *,
        symbol: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 1000,
        order: str = "asc",
    ) -> dict[str, Any]:
        if kind not in EVENT_KINDS:
            raise ValueError(f"unsupported large-order event kind: {kind}")
        if order not in {"asc", "desc"}:
            raise ValueError("order must be asc or desc")
        limit = max(1, min(int(limit), 10_000))
        frame = self.read_day(
            kind,
            trade_date,
            symbol=symbol,
            from_ms=from_ms,
            to_ms=to_ms,
        )
        if frame.is_empty():
            return {"rows": [], "count": 0, "truncated": False}
        frame = frame.sort("event_ts_ms", descending=order == "desc")
        truncated = frame.height > limit
        rows = frame.head(limit).to_dicts()
        for row in rows:
            if isinstance(row.get("trade_date"), date):
                row["trade_date"] = row["trade_date"].isoformat()
        return {"rows": rows, "count": len(rows), "truncated": truncated}

    def read_day(
        self,
        kind: str,
        trade_date: date,
        *,
        symbol: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
    ) -> pl.DataFrame:
        """Read one normalized event dataset without imposing an API page limit."""
        if kind not in EVENT_KINDS:
            raise ValueError(f"unsupported large-order event kind: {kind}")
        self.flush_now()
        files = sorted((self.root / kind / f"date={trade_date.isoformat()}").glob("**/*.parquet"))
        if not files:
            return pl.DataFrame(schema=_KIND_SCHEMA[kind])
        frames = []
        schema = _KIND_SCHEMA[kind]
        for file in files:
            source = pl.read_parquet(file).with_row_index("_legacy_row")
            fallback_id = pl.concat_str(
                [
                    pl.lit(f"legacy:{kind}:{file.relative_to(self.root)}:"),
                    pl.col("_legacy_row").cast(pl.String),
                ]
            )
            expressions = []
            for column, dtype in schema.items():
                if column == "event_id":
                    value = (
                        pl.col(column).cast(pl.String, strict=False)
                        if column in source.columns
                        else pl.lit(None, dtype=pl.String)
                    )
                    expressions.append(
                        pl.when(value.is_not_null() & (value != ""))
                        .then(value)
                        .otherwise(fallback_id)
                        .alias(column)
                    )
                elif column in source.columns:
                    expressions.append(pl.col(column).cast(dtype, strict=False).alias(column))
                else:
                    expressions.append(pl.lit(None, dtype=dtype).alias(column))
            frames.append(source.select(expressions))
        frame = pl.concat(frames, how="vertical_relaxed")
        frame = frame.unique(subset=["event_id"], keep="last", maintain_order=True)
        if symbol:
            frame = frame.filter(pl.col("symbol") == symbol.strip().upper())
        if from_ms is not None:
            frame = frame.filter(pl.col("event_ts_ms") >= int(from_ms))
        if to_ms is not None:
            frame = frame.filter(pl.col("event_ts_ms") <= int(to_ms))
        return frame

    @staticmethod
    def _encode_cursor(row: dict[str, Any]) -> str:
        payload = json.dumps(
            [int(row["event_ts_ms"]), str(row["event_kind"]), str(row["event_id"])],
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(payload).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str) -> tuple[int, str, str]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            value = json.loads(base64.urlsafe_b64decode(padded).decode())
            if not isinstance(value, list) or len(value) != 3:
                raise ValueError
            return int(value[0]), str(value[1]), str(value[2])
        except (ValueError, TypeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError("invalid large-order history cursor") from exc

    def query_events(
        self,
        trade_date: date,
        *,
        kinds: tuple[str, ...] = EVENT_KINDS,
        symbol: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        cursor: str | None = None,
        limit: int = 1000,
        order: str = "desc",
    ) -> dict[str, Any]:
        if not kinds or any(kind not in EVENT_KINDS for kind in kinds):
            raise ValueError("unsupported large-order event kind")
        if order not in {"asc", "desc"}:
            raise ValueError("order must be asc or desc")
        limit = max(1, min(int(limit), 10_000))
        cursor_value = self._decode_cursor(cursor) if cursor else None
        frames = []
        for kind in dict.fromkeys(kinds):
            frame = self.read_day(
                kind,
                trade_date,
                symbol=symbol,
                from_ms=from_ms,
                to_ms=to_ms,
            )
            if not frame.is_empty():
                frames.append(frame.with_columns(pl.lit(kind).alias("event_kind")))
        if not frames:
            return {
                "rows": [],
                "count": 0,
                "has_more": False,
                "truncated": False,
                "next_cursor": None,
            }
        frame = pl.concat(frames, how="diagonal_relaxed")
        frame = frame.unique(
            subset=["event_kind", "event_id"],
            keep="last",
            maintain_order=True,
        )
        if cursor_value:
            cursor_ts, cursor_kind, cursor_id = cursor_value
            timestamp = pl.col("event_ts_ms")
            kind = pl.col("event_kind")
            event_id = pl.col("event_id")
            if order == "asc":
                after_cursor = (timestamp > cursor_ts) | (
                    (timestamp == cursor_ts)
                    & ((kind > cursor_kind) | ((kind == cursor_kind) & (event_id > cursor_id)))
                )
            else:
                after_cursor = (timestamp < cursor_ts) | (
                    (timestamp == cursor_ts)
                    & ((kind < cursor_kind) | ((kind == cursor_kind) & (event_id < cursor_id)))
                )
            frame = frame.filter(after_cursor)
        descending = order == "desc"
        frame = frame.sort(
            ["event_ts_ms", "event_kind", "event_id"],
            descending=[descending, descending, descending],
        )
        has_more = frame.height > limit
        page = frame.head(limit)
        rows = page.to_dicts()
        for row in rows:
            if isinstance(row.get("trade_date"), date):
                row["trade_date"] = row["trade_date"].isoformat()
        return {
            "rows": rows,
            "count": len(rows),
            "has_more": has_more,
            "truncated": has_more,
            "next_cursor": self._encode_cursor(rows[-1]) if has_more and rows else None,
        }

    def available_dates(self, *, limit: int = 30) -> list[str]:
        values: set[date] = set()
        for kind in EVENT_KINDS:
            for day_root in (self.root / kind).glob("date=*"):
                try:
                    value = date.fromisoformat(day_root.name.removeprefix("date="))
                except ValueError:
                    continue
                if any(day_root.glob("**/*.parquet")):
                    values.add(value)
        return [value.isoformat() for value in sorted(values, reverse=True)[: max(1, limit)]]

    def compact(self, trade_date: date, kind: str | None = None) -> dict[str, int]:
        """Merge a day's immutable fragments and deduplicate by event_id."""
        self.flush_now()
        kinds = (kind,) if kind else EVENT_KINDS
        result: dict[str, int] = {}
        for current_kind in kinds:
            if current_kind not in EVENT_KINDS:
                raise ValueError(f"unsupported large-order event kind: {current_kind}")
            day_root = self.root / current_kind / f"date={trade_date.isoformat()}"
            files = sorted(day_root.glob("**/*.parquet"))
            if not files:
                result[current_kind] = 0
                continue
            # Reuse the normalized reader so legacy fragments with missing
            # columns can be compacted alongside current-schema fragments.
            frame = self.read_day(current_kind, trade_date)
            frame = frame.sort("event_ts_ms")
            target = day_root / "part.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
            try:
                frame.write_parquet(temporary)
                os.replace(temporary, target)
            finally:
                if temporary.exists():
                    temporary.unlink()
            for path in files:
                if path != target and path.exists():
                    path.unlink()
            for directory in sorted(day_root.glob("**"), reverse=True):
                if directory.is_dir() and directory != day_root:
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
            result[current_kind] = frame.height
        return result

    def compact_unsealed_days(self, *, today: date | None = None) -> dict[str, int]:
        """Compact prior-day fragments left by a previous process instance."""
        cutoff = today or cn_today()
        dates: set[date] = set()
        for kind in EVENT_KINDS:
            for day_root in (self.root / kind).glob("date=*"):
                try:
                    value = date.fromisoformat(day_root.name.removeprefix("date="))
                except ValueError:
                    continue
                if value < cutoff and any(day_root.glob("hour=*/part-*.parquet")):
                    dates.add(value)
        compacted = 0
        rows = 0
        for value in sorted(dates):
            result = self.compact(value)
            compacted += 1
            rows += sum(result.values())
        return {"days": compacted, "rows": rows}

    def _writer_loop(self) -> None:
        pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
        pending_rows = 0
        deadline = time.monotonic() + self.flush_interval
        while True:
            if self._stop_event.is_set():
                while True:
                    try:
                        kind, rows = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    pending[kind].extend(rows)
                    pending_rows += len(rows)
                    with self._lock:
                        self._queued_rows = max(0, self._queued_rows - len(rows))
                if pending_rows:
                    self._flush_pending(pending)
                return

            timeout = max(0.05, min(0.5, deadline - time.monotonic()))
            try:
                kind, rows = self._queue.get(timeout=timeout)
                pending[kind].extend(rows)
                pending_rows += len(rows)
                with self._lock:
                    self._queued_rows = max(0, self._queued_rows - len(rows))
            except queue.Empty:
                pass

            should_flush = pending_rows >= self.max_batch_rows or time.monotonic() >= deadline
            if self._flush_request.is_set():
                should_flush = True
            if should_flush:
                if pending_rows:
                    self._flush_pending(pending)
                    pending = defaultdict(list)
                    pending_rows = 0
                deadline = time.monotonic() + self.flush_interval
                if self._flush_request.is_set():
                    self._flush_request.clear()
                    self._flush_ack.set()

    def _flush_pending(self, pending: dict[str, list[dict[str, Any]]]) -> None:
        grouped: dict[tuple[str, date, int], list[dict[str, Any]]] = defaultdict(list)
        invalid = 0
        for kind, rows in pending.items():
            for row in rows:
                normalized = self._normalize_row(kind, row)
                if normalized is None:
                    invalid += 1
                    continue
                grouped[(kind, normalized["trade_date"], _event_hour(normalized["event_ts_ms"]))].append(normalized)
        if invalid:
            with self._lock:
                self._invalid_rows += invalid
        written = 0
        for (kind, trade_date, hour), rows in grouped.items():
            try:
                schema = _KIND_SCHEMA[kind]
                frame = pl.DataFrame(
                    [{field: row.get(field) for field in schema} for row in rows],
                    schema=schema,
                    strict=False,
                )
                directory = self.root / kind / f"date={trade_date.isoformat()}" / f"hour={hour:02d}"
                directory.mkdir(parents=True, exist_ok=True)
                target = directory / f"part-{int(time.time() * 1000)}-{uuid4().hex}.parquet"
                temporary = target.with_name(f".{target.name}.tmp")
                try:
                    frame.write_parquet(temporary)
                    os.replace(temporary, target)
                finally:
                    if temporary.exists():
                        temporary.unlink()
                written += frame.height
            except Exception as exc:  # noqa: BLE001
                logger.exception("实时大单事件写入失败 kind=%s date=%s", kind, trade_date)
                with self._lock:
                    self._last_error = str(exc)
                    self._dropped_rows += len(rows)
        with self._lock:
            self._written_rows += written
            self._last_flush_ms = int(time.time() * 1000)

    @staticmethod
    def _normalize_row(kind: str, row: dict[str, Any]) -> dict[str, Any] | None:
        event_ts_ms = _as_int(row.get("event_ts_ms")) or _as_int(row.get("received_at_ms"))
        if event_ts_ms is None:
            event_ts_ms = int(time.time() * 1000)
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            return None
        event_id = str(row.get("event_id") or "").strip()
        if not event_id:
            event_id = stable_content_hash({"kind": kind, **row})
        normalized: dict[str, Any] = {
            "trade_date": _event_date(row.get("trade_date"), event_ts_ms),
            "event_ts_ms": event_ts_ms,
            "symbol": symbol,
            "name": str(row.get("name") or symbol),
            "price": _as_float(row.get("price")),
            "amount": _as_float(row.get("amount")),
            "volume": _as_float(row.get("volume")),
            "source": str(row.get("source") or kind),
            "event_id": event_id,
            "received_at_ms": _as_int(row.get("received_at_ms")) or event_ts_ms,
            "schema_version": str(row.get("schema_version") or SCHEMA_VERSION),
            "parser_version": str(row.get("parser_version") or PARSER_VERSION),
        }
        for field in _KIND_SCHEMA[kind]:
            if field in _COMMON_SCHEMA:
                continue
            value = row.get(field)
            if field in {"delta_amount", "delta_volume", "buy_amount", "sell_amount"}:
                normalized[field] = _as_float(value)
            elif field in {"bid_prices", "bid_volumes", "ask_prices", "ask_volumes"}:
                normalized[field] = [item for item in (_as_float(item) for item in (value or [])) if item is not None]
            elif field in {"book_imbalance", "ofi"}:
                normalized[field] = _as_float(value)
            elif field == "freshness_ms":
                normalized[field] = _as_int(value)
            elif field in {
                "direction_code", "side_code", "side", "limit_flag_code", "cancel_flag_code",
            }:
                normalized[field] = _as_int(value) if field != "side" or isinstance(value, (int, float)) else value
            elif field in {"limit_flag", "cancel_flag"}:
                normalized[field] = _as_bool(value)
            elif value is not None:
                normalized[field] = str(value)
            else:
                normalized[field] = None
        return normalized
