"""Auditable P0 historical backfill workflow.

The regular synchronization jobs are intentionally optimized for incremental
updates.  A historical backfill has a different failure boundary: every
requested batch must be accounted for before any canonical table is replaced.
This module therefore works in a shadow data directory and publishes only
after all source batches, key checks, and derived-table checks pass.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from app.indicators.pipeline import run_pipeline
from app.services import kline_sync
from app.services.daily_valuation import build_daily_valuation
from app.services.ingestion_manifest import stable_content_hash
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet
from app.tickflow.client import get_client
from app.tickflow.rate_limits import chunked, resolve_limit, sleep_between_batches

logger = logging.getLogger(__name__)


FINANCIAL_TABLES = ("metrics", "income", "balance_sheet", "cash_flow", "shares")
DATASETS = ("daily", "adj_factor", "financials", "valuation")
_DAILY_KEY = ("symbol", "date")
_ADJ_KEY = ("symbol", "trade_date")
_FINANCIAL_KEY = ("symbol", "period_end", "announce_date")
_DAILY_VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "amount")


class BackfillBlocked(RuntimeError):
    """Raised when a backfill cannot be safely staged or published."""


@dataclass(frozen=True)
class BackfillConfig:
    data_dir: Path
    start: date
    end: date
    datasets: tuple[str, ...] = DATASETS
    symbols: tuple[str, ...] | None = None
    batch_size: int | None = None
    rpm: int | None = None
    run_id: str | None = None
    publish: bool = False
    max_symbols: int | None = None

    def normalized(self) -> "BackfillConfig":
        data_dir = Path(self.data_dir).expanduser().resolve()
        datasets = tuple(dict.fromkeys(self.datasets))
        unknown = sorted(set(datasets) - set(DATASETS))
        if unknown:
            raise ValueError(f"unknown P0 dataset(s): {', '.join(unknown)}")
        if not datasets:
            raise ValueError("at least one P0 dataset is required")
        if self.start > self.end:
            raise ValueError("start must not be after end")
        if self.batch_size is not None and self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.rpm is not None and self.rpm <= 0:
            raise ValueError("rpm must be positive")
        symbols = None
        if self.symbols is not None:
            symbols = tuple(
                dict.fromkeys(
                    str(item).strip().upper() for item in self.symbols if str(item).strip()
                )
            )
            if not symbols:
                raise ValueError("symbols must not be empty")
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive")
        return BackfillConfig(
            data_dir=data_dir,
            start=self.start,
            end=self.end,
            datasets=datasets,
            symbols=symbols,
            batch_size=self.batch_size,
            rpm=self.rpm,
            run_id=self.run_id,
            publish=self.publish,
            max_symbols=self.max_symbols,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BackfillBlocked(f"invalid backfill manifest: {path}") from exc
    if not isinstance(value, dict):
        raise BackfillBlocked(f"invalid backfill manifest: {path}")
    return value


def _hash_symbols(symbols: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(set(symbols))).encode("utf-8")).hexdigest()


def _run_root(config: BackfillConfig) -> Path:
    run_id = (
        config.run_id
        or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    )
    return config.data_dir / "backfill_state" / "p0_history" / run_id


def _manifest_path(run_root: Path) -> Path:
    return run_root / "manifest.json"


def _new_manifest(config: BackfillConfig, symbols: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "p0_history_backfill",
        "run_id": config.run_id,
        "data_dir": str(config.data_dir),
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
        "datasets": list(config.datasets),
        "symbol_count": len(symbols),
        "symbols_hash": _hash_symbols(symbols),
        "status": "staging",
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "batches": {},
        "daily_valid_empty_symbols": [],
        "coverage": {},
        "publish_targets": [],
    }


def _get_manifest(config: BackfillConfig, symbols: list[str]) -> tuple[Path, dict[str, Any]]:
    root = _run_root(config)
    path = _manifest_path(root)
    if path.exists():
        manifest = _read_json(path)
        expected = {
            "data_dir": str(config.data_dir),
            "start": config.start.isoformat(),
            "end": config.end.isoformat(),
            "datasets": list(config.datasets),
            "symbol_count": len(symbols),
            "symbols_hash": _hash_symbols(symbols),
        }
        for key, value in expected.items():
            if manifest.get(key) != value:
                raise BackfillBlocked(f"resume configuration mismatch for {key}")
    else:
        manifest = _new_manifest(config, symbols)
        manifest["run_id"] = root.name
        _atomic_json(path, manifest)
    return root, manifest


def _save_manifest(path: Path, manifest: dict[str, Any], **updates: Any) -> None:
    manifest.update(updates)
    manifest["updated_at"] = _utc_now()
    _atomic_json(path, manifest)


def _batch_state(manifest: dict[str, Any], dataset: str, batch_id: str) -> dict[str, Any]:
    batches = manifest.setdefault("batches", {})
    dataset_batches = batches.setdefault(dataset, {})
    return dataset_batches.setdefault(batch_id, {})


def _record_batch(
    manifest_path: Path,
    manifest: dict[str, Any],
    dataset: str,
    batch_id: str,
    *,
    status: str,
    path: Path | None = None,
    frame: pl.DataFrame | None = None,
    error: str | None = None,
    requested_symbols: Iterable[str] | None = None,
) -> None:
    state = _batch_state(manifest, dataset, batch_id)
    state.update(
        {
            "status": status,
            "updated_at": _utc_now(),
            "error": error,
        }
    )
    if path is not None:
        state["path"] = str(path)
    if frame is not None:
        returned_symbols = (
            sorted(set(str(item) for item in frame["symbol"].drop_nulls().to_list()))
            if "symbol" in frame.columns
            else []
        )
        state.update(
            {
                "rows": frame.height,
                "symbols": len(returned_symbols),
                "content_hash": stable_content_hash(frame.to_dicts()),
            }
        )
        date_column = (
            "date"
            if "date" in frame.columns
            else "trade_date"
            if "trade_date" in frame.columns
            else None
        )
        if date_column and frame.height:
            state["min_date"] = str(frame[date_column].min())
            state["max_date"] = str(frame[date_column].max())
        if requested_symbols is not None:
            requested = sorted(set(str(item) for item in requested_symbols))
            valid_empty = sorted(set(requested) - set(returned_symbols))
            state.update(
                {
                    "requested_symbol_count": len(requested),
                    "returned_symbol_count": len(returned_symbols),
                    "valid_empty_symbol_count": len(valid_empty),
                    "valid_empty_symbols": valid_empty,
                    "valid_empty_symbol_sample": valid_empty[:20],
                }
            )
    _save_manifest(manifest_path, manifest)


def _copy_tree(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    try:
        shutil.copytree(source, target, copy_function=os.link)
    except OSError:
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(source, target)


def _copy_file(source: Path, target: Path) -> None:
    if not source.exists():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def _prepare_shadow(config: BackfillConfig, run_root: Path) -> Path:
    shadow = run_root / "data"
    if shadow.exists():
        return shadow
    shadow.mkdir(parents=True)
    # These are the only inputs needed by the P0 raw and derived tables.
    for directory in (
        "instruments",
        "instrument_name_history",
        "kline_daily",
        "kline_daily_enriched",
        "valuation_daily",
    ):
        _copy_tree(config.data_dir / directory, shadow / directory)
    _copy_file(
        config.data_dir / "adj_factor" / "all.parquet", shadow / "adj_factor" / "all.parquet"
    )
    for table in FINANCIAL_TABLES:
        _copy_file(
            config.data_dir / "financials" / table / "part.parquet",
            shadow / "financials" / table / "part.parquet",
        )
    return shadow


def _symbols_from_data(
    data_dir: Path,
    explicit: tuple[str, ...] | None,
    max_symbols: int | None,
    *,
    start: date,
    end: date,
) -> list[str]:
    if explicit is not None:
        symbols = list(explicit)
    else:
        path = data_dir / "instruments" / "instruments.parquet"
        if not path.exists():
            raise BackfillBlocked(f"missing instruments table: {path}")
        frame = pl.read_parquet(path)
        if "asset_type" in frame.columns:
            frame = frame.filter(pl.col("asset_type").is_null() | (pl.col("asset_type") == "stock"))
        listing_column = "list_date" if "list_date" in frame.columns else "listing_date"
        if listing_column in frame.columns:
            frame = frame.filter(
                pl.col(listing_column).is_null() | (pl.col(listing_column) <= pl.lit(end))
            )
        if "delist_date" in frame.columns:
            frame = frame.filter(
                pl.col("delist_date").is_null() | (pl.col("delist_date") >= pl.lit(start))
            )
        symbols = sorted(set(str(item) for item in frame["symbol"].drop_nulls().to_list()))
    if max_symbols is not None:
        symbols = symbols[:max_symbols]
    if not symbols:
        raise BackfillBlocked("symbol universe is empty")
    return symbols


def _capset_from_file(data_dir: Path) -> CapabilitySet:
    path = data_dir / "capabilities.json"
    if not path.exists():
        raise BackfillBlocked(f"missing capability cache: {path}")
    value = _read_json(path)
    caps: dict[Cap, CapabilityLimits] = {}
    for name, limits in (value.get("capabilities") or {}).items():
        try:
            cap = Cap(name)
        except ValueError:
            continue
        limits = limits if isinstance(limits, dict) else {}
        caps[cap] = CapabilityLimits(
            rpm=limits.get("rpm"),
            batch=limits.get("batch"),
            subscribe=limits.get("subscribe"),
        )
    return CapabilitySet(caps)


def _check_capabilities(config: BackfillConfig, capset: CapabilitySet) -> None:
    required: list[Cap] = []
    if "daily" in config.datasets:
        required.append(Cap.KLINE_DAILY_BATCH)
    if "adj_factor" in config.datasets:
        required.append(Cap.ADJ_FACTOR)
    if "financials" in config.datasets:
        required.append(Cap.FINANCIAL)
    missing = [cap.value for cap in required if not capset.has(cap)]
    if missing:
        raise BackfillBlocked(f"missing TickFlow capabilities: {', '.join(missing)}")


def _write_batch(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _batch_path(run_root: Path, dataset: str, batch_id: str, table: str | None = None) -> Path:
    directory = run_root / "batches" / dataset
    if table:
        directory /= table
    return directory / f"batch-{batch_id}.parquet"


def _normalize_daily_result(raw: Any) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    if isinstance(raw, dict):
        for symbol, value in raw.items():
            frame = kline_sync._normalize_daily(value, default_symbol=str(symbol))
            if not frame.is_empty():
                frames.append(frame)
    else:
        frame = kline_sync._normalize_daily(raw)
        if not frame.is_empty():
            frames.append(frame)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _normalize_adj_result(raw: Any) -> pl.DataFrame:
    return kline_sync._normalize_adj_factor(raw)


def _empty_adj_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64}
    )


def _empty_daily_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "symbol": pl.String,
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "amount": pl.Float64,
        }
    )


def _normalize_financial_result(raw: Any) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        for symbol, records in raw.items():
            if isinstance(records, list):
                for record in records:
                    if isinstance(record, dict):
                        row = dict(record)
                        row["symbol"] = str(symbol)
                        rows.append(row)
    elif isinstance(raw, pl.DataFrame):
        return raw
    elif hasattr(raw, "to_dict"):
        try:
            return pl.from_pandas(raw.reset_index() if hasattr(raw, "reset_index") else raw)
        except Exception as exc:  # noqa: BLE001
            raise BackfillBlocked(f"cannot normalize financial response: {exc}") from exc
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def _date_filter(frame: pl.DataFrame, column: str, start: date, end: date) -> pl.DataFrame:
    if frame.is_empty() or column not in frame.columns:
        return frame
    if frame.schema[column] != pl.Date:
        frame = frame.with_columns(pl.col(column).cast(pl.Date, strict=False))
    return frame.filter(pl.col(column).is_between(pl.lit(start), pl.lit(end), closed="both"))


def _dedupe_or_raise(frame: pl.DataFrame, key: tuple[str, ...], label: str) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    missing = [column for column in key if column not in frame.columns]
    if missing:
        raise BackfillBlocked(f"{label} missing key columns: {', '.join(missing)}")
    if any(frame.select([pl.col(column).is_null().any() for column in key]).row(0)):
        raise BackfillBlocked(f"{label} contains null primary-key values")
    value_columns = [column for column in frame.columns if column not in key]
    if value_columns:
        conflicts = (
            frame.group_by(list(key))
            .agg(pl.struct(value_columns).n_unique().alias("distinct_values"))
            .filter(pl.col("distinct_values") > 1)
        )
        if not conflicts.is_empty():
            sample = conflicts.head(5).to_dicts()
            raise BackfillBlocked(f"{label} has conflicting duplicate keys: {sample}")
    return frame.unique(subset=list(key), keep="last", maintain_order=False)


def _load_batch_frames(
    run_root: Path, dataset: str, table: str | None = None
) -> list[pl.DataFrame]:
    paths = sorted((_batch_path(run_root, dataset, "", table).parent).glob("batch-*.parquet"))
    return [pl.read_parquet(path) for path in paths]


def _fetch_daily(
    config: BackfillConfig,
    run_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    symbols: list[str],
    client: Any,
    capset: CapabilitySet,
) -> None:
    limit = resolve_limit(capset, Cap.KLINE_DAILY_BATCH, default_batch=100, default_rpm=30)
    batch_size = config.batch_size or limit.batch or 100
    rpm = config.rpm or limit.rpm
    total = (len(symbols) + batch_size - 1) // batch_size
    for index, chunk in enumerate(chunked(symbols, batch_size)):
        batch_id = f"{index:05d}"
        state = _batch_state(manifest, "daily", batch_id)
        path = _batch_path(run_root, "daily", batch_id)
        if state.get("status") == "completed" and path.exists():
            # Older checkpoints only stored a sample. Re-read the staged batch
            # once so resumed runs can account for every confirmed empty symbol.
            if "valid_empty_symbols" not in state:
                try:
                    _record_batch(
                        manifest_path,
                        manifest,
                        "daily",
                        batch_id,
                        status="completed",
                        path=path,
                        frame=pl.read_parquet(path),
                        requested_symbols=chunk,
                    )
                except Exception as exc:  # noqa: BLE001
                    state["status"] = "source_error"
                    state["error"] = f"checkpoint read failed: {type(exc).__name__}: {exc}"
                    _save_manifest(manifest_path, manifest)
                else:
                    continue
            else:
                continue
        try:
            sleep_between_batches(index, rpm)
            raw = client.klines.batch(
                chunk,
                period="1d",
                adjust="none",
                start_time=int(
                    datetime.combine(
                        config.start, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                    * 1000
                ),
                end_time=int(
                    datetime.combine(
                        config.end, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                    * 1000
                ),
                count=10000,
                as_dataframe=True,
                show_progress=False,
            )
            frame = _date_filter(_normalize_daily_result(raw), "date", config.start, config.end)
            frame = _dedupe_or_raise(frame, _DAILY_KEY, "daily")
            returned = set(frame["symbol"].unique().to_list()) if not frame.is_empty() else set()
            missing = sorted(set(chunk) - returned)
            confirmed_empty: set[str] = set()
            if missing:
                by_symbol_limit = resolve_limit(
                    capset,
                    Cap.KLINE_DAILY_BY_SYMBOL,
                    default_batch=1,
                    default_rpm=60,
                )
                fallback_frames: list[pl.DataFrame] = []
                unresolved: list[str] = []
                for single_index, symbol in enumerate(missing):
                    try:
                        sleep_between_batches(single_index, by_symbol_limit.rpm)
                        single_raw = client.klines.get(
                            symbol,
                            period="1d",
                            adjust="none",
                            start_time=int(
                                datetime.combine(
                                    config.start,
                                    datetime.min.time(),
                                    tzinfo=timezone.utc,
                                ).timestamp()
                                * 1000
                            ),
                            end_time=int(
                                datetime.combine(
                                    config.end,
                                    datetime.min.time(),
                                    tzinfo=timezone.utc,
                                ).timestamp()
                                * 1000
                            ),
                            count=10000,
                            as_dataframe=True,
                        )
                        single = _date_filter(
                            kline_sync._normalize_daily(single_raw, default_symbol=symbol),
                            "date",
                            config.start,
                            config.end,
                        )
                        single = _dedupe_or_raise(single, _DAILY_KEY, "daily")
                        if not single.is_empty():
                            fallback_frames.append(single)
                        else:
                            confirmed_empty.add(symbol)
                    except Exception:  # noqa: BLE001
                        unresolved.append(symbol)
                if fallback_frames:
                    frame = _dedupe_or_raise(
                        pl.concat([frame, *fallback_frames], how="diagonal_relaxed"),
                        _DAILY_KEY,
                        "daily",
                    )
                returned = (
                    set(frame["symbol"].unique().to_list()) if not frame.is_empty() else set()
                )
                missing = sorted(set(unresolved))
            if frame.is_empty() and not missing and not confirmed_empty:
                raise BackfillBlocked("daily batch returned no rows")
            if missing:
                raise BackfillBlocked(f"daily batch missing symbols: {missing[:8]}")
            if frame.is_empty():
                frame = _empty_daily_frame()
            _write_batch(path, frame.sort(["symbol", "date"]))
            _record_batch(
                manifest_path,
                manifest,
                "daily",
                batch_id,
                status="completed",
                path=path,
                frame=frame,
                requested_symbols=chunk,
            )
        except Exception as exc:  # noqa: BLE001
            _record_batch(
                manifest_path,
                manifest,
                "daily",
                batch_id,
                status="source_error",
                path=path,
                error=f"{type(exc).__name__}: {exc}",
            )

    failed = [
        key
        for key, state in (manifest.get("batches", {}).get("daily", {}) or {}).items()
        if state.get("status") != "completed"
    ]
    if failed:
        raise BackfillBlocked(f"daily batches failed: {failed[:8]}")
    valid_empty_symbols = sorted(
        {
            symbol
            for state in (manifest.get("batches", {}).get("daily", {}) or {}).values()
            for symbol in state.get("valid_empty_symbols", [])
        }
    )
    _save_manifest(
        manifest_path,
        manifest,
        daily_batches=total,
        daily_valid_empty_symbols=valid_empty_symbols,
    )


def _fetch_adj_factor(
    config: BackfillConfig,
    run_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    symbols: list[str],
    client: Any,
    capset: CapabilitySet,
) -> None:
    limit = resolve_limit(capset, Cap.ADJ_FACTOR, default_batch=50, default_rpm=30)
    batch_size = config.batch_size or limit.batch or 50
    rpm = config.rpm or limit.rpm
    for index, chunk in enumerate(chunked(symbols, batch_size)):
        batch_id = f"{index:05d}"
        state = _batch_state(manifest, "adj_factor", batch_id)
        path = _batch_path(run_root, "adj_factor", batch_id)
        if state.get("status") == "completed" and path.exists():
            continue
        try:
            sleep_between_batches(index, rpm)
            raw = client.klines.ex_factors(
                chunk,
                start_time=int(
                    datetime.combine(
                        config.start, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                    * 1000
                ),
                end_time=int(
                    datetime.combine(
                        config.end, datetime.min.time(), tzinfo=timezone.utc
                    ).timestamp()
                    * 1000
                ),
                as_dataframe=True,
                batch_size=batch_size,
                show_progress=False,
            )
            if raw is None:
                raise BackfillBlocked("adj_factor batch returned no response")
            frame = _date_filter(_normalize_adj_result(raw), "trade_date", config.start, config.end)
            frame = _dedupe_or_raise(frame, _ADJ_KEY, "adj_factor")
            if frame.is_empty():
                frame = _empty_adj_frame()
            unexpected = sorted(set(frame["symbol"].to_list()) - set(chunk))
            if unexpected:
                raise BackfillBlocked(
                    f"adj_factor batch returned unexpected symbols: {unexpected[:8]}"
                )
            _write_batch(path, frame)
            _record_batch(
                manifest_path,
                manifest,
                "adj_factor",
                batch_id,
                status="completed",
                path=path,
                frame=frame,
                requested_symbols=chunk,
            )
        except Exception as exc:  # noqa: BLE001
            _record_batch(
                manifest_path,
                manifest,
                "adj_factor",
                batch_id,
                status="source_error",
                path=path,
                error=f"{type(exc).__name__}: {exc}",
            )

    failed = [
        key
        for key, state in (manifest.get("batches", {}).get("adj_factor", {}) or {}).items()
        if state.get("status") != "completed"
    ]
    if failed:
        raise BackfillBlocked(f"adj_factor batches failed: {failed[:8]}")


def _fetch_financials(
    config: BackfillConfig,
    run_root: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    symbols: list[str],
    client: Any,
    capset: CapabilitySet,
) -> None:
    limit = resolve_limit(capset, Cap.FINANCIAL, default_batch=100, default_rpm=30)
    batch_size = config.batch_size or limit.batch or 100
    rpm = config.rpm or limit.rpm
    for table in FINANCIAL_TABLES:
        method = getattr(client.financials, table, None)
        if method is None:
            raise BackfillBlocked(f"TickFlow SDK does not support financial table: {table}")
        for index, chunk in enumerate(chunked(symbols, batch_size)):
            batch_id = f"{index:05d}"
            state = _batch_state(manifest, f"financials/{table}", batch_id)
            path = _batch_path(run_root, "financials", batch_id, table)
            if state.get("status") == "completed" and path.exists():
                continue
            try:
                sleep_between_batches(index, rpm)
                # Request the full available statement history. TTM construction
                # needs periods before the requested price window.
                raw = method(
                    chunk,
                    latest=False,
                    as_dataframe=True,
                    batch_size=batch_size,
                    show_progress=False,
                )
                frame = _normalize_financial_result(raw)
                if frame.is_empty():
                    raise BackfillBlocked(f"financial {table} batch returned no rows")
                frame = _dedupe_or_raise(frame, _FINANCIAL_KEY, f"financials/{table}")
                missing = sorted(set(chunk) - set(frame["symbol"].unique().to_list()))
                if missing:
                    raise BackfillBlocked(f"financial {table} batch missing symbols: {missing[:8]}")
                _write_batch(path, frame)
                _record_batch(
                    manifest_path,
                    manifest,
                    f"financials/{table}",
                    batch_id,
                    status="completed",
                    path=path,
                    frame=frame,
                )
            except Exception as exc:  # noqa: BLE001
                _record_batch(
                    manifest_path,
                    manifest,
                    f"financials/{table}",
                    batch_id,
                    status="source_error",
                    path=path,
                    error=f"{type(exc).__name__}: {exc}",
                )
        failed = [
            key
            for key, state in (
                manifest.get("batches", {}).get(f"financials/{table}", {}) or {}
            ).items()
            if state.get("status") != "completed"
        ]
        if failed:
            raise BackfillBlocked(f"financial {table} batches failed: {failed[:8]}")


def _merge_existing_and_batches(
    existing: pl.DataFrame,
    batches: list[pl.DataFrame],
    key: tuple[str, ...],
    label: str,
) -> pl.DataFrame:
    frames = [frame for frame in [existing, *batches] if not frame.is_empty()]
    if not frames:
        return pl.DataFrame()
    return _dedupe_or_raise(pl.concat(frames, how="diagonal_relaxed"), key, label).sort(list(key))


def _merge_daily(shadow: Path, run_root: Path) -> list[Path]:
    batch_frames = _load_batch_frames(run_root, "daily")
    if not batch_frames:
        raise BackfillBlocked("no staged daily batches")
    affected: set[date] = set()
    for frame in batch_frames:
        affected.update(frame["date"].drop_nulls().to_list())
    target = shadow / "kline_daily"
    for day in sorted(affected):
        path = target / f"date={day.isoformat()}" / "part.parquet"
        existing = pl.read_parquet(path) if path.exists() else pl.DataFrame()
        incoming = pl.concat(
            [frame.filter(pl.col("date") == pl.lit(day)) for frame in batch_frames],
            how="diagonal_relaxed",
        )
        merged = _merge_existing_and_batches(existing, [incoming], _DAILY_KEY, "kline_daily")
        _write_batch(path, merged)
    return [target / f"date={day.isoformat()}" / "part.parquet" for day in sorted(affected)]


def _merge_adj_factor(shadow: Path, run_root: Path) -> Path:
    batches = _load_batch_frames(run_root, "adj_factor")
    target = shadow / "adj_factor" / "all.parquet"
    existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
    merged = _merge_existing_and_batches(existing, batches, _ADJ_KEY, "adj_factor")
    if merged.is_empty():
        raise BackfillBlocked("no staged adjustment factors")
    _write_batch(target, merged)
    return target


def _merge_financials(shadow: Path, run_root: Path) -> list[Path]:
    targets: list[Path] = []
    for table in FINANCIAL_TABLES:
        batches = _load_batch_frames(run_root, "financials", table)
        target = shadow / "financials" / table / "part.parquet"
        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        merged = _merge_existing_and_batches(
            existing, batches, _FINANCIAL_KEY, f"financials/{table}"
        )
        if merged.is_empty():
            raise BackfillBlocked(f"no staged financial rows: {table}")
        _write_batch(target, merged)
        targets.append(target)
    return targets


def _coverage(
    shadow: Path,
    symbols: list[str],
    config: BackfillConfig,
    valid_empty_symbols: Iterable[str] = (),
) -> dict[str, Any]:
    valid_empty = sorted(set(str(symbol) for symbol in valid_empty_symbols) & set(symbols))
    daily_glob = shadow / "kline_daily" / "**" / "*.parquet"
    if not any(shadow.joinpath("kline_daily").rglob("*.parquet")):
        return {
            "rows": 0,
            "symbols": 0,
            "missing_symbols": sorted(set(symbols) - set(valid_empty)),
            "valid_empty_symbols": valid_empty,
        }
    frame = (
        pl.scan_parquet(
            str(daily_glob),
            extra_columns="ignore",
            missing_columns="insert",
        )
        .filter(pl.col("date").is_between(pl.lit(config.start), pl.lit(config.end), closed="both"))
        .select(
            pl.len().alias("rows"),
            pl.col("symbol").n_unique().alias("symbols"),
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        )
        .collect()
    )
    observed = (
        pl.scan_parquet(
            str(daily_glob),
            extra_columns="ignore",
            missing_columns="insert",
        )
        .filter(pl.col("date").is_between(pl.lit(config.start), pl.lit(config.end), closed="both"))
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )
    missing = sorted(set(symbols) - set(observed))
    if frame.is_empty():
        return {
            "rows": 0,
            "symbols": 0,
            "missing_symbols": sorted(set(symbols) - set(valid_empty)),
            "valid_empty_symbols": valid_empty,
        }
    return {
        "rows": int(frame["rows"][0]),
        "symbols": int(frame["symbols"][0]),
        "min_date": str(frame["min_date"][0]) if frame["min_date"][0] is not None else None,
        "max_date": str(frame["max_date"][0]) if frame["max_date"][0] is not None else None,
        "missing_symbols": sorted(set(missing) - set(valid_empty)),
        "valid_empty_symbols": valid_empty,
    }


def _publish_targets(run_root: Path, shadow: Path, data_dir: Path, targets: list[str]) -> list[str]:
    backup_root = run_root / "backups"
    backup_root.mkdir(parents=True, exist_ok=True)
    moved: list[tuple[Path, Path, Path]] = []
    try:
        for relative in targets:
            source = shadow / relative
            target = data_dir / relative
            if not source.exists():
                raise BackfillBlocked(f"missing staged publish target: {source}")
            backup = backup_root / relative
            backup.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                os.replace(target, backup)
            os.replace(source, target)
            moved.append((target, backup, source))
    except Exception:
        for target, backup, _source in reversed(moved):
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
            if backup.exists():
                os.replace(backup, target)
        raise
    return [str(data_dir / relative) for relative in targets]


def run_p0_backfill(
    config: BackfillConfig,
    *,
    client: Any | None = None,
    capset: CapabilitySet | None = None,
    compute_enriched: Callable[..., int] | None = None,
    compute_valuation: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run a P0 backfill in a shadow directory and optionally publish it."""
    config = config.normalized()
    symbols = _symbols_from_data(
        config.data_dir,
        config.symbols,
        config.max_symbols,
        start=config.start,
        end=config.end,
    )
    run_root, manifest = _get_manifest(config, symbols)
    manifest_path = _manifest_path(run_root)
    shadow = _prepare_shadow(config, run_root)
    capset = capset or _capset_from_file(config.data_dir)
    client = client or get_client()

    try:
        _check_capabilities(config, capset)
        if "daily" in config.datasets:
            _fetch_daily(config, run_root, manifest, manifest_path, symbols, client, capset)
        if "adj_factor" in config.datasets:
            _fetch_adj_factor(config, run_root, manifest, manifest_path, symbols, client, capset)
        if "financials" in config.datasets:
            _fetch_financials(config, run_root, manifest, manifest_path, symbols, client, capset)

        publish_targets: list[str] = []
        if "daily" in config.datasets:
            _merge_daily(shadow, run_root)
            publish_targets.append("kline_daily")
        if "adj_factor" in config.datasets:
            _merge_adj_factor(shadow, run_root)
            publish_targets.append("adj_factor/all.parquet")
        if "financials" in config.datasets:
            _merge_financials(shadow, run_root)
            publish_targets.extend(f"financials/{table}/part.parquet" for table in FINANCIAL_TABLES)

        if "valuation" in config.datasets:
            pipeline = compute_enriched or run_pipeline
            valuation_builder = compute_valuation or build_daily_valuation
            pipeline(data_dir=shadow, keep_backup=False)
            valuation_result = valuation_builder(shadow, keep_backup=False)
            if not valuation_result.get("rows"):
                raise BackfillBlocked("valuation rebuild produced no rows")
            publish_targets.extend(["kline_daily_enriched", "valuation_daily"])

        coverage = _coverage(
            shadow,
            symbols,
            config,
            manifest.get("daily_valid_empty_symbols", []),
        )
        if "daily" in config.datasets and coverage.get("missing_symbols"):
            raise BackfillBlocked(
                f"daily coverage missing symbols: {coverage['missing_symbols'][:8]}"
            )
        manifest["coverage"] = coverage
        manifest["publish_targets"] = sorted(set(publish_targets))
        _save_manifest(manifest_path, manifest, status="staged")

        published: list[str] = []
        if config.publish:
            published = _publish_targets(
                run_root, shadow, config.data_dir, manifest["publish_targets"]
            )
            _save_manifest(
                manifest_path,
                manifest,
                status="published",
                published=published,
                published_at=_utc_now(),
            )
        return {
            "status": "published" if config.publish else "staged",
            "run_id": run_root.name,
            "manifest": str(manifest_path),
            "coverage": coverage,
            "publish_targets": manifest["publish_targets"],
            "published": published,
        }
    except Exception as exc:  # noqa: BLE001
        _save_manifest(
            manifest_path, manifest, status="blocked", error=f"{type(exc).__name__}: {exc}"
        )
        raise
