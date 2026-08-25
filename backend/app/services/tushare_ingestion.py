"""Audited, resumable Tushare Proxy ingestion into local TickFlow parquet."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import shutil
import threading
from typing import Any
from uuid import uuid4

import polars as pl

from app.plugins.pit_history.storage import (
    CNINFO_SW_STANDARD,
    CNINFO_SW_STANDARD_CODE,
    INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    merge_index_membership_frames,
    normalize_index_membership_history,
    normalize_industry_membership_history,
    normalize_instrument_lifecycle_events,
    validate_index_membership_history,
)
from app.services.ext_data import ExtConfig, ExtConfigStore, ExtField
from app.services.ingestion_manifest import (
    archive_source_payload,
    load_ingestion_manifest,
    record_ingestion_batch,
    stable_content_hash,
    update_ingestion_manifest,
)
from app.services.tushare_datasets import (
    DATASET_SPECS,
    TushareDatasetSpec,
    TushareField,
)


SOURCE = "tushare_proxy"
INGESTION_SCHEMA_VERSION = 2
PARSER_VERSION = "tushare_ingestion_v5"
DEFAULT_HISTORY_START = date(2010, 1, 1)
_NUMERIC_DTYPES = {"float": pl.Float64, "int": pl.Int64, "bool": pl.Boolean}
_DATE_FORMATS = ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d")
_DATE_FIELDS = {
    "date",
    "trade_date",
    "ann_date",
    "announce_date",
    "announcement_date",
    "end_date",
    "period_end",
    "effective_from",
    "effective_to",
    "ex_date",
    "first_ann_date",
    "pre_date",
    "pretrade_date",
    "actual_date",
    "modify_date",
    "begin_date",
    "cal_date",
    "close_date",
    "delist_date",
    "exp_date",
    "float_date",
    "list_date",
    "pay_date",
    "record_date",
    "resume_date",
    "start_date",
    "suspend_date",
}
_DAILY_VALUES = ("open", "high", "low", "close", "volume", "amount")
_DAILY_TOLERANCES = {
    "open": 1e-6,
    "high": 1e-6,
    "low": 1e-6,
    "close": 1e-6,
    "volume": 0.5,
    "amount": 100.0,
}
_LOCKS: dict[Path, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()
_INCREMENTAL_DATE_PARAMS = {
    "daily": "trade_date",
    "fund_daily": "trade_date",
    "index_daily": "trade_date",
    "daily_basic": "trade_date",
    "moneyflow": "trade_date",
    "margin": "trade_date",
    "margin_detail": "trade_date",
    "top_list": "trade_date",
    "limit_list_d": "trade_date",
    "limit_list_ths": "trade_date",
    "block_trade": "trade_date",
    "cyq_perf": "trade_date",
    "cyq_chips": "trade_date",
    "income": "ann_date",
    "balancesheet": "ann_date",
    "cashflow": "ann_date",
    "fina_indicator": "ann_date",
    "forecast": "ann_date",
    "express": "ann_date",
    "disclosure_date": "ann_date",
    "stk_holdernumber": "ann_date",
    "top10_holders": "ann_date",
    "top10_floatholders": "ann_date",
    "stk_holdertrade": "ann_date",
    "repurchase": "ann_date",
    "share_float": "ann_date",
    "dividend": "ann_date",
    "suspend_d": "trade_date",
}
_INCREMENTAL_PAGED_APIS = {"daily_basic", "moneyflow"}
_PAGE_SIZE = 4_000
_MAX_PAGES_PER_BATCH = 10


class TushareIngestionBlocked(RuntimeError):
    """A dataset failed validation and cannot be published."""


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    data_dir: Path
    run_id: str
    start: date = DEFAULT_HISTORY_START
    end: date = date.today()
    publish: bool = False
    incremental: bool = False

    def normalized(self) -> "IngestionConfig":
        root = Path(self.data_dir).expanduser().resolve()
        if self.start > self.end:
            raise ValueError("Tushare ingestion start must not be after end")
        if not str(self.run_id).strip():
            raise ValueError("Tushare ingestion run_id is required")
        return IngestionConfig(
            root,
            str(self.run_id).strip(),
            self.start,
            self.end,
            self.publish,
            self.incremental,
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_part(value: object) -> str:
    text = "".join(ch if ch.isalnum() or ch in "_.=-" else "-" for ch in str(value))
    text = text.strip("-.")
    if not text:
        raise ValueError("empty path component")
    return text[:160]


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
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


def _path_lock(path: Path) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(path, threading.Lock())


def _normalize_symbol(value: object) -> str | None:
    if value is None:
        return None
    symbol = str(value).strip().upper()
    if not symbol or symbol.lower() in {"nan", "none", "null"}:
        return None
    for source, target in {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}.items():
        if symbol.endswith(source):
            return f"{symbol[: -len(source)]}{target}"
    if "." in symbol:
        return symbol
    if len(symbol) == 6 and symbol.isdigit():
        if symbol.startswith(("4", "8", "9")):
            return f"{symbol}.BJ"
        if symbol.startswith(("5", "6", "9")):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"
    return symbol


def _parse_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _iso_date(value: object) -> str | None:
    parsed = _parse_date(value)
    return parsed.isoformat() if parsed is not None else None


def _interval_defects(
    frame: pl.DataFrame,
    group_columns: tuple[str, ...],
) -> tuple[int, int]:
    required = {*group_columns, "effective_from", "effective_to"}
    if frame.is_empty() or not required <= set(frame.columns):
        return 0, 0
    groups: dict[tuple[Any, ...], list[tuple[date, date | None]]] = {}
    invalid = 0
    for row in frame.select(*group_columns, "effective_from", "effective_to").to_dicts():
        start = _parse_date(row["effective_from"])
        end = _parse_date(row["effective_to"])
        if start is None:
            invalid += 1
            continue
        if end is not None and end <= start:
            invalid += 1
        key = tuple(row[column] for column in group_columns)
        groups.setdefault(key, []).append((start, end))
    overlaps = 0
    for intervals in groups.values():
        previous_end: date | None = None
        has_previous = False
        for start, end in sorted(intervals):
            if has_previous and (previous_end is None or start < previous_end):
                overlaps += 1
            if not has_previous or previous_end is not None:
                previous_end = end
            has_previous = True
    return invalid, overlaps


def _pick(row: Mapping[str, Any], field: TushareField) -> Any:
    for name in (field.name, *field.aliases):
        if name in row and row[name] not in (None, ""):
            return row[name]
    return None


def _row_hash(row: Mapping[str, Any]) -> str:
    return stable_content_hash(dict(row))


def _empty_frame(spec: TushareDatasetSpec) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {}
    for field in spec.fields:
        schema[field.name] = _NUMERIC_DTYPES.get(field.dtype, pl.String)
    schema.update(
        {
            "source": pl.String,
            "collected_at": pl.String,
            "source_revision_hash": pl.String,
        }
    )
    if spec.revisioned:
        schema["provider_revision_flag"] = pl.String
    return pl.DataFrame(schema=schema)


def normalize_dataset_rows(
    spec: TushareDatasetSpec,
    rows: Iterable[Mapping[str, Any]],
    *,
    collected_at: str | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Normalize one response using a frozen contract and return its audit."""
    timestamp = collected_at or _utc_now()
    normalized: list[dict[str, Any]] = []
    rejected = 0
    for provider_row in rows:
        if spec.api_name == "index_member_all":
            provider_rows = []
            for level in (1, 2, 3):
                expanded = dict(provider_row)
                expanded.update({
                    "industry_standard": CNINFO_SW_STANDARD,
                    "industry_standard_code": CNINFO_SW_STANDARD_CODE,
                    "industry_level": level,
                    "industry_code": provider_row.get(f"l{level}_code"),
                    "industry_name": provider_row.get(f"l{level}_name"),
                })
                provider_rows.append(expanded)
        else:
            provider_rows = [provider_row]

        for normalized_provider_row in provider_rows:
            output: dict[str, Any] = {}
            for field in spec.fields:
                value = _pick(normalized_provider_row, field)
                if field.name in {"symbol", "member_symbol", "index_symbol"}:
                    value = _normalize_symbol(value)
                elif field.name in _DATE_FIELDS:
                    value = _iso_date(value)
                output[field.name] = value
            if spec.api_name == "ci_index_member":
                output["industry_standard"] = output.get("industry_standard") or "citics"
            missing_key = any(
                output.get(column) in (None, "") for column in spec.primary_key
            )
            missing_industry = spec.api_name == "index_member_all" and any(
                output.get(column) in (None, "")
                for column in ("industry_code", "industry_name")
            )
            if missing_key or missing_industry:
                rejected += 1
                continue
            output["source"] = SOURCE
            output["collected_at"] = timestamp
            output["source_revision_hash"] = _row_hash(provider_row)
            if spec.revisioned:
                revision_flag = provider_row.get("update_flag")
                output["provider_revision_flag"] = (
                    None if revision_flag in (None, "") else str(revision_flag)
                )
            normalized.append(output)

    if not normalized:
        frame = _empty_frame(spec)
    else:
        frame = pl.DataFrame(normalized, infer_schema_length=None)
        expressions: list[pl.Expr] = []
        for field in spec.fields:
            if field.name not in frame.columns:
                continue
            dtype = _NUMERIC_DTYPES.get(field.dtype)
            if dtype is not None:
                if dtype == pl.Boolean:
                    expressions.append(
                        pl.col(field.name)
                        .cast(pl.String, strict=False)
                        .str.to_lowercase()
                        .is_in(["1", "true", "yes"])
                        .alias(field.name)
                    )
                else:
                    expressions.append(pl.col(field.name).cast(dtype, strict=False))
            else:
                expressions.append(pl.col(field.name).cast(pl.String, strict=False))
        frame = frame.with_columns(expressions)

        # Tushare and canonical daily volume are both in lots; amount is converted
        # from thousand yuan to yuan.
        if spec.kind == "daily":
            frame = frame.with_columns(
                (pl.col("amount") * 1_000.0).alias("amount"),
            )

        key = list(spec.normalized_primary_key)
        value_columns = [column for column in frame.columns if column not in key]
        if value_columns:
            conflicts = (
                frame.group_by(key)
                .agg(pl.struct(value_columns).n_unique().alias("versions"))
                .filter(pl.col("versions") > 1)
            )
            if not conflicts.is_empty():
                raise TushareIngestionBlocked(
                    f"{spec.api_name} contains conflicting duplicate keys: "
                    f"{conflicts.head(5).to_dicts()}"
                )
        frame = frame.unique(subset=key, keep="last").sort(key)

    null_rates: dict[str, float] = {}
    if frame.height:
        for field in spec.fields:
            null_rates[field.name] = round(
                float(frame[field.name].null_count()) / frame.height,
                6,
            )
    date_values = (
        [_parse_date(item) for item in frame[spec.logical_date].to_list()]
        if frame.height and spec.logical_date in frame.columns
        else []
    )
    valid_dates = [item for item in date_values if item is not None]
    audit = {
        "rows": frame.height,
        "rejected_rows": rejected,
        "duplicate_keys": 0,
        "symbols": int(frame["symbol"].n_unique())
        if frame.height and "symbol" in frame.columns
        else 0,
        "min_date": min(valid_dates).isoformat() if valid_dates else None,
        "max_date": max(valid_dates).isoformat() if valid_dates else None,
        "null_rates": null_rates,
        "content_hash": stable_content_hash(frame.to_dicts()),
    }
    return frame, audit


def extension_config(
    spec: TushareDatasetSpec,
    *,
    factor_ready: bool = False,
) -> ExtConfig:
    fields = [ExtField(field.name, field.dtype, field.label) for field in spec.fields]
    fields.extend(
        [
            ExtField("source", "string", "数据来源"),
            ExtField("collected_at", "string", "采集时间"),
            ExtField("source_revision_hash", "string", "供应商修订哈希"),
        ]
    )
    if spec.revisioned:
        fields.append(ExtField("provider_revision_flag", "string", "供应商修订标记"))
    return ExtConfig(
        id=spec.table_id,
        label=f"Tushare {spec.label}",
        mode="timeseries",
        fields=fields,
        description=(
            f"Tushare Proxy {spec.api_name} 规范历史；主键="
            f"{','.join(spec.normalized_primary_key)}；逻辑日期={spec.logical_date}"
        ),
        symbol_map={"type": "mapped", "col": "symbol"}
        if any(field.name == "symbol" for field in spec.fields)
        else {},
        code_map={"type": "computed", "from": "symbol", "method": "strip_exchange"}
        if any(field.name == "symbol" for field in spec.fields)
        else {},
        schema_version=1,
        allowed_usage=[
            "display",
            "filter",
            "event-context",
            *(["factor-input"] if factor_ready and spec.factor_input else []),
        ],
        primary_key=list(spec.normalized_primary_key),
        logical_date=spec.logical_date,
        units=spec.units,
    )


def ensure_extension_configs(
    data_dir: Path,
    specs: Iterable[TushareDatasetSpec],
    *,
    factor_ready: bool = False,
) -> None:
    store = ExtConfigStore(Path(data_dir))
    for spec in specs:
        if spec.kind == "extension":
            existing = store.get(spec.table_id)
            ready = factor_ready or bool(existing and "factor-input" in existing.allowed_usage)
            store.upsert(extension_config(spec, factor_ready=ready))


def _year_windows(start: date, end: date) -> list[tuple[date, date]]:
    windows: list[tuple[date, date]] = []
    current = start
    while current <= end:
        window_end = min(end, date(current.year, 12, 31))
        windows.append((current, window_end))
        current = date(current.year + 1, 1, 1)
    return windows


def _staging_path(run_root: Path, spec: TushareDatasetSpec, batch_id: str) -> Path:
    return run_root / "datasets" / spec.api_name / f"{_safe_part(batch_id)}.parquet"


def _date_params(start: date, end: date) -> dict[str, str]:
    return {"start_date": start.strftime("%Y%m%d"), "end_date": end.strftime("%Y%m%d")}


def _batch_requests(
    spec: TushareDatasetSpec,
    config: IngestionConfig,
    *,
    symbols: Iterable[str],
    indexes: Iterable[str],
    trade_dates: Iterable[date],
) -> list[tuple[str, dict[str, Any]]]:
    requests: list[tuple[str, dict[str, Any]]] = []
    if spec.api_name == "stock_basic":
        return [
            (f"list-status-{status}", {"exchange": "", "list_status": status})
            for status in ("L", "D", "P")
        ]
    if spec.api_name == "etf_basic":
        return [("all", {"exchange": ""})]
    if spec.api_name == "index_basic":
        return [(f"market-{market}", {"market": market}) for market in ("SSE", "SZSE", "CSI")]
    if spec.api_name == "trade_cal":
        return [
            (
                str(start.year),
                {"exchange": "SSE", **_date_params(start, end)},
            )
            for start, end in _year_windows(config.start, config.end)
        ]
    incremental_param = _INCREMENTAL_DATE_PARAMS.get(spec.api_name)
    if config.incremental and incremental_param:
        dates = (
            sorted(set(trade_dates)) if incremental_param == "trade_date" and trade_dates else []
        )
        if not dates:
            current = config.start
            while current <= config.end:
                dates.append(current)
                current += timedelta(days=1)
        for current in dates:
            value = current.strftime("%Y%m%d")
            params: dict[str, Any] = {incremental_param: value}
            batch_id = f"{incremental_param}-{value}"
            if spec.api_name in _INCREMENTAL_PAGED_APIS:
                params.update(limit=_PAGE_SIZE, offset=0)
                batch_id = f"{batch_id}-offset-0"
            requests.append((batch_id, params))
        return requests
    if spec.scope == "trade_date":
        for item in sorted(set(trade_dates)):
            requests.append((item.isoformat(), {"trade_date": item.strftime("%Y%m%d")}))
        return requests
    if spec.scope == "global":
        return [("all", _date_params(config.start, config.end))]
    targets = list(indexes if spec.scope == "index" else symbols)
    for target in targets:
        for start, end in _year_windows(config.start, config.end):
            params: dict[str, Any] = _date_params(start, end)
            if spec.scope == "index" and spec.api_name in {"index_member_all", "index_weight"}:
                params["index_code"] = target
            else:
                params["ts_code"] = target
            requests.append((f"{target}-{start.year}", params))
    return requests


class TushareDatasetIngestion:
    """Collect and publish dataset-specific Tushare responses."""

    def __init__(self, config: IngestionConfig, client: Any) -> None:
        self.config = config.normalized()
        self.client = client
        self.run_root = (
            self.config.data_dir
            / "backfill_state"
            / "tushare_proxy"
            / _safe_part(self.config.run_id)
        )
        self._trade_dates_cache: list[date] | None = None

    def _archive(self, spec: TushareDatasetSpec, batch_id: str, response: Any) -> str:
        _, content_hash = archive_source_payload(
            self.config.data_dir,
            SOURCE,
            spec.api_name,
            self.config.run_id,
            batch_id,
            response.raw,
            parser_version=PARSER_VERSION,
        )
        return content_hash

    def trade_dates(self) -> list[date]:
        if self._trade_dates_cache is not None:
            return list(self._trade_dates_cache)
        response = self.client.request(
            "trade_cal",
            {
                "exchange": "SSE",
                **_date_params(self.config.start, self.config.end),
                "is_open": 1,
            },
        )
        archive_source_payload(
            self.config.data_dir,
            SOURCE,
            "trade_cal",
            self.config.run_id,
            "dataset-calendar",
            response.raw,
            parser_version=PARSER_VERSION,
        )
        result: list[date] = []
        for row in response.rows:
            parsed = _parse_date(row.get("cal_date") or row.get("trade_date"))
            if parsed is not None and str(row.get("is_open", "1")) not in {"0", "False", "false"}:
                result.append(parsed)
        if not result:
            raise TushareIngestionBlocked("trade_cal returned no open dates")
        self._trade_dates_cache = sorted(set(result))
        return list(self._trade_dates_cache)

    def collect(
        self,
        specs: Iterable[TushareDatasetSpec],
        *,
        symbols: Iterable[str] = (),
        indexes: Iterable[str] = (),
        trade_dates: Iterable[date] | None = None,
    ) -> dict[str, Any]:
        specs = tuple(specs)
        dates = list(trade_dates or ())
        needs_trade_dates = any(
            spec.scope == "trade_date"
            or (
                self.config.incremental
                and _INCREMENTAL_DATE_PARAMS.get(spec.api_name) == "trade_date"
            )
            for spec in specs
        )
        if needs_trade_dates and not dates:
            dates = self.trade_dates()
        summary: dict[str, Any] = {}
        for spec in specs:
            ensure_extension_configs(self.config.data_dir, (spec,))
            requests = _batch_requests(
                spec,
                self.config,
                symbols=symbols,
                indexes=indexes,
                trade_dates=dates,
            )
            if not requests:
                raise TushareIngestionBlocked(f"{spec.api_name} has no request targets")
            existing_manifest = load_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
            )
            staging_compatible = (
                existing_manifest.get("schema_version") == INGESTION_SCHEMA_VERSION
                and existing_manifest.get("parser_version") == PARSER_VERSION
            )
            update_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
                status="running",
                schema_version=INGESTION_SCHEMA_VERSION,
                parser_version=PARSER_VERSION,
                requested_start=self.config.start.isoformat(),
                requested_end=self.config.end.isoformat(),
                requested_batches=len(requests),
                primary_key=list(spec.normalized_primary_key),
                logical_date=spec.logical_date,
            )
            rows = 0
            rejected = 0
            failures: list[str] = []
            empty_unconfirmed: list[str] = []
            request_index = 0
            page_hashes: dict[str, set[str]] = {}
            while request_index < len(requests):
                batch_id, params = requests[request_index]
                request_index += 1
                path = _staging_path(self.run_root, spec, batch_id)
                state = load_ingestion_manifest(
                    self.config.data_dir,
                    SOURCE,
                    spec.api_name,
                    self.config.run_id,
                )
                prior = (state.get("batches") or {}).get(batch_id) or {}
                if (
                    staging_compatible
                    and prior.get("status") in {"completed", "valid_empty"}
                    and (prior.get("status") == "valid_empty" or path.exists())
                ):
                    prior_rows = int(prior.get("row_count") or 0)
                    rows += prior_rows
                    page_key = str(params.get("trade_date") or params.get("ann_date") or "")
                    prior_hash = str(prior.get("source_content_hash") or "")
                    if prior_hash:
                        page_hashes.setdefault(page_key, set()).add(prior_hash)
                    self._append_next_page(spec, requests, batch_id, params, prior_rows)
                    continue
                try:
                    response = self.client.request(spec.api_name, params)
                    source_hash = self._archive(spec, batch_id, response)
                    frame, audit = normalize_dataset_rows(spec, response.rows)
                    if spec.api_name in _INCREMENTAL_PAGED_APIS:
                        if frame.height > _PAGE_SIZE:
                            raise TushareIngestionBlocked(
                                f"{spec.api_name}/{batch_id} exceeded requested page size "
                                f"{_PAGE_SIZE}"
                            )
                        page_key = str(params.get("trade_date") or params.get("ann_date") or "")
                        seen_hashes = page_hashes.setdefault(page_key, set())
                        if source_hash in seen_hashes:
                            raise TushareIngestionBlocked(
                                f"{spec.api_name}/{batch_id} repeated a prior page; "
                                "provider may have ignored offset"
                            )
                        seen_hashes.add(source_hash)
                    elif frame.height >= spec.max_rows:
                        raise TushareIngestionBlocked(
                            f"{spec.api_name}/{batch_id} reached row limit {spec.max_rows}; "
                            "split the request window before publishing"
                        )
                    if frame.height:
                        _atomic_parquet(frame, path)
                        status = "completed"
                        empty_reason = None
                    else:
                        schema_confirms_empty = bool(response.fields)
                        status = (
                            "valid_empty"
                            if spec.empty_is_valid or schema_confirms_empty
                            else "empty_unconfirmed"
                        )
                        empty_reason = (
                            "preflight_confirmed_dataset_absent"
                            if spec.empty_is_valid
                            else "provider_confirmed_empty_with_schema"
                            if schema_confirms_empty
                            else "successful_request_without_rows_unconfirmed"
                        )
                        if status == "empty_unconfirmed":
                            empty_unconfirmed.append(batch_id)
                    rows += frame.height
                    rejected += int(audit["rejected_rows"])
                    record_ingestion_batch(
                        self.config.data_dir,
                        SOURCE,
                        spec.api_name,
                        self.config.run_id,
                        batch_id,
                        status=status,
                        row_count=frame.height,
                        content_hash=audit["content_hash"],
                        source_content_hash=source_hash,
                        empty_reason=empty_reason,
                        request_params_hash=stable_content_hash(params),
                        audit=audit,
                    )
                    self._append_next_page(spec, requests, batch_id, params, frame.height)
                except Exception as exc:  # noqa: BLE001
                    failures.append(batch_id)
                    record_ingestion_batch(
                        self.config.data_dir,
                        SOURCE,
                        spec.api_name,
                        self.config.run_id,
                        batch_id,
                        status="failed",
                        error_code=type(exc).__name__,
                        error_message=str(exc)[:240],
                        request_params_hash=stable_content_hash(params),
                    )
            # Empty windows are valid when the response carried the endpoint's
            # schema, or after another batch proved the same endpoint non-empty.
            # A wholly empty response without fields remains unconfirmed.
            state = load_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
            )
            batches = dict(state.get("batches") or {})
            if rows > 0 and empty_unconfirmed:
                for batch_id in empty_unconfirmed:
                    batch = dict(batches.get(batch_id) or {})
                    batch["status"] = "valid_empty"
                    batch["empty_reason"] = "endpoint_confirmed_by_nonempty_batch"
                    batches[batch_id] = batch
                empty_unconfirmed = []
            status = "failed" if failures else "blocked" if empty_unconfirmed else "completed"
            manifest = update_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
                status=status,
                staged_rows=rows,
                rejected_rows=rejected,
                failed_batches=failures,
                empty_unconfirmed_batches=empty_unconfirmed,
                requested_batches=len(requests),
                batches=batches,
            )
            summary[spec.api_name] = {
                "status": status,
                "rows": rows,
                "rejected_rows": rejected,
                "failed_batches": failures,
                "manifest": str(
                    self.config.data_dir
                    / "ext_data"
                    / "_ingestion"
                    / SOURCE
                    / spec.api_name
                    / f"{self.config.run_id}.json"
                ),
            }
            if failures:
                # Datasets are independent; keep collecting the next one. The
                # failed dataset itself remains impossible to publish.
                manifest["status"] = "failed"
        return summary

    @staticmethod
    def _append_next_page(
        spec: TushareDatasetSpec,
        requests: list[tuple[str, dict[str, Any]]],
        batch_id: str,
        params: Mapping[str, Any],
        row_count: int,
    ) -> None:
        if spec.api_name not in _INCREMENTAL_PAGED_APIS or row_count < _PAGE_SIZE:
            return
        offset = int(params.get("offset") or 0)
        page_number = offset // _PAGE_SIZE + 1
        if page_number >= _MAX_PAGES_PER_BATCH:
            raise TushareIngestionBlocked(
                f"{spec.api_name}/{batch_id} exceeded {_MAX_PAGES_PER_BATCH} pages"
            )
        next_offset = offset + _PAGE_SIZE
        base_id = batch_id.rsplit("-offset-", 1)[0]
        next_request = (f"{base_id}-offset-{next_offset}", {**params, "offset": next_offset})
        if next_request[0] not in {item[0] for item in requests}:
            requests.append(next_request)

    def _staged(self, spec: TushareDatasetSpec) -> pl.DataFrame:
        paths = sorted((self.run_root / "datasets" / spec.api_name).glob("*.parquet"))
        if not paths:
            return _empty_frame(spec)
        return pl.concat([pl.read_parquet(path) for path in paths], how="diagonal_relaxed")

    def _assert_publishable(self, spec: TushareDatasetSpec) -> dict[str, Any]:
        manifest = load_ingestion_manifest(
            self.config.data_dir,
            SOURCE,
            spec.api_name,
            self.config.run_id,
        )
        if manifest.get("status") != "completed" or manifest.get("failed_batches"):
            raise TushareIngestionBlocked(
                f"{spec.api_name} is not fully staged: {manifest.get('status') or 'missing'}"
            )
        return manifest

    def publish(self, specs: Iterable[TushareDatasetSpec]) -> dict[str, Any]:
        if not self.config.publish:
            return {spec.api_name: {"status": "staged_only"} for spec in specs}
        results: dict[str, Any] = {}
        for spec in specs:
            self._assert_publishable(spec)
            frame = self._staged(spec)
            if spec.kind == "extension":
                result = self._publish_extension(spec, frame)
            elif spec.kind == "daily":
                result = self._publish_daily(spec, frame)
            elif spec.kind == "financial":
                result = self._publish_financial(spec, frame)
            elif spec.kind in {"pit_index", "pit_industry"}:
                result = self._publish_pit(spec, frame)
            elif spec.kind == "reference":
                result = self._publish_reference(spec, frame)
            elif spec.kind == "corporate_action":
                result = self._publish_corporate_action(spec, frame)
            else:
                result = {"status": "staged_only", "rows": frame.height}
            update_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
                status="published",
                published_rows=int(result.get("published_rows") or 0),
                publish_report=result,
            )
            results[spec.api_name] = result
        self.write_capability_matrix(tuple(specs))
        return results

    def _publish_extension(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0, "partitions": 0}
        ensure_extension_configs(self.config.data_dir, (spec,))
        logical_dates = [_parse_date(item) for item in frame[spec.logical_date].to_list()]
        if any(item is None for item in logical_dates):
            raise TushareIngestionBlocked(f"{spec.api_name} contains invalid logical dates")
        frame = frame.with_columns(pl.Series("_partition_date", logical_dates, dtype=pl.Date))
        pending: list[tuple[Path, pl.DataFrame]] = []
        key = list(spec.normalized_primary_key)
        added = 0
        for day in sorted(set(logical_dates)):
            assert day is not None
            incoming = frame.filter(pl.col("_partition_date") == day).drop("_partition_date")
            path = (
                self.config.data_dir
                / "ext_data"
                / spec.table_id
                / "timeseries"
                / f"date={day.isoformat()}"
                / "part.parquet"
            )
            existing = pl.read_parquet(path) if path.exists() else pl.DataFrame()
            merged, report = _merge_existing_wins(
                existing,
                incoming,
                key=key,
                compare_columns=[
                    field.name for field in spec.fields if field.name not in spec.overlap_fields
                ],
                label=spec.table_id,
                allow_revisions=spec.revisioned,
            )
            if report["conflicts"]:
                raise TushareIngestionBlocked(
                    f"{spec.table_id} overlap conflicts: {report['conflicts'][:5]}"
                )
            added += report["added_rows"]
            pending.append((path, merged))
        supplements: dict[str, Any] = {}
        if spec.api_name == "daily_basic":
            supplement_files, supplements = self._prepare_daily_basic_supplements(frame)
            pending.extend(supplement_files)
        elif spec.api_name == "index_weight":
            membership_file, membership_rows = self._prepare_index_weight_membership(frame)
            pending.append(membership_file)
            supplements["index_membership_rows"] = membership_rows
        self._publish_files(pending)
        ensure_extension_configs(self.config.data_dir, (spec,), factor_ready=True)
        return {
            "status": "published",
            "published_rows": added,
            "partitions": len(pending),
            "policy": "existing rows win; revisions append by source_revision_hash",
            "canonical_supplements": supplements,
        }

    def _prepare_daily_basic_supplements(
        self,
        frame: pl.DataFrame,
    ) -> tuple[list[tuple[Path, pl.DataFrame]], dict[str, Any]]:
        pending: list[tuple[Path, pl.DataFrame]] = []
        report: dict[str, Any] = {}

        shares = frame.select(
            "symbol",
            pl.col("trade_date").alias("period_end"),
            pl.col("trade_date").alias("announce_date"),
            (pl.col("total_share") * 10_000.0).alias("total_shares"),
            (pl.col("float_share") * 10_000.0).alias("float_shares"),
        ).filter(pl.col("total_shares").is_not_null() | pl.col("float_shares").is_not_null())
        shares_target = self.config.data_dir / "financials" / "shares" / "part.parquet"
        shares_existing = (
            pl.read_parquet(shares_target) if shares_target.exists() else pl.DataFrame()
        )
        shares_merged, shares_report = _merge_existing_wins(
            shares_existing,
            shares,
            key=["symbol", "period_end", "announce_date"],
            compare_columns=["total_shares", "float_shares"],
            label="financials/shares",
        )
        if shares_report["conflicts"]:
            raise TushareIngestionBlocked(
                f"daily_basic share-capital conflicts: {shares_report['conflicts'][:5]}"
            )
        pending.append((shares_target, shares_merged))
        report["shares_added_rows"] = shares_report["added_rows"]

        from app.services.daily_valuation import VALUATION_DAILY_SCHEMA

        valuation = frame.select(
            pl.col("symbol"),
            pl.col("trade_date").str.to_date(strict=False).alias("date"),
            pl.col("close").alias("raw_close"),
            (pl.col("total_share") * 10_000.0).alias("total_shares"),
            (pl.col("float_share") * 10_000.0).alias("float_shares"),
            (pl.col("total_mv") * 10_000.0).alias("market_cap"),
            (pl.col("circ_mv") * 10_000.0).alias("float_market_cap"),
            pl.when(pl.col("total_share") > 0)
            .then(pl.col("float_share") / pl.col("total_share"))
            .otherwise(None)
            .alias("float_share_ratio"),
            pl.col("pe_ttm"),
            pl.col("pb"),
            pl.col("ps_ttm"),
        )
        for column, dtype in VALUATION_DAILY_SCHEMA.items():
            if column not in valuation.columns:
                valuation = valuation.with_columns(pl.lit(None, dtype=dtype).alias(column))
            else:
                valuation = valuation.with_columns(pl.col(column).cast(dtype, strict=False))
        valuation = valuation.select(list(VALUATION_DAILY_SCHEMA))
        valuation_added = 0
        valuation_conflicts: list[dict[str, Any]] = []
        for day_frame in valuation.partition_by("date"):
            day = day_frame["date"][0]
            valuation_target = (
                self.config.data_dir
                / "valuation_daily"
                / f"date={day.isoformat()}"
                / "part.parquet"
            )
            valuation_existing = (
                pl.read_parquet(valuation_target) if valuation_target.exists() else pl.DataFrame()
            )
            valuation_merged, valuation_report = _merge_existing_wins(
                valuation_existing,
                day_frame,
                key=["symbol", "date"],
                compare_columns=[
                    "raw_close",
                    "total_shares",
                    "float_shares",
                    "market_cap",
                    "float_market_cap",
                    "pe_ttm",
                    "pb",
                    "ps_ttm",
                ],
                label="valuation_daily",
            )
            valuation_added += valuation_report["added_rows"]
            valuation_conflicts.extend(valuation_report["conflicts"])
            pending.append((valuation_target, valuation_merged))
        if valuation_conflicts:
            raise TushareIngestionBlocked(
                f"daily_basic valuation conflicts: {valuation_conflicts[:5]}"
            )
        report["valuation_added_rows"] = valuation_added
        return pending, report

    def _prepare_index_weight_membership(
        self,
        frame: pl.DataFrame,
    ) -> tuple[tuple[Path, pl.DataFrame], int]:
        history_paths = sorted(
            (
                self.config.data_dir
                / "ext_data"
                / DATASET_SPECS["index_weight"].table_id
                / "timeseries"
            ).glob("date=*/part.parquet")
        )
        history_frames = [pl.read_parquet(path) for path in history_paths]
        history_frames.append(frame)
        frame = (
            pl.concat(history_frames, how="diagonal_relaxed")
            .unique(
                subset=["index_symbol", "member_symbol", "trade_date"],
                keep="last",
            )
            .sort(["index_symbol", "trade_date", "member_symbol"])
        )
        incoming = normalize_index_membership_history(
            frame.to_dicts(),
            source=SOURCE,
        )
        validation = validate_index_membership_history(incoming)
        if not validation["usable"]:
            raise TushareIngestionBlocked(
                "index_weight cannot publish incomplete daily membership snapshots: "
                f"{validation['invalid_snapshot_dates'][:5]}"
            )
        target = (
            self.config.data_dir
            / "pit_reference"
            / "history"
            / INDEX_MEMBERSHIP_HISTORY_TABLE
            / "part.parquet"
        )
        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        try:
            merged, report = merge_index_membership_frames(existing, incoming)
        except ValueError as exc:
            raise TushareIngestionBlocked(str(exc)) from exc
        return (target, merged), report["added_rows"]

    def _has_valid_empty_publication(self, spec: TushareDatasetSpec) -> bool:
        root = self.config.data_dir / "ext_data" / "_ingestion" / SOURCE / spec.api_name
        for path in sorted(root.glob("*.json"), reverse=True):
            try:
                manifest = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            publish_report = manifest.get("publish_report") or {}
            if (
                manifest.get("status") == "published"
                and publish_report.get("status") == "valid_empty"
                and not manifest.get("failed_batches")
                and not manifest.get("empty_unconfirmed_batches")
            ):
                return True
        return False

    def _publish_daily(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0, "partitions": 0}
        columns = ["symbol", "date", *_DAILY_VALUES]
        incoming_all = frame.select(columns).with_columns(
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        )
        if incoming_all["date"].null_count():
            raise TushareIngestionBlocked(f"{spec.api_name} has invalid trade dates")
        pending: list[tuple[Path, pl.DataFrame]] = []
        added = 0
        conflicts: list[dict[str, Any]] = []
        for day in sorted(set(incoming_all["date"].to_list())):
            incoming = incoming_all.filter(pl.col("date") == day)
            path = (
                self.config.data_dir
                / str(spec.canonical_target)
                / f"date={day.isoformat()}"
                / "part.parquet"
            )
            existing = pl.read_parquet(path) if path.exists() else pl.DataFrame()
            merged, report = _merge_daily_existing_wins(existing, incoming)
            added += report["added_rows"]
            conflicts.extend(report["conflicts"])
            pending.append((path, merged))
        if conflicts:
            raise TushareIngestionBlocked(
                f"{spec.api_name} canonical overlap conflicts: {conflicts[:5]}"
            )
        self._publish_files(pending)
        return {
            "status": "published",
            "published_rows": added,
            "partitions": len(pending),
            "policy": "TickFlow existing keys win",
        }

    def _publish_financial(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0}
        canonical_columns = [field.name for field in spec.fields]
        incoming = frame.select(canonical_columns)
        metadata_columns = [
            column
            for column in ("provider_revision_flag", "source_revision_hash")
            if column in frame.columns
        ]
        if metadata_columns:
            incoming = incoming.with_columns(
                [frame[column].alias(column) for column in metadata_columns]
            )
        value_columns = [column for column in canonical_columns if column not in spec.primary_key]
        ranked = incoming.with_columns(
            pl.col("provider_revision_flag")
            .cast(pl.Int64, strict=False)
            .fill_null(-1)
            .alias("_provider_revision_rank")
            if "provider_revision_flag" in incoming.columns
            else pl.lit(-1, dtype=pl.Int64).alias("_provider_revision_rank")
        ).with_columns(
            pl.col("_provider_revision_rank")
            .max()
            .over(list(spec.primary_key))
            .alias("_max_provider_revision_rank")
        )
        top_ranked = ranked.filter(
            pl.col("_provider_revision_rank") == pl.col("_max_provider_revision_rank")
        )
        duplicate_conflicts = (
            top_ranked.group_by(list(spec.primary_key))
            .agg(pl.struct(value_columns).n_unique().alias("versions"))
            .filter(pl.col("versions") > 1)
        )
        if not duplicate_conflicts.is_empty():
            raise TushareIngestionBlocked(
                f"{spec.api_name} has same-version revisions with different values: "
                f"{duplicate_conflicts.head(5).to_dicts()}"
            )
        # Select the highest provider revision deterministically. Every raw
        # version remains queryable in the revision archive by content hash.
        incoming = (
            top_ranked.sort([*spec.primary_key, "_provider_revision_rank", "source_revision_hash"])
            .unique(subset=list(spec.primary_key), keep="last")
            .select(canonical_columns)
            .sort(list(spec.primary_key))
        )
        target = self.config.data_dir / str(spec.canonical_target)
        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        if not existing.is_empty():
            for column, dtype in existing.schema.items():
                if column not in incoming.columns:
                    incoming = incoming.with_columns(pl.lit(None, dtype=dtype).alias(column))
                else:
                    incoming = incoming.with_columns(pl.col(column).cast(dtype, strict=False))
            incoming = incoming.select(existing.columns)
        merged, report = _merge_existing_wins(
            existing,
            incoming,
            key=list(spec.primary_key),
            compare_columns=[
                column for column in incoming.columns if column not in spec.primary_key
            ],
            label=str(spec.canonical_target),
        )
        if report["conflicts"]:
            raise TushareIngestionBlocked(
                f"{spec.api_name} canonical overlap conflicts: {report['conflicts'][:5]}"
            )
        revision_target = (
            self.config.data_dir / "financials" / "_revisions" / spec.api_name / "part.parquet"
        )
        revision_existing = (
            pl.read_parquet(revision_target) if revision_target.exists() else pl.DataFrame()
        )
        revision_merged, revision_report = _merge_existing_wins(
            revision_existing,
            frame,
            key=list(spec.normalized_primary_key),
            compare_columns=[],
            label=f"financials/_revisions/{spec.api_name}",
            compare_overlap=False,
        )
        self._publish_files([(target, merged), (revision_target, revision_merged)])
        return {
            "status": "published",
            "published_rows": report["added_rows"],
            "revision_rows": revision_report["added_rows"],
            "policy": "TickFlow existing keys win",
        }

    def _publish_pit(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0}
        if spec.kind == "pit_index":
            incoming = normalize_index_membership_history(frame.to_dicts(), source=SOURCE)
            validation = validate_index_membership_history(incoming)
            if not validation["usable"]:
                raise TushareIngestionBlocked(
                    "PIT index dataset cannot publish incomplete daily membership snapshots: "
                    f"{validation['invalid_snapshot_dates'][:5]}"
                )
            target = (
                self.config.data_dir
                / "pit_reference"
                / "history"
                / INDEX_MEMBERSHIP_HISTORY_TABLE
                / "part.parquet"
            )
            existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
            try:
                merged, report = merge_index_membership_frames(existing, incoming)
            except ValueError as exc:
                raise TushareIngestionBlocked(str(exc)) from exc
            self._publish_files([(target, merged)])
            return {
                "status": "published",
                "published_rows": report["added_rows"],
                "policy": "complete new dates append; same-date conflicts reject",
            }

        incoming = normalize_industry_membership_history(
            frame.to_dicts(),
            source=SOURCE,
        )
        table = INDUSTRY_MEMBERSHIP_HISTORY_TABLE
        key = ["member_symbol", "industry_standard", "effective_from"]
        if spec.api_name == "index_member_all":
            key[2:2] = ["industry_standard_code", "industry_level"]
        if not incoming.is_empty() and spec.kind == "pit_industry":
            incoming = incoming.with_columns(pl.lit(self.config.end).alias("source_snapshot_date"))
        target = self.config.data_dir / "pit_reference" / "history" / table / "part.parquet"
        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        merged, report = _merge_existing_wins(
            existing,
            incoming,
            key=key,
            compare_columns=[column for column in incoming.columns if column not in key],
            label=table,
            compare_overlap=False,
        )
        self._publish_files([(target, merged)])
        return {
            "status": "published",
            "published_rows": report["added_rows"],
            "policy": "existing PIT events win on overlapping keys",
        }

    def _publish_corporate_action(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if spec.api_name != "dividend":
            raise TushareIngestionBlocked(
                f"unsupported corporate action publisher: {spec.api_name}"
            )
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0}
        incoming = (
            frame.select(
                "symbol",
                pl.col("ex_date").str.to_date(strict=False).alias("event_date"),
                pl.col("cash_div").alias("cash_per_share"),
                pl.col("source_revision_hash").alias("source_record"),
            )
            .filter(
                pl.col("event_date").is_not_null()
                & pl.col("cash_per_share").is_finite()
                & (pl.col("cash_per_share") > 0)
            )
            .unique(subset=["symbol", "event_date"], keep="last")
        )
        if incoming.is_empty():
            raise TushareIngestionBlocked(
                "dividend rows lack a verified ex-date and positive per-share cash amount"
            )
        target = self.config.data_dir / str(spec.canonical_target)
        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        merged, report = _merge_existing_wins(
            existing,
            incoming,
            key=["symbol", "event_date"],
            compare_columns=["cash_per_share"],
            label=str(spec.canonical_target),
        )
        if report["conflicts"]:
            raise TushareIngestionBlocked(
                f"dividend canonical overlap conflicts: {report['conflicts'][:5]}"
            )
        self._publish_files([(target, merged)])
        return {
            "status": "published",
            "published_rows": report["added_rows"],
            "policy": "TickFlow existing event-date dividends win",
        }

    def _publish_reference(
        self,
        spec: TushareDatasetSpec,
        frame: pl.DataFrame,
    ) -> dict[str, Any]:
        if frame.is_empty():
            return {"status": "valid_empty", "published_rows": 0}
        snapshot_date = self.config.end
        if spec.api_name in {"stock_basic", "etf_basic", "index_basic"}:
            asset_type = {
                "stock_basic": "stock",
                "etf_basic": "etf",
                "index_basic": "index",
            }[spec.api_name]
            incoming = frame.drop("collected_at", "source_revision_hash").with_columns(
                pl.col("symbol").str.split(".").list.first().alias("code"),
                pl.col("symbol").str.split(".").list.last().alias("exchange"),
                pl.lit(asset_type).alias("asset_type"),
                pl.lit(snapshot_date).alias("as_of"),
            )
            for column in ("list_date", "delist_date"):
                if column in incoming.columns:
                    incoming = incoming.with_columns(
                        pl.col(column).str.to_date(strict=False).alias(column)
                    )
            if "list_date" in incoming.columns:
                incoming = incoming.with_columns(pl.col("list_date").alias("listing_date"))
            if asset_type == "stock":
                incoming = incoming.with_columns(
                    pl.lit("stock").alias("type"),
                    pl.when(pl.col("list_status") == "D")
                    .then(pl.lit("delisted"))
                    .when(pl.col("list_status") == "P")
                    .then(pl.lit("paused"))
                    .otherwise(pl.lit("active"))
                    .alias("status"),
                )
            target = self.config.data_dir / str(spec.canonical_target)
            lifecycle_pending: tuple[Path, pl.DataFrame] | None = None
            if asset_type == "stock":
                lifecycle_rows = []
                for row in frame.to_dicts():
                    lifecycle_rows.append(
                        {
                            "symbol": row.get("symbol"),
                            "name": row.get("name"),
                            "exchange": row.get("exchange"),
                            "listed_date": row.get("list_date"),
                            "delisted_date": row.get("delist_date"),
                        }
                    )
                lifecycle = normalize_instrument_lifecycle_events(
                    lifecycle_rows,
                    source=SOURCE,
                )
                if not lifecycle.is_empty():
                    lifecycle_target = (
                        self.config.data_dir
                        / "pit_reference"
                        / "history"
                        / INSTRUMENT_LIFECYCLE_EVENTS_TABLE
                        / "part.parquet"
                    )
                    lifecycle_existing = (
                        pl.read_parquet(lifecycle_target)
                        if lifecycle_target.exists()
                        else pl.DataFrame()
                    )
                    lifecycle, _ = _merge_existing_wins(
                        lifecycle_existing,
                        lifecycle,
                        key=["symbol", "event_type", "event_date"],
                        compare_columns=[],
                        label=INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
                        compare_overlap=False,
                    )
                    lifecycle_pending = (lifecycle_target, lifecycle)
        elif spec.api_name == "trade_cal":
            incoming = frame.drop("collected_at", "source_revision_hash").with_columns(
                pl.col("cal_date").str.to_date(strict=False),
                pl.col("pretrade_date").str.to_date(strict=False),
                pl.lit(snapshot_date).alias("source_snapshot_date"),
            )
            target = self.config.data_dir / str(spec.canonical_target)
            lifecycle_pending = None
        elif spec.api_name == "namechange":
            incoming = (
                frame.sort(["symbol", "start_date"])
                .with_columns(
                    pl.col("start_date").str.to_date(strict=False).alias("change_date"),
                    pl.col("name").shift(1).over("symbol").alias("before_name"),
                    pl.col("name").alias("after_name"),
                    pl.lit(snapshot_date).alias("source_snapshot_date"),
                )
                .select(
                    "symbol",
                    "change_date",
                    "before_name",
                    "after_name",
                    "source",
                    "source_snapshot_date",
                )
            )
            target = self.config.data_dir / str(spec.canonical_target)
            lifecycle_pending = None
        else:
            raise TushareIngestionBlocked(f"unsupported reference publisher: {spec.api_name}")

        existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        key = list(spec.primary_key)
        if spec.api_name == "namechange":
            key = ["symbol", "change_date"]
        common = [
            column
            for column in incoming.columns
            if column in existing.columns and column not in key
        ]
        merged, report = _merge_existing_wins(
            existing,
            incoming,
            key=key,
            compare_columns=common,
            label=str(spec.canonical_target),
        )
        if report["conflicts"]:
            raise TushareIngestionBlocked(
                f"{spec.api_name} canonical overlap conflicts: {report['conflicts'][:5]}"
            )
        pending = [(target, merged)]
        if lifecycle_pending is not None:
            pending.append(lifecycle_pending)
        self._publish_files(pending)
        return {
            "status": "published",
            "published_rows": report["added_rows"],
            "policy": "TickFlow existing keys win",
        }

    def _publish_files(self, pending: list[tuple[Path, pl.DataFrame]]) -> None:
        backup_root = self.run_root / "backups"
        staged_root = self.run_root / "publish_staging"
        prepared: list[tuple[Path, Path, Path | None]] = []
        for target, frame in pending:
            try:
                relative = target.relative_to(self.config.data_dir)
            except ValueError as exc:
                raise TushareIngestionBlocked("publish target escaped data directory") from exc
            staged = staged_root / relative
            _atomic_parquet(frame, staged)
            backup = backup_root / relative if target.exists() else None
            prepared.append((target, staged, backup))

        published: list[tuple[Path, Path | None]] = []
        try:
            for target, staged, backup in prepared:
                target.parent.mkdir(parents=True, exist_ok=True)
                if backup is not None:
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    if backup.exists():
                        if backup.is_dir():
                            shutil.rmtree(backup)
                        else:
                            backup.unlink()
                    os.replace(target, backup)
                published.append((target, backup))
                os.replace(staged, target)
        except Exception:
            for target, backup in reversed(published):
                if target.exists():
                    target.unlink()
                if backup is not None and backup.exists():
                    os.replace(backup, target)
            raise

    def audit(self, specs: Iterable[TushareDatasetSpec]) -> dict[str, Any]:
        datasets: dict[str, Any] = {}
        issues: list[dict[str, Any]] = []
        calendar_path = (
            self.config.data_dir / "pit_reference" / "history" / "trade_calendar" / "part.parquet"
        )
        open_dates: set[date] = set()
        if calendar_path.exists():
            calendar = pl.read_parquet(calendar_path)
            if {"cal_date", "is_open"} <= set(calendar.columns):
                open_dates = {
                    parsed
                    for row in calendar.select("cal_date", "is_open").to_dicts()
                    if bool(row["is_open"]) and (parsed := _parse_date(row["cal_date"])) is not None
                }
        for spec in specs:
            if spec.kind == "extension":
                paths = sorted(
                    (self.config.data_dir / "ext_data" / spec.table_id / "timeseries").glob(
                        "date=*/part.parquet"
                    )
                )
            elif spec.kind == "daily":
                paths = sorted(
                    (self.config.data_dir / str(spec.canonical_target)).glob("date=*/part.parquet")
                )
            elif spec.kind == "financial":
                target = self.config.data_dir / str(spec.canonical_target)
                paths = [target] if target.exists() else []
            elif spec.kind in {"pit_index", "pit_industry"}:
                table = (
                    INDEX_MEMBERSHIP_HISTORY_TABLE
                    if spec.kind == "pit_index"
                    else INDUSTRY_MEMBERSHIP_HISTORY_TABLE
                )
                target = self.config.data_dir / "pit_reference" / "history" / table / "part.parquet"
                paths = [target] if target.exists() else []
            else:
                target = self.config.data_dir / str(spec.canonical_target)
                paths = [target] if target.exists() else []
            if not paths:
                if self._has_valid_empty_publication(spec):
                    datasets[spec.api_name] = {
                        "status": "valid_empty",
                        "rows": 0,
                        "partitions": 0,
                    }
                    continue
                item = {"status": "missing", "rows": 0, "partitions": 0}
                datasets[spec.api_name] = item
                issues.append({"dataset": spec.api_name, **item})
                continue
            frame = pl.concat([pl.read_parquet(path) for path in paths], how="diagonal_relaxed")
            key = list(
                spec.normalized_primary_key if spec.kind == "extension" else spec.primary_key
            )
            if spec.api_name == "namechange":
                key = ["symbol", "change_date"]
            elif spec.kind == "corporate_action":
                key = ["symbol", "event_date"]
            missing_keys = [column for column in key if column not in frame.columns]
            duplicate_keys = 0
            null_keys = 0
            if not missing_keys:
                duplicate_keys = int(frame.group_by(key).len().filter(pl.col("len") > 1).height)
                null_keys = int(
                    frame.filter(
                        pl.any_horizontal(pl.col(column).is_null() for column in key)
                    ).height
                )
            status = (
                "healthy"
                if not missing_keys and not duplicate_keys and not null_keys
                else "unhealthy"
            )
            item = {
                "status": status,
                "rows": frame.height,
                "partitions": len(paths),
                "missing_key_columns": missing_keys,
                "duplicate_keys": duplicate_keys,
                "null_key_rows": null_keys,
            }
            if spec.kind == "daily" and open_dates and "date" in frame.columns:
                observed_dates = {
                    parsed
                    for value in frame["date"].to_list()
                    if (parsed := _parse_date(value)) is not None
                }
                if observed_dates:
                    earliest = min(observed_dates)
                    latest = max(observed_dates)
                    missing_dates = sorted(
                        day
                        for day in open_dates
                        if earliest <= day <= latest and day not in observed_dates
                    )
                    item["missing_open_date_count"] = len(missing_dates)
                    item["missing_open_dates"] = [day.isoformat() for day in missing_dates[:20]]
                    if missing_dates:
                        item["status"] = "unhealthy"
            if spec.kind == "financial" and {"period_end", "announce_date"} <= set(frame.columns):
                front_look_rows = sum(
                    1
                    for period_value, announce_value in zip(
                        frame["period_end"].to_list(),
                        frame["announce_date"].to_list(),
                        strict=True,
                    )
                    if (period := _parse_date(period_value)) is not None
                    and (announced := _parse_date(announce_value)) is not None
                    and announced < period
                )
                item["pit_front_look_rows"] = front_look_rows
                if front_look_rows:
                    item["status"] = "unhealthy"
            if spec.kind == "pit_index":
                membership_validation = validate_index_membership_history(frame)
                item["membership_validation"] = membership_validation
                if not membership_validation["usable"]:
                    item["status"] = "unhealthy"
            elif spec.kind == "pit_industry":
                interval_columns = ["member_symbol", "industry_standard"]
                for column in ("industry_standard_code", "industry_level"):
                    if column in frame.columns:
                        interval_columns.append(column)
                invalid_intervals, overlap_intervals = _interval_defects(
                    frame,
                    tuple(interval_columns),
                )
                item["invalid_intervals"] = invalid_intervals
                item["overlap_intervals"] = overlap_intervals
                if invalid_intervals or overlap_intervals:
                    item["status"] = "unhealthy"
            if spec.api_name == "index_weight":
                membership_path = (
                    self.config.data_dir
                    / "pit_reference"
                    / "history"
                    / INDEX_MEMBERSHIP_HISTORY_TABLE
                    / "part.parquet"
                )
                memberships = (
                    pl.read_parquet(membership_path) if membership_path.exists() else pl.DataFrame()
                )
                membership_validation = validate_index_membership_history(memberships)
                item["pit_membership_validation"] = membership_validation
                if not membership_validation["usable"]:
                    item["status"] = "unhealthy"
            datasets[spec.api_name] = item
            if item["status"] != "healthy":
                issues.append({"dataset": spec.api_name, **item})
        pit_checks: dict[str, Any] = {}
        valuation_paths = sorted(
            (self.config.data_dir / "valuation_daily").glob("date=*/part.parquet")
        )
        if valuation_paths:
            lookahead_rows = 0
            for path in valuation_paths:
                valuation = pl.read_parquet(path)
                if "date" not in valuation.columns:
                    continue
                announcement_columns = [
                    column for column in valuation.columns if column.endswith("_announce_date")
                ]
                for row in valuation.select("date", *announcement_columns).to_dicts():
                    visible_date = _parse_date(row["date"])
                    if visible_date is None:
                        continue
                    if any(
                        (announced := _parse_date(row[column])) is not None
                        and announced > visible_date
                        for column in announcement_columns
                    ):
                        lookahead_rows += 1
            pit_checks["valuation_lookahead_rows"] = lookahead_rows
            if lookahead_rows:
                issues.append(
                    {
                        "dataset": "valuation_daily",
                        "status": "unhealthy",
                        "pit_front_look_rows": lookahead_rows,
                    }
                )
        report = {
            "status": "healthy" if not issues else "unhealthy",
            "generated_at": _utc_now(),
            "datasets": datasets,
            "pit_checks": pit_checks,
            "issues": issues,
        }
        _atomic_json(report, self.run_root / "weekly_audit.json")
        return report

    def write_capability_matrix(
        self,
        specs: Iterable[TushareDatasetSpec],
    ) -> dict[str, Any]:
        datasets: dict[str, Any] = {}
        for spec in specs:
            manifest = load_ingestion_manifest(
                self.config.data_dir,
                SOURCE,
                spec.api_name,
                self.config.run_id,
            )
            batches = manifest.get("batches") or {}
            frame = self._staged(spec)
            non_null = {
                column: round(1.0 - (frame[column].null_count() / frame.height), 6)
                for column in frame.columns
                if frame.height
            }
            logical_dates = (
                [_parse_date(value) for value in frame[spec.logical_date].to_list()]
                if frame.height and spec.logical_date in frame.columns
                else []
            )
            valid_dates = [value for value in logical_dates if value is not None]
            datasets[spec.api_name] = {
                "status": manifest.get("status", "missing"),
                "staged_rows": int(manifest.get("staged_rows") or 0),
                "published_rows": int(manifest.get("published_rows") or 0),
                "batches": len(batches),
                "failed_batches": list(manifest.get("failed_batches") or []),
                "empty_unconfirmed_batches": list(manifest.get("empty_unconfirmed_batches") or []),
                "logical_date": spec.logical_date,
                "primary_key": list(spec.normalized_primary_key),
                "symbols": int(frame["symbol"].n_unique())
                if frame.height and "symbol" in frame.columns
                else 0,
                "min_date": min(valid_dates).isoformat() if valid_dates else None,
                "max_date": max(valid_dates).isoformat() if valid_dates else None,
                "field_non_null_rate": non_null,
                "factor_input": bool(spec.factor_input and manifest.get("status") == "published"),
            }
        matrix = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "source": SOURCE,
            "runtime_source": "local_parquet_only",
            "history_start": self.config.start.isoformat(),
            "history_end": self.config.end.isoformat(),
            "datasets": datasets,
        }
        _atomic_json(matrix, self.run_root / "capability_matrix.json")
        return matrix


def _numeric_different(left: Any, right: Any, tolerance: float = 1e-6) -> bool:
    if left is None or right is None:
        return left is not right
    try:
        lvalue = float(left)
        rvalue = float(right)
    except (TypeError, ValueError):
        return str(left) != str(right)
    if math.isnan(lvalue) and math.isnan(rvalue):
        return False
    allowed = max(tolerance, abs(lvalue) * 1e-9, abs(rvalue) * 1e-9)
    return abs(lvalue - rvalue) > allowed


def _merge_existing_wins(
    existing: pl.DataFrame,
    incoming: pl.DataFrame,
    *,
    key: list[str],
    compare_columns: list[str],
    label: str,
    allow_revisions: bool = False,
    compare_overlap: bool = True,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    for frame_name, frame in (("existing", existing), ("incoming", incoming)):
        missing = [column for column in key if column not in frame.columns]
        if frame.is_empty():
            continue
        if missing:
            raise TushareIngestionBlocked(f"{label} {frame_name} missing key columns: {missing}")
        if frame.filter(pl.any_horizontal(pl.col(column).is_null() for column in key)).height:
            raise TushareIngestionBlocked(f"{label} {frame_name} has null primary keys")
        duplicates = frame.group_by(key).len().filter(pl.col("len") > 1)
        if not duplicates.is_empty():
            raise TushareIngestionBlocked(
                f"{label} {frame_name} has duplicate keys: {duplicates.head(5).to_dicts()}"
            )
    if existing.is_empty():
        return incoming.sort(key), {
            "added_rows": incoming.height,
            "overlap_rows": 0,
            "conflicts": [],
        }
    if incoming.is_empty():
        return existing.sort(key), {"added_rows": 0, "overlap_rows": 0, "conflicts": []}
    overlap = existing.join(incoming, on=key, how="inner", suffix="_incoming")
    conflicts: list[dict[str, Any]] = []
    if compare_overlap and not allow_revisions:
        for row in overlap.head(10_000).to_dicts():
            changed = []
            for column in compare_columns:
                incoming_column = f"{column}_incoming"
                if column not in row or incoming_column not in row:
                    continue
                if _numeric_different(row[column], row[incoming_column]):
                    changed.append(column)
            if changed:
                conflicts.append({**{column: row[column] for column in key}, "columns": changed})
                if len(conflicts) >= 100:
                    break
    missing = incoming.join(existing.select(key), on=key, how="anti")
    merged = pl.concat([existing, missing], how="diagonal_relaxed").sort(key)
    return merged, {
        "added_rows": missing.height,
        "overlap_rows": overlap.height,
        "conflicts": conflicts,
    }


def _merge_daily_existing_wins(
    existing: pl.DataFrame,
    incoming: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    key = ["symbol", "date"]
    if existing.is_empty():
        return incoming.sort(key), {
            "added_rows": incoming.height,
            "overlap_rows": 0,
            "conflicts": [],
        }
    overlap = existing.join(incoming, on=key, how="inner", suffix="_incoming")
    conflicts: list[dict[str, Any]] = []
    for row in overlap.to_dicts():
        changed = [
            column
            for column in _DAILY_VALUES
            if _numeric_different(
                row.get(column),
                row.get(f"{column}_incoming"),
                _DAILY_TOLERANCES[column],
            )
        ]
        if changed:
            conflicts.append(
                {"symbol": row["symbol"], "date": str(row["date"]), "columns": changed}
            )
            if len(conflicts) >= 100:
                break
    missing = incoming.join(existing.select(key), on=key, how="anti")
    merged = pl.concat([existing, missing], how="diagonal_relaxed").sort(key)
    return merged, {
        "added_rows": missing.height,
        "overlap_rows": overlap.height,
        "conflicts": conflicts,
    }


FACTOR_SPECS = tuple(
    spec for spec in DATASET_SPECS.values() if spec.kind == "extension" and spec.factor_input
)
