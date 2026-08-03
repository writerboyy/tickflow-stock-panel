"""Temporary, auditable Tushare Proxy history backfill.

The module deliberately uses the small HTTP protocol documented by the proxy
instead of importing the Tushare SDK.  It is a batch tool, not a runtime data
provider: after a successful publish all reads continue to use local parquet.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import re
import threading
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import polars as pl

from app import secrets_store
from app.services.data_authority import tushare_history_policy
from app.services.ingestion_manifest import archive_source_payload, stable_content_hash
from app.services.minute_quality import minute_coverage_manifest
from app.services.tushare_datasets import GROUPS, TushareDatasetSpec, resolve_datasets
from app.services.tushare_ingestion import (
    DEFAULT_HISTORY_START,
    IngestionConfig,
    TushareDatasetIngestion,
)

logger = logging.getLogger(__name__)

TUSHARE_PROXY_URL = "https://teajoin.com/"
MAX_MINUTE_ROWS = 8_000
MIN_FREE_BYTES = 50 * 1024**3
DEFAULT_RATE_INTERVAL = 0.2
MAX_WORKERS = 4

PHASES = (
    "universe",
    "reference",
    "daily",
    "financials",
    "factors",
    "adjustment",
    "stock_minute",
    "etf_minute",
    "publish_minute",
    "audit",
    "p0",
    "research",
)

# These are intentionally explicit.  The temporary key must not accidentally
# turn into a general-purpose cross-asset downloader.
RESEARCH_APIS: tuple[str, ...] = (
    "daily",
    "weekly",
    "monthly",
    "daily_basic",
    "income",
    "balancesheet",
    "cashflow",
    "fina_indicator",
    "forecast",
    "express",
    "dividend",
    "disclosure_date",
    "top10_holders",
    "top10_floatholders",
    "share_float",
    "stk_holdernumber",
    "stk_holdertrade",
    "repurchase",
    "block_trade",
    "index_daily",
    "index_weekly",
    "index_monthly",
    "index_weight",
    "index_dailybasic",
    "fund_nav",
    "fund_daily",
    "fund_portfolio",
    "fund_share",
    "moneyflow",
    "margin",
    "margin_detail",
    "limit_list_d",
    "limit_list_ths",
    "top_list",
    "kpl_list",
    "ths_daily",
    "ths_member",
    "cyq_perf",
    "cyq_chips",
    "stk_factor",
    "stk_factor_pro",
    "index_member_all",
    "sw_daily",
    "ci_index_member",
    "fund_basic",
    "etf_index",
    "etf_share_size",
)

PREFLIGHT_APIS: tuple[str, ...] = (
    "stock_basic",
    "etf_basic",
    "trade_cal",
    "adj_factor",
    "fund_adj",
    "stk_mins",
    "etf_mins",
    "daily",
    "daily_basic",
    "income",
    "index_daily",
    "fund_nav",
    "moneyflow",
)

_SAFE_PART = re.compile(r"[^A-Za-z0-9_.=-]+")
_MINUTE_FIELDS = ("symbol", "datetime", "open", "high", "low", "close", "volume", "amount")
_PRICE_COLUMNS = ("open", "high", "low", "close")
_TOLERANCES = {"open": 1e-6, "high": 1e-6, "low": 1e-6, "close": 1e-6, "volume": 0.5, "amount": 100.0}


class TushareError(RuntimeError):
    """Base error for HTTP/protocol/provider failures."""


class TusharePermissionError(TushareError):
    """The API key is invalid or the requested endpoint is not enabled."""


class TushareProtocolError(TushareError):
    """The proxy did not return the standard Tushare response shape."""


class TushareRetryableError(TushareError):
    """A retryable HTTP or transport failure after all attempts."""


class BackfillBlocked(RuntimeError):
    """A safety check prevented staging or publication."""


def _safe_part(value: object) -> str:
    result = _SAFE_PART.sub("-", str(value)).strip("-.")
    if not result:
        raise ValueError("empty path component")
    return result[:120]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class GlobalRateLimiter:
    """One monotonic limiter shared by every worker in a process."""

    def __init__(self, interval: float = DEFAULT_RATE_INTERVAL) -> None:
        if interval < 0:
            raise ValueError("rate interval must be non-negative")
        self.interval = float(interval)
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._next_at = now + self.interval

    def slow_down(self, factor: float = 2.0, maximum: float = 5.0) -> None:
        if factor <= 1 or maximum <= 0:
            raise ValueError("rate limiter slowdown parameters are invalid")
        with self._lock:
            self.interval = min(maximum, max(self.interval, self.interval * factor))


@dataclass(frozen=True)
class TushareResponse:
    api_name: str
    code: int
    msg: str
    fields: tuple[str, ...]
    items: tuple[tuple[Any, ...], ...]
    raw: dict[str, Any]

    @property
    def rows(self) -> list[dict[str, Any]]:
        return [dict(zip(self.fields, item, strict=False)) for item in self.items]


class TushareProxyClient:
    """Small HTTP client for the fixed Tushare-compatible proxy endpoint."""

    def __init__(
        self,
        token: str,
        *,
        base_url: str = TUSHARE_PROXY_URL,
        timeout: float = 60.0,
        attempts: int = 4,
        limiter: GlobalRateLimiter | None = None,
        opener: Callable[..., Any] | None = None,
        backoff: Callable[[float], None] | None = None,
    ) -> None:
        if not token or not token.strip():
            raise ValueError("Tushare API key is required")
        normalized = base_url.rstrip("/") + "/"
        if normalized != TUSHARE_PROXY_URL:
            raise ValueError("Tushare Proxy URL is fixed to https://teajoin.com/")
        if attempts < 1:
            raise ValueError("attempts must be positive")
        self._token = token.strip()
        self.base_url = normalized
        self.timeout = timeout
        self.attempts = attempts
        self.limiter = limiter or GlobalRateLimiter()
        self._opener = opener or urlopen
        self._backoff = backoff or time.sleep

    def request(self, api_name: str, params: Mapping[str, Any] | None = None) -> TushareResponse:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", api_name):
            raise ValueError("invalid Tushare API name")
        payload = {"api_name": api_name, "token": self._token, "params": dict(params or {})}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = Request(self.base_url, data=body, headers={"Content-Type": "application/json", "Accept": "application/json"}, method="POST")
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            self.limiter.wait()
            try:
                response_context = self._opener(request, timeout=self.timeout)
                with response_context as response:
                    status_value = getattr(response, "status", None)
                    status = int(status_value if status_value is not None else response.getcode())
                    raw_body = response.read()
                if status == 429 or status >= 500:
                    raise TushareRetryableError(f"HTTP {status}")
                if status >= 400:
                    raise TusharePermissionError(f"HTTP {status}") if status in {401, 403} else TushareError(f"HTTP {status}")
                try:
                    decoded = json.loads(raw_body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise TushareProtocolError("response is not JSON") from exc
                return self._parse_response(api_name, decoded)
            except HTTPError as exc:
                last_error = exc
                if exc.code not in {429, 500, 502, 503, 504}:
                    if exc.code in {401, 403}:
                        raise TusharePermissionError(f"HTTP {exc.code}") from exc
                    raise TushareError(f"HTTP {exc.code}") from exc
            except (URLError, TimeoutError, OSError, TushareRetryableError) as exc:
                last_error = exc
            if attempt + 1 < self.attempts:
                self.limiter.slow_down()
                self._backoff(min(30.0, 0.5 * (2**attempt)))
        raise TushareRetryableError(f"request failed after {self.attempts} attempts: {type(last_error).__name__}") from last_error

    @staticmethod
    def _parse_response(api_name: str, decoded: Any) -> TushareResponse:
        if not isinstance(decoded, dict) or not isinstance(decoded.get("code"), int):
            raise TushareProtocolError("response missing integer code")
        code = int(decoded["code"])
        msg = str(decoded.get("msg") or "")
        if code != 0:
            lowered = msg.lower()
            if code in {-2001, -2002, -2003, -2004} or any(word in lowered for word in ("权限", "token", "auth", "permission")):
                raise TusharePermissionError(f"{api_name}: provider rejected request ({code})")
            raise TushareError(f"{api_name}: provider error ({code})")
        data = decoded.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("fields"), list) or not isinstance(data.get("items"), list):
            raise TushareProtocolError(f"{api_name}: response missing data.fields/data.items")
        fields = tuple(str(field) for field in data["fields"])
        items: list[tuple[Any, ...]] = []
        for item in data["items"]:
            if not isinstance(item, (list, tuple)) or len(item) != len(fields):
                raise TushareProtocolError(f"{api_name}: field/item length mismatch")
            items.append(tuple(item))
        return TushareResponse(api_name, code, msg, fields, tuple(items), decoded)


def _secret_path(data_dir: Path | None = None) -> Path:
    if data_dir is None:
        return secrets_store._path()
    path = Path(data_dir) / "user_data" / "secrets.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def save_tushare_key(value: str, *, data_dir: Path | None = None) -> str:
    """Validate and persist a Tushare key with mode 0600."""
    value = str(value).strip()
    if not value or len(value) > 256 or any(ch.isspace() for ch in value):
        raise ValueError("invalid Tushare API key")
    path = _secret_path(data_dir)
    if data_dir is None:
        secrets_store.save({"tushare_proxy_api_key": value})
    else:
        current: dict[str, Any] = {}
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    current = loaded
            except (OSError, json.JSONDecodeError):
                current = {}
        current["tushare_proxy_api_key"] = value
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return value


def save_tushare_key_from_stdin(stream: Any, *, data_dir: Path | None = None) -> str:
    """Read one key without echoing it and persist it with mode 0600."""
    return save_tushare_key(str(stream.readline()), data_dir=data_dir)


def load_tushare_key(*, data_dir: Path | None = None) -> str:
    if data_dir is None:
        return str(secrets_store.load().get("tushare_proxy_api_key") or "")
    path = _secret_path(data_dir)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(value.get("tushare_proxy_api_key") or "") if isinstance(value, dict) else ""


def clear_tushare_key(*, data_dir: Path | None = None) -> None:
    if data_dir is None:
        secrets_store.clear("tushare_proxy_api_key")
        return
    path = _secret_path(data_dir)
    if not path.exists():
        return
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(value, dict):
        return
    value.pop("tushare_proxy_api_key", None)
    if value:
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    else:
        path.unlink()


def free_space_bytes(path: Path) -> int:
    return int(os.statvfs(path).f_bavail * os.statvfs(path).f_frsize)


def assert_disk_reserve(path: Path, *, minimum: int = MIN_FREE_BYTES) -> None:
    available = free_space_bytes(path)
    if available < minimum:
        raise BackfillBlocked(f"free space reserve reached: {available / 1024**3:.1f} GiB available")


def normalize_rows(rows: Iterable[Mapping[str, Any]], *, asset_type: str | None = None) -> pl.DataFrame:
    """Normalize Tushare rows to the project's canonical minute schema."""
    frame = pl.DataFrame([dict(row) for row in rows])
    if frame.is_empty():
        return pl.DataFrame(schema={
            "symbol": pl.String, "datetime": pl.Datetime("us"), "open": pl.Float64,
            "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
            "volume": pl.Float64, "amount": pl.Float64,
        })
    aliases = {"ts_code": "symbol", "trade_time": "datetime", "vol": "volume", "amt": "amount"}
    frame = frame.rename({source: target for source, target in aliases.items() if source in frame.columns and target not in frame.columns})
    if "symbol" not in frame.columns:
        raise BackfillBlocked("minute response missing ts_code")
    if "datetime" not in frame.columns:
        raise BackfillBlocked("minute response missing trade_time")
    dtype = frame.schema["datetime"]
    if dtype == pl.String:
        frame = frame.with_columns(
            pl.coalesce(
                pl.col("datetime").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False),
                pl.col("datetime").str.strptime(pl.Datetime("us"), "%Y%m%d%H%M%S", strict=False),
            ).alias("datetime")
        )
    else:
        frame = frame.with_columns(pl.col("datetime").cast(pl.Datetime("us"), strict=False))
    for column in _PRICE_COLUMNS + ("volume", "amount"):
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
        else:
            frame = frame.with_columns(pl.col(column).cast(pl.Float64, strict=False))
    frame = frame.with_columns(pl.col("symbol").cast(pl.String))
    if asset_type:
        frame = frame.with_columns(pl.lit(asset_type).alias("asset_type"))
    return frame.select([*(_MINUTE_FIELDS), *(["asset_type"] if asset_type else [])])


def validate_minute_frame(frame: pl.DataFrame) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Return valid rows and an audit list for rejected/duplicate rows."""
    if frame.is_empty():
        return frame, []
    required = set(_MINUTE_FIELDS)
    if not required <= set(frame.columns):
        raise BackfillBlocked("minute frame does not contain canonical fields")
    invalid = frame.filter(
        pl.any_horizontal(
            pl.col(column).is_null() | ~pl.col(column).is_finite() | (pl.col(column) <= 0)
            for column in _PRICE_COLUMNS
        )
        | (pl.col("datetime").is_null())
    )
    valid = frame.filter(~pl.col("datetime").is_null()).filter(
        pl.all_horizontal(pl.col(column).is_not_null() & pl.col(column).is_finite() & (pl.col(column) > 0) for column in _PRICE_COLUMNS)
    ).filter(
        (pl.col("high") >= pl.max_horizontal("open", "close"))
        & (pl.col("low") <= pl.min_horizontal("open", "close"))
    )
    hour = pl.col("datetime").dt.hour()
    minute = pl.col("datetime").dt.minute()
    valid = valid.filter(
        ((hour == 9) & (minute >= 30))
        | (hour == 10)
        | ((hour == 11) & (minute <= 30))
        | ((hour == 13) & (minute >= 1))
        | (hour == 14)
        | ((hour == 15) & (minute == 0))
    )
    duplicate = valid.group_by(["symbol", "datetime"]).agg(pl.len().alias("rows")).filter(pl.col("rows") > 1)
    if not duplicate.is_empty():
        conflicts = valid.join(duplicate.select("symbol", "datetime"), on=["symbol", "datetime"], how="inner").group_by(["symbol", "datetime"]).agg(pl.struct([*(_PRICE_COLUMNS), "volume", "amount"]).n_unique().alias("distinct")).filter(pl.col("distinct") > 1)
        if not conflicts.is_empty():
            raise BackfillBlocked("minute response contains conflicting duplicate keys")
        valid = valid.unique(subset=["symbol", "datetime"], keep="last")
    audit = [{"reason": "invalid_ohlc_or_timestamp", "rows": frame.height - valid.height}]
    return valid.sort(["symbol", "datetime"]), audit


def forward_adjust_minutes(raw: pl.DataFrame, factors: pl.DataFrame) -> pl.DataFrame:
    """Apply Tushare cumulative adjustment factors while preserving volume/amount."""
    if raw.is_empty() or factors.is_empty():
        return raw
    rename_map = {
        source: target
        for source, target in (("ts_code", "symbol"), ("trade_date", "date"), ("adj_factor", "factor"))
        if source in factors.columns and target not in factors.columns
    }
    factors = factors.rename(rename_map)
    if "date" not in factors.columns or "factor" not in factors.columns:
        raise BackfillBlocked("adjustment response missing trade_date/adj_factor")
    factors = factors.with_columns(
        pl.col("symbol").cast(pl.String), pl.col("date").cast(pl.Date, strict=False), pl.col("factor").cast(pl.Float64, strict=False)
    ).drop_nulls(["symbol", "date", "factor"]).sort(["date", "symbol"])
    latest = factors.group_by("symbol").agg(pl.col("factor").last().alias("latest_factor"))
    bars = raw.with_columns(pl.col("datetime").dt.date().alias("date")).sort(["date", "symbol"])
    bars = bars.join_asof(factors.select("symbol", "date", "factor"), left_on="date", right_on="date", by="symbol", strategy="backward")
    bars = bars.join(latest, on="symbol", how="left")
    ratio = pl.col("factor").fill_null(1.0) / pl.col("latest_factor").fill_null(1.0)
    return bars.with_columns([(pl.col(column) * ratio).alias(column) for column in _PRICE_COLUMNS]).drop(["date", "factor", "latest_factor"])


def normalize_adjustment_rows(frame: pl.DataFrame) -> pl.DataFrame:
    """Convert Tushare cumulative factors to the local event-factor schema."""
    if frame.is_empty():
        return pl.DataFrame(schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64})
    rename_map = {
        source: target
        for source, target in (("ts_code", "symbol"), ("date", "trade_date"), ("adj_factor", "factor"))
        if source in frame.columns and target not in frame.columns
    }
    frame = frame.rename(rename_map)
    if {"symbol", "trade_date", "ex_factor"} <= set(frame.columns) and "factor" not in frame.columns:
        return frame.with_columns(
            pl.col("symbol").cast(pl.String),
            pl.col("trade_date").cast(pl.Date, strict=False),
            pl.col("ex_factor").cast(pl.Float64, strict=False),
        ).drop_nulls(["symbol", "trade_date", "ex_factor"]).select("symbol", "trade_date", "ex_factor")
    if not {"symbol", "trade_date", "factor"} <= set(frame.columns):
        raise BackfillBlocked("adjustment response missing symbol/trade_date/adj_factor")
    frame = frame.with_columns(
        pl.col("symbol").cast(pl.String),
        pl.col("trade_date").cast(pl.Date, strict=False),
        pl.col("factor").cast(pl.Float64, strict=False),
    ).drop_nulls(["symbol", "trade_date", "factor"]).sort(["symbol", "trade_date"])
    frame = frame.unique(subset=["symbol", "trade_date"], keep="last").sort(["symbol", "trade_date"])
    return frame.with_columns(
        (pl.col("factor") / pl.col("factor").shift(1).over("symbol").fill_null(1.0)).alias("ex_factor")
    ).select("symbol", "trade_date", "ex_factor")


def overlap_merge(existing: pl.DataFrame, incoming: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Merge Tushare rows without overwriting TickFlow overlap keys."""
    if existing.is_empty():
        return incoming.unique(subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"]), {"existing_rows": 0, "added_rows": incoming.height, "conflicts": []}
    if incoming.is_empty():
        return existing, {"existing_rows": existing.height, "added_rows": 0, "conflicts": []}
    left = existing.select(_MINUTE_FIELDS).unique(subset=["symbol", "datetime"], keep="last")
    right = incoming.select(_MINUTE_FIELDS).unique(subset=["symbol", "datetime"], keep="last")
    overlap = left.join(right, on=["symbol", "datetime"], how="inner", suffix="_tushare")
    conflicts: list[dict[str, Any]] = []
    for row in overlap.to_dicts():
        mismatches = [column for column, tolerance in _TOLERANCES.items() if abs(float(row.get(column) or 0) - float(row.get(f"{column}_tushare") or 0)) > tolerance]
        if mismatches:
            conflicts.append({"symbol": row["symbol"], "datetime": str(row["datetime"]), "columns": mismatches})
    if conflicts:
        raise BackfillBlocked(f"minute overlap conflicts exceed tolerance: {conflicts[:3]}")
    missing = right.join(left.select(["symbol", "datetime"]), on=["symbol", "datetime"], how="anti")
    return pl.concat([left, missing], how="vertical_relaxed").sort(["symbol", "datetime"]), {"existing_rows": left.height, "added_rows": missing.height, "overlap_rows": overlap.height, "conflicts": conflicts}


def _symbol_hash(symbols: Iterable[str]) -> str:
    return sha256("\n".join(sorted(set(symbols))).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BackfillConfig:
    data_dir: Path
    run_id: str | None = None
    phases: tuple[str, ...] = PHASES
    symbols: tuple[str, ...] | None = None
    etfs: tuple[str, ...] | None = None
    indexes: tuple[str, ...] | None = None
    max_symbols: int | None = None
    rate_interval: float = DEFAULT_RATE_INTERVAL
    attempts: int = 4
    publish: bool = False
    start: date = DEFAULT_HISTORY_START
    end: date | None = None
    datasets: tuple[str, ...] = ()
    incremental: bool = False

    def normalized(self) -> "BackfillConfig":
        root = Path(self.data_dir).expanduser().resolve()
        phases = tuple(dict.fromkeys(self.phases))
        unknown = sorted(set(phases) - set(PHASES))
        if unknown:
            raise ValueError(f"unknown Tushare backfill phase(s): {', '.join(unknown)}")
        symbols = tuple(dict.fromkeys(str(item).strip().upper() for item in self.symbols or () if str(item).strip())) or None
        etfs = tuple(dict.fromkeys(str(item).strip().upper() for item in self.etfs or () if str(item).strip())) or None
        indexes = tuple(dict.fromkeys(str(item).strip().upper() for item in self.indexes or () if str(item).strip())) or None
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive")
        end = self.end or date.today()
        if self.start > end:
            raise ValueError("start must not be after end")
        datasets = tuple(dict.fromkeys(str(item).strip() for item in self.datasets if str(item).strip()))
        if datasets:
            resolve_datasets(datasets)
        return BackfillConfig(
            data_dir=root,
            run_id=self.run_id,
            phases=phases,
            symbols=symbols,
            etfs=etfs,
            indexes=indexes,
            max_symbols=self.max_symbols,
            rate_interval=self.rate_interval,
            attempts=self.attempts,
            publish=self.publish,
            start=self.start,
            end=end,
            datasets=datasets,
            incremental=self.incremental,
        )


class TushareHistoryBackfill:
    def __init__(self, config: BackfillConfig, client: TushareProxyClient) -> None:
        self.config = config.normalized()
        self.client = client
        self._manifest_lock = threading.RLock()
        self.run_id = self.config.run_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
        self.run_root = self.config.data_dir / "backfill_state" / "tushare_proxy" / _safe_part(self.run_id)
        self.manifest_path = self.run_root / "manifest.json"
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> dict[str, Any]:
        if self.manifest_path.exists():
            try:
                value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise BackfillBlocked("invalid Tushare backfill manifest") from exc
            if not isinstance(value, dict):
                raise BackfillBlocked("invalid Tushare backfill manifest")
            schema_version = int(value.get("schema_version") or 1)
            expected = {
                "data_dir": str(self.config.data_dir),
                "phases": list(self.config.phases),
                "requested_symbols_hash": _symbol_hash(self._requested_symbols()),
            }
            if schema_version >= 2:
                expected.update({
                    "history_start": self.config.start.isoformat(),
                    "history_end": (self.config.end or date.today()).isoformat(),
                    "datasets": list(self.config.datasets),
                    "incremental": self.config.incremental,
                })
            for key, val in expected.items():
                if value.get(key) != val:
                    raise BackfillBlocked(f"resume configuration mismatch for {key}")
            # Schema v1 runs predate formal dataset controls. Preserve their
            # completed batches and add only backward-compatible defaults.
            value.setdefault("schema_version", schema_version)
            value.setdefault("history_start", self.config.start.isoformat())
            value.setdefault("history_end", (self.config.end or date.today()).isoformat())
            value.setdefault("datasets", list(self.config.datasets))
            value.setdefault("incremental", self.config.incremental)
            return value
        value = {
            "schema_version": 2,
            "kind": "tushare_proxy_history_backfill",
            "run_id": self.run_id,
            "data_dir": str(self.config.data_dir),
            "phases": list(self.config.phases),
            "status": "staging",
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "symbols_hash": _symbol_hash(self._requested_symbols()),
            "requested_symbols_hash": _symbol_hash(self._requested_symbols()),
            "phases_state": {},
            "coverage": {},
            "publish": {"status": "not_requested", "conflicts": []},
            "history_start": self.config.start.isoformat(),
            "history_end": (self.config.end or date.today()).isoformat(),
            "datasets": list(self.config.datasets),
            "incremental": self.config.incremental,
        }
        _atomic_json(self.manifest_path, value)
        return value

    def _requested_symbols(self) -> tuple[str, ...]:
        return (
            *(self.config.symbols or ()),
            *(self.config.etfs or ()),
            *(self.config.indexes or ()),
        )

    def _save(self, **updates: Any) -> None:
        with self._manifest_lock:
            self.manifest.update(updates)
            self.manifest["updated_at"] = _utc_now()
            _atomic_json(self.manifest_path, self.manifest)

    def _phase(self, name: str) -> dict[str, Any]:
        phases = self.manifest.setdefault("phases_state", {})
        return phases.setdefault(name, {"status": "pending", "items": {}})

    def _record(self, phase: str, key: str, **updates: Any) -> None:
        with self._manifest_lock:
            state = self._phase(phase)
            items = state.setdefault("items", {})
            item = items.setdefault(key, {})
            item.update(updates)
            self._save()

    def _item_state(self, phase: str, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._manifest_lock:
            state = self._phase(phase)
            item = state.setdefault("items", {}).setdefault(key, dict(default or {}))
            return dict(item)

    def _archive(self, api_name: str, key: str, response: TushareResponse) -> None:
        archive_source_payload(self.config.data_dir, "tushare_proxy", api_name, self.run_id, key, response.raw, parser_version="tushare_proxy_v1")

    def _safe_error(self, error: Exception) -> str:
        return str(error).replace(self.client._token, "[REDACTED]")[:240]

    def preflight(self) -> dict[str, Any]:
        result: dict[str, Any] = {"base_url": TUSHARE_PROXY_URL, "checked_at": _utc_now(), "apis": {}}
        samples: dict[str, dict[str, Any]] = {
            "stock_basic": {"exchange": "", "list_status": "L"},
            "etf_basic": {},
            "trade_cal": {"exchange": "SSE", "start_date": "20250101", "end_date": "20250110"},
            "adj_factor": {"ts_code": "000001.SZ", "start_date": "20250101", "end_date": "20250110"},
            "fund_adj": {"ts_code": "510300.SH", "start_date": "20250101", "end_date": "20250110"},
            "stk_mins": {"ts_code": "000001.SZ", "start_date": "2025-01-01 09:30:00", "end_date": "2025-01-02 15:00:00", "freq": "1min"},
            "etf_mins": {"ts_code": "510300.SH", "start_date": "2025-01-01 09:30:00", "end_date": "2025-01-02 15:00:00", "freq": "1min"},
            "income": {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20250110"},
            "index_daily": {"ts_code": "000001.SH", "start_date": "20250101", "end_date": "20250110"},
            "index_basic": {"market": "SSE"},
            "index_member_all": {"index_code": "000300.SH"},
            "index_weight": {"index_code": "000300.SH", "start_date": "20240101", "end_date": "20250110"},
            "ci_index_member": {},
            "namechange": {"ts_code": "000001.SZ", "start_date": "20200101", "end_date": "20250110"},
            "suspend_d": {"ts_code": "000001.SZ", "start_date": "20250101", "end_date": "20250110"},
            "dividend": {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20250110"},
            "fund_nav": {"ts_code": "510300.SH", "start_date": "20230101", "end_date": "20250110"},
            "fund_daily": {"ts_code": "510300.SH", "start_date": "20230101", "end_date": "20250110"},
        }
        samples.update({api: {"ts_code": "000001.SZ", "start_date": "20250101", "end_date": "20250110"} for api in PREFLIGHT_APIS if api not in samples})
        selected = (
            [spec.api_name for spec in resolve_datasets(self.config.datasets)]
            if self.config.datasets
            else []
        )
        preflight_apis = tuple(dict.fromkeys([*PREFLIGHT_APIS, *selected]))
        samples.update({
            api: {"ts_code": "000001.SZ", "start_date": "20250101", "end_date": "20250110"}
            for api in preflight_apis
            if api not in samples
        })
        for api_name in preflight_apis:
            try:
                response = self.client.request(api_name, samples[api_name])
                status = "ok" if response.items else "empty_unconfirmed"
                result["apis"][api_name] = {"status": status, "rows": len(response.items), "fields": list(response.fields)}
                self._archive(api_name, "preflight", response)
            except TusharePermissionError as exc:
                result["apis"][api_name] = {"status": "blocked", "reason": "permission_or_auth", "error": self._safe_error(exc)}
            except Exception as exc:  # noqa: BLE001
                result["apis"][api_name] = {"status": "error", "error": self._safe_error(exc)}
        result["blocked"] = sorted(api for api, value in result["apis"].items() if value["status"] in {"blocked", "error"})
        result["empty_unconfirmed"] = sorted(api for api, value in result["apis"].items() if value["status"] == "empty_unconfirmed")
        self._save(preflight=result)
        return result

    def _formal_specs(self) -> tuple[TushareDatasetSpec, ...]:
        if self.config.datasets:
            return resolve_datasets(self.config.datasets)
        names: list[str] = []
        for phase in ("reference", "daily", "financials", "factors"):
            if phase in self.config.phases:
                names.extend(GROUPS[phase])
        if not names and "audit" in self.config.phases:
            for phase in ("reference", "daily", "financials", "factors"):
                names.extend(GROUPS[phase])
        return resolve_datasets(names)

    def _run_formal_datasets(
        self,
        stocks: list[str],
        etfs: list[str],
        indexes: list[str],
    ) -> tuple[dict[str, Any], TushareDatasetIngestion | None]:
        specs = self._formal_specs()
        if not specs:
            return {}, None
        engine = TushareDatasetIngestion(
            IngestionConfig(
                self.config.data_dir,
                self.run_id,
                start=self.config.start,
                end=self.config.end or date.today(),
                publish=self.config.publish,
                incremental=self.config.incremental,
            ),
            self.client,
        )
        if set(self.config.phases) == {"audit"}:
            return {}, engine
        result: dict[str, Any] = {}
        for spec in specs:
            phase = self._phase(spec.group)
            phase["status"] = "running"
            symbol_pool = etfs if spec.api_name == "fund_daily" else stocks
            try:
                collected = engine.collect(
                    (spec,),
                    symbols=symbol_pool,
                    indexes=indexes,
                )[spec.api_name]
                if self.config.publish:
                    published = engine.publish((spec,))[spec.api_name]
                else:
                    published = {"status": "staged_only"}
                result[spec.api_name] = {"collect": collected, "publish": published}
                phase.setdefault("items", {})[spec.api_name] = {
                    "status": published["status"] if self.config.publish else collected["status"],
                    "rows": collected["rows"],
                    "published_rows": published.get("published_rows", 0),
                }
            except Exception as exc:  # noqa: BLE001
                result[spec.api_name] = {
                    "collect": {"status": "failed"},
                    "publish": {"status": "blocked"},
                    "error": type(exc).__name__,
                    "message": self._safe_error(exc),
                }
                phase.setdefault("items", {})[spec.api_name] = {
                    "status": "failed",
                    "error": type(exc).__name__,
                    "message": self._safe_error(exc),
                }
            phase["status"] = (
                "incomplete"
                if any(item.get("status") in {"failed", "blocked"} for item in phase["items"].values())
                else "completed"
            )
            self._save(dataset_ingestion=result)
        if self.config.publish:
            try:
                derived = self._rebuild_after_formal_publish(specs, result)
                phase = self._phase("derived")
                phase["status"] = "completed"
                phase.pop("error", None)
                phase.pop("message", None)
                self._save(derived_rebuild=derived)
            except Exception as exc:  # noqa: BLE001
                phase = self._phase("derived")
                failure = {
                    "status": "failed",
                    "error": type(exc).__name__,
                    "message": self._safe_error(exc),
                }
                phase.update(failure)
                self._save(derived_rebuild=failure)
        engine.write_capability_matrix(specs)
        return result, engine

    def _rebuild_after_formal_publish(
        self,
        specs: tuple[TushareDatasetSpec, ...],
        result: Mapping[str, Any],
    ) -> dict[str, Any]:
        changed = {
            spec.api_name: spec
            for spec in specs
            if result.get(spec.api_name, {}).get("publish", {}).get("status") == "published"
            and int(result.get(spec.api_name, {}).get("publish", {}).get("published_rows") or 0) > 0
        }
        report: dict[str, Any] = {"datasets": sorted(changed)}
        daily_paths = sorted(
            (self.config.data_dir / "kline_daily").glob("date=*/part.parquet")
        )
        enriched_paths = sorted(
            (self.config.data_dir / "kline_daily_enriched").glob("date=*/part.parquet")
        )
        shares_path = self.config.data_dir / "financials" / "shares" / "part.parquet"
        stale_share_schema = bool(
            daily_paths
            and shares_path.exists()
            and (
                not enriched_paths
                or any(
                    not {"total_shares", "float_shares"}
                    <= set(pl.read_parquet_schema(path))
                    for path in enriched_paths
                )
            )
        )
        if stale_share_schema:
            report["recovery_dependencies"] = ["historical_share_columns"]
        if not changed and not stale_share_schema:
            report["status"] = "not_needed"
            return report

        if "daily" in changed or stale_share_schema:
            from app.indicators.pipeline import run_pipeline

            report["kline_daily_enriched_rows"] = run_pipeline(
                self.config.data_dir,
                new_dates_only=self.config.incremental and not stale_share_schema,
                keep_backup=stale_share_schema or not self.config.incremental,
            )

        from app.indicators.pipeline import ENRICHED_STORAGE_COLS, compute_enriched

        for api_name, raw_table, enriched_table, factor_table in (
            ("index_daily", "kline_index_daily", "kline_index_enriched", None),
            ("fund_daily", "kline_etf_daily", "kline_etf_enriched", "adj_factor_etf"),
        ):
            if api_name not in changed:
                continue
            paths = sorted((self.config.data_dir / raw_table).glob("date=*/part.parquet"))
            if not paths:
                continue
            raw = pl.concat([pl.read_parquet(path) for path in paths], how="diagonal_relaxed")
            factors = pl.DataFrame()
            if factor_table:
                factor_path = self.config.data_dir / factor_table / "all.parquet"
                if factor_path.exists():
                    factors = pl.read_parquet(factor_path)
            enriched = compute_enriched(
                raw.sort(["symbol", "date"]),
                factors=factors if not factors.is_empty() else None,
                instruments=None,
            )
            storage_columns = [column for column in ENRICHED_STORAGE_COLS if column in enriched.columns]
            written = 0
            for day_frame in enriched.select(storage_columns).partition_by("date"):
                day = day_frame["date"][0]
                _atomic_parquet(
                    day_frame.sort("symbol"),
                    self.config.data_dir / enriched_table / f"date={day.isoformat()}" / "part.parquet",
                )
                written += day_frame.height
            report[f"{enriched_table}_rows"] = written

        if stale_share_schema or changed.keys() & {
            "daily", "daily_basic", "income", "balancesheet", "cashflow", "fina_indicator"
        }:
            from app.services.daily_valuation import build_daily_valuation

            report["valuation_daily"] = build_daily_valuation(
                self.config.data_dir,
                keep_backup=not self.config.incremental,
            )
        report["status"] = "completed"
        return report

    def _write_rows(self, phase: str, key: str, frame: pl.DataFrame) -> Path:
        path = self.run_root / "batches" / _safe_part(phase) / f"{_safe_part(key)}.parquet"
        _atomic_parquet(frame, path)
        return path

    def _universe(self) -> tuple[list[str], list[str], list[str]]:
        state = self._phase("universe")
        if self._requested_symbols():
            stocks = list(self.config.symbols or ())
            etfs = list(self.config.etfs or ())
            indexes = list(self.config.indexes or ())
        else:
            stocks, etfs, indexes = [], [], []
            for list_status in ("L", "D", "P"):
                key = f"stock_basic_{list_status}"
                try:
                    response = self.client.request(
                        "stock_basic",
                        {"exchange": "", "list_status": list_status, "fields": "ts_code,name,list_date,delist_date"},
                    )
                    self._archive("stock_basic", key, response)
                    stocks.extend(str(row.get("ts_code")) for row in response.rows if row.get("ts_code"))
                    frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
                    if not frame.is_empty():
                        self._write_rows("universe", key, frame)
                    state["items"][key] = {"status": "completed", "rows": len(response.items)}
                except Exception as exc:  # noqa: BLE001
                    state["items"][key] = {"status": "blocked", "error": type(exc).__name__}
                    if list_status == "L":
                        raise BackfillBlocked("active stock_basic is required to build the universe") from exc
            for api, params, target in (
                ("etf_basic", {"exchange": "", "fields": "ts_code,name,list_date,delist_date"}, etfs),
                ("index_basic", {"market": "SSE,SZSE", "fields": "ts_code,name,list_date"}, indexes),
            ):
                try:
                    response = self.client.request(api, params)
                    self._archive(api, "universe", response)
                    target.extend(str(row.get("ts_code")) for row in response.rows if row.get("ts_code"))
                    frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
                    if not frame.is_empty():
                        self._write_rows("universe", api, frame)
                    state["items"][api] = {"status": "completed", "rows": len(response.items)}
                except Exception as exc:  # noqa: BLE001
                    state["items"][api] = {"status": "blocked", "error": type(exc).__name__}
            try:
                calendar = self.client.request(
                    "trade_cal",
                    {"exchange": "SSE", "start_date": "19900101", "end_date": date.today().strftime("%Y%m%d")},
                )
                self._archive("trade_cal", "universe", calendar)
                if calendar.rows:
                    self._write_rows("universe", "trade_cal", pl.DataFrame(calendar.rows))
                state["items"]["trade_cal"] = {"status": "completed", "rows": len(calendar.items)}
            except Exception as exc:  # noqa: BLE001
                state["items"]["trade_cal"] = {"status": "blocked", "error": type(exc).__name__}
            etfs = sorted(set(etfs))
            stocks = sorted(set(stocks))
            indexes = sorted(set(indexes))
        if self.config.max_symbols is not None:
            stocks = stocks[: self.config.max_symbols]
            etfs = etfs[: self.config.max_symbols]
        state.update({"status": "completed", "stock_count": len(stocks), "etf_count": len(etfs), "index_count": len(indexes)})
        self._save(symbols={"stocks": stocks, "etfs": etfs, "indexes": indexes}, symbols_hash=_symbol_hash([*stocks, *etfs, *indexes]))
        return stocks, etfs, indexes

    def _symbols(self, kind: str) -> list[str]:
        symbols = self.manifest.get("symbols") or {}
        if kind in symbols:
            return list(symbols[kind])
        return list(self.config.symbols or ()) if kind == "stocks" else []

    def _fetch_adjustment(self, kind: str, api_name: str, symbols: list[str]) -> None:
        phase = self._phase("adjustment")
        phase["status"] = "running"

        def fetch_one(symbol: str) -> None:
            assert_disk_reserve(self.config.data_dir)
            state = self._item_state("adjustment", symbol)
            path = self.run_root / "batches" / "adjustment" / kind / f"{_safe_part(symbol)}.parquet"
            if state.get("status") == "completed" and path.exists():
                return
            try:
                params: dict[str, Any] = {"ts_code": symbol}
                if self.config.incremental:
                    params.update({
                        "start_date": self.config.start.strftime("%Y%m%d"),
                        "end_date": (self.config.end or date.today()).strftime("%Y%m%d"),
                    })
                response = self.client.request(api_name, params)
                self._archive(api_name, symbol, response)
                frame = pl.DataFrame(response.rows) if response.rows else pl.DataFrame()
                if not frame.is_empty():
                    _atomic_parquet(
                        frame,
                        self.run_root / "batches" / "adjustment" / kind / f"{_safe_part(symbol)}.parquet",
                    )
                self._record("adjustment", symbol, status="completed", api=api_name, rows=frame.height, empty=frame.is_empty(), content_hash=stable_content_hash(response.raw))
            except TusharePermissionError as exc:
                self._record("adjustment", symbol, status="blocked", api=api_name, reason="permission_or_auth", error=type(exc).__name__)
            except Exception as exc:  # noqa: BLE001
                self._record("adjustment", symbol, status="retry", api=api_name, error=type(exc).__name__)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="tushare-adjustment") as executor:
            list(executor.map(fetch_one, symbols))
        phase["status"] = "completed"
        self._save()

    def _fetch_minutes(self, kind: str, api_name: str, symbols: list[str]) -> None:
        phase_name = f"{kind}_minute"
        phase = self._phase(phase_name)
        phase["status"] = "running"
        today = date.today()

        def fetch_one(symbol: str) -> None:
            assert_disk_reserve(self.config.data_dir)
            page_root = self.run_root / "batches" / phase_name / _safe_part(symbol)
            raw_root = self.config.data_dir / "tushare_archive" / ("minute_stock_raw" if kind == "stock" else "minute_etf_raw") / f"symbol={_safe_part(symbol)}"
            raw_path = raw_root / "part.parquet"
            state = self._item_state(phase_name, symbol, {"status": "pending", "pages": 0})
            if state.get("status") == "completed" and raw_path.exists():
                self._clear_minute_pages(page_root)
                return
            if self.config.incremental:
                try:
                    existing = (
                        pl.read_parquet(raw_path)
                        if raw_path.exists()
                        else normalize_rows([], asset_type=kind)
                    )
                    start_at = (
                        existing["datetime"].max() + timedelta(minutes=1)
                        if existing.height
                        else datetime.combine(self.config.start, datetime.min.time())
                    )
                    end_at = datetime.combine(
                        self.config.end or today,
                        datetime.max.time().replace(microsecond=0),
                    )
                    response = self.client.request(
                        api_name,
                        {
                            "ts_code": symbol,
                            "freq": "1min",
                            "start_date": start_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "end_date": end_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "limit": MAX_MINUTE_ROWS,
                        },
                    )
                    self._archive(api_name, f"{symbol}-incremental", response)
                    if len(response.items) >= MAX_MINUTE_ROWS:
                        raise BackfillBlocked(
                            f"incremental minute window reached {MAX_MINUTE_ROWS} rows for {symbol}"
                        )
                    incoming, audit = validate_minute_frame(
                        normalize_rows(response.rows, asset_type=kind)
                    )
                    if audit:
                        raise BackfillBlocked(
                            f"incremental minute quality rejected rows for {symbol}: {audit[:3]}"
                        )
                    merged, overlap = overlap_merge(existing, incoming)
                    _atomic_parquet(merged, raw_path)
                    self._record(
                        phase_name,
                        symbol,
                        status="completed",
                        rows=merged.height,
                        added_rows=overlap.get("added_rows", 0),
                        min_datetime=str(merged["datetime"].min()) if merged.height else None,
                        max_datetime=str(merged["datetime"].max()) if merged.height else None,
                        content_hash=stable_content_hash(merged.to_dicts()),
                    )
                except TusharePermissionError as exc:
                    self._record(
                        phase_name,
                        symbol,
                        status="blocked",
                        reason="permission_or_auth",
                        error=type(exc).__name__,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._record(
                        phase_name,
                        symbol,
                        status="failed",
                        error=type(exc).__name__,
                        message=self._safe_error(exc),
                    )
                return
            page_paths = sorted(page_root.glob("page-*.parquet"))
            raw_pages = [pl.read_parquet(path) for path in page_paths]
            staged_rows = sum(validate_minute_frame(page)[0].height for page in raw_pages)
            if raw_pages:
                last_valid, _ = validate_minute_frame(raw_pages[-1])
                if last_valid.is_empty():
                    raise BackfillBlocked(f"staged minute page has no valid cursor for {symbol}")
                cursor = (last_valid["datetime"].min() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
            else:
                cursor = str(state.get("cursor") or f"{today.isoformat()} 23:59:59")
            state.update({"pages": len(raw_pages), "rows": staged_rows, "cursor": cursor})
            self._record(phase_name, symbol, status="running", pages=state["pages"], rows=state["rows"], cursor=cursor)
            try:
                while True:
                    page_number = int(state.get("pages", 0))
                    response = self.client.request(api_name, {"ts_code": symbol, "freq": "1min", "start_date": "1990-01-01 00:00:00", "end_date": cursor, "limit": MAX_MINUTE_ROWS})
                    self._archive(api_name, f"{symbol}-page-{page_number:06d}", response)
                    if not response.items:
                        state.update({"status": "completed", "reason": "no_earlier_data", "cursor": cursor})
                        break
                    frame = normalize_rows(response.rows, asset_type=kind)
                    valid, audit = validate_minute_frame(frame)
                    timestamps = [item for item in valid["datetime"].to_list()] if not valid.is_empty() else []
                    oldest = min(timestamps) if timestamps else None
                    previous = datetime.fromisoformat(cursor)
                    if oldest is None or oldest > previous:
                        raise BackfillBlocked(f"minute cursor did not strictly decrease for {symbol}")
                    next_cursor = oldest - timedelta(minutes=1)
                    if next_cursor >= previous:
                        raise BackfillBlocked(f"minute cursor did not strictly decrease for {symbol}")
                    _atomic_parquet(frame, page_root / f"page-{page_number:06d}.parquet")
                    raw_pages.append(frame)
                    state.update(
                        {
                            "pages": page_number + 1,
                            "cursor": next_cursor.strftime("%Y-%m-%d %H:%M:%S"),
                            "rows": int(state.get("rows", 0)) + valid.height,
                            "last_page_hash": stable_content_hash(response.raw),
                        }
                    )
                    self._record(
                        phase_name,
                        symbol,
                        status="running",
                        pages=state["pages"],
                        cursor=state["cursor"],
                        rows=state["rows"],
                        last_page_hash=state["last_page_hash"],
                    )
                    if len(response.items) < MAX_MINUTE_ROWS:
                        state.update({"status": "completed", "reason": "provider_page_short", "cursor": state["cursor"]})
                        break
                if raw_pages:
                    frame = pl.concat(raw_pages, how="vertical_relaxed").unique(subset=["symbol", "datetime"], keep="last").sort(["symbol", "datetime"])
                else:
                    frame = normalize_rows([], asset_type=kind)
                _atomic_parquet(frame, raw_path)
                self._record(phase_name, symbol, status="completed", rows=frame.height, min_datetime=str(frame["datetime"].min()) if frame.height else None, max_datetime=str(frame["datetime"].max()) if frame.height else None, content_hash=stable_content_hash(frame.to_dicts()))
                self._clear_minute_pages(page_root)
            except TusharePermissionError as exc:
                self._record(phase_name, symbol, status="blocked", reason="permission_or_auth", error=type(exc).__name__)
            except Exception as exc:  # noqa: BLE001
                self._record(phase_name, symbol, status="failed", error=type(exc).__name__, message=str(exc)[:240])

        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=f"tushare-{phase_name}") as executor:
            list(executor.map(fetch_one, symbols))
        phase["status"] = "completed"
        self._save()

    @staticmethod
    def _clear_minute_pages(page_root: Path) -> None:
        for path in page_root.glob("page-*.parquet"):
            path.unlink()
        try:
            page_root.rmdir()
        except OSError:
            pass

    def _adjustment_frame(self, kind: str, symbol: str) -> pl.DataFrame:
        path = self.run_root / "batches" / "adjustment" / kind / f"{_safe_part(symbol)}.parquet"
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def publish_adjustments(self) -> dict[str, Any]:
        """Publish audited event factors without replacing existing overlap keys."""
        report: dict[str, Any] = {"stock_rows": 0, "etf_rows": 0, "conflicts": []}
        for kind, table in (("stock", "adj_factor"), ("etf", "adj_factor_etf")):
            frames = [
                normalize_adjustment_rows(pl.read_parquet(path))
                for path in sorted((self.run_root / "batches" / "adjustment" / kind).glob("*.parquet"))
            ]
            incoming = pl.concat(frames, how="vertical_relaxed") if frames else normalize_adjustment_rows(pl.DataFrame())
            if incoming.is_empty():
                continue
            target = self.config.data_dir / table / "all.parquet"
            existing = pl.read_parquet(target) if target.exists() else normalize_adjustment_rows(pl.DataFrame())
            if existing.is_empty():
                merged = incoming
            else:
                existing = normalize_adjustment_rows(existing)
                overlap = existing.join(incoming, on=["symbol", "trade_date"], how="inner", suffix="_tushare")
                conflicts = overlap.filter((pl.col("ex_factor") - pl.col("ex_factor_tushare")).abs() > 1e-6)
                if not conflicts.is_empty():
                    values = conflicts.select("symbol", "trade_date").to_dicts()
                    report["conflicts"].extend(values)
                    continue
                merged = pl.concat([existing, incoming.join(existing.select(["symbol", "trade_date"]), on=["symbol", "trade_date"], how="anti")], how="vertical_relaxed")
            _atomic_parquet(merged.unique(subset=["symbol", "trade_date"], keep="last").sort(["symbol", "trade_date"]), target)
            report[f"{kind}_rows"] = merged.height
        if report["conflicts"]:
            self._save(publish_adjustment={"status": "blocked", "conflicts": report["conflicts"]})
            raise BackfillBlocked("adjustment publication blocked by overlap conflicts")
        self._save(publish_adjustment={"status": "published", "report": report})
        return report

    def publish_minutes(self, kinds: Iterable[str] = ("stock", "etf")) -> dict[str, Any]:
        phase = self._phase("publish_minute")
        phase["status"] = "running"
        report: dict[str, Any] = {"partitions": 0, "added_rows": 0, "conflicts": []}
        pending: list[tuple[Path, Path, pl.DataFrame, dict[str, Any], dict[str, Any]]] = []
        for kind in kinds:
            raw_root = self.config.data_dir / "tushare_archive" / ("minute_stock_raw" if kind == "stock" else "minute_etf_raw")
            target_root = self.config.data_dir / ("kline_minute" if kind == "stock" else "kline_etf_minute")
            for raw_path in sorted(raw_root.glob("symbol=*/part.parquet")):
                symbol = raw_path.parent.name.removeprefix("symbol=")
                raw = pl.read_parquet(raw_path)
                adjusted = forward_adjust_minutes(raw, self._adjustment_frame(kind, symbol))
                adjusted, audit = validate_minute_frame(adjusted)
                for day in sorted(set(adjusted["datetime"].dt.date().to_list())) if adjusted.height else ():
                    assert_disk_reserve(self.config.data_dir)
                    incoming = adjusted.filter(pl.col("datetime").dt.date() == day).select(_MINUTE_FIELDS)
                    out = target_root / f"date={day.isoformat()}" / "part.parquet"
                    existing = pl.read_parquet(out) if out.exists() else pl.DataFrame(schema=incoming.schema)
                    merged, overlap_report = overlap_merge(existing, incoming)
                    coverage = minute_coverage_manifest(merged)
                    coverage.update({"trade_date": day.isoformat(), "source": "tushare_proxy", "ownership": "tickflow_overlap_priority"})
                    pending.append((out, target_root / "_coverage" / f"date={day.isoformat()}.json", merged, coverage, overlap_report))
                    report["partitions"] += 1
                    report["added_rows"] += overlap_report.get("added_rows", 0)
                    if overlap_report.get("conflicts"):
                        report["conflicts"].extend(overlap_report["conflicts"])
                    self._record("publish_minute", f"{kind}/{day.isoformat()}", status="staged", rows=merged.height, audit=audit, overlap=overlap_report)
        if report["conflicts"]:
            phase["status"] = "blocked"
            self._save(publish={"status": "blocked", "conflicts": report["conflicts"]})
            raise BackfillBlocked("minute publication blocked by overlap conflicts")
        staged_root = self.run_root / "publish_staging" / "minute"
        backup_root = self.run_root / "backups" / "minute"
        prepared: list[tuple[Path, Path, Path | None]] = []
        for out, coverage_path, merged, coverage, _overlap_report in pending:
            for target, writer in (
                (out, lambda path, value=merged: _atomic_parquet(value, path)),
                (coverage_path, lambda path, value=coverage: _atomic_json(path, value)),
            ):
                try:
                    relative = target.relative_to(self.config.data_dir)
                except ValueError as exc:
                    raise BackfillBlocked("minute publish target escaped data directory") from exc
                staged = staged_root / relative
                writer(staged)
                backup = backup_root / relative if target.exists() else None
                prepared.append((target, staged, backup))

        published: list[tuple[Path, Path | None]] = []
        try:
            for target, staged, backup in prepared:
                target.parent.mkdir(parents=True, exist_ok=True)
                if backup is not None:
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    if backup.exists():
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

        for out, _coverage_path, merged, _coverage, overlap_report in pending:
            key = f"{out.parent.parent.name}/{out.parent.name.removeprefix('date=').removesuffix('.parquet')}"
            self._record("publish_minute", key, status="published", rows=merged.height, overlap=overlap_report)
        phase["status"] = "published"
        self._save(publish={"status": "published", "report": report})
        return report

    def research(self, symbols: list[str], apis: Iterable[str] = RESEARCH_APIS) -> None:
        phase = self._phase("research")
        phase["status"] = "running"
        for api_name in apis:
            for symbol in symbols:
                key = f"{api_name}/{symbol}"
                if phase["items"].get(key, {}).get("status") == "completed":
                    continue
                try:
                    response = self.client.request(api_name, {"ts_code": symbol})
                    self._archive(api_name, key, response)
                    rows = response.rows
                    if rows:
                        frame = pl.DataFrame(rows)
                        target = (
                            self.config.data_dir
                            / "ext_data"
                            / f"tushare_{_safe_part(api_name)}"
                            / f"snapshot={_safe_part(self.run_id)}"
                            / f"symbol={_safe_part(symbol)}.parquet"
                        )
                        _atomic_parquet(frame, target)
                    self._record("research", key, status="completed", rows=len(rows), empty=not rows, content_hash=stable_content_hash(response.raw))
                except TusharePermissionError as exc:
                    self._record("research", key, status="blocked", reason="permission_or_auth", error=type(exc).__name__)
                except Exception as exc:  # noqa: BLE001
                    self._record("research", key, status="failed", error=type(exc).__name__)
        phase["status"] = "completed"
        self._save()

    def run(self) -> dict[str, Any]:
        assert_disk_reserve(self.config.data_dir)
        if set(self.config.phases) == {"audit"}:
            stocks, etfs, indexes = [], [], []
        else:
            stocks, etfs, indexes = self._universe() if "universe" in self.config.phases or not self.manifest.get("symbols") else (self._symbols("stocks"), self._symbols("etfs"), self._symbols("indexes"))
        formal_result, formal_engine = self._run_formal_datasets(stocks, etfs, indexes)
        if "adjustment" in self.config.phases:
            self._fetch_adjustment("stock", "adj_factor", stocks)
            self._fetch_adjustment("etf", "fund_adj", etfs)
        if "stock_minute" in self.config.phases:
            self._fetch_minutes("stock", "stk_mins", stocks)
        if "etf_minute" in self.config.phases:
            self._fetch_minutes("etf", "etf_mins", etfs)
        if "publish_minute" in self.config.phases and self.config.publish:
            if "adjustment" in self.config.phases:
                self.publish_adjustments()
            self.publish_minutes()
        if "p0" in self.config.phases:
            self.research(stocks, apis=("daily", "daily_basic", "income", "balancesheet", "cashflow", "fina_indicator", "forecast", "express", "index_daily"))
        if "research" in self.config.phases:
            self.research([*stocks, *indexes, *etfs])
        if "audit" in self.config.phases and formal_engine is not None:
            self._save(dataset_audit=formal_engine.audit(self._formal_specs()))
        policy = tushare_history_policy()
        coverage = {
            "schema_version": 1,
            "run_id": self.run_id,
            "source": "tushare_proxy",
            "policy": policy,
            "phases": self.manifest.get("phases_state", {}),
            "symbols": self.manifest.get("symbols", {}),
        }
        _atomic_json(self.run_root / "coverage_catalog.json", coverage)
        matrix_path = self.run_root / "capability_matrix.json"
        try:
            matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            matrix = {
                "schema_version": 1,
                "run_id": self.run_id,
                "source": "tushare_proxy",
                "runtime_source": "local_parquet_only",
                "datasets": {},
            }
        matrix["legacy_phases"] = {
            "stock_minute_raw": self.manifest.get("phases_state", {}).get("stock_minute", {}).get("status"),
            "etf_minute_raw": self.manifest.get("phases_state", {}).get("etf_minute", {}).get("status"),
            "minute_canonical": self.manifest.get("publish", {}).get("status"),
            "research_extensions": self.manifest.get("phases_state", {}).get("research", {}).get("status"),
        }
        matrix["formal_publish"] = {
            name: value.get("publish", {}).get("status")
            for name, value in formal_result.items()
        }
        _atomic_json(matrix_path, matrix)
        phase_values = self.manifest.get("phases_state", {}).values()
        failed = any(
            value.get("status") in {"blocked", "failed", "retry"}
            or any(item.get("status") in {"blocked", "failed", "retry"} for item in (value.get("items") or {}).values())
            for value in phase_values
        )
        failed = failed or self.manifest.get("dataset_audit", {}).get("status") == "unhealthy"
        self._save(
            coverage={"catalog": str(self.run_root / "coverage_catalog.json"), "capability_matrix": str(self.run_root / "capability_matrix.json"), "policy": policy},
            status="incomplete" if failed else "completed",
        )
        return self.manifest
