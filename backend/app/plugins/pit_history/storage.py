"""Normalize raw historical reference files into PIT interval/event tables.

This module is intentionally source-agnostic. It accepts rows exported from
public pages, AKShare one-shot pulls, or manually cached CSV/XLSX files and
publishes only auditable historical/event facts. Current snapshots remain in
their own source-specific tables and must not be used to backfill history.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime
from hashlib import sha256
import os
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

import polars as pl

from app.services.ingestion_manifest import stable_content_hash


SOURCE = "pit_history"
INDEX_MEMBERSHIP_EVENTS_TABLE = "index_membership_events"
INDUSTRY_MEMBERSHIP_HISTORY_TABLE = "industry_membership_history"
INSTRUMENT_LIFECYCLE_EVENTS_TABLE = "instrument_lifecycle_events"
PARSER_VERSION = "pit_history_v1"
DEFAULT_STRICT_INDEX_MIN_MEMBERS = 250
DEFAULT_CSI300_COVERAGE_DATES = (
    date(2021, 8, 2),
    date(2024, 1, 2),
    date(2026, 7, 31),
)
STRICT_INDEX_EXPECTATIONS: dict[str, dict[str, Any]] = {
    "000300.SH": {
        "expected_min_members": DEFAULT_STRICT_INDEX_MIN_MEMBERS,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000905.SH": {
        "expected_min_members": 450,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000906.SH": {
        "expected_min_members": 750,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000852.SH": {
        "expected_min_members": 950,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
}
COMPLETE_LIFECYCLE_EVENT_TYPES = (
    "listed",
    "delist_decision",
    "delist_period_start",
    "delist_period_end",
    "delisted",
)

_DIGITS = re.compile(r"\d+")
_DATE_DIGITS = re.compile(r"^\d{8}$")


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.casefold() in {"", "nan", "nat", "none", "null", "<na>", "--", "-"}:
        return ""
    return text


def _normalized_row(row: dict[str, Any]) -> dict[str, Any]:
    return {str(key).strip(): value for key, value in row.items()}


def _pick(row: dict[str, Any], aliases: Iterable[str]) -> object:
    normalized = _normalized_row(row)
    by_lower = {key.casefold(): value for key, value in normalized.items()}
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
        value = by_lower.get(alias.casefold())
        if value is not None:
            return value
    return None


def _first_text(row: dict[str, Any], aliases: Iterable[str]) -> str:
    for alias in aliases:
        value = _text(_pick(row, [alias]))
        if value:
            return value
    return ""


def _parse_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = _text(value)
    if not text or text in {"至今", "现在", "当前", "目前"}:
        return None
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    if _DATE_DIGITS.match(text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    text = text.replace("/", "-").replace(".", "-")
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return parsed.date()
    raise ValueError(f"invalid date value: {value!r}")


def _source_hash(row: dict[str, Any]) -> str:
    return stable_content_hash(_normalized_row(row))


def normalize_symbol(value: object) -> str:
    text = _text(value).upper()
    if not text:
        return ""
    text = text.replace(".XSHG", ".SH").replace(".XSHE", ".SZ").replace(".XBEI", ".BJ")
    if text.endswith((".SH", ".SZ", ".BJ")):
        code, suffix = text.split(".", 1)
        digits = "".join(_DIGITS.findall(code))
        return f"{digits.zfill(6)}.{suffix}" if digits else text
    lowered = text.lower()
    if lowered.startswith(("sh", "sz", "bj")):
        prefix = lowered[:2]
        code = "".join(_DIGITS.findall(text))
        suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}[prefix]
        return f"{code.zfill(6)}.{suffix}" if code else ""
    code = "".join(_DIGITS.findall(text))
    if not code:
        return ""
    code = code.zfill(6)
    if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
        suffix = "SH"
    elif code.startswith(("000", "001", "002", "003", "300", "301", "200")):
        suffix = "SZ"
    elif code.startswith(("4", "8", "9")):
        suffix = "BJ"
    else:
        suffix = ""
    return f"{code}.{suffix}" if suffix else code


def _member_code(symbol: str) -> str:
    return symbol.split(".", 1)[0] if "." in symbol else symbol


def _table_root(data_dir: Path, table: str) -> Path:
    return Path(data_dir) / "pit_reference" / "history" / table


def table_path(data_dir: Path, table: str) -> Path:
    return _table_root(data_dir, table) / "part.parquet"


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def publish_history_table(data_dir: Path, table: str, frame: pl.DataFrame) -> int:
    if frame.is_empty():
        return 0
    _atomic_write_parquet(frame, table_path(data_dir, table))
    return frame.height


def read_history_table(data_dir: Path, table: str) -> pl.DataFrame:
    path = table_path(data_dir, table)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def index_members_on_date(frame: pl.DataFrame, *, index_symbol: str, as_of: date) -> int:
    if frame.is_empty() or not {"index_symbol", "member_symbol", "effective_from"}.issubset(
        frame.columns
    ):
        return 0
    normalized_index = index_symbol.upper()
    active = frame.filter(
        (pl.col("index_symbol") == normalized_index)
        & (pl.col("effective_from") <= pl.lit(as_of))
        & (
            ~pl.col("effective_to").is_not_null()
            | (pl.col("effective_to") > pl.lit(as_of))
        )
    )
    return int(active["member_symbol"].n_unique()) if "member_symbol" in active.columns else 0


def validate_index_membership_coverage(
    frame: pl.DataFrame,
    *,
    index_symbol: str,
    sample_dates: Iterable[date] | None = None,
    expected_min_members: int | None = None,
) -> dict[str, Any]:
    normalized_index = index_symbol.upper()
    expectation = STRICT_INDEX_EXPECTATIONS.get(normalized_index, {})
    dates = tuple(sample_dates or expectation.get("sample_dates") or ())
    minimum = int(expected_min_members or expectation.get("expected_min_members") or 0)
    checks = [
        {
            "date": item.isoformat(),
            "members": index_members_on_date(frame, index_symbol=normalized_index, as_of=item),
            "expected_min_members": minimum,
        }
        for item in dates
    ]
    for item in checks:
        item["ok"] = bool(minimum and item["members"] >= minimum)
    usable = bool(checks) and all(bool(item["ok"]) for item in checks)
    message = (
        "representative PIT membership counts satisfy strict backtest minimum"
        if usable
        else "historical membership is incomplete; do not use this index as a strict PIT pool"
    )
    return {
        "index_symbol": normalized_index,
        "status": "usable" if usable else "incomplete",
        "usable": usable,
        "expected_min_members": minimum,
        "coverage_checks": checks,
        "message": message,
    }


def summarize_industry_standards(frame: pl.DataFrame) -> dict[str, Any]:
    if frame.is_empty() or "industry_standard" not in frame.columns:
        standards: list[dict[str, Any]] = []
    else:
        standards = (
            frame.group_by("industry_standard")
            .agg(
                pl.len().alias("rows"),
                pl.col("member_symbol").n_unique().alias("symbols_covered"),
                pl.col("effective_from").min().alias("earliest_date"),
                pl.col("effective_from").max().alias("latest_date"),
            )
            .sort("industry_standard")
            .to_dicts()
        )
    return {
        "requires_industry_standard": True,
        "usable_with_single_standard": bool(standards),
        "standards": [
            {
                "industry_standard": str(row["industry_standard"]),
                "rows": int(row["rows"] or 0),
                "symbols_covered": int(row["symbols_covered"] or 0),
                "earliest_date": str(row["earliest_date"]) if row["earliest_date"] else None,
                "latest_date": str(row["latest_date"]) if row["latest_date"] else None,
            }
            for row in standards
        ],
        "message": "filter exactly one industry_standard before joining a PIT industry panel",
    }


def validate_industry_history_coverage(
    frame: pl.DataFrame,
    *,
    industry_standard: str,
    sample_dates: Iterable[date] = (),
    daily_frame: pl.DataFrame | None = None,
    min_coverage: float = 0.95,
) -> dict[str, Any]:
    """Validate one industry's PIT intervals against an observed daily universe.

    A current industry snapshot is not sufficient evidence for this check.  When
    ``daily_frame`` is supplied, each sample date is compared with the symbols
    that actually had a canonical daily bar on that date.  The function reports
    missing coverage and interval defects without filling them implicitly.
    """
    if not 0 < min_coverage <= 1:
        raise ValueError("min_coverage must be in (0, 1]")
    required = {
        "member_symbol",
        "industry_standard",
        "effective_from",
        "effective_to",
    }
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        return {
            "industry_standard": industry_standard,
            "status": "invalid",
            "usable": False,
            "missing_columns": missing_columns,
            "message": "industry history is missing required interval columns",
        }

    selected = frame.filter(pl.col("industry_standard") == industry_standard)
    if selected.is_empty():
        return {
            "industry_standard": industry_standard,
            "status": "incomplete",
            "usable": False,
            "rows": 0,
            "symbols_covered": 0,
            "sample_checks": [],
            "message": "no rows for the requested industry standard",
        }

    invalid_intervals = selected.filter(
        pl.col("effective_to").is_not_null()
        & (pl.col("effective_to") <= pl.col("effective_from"))
    ).height
    duplicate_keys = (
        selected.group_by(["member_symbol", "industry_standard", "effective_from"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    ordered = selected.sort(["member_symbol", "effective_from"])
    overlap_count = (
        ordered
        .with_columns(
            pl.col("effective_to").shift(1).over("member_symbol").alias("_previous_to"),
        )
        .filter(
            pl.col("_previous_to").is_not_null()
            & (pl.col("effective_from") < pl.col("_previous_to"))
        )
        .height
    )

    sample_checks: list[dict[str, Any]] = []
    expected_frame = daily_frame
    if expected_frame is not None and not expected_frame.is_empty():
        expected_frame = expected_frame.select("symbol", "date").unique()
    for sample_date in sample_dates:
        active = selected.filter(
            (pl.col("effective_from") <= pl.lit(sample_date))
            & (
                pl.col("effective_to").is_null()
                | (pl.col("effective_to") > pl.lit(sample_date))
            )
        )
        active_symbols = set(active["member_symbol"].to_list())
        expected_members: int | None = None
        covered_members: int | None = None
        coverage: float | None = None
        if expected_frame is not None:
            expected = expected_frame.filter(pl.col("date") == pl.lit(sample_date))
            expected_symbols = set(expected["symbol"].to_list())
            expected_members = len(expected_symbols)
            covered_members = len(expected_symbols & active_symbols)
            coverage = covered_members / expected_members if expected_members else 0.0
        sample_checks.append({
            "date": sample_date.isoformat(),
            "active_members": len(active_symbols),
            "expected_members": expected_members,
            "covered_members": covered_members,
            "coverage": coverage,
            "ok": (
                coverage is not None
                and expected_members is not None
                and expected_members > 0
                and coverage >= min_coverage
            ) if expected_frame is not None else None,
        })

    sample_failures = [item for item in sample_checks if item["ok"] is False]
    usable = bool(
        sample_checks
        and not sample_failures
        and invalid_intervals == 0
        and duplicate_keys == 0
        and overlap_count == 0
    )
    return {
        "industry_standard": industry_standard,
        "status": "usable" if usable else "incomplete",
        "usable": usable,
        "rows": selected.height,
        "symbols_covered": selected["member_symbol"].n_unique(),
        "earliest_date": str(selected["effective_from"].min()),
        "latest_date": str(selected["effective_from"].max()),
        "invalid_intervals": invalid_intervals,
        "duplicate_keys": duplicate_keys,
        "overlap_intervals": overlap_count,
        "min_coverage": min_coverage,
        "sample_checks": sample_checks,
        "message": (
            "industry PIT intervals cover the observed daily universe"
            if usable
            else "industry history is incomplete or has invalid/overlapping intervals"
        ),
    }


def summarize_lifecycle_completeness(frame: pl.DataFrame) -> dict[str, Any]:
    event_types: set[str] = set()
    by_symbol: dict[str, set[str]] = defaultdict(set)
    symbols_with_reason: set[str] = set()
    reason_event_rows = 0
    for row in frame.iter_rows(named=True) if not frame.is_empty() else []:
        symbol = str(row.get("symbol") or "")
        event_type = str(row.get("event_type") or "")
        reason = _text(row.get("reason"))
        if event_type:
            event_types.add(event_type)
        if symbol and event_type:
            by_symbol[symbol].add(event_type)
        if symbol and reason:
            symbols_with_reason.add(symbol)
            reason_event_rows += 1

    required = set(COMPLETE_LIFECYCLE_EVENT_TYPES)
    delisted_symbols = [symbol for symbol, types in by_symbol.items() if "delisted" in types]
    complete_symbols = [
        symbol
        for symbol in delisted_symbols
        if required.issubset(by_symbol[symbol]) and symbol in symbols_with_reason
    ]
    complete = bool(delisted_symbols) and len(complete_symbols) == len(delisted_symbols)
    return {
        "status": "complete" if complete else "partial",
        "complete_lifecycle": complete,
        "required_event_types": list(COMPLETE_LIFECYCLE_EVENT_TYPES),
        "available_event_types": sorted(event_types),
        "missing_event_types": sorted(required - event_types),
        "delisted_symbols": len(delisted_symbols),
        "complete_delisted_symbols": len(complete_symbols),
        "reason_event_rows": reason_event_rows,
        "message": (
            "all delisted symbols include decision, delisting-period and reason fields"
            if complete
            else "source lacks full delisting decision/period/reason coverage for complete lifecycle"
        ),
    }


def normalize_index_membership_events(
    rows: Iterable[dict[str, Any]],
    *,
    index_symbol: str,
    source: str,
) -> pl.DataFrame:
    output: list[dict[str, Any]] = []
    normalized_index = index_symbol.upper()
    for row in rows:
        symbol = normalize_symbol(
            _pick(row, ["member_symbol", "stock_code", "证券代码", "股票代码", "品种代码", "成分券代码"])
        )
        if not symbol:
            continue
        effective_from = _parse_date(
            _pick(row, ["effective_from", "in_date", "纳入日期", "入选日期", "生效日期", "起始日期"])
        )
        if effective_from is None:
            raise ValueError(f"index membership row missing effective_from: {row!r}")
        effective_to = _parse_date(
            _pick(row, ["effective_to", "out_date", "剔除日期", "调出日期", "失效日期", "终止日期"])
        )
        if effective_to is not None and effective_to <= effective_from:
            raise ValueError(f"index membership effective_to must be after effective_from: {row!r}")
        output.append({
            "index_symbol": normalized_index,
            "member_symbol": symbol,
            "member_code": _member_code(symbol),
            "member_name": _text(
                _pick(row, ["member_name", "stock_name", "证券简称", "股票简称", "品种名称", "name"])
            ),
            "effective_from": effective_from,
            "effective_to": effective_to,
            "source": source,
            "provenance": "historical_event",
            "raw_hash": _source_hash(row),
        })

    if not output:
        return pl.DataFrame()
    return pl.DataFrame(output).select([
        pl.col("index_symbol").cast(pl.String),
        pl.col("member_symbol").cast(pl.String),
        pl.col("member_code").cast(pl.String),
        pl.col("member_name").cast(pl.String),
        pl.col("effective_from").cast(pl.Date),
        pl.col("effective_to").cast(pl.Date),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("raw_hash").cast(pl.String),
    ]).unique(
        subset=["index_symbol", "member_symbol", "effective_from"],
        keep="last",
    ).sort(["index_symbol", "effective_from", "member_symbol"])


def normalize_industry_membership_history(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
) -> pl.DataFrame:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = normalize_symbol(
            _pick(row, ["member_symbol", "stock_code", "证券代码", "股票代码", "A股代码"])
        )
        if not symbol:
            continue
        effective_from = _parse_date(
            _pick(row, ["effective_from", "change_date", "变更日期", "变更时间", "生效日期"])
        )
        if effective_from is None:
            raise ValueError(f"industry history row missing effective_from: {row!r}")
        industry_standard = _text(
            _pick(row, ["industry_standard", "分类标准", "行业标准", "standard"])
        )
        if not industry_standard:
            industry_standard = "unknown"
        item = {
            "member_symbol": symbol,
            "member_code": _member_code(symbol),
            "member_name": _text(
                _pick(row, ["member_name", "证券简称", "股票简称", "新证券简称", "name", "股票名称"])
            ),
            "industry_standard": industry_standard,
            "industry_code": _text(_pick(row, ["industry_code", "行业编码", "行业代码"])),
            "industry_name": _first_text(
                row,
                [
                    "industry_name",
                    "行业名称",
                    "所属行业",
                    "行业中类",
                    "行业大类",
                    "行业次类",
                    "行业门类",
                    "门类名称",
                    "大类名称",
                ],
            ),
            "effective_from": effective_from,
            "source": source,
            "provenance": "historical_event",
            "raw_hash": _source_hash(row),
        }
        grouped[(symbol, industry_standard)].append(item)

    output: list[dict[str, Any]] = []
    for items in grouped.values():
        items = sorted(items, key=lambda item: (item["effective_from"], item["industry_code"]))
        deduped: list[dict[str, Any]] = []
        for item in items:
            if deduped and deduped[-1]["effective_from"] == item["effective_from"]:
                deduped[-1] = item
            else:
                deduped.append(item)
        for index, item in enumerate(deduped):
            next_from = deduped[index + 1]["effective_from"] if index + 1 < len(deduped) else None
            item = dict(item)
            item["effective_to"] = next_from
            output.append(item)

    if not output:
        return pl.DataFrame()
    return pl.DataFrame(output).select([
        pl.col("member_symbol").cast(pl.String),
        pl.col("member_code").cast(pl.String),
        pl.col("member_name").cast(pl.String),
        pl.col("industry_standard").cast(pl.String),
        pl.col("industry_code").cast(pl.String),
        pl.col("industry_name").cast(pl.String),
        pl.col("effective_from").cast(pl.Date),
        pl.col("effective_to").cast(pl.Date),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("raw_hash").cast(pl.String),
    ]).sort(["member_symbol", "industry_standard", "effective_from"])


_LIFECYCLE_DATE_ALIASES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("listed", ("listed_date", "上市日期", "挂牌日期"), "listed"),
    ("suspended", ("suspend_date", "暂停上市日期", "暂停交易日期"), "suspended"),
    ("delist_decision", ("delist_decision_date", "终止上市决定日期", "退市决定日期"), "delist_decision"),
    (
        "delist_period_start",
        ("delist_period_start", "退市整理期开始日期", "退市整理起始日"),
        "delist_period",
    ),
    (
        "delist_period_end",
        ("delist_period_end", "退市整理期结束日期", "退市整理终止日"),
        "delist_period",
    ),
    ("delisted", ("delisted_date", "终止上市日期", "退市日期", "摘牌日期"), "delisted"),
)


def normalize_instrument_lifecycle_events(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
    provenance: str = "historical_event",
) -> pl.DataFrame:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, date]] = set()
    for row in rows:
        symbol = normalize_symbol(
            _pick(row, ["symbol", "证券代码", "股票代码", "A股代码", "公司代码", "代码", "stock_code"])
        )
        if not symbol:
            continue
        base = {
            "symbol": symbol,
            "name": _text(_pick(row, ["name", "证券简称", "股票简称", "公司简称", "公司名称", "名称"])),
            "exchange": _text(_pick(row, ["exchange", "交易所", "上市地点", "市场"])),
            "reason": _text(_pick(row, ["reason", "终止上市原因", "退市原因", "摘牌原因"])),
            "source": source,
            "provenance": provenance,
            "raw_hash": _source_hash(row),
        }
        for event_type, aliases, event_status in _LIFECYCLE_DATE_ALIASES:
            event_date = _parse_date(_pick(row, aliases))
            if event_date is None:
                continue
            key = (symbol, event_type, event_date)
            if key in seen:
                continue
            seen.add(key)
            output.append({
                **base,
                "event_date": event_date,
                "event_type": event_type,
                "event_status": event_status,
            })

    if not output:
        return pl.DataFrame()
    return pl.DataFrame(output).select([
        pl.col("symbol").cast(pl.String),
        pl.col("name").cast(pl.String),
        pl.col("exchange").cast(pl.String),
        pl.col("event_date").cast(pl.Date),
        pl.col("event_type").cast(pl.String),
        pl.col("event_status").cast(pl.String),
        pl.col("reason").cast(pl.String),
        pl.col("source").cast(pl.String),
        pl.col("provenance").cast(pl.String),
        pl.col("raw_hash").cast(pl.String),
    ]).sort(["symbol", "event_date", "event_type"])


def source_payload_hash(path: Path, rows: list[dict[str, Any]]) -> str:
    digest = sha256()
    digest.update(str(path).encode("utf-8"))
    digest.update(stable_content_hash(rows).encode("utf-8"))
    return digest.hexdigest()
