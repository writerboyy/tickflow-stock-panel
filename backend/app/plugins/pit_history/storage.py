"""Normalize raw historical reference files into PIT reference tables.

This module is intentionally source-agnostic. It accepts dated index snapshots
and auditable historical/event facts exported from public pages, one-shot
pulls, or manually cached CSV/XLSX files. A snapshot proves membership only on
its own date and must never be expanded into inferred effective intervals.
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
INDEX_MEMBERSHIP_HISTORY_TABLE = "index_membership_history"
INDUSTRY_MEMBERSHIP_HISTORY_TABLE = "industry_membership_history"
INSTRUMENT_LIFECYCLE_EVENTS_TABLE = "instrument_lifecycle_events"
PARSER_VERSION = "pit_history_v1"
INDUSTRY_PARSER_VERSION = "pit_industry_l1_v2"
CNINFO_SW_STANDARD = "申银万国行业分类标准"
CNINFO_SW_STANDARD_CODE = "008003"
DEFAULT_CSI300_COVERAGE_DATES = (
    date(2021, 8, 2),
    date(2024, 1, 2),
    date(2026, 7, 31),
)
STRICT_INDEX_EXPECTATIONS: dict[str, dict[str, Any]] = {
    "000300.SH": {
        "expected_members": 300,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000905.SH": {
        "expected_members": 500,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000906.SH": {
        "expected_members": 800,
        "sample_dates": DEFAULT_CSI300_COVERAGE_DATES,
    },
    "000852.SH": {
        "expected_members": 1000,
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


def _parse_industry_level(value: object) -> int | None:
    text = _text(value).casefold()
    if not text:
        return None
    levels = {
        "1": 1,
        "l1": 1,
        "一级": 1,
        "2": 2,
        "l2": 2,
        "二级": 2,
        "3": 3,
        "l3": 3,
        "三级": 3,
    }
    if text not in levels:
        raise ValueError(f"invalid industry level: {value!r}")
    return levels[text]


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


def validate_index_membership_history(
    frame: pl.DataFrame,
    *,
    index_symbol: str | None = None,
) -> dict[str, Any]:
    required = {"index_symbol", "snapshot_date", "member_symbol"}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        return {
            "status": "invalid",
            "usable": False,
            "missing_columns": missing_columns,
            "message": "index membership history is missing required snapshot columns",
        }

    selected = frame
    normalized_index = index_symbol.upper() if index_symbol else None
    if normalized_index:
        selected = selected.filter(pl.col("index_symbol") == normalized_index)
    if selected.is_empty():
        return {
            "index_symbol": normalized_index,
            "status": "incomplete",
            "usable": False,
            "rows": 0,
            "snapshot_dates": 0,
            "invalid_snapshot_dates": [],
            "message": "no rows for the requested index",
        }

    duplicate_keys = (
        selected.group_by(["index_symbol", "snapshot_date", "member_symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    counts = (
        selected.group_by(["index_symbol", "snapshot_date"])
        .agg(pl.col("member_symbol").n_unique().alias("members"))
        .sort(["index_symbol", "snapshot_date"])
    )
    invalid: list[dict[str, Any]] = []
    for row in counts.iter_rows(named=True):
        expected = int(
            STRICT_INDEX_EXPECTATIONS.get(str(row["index_symbol"]), {}).get("expected_members", 0)
        )
        if expected <= 0 or int(row["members"]) != expected:
            invalid.append(
                {
                    "index_symbol": str(row["index_symbol"]),
                    "snapshot_date": str(row["snapshot_date"]),
                    "members": int(row["members"]),
                    "expected_members": expected or None,
                }
            )
    usable = duplicate_keys == 0 and not invalid
    return {
        "index_symbol": normalized_index,
        "status": "usable" if usable else "incomplete",
        "usable": usable,
        "rows": selected.height,
        "snapshot_dates": counts.height,
        "duplicate_keys": duplicate_keys,
        "invalid_snapshot_dates": invalid[:20],
        "message": (
            "every stored snapshot has the exact expected constituent count"
            if usable
            else "index membership snapshots are incomplete or duplicated; fail closed"
        ),
    }


def merge_index_membership_frames(
    existing: pl.DataFrame,
    incoming: pl.DataFrame,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """Combine complete snapshots while preserving every existing dated fact."""
    if incoming.is_empty():
        validation = validate_index_membership_history(existing)
        if not validation["usable"]:
            raise ValueError(
                "canonical index membership history failed strict validation: "
                f"{validation}"
            )
        return existing, {
            "table": INDEX_MEMBERSHIP_HISTORY_TABLE,
            "incoming_rows": 0,
            "added_rows": 0,
            "incoming_snapshot_dates": 0,
            "skipped_existing_snapshot_dates": 0,
            "total_rows": existing.height,
            "validation": validation,
        }
    incoming_validation = validate_index_membership_history(incoming)
    if not incoming_validation["usable"]:
        raise ValueError(
            "incoming index membership snapshots failed strict validation: "
            f"{incoming_validation}"
        )

    if existing.is_empty():
        merged = incoming
        existing_keys: set[tuple[str, date]] = set()
    else:
        existing_validation = validate_index_membership_history(existing)
        if not existing_validation["usable"]:
            raise ValueError(
                "existing canonical index membership history failed strict validation: "
                f"{existing_validation}"
            )
        existing_keys = {
            (str(row["index_symbol"]), row["snapshot_date"])
            for row in existing.select("index_symbol", "snapshot_date").unique().iter_rows(
                named=True
            )
        }
        incoming_keys = {
            (str(row["index_symbol"]), row["snapshot_date"])
            for row in incoming.select("index_symbol", "snapshot_date").unique().iter_rows(
                named=True
            )
        }
        conflicts: list[dict[str, Any]] = []
        for index_symbol, snapshot_date in sorted(existing_keys & incoming_keys):
            existing_members = set(
                existing.filter(
                    (pl.col("index_symbol") == index_symbol)
                    & (pl.col("snapshot_date") == snapshot_date)
                )["member_symbol"].to_list()
            )
            incoming_members = set(
                incoming.filter(
                    (pl.col("index_symbol") == index_symbol)
                    & (pl.col("snapshot_date") == snapshot_date)
                )["member_symbol"].to_list()
            )
            if existing_members != incoming_members:
                conflicts.append(
                    {
                        "index_symbol": index_symbol,
                        "snapshot_date": snapshot_date.isoformat(),
                        "existing_only": sorted(existing_members - incoming_members)[:20],
                        "incoming_only": sorted(incoming_members - existing_members)[:20],
                    }
                )
        if conflicts:
            raise ValueError(
                "same-date index membership conflict; canonical table was not changed: "
                f"{conflicts[:20]}"
            )

        key_frame = pl.DataFrame(
            {
                "index_symbol": [item[0] for item in existing_keys],
                "snapshot_date": [item[1] for item in existing_keys],
            },
            schema={"index_symbol": pl.String, "snapshot_date": pl.Date},
        )
        additions = incoming.join(
            key_frame,
            on=["index_symbol", "snapshot_date"],
            how="anti",
        )
        merged = (
            pl.concat([existing, additions], how="diagonal_relaxed")
            .unique(
                subset=["index_symbol", "snapshot_date", "member_symbol"],
                keep="first",
            )
            .sort(["index_symbol", "snapshot_date", "member_symbol"])
        )

    validation = validate_index_membership_history(merged)
    if not validation["usable"]:
        raise ValueError(f"merged index membership history failed strict validation: {validation}")
    incoming_key_count = incoming.select("index_symbol", "snapshot_date").unique().height
    skipped_key_count = len(
        existing_keys
        & {
            (str(row["index_symbol"]), row["snapshot_date"])
            for row in incoming.select("index_symbol", "snapshot_date").unique().iter_rows(
                named=True
            )
        }
    )
    return merged, {
        "table": INDEX_MEMBERSHIP_HISTORY_TABLE,
        "incoming_rows": incoming.height,
        "added_rows": merged.height - existing.height,
        "incoming_snapshot_dates": incoming_key_count,
        "skipped_existing_snapshot_dates": skipped_key_count,
        "total_rows": merged.height,
        "validation": validation,
    }


def merge_index_membership_history(
    data_dir: Path,
    incoming: pl.DataFrame,
) -> dict[str, Any]:
    """Append complete dated snapshots without replacing conflicting facts."""
    existing = read_history_table(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
    merged, result = merge_index_membership_frames(existing, incoming)
    if incoming.is_empty():
        result["published_rows"] = 0
        return result
    published_rows = publish_history_table(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE, merged)
    stored = read_history_table(data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
    if stored.height != merged.height:
        raise RuntimeError(
            "canonical index membership history verification failed: "
            f"expected {merged.height} rows, found {stored.height}"
        )
    result["published_rows"] = published_rows
    return result


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
    industry_standard_code: str | None = None,
    industry_level: int | None = None,
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
    if industry_standard_code is not None:
        if "industry_standard_code" not in frame.columns:
            return {
                "industry_standard": industry_standard,
                "status": "invalid",
                "usable": False,
                "missing_columns": ["industry_standard_code"],
                "message": "industry history does not identify the requested standard code",
            }
        selected = selected.filter(pl.col("industry_standard_code") == industry_standard_code)
    if industry_level is not None:
        if "industry_level" not in frame.columns:
            return {
                "industry_standard": industry_standard,
                "status": "invalid",
                "usable": False,
                "missing_columns": ["industry_level"],
                "message": "industry history does not identify the requested level",
            }
        selected = selected.filter(pl.col("industry_level") == industry_level)
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
        pl.col("effective_to").is_not_null() & (pl.col("effective_to") <= pl.col("effective_from"))
    ).height
    interval_groups = ["member_symbol", "industry_standard"]
    if "industry_standard_code" in selected.columns:
        interval_groups.append("industry_standard_code")
    if "industry_level" in selected.columns:
        interval_groups.append("industry_level")
    duplicate_keys = (
        selected.group_by([*interval_groups, "effective_from"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    ordered = selected.sort([*interval_groups, "effective_from"])
    overlap_count = (
        ordered.with_columns(
            pl.col("effective_to").shift(1).over(interval_groups).alias("_previous_to"),
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
            & (pl.col("effective_to").is_null() | (pl.col("effective_to") > pl.lit(sample_date)))
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
        sample_checks.append(
            {
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
                )
                if expected_frame is not None
                else None,
            }
        )

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
        "industry_standard_code": industry_standard_code,
        "industry_level": industry_level,
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


def _normalize_index_symbol(value: object) -> str:
    text = _text(value).upper()
    return text.replace(".XSHG", ".SH").replace(".XSHE", ".SZ").replace(".XBEI", ".BJ")


def normalize_index_membership_history(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
    index_symbol: str | None = None,
) -> pl.DataFrame:
    output: list[dict[str, Any]] = []
    for row in rows:
        normalized_index = _normalize_index_symbol(
            index_symbol or _pick(row, ["index_symbol", "index_code", "指数代码", "指数编码"])
        )
        if not normalized_index:
            raise ValueError(f"index membership row missing index_symbol: {row!r}")
        symbol = normalize_symbol(
            _pick(
                row,
                ["member_symbol", "stock_code", "证券代码", "股票代码", "品种代码", "成分券代码"],
            )
        )
        if not symbol:
            continue
        snapshot_date = _parse_date(
            _pick(row, ["snapshot_date", "trade_date", "as_of", "日期", "快照日期"])
        )
        if snapshot_date is None:
            raise ValueError(f"index membership row missing snapshot_date: {row!r}")
        source_update_date = _parse_date(
            _pick(row, ["source_update_date", "update_date", "source_timestamp"])
        )
        output.append(
            {
                "index_symbol": normalized_index,
                "index_name": _text(_pick(row, ["index_name", "指数名称"])),
                "member_symbol": symbol,
                "member_code": _member_code(symbol),
                "member_name": _text(
                    _pick(
                        row,
                        ["member_name", "stock_name", "证券简称", "股票简称", "品种名称", "name"],
                    )
                ),
                "snapshot_date": snapshot_date,
                "source_update_date": source_update_date,
                "source": source,
                "provenance": _text(_pick(row, ["provenance"])) or "dated_snapshot",
                "snapshot_hash": _text(_pick(row, ["snapshot_hash"])) or _source_hash(row),
            }
        )

    if not output:
        return pl.DataFrame()
    return (
        pl.DataFrame(output)
        .select(
            [
                pl.col("index_symbol").cast(pl.String),
                pl.col("index_name").cast(pl.String),
                pl.col("member_symbol").cast(pl.String),
                pl.col("member_code").cast(pl.String),
                pl.col("member_name").cast(pl.String),
                pl.col("snapshot_date").cast(pl.Date),
                pl.col("source_update_date").cast(pl.Date),
                pl.col("source").cast(pl.String),
                pl.col("provenance").cast(pl.String),
                pl.col("snapshot_hash").cast(pl.String),
            ]
        )
        .unique(
            subset=["index_symbol", "snapshot_date", "member_symbol"],
            keep="last",
        )
        .sort(["index_symbol", "snapshot_date", "member_symbol"])
    )


def normalize_industry_membership_history(
    rows: Iterable[dict[str, Any]],
    *,
    source: str,
) -> pl.DataFrame:
    grouped: dict[tuple[str, str, str, int | None], list[dict[str, Any]]] = defaultdict(list)
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
        industry_standard_code = _text(
            _pick(
                row,
                [
                    "industry_standard_code",
                    "classification_standard_code",
                    "分类标准编码",
                ],
            )
        )
        industry_level = _parse_industry_level(
            _pick(row, ["industry_level", "level", "行业级别"])
        )
        is_cninfo_sw = (
            industry_standard_code == CNINFO_SW_STANDARD_CODE
            or industry_standard == CNINFO_SW_STANDARD
        )
        if is_cninfo_sw:
            industry_standard = CNINFO_SW_STANDARD
            industry_standard_code = CNINFO_SW_STANDARD_CODE
            industry_level = 1
            industry_code = _first_text(row, ["industry_l1_code", "l1_code"])
            industry_name = _first_text(
                row,
                ["industry_l1_name", "l1_name", "行业门类", "门类名称"],
            )
        elif industry_level == 1:
            industry_code = _first_text(
                row,
                ["industry_l1_code", "l1_code", "industry_code", "行业代码"],
            )
            industry_name = _first_text(
                row,
                [
                    "industry_l1_name",
                    "l1_name",
                    "industry_name",
                    "行业名称",
                    "行业门类",
                    "门类名称",
                ],
            )
        else:
            industry_code = _text(_pick(row, ["industry_code", "行业编码", "行业代码"]))
            industry_name = _first_text(
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
            )
        if not industry_name:
            continue
        item = {
            "member_symbol": symbol,
            "member_code": _member_code(symbol),
            "member_name": _text(
                _pick(
                    row, ["member_name", "证券简称", "股票简称", "新证券简称", "name", "股票名称"]
                )
            ),
            "industry_standard": industry_standard,
            "industry_standard_code": industry_standard_code,
            "industry_level": industry_level,
            "industry_code": industry_code,
            "industry_name": industry_name,
            "effective_from": effective_from,
            "_provider_effective_to": _parse_date(
                _pick(row, ["effective_to", "out_date", "失效日期", "结束日期"])
            ),
            "source": source,
            "provenance": "historical_event",
            "raw_hash": _source_hash(row),
        }
        grouped[(symbol, industry_standard, industry_standard_code, industry_level)].append(item)

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
            item["effective_to"] = item.pop("_provider_effective_to") or next_from
            output.append(item)

    if not output:
        return pl.DataFrame()
    return (
        pl.DataFrame(output)
        .select(
            [
                pl.col("member_symbol").cast(pl.String),
                pl.col("member_code").cast(pl.String),
                pl.col("member_name").cast(pl.String),
                pl.col("industry_standard").cast(pl.String),
                pl.col("industry_standard_code").cast(pl.String),
                pl.col("industry_level").cast(pl.Int64),
                pl.col("industry_code").cast(pl.String),
                pl.col("industry_name").cast(pl.String),
                pl.col("effective_from").cast(pl.Date),
                pl.col("effective_to").cast(pl.Date),
                pl.col("source").cast(pl.String),
                pl.col("provenance").cast(pl.String),
                pl.col("raw_hash").cast(pl.String),
            ]
        )
        .sort(
            [
                "member_symbol",
                "industry_standard",
                "industry_standard_code",
                "industry_level",
                "effective_from",
            ]
        )
    )


_LIFECYCLE_DATE_ALIASES: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("listed", ("listed_date", "上市日期", "挂牌日期"), "listed"),
    ("suspended", ("suspend_date", "暂停上市日期", "暂停交易日期"), "suspended"),
    (
        "delist_decision",
        ("delist_decision_date", "终止上市决定日期", "退市决定日期"),
        "delist_decision",
    ),
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
            _pick(
                row, ["symbol", "证券代码", "股票代码", "A股代码", "公司代码", "代码", "stock_code"]
            )
        )
        if not symbol:
            continue
        base = {
            "symbol": symbol,
            "name": _text(
                _pick(row, ["name", "证券简称", "股票简称", "公司简称", "公司名称", "名称"])
            ),
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
            output.append(
                {
                    **base,
                    "event_date": event_date,
                    "event_type": event_type,
                    "event_status": event_status,
                }
            )

    if not output:
        return pl.DataFrame()
    return (
        pl.DataFrame(output)
        .select(
            [
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
            ]
        )
        .sort(["symbol", "event_date", "event_type"])
    )


def source_payload_hash(path: Path, rows: list[dict[str, Any]]) -> str:
    digest = sha256()
    digest.update(str(path).encode("utf-8"))
    digest.update(stable_content_hash(rows).encode("utf-8"))
    return digest.hexdigest()
