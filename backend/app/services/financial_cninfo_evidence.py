"""Collect Cninfo announcement evidence for unresolved financial conflicts."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import polars as pl


FINANCIAL_CONFLICT_TABLES = ("metrics", "income", "balance_sheet", "cash_flow")
_KEYS = ("symbol", "period_end", "announce_date")
_AXDATA_PATHS = (
    ("libs", "axdata_core"),
    ("packages", "axdata-source-cninfo", "src"),
)


class CninfoEvidenceError(RuntimeError):
    """Raised when Cninfo evidence cannot be collected safely."""


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=_json_default)
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_table(data_dir: Path, table: str) -> pl.DataFrame:
    path = data_dir / "financials" / table / "part.parquet"
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def _same(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _conflicting_groups(frame: pl.DataFrame) -> list[pl.DataFrame]:
    if frame.is_empty() or not set(_KEYS) <= set(frame.columns):
        return []
    deduped = frame.unique(maintain_order=True)
    groups = (
        deduped.group_by(list(_KEYS))
        .agg(pl.len().alias("_rows"))
        .filter(pl.col("_rows") > 1)
        .sort(list(_KEYS))
    )
    conflicts: list[pl.DataFrame] = []
    for key in groups.iter_rows(named=True):
        rows = deduped.filter(pl.all_horizontal(pl.col(field) == key[field] for field in _KEYS))
        if rows.height > 1:
            conflicts.append(rows)
    return conflicts


def _different_fields(rows: pl.DataFrame) -> list[str]:
    fields: list[str] = []
    for column in rows.columns:
        if column in _KEYS:
            continue
        values = rows[column].to_list()
        if any(not _same(values[0], value) for value in values[1:]):
            fields.append(column)
    return sorted(fields)


def _candidate_values(rows: pl.DataFrame, fields: list[str]) -> dict[str, list[Any]]:
    return {
        field: [_jsonable(value) for value in rows[field].to_list()]
        for field in fields
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None:
        return None
    try:
        if value != value:  # NaN
            return None
    except TypeError:
        pass
    return value


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is empty")
    return date.fromisoformat(text[:10])


def _date_text(day: date) -> str:
    return day.strftime("%Y%m%d")


def _category_for_period(period_end: date) -> str | None:
    month_day = (period_end.month, period_end.day)
    if month_day == (3, 31):
        return "一季报"
    if month_day == (6, 30):
        return "半年报"
    if month_day == (9, 30):
        return "三季报"
    if month_day == (12, 31):
        return "年报"
    return None


def _financial_year(period_end: date) -> str:
    return str(period_end.year)


def _announcement_queries(symbol: str, period_end: date, announce_date: date, window_days: int) -> list[dict[str, Any]]:
    start = announce_date - timedelta(days=window_days)
    end = announce_date + timedelta(days=window_days)
    base = {
        "code": symbol,
        "start_date": _date_text(start),
        "end_date": _date_text(end),
        "limit": 100,
    }
    category = _category_for_period(period_end)
    queries: list[dict[str, Any]] = []
    if category:
        queries.append({**base, "category": category})
    queries.append({**base, "category": "补充更正"})
    queries.append({**base, "keyword": _financial_year(period_end)})
    queries.append(base)
    unique: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for query in queries:
        key = tuple(sorted((name, str(value)) for name, value in query.items()))
        if key not in seen:
            seen.add(key)
            unique.append(query)
    return unique


def _prepare_axdata_paths(axdata_root: Path | None) -> dict[str, Any]:
    if axdata_root is None:
        return {"axdata_root": None, "paths_added": []}
    root = Path(axdata_root)
    if not root.exists():
        raise CninfoEvidenceError(f"AxData root does not exist: {root}")
    added: list[str] = []
    for parts in _AXDATA_PATHS:
        candidate = root.joinpath(*parts)
        if candidate.exists():
            text = str(candidate)
            if text not in sys.path:
                sys.path.insert(0, text)
                added.append(text)
    return {"axdata_root": str(root), "paths_added": added}


def _create_cninfo_adapter(axdata_root: Path | None = None) -> tuple[Any, dict[str, Any]]:
    meta = _prepare_axdata_paths(axdata_root)
    try:
        from axdata_core.adapters.cninfo import CninfoRequestAdapter
    except Exception as exc:  # noqa: BLE001
        raise CninfoEvidenceError(
            "Cannot import AxData CninfoRequestAdapter; install AxData or pass --axdata-root"
        ) from exc
    return CninfoRequestAdapter(), meta


def _normalize_announcement(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "announcement_id": _jsonable(row.get("announcement_id")),
        "title": _jsonable(row.get("title")),
        "publish_date": _jsonable(row.get("publish_date")),
        "file_type": _jsonable(row.get("file_type")),
        "file_size_kb": _jsonable(row.get("file_size_kb")),
        "download_url": _jsonable(row.get("download_url")),
    }


def _announcement_relevance(announcement: Mapping[str, Any], period_end: date) -> int:
    title = str(announcement.get("title") or "")
    score = 0
    if _financial_year(period_end) in title:
        score += 20
    if "更正后" in title:
        score += 30
    for keyword in ("差错", "更正", "追溯调整", "财务报表", "财务报告"):
        if keyword in title:
            score += 40
    for keyword in ("年度报告", "半年度报告", "第一季度报告", "第三季度报告", "季度报告"):
        if keyword in title:
            score += 25
    category = _category_for_period(period_end)
    if category and category in title:
        score += 20
    if "摘要" in title:
        score -= 5
    for keyword in ("独立董事", "公司章程", "薪酬", "募集资金", "ESG", "董事会", "监事会"):
        if keyword in title:
            score -= 20
    return score


def _detail_for_announcement(adapter: Any, announcement: Mapping[str, Any]) -> dict[str, Any]:
    download_url = str(announcement.get("download_url") or "").strip()
    if not download_url:
        return {"status": "missing_download_url"}
    try:
        detail = adapter.request(
            "cninfo_announcement_detail",
            {
                "announcement_id": announcement.get("announcement_id"),
                "url": download_url,
                "title": announcement.get("title"),
            },
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "unavailable", "error": type(exc).__name__, "message": str(exc)}
    if not detail:
        return {"status": "empty"}
    item = dict(detail[0])
    item["status"] = "available"
    return {key: _jsonable(value) for key, value in item.items()}


def _download_pdf(url: str, *, timeout: float = 20.0) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 TickFlow/0.1",
            "Accept": "application/pdf,*/*",
            "Referer": "https://www.cninfo.com.cn/",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - trusted Cninfo URL from AxData.
            return response.read()
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise CninfoEvidenceError(f"failed to download Cninfo PDF: {exc}") from exc


def _cache_pdf(
    data_dir: Path,
    announcement: Mapping[str, Any],
    *,
    pdf_fetcher: Callable[[str], bytes] = _download_pdf,
) -> dict[str, Any]:
    download_url = str(announcement.get("download_url") or "").strip()
    announcement_id = str(announcement.get("announcement_id") or "").strip()
    if not download_url or not announcement_id:
        return {"status": "skipped", "reason": "missing announcement_id_or_url"}
    content = pdf_fetcher(download_url)
    digest = sha256(content).hexdigest()
    cache_dir = data_dir / "financials" / "cninfo_evidence_pdfs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{announcement_id}.pdf"
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {
        "status": "cached",
        "path": path.relative_to(data_dir).as_posix(),
        "sha256": digest,
        "bytes": len(content),
    }


def _collect_group_evidence(
    adapter: Any,
    data_dir: Path,
    table: str,
    rows: pl.DataFrame,
    *,
    window_days: int,
    download_pdfs: bool,
    max_pdf_downloads: int,
    pdf_fetcher: Callable[[str], bytes],
    pdf_download_count: int,
) -> tuple[dict[str, Any], int]:
    symbol = str(rows["symbol"][0])
    period_end = _parse_date(rows["period_end"][0], "period_end")
    announce_date = _parse_date(rows["announce_date"][0], "announce_date")
    fields = _different_fields(rows)
    queries = _announcement_queries(symbol, period_end, announce_date, window_days)
    by_id: dict[str, dict[str, Any]] = {}
    query_results: list[dict[str, Any]] = []
    for query in queries:
        try:
            announcements = adapter.request("stock_zh_a_disclosure_report_cninfo", query)
            query_status = "available"
            error = None
        except Exception as exc:  # noqa: BLE001
            announcements = []
            query_status = "unavailable"
            error = {"type": type(exc).__name__, "message": str(exc)}
        normalized = [_normalize_announcement(row) for row in announcements]
        for announcement in normalized:
            key = str(announcement.get("announcement_id") or announcement.get("download_url") or "")
            if key:
                by_id.setdefault(key, announcement)
        item: dict[str, Any] = {
            "query": query,
            "status": query_status,
            "rows": len(normalized),
        }
        if error:
            item["error"] = error
        query_results.append(item)

    announcements_out: list[dict[str, Any]] = []
    ranked_announcements = sorted(
        by_id.values(),
        key=lambda row: (
            -_announcement_relevance(row, period_end),
            str(row.get("publish_date")),
            str(row.get("announcement_id")),
        ),
    )
    for announcement in ranked_announcements:
        relevance_score = _announcement_relevance(announcement, period_end)
        detail = _detail_for_announcement(adapter, announcement)
        entry = {
            **announcement,
            "relevance_score": relevance_score,
            "detail": detail,
        }
        if download_pdfs and relevance_score <= 0:
            entry["pdf_cache"] = {"status": "skipped", "reason": "low_relevance"}
        elif download_pdfs and pdf_download_count < max_pdf_downloads:
            try:
                entry["pdf_cache"] = _cache_pdf(data_dir, announcement, pdf_fetcher=pdf_fetcher)
                if entry["pdf_cache"].get("status") == "cached":
                    pdf_download_count += 1
            except CninfoEvidenceError as exc:
                entry["pdf_cache"] = {
                    "status": "unavailable",
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
        elif download_pdfs:
            entry["pdf_cache"] = {"status": "skipped", "reason": "max_pdf_downloads_reached"}
        announcements_out.append(entry)

    return {
        "table": table,
        "symbol": symbol,
        "period_end": period_end.isoformat(),
        "announce_date": announce_date.isoformat(),
        "differing_fields": fields,
        "candidate_values": _candidate_values(rows, fields),
        "report_category": _category_for_period(period_end),
        "announcement_query_window_days": window_days,
        "query_results": query_results,
        "announcement_candidates": announcements_out,
        "official_evidence_status": (
            "candidate_announcements_found" if announcements_out else "no_candidate_announcements"
        ),
        "can_repair": False,
        "blocked_reason": "cninfo_pdf_not_parsed_to_financial_fields",
    }, pdf_download_count


def collect_cninfo_financial_conflict_evidence(
    data_dir: Path,
    *,
    output: Path | None = None,
    axdata_root: Path | None = None,
    tables: tuple[str, ...] = FINANCIAL_CONFLICT_TABLES,
    window_days: int = 3,
    download_pdfs: bool = False,
    max_pdf_downloads: int = 20,
    adapter_factory: Callable[[], Any] | None = None,
    pdf_fetcher: Callable[[str], bytes] = _download_pdf,
) -> dict[str, Any]:
    """Collect official announcement candidates for unresolved financial conflicts.

    This only records evidence. It does not choose a financial revision and never
    mutates financial parquet tables.
    """
    data_dir = Path(data_dir)
    groups: list[tuple[str, pl.DataFrame]] = []
    for table in tables:
        frame = _read_table(data_dir, table)
        groups.extend((table, group) for group in _conflicting_groups(frame))

    if groups:
        if adapter_factory is None:
            adapter, axdata_meta = _create_cninfo_adapter(axdata_root)
        else:
            adapter = adapter_factory()
            axdata_meta = {"axdata_root": str(axdata_root) if axdata_root else None, "paths_added": []}
    else:
        adapter = None
        axdata_meta = {"axdata_root": str(axdata_root) if axdata_root else None, "paths_added": []}

    pdf_download_count = 0
    rows: list[dict[str, Any]] = []
    for table, group in groups:
        row, pdf_download_count = _collect_group_evidence(
            adapter,
            data_dir,
            table,
            group,
            window_days=window_days,
            download_pdfs=download_pdfs,
            max_pdf_downloads=max_pdf_downloads,
            pdf_fetcher=pdf_fetcher,
            pdf_download_count=pdf_download_count,
        )
        rows.append(row)

    found = sum(1 for row in rows if row["announcement_candidates"])
    result = {
        "schema_version": 1,
        "status": "no_conflicts" if not rows else ("candidate_announcements_found" if found else "blocked"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "conflict_groups": len(rows),
        "groups_with_candidate_announcements": found,
        "pdfs_cached": pdf_download_count,
        "download_pdfs": download_pdfs,
        "source": {
            "provider": "AxData",
            "source": "cninfo",
            "interfaces": [
                "stock_zh_a_disclosure_report_cninfo",
                "cninfo_announcement_detail",
            ],
            "axdata_meta": axdata_meta,
        },
        "rows": rows,
    }
    if output is not None:
        result["output_path"] = str(output)
        _atomic_json(Path(output), result)
    return result
