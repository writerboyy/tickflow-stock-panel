"""Build canonical PIT history tables from cached raw public-data files.

The script is a one-shot/offline normalizer. It does not install or register a
runtime data provider, and it never uses current snapshots to backfill history.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.request import urlopen

import pandas as pd
import polars as pl

from app.config import settings
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_EVENTS_TABLE,
    INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    PARSER_VERSION,
    SOURCE,
    normalize_index_membership_events,
    normalize_industry_membership_history,
    normalize_instrument_lifecycle_events,
    publish_history_table,
    validate_index_membership_coverage,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)


SINA_HISTORY_URL = (
    "https://vip.stock.finance.sina.com.cn/corp/go.php/"
    "vII_HistoryComponent/indexid/{indexid}.phtml"
)
SINA_HISTORY_PAGE_URL = (
    "https://vip.stock.finance.sina.com.cn/corp/view/"
    "vII_HistoryComponent.php?indexid={indexid}&page={page}"
)
_LEADING_HEADER_INDEX = re.compile(r"^\d+\.\s*")
_SINA_TOTAL_PAGES = re.compile(r"共\s*(\d+)\s*页")
_HISTORY_HEADERS = {
    "品种代码",
    "证券代码",
    "股票代码",
    "成分券代码",
    "纳入日期",
    "剔除日期",
    "变更日期",
    "终止上市日期",
}


class _HtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        tag = tag.lower()
        if tag == "table":
            if self._table_depth == 0:
                self._current_table = []
            self._table_depth += 1
        elif tag == "tr" and self._table_depth:
            self._current_row = []
        elif tag in {"td", "th"} and self._table_depth and self._current_row is not None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._current_cell is not None:
            if self._current_row is not None:
                self._current_row.append(" ".join("".join(self._current_cell).split()))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_table is not None and any(cell for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0 and self._current_table is not None:
                self.tables.append(self._current_table)
                self._current_table = None


def _clean_header(value: str) -> str:
    value = " ".join(value.split())
    return _LEADING_HEADER_INDEX.sub("", value)


def _rows_from_html(html_text: str) -> list[dict]:
    parser = _HtmlTableParser()
    parser.feed(html_text)
    fallback: list[dict] = []
    for table in parser.tables:
        if len(table) < 2:
            continue
        header_index = 0
        for index, candidate in enumerate(table[:-1]):
            cleaned = [_clean_header(cell) for cell in candidate]
            if _HISTORY_HEADERS.intersection(set(cleaned)):
                header_index = index
                break
        header = [_clean_header(cell) for cell in table[header_index]]
        if not any(header):
            continue
        rows: list[dict] = []
        for raw_row in table[header_index + 1 :]:
            if not any(raw_row):
                continue
            padded = raw_row[: len(header)] + [""] * max(0, len(header) - len(raw_row))
            row = {header[index]: value for index, value in enumerate(padded) if header[index]}
            if row:
                rows.append(row)
        if rows and _HISTORY_HEADERS.intersection(set(header)):
            return rows
        if rows and not fallback:
            fallback = rows
    return fallback


def _clean_frame(frame: pd.DataFrame) -> list[dict]:
    frame = frame.dropna(how="all")
    frame.columns = [str(col).strip() for col in frame.columns]
    return json.loads(
        frame.astype(object).where(pd.notnull(frame), None).to_json(
            orient="records",
            force_ascii=False,
            date_format="iso",
        )
    )


def _read_csv(path: Path, encoding: str) -> list[dict]:
    encodings = [encoding]
    if encoding.casefold() != "gb18030":
        encodings.append("gb18030")
    last_error: Exception | None = None
    for item in encodings:
        try:
            return _clean_frame(pd.read_csv(path, dtype=object, encoding=item))
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return []


def read_raw_rows(path: Path, *, encoding: str = "utf-8") -> list[dict]:
    suffix = path.suffix.casefold()
    if suffix in {".csv", ".txt"}:
        return _read_csv(path, encoding)
    if suffix in {".xlsx", ".xls"}:
        return _clean_frame(pd.read_excel(path, dtype=object))
    if suffix == ".parquet":
        return pl.read_parquet(path).to_dicts()
    if suffix in {".html", ".htm"}:
        return _rows_from_html(path.read_text(encoding=encoding))
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding=encoding))
        if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
            raise ValueError(f"JSON raw file must contain a list of objects: {path}")
        return payload
    raise ValueError(f"unsupported raw file type: {path}")


def _fetch_sina_history_page(indexid: str, page: int, *, encoding: str = "gb2312") -> tuple[list[dict], int | None]:
    url = SINA_HISTORY_URL.format(indexid=indexid) if page == 1 else SINA_HISTORY_PAGE_URL.format(
        indexid=indexid,
        page=page,
    )
    with urlopen(url, timeout=30) as response:  # noqa: S310
        html = response.read().decode(encoding, errors="ignore")
    match = _SINA_TOTAL_PAGES.search(html)
    total_pages = int(match.group(1)) if match else None
    return _rows_from_html(html), total_pages


def fetch_sina_index_history(
    indexid: str,
    *,
    encoding: str = "gb2312",
    max_pages: int = 20,
) -> list[dict]:
    rows, total_pages = _fetch_sina_history_page(indexid, 1, encoding=encoding)
    pages = min(total_pages or 1, max_pages)
    seen = {json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows}
    for page in range(2, pages + 1):
        page_rows, _ = _fetch_sina_history_page(indexid, page, encoding=encoding)
        for row in page_rows:
            key = json.dumps(row, ensure_ascii=False, sort_keys=True)
            if key not in seen:
                seen.add(key)
                rows.append(row)
    return rows


def _publish(
    *,
    data_dir: Path,
    table: str,
    source: str,
    logical_snapshot: str,
    raw_label: str,
    raw_rows: list[dict],
    frame: pl.DataFrame,
) -> int:
    _, source_hash = archive_source_payload(
        data_dir,
        SOURCE,
        table,
        logical_snapshot,
        raw_label,
        raw_rows,
        parser_version=PARSER_VERSION,
    )
    count = publish_history_table(data_dir, table, frame)
    update_ingestion_manifest(
        data_dir,
        SOURCE,
        table,
        logical_snapshot,
        status="published" if count else "valid_empty",
        parser_version=PARSER_VERSION,
        schema_version=1,
        source_content_hash=source_hash,
        content_hash=stable_content_hash(frame.to_dicts()) if count else None,
        published_rows=count,
        provenance="historical_event",
        upstream_source=source,
        empty_reason=None if count else "source_empty",
    )
    return count


def build_index_history(
    *,
    data_dir: Path,
    raw_rows: list[dict],
    index_symbol: str,
    source: str,
    logical_snapshot: str,
    raw_label: str,
    validate_strict: bool = False,
) -> int:
    frame = normalize_index_membership_events(
        raw_rows,
        index_symbol=index_symbol,
        source=source,
    )
    if validate_strict:
        coverage = validate_index_membership_coverage(frame, index_symbol=index_symbol)
        if not coverage["usable"]:
            failed = ", ".join(
                f"{item['date']}={item['members']}/{item['expected_min_members']}"
                for item in coverage["coverage_checks"]
                if not item["ok"]
            )
            raise ValueError(
                f"incomplete strict index history for {index_symbol.upper()}: {failed}; "
                "pass --allow-incomplete-index only for archived non-backtest reference data"
            )
    return _publish(
        data_dir=data_dir,
        table=INDEX_MEMBERSHIP_EVENTS_TABLE,
        source=source,
        logical_snapshot=logical_snapshot,
        raw_label=raw_label,
        raw_rows=raw_rows,
        frame=frame,
    )


def build_industry_history(
    *,
    data_dir: Path,
    raw_rows: list[dict],
    source: str,
    logical_snapshot: str,
    raw_label: str,
) -> int:
    frame = normalize_industry_membership_history(raw_rows, source=source)
    return _publish(
        data_dir=data_dir,
        table=INDUSTRY_MEMBERSHIP_HISTORY_TABLE,
        source=source,
        logical_snapshot=logical_snapshot,
        raw_label=raw_label,
        raw_rows=raw_rows,
        frame=frame,
    )


def build_lifecycle_events(
    *,
    data_dir: Path,
    raw_rows: list[dict],
    source: str,
    logical_snapshot: str,
    raw_label: str,
) -> int:
    frame = normalize_instrument_lifecycle_events(raw_rows, source=source)
    return _publish(
        data_dir=data_dir,
        table=INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
        source=source,
        logical_snapshot=logical_snapshot,
        raw_label=raw_label,
        raw_rows=raw_rows,
        frame=frame,
    )


def _label(path: Path | None, fallback: str) -> str:
    return path.name if path else fallback


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--logical-snapshot", default=date.today().isoformat())
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--index-history-file", type=Path)
    parser.add_argument("--index-symbol", default="000300.SH")
    parser.add_argument("--index-source", default="sina")
    parser.add_argument("--fetch-sina-index", help="Fetch one Sina history component page, e.g. 399300")
    parser.add_argument("--sina-max-pages", type=int, default=20)
    parser.add_argument(
        "--allow-incomplete-index",
        action="store_true",
        help="Archive/publish incomplete index history instead of failing strict PIT coverage checks",
    )
    parser.add_argument("--industry-history-file", type=Path)
    parser.add_argument("--industry-source", default="cninfo")
    parser.add_argument("--lifecycle-file", type=Path)
    parser.add_argument("--lifecycle-source", default="exchange")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow publishing a valid_empty manifest when an input source yields no rows",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.fetch_sina_index and args.index_history_file:
        raise ValueError("--fetch-sina-index and --index-history-file are mutually exclusive")

    published: dict[str, int] = {}
    if args.fetch_sina_index:
        rows = fetch_sina_index_history(args.fetch_sina_index, max_pages=args.sina_max_pages)
        published[INDEX_MEMBERSHIP_EVENTS_TABLE] = build_index_history(
            data_dir=args.data_dir,
            raw_rows=rows,
            index_symbol=args.index_symbol,
            source=args.index_source,
            logical_snapshot=args.logical_snapshot,
            raw_label=f"sina-{args.fetch_sina_index}-pages-{args.sina_max_pages}",
            validate_strict=not args.allow_incomplete_index,
        )
    elif args.index_history_file:
        rows = read_raw_rows(args.index_history_file, encoding=args.encoding)
        published[INDEX_MEMBERSHIP_EVENTS_TABLE] = build_index_history(
            data_dir=args.data_dir,
            raw_rows=rows,
            index_symbol=args.index_symbol,
            source=args.index_source,
            logical_snapshot=args.logical_snapshot,
            raw_label=_label(args.index_history_file, "index"),
            validate_strict=not args.allow_incomplete_index,
        )

    if args.industry_history_file:
        rows = read_raw_rows(args.industry_history_file, encoding=args.encoding)
        published[INDUSTRY_MEMBERSHIP_HISTORY_TABLE] = build_industry_history(
            data_dir=args.data_dir,
            raw_rows=rows,
            source=args.industry_source,
            logical_snapshot=args.logical_snapshot,
            raw_label=_label(args.industry_history_file, "industry"),
        )

    if args.lifecycle_file:
        rows = read_raw_rows(args.lifecycle_file, encoding=args.encoding)
        published[INSTRUMENT_LIFECYCLE_EVENTS_TABLE] = build_lifecycle_events(
            data_dir=args.data_dir,
            raw_rows=rows,
            source=args.lifecycle_source,
            logical_snapshot=args.logical_snapshot,
            raw_label=_label(args.lifecycle_file, "lifecycle"),
        )

    if not published:
        raise SystemExit("no input file or fetch target provided")
    empty_tables = [table for table, rows in published.items() if rows == 0]
    if empty_tables and not args.allow_empty:
        raise SystemExit(f"empty PIT history output for: {','.join(sorted(empty_tables))}")
    print(" ".join(f"{table}={rows}" for table, rows in sorted(published.items())))


if __name__ == "__main__":
    main()
