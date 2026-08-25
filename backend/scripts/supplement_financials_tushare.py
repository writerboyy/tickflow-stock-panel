#!/usr/bin/env python3
"""Apply the audited, temporary Tushare financial supplements.

This command is intentionally limited to the currently verified gaps.  It
does not back up or merge arbitrary financial data.  Without ``--apply`` it
only fetches the source evidence and prints the proposed changes; ``--apply``
atomically replaces the metrics parquet and records an audit manifest.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl

from app.services.tushare_history import TushareProxyClient, load_tushare_key


METRICS_PATH = Path("financials/metrics/part.parquet")
PIT_KEYS = ("symbol", "period_end", "announce_date")

# The first target has a source-confirmed null local ROE.  The other targets
# are incorrect PIT rows; their source-confirmed announcement rows must stay.
ROE_SUPPLEMENT = ("001338.SZ", "2025-09-30", "2025-10-27")
PIT_CLEANUPS = (
    ("601231.SH", "2025-06-30", "2025-07-29", "2025-08-27"),
    ("603053.SH", "2025-06-30", "2025-07-16", "2025-08-23"),
    ("000736.SZ", "2025-09-30", "2026-01-31", "2025-10-31"),
)


def _as_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).replace("/", "-").replace(".", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _source_rows(
    client: TushareProxyClient,
    symbol: str,
    period: str,
    responses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    response = client.request(
        "fina_indicator",
        {"ts_code": symbol, "period": period.replace("-", "")},
    )
    rows = [
        row for row in response.rows
        if _as_date(row.get("end_date")) == period
    ]
    responses.append({
        "api_name": "fina_indicator",
        "symbol": symbol,
        "period": period,
        "fields": list(response.fields),
        "rows": response.rows,
    })
    return rows


def _source_announce(
    client: TushareProxyClient,
    symbol: str,
    period: str,
    responses: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    rows = _source_rows(client, symbol, period, responses)
    announcements = {
        _as_date(row.get("ann_date"))
        for row in rows
        if _as_date(row.get("ann_date"))
    }
    if len(announcements) != 1:
        raise ValueError(
            f"Tushare fina_indicator announcement is ambiguous: {symbol}/{period}"
        )
    return next(iter(announcements)), rows


def _conflicts(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    unique = frame.unique(maintain_order=True)
    return (
        unique.group_by(list(PIT_KEYS))
        .len()
        .filter(pl.col("len") > 1)
        .select(PIT_KEYS)
        .sort(list(PIT_KEYS))
        .to_dicts()
    )


def _rows_for_key(frame: pl.DataFrame, key: tuple[str, str, str]) -> list[dict[str, Any]]:
    symbol, period, announcement = key
    return frame.filter(
        (pl.col("symbol") == symbol)
        & (pl.col("period_end") == period)
        & (pl.col("announce_date") == announcement)
    ).to_dicts()


def build_plan(
    data_dir: Path,
    client: TushareProxyClient,
) -> tuple[dict[str, Any], pl.DataFrame]:
    path = data_dir / METRICS_PATH
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pl.read_parquet(path)
    required = set(PIT_KEYS) | {"roe", "roe_diluted"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"metrics parquet missing columns: {', '.join(missing)}")

    responses: list[dict[str, Any]] = []
    changes: list[dict[str, Any]] = []

    symbol, period, announcement = ROE_SUPPLEMENT
    local_rows = _rows_for_key(frame, ROE_SUPPLEMENT)
    if len(local_rows) != 1:
        raise ValueError(f"expected one local ROE row: {ROE_SUPPLEMENT}")
    source_rows = _source_rows(client, symbol, period, responses)
    matching = [row for row in source_rows if _as_date(row.get("ann_date")) == announcement]
    if len(matching) != 1:
        raise ValueError(f"Tushare ROE row is not unique: {ROE_SUPPLEMENT}")
    source = matching[0]
    if source.get("roe") in (None, ""):
        raise ValueError(f"Tushare ROE is empty: {ROE_SUPPLEMENT}")
    if local_rows[0].get("roe") is not None:
        raise ValueError(f"local ROE row is no longer missing: {ROE_SUPPLEMENT}")
    replacement = {
        "roe": float(source["roe"]),
        "roe_diluted": (
            None if source.get("roe_dt") in (None, "") else float(source["roe_dt"])
        ),
    }
    changes.append({
        "action": "fill_fields",
        "table": "metrics",
        "symbol": symbol,
        "period_end": period,
        "announce_date": announcement,
        "fields": replacement,
        "source": source,
    })

    for symbol, period, old_announcement, expected_announcement in PIT_CLEANUPS:
        old_key = (symbol, period, old_announcement)
        correct_key = (symbol, period, expected_announcement)
        if len(_rows_for_key(frame, old_key)) != 1:
            raise ValueError(f"expected one obsolete PIT row: {old_key}")
        if len(_rows_for_key(frame, correct_key)) != 1:
            raise ValueError(f"expected one correct PIT row: {correct_key}")
        source_announcement, source = _source_announce(client, symbol, period, responses)
        if source_announcement != expected_announcement:
            raise ValueError(
                f"Tushare announcement mismatch for {symbol}/{period}: "
                f"expected {expected_announcement}, got {source_announcement}"
            )
        changes.append({
            "action": "delete_row",
            "table": "metrics",
            "symbol": symbol,
            "period_end": period,
            "announce_date": old_announcement,
            "preserve_announce_date": expected_announcement,
            "source_announcement": source_announcement,
            "source_rows": source,
        })

    result = frame.with_columns(
        pl.when(
            (pl.col("symbol") == ROE_SUPPLEMENT[0])
            & (pl.col("period_end") == ROE_SUPPLEMENT[1])
            & (pl.col("announce_date") == ROE_SUPPLEMENT[2])
        )
        .then(pl.lit(replacement["roe"]))
        .otherwise(pl.col("roe"))
        .alias("roe"),
        pl.when(
            (pl.col("symbol") == ROE_SUPPLEMENT[0])
            & (pl.col("period_end") == ROE_SUPPLEMENT[1])
            & (pl.col("announce_date") == ROE_SUPPLEMENT[2])
        )
        .then(pl.lit(replacement["roe_diluted"]))
        .otherwise(pl.col("roe_diluted"))
        .alias("roe_diluted"),
    )
    for symbol, period, old_announcement, _expected in PIT_CLEANUPS:
        result = result.filter(
            ~(
                (pl.col("symbol") == symbol)
                & (pl.col("period_end") == period)
                & (pl.col("announce_date") == old_announcement)
            )
        )
    result = result.sort(list(PIT_KEYS))
    conflicts = _conflicts(result)
    if conflicts:
        raise ValueError(f"unresolved metrics PIT conflicts: {conflicts[:8]}")
    return {"changes": changes, "source_responses": responses}, result


def _write_atomic(path: Path, frame: pl.DataFrame) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    data_dir = args.data_dir.resolve()
    repair_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    client = TushareProxyClient(load_tushare_key(data_dir=data_dir), direct=True, attempts=3)
    try:
        plan, frame = build_plan(data_dir, client)
    finally:
        client.close()
    manifest = {
        "schema_version": 1,
        "repair_id": repair_id,
        "status": "planned",
        "apply": args.apply,
        "backup": False,
        "data_source": "tushare_proxy",
        "table": str(METRICS_PATH),
        "rows_before": pl.read_parquet(data_dir / METRICS_PATH).height,
        "rows_after": frame.height,
        "changes": plan["changes"],
        "source_responses": plan["source_responses"],
        "remaining_conflicts": _conflicts(frame),
    }
    manifest_path = data_dir / "financials" / f"tushare-supplement-manifest-{repair_id}.json"
    if args.apply:
        _write_atomic(data_dir / METRICS_PATH, frame)
        manifest["status"] = "published"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
