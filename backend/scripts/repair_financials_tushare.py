#!/usr/bin/env python3
"""Repair the audited financial PIT conflicts using Tushare evidence.

The command is intentionally narrow. It does not guess a revision from file
order and it only replaces the eight conflict groups recorded by the audit.
Use without ``--apply`` to inspect the plan; ``--apply`` atomically replaces
the two canonical parquet files without creating a legacy-data backup.
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


PIT_KEYS = ("symbol", "period_end", "announce_date")
TARGETS = (
    ("metrics", "920249.BJ", "2021-12-31", "2026-01-30"),
    ("metrics", "002010.SZ", "2024-12-31", "2026-04-21"),
    ("metrics", "688132.SH", "2024-12-31", "2026-03-20"),
    ("metrics", "002462.SZ", "2022-12-31", "2026-03-14"),
    ("metrics", "300205.SZ", "2024-12-31", "2026-04-23"),
    ("income", "601118.SH", "2024-12-31", "2026-04-18"),
    ("income", "300500.SZ", "2024-12-31", "2025-12-31"),
    ("income", "600169.SH", "2021-12-31", "2025-11-01"),
)

METRIC_FIELDS = {
    "eps_basic": "eps",
    "eps_diluted": "dt_eps",
    "bps": "bps",
    "net_income_yoy": "netprofit_yoy",
    "roe": "roe",
    "ocfps": "ocfps",
}
INCOME_FIELDS = {"net_income_deducted": "profit_dedt"}


def _as_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    value = str(value).replace("/", "-").replace(".", "-")
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _equal(left: Any, right: Any) -> bool:
    left_number = _number(left)
    right_number = _number(right)
    if left_number is not None and right_number is not None:
        return abs(left_number - right_number) <= max(
            1e-6, abs(left_number) * 1e-5, abs(right_number) * 1e-5
        )
    return left == right


def _matches(row: dict[str, Any], source: dict[str, Any], mapping: dict[str, str]) -> bool:
    compared = 0
    for local_field, source_field in mapping.items():
        if source_field not in source or local_field not in row:
            continue
        source_value = source.get(source_field)
        local_value = row.get(local_field)
        if source_value is None or local_value is None:
            continue
        compared += 1
        if not _equal(local_value, source_value):
            return False
    return compared > 0


def _varying_mapping(rows: list[dict[str, Any]], mapping: dict[str, str]) -> dict[str, str]:
    """Only compare fields that actually distinguish the local conflict rows."""
    varying: dict[str, str] = {}
    for local_field, source_field in mapping.items():
        values = {
            json.dumps(row.get(local_field), ensure_ascii=False, sort_keys=True)
            for row in rows
        }
        if len(values) > 1:
            varying[local_field] = source_field
    return varying


def _conflicts(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    unique = frame.unique(maintain_order=True)
    groups = (
        unique.group_by(list(PIT_KEYS)).agg(pl.len().alias("_rows"))
        .filter(pl.col("_rows") > 1)
        .sort(list(PIT_KEYS))
    )
    return groups.select(PIT_KEYS).to_dicts()


def _read(data_dir: Path, table: str) -> tuple[Path, pl.DataFrame]:
    path = data_dir / "financials" / table / "part.parquet"
    return path, pl.read_parquet(path) if path.exists() else pl.DataFrame()


def _source_rows(
    client: TushareProxyClient,
    api_name: str,
    symbol: str,
    period: str,
    responses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    response = client.request(api_name, {"ts_code": symbol, "period": period.replace("-", "")})
    responses.append(
        {
            "api_name": api_name,
            "symbol": symbol,
            "period": period,
            "fields": list(response.fields),
            "rows": response.rows,
        }
    )
    return [
        row
        for row in response.rows
        if _as_date(row.get("end_date")) == period
    ]


def _pick_metric(
    local_rows: list[dict[str, Any]],
    source_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not source_rows:
        raise ValueError("Tushare fina_indicator returned no matching row")
    mapping = _varying_mapping(local_rows, METRIC_FIELDS)
    if not mapping:
        raise ValueError("local conflict has no varying mapped fields")
    candidate_pairs = [
        (local, source)
        for local in local_rows
        for source in source_rows
        if _matches(local, source, mapping)
    ]
    if len(source_rows) == 1:
        if len(candidate_pairs) != 1:
            raise ValueError("single Tushare row did not uniquely match a local candidate")
        return candidate_pairs[0]
    if len(candidate_pairs) == 1:
        return candidate_pairs[0]

    # Tushare's fina_indicator response has no update_flag, while income does.
    # Require aligned rows and use the highest report revision only when the
    # provider returns the same version cardinality and dates for both APIs.
    if len(income_rows) != len(source_rows):
        raise ValueError("multiple indicator rows lack an aligned income revision list")
    if any(_as_date(row.get("ann_date")) != _as_date(source.get("ann_date")) for row, source in zip(income_rows, source_rows, strict=True)):
        raise ValueError("indicator and income revision dates are not aligned")
    ranked = sorted(
        enumerate(income_rows),
        key=lambda item: int(str(item[1].get("update_flag") or "-1")),
    )
    selected_index = ranked[-1][0]
    source = source_rows[selected_index]
    matching = [local for local in local_rows if _matches(local, source, mapping)]
    if len(matching) != 1:
        raise ValueError("highest Tushare revision did not uniquely match a local candidate")
    return matching[0], source


def _pick_income(
    local_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
    indicator_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    if not income_rows:
        raise ValueError("Tushare income returned no matching row")
    mapping = _varying_mapping(local_rows, INCOME_FIELDS)
    pairs = [
        (local, indicator)
        for local in local_rows
        for indicator in indicator_rows
        if _matches(local, indicator, mapping)
    ]
    if len(indicator_rows) == 1:
        if len(pairs) != 1:
            raise ValueError("single Tushare indicator row did not uniquely match income")
        local, indicator = pairs[0]
        dates = [
            row
            for row in income_rows
            if _as_date(row.get("f_ann_date")) == _as_date(indicator.get("ann_date"))
        ]
        if not dates:
            dates = [
                row
                for row in income_rows
                if _as_date(row.get("ann_date")) == _as_date(indicator.get("ann_date"))
            ]
        if len(dates) != 1:
            raise ValueError("Tushare income announcement date is not unique")
        return local, dates[0], indicator

    if len(indicator_rows) != len(income_rows):
        raise ValueError("income and indicator revisions have different cardinality")
    ranked = sorted(
        enumerate(income_rows),
        key=lambda item: int(str(item[1].get("update_flag") or "-1")),
    )
    selected_index = ranked[-1][0]
    income = income_rows[selected_index]
    indicator = indicator_rows[selected_index]
    matching = [local for local in local_rows if _matches(local, indicator, mapping)]
    if len(matching) != 1:
        raise ValueError("highest Tushare income revision did not uniquely match local row")
    return matching[0], income, indicator


def build_plan(data_dir: Path, client: TushareProxyClient) -> tuple[dict[str, Any], dict[str, pl.DataFrame]]:
    frames: dict[str, pl.DataFrame] = {}
    for table in {target[0] for target in TARGETS}:
        _, frames[table] = _read(data_dir, table)
    responses: list[dict[str, Any]] = []
    changes: list[dict[str, Any]] = []
    selected_by_table: dict[str, list[dict[str, Any]]] = {"metrics": [], "income": []}
    for table, symbol, period, old_announce in TARGETS:
        frame = frames[table]
        local = frame.filter(
            (pl.col("symbol") == symbol)
            & (pl.col("period_end") == period)
            & (pl.col("announce_date") == old_announce)
        ).to_dicts()
        if len(local) < 2:
            raise ValueError(f"expected a conflict group with at least two rows: {table}/{symbol}")
        if table == "metrics":
            indicator = _source_rows(client, "fina_indicator", symbol, period, responses)
            income = _source_rows(client, "income", symbol, period, responses)
            try:
                chosen, source = _pick_metric(local, indicator, income)
            except ValueError as exc:
                raise ValueError(f"{table}/{symbol}/{period}: {exc}") from exc
            new_announce = _as_date(source.get("ann_date"))
            evidence = {"indicator": source, "income_revisions": income}
        else:
            income = _source_rows(client, "income", symbol, period, responses)
            indicator = _source_rows(client, "fina_indicator", symbol, period, responses)
            try:
                chosen, source_income, source_indicator = _pick_income(local, income, indicator)
            except ValueError as exc:
                raise ValueError(f"{table}/{symbol}/{period}: {exc}") from exc
            new_announce = _as_date(source_income.get("f_ann_date") or source_income.get("ann_date"))
            source = source_income
            evidence = {"income": source_income, "indicator": source_indicator, "income_revisions": income}
        if not new_announce:
            raise ValueError(f"Tushare did not provide an announcement date: {table}/{symbol}")
        replacement = dict(chosen)
        replacement["announce_date"] = new_announce
        replacement["symbol"] = symbol
        replacement["period_end"] = period
        selected_by_table[table].append(replacement)
        changes.append({
            "table": table,
            "symbol": symbol,
            "period_end": period,
            "old_announce_date": old_announce,
            "new_announce_date": new_announce,
            "source": source,
            "local_row": chosen,
            "evidence": evidence,
        })

    result_frames: dict[str, pl.DataFrame] = {}
    for table, replacements in selected_by_table.items():
        frame = frames[table]
        for replacement in replacements:
            # Remove the entire old conflicting key group before inserting one
            # Tushare-supported canonical row.
            frame = frame.filter(
                ~(
                    (pl.col("symbol") == replacement["symbol"])
                    & (pl.col("period_end") == replacement["period_end"])
                    & (pl.col("announce_date").is_in([next(item["old_announce_date"] for item in changes if item["table"] == table and item["symbol"] == replacement["symbol"] and item["period_end"] == replacement["period_end"] and item["new_announce_date"] == replacement["announce_date"])]))
                )
            )
            frame = pl.concat([frame, pl.DataFrame([replacement], schema=frame.schema)], how="vertical_relaxed")
        conflicts = _conflicts(frame)
        if conflicts:
            raise ValueError(f"unresolved conflicts remain in {table}: {conflicts[:8]}")
        result_frames[table] = frame.sort(list(PIT_KEYS))
    return {"changes": changes, "responses": responses}, result_frames


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
        plan, frames = build_plan(data_dir, client)
    finally:
        client.close()
    manifest = {
        "schema_version": 1,
        "repair_id": repair_id,
        "status": "planned",
        "apply": args.apply,
        "backup": False,
        "changes": plan["changes"],
        "source_responses": plan["responses"],
        "tables": {table: {"rows": frame.height, "conflicts": _conflicts(frame)} for table, frame in frames.items()},
    }
    path = data_dir / "financials" / f"tushare-repair-manifest-{repair_id}.json"
    if args.apply:
        for table, frame in frames.items():
            _write_atomic(data_dir / "financials" / table / "part.parquet", frame)
        manifest["status"] = "published"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
