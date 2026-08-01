#!/usr/bin/env python3
"""Checkpointed full-market backfill for TDX F10 dividend history."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import polars as pl

from app.plugins.easy_tdx.client import fetch_dividend_history_batch
from app.plugins.easy_tdx.storage import DIVIDEND_HISTORY_TABLE, ensure_config, upsert_records


def _stock_codes(data_dir: Path) -> list[str]:
    path = data_dir / "instruments" / "instruments.parquet"
    if not path.exists():
        raise FileNotFoundError(f"instrument snapshot not found: {path}")
    frame = pl.read_parquet(path, columns=["code", "type"])
    return sorted({str(code).zfill(6) for code, kind in frame.iter_rows() if kind == "stock"})


def _load_state(path: Path, codes: list[str]) -> dict:
    if not path.exists():
        return {"version": 1, "total": len(codes), "completed": [], "failures": {}}
    state = json.loads(path.read_text())
    if state.get("version") != 1 or state.get("total") != len(codes):
        raise ValueError(f"incompatible checkpoint: {path}")
    return state


def _write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True))
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def backfill(
    data_dir: Path,
    state_path: Path,
    *,
    batch_size: int = 50,
    workers: int = 4,
) -> dict:
    codes = _stock_codes(data_dir)
    ensure_config(data_dir)
    state = _load_state(state_path, codes)
    completed = set(state.get("completed", []))
    failures_by_code = dict(state.get("failures", {}))
    pending = [code for code in codes if code not in completed]
    for offset in range(0, len(pending), batch_size):
        batch = pending[offset:offset + batch_size]
        rows, failures = fetch_dividend_history_batch(batch, workers=workers)
        collected_at = datetime.now().astimezone().isoformat()
        written = upsert_records(
            data_dir,
            DIVIDEND_HISTORY_TABLE,
            [{**row, "collected_at": collected_at} for row in rows],
            ("symbol", "record_date", "plan"),
        )
        succeeded = sorted(set(batch) - set(failures))
        completed.update(succeeded)
        for code in succeeded:
            failures_by_code.pop(code, None)
        failures_by_code.update(failures)
        state["completed"] = sorted(completed)
        state["failures"] = failures_by_code
        state["updated_at"] = collected_at
        state["last_batch"] = {"codes": batch, "rows_written": written, "failed": failures}
        _write_state(state_path, state)
        print(json.dumps({
            "completed": len(completed), "total": len(codes), "rows_written": written,
            "failed": len(failures), "checkpoint": str(state_path),
        }, ensure_ascii=False), flush=True)
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill TDX F10 dividend history with checkpoints")
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.batch_size < 1 or args.workers < 1:
        parser.error("batch-size and workers must be positive")
    data_dir = args.data_dir.resolve()
    state_path = args.state or data_dir / "backfill_state" / "easy_tdx_dividends.json"
    state = backfill(data_dir, state_path.resolve(), batch_size=args.batch_size, workers=args.workers)
    if len(state.get("completed", [])) != state.get("total") or state.get("failures"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
