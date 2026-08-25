from __future__ import annotations

from datetime import datetime
import json
import os

import polars as pl

from app.services.minute_repair import repair_minute_table


def _write(path, rows):
    path.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(path)


def test_minute_repair_rewrites_invalid_partition_and_hardlinks_clean_partition(tmp_path):
    first = tmp_path / "kline_minute" / "date=2026-07-21" / "part.parquet"
    second = tmp_path / "kline_minute" / "date=2026-07-22" / "part.parquet"
    base = {
        "symbol": "600000.SH",
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "volume": 100.0,
        "amount": 1000.0,
    }
    _write(first, [
        {**base, "datetime": datetime(2026, 7, 21, 9, 31)},
        {**base, "datetime": datetime(2026, 7, 21, 9, 32), "open": None, "high": None, "low": None, "close": None},
    ])
    _write(second, [{**base, "datetime": datetime(2026, 7, 22, 9, 31)}])
    clean_inode = second.stat().st_ino

    result = repair_minute_table(tmp_path, "kline_minute", apply=True)

    assert result["status"] == "published"
    assert result["source_rows"] == 3
    assert result["published_rows"] == 2
    assert result["rejected_rows"] == 1
    assert result["rewritten_files"] == 1
    assert result["hardlinked_files"] == 1
    assert second.stat().st_ino == clean_inode
    assert pl.read_parquet(first).height == 1
    backup = tmp_path / f".kline_minute.pre-repair-{result['repair_id']}"
    assert pl.read_parquet(backup / "date=2026-07-21" / "part.parquet").height == 2
    coverage = json.loads(
        (tmp_path / "kline_minute" / "_coverage" / "date=2026-07-21.json").read_text()
    )
    assert coverage["rejected_rows"] == 1
    assert os.stat(second).st_ino == os.stat(
        backup / "date=2026-07-22" / "part.parquet"
    ).st_ino
