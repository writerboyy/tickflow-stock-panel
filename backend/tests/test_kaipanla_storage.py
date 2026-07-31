from __future__ import annotations

from datetime import date

import polars as pl

from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    NORTHBOUND_SECTOR_TABLE,
    TABLE_IDS,
    atomic_upsert,
    atomic_upsert_records,
    ensure_configs,
)
from app.services.ext_data import ExtConfigStore


def test_plugin_registers_timeseries_configs_without_generic_pull(tmp_path):
    ensure_configs(tmp_path)
    configs = [ExtConfigStore(tmp_path).get(table_id) for table_id in TABLE_IDS]
    assert all(config is not None for config in configs)
    assert len(configs) == 12
    assert all(config.mode == "timeseries" for config in configs if config)
    assert all(config.pull is None for config in configs if config)


def test_atomic_upsert_merges_non_null_checkpoints_and_is_idempotent(tmp_path):
    trade_date = date(2026, 5, 15)
    atomic_upsert(
        tmp_path,
        AUCTION_TABLE,
        trade_date,
        [
            {
                "symbol": "002969",
                "code": "002969",
                "name": "嘉美包装",
                "source_0915": "/115",
                "auction_change_pct_0915": 2.1,
            }
        ],
    )
    atomic_upsert(
        tmp_path,
        AUCTION_TABLE,
        trade_date,
        [
            {
                "symbol": "002969",
                "code": "002969",
                "name": None,
                "source_0925": "/115",
                "auction_change_pct_0925": 9.96,
            }
        ],
    )
    atomic_upsert(
        tmp_path,
        AUCTION_TABLE,
        trade_date,
        [
            {
                "symbol": "002969",
                "code": "002969",
                "bid_points": 2,
            }
        ],
    )

    path = tmp_path / "ext_data" / AUCTION_TABLE / "timeseries" / "date=2026-05-15" / "part.parquet"
    frame = pl.read_parquet(path)
    assert len(frame) == 1
    row = frame.to_dicts()[0]
    assert row["symbol"] == "002969.SZ"
    assert row["name"] == "嘉美包装"
    assert row["auction_change_pct_0915"] == 2.1
    assert row["auction_change_pct_0925"] == 9.96
    assert row["bid_points"] == 2


def test_atomic_upsert_records_keeps_multiple_plate_rows(tmp_path):
    trade_date = date(2026, 6, 30)
    atomic_upsert_records(
        tmp_path,
        NORTHBOUND_SECTOR_TABLE,
        trade_date,
        [
            {"report_date": "2026-06-30", "plate_id": "A", "plate_name": "板块甲"},
            {"report_date": "2026-06-30", "plate_id": "B", "plate_name": "板块乙"},
        ],
        ("plate_id",),
    )
    atomic_upsert_records(
        tmp_path,
        NORTHBOUND_SECTOR_TABLE,
        trade_date,
        [{"report_date": "2026-06-30", "plate_id": "A", "holding_amount": 12.5}],
        ("plate_id",),
    )

    path = tmp_path / "ext_data" / NORTHBOUND_SECTOR_TABLE / "timeseries" / "date=2026-06-30" / "part.parquet"
    rows = pl.read_parquet(path).sort("plate_id").to_dicts()
    assert rows[0]["plate_name"] == "板块甲"
    assert rows[0]["holding_amount"] == 12.5
    assert rows[1]["plate_name"] == "板块乙"
