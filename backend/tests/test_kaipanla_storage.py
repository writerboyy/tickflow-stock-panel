from __future__ import annotations

import gzip
import json
from datetime import date

import polars as pl

from app.plugins.kaipanla.storage import (
    AUCTION_TABLE,
    NORTHBOUND_SECTOR_TABLE,
    TABLE_IDS,
    archive_raw,
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


def test_archive_raw_defaults_to_gzip_without_storing_request_parameters(tmp_path):
    path = archive_raw(
        tmp_path,
        "fund_interval",
        date(2026, 7, 31),
        {"ok": True},
        "offset=0&token=secret",
    )

    assert path.suffix == ".gz"
    assert "token" not in path.name
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        payload = json.load(fh)
    assert payload["endpoint"] == "/fund_interval"
    assert len(payload["content_hash"]) == 64
    assert payload["parser_version"] == "kaipanla_v1"
    assert payload["response"] == {"ok": True}


def test_archive_raw_can_keep_plain_json_for_operator_debug(tmp_path):
    path = archive_raw(
        tmp_path,
        15,
        date(2026, 7, 31),
        {"rows": []},
        compress=False,
    )

    assert path.suffix == ".json"
    assert json.loads(path.read_text(encoding="utf-8"))["endpoint"] == "/15"
