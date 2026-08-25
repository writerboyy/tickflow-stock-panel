from __future__ import annotations

import json
from datetime import date

import polars as pl

from app.plugins.kaipanla.lhb_detail_repair import repair_lhb_details
from app.plugins.kaipanla.replay import replay_archives
from app.plugins.kaipanla.storage import (
    LHB_DETAIL_TABLE,
    archive_raw,
    ensure_configs,
)


def _detail(log_id: str, reason_type: str, amount: int) -> dict:
    return {
        "ID": "D1",
        "Name": "席位甲",
        "Buy": amount,
        "Sell": 20,
        "PX": 1,
        "LogID": log_id,
        "ReasonType": reason_type,
        "GroupIcon": [],
    }


def test_lhb_detail_repair_recovers_duplicate_rank_rows_and_keeps_rollback(tmp_path):
    ensure_configs(tmp_path)
    config_path = tmp_path / "ext_data" / LHB_DETAIL_TABLE / "config.json"
    legacy_config = json.loads(config_path.read_text(encoding="utf-8"))
    legacy_config["schema_version"] = 1
    legacy_config["fields"] = [
        field
        for field in legacy_config["fields"]
        if field["name"] not in {"log_id", "reason_type"}
    ]
    config_path.write_text(json.dumps(legacy_config), encoding="utf-8")

    old_partition = (
        tmp_path / "ext_data" / LHB_DETAIL_TABLE
        / "timeseries" / "date=2026-07-31" / "part.parquet"
    )
    old_partition.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600126.SH"]}).write_parquet(old_partition)
    archive_raw(
        tmp_path,
        "dragon_tiger_details",
        date(2026, 7, 31),
        {"List": [
            {"BuyList": [_detail("log-0", "0", 100)], "SellList": []},
            {"BuyList": [_detail("log-1", "1", 200)], "SellList": []},
        ]},
        "600126",
    )

    validated = repair_lhb_details(tmp_path)

    assert validated["status"] == "validated"
    assert validated["source_rows"] == 1
    assert validated["archive_rows"] == 2
    assert validated["recovered_rows"] == 1
    assert pl.read_parquet(old_partition).height == 1

    published = repair_lhb_details(tmp_path, apply=True)

    assert published["status"] == "published"
    repaired = pl.read_parquet(old_partition).sort("log_id")
    assert repaired["log_id"].to_list() == ["log-0", "log-1"]
    assert repaired["reason_type"].to_list() == ["0", "1"]
    table_root = tmp_path / "ext_data" / LHB_DETAIL_TABLE
    assert (table_root / f"timeseries.pre-repair-{published['repair_id']}").exists()
    assert (table_root / f"config.pre-repair-{published['repair_id']}.json").exists()
    assert json.loads(config_path.read_text(encoding="utf-8"))["schema_version"] == 2
    assert replay_archives(tmp_path)["status"] == "passed"
