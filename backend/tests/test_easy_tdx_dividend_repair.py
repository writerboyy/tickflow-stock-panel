from __future__ import annotations

import json

import polars as pl

from app.api.ext_data import _date_range, _read_ext_dataframe
from app.plugins.easy_tdx.dividend_repair import repair_dividend_history
from app.plugins.easy_tdx.storage import DIVIDEND_HISTORY_TABLE
from app.services.ext_data import ExtConfig, ExtField


def test_dividend_repair_shadow_validates_then_atomically_switches(tmp_path):
    table_root = tmp_path / "ext_data" / DIVIDEND_HISTORY_TABLE
    source = table_root / "timeseries"
    rows = [
        {
            "symbol": "600000.SH",
            "record_date": "2025-06-01",
            "report_date": "2025-06-01",
            "plan": "10转增3股派3元",
            "cash_per_share": 1.0,
        },
        {
            "symbol": "000001.SZ",
            "record_date": "2026-06-01",
            "report_date": "2026-06-01",
            "plan": "10派2元",
            "cash_per_share": 0.2,
        },
    ]
    for row in rows:
        path = source / f"date={row['record_date']}" / "part.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame([row]).write_parquet(path)

    shadow = repair_dividend_history(tmp_path)

    assert shadow["status"] == "validated"
    assert shadow["corrected_rows"] == 1
    assert shadow["source_files"] == 2
    assert shadow["published_files"] == 2
    assert list(source.glob("date=*/part.parquet"))

    published = repair_dividend_history(tmp_path, apply=True)

    assert published["status"] == "published"
    assert published["corrected_rows"] == 1
    assert not list(source.glob("date=*/part.parquet"))
    assert len(list(source.glob("year=*/part.parquet"))) == 2
    corrected = pl.read_parquet(source / "year=2025" / "part.parquet")
    assert corrected["cash_per_share"].to_list() == [0.3]
    assert (table_root / f"timeseries.pre-repair-{published['repair_id']}").exists()
    manifest = json.loads((table_root / "repair-manifest.json").read_text(encoding="utf-8"))
    assert manifest["authoritative_corporate_actions_changed"] is False

    config = ExtConfig(
        id=DIVIDEND_HISTORY_TABLE,
        label="EasyTDX 分红历史",
        mode="timeseries",
        fields=[ExtField("symbol")],
    )
    selected, selected_date = _read_ext_dataframe(config, tmp_path, "2025-06-01")
    latest, latest_date = _read_ext_dataframe(config, tmp_path)
    assert selected["symbol"].to_list() == ["600000.SH"]
    assert selected_date == "2025-06-01"
    assert latest["symbol"].to_list() == ["000001.SZ"]
    assert latest_date == "2026-06-01"
    assert _date_range(config, tmp_path) == ["2025-06-01", "2026-06-01"]
