from __future__ import annotations

import polars as pl

from scripts import repair_financials_tushare as repair


def test_pick_metric_uses_tushare_value_to_select_local_revision() -> None:
    local = [
        {"eps_basic": 0.46, "eps_diluted": 0.46, "net_income_yoy": 1.0},
        {"eps_basic": 0.48, "eps_diluted": 0.48, "net_income_yoy": 1.0},
    ]
    source = [{"eps": 0.48, "dt_eps": 0.48, "netprofit_yoy": 1.0, "ann_date": "20230131"}]

    chosen, selected = repair._pick_metric(local, source, [])

    assert chosen["eps_basic"] == 0.48
    assert selected["ann_date"] == "20230131"


def test_pick_metric_requires_update_flag_alignment_for_multiple_versions() -> None:
    local = [
        {"net_income_yoy": -16.8038},
        {"net_income_yoy": -16.2062},
    ]
    source = [
        {"netprofit_yoy": -16.8038, "ann_date": "20230421"},
        {"netprofit_yoy": -16.0906, "ann_date": "20230421"},
    ]
    income = [
        {"ann_date": "20230421", "update_flag": "1"},
        {"ann_date": "20230421", "update_flag": "0"},
    ]

    chosen, _ = repair._pick_metric(local, source, income)

    assert chosen["net_income_yoy"] == -16.8038


def test_conflicts_detected_after_repair_frame_assembly() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["000001.SZ", "000001.SZ"],
            "period_end": ["2024-12-31", "2024-12-31"],
            "announce_date": ["2025-04-20", "2025-04-20"],
            "value": [1.0, 2.0],
        }
    )

    assert repair._conflicts(frame) == [
        {"symbol": "000001.SZ", "period_end": "2024-12-31", "announce_date": "2025-04-20"}
    ]
