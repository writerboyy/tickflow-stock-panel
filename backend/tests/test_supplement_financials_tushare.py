from __future__ import annotations

from types import SimpleNamespace

import polars as pl

from scripts import supplement_financials_tushare as supplement


class FakeClient:
    def __init__(self, rows_by_symbol: dict[str, list[dict]]) -> None:
        self.rows_by_symbol = rows_by_symbol

    def request(self, api_name: str, _params: dict):
        assert api_name == "fina_indicator"
        rows = self.rows_by_symbol[_params["ts_code"]]
        fields = tuple(rows[0])
        return SimpleNamespace(fields=fields, rows=rows)


def _write_metrics(tmp_path):
    path = tmp_path / supplement.METRICS_PATH
    path.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": [
            "001338.SZ", "601231.SH", "601231.SH", "603053.SH", "603053.SH",
            "000736.SZ", "000736.SZ",
        ],
        "period_end": [
            "2025-09-30", "2025-06-30", "2025-06-30", "2025-06-30", "2025-06-30",
            "2025-09-30", "2025-09-30",
        ],
        "announce_date": [
            "2025-10-27", "2025-07-29", "2025-08-27", "2025-07-16", "2025-08-23",
            "2025-10-31", "2026-01-31",
        ],
        "roe": [None, None, 3.5036, None, 5.9077, None, None],
        "roe_diluted": [None, None, 3.48, None, 5.74, 414.06, -37.66],
    }).write_parquet(path)


def test_build_plan_fills_source_roe_and_removes_obsolete_pit_rows(tmp_path):
    _write_metrics(tmp_path)
    client = FakeClient({
        "001338.SZ": [{"end_date": "20250930", "ann_date": "20251027", "roe": 6.8895, "roe_dt": 6.8427}],
        "601231.SH": [{"end_date": "20250630", "ann_date": "20250827", "roe": 3.5036}],
        "603053.SH": [{"end_date": "20250630", "ann_date": "20250823", "roe": 5.9077}],
        "000736.SZ": [{"end_date": "20250930", "ann_date": "20251031", "roe": None}],
    })

    plan, result = supplement.build_plan(tmp_path, client)

    row = result.filter(
        (pl.col("symbol") == "001338.SZ")
        & (pl.col("period_end") == "2025-09-30")
    ).to_dicts()[0]
    assert row["roe"] == 6.8895
    assert row["roe_diluted"] == 6.8427
    assert result.filter(pl.col("symbol") == "601231.SH")["announce_date"].to_list() == ["2025-08-27"]
    assert result.filter(pl.col("symbol") == "603053.SH")["announce_date"].to_list() == ["2025-08-23"]
    assert result.filter(pl.col("symbol") == "000736.SZ")["announce_date"].to_list() == ["2025-10-31"]
    assert len(plan["changes"]) == 4
    assert supplement._conflicts(result) == []
