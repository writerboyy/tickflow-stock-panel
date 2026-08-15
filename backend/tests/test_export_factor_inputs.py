from __future__ import annotations

from datetime import date
import json

import polars as pl

from app.services.ext_data import ExtConfigStore
from scripts.export_factor_inputs import export_factor_inputs


def _write(data_dir, relative: str, rows: list[dict]) -> None:
    path = data_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def _fixture(data_dir) -> None:
    _write(data_dir, "kline_daily_enriched/date=2023-02-01/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2023, 2, 1), "close": 10.0, "raw_close": 10.0, "turnover_rate": 4.0},
    ])
    _write(data_dir, "kline_daily_enriched/date=2024-01-02/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2024, 1, 2), "close": 10.0, "raw_close": 10.0, "turnover_rate": 5.0},
    ])
    _write(data_dir, "kline_daily_enriched/date=2024-04-02/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2024, 4, 2), "close": 10.0, "raw_close": 10.0, "turnover_rate": 6.0},
    ])
    _write(data_dir, "valuation_daily/date=2024-01-02/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2024, 1, 2), "pe_ttm": 2.0, "pb": 1.0, "ps_ttm": 0.5, "net_income_ttm": 10.0},
    ])
    _write(data_dir, "valuation_daily/date=2023-02-01/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2023, 2, 1), "pe_ttm": 1.0, "pb": 0.8, "ps_ttm": 0.4, "net_income_ttm": 10.0},
    ])
    _write(data_dir, "valuation_daily/date=2024-04-02/part.parquet", [
        {"symbol": "000001.SZ", "date": date(2024, 4, 2), "pe_ttm": 3.0, "pb": 1.2, "ps_ttm": 0.6, "net_income_ttm": 10.0},
    ])
    _write(data_dir, "corporate_actions/stock_dividends.parquet", [
        {"symbol": "000001.SZ", "event_date": date(2024, 1, 1), "cash_per_share": 0.2},
        {"symbol": "000001.SZ", "event_date": date(2024, 1, 3), "cash_per_share": 5.0},
    ])
    _write(data_dir, "financials/balance_sheet/part.parquet", [
        {"symbol": "000001.SZ", "period_end": "2022-12-31", "announce_date": "2023-03-01", "total_assets": 100.0},
        {"symbol": "000001.SZ", "period_end": "2023-12-31", "announce_date": "2024-03-01", "total_assets": 120.0},
    ])
    _write(data_dir, "instruments/instruments.parquet", [
        {"symbol": "000001.SZ", "name": "平安银行", "listing_date": date(2010, 1, 1)},
    ])
    _write(data_dir, "ext_data/ext_tdx_margin/timeseries/date=2024-04-02/part.parquet", [
        {"symbol": "000001.SZ", "report_date": "2024-04-02", "margin_balance_10k": 12.5, "short_balance_10k": 3.0},
    ])


def test_export_preserves_source_units_and_does_not_leak_future_dividend(tmp_path):
    _fixture(tmp_path)

    result = export_factor_inputs(tmp_path, date(2024, 1, 2), date(2024, 4, 2))
    output = pl.concat([
        pl.read_parquet(tmp_path / "ext_data/ext_factor_inputs/timeseries/date=2024-01-02/part.parquet"),
        pl.read_parquet(tmp_path / "ext_data/ext_factor_inputs/timeseries/date=2024-04-02/part.parquet"),
    ]).sort("date")

    assert output.schema["date"] == pl.Date
    assert output["pe_ttm"].to_list() == [2.0, 3.0]
    assert output["turnover_rate_f"].to_list() == [5.0, 6.0]
    assert output["dv_ttm"].to_list()[0] == 2.0
    assert output["listing_date"].to_list() == [date(2010, 1, 1), date(2010, 1, 1)]
    assert output["margin_balance"].to_list() == [None, 125000.0]
    assert output["short_balance"].to_list() == [None, 30000.0]
    assert result["unsupported_fields"]["margin_ratio"]
    assert result["minute_coverage"]["i_fields_generated"] is False


def test_financial_fields_obey_announcement_date_and_metadata_is_written(tmp_path):
    _fixture(tmp_path)
    before = export_factor_inputs(tmp_path, date(2023, 2, 1), date(2023, 2, 1))
    before_row = pl.read_parquet(
        tmp_path / "ext_data/ext_factor_inputs/timeseries/date=2023-02-01/part.parquet"
    ).row(0, named=True)
    assert before_row["roa"] is None
    assert before_row["assets_yoy"] is None

    after = export_factor_inputs(tmp_path, date(2024, 4, 2), date(2024, 4, 2), run_id="pit")
    after_row = pl.read_parquet(
        tmp_path / "ext_data/ext_factor_inputs/timeseries/date=2024-04-02/part.parquet"
    ).row(0, named=True)
    after_output = pl.read_parquet(
        tmp_path / "ext_data/ext_factor_inputs/timeseries/date=2024-04-02/part.parquet"
    )
    assert round(after_row["roa"], 6) == round(10 / 120 * 100, 6)
    assert round(after_row["assets_yoy"], 6) == 20.0
    assert after_row["stock_basic"] == "平安银行"
    assert after["fields"]["roa"]["null_rate"] == 0.0
    assert after["fields"]["margin_ratio"]["unavailable_reason"]

    config = ExtConfigStore(tmp_path).get("ext_factor_inputs")
    assert config is not None
    assert config.primary_key == ["symbol", "date"]
    assert {field.name: field.dtype for field in config.fields}["date"] == "date"
    assert {field.name: field.dtype for field in config.fields}["listing_date"] == "date"
    assert after_output.schema["date"] == pl.Date
    assert after_output.schema["listing_date"] == pl.Date
    manifest = json.loads(
        (tmp_path / "ext_data/_ingestion/factor_inputs_export/ext_factor_inputs/pit.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["fields"]["pe_ttm"]["unit"] == "ratio"
    assert manifest["unsupported_fields"]["i_*"]
