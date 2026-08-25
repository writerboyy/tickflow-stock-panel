from datetime import date
from types import SimpleNamespace

import polars as pl

from app.free_strategy.process import _instrument_records


class Repository:
    def __init__(self, data_dir, instruments):
        self.store = SimpleNamespace(data_dir=data_dir)
        self._instruments = instruments

    def get_instruments_asset(self, _asset_type):
        return self._instruments


def test_instrument_records_use_standard_easy_tdx_snapshot_and_authoritative_name_history(
    tmp_path,
):
    industry_dir = tmp_path / "ext_data" / "ext_industry_tdx"
    industry_dir.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "industry_sw": ["X480101"],
        "industry_tdx": ["T01"],
    }).write_parquet(industry_dir / "part.parquet")
    legacy_dir = tmp_path / "ext_data" / "ext_hy_ths"
    legacy_dir.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "所属同花顺行业": ["不应再读取-旧行业"],
    }).write_parquet(legacy_dir / "part.parquet")
    history_dir = tmp_path / "instrument_name_history"
    history_dir.mkdir()
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "change_date": [date(2026, 4, 30)],
        "before_name": ["浦发银行"],
        "after_name": ["ST浦发"],
    }).write_parquet(history_dir / "part.parquet")
    repo = Repository(
        tmp_path,
        pl.DataFrame({
            "symbol": ["600000.SH"],
            "name": ["ST浦发"],
            "listing_date": [date(1999, 11, 10)],
        }),
    )

    records = _instrument_records(repo, "stock", "1d")

    assert records == [{
        "symbol": "600000.SH",
        "name": "ST浦发",
        "listing_date": date(1999, 11, 10),
        "asset_type": "stock",
        "has_minute": True,
        "name_changes": [{
            "date": "2026-04-30",
            "before": "浦发银行",
            "after": "ST浦发",
        }],
        "industry_sw": "X480101",
        "industry_tdx": "T01",
    }]


def test_instrument_records_exclude_placeholder_listing_date(tmp_path):
    repo = Repository(
        tmp_path,
        pl.DataFrame({
            "symbol": ["301717.SZ", "600000.SH"],
            "name": ["待上市", "浦发银行"],
            "listing_date": [date(1970, 1, 1), date(1999, 11, 10)],
        }),
    )

    records = _instrument_records(repo, "stock", "1d")

    assert [record["symbol"] for record in records] == ["600000.SH"]
