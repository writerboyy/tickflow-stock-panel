from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl

from app.free_strategy import fund_nav
from app.services.fund_nav_schema import write_fund_nav_schema_registry


def test_fund_nav_uses_china_calendar_date(monkeypatch):
    timestamp = datetime(2025, 7, 25, tzinfo=timezone(timedelta(hours=8))).timestamp() * 1000
    response = httpx.Response(
        200,
        text=f"var Data_netWorthTrend = [{{\"x\": {timestamp}, \"y\": 1.03}}];",
        request=httpx.Request("GET", "https://fund.eastmoney.com"),
    )
    monkeypatch.setattr(fund_nav.httpx, "get", lambda *_args, **_kwargs: response)

    rows = fund_nav._fetch_fund_nav("560860.SH")

    assert rows == [{
        "symbol": "560860.SH",
        "date": date(2025, 7, 25),
        "unit_net_value": 1.03,
        "date_timezone": "Asia/Shanghai",
    }]


def test_legacy_utc_nav_cache_is_shifted_to_china_date(tmp_path):
    path = tmp_path / "part.parquet"
    pl.DataFrame({
        "symbol": ["560860.SH"],
        "date": [date(2025, 7, 24)],
        "unit_net_value": [1.03],
    }).write_parquet(path)

    rows = fund_nav._read_cached_nav(path)

    assert rows == [{
        "symbol": "560860.SH",
        "date": date(2025, 7, 25),
        "unit_net_value": 1.03,
    }]


def test_fund_nav_registry_tracks_both_backward_compatible_physical_schemas(tmp_path):
    legacy = tmp_path / "fund_nav" / "symbol=510300.SH" / "part.parquet"
    current = tmp_path / "fund_nav" / "symbol=560860.SH" / "part.parquet"
    legacy.parent.mkdir(parents=True)
    current.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "date": [date(2025, 7, 24)],
        "unit_net_value": [1.0],
    }).write_parquet(legacy)
    pl.DataFrame({
        "symbol": ["560860.SH"],
        "date": [date(2025, 7, 25)],
        "unit_net_value": [1.03],
        "date_timezone": ["Asia/Shanghai"],
    }).write_parquet(current)

    metadata = write_fund_nav_schema_registry(tmp_path)

    assert metadata["logical_schema"] == ["symbol", "date", "unit_net_value"]
    assert metadata["physical_versions"]["1"]["files"] == 1
    assert metadata["physical_versions"]["2"]["files"] == 1
    assert (tmp_path / "fund_nav" / "metadata.json").exists()
