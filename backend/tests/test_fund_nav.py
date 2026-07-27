from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl

from app.free_strategy import fund_nav


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
