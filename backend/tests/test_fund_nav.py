from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl

from app.free_strategy import fund_nav
from app.services.fund_nav_schema import write_fund_nav_schema_registry


class FakeEngine:
    def __init__(self) -> None:
        self.extra_history = {}

    def set_extra_history(self, name, values) -> None:
        self.extra_history.setdefault(name, {}).update(values)


def _write_current_cache(path, symbol: str, rows: list[tuple[date, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": [symbol for _day, _value in rows],
        "date": [day for day, _value in rows],
        "unit_net_value": [value for _day, value in rows],
        "date_timezone": ["Asia/Shanghai" for _day, _value in rows],
    }).write_parquet(path)


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


def test_stale_nonempty_cache_is_refreshed_merged_and_written_as_v2(tmp_path, monkeypatch):
    symbol = "510300.SH"
    path = fund_nav._fund_nav_path(tmp_path, symbol)
    _write_current_cache(path, symbol, [
        (date(2025, 7, 22), 1.00),
        (date(2025, 7, 23), 1.01),
    ])
    fetched = []

    def fetch(requested):
        fetched.append(requested)
        return [
            {"symbol": symbol, "date": date(2025, 7, 23), "unit_net_value": 1.02, "date_timezone": "Asia/Shanghai"},
            {"symbol": symbol, "date": date(2025, 7, 24), "unit_net_value": 1.03, "date_timezone": "Asia/Shanghai"},
        ]

    monkeypatch.setattr(fund_nav, "_fetch_fund_nav", fetch)
    engine = FakeEngine()

    result = fund_nav.load_fund_nav_history(
        tmp_path, engine, [symbol], date(2025, 7, 1), date(2025, 7, 24),
    )

    assert fetched == [symbol]
    assert engine.extra_history["unit_net_value"][symbol] == {
        date(2025, 7, 22): 1.00,
        date(2025, 7, 23): 1.02,
        date(2025, 7, 24): 1.03,
    }
    stored = pl.read_parquet(path)
    assert stored.columns == ["symbol", "date", "unit_net_value", "date_timezone"]
    assert stored["date"].to_list() == [
        date(2025, 7, 22), date(2025, 7, 23), date(2025, 7, 24),
    ]
    assert list(path.parent.glob(".*.tmp")) == []
    assert result["symbol_freshness"][symbol]["freshness_status"] == "fresh"


def test_refresh_failure_preserves_stale_cache_and_reports_unavailable(tmp_path, monkeypatch):
    symbol = "510300.SH"
    path = fund_nav._fund_nav_path(tmp_path, symbol)
    _write_current_cache(path, symbol, [(date(2025, 7, 23), 1.01)])
    original = path.read_bytes()

    def fail(_symbol):
        raise httpx.TransportError("offline")

    monkeypatch.setattr(fund_nav, "_fetch_fund_nav", fail)
    result = fund_nav.load_fund_nav_history(
        tmp_path, FakeEngine(), [symbol], date(2025, 7, 1), date(2025, 7, 24),
    )

    assert path.read_bytes() == original
    assert result["symbol_freshness"][symbol] == {
        "required_date": "2025-07-24",
        "actual_date": "2025-07-23",
        "freshness_status": "unavailable",
    }
    assert result["unavailable_symbols"] == [symbol]


def test_fresh_cache_does_not_fetch(tmp_path, monkeypatch):
    symbol = "510300.SH"
    path = fund_nav._fund_nav_path(tmp_path, symbol)
    _write_current_cache(path, symbol, [(date(2025, 7, 24), 1.03)])
    monkeypatch.setattr(
        fund_nav,
        "_fetch_fund_nav",
        lambda _symbol: (_ for _ in ()).throw(AssertionError("fresh cache must not fetch")),
    )

    result = fund_nav.load_fund_nav_history(
        tmp_path, FakeEngine(), [symbol], date(2025, 7, 1), date(2025, 7, 24),
    )

    assert result["symbol_freshness"][symbol]["freshness_status"] == "fresh"
