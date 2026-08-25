from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.kline import router
from app.services import etf_data_repair as repair
from app.tickflow.repository import DataStore, KlineRepository


def _repo(tmp_path) -> KlineRepository:
    store = DataStore(tmp_path)
    daily_path = tmp_path / "kline_etf_daily" / "part.parquet"
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.SH", "510300.SH"],
        "date": [date(2026, 7, 20), date(2026, 7, 21)],
        "open": [4.0, 4.1], "high": [4.1, 4.2], "low": [3.9, 4.0], "close": [4.05, 4.15],
        "volume": [1_000.0, 1_100.0], "amount": [405_000.0, 456_500.0],
    }).write_parquet(daily_path)
    minute_path = tmp_path / "kline_etf_minute" / "date=2026-07-20" / "part.parquet"
    minute_path.parent.mkdir(parents=True, exist_ok=True)
    _minute_frame("510300.SH", date(2026, 7, 20)).write_parquet(minute_path)
    factor_path = tmp_path / "adj_factor_etf" / "all.parquet"
    factor_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.SH"], "trade_date": [date(2026, 7, 21)], "ex_factor": [1.994565],
    }).write_parquet(factor_path)
    return KlineRepository(store)


def _minute_frame(symbol: str, day: date) -> pl.DataFrame:
    morning = [datetime.combine(day, datetime.min.time()).replace(hour=9, minute=31) + timedelta(minutes=index) for index in range(120)]
    afternoon = [datetime.combine(day, datetime.min.time()).replace(hour=13, minute=1) + timedelta(minutes=index) for index in range(120)]
    times = morning + afternoon
    prices = [4.1 + index * 0.0001 for index in range(len(times))]
    return pl.DataFrame({
        "symbol": [symbol] * len(times), "datetime": times,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": [100.0] * len(times), "amount": [41_000.0] * len(times),
    })


def _scan(repo: KlineRepository) -> dict:
    return repair.inspect_etf_data(
        repo,
        ["510300.SH"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        require_minute=True,
        persist_scan=True,
    )


def test_inspection_marks_only_minute_gap_as_repairable(tmp_path):
    repo = _repo(tmp_path)

    result = _scan(repo)

    assert result["status"] == "issues"
    issues = {issue["type"]: issue for issue in result["issues"]}
    assert issues["minute_gap"]["repairable"] is True
    assert issues["minute_gap"]["missing_dates"] == ["2026-07-21"]
    assert issues["split_rounding"]["repairable"] is False
    assert (tmp_path / "etf_data_repairs" / "scans" / f"{result['scan_id']}.json").exists()


def test_inspection_tolerates_added_daily_quote_timestamp(tmp_path):
    repo = _repo(tmp_path)
    extra = tmp_path / "kline_etf_daily" / "date=2026-07-22" / "part.parquet"
    extra.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["510300.SH"],
        "date": [date(2026, 7, 22)],
        "open": [4.2],
        "high": [4.3],
        "low": [4.1],
        "close": [4.25],
        "volume": [1200.0],
        "amount": [5100.0],
        "quote_ts": [1784683800000],
    }).write_parquet(extra)

    result = repair.inspect_etf_data(
        repo,
        ["510300.SH"],
        date(2026, 7, 20),
        date(2026, 7, 22),
        require_minute=False,
    )

    assert not any(issue["type"] == "daily_missing" for issue in result["issues"])


def test_inspection_rejects_per_symbol_stale_daily_tail(tmp_path):
    repo = _repo(tmp_path)
    latest = tmp_path / "kline_etf_daily" / "date=2026-07-22" / "part.parquet"
    latest.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["515880.SH"], "date": [date(2026, 7, 22)],
        "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
        "volume": [100.0], "amount": [10_000.0],
    }).write_parquet(latest)

    result = repair.inspect_etf_data(
        repo, ["510300.SH"], date(2026, 7, 20), date(2026, 7, 22),
        require_minute=False,
    )

    issue = next(item for item in result["issues"] if item["type"] == "daily_tail_stale")
    assert issue["latest_date"] == "2026-07-21"
    assert issue["expected_date"] == "2026-07-22"


def test_inspection_rejects_short_daily_warmup(tmp_path):
    repo = _repo(tmp_path)

    result = repair.inspect_etf_data(
        repo, ["510300.SH"], date(2026, 7, 20), date(2026, 7, 21),
        require_minute=False, min_daily_bars=3,
    )

    issue = next(item for item in result["issues"] if item["type"] == "daily_history_short")
    assert issue["available_bars"] == 2
    assert issue["required_bars"] == 3


def test_inspection_rejects_minute_day_with_invalid_ohlc(tmp_path):
    repo = _repo(tmp_path)
    path = tmp_path / "kline_etf_minute" / "date=2026-07-21" / "part.parquet"
    path.parent.mkdir(parents=True)
    frame = _minute_frame("510300.SH", date(2026, 7, 21)).with_columns(
        pl.when(pl.col("datetime") == datetime(2026, 7, 21, 9, 31))
        .then(0.0)
        .otherwise(pl.col("open"))
        .alias("open")
    )
    frame.write_parquet(path)

    result = repair.inspect_etf_data(
        repo, ["510300.SH"], date(2026, 7, 21), date(2026, 7, 21),
        require_minute=True,
    )

    issue = next(item for item in result["issues"] if item["type"] == "minute_gap")
    assert issue["missing_dates"] == ["2026-07-21"]


def test_inspection_rejects_minute_day_with_invalid_ohlc_bounds(tmp_path):
    repo = _repo(tmp_path)
    path = tmp_path / "kline_etf_minute" / "date=2026-07-21" / "part.parquet"
    path.parent.mkdir(parents=True)
    frame = _minute_frame("510300.SH", date(2026, 7, 21)).with_columns(
        pl.when(pl.col("datetime") == datetime(2026, 7, 21, 9, 31))
        .then(pl.col("open") - 0.01)
        .otherwise(pl.col("high"))
        .alias("high")
    )
    frame.write_parquet(path)

    result = repair.inspect_etf_data(
        repo, ["510300.SH"], date(2026, 7, 21), date(2026, 7, 21),
        require_minute=True,
    )

    issue = next(item for item in result["issues"] if item["type"] == "minute_gap")
    assert issue["missing_dates"] == ["2026-07-21"]


def test_repair_uses_configured_minute_source_without_overwriting_existing(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    scan = _scan(repo)
    minute_issue = next(issue for issue in scan["issues"] if issue["type"] == "minute_gap")
    captured = {}

    def fetch(symbols, **kwargs):
        captured["symbols"] = symbols
        captured.update(kwargs)
        return _minute_frame("510300.SH", date(2026, 7, 21))

    monkeypatch.setattr(repair.kline_sync, "sync_minute_batch", fetch)

    result = repair.repair_etf_data(repo, scan["scan_id"], [minute_issue["id"]])

    assert result["status"] == "succeeded"
    assert result["minute_rows"] == 240
    assert result["source"] == "configured_minute_provider"
    assert captured["symbols"] == ["510300.SH"]
    assert captured["asset_type"] == "etf"
    stored = pl.read_parquet(tmp_path / "kline_etf_minute" / "date=2026-07-21" / "part.parquet")
    assert stored.filter(pl.col("symbol") == "510300.SH").height == 240
    assert repair.list_repair_records(tmp_path)[0]["scan_id"] == scan["scan_id"]


def test_repair_fails_closed_when_current_source_returns_incomplete_day(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    scan = _scan(repo)
    minute_issue = next(issue for issue in scan["issues"] if issue["type"] == "minute_gap")
    monkeypatch.setattr(
        repair.kline_sync,
        "sync_minute_batch",
        lambda *_args, **_kwargs: _minute_frame("510300.SH", date(2026, 7, 21)).head(1),
    )

    with pytest.raises(RuntimeError, match="完整交易日"):
        repair.repair_etf_data(repo, scan["scan_id"], [minute_issue["id"]])

    assert not (tmp_path / "kline_etf_minute" / "date=2026-07-21" / "part.parquet").exists()
    assert repair.list_repair_records(tmp_path)[0]["status"] == "failed"


def test_repair_fails_closed_when_current_source_has_invalid_ohlc_bounds(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    scan = _scan(repo)
    minute_issue = next(issue for issue in scan["issues"] if issue["type"] == "minute_gap")
    invalid = _minute_frame("510300.SH", date(2026, 7, 21)).with_columns(
        pl.when(pl.col("datetime") == datetime(2026, 7, 21, 9, 31))
        .then(pl.col("open") - 0.01)
        .otherwise(pl.col("high"))
        .alias("high")
    )
    monkeypatch.setattr(
        repair.kline_sync,
        "sync_minute_batch",
        lambda *_args, **_kwargs: invalid,
    )

    with pytest.raises(RuntimeError, match="240 根连续竞价数据"):
        repair.repair_etf_data(repo, scan["scan_id"], [minute_issue["id"]])

    assert not (tmp_path / "kline_etf_minute" / "date=2026-07-21" / "part.parquet").exists()
    assert repair.list_repair_records(tmp_path)[0]["status"] == "failed"


def test_repair_api_rejects_non_repairable_issue(tmp_path):
    repo = _repo(tmp_path)
    scan = _scan(repo)
    split = next(issue for issue in scan["issues"] if issue["type"] == "split_rounding")

    class Capabilities:
        def has(self, _cap):
            return True

    app = FastAPI()
    app.state.repo = repo
    app.state.capabilities = Capabilities()
    app.include_router(router)

    response = TestClient(app).post("/api/kline/etf-data/repair", json={
        "scan_id": scan["scan_id"], "issue_ids": [split["id"]],
    })

    assert response.status_code == 400
    assert "当前分钟数据源" in response.json()["detail"]
