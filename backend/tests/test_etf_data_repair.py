from __future__ import annotations

from datetime import date, datetime

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
    pl.DataFrame({
        "symbol": ["510300.SH"], "datetime": [datetime(2026, 7, 20, 9, 30)],
        "open": [4.0], "high": [4.0], "low": [4.0], "close": [4.0],
        "volume": [100.0], "amount": [40_000.0],
    }).write_parquet(minute_path)
    factor_path = tmp_path / "adj_factor_etf" / "all.parquet"
    factor_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["510300.SH"], "trade_date": [date(2026, 7, 21)], "ex_factor": [1.994565],
    }).write_parquet(factor_path)
    return KlineRepository(store)


def test_inspection_detects_minute_gap_and_approximate_split(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    monkeypatch.setattr(
        repair,
        "axdata_status",
        lambda _url: {"available": True, "url": "http://axdata", "message": "AxData 可用"},
    )

    result = repair.inspect_etf_data(
        repo,
        ["510300.SH"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        require_minute=True,
        verify_axdata=False,
        axdata_url="http://axdata",
        persist_scan=True,
    )

    assert result["status"] == "issues"
    assert {issue["type"] for issue in result["issues"]} == {"minute_gap", "split_rounding"}
    minute_issue = next(issue for issue in result["issues"] if issue["type"] == "minute_gap")
    assert minute_issue["missing_days"] == 1
    assert minute_issue["start"] == "2026-07-21"
    assert (tmp_path / "etf_data_repairs" / "scans" / f"{result['scan_id']}.json").exists()


def test_replacement_issue_requires_confirmation_and_records_success(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    monkeypatch.setattr(
        repair,
        "axdata_status",
        lambda _url: {"available": True, "url": "http://axdata", "message": "AxData 可用"},
    )
    scan = repair.inspect_etf_data(
        repo,
        ["510300.SH"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        require_minute=True,
        verify_axdata=False,
        axdata_url="http://axdata",
        persist_scan=True,
    )
    split = next(issue for issue in scan["issues"] if issue["type"] == "split_rounding")

    with pytest.raises(PermissionError, match="明确确认覆盖"):
        repair.validate_repair_request(
            tmp_path, scan["scan_id"], [split["id"]], replace_existing=False,
        )

    calls = []
    monkeypatch.setattr(
        repair,
        "import_symbol",
        lambda **kwargs: calls.append(kwargs) or (2, 480),
    )
    result = repair.repair_etf_data(
        repo,
        scan["scan_id"],
        [split["id"]],
        replace_existing=True,
        axdata_url="http://axdata",
    )

    assert result["status"] == "succeeded"
    assert result["minute_rows"] == 480
    assert calls[0]["replace_minute"] is True
    assert repair.list_repair_records(tmp_path)[0]["scan_id"] == scan["scan_id"]


def test_repair_api_rejects_unconfirmed_replacement(monkeypatch, tmp_path):
    repo = _repo(tmp_path)
    monkeypatch.setattr(
        repair,
        "axdata_status",
        lambda _url: {"available": True, "url": "http://axdata", "message": "AxData 可用"},
    )
    scan = repair.inspect_etf_data(
        repo,
        ["510300.SH"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        require_minute=True,
        verify_axdata=False,
        axdata_url="http://axdata",
        persist_scan=True,
    )
    split = next(issue for issue in scan["issues"] if issue["type"] == "split_rounding")
    app = FastAPI()
    app.state.repo = repo
    app.include_router(router)

    response = TestClient(app).post("/api/kline/etf-data/repair", json={
        "scan_id": scan["scan_id"], "issue_ids": [split["id"]], "replace_existing": False,
    })

    assert response.status_code == 409
    assert "明确确认覆盖" in response.json()["detail"]
