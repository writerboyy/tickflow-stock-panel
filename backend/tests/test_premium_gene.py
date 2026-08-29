from datetime import date, timedelta

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import stock_analysis
from app.services import premium_gene


def _history() -> pl.DataFrame:
    start = date(2026, 1, 5)
    return pl.DataFrame({
        "symbol": ["A"] * 8,
        "date": [start + timedelta(days=i) for i in range(8)],
        "change_pct": [0.0, -0.01, 0.0, 0.0, 0.06, -0.01, 0.0, 0.02],
        "signal_limit_up": [False, False, False, True, True, False, False, True],
        "signal_broken_limit_up": [False, True, False, False, False, False, False, False],
        "consecutive_limit_ups": [0, 0, 0, 1, 2, 0, 0, 1],
    })


def test_calculate_premium_gene_metrics_and_future_day_exclusion():
    result = premium_gene.calculate(_history())

    row = result.row(0, named=True)
    assert row["limit_up_count"] == 3
    assert row["premium_5_count"] == 1
    # 最后一个涨停日没有次日数据，不进入次日红盘率分母。
    assert row["next_day_observation_count"] == 2
    assert row["next_day_red_count"] == 1
    assert row["next_day_red_rate"] == 0.5
    assert row["first_board_attempt_count"] == 3
    assert row["first_board_sealed_count"] == 2
    assert row["first_board_broken_count"] == 1
    assert row["first_board_seal_rate"] == 2 / 3
    assert row["first_board_broken_rate"] == 1 / 3
    assert row["consecutive_limit_up_count"] == 1
    assert row["consecutive_rate"] == 1 / 2


def test_consecutive_rate_counts_one_advancement_for_a_three_board_chain():
    start = date(2026, 1, 5)
    history = pl.DataFrame({
        "symbol": ["A"] * 4,
        "date": [start + timedelta(days=i) for i in range(4)],
        "change_pct": [0.0, 0.0, 0.0, 0.0],
        "signal_limit_up": [False, True, True, True],
        "signal_broken_limit_up": [True, False, False, False],
        "consecutive_limit_ups": [0, 1, 2, 3],
    })

    row = premium_gene.calculate(history).row(0, named=True)

    assert row["consecutive_limit_up_count"] == 2
    assert row["first_board_sealed_count"] == 1
    assert row["first_board_broken_count"] == 1
    assert row["consecutive_rate"] == 1.0


def test_calculate_uses_last_n_trading_rows_per_symbol():
    result = premium_gene.calculate(_history(), window_days=3)

    row = result.row(0, named=True)
    # 窗口是最后三条记录: 两条非涨停、最后一条涨停无次日。
    assert row["window_days"] == 3
    assert row["limit_up_count"] == 1
    assert row["next_day_observation_count"] == 0
    assert row["first_board_attempt_count"] == 1
    assert row["first_board_seal_rate"] == 1.0
    assert row["consecutive_rate"] == 0.0


def test_snapshot_persistence_round_trip(tmp_path):
    rows = premium_gene.calculate(_history())
    premium_gene.persist_snapshot(tmp_path, rows)

    loaded = premium_gene.load_snapshot(tmp_path)
    assert loaded.select(premium_gene.SNAPSHOT_COLUMNS).equals(rows)


def test_get_for_symbol_returns_unavailable_without_enriched_date(tmp_path):
    class _Repo:
        class store:
            data_dir = tmp_path

        @staticmethod
        def latest_enriched_date(_asset_type):
            return None

    result = premium_gene.get_for_symbol(_Repo(), "000001.SZ")
    assert result == {
        "available": False,
        "symbol": "000001.SZ",
        "as_of": None,
        "window_days": 200,
    }


def test_get_for_symbol_can_read_stale_snapshot_without_refresh(tmp_path, monkeypatch):
    class _Repo:
        class store:
            data_dir = tmp_path

        @staticmethod
        def latest_enriched_date(_asset_type):
            return date(2026, 8, 14)

    premium_gene.persist_snapshot(tmp_path, premium_gene.calculate(_history()))
    monkeypatch.setattr(
        premium_gene,
        "refresh",
        lambda *_args, **_kwargs: pytest.fail("snapshot mode must not refresh the full universe"),
    )

    result = premium_gene.get_for_symbol(_Repo(), "A", refresh_if_stale=False)

    assert result["available"] is True
    assert result["as_of"] == "2026-01-12"


def test_premium_gene_api_contract(tmp_path):
    class _Repo:
        class store:
            data_dir = tmp_path

        @staticmethod
        def latest_enriched_date(_asset_type):
            return None

    app = FastAPI()
    app.state.repo = _Repo()
    app.include_router(stock_analysis.router)

    response = TestClient(app).get(
        "/api/stock-analysis/premium-gene?symbol=000001.SZ"
    )
    assert response.status_code == 200
    assert response.json() == {
        "available": False,
        "symbol": "000001.SZ",
        "as_of": None,
        "window_days": 200,
    }


def test_premium_gene_api_snapshot_mode_avoids_live_lookup(tmp_path, monkeypatch):
    class _Repo:
        class store:
            data_dir = tmp_path

    expected = {"available": False, "symbol": "000001.SZ", "as_of": "2026-08-14", "window_days": 200}

    monkeypatch.setattr(premium_gene, "get_for_symbol", lambda *_args, **_kwargs: expected)

    async def _unexpected_live(*_args, **_kwargs):
        pytest.fail("snapshot mode must not call the live provider")

    monkeypatch.setattr(premium_gene, "get_for_symbol_async", _unexpected_live)
    app = FastAPI()
    app.state.repo = _Repo()
    app.include_router(stock_analysis.router)

    response = TestClient(app).get(
        "/api/stock-analysis/premium-gene?symbol=000001.SZ&live=false"
    )

    assert response.status_code == 200
    assert response.json() == expected


@pytest.mark.asyncio
async def test_get_for_symbol_async_prefers_kaipanla_gene_result(tmp_path, monkeypatch):
    class _Repo:
        class store:
            data_dir = tmp_path

        @staticmethod
        def latest_enriched_date(_asset_type):
            return date(2026, 8, 14)

    class _Credentials:
        pass

    class _Client:
        def __init__(self, **_kwargs):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, endpoint, params):
            self.calls.append((endpoint, params))
            assert endpoint == 76
            assert params == {"StockID": "001330"}
            return {"List": [9, 3, 100, 88.8889, 11.1111, 12.5], "errcode": "0"}

    monkeypatch.setattr(premium_gene, "load_credentials", lambda: _Credentials())
    monkeypatch.setattr(premium_gene, "KaipanlaClient", _Client)
    premium_gene._live_cache.clear()

    result = await premium_gene.get_for_symbol_async(_Repo(), "001330.SZ")

    assert result["available"] is True
    assert result["symbol"] == "001330.SZ"
    assert result["limit_up_count"] == 9
    assert result["premium_5_count"] == 3
    assert result["next_day_red_rate"] == 1.0
    assert result["first_board_seal_rate"] == pytest.approx(0.888889)
    assert result["first_board_broken_rate"] == pytest.approx(0.111111)
    assert result["consecutive_rate"] == 0.125
    assert result["max_score"] == 10.0
    assert result["passed"] is True


@pytest.mark.asyncio
async def test_get_for_symbol_async_fills_live_result_with_snapshot_counts(tmp_path, monkeypatch):
    class _Repo:
        class store:
            data_dir = tmp_path

        @staticmethod
        def latest_enriched_date(_asset_type):
            return date(2026, 8, 14)

    snapshot = premium_gene.calculate(
        _history().with_columns(pl.lit("001331.SZ").alias("symbol")),
    )
    premium_gene.persist_snapshot(tmp_path, snapshot)

    class _Credentials:
        pass

    class _Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, endpoint, params):
            assert endpoint == 76
            assert params == {"StockID": "001331"}
            return {"List": [9, 3, 100, 88.8889, 11.1111, 12.5], "errcode": "0"}

    monkeypatch.setattr(premium_gene, "load_credentials", lambda: _Credentials())
    monkeypatch.setattr(premium_gene, "KaipanlaClient", _Client)
    premium_gene._live_cache.clear()

    result = await premium_gene.get_for_symbol_async(_Repo(), "001331.SZ")

    # Provider values remain authoritative, while fields absent from /76 are
    # filled from the local snapshot for a complete UI card.
    assert result["next_day_red_rate"] == 1.0
    assert result["first_board_seal_rate"] == pytest.approx(0.888889)
    assert result["first_board_attempt_count"] == 3
    assert result["first_board_broken_count"] == 1
    assert result["next_day_observation_count"] == 2
