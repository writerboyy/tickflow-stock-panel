from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from app.api import data as data_api
from app.indicators import pipeline
from app.share_capital import apply_point_in_time_shares
from app.services import financial_sync
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet


def _write_instruments(data_dir, symbols: list[str]) -> None:
    path = data_dir / "instruments" / "instruments.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(path)


def test_first_share_sync_fetches_complete_history(tmp_path, monkeypatch):
    _write_instruments(tmp_path, ["600000.SH"])
    calls: list[tuple[list[str], bool]] = []

    def fake_fetch(table, symbols, capset, latest_only=True):
        assert table == "shares"
        calls.append((symbols, latest_only))
        return pl.DataFrame({
            "symbol": ["600000.SH", "600000.SH"],
            "period_end": ["2023-12-31", "2024-06-30"],
            "float_shares": [10.0, 12.0],
        })

    monkeypatch.setattr(financial_sync, "_fetch_table", fake_fetch)

    rows = financial_sync.sync_shares(tmp_path, CapabilitySet())

    assert rows == 2
    assert calls == [(["600000.SH"], False)]
    stored = pl.read_parquet(tmp_path / "financials" / "shares" / "part.parquet")
    assert stored["period_end"].to_list() == ["2023-12-31", "2024-06-30"]


def test_incremental_share_sync_updates_existing_and_backfills_new_symbols(tmp_path, monkeypatch):
    _write_instruments(tmp_path, ["600000.SH", "000001.SZ"])
    path = tmp_path / "financials" / "shares" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "period_end": ["2024-06-30"],
        "float_shares": [10.0],
    }).write_parquet(path)
    calls: list[tuple[list[str], bool]] = []

    def fake_fetch(table, symbols, capset, latest_only=True):
        assert table == "shares"
        calls.append((symbols, latest_only))
        if latest_only:
            return pl.DataFrame({
                "symbol": ["600000.SH"],
                "period_end": ["2024-06-30"],
                "float_shares": [11.0],
            })
        return pl.DataFrame({
            "symbol": ["000001.SZ", "000001.SZ"],
            "period_end": ["2023-12-31", "2024-06-30"],
            "float_shares": [20.0, 21.0],
        })

    monkeypatch.setattr(financial_sync, "_fetch_table", fake_fetch)

    rows = financial_sync.sync_shares(tmp_path, CapabilitySet())

    assert rows == 3
    assert calls == [(["000001.SZ"], False), (["600000.SH"], True)]
    stored = pl.read_parquet(path).sort(["symbol", "period_end"])
    assert stored.filter(pl.col("symbol") == "600000.SH")["float_shares"].to_list() == [11.0]
    assert stored.filter(pl.col("symbol") == "000001.SZ")["float_shares"].to_list() == [20.0, 21.0]


def test_custom_financial_provider_receives_shares_contract(monkeypatch):
    received: list[tuple[str, list[str], bool]] = []

    class Provider:
        def get_financials(self, table, symbols, latest_only=True):
            received.append((table, symbols, latest_only))
            return pl.DataFrame({
                "symbol": symbols,
                "period_end": ["2024-06-30"],
                "float_shares": [10.0],
            })

    from app.data_providers import custom as custom_sources
    from app.services import preferences

    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: "custom-test")
    monkeypatch.setattr(custom_sources, "get_provider", lambda _name: Provider())

    result = financial_sync._fetch_table(
        "shares",
        ["600000.SH"],
        CapabilitySet(),
        latest_only=False,
    )

    assert result.height == 1
    assert received == [("shares", ["600000.SH"], False)]


def test_custom_financial_provider_receives_valuation_contract(monkeypatch):
    received: list[tuple[str, list[str], bool]] = []

    class Provider:
        def get_financials(self, table, symbols, latest_only=True):
            received.append((table, symbols, latest_only))
            return pl.DataFrame({
                "symbol": symbols,
                "date": ["2024-06-30"],
                "market_cap": [10.0],
            })

    from app.data_providers import custom as custom_sources
    from app.services import preferences

    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)
    monkeypatch.setattr(preferences, "get_financial_provider", lambda: "custom-test")
    monkeypatch.setattr(custom_sources, "get_provider", lambda _name: Provider())

    result = financial_sync._fetch_table(
        "valuation",
        ["600000.SH"],
        CapabilitySet(),
        latest_only=False,
    )

    assert result.height == 1
    assert received == [("valuation", ["600000.SH"], False)]


def test_tickflow_financial_provider_skips_unsupported_valuation(monkeypatch):
    financials = SimpleNamespace(
        metrics=lambda *_args, **_kwargs: {},
        income=lambda *_args, **_kwargs: {},
        balance_sheet=lambda *_args, **_kwargs: {},
        cash_flow=lambda *_args, **_kwargs: {},
        shares=lambda *_args, **_kwargs: {},
    )

    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: False)
    monkeypatch.setattr(
        "app.tickflow.client.get_client",
        lambda: SimpleNamespace(financials=financials),
    )

    result = financial_sync._fetch_table(
        "valuation",
        ["600000.SH"],
        CapabilitySet({Cap.FINANCIAL: CapabilityLimits()}),
        latest_only=False,
    )

    assert result.is_empty()


def test_statement_sync_fetches_complete_history_for_backtests(tmp_path, monkeypatch):
    _write_instruments(tmp_path, ["600000.SH"])
    calls: list[tuple[str, list[str], bool]] = []

    def fake_fetch(table, symbols, capset, latest_only=True):
        calls.append((table, symbols, latest_only))
        return pl.DataFrame({
            "symbol": ["600000.SH", "600000.SH"],
            "period_end": ["2023-12-31", "2024-03-31"],
            "announce_date": ["2024-04-15", "2024-04-30"],
        })

    monkeypatch.setattr(financial_sync, "_fetch_table", fake_fetch)

    assert financial_sync.sync_metrics(tmp_path, CapabilitySet()) == 2
    assert financial_sync.sync_income(tmp_path, CapabilitySet()) == 2
    assert financial_sync.sync_balance_sheet(tmp_path, CapabilitySet()) == 2
    assert financial_sync.sync_cash_flow(tmp_path, CapabilitySet()) == 2

    assert calls == [
        ("metrics", ["600000.SH"], False),
        ("income", ["600000.SH"], False),
        ("balance_sheet", ["600000.SH"], False),
        ("cash_flow", ["600000.SH"], False),
    ]


def test_sync_all_fetches_statement_history_not_latest_only(tmp_path, monkeypatch):
    _write_instruments(tmp_path, ["600000.SH"])
    calls: list[tuple[str, bool]] = []

    def fake_fetch(table, symbols, capset, latest_only=True):
        calls.append((table, latest_only))
        return pl.DataFrame({
            "symbol": ["600000.SH"],
            "period_end": ["2024-03-31"],
            "announce_date": ["2024-04-30"],
        })

    monkeypatch.setattr(financial_sync, "_fetch_table", fake_fetch)

    capset = CapabilitySet({Cap.FINANCIAL: CapabilityLimits()})
    result = financial_sync.sync_all(tmp_path, capset)

    assert result == {
        "metrics": 1,
        "income": 1,
        "balance_sheet": 1,
        "cash_flow": 1,
        "shares": 1,
        "valuation": 1,
    }
    assert calls == [
        ("metrics", False),
        ("income", False),
        ("balance_sheet", False),
        ("cash_flow", False),
        ("shares", False),
        ("valuation", False),
    ]


def test_historical_turnover_uses_only_available_share_capital(monkeypatch):
    monkeypatch.setattr(pipeline, "cn_today", lambda: date(2026, 7, 18))
    bars = pl.DataFrame({
        "symbol": ["600000.SH"] * 5,
        "date": [
            date(2024, 3, 31),
            date(2024, 4, 14),
            date(2024, 4, 15),
            date(2024, 6, 30),
            date(2026, 7, 18),
        ],
        "volume": [10_000.0] * 5,
    })
    instruments = pl.DataFrame({
        "symbol": ["600000.SH"],
        "total_shares": [400_000_000.0],
        "float_shares": [200_000_000.0],
    })
    shares = pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH"],
        "period_end": ["2023-12-31", "2024-06-30"],
        "announce_date": ["2024-04-15", None],
        "total_shares": [300_000_000.0, 350_000_000.0],
        "float_shares": [100_000_000.0, 50_000_000.0],
    })

    result = pipeline.compute_limit_signals(
        bars,
        instruments,
        needed={"turnover_rate"},
        historical_shares=shares,
    )

    values = result["turnover_rate"].to_list()
    assert values[:2] == [None, None]
    assert values[2:] == pytest.approx([1.0, 2.0, 0.5])
    assert result["total_shares"].to_list() == [
        None,
        None,
        300_000_000.0,
        350_000_000.0,
        400_000_000.0,
    ]
    assert result["float_shares"].to_list() == [
        None,
        None,
        100_000_000.0,
        50_000_000.0,
        200_000_000.0,
    ]


def test_turnover_without_share_history_does_not_use_current_shares_for_history(monkeypatch):
    monkeypatch.setattr(pipeline, "cn_today", lambda: date(2026, 7, 18))
    bars = pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2024, 4, 15)],
        "volume": [10_000.0],
    })
    instruments = pl.DataFrame({
        "symbol": ["600000.SH"],
        "float_shares": [200_000_000.0],
    })

    result = pipeline.compute_limit_signals(
        bars,
        instruments,
        needed={"turnover_rate"},
    )

    assert result["turnover_rate"][0] is None


def test_share_capital_waits_for_both_announcement_and_effective_date():
    rows = pl.DataFrame({
        "symbol": ["600000.SH"] * 3,
        "date": [date(2024, 6, 14), date(2024, 6, 15), date(2024, 6, 30)],
        "total_shares": [200.0] * 3,
        "float_shares": [100.0] * 3,
        "_instrument_as_of": [date(2026, 7, 18)] * 3,
    })
    shares = pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH"],
        "period_end": ["2023-12-31", "2024-06-30"],
        "announce_date": ["2024-01-15", "2024-06-15"],
        "total_shares": [80.0, 160.0],
        "float_shares": [40.0, 80.0],
    })

    result = apply_point_in_time_shares(rows, shares, today=date(2026, 7, 18))

    assert result["total_shares"].to_list() == [80.0, 80.0, 160.0]
    assert result["float_shares"].to_list() == [40.0, 40.0, 80.0]


def test_instrument_snapshot_is_only_used_on_or_after_its_as_of_date():
    rows = pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH"],
        "date": [date(2024, 5, 31), date(2024, 6, 1)],
        "total_shares": [200.0, 200.0],
        "float_shares": [100.0, 100.0],
        "_instrument_as_of": [date(2024, 6, 1), date(2024, 6, 1)],
    })

    result = apply_point_in_time_shares(rows, pl.DataFrame(), today=date(2026, 7, 18))

    assert result["total_shares"].to_list() == [None, 200.0]
    assert result["float_shares"].to_list() == [None, 100.0]


def test_late_older_period_does_not_override_newer_share_event():
    rows = pl.DataFrame({
        "symbol": ["301491.SZ", "301491.SZ"],
        "date": [date(2025, 8, 28), date(2025, 8, 29)],
        "total_shares": [200.0, 200.0],
        "float_shares": [100.0, 100.0],
        "_instrument_as_of": [date(2026, 7, 18)] * 2,
    })
    shares = pl.DataFrame({
        "symbol": ["301491.SZ", "301491.SZ"],
        "period_end": ["2025-08-06", "2025-06-30"],
        "announce_date": ["2025-08-05", "2025-08-29"],
        "total_shares": [129.0, 96.75],
        "float_shares": [27.0, 0.0],
    })

    result = apply_point_in_time_shares(rows, shares, today=date(2026, 7, 18))

    assert result["total_shares"].to_list() == [129.0, 129.0]
    assert result["float_shares"].to_list() == [27.0, 27.0]


def test_enriched_storage_keeps_point_in_time_share_capital():
    frame = pl.DataFrame({
        "symbol": ["600000.SH"],
        "date": [date(2024, 6, 30)],
        "total_shares": [200.0],
        "float_shares": [100.0],
    })

    result = pipeline._select_storage_cols(frame)

    assert result.columns == ["symbol", "date", "total_shares", "float_shares"]


def test_data_status_includes_share_history(tmp_path):
    path = tmp_path / "financials" / "shares" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH", "000001.SZ"],
        "period_end": ["2023-12-31", "2024-06-30", "2024-06-30"],
    }).write_parquet(path)

    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    result = data_api._safe_aggregate_financials(repo)

    assert result is not None
    assert result["rows"] == 3
    assert result["tables"]["shares"] == {"rows": 3, "symbols": 2}
