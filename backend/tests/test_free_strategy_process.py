from __future__ import annotations

import queue
from datetime import datetime

import polars as pl

from app.free_strategy.process import execute_backtest


def test_minute_backtest_records_exact_data_coverage(monkeypatch, tmp_path):
    class FakeRepository:
        def __init__(self, _store):
            pass

        def get_minute_range(self, symbols, start, _end, _asset_type):
            timestamps = [datetime.combine(start, datetime.min.time()).replace(hour=9, minute=30)] * len(symbols)
            return pl.DataFrame({
                "symbol": symbols,
                "datetime": timestamps,
                "open": [1.0] * len(symbols),
                "high": [1.0] * len(symbols),
                "low": [1.0] * len(symbols),
                "close": [1.0] * len(symbols),
                "volume": [100.0] * len(symbols),
                "amount": [100.0] * len(symbols),
            })

    monkeypatch.setattr("app.tickflow.repository.DataStore", lambda path: path)
    monkeypatch.setattr("app.tickflow.repository.KlineRepository", FakeRepository)
    output: queue.SimpleQueue = queue.SimpleQueue()
    execute_backtest({
        "data_dir": str(tmp_path),
        "source": "def on_bar(context, bars):\n    pass\n",
        "strategy_id": "coverage-test",
        "strategy_name": "覆盖测试",
        "source_revision": 1,
        "symbols": ["510300.SH", "511880.SH"],
        "timeframe": "1m",
        "asset_type": "etf",
        "start": "2024-01-02",
        "end": "2024-01-03",
        "config": {"asset_type": "etf"},
        "data_provider": "tickflow",
    }, output)

    event = output.get()
    while event["type"] == "progress":
        event = output.get()

    assert event["type"] == "result"
    coverage = event["result"]["metadata"]["data_coverage"]
    assert coverage == {
        "rows": 4,
        "first_bar": "2024-01-02T09:30:00",
        "last_bar": "2024-01-03T09:30:00",
        "trading_days": 2,
        "requested_symbols": ["510300.SH", "511880.SH"],
        "seen_symbols": ["510300.SH", "511880.SH"],
        "missing_symbols": [],
        "configured_provider": "tickflow",
        "storage": "kline_etf_minute",
    }
