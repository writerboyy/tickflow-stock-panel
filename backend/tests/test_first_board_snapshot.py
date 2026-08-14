from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace

import polars as pl

from app.free_strategy.first_board_snapshot import FirstBoardSnapshotCache


class _SnapshotRepo:
    def __init__(self, data_dir, *, include_minute: bool = True) -> None:
        self.store = SimpleNamespace(data_dir=data_dir)
        self.symbols = ["000001.SZ", "000002.SZ"]
        dates = []
        cursor = date(2024, 1, 1)
        while len(dates) < 46:
            if cursor.weekday() < 5:
                dates.append(cursor)
            cursor += timedelta(days=1)
        target = dates[-1]
        rows = []
        for symbol_index, symbol in enumerate(self.symbols):
            for index, day in enumerate(dates):
                close = 12 - index * 0.02
                previous = 12 - (index - 1) * 0.02 if index else close
                limit_price = round(previous * 1.1 + 1e-9, 2)
                rows.append({
                    "symbol": symbol,
                    "date": day,
                    "open": close,
                    "high": limit_price if day == target else close + 0.05,
                    "low": close - 0.05,
                    "close": close,
                    "volume": 20_000_000,
                    "amount": 200_000_000,
                    "raw_close": close,
                    "raw_high": limit_price if day == target else close + 0.05,
                    "raw_low": close - 0.05,
                    "total_shares": 2_000_000_000,
                })
        self.daily = pl.DataFrame(rows)
        coverage = data_dir / f"kline_minute/_coverage/date={target.isoformat()}.json"
        coverage.parent.mkdir(parents=True)
        coverage.write_text(json.dumps({
            "groups": [
                {"symbol": "000001.SZ", "bars": 240 if include_minute else 0},
                {"symbol": "000002.SZ", "bars": 240},
            ]
        }), encoding="utf-8")

    def get_instruments_asset(self, _asset):
        return pl.DataFrame([
            {
                "symbol": "000001.SZ", "name": "平安银行", "exchange": "SZ",
                "listing_date": date(1991, 4, 3), "delist_date": None,
            },
            {
                "symbol": "000002.SZ", "name": "ST历史样本", "exchange": "SZ",
                "listing_date": date(1991, 1, 29), "delist_date": None,
            },
        ])

    def get_daily_asset_batch(self, _asset, symbols, start, end, columns):
        return self.daily.filter(
            pl.col("symbol").is_in(symbols)
            & pl.col("date").is_between(start, end)
        ).select(columns)


def test_first_board_snapshot_uses_previous_trading_day_and_filters_st(tmp_path):
    repo = _SnapshotRepo(tmp_path)
    target = repo.daily["date"].max()
    cache = FirstBoardSnapshotCache(
        repo, target, target, {"lookback_days": 30},
    )

    snapshot = cache.snapshot(target)

    assert target.weekday() == 0
    assert snapshot["as_of"] == (target - timedelta(days=3)).isoformat()
    assert snapshot["scan_index_only"] == "daily_high_limit_touch"
    assert [row["symbol"] for row in snapshot["candidates"]] == ["000001.SZ"]
    candidate = snapshot["candidates"][0]
    assert candidate["ret5_d1"] < 0
    assert candidate["prior_limit_close_5d"] == 0
    assert candidate["market_cap_d1"] > 10_000_000_000


def test_first_board_snapshot_excludes_symbol_without_minute_rows(tmp_path):
    repo = _SnapshotRepo(tmp_path, include_minute=False)
    target = repo.daily["date"].max()

    cache = FirstBoardSnapshotCache(repo, target, target, {"lookback_days": 30})

    assert cache.snapshot(target)["candidates"] == []
