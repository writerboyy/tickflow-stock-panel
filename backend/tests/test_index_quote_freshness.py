from __future__ import annotations

from types import SimpleNamespace

import polars as pl

from app.api import intraday


class _Repo:
    def execute_all(self, sql, params):
        return [("000001.SH", "2026-07-27", 3858.24, 3903.12)]


class _QuoteService:
    def __init__(self, fresh: bool) -> None:
        self.fresh = fresh
        self.calls = 0

    def has_fresh_index_quotes(self) -> bool:
        return self.fresh

    def get_index_quotes(self, symbols=None) -> pl.DataFrame:
        self.calls += 1
        return pl.DataFrame({
            "symbol": ["000001.SH"],
            "name": ["上证指数"],
            "last_price": [3888.0],
            "close": [3888.0],
            "prev_close": [3858.24],
            "change_pct": [0.77],
        })


def _request(qs):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(quote_service=qs, repo=_Repo())))


def test_index_quotes_use_fresh_realtime_cache():
    qs = _QuoteService(fresh=True)

    res = intraday.index_quotes(_request(qs), symbols="000001.SH")

    assert res["source"] == "realtime"
    assert qs.calls == 1
    assert res["rows"][0]["last_price"] == 3888.0


def test_index_quotes_fall_back_when_realtime_cache_is_stale():
    qs = _QuoteService(fresh=False)

    res = intraday.index_quotes(_request(qs), symbols="000001.SH")

    assert res["source"] == "index_daily"
    assert qs.calls == 0
    assert res["rows"][0]["last_price"] == 3858.24
