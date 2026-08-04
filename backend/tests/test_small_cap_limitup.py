from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.free_strategy.bars import Bar
from app.free_strategy.small_cap_limitup import (
    INITIAL_POOL_SIZE,
    ST_STATUS_DAYS,
    _afternoon_selection_symbols,
    _eligible_market_records,
    _history_limit_flags,
    _is_historical_st,
    _is_weekly_rebalance_day,
    _get_stock_list,
    _select_industries,
    _turnover_sell_symbols,
)


def test_known_limit_price_prevents_false_limit_up_inference():
    bar = Bar(
        "000001.SZ",
        datetime(2025, 7, 28, 15),
        10,
        10.5,
        10,
        10.5,
        previous_close=10,
        limit_up=11,
    )

    assert _history_limit_flags([bar]) == [False]


def test_missing_limit_price_falls_back_to_board_rate_inference():
    bar = Bar(
        "000001.SZ",
        datetime(2025, 7, 28, 15),
        10,
        10.5,
        10,
        10.5,
        previous_close=10,
    )

    assert _history_limit_flags([bar]) == [True]


def test_consecutive_five_pct_limits_enable_historical_st_limit_prices():
    bars = [
        Bar(
            "X", datetime(2025, 3, 4, 15), 10, 10.5, 10, 10.5,
            raw_high=10.5, raw_low=10, raw_close=10.5,
            previous_close=10, limit_up=11,
        ),
        Bar(
            "X", datetime(2025, 3, 5, 15), 10.5, 11.03, 10.5, 11.03,
            raw_high=11.03, raw_low=10.5, raw_close=11.03,
            previous_close=10.5, limit_up=11.55,
        ),
    ]

    assert _history_limit_flags(bars) == [True, True]


def test_historical_st_limit_inference_ends_after_normal_limit_regime():
    bars = [
        Bar(
            "X", datetime(2025, 3, 3, 15), 10, 10.5, 10, 10.5,
            raw_high=10.5, raw_low=10, raw_close=10.5,
            previous_close=10, limit_up=11,
        ),
        Bar(
            "X", datetime(2025, 3, 4, 15), 10.5, 11.03, 10.5, 11.03,
            raw_high=11.03, raw_low=10.5, raw_close=11.03,
            previous_close=10.5, limit_up=11.55,
        ),
        Bar(
            "X", datetime(2025, 3, 5, 15), 11.03, 11.8, 11.0, 11.7,
            raw_high=11.8, raw_low=11.0, raw_close=11.7,
            previous_close=11.03, limit_up=12.13,
        ),
        Bar(
            "X", datetime(2025, 3, 6, 15), 11.7, 12.29, 11.7, 12.29,
            raw_high=12.29, raw_low=11.7, raw_close=12.29,
            previous_close=11.7, limit_up=12.87,
        ),
    ]

    assert _history_limit_flags(bars) == [True, True, False, False]


def test_authoritative_name_history_disables_inferred_st_limit_prices():
    bars = [
        Bar(
            "X", datetime(2025, 3, 4, 15), 10, 10.5, 10, 10.5,
            raw_high=10.5, raw_low=10, raw_close=10.5,
            previous_close=10, limit_up=11,
        ),
        Bar(
            "X", datetime(2025, 3, 5, 15), 10.5, 11.03, 10.5, 11.03,
            raw_high=11.03, raw_low=10.5, raw_close=11.03,
            previous_close=10.5, limit_up=11.55,
        ),
    ]

    assert _history_limit_flags(bars, infer_historical_st=False) == [False, False]


def test_historical_st_status_uses_latest_observed_price_limit_regime():
    bars = [
        Bar(
            "X", datetime(2025, 3, 3, 15), 10, 10, 10, 10,
            raw_high=10, raw_low=10, raw_close=10, previous_close=9.8,
        ),
        Bar(
            "X", datetime(2025, 3, 4, 15), 10, 10.5, 9.9, 10.5,
            raw_high=10.5, raw_low=9.9, raw_close=10.5, previous_close=10,
        ),
        Bar(
            "X", datetime(2025, 3, 5, 15), 10.5, 11.03, 10.4, 11.03,
            raw_high=11.03, raw_low=10.4, raw_close=11.03, previous_close=10.5,
        ),
        Bar(
            "X", datetime(2025, 3, 6, 15), 11.03, 12.2, 10.9, 11.5,
            raw_high=12.2, raw_low=10.9, raw_close=11.5, previous_close=11.03,
        ),
    ]

    assert _is_historical_st(bars[:3]) is True
    assert _is_historical_st(bars) is False


def test_historical_st_status_detects_quiet_regime_after_annual_review():
    normal = Bar(
        "X", datetime(2025, 4, 25, 15), 10, 11, 10, 10.2,
        raw_high=11, raw_low=10, raw_close=10.2, previous_close=10,
    )
    quiet = [
        Bar(
            "X", datetime(2025, 4, 25, 15) + timedelta(days=index + 1),
            10, 10.2, 9.8, 10,
            raw_high=10.2, raw_low=9.8, raw_close=10, previous_close=10,
        )
        for index in range(40)
    ]

    assert _is_historical_st([normal, *quiet]) is True


def test_turnover_uses_current_session_volume_without_minute_history_scan():
    current = SimpleNamespace(
        close=10,
        raw_close=10,
        limit_up=11,
        float_shares=1_000_000,
        session_volume=2_000,
    )
    daily = {
        "X": [SimpleNamespace(volume=500) for _ in range(20)],
    }
    context = SimpleNamespace(
        now=datetime(2025, 7, 28, 14, 20),
        portfolio=SimpleNamespace(
            positions={"X": 100},
            available_positions={"X": 100},
        ),
        current_bars=lambda: {"X": current},
        history_batch=lambda *_args, **_kwargs: daily,
        history_bars=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("session volume must avoid minute history reads")
        ),
    )

    assert _turnover_sell_symbols(context, ["X"]) == ["X"]


def test_weekly_rebalance_uses_second_trading_day_after_holidays():
    context = SimpleNamespace(
        now=datetime(2025, 10, 10, 10, 15),
        state={"small_cap_limitup": {}},
        instruments=lambda _asset: [{"symbol": "X"}],
        history_batch=lambda symbols, **_kwargs: {symbols[0]: [
            SimpleNamespace(date=date(2025, 9, 30)),
            SimpleNamespace(date=date(2025, 10, 9)),
        ]},
    )

    assert _is_weekly_rebalance_day(context) is True

    context.now = datetime(2026, 5, 7, 10, 15)
    context.history_batch = lambda symbols, **_kwargs: {symbols[0]: [
        SimpleNamespace(date=date(2026, 4, 30)),
        SimpleNamespace(date=date(2026, 5, 6)),
    ]}

    assert _is_weekly_rebalance_day(context) is True


def test_weekly_rebalance_rejects_third_trading_day():
    context = SimpleNamespace(
        now=datetime(2025, 10, 15, 10, 15),
        state={"small_cap_limitup": {}},
        instruments=lambda _asset: [{"symbol": "X"}],
        history_batch=lambda symbols, **_kwargs: {symbols[0]: [
            SimpleNamespace(date=date(2025, 10, 13)),
            SimpleNamespace(date=date(2025, 10, 14)),
        ]},
    )

    assert _is_weekly_rebalance_day(context) is False


def test_market_candidates_only_load_st_history_until_pool_is_full():
    records = [
        {
            "symbol": f"{index:06d}.SZ",
            "name": f"company-{index}",
            "listing_date": datetime(2020, 1, 1).date(),
        }
        for index in range(INITIAL_POOL_SIZE * 2)
    ]
    bars = {
        item["symbol"]: SimpleNamespace(
            close=10,
            raw_close=10,
            previous_close=10,
            total_shares=index + 1,
            tradable=True,
            suspended=False,
            limit_up=11,
            limit_down=9,
        )
        for index, item in enumerate(records)
    }
    history_calls = []
    context = SimpleNamespace(
        now=datetime(2025, 7, 29, 10, 15),
        state={"small_cap_limitup": {"loss_black": {}}},
        portfolio=SimpleNamespace(positions={}),
        instruments=lambda _asset: records,
        current_bars=lambda: bars,
        history_bars=lambda *_args, **_kwargs: [
            SimpleNamespace(date=datetime(2025, 7, 28).date()),
        ],
        history_batch=lambda symbols, **_kwargs: history_calls.append(list(symbols)) or {
            symbol: [] for symbol in symbols
        },
    )

    result, _bars = _eligible_market_records(context)

    assert len(result) == INITIAL_POOL_SIZE
    assert len(history_calls) == 1
    assert len(history_calls[0]) == INITIAL_POOL_SIZE


def test_market_candidates_do_not_let_holdings_bypass_selection_pool():
    records = [
        {
            "symbol": symbol,
            "name": symbol,
            "listing_date": date(2020, 1, 1),
        }
        for symbol in ("POOL", "HELD")
    ]
    bars = {
        symbol: SimpleNamespace(
            close=10,
            raw_close=10,
            previous_close=10,
            total_shares=shares,
            tradable=True,
            suspended=False,
            limit_up=11,
            limit_down=9,
        )
        for symbol, shares in (("POOL", 2), ("HELD", 1))
    }
    context = SimpleNamespace(
        now=datetime(2025, 10, 14, 10, 15),
        state={"small_cap_limitup": {
            "loss_black": {},
            "selection_scope_key": ("2025-10-13", ()),
            "selection_scope_symbols": ["POOL"],
        }},
        portfolio=SimpleNamespace(positions={"HELD": 100}),
        instruments=lambda _asset: records,
        current_bars=lambda: bars,
        history_bars=lambda *_args, **_kwargs: [
            SimpleNamespace(date=date(2025, 10, 13)),
        ],
        history_batch=lambda symbols, **_kwargs: {symbol: [] for symbol in symbols},
    )

    result, _bars = _eligible_market_records(context)

    assert [item["symbol"] for item in result] == ["POOL"]


def test_market_candidates_use_name_valid_on_historical_date():
    records = [
        {
            "symbol": "002207.SZ",
            "name": "*ST准油",
            "listing_date": datetime(2008, 1, 28).date(),
            "name_changes": [{
                "date": "2026-04-29",
                "before": "准油股份",
                "after": "*ST准油",
            }],
        },
        {
            "symbol": "300477.SZ",
            "name": "ST合纵",
            "listing_date": datetime(2015, 6, 10).date(),
            "name_changes": [{
                "date": "2025-04-30",
                "before": "合纵科技",
                "after": "ST合纵",
            }],
        },
    ]
    bars = {
        item["symbol"]: SimpleNamespace(
            close=10,
            raw_close=10,
            previous_close=10,
            total_shares=100_000_000,
            tradable=True,
            suspended=False,
            limit_up=11,
            limit_down=9,
        )
        for item in records
    }
    context = SimpleNamespace(
        now=datetime(2025, 8, 18, 14, 20),
        state={"small_cap_limitup": {"loss_black": {}}},
        portfolio=SimpleNamespace(positions={}),
        instruments=lambda _asset: records,
        current_bars=lambda: bars,
        history_bars=lambda *_args, **_kwargs: [
            SimpleNamespace(date=datetime(2025, 8, 15).date()),
        ],
        history_batch=lambda symbols, **_kwargs: {symbol: [] for symbol in symbols},
    )

    result, _bars = _eligible_market_records(context)

    assert [item["symbol"] for item in result] == ["002207.SZ"]


def test_market_candidates_trust_historical_name_over_price_limit_inference():
    record = {
        "symbol": "000056.SZ",
        "name": "*ST皇庭",
        "listing_date": date(1996, 7, 8),
        "name_changes": [{
            "date": "2026-04-27",
            "before": "皇庭国际",
            "after": "*ST皇庭",
        }],
    }
    current = SimpleNamespace(
        close=2.31,
        raw_close=2.31,
        previous_close=2.31,
        total_shares=1_182_528_220,
        tradable=True,
        suspended=False,
        limit_up=2.43,
        limit_down=2.19,
    )
    context = SimpleNamespace(
        now=datetime(2025, 11, 25, 10, 15),
        state={"small_cap_limitup": {"loss_black": {}}},
        portfolio=SimpleNamespace(positions={}),
        instruments=lambda _asset: [record],
        current_bars=lambda: {"000056.SZ": current},
        history_bars=lambda *_args, **_kwargs: [
            SimpleNamespace(date=date(2025, 11, 24)),
        ],
        history_batch=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("authoritative name history must avoid ST price inference")
        ),
    )

    result, _bars = _eligible_market_records(context)

    assert [item["symbol"] for item in result] == ["000056.SZ"]


def test_selection_pool_loads_st_history_only_for_symbols_without_name_history():
    records = [
        {
            "symbol": "000001.SZ",
            "name": "alpha",
            "listing_date": date(2020, 1, 1),
            "name_changes": [{
                "date": "2025-01-01",
                "before": "old-alpha",
                "after": "alpha",
            }],
        },
        {
            "symbol": "000002.SZ",
            "name": "beta",
            "listing_date": date(2020, 1, 1),
        },
    ]
    histories = {
        item["symbol"]: [SimpleNamespace(
            date=date(2025, 7, 28),
            close=10,
            raw_close=10,
            raw_high=10,
            raw_low=10,
            previous_close=10,
            total_shares=index + 1,
        )]
        for index, item in enumerate(records)
    }
    history_calls = []

    def history_batch(symbols, *, count, **_kwargs):
        history_calls.append((list(symbols), count))
        return {symbol: histories[symbol] for symbol in symbols}

    context = SimpleNamespace(
        now=datetime(2025, 7, 29, 14, 20),
        state={"small_cap_limitup": {
            "loss_black": {},
            "selection_scope_key": None,
            "selection_scope_symbols": [],
        }},
        portfolio=SimpleNamespace(positions={}),
        instruments=lambda _asset: records,
        history_bars=lambda symbol, **_kwargs: histories.get(symbol, []),
        history_batch=history_batch,
    )

    symbols = _afternoon_selection_symbols(context, context.now)

    assert symbols == ["000001.SZ", "000002.SZ"]
    assert history_calls == [
        (["000001.SZ", "000002.SZ"], 1),
        (["000002.SZ"], ST_STATUS_DAYS),
    ]


def test_afternoon_scope_uses_daily_market_cap_pool_instead_of_full_market_minutes():
    records = [
        {
            "symbol": f"{index:06d}.SZ",
            "name": f"company-{index}",
            "listing_date": datetime(2020, 1, 1).date(),
        }
        for index in range(INITIAL_POOL_SIZE + 2)
    ]
    histories = {
        item["symbol"]: [SimpleNamespace(
            date=datetime(2025, 7, 28).date(),
            close=10,
            raw_close=10,
            raw_high=10,
            raw_low=10,
            previous_close=10,
            total_shares=index + 1,
        )]
        for index, item in enumerate(records)
    }
    history_calls = []

    def history_batch(symbols, *, count, **_kwargs):
        history_calls.append((list(symbols), count))
        return {symbol: histories[symbol] for symbol in symbols}

    context = SimpleNamespace(
        now=datetime(2025, 7, 29, 14, 20),
        state={"small_cap_limitup": {
            "loss_black": {},
            "selection_scope_key": None,
            "selection_scope_symbols": [],
        }},
        portfolio=SimpleNamespace(positions={"HELD": 100}),
        instruments=lambda _asset: records,
        history_bars=lambda symbol, **_kwargs: histories.get(symbol, []),
        history_batch=history_batch,
    )

    symbols = _afternoon_selection_symbols(context, context.now)

    assert symbols[0] == "HELD"
    assert len(symbols) == INITIAL_POOL_SIZE + 1
    assert records[-1]["symbol"] not in symbols
    assert [count for _symbols, count in history_calls] == [1, ST_STATUS_DAYS]


def test_select_industries_deduplicates_with_easy_tdx_sw_code():
    records = [
        {"symbol": "A", "industry_sw": "X480101"},
        {"symbol": "B", "industry_sw": "X480101"},
        {"symbol": "C", "industry_sw": "X490101"},
    ]

    assert _select_industries(["A", "B", "C"], records) == ["A", "C"]


def test_select_industries_fails_closed_when_ranked_symbol_has_no_sw_industry():
    records = [
        {"symbol": "A", "industry_sw": "X480101"},
        {"symbol": "B", "industry_sw": ""},
    ]

    with pytest.raises(ValueError, match="EasyTDX 申万行业"):
        _select_industries(["A", "B"], records)


def test_stock_selection_is_not_computable_without_industry_snapshot():
    context = SimpleNamespace(
        state={"small_cap_limitup": {
            "stock_list_cache_date": None,
            "stock_list_cache": [],
        }},
        instruments=lambda _asset: [{"symbol": "000001.SZ"}],
    )

    with pytest.raises(ValueError, match="EasyTDX 申万行业快照"):
        _get_stock_list(context)


def test_empty_stock_list_cache_is_recomputed_for_same_trading_date(monkeypatch):
    context = SimpleNamespace(
        now=datetime(2026, 8, 4, 10, 30),
        state={"small_cap_limitup": {
            "stock_list_cache_date": "2026-08-03",
            "stock_list_cache": [],
        }},
        instruments=lambda _asset: [{"symbol": "A", "industry_sw": "I"}],
        history_batch=lambda *_args, **_kwargs: {"A": [SimpleNamespace()]},
        log=lambda _message: None,
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._previous_trading_date",
        lambda _context, _records: date(2026, 8, 3),
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._eligible_market_records",
        lambda _context: ([{"symbol": "A", "industry_sw": "I"}], {"A": object()}),
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._rank_history_candidates",
        lambda _history, _bars, _reliable: ["A"],
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._select_industries",
        lambda _ranked, _records: ["A"],
    )

    assert _get_stock_list(context) == ["A"]
    assert context.state["small_cap_limitup"]["stock_list_cache"] == ["A"]


def test_empty_selection_is_not_cached(monkeypatch):
    context = SimpleNamespace(
        now=datetime(2026, 8, 4, 10, 30),
        state={"small_cap_limitup": {
            "stock_list_cache_date": None,
            "stock_list_cache": [],
        }},
        instruments=lambda _asset: [{"symbol": "A", "industry_sw": "I"}],
        history_batch=lambda *_args, **_kwargs: {"A": [SimpleNamespace()]},
        log=lambda _message: None,
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._previous_trading_date",
        lambda _context, _records: date(2026, 8, 3),
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._eligible_market_records",
        lambda _context: ([{"symbol": "A", "industry_sw": "I"}], {"A": object()}),
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._rank_history_candidates",
        lambda _history, _bars, _reliable: [],
    )
    monkeypatch.setattr(
        "app.free_strategy.small_cap_limitup._select_industries",
        lambda _ranked, _records: [],
    )

    assert _get_stock_list(context) == []
    assert context.state["small_cap_limitup"]["stock_list_cache_date"] is None
