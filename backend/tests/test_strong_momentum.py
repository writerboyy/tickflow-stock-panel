from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import polars as pl

from app.free_strategy.bars import Bar
from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine, Quote
from app.free_strategy.strong_momentum import _passes_intraday_gate
from app.free_strategy.strong_momentum_snapshot import (
    StrongMomentumSnapshotCache,
    _filter_source_candidates,
    _with_candidate_features,
)
from app.free_strategy.templates import TEMPLATES


SYMBOL = "000001.SZ"


def _snapshot(day: date) -> dict:
    return {
        "date": day.isoformat(),
        "as_of": (day - timedelta(days=1)).isoformat(),
        "selection_mode": "strict",
        "candidates": [{
            "symbol": SYMBOL,
            "name": "平安银行",
            "previous_raw_close": 10.0,
            "limit_price": 11.0,
            "previous_change": 0.08,
            "previous_ret5": 0.20,
            "previous_ret20": 0.30,
            "previous_amplitude": 0.10,
            "previous_turnover_rate": 8.0,
            "previous_volume_growth": 1.3,
            "previous_limit_up": False,
            "previous_high_volume_limit": False,
            "tail_gain_d1": 0.01,
        }],
    }


def _config() -> FreeStrategyConfig:
    raw = dict(TEMPLATES["strong_momentum"]["config"])
    raw.pop("timeframe")
    raw.pop("asset_type")
    return FreeStrategyConfig(**raw)


def _engine() -> FreeStrategyEngine:
    engine = FreeStrategyEngine(
        TEMPLATES["strong_momentum"]["source"],
        timeframe="1m",
        config=_config(),
        instruments=[{
            "symbol": SYMBOL,
            "name": "平安银行",
            "asset_type": "stock",
            "has_minute": True,
        }],
    )
    engine.set_strong_momentum_snapshot_loader(_snapshot)
    return engine


def _bar(
    day: date,
    hour: int,
    minute: int,
    *,
    open_price: float,
    close: float,
    high: float | None = None,
    low: float | None = None,
    limit_up: float = 11.0,
    limit_down: float = 9.0,
) -> Bar:
    high = max(open_price, close) if high is None else high
    low = min(open_price, close) if low is None else low
    return Bar(
        SYMBOL,
        datetime(day.year, day.month, day.day, hour, minute),
        open_price,
        high,
        low,
        close,
        volume=10_000,
        amount=100_000,
        raw_open=open_price,
        raw_high=high,
        raw_low=low,
        raw_close=close,
        limit_up=limit_up,
        limit_down=limit_down,
    )


def test_strong_momentum_template_uses_minute_morning_contract():
    template = TEMPLATES["strong_momentum"]

    assert template["name"] == "强者恒强·项目适配"
    assert template["config"]["timeframe"] == "1m"
    assert template["config"]["asset_type"] == "stock"
    assert template["config"]["settlement"] == "t1"
    assert template["config"]["fill_policy"] == "close"
    assert template["config"]["benchmark_symbol"] == "000300.SH"
    assert "jqdata" not in template["source"]
    assert "context.require_strong_momentum_snapshot" in template["source"]
    assert "09:30:16" in template["source"]

    engine = _engine()
    assert engine.scheduled_times == [
        "09:30:16", "09:31:00", "09:32:00", "09:37:00", "10:29:00",
    ]
    assert engine.second_precision_schedules == engine.scheduled_times


def test_candidate_features_shift_all_selection_inputs_to_d1():
    start = date(2025, 12, 1)
    rows = []
    for offset in range(22):
        close = 10.0 + offset * 0.1
        rows.append({
            "symbol": SYMBOL,
            "date": start + timedelta(days=offset),
            "open": close,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "volume": 1_000 + offset * 100,
            "amount": 1_000_000.0,
            "raw_close": close,
            "raw_high": close * 1.02,
            "raw_low": close * 0.98,
            "turnover_rate": 5.0 + offset * 0.1,
            "pit_name": "平安银行",
        })
    base = pl.DataFrame(rows)
    changed = base.with_columns(
        pl.when(pl.col("date") == rows[-1]["date"])
        .then(pl.lit(99.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )

    original = _with_candidate_features(base).row(-1, named=True)
    modified = _with_candidate_features(changed).row(-1, named=True)

    for field in (
        "previous_raw_close",
        "previous_change",
        "previous_ret5",
        "previous_ret20",
        "previous_turnover_rate",
        "previous_volume_growth",
        "recent_limit_down_count",
    ):
        assert modified[field] == original[field]


def test_source_candidate_filters_use_gem_thresholds():
    frame = pl.DataFrame({
        "symbol": ["600001.SH", "300001.SZ"],
        "previous_change": [0.08, 0.08],
        "previous_ret3": [0.70, 0.70],
        "previous_ret5": [0.70, 0.70],
        "previous_ret20": [0.30, 0.30],
        "previous_amplitude": [0.18, 0.18],
        "previous_turnover_rate": [27.0, 27.0],
        "recent_limit_down_count": [0, 0],
        "previous_shrink_rise_3d": [False, False],
    })

    selected = _filter_source_candidates(frame)

    assert selected["symbol"].to_list() == ["300001.SZ"]


def test_source_candidate_filters_leave_five_percent_fallback_available():
    frame = pl.DataFrame({
        "symbol": ["600001.SH"],
        "previous_change": [0.055],
        "previous_ret3": [0.10],
        "previous_ret5": [0.10],
        "previous_ret20": [0.30],
        "previous_amplitude": [0.10],
        "previous_turnover_rate": [5.0],
        "recent_limit_down_count": [0],
        "previous_shrink_rise_3d": [False],
    })

    assert _filter_source_candidates(frame)["symbol"].to_list() == ["600001.SH"]


def test_snapshot_does_not_drop_candidates_for_minute_coverage(tmp_path):
    target = date(2026, 8, 20)
    dates = [target - timedelta(days=offset) for offset in range(21, -1, -1)]
    closes = [10.0] * len(dates)
    closes[-2] = 10.8
    daily = pl.DataFrame({
        "symbol": ["600001.SH"] * len(dates),
        "date": dates,
        "open": closes,
        "high": [value * 1.02 for value in closes],
        "low": [value * 0.98 for value in closes],
        "close": closes,
        "volume": [1_000.0] * len(dates),
        "amount": [10_000.0] * len(dates),
        "raw_close": closes,
        "raw_high": [value * 1.02 for value in closes],
        "raw_low": [value * 0.98 for value in closes],
        "turnover_rate": [5.0] * len(dates),
    })

    class Repo:
        store = SimpleNamespace(data_dir=tmp_path)

        def get_instruments_asset(self, _asset_type):
            return pl.DataFrame({
                "symbol": ["600001.SH"],
                "name": ["测试股票"],
                "listing_date": [date(2020, 1, 1)],
            })

        def get_daily_asset_batch(self, *_args):
            return daily

        def get_minute_range(self, *_args, **_kwargs):
            return pl.DataFrame()

    coverage = tmp_path / "kline_minute" / "_coverage"
    coverage.mkdir(parents=True)
    (coverage / f"date={target.isoformat()}.json").write_text(
        '{"groups": []}',
        encoding="utf-8",
    )

    cache = StrongMomentumSnapshotCache(
        Repo(), target, target, {"lookback_days": 30, "require_auction": False},
    )

    assert [row["symbol"] for row in cache.snapshot(target)["candidates"]] == ["600001.SH"]


def test_entry_fills_at_first_supported_morning_minute_and_never_at_close():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.2, close=10.2),
        _bar(day, 9, 31, open_price=10.2, close=10.3),
        _bar(day, 15, 0, open_price=10.5, close=10.5),
    ])

    assert len(result["fills"]) == 1
    assert result["fills"][0]["side"] == "buy"
    assert result["fills"][0]["timestamp"] == "2026-01-05T09:31:00"
    assert all(not fill["timestamp"].endswith("T15:00:00") for fill in result["fills"] if fill["side"] == "buy")
    assert result["strategy_signals"][0]["signal_type"] == "strong_momentum_entry"


def test_entry_ignores_unsupported_minute_and_can_buy_at_0937():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.2, close=10.2),
        _bar(day, 9, 33, open_price=10.3, close=10.4),
        _bar(day, 9, 37, open_price=10.4, close=10.5),
    ])

    assert [fill["timestamp"] for fill in result["fills"]] == ["2026-01-05T09:37:00"]


def test_second_level_entry_matches_reference_symbols_and_lots():
    day = date(2026, 8, 20)
    metas = {
        symbol: {
            "symbol": symbol,
            "name": name,
            "previous_raw_close": previous,
            "previous_change": 0.07,
            "previous_turnover_rate": 5.0,
            "previous_volume_growth": 1.2,
            "previous_limit_up": False,
            "previous_high_volume_limit": False,
            "auction_required": False,
        }
        for symbol, name, previous in (
            ("600127.SH", "金健米业", 8.14),
            ("603207.SH", "小方制药", 25.33),
            ("600610.SH", "中毅达", 7.80),
        )
    }
    snapshot = {
        "date": day.isoformat(),
        "as_of": "2026-08-19",
        "candidates": list(metas.values()),
    }
    engine = FreeStrategyEngine(
        TEMPLATES["strong_momentum"]["source"],
        timeframe="1m",
        config=_config(),
        instruments=[
            {"symbol": symbol, "name": meta["name"], "asset_type": "stock", "has_minute": True}
            for symbol, meta in metas.items()
        ],
    )
    engine.set_strong_momentum_snapshot_loader(lambda _day: snapshot)
    timestamp = datetime(day.year, day.month, day.day, 9, 30, 16)
    engine.advance_event(
        timestamp,
        event_type="quote",
        quotes=[
            Quote("600127.SH", timestamp, 8.30, prev_close=8.14, open=8.14, high=8.30, low=8.14, volume=1000, limit_up=8.60),
            Quote("603207.SH", timestamp, 25.33, prev_close=25.33, open=25.33, high=25.33, low=25.33, volume=1000, limit_up=27.86),
            Quote("600610.SH", timestamp, 7.95, prev_close=7.80, open=7.80, high=7.95, low=7.80, volume=1000, limit_up=8.58),
        ],
    )

    fills = [fill for fill in engine.account.fills if fill.side == "buy"]
    assert [(fill.timestamp, fill.symbol, fill.quantity) for fill in fills] == [
        ("2026-08-20T09:30:16", "600127.SH", 4000),
        ("2026-08-20T09:30:16", "600610.SH", 4200),
        ("2026-08-20T09:30:16", "603207.SH", 1300),
    ]


def test_entry_accepts_latest_trade_before_explicit_zero_second_callback():
    day = date(2026, 8, 20)
    engine = _engine()
    engine.begin_session(day)
    engine.advance_event(
        datetime(day.year, day.month, day.day, 9, 31),
        [Bar(
            SYMBOL,
            datetime(day.year, day.month, day.day, 9, 30, 59),
            10.0, 10.2, 10.0, 10.2,
            volume=10_000,
            amount=102_000,
            raw_open=10.0,
            raw_high=10.2,
            raw_low=10.0,
            raw_close=10.2,
            limit_up=11.0,
            limit_down=9.0,
        )],
        event_type="scheduled",
        scheduled_at="09:31:00",
    )

    assert engine.context.now == datetime(day.year, day.month, day.day, 9, 31)
    assert engine.context.current_bars()[SYMBOL].timestamp == datetime(
        day.year, day.month, day.day, 9, 30, 59,
    )


def test_auction_gate_rejects_open_above_eight_percent():
    day = date(2026, 1, 5)
    result = _engine().run([
        _bar(day, 9, 30, open_price=10.9, close=10.9),
        _bar(day, 9, 31, open_price=10.9, close=10.8),
        _bar(day, 9, 32, open_price=10.8, close=10.7),
        _bar(day, 9, 37, open_price=10.7, close=10.6),
        _bar(day, 10, 29, open_price=10.6, close=10.5),
    ])

    assert result["fills"] == []
    assert result["strategy_signals"] == []


def test_strong_momentum_requires_direct_bid_detail_when_enabled():
    state = {"session_open": {SYMBOL: 10.2}}
    meta = {
        "previous_raw_close": 10.0,
        "auction_required": True,
        "auction_change_pct_0925": 8.1,
    }
    bar = _bar(date(2026, 1, 5), 9, 31, open_price=10.2, close=10.3)
    assert _passes_intraday_gate(state, SYMBOL, meta, bar) is None

    meta["auction_change_pct_0925"] = 5.0
    assert _passes_intraday_gate(state, SYMBOL, meta, bar) is not None


def test_strong_momentum_bid_symbols_uses_static_candidates_before_auction(
    monkeypatch,
):
    import app.free_strategy.strong_momentum_snapshot as snapshot

    class FakeCache:
        def __init__(self, *_args, **_kwargs):
            pass

        def snapshot(self, _day):
            return {
                "static_candidates": [
                    {"symbol": "600001.SH"},
                    {"symbol": "000001.SZ"},
                ],
                "candidates": [],
            }

    monkeypatch.setattr(snapshot, "StrongMomentumSnapshotCache", FakeCache)
    assert snapshot.strong_momentum_bid_symbols(object(), date(2026, 8, 20)) == [
        "000001.SZ",
        "600001.SH",
    ]


def test_strong_momentum_auction_rows_ignore_unrelated_component_failure(
    tmp_path, monkeypatch
):
    import json
    import app.free_strategy.strong_momentum_snapshot as snapshot

    day = date(2026, 8, 20)
    manifest_dir = tmp_path / "ext_data" / "_ingestion" / "kaipanla" / "auction_completion"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / f"{day.isoformat()}.json").write_text(
        json.dumps({
            "status": "incomplete",
            "expected_components": ["four_mode_bid_detail", "strong_momentum_bid_detail"],
            "components": {
                "four_mode_bid_detail": {"status": "source_error", "rows": 0},
                "strong_momentum_bid_detail": {"status": "complete", "rows": 1},
            },
        }),
        encoding="utf-8",
    )
    partition = tmp_path / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
    partition.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600001.SH"],
        "source_0925": ["/31"],
        "auction_change_pct_0925": [5.0],
    }).write_parquet(partition / "part.parquet")

    class Store:
        data_dir = tmp_path

    class Repo:
        store = Store()

    monkeypatch.setattr(snapshot, "cn_today", lambda: day)
    cache = object.__new__(snapshot.StrongMomentumSnapshotCache)
    cache.repo = Repo()
    cache.requirement = {"require_auction": True}
    rows, gaps = cache._auction_rows(day)
    assert gaps == []
    assert rows["600001.SH"]["auction_change_pct_0925"] == 5.0


def test_strong_momentum_snapshot_refreshes_after_auction_manifest_changes(
    monkeypatch,
):
    import app.free_strategy.strong_momentum_snapshot as snapshot

    day = date(2026, 8, 20)
    cache = object.__new__(snapshot.StrongMomentumSnapshotCache)
    cache.requirement = {"require_auction": True}
    cache._snapshots = {day: {"state": "waiting_data", "candidates": []}}
    cache._auction_signatures = {day: ("before",)}
    monkeypatch.setattr(cache, "_auction_signature", lambda _day: ("after",))

    def rebuild():
        cache._snapshots[day] = {"state": "ready", "candidates": []}
        cache._auction_signatures[day] = ("after",)

    monkeypatch.setattr(cache, "_build", rebuild)
    assert cache.snapshot(day)["state"] == "ready"


def test_strong_momentum_bootstrap_uses_static_candidates_while_waiting():
    from app.free_strategy.strong_momentum_snapshot import StrongMomentumSnapshotCache

    cache = object.__new__(StrongMomentumSnapshotCache)
    cache._snapshots = {
        date(2026, 8, 20): {
            "static_candidates": [{"symbol": "600001.SH"}],
            "candidates": [],
        }
    }
    assert cache.bootstrap_symbols == ["600001.SH"]


def test_t1_blocks_same_day_stop_and_allows_next_day_exit():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.2, close=10.2),
        _bar(first, 9, 31, open_price=10.2, close=10.3),
        _bar(first, 10, 20, open_price=10.0, close=9.7),
        _bar(first, 15, 0, open_price=9.8, close=10.0),
        _bar(second, 9, 30, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(second, 9, 31, open_price=10.0, close=9.5, limit_up=11.0),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-06T09:31:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "成本止损-4%"


def test_limit_touch_then_drawdown_exits_after_1020():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.2, close=10.2),
        _bar(first, 9, 31, open_price=10.2, close=10.3),
        _bar(first, 15, 0, open_price=10.3, close=10.4),
        _bar(second, 9, 30, open_price=10.95, close=10.95),
        _bar(second, 10, 20, open_price=10.9, close=11.0, high=11.0),
        _bar(second, 10, 21, open_price=10.9, close=10.8, high=10.9),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-06T10:21:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "涨停后回落1.5%"


def test_take_profit_exits_on_a_later_trading_day():
    first = date(2026, 1, 5)
    second = date(2026, 1, 6)
    engine = _engine()
    engine.set_trading_calendar([first, second])
    result = engine.run([
        _bar(first, 9, 30, open_price=10.0, close=10.0),
        _bar(first, 9, 31, open_price=10.0, close=10.0),
        _bar(first, 15, 0, open_price=10.9, close=11.0),
        _bar(second, 9, 30, open_price=11.55, close=11.55, limit_up=12.1),
        _bar(second, 9, 31, open_price=11.7, close=12.0, limit_up=12.1),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "止盈19%"


def test_position_exits_after_three_future_sessions():
    days = [date(2026, 1, day) for day in (5, 6, 7, 8)]
    engine = _engine()
    engine.set_trading_calendar(days)
    result = engine.run([
        _bar(days[0], 9, 30, open_price=10.0, close=10.0),
        _bar(days[0], 9, 31, open_price=10.0, close=10.0),
        _bar(days[0], 15, 0, open_price=10.0, close=10.0),
        _bar(days[1], 9, 30, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(days[1], 15, 0, open_price=10.5, close=10.5, limit_up=11.0),
        _bar(days[2], 9, 30, open_price=11.025, close=11.0, limit_up=11.55),
        _bar(days[2], 15, 0, open_price=11.0, close=11.0, limit_up=11.55),
        _bar(days[3], 9, 30, open_price=11.55, close=11.55, limit_up=12.1),
    ])

    assert [fill["side"] for fill in result["fills"]] == ["buy", "sell"]
    assert result["fills"][1]["timestamp"] == "2026-01-08T09:30:00"
    exits = [event for event in result["strategy_signals"] if event["signal_type"] == "strong_momentum_exit"]
    assert exits[0]["payload"]["reason"] == "持有满3个交易日"
