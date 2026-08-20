from datetime import date, datetime
from app.free_strategy import four_mode_snapshot as snapshot
from app.free_strategy.engine import FreeStrategyEngine
from app.free_strategy.templates import TEMPLATES


def test_four_mode_template_is_native_and_scheduled():
    template = TEMPLATES["four_mode"]
    assert template["config"]["asset_type"] == "stock"
    assert "jqdata" not in template["source"]
    engine = FreeStrategyEngine(
        template["source"],
        timeframe="1m",
        instruments=[{"symbol": "000001.SZ", "asset_type": "stock", "has_minute": True}],
    )
    assert engine.context.four_mode_snapshot_requirement == {
        "lookback_days": 80,
        "trend_history_days": 65,
        "index_symbol": "000852.SH",
        "require_auction": True,
    }
    assert [task.resolved_time for task in engine.context._scheduled] == [
        "09:05", "09:24", "09:25", "09:27", "09:28", "10:00",
        "11:30", "14:30", "15:01", "15:02", "15:05",
    ]


def test_trend_static_evaluation_does_not_consume_auction(monkeypatch):
    features = {
        "ma5": 12.0, "ma10": 11.0, "ma20": 10.0, "ma60": 9.0,
        "ma5_slope": 0.02, "ma10_slope": 0.02, "ma20_slope": 0.02,
        "volume_ratio": 2.0, "volume_ma_up": True, "close": 12.0,
        "upper_shadow_ratio": 0.1, "macd": 1.0, "rsi_14": 65.0,
    }
    monkeypatch.setattr(snapshot, "trend_features", lambda _rows: dict(features))
    monkeypatch.setattr(
        snapshot,
        "_combo_score",
        lambda _latest, _previous, combo: (combo == "combo_3", 95.0, 4, 5),
    )
    rows = [{"symbol": "000001.SZ", "close": 12.0}] * 65
    candidate, reason = snapshot.evaluate_trend(rows, None)
    assert reason is None
    assert candidate["combo_matches"][0]["combo_type"] == "combo_3"
    assert "open_chg" not in candidate
    assert snapshot.confirm_trend_candidate(candidate, 4.0)["combo_type"] == "combo_3"
    assert snapshot.confirm_trend_candidate(candidate, 2.0)["combo_type"] == "combo_6"


def test_snapshot_day_normalization_handles_datetime_index():
    class Repo:
        def get_daily_asset(self, *_args, **_kwargs):
            import polars as pl

            return pl.DataFrame({"date": [date(2026, 8, 20)]})

    cache = object.__new__(snapshot.FourModeSnapshotCache)
    cache.repo = Repo()
    cache.start = date(2026, 8, 20)
    cache.end = date(2026, 8, 20)
    cache.requirement = {"index_symbol": "000852.SH"}
    assert cache._trading_days() == [date(2026, 8, 20)]


def test_snapshot_build_filters_stock_symbols_without_polars_list_cast_error():
    import polars as pl

    class Store:
        data_dir = "/tmp/four-mode-test"

    class Repo:
        store = Store()

        def get_instruments_asset(self, asset_type):
            assert asset_type == "stock"
            return pl.DataFrame({"symbol": ["000001.SZ", "600000.SH", "430001.BJ"]})

        def get_daily_asset_batch(self, *_args, **_kwargs):
            return pl.DataFrame()

        def get_daily_asset(self, *_args, **_kwargs):
            return pl.DataFrame({"date": [date(2026, 8, 20)]})

    cache = snapshot.FourModeSnapshotCache(
        Repo(), date(2026, 8, 20), date(2026, 8, 20), {"index_symbol": "000852.SH"}
    )
    assert cache.all_symbols == ["000001.SZ", "600000.SH"]


def test_valid_empty_auction_manifest_is_not_a_storage_gap():
    manifest = {
        "status": "complete",
        "expected_components": ["0915", "0920", "0925", "bid_detail"],
        "components": {
            name: {"status": "valid_empty", "rows": 0}
            for name in ("0915", "0920", "0925")
        } | {"bid_detail": {"status": "not_applicable", "rows": 0}},
    }
    assert snapshot._auction_manifest_is_valid_empty(manifest)


def test_four_mode_auction_ignores_unrelated_strong_momentum_failure(tmp_path):
    import json
    import polars as pl

    day = date(2026, 8, 20)
    manifest_dir = tmp_path / "ext_data" / "_ingestion" / "kaipanla" / "auction_completion"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / f"{day.isoformat()}.json").write_text(
        json.dumps({
            "status": "incomplete",
            "expected_components": [
                "0915",
                "0920",
                "0925",
                "bid_detail",
                "four_mode_bid_detail",
                "strong_momentum_bid_detail",
            ],
            "components": {
                "0915": {"status": "valid_empty", "rows": 0},
                "0920": {"status": "valid_empty", "rows": 0},
                "0925": {"status": "published", "rows": 1},
                "bid_detail": {"status": "not_applicable", "rows": 0},
                "four_mode_bid_detail": {"status": "complete", "rows": 1},
                "strong_momentum_bid_detail": {"status": "source_error", "rows": 0},
            },
        }),
        encoding="utf-8",
    )
    partition = tmp_path / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
    partition.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"], "source_0925": ["/31"]}).write_parquet(
        partition / "part.parquet"
    )

    class Store:
        data_dir = tmp_path

    class Repo:
        store = Store()

    cache = object.__new__(snapshot.FourModeSnapshotCache)
    cache.repo = Repo()
    rows, gaps, _manifest = cache._auction(day)
    assert gaps == []
    assert rows["000001.SZ"]["source_0925"] == "/31"


def test_four_mode_bid_symbols_only_include_weak_reversal_and_trend(monkeypatch):
    class FakeCache:
        def __init__(self, *_args, **_kwargs):
            pass

        def snapshot(self, _day):
            return {
                "static_modes": {
                    "yje": {"candidates": [{"symbol": "000001.SZ"}]},
                    "rzq": {"candidates": [{"symbol": "000002.SZ"}]},
                    "qs": {"candidates": [{"symbol": "600000.SH"}]},
                    "sb": {"candidates": [{"symbol": "000003.SZ"}]},
                }
            }

    monkeypatch.setattr(snapshot, "FourModeSnapshotCache", FakeCache)
    assert snapshot.four_mode_bid_symbols(object(), date(2026, 8, 20)) == [
        "000002.SZ",
        "600000.SH",
    ]


def test_valid_empty_auction_does_not_reuse_stale_partition(tmp_path):
    import json
    import polars as pl

    day = date(2026, 8, 20)
    manifest_dir = tmp_path / "ext_data" / "_ingestion" / "kaipanla" / "auction_completion"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / f"{day.isoformat()}.json").write_text(
        json.dumps({
            "status": "complete",
            "expected_components": ["0925"],
            "components": {"0925": {"status": "valid_empty", "rows": 0}},
        }),
        encoding="utf-8",
    )
    partition = tmp_path / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
    partition.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"], "auction_change_pct_0925": [5.0]}).write_parquet(partition / "part.parquet")

    class Store:
        data_dir = tmp_path

    class Repo:
        store = Store()

    cache = object.__new__(snapshot.FourModeSnapshotCache)
    cache.repo = Repo()
    rows, gaps, _manifest = cache._auction(day)
    assert rows == {}
    assert gaps == []


def test_snapshot_refreshes_when_auction_input_changes(monkeypatch):
    day = date(2026, 8, 20)
    cache = object.__new__(snapshot.FourModeSnapshotCache)
    cache.end = day
    cache._snapshots = {day: {"state": "waiting_data"}}
    cache._auction_signatures = {day: ("before",)}
    calls = []

    monkeypatch.setattr(cache, "_auction_signature", lambda _day: ("after",))

    def rebuild():
        calls.append(True)
        cache._snapshots[day] = {"state": "ready"}
        cache._auction_signatures[day] = ("after",)

    monkeypatch.setattr(cache, "_build", rebuild)
    assert cache.snapshot(day)["state"] == "ready"
    assert calls == [True]


def test_snapshot_extends_cache_for_a_new_trading_day(monkeypatch):
    first = date(2026, 8, 20)
    second = date(2026, 8, 21)
    cache = object.__new__(snapshot.FourModeSnapshotCache)
    cache.start = first
    cache.end = first
    cache._snapshots = {first: {"state": "ready"}}
    cache._auction_signatures = {first: ("same",)}
    calls = []

    def rebuild():
        calls.append(cache.end)
        cache._snapshots[second] = {"state": "ready"}
        cache._auction_signatures[second] = ("same",)

    monkeypatch.setattr(cache, "_build", rebuild)
    assert cache.snapshot(second)["state"] == "ready"
    assert cache.end == second
    assert calls == [second]


def test_four_mode_minute_preparation_targets_only_limit_up_symbols():
    import polars as pl

    target = date(2026, 8, 20)

    class Repo:
        def get_instruments_asset(self, asset_type):
            assert asset_type == "stock"
            return pl.DataFrame({"symbol": ["000001.SZ", "600000.SH"]})

        def get_daily_asset_batch(self, asset_type, symbols, start, end, columns):
            assert asset_type == "stock"
            assert set(symbols) == {"000001.SZ", "600000.SH"}
            return pl.DataFrame({
                "symbol": ["000001.SZ", "000001.SZ", "600000.SH", "600000.SH"],
                "date": [date(2026, 8, 19), target, date(2026, 8, 19), target],
                "raw_close": [10.0, 11.0, 20.0, 20.5],
                "raw_open": [10.0, 10.5, 20.0, 20.1],
                "raw_high": [10.0, 11.0, 20.0, 20.5],
                "raw_low": [10.0, 10.0, 20.0, 20.0],
                "close": [10.0, 11.0, 20.0, 20.5],
                "open": [10.0, 10.5, 20.0, 20.1],
                "high": [10.0, 11.0, 20.0, 20.5],
                "low": [10.0, 10.0, 20.0, 20.0],
                "volume": [100.0, 120.0, 100.0, 110.0],
                "amount": [1000.0, 1200.0, 2000.0, 2200.0],
            })

    assert snapshot.four_mode_limit_up_symbols(Repo(), target) == ["000001.SZ"]


def test_four_mode_minute_preparation_uses_exact_session_window(monkeypatch):
    import polars as pl

    target = date(2026, 8, 20)
    calls = []
    covered = set()

    class Repo:
        def get_minute_range(self, symbols, start, end, asset_type):
            if covered:
                return pl.DataFrame({"symbol": sorted(covered), "datetime": ["2026-08-20 09:30:00"] * len(covered)})
            return pl.DataFrame()

    def sync(symbols, repo, capset, **kwargs):
        calls.append((symbols, kwargs["window_start"], kwargs["window_end"]))
        covered.update(symbols)
        return 1

    monkeypatch.setattr("app.services.kline_sync.sync_and_persist_minute", sync)
    result = snapshot.ensure_four_mode_minute_data(
        Repo(), object(), {target: ["000001.SZ"]},
    )
    assert result["attempted_symbols"] == 1
    assert result["unresolved"] == {}
    assert calls == [(
        ["000001.SZ"],
        datetime(2026, 8, 20, 9, 25),
        datetime(2026, 8, 20, 15, 5),
    )]
