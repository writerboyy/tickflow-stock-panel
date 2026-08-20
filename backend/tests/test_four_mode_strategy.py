from datetime import date
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
