from datetime import date, timedelta
from types import SimpleNamespace

import polars as pl

from app.services import rps_rotation


class _DiskFallbackRepo:
    def __init__(self, data_dir, latest: date) -> None:
        self.store = SimpleNamespace(data_dir=data_dir)
        self._latest = latest
        # Simulate a populated but stale history cache. The repository rejects
        # it after the matrix generation changes while disk data remains valid.
        self._enriched_history_cache = pl.DataFrame({
            "symbol": ["A.SH"],
            "date": [latest - timedelta(days=1)],
            "change_pct": [0.01],
        })

    def latest_enriched_date(self, _asset_type: str = "stock") -> date:
        return self._latest

    def get_enriched_range(self, *_args, **_kwargs):
        return None


def test_rotation_falls_back_to_recent_parquet_and_caches_full_window(
    tmp_path, monkeypatch,
):
    start = date(2026, 7, 20)
    trading_dates = [start + timedelta(days=offset) for offset in range(31)]
    enriched_root = tmp_path / "kline_daily_enriched"
    for index, trading_date in enumerate(trading_dates):
        partition = enriched_root / f"date={trading_date.isoformat()}"
        partition.mkdir(parents=True)
        pl.DataFrame({
            "symbol": ["A.SH", "B.SH"],
            "date": [trading_date, trading_date],
            "close": [10.0 + index, 20.0 - index * 0.1],
        }).write_parquet(partition / "part.parquet")

    monkeypatch.setattr(
        rps_rotation,
        "_load_concept_map_df",
        lambda _repo, kind: (
            pl.DataFrame({"_sym_up": ["A.SH", "B.SH"], kind: ["人工智能", "银行"]}),
            2,
        ),
    )
    rps_rotation.invalidate_cache()
    repo = _DiskFallbackRepo(tmp_path, trading_dates[-1])

    seven_days = rps_rotation.build_rps_rotation(repo, days=7)
    thirty_days = rps_rotation.build_rps_rotation(repo, days=30)

    assert len(seven_days["dates"]) == 7
    assert len(thirty_days["dates"]) == 30
    assert thirty_days["dates"][0] == trading_dates[-1].isoformat()
    assert thirty_days["concept_count"] == 2
    latest_rows = thirty_days["columns"][trading_dates[-1].isoformat()]
    assert latest_rows[0][0] == "人工智能"
    assert latest_rows[0][1] == (40.0 / 39.0 - 1.0)
