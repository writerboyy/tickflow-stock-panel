from __future__ import annotations

from pathlib import Path

import polars as pl

from scripts import backfill_easy_tdx_dividends as backfill_script


def test_backfill_persists_completed_codes_and_retries_only_failures(tmp_path, monkeypatch):
    instruments = tmp_path / "instruments"
    instruments.mkdir()
    pl.DataFrame({
        "code": ["000001", "000002", "000003"],
        "type": ["stock", "stock", "stock"],
    }).write_parquet(instruments / "instruments.parquet")
    calls: list[list[str]] = []

    def fetch(codes, **_kwargs):
        batch = list(codes)
        calls.append(batch)
        return [], {"000002": "TimeoutError"} if "000002" in batch else {}

    monkeypatch.setattr(backfill_script, "fetch_dividend_history_batch", fetch)
    state_path = tmp_path / "state.json"

    state = backfill_script.backfill(tmp_path, state_path, batch_size=2)

    assert calls == [["000001", "000002"], ["000003"]]
    assert state["completed"] == ["000001", "000003"]
    assert state["failures"] == {"000002": "TimeoutError"}

    monkeypatch.setattr(backfill_script, "fetch_dividend_history_batch", lambda codes, **_kwargs: ([], {}))
    state = backfill_script.backfill(tmp_path, state_path, batch_size=2)

    assert state["completed"] == ["000001", "000002", "000003"]
    assert state["failures"] == {}
