from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from app.services.p0_backfill import (
    BackfillBlocked,
    BackfillConfig,
    _dedupe_or_raise,
    run_p0_backfill,
)
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet


def _capabilities(*caps: Cap) -> CapabilitySet:
    return CapabilitySet({cap: CapabilityLimits(batch=1, rpm=None) for cap in caps})


def _write_universe(data_dir: Path, symbols: list[str]) -> None:
    path = data_dir / "instruments" / "instruments.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(path)
    (data_dir / "capabilities.json").write_text(
        json.dumps({"capabilities": {Cap.KLINE_DAILY_BATCH.value: {"batch": 1}}}),
        encoding="utf-8",
    )


def _daily(symbol: str, close: float, day: date = date(2024, 1, 2)) -> pl.DataFrame:
    return pl.DataFrame({
        "symbol": [symbol],
        "date": [day],
        "open": [close],
        "high": [close],
        "low": [close],
        "close": [close],
        "volume": [100.0],
        "amount": [1000.0],
    })


class _FakeKlines:
    def __init__(self, by_symbol: dict[str, pl.DataFrame], *, fail_after: int | None = None) -> None:
        self.by_symbol = by_symbol
        self.calls = 0
        self.fail_after = fail_after

    def batch(self, symbols: list[str], **_kwargs):
        self.calls += 1
        if self.fail_after is not None and self.calls > self.fail_after:
            raise RuntimeError("synthetic source outage")
        return {symbol: self.by_symbol.get(symbol, pl.DataFrame()) for symbol in symbols}


class _FakeClient:
    def __init__(self, klines: _FakeKlines) -> None:
        self.klines = klines


def _config(data_dir: Path, *, run_id: str | None = None, publish: bool = False) -> BackfillConfig:
    return BackfillConfig(
        data_dir=data_dir,
        start=date(2024, 1, 1),
        end=date(2024, 1, 3),
        datasets=("daily",),
        symbols=("600000.SH", "000001.SZ"),
        batch_size=1,
        run_id=run_id,
        publish=publish,
    )


def test_daily_backfill_stages_then_publishes_from_checkpoint(tmp_path: Path) -> None:
    _write_universe(tmp_path, ["600000.SH", "000001.SZ"])
    canonical = tmp_path / "kline_daily" / "date=2024-01-01" / "part.parquet"
    canonical.parent.mkdir(parents=True)
    _daily("600000.SH", 9.0, date(2024, 1, 1)).write_parquet(canonical)
    first_client = _FakeClient(_FakeKlines({
        "600000.SH": _daily("600000.SH", 10.0),
        "000001.SZ": _daily("000001.SZ", 20.0),
    }))

    staged = run_p0_backfill(
        _config(tmp_path),
        client=first_client,
        capset=_capabilities(Cap.KLINE_DAILY_BATCH),
    )

    assert staged["status"] == "staged"
    assert first_client.klines.calls == 2
    assert pl.read_parquet(canonical)["close"].to_list() == [9.0]

    # Completed batches are reused; publishing does not hit the source again.
    second_client = _FakeClient(_FakeKlines({}, fail_after=0))
    published = run_p0_backfill(
        _config(tmp_path, run_id=staged["run_id"], publish=True),
        client=second_client,
        capset=_capabilities(Cap.KLINE_DAILY_BATCH),
    )

    assert published["status"] == "published"
    assert second_client.klines.calls == 0
    rows = pl.scan_parquet(str(tmp_path / "kline_daily" / "**" / "*.parquet")).collect()
    assert set(rows["symbol"].to_list()) == {"600000.SH", "000001.SZ"}
    assert (tmp_path / "backfill_state" / "p0_history" / staged["run_id"] / "backups" / "kline_daily").exists()


def test_partial_batch_failure_blocks_without_replacing_canonical(tmp_path: Path) -> None:
    _write_universe(tmp_path, ["600000.SH", "000001.SZ"])
    client = _FakeClient(_FakeKlines({"600000.SH": _daily("600000.SH", 10.0)}, fail_after=1))

    with pytest.raises(BackfillBlocked, match="daily batches failed"):
        run_p0_backfill(
            _config(tmp_path, publish=True),
            client=client,
            capset=_capabilities(Cap.KLINE_DAILY_BATCH),
        )

    assert not (tmp_path / "kline_daily").exists()
    manifest = next((tmp_path / "backfill_state" / "p0_history").glob("*/manifest.json"))
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "blocked"


def test_conflicting_duplicate_keys_are_rejected() -> None:
    frame = pl.DataFrame({
        "symbol": ["600000.SH", "600000.SH"],
        "date": [date(2024, 1, 2), date(2024, 1, 2)],
        "close": [10.0, 11.0],
    })
    with pytest.raises(BackfillBlocked, match="conflicting duplicate"):
        _dedupe_or_raise(frame, ("symbol", "date"), "daily")


def test_missing_capability_blocks_before_source_fetch(tmp_path: Path) -> None:
    _write_universe(tmp_path, ["600000.SH", "000001.SZ"])
    client = _FakeClient(_FakeKlines({"600000.SH": _daily("600000.SH", 10.0)}))

    with pytest.raises(BackfillBlocked, match="missing TickFlow capabilities"):
        run_p0_backfill(_config(tmp_path), client=client, capset=CapabilitySet())

    assert client.klines.calls == 0
    manifest = next((tmp_path / "backfill_state" / "p0_history").glob("*/manifest.json"))
    assert json.loads(manifest.read_text(encoding="utf-8"))["status"] == "blocked"


def test_financial_backfill_fetches_all_statement_tables(tmp_path: Path) -> None:
    _write_universe(tmp_path, ["600000.SH"])
    calls: list[str] = []

    def fetch(table: str):
        def method(symbols: list[str], **_kwargs):
            calls.append(table)
            return pl.DataFrame({
                "symbol": symbols,
                "period_end": ["2023-12-31"],
                "announce_date": ["2024-04-15"],
                "value": [1.0],
            })

        return method

    client = SimpleNamespace(financials=SimpleNamespace(**{
        table: fetch(table)
        for table in ("metrics", "income", "balance_sheet", "cash_flow", "shares")
    }))
    config = BackfillConfig(
        data_dir=tmp_path,
        start=date(2015, 1, 1),
        end=date(2024, 12, 31),
        datasets=("financials",),
        symbols=("600000.SH",),
        batch_size=1,
    )

    result = run_p0_backfill(
        config,
        client=client,
        capset=_capabilities(Cap.FINANCIAL),
    )

    assert result["status"] == "staged"
    assert calls == ["metrics", "income", "balance_sheet", "cash_flow", "shares"]
    run_root = Path(result["manifest"]).parent
    for table in ("metrics", "income", "balance_sheet", "cash_flow", "shares"):
        assert (run_root / "batches" / "financials" / table / "batch-00000.parquet").exists()
