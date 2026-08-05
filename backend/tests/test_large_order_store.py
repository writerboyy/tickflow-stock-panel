from datetime import date, datetime
from pathlib import Path

import polars as pl

from app.services.large_order_store import LargeOrderStore


def _event_ts(hour: int, minute: int = 1, second: int = 2) -> int:
    value = datetime(2026, 8, 4, hour, minute, second)
    return int(value.timestamp() * 1000)


def test_store_writes_partitioned_events_and_compacts(tmp_path: Path):
    day = date(2026, 8, 4)
    store = LargeOrderStore(tmp_path, flush_interval=0.01, max_batch_rows=2)
    store.start()
    store.submit(
        "proxy_flow",
        [
            {
                "trade_date": day,
                "event_ts_ms": _event_ts(9),
                "symbol": "000001.SZ",
                "name": "平安银行",
                "price": 10.1,
                "amount": 200000,
                "volume": 1000,
                "delta_amount": 200000,
                "delta_volume": 1000,
                "buy_amount": 200000,
                "sell_amount": 0,
                "side": 1,
                "event_id": "flow-1",
                "source": "tickflow_proxy",
            },
            {
                "trade_date": day,
                "event_ts_ms": _event_ts(10),
                "symbol": "000001.SZ",
                "name": "平安银行",
                "price": 10.2,
                "amount": 300000,
                "volume": 1500,
                "delta_amount": 300000,
                "delta_volume": 1500,
                "buy_amount": 0,
                "sell_amount": 300000,
                "side": -1,
                "event_id": "flow-2",
                "source": "tickflow_proxy",
            },
        ],
    )
    store.stop(compact_date=day)

    result = store.query("proxy_flow", day, symbol="000001.SZ", limit=10)
    assert result["count"] == 2
    assert [row["event_id"] for row in result["rows"]] == ["flow-1", "flow-2"]
    assert (tmp_path / "large_orders" / "proxy_flow" / "date=2026-08-04" / "part.parquet").exists()
    assert store.status()["written_rows"] == 2


def test_store_deduplicates_on_compaction_and_filters_with_truncation(tmp_path: Path):
    day = date(2026, 8, 4)
    store = LargeOrderStore(tmp_path, flush_interval=0.01)
    store.start()
    rows = [
        {
            "trade_date": day,
            "event_ts_ms": _event_ts(9, second=index),
            "symbol": "600000.SH",
            "event_id": "same-event" if index < 2 else f"event-{index}",
            "amount": 100 + index,
            "volume": 10,
            "source": "tickflow_proxy",
        }
        for index in range(3)
    ]
    store.submit("proxy_flow", rows)
    store.flush_now()

    result = store.query(
        "proxy_flow",
        day,
        symbol="600000.SH",
        from_ms=_event_ts(9, second=1),
        to_ms=_event_ts(9, second=2),
        limit=1,
    )
    assert result["count"] == 1
    assert result["truncated"] is True
    assert result["rows"][0]["event_id"] == "same-event"
    store.stop(compact_date=day)


def test_store_history_cursor_is_stable_for_same_timestamp(tmp_path: Path):
    day = date(2026, 8, 4)
    store = LargeOrderStore(tmp_path, flush_interval=0.01)
    store.start()
    timestamp = _event_ts(9)
    store.submit(
        "proxy_flow",
        [
            {
                "trade_date": day,
                "event_ts_ms": timestamp,
                "symbol": "000001.SZ",
                "event_id": f"flow-{index}",
                "amount": 100 + index,
            }
            for index in range(3)
        ],
    )
    store.submit(
        "kaipanla_trade",
        [{
            "trade_date": day,
            "event_ts_ms": timestamp,
            "symbol": "000001.SZ",
            "event_id": "trade-1",
            "amount": 300,
            "direction": "active_buy",
        }],
    )

    first = store.query_events(
        day,
        kinds=("proxy_flow", "kaipanla_trade"),
        limit=2,
        order="desc",
    )
    second = store.query_events(
        day,
        kinds=("proxy_flow", "kaipanla_trade"),
        limit=2,
        order="desc",
        cursor=first["next_cursor"],
    )

    event_keys = [
        (row["event_kind"], row["event_id"])
        for row in first["rows"] + second["rows"]
    ]
    assert len(event_keys) == 4
    assert len(set(event_keys)) == 4
    assert first["has_more"] is True
    assert second["has_more"] is False
    store.stop(compact_date=day)


def test_store_history_reads_legacy_parquet_with_missing_columns(tmp_path: Path):
    day = date(2026, 8, 4)
    day_root = tmp_path / "large_orders" / "proxy_flow" / f"date={day}"
    day_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [day],
            "event_ts_ms": [_event_ts(9)],
            "symbol": ["000001.SZ"],
            "event_id": ["legacy-flow"],
            "amount": [1_000.0],
        }
    ).write_parquet(day_root / "legacy.parquet")
    store = LargeOrderStore(tmp_path)

    result = store.query_events(day, kinds=("proxy_flow",))

    assert result["count"] == 1
    assert result["rows"][0]["event_id"] == "legacy-flow"
    assert result["rows"][0]["buy_amount"] is None
    assert result["rows"][0]["event_kind"] == "proxy_flow"
