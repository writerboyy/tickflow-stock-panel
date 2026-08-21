from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from app.data_providers.normalizer import normalize_tick
from app.data_providers.qmt_provider import QmtProvider
from app.data_providers.tickflow_provider import TickFlowProvider
from app.free_strategy.tick_health import inspect_tick_data, require_tick_data
from app.services.qmt_tick_import import import_qmt_ticks
from app.tickflow.repository import DataStore, KlineRepository


def _raw_tick(symbol: str, timestamp: str, price: float, sequence: int = 1) -> dict:
    return {
        "stockCode": symbol,
        "time": timestamp,
        "lastPrice": price,
        "open": price,
        "high": price,
        "low": price,
        "volume": 100,
        "amount": price * 100,
        "seq": sequence,
    }


@pytest.mark.parametrize(
    "raw",
    [
        pd.DataFrame([_raw_tick("600000.SH", "20240801093000", 10.0)]),
        {
            "__bigqmt_type__": "DataFrame",
            "records": [_raw_tick("600000.SH", "20240801093000", 10.0)],
        },
        {"600000.SH": [_raw_tick("600000.SH", "20240801093000", 10.0)]},
    ],
)
def test_normalize_tick_accepts_qmt_dataframe_and_dict_shapes(raw):
    frame = normalize_tick(raw, source="qmt")

    assert frame.height == 1
    assert frame.row(0, named=True) == {
        "symbol": "600000.SH",
        "datetime": datetime(2024, 8, 1, 9, 30),
        "last_price": 10.0,
        "close": 10.0,
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "prev_close": None,
        "volume": 100.0,
        "amount": 1000.0,
        "limit_up": None,
        "limit_down": None,
        "suspended": False,
        "source": "qmt",
        "sequence": "1",
        "trade_id": None,
        "source_order": 0,
    }


def test_normalize_tick_converts_epoch_to_beijing_and_drops_invalid_rows():
    epoch = datetime(2024, 8, 1, 1, 30, tzinfo=timezone.utc).timestamp() * 1000
    frame = normalize_tick([
        {"stock_code": "600000.SH", "time": epoch, "lastPrice": 10},
        {"stock_code": "600000.SH", "time": epoch, "lastPrice": 0},
        {"stock_code": "600000.SH", "time": "invalid", "lastPrice": 10},
    ], source="qmt")

    assert frame.height == 1
    assert frame["datetime"].item() == datetime(2024, 8, 1, 9, 30)


def test_normalize_tick_replaces_zero_optional_prices_and_parses_false_suspension():
    frame = normalize_tick([{
        "stock_code": "600000.SH",
        "time": "20240801093000",
        "lastPrice": 10,
        "open": 0,
        "high": 0,
        "low": 0,
        "lastClose": 0,
        "upperLimit": 0,
        "lowerLimit": 0,
        "isSuspended": "false",
    }], source="qmt")

    row = frame.row(0, named=True)
    assert (row["open"], row["high"], row["low"]) == (10, 10, 10)
    assert (row["prev_close"], row["limit_up"], row["limit_down"]) == (None, None, None)
    assert row["suspended"] is False


def test_qmt_provider_uses_tick_download_and_market_data_calls():
    class Client:
        def __init__(self):
            self.calls = []

        def call(self, method, params=None):
            self.calls.append((method, params))
            if method == "get_market_data_ex":
                return {"600000.SH": [_raw_tick("600000.SH", "20240801093000", 10)]}
            return {}

    client = Client()
    provider = QmtProvider(client)
    frame = provider.get_tick(
        ["600000.SH"],
        datetime(2024, 8, 1),
        datetime(2024, 8, 1, 23, 59),
        "stock",
    )

    assert frame.height == 1
    assert [method for method, _params in client.calls] == [
        "download_history_data", "get_market_data_ex",
    ]
    assert all(params["period"] == "tick" for _method, params in client.calls)
    with pytest.raises(ValueError, match="仅支持股票"):
        provider.get_tick(
            ["510300.SH"], datetime(2024, 8, 1), datetime(2024, 8, 1), "etf",
        )
    assert QmtProvider.capabilities.tick is True
    assert TickFlowProvider.capabilities.tick is False


def test_qmt_provider_propagates_rpc_errors():
    class Client:
        @staticmethod
        def call(_method, _params=None):
            raise RuntimeError("rpc failed")

    with pytest.raises(RuntimeError, match="rpc failed"):
        QmtProvider(Client()).get_tick(
            ["600000.SH"],
            datetime(2024, 8, 1),
            datetime(2024, 8, 1, 23, 59),
            "stock",
        )


class _ImportProvider:
    name = "qmt"
    capabilities = SimpleNamespace(tick=True)

    def __init__(self, rows: dict[str, list[dict]], fail_symbol: str | None = None) -> None:
        self.rows = rows
        self.fail_symbol = fail_symbol

    def get_trading_dates(self, _start, _end):
        return [date(2024, 8, 1)]

    def get_tick(self, symbols, _start, _end, _asset_type):
        symbol = symbols[0]
        if symbol == self.fail_symbol:
            return pl.DataFrame()
        return normalize_tick(self.rows.get(symbol, []), default_symbol=symbol, source="qmt")


def test_import_is_atomic_per_day_and_preserves_duplicate_timestamps(tmp_path):
    provider = _ImportProvider({
        "600000.SH": [
            _raw_tick("600000.SH", "20240801093000.001", 10.0, 1),
            _raw_tick("600000.SH", "20240801093000.001", 10.1, 2),
        ],
        "000001.SZ": [_raw_tick("000001.SZ", "20240801093000.002", 11.0, 1)],
    })

    result = import_qmt_ticks(
        provider, tmp_path, ["600000.SH", "000001.SZ"],
        date(2024, 8, 1), date(2024, 8, 1),
    )
    part = tmp_path / "tick" / "date=2024-08-01" / "part.parquet"
    frame = pl.read_parquet(part)

    assert result["rows"] == 3
    assert frame.height == 3
    assert frame.filter(pl.col("symbol") == "600000.SH")["last_price"].to_list() == [10.0, 10.1]

    before = part.read_bytes()
    failing = _ImportProvider(provider.rows, fail_symbol="000001.SZ")
    with pytest.raises(ValueError, match="QMT Tick 为空"):
        import_qmt_ticks(
            failing, tmp_path, ["600000.SH", "000001.SZ"],
            date(2024, 8, 1), date(2024, 8, 1),
        )
    assert part.read_bytes() == before


def test_import_rejects_invalid_canonical_values_before_replacing_partition(tmp_path):
    day = date(2024, 8, 1)
    provider = _ImportProvider({
        "600000.SH": [_raw_tick("600000.SH", "20240801093000", 10.0)],
    })
    import_qmt_ticks(provider, tmp_path, ["600000.SH"], day, day)
    part = tmp_path / "tick" / f"date={day.isoformat()}" / "part.parquet"
    before = part.read_bytes()

    class InvalidProvider(_ImportProvider):
        def get_tick(self, symbols, _start, _end, _asset_type):
            frame = super().get_tick(symbols, _start, _end, _asset_type)
            return frame.with_columns(pl.lit(-1.0).alias("volume"))

    with pytest.raises(ValueError, match="无效记录"):
        import_qmt_ticks(
            InvalidProvider(provider.rows), tmp_path, ["600000.SH"], day, day,
        )
    assert part.read_bytes() == before


def test_import_rejects_missing_canonical_fields(tmp_path):
    class Provider(_ImportProvider):
        def get_tick(self, _symbols, _start, _end, _asset_type):
            return pl.DataFrame({
                "symbol": ["600000.SH"],
                "datetime": [datetime(2024, 8, 1, 9, 30)],
                "last_price": [10.0],
            })

    with pytest.raises(ValueError, match="Tick 缺少字段"):
        import_qmt_ticks(
            Provider({}),
            tmp_path,
            ["600000.SH"],
            date(2024, 8, 1),
            date(2024, 8, 1),
        )


def test_repository_tick_snapshot_uses_last_source_order(tmp_path):
    part = tmp_path / "tick" / "date=2024-08-01" / "part.parquet"
    part.parent.mkdir(parents=True)
    normalize_tick([
        _raw_tick("600000.SH", "20240801093000.001", 10.0, 1),
        _raw_tick("600000.SH", "20240801093000.001", 10.1, 2),
    ], source="qmt").write_parquet(part)
    repo = KlineRepository(DataStore(tmp_path))

    frame = repo.get_tick_range(
        ["600000.SH"], date(2024, 8, 1), date(2024, 8, 1), "stock",
    )
    snapshot = repo.get_tick_snapshot(
        ["600000.SH"], datetime(2024, 8, 1, 9, 30, 1), "stock",
    )

    assert frame.height == 2
    assert frame["last_price"].to_list() == [10.0, 10.1]
    assert snapshot["last_price"].item() == 10.1


def test_tick_health_reports_symbol_date_gaps_and_fails_closed(tmp_path):
    day = date(2024, 8, 1)
    part = tmp_path / "tick" / f"date={day.isoformat()}" / "part.parquet"
    part.parent.mkdir(parents=True)
    normalize_tick([
        _raw_tick("600000.SH", "20240801093000", 10.0),
    ], source="qmt").write_parquet(part)
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))

    report = inspect_tick_data(
        repo, ["600000.SH", "000001.SZ"], day, day, expected_dates=[day],
    )

    assert report["status"] == "issues"
    assert report["rows"] == 1
    assert report["sources"] == ["qmt"]
    assert any(issue["type"] == "missing_symbol_date" for issue in report["issues"])
    with pytest.raises(ValueError, match="Tick 数据预检失败"):
        require_tick_data(
            repo, ["600000.SH", "000001.SZ"], day, day, expected_dates=[day],
        )


def test_tick_health_reports_invalid_schema_without_raising_polars_error(tmp_path):
    day = date(2024, 8, 1)
    part = tmp_path / "tick" / f"date={day.isoformat()}" / "part.parquet"
    part.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["600000.SH"],
        "datetime": ["2024-08-01 09:30:00"],
        "last_price": [10.0],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "prev_close": [9.9],
        "volume": [100.0],
        "amount": [1000.0],
        "limit_up": [11.0],
        "limit_down": [9.0],
        "suspended": [False],
        "source": ["qmt"],
        "source_order": [0],
    }).write_parquet(part)
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))

    report = inspect_tick_data(
        repo, ["600000.SH"], day, day, expected_dates=[day],
    )

    assert report["status"] == "issues"
    assert report["issues"][0]["type"] == "invalid_schema"
