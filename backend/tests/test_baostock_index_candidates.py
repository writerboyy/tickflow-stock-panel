from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.baostock.index_candidates import (
    BaoStockIndexMembershipCollector,
    _SocketWithTimeout,
    derive_csi800,
    normalize_index_membership_snapshot,
)
from app.plugins.baostock.instrument_lifecycle import (
    BaoStockInstrumentLifecycleCollector,
    lookback_start,
)
from app.plugins.baostock.socket_proxy import (
    force_proxy_enabled,
    iter_proxy_candidates,
    parse_http_proxy_url,
)
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    INSTRUMENT_LIFECYCLE_EVENTS_TABLE,
    normalize_instrument_lifecycle_events,
    publish_history_table,
    read_history_table,
    table_path,
)
from scripts.backfill_index_membership_history import local_trading_dates, main


class _LoginResult:
    error_code = "0"
    error_msg = ""


class _BaoStockResult:
    error_code = "0"
    error_msg = ""
    fields = ["updateDate", "code", "code_name"]

    def __init__(self, rows: list[list[str]]) -> None:
        self._rows = rows
        self._idx = -1

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._idx]


class _FakeBaoStock:
    def __init__(self, by_query: dict[tuple[str, str], list[list[str]]] | None = None) -> None:
        self.by_query = by_query or {}
        self.queries: list[tuple[str, str]] = []
        self.logout_count = 0

    def login(self) -> _LoginResult:
        return _LoginResult()

    def logout(self) -> None:
        self.logout_count += 1

    def query_hs300_stocks(self, date: str = "") -> _BaoStockResult:
        self.queries.append(("000300.SH", date))
        return _BaoStockResult(self.by_query.get(("000300.SH", date), []))

    def query_zz500_stocks(self, date: str = "") -> _BaoStockResult:
        self.queries.append(("000905.SH", date))
        return _BaoStockResult(self.by_query.get(("000905.SH", date), []))


class _StockBasicResult:
    error_code = "0"
    error_msg = ""
    fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]

    def __init__(self, rows: list[list[str]]) -> None:
        self._rows = rows
        self._idx = -1

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._idx]


class _FakeBaoStockBasic:
    def __init__(self, rows: list[list[str]]) -> None:
        self.rows = rows
        self.logout_count = 0

    def login(self) -> _LoginResult:
        return _LoginResult()

    def logout(self) -> None:
        self.logout_count += 1

    def query_stock_basic(self) -> _StockBasicResult:
        return _StockBasicResult(self.rows)


class _BlacklistedLoginResult:
    error_code = "10001011"
    error_msg = "blacklisted"


class _BlacklistedBaoStock(_FakeBaoStock):
    def login(self) -> _BlacklistedLoginResult:
        return _BlacklistedLoginResult()


class _ClosedSocket:
    def __init__(self) -> None:
        self.timeout = None

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def recv(self, size: int) -> bytes:
        del size
        return b""


class _DirectFailSocket:
    def __init__(self) -> None:
        self.closed = False
        self.timeout = None

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout

    def connect(self, address: tuple[str, int]) -> None:
        del address
        raise TimeoutError("direct failed")

    def close(self) -> None:
        self.closed = True


class _ConnectedProxySocket:
    def __init__(self) -> None:
        self.timeout = None

    def settimeout(self, timeout: float) -> None:
        self.timeout = timeout


def _write_daily_dates(data_dir, dates: list[date]) -> None:
    root = data_dir / "kline_daily"
    root.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"] * len(dates),
        "date": dates,
    }).write_parquet(root / "part.parquet")


def test_normalize_baostock_index_membership_keeps_source_update_date():
    frame = normalize_index_membership_snapshot(
        [
            {"date": "2020-01-03", "code": "sh.600000", "code_name": "浦发银行"},
            {"date": "2020-01-03", "code": "sz.000001", "code_name": "平安银行"},
            {"date": "2020-01-03", "code": "sz.000001", "code_name": "平安银行"},
        ],
        index_symbol="000300.SH",
        index_name="沪深300",
        snapshot_date=date(2020, 1, 3),
    )

    assert frame.select(
        "index_symbol",
        "member_symbol",
        "member_code",
        "member_name",
        "snapshot_date",
        "source_update_date",
        "source",
        "provenance",
    ).to_dicts() == [
        {
            "index_symbol": "000300.SH",
            "member_symbol": "000001.SZ",
            "member_code": "000001",
            "member_name": "平安银行",
            "snapshot_date": date(2020, 1, 3),
            "source_update_date": date(2020, 1, 3),
            "source": "baostock",
            "provenance": "source_dated_snapshot",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600000.SH",
            "member_code": "600000",
            "member_name": "浦发银行",
            "snapshot_date": date(2020, 1, 3),
            "source_update_date": date(2020, 1, 3),
            "source": "baostock",
            "provenance": "source_dated_snapshot",
        },
    ]


def test_baostock_collector_fetches_complete_snapshots_without_provider_table(tmp_path):
    hs300 = [["2020-01-03", f"sh.{600000 + index}", f"h{index}"] for index in range(300)]
    zz500 = [["2020-01-03", f"sz.{index + 1:06d}", f"z{index}"] for index in range(500)]
    fake = _FakeBaoStock({
        ("000300.SH", "2020-01-03"): hs300,
        ("000905.SH", "2020-01-03"): zz500,
    })
    collector = BaoStockIndexMembershipCollector(tmp_path, bs_module=fake)

    frame = collector.fetch_index_snapshots(
        ["000300.SH", "000905.SH"], snapshot_dates=[date(2020, 1, 3)]
    )

    assert frame.height == 800
    assert fake.queries == [
        ("000300.SH", "2020-01-03"),
        ("000905.SH", "2020-01-03"),
    ]
    assert fake.logout_count == 1
    manifest = (
        tmp_path
        / "ext_data"
        / "_ingestion"
        / "baostock"
        / INDEX_MEMBERSHIP_HISTORY_TABLE
        / "2020-01-03_2020-01-03.json"
    )
    assert not manifest.exists()
    assert not table_path(tmp_path, INDEX_MEMBERSHIP_HISTORY_TABLE).exists()


def test_baostock_lifecycle_collector_publishes_recent_overlap_and_keeps_existing(tmp_path):
    existing = normalize_instrument_lifecycle_events(
        [
            {
                "证券代码": "600002",
                "证券简称": "齐鲁石化",
                "上市日期": "1998-04-08",
                "终止上市日期": "2006-04-24",
            }
        ],
        source="exchange",
    )
    publish_history_table(tmp_path, INSTRUMENT_LIFECYCLE_EVENTS_TABLE, existing)
    fake = _FakeBaoStockBasic([
        ["sh.600000", "浦发银行", "1999-11-10", "", "1", "1"],
        ["sh.600001", "邯郸钢铁", "1998-01-22", "2024-01-03", "1", "0"],
        ["sz.000003", "PT金田A", "1991-07-03", "2019-12-31", "1", "0"],
        ["sz.300999", "未来新股", "2027-01-01", "", "1", "1"],
    ])
    collector = BaoStockInstrumentLifecycleCollector(tmp_path, bs_module=fake)

    result = collector.collect_stock_lifecycle(
        start_date=date(2021, 1, 1),
        end_date=date(2026, 1, 1),
    )

    assert fake.logout_count == 1
    assert result["source_rows"] == 4
    assert result["candidate_rows"] == 2
    assert result["published_rows"] == 3
    assert result["total_table_rows"] == 5
    frame = read_history_table(tmp_path, INSTRUMENT_LIFECYCLE_EVENTS_TABLE)
    assert frame.select(["symbol", "event_type", "event_date", "source"]).to_dicts() == [
        {
            "symbol": "600000.SH",
            "event_type": "listed",
            "event_date": date(1999, 11, 10),
            "source": "baostock",
        },
        {
            "symbol": "600001.SH",
            "event_type": "listed",
            "event_date": date(1998, 1, 22),
            "source": "baostock",
        },
        {
            "symbol": "600001.SH",
            "event_type": "delisted",
            "event_date": date(2024, 1, 3),
            "source": "baostock",
        },
        {
            "symbol": "600002.SH",
            "event_type": "listed",
            "event_date": date(1998, 4, 8),
            "source": "exchange",
        },
        {
            "symbol": "600002.SH",
            "event_type": "delisted",
            "event_date": date(2006, 4, 24),
            "source": "exchange",
        },
    ]


def test_baostock_lifecycle_lookback_handles_leap_day():
    assert lookback_start(date(2024, 2, 29), years=5) == date(2019, 2, 28)


def test_historical_fetch_expands_only_from_source_update_date(tmp_path):
    rows = [["2020-01-06", f"sh.{600000 + index}", f"h{index}"] for index in range(300)]
    fake = _FakeBaoStock({
        ("000300.SH", "2020-01-08"): rows,
        ("000300.SH", "2020-01-03"): [
            ["2020-01-02", f"sh.{600300 + index}", f"o{index}"] for index in range(300)
        ],
    })
    collector = BaoStockIndexMembershipCollector(tmp_path, bs_module=fake)

    frame = collector.fetch_historical_membership(
        "000300.SH",
        trading_dates=[
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 6),
            date(2020, 1, 7),
            date(2020, 1, 8),
        ],
    )

    assert fake.queries == [
        ("000300.SH", "2020-01-08"),
        ("000300.SH", "2020-01-03"),
    ]
    assert frame["snapshot_date"].n_unique() == 5
    assert frame.filter(pl.col("snapshot_date") == date(2020, 1, 3)).height == 300
    assert frame.filter(pl.col("snapshot_date") == date(2020, 1, 6)).height == 300
    assert set(frame["provenance"].unique().to_list()) == {"source_effective_snapshot"}


def test_historical_fetch_skips_incomplete_source_interval_when_allowed(tmp_path):
    complete = [
        ["2020-01-02", f"sh.{600000 + index}", f"h{index}"]
        for index in range(300)
    ]
    incomplete = [
        ["2020-01-06", f"sh.{600300 + index}", f"i{index}"]
        for index in range(299)
    ]
    fake = _FakeBaoStock({
        ("000300.SH", "2020-01-08"): incomplete,
        ("000300.SH", "2020-01-03"): complete,
    })
    collector = BaoStockIndexMembershipCollector(tmp_path, bs_module=fake)

    frame = collector.fetch_historical_membership(
        "000300.SH",
        trading_dates=[
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 6),
            date(2020, 1, 7),
            date(2020, 1, 8),
        ],
        allow_incomplete_snapshots=True,
    )

    assert frame["snapshot_date"].unique().sort().to_list() == [
        date(2020, 1, 2),
        date(2020, 1, 3),
    ]
    assert collector._historical_gaps["000300.SH"] == [
        {
            "index_symbol": "000300.SH",
            "query_date": "2020-01-08",
            "source_update_date": "2020-01-06",
            "returned_members": 299,
            "expected_members": 300,
            "missing_date_start": "2020-01-06",
            "missing_date_end": "2020-01-08",
            "missing_snapshot_dates": 3,
        }
    ]


def test_derive_csi800_requires_exact_disjoint_union():
    snapshot_date = date(2020, 1, 3)
    hs300 = normalize_index_membership_snapshot(
        [{"updateDate": "2020-01-03", "code": f"sh.{600000 + index}"} for index in range(300)],
        index_symbol="000300.SH",
        index_name="沪深300",
        snapshot_date=snapshot_date,
    )
    zz500 = normalize_index_membership_snapshot(
        [{"updateDate": "2020-01-03", "code": f"sz.{index + 1:06d}"} for index in range(500)],
        index_symbol="000905.SH",
        index_name="中证500",
        snapshot_date=snapshot_date,
    )

    derived = derive_csi800(pl.concat([hs300, zz500]))

    assert derived.height == 800
    assert derived["index_symbol"].unique().to_list() == ["000906.SH"]
    assert derived["provenance"].unique().to_list() == ["derived_union_csi300_csi500"]


def test_baostock_socket_wrapper_fails_fast_on_closed_connection():
    socket = _ClosedSocket()
    wrapped = _SocketWithTimeout(socket, 3.0)

    try:
        wrapped.recv(8192)
    except ConnectionError as exc:
        assert "BaoStock socket closed" in str(exc)
    else:
        raise AssertionError("closed BaoStock socket should fail fast")

    assert socket.timeout == 3.0


def test_baostock_socket_wrapper_falls_back_to_proxy(monkeypatch):
    direct_socket = _DirectFailSocket()
    proxy_socket = _ConnectedProxySocket()
    calls: list[tuple[str, int, float]] = []

    def fake_proxy(host: str, port: int, *, timeout: float):
        calls.append((host, port, timeout))
        return proxy_socket

    monkeypatch.setattr(
        "app.plugins.baostock.index_candidates.open_baostock_proxy_connection",
        fake_proxy,
    )

    wrapped = _SocketWithTimeout(direct_socket, 7.0)
    wrapped.connect(("www.baostock.com", 10030))

    assert direct_socket.closed is True
    assert calls == [("www.baostock.com", 10030, 7.0)]
    assert wrapped._value is proxy_socket


def test_baostock_proxy_env_helpers():
    env = {
        "BAOSTOCK_PROXY_URL": "127.0.0.1:7890",
        "HTTP_PROXY": "http://127.0.0.1:8080",
        "ICUBE_PROXY_HOST": "host.docker.internal",
        "BAOSTOCK_FORCE_PROXY": "true",
    }

    assert list(iter_proxy_candidates(env)) == [
        "127.0.0.1:7890",
        "http://127.0.0.1:8080",
        "http://host.docker.internal:7890",
    ]
    assert parse_http_proxy_url("127.0.0.1:7890") == ("127.0.0.1", 7890)
    assert parse_http_proxy_url("socks5://127.0.0.1:7890") is None
    assert force_proxy_enabled(env) is True


def test_baostock_collector_reports_blacklisted_login_code(tmp_path):
    collector = BaoStockIndexMembershipCollector(tmp_path, bs_module=_BlacklistedBaoStock())

    with pytest.raises(RuntimeError) as exc:
        collector.fetch_index_snapshots(
            ["000300.SH"], snapshot_dates=[date(2020, 1, 3)]
        )

    assert "error_code=10001011" in str(exc.value)
    assert "blacklist" in str(exc.value)


def test_backfill_uses_local_trading_dates(tmp_path):
    _write_daily_dates(
        tmp_path,
        [
            date(2020, 12, 31),
            date(2021, 1, 4),
            date(2021, 1, 6),
            date(2021, 1, 8),
        ],
    )

    dates = local_trading_dates(
        tmp_path,
        start_date=date(2021, 1, 1),
        end_date=date(2021, 1, 6),
    )

    assert dates == [date(2021, 1, 4), date(2021, 1, 6)]


def test_backfill_trading_dates_ignore_extra_daily_columns(tmp_path):
    root = tmp_path / "kline_daily"
    first = root / "date=2021-01-04"
    second = root / "date=2021-01-05"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2021, 1, 4)],
    }).write_parquet(first / "part.parquet")
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2021, 1, 5)],
        "quote_ts": ["2021-01-05T15:00:00+08:00"],
    }).write_parquet(second / "part.parquet")

    dates = local_trading_dates(
        tmp_path,
        start_date=date(2021, 1, 1),
        end_date=date(2021, 1, 6),
    )

    assert dates == [date(2021, 1, 4), date(2021, 1, 5)]


def test_backfill_dry_run_does_not_publish(tmp_path, capsys):
    _write_daily_dates(tmp_path, [date(2021, 1, 4), date(2021, 1, 5)])

    result = main([
        "--data-dir",
        str(tmp_path),
        "--start-date",
        "2021-01-01",
        "--end-date",
        "2021-01-05",
        "--dry-run",
    ])

    assert result == 0
    output = capsys.readouterr().out
    assert '"trading_dates": 2' in output
    assert '"derive_csi800": true' in output
    assert not (tmp_path / "pit_reference" / "baostock").exists()
