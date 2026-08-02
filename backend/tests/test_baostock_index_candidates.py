from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.plugins.baostock.index_candidates import (
    INDEX_CONSTITUENT_CANDIDATES_TABLE,
    BaoStockIndexCandidateCollector,
    _SocketWithTimeout,
    normalize_index_constituent_candidates,
    partition_path,
)
from app.plugins.baostock.socket_proxy import (
    force_proxy_enabled,
    iter_proxy_candidates,
    parse_http_proxy_url,
)
from app.plugins.pit_history.storage import INDEX_MEMBERSHIP_EVENTS_TABLE, table_path
from app.services import pit_reference
from scripts.collect_baostock_hs300_candidates import candidate_dates, main, query_dates


class _LoginResult:
    error_code = "0"
    error_msg = ""


class _BaoStockResult:
    error_code = "0"
    error_msg = ""
    fields = ["date", "code", "code_name"]

    def __init__(self, rows: list[list[str]]) -> None:
        self._rows = rows
        self._idx = -1

    def next(self) -> bool:
        self._idx += 1
        return self._idx < len(self._rows)

    def get_row_data(self) -> list[str]:
        return self._rows[self._idx]


class _FakeBaoStock:
    def __init__(self, by_date: dict[str, list[list[str]]] | None = None) -> None:
        self.by_date = by_date or {}
        self.queries: list[str] = []
        self.logout_count = 0

    def login(self) -> _LoginResult:
        return _LoginResult()

    def logout(self) -> None:
        self.logout_count += 1

    def query_hs300_stocks(self, date: str = "") -> _BaoStockResult:
        self.queries.append(date)
        return _BaoStockResult(self.by_date.get(date, []))


class _FailingCollector:
    def collect_hs300_snapshots(self, snapshot_dates):
        raise RuntimeError("offline")


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


def test_normalize_baostock_hs300_candidates_keeps_snapshot_provenance():
    frame = normalize_index_constituent_candidates(
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
            "provenance": "candidate_snapshot",
        },
        {
            "index_symbol": "000300.SH",
            "member_symbol": "600000.SH",
            "member_code": "600000",
            "member_name": "浦发银行",
            "snapshot_date": date(2020, 1, 3),
            "source_update_date": date(2020, 1, 3),
            "source": "baostock",
            "provenance": "candidate_snapshot",
        },
    ]


def test_baostock_collector_publishes_candidate_snapshots_and_manifest(tmp_path):
    fake = _FakeBaoStock({
        "2020-01-03": [["2020-01-03", "sh.600000", "浦发银行"]],
        "2020-01-06": [["2020-01-06", "sz.000001", "平安银行"]],
    })
    collector = BaoStockIndexCandidateCollector(tmp_path, bs_module=fake)

    rows = collector.collect_hs300_snapshots([date(2020, 1, 6), date(2020, 1, 3)])

    assert rows == 2
    assert fake.queries == ["2020-01-03", "2020-01-06"]
    assert fake.logout_count == 1
    first = pl.read_parquet(
        partition_path(tmp_path, INDEX_CONSTITUENT_CANDIDATES_TABLE, date(2020, 1, 3))
    )
    assert first.select("member_symbol", "provenance").to_dicts() == [
        {"member_symbol": "600000.SH", "provenance": "candidate_snapshot"}
    ]
    manifest = (
        tmp_path
        / "ext_data"
        / "_ingestion"
        / "baostock"
        / INDEX_CONSTITUENT_CANDIDATES_TABLE
        / "000300.SH_2020-01-03_2020-01-06.json"
    )
    assert manifest.exists()
    assert not table_path(tmp_path, INDEX_MEMBERSHIP_EVENTS_TABLE).exists()


def test_pit_reference_status_reports_baostock_candidate_as_non_strict(tmp_path):
    collector = BaoStockIndexCandidateCollector(
        tmp_path,
        bs_module=_FakeBaoStock({
            "2020-01-03": [["2020-01-03", "sh.600000", "浦发银行"]],
        }),
    )
    collector.collect_hs300_snapshots([date(2020, 1, 3)])

    status = pit_reference.get_status(tmp_path)
    candidate = status["snapshots"][INDEX_CONSTITUENT_CANDIDATES_TABLE]

    assert candidate["source"] == "baostock"
    assert candidate["rows"] == 1
    assert candidate["latest_snapshot_date"] == "2020-01-03"
    assert candidate["provenance_counts"] == {"candidate_snapshot": 1}
    assert candidate["candidate_source"]["strict_backtest_usable"] is False
    assert status["summary"]["strict_index_membership_usable"] is False


def test_sync_baostock_candidates_returns_failed_without_partial_claim(tmp_path):
    result = pit_reference.sync_baostock_index_candidates(
        tmp_path,
        snapshot_dates=[date(2020, 1, 3)],
        collector=_FailingCollector(),
    )

    assert result["status"] == "failed"
    assert result["published_rows"] == 0
    assert result["errors"] == ["index_constituent_candidates: offline"]


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
    collector = BaoStockIndexCandidateCollector(tmp_path, bs_module=_BlacklistedBaoStock())

    with pytest.raises(RuntimeError) as exc:
        collector.collect_hs300_snapshots([date(2020, 1, 3)])

    assert "error_code=10001011" in str(exc.value)
    assert "blacklist" in str(exc.value)


def test_baostock_candidate_dates_use_local_trading_dates(tmp_path):
    _write_daily_dates(
        tmp_path,
        [
            date(2020, 12, 31),
            date(2021, 1, 4),
            date(2021, 1, 6),
            date(2021, 1, 8),
        ],
    )

    dates, source = candidate_dates(
        tmp_path,
        start_date=date(2021, 1, 1),
        end_date=date(2021, 1, 6),
        weekday_fallback=False,
    )

    assert dates == [date(2021, 1, 4), date(2021, 1, 6)]
    assert source == "local_trading_dates"


def test_baostock_candidate_dates_require_explicit_weekday_fallback(tmp_path):
    dates, source = candidate_dates(
        tmp_path,
        start_date=date(2021, 1, 1),
        end_date=date(2021, 1, 5),
        weekday_fallback=False,
    )

    assert dates == []
    assert source == "none"

    fallback_dates, fallback_source = candidate_dates(
        tmp_path,
        start_date=date(2021, 1, 1),
        end_date=date(2021, 1, 5),
        weekday_fallback=True,
    )

    assert fallback_dates == [date(2021, 1, 1), date(2021, 1, 4), date(2021, 1, 5)]
    assert fallback_source == "weekday_fallback"


def test_collect_baostock_candidates_dry_run_does_not_publish(tmp_path, capsys):
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
    assert "candidate_dates=2" in output
    assert "query_dates=2" in output
    assert "source=local_trading_dates" in output
    assert not (tmp_path / "pit_reference" / "baostock").exists()


def test_collect_baostock_candidates_dry_run_skips_existing_and_caps_dates(tmp_path, capsys):
    _write_daily_dates(
        tmp_path,
        [date(2021, 1, 4), date(2021, 1, 5), date(2021, 1, 6)],
    )
    existing = partition_path(
        tmp_path,
        INDEX_CONSTITUENT_CANDIDATES_TABLE,
        date(2021, 1, 4),
    )
    existing.parent.mkdir(parents=True)
    pl.DataFrame({"member_symbol": ["000001.SZ"]}).write_parquet(existing)

    selected = query_dates(
        tmp_path,
        [date(2021, 1, 4), date(2021, 1, 5), date(2021, 1, 6)],
        refresh_existing=False,
    )
    assert selected == [date(2021, 1, 5), date(2021, 1, 6)]

    result = main([
        "--data-dir",
        str(tmp_path),
        "--start-date",
        "2021-01-01",
        "--end-date",
        "2021-01-06",
        "--max-dates",
        "1",
        "--dry-run",
    ])

    assert result == 0
    output = capsys.readouterr().out
    assert "candidate_dates=3" in output
    assert "query_dates=1" in output
    assert "skipped_existing=1" in output
    assert "range=2021-01-05..2021-01-05" in output
