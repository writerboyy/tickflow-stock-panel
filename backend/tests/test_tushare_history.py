from __future__ import annotations

from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor
from http.client import IncompleteRead
import io
import json
import os
from pathlib import Path
import threading

import polars as pl
import pytest

from app.services import tushare_history as th
from app.services.tushare_datasets import DATASET_SPECS


class _Response:
    status = 200

    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload


def _payload(fields=("ts_code", "trade_date"), items=(('000001.SZ', '20250101'),)):
    return {"code": 0, "msg": "", "data": {"fields": list(fields), "items": [list(item) for item in items]}}


def test_http_client_validates_protocol_and_redacts_token_from_errors():
    seen = []

    def opener(request, timeout):
        seen.append((request, timeout))
        return _Response(_payload())

    client = th.TushareProxyClient("secret-token", opener=opener, attempts=1, limiter=th.GlobalRateLimiter(0))
    response = client.request("daily", {"ts_code": "000001.SZ"})

    assert response.rows == [{"ts_code": "000001.SZ", "trade_date": "20250101"}]
    body = seen[0][0].data.decode()
    assert "secret-token" in body  # sent over the request, never put in response/manifest
    assert response.raw["code"] == 0
    with pytest.raises(ValueError, match="fixed"):
        th.TushareProxyClient("x", base_url="https://example.com", attempts=1)
    with pytest.raises(ValueError, match="timeout"):
        th.TushareProxyClient("x", timeout=0)


def test_http_client_rejects_field_mismatch_and_empty_is_valid():
    def malformed(request, timeout):
        return _Response({"code": 0, "msg": "", "data": {"fields": ["a"], "items": [[1, 2]]}})

    with pytest.raises(th.TushareProtocolError):
        th.TushareProxyClient("x", opener=malformed, attempts=1, limiter=th.GlobalRateLimiter(0)).request("daily")

    empty = th.TushareProxyClient("x", opener=lambda *_args, **_kwargs: _Response(_payload(items=())), attempts=1, limiter=th.GlobalRateLimiter(0)).request("daily")
    assert empty.items == ()


def test_http_client_retries_429_and_does_not_log_or_expose_key():
    calls = 0

    class TrackingLimiter:
        def __init__(self):
            self.slow_down_calls = 0

        def wait(self):
            pass

        def slow_down(self, **_kwargs):
            self.slow_down_calls += 1

        def recover(self, **_kwargs):
            pass

    class RetryResponse(_Response):
        status = 429

    def opener(request, timeout):
        nonlocal calls
        calls += 1
        if calls < 3:
            return RetryResponse(_payload())
        return _Response(_payload())

    limiter = TrackingLimiter()
    client = th.TushareProxyClient("very-secret", opener=opener, attempts=3, limiter=limiter, backoff=lambda _: None)
    assert client.request("daily").code == 0
    assert calls == 3
    assert limiter.slow_down_calls == 2


def test_http_client_retries_incomplete_response_reads():
    calls = 0
    delays = []

    class TrackingLimiter:
        def __init__(self):
            self.slow_down_calls = 0

        def wait(self):
            pass

        def slow_down(self, **_kwargs):
            self.slow_down_calls += 1

        def recover(self, **_kwargs):
            pass

    class InterruptedResponse(_Response):
        def read(self):
            raise IncompleteRead(b"partial", 100)

    def opener(request, timeout):
        nonlocal calls
        calls += 1
        return InterruptedResponse(_payload()) if calls < 3 else _Response(_payload())

    limiter = TrackingLimiter()
    client = th.TushareProxyClient(
        "very-secret",
        opener=opener,
        attempts=3,
        limiter=limiter,
        backoff=delays.append,
    )
    assert client.request("daily").code == 0
    assert calls == 3
    assert delays == [0.5, 1.0]
    assert limiter.slow_down_calls == 0


def test_http_client_direct_mode_uses_injected_connection_pool():
    class DirectResponse:
        status_code = 200
        content = json.dumps(_payload()).encode()

    class DirectClient:
        def __init__(self):
            self.calls = []
            self.closed = False

        def post(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return DirectResponse()

        def close(self):
            self.closed = True

    transport = DirectClient()
    client = th.TushareProxyClient(
        "secret-token",
        direct=True,
        http_client=transport,
        attempts=1,
        limiter=th.GlobalRateLimiter(0),
    )

    response = client.request("daily", {"ts_code": "000001.SZ"})
    client.close()

    assert response.code == 0
    assert transport.calls[0][0] == th.TUSHARE_PROXY_URL
    assert b"secret-token" in transport.calls[0][1]["content"]
    assert transport.closed is True


def test_global_rate_limiter_is_shared_across_worker_threads(monkeypatch):
    now = 0.0
    sleeps = []

    def monotonic():
        return now

    def sleep(delay):
        nonlocal now
        sleeps.append(delay)
        now += delay

    monkeypatch.setattr(th.time, "monotonic", monotonic)
    monkeypatch.setattr(th.time, "sleep", sleep)
    limiter = th.GlobalRateLimiter(0.2)
    barrier = threading.Barrier(4)

    def wait_once(_index):
        barrier.wait(timeout=2)
        limiter.wait()

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(wait_once, range(4)))

    assert sleeps == pytest.approx([0.2, 0.2, 0.2])


def test_backfill_rate_limit_enforces_provider_ceiling_and_recovers_after_backoff(tmp_path):
    with pytest.raises(ValueError, match="450 requests/minute"):
        th.BackfillConfig(tmp_path, rate_interval=(60 / 450) - 0.001).normalized()

    limiter = th.GlobalRateLimiter(0.2)
    limiter.slow_down(factor=2)
    assert limiter.interval == pytest.approx(0.4)
    limiter.recover()
    assert limiter.interval == pytest.approx(0.32)
    for _ in range(10):
        limiter.recover()
    assert limiter.interval == pytest.approx(0.2)


def test_backfill_worker_count_is_configurable_and_bounded(tmp_path):
    assert th.BackfillConfig(tmp_path, workers=32).normalized().workers == 32
    with pytest.raises(ValueError, match="workers must be between"):
        th.BackfillConfig(tmp_path, workers=65).normalized()


def test_phase_resume_replaces_stale_running_status_with_pending(tmp_path):
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="resume-state", phases=("stock_minute",), symbols=("000001.SZ",)),
        _FakeClient(),
    )
    run._record("stock_minute", "000001.SZ", status="running", pages=3)

    phase = run._start_phase("stock_minute")

    assert phase["status"] == "running"
    assert phase["items"]["000001.SZ"]["status"] == "pending"


def test_normalize_and_forward_adjustment_preserve_volume_and_amount():
    raw = th.normalize_rows([
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 11, "low": 9, "close": 10, "vol": 100, "amount": 1000},
        {"ts_code": "000001.SZ", "trade_time": "2025-01-03 09:31:00", "open": 20, "high": 21, "low": 19, "close": 20, "vol": 200, "amount": 4000},
    ])
    factors = pl.DataFrame({"ts_code": ["000001.SZ", "000001.SZ"], "trade_date": ["2025-01-02", "2025-01-03"], "adj_factor": [1.0, 2.0]})
    adjusted = th.forward_adjust_minutes(raw, factors)
    assert adjusted["close"].to_list() == [5.0, 20.0]
    assert adjusted["volume"].to_list() == [100.0, 200.0]
    assert adjusted["amount"].to_list() == [1000.0, 4000.0]
    events = th.normalize_adjustment_rows(factors)
    assert events["ex_factor"].to_list() == [1.0, 2.0]


def test_tushare_minute_volume_converts_shares_to_canonical_lots():
    raw = th.normalize_rows([
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 12_345, "amount": 123_450},
    ])

    canonical = th.canonicalize_minute_units(raw)

    assert canonical.select("volume", "amount").row(0) == (123.45, 123_450.0)


def test_adjustment_normalization_accepts_compact_tushare_dates():
    factors = pl.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": ["20260802", "20260807"],
        "adj_factor": [1.0, 2.0],
    })

    events = th.normalize_adjustment_rows(factors)

    assert events["trade_date"].to_list() == [date(2026, 8, 2), date(2026, 8, 7)]
    assert events["ex_factor"].to_list() == [1.0, 2.0]


def test_forward_adjustment_uses_compact_tushare_dates():
    raw = th.normalize_rows([
        {"ts_code": "000001.SZ", "trade_time": "2026-08-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 100, "amount": 1000},
    ])
    factors = pl.DataFrame({
        "ts_code": ["000001.SZ", "000001.SZ"],
        "trade_date": ["20260802", "20260807"],
        "adj_factor": [1.0, 2.0],
    })

    adjusted = th.forward_adjust_minutes(raw, factors)

    assert adjusted["close"].to_list() == [5.0]
    assert adjusted["volume"].to_list() == [100.0]


def test_invalid_ohlc_and_conflicting_overlap_fail_closed():
    frame = th.normalize_rows([{"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 8, "low": 9, "close": 10, "vol": 1, "amount": 1}])
    valid, audit = th.validate_minute_frame(frame)
    assert valid.is_empty()
    assert audit[0]["rows"] == 1

    existing = th.normalize_rows([{"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 1}])
    incoming = existing.with_columns(pl.lit(11.0).alias("close"))
    with pytest.raises(th.BackfillBlocked, match="conflicts"):
        th.overlap_merge(existing, incoming)


def test_overlap_merge_keeps_existing_and_adds_only_missing_keys():
    existing = th.normalize_rows([{"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 1}])
    incoming = th.normalize_rows([
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 1},
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:32:00", "open": 11, "high": 11, "low": 11, "close": 11, "vol": 2, "amount": 2},
    ])
    merged, report = th.overlap_merge(existing, incoming)
    assert merged.height == 2
    assert report["added_rows"] == 1


def test_manifest_path_and_disk_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "free_space_bytes", lambda _: 49 * 1024**3)
    with pytest.raises(th.BackfillBlocked, match="free space"):
        th.assert_disk_reserve(tmp_path)
    config = th.BackfillConfig(tmp_path, run_id="sample", phases=("universe",)).normalized()
    assert config.data_dir == Path(tmp_path).resolve()
    assert config.phases == ("universe",)


def test_key_stdin_uses_requested_data_dir_and_mode_0600(tmp_path):
    key = "secret-key-not-in-manifest"
    th.save_tushare_key_from_stdin(io.StringIO(key + "\n"), data_dir=tmp_path)
    path = tmp_path / "user_data" / "secrets.json"
    assert th.load_tushare_key(data_dir=tmp_path) == key
    assert oct(os.stat(path).st_mode & 0o777) == "0o600"
    th.clear_tushare_key(data_dir=tmp_path)
    assert not path.exists()


class _FakeClient:
    def __init__(self):
        self.calls = []

    def request(self, api_name, params):
        self.calls.append((api_name, dict(params)))
        if api_name == "adj_factor":
            return th.TushareResponse(api_name, 0, "", ("ts_code", "trade_date", "adj_factor"), (("000001.SZ", "2025-01-02", 1.0),), {"code": 0, "data": {"fields": ["ts_code", "trade_date", "adj_factor"], "items": [["000001.SZ", "2025-01-02", 1.0]]}})
        return th.TushareResponse(api_name, 0, "", ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"), (("000001.SZ", "2025-01-02 09:31:00", 10, 10, 10, 10, 1, 10),), {"code": 0, "data": {"fields": [], "items": []}})


def test_adjustment_and_minute_phases_are_resumable(tmp_path):
    client = _FakeClient()
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="resume-me", phases=("adjustment", "stock_minute"), symbols=("000001.SZ",)),
        client,
    )
    result = run.run()
    assert result["status"] == "completed"
    assert (tmp_path / "tushare_archive" / "minute_stock_raw" / "symbol=000001.SZ" / "part.parquet").exists()
    assert not list((tmp_path / "backfill_state" / "tushare_proxy" / "resume-me" / "batches" / "stock_minute" / "000001.SZ").glob("page-*.parquet"))
    assert (tmp_path / "backfill_state" / "tushare_proxy" / "resume-me" / "batches" / "adjustment" / "stock" / "000001.SZ.parquet").exists()
    assert json.loads((tmp_path / "backfill_state" / "tushare_proxy" / "resume-me" / "manifest.json").read_text())["phases_state"]["stock_minute"]["items"]["000001.SZ"]["status"] == "completed"
    minute_params = next(params for api, params in client.calls if api == "stk_mins")
    assert minute_params["start_date"] == "2010-01-01 00:00:00"
    assert minute_params["end_date"].count("-") == 2
    item = json.loads((tmp_path / "backfill_state" / "tushare_proxy" / "resume-me" / "manifest.json").read_text())["phases_state"]["stock_minute"]["items"]["000001.SZ"]
    assert item["pages"] == 1
    assert item["cursor"] == "2025-01-02 09:30:00"
    assert item["last_page_hash"]


def test_adjustment_resume_skips_a_completed_empty_response(tmp_path):
    class EmptyAdjustmentClient:
        def __init__(self):
            self.calls = 0

        def request(self, api_name, params):
            self.calls += 1
            fields = ("ts_code", "trade_date", "adj_factor")
            raw = {"code": 0, "data": {"fields": list(fields), "items": []}}
            return th.TushareResponse(api_name, 0, "", fields, (), raw)

    client = EmptyAdjustmentClient()
    config = th.BackfillConfig(
        tmp_path,
        run_id="empty-adjustment",
        phases=("adjustment",),
        symbols=("000001.SZ",),
    )

    first = th.TushareHistoryBackfill(config, client).run()
    second = th.TushareHistoryBackfill(config, client).run()

    assert first["status"] == "completed"
    assert second["status"] == "completed"
    assert client.calls == 1
    item = second["phases_state"]["adjustment"]["items"]["000001.SZ"]
    assert item["status"] == "completed"
    assert item["empty"] is True


def test_new_run_reuses_completed_adjustment_batch(tmp_path):
    prior_root = tmp_path / "backfill_state/tushare_proxy/prior"
    source = prior_root / "batches/adjustment/stock/000001.SZ.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20250102"], "adj_factor": [1.0]}).write_parquet(source)
    th._atomic_json(
        prior_root / "manifest.json",
        {
            "schema_version": 1,
            "kind": "tushare_proxy_history_backfill",
            "run_id": "prior",
            "phases_state": {
                "adjustment": {
                    "status": "completed",
                    "items": {"000001.SZ": {"status": "completed", "rows": 1, "empty": False}},
                }
            },
        },
    )

    class NoNetworkClient:
        def request(self, *_args, **_kwargs):
            raise AssertionError("reused adjustment must not access the network")

    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="current",
            reuse_run_id="prior",
            phases=("adjustment",),
            symbols=("000001.SZ",),
        ),
        NoNetworkClient(),
    )
    run._fetch_adjustment("stock", "adj_factor", ["000001.SZ"])

    target = run.run_root / "batches/adjustment/stock/000001.SZ.parquet"
    assert target.exists()
    assert run.manifest["phases_state"]["adjustment"]["items"]["000001.SZ"]["reused_from_run"] == "prior"


def test_new_run_reuses_and_clips_completed_minute_raw_without_overwriting_source(tmp_path):
    prior_root = tmp_path / "backfill_state/tushare_proxy/prior"
    th._atomic_json(
        prior_root / "manifest.json",
        {
            "schema_version": 1,
            "kind": "tushare_proxy_history_backfill",
            "run_id": "prior",
            "phases_state": {
                "stock_minute": {
                    "status": "running",
                    "items": {"000001.SZ": {"status": "completed", "rows": 2}},
                }
            },
        },
    )
    source = tmp_path / "tushare_archive/minute_stock_raw/symbol=000001.SZ/part.parquet"
    source.parent.mkdir(parents=True)
    th.normalize_rows(
        [
            {"ts_code": "000001.SZ", "trade_time": "2019-12-31 15:00:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 10},
            {"ts_code": "000001.SZ", "trade_time": "2020-01-02 15:00:00", "open": 11, "high": 11, "low": 11, "close": 11, "vol": 1, "amount": 11},
        ],
        asset_type="stock",
    ).write_parquet(source)

    class NoNetworkClient:
        def request(self, *_args, **_kwargs):
            raise AssertionError("covered minute history must not access the network")

    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="current",
            reuse_run_id="prior",
            phases=("stock_minute",),
            symbols=("000001.SZ",),
            start=date(2020, 1, 1),
            end=date(2020, 1, 2),
        ),
        NoNetworkClient(),
    )
    run._fetch_minutes("stock", "stk_mins", ["000001.SZ"])

    target = run.run_root / "raw/minute_stock_raw/symbol=000001.SZ/part.parquet"
    assert pl.read_parquet(source).height == 2
    reused = pl.read_parquet(target)
    assert reused.height == 1
    assert reused["datetime"].min() == datetime(2020, 1, 2, 15, 0)
    assert run.manifest["phases_state"]["stock_minute"]["items"]["000001.SZ"]["reused_from_run"] == "prior"


def test_minute_pagination_uses_the_next_cursor_for_each_request(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "MAX_MINUTE_ROWS", 2)

    class CursorClient:
        def __init__(self):
            self.calls = []

        def request(self, api_name, params):
            self.calls.append((api_name, dict(params)))
            fields = ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount")
            if len(self.calls) == 1:
                items = (
                    ("000001.SZ", "2025-01-02 09:32:00", 10, 10, 10, 10, 1, 10),
                    ("000001.SZ", "2025-01-02 09:33:00", 10, 10, 10, 10, 1, 10),
                )
            else:
                items = (("000001.SZ", "2025-01-02 09:31:00", 10, 10, 10, 10, 1, 10),)
            raw = {"code": 0, "data": {"fields": list(fields), "items": [list(item) for item in items]}}
            return th.TushareResponse(api_name, 0, "", fields, items, raw)

    client = CursorClient()
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="cursor-advance", phases=("stock_minute",), symbols=("000001.SZ",)),
        client,
    )

    result = run.run()

    assert result["status"] == "completed"
    assert [call[1]["end_date"] for call in client.calls] == [
        f"{date.today().isoformat()} 23:59:59",
        "2025-01-02 09:31:00",
    ]


def test_minute_pagination_rejects_a_page_that_ignores_the_cursor(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "MAX_MINUTE_ROWS", 2)

    class RepeatingPageClient:
        def __init__(self):
            self.calls = []

        def request(self, api_name, params):
            self.calls.append((api_name, dict(params)))
            fields = ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount")
            if len(self.calls) == 1:
                items = (
                    ("000001.SZ", "2025-01-02 09:32:00", 10, 10, 10, 10, 1, 10),
                    ("000001.SZ", "2025-01-02 09:33:00", 10, 10, 10, 10, 1, 10),
                )
            else:
                items = (
                    ("000001.SZ", "2025-01-02 09:30:00", 10, 10, 10, 10, 1, 10),
                    ("000001.SZ", "2025-01-02 09:32:00", 10, 10, 10, 10, 1, 10),
                )
            raw = {"code": 0, "data": {"fields": list(fields), "items": [list(item) for item in items]}}
            return th.TushareResponse(api_name, 0, "", fields, items, raw)

    client = RepeatingPageClient()
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="repeated-page", phases=("stock_minute",), symbols=("000001.SZ",)),
        client,
    )

    result = run.run()

    assert result["status"] == "incomplete"
    assert len(client.calls) == 2
    item = result["phases_state"]["stock_minute"]["items"]["000001.SZ"]
    assert item["status"] == "failed"
    assert item["error"] == "BackfillBlocked"
    assert "after requested cursor" in item["message"]


def test_adjustment_and_minute_phases_use_four_workers(tmp_path):
    symbols = tuple(f"00000{index}.SZ" for index in range(1, 5))

    class ParallelClient:
        def __init__(self):
            self.barriers = {
                "adj_factor": threading.Barrier(4),
                "stk_mins": threading.Barrier(4),
            }
            self.threads = {"adj_factor": set(), "stk_mins": set()}
            self.lock = threading.Lock()

        def request(self, api_name, params):
            symbol = params["ts_code"]
            with self.lock:
                self.threads[api_name].add(threading.current_thread().name)
            self.barriers[api_name].wait(timeout=2)
            if api_name == "adj_factor":
                fields = ("ts_code", "trade_date", "adj_factor")
                items = ((symbol, "2025-01-02", 1.0),)
            else:
                fields = ("ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount")
                items = ((symbol, "2025-01-02 09:31:00", 10, 10, 10, 10, 1, 10),)
            raw = {"code": 0, "data": {"fields": list(fields), "items": [list(items[0])]}}
            return th.TushareResponse(api_name, 0, "", fields, items, raw)

    client = ParallelClient()
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="four-workers", phases=("adjustment", "stock_minute"), symbols=symbols),
        client,
    )
    result = run.run()

    assert result["status"] == "completed"
    assert len(client.threads["adj_factor"]) == 4
    assert len(client.threads["stk_mins"]) == 4
    manifest = json.loads((tmp_path / "backfill_state" / "tushare_proxy" / "four-workers" / "manifest.json").read_text())
    for symbol in symbols:
        assert manifest["phases_state"]["adjustment"]["items"][symbol]["status"] == "completed"
        assert manifest["phases_state"]["stock_minute"]["items"][symbol]["status"] == "completed"


def test_manifest_updates_are_thread_safe(tmp_path):
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="thread-safe", phases=("stock_minute",), symbols=("000001.SZ",)),
        _FakeClient(),
    )

    def record(index):
        run._record("stock_minute", f"symbol-{index}", status="running", rows=index)

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(record, range(40)))

    manifest = json.loads(run.manifest_path.read_text())
    items = manifest["phases_state"]["stock_minute"]["items"]
    assert len(items) == 40
    assert items["symbol-39"]["rows"] == 39


def test_v1_manifest_resumes_without_discarding_completed_state(tmp_path):
    run_root = tmp_path / "backfill_state/tushare_proxy/v1-run"
    run_root.mkdir(parents=True)
    manifest = {
        "schema_version": 1,
        "kind": "tushare_proxy_history_backfill",
        "run_id": "v1-run",
        "data_dir": str(tmp_path.resolve()),
        "phases": ["adjustment"],
        "requested_symbols_hash": th._symbol_hash(["000001.SZ"]),
        "phases_state": {"adjustment": {"status": "completed", "items": {}}},
    }
    (run_root / "manifest.json").write_text(json.dumps(manifest))

    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="v1-run",
            phases=("adjustment",),
            symbols=("000001.SZ",),
        ),
        _FakeClient(),
    )

    assert run.manifest["schema_version"] == 1
    assert run.manifest["phases_state"]["adjustment"]["status"] == "completed"
    assert run.manifest["history_start"] == "2010-01-01"


def test_v2_manifest_rejects_changed_history_or_dataset_scope(tmp_path):
    th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="strict-resume",
            phases=("universe",),
            symbols=("000001.SZ",),
            datasets=("income",),
            start=date(2025, 1, 1),
            end=date(2025, 12, 31),
        ),
        _FakeClient(),
    )

    with pytest.raises(th.BackfillBlocked, match="history_start"):
        th.TushareHistoryBackfill(
            th.BackfillConfig(
                tmp_path,
                run_id="strict-resume",
                phases=("universe",),
                symbols=("000001.SZ",),
                datasets=("income",),
                start=date(2024, 1, 1),
                end=date(2025, 12, 31),
            ),
            _FakeClient(),
        )

    with pytest.raises(th.BackfillBlocked, match="datasets"):
        th.TushareHistoryBackfill(
            th.BackfillConfig(
                tmp_path,
                run_id="strict-resume",
                phases=("universe",),
                symbols=("000001.SZ",),
                datasets=("cashflow",),
                start=date(2025, 1, 1),
                end=date(2025, 12, 31),
            ),
            _FakeClient(),
        )


def test_formal_run_can_select_one_dataset_and_preserves_rich_matrix(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "assert_disk_reserve", lambda *_args, **_kwargs: None)

    class FormalClient:
        _token = "not-secret"

        def request(self, api_name, params):
            assert api_name == "moneyflow"
            fields = ("ts_code", "trade_date", "net_mf_vol", "net_mf_amount")
            items = (("000001.SZ", "20250102", 1, 2),)
            raw = {"code": 0, "data": {"fields": list(fields), "items": [list(items[0])]}}
            return th.TushareResponse(api_name, 0, "", fields, items, raw)

    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="formal-one",
            phases=("universe",),
            symbols=("000001.SZ",),
            datasets=("moneyflow",),
            start=date(2025, 1, 1),
            end=date(2025, 12, 31),
        ),
        FormalClient(),
    )

    result = run.run()

    assert result["status"] == "completed"
    matrix = json.loads((run.run_root / "capability_matrix.json").read_text())
    assert matrix["datasets"]["moneyflow"]["symbols"] == 1
    assert matrix["datasets"]["moneyflow"]["field_non_null_rate"]["net_mf_amount"] == 1.0
    assert matrix["legacy_phases"]["stock_minute_raw"] is None


def test_derived_retry_rebuilds_stale_enriched_share_schema(tmp_path):
    daily = tmp_path / "kline_daily/date=2025-01-02/part.parquet"
    daily.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2025, 1, 2)],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "close": [10.0],
        "volume": [100.0],
        "amount": [1_000.0],
    }).write_parquet(daily)
    instruments = tmp_path / "instruments/instruments.parquet"
    instruments.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"]}).write_parquet(instruments)
    shares = tmp_path / "financials/shares/part.parquet"
    shares.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "period_end": ["2025-01-02"],
        "announce_date": ["2025-01-02"],
        "total_shares": [1_000_000.0],
        "float_shares": [800_000.0],
    }).write_parquet(shares)
    stale = tmp_path / "kline_daily_enriched/date=2025-01-02/part.parquet"
    stale.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["000001.SZ"],
        "date": [date(2025, 1, 2)],
        "raw_close": [10.0],
    }).write_parquet(stale)
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="derived-retry", phases=("universe",)),
        _FakeClient(),
    )

    report = run._rebuild_after_formal_publish(
        (DATASET_SPECS["income"],),
        {"income": {"publish": {"status": "published", "published_rows": 1}}},
    )

    rebuilt = pl.read_parquet(stale)
    assert report["status"] == "completed"
    assert report["recovery_dependencies"] == ["historical_share_columns"]
    assert rebuilt.select("total_shares", "float_shares").row(0) == (
        1_000_000.0,
        800_000.0,
    )
    assert report["valuation_daily"]["rows"] == 1


def test_audit_only_does_not_query_network(tmp_path, monkeypatch):
    monkeypatch.setattr(th, "assert_disk_reserve", lambda *_args, **_kwargs: None)

    class NoNetworkClient:
        _token = "not-secret"

        def request(self, *_args, **_kwargs):
            raise AssertionError("audit-only mode must not query the provider")

    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="audit-only",
            phases=("audit",),
            datasets=("moneyflow",),
        ),
        NoNetworkClient(),
    )

    result = run.run()

    assert result["dataset_audit"]["status"] == "unhealthy"
    assert result["status"] == "incomplete"


def test_universe_fetches_stock_statuses_separately(tmp_path):
    class UniverseClient:
        def __init__(self):
            self.statuses = []

        def request(self, api_name, params):
            if api_name == "stock_basic":
                status = params["list_status"]
                self.statuses.append(status)
                rows = ((f"00000{len(self.statuses)}.SZ", f"stock-{status}"),)
                return th.TushareResponse(api_name, 0, "", ("ts_code", "name"), rows, {"code": 0, "data": {"fields": ["ts_code", "name"], "items": rows}})
            if api_name == "etf_basic":
                rows = (("510300.SH", "ETF"),)
                return th.TushareResponse(api_name, 0, "", ("ts_code", "name"), rows, {"code": 0, "data": {"fields": ["ts_code", "name"], "items": rows}})
            return th.TushareResponse(api_name, 0, "", (), (), {"code": 0, "data": {"fields": [], "items": []}})

    client = UniverseClient()
    result = th.TushareHistoryBackfill(
        th.BackfillConfig(tmp_path, run_id="universe", phases=("universe",)), client
    ).run()
    assert client.statuses == ["L", "D", "P"]
    assert result["symbols"]["stocks"] == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert result["symbols"]["etfs"] == ["510300.SH"]


def test_publish_checks_all_partitions_before_replacing_any(tmp_path):
    run = th.TushareHistoryBackfill(th.BackfillConfig(tmp_path, run_id="atomic", phases=("publish_minute",), symbols=("000001.SZ",)), _FakeClient())
    raw_root = tmp_path / "tushare_archive" / "minute_stock_raw"
    first = th.normalize_rows([{"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 1}])
    second = th.normalize_rows([{"ts_code": "000001.SZ", "trade_time": "2025-01-03 09:31:00", "open": 20, "high": 20, "low": 20, "close": 20, "vol": 1, "amount": 1}])
    (raw_root / "symbol=000001.SZ").mkdir(parents=True)
    pl.concat([first, second]).write_parquet(raw_root / "symbol=000001.SZ" / "part.parquet")
    first_target = tmp_path / "kline_minute" / "date=2025-01-02" / "part.parquet"
    second_target = tmp_path / "kline_minute" / "date=2025-01-03" / "part.parquet"
    original_first = th.canonicalize_minute_units(first).with_columns(pl.lit(10.0).alias("close"))
    original_second = th.canonicalize_minute_units(second).with_columns(pl.lit(19.0).alias("close"))
    first_target.parent.mkdir(parents=True)
    second_target.parent.mkdir(parents=True)
    original_first.write_parquet(first_target)
    original_second.write_parquet(second_target)
    with pytest.raises(th.BackfillBlocked, match="conflicts"):
        run.publish_minutes(("stock",))
    assert pl.read_parquet(first_target)["close"].to_list() == [10.0]
    assert pl.read_parquet(second_target)["close"].to_list() == [19.0]


def test_minute_publish_rolls_back_all_partitions_on_replace_failure(tmp_path, monkeypatch):
    run = th.TushareHistoryBackfill(
        th.BackfillConfig(
            tmp_path,
            run_id="minute-rollback",
            phases=("publish_minute",),
            symbols=("000001.SZ",),
        ),
        _FakeClient(),
    )
    raw_root = tmp_path / "tushare_archive/minute_stock_raw/symbol=000001.SZ"
    raw_root.mkdir(parents=True)
    raw = th.normalize_rows([
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:31:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 10},
        {"ts_code": "000001.SZ", "trade_time": "2025-01-02 09:32:00", "open": 10, "high": 10, "low": 10, "close": 10, "vol": 1, "amount": 10},
        {"ts_code": "000001.SZ", "trade_time": "2025-01-03 09:31:00", "open": 20, "high": 20, "low": 20, "close": 20, "vol": 1, "amount": 20},
        {"ts_code": "000001.SZ", "trade_time": "2025-01-03 09:32:00", "open": 20, "high": 20, "low": 20, "close": 20, "vol": 1, "amount": 20},
    ])
    raw.write_parquet(raw_root / "part.parquet")
    first_target = tmp_path / "kline_minute/date=2025-01-02/part.parquet"
    second_target = tmp_path / "kline_minute/date=2025-01-03/part.parquet"
    first_target.parent.mkdir(parents=True)
    second_target.parent.mkdir(parents=True)
    canonical = th.canonicalize_minute_units(raw)
    canonical.filter(pl.col("datetime").dt.date() == date(2025, 1, 2)).head(1).select(
        th._MINUTE_FIELDS
    ).write_parquet(first_target)
    canonical.filter(pl.col("datetime").dt.date() == date(2025, 1, 3)).head(1).select(
        th._MINUTE_FIELDS
    ).write_parquet(second_target)
    replace = th.os.replace

    def fail_second_target(source, target):
        if Path(target) == second_target and "publish_staging/minute" in str(source):
            raise OSError("injected replace failure")
        return replace(source, target)

    monkeypatch.setattr(th.os, "replace", fail_second_target)

    with pytest.raises(OSError, match="injected"):
        run.publish_minutes(("stock",))

    assert pl.read_parquet(first_target).height == 1
    assert pl.read_parquet(second_target).height == 1
