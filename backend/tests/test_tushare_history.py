from __future__ import annotations

from datetime import datetime
import io
import json
import os
from pathlib import Path

import polars as pl
import pytest

from app.services import tushare_history as th


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


def test_http_client_rejects_field_mismatch_and_empty_is_valid():
    def malformed(request, timeout):
        return _Response({"code": 0, "msg": "", "data": {"fields": ["a"], "items": [[1, 2]]}})

    with pytest.raises(th.TushareProtocolError):
        th.TushareProxyClient("x", opener=malformed, attempts=1, limiter=th.GlobalRateLimiter(0)).request("daily")

    empty = th.TushareProxyClient("x", opener=lambda *_args, **_kwargs: _Response(_payload(items=())), attempts=1, limiter=th.GlobalRateLimiter(0)).request("daily")
    assert empty.items == ()


def test_http_client_retries_429_and_does_not_log_or_expose_key():
    calls = 0

    class RetryResponse(_Response):
        status = 429

    def opener(request, timeout):
        nonlocal calls
        calls += 1
        if calls < 3:
            return RetryResponse(_payload())
        return _Response(_payload())

    client = th.TushareProxyClient("very-secret", opener=opener, attempts=3, limiter=th.GlobalRateLimiter(0), backoff=lambda _: None)
    assert client.request("daily").code == 0
    assert calls == 3


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
    assert minute_params["start_date"] == "1990-01-01 00:00:00"
    assert minute_params["end_date"].count("-") == 2


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
    original_first = first.with_columns(pl.lit(10.0).alias("close"))
    original_second = second.with_columns(pl.lit(19.0).alias("close"))
    first_target.parent.mkdir(parents=True)
    second_target.parent.mkdir(parents=True)
    original_first.write_parquet(first_target)
    original_second.write_parquet(second_target)
    with pytest.raises(th.BackfillBlocked, match="conflicts"):
        run.publish_minutes(("stock",))
    assert pl.read_parquet(first_target)["close"].to_list() == [10.0]
    assert pl.read_parquet(second_target)["close"].to_list() == [19.0]
