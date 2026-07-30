"""自定义分钟数据源路由回归测试。

对应设计文档 §4 测试矩阵 (docs/superpowers/specs/2026-07-18-minute-provider-unification-design.md)。

覆盖三个阻断问题:
1. stock-sdk 默认 freq 漂移 (5m → 1m)
2. 自定义源异常直接 500 (无 try/except)
3. 插件化路由重复 + asset_type 未透传

mock 范式沿用 test_stocksdk_provider.py (monkeypatch 模块属性)。
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import httpx
import polars as pl
import pytest

from app.plugins.stocksdk import provider as sp
from app.plugins.stocksdk.provider import StockSDKProvider
from app.services import kline_sync
from app.tickflow.repository import DataStore, KlineRepository


# ---------- 辅助 ----------

def _mock_minute_df(symbol: str = "600519.SH") -> pl.DataFrame:
    """构造非空分钟 K df, 用于 mock provider.get_minute 返回值。"""
    return pl.DataFrame({
        "symbol": [symbol],
        "datetime": [datetime(2026, 1, 15, 9, 35, 0)],
        "open": [100.0],
        "high": [101.0],
        "low": [99.5],
        "close": [100.5],
        "volume": [1000.0],
        "amount": [100500.0],
    })


def _setup_custom_provider(monkeypatch, provider: object, has_dataset: bool = True) -> None:
    """统一 mock 自定义分钟源路由前置: preferences + provider_has_dataset + get_provider。

    - preferences.get_minute_data_provider → "mock_src"
    - custom.provider_has_dataset → has_dataset
    - custom.get_provider → provider
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: has_dataset,
    )
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: provider,
    )


def test_tickflow_minute_epoch_is_normalized_to_beijing_wall_clock():
    """TickFlow epoch 01:35 UTC 必须以 09:35 北京时间存储。"""
    raw = pl.DataFrame({
        "symbol": ["510300.SH"],
        "timestamp": [1779327300000],
        "open": [4.0], "high": [4.1], "low": [3.9], "close": [4.05],
        "volume": [100.0], "amount": [405.0],
    })
    normalized = kline_sync._normalize_minute(raw)
    value = normalized["datetime"][0]
    assert (value.hour, value.minute) == (9, 35)


def test_write_minute_partition_drops_invalid_rows_and_normalizes_ohlc(tmp_path):
    minute_dir = tmp_path / "kline_etf_minute"
    frame = pl.DataFrame({
        "symbol": ["510300.SH", "159509.SZ"],
        "datetime": [datetime(2026, 7, 21, 9, 30), datetime(2026, 7, 21, 9, 30)],
        "open": [4.0, None],
        "high": [4.05 - 1e-13, None],
        "low": [4.0 + 1e-13, None],
        "close": [4.05, None],
        "volume": [100.0, 100.0],
        "amount": [405.0, 0.0],
    })

    written = kline_sync._write_minute_partition(frame, minute_dir)
    stored = pl.read_parquet(minute_dir / "date=2026-07-21" / "part.parquet")

    assert written == 1
    assert stored["symbol"].to_list() == ["510300.SH"]
    assert stored["high"][0] == 4.05
    assert stored["low"][0] == 4.0


def test_write_minute_partition_cleans_existing_rows_when_merging(tmp_path):
    minute_dir = tmp_path / "kline_etf_minute"
    partition = minute_dir / "date=2026-07-21" / "part.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame({
        "symbol": ["159509.SZ"],
        "datetime": [datetime(2026, 7, 21, 9, 30)],
        "open": [None], "high": [None], "low": [None], "close": [None],
        "volume": [0.0], "amount": [0.0],
    }).write_parquet(partition)

    written = kline_sync._write_minute_partition(
        _mock_minute_df("510300.SH").with_columns(
            pl.lit(datetime(2026, 7, 21, 9, 31)).alias("datetime"),
        ),
        minute_dir,
    )
    stored = pl.read_parquet(partition)

    assert written == 1
    assert stored["symbol"].to_list() == ["510300.SH"]


def test_scheduled_minute_queries_read_only_explicit_date_partitions(tmp_path, monkeypatch):
    rows = pl.DataFrame({
        "symbol": ["600519.SH", "600519.SH", "600519.SH", "000001.SZ"],
        "datetime": [
            datetime(2026, 1, 15, 9, 59),
            datetime(2026, 1, 15, 10, 0),
            datetime(2026, 1, 15, 10, 1),
            datetime(2026, 1, 15, 10, 0),
        ],
        "open": [99.0, 100.0, 101.0, 10.0],
        "high": [99.0, 100.0, 101.0, 10.0],
        "low": [99.0, 100.0, 101.0, 10.0],
        "close": [99.0, 100.0, 101.0, 10.0],
        "volume": [40.0, 60.0, 100.0, 100.0],
        "amount": [3_960.0, 6_000.0, 10_100.0, 1_000.0],
    })
    target = tmp_path / "kline_minute" / "date=2026-01-15" / "part.parquet"
    target.parent.mkdir(parents=True)
    rows.write_parquet(target)
    repo = KlineRepository(DataStore(tmp_path))
    unrelated = tmp_path / "kline_minute" / "date=2026-01-16" / "part.parquet"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"")

    scanned: list[list[str]] = []
    original_scan = pl.scan_parquet

    def recording_scan(source, *args, **kwargs):
        values = [str(item) for item in source] if isinstance(source, list) else [str(source)]
        scanned.append(values)
        return original_scan(source, *args, **kwargs)

    monkeypatch.setattr(pl, "scan_parquet", recording_scan)

    available = repo.get_minute_symbols("stock", date(2026, 1, 15), date(2026, 1, 15))
    snapshot = repo.get_minute_snapshot(
        ["600519.SH"], datetime(2026, 1, 15, 10, 0, 30), "stock",
    )
    following = repo.get_minute_next(
        ["600519.SH"],
        datetime(2026, 1, 15, 10, 0),
        datetime(2026, 1, 15, 10, 2),
        "stock",
    )

    assert available == {"600519.SH", "000001.SZ"}
    assert snapshot["close"].to_list() == [100.0]
    assert snapshot["session_volume"].to_list() == [100.0]
    assert following["close"].to_list() == [101.0]
    assert scanned == [[str(target)], [str(target)], [str(target)]]


# ---------- 测试 1: 自定义源成功返回 1 分钟 K ----------

def test_custom_minute_provider_returns_1m_k(monkeypatch):
    """§4 测试 1: 自定义源成功返回 1m K, 且 provider 收到 freq="1m"。"""
    spy = MagicMock(return_value=_mock_minute_df())
    mock_provider = MagicMock()
    mock_provider.get_minute = spy
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"],
        datetime(2026, 1, 15, 9, 25, 0),
        datetime(2026, 1, 15, 15, 5, 0),
        asset_type="stock",
    )

    assert fallback is False
    assert df is not None
    assert not df.is_empty()
    # spy 收到 freq="1m" 和 asset_type="stock"
    spy.assert_called_once()
    _, kwargs = spy.call_args
    assert kwargs.get("freq") == "1m"
    assert kwargs.get("asset_type") == "stock"


# ---------- 测试 2: stock-sdk 收到 freq=1m → bridge job period="1" ----------

def test_stocksdk_get_minute_receives_freq_1m(monkeypatch):
    """§4 测试 2: StockSDKProvider.get_minute(freq="1m") → bridge job period == "1"。

    bridge.mjs opMinute 用 String(period), 1m → "1"。
    """
    captured: dict = {}

    def fake_run_job(job, timeout=None):
        captured["job"] = job
        # 返回空结果, 测试只验证 job.period
        return {"ok": True, "op": job["op"], "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake_run_job)

    StockSDKProvider().get_minute(
        ["600519.SH"], None, None, freq="1m",
    )

    assert captured["job"]["op"] == "minute"
    assert captured["job"]["period"] == "1"


# ---------- 测试 3: 自定义源异常 + TickFlow 也失败 → 返回空 (非 500) ----------

def test_custom_provider_exception_no_500(monkeypatch):
    """§4 测试 3: 自定义源抛异常 + TickFlow 也失败,
    fetch_minute_single / sync_minute_batch 返回空 df。
    """
    # 自定义源抛异常
    mock_provider = MagicMock()
    mock_provider.get_minute.side_effect = httpx.TimeoutException("timeout")
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    # mock get_client 返回 mock client, 其 klines.batch raise (TickFlow 也失败)
    mock_tf = MagicMock()
    mock_tf.klines.batch.side_effect = Exception("tickflow fail")
    monkeypatch.setattr(kline_sync, "get_client", lambda: mock_tf)

    # fetch_minute_single: 自定义源异常 → fall through → TickFlow 异常 → 返回空
    df_single = kline_sync.fetch_minute_single(
        "600519.SH", date(2026, 1, 15), asset_type="stock",
    )
    assert isinstance(df_single, pl.DataFrame)
    assert df_single.is_empty()

    # sync_minute_batch: 同一路径, 返回空
    df_batch = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        asset_type="stock",
    )
    assert isinstance(df_batch, pl.DataFrame)
    assert df_batch.is_empty()


# ---------- 测试 4: 未配 minute dataset → 回退 TickFlow ----------

def test_provider_without_minute_dataset_fallback(monkeypatch):
    """§4 测试 4: provider_has_dataset 返回 False → (None, True) 回退 TickFlow。"""
    mock_provider = MagicMock()
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=False)

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"], None, None, asset_type="stock",
    )

    assert fallback is True
    assert df is None
    # provider.get_minute 不应被调用 (回退决策在前)
    mock_provider.get_minute.assert_not_called()


# ---------- 测试 5: asset_type 透传到 provider ----------

def test_asset_type_threaded_to_provider(monkeypatch):
    """§4 测试 5: stock/etf/index asset_type 透传到 provider.get_minute。"""
    spy = MagicMock(return_value=_mock_minute_df())
    mock_provider = MagicMock()
    mock_provider.get_minute = spy
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    # 三次调用不同 asset_type
    kline_sync.fetch_minute_single("600519.SH", date(2026, 1, 15), asset_type="stock")
    kline_sync.fetch_minute_single("510300.SH", date(2026, 1, 15), asset_type="etf")
    kline_sync.fetch_minute_single("000001.SH", date(2026, 1, 15), asset_type="index")

    # spy 被调 3 次, 每次收到对应 asset_type
    assert spy.call_count == 3
    received_assets = [call.kwargs.get("asset_type") for call in spy.call_args_list]
    assert received_assets == ["stock", "etf", "index"]


# ---------- 测试 6: 自定义源成功时不调 TickFlow ----------

def test_custom_success_skips_tickflow(monkeypatch):
    """§4 测试 6: fetch_minute_single 自定义源成功 → 不调 get_client。"""
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    # get_client 设为 spy, 若被调说明路由失败
    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    df = kline_sync.fetch_minute_single(
        "600519.SH", date(2026, 1, 15), asset_type="stock",
    )

    # 返回的是 mock provider 的 df
    assert df is expected_df
    # TickFlow 路径未进入
    get_client_spy.assert_not_called()


# ---------- 测试 7: sync_minute_batch 自定义源成功直接返回 ----------

def test_sync_minute_batch_custom_success_returns_directly(monkeypatch):
    """§4 测试 7: sync_minute_batch 自定义源成功 + 未传 on_segment → 原样返回 df (实时补拉契约)。

    传了 on_segment 时走流式落盘分支 (见测试 10), 此处验证未传时的实时补拉契约。
    """
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    df = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        asset_type="stock",
    )

    # 返回 mock provider 的 df, 不走 segment
    assert df is expected_df
    get_client_spy.assert_not_called()


# ---------- 测试 8: on_chunk_done 包装 (2参 → 3参补 seg_label='custom') ----------

def test_on_chunk_done_wrapped_to_3_args(monkeypatch):
    """on_chunk_done 包装: provider 内部以 2 参 (cur, total) 调用 →
    上层 3 参 (cur, total, seg_label) spy 收到 seg_label='custom'。

    设计文档 §2: 保证自定义源路径进度展示不降级 (与 TickFlow 路径 3 参回调对齐)。
    """
    upper_cb = MagicMock(name="upper_3arg_cb")

    def provider_get_minute_side_effect(symbols, *, start_time, end_time,
                                        asset_type, freq, on_chunk_done):
        # 模拟 provider 实现内部以 2 参调用 on_chunk_done
        # (如 GenericHTTPProvider/provider.py:127 / StockSDKProvider/provider.py:166)
        if on_chunk_done is not None:
            on_chunk_done(1, 3)
        return _mock_minute_df()

    mock_provider = MagicMock()
    mock_provider.get_minute.side_effect = provider_get_minute_side_effect
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"],
        datetime(2026, 1, 15, 9, 25, 0),
        datetime(2026, 1, 15, 15, 5, 0),
        asset_type="stock",
        on_chunk_done=upper_cb,
    )

    assert fallback is False
    assert df is not None
    # 上层 3 参 spy 被调用一次, 收到 (1, 3, "custom")
    upper_cb.assert_called_once_with(1, 3, "custom")


# ---------- 测试 9: get_minute_batch 按 asset_type 拆分调用 sync_minute_batch ----------

def test_get_minute_batch_splits_stock_and_etf(monkeypatch):
    """get_minute_batch 把 incomplete 拆成 stock/ETF 两组, 分别以
    asset_type='stock'/'etf' 调用 sync_minute_batch, 结果 concat 返回。

    覆盖 kline.py get_minute_batch 的双调用拼接逻辑 (本次提交改动量最大的部分)。
    契约: 本端点只接受 stock/ETF (指数走 /api/index/minute), 故两分支覆盖全部 incomplete。
    """
    from app.api import kline as kline_api

    # mock sync_minute_batch: stock 返回 df_s, etf 返回 df_e (不同 symbol 便于 concat 后 filter 验证)
    def fake_sync(symbols, *, start_time, end_time, batch_size, rpm, asset_type):
        if asset_type == "stock":
            return _mock_minute_df(symbol="600519.SH")
        if asset_type == "etf":
            return _mock_minute_df(symbol="510300.SH")
        return pl.DataFrame()
    sync_spy = MagicMock(side_effect=fake_sync)
    monkeypatch.setattr(kline_api.kline_sync, "sync_minute_batch", sync_spy)

    # mock repo: ETF 集合含 510300.SH; 本地分钟K返回空 (强制走 incomplete 补拉)
    mock_repo = MagicMock()
    mock_repo.get_etf_symbol_set.return_value = {"510300.SH"}
    mock_repo.get_minute_batch.return_value = pl.DataFrame()

    # mock capset: 有权限, limits 返回 None (lim.batch 访问被 `if lim else` 守护)
    mock_capset = MagicMock()
    mock_capset.has.return_value = True
    mock_capset.limits.return_value = None

    mock_request = MagicMock()
    mock_request.app.state.repo = mock_repo
    mock_request.app.state.capabilities = mock_capset

    body = {"symbols": ["600519.SH", "510300.SH"], "date": "2026-01-15"}
    result = kline_api.get_minute_batch(mock_request, body)

    # sync_minute_batch 被调 2 次, asset_type 分别为 stock 和 etf
    assert sync_spy.call_count == 2
    call_assets = sorted(call.kwargs.get("asset_type") for call in sync_spy.call_args_list)
    assert call_assets == ["etf", "stock"]

    # 两个 symbol 都在结果里 (concat 后按 symbol filter 命中)
    assert "600519.SH" in result["data"]
    assert "510300.SH" in result["data"]


# ---------- 测试 10: sync_minute_batch 自定义源成功时调 on_segment (Issue 1) ----------

def test_sync_minute_batch_custom_calls_on_segment(monkeypatch):
    """Issue 1: sync_minute_batch 自定义源成功 + 传了 on_segment →
    调 on_segment(df), 返回空 df (数据已落盘)。
    """
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    on_segment_spy = MagicMock(name="on_segment_spy")
    df = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        on_segment=on_segment_spy,
        asset_type="stock",
    )

    on_segment_spy.assert_called_once_with(expected_df)
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()
    get_client_spy.assert_not_called()


# ---------- 测试 11: 自定义源返回空 df 时不调 on_segment (Issue 1 边界) ----------

def test_sync_minute_batch_custom_empty_df_skips_on_segment(monkeypatch):
    """Issue 1 边界: 自定义源返回空 df → 不调 on_segment (与 TickFlow `if seg_out:` 对称)。
    """
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = pl.DataFrame()
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    on_segment_spy = MagicMock(name="on_segment_spy")
    df = kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2026, 1, 15, 9, 25, 0),
        end_time=datetime(2026, 1, 15, 15, 5, 0),
        on_segment=on_segment_spy,
        asset_type="stock",
    )

    on_segment_spy.assert_not_called()
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()


def test_sync_minute_batch_stops_before_fetch_when_cancelled(monkeypatch):
    """取消信号必须阻止下一批请求,避免已失败任务继续后台取数。"""
    monkeypatch.setattr(kline_sync, "_try_custom_minute", lambda *a, **kw: (None, True))
    client = MagicMock()
    monkeypatch.setattr(kline_sync, "get_client", lambda: client)

    with pytest.raises(kline_sync.MinuteSyncCancelled):
        kline_sync.sync_minute_batch(
            ["600519.SH"],
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            batch_size=1,
            should_cancel=lambda: True,
        )

    client.klines.batch.assert_not_called()


def test_minute_segment_progress_label_includes_year(monkeypatch):
    """跨年补历史时日志必须显示年份,避免把不同年份误认成重复区间。"""
    monkeypatch.setattr(kline_sync, "_try_custom_minute", lambda *a, **kw: (None, True))
    client = MagicMock()
    client.klines.batch.return_value = None
    monkeypatch.setattr(kline_sync, "get_client", lambda: client)
    labels = []

    kline_sync.sync_minute_batch(
        ["600519.SH"],
        start_time=datetime(2025, 1, 1),
        end_time=datetime(2025, 1, 2),
        batch_size=1,
        on_chunk_done=lambda _done, _total, label: labels.append(label),
    )

    assert labels == ["2025-01-01~2025-01-02"]


def test_one_year_minute_request_uses_trading_days():
    """UI 旧请求 days=365 应归一为约一年的 250 个交易日。"""
    from app.api import kline as kline_api

    assert kline_api._normalize_minute_sync_request(365, False) == (250, True)
    assert kline_api._normalize_minute_sync_request(20, True) == (20, True)
    assert kline_api._normalize_minute_sync_request(5, False) == (5, False)


def test_latest_year_minute_sync_uses_latest_trade_date(monkeypatch, tmp_path):
    """最近一年必须以最新交易日回溯 365 天,不从本地最早分钟数据继续向前。"""
    monkeypatch.setattr(kline_sync.preferences, "get_minute_data_provider", lambda: "tickflow")
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda *args: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda *args: None)
    monkeypatch.setattr(
        kline_sync,
        "resolve_limit",
        lambda *args, **kwargs: MagicMock(batch=100, rpm=30),
    )
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)
    sync_spy = MagicMock(return_value=pl.DataFrame())
    monkeypatch.setattr(kline_sync, "sync_minute_batch", sync_spy)

    repo = MagicMock()
    repo.latest_daily_date.return_value = date(2026, 7, 24)
    repo.store.data_dir = tmp_path
    capset = MagicMock()
    capset.has.return_value = True

    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"], repo, capset, latest_year=True,
    )

    assert written == 0
    kwargs = sync_spy.call_args.kwargs
    assert kwargs["start_time"] == datetime(2025, 7, 24)
    assert kwargs["end_time"] == datetime(2026, 7, 25)


# ---------- 测试 12: sync_and_persist_minute + custom provider 端到端落盘 (Issue 1) ----------

def test_sync_and_persist_minute_custom_persists(monkeypatch, tmp_path):
    """Issue 1 端到端: sync_and_persist_minute + 自定义源 →
    _write_minute_partition 被调, written > 0。
    """
    expected_df = _mock_minute_df()
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    # mock sync_and_persist_minute 内部依赖 (通过 monkeypatch kline_sync 模块属性)
    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda *args: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda *args: None)
    monkeypatch.setattr(kline_sync, "_latest_minute_datetime", lambda *args: None)
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *a, **kw: MagicMock(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)

    # _write_minute_partition spy: 记录调用, 返回行数
    write_spy = MagicMock(return_value=expected_df.height)
    monkeypatch.setattr(kline_sync, "_write_minute_partition", write_spy)

    # get_client spy: 自定义源成功时不应走 TickFlow
    get_client_spy = MagicMock(name="get_client_spy")
    monkeypatch.setattr(kline_sync, "get_client", get_client_spy)

    # mock repo
    mock_repo = MagicMock()
    mock_repo.store.data_dir = tmp_path
    mock_repo.db.execute = MagicMock()

    # mock capset (minute_is_custom=True 绕过 has() 检查, resolve_limit 已 mock)
    mock_capset = MagicMock()

    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"], mock_repo, mock_capset,
    )

    assert write_spy.called
    assert written == expected_df.height
    assert written > 0
    get_client_spy.assert_not_called()


def test_sync_and_persist_etf_minute_uses_separate_storage(monkeypatch, tmp_path):
    """ETF 分钟K必须写入 kline_etf_minute，且向数据源透传 asset_type。"""
    expected_df = _mock_minute_df("510300.SH")
    mock_provider = MagicMock()
    mock_provider.get_minute.return_value = expected_df
    _setup_custom_provider(monkeypatch, mock_provider, has_dataset=True)

    monkeypatch.setattr(kline_sync, "_cleanup_null_datetime_minute", lambda *args: None)
    monkeypatch.setattr(kline_sync, "_migrate_symbol_to_date_partition", lambda *args: None)
    monkeypatch.setattr(kline_sync, "_latest_minute_datetime", lambda *args: None)
    monkeypatch.setattr(kline_sync, "resolve_limit", lambda *args, **kwargs: MagicMock(batch=100, rpm=30))
    monkeypatch.setattr(kline_sync.preferences, "get_minute_sync_segment_days", lambda: 20)

    written_paths = []

    def write_partition(df, path):
        written_paths.append(path)
        return df.height

    monkeypatch.setattr(kline_sync, "_write_minute_partition", write_partition)

    repo = MagicMock()
    repo.store.data_dir = tmp_path
    capset = MagicMock()

    written = kline_sync.sync_and_persist_minute(
        ["510300.SH"], repo, capset, asset_type="etf",
    )

    assert written == expected_df.height
    assert written_paths == [tmp_path / "kline_etf_minute"]
    _, kwargs = mock_provider.get_minute.call_args
    assert kwargs["asset_type"] == "etf"
    assert "kline_etf_minute" in repo.db.execute.call_args.args[0]


# ---------- 测试 13: get_provider 异常时 fall through TickFlow (Issue 2) ----------

def test_get_provider_exception_falls_back_to_tickflow(monkeypatch):
    """Issue 2: get_provider raise ValueError →
    _try_custom_minute 返回 (None, True), 无异常穿透。
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: True,  # provider 存在, 但 get_provider 会抛
    )

    def _raising_get_provider(name):
        raise ValueError("not found")
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        _raising_get_provider,
    )

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"], None, None, asset_type="stock",
    )

    assert fallback is True
    assert df is None


# ---------- 测试 14: provider_has_dataset 异常时 fall through (Issue 2) ----------

def test_provider_has_dataset_exception_falls_back(monkeypatch):
    """Issue 2: provider_has_dataset raise →
    _try_custom_minute 返回 (None, True), 无异常穿透。
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )

    def _raising_has_dataset(name, ds):
        raise RuntimeError("registry corrupted")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        _raising_has_dataset,
    )

    df, fallback = kline_sync._try_custom_minute(
        ["600519.SH"], None, None, asset_type="stock",
    )

    assert fallback is True
    assert df is None


# ---------- 测试 15-17: GenericHTTPProvider opt-in 参数传递 (Issue 3) ----------

from app.data_providers.custom.config import CustomSourceConfig, DatasetConfig
from app.data_providers.custom.provider import GenericHTTPProvider


def _make_minute_config(**extra) -> CustomSourceConfig:
    """构造带 minute dataset 的最小 CustomSourceConfig, extra 传给 DatasetConfig。"""
    field_map = {f: f for f in (
        "symbol", "datetime", "open", "high", "low", "close", "volume", "amount"
    )}
    return CustomSourceConfig(
        name="test_src",
        display_name="Test Source",
        datasets={"minute": DatasetConfig(
            url="http://example.com/minute", field_map=field_map, **extra,
        )},
    )


def _capture_request_rows(provider):
    """替换 _request_rows 为捕获 spy, 返回 captured dict。"""
    captured: dict = {}

    def fake_request_rows(cfg, *, symbols=None, start_time=None, end_time=None,
                          override_params=None, override_body=None):
        captured["override_params"] = override_params
        captured["override_body"] = override_body
        return []  # 空行 → 空 df

    provider._request_rows = fake_request_rows
    return captured


def test_generic_http_get_minute_passes_asset_type_when_configured():
    """Issue 3: 配了 asset_type_param="asset" → override 含 {"asset": "etf"}。"""
    config = _make_minute_config(asset_type_param="asset")
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="etf", freq="1m")

    assert captured["override_params"] == {"asset": "etf"}
    assert captured["override_body"] == {"asset": "etf"}


def test_generic_http_get_minute_passes_freq_when_configured():
    """Issue 3: 配了 freq_param="period" → override 含 {"period": "1m"}。"""
    config = _make_minute_config(freq_param="period")
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="stock", freq="1m")

    assert captured["override_params"] == {"period": "1m"}
    assert captured["override_body"] == {"period": "1m"}


def test_generic_http_get_minute_omits_params_when_not_configured():
    """Issue 3 向后兼容: 未配 asset_type_param/freq_param → override 为 None, 不传上游。"""
    config = _make_minute_config()  # 无 asset_type_param / freq_param
    provider = GenericHTTPProvider(config)
    captured = _capture_request_rows(provider)

    provider.get_minute(["600519.SH"], None, None, asset_type="etf", freq="1m")

    # override 为 None (空 dict → `override or None`), 不传上游
    assert captured["override_params"] is None
    assert captured["override_body"] is None


# ---------- 测试 18: sync_and_persist_minute resolver 异常时优雅返回 0 (观察项加固) ----------

def test_sync_and_persist_minute_resolver_exception_returns_zero(monkeypatch, tmp_path):
    """观察项加固: sync_and_persist_minute 开头 _resolve_minute_provider 异常 →
    不向接口抛 500, 优雅降级 (minute_is_custom=False → 走 capset 检查 → 无权限 return 0)。
    """
    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "mock_src",
    )
    # provider_has_dataset 抛异常 (模拟 registry 损坏)
    def _raising_has_dataset(name, ds):
        raise RuntimeError("registry corrupted")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        _raising_has_dataset,
    )

    # 无 KLINE_MINUTE_BATCH 权限 → resolver 异常视为非 custom → capset 检查失败 → return 0
    mock_capset = MagicMock()
    mock_capset.has.return_value = False

    mock_repo = MagicMock()
    mock_repo.store.data_dir = tmp_path

    # 不应抛异常, 优雅降级到 0
    written = kline_sync.sync_and_persist_minute(
        ["600519.SH"], mock_repo, mock_capset,
    )

    assert written == 0


# ---------- 测试 19: _resolve_minute_provider helper 单元测试 ----------

def test_resolve_minute_provider_tickflow_returns_silent_fallback():
    """观察项加固: provider_name == "tickflow" → (None, True, None) 静默降级, 无 err。"""
    provider, fallback, err = kline_sync._resolve_minute_provider("tickflow")
    assert provider is None
    assert fallback is True
    assert err is None


def test_resolve_minute_provider_no_dataset_returns_silent_fallback(monkeypatch):
    """观察项加固: 配了 custom 但未配 minute dataset → (None, True, None) 静默降级。"""
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: False,  # 已注册但未配 minute
    )
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is None
    assert fallback is True
    assert err is None  # 未配 ≠ 异常, 不应触发 warning


def test_resolve_minute_provider_has_dataset_exception_returns_err(monkeypatch):
    """观察项加固: provider_has_dataset 抛异常 → (None, True, str(e)), 上层据此 warning。"""
    def _raising(name, ds):
        raise RuntimeError("registry corrupted")
    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", _raising)
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is None
    assert fallback is True
    assert err is not None
    assert "registry corrupted" in err


def test_resolve_minute_provider_get_provider_exception_returns_err(monkeypatch):
    """观察项加固: provider_has_dataset 返回 True 但 get_provider 抛 → (None, True, str(e))。"""
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: True,
    )
    def _raising_get(name):
        raise ValueError("not found")
    monkeypatch.setattr("app.data_providers.custom.get_provider", _raising_get)
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is None
    assert fallback is True
    assert err is not None
    assert "not found" in err


def test_resolve_minute_provider_success_returns_provider(monkeypatch):
    """观察项加固: 正常路径 → (provider, False, None)。"""
    mock_provider = object()  # 任意 truthy 对象即可
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, ds: True,
    )
    monkeypatch.setattr(
        "app.data_providers.custom.get_provider",
        lambda name: mock_provider,
    )
    provider, fallback, err = kline_sync._resolve_minute_provider("mock_src")
    assert provider is mock_provider
    assert fallback is False
    assert err is None


def test_minute_allowed_resolver_exception_returns_false(monkeypatch):
    """权限入口复用安全 resolver, 插件注册异常不再穿透为 500。"""
    from app.api import kline as kline_api
    from app.tickflow.capabilities import CapabilitySet

    monkeypatch.setattr(
        "app.services.preferences.get_minute_data_provider",
        lambda: "broken",
    )

    def _raising(name, dataset):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", _raising)

    assert kline_api._minute_allowed(CapabilitySet()) is False


def test_intraday_monitor_support_resolver_exception_falls_back(monkeypatch):
    """监控入口解析自定义源失败后继续按 TickFlow 能力判断。"""
    from app.tickflow.capabilities import Cap, CapabilitySet

    monkeypatch.setattr(
        kline_sync.preferences,
        "get_minute_data_provider",
        lambda: "broken",
    )

    def _raising(name, dataset):
        raise RuntimeError("registry corrupted")

    monkeypatch.setattr("app.data_providers.custom.provider_has_dataset", _raising)
    capset = CapabilitySet()
    capset.grant(Cap.KLINE_MINUTE_BATCH)

    support = kline_sync.intraday_monitor_support(capset)

    assert support["available"] is True
    assert support["source"] == "minute_batch"
