"""回归测试: 本轮修复的几处高风险行为(并发单飞 / 重任务槽 / sector fail-closed)。

均为纯逻辑, 不触网, 不依赖真实数据源。
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
import pytest

from app.jobs import daily_pipeline
from app.services import daily_valuation, pipeline_jobs, quote_service
from app.services.pipeline_jobs import JobStore
from app.services.quote_service import QuoteService
from app.strategy import monitor_rules
from app.strategy.monitor import MonitorRuleEngine
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

# ── JobStore 单飞 ────────────────────────────────────────────────────────

def test_create_singleflight_dedupes_pending_window(tmp_path):
    """两次快速 create() 在 pending 窗口内应复用同一 job(is_new=False)。"""
    store = JobStore(store_dir=tmp_path / "jobs")

    jid1, new1 = store.create()
    assert new1 is True

    # 尚未 start(), job 仍是 pending —— 旧实现会在此另起新 job(并发双跑根因)
    jid2, new2 = store.create()
    assert jid2 == jid1
    assert new2 is False

    # start() 后仍复用同一活跃 job
    store.start(jid1)
    jid3, new3 = store.create()
    assert jid3 == jid1
    assert new3 is False


def test_create_new_after_terminal(tmp_path):
    """job 终态(succeed/fail)后, create() 应给出新 job。"""
    store = JobStore(store_dir=tmp_path / "jobs")
    jid1, _ = store.create()
    store.start(jid1)
    store.succeed(jid1, {"ok": True})

    jid2, new2 = store.create()
    assert jid2 != jid1
    assert new2 is True


def test_run_slot_is_exclusive():
    """重任务执行槽同一时刻只允许一个持有者(防僵尸并发)。"""
    assert pipeline_jobs.try_acquire_run_slot() is True
    try:
        # 已被占用, 第二次获取失败
        assert pipeline_jobs.try_acquire_run_slot() is False
    finally:
        pipeline_jobs.release_run_slot()
    # 释放后可再次获取
    assert pipeline_jobs.try_acquire_run_slot() is True
    pipeline_jobs.release_run_slot()
    # 重复释放幂等, 不抛
    pipeline_jobs.release_run_slot()


def _iso_ago(**delta) -> str:
    return (datetime.now(timezone.utc) - timedelta(**delta)).isoformat()


def test_job_activity_timestamp_has_subsecond_precision():
    """竞态检查依赖时间戳能区分同一秒内的两次进度更新。"""
    assert re.search(r"\.\d{6}Z$", pipeline_jobs._utc_now_iso())


def test_reap_stale_uses_last_progress_not_total_runtime(tmp_path):
    """长任务只要仍在推进,就不应因总运行时间超过阈值而失败。"""
    store = JobStore(store_dir=tmp_path / "jobs")
    jid, _ = store.create(timeout_s=60)
    store.start(jid)
    store._active_jobs[jid]["started_at"] = _iso_ago(hours=2)
    store._active_jobs[jid]["last_progress_at"] = _iso_ago(seconds=5)

    store.reap_stale()

    assert store.get(jid)["status"] == "running"


def test_reap_stale_does_not_release_live_worker_slot(tmp_path):
    """标记超时不等于线程已退出,重任务锁必须由实际 worker 的 finally 释放。"""
    store = JobStore(store_dir=tmp_path / "jobs")
    jid, _ = store.create(timeout_s=60)
    store.start(jid)
    store._active_jobs[jid]["last_progress_at"] = _iso_ago(minutes=2)

    assert pipeline_jobs.try_acquire_run_slot() is True
    try:
        store.reap_stale()

        assert store.get(jid)["status"] == "failed"
        assert pipeline_jobs.try_acquire_run_slot() is False
    finally:
        pipeline_jobs.release_run_slot()


# ── 盘后 ETF 分钟K ───────────────────────────────────────────────────

def test_daily_pipeline_syncs_etf_minute_when_enabled(monkeypatch, tmp_path):
    """ETF 分钟K开关开启后应跟随盘后管道执行，且不混入 A 股标的。"""
    for dirname in ("kline_daily", "kline_daily_enriched"):
        (tmp_path / dirname / "date=2026-07-24").mkdir(parents=True)

    repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        latest_daily_date=MagicMock(return_value=None),
        get_etf_instruments=MagicMock(return_value=pl.DataFrame({
            "symbol": ["510300.SH", "159915.SZ", "510300.SH"],
        })),
        rebuild_views=MagicMock(),
    )
    capset = CapabilitySet({
        Cap.KLINE_MINUTE_BATCH: CapabilityLimits(batch=100, rpm=30),
    })
    sync_calls = []

    def sync_minute(symbols, passed_repo, passed_capset, **kwargs):
        sync_calls.append((symbols, passed_repo, passed_capset, kwargs))
        return 321

    monkeypatch.setattr(daily_pipeline.instrument_sync, "sync_instruments", lambda *_: 0)
    monkeypatch.setattr(daily_pipeline, "_resolve_universe", lambda *_: [])
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *_: None)
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *_: None)
    monkeypatch.setattr(
        daily_valuation,
        "sync_missing_daily_valuation",
        lambda *_: {"rows": 0, "trading_days": 0},
    )
    monkeypatch.setattr(daily_pipeline.kline_sync, "sync_and_persist_minute", sync_minute)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_a_share", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_adj_factor_provider", lambda: "tickflow")
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_index", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_pipeline_pull_etf", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_minute_sync_enabled", lambda: False)
    monkeypatch.setattr(daily_pipeline._prefs, "get_etf_minute_sync_enabled", lambda: True)
    monkeypatch.setattr(daily_pipeline._prefs, "get_minute_sync_days", lambda: 5)
    monkeypatch.setattr(
        daily_pipeline.pit_reference,
        "sync_hithink_snapshots",
        lambda *_: {"status": "skipped", "published_rows": 0, "reason": "missing_hithink_api_key"},
    )
    monkeypatch.setattr(
        daily_pipeline.pit_reference,
        "sync_baostock_lifecycle",
        lambda *_: {
            "status": "published",
            "published_rows": 12,
            "instrument_appended_symbols": 2,
            "errors": [],
        },
    )

    result = daily_pipeline.run_now(repo, capset)

    assert result["etf_minute_rows"] == 321
    assert result["pit_reference_baostock_lifecycle_rows"] == 12
    assert result["pit_reference_instrument_appended_symbols"] == 2
    assert "sync_pit_reference" not in result["skipped_stages"]
    assert "sync_etf_minute" not in result["skipped_stages"]
    assert len(sync_calls) == 1
    symbols, passed_repo, passed_capset, kwargs = sync_calls[0]
    assert symbols == ["159915.SZ", "510300.SH"]
    assert passed_repo is repo
    assert passed_capset is capset
    assert kwargs["days"] == 5
    assert kwargs["asset_type"] == "etf"


# ── 监控 sector fail-closed ──────────────────────────────────────────────

def _base_price_rule(scope: str) -> dict:
    return {
        "id": "r_test",
        "name": "t",
        "type": "price",
        "conditions": [{"field": "close", "op": ">", "value": 10}],
        "logic": "and",
        "scope": scope,
    }


def test_validate_rejects_sector_scope():
    with pytest.raises(ValueError):
        monitor_rules.validate(_base_price_rule("sector"))


def test_validate_accepts_symbols_scope():
    rule = _base_price_rule("symbols")
    rule["symbols"] = ["600000.SH"]
    monitor_rules.validate(rule)  # 不应抛


def test_apply_scope_sector_fails_closed():
    """历史遗留 sector 规则在评估时应返回空(绝不退化为全市场)。"""
    df = pl.DataFrame({"symbol": ["600000.SH", "000001.SZ"], "close": [10.0, 20.0]})
    out = MonitorRuleEngine._apply_scope(df, {"id": "r_old", "scope": "sector"})
    assert out.is_empty()

    # 对照: scope=all 返回全量, symbols 过滤子集
    assert MonitorRuleEngine._apply_scope(df, {"scope": "all"}).height == 2
    picked = MonitorRuleEngine._apply_scope(
        df, {"scope": "symbols", "symbols": ["600000.SH"]}
    )
    assert picked.height == 1


def test_ladder_webhook_uses_chinese_title_without_brand(monkeypatch):
    calls = []

    class CaptureExecutor:
        def submit(self, fn, *args):
            calls.append((fn, args))

    monkeypatch.setattr(quote_service, "_WEBHOOK_EXECUTOR", CaptureExecutor())
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_url", lambda: "https://open.feishu.cn/open-apis/bot/v2/hook/test")
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_secret", lambda: "secret")
    monkeypatch.setattr("app.services.preferences.get_wecom_webhook_url", lambda: "wecom-key")

    engine = type("Engine", (), {
        "rules": {"r_ladder": {"webhook_channels": ["feishu", "wecom"]}},
    })()
    QuoteService._maybe_send_webhook(
        object.__new__(QuoteService),
        [{
            "rule_id": "r_ladder",
            "source": "ladder",
            "symbol": "600000.SH",
            "name": "浦发银行",
            "message": "炸板预警",
        }],
        engine,
    )

    assert [args[1] for _, args in calls] == ["连板梯队", "连板梯队"]
    assert all("TickFlow" not in args[1] for _, args in calls)


def test_review_webhooks_use_title_without_brand(monkeypatch):
    calls = []
    monkeypatch.setattr("app.services.preferences.get_review_push_channels", lambda: ["feishu", "wecom"])
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_url", lambda: "feishu-url")
    monkeypatch.setattr("app.services.preferences.get_feishu_webhook_secret", lambda: "secret")
    monkeypatch.setattr("app.services.preferences.get_wecom_webhook_url", lambda: "wecom-url")
    monkeypatch.setattr(
        "app.services.webhook_adapter.send_feishu_card",
        lambda *args: calls.append(("feishu", args)) or True,
    )
    monkeypatch.setattr(
        "app.services.webhook_adapter.send_wecom_markdown",
        lambda *args: calls.append(("wecom", args)) or True,
    )

    daily_pipeline._maybe_push_review("复盘正文", {"as_of": "2026-07-18"})

    assert [args[1] for _, args in calls] == ["每日复盘", "每日复盘"]
    assert all("TickFlow" not in args[1] for _, args in calls)
