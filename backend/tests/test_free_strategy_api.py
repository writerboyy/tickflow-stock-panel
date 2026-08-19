import json
import queue
from hashlib import sha256
from datetime import date, datetime
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import free_strategy
from app.api.free_strategy import (
    BacktestWrite,
    PaperWrite,
    _job_payload,
    cleanup_incomplete_backtests,
    migrate_legacy_external_large_amount_first_board,
    migrate_legacy_five_fortunes_strategies,
    migrate_managed_etf_nav_alignment,
    migrate_managed_large_amount_first_board,
    router,
)
from app.free_strategy.store import FreeStrategyStore, PaperAccountStore
from app.free_strategy.templates import LEGACY_FIVE_FORTUNES_SOURCE, TEMPLATES


def test_etf_asset_type_is_preserved_in_engine_config(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {"id": "five", "name": "五福", "source": "def on_bar(context, bars): pass", "revision": 3}
    payload = _job_payload(
        BacktestWrite(
            strategy_id="five",
            symbols=["510300.SH"],
            timeframe="1m",
            start=date(2024, 1, 1),
            end=date(2024, 1, 31),
            asset_type="etf",
        ),
        strategy,
        request,
    )

    assert payload["config"]["asset_type"] == "etf"
    assert payload["config"]["callback_timeout_seconds"] == 30
    assert payload["strategy_name"] == "五福"


def test_paper_write_uses_longer_callback_timeout_than_backtests():
    assert PaperWrite(strategy_id="paper").callback_timeout_seconds == 120


def test_paper_logs_only_return_strategy_output(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper", "status": "running"})
    store.append_event("paper", {
        "type": "log", "message": "策略输出", "source": "strategy",
    })
    store.append_event("paper", {
        "type": "log", "message": "框架日志", "source": "engine",
    })
    store.append_event("paper", {"type": "error", "message": "运行错误"})
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)

    response = TestClient(app).get("/api/free-strategies/paper/accounts/paper/logs")

    assert response.status_code == 200
    assert [item["message"] for item in response.json()["logs"]] == ["策略输出"]


def test_backtest_payload_preserves_broker_and_symbol_settlement_options(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {"id": "seven", "name": "七星", "source": "def on_bar(context, bars): pass", "revision": 1}

    payload = _job_payload(
        BacktestWrite(
            strategy_id="seven",
            reserve_buy_fees=False,
            sell_commission_pct=0.0001,
            t0_symbols=["513310.SH"],
            allow_stale_fills=True,
            limit_up_touch_fill=True,
        ),
        strategy,
        request,
    )

    assert payload["config"]["reserve_buy_fees"] is False
    assert payload["config"]["sell_commission_pct"] == 0.0001
    assert payload["config"]["t0_symbols"] == ["513310.SH"]
    assert payload["config"]["allow_stale_fills"] is True
    assert payload["config"]["limit_up_touch_fill"] is True


def test_backtest_request_can_leave_universe_to_strategy_source(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {"id": "source-pool", "name": "源码股票池", "source": "def on_bar(context, bars): pass", "revision": 1}

    payload = _job_payload(BacktestWrite(strategy_id="source-pool"), strategy, request)

    assert payload["symbols"] == []


def test_data_health_uses_saved_strategy_universe(monkeypatch, tmp_path):
    source = """def initialize(context):
    context.set_universe([\"510300.SH\"])

def on_bar(context, bars):
    pass
"""
    strategy = FreeStrategyStore(tmp_path).save(None, "ETF预检", source, {})
    captured = {}
    monkeypatch.setattr(
        "app.free_strategy.process._instrument_records",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "app.services.etf_data_repair.inspect_etf_data",
        lambda _repo, symbols, *_args, **kwargs: captured.update(symbols=symbols, kwargs=kwargs) or {
            "status": "healthy", "issues": [], "symbol_count": len(symbols), "scan_id": None,
        },
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.state.repo = SimpleNamespace()
    app.include_router(router)

    response = TestClient(app).post("/api/free-strategies/backtest/data-health", json={
        "strategy_id": strategy["id"], "asset_type": "etf", "timeframe": "1m",
        "start": "2026-07-20", "end": "2026-07-21",
    })

    assert response.status_code == 200
    assert captured["symbols"] == ["510300.SH"]
    assert captured["kwargs"] == {
        "require_minute": True,
        "min_daily_bars": 1,
        "persist_scan": False,
    }


def test_job_payload_keeps_legacy_saved_universe_as_fallback(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {
        "id": "legacy",
        "name": "旧策略",
        "source": "def on_bar(context, bars): pass",
        "revision": 1,
        "config": {"symbols": ["510300.SH"]},
    }

    payload = _job_payload(BacktestWrite(strategy_id="legacy"), strategy, request)

    assert payload["symbols"] == ["510300.SH"]


def test_legacy_five_fortunes_strategy_is_migrated_once(tmp_path):
    store = FreeStrategyStore(tmp_path)
    legacy = store.save(
        None,
        "五福策略",
        LEGACY_FIVE_FORTUNES_SOURCE,
        {"timeframe": "1m", "asset_type": "etf"},
    )
    customized = store.save(
        None,
        "自行修改的五福策略",
        f"{LEGACY_FIVE_FORTUNES_SOURCE}\n# user customization\n",
        {"timeframe": "1m", "asset_type": "etf"},
    )

    migrated = migrate_legacy_five_fortunes_strategies(tmp_path)
    repeated = migrate_legacy_five_fortunes_strategies(tmp_path)

    loaded = store.get(legacy["id"])
    assert migrated == [legacy["id"]]
    assert repeated == []
    assert loaded["revision"] == 2
    assert loaded["source"] == TEMPLATES["five_fortunes"]["source"]
    assert loaded["config"] == TEMPLATES["five_fortunes"]["config"]
    assert (
        tmp_path / "free_strategies" / legacy["id"] / "revisions" / "0001.py"
    ).read_text(encoding="utf-8") == LEGACY_FIVE_FORTUNES_SOURCE
    assert store.get(customized["id"])["revision"] == 1


def test_managed_five_fortunes_snapshot_migrates_only_legacy_default_config(monkeypatch, tmp_path):
    source = "# managed five fortunes snapshot\n"
    monkeypatch.setattr(
        free_strategy,
        "MANAGED_FIVE_FORTUNES_SHA256",
        frozenset({sha256(source.encode("utf-8")).hexdigest()}),
    )
    store = FreeStrategyStore(tmp_path)
    legacy = store.save(None, "旧五福", source, {
        "timeframe": "1m",
        "asset_type": "etf",
        "start": "2025-07-24",
        "end": "2026-07-24",
        "initial_capital": 1_000_000,
        "fees_pct": 0.0002,
        "commission_pct": None,
        "stamp_tax_pct": 0.001,
        "slippage_bps": 5,
        "lot_size": 100,
        "max_exposure_pct": 1,
        "settlement": "t1",
        "fill_policy": "next_open",
        "benchmark_symbol": "510300.SH",
    })
    customized = store.save(None, "改过参数的五福", source, {
        "timeframe": "1m",
        "asset_type": "etf",
        "initial_capital": 500_000,
    })

    migrated = migrate_legacy_five_fortunes_strategies(tmp_path)

    assert set(migrated) == {legacy["id"], customized["id"]}
    migrated_config = store.get(legacy["id"])["config"]
    assert migrated_config["start"] == "2025-07-24"
    assert migrated_config["end"] == "2026-07-24"
    assert migrated_config["initial_capital"] == 100_000
    assert migrated_config["commission_pct"] == 0.0001
    assert migrated_config["slippage_bps"] == 0.5
    assert migrated_config["price_tick"] == 0.001
    assert migrated_config["fill_policy"] == "close"
    assert store.get(customized["id"])["config"]["initial_capital"] == 500_000


def test_managed_etf_nav_alignment_migrates_strategy_and_paper_snapshot(
    monkeypatch,
    tmp_path,
):
    old_source = "# old managed ETF source\n"
    new_source = "# aligned managed ETF source\n"
    old_hash = sha256(old_source.encode("utf-8")).hexdigest()
    new_hash = sha256(new_source.encode("utf-8")).hexdigest()
    monkeypatch.setattr(
        free_strategy,
        "MANAGED_ETF_NAV_ALIGNMENT_SHA256",
        {"five_fortunes": frozenset({old_hash})},
    )
    monkeypatch.setitem(TEMPLATES["five_fortunes"], "source", new_source)

    strategy_store = FreeStrategyStore(tmp_path)
    strategy = strategy_store.save("managed", "受管 ETF", old_source, {"timeframe": "1m"})
    custom = strategy_store.save(
        "custom", "自定义 ETF", f"{old_source}# customized\n", {"timeframe": "1m"},
    )
    paper_store = PaperAccountStore(tmp_path)
    paper_store.save({
        "id": "paper",
        "strategy_id": strategy["id"],
        "source_hash": old_hash,
        "source_revision": strategy["revision"],
        "status": "running",
        "checkpoint": {"state": {"five_fortunes": {"target": ["510300.SH"]}}},
    })
    paper_root = paper_store._path("paper")
    (paper_root / "strategy.py").write_text(old_source, encoding="utf-8")

    migrated = migrate_managed_etf_nav_alignment(tmp_path)
    repeated = migrate_managed_etf_nav_alignment(tmp_path)

    assert migrated == {"strategies": ["managed"], "accounts": ["paper"]}
    assert repeated == {"strategies": [], "accounts": []}
    loaded_strategy = strategy_store.get("managed")
    assert loaded_strategy["source"] == new_source
    assert loaded_strategy["revision"] == 2
    assert strategy_store.get(custom["id"])["revision"] == 1
    loaded_account = paper_store.get("paper")
    assert loaded_account["source_hash"] == new_hash
    assert loaded_account["source_revision"] == 2
    assert loaded_account["status"] == "running"
    assert loaded_account["checkpoint"]["state"]["five_fortunes"]["target"] == [
        "510300.SH",
    ]
    assert (paper_root / "strategy.py").read_text(encoding="utf-8") == new_source
    backup_roots = list((paper_root / "backups").glob("managed-nav-alignment-*"))
    assert len(backup_roots) == 1
    assert (backup_roots[0] / "strategy.py").read_text(encoding="utf-8") == old_source
    migration_events = [
        event for event in paper_store.events("paper")
        if event["type"] == "strategy_migration"
    ]
    assert len(migration_events) == 1
    assert migration_events[0]["to_source_hash"] == new_hash


def test_managed_large_amount_first_board_migrates_only_unchanged_source(
    monkeypatch,
    tmp_path,
):
    old_source = "# old managed first-board source\n"
    old_hash = sha256(old_source.encode("utf-8")).hexdigest()
    monkeypatch.setattr(
        free_strategy,
        "MANAGED_LARGE_AMOUNT_FIRST_BOARD_SHA256",
        frozenset({old_hash}),
    )
    store = FreeStrategyStore(tmp_path)
    managed = store.save(
        "managed", "大成交首板", old_source, {"timeframe": "1m"},
    )
    custom = store.save(
        "custom", "自定义首板", f"{old_source}# customized\n", {"timeframe": "1m"},
    )

    migrated = migrate_managed_large_amount_first_board(tmp_path)
    repeated = migrate_managed_large_amount_first_board(tmp_path)

    assert migrated == [managed["id"]]
    assert repeated == []
    loaded = store.get(managed["id"])
    assert loaded["source"] == TEMPLATES["large_amount_first_board"]["source"]
    assert loaded["revision"] == 2
    assert store.get(custom["id"])["revision"] == 1


def test_legacy_external_large_amount_first_board_uses_native_minute_template_once(
    monkeypatch,
    tmp_path,
):
    source = """# Clone source: https://www.joinquant.com/post/59883
# Title: first-board large turnover tick strategy
from jqdata import *
"""
    store = FreeStrategyStore(tmp_path)
    legacy = store.save(
        "legacy", "首板大成交", source, {"timeframe": "1d", "asset_type": "etf"},
    )
    custom = store.save(
        "custom", "首板大成交", f"{source}\n# user change\n", {"timeframe": "1d"},
    )
    joinquant = store.save(
        "joinquant", "首板大成交", source, {"timeframe": "1d"}, dialect="joinquant",
    )
    monkeypatch.setattr(
        free_strategy,
        "LEGACY_EXTERNAL_LARGE_AMOUNT_FIRST_BOARD_SHA256",
        frozenset({sha256(source.encode("utf-8")).hexdigest()}),
    )

    migrated = migrate_legacy_external_large_amount_first_board(tmp_path)
    repeated = migrate_legacy_external_large_amount_first_board(tmp_path)

    assert migrated == [legacy["id"]]
    assert repeated == []
    loaded = store.get(legacy["id"])
    assert loaded["source"] == TEMPLATES["large_amount_first_board"]["source"]
    assert loaded["config"] == TEMPLATES["large_amount_first_board"]["config"]
    assert loaded["revision"] == 2
    assert store.get(custom["id"])["revision"] == 1
    assert store.get(joinquant["id"])["revision"] == 1


def test_backtest_snapshot_manifest_and_worker_payload_share_source_hash(monkeypatch, tmp_path):
    source = "def initialize(context):\n    context.set_universe(['X'])\n\ndef on_bar(context, bars):\n    pass\n"
    strategy = FreeStrategyStore(tmp_path).save(None, "一致性策略", source, {})
    captured: dict = {}

    class FakeProcess:
        def is_alive(self):
            return True

    def fake_start_process(payload):
        captured.update(payload)
        return FakeProcess(), queue.SimpleQueue()

    monkeypatch.setattr(free_strategy, "start_process", fake_start_process)
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    response = client.post("/api/free-strategies/backtest", json={"strategy_id": strategy["id"]})

    assert response.status_code == 200
    job_id = response.json()["job_id"]
    try:
        run_dir = tmp_path / "free_strategy_runs" / job_id
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        digest = sha256(source.encode("utf-8")).hexdigest()
        assert (run_dir / "strategy.py").read_text(encoding="utf-8") == source
        assert manifest["strategy_source_sha256"] == digest
        assert captured["strategy_source_sha256"] == digest
    finally:
        free_strategy._jobs.pop(job_id, None)


def test_joinquant_unavailable_api_is_rejected_before_starting_backtest(tmp_path):
    strategy = FreeStrategyStore(tmp_path).save(
        None,
        "缺 Tick 数据的聚宽策略",
        "from jqdata import *\n\ndef handle_data(context, data):\n    get_ticks('000001.XSHE')\n",
        {},
        dialect="joinquant",
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)

    response = TestClient(app).post(
        "/api/free-strategies/backtest",
        json={"strategy_id": strategy["id"]},
    )

    assert response.status_code == 400
    assert "get_ticks" in response.json()["detail"]


def test_saved_backtest_routes_are_not_captured_by_strategy_id(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "saved-run"
    run_dir.mkdir(parents=True)
    result = {
        "final_equity": 1_234_567.89,
        "return_pct": 23.45,
        "max_drawdown_pct": 6.78,
        "fills": [{"order_id": "o1"}],
        "metadata": {"strategy_name": "五福"},
    }
    (run_dir / "result.json").write_text(json.dumps(result), encoding="utf-8")

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    listing = client.get("/api/free-strategies/backtest")
    assert listing.status_code == 200
    assert listing.json()["runs"] == [{
        "job_id": "saved-run",
        "name": "五福",
        "final_equity": 1_234_567.89,
        "return_pct": 23.45,
        "max_drawdown_pct": 6.78,
        "fills": 1,
        "metadata": {"strategy_name": "五福"},
    }]

    saved = client.get("/api/free-strategies/backtest/saved-run")
    assert saved.status_code == 200
    assert saved.json() == result


def test_strategy_rename_preserves_source_revision(tmp_path):
    strategy = FreeStrategyStore(tmp_path).save(
        None, "旧名称", "def on_bar(context, bars):\n    pass\n", {"timeframe": "1d"},
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    response = client.patch(f"/api/free-strategies/{strategy['id']}", json={"name": "  新名称  "})

    assert response.status_code == 200
    assert response.json()["name"] == "新名称"
    assert response.json()["revision"] == 1
    loaded = FreeStrategyStore(tmp_path).get(strategy["id"])
    assert loaded["revision"] == 1
    assert loaded["source"] == strategy["source"]


def test_legacy_backtest_can_be_renamed_and_deleted(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "legacy-run"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text(json.dumps({"metadata": {"strategy_name": "旧名"}}), encoding="utf-8")
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    renamed = client.patch("/api/free-strategies/backtest/legacy-run", json={"name": "实验 A"})

    assert renamed.status_code == 200
    assert renamed.json() == {"job_id": "legacy-run", "name": "实验 A"}
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["display_name"] == "实验 A"
    assert client.get("/api/free-strategies/backtest").json()["runs"][0]["name"] == "实验 A"

    deleted = client.delete("/api/free-strategies/backtest/legacy-run")
    assert deleted.status_code == 200
    assert not run_dir.exists()


def test_running_backtest_cannot_be_deleted(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "running-run"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text("{}", encoding="utf-8")
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)
    free_strategy._jobs["running-run"] = (object(), object())
    try:
        response = client.delete("/api/free-strategies/backtest/running-run")
    finally:
        free_strategy._jobs.pop("running-run", None)

    assert response.status_code == 409
    assert run_dir.exists()


def test_inactive_incomplete_backtest_can_be_deleted(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "failed-run"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps({"job_id": "failed-run", "strategy_id": "strategy-1"}),
        encoding="utf-8",
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    response = client.delete("/api/free-strategies/backtest/failed-run")

    assert response.status_code == 200
    assert not run_dir.exists()


def test_startup_cleanup_removes_only_incomplete_backtests(tmp_path):
    incomplete = tmp_path / "free_strategy_runs" / "incomplete"
    completed = tmp_path / "free_strategy_runs" / "completed"
    incomplete.mkdir(parents=True)
    completed.mkdir(parents=True)
    (incomplete / "manifest.json").write_text("{}", encoding="utf-8")
    (completed / "result.json").write_text("{}", encoding="utf-8")

    cleanup_incomplete_backtests(tmp_path)

    assert not incomplete.exists()
    assert completed.exists()


def test_dead_worker_is_removed_from_active_jobs(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "dead-run"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text("{}", encoding="utf-8")

    class DeadProcess:
        def is_alive(self):
            return False

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)
    free_strategy._jobs["dead-run"] = (DeadProcess(), queue.SimpleQueue())
    try:
        response = client.get("/api/free-strategies/backtest/dead-run/stream")

        assert response.status_code == 200
        assert "回测子进程异常退出" in response.text
        assert "dead-run" not in free_strategy._jobs
        assert client.delete("/api/free-strategies/backtest/dead-run").status_code == 200
    finally:
        free_strategy._jobs.pop("dead-run", None)


def test_delete_self_heals_dead_job_without_stream_consumer(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "dead-delete"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text("{}", encoding="utf-8")

    class DeadProcess:
        def is_alive(self):
            return False

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)
    free_strategy._jobs["dead-delete"] = (DeadProcess(), queue.SimpleQueue())
    try:
        response = client.delete("/api/free-strategies/backtest/dead-delete")

        assert response.status_code == 200
        assert "dead-delete" not in free_strategy._jobs
        assert not run_dir.exists()
    finally:
        free_strategy._jobs.pop("dead-delete", None)


def test_cancelled_backtest_removes_job_and_incomplete_run(tmp_path):
    run_dir = tmp_path / "free_strategy_runs" / "cancelled-run"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    class RunningProcess:
        alive = True

        def is_alive(self):
            return self.alive

        def terminate(self):
            self.alive = False

        def join(self, timeout):
            assert timeout == 2

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)
    free_strategy._jobs["cancelled-run"] = (RunningProcess(), queue.SimpleQueue())
    try:
        response = client.post("/api/free-strategies/backtest/cancelled-run/cancel")

        assert response.status_code == 200
        assert "cancelled-run" not in free_strategy._jobs
        assert not run_dir.exists()
    finally:
        free_strategy._jobs.pop("cancelled-run", None)


def test_strategy_delete_is_blocked_until_links_are_removed(tmp_path):
    strategy = FreeStrategyStore(tmp_path).save(
        None, "有关联策略", "def on_bar(context, bars):\n    pass\n", {},
    )
    run_dir = tmp_path / "free_strategy_runs" / "linked-run"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text("{}", encoding="utf-8")
    (run_dir / "manifest.json").write_text(
        json.dumps({"job_id": "linked-run", "strategy_id": strategy["id"]}), encoding="utf-8",
    )
    PaperAccountStore(tmp_path).save({
        "id": "linked-paper", "name": "关联账户", "strategy_id": strategy["id"], "status": "stopped",
        "continuation": {"job_id": "linked-run"},
    })
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    blocked = client.delete(f"/api/free-strategies/{strategy['id']}")
    assert blocked.status_code == 409
    assert "1 条回测记录" in blocked.json()["detail"]
    assert "1 个模拟盘账户" in blocked.json()["detail"]

    linked_backtest = client.delete("/api/free-strategies/backtest/linked-run")
    assert linked_backtest.status_code == 409
    assert "关联账户(linked-paper)" in linked_backtest.json()["detail"]
    assert client.delete("/api/free-strategies/paper/accounts/linked-paper").status_code == 200
    assert client.delete("/api/free-strategies/backtest/linked-run").status_code == 200
    assert client.delete(f"/api/free-strategies/{strategy['id']}").status_code == 200


def test_paper_account_must_be_stopped_before_delete(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-active", "status": "paused"})
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    blocked = client.delete("/api/free-strategies/paper/accounts/paper-active")
    assert blocked.status_code == 409
    assert store._path("paper-active").exists()

    store.save({"id": "paper-active", "status": "stopped"})
    deleted = client.delete("/api/free-strategies/paper/accounts/paper-active")
    assert deleted.status_code == 200
    assert not store._path("paper-active").exists()


def test_create_paper_account_continues_from_backtest_without_private_fields(
    monkeypatch,
    tmp_path,
):
    strategy = FreeStrategyStore(tmp_path).save(
        "small-cap",
        "小市值",
        "def on_bar(context, bars):\n    pass\n",
        {},
    )
    captured = {}

    def continue_account(data_dir, account_id, job_id):
        captured.update({
            "data_dir": data_dir,
            "account_id": account_id,
            "job_id": job_id,
        })
        store = PaperAccountStore(data_dir)
        state = store.get(account_id)
        assert "continuation_job_id" not in state["config"]
        assert state["config"]["start"] == "2025-07-24"
        assert state["config"]["end"] == "2026-07-03"
        state["checkpoint"] = {"private": True}
        state["continuation"] = {"job_id": job_id}
        return store.save(state)

    monkeypatch.setattr(free_strategy, "continue_account_from_backtest", continue_account)
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    response = TestClient(app).post("/api/free-strategies/paper/accounts", json={
        "strategy_id": strategy["id"],
        "name": "小市值模拟",
        "timeframe": "1d",
        "market_mode": "bar_1d",
        "start": "2025-07-24",
        "end": "2026-07-03",
        "continuation_job_id": "verified-run",
    })

    assert response.status_code == 200
    assert captured["data_dir"] == tmp_path
    assert captured["job_id"] == "verified-run"
    assert response.json()["strategy_id"] == strategy["id"]
    assert "checkpoint" not in response.json()
    assert "continuation" not in response.json()


def test_create_paper_account_rolls_back_when_continuation_fails(monkeypatch, tmp_path):
    strategy = FreeStrategyStore(tmp_path).save(
        "small-cap",
        "小市值",
        "def on_bar(context, bars):\n    pass\n",
        {},
    )
    monkeypatch.setattr(
        free_strategy,
        "continue_account_from_backtest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("参数不一致")),
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    response = TestClient(app).post("/api/free-strategies/paper/accounts", json={
        "strategy_id": strategy["id"],
        "name": "小市值模拟",
        "timeframe": "1d",
        "market_mode": "bar_1d",
        "continuation_job_id": "bad-run",
    })

    assert response.status_code == 409
    assert response.json()["detail"] == "参数不一致"
    assert PaperAccountStore(tmp_path).list() == []


def test_create_paper_account_preserves_joinquant_runtime_contract(tmp_path):
    strategy = FreeStrategyStore(tmp_path).save(
        "jq-paper",
        "聚宽模拟策略",
        "from jqdata import *\n\ndef handle_data(context, data):\n    pass\n",
        {},
        dialect="joinquant",
    )
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)

    response = TestClient(app).post("/api/free-strategies/paper/accounts", json={
        "strategy_id": strategy["id"],
        "name": "聚宽模拟策略",
        "timeframe": "1d",
        "market_mode": "bar_1d",
    })

    assert response.status_code == 200
    assert response.json()["dialect"] == "joinquant"
    assert response.json()["compatibility_report"]["version"] == "jq-v1"


def test_resume_restarts_missing_paper_process(monkeypatch, tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-1", "status": "paused"})
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))

    class FakeProcess:
        def __init__(self):
            self.started = False

        def start(self):
            self.started = True

        def is_alive(self):
            return self.started

    process = FakeProcess()
    context = SimpleNamespace(Process=lambda **_kwargs: process)
    monkeypatch.setattr(free_strategy.mp, "get_context", lambda _method: context)
    free_strategy._paper.pop("paper-1", None)

    result = free_strategy.paper_action("paper-1", "resume", request)

    assert process.started is True
    assert result["status"] == "running"
    free_strategy._paper.pop("paper-1", None)


def test_paper_orders_and_fills_are_separate(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-1",
        "status": "paused",
        "account": {
            "orders": [{"id": "o1", "status": "filled"}],
            "fills": [{"order_id": "o1", "side": "buy"}],
        },
    })
    store.append_event("paper-1", {"type": "fill", "order_id": "o1", "symbol": "510300.SH"})

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    assert client.get("/api/free-strategies/paper/accounts/paper-1/orders").json() == {
        "orders": [{"id": "o1", "status": "filled", "executed_side": "buy"}],
    }
    fills = client.get("/api/free-strategies/paper/accounts/paper-1/fills").json()["fills"]
    assert fills[0]["type"] == "fill"
    assert fills[0]["order_id"] == "o1"


def test_paper_detail_uses_persisted_curve_without_private_continuation_fields(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-curve",
        "status": "paused",
        "config": {"initial_capital": 100},
        "continuation": {"job_id": "private-run"},
        "checkpoint": {"account": {"cash": 110, "orders": [], "fills": []}},
        "state": {"private": True},
    })
    store.upsert_equity_curve("paper-curve", [{
        "timestamp": datetime.now().replace(microsecond=0).isoformat(),
        "equity": 110,
        "cash": 110,
        "nav": 1.1,
        "drawdown_pct": 0,
        "positions": {},
        "source": "backtest",
    }])
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    payload = client.get("/api/free-strategies/paper/accounts/paper-curve").json()

    assert payload["account"]["equity_curve"][0]["nav"] == 1.1
    assert "source" not in payload["account"]["equity_curve"][0]
    assert "continuation" not in payload
    assert "checkpoint" not in payload
    assert "state" not in payload


def test_paper_account_views_expose_latest_session_return(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-daily",
        "name": "当日收益测试",
        "status": "paused",
        "equity": 120,
        "return_pct": 20,
        "config": {"initial_capital": 100},
    })
    store.upsert_equity_curve("paper-daily", [
        {
            "timestamp": "2026-07-31T15:00:00",
            "equity": 100,
            "cash": 100,
            "nav": 1,
            "drawdown_pct": 0,
            "positions": {},
            "source": "paper",
        },
        {
            "timestamp": "2026-08-03T15:00:00",
            "equity": 120,
            "cash": 120,
            "nav": 1.2,
            "drawdown_pct": 0,
            "positions": {},
            "source": "paper",
        },
    ])

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    listed = client.get("/api/free-strategies/paper/accounts").json()["accounts"][0]
    detail = client.get("/api/free-strategies/paper/accounts/paper-daily").json()

    assert listed["today_return_pct"] == 20
    assert listed["today_return_date"] == "2026-08-03"
    assert detail["today_return_pct"] == 20
    assert detail["today_return_date"] == "2026-08-03"


def test_paper_account_endpoints_overlay_live_valuation_without_persisting_it(tmp_path):
    store = PaperAccountStore(tmp_path)
    stored = store.save({
        "id": "paper-live",
        "name": "实时账户",
        "status": "running",
        "cash": 100,
        "equity": 1_000,
        "return_pct": 0,
        "drawdown_pct": 0,
        "positions": {"513690.SH": 500},
        "config": {"initial_capital": 1_000},
    })

    class Supervisor:
        @staticmethod
        def live_valuation(_state):
            return {
                "live": True,
                "as_of": "2026-07-29T10:00:00+08:00",
                "date": "2026-07-29",
                "missing_symbols": [],
                "equity": 1_100,
                "return_pct": 10,
                "drawdown_pct": 2,
            }

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.state.paper_supervisor = Supervisor()
    app.include_router(router)
    client = TestClient(app)

    listed = client.get("/api/free-strategies/paper/accounts").json()["accounts"][0]
    detail = client.get("/api/free-strategies/paper/accounts/paper-live").json()

    for payload in (listed, detail):
        assert payload["valuation"]["live"] is True
        assert payload["equity"] == 1_100
        assert payload["return_pct"] == 10
        assert payload["drawdown_pct"] == 2
    assert store.get("paper-live") == stored


def test_running_paper_account_can_be_renamed(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({
        "id": "paper-rename",
        "name": "原名称",
        "status": "running",
        "config": {"name": "原名称"},
    })
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    response = client.patch(
        "/api/free-strategies/paper/accounts/paper-rename",
        json={"name": "  五福实盘模拟  "},
    )

    assert response.status_code == 200
    assert response.json()["name"] == "五福实盘模拟"
    assert store.get("paper-rename")["config"]["name"] == "五福实盘模拟"
    assert store.events("paper-rename")[-1]["type"] == "renamed"


def test_paper_account_name_must_be_1_to_40_characters(tmp_path):
    store = PaperAccountStore(tmp_path)
    store.save({"id": "paper-rename", "name": "原名称", "status": "stopped"})
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    assert client.patch("/api/free-strategies/paper/accounts/paper-rename", json={"name": "   "}).status_code == 422
    assert client.patch("/api/free-strategies/paper/accounts/paper-rename", json={"name": "x" * 41}).status_code == 422
