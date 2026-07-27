import json
import queue
from hashlib import sha256
from datetime import date
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import free_strategy
from app.api.free_strategy import (
    BacktestWrite,
    _job_payload,
    cleanup_incomplete_backtests,
    migrate_legacy_five_fortunes_strategies,
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
    assert payload["strategy_name"] == "五福"


def test_backtest_request_can_leave_universe_to_strategy_source(tmp_path):
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        datastore=SimpleNamespace(data_dir=tmp_path),
    )))
    strategy = {"id": "source-pool", "name": "源码股票池", "source": "def on_bar(context, bars): pass", "revision": 1}

    payload = _job_payload(BacktestWrite(strategy_id="source-pool"), strategy, request)

    assert payload["symbols"] == []


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
    assert (
        tmp_path / "free_strategies" / legacy["id"] / "revisions" / "0001.py"
    ).read_text(encoding="utf-8") == LEGACY_FIVE_FORTUNES_SOURCE
    assert store.get(customized["id"])["revision"] == 1


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
    })
    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    blocked = client.delete(f"/api/free-strategies/{strategy['id']}")
    assert blocked.status_code == 409
    assert "1 条回测记录" in blocked.json()["detail"]
    assert "1 个模拟盘账户" in blocked.json()["detail"]

    assert client.delete("/api/free-strategies/backtest/linked-run").status_code == 200
    assert client.delete("/api/free-strategies/paper/accounts/linked-paper").status_code == 200
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
        "account": {"orders": [{"id": "o1", "status": "filled"}]},
    })
    store.append_event("paper-1", {"type": "fill", "order_id": "o1", "symbol": "510300.SH"})

    app = FastAPI()
    app.state.datastore = SimpleNamespace(data_dir=tmp_path)
    app.include_router(router)
    client = TestClient(app)

    assert client.get("/api/free-strategies/paper/accounts/paper-1/orders").json() == {
        "orders": [{"id": "o1", "status": "filled"}],
    }
    fills = client.get("/api/free-strategies/paper/accounts/paper-1/fills").json()["fills"]
    assert fills[0]["type"] == "fill"
    assert fills[0]["order_id"] == "o1"
