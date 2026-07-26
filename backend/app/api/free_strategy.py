"""自由策略 CRUD、历史回测和模拟账户 API。

该路由使用独立前缀，现有 ``/api/backtest/*`` 的请求结构和任务表完全不变。
"""
from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import threading
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.free_strategy.process import start_process
from app.free_strategy.store import FreeStrategyStore, PaperAccountStore, now_iso
from app.free_strategy.templates import TEMPLATES

router = APIRouter(prefix="/api/free-strategies", tags=["free-strategy"])
_jobs: dict[str, tuple[mp.Process, Any]] = {}
_jobs_lock = threading.Lock()
_paper: dict[str, mp.Process] = {}


def _strategy_store(request: Request) -> FreeStrategyStore:
    return FreeStrategyStore(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir)


def _paper_store(request: Request) -> PaperAccountStore:
    return PaperAccountStore(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir)


def _run_root(request: Request) -> Path:
    data_dir = getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir
    return Path(data_dir) / "free_strategy_runs"


def _run_path(request: Request, job_id: str) -> Path:
    if not job_id or "/" in job_id or "\\" in job_id or job_id in {".", ".."}:
        raise HTTPException(status_code=400, detail="非法回测任务 ID")
    return _run_root(request) / job_id


def recover_paper_accounts(data_dir: Path) -> None:
    """应用重启时恢复之前处于 running 的受管进程。"""
    store = PaperAccountStore(data_dir)
    for state in store.list():
        if state.get("status") != "running" or state["id"] in _paper:
            continue
        ctx = mp.get_context("spawn")
        process = ctx.Process(target=_paper_loop, args=(state["id"], str(store.root)), daemon=True)
        process.start()
        _paper[state["id"]] = process


class StrategyWrite(BaseModel):
    id: str | None = None
    name: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)


class BacktestWrite(BaseModel):
    strategy_id: str
    symbols: list[str] = Field(min_length=1)
    timeframe: Literal["1d", "30m", "5m", "1m"] = "1d"
    start: date | None = None
    end: date | None = None
    asset_type: Literal["stock", "etf"] = "stock"
    initial_capital: float = Field(default=1_000_000, gt=0)
    fees_pct: float = Field(default=0.0002, ge=0)
    commission_pct: float | None = Field(default=None, ge=0)
    stamp_tax_pct: float = Field(default=0.001, ge=0)
    slippage_bps: float = Field(default=5, ge=0)
    lot_size: int = Field(default=100, ge=1)
    max_exposure_pct: float = Field(default=1.0, gt=0, le=1)
    settlement: Literal["t1", "t0"] = "t1"
    fill_policy: Literal["next_open", "close"] = "next_open"
    benchmark_symbol: str = "510300.SH"


class PaperWrite(BacktestWrite):
    name: str = "自由策略模拟账户"


def _validate_payload(req: BacktestWrite, request: Request) -> None:
    if req.timeframe != "1d":
        capabilities = getattr(request.app.state, "capabilities", None)
        from app.tickflow.capabilities import Cap
        if capabilities is not None and not capabilities.has(Cap.KLINE_MINUTE_BATCH):
            raise HTTPException(status_code=403, detail="当前没有分钟K能力，无法运行该周期；请开通分钟K或配置自定义分钟数据源")
    if req.asset_type not in {"stock", "etf"}:
        raise HTTPException(status_code=400, detail="自由策略只支持股票或 ETF，单次任务不可混合")


@router.get("")
def list_strategies(request: Request):
    return {"strategies": _strategy_store(request).list(), "templates": [{"id": k, "name": v["name"]} for k, v in TEMPLATES.items()]}


@router.get("/templates")
def list_templates():
    return {"templates": [{"id": key, **value} for key, value in TEMPLATES.items()]}


@router.post("")
def create_strategy(req: StrategyWrite, request: Request):
    return _strategy_store(request).save(req.id, req.name, req.source, req.config)


@router.get("/backtest")
def list_backtests(request: Request):
    result = []
    for path in sorted(_run_root(request).glob("*"), key=lambda item: item.stat().st_mtime, reverse=True):
        result_path = path / "result.json"
        if not path.is_dir() or not result_path.exists():
            continue
        try:
            run = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        result.append({
            "job_id": path.name,
            "final_equity": run.get("final_equity"),
            "return_pct": run.get("return_pct"),
            "max_drawdown_pct": run.get("max_drawdown_pct"),
            "fills": len(run.get("fills", [])),
            "metadata": run.get("metadata", {}),
        })
    return {"runs": result}


@router.get("/backtest/{job_id}")
def get_backtest_result(job_id: str, request: Request):
    path = _run_path(request, job_id) / "result.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="回测结果不存在")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="回测结果文件损坏") from None


@router.get("/{strategy_id}")
def get_strategy(strategy_id: str, request: Request):
    try:
        return _strategy_store(request).get(strategy_id)
    except (FileNotFoundError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail="自由策略不存在") from None


@router.put("/{strategy_id}")
def update_strategy(strategy_id: str, req: StrategyWrite, request: Request):
    return _strategy_store(request).save(strategy_id, req.name, req.source, req.config)


@router.delete("/{strategy_id}")
def delete_strategy(strategy_id: str, request: Request):
    try:
        _strategy_store(request).delete(strategy_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="自由策略不存在") from None
    return {"ok": True}


def _job_payload(req: BacktestWrite, strategy: dict[str, Any], request: Request) -> dict[str, Any]:
    end = req.end or date.today()
    start = req.start or (end - timedelta(days=365 * 3 if req.timeframe == "1d" else 90))
    config = req.model_dump(exclude={"strategy_id", "symbols", "timeframe", "start", "end", "asset_type"})
    return {"data_dir": str(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir),
            "source": strategy["source"], "source_revision": strategy.get("revision"), "symbols": req.symbols,
            "timeframe": req.timeframe, "asset_type": req.asset_type, "start": start.isoformat(), "end": end.isoformat(), "config": config}


@router.post("/backtest")
def create_backtest(req: BacktestWrite, request: Request):
    _validate_payload(req, request)
    try:
        strategy = _strategy_store(request).get(req.strategy_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="自由策略不存在") from None
    job_id = uuid.uuid4().hex[:12]
    payload = _job_payload(req, strategy, request)
    run_root = _run_path(request, job_id)
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "strategy.py").write_text(strategy["source"], encoding="utf-8")
    payload["run_dir"] = str(run_root)
    (run_root / "manifest.json").write_text(json.dumps({"job_id": job_id, "strategy_id": req.strategy_id, "source_revision": strategy.get("revision"), "payload": {k: v for k, v in payload.items() if k != "source"}}, ensure_ascii=False, indent=2), encoding="utf-8")
    process, output = start_process(payload)
    with _jobs_lock:
        _jobs[job_id] = (process, output)
    return {"job_id": job_id, "status": "running", "source_revision": strategy.get("revision")}


@router.get("/backtest/{job_id}/stream")
async def stream_backtest(job_id: str, request: Request):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="回测任务不存在")
    process, output = job

    async def events():
        while True:
            if await request.is_disconnected():
                return
            try:
                event = output.get_nowait()
            except Exception:
                event = None
            if event is not None:
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event.get("type") in {"result", "error", "cancelled"}:
                    with _jobs_lock:
                        _jobs.pop(job_id, None)
                    return
            elif not process.is_alive():
                yield f"data: {json.dumps({'type': 'error', 'error': '回测子进程异常退出'}, ensure_ascii=False)}\n\n"
                return
            await asyncio.sleep(0.1)

    return StreamingResponse(events(), media_type="text/event-stream")


@router.post("/backtest/{job_id}/cancel")
def cancel_backtest(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="回测任务不存在")
    process, output = job
    if process.is_alive():
        process.terminate()
        process.join(timeout=2)
    try:
        output.put({"type": "cancelled", "message": "回测已取消"})
    except Exception:
        pass
    return {"ok": True}


def _paper_loop(account_id: str, root: str) -> None:
    from dataclasses import asdict
    import time
    from datetime import date, datetime, time as clock_time
    from app.free_strategy.process import _read_rows
    from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
    from app.tickflow.repository import DataStore, KlineRepository
    account_root = Path(root) / account_id
    path = account_root / "heartbeat.json"
    state_path = account_root / "state.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        source = (account_root / "strategy.py").read_text(encoding="utf-8")
        config = dict(state.get("config", {}))
        symbols = config.pop("symbols", [])
        timeframe = config.pop("timeframe", "1m")
        asset_type = config.pop("asset_type", "stock")
        config.pop("strategy_id", None)
        config.pop("start", None); config.pop("end", None)
        config.pop("name", None)
        engine_config = FreeStrategyConfig(**config)
        repo = KlineRepository(DataStore(Path(root).parent))
        engine = FreeStrategyEngine(source, timeframe, engine_config, state=state.get("state", {}))
        engine.account.restore(state.get("account", {}))
        engine.restore_runtime(state.get("runtime"))
    except Exception:
        # Keep the supervisor alive and make the failure inspectable from the API.
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {"id": account_id}
        state["status"] = "paused"
        state["last_error"] = "模拟账户初始化失败"
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        return
    last_bar = state.get("last_bar")
    while True:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("status") != "running":
            time.sleep(2)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"timestamp": now_iso(), "account_id": account_id}), encoding="utf-8")
        try:
            today = date.today()
            try:
                bars = _read_rows(repo, symbols, today, today, asset_type, timeframe)
            except ValueError as exc:
                # 盘前和盘中尚未有完整 bar 时保持账户运行，等下一轮数据同步即可。
                if "没有可用" not in str(exc):
                    raise
                bars = []
            fresh = [bar for bar in bars if bar.timestamp.isoformat() > (last_bar or "")]
            fill_count = len(engine.account.fills)
            log_count = len(engine.logs)
            if fresh:
                engine.run(fresh, finalize_session=False)
                last_bar = fresh[-1].timestamp.isoformat()
            # 分钟策略在最后一根收盘 bar 写入后执行；日线策略则在盘后同步到当天
            # 日K后执行。状态落盘后同一交易日不会再次触发。
            did_finish = False
            if datetime.now().time() >= clock_time(15, 1):
                did_finish = engine.finish_session()
            if fresh or did_finish:
                state["account"] = engine.account.snapshot()
                state["state"] = engine.state
                state["runtime"] = engine.runtime_snapshot()
                state["last_bar"] = last_bar
                for fill in engine.account.fills[fill_count:]:
                    with (account_root / "ledger.jsonl").open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps({"timestamp": now_iso(), "type": "fill", **asdict(fill)}, ensure_ascii=False) + "\n")
                for log in engine.logs[log_count:]:
                    with (account_root / "ledger.jsonl").open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps({"type": "log", **log}, ensure_ascii=False) + "\n")
                state_path.write_text(json.dumps({**state, "updated_at": now_iso()}, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            state["status"] = "paused"
            state["last_error"] = str(exc)
            state_path.write_text(json.dumps({**state, "updated_at": now_iso()}, ensure_ascii=False, indent=2), encoding="utf-8")
        time.sleep(5)


@router.get("/paper/accounts")
def list_paper_accounts(request: Request):
    return {"accounts": _paper_store(request).list()}


@router.post("/paper/accounts")
def create_paper_account(req: PaperWrite, request: Request):
    _validate_payload(req, request)
    strategy = _strategy_store(request).get(req.strategy_id)
    account_id = uuid.uuid4().hex[:12]
    state = {"id": account_id, "name": req.name, "strategy_id": req.strategy_id, "source_revision": strategy.get("revision"),
             "status": "stopped", "config": req.model_dump(), "cash": req.initial_capital, "positions": {}, "state": {}, "created_at": now_iso()}
    store = _paper_store(request)
    result = store.save(state)
    path = store._path(account_id)
    (path / "strategy.py").write_text(strategy["source"], encoding="utf-8")
    store.append_event(account_id, {"type": "created", "strategy_revision": strategy.get("revision")})
    return result


@router.get("/paper/accounts/{account_id}")
def get_paper_account(account_id: str, request: Request):
    try:
        result = _paper_store(request).get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    result["events"] = _paper_store(request).events(account_id)
    return result


@router.post("/paper/accounts/{account_id}/{action}")
def paper_action(account_id: str, action: Literal["start", "pause", "resume", "stop"], request: Request):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    if action in {"start", "resume"}:
        if action == "start" and account_id not in _paper:
            ctx = mp.get_context("spawn")
            process = ctx.Process(target=_paper_loop, args=(account_id, str(store.root)), daemon=True)
            process.start()
            _paper[account_id] = process
        state["status"] = "running"
    elif action == "pause":
        state["status"] = "paused"
    else:
        process = _paper.pop(account_id, None)
        if process and process.is_alive():
            process.terminate()
        state["status"] = "stopped"
    store.append_event(account_id, {"type": action})
    return store.save(state)


@router.get("/paper/accounts/{account_id}/orders")
def paper_orders(account_id: str, request: Request):
    return {"orders": [event for event in _paper_store(request).events(account_id) if event.get("type") == "fill"]}


@router.get("/paper/accounts/{account_id}/logs")
def paper_logs(account_id: str, request: Request):
    return {"logs": [event for event in _paper_store(request).events(account_id) if event.get("type") in {"log", "error"}]}
