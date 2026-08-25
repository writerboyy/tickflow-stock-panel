"""自由策略 CRUD、历史回测和模拟账户 API。

该路由使用独立前缀，现有 ``/api/backtest/*`` 的请求结构和任务表完全不变。
"""
from __future__ import annotations

import asyncio
import json
import math
import multiprocessing as mp
import os
import shutil
import threading
import uuid
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from app.config import settings
from app.free_strategy.algorithm_profile import build_algorithm_profile
from app.free_strategy.continuation import continue_account_from_backtest
from app.free_strategy.jq_compat.capabilities import analyze_source
from app.free_strategy.process import start_process
from app.free_strategy.paper import MARKET_MODES, PaperTradingSupervisor
from app.free_strategy.store import FreeStrategyStore, PaperAccountStore, now_iso
from app.free_strategy.templates import (
    LEGACY_FIVE_FORTUNES_SOURCE,
    LEGACY_EXTERNAL_LARGE_AMOUNT_FIRST_BOARD_MARKERS,
    LEGACY_EXTERNAL_LARGE_AMOUNT_FIRST_BOARD_SHA256,
    MANAGED_ETF_NAV_ALIGNMENT_SHA256,
    MANAGED_FIVE_FORTUNES_SHA256,
    MANAGED_FOUR_MODE_SHA256,
    MANAGED_LARGE_AMOUNT_FIRST_BOARD_SHA256,
    MANAGED_STRONG_MOMENTUM_SHA256,
    TEMPLATES,
)
from app.market_time import cn_naive_now, cn_today
from app.services import preferences

router = APIRouter(prefix="/api/free-strategies", tags=["free-strategy"])
_jobs: dict[str, tuple[mp.Process, Any]] = {}
_jobs_lock = threading.Lock()
_paper: dict[str, mp.Process] = {}


def _strategy_store(request: Request) -> FreeStrategyStore:
    return FreeStrategyStore(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir)


def _paper_store(request: Request) -> PaperAccountStore:
    return PaperAccountStore(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir)


def _paper_supervisor(request: Request) -> PaperTradingSupervisor | None:
    return getattr(request.app.state, "paper_supervisor", None)


def _public_paper_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in state.items()
        if key not in {"checkpoint", "state", "runtime", "continuation"}
    }


def _paper_daily_return(result: dict[str, Any], store: PaperAccountStore) -> dict[str, Any]:
    """Return the latest session return without confusing it with cumulative return."""
    rows = store.equity_curve(str(result.get("id")))
    if not rows:
        return {"today_return_pct": None, "today_return_date": None}

    valuation = result.get("valuation") if isinstance(result.get("valuation"), dict) else {}
    valuation_date = str(valuation.get("date") or "")[:10] if valuation.get("live") else ""
    latest_date = valuation_date or str(rows[-1].get("timestamp") or "")[:10]
    current_equity = result.get("equity") if valuation_date else rows[-1].get("equity")
    try:
        current = float(current_equity)
    except (TypeError, ValueError):
        current = math.nan
    if not latest_date or not math.isfinite(current):
        return {"today_return_pct": None, "today_return_date": latest_date or None}

    previous_close = next(
        (
            float(row["equity"])
            for row in reversed(rows)
            if str(row.get("timestamp") or "")[:10] < latest_date
            and math.isfinite(float(row.get("equity")))
        ),
        None,
    )
    baseline = previous_close
    if baseline is None:
        config = result.get("config") if isinstance(result.get("config"), dict) else {}
        try:
            baseline = float(config.get("initial_capital"))
        except (TypeError, ValueError):
            baseline = math.nan
    if baseline is None or not math.isfinite(baseline) or baseline <= 0:
        return {"today_return_pct": None, "today_return_date": latest_date}
    return {
        "today_return_pct": (current - baseline) / baseline * 100,
        "today_return_date": latest_date,
    }


def _valued_paper_state(
    state: dict[str, Any],
    supervisor: PaperTradingSupervisor | None,
    store: PaperAccountStore | None = None,
) -> dict[str, Any]:
    result = _public_paper_state(state)
    persisted_max_drawdown = float(state.get("max_drawdown_pct") or 0.0)
    if store is not None:
        persisted_max_drawdown = max(
            persisted_max_drawdown,
            store.max_drawdown_pct(str(state["id"])),
        )
    result["max_drawdown_pct"] = persisted_max_drawdown
    if supervisor is not None and state.get("status") == "running":
        valuation = supervisor.live_valuation(state)
        result["valuation"] = valuation
        if valuation.get("live"):
            for key in ("equity", "return_pct", "drawdown_pct"):
                result[key] = valuation[key]
            result["max_drawdown_pct"] = max(
                persisted_max_drawdown,
                float(valuation.get("max_drawdown_pct") or valuation["drawdown_pct"]),
            )
    result.update(_paper_daily_return(result, store) if store is not None else {
        "today_return_pct": None,
        "today_return_date": None,
    })
    return result


def _paper_account_view(account: dict[str, Any]) -> dict[str, Any]:
    result = dict(account)
    fills_by_order = {
        str(fill.get("order_id")): fill
        for fill in account.get("fills", [])
        if fill.get("order_id")
    }
    result["orders"] = [
        {
            **order,
            "executed_side": fills_by_order.get(str(order.get("id")), {}).get("side"),
        }
        for order in account.get("orders", [])
    ]
    return result


def _daily_report_signal(
    report: dict[str, Any],
    *,
    strategy: str,
    decision_reason_payload,
) -> dict[str, Any] | None:
    trading_date = str(report.get("date") or "")
    if not trading_date:
        return None
    target = [str(symbol) for symbol in report.get("target", []) if symbol]
    decision = dict(report.get("decision") or {})
    held = str(decision.get("held") or "")
    holdings = [held] if held else []
    if decision.get("reason") == "pending" and holdings == target:
        decision["reason"] = "hold_top_rank"
    decision_type = "empty" if not target and not holdings else "hold" if target == holdings else "rebalance"
    return {
        "id": f"signal:{strategy}:{trading_date}:decision",
        "type": "signal",
        "timestamp": f"{trading_date}T13:10:00",
        "signal_type": "daily_decision",
        "strategy": strategy,
        "trading_date": trading_date,
        "decision": decision_type,
        "regime": report.get("regime"),
        "raw_regime": report.get("raw_regime"),
        "target_symbols": target,
        "holding_symbols": holdings,
        "candidates": [
            {"symbol": row.get("symbol"), "score": row.get("score")}
            for row in list(report.get("candidates") or [])[:10]
            if isinstance(row, dict) and row.get("symbol")
        ],
        **decision_reason_payload(decision),
    }


def _five_fortunes_report_signal(report: dict[str, Any]) -> dict[str, Any] | None:
    from app.free_strategy.five_fortunes import decision_reason_payload

    return _daily_report_signal(
        report,
        strategy="five_fortunes",
        decision_reason_payload=decision_reason_payload,
    )


def _five_fortunes_v2_report_signal(report: dict[str, Any]) -> dict[str, Any] | None:
    from app.free_strategy.five_fortunes_v2 import decision_reason_payload

    return _daily_report_signal(
        report,
        strategy="five_fortunes_v2",
        decision_reason_payload=decision_reason_payload,
    )


def _paper_historical_signals(state: dict[str, Any], request: Request) -> list[dict[str, Any]]:
    reports_by_id: dict[str, dict[str, Any]] = {}
    result: dict[str, Any] = {}
    job_id = str(state.get("continuation", {}).get("job_id") or "")
    if job_id:
        result_path = _run_path(request, job_id) / "result.json"
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result = {}

    for source in (
        result.get("state", {}),
        state.get("checkpoint", {}).get("state", {}),
    ):
        if not isinstance(source, dict):
            continue
        five_fortunes = source.get("five_fortunes")
        if not isinstance(five_fortunes, dict):
            five_fortunes = {}
        for report in five_fortunes.get("daily_reports", []):
            if isinstance(report, dict) and (signal := _five_fortunes_report_signal(report)) is not None:
                reports_by_id[str(signal["id"])] = signal

        five_fortunes_v2 = source.get("five_fortunes_v2")
        if not isinstance(five_fortunes_v2, dict):
            five_fortunes_v2 = {}
        for report in five_fortunes_v2.get("daily_reports", []):
            if isinstance(report, dict) and (signal := _five_fortunes_v2_report_signal(report)) is not None:
                reports_by_id[str(signal["id"])] = signal

    signals = list(reports_by_id.values())
    report_dates = {str(signal["trading_date"]) for signal in signals}
    for raw in result.get("strategy_signals", []):
        if not isinstance(raw, dict) or not raw.get("timestamp"):
            continue
        if raw.get("signal_type") == "daily_decision" and str(raw["timestamp"])[:10] in report_dates:
            continue
        payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
        signals.append({
            **payload,
            "id": f"signal:history:{raw.get('id') or raw['timestamp']}",
            "type": "signal",
            "timestamp": str(raw["timestamp"]),
            "signal_type": str(raw.get("signal_type") or "strategy_signal"),
        })
    return signals


def _legacy_paper_curve(state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    initial = float(state.get("config", {}).get("initial_capital") or 1)
    peak = initial
    result = []
    for row in rows:
        equity = float(row.get("equity", initial))
        peak = max(peak, equity)
        result.append({
            "timestamp": str(row["timestamp"]),
            "equity": equity,
            "cash": float(row.get("cash", equity)),
            "nav": equity / initial,
            "drawdown_pct": ((peak - equity) / peak * 100) if peak else 0.0,
            "positions": dict(row.get("positions", {})),
        })
    return result


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


def cleanup_incomplete_backtests(data_dir: Path) -> None:
    """清理上次进程退出后没有结果文件的回测快照。"""
    root = Path(data_dir) / "free_strategy_runs"
    if not root.exists():
        return
    for path in root.iterdir():
        if path.is_dir() and not (path / "result.json").exists():
            shutil.rmtree(path, ignore_errors=True)


def provision_managed_template_strategies(data_dir: Path) -> list[str]:
    """将必须可直接用于模拟盘的内置模板幂等写入策略存储。"""
    store = FreeStrategyStore(data_dir)
    existing = {str(item.get("id")) for item in store.list()}
    provisioned: list[str] = []
    for template_id in ("strong_momentum", "four_mode"):
        if template_id in existing:
            continue
        template = TEMPLATES[template_id]
        store.save(
            template_id,
            str(template["name"]),
            str(template["source"]),
            dict(template.get("config") or {}),
            dialect="native",
        )
        provisioned.append(template_id)
    return provisioned


_LEGACY_FIVE_FORTUNES_CONFIG = {
    "timeframe": "1m",
    "asset_type": "etf",
    "initial_capital": 1_000_000,
    "fees_pct": 0.0002,
    "commission_pct": None,
    "min_commission": 0,
    "stamp_tax_pct": 0.001,
    "transfer_fee_pct": 0,
    "slippage_bps": 5,
    "price_tick": None,
    "lot_size": 100,
    "max_exposure_pct": 1,
    "settlement": "t1",
    "fill_policy": "next_open",
    "benchmark_symbol": "510300.SH",
}


def _migrate_five_fortunes_config(config: dict[str, Any]) -> dict[str, Any]:
    if any(
        key in config and config[key] != value
        for key, value in _LEGACY_FIVE_FORTUNES_CONFIG.items()
    ):
        return config
    return {**config, **TEMPLATES["five_fortunes"]["config"]}


def migrate_legacy_five_fortunes_strategies(data_dir: Path) -> list[str]:
    """Upgrade untouched managed Five Fortunes snapshots and legacy defaults."""
    store = FreeStrategyStore(data_dir)
    migrated = []
    replacement = TEMPLATES["five_fortunes"]["source"]
    for summary in store.list():
        strategy = store.get(str(summary["id"]))
        source = str(strategy["source"])
        if (
            source != LEGACY_FIVE_FORTUNES_SOURCE
            and sha256(source.encode("utf-8")).hexdigest() not in MANAGED_FIVE_FORTUNES_SHA256
        ):
            continue
        store.save(
            strategy["id"],
            strategy["name"],
            replacement,
            _migrate_five_fortunes_config(strategy.get("config", {})),
        )
        migrated.append(str(strategy["id"]))
    return migrated


def migrate_managed_large_amount_first_board(data_dir: Path) -> list[str]:
    """Upgrade unmodified managed first-board strategies to the latest gates."""
    store = FreeStrategyStore(data_dir)
    replacement = TEMPLATES["large_amount_first_board"]["source"]
    replacement_hash = sha256(replacement.encode("utf-8")).hexdigest()
    migrated: list[str] = []
    for summary in store.list():
        strategy = store.get(str(summary["id"]))
        source = str(strategy["source"])
        source_hash = sha256(source.encode("utf-8")).hexdigest()
        if source_hash == replacement_hash:
            continue
        if source_hash not in MANAGED_LARGE_AMOUNT_FIRST_BOARD_SHA256:
            continue
        store.save(
            strategy["id"],
            strategy["name"],
            replacement,
            strategy.get("config", {}),
        )
        migrated.append(str(strategy["id"]))
    return migrated


def migrate_managed_strong_momentum(data_dir: Path) -> list[str]:
    """Upgrade the known pre-tick strong-momentum snapshot to second schedules."""
    store = FreeStrategyStore(data_dir)
    replacement = TEMPLATES["strong_momentum"]["source"]
    replacement_hash = sha256(replacement.encode("utf-8")).hexdigest()
    migrated: list[str] = []
    for summary in store.list():
        strategy = store.get(str(summary["id"]))
        if strategy.get("dialect") == "joinquant":
            continue
        source_hash = sha256(str(strategy["source"]).encode("utf-8")).hexdigest()
        if source_hash == replacement_hash or source_hash not in MANAGED_STRONG_MOMENTUM_SHA256:
            continue
        store.save(
            strategy["id"],
            strategy["name"],
            replacement,
            strategy.get("config", {}),
            dialect="native",
        )
        migrated.append(str(strategy["id"]))
    return migrated


def migrate_managed_four_mode(data_dir: Path) -> dict[str, list[str]]:
    """Upgrade the managed four-mode template and unchanged paper snapshots."""
    strategy_store = FreeStrategyStore(data_dir)
    strategy_id = "four_mode"
    try:
        strategy = strategy_store.get(strategy_id)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"strategies": [], "accounts": []}

    replacement = str(TEMPLATES[strategy_id]["source"])
    replacement_hash = sha256(replacement.encode("utf-8")).hexdigest()
    source_hash = sha256(str(strategy["source"]).encode("utf-8")).hexdigest()
    if source_hash == replacement_hash:
        target = strategy
    elif source_hash in MANAGED_FOUR_MODE_SHA256:
        target = strategy_store.save(
            strategy_id,
            strategy["name"],
            replacement,
            strategy.get("config", {}),
            dialect="native",
        )
    else:
        return {"strategies": [], "accounts": []}

    paper_store = PaperAccountStore(data_dir)
    migrated_accounts: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for account in paper_store.list():
        if str(account.get("strategy_id") or "") != strategy_id:
            continue
        account_id = str(account["id"])
        account_root = paper_store._path(account_id)
        source_path = account_root / "strategy.py"
        state_path = account_root / "state.json"
        try:
            current_source = source_path.read_text(encoding="utf-8")
        except OSError:
            continue
        current_hash = sha256(current_source.encode("utf-8")).hexdigest()
        state_hash = str(account.get("source_hash") or current_hash)
        if current_hash == replacement_hash and state_hash == replacement_hash:
            continue
        if current_hash not in MANAGED_FOUR_MODE_SHA256 and state_hash not in MANAGED_FOUR_MODE_SHA256:
            continue

        backup_root = account_root / "backups" / f"managed-four-mode-{stamp}-{account_id}"
        backup_root.mkdir(parents=True, exist_ok=False)
        shutil.copy2(state_path, backup_root / "state.json")
        shutil.copy2(source_path, backup_root / "strategy.py")
        temporary = source_path.with_name(f".{source_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(replacement, encoding="utf-8")
            os.replace(temporary, source_path)
        finally:
            temporary.unlink(missing_ok=True)
        paper_store.update_fields(account_id, {
            "source_hash": replacement_hash,
            "source_revision": target["revision"],
        })
        paper_store.append_event_once(account_id, {
            "id": f"managed-four-mode:{replacement_hash}",
            "type": "strategy_migration",
            "from_source_hash": state_hash,
            "to_source_hash": replacement_hash,
            "source_revision": target["revision"],
        })
        migrated_accounts.append(account_id)
    return {
        "strategies": [strategy_id] if source_hash != replacement_hash else [],
        "accounts": migrated_accounts,
    }


def migrate_legacy_external_large_amount_first_board(data_dir: Path) -> list[str]:
    """Replace the untouched JoinQuant first-board import with the native minute strategy."""
    store = FreeStrategyStore(data_dir)
    template = TEMPLATES["large_amount_first_board"]
    replacement = str(template["source"])
    replacement_hash = sha256(replacement.encode("utf-8")).hexdigest()
    migrated: list[str] = []
    for summary in store.list():
        strategy = store.get(str(summary["id"]))
        source = str(strategy["source"])
        if strategy.get("dialect") == "joinquant":
            continue
        if str(strategy.get("name") or "").strip() != "首板大成交":
            continue
        source_hash = sha256(source.encode("utf-8")).hexdigest()
        if source_hash not in LEGACY_EXTERNAL_LARGE_AMOUNT_FIRST_BOARD_SHA256:
            continue
        if not all(marker in source for marker in LEGACY_EXTERNAL_LARGE_AMOUNT_FIRST_BOARD_MARKERS):
            continue
        if source_hash == replacement_hash:
            continue
        store.save(
            strategy["id"],
            strategy["name"],
            replacement,
            dict(template["config"]),
            dialect="native",
        )
        migrated.append(str(strategy["id"]))
    return migrated


def migrate_managed_etf_nav_alignment(data_dir: Path) -> dict[str, list[str]]:
    """Upgrade exact managed ETF strategy snapshots while preserving account state."""
    strategy_store = FreeStrategyStore(data_dir)
    targets: dict[str, dict[str, Any]] = {}
    migrated_strategies: list[str] = []
    for summary in strategy_store.list():
        strategy_id = str(summary["id"])
        strategy = strategy_store.get(strategy_id)
        source = str(strategy["source"])
        source_hash = sha256(source.encode("utf-8")).hexdigest()
        template_id = next(
            (
                candidate
                for candidate, hashes in MANAGED_ETF_NAV_ALIGNMENT_SHA256.items()
                if source_hash in hashes
                or source_hash == sha256(TEMPLATES[candidate]["source"].encode("utf-8")).hexdigest()
            ),
            None,
        )
        if template_id is None:
            continue
        replacement = TEMPLATES[template_id]["source"]
        replacement_hash = sha256(replacement.encode("utf-8")).hexdigest()
        if source_hash != replacement_hash:
            strategy = strategy_store.save(
                strategy_id,
                strategy["name"],
                replacement,
                strategy.get("config", {}),
            )
            migrated_strategies.append(strategy_id)
        targets[strategy_id] = {
            "source": replacement,
            "source_hash": replacement_hash,
            "source_revision": strategy["revision"],
            "old_hashes": MANAGED_ETF_NAV_ALIGNMENT_SHA256[template_id],
        }

    paper_store = PaperAccountStore(data_dir)
    migrated_accounts: list[str] = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for account in paper_store.list():
        account_id = str(account["id"])
        target = targets.get(str(account.get("strategy_id")))
        if target is None:
            continue
        account_root = paper_store._path(account_id)
        source_path = account_root / "strategy.py"
        try:
            current_source = source_path.read_text(encoding="utf-8")
        except OSError:
            continue
        current_hash = sha256(current_source.encode("utf-8")).hexdigest()
        state_hash = str(account.get("source_hash") or current_hash)
        if current_hash == target["source_hash"] and state_hash == target["source_hash"]:
            continue
        if current_hash not in target["old_hashes"] and state_hash not in target["old_hashes"]:
            continue

        backup_root = account_root / "backups" / f"managed-nav-alignment-{stamp}"
        backup_root.mkdir(parents=True, exist_ok=False)
        shutil.copy2(account_root / "state.json", backup_root / "state.json")
        shutil.copy2(source_path, backup_root / "strategy.py")
        temporary = source_path.with_name(f".{source_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(str(target["source"]), encoding="utf-8")
            os.replace(temporary, source_path)
        finally:
            temporary.unlink(missing_ok=True)
        paper_store.update_fields(account_id, {
            "source_hash": target["source_hash"],
            "source_revision": target["source_revision"],
        })
        paper_store.append_event_once(account_id, {
            "id": f"managed-nav-alignment:{target['source_hash']}",
            "type": "strategy_migration",
            "from_source_hash": state_hash,
            "to_source_hash": target["source_hash"],
            "source_revision": target["source_revision"],
        })
        migrated_accounts.append(account_id)
    return {
        "strategies": migrated_strategies,
        "accounts": migrated_accounts,
    }


class StrategyWrite(BaseModel):
    id: str | None = None
    name: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    dialect: Literal["native", "joinquant"] | None = None


class RenameWrite(BaseModel):
    name: str = Field(min_length=1, max_length=120)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("名称不能为空")
        return value


class BacktestWrite(BaseModel):
    strategy_id: str
    symbols: list[str] = Field(default_factory=list)
    timeframe: Literal["1d", "30m", "5m", "1m", "tick"] = "1d"
    start: date | None = None
    end: date | None = None
    asset_type: Literal["stock", "etf"] = "stock"
    initial_capital: float = Field(default=1_000_000, gt=0)
    fees_pct: float = Field(default=0.0002, ge=0)
    commission_pct: float | None = Field(default=None, ge=0)
    sell_commission_pct: float | None = Field(default=None, ge=0)
    min_commission: float = Field(default=0, ge=0)
    reserve_buy_fees: bool = True
    stamp_tax_pct: float = Field(default=0.001, ge=0)
    transfer_fee_pct: float = Field(default=0, ge=0)
    slippage_bps: float = Field(default=5, ge=0)
    price_tick: float | None = Field(default=None, gt=0)
    callback_timeout_seconds: float = Field(default=30, ge=0.1, le=120)
    lot_size: int = Field(default=100, ge=1)
    max_exposure_pct: float = Field(default=1.0, gt=0, le=1)
    settlement: Literal["t1", "t0"] = "t1"
    t0_symbols: list[str] = Field(default_factory=list)
    allow_stale_fills: bool = False
    fill_policy: Literal["next_open", "close"] = "next_open"
    limit_up_touch_fill: bool = False
    benchmark_symbol: str = "510300.SH"


class DataHealthWrite(BacktestWrite):
    persist_scan: bool = False


class PaperRiskWrite(BaseModel):
    max_symbol_exposure_pct: float = Field(default=1.0, gt=0, le=1.0)
    daily_loss_pct: float = Field(default=0.10, gt=0, le=0.10)
    max_drawdown_pct: float = Field(default=0.30, gt=0, le=0.30)
    max_orders_per_minute: int = Field(default=60, ge=1, le=60)


class PaperWrite(BacktestWrite):
    callback_timeout_seconds: float = Field(default=120, ge=0.1, le=120)
    name: str = Field(default="量化策略 · 模拟", min_length=1, max_length=40)
    market_mode: Literal["bar_1m", "bar_1d", "poll_3s", "websocket"] | None = None
    market_mode_explicit: bool = Field(default=False, exclude=True, repr=False)
    continuation_job_id: str | None = Field(default=None, min_length=1, max_length=64)
    risk_config: PaperRiskWrite = Field(default_factory=PaperRiskWrite)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: Any) -> str:
        name = str(value).strip()
        if not name:
            raise ValueError("模拟名称不能为空")
        return name

    @model_validator(mode="before")
    @classmethod
    def remember_market_mode_explicit(cls, values: Any):
        data = dict(values or {})
        data["market_mode_explicit"] = "market_mode" in data
        return data

    @model_validator(mode="after")
    def normalize_market_mode(self):
        self.market_mode = self.market_mode or ("bar_1m" if self.timeframe == "1m" else "bar_1d")
        self.timeframe = "1m" if self.market_mode in {"bar_1m", "poll_3s", "websocket"} else "1d"
        return self


class PaperRenameWrite(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=40)
    system_notify_enabled: bool | None = None

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: Any) -> str:
        if value is None:
            return None
        name = str(value).strip()
        if not name:
            raise ValueError("模拟名称不能为空")
        return name


def _validate_payload(req: BacktestWrite, request: Request) -> None:
    if req.timeframe == "tick" and req.asset_type != "stock":
        raise HTTPException(status_code=400, detail="Tick 回测首版仅支持股票，不支持 ETF")
    if req.timeframe not in {"1d", "tick"}:
        capabilities = getattr(request.app.state, "capabilities", None)
        from app.tickflow.capabilities import Cap
        if capabilities is not None and not capabilities.has(Cap.KLINE_MINUTE_BATCH):
            raise HTTPException(status_code=403, detail="当前没有分钟K能力，无法运行该周期；请开通分钟K或配置自定义分钟数据源")
    if req.asset_type not in {"stock", "etf"}:
        raise HTTPException(status_code=400, detail="量化策略只支持股票或 ETF，单次任务不可混合")


def _validate_paper_payload(req: PaperWrite, request: Request) -> None:
    if req.market_mode not in MARKET_MODES:
        raise HTTPException(status_code=400, detail="不支持的模拟盘行情模式")
    if req.market_mode == "bar_1m":
        _validate_payload(req, request)
    if req.market_mode in {"poll_3s", "websocket"}:
        quote_service = getattr(request.app.state, "quote_service", None)
        if quote_service is None or not quote_service.is_realtime_allowed():
            raise HTTPException(status_code=403, detail="当前套餐没有实时行情能力")
        if req.market_mode == "poll_3s" and quote_service.get_min_interval() > 3:
            raise HTTPException(status_code=403, detail=f"当前套餐最小行情间隔为 {quote_service.get_min_interval():g} 秒")


def _validate_strategy_runtime(strategy: dict[str, Any]) -> dict[str, Any] | None:
    """Fail before a worker starts when the saved JQ source needs missing data."""
    if str(strategy.get("dialect") or "native") != "joinquant":
        return None
    report = analyze_source(str(strategy.get("source") or ""))
    unavailable = [
        str(item.get("name"))
        for item in report.get("apis", [])
        if item.get("status") == "unavailable"
    ]
    if unavailable:
        raise HTTPException(
            status_code=400,
            detail=f"聚宽策略包含当前不可用能力: {', '.join(unavailable)}",
        )
    strategy["compatibility_report"] = report
    return report


@router.get("")
def list_strategies(request: Request):
    return {"strategies": _strategy_store(request).list(), "templates": [{"id": k, "name": v["name"]} for k, v in TEMPLATES.items()]}


@router.get("/templates")
def list_templates():
    return {"templates": [{"id": key, **value} for key, value in TEMPLATES.items()]}


@router.post("")
def create_strategy(req: StrategyWrite, request: Request):
    return _strategy_store(request).save(
        req.id,
        req.name,
        req.source,
        req.config,
        dialect=req.dialect,
    )


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
        metadata = run.get("metadata", {})
        manifest = _read_run_manifest(path)
        result.append({
            "job_id": path.name,
            "name": manifest.get("display_name") or metadata.get("strategy_name") or path.name,
            "final_equity": run.get("final_equity"),
            "return_pct": run.get("return_pct"),
            "max_drawdown_pct": run.get("max_drawdown_pct"),
            "fills": len(run.get("fills", [])),
            "metadata": metadata,
        })
    return {"runs": result}


def _read_run_manifest(path: Path) -> dict[str, Any]:
    manifest_path = path / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _active_backtest(job_id: str) -> bool:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            return False
        is_alive = getattr(job[0], "is_alive", None)
        if callable(is_alive) and not is_alive():
            _jobs.pop(job_id, None)
            return False
        return True


def _paper_backtest_links(job_id: str, request: Request) -> list[dict[str, str]]:
    """Find paper accounts whose historical state still depends on a run."""
    links: list[dict[str, str]] = []
    for account in _paper_store(request).list():
        continuation = account.get("continuation")
        if not isinstance(continuation, dict) or str(continuation.get("job_id") or "") != job_id:
            continue
        account_id = str(account.get("id") or "")
        links.append({
            "id": account_id,
            "name": str(account.get("name") or account_id),
        })
    return links


@router.get("/backtest/{job_id}")
def get_backtest_result(job_id: str, request: Request):
    path = _run_path(request, job_id) / "result.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="回测结果不存在")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="回测结果文件损坏") from None


@router.patch("/backtest/{job_id}")
def rename_backtest(job_id: str, req: RenameWrite, request: Request):
    if _active_backtest(job_id):
        raise HTTPException(status_code=409, detail="运行中的回测不能重命名")
    path = _run_path(request, job_id)
    if not (path / "result.json").exists():
        raise HTTPException(status_code=404, detail="回测结果不存在")
    manifest = _read_run_manifest(path)
    manifest.update({"job_id": job_id, "display_name": req.name, "updated_at": now_iso()})
    (path / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"job_id": job_id, "name": req.name}


@router.delete("/backtest/{job_id}")
def delete_backtest(job_id: str, request: Request):
    if _active_backtest(job_id):
        raise HTTPException(status_code=409, detail="运行中的回测不能删除，请先停止任务")
    path = _run_path(request, job_id)
    if not path.is_dir():
        raise HTTPException(status_code=404, detail="回测结果不存在")
    links = _paper_backtest_links(job_id, request)
    if links:
        labels = ", ".join(f"{item['name']}({item['id']})" for item in links)
        raise HTTPException(
            status_code=409,
            detail=f"回测仍被模拟账户引用，请先停止并删除或迁移账户: {labels}",
        )
    shutil.rmtree(path)
    return {"ok": True}


@router.get("/{strategy_id}")
def get_strategy(strategy_id: str, request: Request):
    try:
        return _strategy_store(request).get(strategy_id)
    except (FileNotFoundError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail="量化策略不存在") from None


@router.put("/{strategy_id}")
def update_strategy(strategy_id: str, req: StrategyWrite, request: Request):
    return _strategy_store(request).save(
        strategy_id,
        req.name,
        req.source,
        req.config,
        dialect=req.dialect,
    )


@router.patch("/{strategy_id}")
def rename_strategy(strategy_id: str, req: RenameWrite, request: Request):
    try:
        return _strategy_store(request).rename(strategy_id, req.name)
    except (FileNotFoundError, json.JSONDecodeError):
        raise HTTPException(status_code=404, detail="量化策略不存在") from None


def _strategy_links(strategy_id: str, request: Request) -> tuple[int, int]:
    backtests = sum(
        1 for path in _run_root(request).glob("*")
        if path.is_dir() and _read_run_manifest(path).get("strategy_id") == strategy_id
    )
    accounts = sum(1 for account in _paper_store(request).list() if account.get("strategy_id") == strategy_id)
    return backtests, accounts


@router.delete("/{strategy_id}")
def delete_strategy(strategy_id: str, request: Request):
    store = _strategy_store(request)
    try:
        store.get(strategy_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="量化策略不存在") from None
    backtests, accounts = _strategy_links(strategy_id, request)
    if backtests or accounts:
        links = []
        if backtests:
            links.append(f"{backtests} 条回测记录")
        if accounts:
            links.append(f"{accounts} 个模拟盘账户")
        raise HTTPException(status_code=409, detail=f"无法删除量化策略：仍关联{' 和 '.join(links)}，请先逐项删除")
    store.delete(strategy_id)
    return {"ok": True}


def _job_payload(req: BacktestWrite, strategy: dict[str, Any], request: Request) -> dict[str, Any]:
    end = req.end or cn_today()
    start = req.start or (end - timedelta(days=365 * 3 if req.timeframe == "1d" else 90))
    config = req.model_dump(exclude={
        "strategy_id", "symbols", "timeframe", "start", "end", "persist_scan",
    })
    legacy_symbols = req.symbols or strategy.get("config", {}).get("symbols", [])
    source_digest = sha256(strategy["source"].encode("utf-8")).hexdigest()
    return {"data_dir": str(getattr(request.app.state, "datastore", None).data_dir if hasattr(request.app.state, "datastore") else settings.data_dir),
            "source": strategy["source"], "strategy_id": strategy.get("id"), "strategy_name": strategy.get("name"),
            "source_revision": strategy.get("revision"), "strategy_source_sha256": source_digest,
            "dialect": strategy.get("dialect", "native"),
            "compatibility_report": strategy.get("compatibility_report"), "symbols": legacy_symbols,
            "timeframe": req.timeframe, "asset_type": req.asset_type, "start": start.isoformat(), "end": end.isoformat(), "config": config,
            "data_provider": (
                "qmt"
                if req.timeframe == "tick"
                else preferences.get_daily_data_provider()
                if req.timeframe == "1d"
                else preferences.get_minute_data_provider()
            )}


@router.post("/backtest/data-health")
async def backtest_data_health(req: DataHealthWrite, request: Request):
    """Resolve the strategy universe, then inspect its required backtest data."""
    _validate_payload(req, request)
    if req.asset_type != "etf" and req.timeframe != "tick":
        return {"status": "not_applicable", "issues": [], "symbol_count": 0, "scan_id": None}
    try:
        strategy = _strategy_store(request).get(req.strategy_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="量化策略不存在") from None
    _validate_strategy_runtime(strategy)

    from datetime import time

    from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
    from app.free_strategy.process import _instrument_records, _resolve_symbols
    from app.services.etf_data_repair import inspect_etf_data

    payload = _job_payload(req, strategy, request)
    repo = request.app.state.repo
    try:
        config = FreeStrategyConfig(**payload["config"])
        instruments = _instrument_records(
            repo,
            payload["asset_type"],
            payload["timeframe"],
            date.fromisoformat(payload["start"]),
            date.fromisoformat(payload["end"]),
        )
        engine = FreeStrategyEngine(
            strategy["source"],
            payload["timeframe"],
            config,
            instruments=instruments,
            dialect=strategy.get("dialect", "native"),
        )
        symbols, universe_source = _resolve_symbols(engine, payload)
        if req.timeframe == "tick":
            from app.free_strategy.tick_health import inspect_tick_data

            result = await asyncio.to_thread(
                inspect_tick_data,
                repo,
                symbols,
                date.fromisoformat(payload["start"]),
                date.fromisoformat(payload["end"]),
            )
            return {
                **result,
                "execution_mode": engine.execution_mode,
                "universe_source": universe_source,
                "provider": "qmt",
            }
        scheduled_intraday = engine.execution_mode == "scheduled" and any(
            time.fromisoformat(value) < time(15, 0) for value in engine.scheduled_times
        )
        result = await asyncio.to_thread(
            inspect_etf_data,
            repo,
            symbols,
            date.fromisoformat(payload["start"]),
            date.fromisoformat(payload["end"]),
            require_minute=req.timeframe != "1d" or scheduled_intraday,
            min_daily_bars=max(1, engine.history_requirements.get("1d", 0)),
            persist_scan=req.persist_scan,
        )
        return {
            **result,
            "execution_mode": engine.execution_mode,
            "universe_source": universe_source,
        }
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backtest")
def create_backtest(req: BacktestWrite, request: Request):
    _validate_payload(req, request)
    try:
        strategy = _strategy_store(request).get(req.strategy_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="量化策略不存在") from None
    _validate_strategy_runtime(strategy)
    job_id = uuid.uuid4().hex[:12]
    payload = _job_payload(req, strategy, request)
    run_root = _run_path(request, job_id)
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "strategy.py").write_text(strategy["source"], encoding="utf-8")
    payload["run_dir"] = str(run_root)
    (run_root / "manifest.json").write_text(json.dumps({"job_id": job_id, "strategy_id": req.strategy_id, "source_revision": strategy.get("revision"), "strategy_source_sha256": payload["strategy_source_sha256"], "payload": {k: v for k, v in payload.items() if k != "source"}}, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        process, output = start_process(payload)
    except Exception:
        shutil.rmtree(run_root, ignore_errors=True)
        raise
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
                with _jobs_lock:
                    _jobs.pop(job_id, None)
                yield f"data: {json.dumps({'type': 'error', 'error': '回测子进程异常退出'}, ensure_ascii=False)}\n\n"
                return
            await asyncio.sleep(0.1)

    return StreamingResponse(events(), media_type="text/event-stream")


@router.post("/backtest/{job_id}/cancel")
def cancel_backtest(job_id: str, request: Request):
    with _jobs_lock:
        job = _jobs.pop(job_id, None)
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
    shutil.rmtree(_run_path(request, job_id), ignore_errors=True)
    return {"ok": True}


def _paper_loop(account_id: str, root: str) -> None:
    from dataclasses import asdict
    import time
    from datetime import time as clock_time
    from app.free_strategy.process import (
        configure_strategy_data_loaders,
        _instrument_records,
        _load_market_data,
        _load_scheduled_history,
        _load_scheduled_history_batch,
        _merge_market_data,
        _prepare_market_data,
        _prepare_market_reference,
        _read_rows,
        _resolve_symbols,
        advance_scheduled_session,
    )
    from app.free_strategy.engine import FreeStrategyConfig, FreeStrategyEngine
    from app.tickflow.repository import DataStore, KlineRepository
    account_root = Path(root) / account_id
    paper_store = PaperAccountStore(Path(root).parent)
    path = account_root / "heartbeat.json"
    state_path = account_root / "state.json"
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        source = (account_root / "strategy.py").read_text(encoding="utf-8")
        config = dict(state.get("config", {}))
        legacy_symbols = config.pop("symbols", [])
        timeframe = config.pop("timeframe", "1m")
        asset_type = config.pop("asset_type", "stock")
        config.pop("strategy_id", None)
        configured_start = config.pop("start", None)
        config.pop("end", None)
        config.pop("name", None)
        engine_config = FreeStrategyConfig(asset_type=asset_type, **config)
        repo = KlineRepository(DataStore(Path(root).parent))
        instruments = _instrument_records(repo, asset_type, timeframe)
        engine = FreeStrategyEngine(
            source,
            timeframe,
            engine_config,
            state=state.get("state", {}),
            instruments=instruments,
            dialect=str(state.get("dialect") or "native"),
        )
        today = cn_today()
        try:
            strategy_start = date.fromisoformat(str(configured_start))
        except (TypeError, ValueError):
            strategy_start = today
        configure_strategy_data_loaders(
            engine,
            repo,
            repo.store.data_dir,
            source,
            strategy_start,
            today,
        )
        symbols, universe_source = _resolve_symbols(engine, {"symbols": legacy_symbols})
        state["universe"] = symbols
        state["universe_source"] = universe_source
        if state.get("checkpoint"):
            engine.restore_checkpoint(state["checkpoint"])
        else:
            engine.account.restore(state.get("account", {}))
            engine.restore_runtime(state.get("runtime"))
        market_data, warmup_metadata = _prepare_market_data(
            repo, engine, symbols, today, today, asset_type, timeframe,
        )
        if engine.execution_mode == "scheduled":
            engine.set_history_loader(
                lambda symbol, count, period, cutoff: _load_scheduled_history(
                    repo, market_data, asset_type, symbol, count, period, cutoff,
                )
            )
            engine.set_history_batch_loader(
                lambda symbols, count, period, cutoff: _load_scheduled_history_batch(
                    repo,
                    market_data,
                    asset_type,
                    symbols,
                    count,
                    period,
                    cutoff,
                )
            )
        state["warmup"] = warmup_metadata
        state["execution_mode"] = engine.execution_mode
        state["scheduled_times"] = engine.scheduled_times
        state = paper_store.update_fields(account_id, {
            "universe": symbols,
            "universe_source": universe_source,
            "warmup": warmup_metadata,
            "execution_mode": engine.execution_mode,
            "scheduled_times": engine.scheduled_times,
        })
    except Exception as exc:
        # Keep the supervisor alive and make the failure inspectable from the API.
        paper_store.update_fields(account_id, {
            "status": "paused",
            "last_error": f"模拟账户初始化失败: {exc}",
        })
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
            today = cn_today()
            fill_count = len(engine.account.fills)
            log_count = len(engine.logs)
            callback_count = engine.callbacks_executed
            rows_consumed = engine.market_rows_consumed
            did_finish = False
            if engine.execution_mode == "scheduled":
                if not any(
                    symbol in symbols and symbol_day == today
                    for symbol, symbol_day in market_data.daily
                ):
                    daily_update = _load_market_data(repo, symbols, today, today, asset_type)
                    _merge_market_data(market_data, daily_update)
                trading_day = any(
                    symbol in symbols and symbol_day == today
                    for symbol, symbol_day in market_data.daily
                )
                if trading_day:
                    now = cn_naive_now()
                    was_finished = engine._session_finished
                    session_end = max(
                        clock_time(15, 1),
                        max(clock_time.fromisoformat(value) for value in engine.scheduled_times),
                    )
                    advance_scheduled_session(
                        repo,
                        engine,
                        market_data,
                        today,
                        now,
                        asset_type,
                        timeframe,
                        finalize=now.time() >= session_end and not engine._session_finished,
                    )
                    symbols = engine.universe
                    state["universe"] = symbols
                    state["callbacks_executed"] = engine.callbacks_executed
                    state["market_rows_consumed"] = engine.market_rows_consumed
                    last_bar = (
                        engine._last_timestamp.isoformat()
                        if engine._last_timestamp is not None else last_bar
                    )
                    did_finish = not was_finished and engine._session_finished
            else:
                if engine.market_history_requirements and engine._active_session_date != today:
                    engine.market_history_metadata = _prepare_market_reference(
                        repo, engine, today, today, asset_type, market_data,
                    )
                    if today.weekday() < 5:
                        engine.begin_session(today)
                        symbols = engine.universe
                        state["universe"] = symbols
                if timeframe == "1d" and not any(
                    symbol_day == today for _, symbol_day in market_data.daily
                ):
                    daily_update = _load_market_data(repo, symbols, today, today, asset_type)
                    _merge_market_data(market_data, daily_update)
                try:
                    bars = _read_rows(
                        repo, symbols, today, today, asset_type, timeframe,
                        market_data=market_data,
                    )
                except ValueError as exc:
                    # 盘前和盘中尚未有完整 bar 时保持账户运行，等下一轮数据同步即可。
                    if "没有可用" not in str(exc):
                        raise
                    bars = []
                fresh = [bar for bar in bars if bar.timestamp.isoformat() > (last_bar or "")]
                if fresh:
                    engine.run(fresh, finalize_session=False)
                    last_bar = fresh[-1].timestamp.isoformat()
                if cn_naive_now().time() >= clock_time(15, 1):
                    did_finish = engine.finish_session()
            changed = (
                len(engine.account.fills) != fill_count
                or len(engine.logs) != log_count
                or engine.callbacks_executed != callback_count
                or engine.market_rows_consumed != rows_consumed
                or did_finish
            )
            if changed:
                checkpoint = engine.checkpoint()
                state["checkpoint"] = checkpoint
                state["account"] = checkpoint["account"]
                state["state"] = checkpoint["state"]
                state["runtime"] = checkpoint["runtime"]
                state["last_bar"] = last_bar
                for fill in engine.account.fills[fill_count:]:
                    paper_store.append_event(account_id, {"type": "fill", **asdict(fill)})
                for log in engine.logs[log_count:]:
                    paper_store.append_event(account_id, {"type": "log", **log})
                paper_store.update_fields(account_id, {
                    "checkpoint": state["checkpoint"],
                    "account": state["account"],
                    "state": state["state"],
                    "runtime": state["runtime"],
                    "last_bar": last_bar,
                    "universe": state.get("universe", []),
                    "callbacks_executed": state.get("callbacks_executed", 0),
                    "market_rows_consumed": state.get("market_rows_consumed", 0),
                })
        except Exception as exc:  # noqa: BLE001
            paper_store.update_fields(account_id, {
                "status": "paused",
                "last_error": str(exc),
            })
        time.sleep(5)


@router.get("/paper/accounts")
def list_paper_accounts(request: Request):
    supervisor = _paper_supervisor(request)
    store = _paper_store(request)
    return {"accounts": [
        _valued_paper_state(state, supervisor, store)
        for state in store.list()
    ]}


@router.post("/paper/accounts")
def create_paper_account(req: PaperWrite, request: Request):
    strategy = _strategy_store(request).get(req.strategy_id)
    _validate_strategy_runtime(strategy)
    if not req.market_mode_explicit:
        if strategy.get("execution_mode_hint") == "quote":
            req.market_mode = "websocket"
            req.timeframe = "1m"
        elif (strategy.get("config") or {}).get("timeframe") == "1m":
            req.market_mode = "bar_1m"
            req.timeframe = "1m"
    _validate_paper_payload(req, request)
    account_id = uuid.uuid4().hex[:12]
    payload = req.model_dump(mode="json")
    risk_config = payload.pop("risk_config")
    continuation_job_id = payload.pop("continuation_job_id", None)
    state = {
        "id": account_id,
        "name": req.name,
        "strategy_id": req.strategy_id,
        "source_revision": strategy.get("revision"),
        "source_hash": sha256(strategy["source"].encode("utf-8")).hexdigest(),
        "dialect": strategy.get("dialect", "native"),
        "compatibility_report": strategy.get("compatibility_report"),
        "market_mode": req.market_mode,
        "risk_config": risk_config,
        "risk_status": {"daily_loss_locked": False, "drawdown_locked": False, "reason": None},
        "notification_channels": preferences.get_webhook_default_channels(),
        "system_notify_enabled": False,
        "status": "stopped",
        "sync": {
            "phase": "idle",
            "from": None,
            "target": None,
            "through": None,
            "processed_days": 0,
            "total_days": 0,
            "missing_symbols": [],
            "updated_at": now_iso(),
        },
        "config": payload,
        "cash": req.initial_capital,
        "equity": req.initial_capital,
        "return_pct": 0.0,
        "drawdown_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "positions": {},
        "state": {},
        "created_at": now_iso(),
    }
    store = _paper_store(request)
    result = store.save(state)
    path = store._path(account_id)
    (path / "strategy.py").write_text(strategy["source"], encoding="utf-8")
    store.append_event(account_id, {"type": "created", "strategy_revision": strategy.get("revision")})
    if continuation_job_id:
        try:
            _run_path(request, continuation_job_id)
            data_dir = Path(
                getattr(request.app.state, "datastore", None).data_dir
                if hasattr(request.app.state, "datastore") else settings.data_dir
            )
            result = continue_account_from_backtest(data_dir, account_id, continuation_job_id)
        except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError, OSError) as exc:
            store.delete(account_id)
            raise HTTPException(status_code=409, detail=str(exc)) from None
        store.append_event(account_id, {"type": "continued", "job_id": continuation_job_id})
        return _public_paper_state(result)
    return result


@router.get("/paper/status")
def paper_status(request: Request):
    supervisor = _paper_supervisor(request)
    if supervisor is not None:
        return supervisor.status()
    return {
        "running_accounts": 0,
        "mode_counts": {},
        "poll_3s": {"active": False, "available": False, "min_interval_s": None, "interval_s": None, "actual_fetch_ms": None},
        "websocket": {"status": "disconnected", "symbols": 0, "capacity": 200, "last_error": None},
        "last_quote_at": None,
    }


@router.get("/paper/accounts/{account_id}")
def get_paper_account(account_id: str, request: Request):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    result = _valued_paper_state(state, _paper_supervisor(request), store)
    account = dict(state.get("account") or state.get("checkpoint", {}).get("account", {}))
    curve = store.equity_curve(account_id)
    if not curve and account.get("equity_curve"):
        curve = _legacy_paper_curve(state, account["equity_curve"])
    account["equity_curve"] = curve
    result["account"] = _paper_account_view(account)
    try:
        strategy = _strategy_store(request).get(str(state.get("strategy_id") or ""))
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        strategy = {
            "id": state.get("strategy_id"),
            "name": state.get("name"),
            "source": "",
        }
    result["algorithm"] = build_algorithm_profile(strategy, state)
    result["events"] = store.events(account_id)
    return result


@router.patch("/paper/accounts/{account_id}")
def rename_paper_account(account_id: str, req: PaperRenameWrite, request: Request):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    previous = str(state.get("name") or "")

    def rename(current: dict[str, Any]) -> dict[str, Any]:
        if req.name is not None:
            current["name"] = req.name
            config = dict(current.get("config", {}))
            config["name"] = req.name
            current["config"] = config
        if req.system_notify_enabled is not None:
            current["system_notify_enabled"] = req.system_notify_enabled
        return current

    saved = store.update(account_id, rename)
    if req.name is not None and previous != req.name:
        store.append_event(account_id, {
            "type": "renamed",
            "previous_name": previous,
            "name": req.name,
        })
    return _public_paper_state(saved)


@router.delete("/paper/accounts/{account_id}")
def delete_paper_account(account_id: str, request: Request):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    supervisor = _paper_supervisor(request)
    process_alive = supervisor.is_alive(account_id) if supervisor is not None else bool(_paper.get(account_id) and _paper[account_id].is_alive())
    if state.get("status") != "stopped" or process_alive:
        raise HTTPException(status_code=409, detail="请先停止模拟盘账户再删除")
    _paper.pop(account_id, None)
    store.delete(account_id)
    return {"ok": True}


@router.post("/paper/accounts/{account_id}/{action}")
def paper_action(account_id: str, action: Literal["start", "pause", "resume", "stop", "unlock-risk"], request: Request):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    supervisor = _paper_supervisor(request)
    if supervisor is not None:
        try:
            if action in {"start", "resume"}:
                return _public_paper_state(supervisor.start(account_id))
            if action == "unlock-risk":
                return _public_paper_state(supervisor.unlock_risk(account_id))
            return _public_paper_state(
                supervisor.pause_or_stop(
                    account_id, "paused" if action == "pause" else "stopped",
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None
    if action in {"start", "resume"}:
        process = _paper.get(account_id)
        if process is None or not process.is_alive():
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
    return _public_paper_state(store.update_fields(account_id, {"status": state["status"]}))


@router.get("/paper/accounts/{account_id}/events")
def paper_events(
    account_id: str,
    request: Request,
    cursor: int | None = None,
    limit: int = 100,
    types: str | None = None,
):
    store = _paper_store(request)
    try:
        store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    event_types = {item.strip() for item in types.split(",") if item.strip()} if types else None
    return store.events_page(account_id, cursor=cursor, limit=limit, event_types=event_types)


@router.get("/paper/accounts/{account_id}/signals")
def paper_signals(account_id: str, request: Request, limit: int = 500):
    store = _paper_store(request)
    try:
        state = store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    combined = {
        str(signal["id"]): signal
        for signal in _paper_historical_signals(state, request)
    }
    for event in store.events(account_id, limit=100_000):
        if event.get("type") == "signal" and event.get("id"):
            combined[str(event["id"])] = event
    rows = sorted(combined.values(), key=lambda row: str(row.get("timestamp") or ""), reverse=True)
    bounded_limit = max(1, min(limit, 2_000))
    return {"signals": rows[:bounded_limit], "total": len(rows)}


@router.get("/paper/accounts/{account_id}/stream")
async def paper_event_stream(account_id: str, request: Request, after: int = 0):
    store = _paper_store(request)
    try:
        store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None

    async def generate():
        cursor = max(0, after)
        while not await request.is_disconnected():
            rows = [event for event in store.events(account_id, limit=500) if int(event.get("sequence", 0)) > cursor]
            if rows:
                for event in rows:
                    cursor = max(cursor, int(event.get("sequence", 0)))
                    yield f"id: {cursor}\nevent: paper\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"
            else:
                yield ": keep-alive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@router.get("/paper/accounts/{account_id}/orders")
def paper_orders(account_id: str, request: Request):
    try:
        state = _paper_store(request).get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    account = state.get("account") or state.get("checkpoint", {}).get("account", {})
    return {"orders": _paper_account_view(account)["orders"]}


@router.get("/paper/accounts/{account_id}/fills")
def paper_fills(account_id: str, request: Request):
    store = _paper_store(request)
    try:
        store.get(account_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="模拟账户不存在") from None
    return {"fills": [event for event in store.events(account_id) if event.get("type") == "fill"]}


@router.get("/paper/accounts/{account_id}/logs")
def paper_logs(account_id: str, request: Request):
    logs = _paper_store(request).log_events(account_id)
    return {
        "logs": [
            {
                **event,
                "level": event.get("level") or ("ERROR" if event.get("type") == "error" else "INFO"),
                "source": event.get("source") or "runtime",
            }
            for event in logs
        ],
    }
