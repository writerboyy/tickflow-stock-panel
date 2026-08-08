"""Initialize a paper account from a compatible historical backtest checkpoint."""
from __future__ import annotations

import copy
import json
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.free_strategy.store import PaperAccountStore, now_iso
from app.market_time import cn_today


_EXECUTION_CONFIG_KEYS = (
    "asset_type",
    "initial_capital",
    "fees_pct",
    "commission_pct",
    "sell_commission_pct",
    "min_commission",
    "reserve_buy_fees",
    "stamp_tax_pct",
    "transfer_fee_pct",
    "slippage_bps",
    "price_tick",
    "lot_size",
    "max_exposure_pct",
    "settlement",
    "t0_symbols",
    "allow_stale_fills",
    "fill_policy",
    "benchmark_symbol",
)

_BAR_TIMEFRAMES = {
    "bar_1m": "1m",
    "bar_5m": "5m",
    "bar_30m": "30m",
    "bar_1d": "1d",
}
_QUOTE_MODES = {"poll_3s", "websocket"}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_compatibility(
    account: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    if account.get("status") == "running":
        raise ValueError("模拟账户运行中，请先暂停或停止")
    if account.get("continuation"):
        raise ValueError("模拟账户已经从历史检查点续跑")
    if account.get("strategy_id") != manifest.get("strategy_id"):
        raise ValueError("回测策略与模拟账户策略不一致")
    if account.get("source_hash") != manifest.get("strategy_source_sha256"):
        raise ValueError("回测源码与模拟账户源码不一致")
    paper_config = account.get("config", {})
    backtest_config = manifest.get("payload", {}).get("config", {})
    mismatches = [
        key
        for key in _EXECUTION_CONFIG_KEYS
        if paper_config.get(key) != backtest_config.get(key)
    ]
    if mismatches:
        raise ValueError(f"回测与模拟账户执行参数不一致: {', '.join(mismatches)}")


def _validate_execution_compatibility(
    account: dict[str, Any],
    manifest: dict[str, Any],
    result: dict[str, Any],
) -> None:
    """Keep the historical checkpoint on the same execution contract."""
    payload = manifest.get("payload") if isinstance(manifest.get("payload"), dict) else {}
    metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    backtest_config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    backtest_timeframe = str(
        payload.get("timeframe")
        or metadata.get("timeframe")
        or backtest_config.get("timeframe")
        or ""
    ).strip()
    backtest_asset_type = str(
        payload.get("asset_type")
        or metadata.get("asset_type")
        or backtest_config.get("asset_type")
        or ""
    ).strip()
    paper_config = account.get("config") if isinstance(account.get("config"), dict) else {}
    paper_timeframe = str(paper_config.get("timeframe") or "").strip()
    paper_asset_type = str(paper_config.get("asset_type") or account.get("asset_type") or "").strip()
    mismatches: list[str] = []
    if not backtest_timeframe:
        mismatches.append("timeframe(回测缺少执行周期)")
    elif paper_timeframe != backtest_timeframe:
        mismatches.append("timeframe")
    if backtest_asset_type and paper_asset_type != backtest_asset_type:
        mismatches.append("asset_type")

    execution_mode = str(result.get("execution_mode") or metadata.get("execution_mode") or "").strip()
    scheduled_times = tuple(str(value) for value in (result.get("scheduled_times") or metadata.get("scheduled_times") or ()))
    if execution_mode not in {"full_bar", "scheduled", "quote"}:
        mismatches.append("execution_mode(回测缺少执行模式)")
    if execution_mode == "scheduled" and not scheduled_times:
        mismatches.append("scheduled_times(回测缺少定时点)")

    market_mode = str(account.get("market_mode") or paper_config.get("market_mode") or "").strip()
    if not market_mode:
        market_mode = next(
            (mode for mode, timeframe in _BAR_TIMEFRAMES.items() if timeframe == paper_timeframe),
            "",
        )
    expected_bar_timeframe = _BAR_TIMEFRAMES.get(market_mode)
    if expected_bar_timeframe and paper_timeframe != expected_bar_timeframe:
        mismatches.append("market_mode/timeframe")
    if execution_mode == "quote" and market_mode not in _QUOTE_MODES:
        mismatches.append("market_mode(需要实时行情模式)")
    elif execution_mode in {"full_bar", "scheduled"} and market_mode in _QUOTE_MODES:
        mismatches.append("market_mode(需要K线模式)")

    saved_execution_mode = str(account.get("execution_mode") or "").strip()
    if saved_execution_mode and execution_mode and saved_execution_mode != execution_mode:
        mismatches.append("execution_mode")
    saved_schedule = tuple(str(value) for value in (account.get("scheduled_times") or ()))
    if saved_schedule and scheduled_times and saved_schedule != scheduled_times:
        mismatches.append("scheduled_times")
    if mismatches:
        raise ValueError(f"回测与模拟账户执行参数不一致: {', '.join(dict.fromkeys(mismatches))}")


def compact_paper_checkpoint(checkpoint: dict[str, Any]) -> dict[str, Any]:
    account = checkpoint.get("account", {})
    account["equity_curve"] = []
    five_fortunes = checkpoint.get("state", {}).get("five_fortunes")
    if isinstance(five_fortunes, dict) and five_fortunes.get("daily"):
        five_fortunes["daily"] = {"__paper_history_loader__": []}
        five_fortunes["daily_reports"] = list(five_fortunes.get("daily_reports", []))[-30:]
    five_fortunes_v2 = checkpoint.get("state", {}).get("five_fortunes_v2")
    if isinstance(five_fortunes_v2, dict) and five_fortunes_v2.get("daily"):
        five_fortunes_v2["daily"] = {"__paper_history_loader__": []}
        five_fortunes_v2["daily_reports"] = list(five_fortunes_v2.get("daily_reports", []))[-30:]
    performance_small_cap = checkpoint.get("state", {}).get("performance_small_cap")
    if isinstance(performance_small_cap, dict):
        performance_small_cap["daily_reports"] = list(
            performance_small_cap.get("daily_reports", [])
        )[-30:]
    return checkpoint


def continue_account_from_backtest(
    data_dir: Path,
    account_id: str,
    job_id: str,
    *,
    today: date | None = None,
) -> dict[str, Any]:
    data_dir = Path(data_dir)
    store = PaperAccountStore(data_dir)
    state = store.get(account_id)
    run_root = data_dir / "free_strategy_runs" / job_id
    manifest = _load_json(run_root / "manifest.json")
    result = _load_json(run_root / "result.json")
    result_metadata = result.get("metadata") if isinstance(result.get("metadata"), dict) else {}
    _validate_compatibility(state, manifest)
    _validate_execution_compatibility(state, manifest, result)

    checkpoint = copy.deepcopy(result.get("checkpoint"))
    if not checkpoint or not checkpoint.get("account") or not checkpoint.get("runtime"):
        raise ValueError("回测结果不包含可续跑的完整检查点")
    daily_curve = list(result.get("daily_equity_curve") or [])
    if not daily_curve:
        raise ValueError("回测结果不包含历史净值")

    backup_root = store._path(account_id) / "backups"
    backup_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    shutil.copy2(store._path(account_id) / "state.json", backup_root / f"state-{stamp}.json")

    initial_capital = float(state["config"]["initial_capital"])
    cutoff = (today or cn_today()) - timedelta(days=365)
    equity_rows = [
        {
            "timestamp": str(row["timestamp"]),
            "equity": float(row["equity"]),
            "cash": float(row["cash"]),
            "nav": float(row.get("strategy_nav") or float(row["equity"]) / initial_capital),
            "drawdown_pct": float(row.get("drawdown_pct") or 0),
            "positions": {
                symbol: float(quantity)
                for symbol, quantity in row.get("positions", {}).items()
                if float(quantity) > 0
            },
            "avg_cost": {
                symbol: float(value)
                for symbol, value in row.get("avg_cost", {}).items()
                if symbol in row.get("positions", {})
                and float(row["positions"][symbol]) > 0
            },
            "source": "backtest",
        }
        for row in daily_curve
        if date.fromisoformat(str(row.get("date") or row["timestamp"])[:10]) >= cutoff
    ]
    peak_equity = max(float(row["equity"]) for row in daily_curve)
    final_equity = float(result["final_equity"])

    account = checkpoint["account"]
    account["orders"] = []
    account["fills"] = []
    account["corporate_actions"] = []
    compact_paper_checkpoint(checkpoint)
    checkpoint["order_counter"] = 0
    checkpoint["pending_orders"] = []
    checkpoint["risk"] = {
        **checkpoint.get("risk", {}),
        "status": {
            "daily_loss_locked": False,
            "drawdown_locked": False,
            "reason": None,
            "triggered_at": None,
        },
        "peak_equity": peak_equity,
        "day": None,
        "day_start_equity": final_equity,
        "order_times": [],
    }
    if not checkpoint.get("universe"):
        state_payload = checkpoint.get("state", {})
        strategy_state = state_payload.get("five_fortunes", {})
        if not strategy_state:
            strategy_state = state_payload.get("five_fortunes_v2", {})
        if not strategy_state:
            strategy_state = state_payload.get("performance_small_cap", {})
        checkpoint["universe"] = sorted(set(
            strategy_state.get("subscription_pool", [])
            + strategy_state.get("sorted_stocks", [])
            + strategy_state.get("selection_cache", [])
            + [
                symbol
                for symbol, quantity in account.get("positions", {}).items()
                if float(quantity) > 0
            ]
            + [str(state["config"].get("benchmark_symbol", "510300.SH"))]
        ))

    positions = {
        symbol: float(quantity)
        for symbol, quantity in account.get("positions", {}).items()
        if float(quantity) > 0
    }
    for key in ("account", "state", "runtime"):
        state.pop(key, None)
    state.update({
        "checkpoint": checkpoint,
        "cash": float(account["cash"]),
        "equity": final_equity,
        "return_pct": (final_equity / initial_capital - 1) * 100,
        "drawdown_pct": (peak_equity - final_equity) / peak_equity * 100,
        "max_drawdown_pct": max(
            float(state.get("max_drawdown_pct", 0.0)),
            float(result.get("max_drawdown_pct", 0.0)),
        ),
        "positions": positions,
        "equity_peak": peak_equity,
        "risk_status": checkpoint["risk"]["status"],
        "last_bar": checkpoint["runtime"].get("last_timestamp"),
        "last_error": None,
        "execution_mode": str(
            result.get("execution_mode")
            or result_metadata.get("execution_mode")
        ),
        "scheduled_times": list(
            result.get("scheduled_times")
            or result_metadata.get("scheduled_times")
            or ()
        ),
        "continuation": {
            "job_id": job_id,
            "source_end": checkpoint["runtime"].get("last_timestamp"),
            "initialized_at": now_iso(),
        },
    })
    saved = store.save(state)
    store.replace_equity_curve(account_id, equity_rows)
    return saved
