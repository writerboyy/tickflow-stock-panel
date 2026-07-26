"""自由策略历史回测/模拟盘的受管子进程入口。"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import queue
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from .bars import Bar, group_bars
from .engine import FreeStrategyConfig, FreeStrategyEngine

logger = logging.getLogger(__name__)


def _read_rows(repo: Any, symbols: list[str], start: date, end: date, asset_type: str, timeframe: str) -> list[Bar]:
    if timeframe == "1d":
        rows: list[Bar] = []
        for symbol in symbols:
            frame = repo.get_daily_asset(asset_type, symbol, start, end, ["date", "open", "high", "low", "close", "volume", "amount"])
            for row in frame.iter_rows(named=True):
                rows.append(Bar(symbol=symbol, timestamp=datetime.combine(row["date"], time(15, 0)), open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]), volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0)))
        if not rows:
            raise ValueError("没有可用的日K数据，请先同步历史行情")
        return rows
    frame = repo.get_minute_range(symbols, start, end, asset_type)
    rows = [Bar(symbol=str(row["symbol"]), timestamp=row["datetime"], open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]), volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0)) for row in frame.iter_rows(named=True)]
    if not rows:
        asset_label = "ETF" if asset_type == "etf" else "股票"
        raise ValueError(
            f"没有可用的{asset_label}分钟K历史数据。请先同步{asset_label}分钟K，"
            "或将周期切换为 1d 后重新运行。"
        )
    return group_bars(rows, timeframe)


def execute_backtest(payload: dict[str, Any], output: Any) -> None:
    try:
        from app.tickflow.repository import DataStore, KlineRepository
        output.put({"type": "progress", "message": "读取行情数据", "progress": 0.1})
        repo = KlineRepository(DataStore(Path(payload["data_dir"])))
        start, end = date.fromisoformat(payload["start"]), date.fromisoformat(payload["end"])
        bars = _read_rows(repo, payload["symbols"], start, end, payload["asset_type"], payload["timeframe"])
        output.put({"type": "progress", "message": f"回放 {len(bars)} 根 bar", "progress": 0.35})
        config = FreeStrategyConfig(**payload["config"])
        engine = FreeStrategyEngine(payload["source"], payload["timeframe"], config)
        result = engine.run(bars)
        result["metadata"] = {"timeframe": payload["timeframe"], "asset_type": payload["asset_type"], "source_revision": payload.get("source_revision"), "nav_filter": "skipped_no_data"}
        if payload.get("run_dir"):
            Path(payload["run_dir"]).mkdir(parents=True, exist_ok=True)
            (Path(payload["run_dir"]) / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        output.put({"type": "result", "result": result})
    except BaseException as exc:  # noqa: BLE001 - worker must report all script errors
        logger.exception("free strategy backtest failed")
        output.put({"type": "error", "error": str(exc)})


def start_process(payload: dict[str, Any]) -> tuple[mp.Process, Any]:
    ctx = mp.get_context("spawn")
    output = ctx.Queue()
    process = ctx.Process(target=execute_backtest, args=(payload, output), daemon=True)
    process.start()
    return process, output
