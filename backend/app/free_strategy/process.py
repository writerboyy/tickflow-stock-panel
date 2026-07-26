"""自由策略历史回测/模拟盘的受管子进程入口。"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import queue
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

from .bars import Bar, group_bars
from .engine import FreeStrategyConfig, FreeStrategyEngine

logger = logging.getLogger(__name__)


def _read_rows(
    repo: Any,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    timeframe: str,
    *,
    require_all_symbols: bool = True,
    allow_empty: bool = False,
) -> Iterable[Bar]:
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
    if not frame.is_empty():
        frame = frame.drop_nulls(["open", "high", "low", "close"])
    if frame.is_empty():
        if allow_empty:
            return []
        asset_label = "ETF" if asset_type == "etf" else "股票"
        raise ValueError(
            f"没有可用的{asset_label}分钟K历史数据。请先同步{asset_label}分钟K，"
            "或将周期切换为 1d 后重新运行。"
        )
    found = set(frame["symbol"].unique().to_list())
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing and require_all_symbols:
        raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")

    def minute_rows() -> Iterable[Bar]:
        for row in frame.iter_rows(named=True):
            yield Bar(
                symbol=str(row["symbol"]), timestamp=row["datetime"],
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]),
                close=float(row["close"]), volume=float(row.get("volume") or 0),
                amount=float(row.get("amount") or 0),
            )

    if timeframe == "1m":
        return minute_rows()
    return group_bars(minute_rows(), timeframe)


def execute_backtest(payload: dict[str, Any], output: Any) -> None:
    try:
        from app.tickflow.repository import DataStore, KlineRepository
        output.put({"type": "progress", "message": "读取行情数据", "progress": 0.1})
        repo = KlineRepository(DataStore(Path(payload["data_dir"])))
        start, end = date.fromisoformat(payload["start"]), date.fromisoformat(payload["end"])
        config = FreeStrategyConfig(**payload["config"])
        engine = FreeStrategyEngine(payload["source"], payload["timeframe"], config)
        if payload.get("checkpoint"):
            engine.restore_checkpoint(payload["checkpoint"])
        if payload["timeframe"] == "1d":
            bars = _read_rows(repo, payload["symbols"], start, end, payload["asset_type"], payload["timeframe"])
            output.put({"type": "progress", "message": f"回放 {len(bars)} 根日K", "progress": 0.35})
            result = engine.run(bars)
        else:
            output.put({"type": "progress", "message": "按交易日读取并回放分钟K", "progress": 0.35})
            cursor = start
            days_seen = 0
            days_with_bars = 0
            symbols_seen: set[str] = set()
            while cursor <= end:
                if cursor.weekday() < 5:
                    bars = _read_rows(
                        repo, payload["symbols"], cursor, cursor,
                        payload["asset_type"], payload["timeframe"], require_all_symbols=False, allow_empty=True,
                    )
                    rows = list(bars)
                    if rows:
                        symbols_seen.update(bar.symbol for bar in rows)
                        engine.run(rows, return_result=False)
                        days_with_bars += 1
                days_seen += 1
                if days_seen % 20 == 0:
                    progress = min(0.9, 0.35 + 0.55 * days_seen / max((end - start).days + 1, 1))
                    output.put({"type": "progress", "message": f"已回放 {days_with_bars} 个交易日", "progress": progress})
                cursor += timedelta(days=1)
            if not days_with_bars:
                raise ValueError("没有可用的分钟K历史数据，请先同步后重试")
            missing = [symbol for symbol in payload["symbols"] if symbol not in symbols_seen]
            if missing:
                raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
            engine.state = engine.context.state.copy()
            result = engine.result()
        result["metadata"] = {
            "timeframe": payload["timeframe"], "asset_type": payload["asset_type"],
            "source_revision": payload.get("source_revision"), "nav_filter": "skipped_no_data",
            "resumed_from_checkpoint": bool(payload.get("checkpoint")),
        }
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
