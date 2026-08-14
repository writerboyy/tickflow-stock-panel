#!/usr/bin/env python3
"""Run and persist the large-turnover morning first-board minute research."""
from __future__ import annotations

import argparse
import json
import queue
import sys
import uuid
from datetime import date
from hashlib import sha256
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.config import settings  # noqa: E402
from app.free_strategy.process import execute_backtest  # noqa: E402
from app.free_strategy.store import FreeStrategyStore  # noqa: E402
from app.free_strategy.templates import TEMPLATES  # noqa: E402
from app.tickflow.repository import DataStore, KlineRepository  # noqa: E402


TEMPLATE_ID = "large_amount_first_board"


def _latest_benchmark_day(repo: KlineRepository) -> date:
    frame = repo.get_daily_asset(
        "index", "000905.SH", date(2021, 7, 30), date.today(), ["date"],
    )
    if frame.is_empty():
        raise RuntimeError("中证 500 日线为空，无法确定最新完整交易日")
    return frame["date"].max()


def main() -> int:
    parser = argparse.ArgumentParser(description="运行大成交首板上午打板分钟回测")
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2021, 7, 30))
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    repo = KlineRepository(DataStore(data_dir))
    end = args.end or _latest_benchmark_day(repo)
    if args.start > end:
        parser.error("--start 不能晚于 --end")
    template = TEMPLATES[TEMPLATE_ID]
    store = FreeStrategyStore(data_dir)
    try:
        saved = store.get(TEMPLATE_ID)
    except FileNotFoundError:
        saved = None
    if saved is None or saved.get("source") != template["source"]:
        saved = store.save(
            TEMPLATE_ID, template["name"], template["source"], template["config"],
        )

    source = str(template["source"])
    digest = sha256(source.encode("utf-8")).hexdigest()
    run_id = f"large-first-board-{uuid.uuid4().hex[:8]}"
    run_dir = data_dir / "free_strategy_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    config = dict(template["config"])
    config.pop("timeframe", None)
    config.pop("asset_type", None)
    config["callback_timeout_seconds"] = 120
    payload = {
        "data_dir": str(data_dir),
        "source": source,
        "strategy_id": TEMPLATE_ID,
        "strategy_name": template["name"],
        "source_revision": int(saved["revision"]),
        "strategy_source_sha256": digest,
        "symbols": [],
        "timeframe": "1m",
        "asset_type": "stock",
        "start": args.start.isoformat(),
        "end": end.isoformat(),
        "config": config,
        "data_provider": "tickflow",
        "run_dir": str(run_dir),
    }
    (run_dir / "strategy.py").write_text(source, encoding="utf-8")
    (run_dir / "manifest.json").write_text(json.dumps({
        "job_id": run_id,
        "strategy_id": TEMPLATE_ID,
        "source_revision": int(saved["revision"]),
        "strategy_source_sha256": digest,
        "payload": {key: value for key, value in payload.items() if key != "source"},
        "research_group": "large_amount_first_board",
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    output: queue.Queue = queue.Queue()
    print(f"[{TEMPLATE_ID}] {args.start} ~ {end} 开始", flush=True)
    execute_backtest(payload, output)
    final = None
    while not output.empty():
        event = output.get_nowait()
        if event.get("type") == "progress":
            print(f"  {event.get('message')}", flush=True)
        if event.get("type") in {"result", "error"}:
            final = event
    if final is None or final.get("type") != "result":
        error = final.get("error") if final else "未收到回测结果"
        print(f"[{TEMPLATE_ID}] 失败: {error}", file=sys.stderr)
        return 1
    result = final["result"]
    print(
        f"[{TEMPLATE_ID}] 完成: {run_id}，收益 {float(result['return_pct']):.2f}%，"
        f"最大回撤 {float(result['max_drawdown_pct']):.2f}%",
        flush=True,
    )
    for name in ("training", "out_of_sample"):
        segment = (result.get("research_performance") or {}).get(name)
        if segment:
            print(
                f"  {name}: 收益 {segment['return_pct']:.2f}%，"
                f"超额 {segment['excess_return_pct']:.2f}%，买入 {segment['entry_count']} 笔",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
