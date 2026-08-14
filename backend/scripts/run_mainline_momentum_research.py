#!/usr/bin/env python3
"""Run the four mainline momentum models sequentially and persist normal runs."""
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


MODELS = (
    "mainline_momentum_breakout",
    "mainline_momentum_pullback",
    "mainline_momentum_resonance",
    "mainline_momentum_combined",
)


def _latest_benchmark_day(repo: KlineRepository) -> date:
    frame = repo.get_daily_asset(
        "index", "000905.SH", date(2021, 7, 30), date.today(), ["date"],
    )
    if frame.is_empty():
        raise RuntimeError("中证 500 日线为空，无法确定最新完整交易日")
    return frame["date"].max()


def _payload(
    template_id: str,
    source_revision: int,
    data_dir: Path,
    start: date,
    end: date,
    run_dir: Path,
) -> dict:
    template = TEMPLATES[template_id]
    source = str(template["source"])
    digest = sha256(source.encode("utf-8")).hexdigest()
    config = dict(template["config"])
    config.pop("timeframe", None)
    config["callback_timeout_seconds"] = 120
    return {
        "data_dir": str(data_dir),
        "source": source,
        "strategy_id": template_id,
        "strategy_name": template["name"],
        "source_revision": source_revision,
        "strategy_source_sha256": digest,
        "symbols": [],
        "timeframe": "1m",
        "asset_type": "stock",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "config": config,
        "data_provider": "tickflow",
        "run_dir": str(run_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="顺序运行四个主线动量分钟模型")
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2021, 7, 30))
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()

    data_dir = args.data_dir.resolve()
    repo = KlineRepository(DataStore(data_dir))
    end = args.end or _latest_benchmark_day(repo)
    if args.start > end:
        parser.error("--start 不能晚于 --end")
    run_root = data_dir / "free_strategy_runs"
    run_root.mkdir(parents=True, exist_ok=True)
    strategy_store = FreeStrategyStore(data_dir)
    strategy_revisions: dict[str, int] = {}

    for template_id in MODELS:
        template = TEMPLATES[template_id]
        try:
            saved = strategy_store.get(template_id)
        except FileNotFoundError:
            saved = None
        if saved is None or saved.get("source") != template["source"]:
            saved = strategy_store.save(
                template_id, template["name"], template["source"], template["config"],
            )
        strategy_revisions[template_id] = int(saved["revision"])

    for template_id in MODELS:
        run_id = f"mainline-{template_id.rsplit('_', 1)[-1]}-{uuid.uuid4().hex[:8]}"
        run_dir = run_root / run_id
        run_dir.mkdir(parents=True, exist_ok=False)
        payload = _payload(
            template_id, strategy_revisions[template_id], data_dir, args.start, end, run_dir,
        )
        (run_dir / "strategy.py").write_text(payload["source"], encoding="utf-8")
        (run_dir / "manifest.json").write_text(json.dumps({
            "job_id": run_id,
            "strategy_id": template_id,
            "source_revision": strategy_revisions[template_id],
            "strategy_source_sha256": payload["strategy_source_sha256"],
            "payload": {key: value for key, value in payload.items() if key != "source"},
            "research_group": "mainline_momentum_four_models",
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        output: queue.Queue = queue.Queue()
        print(f"[{template_id}] {args.start} ~ {end} 开始", flush=True)
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
            print(f"[{template_id}] 失败: {error}", file=sys.stderr)
            return 1
        print(f"[{template_id}] 完成: {run_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
