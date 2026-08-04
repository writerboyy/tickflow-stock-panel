#!/usr/bin/env python3
"""Run the audited, resumable Tushare Proxy historical ingestion.

The API key is accepted only from stdin (``--key-stdin``) or the existing
0600 secrets file.  It is never a command-line option and is not written to a
manifest or log.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
from pathlib import Path
import sys

from app.config import settings
from app.services.tushare_history import (
    PHASES,
    BackfillBlocked,
    BackfillConfig,
    GlobalRateLimiter,
    TushareHistoryBackfill,
    TushareProxyClient,
    clear_tushare_key,
    load_tushare_key,
    save_tushare_key_from_stdin,
    _safe_part,
)


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _phase_list(value: str) -> tuple[str, ...]:
    phases = tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))
    unknown = sorted(set(phases) - set(PHASES))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown phase(s): {', '.join(unknown)}")
    return phases


def _symbols(values: list[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    return tuple(item.strip().upper() for value in values for item in value.split(",") if item.strip())


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc


def _datasets(value: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tushare Proxy 可审计历史落盘（TickFlow 优先、仅补缺口）")
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--preflight", action="store_true", help="只探测接口权限和响应协议")
    parser.add_argument("--key-stdin", action="store_true", help="从 stdin 读取一行 API key 并保存为 0600")
    parser.add_argument("--run-id", help="可恢复任务 ID；不传则创建新的 ID")
    parser.add_argument("--resume", action="store_true", help="恢复 --run-id 对应的 manifest")
    parser.add_argument("--phases", type=_phase_list, help=f"逗号分隔阶段：{','.join(PHASES)}")
    parser.add_argument("--datasets", type=_datasets, default=(), help="数据集或分组：reference,daily,financials,factors")
    parser.add_argument("--start", type=_date, default=date(2010, 1, 1), help="历史起点 YYYY-MM-DD")
    parser.add_argument("--end", type=_date, default=date.today(), help="历史终点 YYYY-MM-DD")
    parser.add_argument("--incremental", action="store_true", help="仅同步最近 10 个自然日并捕获公告修订")
    parser.add_argument("--publish", action="store_true", help="原子发布已审计缺口；默认仅归档和 staging")
    parser.add_argument("--status", action="store_true", help="读取指定 run-id 的 manifest，不访问网络")
    parser.add_argument("--clear-key", action="store_true", help="清除本地 Tushare key")
    parser.add_argument("--symbols", action="append", help="限制股票代码，逗号分隔；仅用于小样本或恢复")
    parser.add_argument("--etfs", action="append", help="限制 ETF 代码，逗号分隔；用于小样本")
    parser.add_argument("--indexes", action="append", help="限制指数代码，逗号分隔；用于小样本")
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--rate-interval", type=float, default=0.2)
    parser.add_argument("--attempts", type=int, default=4)
    parser.add_argument("--workers", type=int, default=4, help="并发标的数，范围 1-64；请求频率仍受全局限流器控制")
    args = parser.parse_args(argv)
    data_dir = Path(args.data_dir).expanduser().resolve()
    phases = args.phases or (("universe",) if args.datasets else PHASES)
    start = max(args.start, date.today() - timedelta(days=10)) if args.incremental else args.start

    if args.clear_key:
        clear_tushare_key(data_dir=data_dir)
        _print({"status": "cleared"})
        return 0

    if args.status:
        if not args.run_id:
            parser.error("--status requires --run-id")
        try:
            run_id = _safe_part(args.run_id)
        except ValueError as exc:
            parser.error(str(exc))
        path = data_dir / "backfill_state" / "tushare_proxy" / run_id / "manifest.json"
        if not path.exists():
            parser.error(f"manifest not found: {path}")
        _print(json.loads(path.read_text(encoding="utf-8")))
        return 0

    if args.key_stdin:
        try:
            key = save_tushare_key_from_stdin(sys.stdin, data_dir=data_dir)
        except ValueError as exc:
            parser.error(str(exc))
    else:
        key = load_tushare_key(data_dir=data_dir)
    if not key:
        parser.error("missing Tushare key; use --key-stdin (never pass it as an argument)")

    client = TushareProxyClient(
        key,
        limiter=GlobalRateLimiter(args.rate_interval),
        attempts=args.attempts,
    )
    if args.preflight:
        run = TushareHistoryBackfill(
            BackfillConfig(
                data_dir=data_dir,
                run_id=args.run_id,
                phases=tuple(phases),
                start=start,
                end=args.end,
                datasets=tuple(args.datasets),
                symbols=_symbols(args.symbols),
                etfs=_symbols(args.etfs),
                indexes=_symbols(args.indexes),
                incremental=args.incremental,
            ),
            client,
        )
        try:
            _print(run.preflight())
        except (BackfillBlocked, ValueError) as exc:
            parser.exit(1, f"Tushare preflight blocked: {exc}\n")
        return 0

    if args.resume and not args.run_id:
        parser.error("--resume requires --run-id")
    config = BackfillConfig(
        data_dir=data_dir,
        run_id=args.run_id,
        phases=tuple(phases),
        symbols=_symbols(args.symbols),
        etfs=_symbols(args.etfs),
        indexes=_symbols(args.indexes),
        max_symbols=args.max_symbols,
        rate_interval=args.rate_interval,
        attempts=args.attempts,
        workers=args.workers,
        publish=args.publish,
        start=start,
        end=args.end,
        datasets=tuple(args.datasets),
        incremental=args.incremental,
    )
    try:
        result = TushareHistoryBackfill(config, client).run()
    except (BackfillBlocked, ValueError) as exc:
        parser.exit(1, f"Tushare history backfill blocked: {exc}\n")
    _print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
