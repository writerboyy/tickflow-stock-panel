#!/usr/bin/env python3
"""Run the temporary, resumable Tushare Proxy historical backfill.

The API key is accepted only from stdin (``--key-stdin``) or the existing
0600 secrets file.  It is never a command-line option and is not written to a
manifest or log.
"""

from __future__ import annotations

import argparse
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="临时 Tushare Proxy 全历史回填（仅归档/补缺口，不改变日常数据源）")
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--preflight", action="store_true", help="只探测接口权限和响应协议")
    parser.add_argument("--key-stdin", action="store_true", help="从 stdin 读取一行 API key 并保存为 0600")
    parser.add_argument("--run-id", help="可恢复任务 ID；不传则创建新的 ID")
    parser.add_argument("--resume", action="store_true", help="恢复 --run-id 对应的 manifest")
    parser.add_argument("--phases", type=_phase_list, default=PHASES, help=f"逗号分隔阶段：{','.join(PHASES)}")
    parser.add_argument("--publish", action="store_true", help="发布已审计的分钟缺口；默认仅归档")
    parser.add_argument("--status", action="store_true", help="读取指定 run-id 的 manifest，不访问网络")
    parser.add_argument("--clear-key", action="store_true", help="清除本地 Tushare key")
    parser.add_argument("--symbols", action="append", help="限制股票/ETF代码，逗号分隔；仅用于小样本或恢复")
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--rate-interval", type=float, default=0.2)
    parser.add_argument("--attempts", type=int, default=4)
    args = parser.parse_args(argv)
    data_dir = Path(args.data_dir).expanduser().resolve()

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
        run = TushareHistoryBackfill(BackfillConfig(data_dir=data_dir, run_id=args.run_id, phases=tuple(args.phases)), client)
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
        phases=tuple(args.phases),
        symbols=_symbols(args.symbols),
        max_symbols=args.max_symbols,
        rate_interval=args.rate_interval,
        attempts=args.attempts,
        publish=args.publish,
    )
    try:
        result = TushareHistoryBackfill(config, client).run()
    except (BackfillBlocked, ValueError) as exc:
        parser.exit(1, f"Tushare history backfill blocked: {exc}\n")
    _print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
