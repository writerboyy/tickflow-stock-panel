"""Import QMT Tick history over the configured read-only ZMQ RPC."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings  # noqa: E402
from app.data_providers import get_provider  # noqa: E402
from app.services.qmt_tick_import import import_qmt_ticks  # noqa: E402
from app.tickflow.repository import DataStore, KlineRepository  # noqa: E402


def _local_trading_dates(data_dir: Path, start: date, end: date) -> list[date]:
    repo = KlineRepository(DataStore(data_dir))
    frame = repo.get_daily_asset("index", "000001.SH", start, end, ["date"])
    if frame.is_empty() or "date" not in frame.columns:
        raise ValueError(
            f"{start.isoformat()} 至 {end.isoformat()} 缺少本地上证指数交易日历",
        )
    return sorted(set(frame["date"].to_list()))


def main() -> int:
    parser = argparse.ArgumentParser(description="按股票和交易日导入 QMT Tick 历史")
    parser.add_argument("--symbols", required=True, help="逗号分隔，例如 000001.SZ,600000.SH")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--rpc-timeout", type=float, default=120.0)
    args = parser.parse_args()
    if args.rpc_timeout <= 0:
        parser.error("--rpc-timeout 必须大于 0")
    provider = get_provider("qmt")
    client = getattr(provider, "client", None)
    if client is not None and hasattr(client, "timeout"):
        client.timeout = args.rpc_timeout
    trading_dates = _local_trading_dates(args.data_dir, args.start, args.end)
    result = import_qmt_ticks(
        provider,
        args.data_dir,
        args.symbols.split(","),
        args.start,
        args.end,
        trading_dates=trading_dates,
        on_progress=lambda symbol, day, rows: print(
            f"{day.isoformat()} {symbol}: {rows} rows",
            flush=True,
        ),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
