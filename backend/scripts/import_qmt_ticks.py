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


def main() -> int:
    parser = argparse.ArgumentParser(description="按股票和交易日导入 QMT Tick 历史")
    parser.add_argument("--symbols", required=True, help="逗号分隔，例如 000001.SZ,600000.SH")
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    args = parser.parse_args()
    provider = get_provider("qmt")
    result = import_qmt_ticks(
        provider,
        args.data_dir,
        args.symbols.split(","),
        args.start,
        args.end,
        on_progress=lambda symbol, day, rows: print(
            f"{day.isoformat()} {symbol}: {rows} rows",
            flush=True,
        ),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
