"""Compare TickFlow and QMT realtime quote arrival on one observer."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings  # noqa: E402
from app.services.qmt_trading import QmtZmqRpcClient  # noqa: E402
from app.services.tick_latency_probe import (  # noqa: E402
    QmtWholeQuoteSource,
    TickFlowStreamSource,
    run_latency_probe,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="只读对比 TickFlow 与 QMT 实时行情")
    parser.add_argument("--symbols", required=True, help="逗号分隔的股票代码")
    parser.add_argument("--duration", type=float, default=1800, help="采集秒数")
    parser.add_argument("--clocks-synchronized", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_dir or settings.data_dir / "user_data" / "tick_latency" / stamp
    client = QmtZmqRpcClient(settings)
    report = run_latency_probe(
        {
            "qmt": QmtWholeQuoteSource(
                client,
                symbols,
                quote_address=settings.qmt_quote_zmq_connect_address,
            ),
            "tickflow": TickFlowStreamSource(symbols),
        },
        args.duration,
        output_dir,
        clocks_synchronized=args.clocks_synchronized,
    )
    print(json.dumps({"output_dir": str(output_dir), **report}, ensure_ascii=False, indent=2))
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
