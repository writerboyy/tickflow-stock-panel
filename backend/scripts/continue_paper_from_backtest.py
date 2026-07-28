#!/usr/bin/env python3
"""Continue a stopped paper account from a compatible backtest checkpoint."""
from __future__ import annotations

import argparse
from pathlib import Path

from app.free_strategy.continuation import continue_account_from_backtest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("account_id")
    parser.add_argument("job_id")
    parser.add_argument("--data-dir", type=Path, default=Path("../data"))
    args = parser.parse_args()
    state = continue_account_from_backtest(args.data_dir, args.account_id, args.job_id)
    print(
        f"continued {state['id']} from {args.job_id}: "
        f"equity={state['equity']:.2f}, positions={state['positions']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
