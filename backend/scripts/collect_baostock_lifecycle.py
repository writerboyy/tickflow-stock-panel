"""Collect BaoStock stock lifecycle events for recent full-A research windows."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from app.config import settings
from app.services import pit_reference


def _date_arg(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=settings.data_dir)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--start-date", type=_date_arg)
    parser.add_argument("--end-date", type=_date_arg, default=date.today())
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.years <= 0:
        parser.error("--years must be positive")
    result = pit_reference.sync_baostock_lifecycle(
        args.data_dir,
        years=args.years,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    errors = ",".join(result.get("errors") or [])
    print(
        f"status={result['status']} source={result.get('source')} "
        f"start_date={result.get('start_date')} end_date={result.get('end_date')} "
        f"published_rows={result.get('published_rows', 0)} "
        f"instrument_rows={result.get('instrument_rows', 0)} "
        f"instrument_appended_symbols={result.get('instrument_appended_symbols', 0)} "
        f"errors={errors}"
    )
    return 0 if result["status"] == "published" else 1


if __name__ == "__main__":
    raise SystemExit(main())
