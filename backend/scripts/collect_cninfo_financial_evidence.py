#!/usr/bin/env python3
"""Collect Cninfo announcement evidence for unresolved financial conflicts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.services.financial_cninfo_evidence import collect_cninfo_financial_conflict_evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="用 AxData 巨潮接口采集财务冲突官方公告证据")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--axdata-root", type=Path)
    parser.add_argument("--window-days", type=int, default=3)
    parser.add_argument("--download-pdfs", action="store_true")
    parser.add_argument("--max-pdf-downloads", type=int, default=20)
    args = parser.parse_args()
    result = collect_cninfo_financial_conflict_evidence(
        args.data_dir.resolve(),
        output=args.output.resolve(),
        axdata_root=args.axdata_root.resolve() if args.axdata_root else None,
        window_days=args.window_days,
        download_pdfs=args.download_pdfs,
        max_pdf_downloads=args.max_pdf_downloads,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
