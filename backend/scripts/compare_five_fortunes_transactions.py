#!/usr/bin/env python3
"""Compare a TickFlow Five Fortunes result with the exported JoinQuant fills."""
from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any


_SYMBOL_PATTERN = re.compile(r"\((\d+\.X(?:SHG|SHE|BSE))\)")


@dataclass(frozen=True)
class ExpectedFill:
    timestamp: str
    symbol: str
    side: str
    quantity: int
    price: Decimal
    value: Decimal
    fee: Decimal
    status: str


def _decimal_places(value: Decimal) -> int:
    return max(0, -value.as_tuple().exponent)


def _displayed(actual: Any, expected: Decimal) -> Decimal:
    quantum = Decimal(1).scaleb(-_decimal_places(expected))
    return Decimal(str(actual)).quantize(quantum, rounding=ROUND_HALF_EVEN)


def _read_reference_text(path: Path) -> str:
    if path.suffix.lower() == ".csv":
        return path.read_text(encoding="gb18030")
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"成交压缩包必须且只能包含一个 CSV，实际为 {len(names)} 个")
        return archive.read(names[0]).decode("gb18030")


def read_expected(path: Path) -> list[ExpectedFill]:
    text = _read_reference_text(path)
    result = []
    for row in csv.DictReader(io.StringIO(text)):
        match = _SYMBOL_PATTERN.search(row["标的"])
        if match is None:
            raise ValueError(f"无法从标的列解析代码: {row['标的']}")
        signed_quantity = int(row["成交数量"].replace("股", "").replace(",", ""))
        side = "buy" if row["交易类型"] == "买" else "sell"
        result.append(ExpectedFill(
            timestamp=f"{row['日期']}T{row['委托时间']}",
            symbol=match.group(1),
            side=side,
            quantity=abs(signed_quantity),
            price=Decimal(row["成交价"]),
            value=abs(Decimal(row["成交额"])),
            fee=Decimal(row["手续费"]),
            status=row["状态"],
        ))
    return result


def compare(result_path: Path, reference_path: Path) -> list[str]:
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    actual = list(payload.get("fills") or [])
    expected = read_expected(reference_path)
    differences = []
    if len(actual) != len(expected):
        differences.append(f"成交笔数: TickFlow={len(actual)}, 聚宽={len(expected)}")
    for index, (left, right) in enumerate(zip(actual, expected, strict=False), start=1):
        fields = {
            "time": (left.get("timestamp"), right.timestamp),
            "symbol": (left.get("symbol", "").replace(".SH", ".XSHG").replace(".SZ", ".XSHE"), right.symbol),
            "side": (left.get("side"), right.side),
            "quantity": (int(left.get("quantity", 0)), right.quantity),
            "price": (_displayed(left.get("price"), right.price), right.price),
            "value": (_displayed(left.get("value"), right.value), right.value),
            "fee": (_displayed(left.get("fee"), right.fee), right.fee),
            "status": ("全部成交", right.status),
        }
        for field, (actual_value, expected_value) in fields.items():
            if actual_value != expected_value:
                differences.append(
                    f"第 {index} 笔 {field}: TickFlow={actual_value}, 聚宽={expected_value}"
                )
    return differences


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path, help="TickFlow result.json")
    parser.add_argument("reference", type=Path, help="聚宽 transaction.zip 或 transaction.csv")
    parser.add_argument("--max-differences", type=int, default=30)
    args = parser.parse_args()
    differences = compare(args.result, args.reference)
    if differences:
        print(f"FAIL: {len(differences)} differences")
        for item in differences[:args.max_differences]:
            print(item)
        if len(differences) > args.max_differences:
            print(f"... 另有 {len(differences) - args.max_differences} 项差异")
        return 1
    print("PASS: all transactions match")
    return 0


if __name__ == "__main__":
    sys.exit(main())
