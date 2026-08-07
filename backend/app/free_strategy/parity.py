"""Strict sequence-preserving TickFlow versus JoinQuant execution comparison."""
from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from pathlib import Path
from typing import Any


_SYMBOL_PATTERN = re.compile(r"\((\d+\.X(?:SHG|SHE|BSE))\)")
_FEE_COLUMNS = {
    "commission": ("佣金",),
    "stamp_tax": ("印花税",),
    "transfer_fee": ("过户费",),
    "dividend_tax": ("红利税", "股息红利税"),
}
_ACTUAL_FEE_FIELDS = (*_FEE_COLUMNS, "total_fee")
_STATUS = {
    "全部成交": "filled",
    "已成交": "filled",
    "部分成交": "partial",
    "已拒绝": "rejected",
    "废单": "rejected",
    "已撤单": "cancelled",
    "已取消": "cancelled",
    "未成交": "pending",
    "跳过": "skipped",
}


@dataclass(frozen=True, slots=True)
class ReferenceExecution:
    submitted_at: str
    executed_at: str
    symbol: str
    side: str
    requested_quantity: Decimal
    executed_quantity: Decimal
    price: Decimal | None
    amount: Decimal
    commission: Decimal | None
    stamp_tax: Decimal | None
    transfer_fee: Decimal | None
    dividend_tax: Decimal | None
    total_fee: Decimal
    status: str
    reason: str | None


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_reference_text(path: Path) -> str:
    if path.suffix.lower() == ".csv":
        return path.read_text(encoding="gb18030")
    if path.suffix.lower() != ".zip":
        raise ValueError("聚宽参考文件必须是 GB18030 CSV 或 ZIP")
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"成交压缩包必须且只能包含一个 CSV，实际为 {len(names)} 个")
        return archive.read(names[0]).decode("gb18030")


def _value(row: dict[str, str], *names: str, required: bool = True) -> str | None:
    for name in names:
        if name in row and str(row[name]).strip() not in {"", "-"}:
            return str(row[name]).strip()
    if required:
        raise ValueError(f"聚宽参考缺少字段: {'/'.join(names)}")
    return None


def _decimal(value: str | None, *, absolute: bool = False) -> Decimal | None:
    if value is None:
        return None
    normalized = value.replace(",", "").replace("股", "").replace("元", "").strip()
    try:
        result = Decimal(normalized)
    except InvalidOperation as exc:
        raise ValueError(f"无法解析数值: {value}") from exc
    return abs(result) if absolute else result


def _timestamp(value: str) -> str:
    return datetime.fromisoformat(value.replace(" ", "T")).isoformat()


def _actual_timestamp(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return _timestamp(str(value))


def _reference_timestamp(row: dict[str, str], *, executed: bool) -> str:
    if executed:
        last = _value(row, "最后更新时间", "成交时间", required=False)
        if last is not None:
            return _timestamp(last)
    day = _value(row, "日期")
    clock = _value(row, "委托时间")
    return _timestamp(f"{day}T{clock}")


def read_joinquant_executions(path: Path) -> tuple[list[ReferenceExecution], bool]:
    rows = list(csv.DictReader(io.StringIO(_read_reference_text(Path(path)))))
    if not rows:
        return [], False
    headers = set(rows[0])
    component_columns = {
        field: next((name for name in aliases if name in headers), None)
        for field, aliases in _FEE_COLUMNS.items()
    }
    components_complete = all(component_columns.values())
    result = []
    for row in rows:
        raw_symbol = _value(row, "标的", "证券")
        match = _SYMBOL_PATTERN.search(raw_symbol)
        if match is None:
            raise ValueError(f"无法从标的列解析代码: {raw_symbol}")
        trade_type = _value(row, "交易类型", "方向")
        side = {"买": "buy", "买入": "buy", "卖": "sell", "卖出": "sell"}.get(trade_type)
        if side is None:
            raise ValueError(f"无法解析交易方向: {trade_type}")
        status_value = _value(row, "状态")
        status = _STATUS.get(status_value, status_value.lower())
        components = {
            field: _decimal(_value(row, column, required=False), absolute=True) if column else None
            for field, column in component_columns.items()
        }
        result.append(ReferenceExecution(
            submitted_at=_reference_timestamp(row, executed=False),
            executed_at=_reference_timestamp(row, executed=True),
            symbol=match.group(1),
            side=side,
            requested_quantity=_decimal(
                _value(row, "委托数量", "下单数量", "成交数量"), absolute=True,
            ) or Decimal(0),
            executed_quantity=_decimal(_value(row, "成交数量"), absolute=True) or Decimal(0),
            price=_decimal(_value(row, "成交价", required=False), absolute=True),
            amount=_decimal(_value(row, "成交额"), absolute=True) or Decimal(0),
            commission=components["commission"],
            stamp_tax=components["stamp_tax"],
            transfer_fee=components["transfer_fee"],
            dividend_tax=components["dividend_tax"],
            total_fee=_decimal(_value(row, "总费用", "手续费"), absolute=True) or Decimal(0),
            status=status,
            reason=_value(row, "原因", "拒绝原因", required=False),
        ))
    return result, components_complete


def _displayed(actual: Any, expected: Decimal | None) -> Any:
    if expected is None:
        return actual
    if actual is None:
        return None
    quantum = Decimal(1).scaleb(expected.as_tuple().exponent)
    return Decimal(str(actual)).quantize(quantum, rounding=ROUND_HALF_UP)


def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").upper()
    return symbol.replace(".SH", ".XSHG").replace(".SZ", ".XSHE").replace(".BJ", ".XBSE")


def _execution_fields(actual: dict[str, Any], expected: ReferenceExecution) -> dict[str, tuple[Any, Any]]:
    fields: dict[str, tuple[Any, Any]] = {
        "submitted_at": (_actual_timestamp(actual.get("submitted_at")), expected.submitted_at),
        "executed_at": (_actual_timestamp(actual.get("executed_at")), expected.executed_at),
        "symbol": (_normalize_symbol(actual.get("symbol")), expected.symbol),
        "side": (str(actual.get("side")), expected.side),
        "requested_quantity": (
            _displayed(actual.get("requested_quantity"), expected.requested_quantity),
            expected.requested_quantity,
        ),
        "executed_quantity": (
            _displayed(actual.get("executed_quantity"), expected.executed_quantity),
            expected.executed_quantity,
        ),
        "price": (_displayed(actual.get("price"), expected.price), expected.price),
        "amount": (_displayed(actual.get("amount"), expected.amount), expected.amount),
        "total_fee": (_displayed(actual.get("total_fee"), expected.total_fee), expected.total_fee),
        "status": (str(actual.get("status")), expected.status),
    }
    for field in _FEE_COLUMNS:
        expected_value = getattr(expected, field)
        if expected_value is not None:
            fields[field] = (_displayed(actual.get(field), expected_value), expected_value)
    if expected.reason is not None:
        fields["reason"] = (str(actual.get("reason") or ""), expected.reason)
    return fields


def _run_locks(result_path: Path, payload: dict[str, Any], reference_path: Path) -> dict[str, Any]:
    metadata = payload.get("metadata") or {}
    readiness = metadata.get("readiness") or {}
    manifest_path = result_path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    parameters = (manifest.get("payload") or {}) if isinstance(manifest, dict) else {}
    parameters = {
        key: value
        for key, value in parameters.items()
        if key not in {"data_dir", "run_dir", "checkpoint", "strategy_source_sha256"}
    }
    parameters_sha = (
        sha256(json.dumps(parameters, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode()).hexdigest()
        if parameters else None
    )
    strategy_path = result_path.parent / "strategy.py"
    source_sha = metadata.get("strategy_source_sha256")
    source_file_sha = _file_sha256(strategy_path) if strategy_path.exists() else None
    return {
        "strategy_source_sha256": source_sha,
        "strategy_file_sha256": source_file_sha,
        "strategy_hash_matches_file": bool(source_sha and source_sha == source_file_sha),
        "parameters_sha256": parameters_sha,
        "tickflow_data_manifest_sha256": readiness.get("tickflow_data_manifest_sha256"),
        "trading_calendar_sha256": readiness.get("trading_calendar_sha256"),
        "reference_sha256": _file_sha256(reference_path),
    }


def compare_executions(
    result_path: Path,
    reference_path: Path,
    *,
    diagnose_alignment: bool = False,
) -> dict[str, Any]:
    result_path = Path(result_path)
    reference_path = Path(reference_path)
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    if "executions" not in payload:
        raise ValueError("TickFlow 结果缺少 executions 审计账本")
    actual = list(payload["executions"])
    expected, reference_components_complete = read_joinquant_executions(reference_path)
    differences: list[dict[str, Any]] = []
    if len(actual) != len(expected):
        differences.append({
            "index": None,
            "field": "count",
            "tickflow": len(actual),
            "joinquant": len(expected),
        })
    for index, (left, right) in enumerate(zip(actual, expected, strict=False), start=1):
        for field, (actual_value, expected_value) in _execution_fields(left, right).items():
            if actual_value != expected_value:
                differences.append({
                    "index": index,
                    "field": field,
                    "tickflow": str(actual_value),
                    "joinquant": str(expected_value),
                })
    locks = _run_locks(result_path, payload, reference_path)
    required_locks = (
        "strategy_source_sha256",
        "parameters_sha256",
        "tickflow_data_manifest_sha256",
        "trading_calendar_sha256",
        "reference_sha256",
    )
    locks_complete = all(locks.get(key) for key in required_locks) and bool(
        locks["strategy_hash_matches_file"]
    )
    actual_components_complete = all(
        all(field in item and item[field] is not None for field in _ACTUAL_FEE_FIELDS)
        for item in actual
    )
    if differences:
        status = "failed"
    elif not actual_components_complete or not locks_complete:
        status = "reproduction_evidence_incomplete"
    elif not reference_components_complete:
        status = "fee_component_evidence_insufficient"
    else:
        status = "passed"
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": status,
        "sequence_policy": "original_order_only",
        "tickflow_count": len(actual),
        "joinquant_count": len(expected),
        "differences": differences,
        "fee_evidence": {
            "tickflow_components_complete": actual_components_complete,
            "joinquant_components_complete": reference_components_complete,
        },
        "locks": locks,
        "locks_complete": locks_complete,
    }
    if diagnose_alignment:
        actual_keys = [
            (_normalize_symbol(item.get("symbol")), str(item.get("side")), str(item.get("executed_quantity")))
            for item in actual
        ]
        expected_keys = [
            (item.symbol, item.side, str(item.executed_quantity)) for item in expected
        ]
        report["diagnostic_alignment"] = {
            "matching_multiset_rows": sum(
                min(actual_keys.count(key), expected_keys.count(key)) for key in set(actual_keys)
            ),
            "can_change_status": False,
        }
    return report
