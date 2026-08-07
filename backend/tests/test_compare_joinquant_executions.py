import csv
import json
import zipfile
from hashlib import sha256

from app.free_strategy.parity import compare_executions, read_joinquant_executions


COMPONENT_HEADERS = ["佣金", "印花税", "过户费", "红利税"]


def reference_row(symbol="000001.XSHE", side="买", quantity="100股"):
    return {
        "日期": "2024-01-02",
        "委托时间": "09:31:00",
        "标的": f"测试({symbol})",
        "交易类型": side,
        "成交数量": quantity,
        "成交价": "10.00",
        "成交额": "1000.00",
        "委托数量": quantity,
        "手续费": "3.70",
        "佣金": "1.00",
        "印花税": "2.00",
        "过户费": "0.30",
        "红利税": "0.40",
        "状态": "全部成交",
        "最后更新时间": "2024-01-02 09:31:00",
    }


def execution(symbol="000001.SZ", side="buy", quantity=100):
    return {
        "order_id": "o1",
        "submitted_at": "2024-01-02T09:31:00",
        "executed_at": "2024-01-02T09:31:00",
        "symbol": symbol,
        "side": side,
        "requested_quantity": quantity,
        "executed_quantity": quantity,
        "price": 10.0,
        "amount": 1000.0,
        "commission": 1.0,
        "stamp_tax": 2.0,
        "transfer_fee": 0.3,
        "dividend_tax": 0.4,
        "fee": 3.3,
        "total_fee": 3.7,
        "fee_components_complete": True,
        "status": "filled",
        "reason": "",
    }


def write_run(tmp_path, executions):
    run = tmp_path / "run"
    run.mkdir()
    source = "def on_bar(context, bars):\n    pass\n"
    source_sha = sha256(source.encode()).hexdigest()
    (run / "strategy.py").write_text(source, encoding="utf-8")
    (run / "manifest.json").write_text(json.dumps({
        "payload": {
            "start": "2024-01-02",
            "end": "2024-01-02",
            "timeframe": "1m",
            "asset_type": "stock",
            "config": {"fill_policy": "current_close"},
        },
    }), encoding="utf-8")
    result = run / "result.json"
    result.write_text(json.dumps({
        "executions": executions,
        "metadata": {
            "strategy_source_sha256": source_sha,
            "readiness": {
                "tickflow_data_manifest_sha256": "d" * 64,
                "trading_calendar_sha256": "c" * 64,
            },
        },
    }), encoding="utf-8")
    return result


def write_reference(tmp_path, rows, *, components=True):
    path = tmp_path / "reference.csv"
    headers = [
        "日期", "委托时间", "标的", "交易类型", "成交数量", "成交价",
        "成交额", "委托数量", "手续费", "状态", "最后更新时间",
    ]
    if components:
        headers[9:9] = COMPONENT_HEADERS
    with path.open("w", encoding="gb18030", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def test_strict_comparator_passes_only_with_complete_locks_and_fee_components(tmp_path):
    report = compare_executions(
        write_run(tmp_path, [execution()]),
        write_reference(tmp_path, [reference_row()]),
    )

    assert report["status"] == "passed"
    assert report["differences"] == []
    assert report["locks_complete"] is True


def test_total_fee_only_is_evidence_insufficient_not_pass(tmp_path):
    report = compare_executions(
        write_run(tmp_path, [execution()]),
        write_reference(tmp_path, [reference_row()], components=False),
    )

    assert report["differences"] == []
    assert report["status"] == "fee_component_evidence_insufficient"
    assert report["fee_evidence"]["joinquant_components_complete"] is False


def test_alignment_diagnostic_cannot_turn_reordered_rows_into_pass(tmp_path):
    actual = [
        execution("000001.SZ", "buy", 100),
        {**execution("600000.SH", "sell", 100), "order_id": "o2"},
    ]
    expected = [
        reference_row("600000.XSHG", "卖"),
        reference_row("000001.XSHE", "买"),
    ]

    report = compare_executions(
        write_run(tmp_path, actual),
        write_reference(tmp_path, expected),
        diagnose_alignment=True,
    )

    assert report["status"] == "failed"
    assert report["differences"]
    assert report["diagnostic_alignment"] == {
        "matching_multiset_rows": 2,
        "can_change_status": False,
    }


def test_joinquant_gb18030_zip_must_contain_exactly_one_csv(tmp_path):
    csv_path = write_reference(tmp_path, [reference_row()])
    archive_path = tmp_path / "reference.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("transactions.csv", csv_path.read_bytes())

    rows, complete = read_joinquant_executions(archive_path)

    assert len(rows) == 1
    assert complete is True
