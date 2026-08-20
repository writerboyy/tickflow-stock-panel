from pathlib import Path

from app.services.four_mode_parser import parse_four_mode_strategy


def test_four_mode_parser_reads_source_without_executing_it():
    report = parse_four_mode_strategy()

    assert report["state"] == "available"
    assert [mode["name"] for mode in report["modes"]] == ["一进二", "弱转强", "趋势股", "首板"]
    assert report["modes"][1]["config"] == [{"key": "priority_config", "value": ["yje", "sb", "rzq"]}]
    assert {item["key"] for item in report["modes"][2]["config"]} >= {"qs_max_hold_days", "qs_atr_multiplier"}
    assert report["schedule"][0] == {
        "time": "09:05:00",
        "function": "prepare_stock_candidates",
        "description": "昨日数据完成四模式静态预选",
    }
    assert report["live_trading_enabled"] is False
    assert report["execution_state"] == "read_only"
    assert report["source"]["sha256"]
    assert next(item for item in report["dependencies"] if item["name"] == "talib")["available"] is False


def test_four_mode_parser_fails_closed_for_missing_source(tmp_path: Path):
    report = parse_four_mode_strategy(tmp_path / "missing.py")

    assert report["state"] == "unavailable"
    assert report["modes"] == []
    assert "不存在" in report["reason"]
