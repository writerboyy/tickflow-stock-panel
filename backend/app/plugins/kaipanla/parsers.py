"""八个开盘啦接口的严格响应解析。"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any


class ResponseShapeError(ValueError):
    pass


def _text(value: object, field: str, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ResponseShapeError(f"{field} 缺失")
        return None
    result = str(value).strip()
    if required and not result:
        raise ResponseShapeError(f"{field} 为空")
    return result or None


def _float(value: object, field: str) -> float | None:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, str):
            value = value.strip().removesuffix("%")
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ResponseShapeError(f"{field} 不是有效数值") from exc


def _int(value: object, field: str) -> int | None:
    number = _float(value, field)
    return int(number) if number is not None else None


def _rows(payload: dict, key: str) -> list:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ResponseShapeError(f"{key} 不是数组")
    return value


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_trade_date(value: object) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ResponseShapeError("交易日期格式无效")


def parse_auction(payload: dict) -> list[dict]:
    """解析 /115、/30 的 17 列数组，未文档化位置不进入标准表。"""
    result: list[dict] = []
    for index, row in enumerate(_rows(payload, "info")):
        if not isinstance(row, list) or len(row) != 17:
            raise ResponseShapeError(f"info[{index}] 必须恰好包含 17 列")
        code = _text(row[0], f"info[{index}].code", required=True)
        result.append(
            {
                "symbol": code,
                "code": code,
                "name": _text(row[1], f"info[{index}].name", required=True),
                "realtime_change_pct": _float(row[3], "realtime_change_pct"),
                "limit_buy_amount": _float(row[4], "limit_buy_amount"),
                "auction_change_pct": _float(row[5], "auction_change_pct"),
                "auction_net_amount": _float(row[6], "auction_net_amount"),
                "auction_turnover_pct": _float(row[7], "auction_turnover_pct"),
                "auction_amount": _float(row[8], "auction_amount"),
                "post_0920_buy_amount": _float(row[9], "post_0920_buy_amount"),
                "themes": _text(row[11], "themes"),
                "float_market_cap": _float(row[12], "float_market_cap"),
                "board_label": _text(row[16], "board_label"),
            }
        )
    return result


def parse_bid_detail(payload: dict) -> dict:
    code = _text(payload.get("code"), "code", required=True)
    bid = _rows(payload, "bid")
    points: list[dict[str, object]] = []
    for index, row in enumerate(bid):
        if not isinstance(row, list) or len(row) != 4:
            raise ResponseShapeError(f"bid[{index}] 必须恰好包含 4 列")
        points.append(
            {
                "time": _text(row[0], f"bid[{index}].time", required=True),
                "price": _float(row[1], f"bid[{index}].price"),
                "volume": _float(row[3], f"bid[{index}].volume"),
            }
        )
    first = points[0] if points else {}
    last = points[-1] if points else {}
    return {
        "symbol": code,
        "code": code,
        "bid_points_json": _json(points),
        "bid_points": len(points),
        "bid_first_time": first.get("time"),
        "bid_last_time": last.get("time"),
        "bid_last_price": last.get("price"),
        "bid_last_volume": last.get("volume"),
        "bid_preclose_price": _float(payload.get("preclose_px"), "preclose_px"),
        "bid_high_price": _float(payload.get("hprice"), "hprice"),
        "bid_low_price": _float(payload.get("lprice"), "lprice"),
        "bid_open_price": _float(payload.get("openpx"), "openpx"),
    }


def parse_limitup(payload: dict) -> list[dict]:
    nums = payload.get("nums") or {}
    if not isinstance(nums, dict):
        raise ResponseShapeError("nums 不是对象")
    common = {
        "market_advance_count": _int(nums.get("SZJS"), "nums.SZJS"),
        "market_decline_count": _int(nums.get("XDJS"), "nums.XDJS"),
        "market_limitup_count": _int(nums.get("ZT"), "nums.ZT"),
        "market_limitdown_count": _int(nums.get("DT"), "nums.DT"),
        "market_broken_rate_pct": _float(nums.get("ZBL"), "nums.ZBL"),
        "yesterday_limitup_change_pct": _float(nums.get("yestRase"), "nums.yestRase"),
    }
    merged: dict[str, dict] = {}
    for plate_index, plate in enumerate(_rows(payload, "list")):
        if not isinstance(plate, dict):
            raise ResponseShapeError(f"list[{plate_index}] 不是对象")
        plate_code = _text(plate.get("ZSCode"), "ZSCode")
        plate_name = _text(plate.get("ZSName"), "ZSName")
        stocks = plate.get("StockList")
        if not isinstance(stocks, list):
            raise ResponseShapeError(f"list[{plate_index}].StockList 不是数组")
        for stock_index, row in enumerate(stocks):
            if not isinstance(row, list) or len(row) < 18:
                raise ResponseShapeError(
                    f"list[{plate_index}].StockList[{stock_index}] 至少需要 18 列"
                )
            code = _text(row[0], "StockList.code", required=True)
            item = merged.setdefault(
                code,
                {
                    "symbol": code,
                    "code": code,
                    "name": _text(row[1], "StockList.name", required=True),
                    "plate_codes": [],
                    "plate_names": [],
                    "limitup_timestamp": _int(row[6], "limitup_timestamp"),
                    "sealed_order_amount": _float(row[8], "sealed_order_amount"),
                    "board_label": _text(row[9], "board_label"),
                    "consecutive_limitups": _int(row[10], "consecutive_limitups"),
                    "themes": _text(row[11], "themes"),
                    "turnover_pct": _float(row[14], "turnover_pct"),
                    "float_market_cap": _float(row[15], "float_market_cap"),
                    "reason": _text(row[16], "reason"),
                    "reason_detail": _text(row[17], "reason_detail"),
                    **common,
                },
            )
            if plate_code and plate_code not in item["plate_codes"]:
                item["plate_codes"].append(plate_code)
            if plate_name and plate_name not in item["plate_names"]:
                item["plate_names"].append(plate_name)
    return [
        {
            **item,
            "plate_codes": ";".join(item["plate_codes"]),
            "plate_names": ";".join(item["plate_names"]),
        }
        for item in merged.values()
    ]


def parse_lhb_list(payload: dict) -> tuple[date | None, list[dict]]:
    trade_date = parse_trade_date(payload.get("Time"))
    result: list[dict] = []
    for index, row in enumerate(_rows(payload, "list")):
        if not isinstance(row, dict):
            raise ResponseShapeError(f"list[{index}] 不是对象")
        code = _text(row.get("ID"), f"list[{index}].ID", required=True)
        result.append(
            {
                "symbol": code,
                "code": code,
                "name": _text(row.get("Name"), f"list[{index}].Name", required=True),
                "increase_pct": _float(row.get("IncreaseAmount"), "IncreaseAmount"),
                "buy_in_amount": _float(row.get("BuyIn"), "BuyIn"),
                "listings_count": _int(row.get("JoinNum"), "JoinNum"),
                "turnover": _float(row.get("Turnover"), "Turnover"),
                "circulating_market_cap": _float(row.get("CircPrice"), "CircPrice"),
                "amplitude_pct": _float(row.get("Amplitude"), "Amplitude"),
                "turnover_pct": _float(row.get("TurnoverRatio"), "TurnoverRatio"),
                "market_cap": _float(row.get("Capitalization"), "Capitalization"),
            }
        )
    return trade_date, result


def parse_lhb_detail(payload: dict, code: str) -> dict:
    buy_list = payload.get("BuyList") or []
    sell_list = payload.get("SellList") or []
    if not isinstance(buy_list, list) or not all(isinstance(item, dict) for item in buy_list):
        raise ResponseShapeError("BuyList 不是对象数组")
    if not isinstance(sell_list, list) or not all(isinstance(item, dict) for item in sell_list):
        raise ResponseShapeError("SellList 不是对象数组")

    def total(rows: list[dict], field: str) -> float:
        return sum((_float(row.get(field), field) or 0.0) for row in rows)

    return {
        "symbol": code,
        "code": code,
        "buy_list_json": _json(buy_list),
        "sell_list_json": _json(sell_list),
        "buy_seat_count": len(buy_list),
        "sell_seat_count": len(sell_list),
        "buy_list_buy_amount": total(buy_list, "Buy"),
        "buy_list_sell_amount": total(buy_list, "Sell"),
        "sell_list_buy_amount": total(sell_list, "Buy"),
        "sell_list_sell_amount": total(sell_list, "Sell"),
    }


def parse_regulatory_monitor(payload: dict) -> list[dict]:
    result = []
    for index, row in enumerate(_rows(payload, "List")):
        if not isinstance(row, list) or len(row) != 5:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 5 列")
        code = _text(row[0], "monitor.code", required=True)
        result.append(
            {
                "symbol": code,
                "code": code,
                "name": _text(row[1], "monitor.name", required=True),
                "monitor_start_date": _text(row[2], "monitor_start_date"),
                "monitor_end_date": _text(row[3], "monitor_end_date"),
                "monitor_category": _int(row[4], "monitor_category"),
            }
        )
    return result


def parse_regulatory_anomaly(payload: dict) -> list[dict]:
    result = []
    for index, row in enumerate(_rows(payload, "List")):
        if not isinstance(row, list) or len(row) != 11:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 11 列")
        code = _text(row[0], "anomaly.code", required=True)
        result.append(
            {
                "symbol": code,
                "code": code,
                "name": _text(row[1], "anomaly.name", required=True),
                "anomaly_day_offset": _int(row[2], "anomaly_day_offset"),
                "anomaly_detail": _text(row[3], "anomaly_detail"),
                "anomaly_type": _int(row[4], "anomaly_type"),
                "anomaly_days": _int(row[5], "anomaly_days"),
                "deviation_3d_pct": _float(row[6], "deviation_3d_pct"),
                "trigger_price": _float(row[7], "trigger_price"),
                "trigger_change_pct": _float(row[8], "trigger_change_pct"),
                "current_price": _float(row[9], "current_price"),
                "current_change_pct": _float(row[10], "current_change_pct"),
            }
        )
    return result
