"""八个开盘啦接口的严格响应解析。"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any


class ResponseShapeError(ValueError):
    pass


_TRADE_DIRECTIONS = {
    1: "passive_sell",
    2: "active_buy",
    3: "passive_buy",
    4: "active_sell",
}


def _direction(value: object, field: str, mapping: dict[int, str]) -> tuple[int, str]:
    number = _int(value, field)
    if number is None or number not in mapping:
        raise ResponseShapeError(f"{field} 方向未知")
    return number, mapping[number]


def _event_id(*values: object) -> str:
    return ":".join(str(value) for value in values)


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


def parse_interval_stock(payload: dict) -> list[dict]:
    """解析 /区间股票统计 的全市场主力资金排名。"""
    result: list[dict] = []
    for index, row in enumerate(_rows(payload, "List")):
        if not isinstance(row, list) or len(row) < 14:
            raise ResponseShapeError(f"List[{index}] 至少需要 14 列")
        code = _text(row[0], f"List[{index}].code", required=True)
        result.append(
            {
                "symbol": code,
                "code": code,
                "name": _text(row[1], f"List[{index}].name"),
                "price": _float(row[2], "price"),
                "change_pct": _float(row[3], "change_pct"),
                "main_buy": _float(row[4], "main_buy"),
                "main_sell": _float(row[5], "main_sell"),
                "main_net": _float(row[6], "main_net"),
                "turnover_pct": _float(row[7], "turnover_rate"),
                "amount": _float(row[8], "amount"),
                "market_cap": _float(row[9], "market_cap"),
                "themes": _text(row[10], "themes"),
                "main_type": _text(row[12], "main_type"),
                "net_inflow_days": _int(row[13], "net_inflow_days"),
            }
        )
    return result


def parse_capital_net(payload: dict, code: str) -> dict:
    """保留分时大单净额，并将最后一个采样点作为日频收盘快照。"""
    points: list[dict[str, object]] = []
    for index, row in enumerate(_rows(payload, "trend")):
        if not isinstance(row, list) or len(row) != 13:
            raise ResponseShapeError(f"trend[{index}] 必须恰好包含 13 列")
        points.append(
            {
                "time": _text(row[0], f"trend[{index}].time", required=True),
                "trade_count": _int(row[1], "trade_count"),
                "big_order_net": _float(row[2], "big_order_net"),
                "intraday_buy": _float(row[3], "intraday_buy"),
                "intraday_sell": _float(row[4], "intraday_sell"),
                "large_buy": _float(row[7], "large_buy"),
                "large_sell": _float(row[8], "large_sell"),
                "medium_buy": _float(row[9], "medium_buy"),
                "medium_sell": _float(row[10], "medium_sell"),
                "small_buy": _float(row[11], "small_buy"),
                "small_sell": _float(row[12], "small_sell"),
            }
        )
    last = points[-1] if points else {}
    return {
        "symbol": code,
        "code": code,
        "capital_net_points_json": _json(points),
        "capital_net_points": len(points),
        "capital_net_last_time": last.get("time"),
        "capital_net_close": last.get("big_order_net"),
        "capital_buy_close": last.get("intraday_buy"),
        "capital_sell_close": last.get("intraday_sell"),
    }


def parse_large_order_statistics(payload: dict, code: str, trade_date: date) -> dict | None:
    """取目标交易日的日度大单净额；不把历史数组的最后一项假定为当天。"""
    dates = _rows({"List": payload.get("Date")}, "List")
    values = {name: _rows({"List": payload.get(key)}, "List") for name, key in (
        ("tdjl_net_amount", "TDJL"),
        ("ddjl_net_amount", "DDJL"),
        ("zdjl_net_amount", "ZDJL"),
        ("xdjl_net_amount", "XDJL"),
    )}
    if any(len(rows) != len(dates) for rows in values.values()):
        raise ResponseShapeError("大单统计日期与金额数组长度不一致")
    for index, value in enumerate(dates):
        if parse_trade_date(value) != trade_date:
            continue
        tdjl = _float(values["tdjl_net_amount"][index], "TDJL")
        ddjl = _float(values["ddjl_net_amount"][index], "DDJL")
        return {
            "symbol": code,
            "code": code,
            "tdjl_net_amount": tdjl,
            "ddjl_net_amount": ddjl,
            "zdjl_net_amount": _float(values["zdjl_net_amount"][index], "ZDJL"),
            "xdjl_net_amount": _float(values["xdjl_net_amount"][index], "XDJL"),
            "main_net_amount_over_300k": tdjl + ddjl if tdjl is not None and ddjl is not None else None,
        }
    return None


def parse_large_order_trades(payload: dict, code: str) -> list[dict]:
    """解析开盘啦 /13 大单成交，严格保留六列已知字段。"""
    rows = _rows(payload, "List")
    result: list[dict] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 6:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 6 列")
        direction_code, direction = _direction(row[0], f"List[{index}].direction", _TRADE_DIRECTIONS)
        timestamp = _int(row[1], f"List[{index}].timestamp")
        if timestamp is None:
            raise ResponseShapeError(f"List[{index}].timestamp 缺失")
        volume = _float(row[2], f"List[{index}].volume")
        amount = _float(row[3], f"List[{index}].amount")
        price = _float(row[4], f"List[{index}].price")
        event_time = _text(row[5], f"List[{index}].time", required=True)
        event_id = _event_id(code, timestamp, direction_code, volume, amount, price)
        if event_id in seen:
            continue
        seen.add(event_id)
        result.append(
            {
                "event_id": event_id,
                "symbol": code,
                "code": code,
                "direction_code": direction_code,
                "direction": direction,
                "timestamp": timestamp,
                "time": event_time,
                "volume": volume,
                "amount": amount,
                "price": price,
                "source": "kaipanla_13",
            }
        )
    return result


def parse_large_order_intents(payload: dict, code: str) -> list[dict]:
    """解析开盘啦 /14 委托；未知的第十列只作为 raw_tail 保存。"""
    rows = _rows(payload, "List")
    result: list[dict] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != 10:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 10 列")
        side_code = _int(row[5], f"List[{index}].side")
        if side_code not in (1, 2):
            raise ResponseShapeError(f"List[{index}].side 方向未知")
        event_time = _text(row[0], f"List[{index}].time", required=True)
        order_id = _text(row[1], f"List[{index}].order_id", required=True)
        price = _float(row[2], f"List[{index}].price")
        volume = _float(row[3], f"List[{index}].volume")
        amount = _float(row[4], f"List[{index}].amount")
        limit_flag = _int(row[7], f"List[{index}].limit_flag")
        cancel_flag = _int(row[8], f"List[{index}].cancel_flag")
        if limit_flag not in (0, 1) or cancel_flag not in (0, 1):
            raise ResponseShapeError(f"List[{index}] 涨停/撤单标记无效")
        timestamp = _int(row[9], f"List[{index}].timestamp")
        event_id = _event_id(code, order_id, event_time, side_code, amount, cancel_flag)
        if event_id in seen:
            continue
        seen.add(event_id)
        result.append(
            {
                "event_id": event_id,
                "symbol": code,
                "code": code,
                "time": event_time,
                "order_id": order_id,
                "price": price,
                "volume": volume,
                "amount": amount,
                "side_code": side_code,
                "side": "buy" if side_code == 1 else "sell",
                "limit_flag": bool(limit_flag),
                "cancel_flag": bool(cancel_flag),
                "timestamp": timestamp,
                "raw_tail": row[6],
                "source": "kaipanla_14",
            }
        )
    return result


def parse_northbound_sector(payload: dict) -> tuple[date, list[dict]]:
    """解析北向季度板块持仓，报告期必须由上游 Date 声明。"""
    report_date = parse_trade_date(payload.get("Date"))
    if report_date is None:
        raise ResponseShapeError("北向板块持仓缺少报告期")
    rows = []
    for index, row in enumerate(_rows(payload, "List")):
        if not isinstance(row, list) or len(row) != 9:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 9 列")
        rows.append(
            {
                "report_date": report_date.isoformat(),
                "plate_id": _text(row[0], f"List[{index}].plate_id", required=True),
                "plate_name": _text(row[1], f"List[{index}].plate_name"),
                "increase_amount": _float(row[2], "increase_amount"),
                "increase_ratio": _float(row[3], "increase_ratio"),
                "holding_amount": _float(row[4], "holding_amount"),
                "holding_ratio": _float(row[5], "holding_ratio"),
                "market_ratio": _float(row[6], "market_ratio"),
                "market_cap": _float(row[7], "market_cap"),
                "state": _int(row[8], "state"),
                "total_increase_amount": _float(payload.get("Sum_ZCJE"), "Sum_ZCJE"),
                "total_holding_amount": _float(payload.get("Sum_ZCC"), "Sum_ZCC"),
            }
        )
    return report_date, rows


def parse_northbound_stocks(payload: dict, plate_id: str) -> tuple[date, list[dict]]:
    """解析一个北向板块下的季度个股持仓。"""
    report_date = parse_trade_date(payload.get("Date"))
    if report_date is None:
        raise ResponseShapeError("北向个股持仓缺少报告期")
    rows = []
    for index, row in enumerate(_rows(payload, "List")):
        if not isinstance(row, list) or len(row) != 12:
            raise ResponseShapeError(f"List[{index}] 必须恰好包含 12 列")
        code = _text(row[0], f"List[{index}].code", required=True)
        rows.append(
            {
                "report_date": report_date.isoformat(),
                "plate_id": plate_id,
                "symbol": code,
                "code": code,
                "name": _text(row[1], f"List[{index}].name"),
                "increase_amount": _float(row[2], "increase_amount"),
                "increase_ratio": _float(row[3], "increase_ratio"),
                "holding_amount": _float(row[4], "holding_amount"),
                "holding_shares": _float(row[5], "holding_shares"),
                "total_shares": _float(row[6], "total_shares"),
                "market_cap": _float(row[7], "market_cap"),
                "holding_ratio": _float(row[8], "holding_ratio"),
                "market_ratio": _float(row[9], "market_ratio"),
                "float_market_cap": _float(row[10], "float_market_cap"),
                "state": _int(row[11], "state"),
            }
        )
    return report_date, rows


def parse_shareholder_changes(payload: dict, code: str, report_date: date) -> list[dict]:
    """解析指定报告期的本期及上期十大流通股东。"""
    rows = []
    for snapshot_kind, key in (("current", "LTGDData"), ("previous", "LTGDData_SQ")):
        for index, item in enumerate(_rows(payload, key)):
            if not isinstance(item, dict):
                raise ResponseShapeError(f"{key}[{index}] 必须是对象")
            shareholder_id = _text(item.get("JGID"), f"{key}[{index}].JGID", required=True)
            change = _text(item.get("SJJZC"), f"{key}[{index}].SJJZC")
            rows.append(
                {
                    "report_date": report_date.isoformat(),
                    "symbol": code,
                    "code": code,
                    "snapshot_kind": snapshot_kind,
                    "shareholder_id": shareholder_id,
                    "shareholder_name": _text(item.get("JG"), f"{key}[{index}].JG"),
                    "holding_10k_shares": _float(item.get("CYSL"), "CYSL"),
                    "holding_ratio_pct": _float(item.get("ZLTBL"), "ZLTBL"),
                    "holding_change": change,
                    "holding_change_pct": None if change in {"新进", "不变"} else _float(change, "SJJZC"),
                    "shareholder_tag": _int(item.get("NiuSan"), "NiuSan"),
                    "relation_color": _int(item.get("Color"), "Color"),
                }
            )
    return rows


def parse_shareholder_count_changes(payload: dict) -> list[dict]:
    """解析股东人数变更列表，每行 Day 是实际统计日期。"""
    rows = []
    for index, item in enumerate(_rows(payload, "List")):
        if not isinstance(item, dict):
            raise ResponseShapeError(f"List[{index}] 必须是对象")
        report_date = parse_trade_date(item.get("Day"))
        if report_date is None:
            raise ResponseShapeError(f"List[{index}].Day 缺失")
        code = _text(item.get("StockID"), f"List[{index}].StockID", required=True)
        updated_date = parse_trade_date(item.get("UpdateDay"))
        rows.append(
            {
                "report_date": report_date.isoformat(),
                "symbol": code,
                "code": code,
                "name": _text(item.get("Name"), f"List[{index}].Name"),
                "float_holding_ratio": _float(item.get("LTZB"), "LTZB"),
                "chip_concentration": _float(item.get("CMJZ"), "CMJZ"),
                "shareholder_change_pct": _float(item.get("JSQBH"), "JSQBH"),
                "updated_date": updated_date.isoformat() if updated_date else None,
                "is_new": bool(_int(item.get("IsNew"), "IsNew")),
            }
        )
    return rows


def parse_dragon_tiger_movement(payload: dict, trade_date: date) -> list[dict]:
    """解析龙虎榜游资、机构的单日买卖动向。"""
    rows = []
    for participant_index, participant in enumerate(_rows(payload, "List")):
        if not isinstance(participant, dict):
            raise ResponseShapeError(f"List[{participant_index}] 必须是对象")
        participant_id = _text(participant.get("BID"), f"List[{participant_index}].BID", required=True)
        for side, field in (("buy", "Buy"), ("sell", "Sell")):
            values = participant.get(field)
            if not isinstance(values, list):
                raise ResponseShapeError(f"List[{participant_index}].{field} 不是数组")
            for index, item in enumerate(values):
                if not isinstance(item, dict):
                    raise ResponseShapeError(f"{field}[{index}] 必须是对象")
                code = _text(item.get("Sto"), f"{field}[{index}].Sto", required=True)
                rows.append(
                    {
                        "symbol": code,
                        "code": code,
                        "name": _text(item.get("StoN"), f"{field}[{index}].StoN"),
                        "participant_id": participant_id,
                        "participant_name": _text(participant.get("BName"), "BName"),
                        "side": side,
                        "amount": _float(item.get("Money"), "Money"),
                        "three_day": _int(item.get("Three"), "Three") == 1,
                    }
                )
    return rows


def parse_dragon_tiger_details(payload: dict, code: str) -> list[dict]:
    """解析一个龙虎榜股票的买卖营业部明细。"""
    rows = []
    for group_index, group in enumerate(_rows(payload, "List")):
        if not isinstance(group, dict):
            raise ResponseShapeError(f"List[{group_index}] 必须是对象")
        for side, field in (("buy", "BuyList"), ("sell", "SellList")):
            for index, item in enumerate(_rows(group, field)):
                if not isinstance(item, dict):
                    raise ResponseShapeError(f"{field}[{index}] 必须是对象")
                tags = _rows(item, "GroupIcon")
                rows.append(
                    {
                        "symbol": code,
                        "code": code,
                        "log_id": _text(
                            item.get("LogID"),
                            f"{field}[{index}].LogID",
                            required=True,
                        ),
                        "reason_type": _text(item.get("ReasonType"), "ReasonType"),
                        "department_id": _text(item.get("ID"), f"{field}[{index}].ID", required=True),
                        "department_name": _text(item.get("Name"), f"{field}[{index}].Name"),
                        "side": side,
                        "buy_amount": _float(item.get("Buy"), "Buy"),
                        "sell_amount": _float(item.get("Sell"), "Sell"),
                        "rank": _int(item.get("PX"), "PX"),
                        "tags_json": _json(tags),
                    }
                )
    return rows


def parse_sector_strength(payload: dict) -> list[dict]:
    """解析板块强度榜，仅用于发现板块成分采集所需的 plate_id。"""
    rows = []
    for index, row in enumerate(_rows(payload, "list")):
        if not isinstance(row, list) or len(row) < 11:
            raise ResponseShapeError(f"list[{index}] 至少需要 11 列")
        rows.append(
            {
                "plate_id": _text(row[0], f"list[{index}].plate_id", required=True),
                "plate_name": _text(row[1], f"list[{index}].plate_name"),
            }
        )
    return rows


def parse_sector_constituents(payload: dict, plate_id: str) -> list[dict]:
    """解析一个开盘啦板块在目标交易日的完整成分。"""
    rows = []
    for index, row in enumerate(_rows(payload, "list")):
        if not isinstance(row, list) or len(row) < 41:
            raise ResponseShapeError(f"list[{index}] 至少需要 41 列")
        code = _text(row[0], f"list[{index}].code", required=True)
        rows.append(
            {
                "plate_id": plate_id,
                "symbol": code,
                "code": code,
                "name": _text(row[1], f"list[{index}].name"),
                "tags": _text(row[4], "tags"),
                "last_price": _float(row[5], "last_price"),
                "change_pct": _float(row[6], "change_pct"),
                "amount": _float(row[7], "amount"),
                "turnover_rate": _float(row[8], "turnover_rate"),
                "float_market_value": _float(row[10], "float_market_value"),
                "main_net": _float(row[13], "main_net"),
                "limit_tag": _text(row[23], "limit_tag"),
                "rank_tag": _text(row[24], "rank_tag"),
                "limit_count": _int(row[40], "limit_count"),
            }
        )
    return rows


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
