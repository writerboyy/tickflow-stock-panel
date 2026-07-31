"""开盘啦四张扩展表注册、原始响应归档与原子合并。"""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import date
from pathlib import Path
from uuid import uuid4

import polars as pl

from app.market_time import cn_now
from app.plugins.kaipanla.parsers import parse_trade_date
from app.services.ext_data import (
    ExtConfig,
    ExtConfigStore,
    ExtField,
    build_code_lookup,
    normalize_symbol,
)

AUCTION_TABLE = "ext_kpl_auction"
LIMITUP_TABLE = "ext_kpl_limitup"
LHB_TABLE = "ext_kpl_lhb"
REGULATORY_TABLE = "ext_kpl_regulatory"
FUNDS_TABLE = "ext_kpl_funds"
TABLE_IDS = (AUCTION_TABLE, LIMITUP_TABLE, LHB_TABLE, REGULATORY_TABLE, FUNDS_TABLE)

_DTYPES = {
    "string": pl.String,
    "int": pl.Int64,
    "float": pl.Float64,
    "bool": pl.Boolean,
}
_LOCKS: dict[Path, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _base_fields() -> list[ExtField]:
    return [
        ExtField("symbol", "string", "标的代码"),
        ExtField("code", "string", "股票代码"),
        ExtField("name", "string", "股票简称"),
    ]


def _auction_config() -> ExtConfig:
    fields = _base_fields()
    checkpoint_fields = [
        ("collected_at", "string", "采集时间"),
        ("source", "string", "来源接口"),
        ("realtime_change_pct", "float", "实时涨幅（%）"),
        ("limit_buy_amount", "float", "涨停委买额（元）"),
        ("auction_change_pct", "float", "竞价涨幅（%）"),
        ("auction_net_amount", "float", "竞价净额（元）"),
        ("auction_turnover_pct", "float", "竞价换手（%）"),
        ("auction_amount", "float", "竞价成交额（元）"),
        ("post_0920_buy_amount", "float", "09:20 后委买额（元）"),
        ("themes", "string", "题材"),
        ("float_market_cap", "float", "实际流通市值（元）"),
        ("board_label", "string", "连板标签"),
    ]
    for checkpoint in ("0915", "0920", "0925"):
        fields.extend(
            ExtField(f"{name}_{checkpoint}", dtype, f"{checkpoint} {label}")
            for name, dtype, label in checkpoint_fields
        )
    fields.extend(
        [
            ExtField("bid_collected_at", "string", "竞价分时采集时间"),
            ExtField("bid_points_json", "string", "竞价分时明细"),
            ExtField("bid_points", "int", "竞价分时点数"),
            ExtField("bid_first_time", "string", "竞价分时起点"),
            ExtField("bid_last_time", "string", "竞价分时终点"),
            ExtField("bid_last_price", "float", "竞价末价"),
            ExtField("bid_last_volume", "float", "竞价末量"),
            ExtField("bid_preclose_price", "float", "昨收价"),
            ExtField("bid_high_price", "float", "竞价最高价"),
            ExtField("bid_low_price", "float", "竞价最低价"),
            ExtField("bid_open_price", "float", "开盘价"),
        ]
    )
    return ExtConfig(
        id=AUCTION_TABLE,
        label="开盘啦竞价",
        mode="timeseries",
        fields=fields,
        description="开盘啦 /115、/30 竞价快照与 /31 个股竞价分时",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def _limitup_config() -> ExtConfig:
    return ExtConfig(
        id=LIMITUP_TABLE,
        label="开盘啦涨停复盘",
        mode="timeseries",
        fields=_base_fields()
        + [
            ExtField("collected_at", "string", "采集时间"),
            ExtField("plate_codes", "string", "板块代码"),
            ExtField("plate_names", "string", "板块名称"),
            ExtField("limitup_timestamp", "int", "涨停时间戳"),
            ExtField("sealed_order_amount", "float", "封单额（元）"),
            ExtField("board_label", "string", "连板标签"),
            ExtField("consecutive_limitups", "int", "连板数"),
            ExtField("themes", "string", "个股属性"),
            ExtField("turnover_pct", "float", "实际换手（%）"),
            ExtField("float_market_cap", "float", "实际流通市值（元）"),
            ExtField("reason", "string", "涨停原因"),
            ExtField("reason_detail", "string", "详细涨停原因"),
            ExtField("market_advance_count", "int", "上涨家数"),
            ExtField("market_decline_count", "int", "下跌家数"),
            ExtField("market_limitup_count", "int", "涨停家数"),
            ExtField("market_limitdown_count", "int", "跌停家数"),
            ExtField("market_broken_rate_pct", "float", "炸板率（%）"),
            ExtField("yesterday_limitup_change_pct", "float", "昨日涨停表现（%）"),
        ],
        description="开盘啦 /15 盘后涨停复盘、题材及详细涨停原因",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def _lhb_config() -> ExtConfig:
    return ExtConfig(
        id=LHB_TABLE,
        label="开盘啦龙虎榜",
        mode="timeseries",
        fields=_base_fields()
        + [
            ExtField("collected_at", "string", "采集时间"),
            ExtField("increase_pct", "float", "涨幅（%）"),
            ExtField("buy_in_amount", "float", "榜单净买额（元）"),
            ExtField("listings_count", "int", "上榜次数"),
            ExtField("turnover", "float", "成交额（元）"),
            ExtField("circulating_market_cap", "float", "流通市值（元）"),
            ExtField("amplitude_pct", "float", "振幅（%）"),
            ExtField("turnover_pct", "float", "换手率（%）"),
            ExtField("market_cap", "float", "总市值（元）"),
            ExtField("detail_collected_at", "string", "席位明细采集时间"),
            ExtField("buy_list_json", "string", "买入席位明细"),
            ExtField("sell_list_json", "string", "卖出席位明细"),
            ExtField("buy_seat_count", "int", "买入席位数"),
            ExtField("sell_seat_count", "int", "卖出席位数"),
            ExtField("buy_list_buy_amount", "float", "买入席位买额（元）"),
            ExtField("buy_list_sell_amount", "float", "买入席位卖额（元）"),
            ExtField("sell_list_buy_amount", "float", "卖出席位买额（元）"),
            ExtField("sell_list_sell_amount", "float", "卖出席位卖额（元）"),
        ],
        description="开盘啦 /100 每日龙虎榜及 /101 买卖营业部明细",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def _regulatory_config() -> ExtConfig:
    fields = _base_fields()
    snapshot_fields = [
        ("collected_at", "string", "采集时间"),
        ("monitor_start_date", "string", "监控开始日期"),
        ("monitor_end_date", "string", "监控结束日期"),
        ("monitor_category", "int", "监控类别"),
        ("anomaly_day_offset", "int", "异动日期偏移"),
        ("anomaly_detail", "string", "异动描述"),
        ("anomaly_type", "int", "异动类型"),
        ("anomaly_days", "int", "异动天数"),
        ("deviation_3d_pct", "float", "三日偏离值（%）"),
        ("trigger_price", "float", "预计触发价格"),
        ("trigger_change_pct", "float", "触发涨幅（%）"),
        ("current_price", "float", "现价"),
        ("current_change_pct", "float", "当前涨幅（%）"),
    ]
    for snapshot, label in (("pre", "盘前"), ("post", "盘后")):
        fields.extend(
            ExtField(f"{snapshot}_{name}", dtype, f"{label}{field_label}")
            for name, dtype, field_label in snapshot_fields
        )
    return ExtConfig(
        id=REGULATORY_TABLE,
        label="开盘啦异动监管",
        mode="timeseries",
        fields=fields,
        description="开盘啦 /108 重点监控与 /109 多次异动盘前盘后快照",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def _funds_config() -> ExtConfig:
    return ExtConfig(
        id=FUNDS_TABLE,
        label="开盘啦资金流",
        mode="timeseries",
        fields=_base_fields()
        + [
            ExtField("collected_at", "string", "采集时间"),
            ExtField("price", "float", "收盘价"),
            ExtField("change_pct", "float", "涨跌幅（%）"),
            ExtField("main_buy", "float", "主力买入额"),
            ExtField("main_sell", "float", "主力卖出额"),
            ExtField("main_net", "float", "主力净额"),
            ExtField("turnover_pct", "float", "换手率（%）"),
            ExtField("amount", "float", "成交额"),
            ExtField("market_cap", "float", "市值"),
            ExtField("themes", "string", "题材"),
            ExtField("main_type", "string", "主力类型"),
            ExtField("net_inflow_days", "int", "连续净流入天数"),
            ExtField("capital_net_points_json", "string", "分时大单净额"),
            ExtField("capital_net_points", "int", "分时点数"),
            ExtField("capital_net_last_time", "string", "最后分时"),
            ExtField("capital_net_close", "float", "收盘大单净额"),
            ExtField("capital_buy_close", "float", "收盘累计买入额"),
            ExtField("capital_sell_close", "float", "收盘累计卖出额"),
            ExtField("tdjl_net_amount", "float", "特大单净额"),
            ExtField("ddjl_net_amount", "float", "大单净额"),
            ExtField("zdjl_net_amount", "float", "中单净额"),
            ExtField("xdjl_net_amount", "float", "小单净额"),
            ExtField("main_net_amount_over_300k", "float", "30万以上大单净额"),
        ],
        description="开盘啦全市场区间主力资金、大单净额及分时大单净额收盘快照",
        symbol_map={"type": "mapped", "col": "symbol"},
        code_map={"type": "mapped", "col": "code"},
    )


def configs() -> list[ExtConfig]:
    return [_auction_config(), _limitup_config(), _lhb_config(), _regulatory_config(), _funds_config()]


def ensure_configs(data_dir: Path) -> None:
    store = ExtConfigStore(data_dir)
    for config in configs():
        if store.get(config.id) is None:
            store.upsert(config)


def _partition_path(data_dir: Path, table_id: str, trade_date: date) -> Path:
    return data_dir / "ext_data" / table_id / "timeseries" / f"date={trade_date}" / "part.parquet"


def _path_lock(path: Path) -> threading.Lock:
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(path, threading.Lock())


def _normalize_rows(rows: list[dict], data_dir: Path) -> list[dict]:
    codes = [str(row.get("symbol") or row.get("code") or "") for row in rows]
    normalized = normalize_symbol(pl.Series("symbol", codes), build_code_lookup(data_dir)).to_list()
    result = []
    for row, symbol in zip(rows, normalized, strict=True):
        if not symbol:
            continue
        code = str(row.get("code") or symbol.split(".", 1)[0])
        result.append({**row, "symbol": symbol, "code": code})
    return result


def _to_frame(rows: list[dict], config: ExtConfig) -> pl.DataFrame:
    columns = {
        field.name: pl.Series(
            field.name,
            [row.get(field.name) for row in rows],
            dtype=_DTYPES[field.dtype],
            strict=False,
        )
        for field in config.fields
    }
    return pl.DataFrame(columns)


def atomic_upsert(data_dir: Path, table_id: str, trade_date: date, rows: list[dict]) -> int:
    """同一交易日按 symbol 非空合并后，以临时文件原子替换。"""
    if not rows:
        return 0
    ensure_configs(data_dir)
    config = ExtConfigStore(data_dir).get(table_id)
    if config is None:
        raise ValueError(f"未知开盘啦扩展表: {table_id}")
    incoming = _normalize_rows(rows, data_dir)
    path = _partition_path(data_dir, table_id, trade_date)
    path.parent.mkdir(parents=True, exist_ok=True)

    with _path_lock(path):
        existing_rows = pl.read_parquet(path).to_dicts() if path.exists() else []
        merged = {str(row.get("symbol")): row for row in existing_rows if row.get("symbol")}
        for row in incoming:
            symbol = str(row["symbol"])
            current = dict(merged.get(symbol, {}))
            current.update({key: value for key, value in row.items() if value is not None})
            merged[symbol] = current
        frame = _to_frame([merged[key] for key in sorted(merged)], config)
        tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            frame.write_parquet(tmp)
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()
    return len(incoming)


def has_auction_0925(data_dir: Path, trade_date: date) -> bool:
    path = _partition_path(data_dir, AUCTION_TABLE, trade_date)
    if not path.exists():
        return False
    try:
        frame = pl.read_parquet(path, columns=["source_0925"])
    except (OSError, pl.exceptions.PolarsError):
        return False
    return not frame.is_empty() and frame["source_0925"].is_not_null().any()


def archive_raw(
    data_dir: Path,
    endpoint: int | str,
    trade_date: date,
    payload: dict,
    context: str = "",
) -> Path:
    """保存已由客户端脱敏的原始响应，不保存请求参数或 URL。"""
    now = cn_now()
    safe_context = re.sub(r"[^A-Za-z0-9_.-]+", "-", context).strip("-")[:80]
    suffix = f"-{safe_context}" if safe_context else ""
    out_dir = data_dir / "ext_data" / "_kaipanla_raw" / f"date={trade_date}" / str(endpoint)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{now.strftime('%H%M%S-%f')}{suffix}.json"
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    content = {
        "endpoint": f"/{endpoint}",
        "captured_at": now.isoformat(),
        "response": payload,
    }
    try:
        tmp.write_text(json.dumps(content, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return path


def recent_trading_dates(data_dir: Path, limit: int = 60) -> list[date]:
    values: set[date] = set()
    for table in ("kline_daily", "kline_daily_enriched"):
        root = data_dir / table
        if not root.exists():
            continue
        for partition in root.glob("date=*"):
            try:
                value = parse_trade_date(partition.name.removeprefix("date="))
            except ValueError:
                continue
            if value is not None:
                values.add(value)
    return sorted(values)[-limit:]
