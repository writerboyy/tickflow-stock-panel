"""自由策略历史回测/模拟盘的受管子进程入口。"""
from __future__ import annotations

import json
import logging
import multiprocessing as mp
import queue
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from .bars import Bar, group_bars
from .engine import FreeStrategyConfig, FreeStrategyEngine

logger = logging.getLogger(__name__)

MARKET_METADATA_CALENDAR_DAYS = 30


@dataclass
class MarketData:
    daily: dict[tuple[str, date], dict[str, Any]] = field(default_factory=dict)
    names: dict[str, str] = field(default_factory=dict)
    previous_scale: dict[str, float] = field(default_factory=dict)
    previous_adjusted_close: dict[str, float] = field(default_factory=dict)


def _load_market_data(
    repo: Any,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
) -> MarketData:
    market = MarketData()
    get_daily = getattr(repo, "get_daily_asset", None)
    if callable(get_daily):
        columns = [
            "date", "open", "high", "low", "close", "volume", "amount",
            "raw_close", "raw_high", "raw_low",
        ]
        for symbol in symbols:
            frame = get_daily(asset_type, symbol, start, end, columns)
            for row in frame.iter_rows(named=True):
                market.daily[(symbol, row["date"])] = dict(row)
    get_instruments = getattr(repo, "get_instruments_asset", None)
    if callable(get_instruments):
        instruments = get_instruments(asset_type)
        if not instruments.is_empty() and {"symbol", "name"}.issubset(instruments.columns):
            market.names = {
                str(symbol): str(name or "")
                for symbol, name in instruments.select("symbol", "name").iter_rows()
            }
    return market


def _limit_pct(symbol: str, asset_type: str, name: str) -> float:
    if asset_type == "etf":
        return 0.10
    code = symbol.split(".", 1)[0]
    if "ST" in name.upper():
        return 0.05
    if symbol.endswith(".BJ"):
        return 0.30
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def _round_limit(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _split_ratio(previous_scale: float | None, current_scale: float, asset_type: str) -> float:
    if asset_type != "etf" or previous_scale is None or current_scale <= 0:
        return 1.0
    observed = previous_scale / current_scale
    nearest = round(observed)
    if nearest >= 2 and abs(observed - nearest) / nearest <= 0.02:
        return float(nearest)
    return 1.0


def _observe_daily_price(
    market: MarketData,
    symbol: str,
    adjusted_close: float,
    raw_close: float,
    asset_type: str,
) -> dict[str, float]:
    scale = raw_close / adjusted_close if adjusted_close > 0 and raw_close > 0 else 1.0
    split_ratio = _split_ratio(market.previous_scale.get(symbol), scale, asset_type)
    previous_close = market.previous_adjusted_close.get(symbol)
    reference = previous_close * scale if previous_close is not None else None
    market.previous_scale[symbol] = scale
    market.previous_adjusted_close[symbol] = adjusted_close
    pct = _limit_pct(symbol, asset_type, market.names.get(symbol, ""))
    return {
        "scale": scale,
        "split_ratio": split_ratio,
        "limit_up": _round_limit(reference * (1 + pct)) if reference is not None else None,
        "limit_down": _round_limit(reference * (1 - pct)) if reference is not None else None,
    }


def _daily_bars(
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    market: MarketData,
) -> list[Bar]:
    rows: list[Bar] = []
    for symbol in symbols:
        for (row_symbol, day), row in sorted(market.daily.items(), key=lambda item: item[0][1]):
            if row_symbol != symbol or not start <= day <= end:
                continue
            close = float(row["close"])
            raw_close = float(row.get("raw_close") or close)
            observed = _observe_daily_price(market, symbol, close, raw_close, asset_type)
            scale = observed["scale"]
            rows.append(Bar(
                symbol=symbol,
                timestamp=datetime.combine(day, time(15, 0)),
                open=float(row["open"]), high=float(row["high"]),
                low=float(row["low"]), close=close,
                volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
                raw_open=float(row["open"]) * scale,
                raw_high=float(row.get("raw_high") or float(row["high"]) * scale),
                raw_low=float(row.get("raw_low") or float(row["low"]) * scale),
                raw_close=raw_close,
                tradable=float(row["open"]) > 0 and float(row["high"]) > 0,
                suspended=float(row["open"]) == 0 and float(row["high"]) == 0,
                limit_up=observed["limit_up"], limit_down=observed["limit_down"],
                split_ratio=observed["split_ratio"],
            ))
    return sorted(rows, key=lambda bar: (bar.timestamp, bar.symbol))


def _minute_metadata(frame: Any, market: MarketData, asset_type: str) -> dict[tuple[str, date], dict[str, float]]:
    metadata: dict[tuple[str, date], dict[str, float]] = {}
    if frame.is_empty():
        return metadata
    closes = (
        frame.with_columns(pl.col("datetime").dt.date().alias("_date"))
        .sort(["symbol", "datetime"])
        .group_by(["symbol", "_date"], maintain_order=True)
        .agg(pl.col("close").last().alias("close"))
        .sort(["_date", "symbol"])
    )
    for row in closes.iter_rows(named=True):
        symbol = str(row["symbol"])
        day = row["_date"]
        adjusted_close = float(row["close"])
        daily = market.daily.get((symbol, day), {})
        raw_close = float(daily.get("raw_close") or daily.get("close") or adjusted_close)
        metadata[(symbol, day)] = _observe_daily_price(
            market, symbol, adjusted_close, raw_close, asset_type,
        )
    return metadata


def _inferred_historical_split(previous_raw_close: float, current_raw_close: float) -> float:
    if previous_raw_close <= 0 or current_raw_close <= 0:
        return 1.0
    observed = previous_raw_close / current_raw_close
    nearest = round(observed)
    if nearest >= 2 and abs(observed - nearest) / nearest <= 0.15:
        return float(nearest)
    return 1.0


def _aligned_warmup_bars(
    symbols: list[str],
    start: date,
    end: date,
    market: MarketData,
) -> list[Bar]:
    """按分钟K在回测起点的复权倍率，向前还原连续的日线预热序列。"""
    result: list[Bar] = []
    for symbol in symbols:
        rows = [
            (day, row)
            for (row_symbol, day), row in market.daily.items()
            if row_symbol == symbol and start <= day <= end
        ]
        rows.sort(key=lambda item: item[0])
        if not rows:
            continue
        last_row = rows[-1][1]
        last_close = float(last_row["close"])
        last_raw_close = float(last_row.get("raw_close") or last_close)
        scale = market.previous_scale.get(symbol, last_raw_close / last_close if last_close > 0 else 1.0)
        scales: dict[date, float] = {}
        for index in range(len(rows) - 1, -1, -1):
            day, row = rows[index]
            scales[day] = scale
            if index > 0:
                previous = rows[index - 1][1]
                previous_raw = float(previous.get("raw_close") or previous["close"])
                current_raw = float(row.get("raw_close") or row["close"])
                scale *= _inferred_historical_split(previous_raw, current_raw)
        for day, row in rows:
            raw_close = float(row.get("raw_close") or row["close"])
            local_close = float(row["close"])
            local_scale = raw_close / local_close if local_close > 0 else 1.0
            aligned_scale = scales[day]
            raw_open = float(row["open"]) * local_scale
            raw_high = float(row.get("raw_high") or float(row["high"]) * local_scale)
            raw_low = float(row.get("raw_low") or float(row["low"]) * local_scale)
            result.append(Bar(
                symbol=symbol, timestamp=datetime.combine(day, time(15, 0)),
                open=raw_open / aligned_scale, high=raw_high / aligned_scale,
                low=raw_low / aligned_scale, close=raw_close / aligned_scale,
                volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0),
                raw_open=raw_open, raw_high=raw_high, raw_low=raw_low, raw_close=raw_close,
            ))
    return sorted(result, key=lambda bar: (bar.timestamp, bar.symbol))


def _prime_minute_market_data(
    repo: Any,
    symbols: list[str],
    start: date,
    asset_type: str,
    market: MarketData,
) -> None:
    frame = repo.get_minute_range(symbols, start - timedelta(days=30), start - timedelta(days=1), asset_type)
    _minute_metadata(frame, market, asset_type)


def _resolve_symbols(engine: FreeStrategyEngine, payload: dict[str, Any]) -> tuple[list[str], str]:
    source_symbols = engine.universe
    if source_symbols:
        return source_symbols, "strategy_source"
    legacy_symbols = [str(symbol).strip() for symbol in payload.get("symbols", []) if str(symbol).strip()]
    if legacy_symbols:
        return legacy_symbols, "legacy_config"
    raise ValueError("策略源码未定义股票池，请在 initialize(context) 中调用 context.set_universe([...])")


def _read_rows(
    repo: Any,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    timeframe: str,
    *,
    require_all_symbols: bool = True,
    allow_empty: bool = False,
    market_data: MarketData | None = None,
) -> Iterable[Bar]:
    if timeframe == "1d":
        if market_data is not None:
            rows = _daily_bars(symbols, start, end, asset_type, market_data)
            if not rows and not allow_empty:
                raise ValueError("没有可用的日K数据，请先同步历史行情")
            return rows
        rows: list[Bar] = []
        for symbol in symbols:
            frame = repo.get_daily_asset(asset_type, symbol, start, end, ["date", "open", "high", "low", "close", "volume", "amount", "raw_close", "raw_high", "raw_low"])
            for row in frame.iter_rows(named=True):
                close = float(row["close"])
                raw_close = float(row.get("raw_close") or close)
                scale = raw_close / close if close > 0 else 1.0
                rows.append(Bar(symbol=symbol, timestamp=datetime.combine(row["date"], time(15, 0)), open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=close, volume=float(row.get("volume") or 0), amount=float(row.get("amount") or 0), raw_open=float(row["open"]) * scale, raw_high=float(row.get("raw_high") or float(row["high"]) * scale), raw_low=float(row.get("raw_low") or float(row["low"]) * scale), raw_close=raw_close))
        if not rows:
            raise ValueError("没有可用的日K数据，请先同步历史行情")
        return rows
    frame = repo.get_minute_range(symbols, start, end, asset_type)
    if not frame.is_empty():
        frame = frame.drop_nulls(["open", "high", "low", "close"])
    if frame.is_empty():
        if allow_empty:
            return []
        asset_label = "ETF" if asset_type == "etf" else "股票"
        raise ValueError(
            f"没有可用的{asset_label}分钟K历史数据。请先同步{asset_label}分钟K，"
            "或将周期切换为 1d 后重新运行。"
        )
    found = set(frame["symbol"].unique().to_list())
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing and require_all_symbols:
        raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
    bar_metadata = _minute_metadata(frame, market_data, asset_type) if market_data is not None else {}

    def minute_rows() -> Iterable[Bar]:
        for row in frame.iter_rows(named=True):
            symbol = str(row["symbol"])
            day = row["datetime"].date()
            observed = bar_metadata.get((symbol, day), {})
            scale = float(observed.get("scale", 1.0))
            yield Bar(
                symbol=symbol, timestamp=row["datetime"],
                open=float(row["open"]), high=float(row["high"]), low=float(row["low"]),
                close=float(row["close"]), volume=float(row.get("volume") or 0),
                amount=float(row.get("amount") or 0),
                raw_open=float(row["open"]) * scale, raw_high=float(row["high"]) * scale,
                raw_low=float(row["low"]) * scale, raw_close=float(row["close"]) * scale,
                limit_up=observed.get("limit_up"), limit_down=observed.get("limit_down"),
                split_ratio=float(observed.get("split_ratio", 1.0)),
            )

    if timeframe == "1m":
        return minute_rows()
    return group_bars(minute_rows(), timeframe)


def _prepare_market_data(
    repo: Any,
    engine: FreeStrategyEngine,
    symbols: list[str],
    start: date,
    end: date,
    asset_type: str,
    timeframe: str,
) -> tuple[MarketData, dict[str, Any]]:
    requested_bars = engine.history_requirements.get("1d", 0)
    lookback_days = max(
        MARKET_METADATA_CALENDAR_DAYS,
        requested_bars * 2 + 14 if requested_bars else 0,
    )
    load_start = start - timedelta(days=lookback_days)
    market_data = _load_market_data(repo, symbols, load_start, end, asset_type)
    if not any(start <= day <= end for _, day in market_data.daily):
        formal_market_data = _load_market_data(repo, symbols, start, end, asset_type)
        market_data.daily.update(formal_market_data.daily)
        market_data.names.update(formal_market_data.names)

    warmup_end = start - timedelta(days=1)
    if timeframe == "1d":
        prior_bars = _daily_bars(symbols, load_start, warmup_end, asset_type, market_data)
    else:
        _prime_minute_market_data(repo, symbols, start, asset_type, market_data)
        prior_bars = (
            _aligned_warmup_bars(symbols, load_start, warmup_end, market_data)
            if requested_bars else []
        )

    selected: list[Bar] = []
    if requested_bars:
        by_symbol: dict[str, list[Bar]] = {}
        for bar in prior_bars:
            by_symbol.setdefault(bar.symbol, []).append(bar)
        selected = sorted(
            (
                bar
                for symbol_bars in by_symbol.values()
                for bar in symbol_bars[-requested_bars:]
            ),
            key=lambda bar: (bar.timestamp, bar.symbol),
        )
        engine.preload_history(selected, "1d")

    dates = [bar.timestamp.date() for bar in selected]
    return market_data, {
        "enabled": bool(requested_bars),
        "timeframe": "1d" if requested_bars else None,
        "requested_bars": requested_bars,
        "rows": len(selected),
        "symbols": len({bar.symbol for bar in selected}),
        "start": min(dates).isoformat() if dates else None,
        "end": max(dates).isoformat() if dates else None,
    }


def execute_backtest(payload: dict[str, Any], output: Any) -> None:
    try:
        from app.tickflow.repository import DataStore, KlineRepository
        output.put({"type": "progress", "message": "初始化策略并读取行情数据", "progress": 0.1})
        repo = KlineRepository(DataStore(Path(payload["data_dir"])))
        start, end = date.fromisoformat(payload["start"]), date.fromisoformat(payload["end"])
        config = FreeStrategyConfig(**payload["config"])
        engine = FreeStrategyEngine(payload["source"], payload["timeframe"], config)
        symbols, universe_source = _resolve_symbols(engine, payload)
        if payload.get("checkpoint"):
            engine.restore_checkpoint(payload["checkpoint"])
        market_data, warmup_metadata = _prepare_market_data(
            repo, engine, symbols, start, end, payload["asset_type"], payload["timeframe"],
        )
        replayed_rows = 0
        first_bar: datetime | None = None
        last_bar: datetime | None = None
        symbols_seen: set[str] = set()
        trading_days = 0
        if payload["timeframe"] == "1d":
            bars = _read_rows(repo, symbols, start, end, payload["asset_type"], payload["timeframe"], market_data=market_data)
            output.put({"type": "progress", "message": f"回放 {len(bars)} 根日K", "progress": 0.35})
            replayed_rows = len(bars)
            symbols_seen.update(bar.symbol for bar in bars)
            trading_days = len({bar.timestamp.date() for bar in bars})
            first_bar = min(bar.timestamp for bar in bars)
            last_bar = max(bar.timestamp for bar in bars)
            result = engine.run(bars)
        else:
            output.put({"type": "progress", "message": "按交易日读取并回放分钟K", "progress": 0.35})
            cursor = start
            days_seen = 0
            days_with_bars = 0
            while cursor <= end:
                if cursor.weekday() < 5:
                    bars = _read_rows(
                        repo, symbols, cursor, cursor,
                        payload["asset_type"], payload["timeframe"], require_all_symbols=False, allow_empty=True,
                        market_data=market_data,
                    )
                    rows = list(bars)
                    if rows:
                        replayed_rows += len(rows)
                        for bar in rows:
                            symbols_seen.add(bar.symbol)
                            first_bar = bar.timestamp if first_bar is None else min(first_bar, bar.timestamp)
                            last_bar = bar.timestamp if last_bar is None else max(last_bar, bar.timestamp)
                        engine.run(rows, return_result=False)
                        days_with_bars += 1
                days_seen += 1
                if days_seen % 20 == 0:
                    progress = min(0.9, 0.35 + 0.55 * days_seen / max((end - start).days + 1, 1))
                    output.put({"type": "progress", "message": f"已回放 {days_with_bars} 个交易日", "progress": progress})
                cursor += timedelta(days=1)
            if not days_with_bars:
                raise ValueError("没有可用的分钟K历史数据，请先同步后重试")
            missing = [symbol for symbol in symbols if symbol not in symbols_seen]
            if missing:
                raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
            engine.state = engine.context.state.copy()
            result = engine.result()
            trading_days = days_with_bars
        five_fortunes = result.get("state", {}).get("five_fortunes", {})
        missing_symbols = [symbol for symbol in symbols if symbol not in symbols_seen]
        minute_table = "kline_etf_minute" if payload["asset_type"] == "etf" else "kline_minute"
        result["metadata"] = {
            "strategy_id": payload.get("strategy_id"), "strategy_name": payload.get("strategy_name"),
            "timeframe": payload["timeframe"], "asset_type": payload["asset_type"],
            "start": payload["start"], "end": payload["end"],
            "symbols": symbols, "symbol_count": len(symbols), "universe_source": universe_source,
            "data_days": len(result.get("daily_equity_curve", [])),
            "source_revision": payload.get("source_revision"),
            "resumed_from_checkpoint": bool(payload.get("checkpoint")),
            "warmup": warmup_metadata,
            "nav_filter": five_fortunes.get("nav_filter"),
            "excluded_no_minute_symbols": five_fortunes.get("excluded_no_minute_symbols", []),
            "liquidity_scope": five_fortunes.get("liquidity_scope"),
            "data_coverage": {
                "rows": replayed_rows,
                "first_bar": first_bar.isoformat() if first_bar else None,
                "last_bar": last_bar.isoformat() if last_bar else None,
                "trading_days": trading_days,
                "requested_symbols": symbols,
                "seen_symbols": sorted(symbols_seen),
                "missing_symbols": missing_symbols,
                "configured_provider": payload.get("data_provider", "tickflow"),
                "storage": "kline_daily" if payload["timeframe"] == "1d" else minute_table,
            },
        }
        if payload.get("run_dir"):
            Path(payload["run_dir"]).mkdir(parents=True, exist_ok=True)
            (Path(payload["run_dir"]) / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        output.put({"type": "result", "result": result})
    except BaseException as exc:  # noqa: BLE001 - worker must report all script errors
        logger.exception("free strategy backtest failed")
        output.put({"type": "error", "error": str(exc)})


def start_process(payload: dict[str, Any]) -> tuple[mp.Process, Any]:
    ctx = mp.get_context("spawn")
    output = ctx.Queue()
    process = ctx.Process(target=execute_backtest, args=(payload, output), daemon=True)
    process.start()
    return process, output
