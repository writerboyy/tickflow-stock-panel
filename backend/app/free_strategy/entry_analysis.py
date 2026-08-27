"""Post-run analysis for mainline momentum entry signals.

Only symbols and dates that produced a signal are read here.  The historical
backtest universe is never narrowed by optional money-flow coverage.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

import polars as pl


TRAIN_END = date(2024, 12, 31)
HORIZONS = ("30m", "close", "next_day", "3d", "5d")


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def _return_pct(price: Any, base: Any) -> float | None:
    price_value, base_value = _finite(price), _finite(base)
    if price_value is None or base_value is None or base_value <= 0:
        return None
    return (price_value / base_value - 1) * 100


def _rows_by_symbol(frame: pl.DataFrame) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if frame.is_empty():
        return result
    for row in frame.sort(["symbol", "date"]).iter_rows(named=True):
        result[str(row["symbol"])].append(row)
    return result


def _minute_prices(
    repo: Any,
    events: list[dict[str, Any]],
    daily_by_symbol: dict[str, list[dict[str, Any]]],
) -> None:
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_day[event["timestamp"].date()].append(event)
    for day, day_events in by_day.items():
        symbols = sorted({event["symbol"] for event in day_events})
        frame = repo.get_minute_range(symbols, day, day, "stock")
        if frame.is_empty():
            continue
        for event in day_events:
            rows = frame.filter(
                (pl.col("symbol") == event["symbol"])
                & (pl.col("datetime") >= event["timestamp"])
            ).sort("datetime")
            if rows.is_empty():
                continue
            daily_row = next(
                (row for row in daily_by_symbol.get(event["symbol"], []) if row["date"] == day),
                {},
            )
            close_value = _finite(daily_row.get("close"))
            raw_close = _finite(daily_row.get("raw_close")) or close_value
            scale = raw_close / close_value if close_value and raw_close else 1.0
            event["entry_adjusted"] = event["entry_price"] / scale
            plus_30 = event["timestamp"] + timedelta(minutes=30)
            future = rows.filter(pl.col("datetime") >= plus_30)
            if not future.is_empty():
                event["prices"]["30m"] = _finite(future.row(0, named=True).get("close"))
            event["prices"]["close"] = _finite(rows.row(-1, named=True).get("close"))
            event["same_day_high"] = _finite(rows["high"].max())
            event["same_day_low"] = _finite(rows["low"].min())


def _benchmark_prices(repo: Any, events: list[dict[str, Any]], symbol: str) -> dict[str, Any]:
    """Return exact intraday benchmark bases when index minutes are available."""
    result: dict[str, Any] = {"available": False, "events": {}}
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_day[event["timestamp"].date()].append(event)
    for day, day_events in by_day.items():
        frame = repo.get_minute_range([symbol], day, day, "index")
        if frame.is_empty():
            continue
        result["available"] = True
        rows = frame.sort("datetime")
        for event in day_events:
            at_or_after = rows.filter(pl.col("datetime") >= event["timestamp"])
            if at_or_after.is_empty():
                continue
            base = _finite(at_or_after.row(0, named=True).get("close"))
            plus_30 = rows.filter(pl.col("datetime") >= event["timestamp"] + timedelta(minutes=30))
            result["events"][event["id"]] = {
                "base": base,
                "30m": _finite(plus_30.row(0, named=True).get("close")) if not plus_30.is_empty() else None,
                "close": _finite(rows.row(-1, named=True).get("close")),
            }
    return result


def _daily_outcomes(
    events: list[dict[str, Any]],
    daily_by_symbol: dict[str, list[dict[str, Any]]],
) -> None:
    horizon_index = {"next_day": 1, "3d": 3, "5d": 5}
    for event in events:
        rows = daily_by_symbol.get(event["symbol"], [])
        same_index = next(
            (index for index, row in enumerate(rows) if row["date"] == event["timestamp"].date()),
            None,
        )
        if same_index is None:
            continue
        for horizon, offset in horizon_index.items():
            target = same_index + offset
            if target < len(rows):
                event["prices"][horizon] = _finite(rows[target].get("close"))
        excursion_rows = rows[same_index + 1:same_index + 6]
        highs = [value for row in excursion_rows if (value := _finite(row.get("high"))) is not None]
        lows = [value for row in excursion_rows if (value := _finite(row.get("low"))) is not None]
        if event.get("same_day_high") is not None:
            highs.insert(0, event["same_day_high"])
        if event.get("same_day_low") is not None:
            lows.insert(0, event["same_day_low"])
        event["mfe_pct"] = _return_pct(max(highs), event.get("entry_adjusted")) if highs else None
        event["mae_pct"] = _return_pct(min(lows), event.get("entry_adjusted")) if lows else None


def _aggregate(events: Iterable[dict[str, Any]], segment: str) -> dict[str, Any]:
    rows = list(events)
    horizons = []
    for horizon in HORIZONS:
        usable = [row for row in rows if row["returns"].get(horizon) is not None]
        excess = [row["excess"].get(horizon) for row in usable if row["excess"].get(horizon) is not None]
        values = [float(row["returns"][horizon]) for row in usable]
        horizons.append({
            "horizon": horizon,
            "count": len(values),
            "average_return_pct": mean(values) if values else None,
            "average_excess_pct": mean(excess) if excess else None,
            "win_rate_pct": sum(value > 0 for value in values) / len(values) * 100 if values else None,
        })
    mfe = [float(row["mfe_pct"]) for row in rows if row.get("mfe_pct") is not None]
    mae = [float(row["mae_pct"]) for row in rows if row.get("mae_pct") is not None]
    return {
        "segment": segment,
        "signal_count": len(rows),
        "average_mfe_pct": mean(mfe) if mfe else None,
        "average_mae_pct": mean(mae) if mae else None,
        "horizons": horizons,
    }


def _read_source(path: Path) -> pl.DataFrame:
    try:
        return pl.read_parquet(path) if path.exists() else pl.DataFrame()
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _read_partitioned(root: Path) -> pl.DataFrame:
    frames = []
    for path in sorted(root.glob("date=*/part.parquet")):
        frame = _read_source(path)
        if frame.is_empty():
            continue
        if "trade_date" not in frame.columns:
            frame = frame.with_columns(pl.lit(path.parent.name.removeprefix("date=")).alias("trade_date"))
        frames.append(frame)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _flow_maps(data_dir: Path) -> dict[str, dict[tuple[str, date], bool]]:
    specs = {
        "开盘啦资金": (
            _read_partitioned(data_dir / "ext_data/ext_kpl_funds/timeseries"),
            "main_net",
        ),
    }
    result: dict[str, dict[tuple[str, date], bool]] = {}
    for source, (frame, value_column) in specs.items():
        if frame.is_empty():
            result[source] = {}
            continue
        symbol_column = "symbol" if "symbol" in frame.columns else "ts_code"
        prepared = frame.with_columns(
            pl.col(symbol_column).cast(pl.String).alias("_symbol"),
            pl.col("trade_date").cast(pl.String).str.to_date(strict=False).alias("_date"),
        )
        if isinstance(value_column, tuple):
            positive = (
                pl.col(value_column[0]).fill_null(0) - pl.col(value_column[1]).fill_null(0) > 0
            )
        else:
            positive = pl.col(value_column).fill_null(0) > 0
        result[source] = {
            (str(row["_symbol"]), row["_date"]): bool(row["_confirmed"])
            for row in prepared.with_columns(positive.alias("_confirmed"))
            .filter(pl.col("_date").is_not_null())
            .select("_symbol", "_date", "_confirmed")
            .iter_rows(named=True)
        }
    return result


def _matched_flow_analysis(
    events: list[dict[str, Any]],
    data_dir: Path,
    trading_dates: list[date],
) -> dict[str, Any]:
    previous = {
        trading_dates[index]: trading_dates[index - 1]
        for index in range(1, len(trading_dates))
    }
    summaries = []
    for source, lookup in _flow_maps(data_dir).items():
        matched = []
        for event in events:
            timestamp = event["timestamp"]
            event_day = (
                timestamp.date()
                if isinstance(timestamp, datetime)
                else datetime.fromisoformat(str(timestamp)).date()
            )
            prior = previous.get(event_day)
            if prior is None or (event["symbol"], prior) not in lookup:
                continue
            matched.append({**event, "confirmed": lookup[(event["symbol"], prior)]})
        groups = []
        for confirmed in (True, False):
            selected = [row for row in matched if row["confirmed"] is confirmed]
            horizons = []
            for horizon in ("next_day", "3d", "5d"):
                values = [row["returns"][horizon] for row in selected if row["returns"].get(horizon) is not None]
                horizons.append({
                    "horizon": horizon,
                    "count": len(values),
                    "average_return_pct": mean(values) if values else None,
                })
            groups.append({"confirmed": confirmed, "signal_count": len(selected), "horizons": horizons})
        summaries.append({"source": source, "matched_signals": len(matched), "groups": groups})
    return {
        "mode": "prior_trading_day_matched_sample",
        "changes_primary_universe": False,
        "excluded_sources": ["ext_money_flow"],
        "sources": summaries,
    }


def build_mainline_entry_analysis(
    repo: Any,
    result: dict[str, Any],
    start: date,
    end: date,
    data_dir: Path,
    benchmark_symbol: str,
) -> dict[str, Any] | None:
    raw_signals = [
        row for row in result.get("strategy_signals", [])
        if row.get("signal_type") == "mainline_momentum_entry"
    ]
    if not raw_signals:
        return None
    events = []
    for index, signal in enumerate(raw_signals):
        payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
        timestamp = datetime.fromisoformat(str(signal["timestamp"]))
        symbol = str(payload.get("symbol") or "")
        entry_price = _finite(payload.get("price"))
        if not symbol or entry_price is None or entry_price <= 0:
            continue
        events.append({
            "id": str(signal.get("id") or f"entry:{index}"),
            "timestamp": timestamp,
            "symbol": symbol,
            "model": str(payload.get("model") or ""),
            "entry_price": entry_price,
            "entry_adjusted": None,
            "l1_name": payload.get("l1_name"),
            "l2_name": payload.get("l2_name"),
            "prices": {},
            "returns": {},
            "excess": {},
        })
    if not events:
        return None
    symbols = sorted({event["symbol"] for event in events})
    extended_end = end + timedelta(days=14)
    daily = repo.get_daily_asset_batch(
        "stock", symbols, start - timedelta(days=7), extended_end,
        ["symbol", "date", "close", "high", "low", "raw_close"],
    )
    daily_by_symbol = _rows_by_symbol(daily)
    _minute_prices(repo, events, daily_by_symbol)
    _daily_outcomes(events, daily_by_symbol)

    benchmark_daily = repo.get_daily_asset(
        "index", benchmark_symbol, start - timedelta(days=7), extended_end,
        ["date", "close"],
    )
    benchmark_rows = benchmark_daily.sort("date").to_dicts() if not benchmark_daily.is_empty() else []
    trading_dates = [row["date"] for row in benchmark_rows]
    benchmark_intraday = _benchmark_prices(repo, events, benchmark_symbol)
    benchmark_by_date = {row["date"]: row for row in benchmark_rows}
    for event in events:
        base = event.get("entry_adjusted")
        for horizon in HORIZONS:
            event["returns"][horizon] = _return_pct(event["prices"].get(horizon), base)
        intraday = benchmark_intraday["events"].get(event["id"], {})
        event["excess"]["30m"] = (
            event["returns"].get("30m") - benchmark_return
            if event["returns"].get("30m") is not None
            and (benchmark_return := _return_pct(intraday.get("30m"), intraday.get("base"))) is not None
            else None
        )
        event["excess"]["close"] = (
            event["returns"].get("close") - benchmark_return
            if event["returns"].get("close") is not None
            and (benchmark_return := _return_pct(intraday.get("close"), intraday.get("base"))) is not None
            else None
        )
        day = event["timestamp"].date()
        if day in trading_dates:
            day_index = trading_dates.index(day)
            base_close = _finite(benchmark_by_date[day].get("close"))
            for horizon, offset in (("next_day", 1), ("3d", 3), ("5d", 5)):
                target = day_index + offset
                benchmark_return = (
                    _return_pct(benchmark_rows[target].get("close"), base_close)
                    if target < len(benchmark_rows) else None
                )
                stock_return = event["returns"].get(horizon)
                event["excess"][horizon] = (
                    stock_return - benchmark_return
                    if stock_return is not None and benchmark_return is not None else None
                )
        event["segment"] = "train" if day <= TRAIN_END else "out_of_sample"
        event["timestamp"] = event["timestamp"].isoformat()
        event.pop("prices", None)
        event.pop("entry_adjusted", None)
        event.pop("same_day_high", None)
        event.pop("same_day_low", None)

    return {
        "model": events[0]["model"],
        "training_period": {"start": "2021-07-30", "end": TRAIN_END.isoformat()},
        "out_of_sample_period": {"start": "2025-01-01", "end": end.isoformat()},
        "parameters_frozen_after": TRAIN_END.isoformat(),
        "benchmark_symbol": benchmark_symbol,
        "intraday_benchmark_available": bool(benchmark_intraday["available"]),
        "summaries": [
            _aggregate(events, "all"),
            _aggregate((row for row in events if row["segment"] == "train"), "train"),
            _aggregate((row for row in events if row["segment"] == "out_of_sample"), "out_of_sample"),
        ],
        "events": events,
        "money_flow": _matched_flow_analysis(events, data_dir, trading_dates),
    }
