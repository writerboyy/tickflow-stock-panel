"""Point-in-time inputs for the original four-mode board strategy.

The archived strategy is intentionally not imported here.  This module only
translates its rules to the repository's standard daily/minute/PIT datasets.
Missing inputs are represented as ``waiting_data`` instead of being replaced
with another strategy's score or universe.
"""
from __future__ import annotations

import copy
import json
import logging
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from app.price_limits import limit_price, price_limit_pct
from app.market_time import cn_today
from app.services.ingestion_manifest import load_ingestion_manifest


logger = logging.getLogger(__name__)


MODE_NAMES = {"yje": "一进二", "rzq": "弱转强", "qs": "趋势股", "sb": "首板"}
COMBO_CONFIG = {
    "combo_1": {"name": "吸筹期地量地价", "priority": 4, "target_adjust": -1},
    "combo_2": {"name": "洗盘期缩量回调", "priority": 3, "target_adjust": 0},
    "combo_3": {"name": "突破型放量启动", "priority": 1, "target_adjust": 2},
    "combo_4": {"name": "主升浪回踩低吸", "priority": 2, "target_adjust": 1},
    "combo_5": {"name": "极端超跌反弹", "priority": 5, "target_adjust": 1},
    "combo_6": {"name": "标准多头排列", "priority": 6, "target_adjust": 0},
}


def _finite(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _as_day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _pct(current: float, previous: float) -> float:
    return (current / previous - 1.0) if previous else 0.0


def _limit(row: dict[str, Any], previous: dict[str, Any] | None) -> float | None:
    if previous is None:
        return None
    close = _finite(previous.get("raw_close", previous.get("close")))
    if close is None or close <= 0:
        return None
    symbol = str(row.get("symbol") or "")
    name = str(row.get("name") or row.get("pit_name") or "")
    return limit_price(close, price_limit_pct(symbol, row["date"], is_risk_warning="ST" in name.upper()), up=True)


def _is_limit(row: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    close = _finite(row.get("raw_close", row.get("close")))
    ceiling = _limit(row, previous)
    return close is not None and ceiling is not None and close >= ceiling - 0.005


def _is_broken_board(row: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    high = _finite(row.get("raw_high", row.get("high")))
    close = _finite(row.get("raw_close", row.get("close")))
    ceiling = _limit(row, previous)
    return bool(high is not None and close is not None and ceiling is not None and high >= ceiling - 0.005 and close < ceiling - 0.005)


def _auction_change_fraction(row: dict[str, Any]) -> float | None:
    """The persisted KaiPanLa ``auction_change_pct`` fields are percentage points."""
    value = _finite(row.get("auction_change_pct_0925"))
    return value / 100.0 if value is not None else None


def _auction_manifest_is_valid_empty(manifest: dict[str, Any]) -> bool:
    """A complete all-empty auction snapshot is a valid provider response.

    The collector intentionally does not publish a zero-row parquet partition
    for ``valid_empty`` responses.  Treating that absence as a storage gap
    would incorrectly block modes whose static rules do not need today's
    auction data.
    """
    if manifest.get("status") != "complete":
        return False
    components = manifest.get("components")
    expected = manifest.get("expected_components")
    if not isinstance(components, dict):
        return False
    names = expected if isinstance(expected, list) and expected else list(components)
    if not names or any(name not in components for name in names):
        return False
    return all(
        isinstance(components[name], dict)
        and components[name].get("status") in {"valid_empty", "not_applicable"}
        and int(components[name].get("rows") or 0) == 0
        for name in names
    )


def _stock_symbols(repo: Any) -> list[str]:
    """Return the native A-share universe used by the four-mode snapshot."""
    instruments = repo.get_instruments_asset("stock")
    if instruments is None or instruments.is_empty() or "symbol" not in instruments.columns:
        return []
    return sorted({
        str(symbol).strip().upper()
        for symbol in instruments["symbol"].to_list()
        if str(symbol).strip().upper().endswith((".SH", ".SZ"))
    })


def four_mode_limit_up_symbols(repo: Any, trade_date: date) -> list[str]:
    """Find the symbols whose *completed* daily bar hit the native limit.

    This is intentionally a small preparation universe for the archived
    first-to-second-board score.  It never uses auction or substitute prices.
    """
    symbols = _stock_symbols(repo)
    if not symbols:
        return []
    daily = repo.get_daily_asset_batch(
        "stock",
        symbols,
        trade_date - timedelta(days=35),
        trade_date,
        ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "raw_open", "raw_high", "raw_low", "raw_close"],
    )
    if daily is None or daily.is_empty() or "symbol" not in daily.columns:
        return []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in daily.sort(["symbol", "date"]).to_dicts():
        day = _as_day(row.get("date"))
        if day is not None:
            row["date"] = day
            grouped.setdefault(str(row.get("symbol") or "").strip().upper(), []).append(row)
    result: list[str] = []
    for symbol, rows in grouped.items():
        visible = [row for row in rows if row["date"] <= trade_date]
        if visible and visible[-1]["date"] == trade_date and len(visible) >= 2 and _is_limit(visible[-1], visible[-2]):
            result.append(symbol)
    return sorted(set(result))


def four_mode_minute_requirements(
    repo: Any,
    start: date,
    end: date,
    requirement: dict[str, Any],
    *,
    max_target_days: int = 5,
) -> dict[date, list[str]]:
    """Return ``{completed_daily_date: symbols}`` needing first-to-second data.

    A paper account can start on a morning before its first callback.  The
    extra lookback before ``start`` lets that account prepare the previous
    completed trading day without opening a full-market minute sync.
    """
    if end < start:
        return {}
    symbols = _stock_symbols(repo)
    if not symbols:
        return {}
    index_symbol = str(requirement.get("index_symbol") or "000852.SH")
    index = repo.get_daily_asset("index", index_symbol, start - timedelta(days=35), end, ["date"])
    if index is None or index.is_empty() or "date" not in index.columns:
        return {}
    trading_days = sorted({
        day for value in index["date"].to_list() if (day := _as_day(value)) is not None
    })
    if not trading_days:
        return {}
    daily = repo.get_daily_asset_batch(
        "stock",
        symbols,
        start - timedelta(days=max(180, int(requirement.get("lookback_days", 80)) * 3)),
        end,
        ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "raw_open", "raw_high", "raw_low", "raw_close"],
    )
    if daily is None or daily.is_empty() or "symbol" not in daily.columns:
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in daily.sort(["symbol", "date"]).to_dicts():
        day = _as_day(row.get("date"))
        if day is not None:
            row["date"] = day
            grouped.setdefault(str(row.get("symbol") or "").strip().upper(), []).append(row)
    requirements: dict[date, set[str]] = {}
    for trading_day in trading_days:
        for symbol, rows in grouped.items():
            visible = [row for row in rows if row["date"] < trading_day]
            if len(visible) >= 2 and _is_limit(visible[-1], visible[-2]):
                requirements.setdefault(visible[-1]["date"], set()).add(symbol)
    selected_days = sorted(requirements)[-max(1, int(max_target_days)):]
    return {day: sorted(requirements[day]) for day in selected_days}


def ensure_four_mode_minute_data(
    repo: Any,
    capset: Any,
    requirements: dict[date, list[str]],
) -> dict[str, Any]:
    """Fill only missing four-mode minute partitions and report unresolved rows."""
    from app.services import kline_sync

    attempted = 0
    written = 0
    unresolved: dict[str, list[str]] = {}
    for trade_date, requested in sorted(requirements.items()):
        symbols = sorted(set(str(symbol).strip().upper() for symbol in requested if str(symbol).strip()))
        if not symbols:
            continue
        try:
            existing = repo.get_minute_range(symbols, trade_date, trade_date, "stock")
        except Exception:  # noqa: BLE001
            existing = pl.DataFrame()
        covered = set(existing["symbol"].cast(pl.String).to_list()) if existing is not None and not existing.is_empty() and "symbol" in existing.columns else set()
        missing = [symbol for symbol in symbols if symbol not in covered]
        if not missing:
            continue
        attempted += len(missing)
        try:
            written += int(kline_sync.sync_and_persist_minute(
                missing,
                repo,
                capset,
                days=1,
                window_start=datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=25),
                window_end=datetime.combine(trade_date, datetime.min.time()).replace(hour=15, minute=5),
                asset_type="stock",
            ) or 0)
        except Exception as exc:  # noqa: BLE001
            logger.warning("四合一分钟K准备失败 (%s, %d只): %s", trade_date, len(missing), type(exc).__name__)
        try:
            refreshed = repo.get_minute_range(missing, trade_date, trade_date, "stock")
        except Exception:  # noqa: BLE001
            refreshed = pl.DataFrame()
        covered_after = set(refreshed["symbol"].cast(pl.String).to_list()) if refreshed is not None and not refreshed.is_empty() and "symbol" in refreshed.columns else set()
        remaining = [symbol for symbol in missing if symbol not in covered_after]
        if remaining:
            unresolved[trade_date.isoformat()] = remaining
    return {
        "attempted_symbols": attempted,
        "written_rows": written,
        "unresolved": unresolved,
        "status": "waiting_data" if unresolved else "ready",
    }


def yje_static_score(rows: list[dict[str, Any]], *, minute_rows: list[dict[str, Any]] | None = None) -> tuple[float, dict[str, Any], str | None]:
    """Reproduce the archived 100-point first-to-second-board score."""
    if len(rows) < 21:
        return 0.0, {}, "需要至少21个交易日日线"
    last = rows[-1]
    previous = rows[-2]
    if not _is_limit(last, previous):
        return 0.0, {}, "昨日不是涨停"
    close = _finite(last.get("raw_close", last.get("close")), 0.0) or 0.0
    volume = _finite(last.get("volume"), 0.0) or 0.0
    prior_volume = _finite(previous.get("volume"), 0.0) or 0.0
    ratio = volume / prior_volume if prior_volume > 0 else 999.0
    volume_score = 20 if ratio < 0.3 else 15 if ratio < 0.5 else 5 if ratio < 1.0 else 0
    decline = _pct(close, _finite(rows[-21].get("raw_close", rows[-21].get("close")), close) or close) * 100
    bottom_score = 15 if decline < -10 else 8 if decline < -5 else 3 if decline < 0 else 0
    seal_ratio = None
    seal_duration = 0
    if minute_rows:
        total = sum(_finite(item.get("volume"), 0.0) or 0.0 for item in minute_rows)
        limit_value = _limit(last, previous) or 0.0
        limit_items = [item for item in minute_rows if (_finite(item.get("close"), 0.0) or 0.0) >= limit_value * 0.998]
        seal_volume = sum(_finite(item.get("volume"), 0.0) or 0.0 for item in limit_items)
        seal_ratio = seal_volume / total if total > 0 else 1.0
        seal_duration = len(minute_rows) - minute_rows.index(limit_items[0]) if limit_items else 0
    else:
        return 0.0, {}, "缺少昨日分钟K，无法计算封板质量"
    board_score = (
        25 if seal_ratio < 0.1 else 15 if seal_ratio < 0.3 else 5 if seal_ratio < 0.5 else 0
    ) + (10 if seal_duration >= 180 else 6 if seal_duration >= 120 else 3 if seal_duration >= 60 else 0)
    high = _finite(last.get("raw_high", last.get("high")), close) or close
    low = _finite(last.get("raw_low", last.get("low")), close) or close
    amplitude = (high - low) / (_limit(last, previous) or close) if close else 0.0
    stability_score = 5 if amplitude < 0.02 else 3 if amplitude < 0.05 else 1 if amplitude < 0.08 else 0
    one_word = bool(minute_rows and (_finite(minute_rows[0].get("close"), 0.0) or 0.0) >= (_limit(last, previous) or 0.0) * 0.998)
    score = board_score + (5 if one_word else 0) + stability_score + volume_score + bottom_score
    return float(score), {
        "封板质量总分": float(board_score + (5 if one_word else 0) + stability_score),
        "封板成交占比": seal_ratio,
        "封板时长": seal_duration,
        "一字板": one_word,
        "价格振幅": amplitude,
        "T-1日量比": ratio,
        "T-1日量比评分": volume_score,
        "20日跌幅": decline,
        "底部首板评分": bottom_score,
        "综合评分": score,
    }, None


def rzq_static_candidate(rows: list[dict[str, Any]], valuation: dict[str, Any] | None) -> tuple[dict[str, Any] | None, str | None]:
    if len(rows) < 10:
        return None, "需要至少10个交易日日线"
    current = rows[-1]
    prev = rows[-2]
    prev_prev = rows[-3]
    if not _is_broken_board(current, prev):
        return None, "昨日不是炸板"
    if _is_limit(prev, prev_prev) or (len(rows) >= 3 and _is_limit(rows[-3], rows[-4] if len(rows) >= 4 else None)):
        return None, "前一或前二日涨停"
    ma5 = sum((_finite(row.get("close"), 0.0) or 0.0) for row in rows[-5:]) / 5
    ma10 = sum((_finite(row.get("close"), 0.0) or 0.0) for row in rows[-10:]) / 10
    preceding = rows[-6:-1]
    previous_limit_count = sum(
        _is_limit(row, preceding[index - 1] if index else None)
        for index, row in enumerate(preceding)
    )
    distance_to_ma5 = abs((_finite(current.get("close"), 0.0) or 0.0) - ma5) / ma5 if ma5 else 1.0
    if not (previous_limit_count in {1, 2, 3} or distance_to_ma5 <= .10):
        return None, "近6日涨停次数/MA5距离不符"
    if (_finite(current.get("close"), 0.0) or 0.0) <= ma10:
        return None, "昨日收盘低于MA10"
    if _pct(_finite(current.get("close"), 0.0) or 0.0, _finite(rows[-7].get("close"), 0.0) or 0.0) > 0.50:
        return None, "六日涨幅超过50%"
    open_value = _finite(current.get("open"), 0.0) or 0.0
    close_value = _finite(current.get("close"), 0.0) or 0.0
    amount = _finite(current.get("amount"), 0.0) or 0.0
    market_cap = _finite((valuation or {}).get("market_cap"))
    float_cap = _finite((valuation or {}).get("float_market_cap", (valuation or {}).get("circulating_market_cap")))
    if close_value <= open_value:
        return None, "昨日不是阳线"
    if not 1e8 <= amount <= 60e8:
        return None, "成交额不在1-60亿"
    if market_cap is None or market_cap < 30e8:
        return None, "总市值不足30亿"
    if float_cap is None or float_cap > 1500e8:
        return None, "流通市值超过1500亿"
    return {
        "symbol": str(current["symbol"]),
        "stock": str(current["symbol"]),
        "yesterday_close": close_value,
        "prev_limit_up_count": int(previous_limit_count),
        "distance_to_ma5": distance_to_ma5,
    }, None


def sb_static_score(rows: list[dict[str, Any]], valuation: dict[str, Any] | None) -> tuple[dict[str, Any] | None, str | None]:
    if len(rows) < 60:
        return None, "需要至少60个交易日日线"
    last, prev = rows[-1], rows[-2]
    if any(_is_limit(rows[index], rows[index - 1] if index else None) for index in range(max(1, len(rows) - 10), len(rows))):
        return None, "近10日已有涨停"
    change = _pct(_finite(last.get("close"), 0.0) or 0.0, _finite(prev.get("close"), 0.0) or 0.0) * 100
    amplitude = ((_finite(last.get("high"), 0.0) or 0.0) - (_finite(last.get("low"), 0.0) or 0.0)) / (_finite(prev.get("close"), 0.0) or 1.0) * 100
    vol_ma5_prev = sum((_finite(row.get("volume"), 0.0) or 0.0) for row in rows[-6:-1]) / 5
    vol_ratio = (_finite(last.get("volume"), 0.0) or 0.0) / vol_ma5_prev if vol_ma5_prev > 0 else 0.0
    if not 2 <= change <= 9 or amplitude <= 2 or vol_ratio <= 1.5:
        return None, "昨日涨幅/振幅/量比不符"
    closes = [_finite(row.get("close"), 0.0) or 0.0 for row in rows]
    volumes = [_finite(row.get("volume"), 0.0) or 0.0 for row in rows]
    ma5 = sum(closes[-5:]) / 5
    if (_finite(last.get("close"), 0.0) or 0.0) < ma5 * 0.98:
        return None, "收盘低于MA5"
    high_ratio_days = 0
    for index in range(len(rows) - 4, len(rows)):
        if index < 5:
            continue
        base = sum(volumes[index - 5:index]) / 5
        if base > 0 and volumes[index] / base > 1.3:
            high_ratio_days += 1
    if high_ratio_days < 2:
        return None, "最近4日量比不足"
    ma20 = sum(closes[-20:]) / 20
    ma20_prev = sum(closes[-21:-1]) / 20
    if ma20_prev <= 0 or (ma20 - ma20_prev) / ma20_prev < 0:
        return None, "MA20斜率为负"
    if sum(volumes[-5:]) / 5 <= sum(volumes[-10:]) / 10:
        return None, "近5日均量未超过近10日"
    ma5_prev = sum(closes[-6:-1]) / 5
    vol_ma5 = sum(volumes[-5:]) / 5
    vol_ma5_prev = sum(volumes[-6:-1]) / 5
    if ma5_prev <= 0 or (ma5 - ma5_prev) / ma5_prev <= 0 or (vol_ma5_prev <= 0 or (vol_ma5 - vol_ma5_prev) / vol_ma5_prev <= 0):
        return None, "MA5或VOL_MA5斜率不为正"
    total_cap = _finite((valuation or {}).get("market_cap"))
    float_cap = _finite((valuation or {}).get("float_market_cap", (valuation or {}).get("circulating_market_cap")))
    pe = _finite((valuation or {}).get("pe_ttm", (valuation or {}).get("pe_ratio")))
    avg_amount = sum((_finite(row.get("amount"), 0.0) or 0.0) for row in rows[-60:]) / 60
    if total_cap is None or total_cap < 50e8 or pe is None or pe <= 0 or float_cap is None or not 50e8 <= float_cap <= 1500e8 or avg_amount < 1e8:
        return None, "估值或流动性不符"
    if not any((_finite(row.get("close"), 0.0) or 0.0) > (_finite(row.get("open"), 0.0) or 0.0) for row in rows[-5:]):
        return None, "近5日无阳线"
    score_change = 30 - abs(change - 4.5) * 2 if 2 <= change <= 7 else max(0, 15 - abs(change - 4.5) * 3)
    score_volume = 25 - abs(vol_ratio - 2.25) * 4 if 1.5 <= vol_ratio <= 3 else max(0, 15 - abs(vol_ratio - 2.25) * 3)
    range_value = (_finite(last.get("high"), 0.0) or 0.0) - (_finite(last.get("low"), 0.0) or 0.0)
    score_body = ((_finite(last.get("close"), 0.0) or 0.0) - (_finite(last.get("low"), 0.0) or 0.0)) / range_value * 20 if range_value > 0 else 10
    distance = abs((_finite(last.get("close"), 0.0) or 0.0) / ma5 - 1) * 100 if ma5 > 0 else 100
    score_ma = max(0, 20 - distance * 4) + max(0, min(5, (ma5 - ma5_prev) / ma5_prev * 100))
    symbol = str(last["symbol"])
    return {"symbol": symbol, "stock": symbol, "score": round(score_change + score_volume + score_body + score_ma, 1), "chg_pct": change, "vol_ratio": vol_ratio}, None


def _rsi(values: list[float], period: int = 14) -> float:
    if len(values) <= period:
        return 50.0
    gains = [max(0.0, values[i] - values[i - 1]) for i in range(len(values) - period, len(values))]
    losses = [max(0.0, values[i - 1] - values[i]) for i in range(len(values) - period, len(values))]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    return 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)


def trend_features(rows: list[dict[str, Any]]) -> dict[str, float | bool]:
    closes = [_finite(row.get("close"), 0.0) or 0.0 for row in rows]
    opens = [_finite(row.get("open"), 0.0) or 0.0 for row in rows]
    highs = [_finite(row.get("high"), 0.0) or 0.0 for row in rows]
    lows = [_finite(row.get("low"), 0.0) or 0.0 for row in rows]
    volumes = [_finite(row.get("volume"), 0.0) or 0.0 for row in rows]
    def average(values: list[float], period: int, end: int | None = None) -> float:
        end = len(values) if end is None else end
        window = values[max(0, end - period):end]
        return sum(window) / period if len(window) == period else 0.0

    ma5, ma10, ma20, ma60 = (average(closes, period) for period in (5, 10, 20, 60))
    prev_ma5, prev_ma10, prev_ma20 = (
        average(closes, period, len(closes) - 5) for period in (5, 10, 20)
    )
    vol_ma5, vol_ma20 = average(volumes, 5), average(volumes, 20)
    prev_vol_ma5 = average(volumes, 5, len(volumes) - 5)
    last_close = closes[-1]
    range_value = max(highs[-1] - lows[-1], 0.0001)
    body = abs(closes[-1] - opens[-1])
    upper = highs[-1] - max(closes[-1], opens[-1])
    lower = min(closes[-1], opens[-1]) - lows[-1]
    def ema(period: int) -> float:
        if not closes:
            return 0.0
        value = closes[0]
        alpha = 2.0 / (period + 1)
        for close in closes[1:]:
            value = alpha * close + (1 - alpha) * value
        return value

    ema12, ema26 = ema(12), ema(26)
    mean_ma = (ma5 + ma10 + ma20 + ma60) / 4 if ma60 else 0.0
    ma_std = math.sqrt(sum((value - mean_ma) ** 2 for value in (ma5, ma10, ma20, ma60)) / 3) if ma60 else 0.0
    increasing = [int(volumes[i] > volumes[i - 1] > volumes[i - 2]) for i in range(2, len(volumes))]
    volume_min_20 = min(volumes[-20:]) if len(volumes) >= 20 else 0.0
    delta = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = sum(max(value, 0.0) for value in delta[-14:]) / 14
    losses = sum(max(-value, 0.0) for value in delta[-14:]) / 14
    rsi = 100.0 if losses == 0 else 100 - 100 / (1 + gains / (losses + 0.0001))
    return {
        "returns_1d": _pct(closes[-1], closes[-2]), "returns_5d": _pct(closes[-1], closes[-6]),
        "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
        "ma5_slope": _pct(ma5, prev_ma5), "ma10_slope": _pct(ma10, prev_ma10), "ma20_slope": _pct(ma20, prev_ma20),
        "ma_spread_ratio": (abs(ma5 - ma10) / (ma10 + 0.0001)) / (abs(ma10 - ma20) / (ma20 + 0.0001) + 0.0001) if ma20 else 0.0,
        "ma_convergence": ma_std / (mean_ma + 0.0001) if mean_ma else 1.0,
        "price_ma_angle": math.degrees(math.atan((_pct(last_close, ma20) if ma20 else 0) * 20)),
        "volume_ma5": vol_ma5, "volume_ma20": vol_ma20, "volume_ratio": volumes[-1] / (vol_ma5 + 0.0001),
        "volume_min_20": volume_min_20, "volume_stack_days": sum(increasing[-5:]),
        "volume_shrink": volumes[-1] < vol_ma20 * 0.6 if vol_ma20 else False,
        "volume_pulse": False,
        "body_ratio": body / range_value, "upper_shadow_ratio": upper / range_value, "lower_shadow_ratio": lower / range_value,
        "hammer": lower / range_value > 0.5 and body / range_value < 0.3 and upper / range_value < 0.1,
        "engulfing": closes[-1] > opens[-1] and closes[-2] < opens[-2] and closes[-1] > opens[-2] and opens[-1] < closes[-2],
        "close_position": (closes[-1] - lows[-1]) / (range_value + 0.0001),
        "open_position": (opens[-1] - lows[-1]) / (range_value + 0.0001),
        "low_open_high_close": (opens[-1] - lows[-1]) / (range_value + 0.0001) < 0.3 and (closes[-1] - lows[-1]) / (range_value + 0.0001) > 0.6,
        "divergence_bottom": closes[-1] == min(closes[-20:]) and volumes[-1] > volume_min_20 * 1.2 if len(closes) >= 20 else False,
        "price_volume_healthy": closes[-1] > closes[-2] and volumes[-1] > volumes[-2],
        "rsi_14": rsi,
        "macd": ema12 - ema26, "volume_ma_up": vol_ma5 > prev_vol_ma5,
    }


def _combo_score(features: dict[str, Any], previous: dict[str, Any], combo: str) -> tuple[bool, float, int, int]:
    f = features
    p = previous
    if combo == "combo_1":
        core = {
            "ma20_shake": abs(f["close"] - f["ma20"]) / f["ma20"] < .03 if f["ma20"] else False,
            "rsi_low": f["rsi_14"] < 40,
            "volume_dry": f["volume"] < f["volume_ma20"] * .6 or abs(f["volume"] - f["volume_min_20"]) < .01,
        }
        edge = {
            "slope_up": f["ma20_slope"] > p.get("ma20_slope", 0),
            "convergence": f["ma_convergence"] < .03,
            "hammer": f["hammer"], "divergence": f["divergence_bottom"],
            "close_pos": f["close_position"] > .6,
            "pv_healthy": f["price_volume_healthy"],
        }
    elif combo == "combo_2":
        deviation = (f["close"] - f["ma10"]) / f["ma10"] if f["ma10"] else 0
        core = {"touch_ma10": -.05 < deviation < -.02, "above_ma20": f["close"] > f["ma20"], "rsi_neutral": 45 < f["rsi_14"] < 60}
        edge = {
            "ma_bull": f["ma5"] > f["ma20"] and f["ma10"] > f["ma20"],
            "volume_shrink": f["volume_ratio"] < .7,
            "long_shadow": f["lower_shadow_ratio"] > f["body_ratio"] * 1.5,
            "no_pulse": not f["volume_pulse"], "close_pos": f["close_position"] > .5,
            "slope_diff": f["ma5_slope"] > f["ma10_slope"],
        }
    elif combo == "combo_3":
        core = {"breakout": f["returns_1d"] > .04, "volume_surge": f["volume_ratio"] > 1.3, "ma_converge": f["ma_convergence"] < .025, "price_angle": f["price_ma_angle"] > 25}
        edge = {"ma_spread": f["ma_spread_ratio"] > 1.8, "body_strong": f["body_ratio"] > .5, "engulfing": f["engulfing"], "close_high": f["close_position"] > .8, "vol_stack": f["volume_stack_days"] >= 2, "no_pump": not f["volume_pulse"]}
    elif combo == "combo_4":
        core = {"bull_arrange": f["close"] > f["ma5"] > f["ma10"] > f["ma20"], "touch_ma10": abs((f["close"] - f["ma10"]) / f["ma10"]) < .03 if f["ma10"] else False, "ma20_up": f["ma20_slope"] > 0, "vol_shrink": f["volume_ratio"] < .8}
        edge = {"lower_shadow": f["lower_shadow_ratio"] > .35, "close_pos": f["close_position"] > .55, "ma_spread": f["ma_spread_ratio"] > 1.3, "above_ma20_safe": f["close"] > f["ma20"] * 1.02, "vol_healthy": f["volume"] > f["volume_ma20"] * .4, "slope_accelerate": f["ma5_slope"] > -.02}
    elif combo == "combo_5":
        core = {"deep_fall": f["returns_5d"] < -.06 or f["rsi_14"] < 28, "volume_freeze": abs(f["volume"] - f["volume_min_20"]) < .01 or f["volume"] < f["volume_ma20"] * .5, "far_ma60": _pct(f["close"], f["ma60"]) < -.06 if f["ma60"] else False}
        edge = {"ma_converge": f["ma_convergence"] < .04, "rebound": f["returns_1d"] > 0, "hammer_engulf": f["hammer"] or (f["engulfing"] and f["returns_1d"] > .03), "divergence": f["divergence_bottom"], "low_open_high": f["low_open_high_close"], "ma_slope_recover": f["ma20_slope"] > p.get("ma20_slope", 0)}
    elif combo == "combo_6":
        core = {"ma_bull": f["ma5"] > f["ma10"] > f["ma20"], "price_above_ma5": f["close"] > f["ma5"], "ma20_up": f["ma20_slope"] > 0, "volume_healthy": f["volume_ratio"] > 1.0}
        edge = {"close_pos": f["close_position"] > .5, "body_strong": f["body_ratio"] > .4, "no_long_shadow": f["upper_shadow_ratio"] < .3, "macd_positive": f["macd"] > 0, "volume_ma_up": f["volume_ma5"] > f["volume_ma20"], "rsi_healthy": 40 < f["rsi_14"] < 70}
    else:
        return False, 0.0, 0, 0
    core_met, edge_met = sum(core.values()), sum(edge.values())
    score = (core_met / len(core) * 3 + edge_met / len(edge)) / 4 * 100
    return core_met == len(core) and score >= 90, score, core_met, edge_met


def evaluate_trend(rows: list[dict[str, Any]], auction_change: float | None) -> tuple[dict[str, Any] | None, str | None]:
    if len(rows) < 65:
        return None, "趋势股需要至少65个交易日日线"
    enriched: list[dict[str, Any]] = []
    for index in range(20, len(rows) + 1):
        values = trend_features(rows[:index])
        values["close"] = _finite(rows[index - 1].get("close"), 0.0) or 0.0
        values["volume"] = _finite(rows[index - 1].get("volume"), 0.0) or 0.0
        enriched.append(values)
    latest, previous = enriched[-1], enriched[-2]
    recent = enriched[-6:]
    cross_count = sum((item["ma5"] > item["ma20"]) != (recent[i - 1]["ma5"] > recent[i - 1]["ma20"]) for i, item in enumerate(recent) if i)
    closes = [_finite(row.get("close"), 0.0) or 0.0 for row in rows[-10:]]
    if cross_count >= 2 and 40 < latest["rsi_14"] < 60 and _pct(max(closes), min(closes)) * 100 < 4:
        return None, "识别为震仓"
    matches = []
    for combo in ("combo_3", "combo_4", "combo_2", "combo_1", "combo_5"):
        passed, score, core_met, edge_met = _combo_score(latest, previous, combo)
        if passed:
            matches.append((combo, score, core_met, edge_met))
    fallback = None
    if latest["ma5"] > latest["ma10"] > latest["ma20"] and latest["volume_ratio"] > 1.2:
        fallback = {
            "static_edge_met": sum((latest["volume_ma_up"], latest["close"] > latest["ma5"], latest["upper_shadow_ratio"] < .3, latest["macd"] > 0)),
        }
    # 09:05 must never consume today's auction.  Preserve the static combo
    # matches and let the 09:25:45 confirmation apply the opening thresholds.
    if auction_change is None:
        if not matches and fallback is None:
            return None, "没有满足静态组合条件"
        symbol = str(rows[-1]["symbol"])
        return {
            "symbol": symbol,
            "stock": symbol,
            "combo_matches": [
                {"combo_type": combo, "score": score, "core_met": core_met, "edge_met": edge_met}
                for combo, score, core_met, edge_met in matches
            ],
            "fallback": fallback,
        }, None
    matches = [
        item for item in matches
        if (3 <= auction_change <= 7 if item[0] == "combo_3" else auction_change > 1)
    ]
    selected = matches[0] if matches else None
    if selected is None and fallback is not None and auction_change > 1.5:
        edge_met = int(fallback["static_edge_met"]) + int(auction_change < 5)
        if edge_met >= 4:
            selected = ("combo_6", 90 + edge_met * 2, 3, edge_met)
    if selected is None:
        return None, "没有满足优先级组合或竞价条件"
    combo, score, core_met, edge_met = selected
    symbol = str(rows[-1]["symbol"])
    return {"symbol": symbol, "stock": symbol, "combo_type": combo, "combo_name": COMBO_CONFIG[combo]["name"], "score": score, "core_met": core_met, "edge_met": edge_met, "priority": COMBO_CONFIG[combo]["priority"], "target_adjust": COMBO_CONFIG[combo]["target_adjust"], "open_chg": auction_change}, None


def confirm_trend_candidate(candidate: dict[str, Any], auction_change: float | None) -> dict[str, Any] | None:
    """Apply the original opening-price gate to a static trend candidate."""
    if auction_change is None:
        return None
    for item in candidate.get("combo_matches") or []:
        combo = item.get("combo_type")
        if combo == "combo_3":
            if not 3 <= auction_change <= 7:
                continue
        elif auction_change <= 1:
            continue
        return {
            "symbol": str(candidate.get("symbol") or candidate.get("stock")),
            "stock": str(candidate.get("symbol") or candidate.get("stock")),
            "combo_type": combo,
            "combo_name": COMBO_CONFIG[combo]["name"],
            "score": item.get("score", 0),
            "core_met": item.get("core_met", 0),
            "edge_met": item.get("edge_met", 0),
            "priority": COMBO_CONFIG[combo]["priority"],
            "target_adjust": COMBO_CONFIG[combo]["target_adjust"],
            "open_chg": auction_change,
        }
    fallback = candidate.get("fallback") or {}
    if auction_change > 1.5:
        edge_met = int(fallback.get("static_edge_met", 0)) + int(auction_change < 5)
        if edge_met >= 4:
            combo = "combo_6"
            return {
                "symbol": str(candidate.get("symbol") or candidate.get("stock")),
                "stock": str(candidate.get("symbol") or candidate.get("stock")),
                "combo_type": combo,
                "combo_name": COMBO_CONFIG[combo]["name"],
                "score": 90 + edge_met * 2,
                "core_met": 3,
                "edge_met": edge_met,
                "priority": COMBO_CONFIG[combo]["priority"],
                "target_adjust": COMBO_CONFIG[combo]["target_adjust"],
                "open_chg": auction_change,
            }
    return None


class FourModeSnapshotCache:
    def __init__(self, repo: Any, start: date, end: date, requirement: dict[str, Any]) -> None:
        self.repo, self.start, self.end, self.requirement = repo, start, end, dict(requirement)
        self._snapshots: dict[date, dict[str, Any]] = {}
        self._auction_signatures: dict[date, tuple[Any, ...]] = {}
        self._all_symbols: set[str] = set()
        self._build()

    @property
    def all_symbols(self) -> list[str]:
        return sorted(self._all_symbols)

    @property
    def bootstrap_symbols(self) -> list[str]:
        return self.all_symbols

    def snapshot(self, trading_day: date) -> dict[str, Any]:
        # A paper account may be started before 09:25.  Rebuild once the
        # collector publishes the final auction manifest/partition so the
        # scheduled confirmation does not remain stuck on the startup view.
        if trading_day > self.end:
            self.end = trading_day
            self._build()
        elif trading_day in self._snapshots:
            signature = self._auction_signature(trading_day)
            if signature != self._auction_signatures.get(trading_day):
                self._build()
        return copy.deepcopy(self._snapshots.get(trading_day, {"date": trading_day.isoformat(), "state": "waiting_data", "data_gaps": ["没有构建该交易日快照"], "modes": {}, "candidates": []}))

    def _auction_signature(self, day: date) -> tuple[Any, ...]:
        data_dir = Path(self.repo.store.data_dir)
        manifest_path = (
            data_dir
            / "ext_data"
            / "_ingestion"
            / "kaipanla"
            / "auction_completion"
            / f"{day.isoformat()}.json"
        )
        try:
            manifest_stat = manifest_path.stat()
            manifest_signature: tuple[Any, ...] = (
                True,
                manifest_stat.st_mtime_ns,
                manifest_stat.st_size,
            )
        except OSError:
            manifest_signature = (False,)
        partition = data_dir / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
        files: list[tuple[str, int, int]] = []
        for path in sorted(partition.glob("*.parquet")):
            try:
                stat = path.stat()
            except OSError:
                continue
            files.append((path.name, stat.st_mtime_ns, stat.st_size))
        return (*manifest_signature, tuple(files))

    def _remember_auction_signatures(self) -> None:
        self._auction_signatures = {
            day: self._auction_signature(day) for day in self._trading_days()
        }

    def _members(self, day: date) -> tuple[set[str], str | None]:
        path = Path(self.repo.store.data_dir) / "pit_reference" / "history" / "index_membership_history" / "part.parquet"
        if not path.exists():
            return set(), "缺少PIT指数成分历史"
        frame = pl.read_parquet(path)
        if frame.is_empty() or not {"index_symbol", "member_symbol"}.issubset(frame.columns):
            return set(), "PIT指数成分表字段不完整"
        frame = frame.filter(pl.col("index_symbol") == self.requirement.get("index_symbol", "000852.SH"))
        if "effective_from" in frame.columns:
            effective_from = pl.col("effective_from").cast(pl.Date, strict=False)
            frame = frame.with_columns(effective_from.alias("effective_from")).filter(pl.col("effective_from") <= day)
            if "effective_to" in frame.columns:
                effective_to = pl.col("effective_to").cast(pl.Date, strict=False)
                frame = frame.with_columns(effective_to.alias("effective_to")).filter(pl.col("effective_to").is_null() | (pl.col("effective_to") > day))
        elif "snapshot_date" in frame.columns:
            frame = frame.with_columns(pl.col("snapshot_date").cast(pl.Date, strict=False)).filter(pl.col("snapshot_date") == day)
        if frame.is_empty():
            return set(), f"PIT成分缺少 {day.isoformat()} 快照"
        return set(frame["member_symbol"].cast(pl.String).to_list()), None

    def _auction(self, day: date) -> tuple[dict[str, dict[str, Any]], list[str], dict[str, Any]]:
        manifest = load_ingestion_manifest(self.repo.store.data_dir, "kaipanla", "auction_completion", day.isoformat())
        components = manifest.get("components") or {}
        four_mode_component = components.get("four_mode_bid_detail")
        if isinstance(four_mode_component, dict):
            base_components = ("0915", "0920", "0925", "bid_detail")
            terminal = {"published", "valid_empty", "complete", "not_applicable"}
            required = (*base_components, "four_mode_bid_detail")
            if any(
                not isinstance(components.get(name), dict)
                or components[name].get("status") not in terminal
                for name in required
            ):
                return {}, ["竞价 completion manifest 未完成"], manifest
            if all(
                components[name].get("status") in {"valid_empty", "not_applicable"}
                and int(components[name].get("rows") or 0) == 0
                for name in required
            ):
                return {}, [], manifest
        elif manifest.get("status") != "complete":
            return {}, ["竞价 completion manifest 未完成"], manifest
        # A valid-empty publication must win over any stale partition left by
        # an earlier non-empty run; never replay old auction rows as today's
        # final snapshot.
        if _auction_manifest_is_valid_empty(manifest):
            return {}, [], manifest
        root = Path(self.repo.store.data_dir) / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
        files = list(root.glob("*.parquet"))
        if not files:
            return {}, ["缺少竞价 parquet"], manifest
        try:
            frame = pl.concat([pl.read_parquet(path) for path in sorted(files)], how="diagonal_relaxed")
        except Exception:
            return {}, ["竞价 parquet 无法读取"], manifest
        if frame.is_empty() or "symbol" not in frame.columns:
            return {}, ["竞价字段不完整"], manifest
        result: dict[str, dict[str, Any]] = {}
        for row in frame.to_dicts():
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol:
                result[symbol] = row
        return result, [], manifest

    def _minute_rows(self, day: date, symbols: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not symbols:
            return {}
        get_range = getattr(self.repo, "get_minute_range", None)
        if not callable(get_range):
            return {}
        try:
            frame = get_range(symbols, day, day, "stock")
        except Exception:
            return {}
        if frame is None or frame.is_empty() or not {"symbol", "close", "volume"}.issubset(frame.columns):
            return {}
        time_column = "datetime" if "datetime" in frame.columns else "timestamp" if "timestamp" in frame.columns else None
        if time_column is None:
            return {}
        result: dict[str, list[dict[str, Any]]] = {}
        for row in frame.sort(["symbol", time_column]).to_dicts():
            result.setdefault(str(row["symbol"]), []).append(row)
        return result

    def _valuation(self, symbols: list[str], cutoff: date) -> dict[str, dict[str, Any]]:
        root = Path(self.repo.store.data_dir) / "valuation_daily"
        if not root.exists() or not symbols:
            return {}
        try:
            frame = pl.scan_parquet(str(root / "**" / "*.parquet"), missing_columns="insert")
            schema = frame.collect_schema()
            if "date" not in schema.names():
                return {}
            date_expr = pl.col("date").cast(pl.Date, strict=False)
            frame = frame.with_columns(date_expr.alias("date")).filter((pl.col("symbol").is_in(symbols)) & (pl.col("date") <= cutoff)).sort(["symbol", "date"]).group_by("symbol", maintain_order=True).tail(1).collect()
        except Exception:
            return {}
        return {str(row["symbol"]): row for row in frame.to_dicts()}

    def _build(self) -> None:
        instruments = self.repo.get_instruments_asset("stock")
        if instruments.is_empty() or "symbol" not in instruments.columns:
            raise ValueError("四合一缺少股票标的目录")
        # Polars' ``ends_with`` accepts one suffix per expression; passing a
        # tuple produces a list-typed literal and fails at runtime on current
        # Polars versions.  Keep the stock universe limited to Shanghai and
        # Shenzhen symbols while expressing the predicate explicitly.
        symbols = [
            str(item)
            for item in instruments.filter(
                pl.col("symbol").str.ends_with(".SH")
                | pl.col("symbol").str.ends_with(".SZ")
            )["symbol"].to_list()
        ]
        self._all_symbols.update(symbols)
        load_start = self.start - timedelta(days=max(180, int(self.requirement.get("lookback_days", 80)) * 3))
        columns = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "raw_open", "raw_high", "raw_low", "raw_close"]
        daily = self.repo.get_daily_asset_batch("stock", symbols, load_start, self.end, columns)
        if daily.is_empty():
            for day in self._trading_days():
                self._snapshots[day] = {"date": day.isoformat(), "state": "waiting_data", "data_gaps": ["缺少股票日线"], "modes": {}, "candidates": []}
            self._remember_auction_signatures()
            return
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in daily.sort(["symbol", "date"]).to_dicts():
            row["date"] = _as_day(row.get("date"))
            grouped.setdefault(str(row["symbol"]), []).append(row)
        for day in self._trading_days():
            members, member_gap = self._members(day)
            auction_rows, auction_gaps, manifest = self._auction(day)
            valuations = self._valuation(list(members or symbols), day - timedelta(days=1))
            static_modes: dict[str, dict[str, Any]] = {}
            modes: dict[str, dict[str, Any]] = {}
            candidates: list[dict[str, Any]] = []
            mode_gaps = list(auction_gaps)
            for mode in MODE_NAMES:
                static_modes[mode] = {"state": "ready", "candidates": [], "data_gaps": []}
            yje_symbols_by_day: dict[date, list[str]] = {}
            for symbol, history in grouped.items():
                visible_history = [row for row in history if row.get("date") and row["date"] < day]
                if len(visible_history) >= 2 and _is_limit(visible_history[-1], visible_history[-2]):
                    yje_symbols_by_day.setdefault(visible_history[-1]["date"], []).append(symbol)
            limit_up_count = sum(
                bool(visible := [row for row in history if row.get("date") and row["date"] < day])
                and len(visible) >= 2
                and _is_limit(visible[-1], visible[-2])
                for history in grouped.values()
            )
            minute_rows_by_key: dict[tuple[str, date], list[dict[str, Any]]] = {}
            minute_missing_symbols_by_day: dict[date, list[str]] = {}
            for minute_day, minute_symbols in yje_symbols_by_day.items():
                loaded = self._minute_rows(minute_day, minute_symbols)
                minute_rows_by_key.update(
                    {(symbol, minute_day): rows for symbol, rows in loaded.items()}
                )
                missing_symbols = sorted(set(minute_symbols) - set(loaded))
                if missing_symbols:
                    minute_missing_symbols_by_day[minute_day] = missing_symbols
            for symbol, history in grouped.items():
                visible = [row for row in history if row.get("date") and row["date"] < day]
                if not visible:
                    continue
                valuation = valuations.get(symbol)
                minute_rows = minute_rows_by_key.get((symbol, visible[-1]["date"]))
                yje_score, yje_detail, yje_reason = yje_static_score(visible, minute_rows=minute_rows)
                if yje_reason and "昨日分钟K" not in yje_reason:
                    pass
                elif not yje_reason:
                    static_modes["yje"]["candidates"].append({"symbol": symbol, "stock": symbol, "score": yje_score, "details": yje_detail, "mode": "yje"})
                rzq, _ = rzq_static_candidate(visible, valuation)
                if rzq:
                    rzq.update({"symbol": symbol, "score": rzq.get("score", 0), "mode": "rzq"})
                    static_modes["rzq"]["candidates"].append(rzq)
                sb, _ = sb_static_score(visible, valuation)
                if sb:
                    sb.update({"symbol": symbol, "mode": "sb"})
                    static_modes["sb"]["candidates"].append(sb)
                if symbol in members:
                    trend, _ = evaluate_trend(visible, None)
                    if trend:
                        trend.update({"symbol": symbol, "mode": "qs"})
                        static_modes["qs"]["candidates"].append(trend)
            # Only the immediately preceding completed board feeds this
            # snapshot's yje score.  Older historical gaps matter during a
            # deliberate multi-day catch-up, but must not block today's live
            # selection when yesterday is fully covered.
            relevant_minute_days = [
                minute_day for minute_day in yje_symbols_by_day
                if minute_day < day
            ]
            latest_minute_day = max(relevant_minute_days, default=None)
            missing_count = len(
                minute_missing_symbols_by_day.get(latest_minute_day, [])
                if latest_minute_day is not None else []
            )
            if missing_count:
                static_modes["yje"]["state"] = "waiting_data"
                static_modes["yje"]["data_gaps"] = [
                    f"缺少昨日分钟K（{missing_count}只）"
                ]
            for mode, payload in static_modes.items():
                payload["candidates"].sort(key=lambda row: (-float(row.get("score", 0)), str(row.get("symbol"))))
                modes[mode] = {
                    "state": payload.get("state", "ready"),
                    "candidates": [],
                    "data_gaps": list(payload.get("data_gaps") or []),
                }
                if self.requirement.get("require_auction") and auction_gaps:
                    modes[mode]["state"] = "waiting_data"
                    modes[mode]["data_gaps"] = list(auction_gaps)
            if not auction_gaps:
                # Apply only the rules that require today's 09:25:00 auction.
                if modes["yje"]["state"] == "ready":
                    modes["yje"]["candidates"] = copy.deepcopy(static_modes["yje"]["candidates"])
                if modes["sb"]["state"] == "ready":
                    modes["sb"]["candidates"] = copy.deepcopy(static_modes["sb"]["candidates"])
                for row in (static_modes["rzq"]["candidates"] if modes["rzq"]["state"] == "ready" else []):
                    change = _auction_change_fraction(auction_rows.get(str(row.get("symbol")), {}))
                    if change is not None and 0 < change <= .06:
                        confirmed = copy.deepcopy(row)
                        confirmed.update({"score": change * 100, "open_chg": change * 100})
                        modes["rzq"]["candidates"].append(confirmed)
                for row in (static_modes["qs"]["candidates"] if modes["qs"]["state"] == "ready" else []):
                    change = _auction_change_fraction(auction_rows.get(str(row.get("symbol")), {}))
                    confirmed = confirm_trend_candidate(row, change * 100 if change is not None else None)
                    if confirmed:
                        modes["qs"]["candidates"].append(confirmed)
            for payload in modes.values():
                payload["candidates"].sort(key=lambda row: (-float(row.get("score", 0)), str(row.get("symbol"))))
            if member_gap:
                modes["qs"]["state"] = "waiting_data"
                modes["qs"]["data_gaps"] = [member_gap]
            for mode, payload in modes.items():
                if payload["state"] == "ready":
                    candidates.extend(payload["candidates"])
            # When the live collector declares a four-mode /31 component, it
            # is an explicit readiness contract for the static weak-reversal
            # and trend candidates.  Do not silently fall back to an older
            # /115 row or a missing value for those modes.
            four_mode_bid_component = (manifest.get("components") or {}).get(
                "four_mode_bid_detail"
            )
            if isinstance(four_mode_bid_component, dict):
                target_symbols = {
                    str(row.get("symbol") or "").strip().upper()
                    for mode in ("rzq", "qs")
                    for row in static_modes[mode]["candidates"]
                    if row.get("symbol")
                }
                missing_direct = sorted(
                    symbol
                    for symbol in target_symbols
                    if (
                        not auction_rows.get(symbol)
                        or auction_rows[symbol].get("source_0925") != "/31"
                        or _finite(auction_rows[symbol].get("auction_change_pct_0925")) is None
                    )
                )
                component_status = four_mode_bid_component.get("status")
                if target_symbols and (component_status != "complete" or missing_direct):
                    count = len(missing_direct) or len(
                        four_mode_bid_component.get("failed_batches") or []
                    )
                    mode_gaps.append(f"缺少四合一 /31 竞价明细（{count}只）")
            snapshot_state = "waiting_data" if mode_gaps or member_gap else "ready"
            # Static candidates are intentionally retained separately.  The
            # strategy only publishes the auction-confirmed list at 09:25:45.
            static_candidates = [
                row for payload in static_modes.values()
                for row in payload["candidates"]
            ]
            self._snapshots[day] = {
                "date": day.isoformat(),
                "as_of": (day - timedelta(days=1)).isoformat(),
                "state": snapshot_state,
                "static_state": "ready" if not member_gap else "waiting_data",
                "data_gaps": [*mode_gaps, *([member_gap] if member_gap else [])],
                "mode_data_gaps": {
                    mode: list(payload.get("data_gaps") or [])
                    for mode, payload in modes.items()
                    if payload.get("data_gaps")
                },
                "auction": {
                    "state": (
                        "valid_empty"
                        if not auction_gaps and not auction_rows and _auction_manifest_is_valid_empty(manifest)
                        else "ready" if not auction_gaps else "waiting_data"
                    ),
                    "rows": auction_rows,
                    "manifest": manifest,
                },
                "modes": modes,
                "static_modes": static_modes,
                "static_candidates": static_candidates,
                "candidates": candidates,
                "limit_up_count": limit_up_count,
            }
        self._remember_auction_signatures()

    def _trading_days(self) -> list[date]:
        frame = self.repo.get_daily_asset("index", self.requirement.get("index_symbol", "000852.SH"), self.start, self.end, ["date"])
        dates = {
            day
            for value in (frame["date"].to_list() if frame is not None and "date" in frame.columns else [])
            if (day := _as_day(value)) is not None
        }
        # The current session's index daily bar is only published after the
        # close.  The live paper runner still needs today's snapshot before
        # the auction, so use the current Beijing date as the session marker;
        # historical windows remain driven by persisted index dates.
        if self.end == cn_today() and self.end.weekday() < 5:
            dates.add(self.end)
        return sorted(dates)


def four_mode_bid_symbols(
    repo: Any,
    trade_date: date,
    requirement: dict[str, Any] | None = None,
) -> list[str]:
    """Return only the static weak-reversal/trend symbols needing /31 data."""
    cache = FourModeSnapshotCache(
        repo,
        trade_date,
        trade_date,
        requirement
        or {
            "lookback_days": 80,
            "trend_history_days": 65,
            "index_symbol": "000852.SH",
            "require_auction": True,
        },
    )
    snapshot = cache.snapshot(trade_date)
    symbols = {
        str(row.get("symbol") or "").strip().upper()
        for mode in ("rzq", "qs")
        for row in (snapshot.get("static_modes", {}).get(mode, {}).get("candidates") or [])
        if row.get("symbol")
    }
    return sorted(symbols)


def configure_four_mode_snapshot(engine: Any, repo: Any, start: date, end: date) -> FourModeSnapshotCache | None:
    requirement = engine.four_mode_snapshot_requirement
    if requirement is None:
        return None
    cache = FourModeSnapshotCache(repo, start, end, requirement)
    engine.set_four_mode_snapshot_loader(cache.snapshot)
    return cache
