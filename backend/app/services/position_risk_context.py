"""持仓风控的大盘、板块、集合竞价和开盘五分钟上下文。"""
from __future__ import annotations

import logging
import math
import threading
import time
from datetime import date, datetime, time as clock_time, timedelta
from typing import Any

import polars as pl

from app.services import regime_builder, rps_rotation
from app.services.market_overview_builder import build_market_overview

logger = logging.getLogger(__name__)

CONTEXT_STATES = {"supportive", "neutral", "weakening", "divergent", "unavailable"}


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def emotion_phase(scores: list[float]) -> str:
    """把已有市场情绪分及其近五日方向映射为确定性周期标签。"""
    values = [value for raw in scores if (value := _finite(raw)) is not None]
    if not values:
        return "数据不足"
    latest = values[-1]
    slope = (latest - values[0]) / max(1, len(values) - 1)
    if latest < 30:
        return "修复" if slope >= 2 else "冰点"
    if latest < 45:
        return "修复" if slope >= 1 else "退潮"
    if latest < 55:
        if slope >= 1:
            return "启动"
        return "退潮" if slope <= -1 else "分化"
    if latest < 70:
        return "发酵" if slope >= 0 else "分化"
    return "高潮" if slope >= 0 else "分化"


def _rotation_values(rotation: dict[str, Any], name: str, today: date) -> tuple[list[float], float | None]:
    values: list[float] = []
    yesterday = None
    for raw_date in rotation.get("dates") or []:
        try:
            current_date = date.fromisoformat(str(raw_date))
        except ValueError:
            continue
        if current_date > today:
            continue
        rows = rotation.get("columns", {}).get(str(raw_date)) or []
        value = next((_finite(item[1]) for item in rows if str(item[0]) == name), None)
        if value is None:
            continue
        if len(values) < 5:
            values.append(value)
        if yesterday is None and current_date < today:
            yesterday = value
    return values, yesterday


def _pearson(frame: pl.DataFrame, left: str, right: str) -> tuple[float | None, int]:
    clean = frame.select([left, right]).drop_nulls().filter(
        pl.col(left).is_finite() & pl.col(right).is_finite(),
    ).tail(20)
    if clean.height < 10:
        return None, clean.height
    value = clean.select(pl.corr(left, right).alias("value")).item()
    return _finite(value), clean.height


def correlation_snapshot(
    history: pl.DataFrame | None,
    symbol: str,
    members: set[str],
    leader_symbol: str | None,
) -> dict[str, Any]:
    required = {"symbol", "date", "change_pct"}
    if history is None or history.is_empty() or not required.issubset(history.columns):
        return {"sector": None, "leader": None, "samples": 0, "leader_samples": 0}
    peer_members = sorted(member for member in members if member != symbol)
    if len(peer_members) < 4:
        return {"sector": None, "leader": None, "samples": 0, "leader_samples": 0}
    base = history.select(["symbol", "date", "change_pct"]).filter(
        pl.col("change_pct").is_not_null() & pl.col("change_pct").is_finite(),
    )
    stock = base.filter(pl.col("symbol") == symbol).select(
        ["date", pl.col("change_pct").alias("stock_return")],
    )
    sector = base.filter(pl.col("symbol").is_in(peer_members)).group_by("date").agg(
        pl.col("change_pct").mean().alias("sector_return"),
        pl.len().alias("member_count"),
    ).filter(pl.col("member_count") >= 4)
    joined = stock.join(sector, on="date", how="inner").sort("date")
    sector_corr, samples = _pearson(joined, "stock_return", "sector_return")

    leader_corr = None
    leader_samples = 0
    if leader_symbol and leader_symbol != symbol:
        leader = base.filter(pl.col("symbol") == leader_symbol).select(
            ["date", pl.col("change_pct").alias("leader_return")],
        )
        leader_joined = stock.join(leader, on="date", how="inner").sort("date")
        leader_corr, leader_samples = _pearson(leader_joined, "stock_return", "leader_return")
    return {
        "sector": sector_corr,
        "leader": leader_corr,
        "samples": samples,
        "leader_samples": leader_samples,
    }


class PositionRiskContextService:
    """编排持仓市场上下文；全市场和历史计算按日期/TTL 缓存。"""

    def __init__(self, repo: Any, quote_service: Any, app_state: Any) -> None:
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._lock = threading.RLock()
        self._overview_cache: tuple[float, dict[str, Any]] | None = None
        self._emotion_cache: tuple[float, list[float]] | None = None
        self._rotation_cache: dict[tuple[str, int | None, str], dict[str, Any]] = {}
        self._correlation_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._history_cache: tuple[str, pl.DataFrame | None] | None = None
        self._auction_quotes: dict[str, dict[str, Any]] = {}
        self._auction_date = ""

    def capture_auction(self, quotes: list[dict[str, Any]], now: datetime) -> None:
        if now.weekday() >= 5 or not clock_time(9, 15) <= now.time() <= clock_time(9, 30):
            return
        day = now.date().isoformat()
        with self._lock:
            if self._auction_date != day:
                self._auction_quotes = {}
                self._auction_date = day
            for quote in quotes:
                symbol = str(quote.get("symbol") or "").strip().upper()
                price = _finite(quote.get("last_price", quote.get("close")))
                volume = _finite(quote.get("volume"))
                amount = _finite(quote.get("amount"))
                if symbol and price and price > 0 and ((volume or 0) > 0 or (amount or 0) > 0):
                    self._auction_quotes[symbol] = {
                        "available": True,
                        "as_of": now.isoformat(),
                        "price": price,
                        "volume": volume,
                        "amount": amount,
                    }

    def _overview(self) -> dict[str, Any]:
        now_mono = time.monotonic()
        with self._lock:
            if self._overview_cache and now_mono - self._overview_cache[0] < 60:
                return self._overview_cache[1]
        try:
            value = build_market_overview(
                self.repo,
                self.quote_service,
                getattr(self.app_state, "depth_service", None),
            )
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控大盘上下文计算失败", exc_info=True)
            value = {}
        with self._lock:
            self._overview_cache = (now_mono, value)
        return value

    def _rotation(self, kind: str, level: int | None, today: date) -> dict[str, Any]:
        key = (kind, level, today.isoformat())
        with self._lock:
            cached = self._rotation_cache.get(key)
            if cached is not None:
                return cached
        try:
            value = rps_rotation.build_rps_rotation(self.repo, 7, kind, level)
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控板块轮动计算失败: %s", kind, exc_info=True)
            value = {}
        with self._lock:
            self._rotation_cache = {
                cache_key: cache_value
                for cache_key, cache_value in self._rotation_cache.items()
                if cache_key[2] == today.isoformat()
            }
            self._rotation_cache[key] = value
        return value

    def _emotion_scores(self, overview: dict[str, Any]) -> list[float]:
        now_mono = time.monotonic()
        with self._lock:
            if self._emotion_cache and now_mono - self._emotion_cache[0] < 60:
                scores = list(self._emotion_cache[1])
                latest = _finite((overview.get("emotion") or {}).get("score"))
                if latest is not None:
                    scores = [*scores[:-1], latest] if scores else [latest]
                return scores[-5:]
        scores: list[float] = []
        try:
            history = regime_builder.load_regime_history(self.repo.store.data_dir)
            score_column = next(
                (column for column in ("emotion_score", "score", "composite_score") if column in history.columns),
                None,
            )
            if score_column:
                scores.extend(
                    float(value) for value in history.sort("date").tail(4)[score_column].to_list()
                    if _finite(value) is not None
                )
        except Exception:  # noqa: BLE001
            logger.debug("持仓风控情绪历史不可用", exc_info=True)
        latest = _finite((overview.get("emotion") or {}).get("score"))
        if latest is not None:
            scores.append(latest)
        result = scores[-5:]
        with self._lock:
            self._emotion_cache = (now_mono, result)
        return result

    def _current_snapshots(self, targets: list[dict[str, Any]], now: datetime) -> dict[str, dict[str, Any]]:
        sector_service = getattr(self.app_state, "sector_monitor_service", None)
        if sector_service is None or not targets:
            return {}
        stock_df, stock_date = self.quote_service.get_enriched_today()
        index_df = self.quote_service.get_index_quotes()
        if stock_date != now.date() or stock_df is None or stock_df.is_empty():
            return {}
        try:
            return sector_service.build_snapshots(
                stock_df,
                index_df,
                targets,
                set(),
                now=now.timestamp(),
            )
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控板块快照计算失败", exc_info=True)
            return {}

    @staticmethod
    def _target_rotation_name(target: dict[str, Any]) -> str:
        name = str(target.get("name") or target.get("value") or "")
        return name.split(" / ")[-1].strip()

    def _select_target(
        self,
        candidates: list[dict[str, Any]],
        snapshots: dict[str, dict[str, Any]],
        now: datetime,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[float], float | None]:
        ranked: list[tuple[float, dict[str, Any], dict[str, Any], list[float], float | None]] = []
        for target in candidates:
            snapshot = snapshots.get(str(target.get("key") or ""))
            if not snapshot or not snapshot.get("valid") or (_finite(snapshot.get("coverage_ratio")) or 0) < 0.8:
                continue
            kind = str(target.get("kind") or "")
            level = 2 if kind == "industry" else None
            rotation = self._rotation(kind, level, now.date())
            values, yesterday = _rotation_values(rotation, self._target_rotation_name(target), now.date())
            if len(values) < 5 or yesterday is None:
                continue
            ranked.append((sum(values) / len(values), target, snapshot, values, yesterday))
        if not ranked:
            return None, None, [], None
        _, target, snapshot, values, yesterday = max(ranked, key=lambda item: item[0])
        return target, snapshot, values, yesterday

    def _correlation_history(self, today: date) -> pl.DataFrame | None:
        cache_key = today.isoformat()
        with self._lock:
            if self._history_cache and self._history_cache[0] == cache_key:
                return self._history_cache[1]
        try:
            history = self.repo.get_enriched_range(
                today - timedelta(days=70),
                self.repo.enriched_latest_date() or today,
                columns=["symbol", "date", "change_pct"],
            )
        except Exception:  # noqa: BLE001
            history = None
        with self._lock:
            self._history_cache = (cache_key, history)
        return history

    def _correlations(
        self,
        symbol: str,
        target: dict[str, Any],
        leader_symbol: str | None,
        today: date,
    ) -> dict[str, Any]:
        sector_service = getattr(self.app_state, "sector_monitor_service", None)
        if sector_service is None:
            return {"sector": None, "leader": None, "samples": 0, "leader_samples": 0}
        key = (today.isoformat(), symbol, str(target.get("key") or ""))
        with self._lock:
            cached = self._correlation_cache.get(key)
            if cached is not None:
                return cached
        members = sector_service.member_symbols(str(target.get("key") or ""))
        history = self._correlation_history(today)
        value = correlation_snapshot(history, symbol, members, leader_symbol)
        with self._lock:
            self._correlation_cache = {
                cache_key: cache_value
                for cache_key, cache_value in self._correlation_cache.items()
                if cache_key[0] == today.isoformat()
            }
            self._correlation_cache[key] = value
        return value

    def build(
        self,
        symbols: set[str],
        features: dict[str, dict[str, Any]],
        quotes: dict[str, dict[str, Any]],
        configs: dict[str, dict[str, Any]],
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        overview = self._overview()
        overview_current = str(overview.get("as_of") or "") == now.date().isoformat()
        breadth = overview.get("breadth") or {}
        market_available = bool(
            overview_current
            and int(_finite(breadth.get("total")) or 0) > 0
            and overview.get("indices")
        )
        phase = emotion_phase(self._emotion_scores(overview))
        market_label = str((overview.get("emotion") or {}).get("label") or "数据不足")
        sector_service = getattr(self.app_state, "sector_monitor_service", None)
        candidates_by_symbol: dict[str, dict[str, list[dict[str, Any]]]] = {}
        target_map: dict[str, dict[str, Any]] = {}
        if sector_service is not None:
            for symbol in symbols:
                concepts = sector_service.targets_for_symbol(symbol, kind="concept")
                industries = sector_service.targets_for_symbol(
                    symbol, kind="industry", industry_level=2,
                )
                candidates_by_symbol[symbol] = {
                    "concept": concepts,
                    "industry": industries,
                }
                for target in [*concepts, *industries]:
                    target_map[str(target.get("key") or "")] = target
        snapshots = self._current_snapshots(list(target_map.values()), now)
        results: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            feature = features.get(symbol) or {}
            config = configs.get(symbol) or {}
            candidates = candidates_by_symbol.get(symbol) or {}
            target, sector, five_days, yesterday = self._select_target(
                candidates.get("concept", []), snapshots, now,
            )
            if target is None:
                target, sector, five_days, yesterday = self._select_target(
                    candidates.get("industry", []), snapshots, now,
                )
            leader = (sector or {}).get("leader") or {}
            correlations = self._correlations(
                symbol,
                target,
                str(leader.get("symbol") or "") or None,
                now.date(),
            ) if target else {"sector": None, "leader": None, "samples": 0, "leader_samples": 0}
            auction = dict(feature.get("auction") or {})
            with self._lock:
                captured = dict(self._auction_quotes.get(symbol) or {}) if self._auction_date == now.date().isoformat() else {}
            if not auction.get("available") and captured.get("available"):
                auction = captured
            opening = dict(feature.get("opening_five_minute") or {})
            min_flow_samples = max(1, int(_finite(config.get("min_flow_samples")) or 3))
            flow_samples = int(_finite(feature.get("flow_samples")) or 0)
            relative_volume = _finite(feature.get("relative_volume"))
            leader_symbol = str(leader.get("symbol") or "")
            sector_correlation_ok = (
                correlations.get("sector") is not None
                and int(correlations.get("samples") or 0) >= 10
            )
            leader_correlation_ok = (
                bool(leader_symbol)
                and (
                    leader_symbol == symbol
                    or (
                        correlations.get("leader") is not None
                        and int(correlations.get("leader_samples") or 0) >= 10
                    )
                )
            )
            required = {
                "market": market_available,
                "sector": bool(target and sector and five_days and yesterday is not None),
                "leader": bool(leader_symbol),
                "correlation": sector_correlation_ok and leader_correlation_ok,
                "auction": bool(auction.get("available")),
                "opening_five_minute": bool(opening.get("available")),
                "opening_volume": relative_volume is not None and (_finite(opening.get("volume")) or 0) > 0,
                "fund_flow": flow_samples >= min_flow_samples and (
                    _finite(feature.get("buy_ratio")) is not None
                    or _finite(feature.get("sell_ratio")) is not None
                ),
            }
            missing = [key for key, available in required.items() if not available]
            state = "unavailable"
            if not missing:
                sector_change = _finite((sector or {}).get("change_pct")) or 0.0
                quote_change = _finite((quotes.get(symbol) or {}).get("change_pct"))
                if quote_change is None:
                    last_price = _finite((quotes.get(symbol) or {}).get("last_price"))
                    previous_close = _finite((quotes.get(symbol) or {}).get("prev_close"))
                    quote_change = last_price / previous_close - 1 if last_price and previous_close else 0.0
                underperformance = quote_change - sector_change
                min_corr = _finite(config.get("min_correlation"))
                min_corr = min_corr if min_corr is not None else 0.50
                weak_sector = _finite(config.get("sector_weakening"))
                weak_sector = weak_sector if weak_sector is not None else -0.005
                weak_relative = _finite(config.get("underperform_threshold"))
                weak_relative = weak_relative if weak_relative is not None else -0.01
                sell_ratio = _finite(feature.get("sell_ratio")) or 0.0
                buy_ratio = _finite(feature.get("buy_ratio")) or 0.0
                if float(correlations["sector"]) < min_corr or underperformance <= weak_relative:
                    state = "divergent"
                elif sector_change <= weak_sector or sell_ratio >= 0.60 or phase in {"退潮", "冰点"}:
                    state = "weakening"
                elif sector_change >= 0 and buy_ratio >= 0.55 and phase in {"启动", "发酵", "高潮", "修复"}:
                    state = "supportive"
                else:
                    state = "neutral"
            results[symbol] = {
                "state": state,
                "gate_open": state in {"supportive", "neutral"},
                "missing": missing,
                "as_of": now.isoformat(),
                "market_state": market_label,
                "emotion_phase": phase,
                "sector_kind": target.get("kind") if target else None,
                "sector_name": str(target.get("name") or self._target_rotation_name(target)) if target else None,
                "sector_change_pct": _finite((sector or {}).get("change_pct")),
                "sector_five_day_change_pct": sum(five_days) if five_days else None,
                "sector_yesterday_change_pct": yesterday,
                "sector_coverage_ratio": _finite((sector or {}).get("coverage_ratio")),
                "leader": leader or None,
                "sector_correlation": correlations.get("sector"),
                "leader_correlation": correlations.get("leader"),
                "correlation_samples": int(correlations.get("samples") or 0),
                "leader_correlation_samples": int(correlations.get("leader_samples") or 0),
                "auction": auction or {"available": False},
                "opening_five_minute": {
                    **opening,
                    "relative_volume": relative_volume,
                    "buy_ratio": _finite(feature.get("buy_ratio")),
                    "sell_ratio": _finite(feature.get("sell_ratio")),
                    "flow_samples": flow_samples,
                },
            }
        return results
