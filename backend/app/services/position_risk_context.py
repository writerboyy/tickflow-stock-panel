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


def correlation_window_snapshot(
    history: pl.DataFrame | None,
    symbol: str,
    members: set[str],
    leader_symbol: str | None,
    *,
    window: int = 20,
) -> dict[str, Any]:
    """返回最近窗口与前窗口相关性，供动态退出规则使用。"""
    required = {"symbol", "date", "change_pct"}
    if history is None or history.is_empty() or not required.issubset(history.columns):
        return {"sector_current": None, "sector_baseline": None, "sector_samples": 0, "leader_current": None, "leader_baseline": None, "leader_samples": 0}
    peers = sorted(member for member in members if member != symbol)
    if len(peers) < 4:
        return {"sector_current": None, "sector_baseline": None, "sector_samples": 0, "leader_current": None, "leader_baseline": None, "leader_samples": 0}
    base = history.select(["symbol", "date", "change_pct"]).filter(
        pl.col("change_pct").is_not_null() & pl.col("change_pct").is_finite(),
    )
    stock = base.filter(pl.col("symbol") == symbol).select(["date", pl.col("change_pct").alias("stock_return")])
    sector = base.filter(pl.col("symbol").is_in(peers)).group_by("date").agg(
        pl.col("change_pct").mean().alias("sector_return"),
        pl.len().alias("member_count"),
    ).filter(pl.col("member_count") >= 4)
    joined = stock.join(sector, on="date", how="inner").sort("date")

    def windows(frame: pl.DataFrame, right: str) -> tuple[float | None, float | None, int]:
        clean = frame.select(["stock_return", right]).drop_nulls().filter(
            pl.col("stock_return").is_finite() & pl.col(right).is_finite(),
        )
        if clean.height < window * 2:
            return None, None, clean.height
        previous = clean.slice(clean.height - window * 2, window)
        current = clean.tail(window)
        previous_value = _finite(previous.select(pl.corr("stock_return", right)).item())
        current_value = _finite(current.select(pl.corr("stock_return", right)).item())
        return current_value, previous_value, current.height

    sector_current, sector_baseline, sector_samples = windows(joined, "sector_return")
    leader_current = leader_baseline = None
    leader_samples = 0
    if leader_symbol and leader_symbol != symbol:
        leader = base.filter(pl.col("symbol") == leader_symbol).select(["date", pl.col("change_pct").alias("leader_return")])
        leader_current, leader_baseline, leader_samples = windows(
            stock.join(leader, on="date", how="inner").sort("date"), "leader_return",
        )
    return {
        "sector_current": sector_current,
        "sector_baseline": sector_baseline,
        "sector_samples": sector_samples,
        "leader_current": leader_current,
        "leader_baseline": leader_baseline,
        "leader_samples": leader_samples,
    }


class PositionRiskContextService:
    """编排持仓市场上下文；全市场和历史计算按日期/TTL 缓存。"""

    def __init__(self, repo: Any, quote_service: Any, app_state: Any) -> None:
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._lock = threading.RLock()
        self._overview_cache: tuple[float, str, dict[str, Any]] | None = None
        self._emotion_cache: tuple[float, list[float]] | None = None
        self._rotation_cache: dict[tuple[str, int | None, str], dict[str, Any]] = {}
        self._correlation_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._history_cache: tuple[str, pl.DataFrame | None] | None = None
        self._minute_proxy_symbols_cache: dict[tuple[str, str], tuple[str, ...]] = {}
        self._minute_proxy_feature_cache: dict[tuple[str, str], tuple[str, dict[str, dict[str, Any]]]] = {}
        self._minute_leader_feature_cache: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
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

    def _data_date(self, fallback: date | None = None) -> date | None:
        try:
            _frame, data_date = self.quote_service.get_enriched_today()
            if data_date is not None:
                return data_date
        except Exception:  # noqa: BLE001
            logger.debug("持仓风控行情截至日读取失败", exc_info=True)
        try:
            data_date = self.repo.enriched_latest_date()
            if data_date is not None:
                return data_date
        except Exception:  # noqa: BLE001
            logger.debug("持仓风控 enriched 截至日读取失败", exc_info=True)
        return fallback

    def _overview(self, as_of: date | None = None) -> dict[str, Any]:
        now_mono = time.monotonic()
        cache_key = as_of.isoformat() if as_of else "live"
        with self._lock:
            if (
                self._overview_cache
                and self._overview_cache[1] == cache_key
                and now_mono - self._overview_cache[0] < 60
            ):
                return self._overview_cache[2]
        try:
            value = build_market_overview(
                self.repo,
                self.quote_service,
                getattr(self.app_state, "depth_service", None),
                as_of=as_of,
            )
        except Exception:  # noqa: BLE001
            logger.warning("持仓风控大盘上下文计算失败", exc_info=True)
            value = {}
        with self._lock:
            self._overview_cache = (now_mono, cache_key, value)
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

    def _current_snapshots(
        self,
        targets: list[dict[str, Any]],
        now: datetime,
        data_date: date | None,
    ) -> dict[str, dict[str, Any]]:
        sector_service = getattr(self.app_state, "sector_monitor_service", None)
        if sector_service is None or not targets:
            return {}
        stock_df, stock_date = self.quote_service.get_enriched_today()
        index_df = self.quote_service.get_index_quotes()
        if data_date is None or stock_date != data_date or stock_df is None or stock_df.is_empty():
            return {}
        snapshot_at = now.timestamp()
        if data_date < now.date():
            snapshot_at = datetime.combine(data_date, clock_time(15, 0)).timestamp()
        try:
            return sector_service.build_snapshots(
                stock_df,
                index_df,
                targets,
                set(),
                now=snapshot_at,
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
        data_date: date,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[float], float | None]:
        ranked: list[tuple[float, dict[str, Any], dict[str, Any], list[float], float | None]] = []
        for target in candidates:
            snapshot = snapshots.get(str(target.get("key") or ""))
            if not snapshot or not snapshot.get("valid") or (_finite(snapshot.get("coverage_ratio")) or 0) < 0.8:
                continue
            kind = str(target.get("kind") or "")
            level = 2 if kind == "industry" else None
            rotation = self._rotation(kind, level, data_date)
            values, yesterday = _rotation_values(rotation, self._target_rotation_name(target), data_date)
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
        data_date = self._data_date(now.date())
        historical = bool(data_date and data_date < now.date())
        overview = self._overview(data_date if historical else None)
        overview_date = str(overview.get("as_of") or "")[:10]
        effective_date = data_date or (date.fromisoformat(overview_date) if overview_date else None)
        cache_date = (effective_date or now.date()).isoformat()
        with self._lock:
            self._minute_proxy_symbols_cache = {
                key: value for key, value in self._minute_proxy_symbols_cache.items()
                if key[0] == cache_date
            }
            self._minute_proxy_feature_cache = {
                key: value for key, value in self._minute_proxy_feature_cache.items()
                if key[0] == cache_date
            }
            self._minute_leader_feature_cache = {
                key: value for key, value in self._minute_leader_feature_cache.items()
                if key[0] == cache_date
            }
        overview_current = effective_date is not None and overview_date == effective_date.isoformat()
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
        snapshots = self._current_snapshots(list(target_map.values()), now, effective_date)
        try:
            stock_df, _ = self.quote_service.get_enriched_today()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            stock_df = pl.DataFrame()
        results: dict[str, dict[str, Any]] = {}
        leader_features: dict[str, dict[str, Any]] = {}
        minute_proxy_features: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            feature = features.get(symbol) or {}
            config = configs.get(symbol) or {}
            candidates = candidates_by_symbol.get(symbol) or {}
            target, sector, five_days, yesterday = self._select_target(
                candidates.get("concept", []), snapshots, effective_date or now.date(),
            )
            if target is None:
                target, sector, five_days, yesterday = self._select_target(
                    candidates.get("industry", []), snapshots, effective_date or now.date(),
                )
            leader = (sector or {}).get("leader") or {}
            correlations = self._correlations(
                symbol,
                target,
                str(leader.get("symbol") or "") or None,
                effective_date or now.date(),
            ) if target else {"sector": None, "leader": None, "samples": 0, "leader_samples": 0}
            window_correlations = correlation_window_snapshot(
                self._correlation_history(effective_date or now.date()),
                symbol,
                sector_service.member_symbols(str(target.get("key") or "")) if target and sector_service else set(),
                str(leader.get("symbol") or "") or None,
            ) if target and sector_service else {}
            dynamic_relative = {
                **dict(feature.get("dynamic_context") or {}),
                **{
                    key: feature.get(key)
                    for key in (
                        "sector_correlation_current", "sector_correlation_baseline",
                        "sector_correlation_samples", "leader_correlation_current",
                        "leader_correlation_baseline",
                    )
                    if feature.get(key) is not None
                },
            }
            leader_symbol = str(leader.get("symbol") or "").strip().upper()
            cache_date = (effective_date or now.date()).isoformat()
            stock_token = str(feature.get("latest_closed_5m_token") or "")
            if leader_symbol and leader_symbol != symbol and leader_symbol not in leader_features:
                leader_cache_key = (cache_date, leader_symbol)
                with self._lock:
                    cached_leader = self._minute_leader_feature_cache.get(leader_cache_key)
                if cached_leader and cached_leader[0] == stock_token:
                    leader_features[leader_symbol] = dict(cached_leader[1])
                else:
                    try:
                        fetched = self.quote_service.get_intraday_features(
                            {leader_symbol}, asset_type="stock", now=now,
                        )
                        leader_features[leader_symbol] = dict(fetched.get(leader_symbol) or {})
                    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                        leader_features[leader_symbol] = {}
                    with self._lock:
                        self._minute_leader_feature_cache[leader_cache_key] = (
                            stock_token, dict(leader_features[leader_symbol]),
                        )
            if leader_symbol and leader_symbol != symbol:
                stock_bars = feature.get("closed_bars_5m") or []
                leader_bars = leader_features.get(leader_symbol, {}).get("closed_bars_5m") or []
                leader_by_time = {
                    str(item.get("datetime")): _finite(item.get("close"))
                    for item in leader_bars
                }
                relative_returns: list[float] = []
                for previous, current in zip(stock_bars[-3:-1], stock_bars[-2:], strict=False):
                    previous_close = _finite(previous.get("close"))
                    current_close = _finite(current.get("close"))
                    previous_leader = leader_by_time.get(str(previous.get("datetime")))
                    current_leader = leader_by_time.get(str(current.get("datetime")))
                    if (
                        previous_close and current_close and previous_leader and current_leader
                        and previous_leader > 0 and current_leader > 0
                    ):
                        relative_returns.append(
                            (current_close / previous_close - 1)
                            - (current_leader / previous_leader - 1),
                        )
                if len(relative_returns) >= 2:
                    dynamic_relative["sector_relative_returns"] = relative_returns
                    dynamic_relative["minute_proxy_available"] = True
                    dynamic_relative["minute_proxy_kind"] = "leader"

            # Prefer the selected leader when its closed bars are available.  If it
            # is unavailable, use a deterministic top-five equal-weight member
            # proxy without adding those symbols to the realtime subscription pool.
            if not dynamic_relative.get("minute_proxy_available") and target and sector_service:
                target_key = str(target.get("key") or "")
                cache_key = ((effective_date or now.date()).isoformat(), target_key)
                with self._lock:
                    proxy_symbols = self._minute_proxy_symbols_cache.get(cache_key)
                if proxy_symbols is None:
                    members = sector_service.member_symbols(target_key) - {symbol}
                    sorted_members = tuple(sorted(members))
                    proxy_symbols = sorted_members
                    if "amount" in stock_df.columns and "symbol" in stock_df.columns:
                        ranked = (
                            stock_df.filter(pl.col("symbol").is_in(list(members)))
                            .select(["symbol", "amount"])
                            .drop_nulls()
                            .sort("amount", descending=True)
                            .get_column("symbol")
                            .to_list()
                        )
                        proxy_symbols = tuple(str(value).strip().upper() for value in ranked[:5] if str(value).strip())
                    if not proxy_symbols:
                        proxy_symbols = sorted_members[:5]
                    else:
                        proxy_symbols = proxy_symbols[:5]
                    with self._lock:
                        self._minute_proxy_symbols_cache = {
                            key_value: value
                            for key_value, value in self._minute_proxy_symbols_cache.items()
                            if key_value[0] == cache_key[0]
                        }
                        self._minute_proxy_symbols_cache[cache_key] = proxy_symbols
                if proxy_symbols:
                    proxy_cache_key = (cache_date, target_key)
                    with self._lock:
                        cached_proxy = self._minute_proxy_feature_cache.get(proxy_cache_key)
                    if cached_proxy and cached_proxy[0] == stock_token:
                        minute_proxy_features.update({key: dict(value) for key, value in cached_proxy[1].items()})
                    else:
                        missing = [item for item in proxy_symbols if item not in minute_proxy_features]
                        if missing:
                            try:
                                fetched = self.quote_service.get_intraday_features(
                                    set(missing), asset_type="stock", now=now,
                                )
                                minute_proxy_features.update({
                                    key: dict(value or {}) for key, value in fetched.items()
                                })
                            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                                pass
                        with self._lock:
                            self._minute_proxy_feature_cache[proxy_cache_key] = (
                                stock_token,
                                {key: dict(minute_proxy_features.get(key) or {}) for key in proxy_symbols},
                            )
                    stock_bars = feature.get("closed_bars_5m") or []
                    proxy_by_time = {
                        proxy: {
                            str(item.get("datetime")): _finite(item.get("close"))
                            for item in (minute_proxy_features.get(proxy, {}).get("closed_bars_5m") or [])
                        }
                        for proxy in proxy_symbols
                    }
                    relative_returns = []
                    for previous, current in zip(stock_bars[-3:-1], stock_bars[-2:], strict=False):
                        previous_close = _finite(previous.get("close"))
                        current_close = _finite(current.get("close"))
                        if not previous_close or not current_close:
                            continue
                        returns = []
                        for values in proxy_by_time.values():
                            previous_proxy = values.get(str(previous.get("datetime")))
                            current_proxy = values.get(str(current.get("datetime")))
                            if previous_proxy and current_proxy and previous_proxy > 0 and current_proxy > 0:
                                returns.append(current_proxy / previous_proxy - 1)
                        if returns:
                            relative_returns.append(
                                current_close / previous_close - 1 - sum(returns) / len(returns),
                            )
                    if len(relative_returns) >= 2:
                        dynamic_relative["sector_relative_returns"] = relative_returns
                        dynamic_relative["minute_proxy_available"] = True
                        dynamic_relative["minute_proxy_kind"] = "sector_members"
                        dynamic_relative["minute_proxy_symbols"] = list(proxy_symbols)
            sector_current = dynamic_relative.get("sector_correlation_current")
            if sector_current is None:
                sector_current = window_correlations.get("sector_current")
            sector_baseline = dynamic_relative.get("sector_correlation_baseline")
            if sector_baseline is None:
                sector_baseline = window_correlations.get("sector_baseline")
            sector_samples = dynamic_relative.get("sector_correlation_samples")
            if sector_samples is None:
                sector_samples = window_correlations.get("sector_samples")
            leader_current = dynamic_relative.get("leader_correlation_current")
            if leader_current is None:
                leader_current = window_correlations.get("leader_current")
            leader_baseline = dynamic_relative.get("leader_correlation_baseline")
            if leader_baseline is None:
                leader_baseline = window_correlations.get("leader_baseline")
            auction = dict(feature.get("auction") or {})
            with self._lock:
                captured = dict(self._auction_quotes.get(symbol) or {}) if self._auction_date == now.date().isoformat() else {}
            if not auction.get("available") and captured.get("available"):
                auction = captured
            opening = dict(feature.get("opening_five_minute") or {})
            min_flow_samples = max(1, int(_finite(config.get("min_flow_samples")) or 3))
            flow_samples = int(_finite(feature.get("flow_samples")) or 0)
            relative_volume = _finite(feature.get("relative_volume"))
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
                "data_as_of": effective_date.isoformat() if effective_date else None,
                "data_status": "historical" if historical else "current" if effective_date == now.date() else "unavailable",
                "data_reason": "当前非交易日，显示上个交易日数据" if historical else None,
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
                "sector_correlation_current": sector_current,
                "sector_correlation_baseline": sector_baseline,
                "sector_correlation_samples": int(sector_samples or 0),
                "leader_correlation_current": leader_current,
                "leader_correlation_baseline": leader_baseline,
                "sector_relative_returns": list(dynamic_relative.get("sector_relative_returns") or []),
                "minute_proxy_available": bool(dynamic_relative.get("minute_proxy_available")),
                "minute_proxy_kind": dynamic_relative.get("minute_proxy_kind"),
                "minute_proxy_symbols": list(dynamic_relative.get("minute_proxy_symbols") or []),
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
