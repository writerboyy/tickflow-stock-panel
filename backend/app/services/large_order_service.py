"""实时大单候选与开盘啦深挖服务。

热路径只消费 QuoteService 已经缓存的快照；开盘啦请求在独立线程中按预算执行。
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any

from app.market_time import CN_TZ, cn_now, cn_today
from app.plugins.kaipanla.client import KaipanlaClient, KaipanlaRequestError
from app.plugins.kaipanla.credentials import load_credentials
from app.plugins.kaipanla.parsers import ResponseShapeError, parse_large_order_intents, parse_large_order_trades

logger = logging.getLogger(__name__)

WINDOWS = (15, 60, 300)
DEFAULTS: dict[str, Any] = {
    "enabled": True,
    "score_threshold": 75,
    "cooldown_seconds": 120,
    "deep_dive_interval_seconds": 60,
    "max_deep_dive_symbols": 3,
    "candidate_limit": 50,
    "daily_call_budget": 60,
    "version": "large_orders_v1",
}


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _robust_z(value: float, values: list[float]) -> float:
    if len(values) < 5:
        return 0.0
    median = _median(values)
    mad = _median([abs(item - median) for item in values])
    scale = 1.4826 * mad
    if scale <= 1e-9:
        return 0.0
    return (value - median) / scale


def _as_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)
    if isinstance(value, (int, float)):
        try:
            seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=CN_TZ)
        except (OverflowError, OSError, ValueError):
            return None
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(CN_TZ) if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ)
        except ValueError:
            return None
    return None


class LargeOrderService:
    """有界的全市场候选扫描 + 候选股 L2 深挖。"""

    def __init__(self, quote_service=None) -> None:
        self._quote_service = quote_service
        self._app_state = None
        self._lock = threading.RLock()
        self._running = False
        self._pending_snapshot: list[dict] | None = None
        self._snapshot_running = False
        self._snapshot_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="large-orders")
        self._deep_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="large-orders-kaipanla")
        self._states: dict[str, dict[str, Any]] = {}
        self._ranking: list[dict] = []
        self._raw_archive: deque[dict] = deque(maxlen=500)
        self._deep_pending: set[str] = set()
        self._last_deep_at: dict[str, float] = {}
        self._deep_calls_date = cn_today()
        self._deep_calls_used = 0
        self._cooldown_until: dict[str, float] = {}
        self._last_update_ms: int | None = None
        self._last_error: str | None = None
        self._trade_date = cn_today()
        self._config = dict(DEFAULTS)

    def set_app_state(self, app_state) -> None:
        self._app_state = app_state

    def start(self) -> None:
        if self._running:
            return
        from app.services import preferences

        self._config.update(preferences.get_large_orders_preferences())
        self._running = True
        if self._quote_service is not None:
            self._quote_service.add_fetch_listener(self._on_quote_fetch)
        logger.info("实时大单服务已启动 (enabled=%s)", self._config["enabled"])

    def stop(self) -> None:
        self._running = False
        if self._quote_service is not None:
            try:
                self._quote_service.remove_fetch_listener(self._on_quote_fetch)
            except (KeyError, ValueError):
                pass
        with self._lock:
            self._pending_snapshot = None
            self._deep_pending.clear()
        self._snapshot_executor.shutdown(wait=False, cancel_futures=True)
        self._deep_executor.shutdown(wait=False, cancel_futures=True)

    def update_preferences(self, updates: dict[str, Any]) -> dict:
        from app.services import preferences

        current = preferences.set_large_orders_preferences(updates)
        with self._lock:
            self._config.update(current)
        return current

    def _on_quote_fetch(self) -> None:
        """行情线程只复制缓存并投递最新任务，不执行开盘啦请求。"""
        if not self._running or not self._config.get("enabled", True) or self._quote_service is None:
            return
        snapshot = self._quote_service.get_latest_quotes()
        with self._lock:
            self._pending_snapshot = snapshot
            if self._snapshot_running:
                return
            self._snapshot_running = True
        self._snapshot_executor.submit(self._drain_snapshots)

    def _drain_snapshots(self) -> None:
        while self._running:
            with self._lock:
                snapshot = self._pending_snapshot
                self._pending_snapshot = None
            if snapshot is None:
                with self._lock:
                    self._snapshot_running = False
                return
            try:
                self._process_snapshot(snapshot)
            except Exception:  # noqa: BLE001
                logger.exception("实时大单快照处理失败")

    def _reset_for_new_day(self) -> None:
        with self._lock:
            self._states.clear()
            self._ranking = []
            self._raw_archive.clear()
            self._last_deep_at.clear()
            self._cooldown_until.clear()
            self._deep_calls_date = self._trade_date
            self._deep_calls_used = 0

    def _process_snapshot(self, records: list[dict]) -> None:
        today = cn_today()
        if today != self._trade_date:
            self._trade_date = today
            self._reset_for_new_day()
        if not records:
            return
        now = cn_now()
        index_symbols: set[str] = set()
        repo = getattr(self._app_state, "repo", None) if self._app_state else None
        if repo is not None:
            try:
                index_symbols = set(repo.get_index_symbol_set())
            except Exception:  # noqa: BLE001
                index_symbols = set()

        with self._lock:
            for raw in records:
                symbol = str(raw.get("symbol") or "").strip().upper()
                if not symbol or symbol in index_symbols:
                    continue
                price = _finite(raw.get("last_price", raw.get("close")))
                amount = _finite(raw.get("amount"))
                volume = _finite(raw.get("volume"))
                if price is None or amount is None or volume is None or amount < 0 or volume < 0:
                    continue
                state = self._states.setdefault(
                    symbol,
                    {
                        "symbol": symbol,
                        "name": raw.get("name") or symbol,
                        "snapshots": deque(maxlen=360),
                        "flows": deque(maxlen=720),
                        "baseline": deque(maxlen=120),
                        "trade_events": deque(maxlen=300),
                        "intent_events": deque(maxlen=300),
                        "trade_ids": set(),
                        "intent_ids": set(),
                        "last_side": 0,
                        "deep_source": "proxy_only",
                        "deep_error": None,
                        "last_deep_ms": None,
                    },
                )
                state["name"] = raw.get("name") or state["name"]
                previous = state["snapshots"][-1] if state["snapshots"] else None
                state["snapshots"].append({"ts": now.timestamp(), "price": price, "amount": amount, "volume": volume})
                if previous is None:
                    continue
                delta_amount = amount - previous["amount"]
                delta_volume = volume - previous["volume"]
                if delta_amount < 0 or delta_volume < 0:
                    # 交易日切换/上游重置，绝不能把重置量当作资金脉冲。
                    state["flows"].clear()
                    state["baseline"].clear()
                    continue
                if delta_amount <= 0 and delta_volume <= 0:
                    continue
                price_delta = price - previous["price"]
                if price_delta > 0:
                    side = 1
                elif price_delta < 0:
                    side = -1
                else:
                    side = state["last_side"] or 1
                state["last_side"] = side
                state["flows"].append({
                    "ts": now.timestamp(),
                    "amount": delta_amount,
                    "volume": delta_volume,
                    "buy": delta_amount if side > 0 else 0.0,
                    "sell": delta_amount if side < 0 else 0.0,
                    "price": price,
                })
                state["baseline"].append(delta_amount)

            self._ranking = self._build_ranking_locked(now.timestamp())
            self._last_update_ms = int(time.time() * 1000)
        self._schedule_deep_dive()
        if self._quote_service is not None:
            self._quote_service.notify_large_orders_updated()

    def _window_metrics_locked(self, state: dict[str, Any], window: int) -> dict[str, float]:
        cutoff = time.time() - window
        flows = [item for item in state["flows"] if item["ts"] >= cutoff]
        buy = sum(item["buy"] for item in flows)
        sell = sum(item["sell"] for item in flows)
        amount = buy + sell
        deep_cutoff = time.time() - window
        trades = [item for item in state["trade_events"] if (_as_datetime(item.get("event_time")) or cn_now()).timestamp() >= deep_cutoff]
        active_buy = sum(float(item.get("amount") or 0) for item in trades if item.get("direction") == "active_buy")
        active_sell = sum(float(item.get("amount") or 0) for item in trades if item.get("direction") == "active_sell")
        if active_buy + active_sell > 0:
            buy, sell = active_buy, active_sell
        baseline = [float(item) for item in state["baseline"] if _finite(item) is not None]
        threshold = max(1_000_000.0, 3.0 * _median([abs(item - _median(baseline)) for item in baseline]))
        zscore = _robust_z(amount, baseline)
        return {
            "amount": amount,
            "buy": buy,
            "sell": sell,
            "net": buy - sell,
            "buy_ratio": buy / amount if amount > 0 else 0.0,
            "zscore": zscore,
            "threshold": threshold,
            "max_order": max((float(item.get("amount") or 0) for item in trades), default=0.0),
        }

    def _build_ranking_locked(self, now_ts: float) -> list[dict]:
        rows: list[dict] = []
        depth_metrics: dict[str, dict[str, float]] = {}
        depth_service = getattr(self._app_state, "depth_service", None) if self._app_state else None
        if depth_service is not None:
            try:
                depth_metrics = depth_service.get_cached_metrics(set(self._states))
            except Exception:  # noqa: BLE001
                depth_metrics = {}
        for symbol, state in self._states.items():
            metrics = self._window_metrics_locked(state, 60)
            latest = state["snapshots"][-1] if state["snapshots"] else {}
            if metrics["amount"] <= 0 and not state["trade_events"]:
                continue
            first = state["snapshots"][0] if state["snapshots"] else latest
            price = float(latest.get("price") or 0)
            prev_price = float(first.get("price") or price)
            change_pct = (price / prev_price - 1.0) if prev_price else 0.0
            price_confirmed = change_pct > 0 or metrics["buy_ratio"] >= 0.65
            book = depth_metrics.get(symbol, {})
            imbalance = float(book.get("book_imbalance") or 0)
            score = (
                min(1.0, max(0.0, metrics["net"] / max(metrics["threshold"] * 3.0, 1.0))) * 35.0
                + min(1.0, max(0.0, metrics["zscore"] / 5.0)) * 25.0
                + min(1.0, max(0.0, (metrics["buy_ratio"] - 0.5) / 0.3)) * 15.0
                + (15.0 if price_confirmed else 0.0)
                + min(1.0, max(0.0, imbalance)) * 10.0
            )
            deep = bool(state["trade_events"])
            confidence = "high" if deep and score >= 75 and metrics["buy_ratio"] >= 0.65 else "medium" if metrics["amount"] else "low"
            source = "kaipanla" if deep else "tick_proxy"
            rows.append({
                "symbol": symbol,
                "name": state["name"],
                "score": round(min(100.0, score), 2),
                "confidence": confidence,
                "source": source,
                "data_quality": "precise" if deep else "proxy_only",
                "active_buy_amount": round(metrics["buy"], 2),
                "active_sell_amount": round(metrics["sell"], 2),
                "net_buy_amount": round(metrics["net"], 2),
                "buy_ratio": round(metrics["buy_ratio"], 4),
                "max_order_amount": round(metrics["max_order"], 2),
                "cancel_rate": self._cancel_rate_locked(state),
                "change_pct": round(change_pct, 6),
                "last_seen_ts": round(float(latest["ts"]), 3) if latest.get("ts") is not None else None,
                "freshness_ms": max(0, int((now_ts - float(latest.get("ts") or now_ts)) * 1000)),
                "large_threshold": round(metrics["threshold"], 2),
                "zscore": round(metrics["zscore"], 3),
                "ofi": round(float(book.get("ofi") or 0), 2),
                "book_imbalance": round(imbalance, 4),
                "windows": {str(window): self._window_metrics_locked(state, window) for window in WINDOWS},
                "explanation": self._explanation(metrics, deep, price_confirmed),
            })
        rows.sort(key=lambda row: (row["score"], row["net_buy_amount"]), reverse=True)
        return rows[: int(self._config.get("candidate_limit", 50))]

    @staticmethod
    def _explanation(metrics: dict[str, float], deep: bool, price_confirmed: bool) -> str:
        parts = ["主动成交" if deep else "快照方向代理", "净买额为正" if metrics["net"] > 0 else "净买额偏弱"]
        if metrics["zscore"] >= 2.5:
            parts.append("成交额突增")
        if price_confirmed:
            parts.append("价格确认")
        return "、".join(parts)

    @staticmethod
    def _cancel_rate_locked(state: dict[str, Any]) -> float:
        intents = list(state["intent_events"])
        if not intents:
            return 0.0
        return round(sum(bool(item.get("cancel_flag")) for item in intents) / len(intents), 4)

    def _schedule_deep_dive(self) -> None:
        if not self._config.get("enabled", True) or load_credentials() is None:
            return
        now = time.time()
        with self._lock:
            if self._deep_calls_date != cn_today():
                self._deep_calls_date = cn_today()
                self._deep_calls_used = 0
            ranked = self._ranking[: int(self._config.get("candidate_limit", 50))]
            watchlist: list[str] = []
            try:
                from app.services import preferences

                watchlist = preferences.get_realtime_watchlist_symbols()
            except Exception:  # noqa: BLE001
                pass
            symbols = list(dict.fromkeys(watchlist + [str(row["symbol"]) for row in ranked]))
            limit = max(0, int(self._config.get("max_deep_dive_symbols", 3)))
            budget = max(0, int(self._config.get("daily_call_budget", 60)))
            available_symbols = max(0, (budget - self._deep_calls_used) // 2)
            limit = min(limit, available_symbols)
            interval = max(15.0, float(self._config.get("deep_dive_interval_seconds", 60)))
            for symbol in symbols:
                if len(self._deep_pending) >= limit or symbol in self._deep_pending:
                    break
                if now - self._last_deep_at.get(symbol, 0.0) < interval:
                    continue
                self._deep_pending.add(symbol)
                self._last_deep_at[symbol] = now
                self._deep_calls_used += 2
                self._deep_executor.submit(self._deep_dive, symbol)

    def _deep_dive(self, symbol: str) -> None:
        try:
            asyncio.run(self._deep_dive_async(symbol))
        finally:
            with self._lock:
                self._deep_pending.discard(symbol)

    async def _deep_dive_async(self, symbol: str) -> None:
        credentials = load_credentials()
        if credentials is None:
            return
        try:
            async with KaipanlaClient(credentials=credentials, attempts=1) as client:
                trade_payload = await client.request(13, {"StockID": symbol})
                intent_payload = await client.request(14, {"StockID": symbol})
            trades = parse_large_order_trades(trade_payload, symbol)
            intents = parse_large_order_intents(intent_payload, symbol)
        except (KaipanlaRequestError, ResponseShapeError, ValueError) as exc:
            with self._lock:
                state = self._states.get(symbol)
                if state:
                    state["deep_error"] = str(exc)
                    state["deep_source"] = "proxy_only"
            self._last_error = "开盘啦深挖暂不可用"
            return
        now_ms = int(time.time() * 1000)
        with self._lock:
            state = self._states.setdefault(symbol, {
                "symbol": symbol, "name": symbol, "snapshots": deque(maxlen=360), "flows": deque(maxlen=720),
                "baseline": deque(maxlen=120), "trade_events": deque(maxlen=300), "intent_events": deque(maxlen=300),
                "trade_ids": set(), "intent_ids": set(), "last_side": 0, "deep_source": "proxy_only", "deep_error": None,
                "last_deep_ms": None,
            })
            for event in trades:
                if event["event_id"] not in state["trade_ids"]:
                    state["trade_ids"].add(event["event_id"])
                    state["trade_events"].append({**event, "event_time": event.get("time")})
            for event in intents:
                if event["event_id"] not in state["intent_ids"]:
                    state["intent_ids"].add(event["event_id"])
                    state["intent_events"].append(event)
            state["trade_ids"] = {item["event_id"] for item in state["trade_events"]}
            state["intent_ids"] = {item["event_id"] for item in state["intent_events"]}
            state["deep_source"] = "kaipanla"
            state["deep_error"] = None
            state["last_deep_ms"] = now_ms
            self._raw_archive.append({"symbol": symbol, "endpoint": 13, "received_at": now_ms, "payload": trade_payload})
            self._raw_archive.append({"symbol": symbol, "endpoint": 14, "received_at": now_ms, "payload": intent_payload})
            self._ranking = self._build_ranking_locked(time.time())
            alerts = self._build_alerts_locked(symbol)
            self._last_update_ms = now_ms
        if self._quote_service is not None:
            self._quote_service.notify_large_orders_updated()
            if alerts:
                self._quote_service.push_alerts(alerts)
                self._persist_alerts(alerts)

    def _build_alerts_locked(self, symbol: str) -> list[dict]:
        if self._quote_service is not None:
            quote_status = self._quote_service.status()
            quote_age = quote_status.get("quote_age_ms")
            interval = float(quote_status.get("interval_s") or 6)
            if quote_age is None or quote_age < 0 or quote_age > max(interval * 2, 30) * 1000:
                return []
        row = next((item for item in self._ranking if item["symbol"] == symbol), None)
        if row is None or row["source"] != "kaipanla" or row["score"] < float(self._config["score_threshold"]):
            return []
        metrics = row["windows"]["60"]
        now = time.time()
        if metrics["net"] <= metrics["threshold"] or metrics["buy_ratio"] < 0.65 or metrics["zscore"] < 2.5:
            return []
        if now < self._cooldown_until.get(symbol, 0.0):
            return []
        self._cooldown_until[symbol] = now + float(self._config["cooldown_seconds"])
        return [{
            "ts": int(now * 1000),
            "source": "large_order",
            "type": "large_order_buy",
            "rule_id": self._config["version"],
            "rule_name": "实时大单",
            "symbol": symbol,
            "name": row["name"],
            "message": f"{row['name']} 主力买入候选：60秒主动净买额 {row['net_buy_amount']:,.0f} 元，评分 {row['score']:.0f}",
            "price": None,
            "change_pct": row["change_pct"],
            "signals": [row["explanation"]],
            "severity": "warn",
            "conditions": ["active_buy_ratio>=65%", "zscore>=2.5", "price_confirmed"],
            "logic": "and",
        }]

    def _persist_alerts(self, alerts: list[dict]) -> None:
        try:
            if self._app_state and getattr(self._app_state, "repo", None):
                from app.services import alert_store

                alert_store.append_many(self._app_state.repo.store.data_dir, alerts)
        except Exception:  # noqa: BLE001
            logger.debug("大单告警归档失败", exc_info=True)

    def status(self) -> dict:
        quote_status = self._quote_service.status() if self._quote_service is not None else {}
        quote_age = quote_status.get("quote_age_ms")
        interval = float(quote_status.get("interval_s") or 6)
        stale = quote_age is None or quote_age < 0 or quote_age > max(interval * 2, 30) * 1000
        with self._lock:
            precise = sum(1 for row in self._ranking if row.get("source") == "kaipanla")
            return {
                "enabled": bool(self._config.get("enabled", True)),
                "running": self._running,
                "data_source": "kaipanla" if load_credentials() else "proxy_only",
                "mode": "stale" if stale else "live",
                "stale": stale,
                "coverage_count": quote_status.get("symbol_count", 0),
                "candidate_count": len(self._ranking),
                "precise_count": precise,
                "last_updated_ms": self._last_update_ms,
                "last_error": self._last_error,
                "market_phase": quote_status.get("market_phase"),
                "is_trading_hours": quote_status.get("is_trading_hours", False),
                "config_version": self._config["version"],
                "deep_dive_budget": int(self._config.get("max_deep_dive_symbols", 3)),
                "deep_dive_calls_used": self._deep_calls_used,
                "deep_dive_calls_remaining": max(0, int(self._config.get("daily_call_budget", 60)) - self._deep_calls_used),
            }

    def ranking(self, window: int = 60, scope: str = "all") -> dict:
        if window not in WINDOWS:
            window = 60
        with self._lock:
            rows = self._build_ranking_locked(time.time())
            if scope == "watchlist":
                try:
                    from app.services import preferences

                    symbols = set(preferences.get_realtime_watchlist_symbols())
                    rows = [row for row in rows if row["symbol"] in symbols]
                except Exception:  # noqa: BLE001
                    rows = []
            for row in rows:
                metrics = row["windows"][str(window)]
                row.update({
                    "active_buy_amount": round(metrics["buy"], 2),
                    "active_sell_amount": round(metrics["sell"], 2),
                    "net_buy_amount": round(metrics["net"], 2),
                    "buy_ratio": round(metrics["buy_ratio"], 4),
                })
            rows.sort(key=lambda item: (item["score"], item["net_buy_amount"]), reverse=True)
        return {"rows": rows, "count": len(rows), "window": window, "scope": scope, "stale": self.status()["stale"]}

    def tape(self, symbol: str) -> dict:
        normalized = str(symbol).strip().upper()
        with self._lock:
            state = self._states.get(normalized)
            if state is None:
                return {"symbol": normalized, "trades": [], "intents": [], "timeline": [], "source": "proxy_only"}
            timeline = []
            for point in list(state["flows"])[-300:]:
                timeline.append({"ts": point["ts"], "amount": point["amount"], "buy": point["buy"], "sell": point["sell"], "price": point["price"]})
            return {
                "symbol": normalized,
                "name": state["name"],
                "trades": list(state["trade_events"]),
                "intents": list(state["intent_events"]),
                "timeline": timeline,
                "source": state["deep_source"],
                "last_deep_ms": state["last_deep_ms"],
                "error": state["deep_error"],
            }
