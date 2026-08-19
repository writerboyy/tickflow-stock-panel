"""First-board discovery and candidate-pool limit-board tracking."""
from __future__ import annotations

import logging
import queue
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.price_limits import is_risk_warning_name, limit_price, price_limit_pct
from app.services import alert_store, premium_gene, rps_rotation
from app.services.limit_board_scoring import (
    intraday_flow_detail,
    premium_gene_detail,
    sector_detail,
    technical_detail,
)
from app.services.limit_board_store import LimitBoardStore


logger = logging.getLogger(__name__)
_ACCOUNT_ID = "limit_board"
_DEPTH_FRESH_SECONDS = 30
_MIN_LIMIT_UP_COUNT = 4
_MIN_NEXT_DAY_RED_RATE = 0.80
_MAX_FIRST_BOARD_BROKEN_RATE = 0.75
_STOCK_PRICE_TICK = 0.01
_HISTORY_RETRY_SECONDS = 60
_HISTORY_WARMUP_RETRY_SECONDS = 5
_HISTORY_WARMUP_REASON = "历史指标缓存尚未就绪，首板/反包扫描已暂停"
_PREMIUM_FILTER_COLUMNS = {
    "symbol",
    "limit_up_count",
    "next_day_red_rate",
    "first_board_broken_rate",
}
_SCORE_REFRESH_SECONDS = 5.0
_SECTOR_CANDIDATE_LIMIT = 10
_AUTOMATIC_CANDIDATES_PER_SECTOR = 10
_AUTOMATIC_CANDIDATE_LIMIT = 30
_SCORE_STOCK_COLUMNS = {
    "symbol", "name", "close", "last_price", "prev_close", "change_pct", "amount",
    "ma5", "ma10", "ma20", "ma60", "momentum_5d", "momentum_20d",
    "vol_ratio_5d", "macd_dif", "macd_dea", "macd_hist", "rsi_14",
}
_SCORE_WEIGHTS = {
    "sector": 50.0,
    "premium_gene": 30.0,
    "intraday_flow": 15.0,
    "technical": 5.0,
}


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None


def _quote_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, CN_TZ)
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(CN_TZ) if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ)
    return None


def _is_trading_time(value: datetime) -> bool:
    current = value.timetz().replace(tzinfo=None)
    return (
        clock_time(9, 30) <= current < clock_time(11, 30)
        or clock_time(13, 0) <= current < clock_time(15, 0)
    )


def _is_main_board_symbol(symbol: str) -> bool:
    value = str(symbol or "").strip().upper()
    if value.endswith(".BJ") or value.startswith(("300", "301", "688", "689")):
        return False
    return value.endswith((".SH", ".SZ"))


def _sweep_ready(
    depth: dict[str, Any], limit_up: float, max_price_levels: int = 5,
) -> bool:
    asks = [
        (price, volume)
        for price, volume in zip(
            depth.get("ask_prices") or [],
            depth.get("ask_volumes") or [],
            strict=False,
        )
        if price > 0 and volume > 0
    ]
    if not asks:
        return False
    best_ask = asks[0][0]
    remaining = limit_up - best_ask
    return -0.001 <= remaining <= max_price_levels * _STOCK_PRICE_TICK + 0.001


def _qualified_premium_stats(rows: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if (
        rows is None
        or rows.is_empty()
        or not _PREMIUM_FILTER_COLUMNS.issubset(rows.columns)
    ):
        return {}
    qualified = rows.select(sorted(_PREMIUM_FILTER_COLUMNS)).filter(
        (pl.col("limit_up_count").cast(pl.Int64, strict=False) >= _MIN_LIMIT_UP_COUNT)
        & (pl.col("next_day_red_rate").cast(pl.Float64, strict=False) >= _MIN_NEXT_DAY_RED_RATE)
        & (
            pl.col("first_board_broken_rate").cast(pl.Float64, strict=False)
            <= _MAX_FIRST_BOARD_BROKEN_RATE
        )
    )
    return {
        str(row["symbol"]).strip().upper(): {
            "limit_up_count": int(row["limit_up_count"]),
            "next_day_red_rate": float(row["next_day_red_rate"]),
            "first_board_broken_rate": float(row["first_board_broken_rate"]),
        }
        for row in qualified.iter_rows(named=True)
        if str(row.get("symbol") or "").strip()
    }


def _premium_stats_by_symbol(rows: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if rows is None or rows.is_empty() or "symbol" not in rows.columns:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for raw in rows.iter_rows(named=True):
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        result[symbol] = {
            key: value.isoformat() if isinstance(value, date) else value
            for key, value in raw.items()
            if key != "symbol"
        }
    return result


class LimitBoardService:
    def __init__(self, data_dir: Path, repo: Any, quote_service: Any, app_state: Any) -> None:
        self.store = LimitBoardStore(data_dir)
        self.repo = repo
        self.quote_service = quote_service
        self.app_state = app_state
        self._lock = threading.RLock()
        self._score_lock = threading.Lock()
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=3)
        self._order_results: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=10)
        self._order_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="limit-board-order")
        self._order_slots = threading.BoundedSemaphore(4)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._polling_lease = False
        self._ws_registered = False
        self._ws_symbols: set[str] = set()
        self._quotes: dict[str, dict[str, Any]] = {}
        self._sector_quote_symbols: set[str] = set()
        self._heat_quote_symbols: set[str] = set()
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=10))
        self._history_date: date | None = None
        self._name_map_date: date | None = None
        self._name_map: dict[str, str] = {}
        self._instrument_limit_up_date: date | None = None
        self._instrument_limit_up: dict[str, object] = {}
        self._first_board_eligible: set[str] = set()
        self._rebound_board_eligible: set[str] = set()
        self._premium_stats: dict[str, dict[str, Any]] = {}
        self._sector_membership_date: date | None = None
        self._sector_memberships = pl.DataFrame()
        self._sector_live_quotes: dict[str, dict[str, Any]] = {}
        self._sector_candidate_key: tuple[date, str, tuple[str, ...]] | None = None
        self._sector_candidate_symbols: set[str] = set()
        self._sector_candidate_plate_ids: set[str] = set()
        self._sector_candidates_by_symbol: dict[str, list[dict[str, str]]] = {}
        self._sector_candidate_scope: dict[str, Any] = {
            "state": "unavailable",
            "plate_count": 0,
            "symbol_count": 0,
            "reason": "正在读取实时板块强度前 10 名",
        }
        self._sector_trend_cache: dict[
            tuple[date, int, str, str],
            tuple[dict[str, Any] | None, dict[str, dict[str, Any]]],
        ] = {}
        self._score_refresh_at = 0.0
        self._rotation_date: date | None = None
        self._rotation_cache: dict[tuple[str, int | None], dict[str, Any]] = {}
        self._history_ready = False
        self._history_attempt_at = 0.0
        self._history_reason = "正在读取涨停历史与溢价基因数据"
        self._last_scan_at: str | None = None
        self._last_error: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.quote_service.add_fetch_listener(self._on_market_fetch)
        self._refresh_symbol_consumer()
        try:
            self.quote_service.acquire_temporary_polling(self._refresh_interval_seconds())
            self._polling_lease = True
        except ValueError as exc:
            self._last_error = str(exc)
        hub = self._hub()
        if hub is not None:
            hub.add_depth_listener(self.enqueue_depth)
        self._thread = threading.Thread(target=self._worker, name="limit-board", daemon=True)
        self._thread.start()
        self._on_market_fetch()

    def stop(self) -> None:
        self.quote_service.remove_fetch_listener(self._on_market_fetch)
        self.quote_service.remove_symbol_consumer(_ACCOUNT_ID)
        hub = self._hub()
        if hub is not None:
            hub.remove_depth_listener(self.enqueue_depth)
            if self._ws_registered:
                try:
                    hub.unregister(_ACCOUNT_ID)
                except Exception:  # noqa: BLE001
                    logger.debug("打板专区移除 WS 订阅失败", exc_info=True)
        self._ws_registered = False
        self._ws_symbols.clear()
        if self._polling_lease:
            self.quote_service.release_temporary_polling()
            self._polling_lease = False
        self._stop.set()
        self._enqueue({"type": "stop"})
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._order_executor.shutdown(wait=False, cancel_futures=True)

    def _hub(self):
        return getattr(getattr(self.app_state, "paper_supervisor", None), "hub", None)

    def _qmt(self):
        return getattr(self.app_state, "qmt_trading_service", None)

    def quote_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        requested = list(dict.fromkeys(
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        ))[:30]
        requested_set = set(requested)
        self._refresh_sector_candidate_universe(cn_today())
        with self._lock:
            self._heat_quote_symbols = requested_set
        self._refresh_symbol_consumer()
        fresh_payload = self._fresh_tickflow_quotes(requested_set)
        quotes = {}
        for symbol, raw in (fresh_payload.get("quotes") or {}).items():
            normalized = str(symbol).strip().upper()
            if normalized not in requested_set:
                continue
            price = _finite(raw.get("last_price", raw.get("close")))
            if price is None:
                continue
            name = self._resolve_name(normalized, raw.get("name"))
            quotes[normalized] = {
                "symbol": normalized,
                "name": name,
                "last_price": price,
                "change_pct": _finite(raw.get("change_pct")),
                "limit_up": self._limit_up(raw, normalized, name, cn_today()),
                "timestamp": raw.get("timestamp"),
                "source": "tickflow",
            }
        timestamps = [
            str(row["timestamp"])
            for row in quotes.values()
            if row.get("timestamp")
        ]
        sector_links = {
            symbol: [dict(item) for item in self._sector_candidates_by_symbol.get(symbol, [])]
            for symbol in requested
            if self._sector_candidates_by_symbol.get(symbol)
        }
        return {
            "state": (
                "live" if len(quotes) == len(requested)
                else "partial" if quotes else "unavailable"
            ),
            "as_of": max(timestamps) if timestamps else None,
            "quotes": quotes,
            "sector_links": sector_links,
            "missing_symbols": [symbol for symbol in requested if symbol not in quotes],
        }

    def _fresh_tickflow_quotes(self, symbols: set[str]) -> dict[str, Any]:
        provider_getter = getattr(self.quote_service, "realtime_provider", None)
        if callable(provider_getter):
            try:
                if str(provider_getter()).strip().lower() != "tickflow":
                    return {}
            except Exception:  # noqa: BLE001
                logger.debug("读取 TickFlow 实时行情 provider 失败", exc_info=True)
                return {}
        fresh_getter = getattr(self.quote_service, "get_fresh_quotes", None)
        if not callable(fresh_getter) or not symbols:
            return {}
        try:
            payload = fresh_getter(symbols)
        except Exception:  # noqa: BLE001
            logger.debug("读取 TickFlow 实时行情快照失败", exc_info=True)
            return {}
        if not isinstance(payload, dict):
            return {}

        # QuoteService 缓存的是 TickFlow 原始报价，不包含 instruments 中的
        # 当日涨跌停价。补入维表哨兵值后，_limit_up 才能识别新股首日无涨跌停。
        today = cn_today()
        if self._instrument_limit_up_date != today:
            try:
                instruments = self.repo.get_instruments()
                if (
                    instruments.is_empty()
                    or "symbol" not in instruments.columns
                    or "limit_up" not in instruments.columns
                ):
                    limit_map = {}
                else:
                    limit_map = {
                        str(row["symbol"]).strip().upper(): row.get("limit_up")
                        for row in instruments.select(["symbol", "limit_up"]).iter_rows(named=True)
                        if str(row.get("symbol") or "").strip()
                    }
            except Exception:  # noqa: BLE001
                logger.debug("读取股票涨停价维表失败", exc_info=True)
                limit_map = {}
            self._instrument_limit_up = limit_map
            self._instrument_limit_up_date = today
        limit_map = self._instrument_limit_up
        if not limit_map:
            return payload

        quotes = payload.get("quotes")
        if not isinstance(quotes, dict):
            return payload
        enriched_quotes = {}
        for raw_symbol, raw_quote in quotes.items():
            symbol = str(raw_symbol).strip().upper()
            if not isinstance(raw_quote, dict):
                enriched_quotes[raw_symbol] = raw_quote
                continue
            quote = dict(raw_quote)
            if quote.get("limit_up") is None and symbol in limit_map:
                quote["limit_up"] = limit_map[symbol]
            enriched_quotes[raw_symbol] = quote
        return {**payload, "quotes": enriched_quotes}

    def _refresh_interval_seconds(self) -> float:
        getter = getattr(self.quote_service, "get_min_interval", None)
        if not callable(getter):
            return _SCORE_REFRESH_SECONDS
        try:
            return max(_SCORE_REFRESH_SECONDS, float(getter()))
        except (TypeError, ValueError):
            return _SCORE_REFRESH_SECONDS

    def _market_sentiment_snapshot(self) -> dict[str, Any] | None:
        collector = getattr(self.app_state, "kaipanla_collector", None)
        getter = getattr(collector, "market_sentiment_snapshot", None)
        if not callable(getter):
            return None
        try:
            value = getter()
        except Exception:  # noqa: BLE001
            logger.debug("读取开盘啦情绪快照失败", exc_info=True)
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _top_sector_rows(
        rows: list[dict[str, Any]],
        limit: int = _SECTOR_CANDIDATE_LIMIT,
    ) -> list[dict[str, Any]]:
        values = [
            {
                "plate_id": str(row.get("plate_id") or "").strip(),
                "plate_name": str(row.get("plate_name") or "").strip(),
                "rank": _finite(row.get("rank")),
                "strength": _finite(row.get("strength")),
            }
            for row in rows
            if isinstance(row, dict) and str(row.get("plate_id") or "").strip()
        ]
        if not values:
            return []
        frame = pl.DataFrame(values).with_columns(
            pl.col("rank").is_null().alias("_rank_missing"),
            pl.col("strength").is_null().alias("_strength_missing"),
        )
        return frame.sort(
            ["_rank_missing", "rank", "_strength_missing", "strength", "plate_id"],
            descending=[False, False, False, True, False],
            nulls_last=True,
        ).head(limit).drop("_rank_missing", "_strength_missing").to_dicts()

    @staticmethod
    def _trend_state(strength_delta: float | None, main_net_delta: float | None) -> str:
        if strength_delta is None or main_net_delta is None:
            return "unavailable"
        if strength_delta > 0 and main_net_delta > 0:
            return "accelerating"
        if strength_delta < 0 and main_net_delta < 0:
            return "weakening"
        if strength_delta == 0 and main_net_delta == 0:
            return "stable"
        return "divergent"

    def _sector_window_trend(
        self,
        today: date,
        snapshot: dict[str, Any],
        timeline: list[str],
        window_minutes: int,
    ) -> tuple[dict[str, Any] | None, dict[str, dict[str, Any]]]:
        captured = _quote_time(snapshot.get("refreshed_at"))
        if captured is None:
            return None, {}
        bucket_end = captured.replace(
            minute=(captured.minute // 5) * 5,
            second=0,
            microsecond=0,
        )
        points = sorted(
            (point, raw)
            for raw in timeline
            if (point := _quote_time(raw)) is not None and point.date() == today
        )
        current = next(
            ((point, raw) for point, raw in reversed(points) if point <= bucket_end),
            None,
        )
        if current is None:
            return None, {}
        base = next(
            (
                (point, raw)
                for point, raw in reversed(points)
                if point <= current[0] - timedelta(minutes=window_minutes)
            ),
            None,
        )
        if base is None:
            return None, {}
        elapsed_minutes = (current[0] - base[0]).total_seconds() / 60.0
        if elapsed_minutes <= 0:
            return None, {}
        cache_key = (today, window_minutes, current[1], base[1])
        cached = self._sector_trend_cache.get(cache_key)
        if cached is not None:
            return cached

        collector = getattr(self.app_state, "kaipanla_collector", None)
        getter = getattr(collector, "sector_strength_snapshot_at", None)
        if not callable(getter):
            return None, {}
        try:
            current_snapshot = getter(today, current[1])
            base_snapshot = getter(today, base[1])
        except Exception:  # noqa: BLE001
            logger.debug("读取开盘啦 5 分钟板块趋势失败", exc_info=True)
            return None, {}
        if not isinstance(current_snapshot, dict) or not isinstance(base_snapshot, dict):
            return None, {}
        current_rows = {
            str(row.get("plate_id") or "").strip(): row
            for row in current_snapshot.get("rows") or []
            if isinstance(row, dict) and str(row.get("plate_id") or "").strip()
        }
        base_rows = {
            str(row.get("plate_id") or "").strip(): row
            for row in base_snapshot.get("rows") or []
            if isinstance(row, dict) and str(row.get("plate_id") or "").strip()
        }
        by_plate: dict[str, dict[str, Any]] = {}
        for plate_id in current_rows.keys() & base_rows.keys():
            current_strength = _finite(current_rows[plate_id].get("strength"))
            base_strength = _finite(base_rows[plate_id].get("strength"))
            current_main_net = _finite(current_rows[plate_id].get("main_net"))
            base_main_net = _finite(base_rows[plate_id].get("main_net"))
            strength_delta = (
                current_strength - base_strength
                if current_strength is not None and base_strength is not None else None
            )
            main_net_delta = (
                current_main_net - base_main_net
                if current_main_net is not None and base_main_net is not None else None
            )
            by_plate[plate_id] = {
                f"strength_delta_{window_minutes}m": strength_delta,
                f"main_net_delta_{window_minutes}m": main_net_delta,
                f"strength_speed_per_min_{window_minutes}m": (
                    strength_delta / elapsed_minutes
                    if strength_delta is not None else None
                ),
                f"main_net_speed_per_min_{window_minutes}m": (
                    main_net_delta / elapsed_minutes
                    if main_net_delta is not None else None
                ),
                f"trend_{window_minutes}m_state": self._trend_state(
                    strength_delta, main_net_delta,
                ),
            }

        top_ids = {
            str(row.get("plate_id") or "")
            for row in self._top_sector_rows(list(current_rows.values()))
        }
        comparable = [
            {
                "plate_id": plate_id,
                "plate_name": str(current_rows[plate_id].get("plate_name") or ""),
                **value,
            }
            for plate_id, value in by_plate.items()
            if plate_id in top_ids
            and value[f"strength_delta_{window_minutes}m"] is not None
            and value[f"main_net_delta_{window_minutes}m"] is not None
        ]
        if comparable:
            strength_delta = sum(
                value[f"strength_delta_{window_minutes}m"] for value in comparable
            ) / len(comparable)
            main_net_delta = sum(
                value[f"main_net_delta_{window_minutes}m"] for value in comparable
            )
            summary = {
                "state": self._trend_state(strength_delta, main_net_delta),
                "window_minutes": window_minutes,
                "elapsed_minutes": elapsed_minutes,
                "captured_at": current[1],
                "base_at": base[1],
                "strength_delta": strength_delta,
                "main_net_delta": main_net_delta,
                "comparable_count": len(comparable),
            }
        else:
            summary = None
        if len(self._sector_trend_cache) >= 256:
            self._sector_trend_cache.clear()
        self._sector_trend_cache[cache_key] = (summary, by_plate)
        return summary, by_plate

    def _sector_strength_view(
        self,
        today: date,
        captured_at: str | None = None,
        *,
        include_timeline: bool = False,
    ) -> dict[str, Any] | None:
        collector = getattr(self.app_state, "kaipanla_collector", None)
        if collector is None:
            return None
        getter = getattr(
            collector,
            "sector_strength_snapshot_at" if captured_at else "sector_strength_snapshot",
            None,
        )
        if not callable(getter):
            return None
        try:
            snapshot = getter(today, captured_at) if captured_at else getter()
        except Exception:  # noqa: BLE001
            logger.debug("读取开盘啦实时板块强度快照失败", exc_info=True)
            return None
        timeline = []
        if include_timeline:
            timeline_getter = getattr(collector, "sector_strength_timeline", None)
            if callable(timeline_getter):
                try:
                    timeline = [str(value) for value in timeline_getter(today) if value]
                except Exception:  # noqa: BLE001
                    logger.debug("读取开盘啦板块强度时间轴失败", exc_info=True)
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("state") != "live"
            or snapshot.get("as_of") != today.isoformat()
        ):
            return {
                "provider": "kaipanla",
                "state": "unavailable",
                "as_of": today.isoformat(),
                "refreshed_at": snapshot.get("refreshed_at") if isinstance(snapshot, dict) else None,
                "institution_label": None,
                "history_state": "live" if timeline else "unavailable",
                "timeline": timeline,
                "rows": [],
            }
        normalized = []
        for row in snapshot.get("rows") or []:
            if not isinstance(row, dict):
                continue
            name = str(row.get("plate_name") or "").strip()
            if not name:
                continue
            change_pct_pct = _finite(row.get("change_pct_pct"))
            speed_pct_pct = _finite(row.get("speed_pct_pct"))
            value = {
                **row,
                "change_pct": change_pct_pct / 100.0 if change_pct_pct is not None else None,
                "speed_pct": speed_pct_pct / 100.0 if speed_pct_pct is not None else None,
            }
            normalized.append(value)

        children: dict[str, list[dict[str, Any]]] = {}
        roots = []
        for row in normalized:
            parent_id = str(row.get("parent_plate_id") or "").strip()
            if parent_id:
                children.setdefault(parent_id, []).append(row)
            else:
                roots.append(row)
        ordered = []
        seen_children = set()
        for row in roots:
            ordered.append(row)
            for child in children.get(str(row.get("plate_id") or ""), []):
                ordered.append(child)
                seen_children.add(str(child.get("plate_id") or ""))
        ordered.extend(
            row for row in normalized
            if row.get("is_child") and str(row.get("plate_id") or "") not in seen_children
        )
        trend_5m, trend_5m_by_plate = (
            self._sector_window_trend(today, snapshot, timeline, 5)
            if include_timeline else (None, {})
        )
        trend_30m, trend_30m_by_plate = (
            self._sector_window_trend(today, snapshot, timeline, 30)
            if include_timeline else (None, {})
        )
        for row in ordered:
            plate_id = str(row.get("plate_id") or "")
            row.update(trend_5m_by_plate.get(plate_id, {}))
            row.update(trend_30m_by_plate.get(plate_id, {}))
        return {
            "provider": "kaipanla",
            "state": "live",
            "as_of": snapshot.get("as_of"),
            "refreshed_at": snapshot.get("refreshed_at"),
            "institution_label": snapshot.get("institution_label") or "机构增仓",
            "history_state": snapshot.get("history_state") or ("live" if timeline else "unavailable"),
            "timeline": timeline,
            "trend_5m": trend_5m,
            "trend_30m": trend_30m,
            "rows": ordered,
        }

    def sector_strength_view(self, captured_at: str | None = None) -> dict[str, Any] | None:
        today = cn_today()
        if captured_at:
            try:
                point = datetime.fromisoformat(captured_at)
            except ValueError as exc:
                raise ValueError("板块强度时间点格式无效") from exc
            if point.tzinfo is None or point.astimezone(CN_TZ).date() != today:
                raise ValueError("只能回看当前交易日的板块强度")
        return self._sector_strength_view(today, captured_at, include_timeline=True)

    async def sector_constituents_view(
        self,
        plate_id: str,
        captured_at: str | None = None,
    ) -> dict[str, Any]:
        today = cn_today()
        requested_point: datetime | None = None
        if captured_at:
            try:
                requested_point = datetime.fromisoformat(captured_at)
            except ValueError as exc:
                raise ValueError("板块强度时间点格式无效") from exc
            if requested_point.tzinfo is None or requested_point.astimezone(CN_TZ).date() != today:
                raise ValueError("只能查看当前交易日的板块成分")
            requested_point = requested_point.astimezone(CN_TZ)
        current_snapshot = self.sector_strength_view()
        after_close = (
            requested_point is not None
            and requested_point.timetz().replace(tzinfo=None) >= clock_time(15, 0)
            and isinstance(current_snapshot, dict)
            and current_snapshot.get("history_state") == "closed"
        )
        current_point = _quote_time(
            current_snapshot.get("refreshed_at")
            if isinstance(current_snapshot, dict)
            else None,
        )
        if requested_point is not None and requested_point != current_point and not after_close:
            raise ValueError("开盘啦当日成分行情不提供历史时点回看")
        snapshot = current_snapshot
        rows = snapshot.get("rows") if isinstance(snapshot, dict) else []
        selected = next(
            (
                row for row in rows or []
                if isinstance(row, dict) and str(row.get("plate_id") or "") == plate_id
            ),
            None,
        )
        if selected is None:
            raise ValueError("该板块在选定时间点不可用")

        collector = getattr(self.app_state, "kaipanla_collector", None)
        snapshot_getter = getattr(collector, "shortline_constituents_snapshot", None)
        live_snapshot = snapshot_getter() if callable(snapshot_getter) else None
        plate_loader = getattr(collector, "shortline_constituents_for_plate", None)
        has_selected_plate = (
            isinstance(live_snapshot, dict)
            and live_snapshot.get("as_of") == today.isoformat()
            and live_snapshot.get("state") in {"live", "partial"}
            and any(
                isinstance(row, dict) and str(row.get("plate_id") or "") == plate_id
                for row in live_snapshot.get("rows") or []
            )
        )
        if not has_selected_plate and callable(plate_loader):
            live_snapshot = await plate_loader(today, plate_id)
        if (
            not isinstance(live_snapshot, dict)
            or live_snapshot.get("state") not in {"live", "partial"}
            or live_snapshot.get("as_of") != today.isoformat()
        ):
            raise RuntimeError("开盘啦当日成分行情暂不可用")
        try:
            instruments = self.repo.get_instruments()
            symbol_lookup = {
                str(symbol).split(".", 1)[0]: str(symbol).upper()
                for symbol in instruments.get_column("symbol").to_list()
            } if "symbol" in instruments.columns else {}
        except Exception:  # noqa: BLE001
            symbol_lookup = {}
        members = []
        for row in live_snapshot.get("rows") or []:
            if not isinstance(row, dict) or str(row.get("plate_id") or "") != plate_id:
                continue
            code = str(row.get("code") or row.get("symbol") or "").split(".", 1)[0]
            if not code:
                continue
            symbol = symbol_lookup.get(code) or f"{code}.{'SH' if code.startswith(('6', '9')) else 'BJ' if code.startswith(('4', '8')) else 'SZ'}"
            members.append({
                "plate_id": plate_id,
                "symbol": symbol,
                "code": code,
                "name": str(row.get("name") or "").strip() or None,
                "tags": str(row.get("tags") or "").strip() or None,
                "last_price": _finite(row.get("last_price")),
                "change_pct": _finite(row.get("change_pct")),
                "amount": _finite(row.get("amount")),
                "turnover_rate": _finite(row.get("turnover_rate")),
                "float_market_value": None,
                "main_net": _finite(row.get("main_net")),
                "limit_tag": str(row.get("limit_tag") or "").strip() or None,
                "rank_tag": None,
                "limit_count": None,
                "quote_available": _finite(row.get("last_price")) is not None,
            })
        if not members:
            raise RuntimeError("开盘啦当日成分行情暂不可用")
        members.sort(key=lambda row: (
            row.get("change_pct") is None,
            -float(row.get("change_pct") or 0),
            -float(row.get("amount") or 0),
            str(row.get("code") or ""),
        ))
        for index, row in enumerate(members, start=1):
            row["rank"] = index
            row["rank_count"] = len(members)
        return {
            "provider": "kaipanla_socket",
            "state": str(live_snapshot.get("state")),
            "as_of": today.isoformat(),
            "captured_at": live_snapshot.get("refreshed_at"),
            "membership_as_of": today.isoformat(),
            "quote_provider": "kaipanla_socket",
            "quote_state": str(live_snapshot.get("state")),
            "quote_as_of": live_snapshot.get("refreshed_at"),
            "quote_available": True,
            "plate_id": plate_id,
            "plate_name": selected.get("plate_name"),
            "rows": members,
        }

        selected_at = requested_point or snapshot.get("refreshed_at")
        if not selected_at:
            raise ValueError("板块强度时间点不可用")
        if isinstance(selected_at, datetime):
            point = selected_at
        else:
            try:
                point = datetime.fromisoformat(str(selected_at))
            except ValueError as exc:
                raise ValueError("板块强度时间点格式无效") from exc
        if point.tzinfo is None or point.astimezone(CN_TZ).date() != today:
            raise ValueError("只能查看当前交易日的板块成分")
        point = point.astimezone(CN_TZ)
        if point.timetz().replace(tzinfo=None) > clock_time(15, 0):
            point = point.replace(hour=15, minute=0, second=0, microsecond=0)
        if not clock_time(9, 25) <= point.timetz().replace(tzinfo=None) <= clock_time(15, 0):
            raise ValueError("板块成分时间点必须在 09:25 至 15:00 之间")

        collector = getattr(self.app_state, "kaipanla_collector", None)
        membership_date_getter = getattr(collector, "latest_completed_trading_date", None)
        if not callable(membership_date_getter):
            raise RuntimeError("开盘啦板块成分交易日暂不可用")
        membership_date = membership_date_getter(today)
        if not isinstance(membership_date, date):
            raise RuntimeError("开盘啦板块成分缺少上一完整交易日")
        getter = getattr(collector, "sector_constituents_at", None)
        if not callable(getter):
            raise RuntimeError("开盘啦板块成分历史数据暂不可用")
        try:
            source_rows = await getter(membership_date, plate_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询开盘啦板块成分失败 (%s)", type(exc).__name__)
            raise RuntimeError("开盘啦板块成分历史数据暂不可用") from exc

        symbol_lookup: dict[str, str] = {}
        try:
            instruments = self.repo.get_instruments()
            if "symbol" in instruments.columns:
                symbol_lookup = {
                    str(symbol).split(".", 1)[0]: str(symbol)
                    for symbol in instruments["symbol"].to_list()
                    if symbol
                }
        except Exception:  # noqa: BLE001
            logger.debug("板块成分股代码维表暂不可用", exc_info=True)

        members = []
        for row in source_rows:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or row.get("symbol") or "").split(".", 1)[0]
            if not code:
                continue
            symbol = symbol_lookup.get(code)
            if not symbol:
                exchange = "SH" if code.startswith(("6", "9")) else "BJ" if code.startswith(("4", "8")) else "SZ"
                symbol = f"{code}.{exchange}"
            members.append({
                "plate_id": plate_id,
                "symbol": symbol,
                "code": code,
                "name": str(row.get("name") or "").strip() or None,
                "tags": str(row.get("tags") or "").strip() or None,
            })

        symbols = {str(row["symbol"]) for row in members}
        with self._lock:
            self._sector_quote_symbols = symbols
        self._refresh_symbol_consumer()

        latest_at = _quote_time(
            current_snapshot.get("refreshed_at")
            if isinstance(current_snapshot, dict)
            else None,
        )
        historical_point = (
            requested_point is not None
            and not after_close
            and latest_at is not None
            and requested_point < latest_at
        )
        quote_rows = [] if historical_point else self.quote_service.get_latest_quotes(symbols)
        quotes_by_symbol: dict[str, dict[str, Any]] = {}
        quote_dates: dict[date, int] = defaultdict(int)
        for quote in quote_rows:
            symbol = str(quote.get("symbol") or "").strip().upper()
            quote_at = _quote_time(quote.get("timestamp"))
            if symbol in symbols and quote_at is not None:
                quotes_by_symbol[symbol] = quote
                quote_dates[quote_at.date()] += 1

        quote_date: date | None = None
        if quote_dates.get(today):
            quote_date = today
        elif not _is_trading_time(cn_now()) and quote_dates.get(membership_date):
            quote_date = membership_date
        if quote_date is not None:
            quotes_by_symbol = {
                symbol: quote
                for symbol, quote in quotes_by_symbol.items()
                if (_quote_time(quote.get("timestamp")) or datetime.min.replace(tzinfo=CN_TZ)).date()
                == quote_date
            }
        else:
            quotes_by_symbol = {}

        normalized = []
        quote_times = []
        for member in members:
            symbol = str(member["symbol"])
            quote = quotes_by_symbol.get(symbol)
            quote_at = _quote_time(quote.get("timestamp")) if quote else None
            if quote_at is not None:
                quote_times.append(quote_at)
            name = str((quote or {}).get("name") or member.get("name") or "").strip() or None
            last_price = _finite((quote or {}).get("last_price", (quote or {}).get("close")))
            limit_up = self._limit_up(quote or {}, symbol, name or "", quote_date or today)
            at_limit = (
                last_price is not None
                and limit_up is not None
                and last_price >= limit_up - 0.005
            )
            normalized.append({
                **member,
                "name": name,
                "last_price": last_price,
                "change_pct": _finite((quote or {}).get("change_pct")),
                "amount": _finite((quote or {}).get("amount")),
                "turnover_rate": _finite((quote or {}).get("turnover_rate")),
                "float_market_value": None,
                "main_net": None,
                "limit_tag": "涨停" if at_limit else None,
                "rank_tag": None,
                "limit_count": None,
                "quote_available": quote is not None and last_price is not None,
            })
        normalized.sort(key=lambda row: (
            row.get("change_pct") is None,
            -float(row.get("change_pct") or 0),
            -float(row.get("amount") or 0),
            str(row.get("code") or ""),
        ))
        for index, row in enumerate(normalized):
            row["rank"] = index + 1
            row["rank_count"] = len(normalized)

        now = cn_now()
        quote_service_status = getattr(self.quote_service, "status", None)
        status = quote_service_status() if callable(quote_service_status) else {}
        latest_quote_at = max(quote_times) if quote_times else None
        closed_snapshot = (
            quote_date is not None
            and (
                quote_date < today
                or (
                    now.timetz().replace(tzinfo=None) >= clock_time(15, 0)
                    and (
                        bool(status.get("final_sync_done"))
                        or (
                            latest_quote_at is not None
                            and latest_quote_at.timetz().replace(tzinfo=None)
                            >= clock_time(15, 0)
                        )
                    )
                )
            )
        )
        if historical_point:
            quote_state = "historical_unavailable"
        elif quote_date is None:
            quote_state = "unavailable"
        elif closed_snapshot:
            quote_state = "closed"
        elif _is_trading_time(now):
            quote_state = "live"
        else:
            quote_state = "paused"
        quote_as_of = latest_quote_at
        if quote_state == "closed" and quote_date is not None:
            quote_as_of = datetime.combine(
                quote_date,
                clock_time(15, 0),
                tzinfo=CN_TZ,
            )

        return {
            "provider": "kaipanla",
            "state": "live",
            "as_of": today.isoformat(),
            "captured_at": point.isoformat(),
            "membership_as_of": membership_date.isoformat(),
            "quote_provider": "tickflow",
            "quote_state": quote_state,
            "quote_as_of": quote_as_of.isoformat() if quote_as_of else None,
            "quote_available": bool(quotes_by_symbol),
            "plate_id": plate_id,
            "plate_name": selected.get("plate_name"),
            "rows": normalized,
        }

    def _sector_strength_snapshot(self, today: date) -> dict[str, dict[str, Any]]:
        view = self._sector_strength_view(today)
        if not view or view.get("state") != "live":
            return {}
        return {
            str(row.get("plate_name") or "").strip(): row
            for row in view.get("rows") or []
            if str(row.get("plate_name") or "").strip()
        }

    def _kaipanla_sector_score_inputs(
        self,
        realtime: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], set[str], dict[str, Any]] | None:
        """Build sector scoring inputs from the current Kaipanla socket snapshot."""
        plate_id = str(realtime.get("plate_id") or "").strip()
        if not plate_id:
            return None
        with self._lock:
            memberships = self._sector_memberships.clone()
            live_quotes = {
                symbol: dict(value)
                for symbol, value in self._sector_live_quotes.items()
            }
        if (
            memberships.is_empty()
            or "plate_id" not in memberships.columns
            or "symbol" not in memberships.columns
        ):
            return None
        member_symbols = {
            str(symbol).strip().upper()
            for symbol in memberships.filter(pl.col("plate_id") == plate_id)
            .get_column("symbol")
            .to_list()
            if str(symbol).strip()
        }
        if not member_symbols:
            return None
        stock_rows = {
            symbol: row
            for symbol in member_symbols
            if (row := live_quotes.get(symbol))
            and str(row.get("source") or "").strip().lower() == "kaipanla_socket"
        }
        valid_rows = {
            symbol: row
            for symbol, row in stock_rows.items()
            if _finite(row.get("change_pct")) is not None
        }
        total_count = len(member_symbols)
        valid_count = len(valid_rows)
        changes = [float(row["change_pct"]) for row in valid_rows.values()]
        sector_change = _finite(realtime.get("change_pct"))
        if sector_change is None and changes:
            sector_change = sum(changes) / len(changes)
        return stock_rows, member_symbols, {
            "valid": total_count >= 5 and valid_count / total_count >= 0.8,
            "change_pct": sector_change,
            "coverage_ratio": valid_count / total_count if total_count else 0.0,
            "valid_count": valid_count,
            "total_count": total_count,
            "up_count": sum(value > 0 for value in changes),
            "down_count": sum(value < 0 for value in changes),
            "data_source": "kaipanla_socket",
        }

    def _set_sector_candidate_unavailable(self, reason: str) -> set[str]:
        self._sector_candidate_key = None
        self._sector_candidate_symbols.clear()
        self._sector_candidate_plate_ids.clear()
        self._sector_candidates_by_symbol.clear()
        self._sector_candidate_scope = {
            "state": "unavailable",
            "plate_count": 0,
            "symbol_count": 0,
            "reason": reason,
        }
        return set()

    def _refresh_sector_candidate_universe(self, today: date) -> set[str]:
        view = self._sector_strength_view(today)
        if not view or view.get("state") != "live":
            return self._set_sector_candidate_unavailable("实时板块强度前 10 名暂不可用")
        top_rows = self._top_sector_rows(view.get("rows") or [])
        if not top_rows:
            return self._set_sector_candidate_unavailable("实时板块强度前 10 名为空")
        plate_ids = tuple(str(row["plate_id"]) for row in top_rows)
        snapshot_at = str(view.get("refreshed_at") or "")
        candidate_key = (today, snapshot_at, plate_ids)
        if self._sector_candidate_key == candidate_key:
            return set(self._sector_candidate_symbols)

        collector = getattr(self.app_state, "kaipanla_collector", None)
        snapshot_getter = getattr(collector, "shortline_constituents_snapshot", None)
        if not callable(snapshot_getter):
            return self._set_sector_candidate_unavailable("开盘啦当日成分行情暂不可用")
        snapshot = snapshot_getter()
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("state") not in {"live", "partial"}
            or snapshot.get("as_of") != today.isoformat()
        ):
            return self._set_sector_candidate_unavailable("开盘啦当日成分行情暂不可用")
        source_rows = [row for row in snapshot.get("rows") or [] if isinstance(row, dict)]
        if not source_rows:
            return self._set_sector_candidate_unavailable("开盘啦当日成分行情为空")
        try:
            instruments = self.repo.get_instruments()
            symbol_lookup = {
                str(symbol).split(".", 1)[0]: str(symbol).upper()
                for symbol in instruments.get_column("symbol").to_list()
            } if "symbol" in instruments.columns else {}
        except Exception:  # noqa: BLE001
            symbol_lookup = {}
        memberships_rows = []
        live_quotes: dict[str, dict[str, Any]] = {}
        for row in source_rows:
            code = str(row.get("code") or row.get("symbol") or "").split(".", 1)[0]
            plate_id = str(row.get("plate_id") or "").strip()
            if not code or not plate_id:
                continue
            symbol = symbol_lookup.get(code) or f"{code}.{'SH' if code.startswith(('6', '9')) else 'BJ' if code.startswith(('4', '8')) else 'SZ'}"
            memberships_rows.append({"plate_id": plate_id, "symbol": symbol})
            live_quotes.setdefault(symbol, {
                **row,
                "symbol": symbol,
                "timestamp": snapshot.get("refreshed_at"),
                "source": "kaipanla_socket",
            })
        if not memberships_rows:
            return self._set_sector_candidate_unavailable("开盘啦当日成分代码无效")
        self._sector_memberships = pl.DataFrame(memberships_rows).unique()
        self._sector_live_quotes = live_quotes

        selected = self._sector_memberships.filter(
            pl.col("plate_id").is_in(plate_ids),
        )
        if selected.is_empty():
            return self._set_sector_candidate_unavailable("前 10 板块未匹配开盘啦当日成分")
        matched_plate_ids = set(selected.get_column("plate_id").unique().to_list())
        missing_plate_ids = set(plate_ids) - matched_plate_ids
        names = {str(row["plate_id"]): str(row.get("plate_name") or "") for row in top_rows}
        by_symbol: dict[str, list[dict[str, str]]] = {}
        grouped = selected.group_by("symbol").agg(pl.col("plate_id").sort())
        for row in grouped.iter_rows(named=True):
            symbol = str(row["symbol"])
            by_symbol[symbol] = [
                {"plate_id": plate_id, "plate_name": names.get(plate_id, "")}
                for plate_id in row["plate_id"]
            ]
        self._sector_candidate_key = candidate_key
        self._sector_candidate_symbols = set(by_symbol)
        self._sector_candidate_plate_ids = set(matched_plate_ids)
        self._sector_candidates_by_symbol = by_symbol
        scope_state = "partial" if missing_plate_ids else "live"
        scope_label = f"开盘啦实时板块强度前 {_SECTOR_CANDIDATE_LIMIT} 名范围"
        if len(plate_ids) < _SECTOR_CANDIDATE_LIMIT:
            scope_label += f"（当前返回 {len(plate_ids)} 个有效板块）"
        scope_reason = (
                f"仅扫描{scope_label}内的当日 {len(by_symbol)} 只去重成分"
            if not missing_plate_ids
            else (
                f"{scope_label}中 {len(matched_plate_ids)} 个有当日成分，"
                f"共 {len(by_symbol)} 只去重成分；{len(missing_plate_ids)} 个板块缺口已跳过"
            )
        )
        self._sector_candidate_scope = {
            "state": scope_state,
            "as_of": snapshot_at or None,
            "membership_as_of": today.isoformat(),
            "plate_count": len(matched_plate_ids),
            "symbol_count": len(by_symbol),
            "plate_ids": sorted(matched_plate_ids),
            "reason": scope_reason,
        }
        return set(self._sector_candidate_symbols)

    def _automatic_candidate_symbols(self, today: date) -> set[str]:
        sector_symbols = self._refresh_sector_candidate_universe(today)
        if not self._history_ready:
            return set()
        result = sector_symbols & (self._first_board_eligible | self._rebound_board_eligible)
        if self.store.load_config()["settings"].get("main_board_only", False):
            result = {symbol for symbol in result if _is_main_board_symbol(symbol)}
        return result

    def _sentiment_guard(self, config: dict[str, Any]) -> dict[str, Any]:
        threshold = _finite(
            config.get("settings", {}).get("max_market_broken_rate_pct", 40.0),
        )
        threshold = 40.0 if threshold is None else max(0.0, min(100.0, threshold))
        snapshot = self._market_sentiment_snapshot()
        if snapshot is None:
            return {
                "state": "unavailable",
                "blocked": False,
                "threshold_pct": threshold,
                "broken_rate_pct": None,
                "reason": "实时情绪快照暂不可用，未触发自动停手",
            }
        state = str(snapshot.get("state") or "unavailable")
        broken_rate = _finite(snapshot.get("market_broken_rate_pct"))
        blocked = state == "live" and broken_rate is not None and broken_rate >= threshold
        if blocked:
            reason = f"今日破板率 {broken_rate:.2f}% 已达到 {threshold:.2f}%，自动打板已停止"
        elif state == "stale":
            reason = f"{snapshot.get('as_of') or '--'} 收盘数据，仅供参考，未触发自动停手"
        elif state == "live" and broken_rate is not None:
            reason = f"今日破板率 {broken_rate:.2f}% 未达到停手阈值 {threshold:.2f}%"
        else:
            reason = "实时情绪快照缺少破板率，未触发自动停手"
        return {
            "state": state if state in {"live", "stale"} else "unavailable",
            "blocked": blocked,
            "threshold_pct": threshold,
            "broken_rate_pct": broken_rate,
            "reason": reason,
        }

    def _enqueue(self, payload: dict[str, Any]) -> None:
        try:
            self._queue.put_nowait(payload)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            pass

    def _on_market_fetch(self) -> None:
        config = self.store.load_config()
        self._refresh_history(config)
        symbols = {
            str(item["symbol"]).strip().upper()
            for key in ("selected", "board_pool")
            for item in config[key]
        }
        if self._market_mode() == "full_market":
            symbols.update(self._automatic_candidate_symbols(cn_today()))
        self._refresh_symbol_consumer(symbols)
        fresh_payload = self._fresh_tickflow_quotes(symbols)
        quotes = [dict(row) for row in (fresh_payload.get("quotes") or {}).values()]
        self._enqueue({"type": "market", "quotes": quotes})

    def _market_mode(self) -> str:
        provider_getter = getattr(self.quote_service, "realtime_provider", None)
        if callable(provider_getter):
            try:
                if str(provider_getter()).strip().lower() != "tickflow":
                    return "none"
            except Exception:  # noqa: BLE001
                return "none"
        mode_getter = getattr(self.quote_service, "realtime_mode", None)
        if not callable(mode_getter):
            return "full_market"
        try:
            return str(mode_getter())
        except Exception:  # noqa: BLE001
            return "none"

    def enqueue_depth(self, records: list[dict[str, Any]]) -> None:
        with self._lock:
            ws_symbols = set(self._ws_symbols)
        selected = [
            dict(record) for record in records
            if str(record.get("symbol") or "").strip().upper() in ws_symbols
        ]
        if selected:
            self._enqueue({"type": "depth", "records": selected})

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                order_result = self._order_results.get_nowait()
            except queue.Empty:
                order_result = None
            if order_result is not None:
                try:
                    self._apply_auto_order_result(order_result)
                except Exception:  # noqa: BLE001
                    logger.exception("打板自动委托结果处理失败")
                continue
            try:
                payload = self._queue.get(timeout=1)
            except queue.Empty:
                try:
                    self._retry_history()
                except Exception:  # noqa: BLE001
                    logger.exception("打板历史数据自动重试失败")
                continue
            if payload.get("type") == "stop":
                break
            try:
                if payload.get("type") == "depth":
                    self._process_depth(payload.get("records") or [])
                else:
                    self._process_quotes(payload.get("quotes") or [])
            except Exception:  # noqa: BLE001
                logger.exception("打板专区行情处理失败")
                self._last_error = "行情处理失败，请查看后端日志"

    def _runtime_for_today(self) -> dict[str, Any]:
        today = cn_today().isoformat()
        runtime = self.store.load_runtime()
        if runtime.get("trading_date") != today:
            runtime = {
                "trading_date": today,
                "symbols": {},
                "blacklist": [],
                "candidate_excluded": [],
            }
            self.store.save_runtime(runtime)
            self._depth.clear()
        return runtime

    def _refresh_history(self, config: dict[str, Any]) -> None:
        today = cn_today()
        now_mono = time.monotonic()
        if self._history_date == today:
            retry_seconds = (
                _HISTORY_WARMUP_RETRY_SECONDS
                if self._history_reason == _HISTORY_WARMUP_REASON
                else _HISTORY_RETRY_SECONDS
            )
            if self._history_ready or now_mono - self._history_attempt_at < retry_seconds:
                return
        self._history_date = today
        self._history_attempt_at = now_mono
        self._history_ready = False
        self._first_board_eligible.clear()
        self._rebound_board_eligible.clear()
        self._premium_stats.clear()
        lookback = max(1, int(config["settings"].get("first_board_lookback_days", 10)))
        latest, latest_date = self.repo.get_enriched_latest()
        if latest_date is None:
            self._history_reason = _HISTORY_WARMUP_REASON
            return
        end = min(latest_date, today - timedelta(days=1))
        start = end - timedelta(days=max(30, lookback * 3))
        history = self.repo.get_enriched_range(
            start, end, columns=["symbol", "date", "signal_limit_up", "signal_broken_limit_up"],
        )
        if history is None:
            self._history_reason = _HISTORY_WARMUP_REASON
            return
        if history.is_empty() or "signal_limit_up" not in history.columns:
            self._history_reason = "近 10 个交易日涨停记录不足，首板/反包扫描已暂停"
            return
        dates = history["date"].unique().sort().to_list()
        if len(dates) < lookback:
            self._history_reason = f"仅有 {len(dates)} 个交易日记录，需要 {lookback} 个"
            return
        selected_dates = dates[-lookback:]
        scoped = history.filter(pl.col("date").is_in(selected_dates))
        blocked = set(
            scoped.filter(pl.col("signal_limit_up").fill_null(False).cast(pl.Boolean))["symbol"].to_list()
        )
        rebound = set()
        if "signal_broken_limit_up" not in scoped.columns:
            scoped = scoped.with_columns(pl.lit(False).alias("signal_broken_limit_up"))
        # 反包要求窗口内先有涨停，随后至少出现一天炸板或断板，且最近一天不能仍是涨停。
        for symbol, rows in scoped.sort(["symbol", "date"]).group_by("symbol", maintain_order=True):
            saw_limit = False
            had_break_after_limit = False
            last_was_limit = False
            for row in rows.iter_rows(named=True):
                is_limit = bool(row.get("signal_limit_up"))
                is_broken = bool(row.get("signal_broken_limit_up"))
                if saw_limit and (is_broken or not is_limit):
                    had_break_after_limit = True
                if is_limit:
                    saw_limit = True
                last_was_limit = is_limit
            if saw_limit and had_break_after_limit and not last_was_limit:
                rebound.add(symbol[0] if isinstance(symbol, tuple) else symbol)
        instruments = self.repo.get_instruments()
        universe = set(instruments["symbol"].to_list()) if not instruments.is_empty() else set()
        self._refresh_name_map()
        universe = {
            symbol for symbol in universe
            if not is_risk_warning_name(self._name_map.get(str(symbol).strip().upper()))
        }
        try:
            premium_rows = premium_gene.refresh(self.repo)
        except Exception:  # noqa: BLE001
            logger.warning("打板专区读取溢价基因快照失败", exc_info=True)
            premium_rows = pl.DataFrame()
        if (
            premium_rows is None
            or premium_rows.is_empty()
            or not _PREMIUM_FILTER_COLUMNS.issubset(premium_rows.columns)
        ):
            self._history_reason = "溢价基因数据不足，自动首板/反包已暂停"
            return
        self._premium_stats = _premium_stats_by_symbol(premium_rows)
        self._first_board_eligible = universe - blocked
        self._rebound_board_eligible = (rebound & universe) - self._first_board_eligible
        self._history_ready = True
        self._history_reason = (
            f"已核对前 {lookback} 个交易日；自动候选仅来自实时板块强度前 10 名，"
            "涨停基因用于 30 分个股排序"
        )

    def _retry_history(self) -> None:
        if self._history_ready:
            return
        previous_ready = self._history_ready
        previous_reason = self._history_reason
        self._refresh_history(self.store.load_config())
        if self._history_ready and not previous_ready:
            self._on_market_fetch()
        if self._history_ready != previous_ready or self._history_reason != previous_reason:
            self._notify_updated()

    def _refresh_name_map(self) -> None:
        today = cn_today()
        if self._name_map_date == today:
            return
        try:
            names = self.repo.get_name_map()
        except Exception:  # noqa: BLE001
            logger.warning("打板专区读取本地证券名称失败", exc_info=True)
            names = {}
        self._name_map = {
            str(symbol).strip().upper(): str(name).strip()
            for symbol, name in names.items()
            if str(symbol).strip() and str(name).strip()
        }
        self._name_map_date = today

    @staticmethod
    def _is_display_name(value: object, symbol: str) -> bool:
        name = str(value or "").strip()
        return bool(name) and name.upper() not in {symbol, symbol.split(".", 1)[0]}

    def _resolve_name(self, symbol: str, quote_name: object = None) -> str:
        self._refresh_name_map()
        authoritative = self._name_map.get(symbol)
        if self._is_display_name(authoritative, symbol):
            return str(authoritative)
        if self._is_display_name(quote_name, symbol):
            return str(quote_name).strip()
        return symbol

    @staticmethod
    def _preselect_automatic_updates(
        updates: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        retained = {
            symbol
            for symbol, quote in updates.items()
            if {"selected", "board_pool"} & set(quote.get("source_modes") or [])
        }
        by_sector: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for symbol, quote in updates.items():
            modes = set(quote.get("source_modes") or [])
            if not {"first_board", "rebound_board"} & modes:
                continue
            plate_ids = [str(value) for value in quote.get("top_sector_ids") or [] if value]
            if not plate_ids:
                retained.add(symbol)
                continue
            for plate_id in plate_ids:
                by_sector[plate_id].append(quote)

        def rank_key(quote: dict[str, Any]) -> tuple[float, float, float, str]:
            change_pct = _finite(quote.get("change_pct"))
            limit_gap_pct = _finite(quote.get("limit_gap_pct"))
            amount = _finite(quote.get("amount"))
            return (
                -(change_pct if change_pct is not None else float("-inf")),
                limit_gap_pct if limit_gap_pct is not None else float("inf"),
                -(amount if amount is not None else 0.0),
                str(quote.get("symbol") or ""),
            )

        for quotes in by_sector.values():
            retained.update(
                str(quote["symbol"])
                for quote in sorted(quotes, key=rank_key)[:_AUTOMATIC_CANDIDATES_PER_SECTOR]
            )
        return {symbol: quote for symbol, quote in updates.items() if symbol in retained}

    def _process_quotes(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        self._refresh_history(config)
        full_market = self._market_mode() == "full_market"
        runtime = self._runtime_for_today()
        selected = {str(item["symbol"]).strip().upper() for item in config["selected"]}
        board_pool = {str(item["symbol"]).strip().upper() for item in config["board_pool"]}
        now = cn_now()
        automatic_candidates = (
            self._automatic_candidate_symbols(now.date()) if full_market else set()
        )
        excluded = {
            str(symbol).strip().upper()
            for symbol in runtime.get("candidate_excluded") or []
        }
        runtime_by_symbol = runtime.setdefault("symbols", {})
        for symbol, state in list(runtime_by_symbol.items()):
            modes = set(state.get("source_modes") or [])
            modes.difference_update({"first_board", "rebound_board"})
            if symbol in selected:
                modes.add("selected")
            if symbol in board_pool:
                modes.add("board_pool")
            if symbol in automatic_candidates and symbol not in excluded:
                if symbol in self._first_board_eligible:
                    modes.add("first_board")
                if symbol in self._rebound_board_eligible:
                    modes.add("rebound_board")
            if modes:
                state["source_modes"] = sorted(modes)
            else:
                runtime_by_symbol.pop(symbol, None)
        updates: dict[str, dict[str, Any]] = {}
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            source = str(raw.get("source") or "").strip().lower()
            if source and source != "tickflow":
                continue
            quote_at = _quote_time(raw.get("timestamp"))
            if quote_at is None or quote_at.date() != now.date():
                continue
            price = _finite(raw.get("last_price", raw.get("close")))
            if price is None or price <= 0:
                continue
            name = self._resolve_name(symbol, raw.get("name"))
            if is_risk_warning_name(name):
                continue
            limit_up = self._limit_up(raw, symbol, name, now.date())
            gap = max(0.0, limit_up / price - 1.0) if limit_up is not None else None
            source_modes = []
            if symbol in board_pool:
                source_modes.append("board_pool")
            if symbol in selected:
                source_modes.append("selected")
            if symbol in automatic_candidates and symbol not in excluded:
                if symbol in self._first_board_eligible:
                    source_modes.append("first_board")
                if symbol in self._rebound_board_eligible:
                    source_modes.append("rebound_board")
            if not source_modes:
                continue
            quote = {
                **raw,
                **self._premium_stats.get(symbol, {}),
                "symbol": symbol,
                "name": name,
                "last_price": price,
                "source": "tickflow",
                "limit_up": limit_up,
                "limit_gap_pct": gap,
                "timestamp": quote_at.isoformat(),
                "source_modes": source_modes,
                "top_sector_ids": [
                    item["plate_id"]
                    for item in self._sector_candidates_by_symbol.get(symbol, [])
                ],
                "top_sector_names": [
                    item["plate_name"]
                    for item in self._sector_candidates_by_symbol.get(symbol, [])
                    if item.get("plate_name")
                ],
            }
            updates[symbol] = quote
        updates = self._preselect_automatic_updates(updates)
        with self._lock:
            self._quotes.update(updates)
        self._evaluate_quotes(updates, runtime, config)
        rows, selected_rows, board_rows = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected_rows, board_rows,
        )
        scoring_rows = {
            str(item.get("symbol") or "").strip().upper(): item
            for item in self._strong_rows(rows)
            if item.get("symbol")
        }
        self._refresh_candidate_scores(runtime, list(scoring_rows.values()), now)
        self._trim_automatic_candidates(runtime)
        self._sync_websocket(runtime, config)
        self._last_scan_at = now.isoformat()
        self._persist_runtime(runtime)
        self._notify_updated()

    @staticmethod
    def _limit_up(raw: dict[str, Any], symbol: str, name: str, trading_date: date) -> float | None:
        authoritative = _finite(raw.get("limit_up"))
        if authoritative is not None:
            if authoritative >= 10_000:
                # instruments uses a large sentinel for IPO/new-stock sessions
                # without a daily price limit. Do not infer a normal board limit.
                return None
            if authoritative > 0:
                return authoritative
        previous = _finite(raw.get("prev_close"))
        if previous is None or previous <= 0:
            return None
        pct = price_limit_pct(
            symbol, trading_date, is_risk_warning=is_risk_warning_name(name),
        )
        return limit_price(previous, pct, up=True)

    def _evaluate_quotes(
        self, updates: dict[str, dict[str, Any]], runtime: dict[str, Any], config: dict[str, Any],
    ) -> None:
        now = cn_now()
        trading_time = _is_trading_time(now)
        now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
        symbols = runtime.setdefault("symbols", {})
        blacklist = set(runtime.setdefault("blacklist", []))
        for symbol, quote in updates.items():
            quote_at = _quote_time(quote.get("timestamp"))
            if quote_at is None or quote_at.date() != now_aware.date():
                continue
            if trading_time and (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS:
                continue
            state = symbols.setdefault(symbol, {})
            state.update({
                "name": quote["name"],
                "last_price": quote["last_price"],
                "change_pct": quote.get("change_pct"),
                "limit_up": quote["limit_up"],
                "limit_gap_pct": quote["limit_gap_pct"],
                "source_modes": quote["source_modes"],
                "top_sector_ids": quote.get("top_sector_ids") or [],
                "top_sector_names": quote.get("top_sector_names") or [],
                "last_quote_at": quote["timestamp"],
            })
            state.update({
                key: value for key, value in self._premium_stats.get(symbol, {}).items()
            })
            state.setdefault("status", "watching")
            if symbol in blacklist:
                state["status"] = "blacklisted"
                continue
            if (
                not trading_time
                or quote.get("limit_up") is None
                or quote.get("limit_gap_pct") is None
            ):
                continue
            at_limit = quote["last_price"] >= quote["limit_up"] - 0.005
            if at_limit and not state.get("touched"):
                state["touched"] = True
                state["touched_at"] = now.isoformat()
                state["status"] = "touched"
                self._emit("touched", quote, state, config, "首次触及涨停价")
            if at_limit:
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="limit_touch",
                )
            if state.get("sealed") and not at_limit:
                self._mark_broken(quote, state, runtime, config, "价格离开涨停价")
            elif state.get("sealed"):
                state["status"] = "sealed"
            elif state.get("had_broken"):
                state["status"] = "broken"
            elif at_limit:
                state["status"] = "touched"
            elif quote["limit_gap_pct"] <= float(config["settings"].get("near_limit_pct", 0.02)):
                state["status"] = "near_limit"
            else:
                state["status"] = "watching"

    def _process_depth(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        sweep_price_levels = int(config["settings"].get("sweep_price_levels", 5))
        queue_confirm_snapshots = int(
            config["settings"].get("queue_confirm_snapshots", 0),
        )
        runtime = self._runtime_for_today()
        now = cn_now()
        if not _is_trading_time(now):
            return
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            quote = self._quotes.get(symbol)
            state = runtime.setdefault("symbols", {}).get(symbol)
            if not quote or not state or symbol in set(runtime.get("blacklist") or []):
                continue
            quote_at = _quote_time(quote.get("timestamp"))
            now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
            if quote_at is None or (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS:
                continue
            normalized = self._normalize_depth(raw, now)
            if normalized is None:
                continue
            self._depth[symbol].append(normalized)
            recent = [
                item for item in self._depth[symbol]
                if (now - item["timestamp"]).total_seconds() <= _DEPTH_FRESH_SECONDS
            ]
            self._depth[symbol] = deque(recent, maxlen=10)
            if not recent:
                continue
            sealed_flags = [self._sealed_snapshot(item, float(quote["limit_up"])) for item in recent]
            confirmed = len(sealed_flags) >= 3 and all(sealed_flags[-3:])
            latest_sealed = sealed_flags[-1]
            consecutive_sealed = 0
            for is_sealed in reversed(sealed_flags):
                if not is_sealed:
                    break
                consecutive_sealed += 1
            state["bid1_volume"] = normalized["bid_volumes"][0] if normalized["bid_volumes"] else 0.0
            state["ask1_volume"] = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
            state["last_depth_at"] = normalized["timestamp"].isoformat()
            if _sweep_ready(
                normalized,
                float(quote["limit_up"]),
                sweep_price_levels,
            ):
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="sweep",
                )
            if queue_confirm_snapshots > 0 and latest_sealed:
                self._maybe_auto_trade(
                    symbol,
                    quote,
                    state,
                    config,
                    runtime=runtime,
                    trigger_mode="queue",
                    queue_confirmed_snapshots=consecutive_sealed,
                )
            if state.get("sealed") and not latest_sealed:
                self._mark_broken(quote, state, runtime, config, "卖一恢复，封板状态中断")
            elif confirmed and not state.get("sealed"):
                state["sealed"] = True
                state["sealed_at"] = now.isoformat()
                if state.get("had_broken"):
                    state["had_broken"] = False
                    state["status"] = "resealed"
                    self._emit("resealed", quote, state, config, "连续 3 个五档快照确认回封")
                else:
                    state["status"] = "sealed"
            elif state.get("sealed"):
                state["status"] = "sealed"
        self._sync_websocket(runtime, config)
        self._persist_runtime(runtime)
        self._notify_updated()

    @staticmethod
    def _normalize_depth(raw: dict[str, Any], now: datetime) -> dict[str, Any] | None:
        try:
            bid_prices = [float(value) for value in (raw.get("bid_prices") or [])[:10]]
            bid_volumes = [float(value) for value in (raw.get("bid_volumes") or [])[:10]]
            ask_prices = [float(value) for value in (raw.get("ask_prices") or [])[:10]]
            ask_volumes = [float(value) for value in (raw.get("ask_volumes") or [])[:10]]
        except (TypeError, ValueError):
            return None
        timestamp = _quote_time(raw.get("timestamp")) or now
        return {
            "timestamp": timestamp,
            "bid_prices": bid_prices,
            "bid_volumes": bid_volumes,
            "ask_prices": ask_prices,
            "ask_volumes": ask_volumes,
        }

    @staticmethod
    def _sealed_snapshot(depth: dict[str, Any], limit_up: float) -> bool:
        bid_price = depth["bid_prices"][0] if depth["bid_prices"] else 0.0
        bid_volume = depth["bid_volumes"][0] if depth["bid_volumes"] else 0.0
        ask_price = depth["ask_prices"][0] if depth["ask_prices"] else 0.0
        ask_volume = depth["ask_volumes"][0] if depth["ask_volumes"] else 0.0
        return abs(bid_price - limit_up) < 0.001 and bid_volume > 0 and not (ask_price and ask_volume)

    def _mark_broken(
        self,
        quote: dict[str, Any],
        state: dict[str, Any],
        runtime: dict[str, Any],
        config: dict[str, Any],
        reason: str,
    ) -> None:
        if not state.get("sealed"):
            return
        state["sealed"] = False
        state["had_broken"] = True
        state["status"] = "broken"
        state["break_count"] = int(state.get("break_count") or 0) + 1
        state["last_broken_at"] = cn_now().isoformat()
        threshold = int(config["settings"].get("blacklist_after_breaks", 3))
        blacklisted = state["break_count"] > threshold
        if blacklisted:
            state["status"] = "blacklisted"
            state["blacklisted_at"] = cn_now().isoformat()
            values = set(runtime.setdefault("blacklist", []))
            values.add(quote["symbol"])
            runtime["blacklist"] = sorted(values)
            reason = f"{reason}；今日第 {state['break_count']} 次炸板，已纳入黑名单"
        else:
            reason = f"{reason}；今日第 {state['break_count']} 次炸板"
        self._emit("broken", quote, state, config, reason)

    def _maybe_auto_trade(
        self,
        symbol: str,
        quote: dict[str, Any],
        state: dict[str, Any],
        config: dict[str, Any],
        *,
        runtime: dict[str, Any] | None = None,
        trigger_mode: str = "queue",
        queue_confirmed_snapshots: int = 0,
    ) -> None:
        if quote.get("source") == "kaipanla_socket":
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "开盘啦成分行情缺少五档盘口与可验证逐笔时效，已阻止自动委托"
            return
        member = next(
            (item for item in config["board_pool"] if str(item.get("symbol")).strip().upper() == symbol),
            None,
        )
        if not member or not bool(member.get("auto_trade")) or state.get("auto_order_key"):
            return
        order_mode = str(member.get("order_mode") or "sweep")
        if trigger_mode != "limit_touch" and order_mode != trigger_mode:
            return
        if trigger_mode == "limit_touch" and order_mode not in {"sweep", "queue"}:
            return
        if order_mode == "queue":
            required_snapshots = int(
                config["settings"].get("queue_confirm_snapshots", 0),
            )
            if required_snapshots > 0 and (
                not state.get("touched")
                or queue_confirmed_snapshots < required_snapshots
            ):
                return
            wait_seconds = int(config["settings"].get("queue_wait_seconds", 0))
            if wait_seconds > 0:
                touched_at = _quote_time(state.get("touched_at"))
                now = cn_now()
                now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
                if (
                    touched_at is None
                    or (now_aware - touched_at).total_seconds() < wait_seconds
                ):
                    return
        max_boards = int(config["settings"].get("max_auto_board_count", 0))
        if max_boards > 0:
            active_runtime = runtime if runtime is not None else self._runtime_for_today()
            used_boards = sum(
                1
                for item in (active_runtime.get("symbols") or {}).values()
                if item.get("auto_order_key")
            )
            if used_boards >= max_boards:
                state["auto_order_status"] = "blocked"
                state["auto_order_error"] = f"已达到每日自动打板上限（{max_boards} 只）"
                return
        if is_risk_warning_name(self._resolve_name(symbol, quote.get("name"))):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "ST 风险警示股票已被打板专区过滤"
            return
        sentiment_guard = self._sentiment_guard(config)
        if sentiment_guard["blocked"]:
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = sentiment_guard["reason"]
            return
        qmt = self._qmt()
        qmt_status = qmt.status() if qmt is not None else {}
        if not (
            qmt_status.get("configured")
            and qmt_status.get("state") == "ready"
            and qmt_status.get("trade_enabled")
        ):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = str(
                qmt_status.get("reason") or "QMT 实盘交易未就绪",
            )
            return
        if not self._order_slots.acquire(blocking=False):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "自动委托队列已满"
            return
        volume, volume_error = self._auto_order_volume(
            float(quote["limit_up"]), config,
        )
        if volume_error:
            self._order_slots.release()
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = volume_error
            return
        key = f"limit-board-{cn_today().strftime('%Y%m%d')}-{symbol}"
        trigger_at = cn_now().isoformat(timespec="milliseconds")
        system_order_at = cn_now().isoformat(timespec="milliseconds")
        state.update({
            "auto_order_key": key,
            "auto_order_status": "submitting",
            "auto_order_mode": order_mode,
            "auto_order_error": None,
            "auto_order_at": system_order_at,
            "auto_order_trigger_at": trigger_at,
            "auto_order_system_at": system_order_at,
            "auto_order_volume": volume,
            "auto_order_amount": round(float(quote["limit_up"]) * volume, 2),
        })
        try:
            self._order_executor.submit(
                self._submit_auto_order,
                symbol,
                float(quote["limit_up"]),
                key,
                volume,
                trigger_at,
                system_order_at,
            )
        except RuntimeError as exc:
            self._order_slots.release()
            state["auto_order_status"] = "unknown"
            state["auto_order_error"] = str(exc)

    @staticmethod
    def _auto_order_volume(
        limit_up: float, config: dict[str, Any],
    ) -> tuple[int, str | None]:
        amount = _finite(config["settings"].get("order_amount_per_board", 0))
        if amount is None or amount < 0:
            return 0, "单板下单资金配置无效"
        if amount == 0:
            return 100, None
        if limit_up <= 0 or limit_up != limit_up:
            return 0, "涨停价无效，已阻止自动委托"
        volume = int(amount / (limit_up * 100)) * 100
        if volume < 100:
            return 0, "单板下单资金不足一手，已阻止自动委托"
        return volume, None

    def _submit_auto_order(
        self,
        symbol: str,
        limit_up: float,
        key: str,
        volume: int,
        trigger_at: str,
        system_order_at: str,
    ) -> None:
        qmt = self._qmt()
        try:
            if qmt is None:
                raise RuntimeError("QMT 交易网关未初始化")
            order = qmt.submit_order({
                "idempotency_key": key,
                "strategy_name": "limit_board",
                "action": "BUY",
                "symbol": symbol,
                "volume": volume,
                "price": limit_up,
                "price_type": "LIMIT",
                "trigger_at": trigger_at,
                "system_order_at": system_order_at,
            })
            result = {
                "symbol": symbol,
                "key": key,
                "status": str(order.get("status") or "unknown"),
                "order_sys_id": order.get("order_sys_id"),
                "error": order.get("error"),
                "trigger_at": order.get("trigger_at") or trigger_at,
                "system_order_at": order.get("system_order_at") or system_order_at,
                "qmt_submit_at": order.get("qmt_submit_at"),
                "qmt_accepted_at": order.get("qmt_accepted_at"),
                "broker_order_at": order.get("broker_order_at"),
                "broker_order_time_raw": order.get("broker_order_time_raw"),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("打板自动委托提交失败: %s", exc)
            result = {
                "symbol": symbol,
                "key": key,
                "status": "unknown",
                "order_sys_id": None,
                "error": str(exc),
                "trigger_at": trigger_at,
                "system_order_at": system_order_at,
                "qmt_submit_at": None,
                "qmt_accepted_at": None,
                "broker_order_at": None,
                "broker_order_time_raw": None,
            }
        try:
            self._order_results.put(result)
        finally:
            self._order_slots.release()

    def _apply_auto_order_result(self, result: dict[str, Any]) -> None:
        runtime = self._runtime_for_today()
        symbol = str(result.get("symbol") or "").strip().upper()
        state = runtime.setdefault("symbols", {}).setdefault(symbol, {})
        if state.get("auto_order_key") != result.get("key"):
            return
        state.update({
            "auto_order_status": result.get("status") or "unknown",
            "auto_order_sys_id": result.get("order_sys_id"),
            "auto_order_error": result.get("error"),
            "auto_order_updated_at": cn_now().isoformat(),
            "auto_order_trigger_at": result.get("trigger_at"),
            "auto_order_system_at": result.get("system_order_at"),
            "auto_order_qmt_submit_at": result.get("qmt_submit_at"),
            "auto_order_qmt_accepted_at": result.get("qmt_accepted_at"),
            "auto_order_broker_at": result.get("broker_order_at"),
            "auto_order_broker_time_raw": result.get("broker_order_time_raw"),
        })
        self._persist_runtime(runtime)
        self._notify_updated()

    def _sync_websocket(self, runtime: dict[str, Any], config: dict[str, Any]) -> None:
        # The shortline workspace now uses Kaipanla constituent snapshots only.
        # They have no verifiable per-tick timestamp or five-level order book.
        hub = self._hub()
        if hub is not None and self._ws_registered:
            try:
                hub.unregister(_ACCOUNT_ID)
            except Exception:  # noqa: BLE001
                logger.debug("打板专区移除 WS 订阅失败", exc_info=True)
        self._ws_registered = False
        self._ws_symbols.clear()
        return

        hub = self._hub()
        if hub is None:
            self._last_error = "共享 WebSocket Hub 不可用"
            return
        blacklist = set(runtime.get("blacklist") or [])
        near = float(config["settings"].get("near_limit_pct", 0.02))
        exit_gap = float(config["settings"].get("exit_limit_pct", 0.03))
        exit_sustain = float(config["settings"].get("exit_sustain_seconds", 30))
        now = cn_now()
        states = runtime.setdefault("symbols", {})
        retained: set[str] = set()
        for symbol in self._ws_symbols:
            quote = self._quotes.get(symbol)
            state = states.setdefault(symbol, {})
            if (
                quote is None
                or symbol in blacklist
                or "board_pool" not in quote.get("source_modes", [])
            ):
                state.pop("ws_exit_since", None)
                continue
            if float(quote.get("limit_gap_pct") or 1) <= exit_gap:
                state.pop("ws_exit_since", None)
                retained.add(symbol)
                continue
            exit_since = _quote_time(state.get("ws_exit_since"))
            if exit_since is None:
                state["ws_exit_since"] = now.isoformat()
                retained.add(symbol)
            elif (now - exit_since).total_seconds() < exit_sustain:
                retained.add(symbol)
        candidates = [
            quote for symbol, quote in self._quotes.items()
            if symbol not in blacklist
            and "board_pool" in quote.get("source_modes", [])
            and (float(quote.get("limit_gap_pct") or 1) <= near or symbol in retained)
        ]
        candidates.sort(key=lambda item: (
            0 if "board_pool" in item.get("source_modes", []) else
            1 if "selected" in item.get("source_modes", []) else 2,
            float(item.get("limit_gap_pct") or 1),
            -float(item.get("amount") or 0),
        ))
        available = hub.websocket_available(exclude=_ACCOUNT_ID)
        desired = {str(item["symbol"]) for item in candidates[:available]}
        try:
            if not desired:
                if self._ws_registered:
                    hub.unregister(_ACCOUNT_ID)
                self._ws_registered = False
                self._ws_symbols.clear()
                return
            if self._ws_registered:
                hub.update_symbols(_ACCOUNT_ID, desired)
            else:
                hub.register(_ACCOUNT_ID, "websocket", desired, "stock", self._queue)
                self._ws_registered = True
            self._ws_symbols = desired
            self._last_error = None
        except (ValueError, RuntimeError) as exc:
            self._last_error = str(exc)

    def _emit(
        self, event_type: str, quote: dict[str, Any], state: dict[str, Any], config: dict[str, Any], reason: str,
    ) -> None:
        labels = {"touched": "涨停", "broken": "炸板", "resealed": "回封"}
        now = cn_now()
        name = self._resolve_name(str(quote["symbol"]), quote.get("name"))
        event = {
            "ts": int(now.timestamp() * 1000),
            "trigger_at": now.isoformat(timespec="milliseconds"),
            "trading_date": now.date().isoformat(),
            "source": "limit_board",
            "type": event_type,
            "rule_id": f"limit_board_{event_type}",
            "rule_name": labels[event_type],
            "symbol": quote["symbol"],
            "name": name,
            "message": f"{name}：{labels[event_type]}",
            "severity": "critical" if event_type == "broken" else "warn",
            "price": quote["last_price"],
            "limit_up": quote["limit_up"],
            "limit_gap_pct": quote["limit_gap_pct"],
            "break_count": int(state.get("break_count") or 0),
            "blacklisted": state.get("status") == "blacklisted",
            "reasons": [reason],
        }
        event["event_identity"] = self.store.event_identity(event)
        enrich = getattr(self.quote_service, "enrich_external_alerts", None)
        if callable(enrich):
            enrich([event])
        if not self.store.append_event_once(event):
            return
        alert_store.append(self.store.root.parents[1], event)
        enabled = bool(config["settings"].get("notifications", {}).get(event_type, True))
        if enabled:
            publish = getattr(self.quote_service, "publish_external_alerts", None)
            if callable(publish):
                publish([event])
            else:
                self.quote_service.push_alerts([event])

    def _persist_runtime(self, runtime: dict[str, Any]) -> None:
        self.store.save_runtime(runtime)

    def _notify_updated(self) -> None:
        notify = getattr(self.quote_service, "notify_limit_board_updated", None)
        if callable(notify):
            notify()

    def _refresh_symbol_consumer(self, additional_symbols: set[str] | None = None) -> None:
        setter = getattr(self.quote_service, "set_symbol_consumer", None)
        if not callable(setter):
            return
        config = self.store.load_config()
        symbols = {
            str(item.get("symbol") or "").strip().upper()
            for key in ("selected", "board_pool")
            for item in config.get(key, [])
            if str(item.get("symbol") or "").strip()
        }
        with self._lock:
            symbols.update(self._sector_quote_symbols)
            symbols.update(self._heat_quote_symbols)
        symbols.update({
            str(symbol).strip().upper()
            for symbol in additional_symbols or set()
            if str(symbol).strip()
        })
        setter(_ACCOUNT_ID, symbols)

    def _enrich_concepts(self, rows: list[dict[str, Any]]) -> None:
        enrich = getattr(self.quote_service, "enrich_external_alerts", None)
        if not rows or not callable(enrich):
            return
        try:
            enrich(rows)
        except Exception:  # noqa: BLE001
            logger.debug("打板专区题材富化失败", exc_info=True)

    @staticmethod
    def _candidate_pool(
        first_board: list[dict[str, Any]],
        rebound_board: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build the user-approval queue without enabling orders implicitly."""
        pool_symbols = {
            str(row.get("symbol") or "").strip().upper()
            for row in board_pool
        }
        candidates: dict[str, dict[str, Any]] = {}
        for row, origin in [
            *((item, "first_board") for item in first_board),
            *((item, "rebound_board") for item in rebound_board),
            *((item, "selected") for item in selected),
        ]:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol or symbol in pool_symbols:
                continue
            current = candidates.get(symbol)
            if current is None:
                modes = sorted(set(row.get("source_modes") or []) | {origin})
                current = {
                    **row,
                    "source": (
                        "first_board" if "first_board" in modes
                        else "rebound_board" if "rebound_board" in modes
                        else "manual"
                    ),
                    "source_modes": modes,
                }
                candidates[symbol] = current
            else:
                modes = set(current.get("source_modes") or [])
                modes.update(row.get("source_modes") or [])
                modes.add(origin)
                current["source_modes"] = sorted(modes)
                if current.get("name") in (None, "", symbol):
                    current["name"] = row.get("name")

        return list(candidates.values())

    @staticmethod
    def _strong_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return rows whose scores are shown in the strong-stock panel."""
        return [
            row for row in rows
            if {"first_board", "rebound_board"} & set(row.get("source_modes", []))
        ]

    @staticmethod
    def _rank_candidates(
        candidates: list[dict[str, Any]], score_cache: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result = []
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "").strip().upper()
            score = score_cache.get(symbol) or {
                "candidate_score": None,
                "candidate_rank": None,
                "candidate_score_state": "unavailable",
                "candidate_score_as_of": None,
                "candidate_score_detail": {},
                "candidate_reasons": LimitBoardService._score_reasons(candidate, {}),
            }
            result.append({**candidate, **score})
        def sort_key(row: dict[str, Any]) -> tuple:
            detail = row.get("candidate_score_detail") or {}
            sector = detail.get("sector") or {}
            intraday = detail.get("intraday_flow") or {}
            gene = detail.get("premium_gene") or {}
            technical = detail.get("technical") or {}
            realtime_rank = _finite(sector.get("realtime_rank"))
            leadership_rank = {
                "leader": 2,
                "front": 1,
                "follower": 0,
            }.get(str(sector.get("leadership") or ""), 0)
            stock_rank = _finite(sector.get("stock_rank"))
            return (
                row.get("candidate_score") is None,
                not bool(sector.get("realtime_available")),
                realtime_rank if realtime_rank is not None else float("inf"),
                -float(_finite(sector.get("realtime_strength")) or 0.0),
                -float(_finite(sector.get("score")) or 0.0),
                -leadership_rank,
                stock_rank if stock_rank is not None else float("inf"),
                -float(row.get("candidate_score") or 0.0),
                -float(gene.get("score") or 0.0),
                -float(intraday.get("score") or 0.0),
                -float(technical.get("score") or 0.0),
                str(row.get("symbol") or ""),
            )

        result.sort(key=sort_key)
        rank = 0
        for row in result:
            if row.get("candidate_score") is None:
                row["candidate_rank"] = None
                continue
            rank += 1
            row["candidate_rank"] = rank
        return result

    @staticmethod
    def _order_timeline(order: dict[str, Any]) -> dict[str, Any]:
        system_order_at = order.get("system_order_at")
        broker_order_at = order.get("broker_order_at")
        system_time = _quote_time(system_order_at)
        broker_time = _quote_time(broker_order_at)
        delay_ms = None
        if system_time is not None and broker_time is not None:
            delay_ms = round((broker_time - system_time).total_seconds() * 1000)
        return {
            "idempotency_key": order.get("idempotency_key"),
            "status": order.get("status"),
            "order_sys_id": order.get("order_sys_id"),
            "trigger_at": order.get("trigger_at"),
            "system_order_at": system_order_at,
            "qmt_submit_at": order.get("qmt_submit_at"),
            "qmt_response_at": order.get("qmt_response_at"),
            "qmt_accepted_at": order.get("qmt_accepted_at"),
            "broker_order_at": broker_order_at,
            "broker_order_time_raw": order.get("broker_order_time_raw"),
            "broker_order_time_field": order.get("broker_order_time_field"),
            "system_to_broker_delay_ms": delay_ms,
            "error": order.get("error"),
        }

    def _trim_automatic_candidates(self, runtime: dict[str, Any]) -> set[str]:
        """Persist only the best scored automatic rows; manual tracking always survives."""
        runtime_by_symbol = runtime.setdefault("symbols", {})
        automatic_rows = [
            {"symbol": symbol, **state}
            for symbol, state in runtime_by_symbol.items()
            if {"first_board", "rebound_board"} & set(state.get("source_modes") or [])
        ]
        ranked = self._rank_candidates(
            automatic_rows,
            runtime.get("candidate_scores") or {},
        )
        scored_order = [
            str(row["symbol"]).strip().upper()
            for row in ranked
            if row.get("candidate_score") is not None
        ][:_AUTOMATIC_CANDIDATE_LIMIT]
        retained_order = [*scored_order]
        if len(retained_order) < _AUTOMATIC_CANDIDATE_LIMIT:
            retained_order.extend(
                str(row["symbol"]).strip().upper()
                for row in ranked
                if row.get("candidate_score") is None
                and str(row["symbol"]).strip().upper() not in retained_order
            )
            retained_order = retained_order[:_AUTOMATIC_CANDIDATE_LIMIT]
        retained = set(retained_order)
        for symbol, state in list(runtime_by_symbol.items()):
            modes = set(state.get("source_modes") or [])
            if symbol not in retained:
                modes.difference_update({"first_board", "rebound_board"})
            if modes:
                state["source_modes"] = sorted(modes)
            else:
                runtime_by_symbol.pop(symbol, None)
        kept_symbols = set(runtime_by_symbol)
        runtime["candidate_scores"] = {
            symbol: value
            for symbol, value in (runtime.get("candidate_scores") or {}).items()
            if symbol in kept_symbols
        }
        return retained

    def _view_collections(
        self, runtime: dict[str, Any], config: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        runtime_by_symbol = runtime.get("symbols", {})
        rows = []
        for symbol, state in runtime_by_symbol.items():
            if not state.get("source_modes"):
                continue
            row = {"symbol": symbol, **state, "ws_active": symbol in self._ws_symbols}
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                rows.append(row)
        rows.sort(key=lambda item: (
            0 if item.get("status") == "blacklisted" else 1,
            float(item.get("limit_gap_pct") or 1),
        ))

        selected = []
        for item in config["selected"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {
                **item,
                **runtime_by_symbol.get(symbol, {}),
                "ws_active": symbol in self._ws_symbols,
            }
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                selected.append(row)

        board_pool = []
        for item in config["board_pool"]:
            symbol = str(item["symbol"]).strip().upper()
            row = {
                "order_mode": "sweep",
                **item,
                **runtime_by_symbol.get(symbol, {}),
                "ws_active": symbol in self._ws_symbols,
            }
            row["name"] = self._resolve_name(symbol, row.get("name"))
            if not is_risk_warning_name(row["name"]):
                board_pool.append(row)
        return rows, selected, board_pool

    def _candidate_rows_for_runtime(
        self,
        runtime: dict[str, Any],
        rows: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        excluded = {
            str(symbol).strip().upper()
            for symbol in runtime.get("candidate_excluded") or []
        }
        eligible = [
            item for item in rows
            if str(item.get("symbol") or "").strip().upper() not in excluded
        ]
        return self._candidate_pool(
            [item for item in eligible if "first_board" in item.get("source_modes", [])],
            [item for item in eligible if "rebound_board" in item.get("source_modes", [])],
            [
                item for item in selected
                if str(item.get("symbol") or "").strip().upper() not in excluded
            ],
            board_pool,
        )

    def _rotation(self, kind: str, level: int | None, today: date) -> dict[str, Any]:
        if self._rotation_date != today:
            self._rotation_cache.clear()
            self._rotation_date = today
        key = (kind, level)
        cached = self._rotation_cache.get(key)
        if cached is not None:
            return cached
        try:
            value = rps_rotation.build_rps_rotation(self.repo, 7, kind, level)
        except Exception:  # noqa: BLE001
            logger.warning("打板备选池读取板块轮动失败: %s", kind, exc_info=True)
            return {}
        if value.get("dates") and value.get("columns"):
            self._rotation_cache[key] = value
        return value

    def _candidate_stock_snapshot(
        self, now: datetime,
    ) -> tuple[pl.DataFrame, dict[str, dict[str, Any]]]:
        getter = getattr(self.quote_service, "get_enriched_today", None)
        if not callable(getter):
            return pl.DataFrame(), {}
        stock_df, stock_date = getter()
        if stock_date != now.date() or stock_df is None or stock_df.is_empty():
            return pl.DataFrame(), {}
        columns = [column for column in stock_df.columns if column in _SCORE_STOCK_COLUMNS]
        if "symbol" not in columns:
            return pl.DataFrame(), {}
        rows = {}
        for raw in stock_df.select(columns).iter_rows(named=True):
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            rows[symbol] = {
                **raw,
                "symbol": symbol,
                "name": self._resolve_name(symbol, raw.get("name")),
            }
        return stock_df, rows

    def _candidate_intraday_features(
        self, symbols: set[str], now: datetime,
    ) -> dict[str, dict[str, Any]]:
        getter = getattr(self.quote_service, "get_intraday_features", None)
        if not callable(getter) or not symbols:
            return {}
        try:
            freshness_seconds = 180 if _is_trading_time(now) else 24 * 60 * 60
            value = getter(
                symbols,
                asset_type="stock",
                now=now,
                freshness_seconds=freshness_seconds,
            )
        except TypeError:
            try:
                value = getter(symbols)
            except Exception:  # noqa: BLE001
                logger.warning("打板备选池读取分时特征失败", exc_info=True)
                return {}
        except Exception:  # noqa: BLE001
            logger.warning("打板备选池读取分时特征失败", exc_info=True)
            return {}
        return value if isinstance(value, dict) else {}

    def _candidate_flow_snapshots(
        self, symbols: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Read one batched large-order snapshot and merge quote-level ratios."""
        result: dict[str, dict[str, Any]] = {}
        large_orders = getattr(self.app_state, "large_order_service", None)
        ranking = getattr(large_orders, "ranking", None)
        if callable(ranking):
            try:
                payload = ranking(window=60, scope="all", mode="combined")
                for row in (payload or {}).get("rows") or []:
                    symbol = str(row.get("symbol") or "").strip().upper()
                    if symbol in symbols:
                        result[symbol] = dict(row)
            except Exception:  # noqa: BLE001
                logger.debug("打板备选池读取大单资金快照失败", exc_info=True)
        with self._lock:
            quotes = {symbol: dict(self._quotes.get(symbol) or {}) for symbol in symbols}
        for symbol, quote in quotes.items():
            ratios = {
                key: quote.get(key)
                for key in ("buy_ratio", "sell_ratio", "net_flow_ratio")
                if _finite(quote.get(key)) is not None
            }
            if ratios:
                result[symbol] = {**result.get(symbol, {}), **ratios}
        return result

    @staticmethod
    def _scale_score_detail(
        value: dict[str, Any] | None,
        target_max: float,
        source_max: float,
    ) -> dict[str, Any] | None:
        if not value:
            return None
        source = _finite(value.get("max_score")) or source_max
        factor = target_max / source if source > 0 else 0.0
        result = dict(value)
        result["score"] = round(float(value.get("score") or 0.0) * factor, 2)
        result["max_score"] = target_max
        for key in (
            "current_score",
            "rotation_score",
            "trend_score",
            "trend_max_score",
            "capital_score",
            "capital_max_score",
        ):
            if _finite(value.get(key)) is not None:
                result[key] = round(float(value[key]) * factor, 2)
        components = value.get("components")
        if isinstance(components, dict):
            result["components"] = {
                key: round(float(component or 0.0) * factor, 2)
                for key, component in components.items()
            }
        return result

    @staticmethod
    def _score_reasons(
        candidate: dict[str, Any], detail: dict[str, dict[str, Any]],
    ) -> list[str]:
        modes = set(candidate.get("source_modes") or [])
        reasons = []
        if "first_board" in modes:
            reasons.append("首板候选")
        if "rebound_board" in modes:
            reasons.append("反包候选")
        if "selected" in modes:
            reasons.append("手工加入")
        sector = detail.get("sector") or {}
        gene = detail.get("premium_gene") or {}
        technical = detail.get("technical") or {}
        intraday_flow = detail.get("intraday_flow") or {}
        if intraday_flow:
            underwater = float(intraday_flow.get("underwater_ratio") or 0.0)
            net_flow = intraday_flow.get("net_flow_ratio")
            if intraday_flow.get("capital_available"):
                reasons.append(
                    f"分时强度/资金 {float(intraday_flow.get('score') or 0):.1f}/15"
                    f" · 水下 {underwater:.0%} · 净流向 {float(net_flow or 0):+.0%}"
                )
            else:
                reasons.append("分时强度已计算，实时资金数据待补")
        if sector:
            reasons.append(
                f"{sector.get('name') or '板块'} {sector.get('rotation_label') or '数据不足'}"
                f" · {sector.get('leadership') or 'follower'}"
            )
        if gene:
            reasons.append(f"涨停基因 {float(gene.get('score') or 0):.1f}/30")
        if technical:
            reasons.append(f"技术面 {float(technical.get('score') or 0):.1f}/5")
        return reasons

    def _refresh_candidate_scores(
        self,
        runtime: dict[str, Any],
        candidates: list[dict[str, Any]],
        now: datetime,
    ) -> bool:
        symbols = {
            str(item.get("symbol") or "").strip().upper()
            for item in candidates if item.get("symbol")
        }
        previous_cache = runtime.get("candidate_scores") or {}
        missing = any(symbol not in previous_cache for symbol in symbols)
        now_mono = time.monotonic()
        if not missing and now_mono - self._score_refresh_at < _SCORE_REFRESH_SECONDS:
            return False
        if not self._score_lock.acquire(blocking=False):
            return False
        try:
            now_mono = time.monotonic()
            previous_cache = runtime.get("candidate_scores") or {}
            missing = any(symbol not in previous_cache for symbol in symbols)
            if not missing and now_mono - self._score_refresh_at < _SCORE_REFRESH_SECONDS:
                return False
            _, stock_rows = self._candidate_stock_snapshot(now)
            intraday_features = self._candidate_intraday_features(symbols, now)
            large_orders = getattr(self.app_state, "large_order_service", None)
            set_score_symbols = getattr(large_orders, "set_score_symbols", None)
            if callable(set_score_symbols):
                try:
                    set_score_symbols(symbols)
                except Exception:  # noqa: BLE001
                    logger.debug("打板候选加入实时资金观察失败", exc_info=True)
            flow_snapshots = self._candidate_flow_snapshots(symbols)
            with self._lock:
                candidate_quotes = {
                    symbol: dict(self._quotes.get(symbol) or {})
                    for symbol in symbols
                }
            sector_service = getattr(self.app_state, "sector_monitor_service", None)
            targets_by_symbol: dict[str, dict[str, list[dict[str, Any]]]] = {}
            if sector_service is not None:
                for symbol in symbols:
                    concepts = sector_service.targets_for_symbol(symbol, kind="concept")
                    industries = sector_service.targets_for_symbol(
                        symbol, kind="industry", industry_level=2,
                    )
                    targets_by_symbol[symbol] = {
                        "concept": concepts,
                        "industry": industries,
                    }
            rotations = {
                "concept": self._rotation("concept", None, now.date()),
                "industry": self._rotation("industry", 2, now.date()),
            } if sector_service is not None else {}
            realtime_sectors = self._sector_strength_snapshot(now.date())

            refreshed: dict[str, dict[str, Any]] = {}
            for candidate in candidates:
                symbol = str(candidate.get("symbol") or "").strip().upper()
                previous = previous_cache.get(symbol) or {}
                previous_detail = previous.get("candidate_score_detail") or {}
                change_pct = _finite(candidate.get("change_pct"))
                if change_pct is None:
                    change_pct = _finite((stock_rows.get(symbol) or {}).get("change_pct"))
                if change_pct is None:
                    change_pct = _finite(previous.get("change_pct"))
                intraday_flow = self._scale_score_detail(
                    intraday_flow_detail(
                        intraday_features.get(symbol),
                        previous_close=(stock_rows.get(symbol) or {}).get("prev_close")
                        or candidate_quotes.get(symbol, {}).get("prev_close"),
                        external_flow=flow_snapshots.get(symbol),
                        as_of=now.isoformat(),
                    ),
                    _SCORE_WEIGHTS["intraday_flow"],
                    50.0,
                )
                sector = None
                symbol_targets = targets_by_symbol.get(symbol) or {}
                for kind in ("concept", "industry"):
                    available = []
                    for target in symbol_targets.get(kind, []):
                        key = str(target.get("key") or "")
                        realtime = (
                            realtime_sectors.get(str(target.get("name") or "").strip())
                            or realtime_sectors.get(
                                str(target.get("name") or "").split(" / ")[-1].strip()
                            )
                        )
                        if realtime is None:
                            continue
                        sector_inputs = self._kaipanla_sector_score_inputs(realtime)
                        if sector_inputs is None:
                            continue
                        sector_stock_rows, sector_members, sector_snapshot = sector_inputs
                        value = sector_detail(
                            symbol=symbol,
                            target=target,
                            snapshot=sector_snapshot,
                            rotation=rotations.get(kind) or {},
                            stock_rows=sector_stock_rows,
                            member_symbols=sector_members,
                            today=now.date(),
                            realtime=realtime,
                            realtime_snapshot=sector_snapshot,
                        )
                        if value is not None:
                            value["as_of"] = now.isoformat()
                            value["data_source"] = "kaipanla_socket"
                            available.append(value)
                    if available:
                        sector = max(
                            available,
                            key=lambda item: (
                                bool(item.get("realtime_available")),
                                -float(_finite(item.get("realtime_rank")) or float("inf")),
                                float(_finite(item.get("realtime_strength")) or 0.0),
                                float(item["score"]),
                                str(item.get("name") or ""),
                            ),
                        )
                        break
                sector = self._scale_score_detail(sector, _SCORE_WEIGHTS["sector"], 50.0)
                gene = self._scale_score_detail(
                    premium_gene_detail(self._premium_stats.get(symbol) or {}),
                    _SCORE_WEIGHTS["premium_gene"],
                    30.0,
                )
                technical = self._scale_score_detail(technical_detail(
                    stock_rows.get(symbol) or {}, as_of=now.isoformat(),
                ), _SCORE_WEIGHTS["technical"], 20.0)
                fresh = {
                    "intraday_flow": intraday_flow,
                    "sector": sector,
                    "premium_gene": gene,
                    "technical": technical,
                }
                detail = {}
                cached_component = False
                for key, value in fresh.items():
                    if value is not None:
                        detail[key] = value
                    elif (
                        key != "intraday_flow"
                        and previous_detail.get(key)
                        and (
                            key != "sector"
                            or previous_detail[key].get("realtime_available") is True
                        )
                    ):
                        detail[key] = previous_detail[key]
                        cached_component = True
                flow_detail = detail.get("intraday_flow") or {}
                complete = (
                    bool(flow_detail.get("capital_available"))
                    and all(detail.get(key) for key in _SCORE_WEIGHTS)
                    and bool((detail.get("sector") or {}).get("rotation_available", True))
                )
                base_score = (
                    sum(float(detail[key]["score"]) for key in _SCORE_WEIGHTS)
                    if complete else None
                )
                score = round(base_score, 1) if base_score is not None else None
                state = "cached" if complete and cached_component else "live" if complete else "unavailable"
                refreshed[symbol] = {
                    "change_pct": change_pct,
                    "candidate_score": score,
                    "candidate_rank": None,
                    "candidate_score_state": state,
                    "candidate_score_as_of": now.isoformat(),
                    "candidate_score_detail": detail,
                    "candidate_reasons": self._score_reasons(candidate, detail),
                }
            changed = refreshed != previous_cache
            runtime["candidate_scores"] = refreshed
            self._score_refresh_at = now_mono
            return changed
        finally:
            self._score_lock.release()

    def view(self) -> dict[str, Any]:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        self._refresh_sector_candidate_universe(cn_today())
        rows, selected, board_pool = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected, board_pool,
        )
        scoring_rows = {
            str(item.get("symbol") or "").strip().upper(): item
            for item in self._strong_rows(rows)
            if item.get("symbol")
        }
        if self._refresh_candidate_scores(runtime, list(scoring_rows.values()), cn_now()):
            self._persist_runtime(runtime)
        # Keep both API projections on the same per-symbol score snapshot. The
        # candidate queue only re-displays scores computed for strong-stock rows.
        score_cache = runtime.get("candidate_scores") or {}
        candidate_pool = self._rank_candidates(
            candidates, score_cache,
        )
        first_board = self._rank_candidates(
            self._strong_rows(rows),
            score_cache,
        )
        rebound_board = self._rank_candidates(
            [item for item in self._strong_rows(rows) if "rebound_board" in item.get("source_modes", [])],
            score_cache,
        )
        qmt = self._qmt()
        stored_events = self.store.events(runtime["trading_date"])
        order_keys = {
            f"limit-board-{str(event.get('trading_date') or '').replace('-', '')}-{str(event.get('symbol') or '').strip().upper()}"
            for event in stored_events
            if event.get("type") == "touched" and event.get("trading_date") and event.get("symbol")
        }
        get_orders = getattr(qmt, "get_orders", None)
        orders = get_orders(order_keys) if callable(get_orders) else {}
        runtime_symbols = runtime.get("symbols") or {}
        events = []
        labels = {"touched": "涨停", "broken": "炸板", "resealed": "回封"}
        for event in stored_events:
            symbol = str(event.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            event["name"] = self._resolve_name(symbol, event.get("name"))
            if is_risk_warning_name(event["name"]):
                continue
            label = str(labels.get(str(event.get("type"))) or event.get("rule_name") or "").strip()
            if label:
                event["rule_name"] = label
                event["message"] = f"{event['name']}：{label}"
            if event.get("type") == "touched":
                order_key = (
                    f"limit-board-{str(event.get('trading_date') or '').replace('-', '')}-{symbol}"
                )
                order = orders.get(order_key)
                state = runtime_symbols.get(symbol) or {}
                if order is None and state.get("auto_order_key") == order_key:
                    order = {
                        "idempotency_key": order_key,
                        "status": state.get("auto_order_status"),
                        "order_sys_id": state.get("auto_order_sys_id"),
                        "trigger_at": state.get("auto_order_trigger_at"),
                        "system_order_at": state.get("auto_order_system_at") or state.get("auto_order_at"),
                        "qmt_submit_at": state.get("auto_order_qmt_submit_at"),
                        "qmt_accepted_at": state.get("auto_order_qmt_accepted_at"),
                        "broker_order_at": state.get("auto_order_broker_at"),
                        "broker_order_time_raw": state.get("auto_order_broker_time_raw"),
                        "error": state.get("auto_order_error"),
                    }
                if order is not None:
                    event["order_timeline"] = self._order_timeline(order)
            events.append(event)
        self._enrich_concepts([
            *rows, *selected, *board_pool, *candidate_pool,
            *first_board, *rebound_board, *events,
        ])
        hub = self._hub()
        capacity = hub.websocket_capacity() if hub is not None else 0
        qmt_status = qmt.status() if qmt is not None else {}
        trading_enabled = bool(
            qmt_status.get("configured")
            and qmt_status.get("state") == "ready"
            and qmt_status.get("trade_enabled")
        )
        sentiment_guard = self._sentiment_guard(config)
        if sentiment_guard["blocked"]:
            trading_enabled = False
            trading_reason = sentiment_guard["reason"]
        elif trading_enabled:
            trading_reason = "QMT 实盘已就绪"
        elif qmt_status.get("state") == "ready":
            trading_reason = "QMT 已连接，实盘模式未开启"
        else:
            trading_reason = str(qmt_status.get("reason") or "QMT 交易网关未就绪")
        sector_strength = self.sector_strength_view()
        market_mode = self._market_mode()
        first_board_enabled = (
            market_mode == "full_market"
            and self._history_ready
            and self._sector_candidate_scope.get("state") in {"live", "partial"}
        )
        return {
            "revision": config["revision"],
            "settings": config["settings"],
            "first_board": first_board,
            "rebound_board": rebound_board,
            "selected": selected,
            "candidate_pool": candidate_pool,
            "board_pool": board_pool,
            "blacklist": [
                symbol for symbol in runtime.get("blacklist", [])
                if not is_risk_warning_name(self._resolve_name(str(symbol).strip().upper()))
            ],
            "market_sentiment": self._market_sentiment_snapshot(),
            "sector_strength": sector_strength,
            "events": events,
            "runtime": {
                "trading_date": runtime["trading_date"],
                "history_ready": self._history_ready,
                "history_reason": self._history_reason,
                "candidate_scope": dict(self._sector_candidate_scope),
                "last_scan_at": self._last_scan_at,
                "last_error": self._last_error,
                "websocket_status": "connected" if self._ws_registered else "idle",
                "websocket_symbols": len(self._ws_symbols),
                "websocket_capacity": capacity,
                "trading_enabled": trading_enabled,
                "trading_reason": trading_reason,
                "sentiment_guard": sentiment_guard,
                "market_mode": market_mode,
                "refresh_cycle": {
                    "as_of": (
                        sector_strength.get("refreshed_at")
                        if isinstance(sector_strength, dict)
                        else self._last_scan_at
                    ),
                    "interval_seconds": int(self._refresh_interval_seconds()),
                },
                "first_board_enabled": first_board_enabled,
            },
        }

    def update_notifications(
        self, notifications: dict[str, Any], revision: int,
    ) -> dict[str, Any]:
        values = {
            key: bool(notifications[key])
            for key in ("touched", "broken", "resealed")
        }

        def update(config: dict[str, Any]) -> None:
            config["settings"]["notifications"] = values

        saved = self.store.update(revision, update)
        self._notify_updated()
        return saved

    def update_advanced_settings(
        self, settings: dict[str, Any], revision: int,
    ) -> dict[str, Any]:
        values = {
            "sweep_price_levels": int(settings["sweep_price_levels"]),
            "queue_wait_seconds": int(settings["queue_wait_seconds"]),
            "queue_confirm_snapshots": int(settings["queue_confirm_snapshots"]),
            "order_amount_per_board": float(settings["order_amount_per_board"]),
            "max_auto_board_count": int(settings["max_auto_board_count"]),
            "max_market_broken_rate_pct": float(
                settings.get("max_market_broken_rate_pct", 40.0),
            ),
            "main_board_only": bool(settings.get("main_board_only", False)),
            "near_limit_pct": float(settings["near_limit_pct"]),
            "exit_limit_pct": float(settings["exit_limit_pct"]),
            "exit_sustain_seconds": int(settings["exit_sustain_seconds"]),
            "first_board_lookback_days": int(settings["first_board_lookback_days"]),
            "blacklist_after_breaks": int(settings["blacklist_after_breaks"]),
        }
        if values["exit_limit_pct"] < values["near_limit_pct"]:
            raise ValueError("扫描退出阈值不能小于临板 WS 阈值")
        if not 0 <= values["max_market_broken_rate_pct"] <= 100:
            raise ValueError("今日破板率停手阈值必须在 0 到 100 之间")

        def update(config: dict[str, Any]) -> None:
            config["settings"].update(values)

        saved = self.store.update(revision, update)
        self._on_market_fetch()
        self._notify_updated()
        return saved

    def add_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["selected"]):
                return
            value["selected"].append({
                "symbol": cleaned,
                "name": name,
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        runtime = self._runtime_for_today()
        excluded = set(runtime.get("candidate_excluded") or [])
        if cleaned in excluded:
            excluded.remove(cleaned)
            runtime["candidate_excluded"] = sorted(excluded)
            self._persist_runtime(runtime)
        self._refresh_symbol_consumer()
        self._on_market_fetch()
        self._notify_updated()
        return saved

    def add_candidate(self, symbol: str, revision: int) -> dict[str, Any]:
        """Add a manual candidate while retaining the legacy storage schema."""
        return self.add_selected(symbol, revision)

    def _validated_stock(self, symbol: str) -> tuple[str, str]:
        cleaned = str(symbol).strip().upper()
        names = self.repo.get_name_map([cleaned])
        if cleaned not in names or self.repo.resolve_asset_type(cleaned) != "stock":
            raise ValueError("仅支持本地股票主数据中的 A 股标的")
        name = str(names[cleaned])
        if is_risk_warning_name(name):
            raise ValueError("打板专区已过滤 ST 风险警示股票")
        return cleaned, name

    def remove_selected(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "selected", [item for item in value["selected"] if str(item.get("symbol")) != cleaned],
            ),
        )
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved

    def remove_candidate(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "selected", [
                    item for item in value["selected"]
                    if str(item.get("symbol")).strip().upper() != cleaned
                ],
            ),
        )
        runtime = self._runtime_for_today()
        excluded = set(runtime.get("candidate_excluded") or [])
        excluded.add(cleaned)
        runtime["candidate_excluded"] = sorted(excluded)
        self._persist_runtime(runtime)
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved

    def add_pool(self, symbol: str, source: str, revision: int) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["board_pool"]):
                return
            value["board_pool"].append({
                "symbol": cleaned,
                "name": name,
                "source": source,
                "auto_trade": True,
                "order_mode": "sweep",
                "added_at": cn_now().isoformat(),
            })

        saved = self.store.update(revision, update)
        self._refresh_symbol_consumer()
        self._on_market_fetch()
        self._notify_updated()
        return saved

    def update_pool(
        self,
        symbol: str,
        auto_trade: bool,
        order_mode: str,
        revision: int,
    ) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        cleaned_mode = str(order_mode or "").strip().lower()
        if cleaned_mode not in {"sweep", "queue"}:
            raise ValueError("打板方式必须是扫板或排板")

        def update(value: dict[str, Any]) -> None:
            member = next(
                (item for item in value["board_pool"] if str(item.get("symbol")) == cleaned),
                None,
            )
            if member is None:
                raise ValueError("打板池中不存在该股票")
            member["auto_trade"] = bool(auto_trade)
            member["order_mode"] = cleaned_mode

        saved = self.store.update(revision, update)
        self._on_market_fetch()
        self._notify_updated()
        return saved

    def remove_pool(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "board_pool",
                [item for item in value["board_pool"] if str(item.get("symbol")) != cleaned],
            ),
        )
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved
