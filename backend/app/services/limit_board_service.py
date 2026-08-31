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
from typing import Any, Callable

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.price_limits import is_risk_warning_name, limit_price, price_limit_pct
from app.services import premium_gene, rps_rotation
from app.services.limit_board_scoring import (
    SCORE_MODEL_VERSION,
    comprehensive_score,
    intraday_flow_detail,
    premium_gene_detail,
    rotation_detail,
    rotation_only_detail,
    sector_detail,
    technical_detail,
)
from app.services.limit_board_store import LimitBoardStore
from app.services.limit_up_queue import LimitUpQueueService
from app.services.qmt_trading import QmtOrderPreflightError
from app.services.screener import ScreenerService


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
_SCORE_DISPLAY_CACHE_SECONDS = 60.0
_SCORE_ON_DEMAND_WAIT_SECONDS = 15.0
_SECTOR_CANDIDATE_LIMIT = 15
_AUTOMATIC_CANDIDATES_PER_SECTOR = 10
_AUTOMATIC_NEAR_LIMIT_PER_SECTOR = 5
_AUTOMATIC_CANDIDATE_LIMIT = 30
_SECTOR_DISPLAY_SWITCH_TIME = clock_time(9, 0)
_ENTRY_MIN_LIMIT_GAP_PCT = 0.005
_ENTRY_MAX_LIMIT_GAP_PCT = 0.03
_ENTRY_QUOTE_FRESH_SECONDS = 10.0
_ENTRY_SCORE_RISING_DELTA = 0.05
# 「跟随全局」(global) 和「一手」(lot) 已废弃: 全局资金方式配置被移除,
# 一手模式也不再提供。旧配置中残留的这两个值在读取时会被显式拒绝。
_POOL_ALLOCATION_MODES = frozenset({"available", "sixth", "fifth", "quarter", "fixed", "volume"})
_LEGACY_POOL_ALLOCATION_MODES = frozenset({"global", "lot"})
# 「固定金额」未填写时的兜底金额(元)。前端新增时默认就填这个值,
# 这里再兜一层, 避免 API 直接调用时因缺金额而报错。
_DEFAULT_FIXED_ALLOCATION_VALUE = 20_000.0
_CLOSE_AUCTION_START = clock_time(14, 57)
_CLOSE_AUCTION_END = clock_time(15, 0)
_SCORE_STOCK_COLUMNS = {
    "symbol", "name", "close", "last_price", "prev_close", "change_pct", "amount",
    "ma5", "ma10", "ma20", "ma60", "momentum_5d", "momentum_20d",
    "vol_ratio_5d", "macd_dif", "macd_dea", "macd_hist", "rsi_14",
    # KDJ 仅用于明细展示，不参与技术面打分（见 technical_detail）
    "kdj_k", "kdj_d", "kdj_j",
}
_SCORE_WEIGHTS = {
    "sector": 50.0,
    "premium_gene": 10.0,
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


def _is_current_score_snapshot(value: dict[str, Any] | None) -> bool:
    if not isinstance(value, dict):
        return False
    detail = value.get("candidate_score_detail") or {}
    intraday_flow = detail.get("intraday_flow") if isinstance(detail, dict) else None
    if isinstance(intraday_flow, dict) and (
        intraday_flow.get("flow_source") == "kaipanla_net_flow"
        or intraday_flow.get("flow_metric") == "main_net_speed"
    ):
        # The old cumulative-net-flow endpoint is no longer the active capital
        # source. Do not restore its persisted result after the endpoint switch.
        return False
    comprehensive = detail.get("comprehensive") if isinstance(detail, dict) else None
    # Older candidate-score snapshots predate the comprehensive projection and
    # remain valid for the independent candidate ranking score.
    if comprehensive is None:
        return True
    return isinstance(comprehensive, dict) and (
        comprehensive.get("score_model_version") == SCORE_MODEL_VERSION
    )


def _is_trading_time(value: datetime) -> bool:
    current = value.timetz().replace(tzinfo=None)
    return (
        clock_time(9, 30) <= current < clock_time(11, 30)
        or clock_time(13, 0) <= current < _CLOSE_AUCTION_START
    )


def _is_close_auction_time(value: datetime) -> bool:
    current = value.timetz().replace(tzinfo=None)
    return (
        value.weekday() < 5
        and _CLOSE_AUCTION_START <= current < _CLOSE_AUCTION_END
    )


def _is_after_close_auction(value: datetime) -> bool:
    return value.weekday() < 5 and value.timetz().replace(tzinfo=None) >= _CLOSE_AUCTION_END


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
        self._order_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="limit-board-order")
        self._order_slots = threading.BoundedSemaphore(4)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._polling_lease = False
        self._ws_registered = False
        self._ws_symbols: set[str] = set()
        self._quotes: dict[str, dict[str, Any]] = {}
        self._preselection_quotes: dict[str, dict[str, Any]] = {}
        self._sector_quote_symbols: set[str] = set()
        self._heat_quote_symbols: set[str] = set()
        self._depth: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=10))
        self._close_auction_depth: dict[str, dict[str, Any]] = {}
        self._close_auction_date: date | None = None
        self._close_auction_finalized_symbols: set[str] = set()
        self._history_date: date | None = None
        self._name_map_date: date | None = None
        self._name_map: dict[str, str] = {}
        self._instrument_limit_up_date: date | None = None
        self._instrument_limit_up: dict[str, object] = {}
        self._instrument_limit_up_source: object | None = None
        self._first_board_eligible: set[str] = set()
        self._rebound_board_eligible: set[str] = set()
        self._premium_stats: dict[str, dict[str, Any]] = {}
        self._screener = ScreenerService(repo)
        self._yesterday_boards_date: date | None = None
        self._yesterday_boards: dict[str, int] = {}
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
            "reason": "正在读取实时板块强度前 15 名",
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
        self._queue_watcher = LimitUpQueueService(on_update=self._notify_updated)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._queue_watcher.start()
        add_fetch_listener = getattr(self.quote_service, "add_fetch_listener", None)
        if callable(add_fetch_listener):
            try:
                add_fetch_listener(self._on_market_fetch)
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"行情监听初始化失败：{exc}"
                logger.warning("打板专区行情监听初始化失败", exc_info=True)
        try:
            self._refresh_symbol_consumer()
        except Exception as exc:  # noqa: BLE001
            self._last_error = f"行情标的订阅初始化失败：{exc}"
            logger.warning("打板专区行情标的订阅初始化失败", exc_info=True)
        from app.services import preferences

        acquire_polling = getattr(self.quote_service, "acquire_temporary_polling", None)
        get_min_interval = getattr(self.quote_service, "get_min_interval", None)
        realtime_enabled = preferences.get_realtime_quotes_enabled()
        monitor_enabled = preferences.get_limit_ladder_monitor_enabled()
        if callable(acquire_polling) and realtime_enabled and monitor_enabled:
            try:
                interval = max(1.0, float(get_min_interval())) if callable(get_min_interval) else 3.0
                acquire_polling(interval)
                self._polling_lease = True
            except ValueError as exc:
                self._last_error = str(exc)
        elif callable(acquire_polling):
            logger.info(
                "打板专区行情轮询未启动: realtime_quotes_enabled=%s, limit_ladder_monitor_enabled=%s",
                realtime_enabled,
                monitor_enabled,
            )
        hub = self._hub()
        if hub is not None:
            try:
                hub.add_depth_listener(self.enqueue_depth)
                # Pool symbols must be subscribed before the first quote arrives;
                # relying on a later market callback leaves a configured pool idle
                # when the provider has no initial snapshot.
                self._sync_websocket(self._runtime_for_today(), self.store.load_config())
            except Exception as exc:  # noqa: BLE001
                self._last_error = f"实时行情订阅初始化失败：{exc}"
                logger.warning("打板专区实时行情订阅初始化失败", exc_info=True)
        self._thread = threading.Thread(target=self._worker, name="limit-board", daemon=True)
        self._started = True
        self._thread.start()
        try:
            self._on_market_fetch()
        except Exception as exc:  # noqa: BLE001
            self._last_error = f"首次行情刷新失败：{exc}"
            logger.warning("打板专区首次行情刷新失败", exc_info=True)

    def stop(self) -> None:
        self._started = False
        self._queue_watcher.stop()
        remove_fetch_listener = getattr(self.quote_service, "remove_fetch_listener", None)
        if callable(remove_fetch_listener):
            remove_fetch_listener(self._on_market_fetch)
        remove_symbol_consumer = getattr(self.quote_service, "remove_symbol_consumer", None)
        if callable(remove_symbol_consumer):
            remove_symbol_consumer(_ACCOUNT_ID)
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
        release_polling = getattr(self.quote_service, "release_temporary_polling", None)
        if self._polling_lease and callable(release_polling):
            release_polling()
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

    def jijiang_realtime_view(self) -> dict[str, Any]:
        collector = getattr(self.app_state, "kaipanla_collector", None)
        getter = getattr(collector, "jijiang_realtime_snapshot", None)
        if not callable(getter):
            return {
                "provider": "kaipanla_socket",
                "state": "unavailable",
                "as_of": cn_today().isoformat(),
                "refreshed_at": None,
                "rows": [],
            }
        try:
            value = getter()
        except Exception:  # noqa: BLE001
            logger.debug("读取即将涨停雷达快照失败", exc_info=True)
            value = None
        if not isinstance(value, dict):
            return {
                "provider": "kaipanla_socket",
                "state": "unavailable",
                "as_of": cn_today().isoformat(),
                "refreshed_at": None,
                "rows": [],
            }
        today = cn_today()
        yesterday_boards = self._load_yesterday_boards(today)
        rows = []
        for raw in value.get("rows") or []:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            symbol = str(row.get("thscode") or "").strip().upper()
            row["yesterday_boards"] = yesterday_boards.get(symbol, 0)
            rows.append(row)
        return {**value, "rows": rows}

    def _load_yesterday_boards(self, today: date) -> dict[str, int]:
        """Load the prior trading day's system ladder board counts once per day."""
        if self._yesterday_boards_date == today:
            return self._yesterday_boards
        try:
            frame = self._screener.load_prior_ladder_boards(today)
        except Exception:  # noqa: BLE001
            logger.debug("读取昨日连板数据失败", exc_info=True)
            frame = pl.DataFrame()
        values: dict[str, int] = {}
        if frame is not None and not frame.is_empty() and {"symbol", "boards"}.issubset(frame.columns):
            prior = (
                frame.select("symbol", "boards")
                .with_columns(
                    pl.col("boards").cast(pl.Int64, strict=False).fill_null(0),
                )
                .filter(pl.col("boards") > 0)
            )
            values = {
                str(row["symbol"]).strip().upper(): max(1, int(row["boards"]))
                for row in prior.iter_rows(named=True)
                if str(row.get("symbol") or "").strip()
            }
        self._yesterday_boards_date = today
        self._yesterday_boards = values
        return values

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
        try:
            instruments = self.repo.get_instruments()
        except Exception:  # noqa: BLE001
            logger.debug("读取股票涨停价维表失败", exc_info=True)
            instruments = pl.DataFrame()
        with self._lock:
            cached_source = self._instrument_limit_up_source
            cached_date = self._instrument_limit_up_date
        if cached_date != today or cached_source is not instruments:
            try:
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
            with self._lock:
                self._instrument_limit_up = limit_map
                self._instrument_limit_up_date = today
                self._instrument_limit_up_source = instruments
        with self._lock:
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

    def invalidate_instrument_limit_up_cache(self) -> None:
        """让盘前维表覆盖后，下一次行情处理重新读取涨停价。"""
        with self._lock:
            self._instrument_limit_up_date = None
            self._instrument_limit_up = {}
            self._instrument_limit_up_source = None

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
        fallback_previous: bool = False,
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
        if (
            fallback_previous
            and captured_at is None
            and self._should_display_previous_sector(today)
            and (
                not isinstance(snapshot, dict)
                or snapshot.get("state") != "live"
                or snapshot.get("as_of") != today.isoformat()
            )
        ):
            previous_date = self._latest_completed_sector_date(today, collector)
            previous_getter = getattr(collector, "sector_strength_snapshot_at", None)
            if previous_date is not None and callable(previous_getter):
                try:
                    previous_snapshot = previous_getter(previous_date, None)
                except Exception:  # noqa: BLE001
                    logger.debug("读取上一交易日板块强度快照失败", exc_info=True)
                    previous_snapshot = None
                if (
                    isinstance(previous_snapshot, dict)
                    and previous_snapshot.get("state") == "live"
                    and previous_snapshot.get("as_of") == previous_date.isoformat()
                ):
                    today = previous_date
                    snapshot = previous_snapshot
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
        rotation_payloads = [
            self._rotation("concept", None, today),
            self._rotation("industry", 2, today),
        ]
        for row in ordered:
            row.update(self._institutional_sector_fields(row, rotation_payloads, today))
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

    @staticmethod
    def _institutional_sector_fields(
        row: dict[str, Any],
        rotations: list[dict[str, Any]],
        today: date,
    ) -> dict[str, Any]:
        """Attach explainable institutional fields to a sector-strength row."""
        name = str(row.get("plate_name") or "").strip()
        histories = [
            detail
            for rotation in rotations
            if isinstance(rotation, dict)
            for detail in [rotation_detail(rotation, name, today)]
            if detail is not None
        ]
        if not histories:
            return {}
        history = max(
            histories,
            key=lambda value: (
                float(value.get("institutional_score") or 0.0)
                / max(float(value.get("institutional_max_score") or 1.0), 1.0),
                float(value.get("institutional_score") or 0.0),
            ),
        )
        score = float(history.get("institutional_score") or 0.0)
        max_score = float(history.get("institutional_max_score") or 0.0)
        components = dict(history.get("institutional_components") or {})
        amount = _finite(row.get("amount"))
        main_net = _finite(row.get("main_net"))
        flow_ratio = main_net / amount if main_net is not None and amount and amount > 0 else None
        if flow_ratio is not None:
            components["money_flow"] = max(0.0, min(1.0, (flow_ratio + 0.20) / 0.60)) * 15.0
            score += components["money_flow"]
            max_score += 15.0
        volume_ratio = _finite(row.get("volume_ratio"))
        if volume_ratio is not None:
            components["liquidity"] = max(0.0, min(1.0, (volume_ratio - 0.80) / 1.20)) * 5.0
            score += components["liquidity"]
            max_score += 5.0
        return {
            "institutional_score": round(score, 2),
            "institutional_max_score": round(max_score, 2),
            "institutional_components": {
                key: round(float(value), 2) for key, value in components.items()
            },
            "one_day_change_pct": history.get("one_day_change_pct"),
            "three_day_change_pct": history.get("three_day_change_pct"),
            "five_day_change_pct": history.get("five_day_change_pct"),
            "twenty_day_change_pct": history.get("twenty_day_change_pct"),
            "top_20_days": history.get("top_20_days"),
        }

    @staticmethod
    def _should_display_previous_sector(today: date) -> bool:
        now = cn_now()
        current = now.timetz().replace(tzinfo=None)
        return today.weekday() >= 5 or current < _SECTOR_DISPLAY_SWITCH_TIME

    @staticmethod
    def _latest_completed_sector_date(today: date, collector: object) -> date | None:
        getter = getattr(collector, "latest_completed_trading_date", None)
        if not callable(getter):
            return None
        try:
            previous = getter(today)
        except Exception:  # noqa: BLE001
            logger.debug("读取上一交易日日期失败", exc_info=True)
            return None
        return previous if isinstance(previous, date) and previous < today else None

    def sector_strength_view(self, captured_at: str | None = None) -> dict[str, Any] | None:
        today = cn_today()
        if captured_at:
            try:
                point = datetime.fromisoformat(captured_at)
            except ValueError as exc:
                raise ValueError("板块强度时间点格式无效") from exc
            if point.tzinfo is None:
                raise ValueError("只能回看带时区的板块强度时间点")
            point_date = point.astimezone(CN_TZ).date()
            collector = getattr(self.app_state, "kaipanla_collector", None)
            previous_date = self._latest_completed_sector_date(today, collector)
            if point_date != today and not (
                self._should_display_previous_sector(today)
                and point_date == previous_date
            ):
                raise ValueError("只能回看当前交易日或非交易时段的上一交易日板块强度")
            today = point_date
        return self._sector_strength_view(
            today,
            captured_at,
            include_timeline=True,
            fallback_previous=captured_at is None,
        )

    async def _historical_sector_constituents_view(
        self,
        collector: object,
        plate_id: str,
        snapshot: dict[str, Any],
        trade_date: date,
    ) -> dict[str, Any]:
        getter = getattr(collector, "sector_constituents_at", None)
        if not callable(getter):
            raise RuntimeError("开盘啦历史板块成分数据暂不可用")
        try:
            source_rows = await getter(trade_date, plate_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("查询开盘啦历史板块成分失败 (%s)", type(exc).__name__)
            raise RuntimeError("开盘啦历史板块成分数据暂不可用") from exc
        if not source_rows:
            raise RuntimeError("开盘啦历史板块成分数据为空")

        try:
            instruments = self.repo.get_instruments()
            symbol_lookup = {
                str(symbol).split(".", 1)[0]: str(symbol).upper()
                for symbol in instruments.get_column("symbol").to_list()
            } if "symbol" in instruments.columns else {}
        except Exception:  # noqa: BLE001
            symbol_lookup = {}

        members = []
        for row in source_rows:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or row.get("symbol") or "").split(".", 1)[0]
            if not code:
                continue
            symbol = symbol_lookup.get(code) or (
                f"{code}.{'SH' if code.startswith(('6', '9')) else 'BJ' if code.startswith(('4', '8')) else 'SZ'}"
            )
            members.append({
                "plate_id": plate_id,
                "symbol": symbol,
                "code": code,
                "name": str(row.get("name") or "").strip() or None,
                "tags": str(row.get("tags") or "").strip() or None,
                "last_price": _finite(row.get("last_price")),
                # /30 historical constituents report percentages (e.g. 10.01),
                # while the page contract uses decimal ratios (0.1001).
                "change_pct": (
                    _finite(row.get("change_pct")) / 100
                    if _finite(row.get("change_pct")) is not None else None
                ),
                "amount": _finite(row.get("amount")),
                "turnover_rate": (
                    _finite(row.get("turnover_rate")) / 100
                    if _finite(row.get("turnover_rate")) is not None else None
                ),
                "main_net": _finite(row.get("main_net")),
                "limit_tag": str(row.get("limit_tag") or "").strip() or None,
            })
        if not members:
            raise RuntimeError("开盘啦历史板块成分代码无效")

        symbols = {str(row["symbol"]) for row in members}
        try:
            quote_rows = self.quote_service.get_latest_quotes(symbols)
        except Exception:  # noqa: BLE001
            quote_rows = []
        quotes_by_symbol = {}
        for quote in quote_rows:
            symbol = str(quote.get("symbol") or "").strip().upper()
            quote_at = _quote_time(quote.get("timestamp"))
            if symbol in symbols and quote_at is not None and quote_at.date() == trade_date:
                quotes_by_symbol[symbol] = quote

        normalized = []
        for member in members:
            symbol = str(member["symbol"])
            quote = quotes_by_symbol.get(symbol)
            name = str((quote or {}).get("name") or member.get("name") or "").strip() or None
            last_price = _finite((quote or {}).get("last_price", member.get("last_price")))
            limit_up = self._limit_up(quote or {}, symbol, name or "", trade_date)
            at_limit = (
                last_price is not None
                and limit_up is not None
                and last_price >= limit_up - 0.005
            )
            normalized.append({
                **member,
                "name": name,
                "last_price": last_price,
                "limit_up": limit_up,
                "change_pct": _finite((quote or {}).get("change_pct", member.get("change_pct"))),
                "amount": _finite((quote or {}).get("amount", member.get("amount"))),
                "turnover_rate": _finite((quote or {}).get("turnover_rate", member.get("turnover_rate"))),
                "float_market_value": None,
                "main_net": member.get("main_net"),
                "limit_tag": "涨停" if at_limit else member.get("limit_tag"),
                "rank_tag": None,
                "limit_count": None,
                "quote_available": last_price is not None,
            })
        normalized.sort(key=lambda row: (
            row.get("change_pct") is None,
            -float(row.get("change_pct") or 0),
            -float(row.get("amount") or 0),
            str(row.get("code") or ""),
        ))
        for index, row in enumerate(normalized, start=1):
            row["rank"] = index
            row["rank_count"] = len(normalized)

        quote_as_of = max(
            (_quote_time(quote.get("timestamp")) for quote in quotes_by_symbol.values()),
            default=None,
        )
        return {
            "provider": "kaipanla",
            "state": "closed",
            "as_of": trade_date.isoformat(),
            "captured_at": snapshot.get("refreshed_at"),
            "membership_as_of": trade_date.isoformat(),
            "quote_provider": "tickflow",
            "quote_state": "closed" if quote_as_of is not None else "unavailable",
            "quote_as_of": quote_as_of.isoformat() if quote_as_of else None,
            "quote_available": bool(quotes_by_symbol),
            "plate_id": plate_id,
            "plate_name": next(
                (
                    str(row.get("plate_name") or "")
                    for row in snapshot.get("rows") or []
                    if isinstance(row, dict) and str(row.get("plate_id") or "") == plate_id
                ),
                None,
            ),
            "rows": normalized,
        }

    async def sector_constituents_view(
        self,
        plate_id: str,
        captured_at: str | None = None,
    ) -> dict[str, Any]:
        today = cn_today()
        collector = getattr(self.app_state, "kaipanla_collector", None)
        previous_date = self._latest_completed_sector_date(today, collector)
        requested_point: datetime | None = None
        if captured_at:
            try:
                requested_point = datetime.fromisoformat(captured_at)
            except ValueError as exc:
                raise ValueError("板块强度时间点格式无效") from exc
            if requested_point.tzinfo is None:
                raise ValueError("只能查看带时区的板块成分时间点")
            point_date = requested_point.astimezone(CN_TZ).date()
            if point_date != today and not (
                self._should_display_previous_sector(today)
                and point_date == previous_date
            ):
                raise ValueError("只能查看当前交易日或非交易时段的上一交易日板块成分")
            requested_point = requested_point.astimezone(CN_TZ)
        current_snapshot = self.sector_strength_view(captured_at)
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

        snapshot_date = None
        try:
            snapshot_date = date.fromisoformat(str(snapshot.get("as_of")))
        except (TypeError, ValueError):
            pass
        if snapshot_date is not None and snapshot_date != today:
            return await self._historical_sector_constituents_view(
                collector,
                plate_id,
                snapshot,
                snapshot_date,
            )

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
                "limit_up": limit_up,
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

    def _sector_strength_snapshot(
        self, today: date,
    ) -> tuple[dict[str, dict[str, Any]], date | None]:
        """板块强度行（按名称索引）+ 数据锚定日期。

        fallback_previous=True：周末/盘前（_should_display_previous_sector）
        回退到最近一个交易日的快照——盘后/非交易日查看时，实时类指标
        按收盘冻结值计算。
        """
        view = self._sector_strength_view(today, fallback_previous=True)
        if not view or view.get("state") != "live":
            return {}, None
        rows = {
            str(row.get("plate_name") or "").strip(): row
            for row in view.get("rows") or []
            if str(row.get("plate_name") or "").strip()
        }
        as_of: date | None = None
        try:
            as_of = date.fromisoformat(str(view.get("as_of") or ""))
        except ValueError:
            as_of = None
        return rows, as_of

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

    def _close_frozen_sector_inputs(
        self,
        realtime: dict[str, Any],
        stock_rows: dict[str, dict[str, Any]],
        anchor_date: date | None,
    ) -> tuple[dict[str, dict[str, Any]], set[str], dict[str, Any]] | None:
        """用最近交易日的收盘快照构建板块评分输入（实时行情不可用时）。

        盘后/周末查看时，实时类指标按「收盘冻结值」计算：成员涨跌幅/
        成交额取自 enriched 日线快照；成分关系优先用内存中的实时表，
        服务重启后回退到持久化的已完成日成分表。
        """
        plate_id = str(realtime.get("plate_id") or "").strip()
        if not plate_id:
            return None
        with self._lock:
            memberships = self._sector_memberships.clone()
        missing = (
            memberships.is_empty()
            or "plate_id" not in memberships.columns
            or "symbol" not in memberships.columns
            or memberships.filter(pl.col("plate_id") == plate_id).is_empty()
        )
        if missing:
            collector = getattr(self.app_state, "kaipanla_collector", None)
            getter = getattr(collector, "sector_constituent_memberships", None)
            if anchor_date is None or not callable(getter):
                return None
            try:
                memberships = getter(anchor_date)
            except Exception:  # noqa: BLE001
                logger.debug("读取持久化板块成分表失败", exc_info=True)
                return None
        if (
            memberships is None
            or memberships.is_empty()
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
        rows = {
            symbol: row
            for symbol in member_symbols
            if (row := stock_rows.get(symbol))
        }
        valid_rows = {
            symbol: row
            for symbol, row in rows.items()
            if _finite(row.get("change_pct")) is not None
        }
        total_count = len(member_symbols)
        valid_count = len(valid_rows)
        changes = [float(row["change_pct"]) for row in valid_rows.values()]
        sector_change = _finite(realtime.get("change_pct"))
        if sector_change is None and changes:
            sector_change = sum(changes) / len(changes)
        return rows, member_symbols, {
            "valid": total_count >= 5 and valid_count / total_count >= 0.8,
            "change_pct": sector_change,
            "coverage_ratio": valid_count / total_count if total_count else 0.0,
            "valid_count": valid_count,
            "total_count": total_count,
            "up_count": sum(value > 0 for value in changes),
            "down_count": sum(value < 0 for value in changes),
            "data_source": "daily_close",
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
            return self._set_sector_candidate_unavailable("实时板块强度前 15 名暂不可用")
        top_rows = self._top_sector_rows(view.get("rows") or [])
        if not top_rows:
            return self._set_sector_candidate_unavailable("实时板块强度前 15 名为空")
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
            return self._set_sector_candidate_unavailable("前 15 板块未匹配开盘啦当日成分")
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
            for key in ("selected", "board_pool", "buy_pool")
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
                "buy_orders": {},
            }
            self.store.save_runtime(runtime)
            self._depth.clear()
            self._preselection_quotes.clear()
        changed = False
        for state in (runtime.get("symbols") or {}).values():
            if (
                state.get("auto_order_status") == "unknown"
                and self._is_known_preflight_error(state.get("auto_order_error"))
            ):
                state["auto_order_status"] = "blocked"
                changed = True
        for order in (runtime.get("buy_orders") or {}).values():
            if (
                order.get("order_status") == "unknown"
                and self._is_known_preflight_error(order.get("order_error"))
            ):
                order["order_status"] = "blocked"
                changed = True
        if changed:
            self.store.save_runtime(runtime)
        return runtime

    @staticmethod
    def _is_known_preflight_error(error: object) -> bool:
        message = str(error or "")
        return message.startswith("QMT 未返回信用账户可买额度")

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
            f"已核对前 {lookback} 个交易日；自动候选仅来自实时板块强度前 15 名，"
            "涨停基因用于 10 分个股排序"
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
        previous_quotes: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        retained = {
            symbol
            for symbol, quote in updates.items()
            if {"selected", "board_pool", "buy_pool"} & set(quote.get("source_modes") or [])
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
            ranked = sorted(quotes, key=rank_key)
            retained.update(
                str(quote["symbol"])
                for quote in ranked[:_AUTOMATIC_CANDIDATES_PER_SECTOR]
            )
            near_limit = sorted(
                quotes,
                key=lambda quote: (
                    _finite(quote.get("limit_gap_pct"))
                    if _finite(quote.get("limit_gap_pct")) is not None
                    else float("inf"),
                    -( _finite(quote.get("amount")) or 0.0),
                    str(quote.get("symbol") or ""),
                ),
            )
            retained.update(
                str(quote["symbol"])
                for quote in near_limit[:_AUTOMATIC_NEAR_LIMIT_PER_SECTOR]
            )
            previous_quotes = previous_quotes or {}
            momentum = []
            for quote in quotes:
                symbol = str(quote.get("symbol") or "").strip().upper()
                current_change = _finite(quote.get("change_pct"))
                previous_change = _finite((previous_quotes.get(symbol) or {}).get("change_pct"))
                if current_change is None or previous_change is None:
                    continue
                momentum.append((current_change - previous_change, quote))
            retained.update(
                str(quote["symbol"])
                for _delta, quote in sorted(
                    momentum,
                    key=lambda item: (-item[0], str(item[1].get("symbol") or "")),
                )[:_AUTOMATIC_NEAR_LIMIT_PER_SECTOR]
            )
        return {symbol: quote for symbol, quote in updates.items() if symbol in retained}

    def _process_quotes(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        self._refresh_history(config)
        full_market = self._market_mode() == "full_market"
        runtime = self._runtime_for_today()
        selected = {str(item["symbol"]).strip().upper() for item in config["selected"]}
        board_pool = {str(item["symbol"]).strip().upper() for item in config["board_pool"]}
        buy_pool = {str(item["symbol"]).strip().upper() for item in config.get("buy_pool", [])}
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
            if symbol in buy_pool:
                modes.add("buy_pool")
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
            if symbol in buy_pool:
                source_modes.append("buy_pool")
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
        all_updates = updates
        updates = self._preselect_automatic_updates(updates, self._preselection_quotes)
        self._preselection_quotes = {
            symbol: dict(quote)
            for symbol, quote in all_updates.items()
            if {"first_board", "rebound_board"} & set(quote.get("source_modes") or [])
        }
        with self._lock:
            self._quotes.update(updates)
        self._evaluate_quotes(updates, runtime, config)
        rows, selected_rows, board_rows, buy_rows = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected_rows, board_rows, buy_rows,
        )
        scoring_rows = self._scoring_rows(rows, board_rows)
        self._refresh_candidate_scores(runtime, scoring_rows, now)
        board_symbols = {str(row.get("symbol") or "").strip().upper() for row in scoring_rows}
        self._trim_automatic_candidates(runtime, keep_symbols=board_symbols)
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
            if self._quote_limit_consistency_error(quote):
                state["status"] = "watching"
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

    @staticmethod
    def _quote_limit_consistency_error(quote: dict[str, Any]) -> str | None:
        last_price = _finite(quote.get("last_price"))
        limit_up = _finite(quote.get("limit_up"))
        if last_price is None or limit_up is None or limit_up <= 0:
            return None
        if last_price > limit_up + _STOCK_PRICE_TICK / 2:
            return (
                f"行情涨停价不一致：最新价 {last_price:.2f} 高于涨停价 {limit_up:.2f}，"
                "已阻止自动委托"
            )
        return None

    def _process_depth(self, records: list[dict[str, Any]]) -> None:
        config = self.store.load_config()
        sweep_price_levels = int(config["settings"].get("sweep_price_levels", 5))
        queue_confirm_snapshots = int(
            config["settings"].get("queue_confirm_snapshots", 0),
        )
        runtime = self._runtime_for_today()
        now = cn_now()
        close_auction = _is_close_auction_time(now)
        after_close_auction = _is_after_close_auction(now)
        if not _is_trading_time(now) and not close_auction and not after_close_auction:
            return
        if (close_auction or after_close_auction) and self._close_auction_date != now.date():
            self._close_auction_depth = {}
            self._close_auction_date = now.date()
            self._close_auction_finalized_symbols = set()
        if after_close_auction:
            # The final auction snapshot may arrive just before 15:00 and no
            # depth event is guaranteed after the auction ends.
            latest: dict[str, dict[str, Any]] = {}
            cached = [
                {**value, "symbol": symbol}
                for symbol, value in self._close_auction_depth.items()
            ]
            for raw in [*cached, *records]:
                symbol = str(raw.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                normalized = self._normalize_depth(raw, now)
                if normalized is None:
                    continue
                if normalized["timestamp"].timetz().replace(tzinfo=None) < _CLOSE_AUCTION_START:
                    continue
                previous = latest.get(symbol)
                if previous is None or normalized["timestamp"] >= previous["timestamp"]:
                    latest[symbol] = {**normalized, "symbol": symbol}
            records = list(latest.values())
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            quote = self._quotes.get(symbol)
            state = runtime.setdefault("symbols", {}).get(symbol)
            if not quote or not state or symbol in set(runtime.get("blacklist") or []):
                continue
            if after_close_auction and symbol in self._close_auction_finalized_symbols:
                continue
            quote_at = _quote_time(quote.get("timestamp"))
            now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
            if (
                not close_auction
                and not after_close_auction
                and (quote_at is None or (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS)
            ):
                continue
            normalized = self._normalize_depth(raw, now)
            if normalized is None:
                continue
            if close_auction:
                # 14:57-15:00 is the closing auction. Its indicative order book
                # must not change the intraday sealed/broken state; retain only
                # the latest quote fields for display until the 15:00 final decision.
                if normalized["timestamp"].timetz().replace(tzinfo=None) >= _CLOSE_AUCTION_START:
                    previous = self._close_auction_depth.get(symbol)
                    if previous is None or normalized["timestamp"] >= previous["timestamp"]:
                        self._close_auction_depth[symbol] = normalized
                state["bid1_volume"] = normalized["bid_volumes"][0] if normalized["bid_volumes"] else 0.0
                state["ask1_volume"] = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
                state["last_depth_at"] = normalized["timestamp"].isoformat()
                continue
            if after_close_auction:
                # The first post-auction snapshot is the final board state. A
                # recovered ask-one here is the only closing-auction break that
                # should be recorded.
                state["bid1_volume"] = normalized["bid_volumes"][0] if normalized["bid_volumes"] else 0.0
                state["ask1_volume"] = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
                state["last_depth_at"] = normalized["timestamp"].isoformat()
                ask_price = normalized["ask_prices"][0] if normalized["ask_prices"] else 0.0
                ask_volume = normalized["ask_volumes"][0] if normalized["ask_volumes"] else 0.0
                if state.get("sealed") and ask_price > 0 and ask_volume > 0:
                    self._mark_broken(quote, state, runtime, config, "收盘集合竞价结束时卖一恢复")
                self._close_auction_finalized_symbols.add(symbol)
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
        consistency_error = self._quote_limit_consistency_error(quote)
        if consistency_error:
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = consistency_error
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
        if self._started and (
            self._hub() is None
            or not self._ws_registered
            or symbol not in self._ws_symbols
        ):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "标的未接入共享 TickFlow WebSocket，已阻止自动委托"
            return
        if not self._order_slots.acquire(blocking=False):
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = "自动委托队列已满"
            return
        allocation_mode, allocation_value, allocation_error = self._auto_order_allocation(
            float(quote["limit_up"]), member,
        )
        if allocation_error:
            self._order_slots.release()
            state["auto_order_status"] = "blocked"
            state["auto_order_error"] = allocation_error
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
            "auto_order_allocation_mode": allocation_mode,
            "auto_order_allocation_value": allocation_value,
            "auto_order_volume": None,
            "auto_order_amount": None,
        })
        try:
            self._order_executor.submit(
                self._submit_auto_order,
                symbol,
                float(quote["limit_up"]),
                key,
                allocation_mode,
                allocation_value,
                trigger_at,
                system_order_at,
                str((member or {}).get("credit_buy_mode") or "collateral"),
            )
        except RuntimeError as exc:
            self._order_slots.release()
            state["auto_order_status"] = "unknown"
            state["auto_order_error"] = str(exc)

    @staticmethod
    def _auto_order_allocation(
        limit_up: float,
        member: dict[str, Any] | None = None,
    ) -> tuple[str, float | None, str | None]:
        """Resolve the per-member allocation for an automatic board order.

        全局资金方式 (order_allocation_mode / order_amount_per_board) 已废弃,
        资金方式只取自打板池成员自身。旧配置残留的 global / lot 会被显式拒绝,
        避免静默改变真实下单金额。
        """
        mode = str((member or {}).get("allocation_mode") or "").strip().lower()
        amount = _finite((member or {}).get("allocation_value"))
        if mode in _LEGACY_POOL_ALLOCATION_MODES:
            legacy = "跟随全局" if mode == "global" else "一手"
            return "fixed", None, f"该股票仍使用已废弃的「{legacy}」资金方式，请重新设置打板交易金额"
        if mode not in _POOL_ALLOCATION_MODES:
            return "fixed", None, "单板资金分配方式无效"
        if mode == "volume":
            volume = int(amount or 0)
            if volume < 100 or volume % 100:
                return mode, amount, "固定数量必须是 100 股的整数倍"
            return mode, float(volume), None
        if mode != "fixed":
            return mode, None, None
        if amount is None or amount <= 0:
            return mode, None, "单板固定金额必须大于 0"
        if limit_up <= 0 or limit_up != limit_up:
            return mode, None, "涨停价无效，已阻止自动委托"
        if amount < limit_up * 100:
            return mode, amount, "单板下单资金不足一手，已阻止自动委托"
        return mode, amount, None

    def _submit_auto_order(
        self,
        symbol: str,
        limit_up: float,
        key: str,
        allocation_mode: str,
        allocation_value: float | None,
        trigger_at: str,
        system_order_at: str,
        credit_buy_mode: str = "collateral",
    ) -> None:
        qmt = self._qmt()
        try:
            if qmt is None:
                raise RuntimeError("QMT 交易网关未初始化")
            request: dict[str, Any] = {
                "idempotency_key": key,
                "strategy_name": "limit_board",
                "action": "BUY",
                "symbol": symbol,
                "price": limit_up,
                "price_type": "LIMIT",
                "credit_buy_mode": credit_buy_mode,
                "trigger_at": trigger_at,
                "system_order_at": system_order_at,
            }
            if allocation_mode == "volume":
                request["volume"] = int(allocation_value or 0)
            else:
                request["allocation_mode"] = allocation_mode
                request["allocation_value"] = allocation_value
            order = qmt.submit_order(request)
            result = {
                "symbol": symbol,
                "key": key,
                "status": str(order.get("status") or "unknown"),
                "order_sys_id": order.get("order_sys_id"),
                "error": order.get("error"),
                "volume": order.get("volume"),
                "estimated_amount": order.get("estimated_amount"),
                "allocation_mode": order.get("allocation_mode") or allocation_mode,
                "allocation_value": order.get("allocation_value", allocation_value),
                "trigger_at": order.get("trigger_at") or trigger_at,
                "system_order_at": order.get("system_order_at") or system_order_at,
                "qmt_submit_at": order.get("qmt_submit_at"),
                "qmt_accepted_at": order.get("qmt_accepted_at"),
                "broker_order_at": order.get("broker_order_at"),
                "credit_buy_mode": order.get("credit_buy_mode") or credit_buy_mode,
                "requested_credit_buy_mode": order.get("requested_credit_buy_mode") or credit_buy_mode,
                "credit_buy_mode_switched": bool(order.get("credit_buy_mode_switched")),
                "credit_buy_mode_reason": order.get("credit_buy_mode_reason"),
                "broker_order_time_raw": order.get("broker_order_time_raw"),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("打板自动委托提交失败: %s", exc)
            result = {
                "symbol": symbol,
                "key": key,
                "status": "blocked" if isinstance(exc, (ValueError, QmtOrderPreflightError)) else "unknown",
                "order_sys_id": None,
                "error": str(exc),
                "volume": None,
                "estimated_amount": None,
                "allocation_mode": allocation_mode,
                "allocation_value": allocation_value,
                "trigger_at": trigger_at,
                "system_order_at": system_order_at,
                "qmt_submit_at": None,
                "qmt_accepted_at": None,
                "broker_order_at": None,
                "broker_order_time_raw": None,
                "credit_buy_mode": credit_buy_mode,
                "requested_credit_buy_mode": credit_buy_mode,
                "credit_buy_mode_switched": False,
                "credit_buy_mode_reason": None,
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
            "auto_order_volume": result.get("volume"),
            "auto_order_amount": result.get("estimated_amount"),
            "auto_order_allocation_mode": result.get("allocation_mode"),
            "auto_order_allocation_value": result.get("allocation_value"),
            "auto_order_updated_at": cn_now().isoformat(),
            "auto_order_trigger_at": result.get("trigger_at"),
            "auto_order_system_at": result.get("system_order_at"),
            "auto_order_qmt_submit_at": result.get("qmt_submit_at"),
            "auto_order_qmt_accepted_at": result.get("qmt_accepted_at"),
            "auto_order_broker_at": result.get("broker_order_at"),
            "auto_order_broker_time_raw": result.get("broker_order_time_raw"),
        })
        self._persist_runtime(runtime)
        self._sync_queue_watchers(runtime, self.store.load_config())
        self._notify_updated()

    def _sync_queue_watchers(
        self,
        runtime: dict[str, Any],
        config: dict[str, Any],
    ) -> None:
        """Project accepted limit-up orders into the optional D202 watcher."""
        states = runtime.get("symbols") or {}
        specs: dict[str, dict[str, Any]] = {}
        for item in config.get("board_pool", []):
            symbol = str(item.get("symbol") or "").strip().upper()
            state = states.get(symbol) or {}
            status = str(state.get("auto_order_status") or "")
            volume = int(_finite(state.get("auto_order_volume")) or 0)
            limit_up = _finite(state.get("limit_up"))
            if (
                symbol
                and limit_up is not None
            ):
                specs[symbol] = {
                    "limit_up": limit_up,
                    "queue_key": state.get("auto_order_key") if status in {
                        "accepted_pending", "submitted", "partial", "unknown",
                    } else None,
                    "queue_volume": volume if volume >= 100 and volume % 100 == 0 else 0,
                }
        self._queue_watcher.sync(specs)

    def _sync_websocket(self, runtime: dict[str, Any], config: dict[str, Any]) -> None:
        self._sync_queue_watchers(runtime, config)
        hub = self._hub()
        if hub is None:
            self._last_error = "共享 WebSocket Hub 不可用"
            return
        desired = {
            str(item.get("symbol") or "").strip().upper()
            for key in ("board_pool", "buy_pool")
            for item in config.get(key, [])
            if str(item.get("symbol") or "").strip()
        }
        available_getter = getattr(hub, "websocket_available", None)
        available = int(available_getter(exclude=_ACCOUNT_ID)) if callable(available_getter) else len(desired)
        if len(desired) > available:
            if self._ws_registered:
                try:
                    hub.unregister(_ACCOUNT_ID)
                except Exception:  # noqa: BLE001
                    logger.debug("打板专区移除超限 WS 订阅失败", exc_info=True)
            self._ws_registered = False
            self._ws_symbols.clear()
            self._last_error = f"买入池和打板池共 {len(desired)} 只，超过可用 WebSocket 容量 {available} 只"
            return
        try:
            if not desired:
                if self._ws_registered:
                    hub.unregister(_ACCOUNT_ID)
                self._ws_registered = False
                self._ws_symbols.clear()
                self._last_error = None
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

    def _pool_websocket_capacity_error(
        self,
        config: dict[str, Any],
        additional_symbols: set[str] | None = None,
    ) -> str | None:
        """Return a fail-closed capacity error before a pool mutation or order."""
        hub = self._hub()
        if hub is None:
            return "共享 WebSocket Hub 不可用" if self._started else None
        available_getter = getattr(hub, "websocket_available", None)
        if not callable(available_getter):
            return None
        try:
            available = max(0, int(available_getter(exclude=_ACCOUNT_ID)))
        except Exception:  # noqa: BLE001
            return "无法确认共享 WebSocket 容量，已阻止池内操作"
        desired = {
            str(item.get("symbol") or "").strip().upper()
            for key in ("board_pool", "buy_pool")
            for item in config.get(key, [])
            if str(item.get("symbol") or "").strip()
        }
        desired.update({
            str(symbol).strip().upper()
            for symbol in additional_symbols or set()
            if str(symbol).strip()
        })
        if len(desired) > available:
            return f"买入池和打板池共 {len(desired)} 只，超过可用 WebSocket 容量 {available} 只"
        return None

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
        if not self.store.append_event_once(event):
            return
        # 打板状态只属于短线猎手自己的时间线。监控中心的公共告警必须由
        # MonitorRuleEngine 规则命中后统一写入 alerts.jsonl 和推送。

    def _persist_runtime(self, runtime: dict[str, Any]) -> None:
        stored = self.store.load_runtime()
        if stored.get("trading_date") == runtime.get("trading_date"):
            snapshots = dict(stored.get("candidate_score_snapshots") or {})
            for symbol, snapshot in (runtime.get("candidate_score_snapshots") or {}).items():
                previous = snapshots.get(symbol) or {}
                previous_at = _quote_time(previous.get("candidate_score_as_of"))
                snapshot_at = _quote_time(snapshot.get("candidate_score_as_of"))
                if previous_at is None or (
                    snapshot_at is not None and snapshot_at >= previous_at
                ):
                    snapshots[symbol] = snapshot
            runtime["candidate_score_snapshots"] = snapshots
        self.store.save_runtime(runtime)

    def _notify_updated(self) -> None:
        notify = getattr(self.quote_service, "notify_limit_board_updated", None)
        if callable(notify):
            notify()

    def notify_large_orders_updated(self) -> None:
        """Invalidate candidate scores after an asynchronous precise tape update."""
        self._score_refresh_at = 0.0
        self._notify_updated()

    def _schedule_pool_refresh(self, runtime: dict[str, Any] | None, config: dict[str, Any]) -> None:
        """Run subscription and market refresh work after the API response path."""
        def refresh() -> None:
            try:
                if runtime is not None:
                    self._sync_websocket(runtime, config)
                self._refresh_symbol_consumer()
                self._on_market_fetch()
                self._notify_updated()
            except Exception:  # noqa: BLE001
                logger.exception("打板池变更后的行情刷新失败")

        try:
            self._order_executor.submit(refresh)
        except RuntimeError:
            # During shutdown the durable config/runtime update is still valid.
            logger.debug("打板池变更后的行情刷新未调度", exc_info=True)

    def _refresh_symbol_consumer(self, additional_symbols: set[str] | None = None) -> None:
        setter = getattr(self.quote_service, "set_symbol_consumer", None)
        if not callable(setter):
            return
        config = self.store.load_config()
        symbols = {
            str(item.get("symbol") or "").strip().upper()
            for key in ("selected", "board_pool", "buy_pool")
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

    @classmethod
    def _scoring_rows(
        cls,
        rows: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Build the candidate-score scan set.

        Strong-stock rows plus board-pool members (e.g. sector-strength manual
        entries) whose confirm dialog needs the full v5 score. Selected-only
        rows stay excluded; duplicate symbols keep the strong-stock row.
        """
        scoring = {
            str(item.get("symbol") or "").strip().upper(): item
            for item in cls._strong_rows(rows)
            if item.get("symbol")
        }
        for item in board_pool:
            symbol = str(item.get("symbol") or "").strip().upper()
            if symbol and symbol not in scoring:
                scoring[symbol] = item
        return list(scoring.values())

    @staticmethod
    def _merge_candidate_score(
        row: dict[str, Any], score_cache: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Attach the cached candidate-score snapshot onto a board-pool row."""
        symbol = str(row.get("symbol") or "").strip().upper()
        score = score_cache.get(symbol) if symbol else None
        if not score:
            return row
        # The scoring snapshot may carry a stale change_pct; keep the row's own
        # live quote value when present.
        merged = {**row, **score}
        if row.get("change_pct") is not None:
            merged["change_pct"] = row.get("change_pct")
        return merged

    @staticmethod
    def _rank_candidates(
        candidates: list[dict[str, Any]],
        score_cache: dict[str, dict[str, Any]],
        *,
        premium_stats_provider: Callable[[str], dict[str, Any]] | None = None,
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
            # 历史涨停基因可离线得到；即使综合评分 unavailable / manual entry
            # 标的，也单独把这一维度补上，前端能单卡渲染。
            if premium_stats_provider is not None and symbol:
                detail = score.get("candidate_score_detail") or {}
                if not detail.get("premium_gene"):
                    gene = premium_gene_detail(premium_stats_provider(symbol) or {})
                    if gene:
                        detail = {**detail, "premium_gene": gene}
                        score = {
                            **score,
                            "candidate_score_detail": detail,
                            "candidate_reasons": LimitBoardService._score_reasons(
                                candidate, detail,
                            ),
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
                -LimitBoardService._sector_candidate_score(sector),
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

    def _trim_automatic_candidates(
        self,
        runtime: dict[str, Any],
        *,
        keep_symbols: set[str] | None = None,
    ) -> set[str]:
        """Persist only the best scored automatic rows; manual tracking always survives."""
        runtime_by_symbol = runtime.setdefault("symbols", {})
        # Board-pool members live outside runtime["symbols"], but their scores
        # must survive trimming so the confirm dialog keeps its v5 snapshot.
        extra_keep = set(keep_symbols or ())
        automatic_rows = [
            {"symbol": symbol, **state}
            for symbol, state in runtime_by_symbol.items()
            if {"first_board", "rebound_board"} & set(state.get("source_modes") or [])
        ]
        ranked = self._rank_candidates(
            automatic_rows,
            runtime.get("candidate_scores") or {},
        )
        live_order = [
            str(row["symbol"]).strip().upper()
            for row in ranked
            if (
                row.get("candidate_score") is not None
                and row.get("candidate_score_state") == "live"
            )
        ][:_AUTOMATIC_CANDIDATE_LIMIT]
        retained_order = [*live_order]
        if len(retained_order) < _AUTOMATIC_CANDIDATE_LIMIT:
            retained_order.extend(
                str(row["symbol"]).strip().upper()
                for row in automatic_rows
                if str(row["symbol"]).strip().upper() not in retained_order
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
        kept_symbols = set(runtime_by_symbol) | extra_keep
        runtime["candidate_scores"] = {
            symbol: value
            for symbol, value in (runtime.get("candidate_scores") or {}).items()
            if symbol in kept_symbols
        }
        runtime["candidate_score_snapshots"] = {
            symbol: value
            for symbol, value in (runtime.get("candidate_score_snapshots") or {}).items()
            if symbol in kept_symbols
        }
        return retained

    def _view_collections(
        self, runtime: dict[str, Any], config: dict[str, Any],
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
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
            row["queue"] = self._queue_watcher.snapshot(symbol)
            if not is_risk_warning_name(row["name"]):
                board_pool.append(row)
        buy_pool = []
        buy_orders = runtime.get("buy_orders") or {}
        for item in config.get("buy_pool", []):
            symbol = str(item["symbol"]).strip().upper()
            row = {
                **item,
                **runtime_by_symbol.get(symbol, {}),
                **(buy_orders.get(symbol) or {}),
                "ws_active": symbol in self._ws_symbols,
            }
            row["name"] = self._resolve_name(symbol, row.get("name"))
            row["queue"] = self._queue_watcher.snapshot(symbol)
            if not is_risk_warning_name(row["name"]):
                buy_pool.append(row)
        return rows, selected, board_pool, buy_pool

    def _candidate_rows_for_runtime(
        self,
        runtime: dict[str, Any],
        rows: list[dict[str, Any]],
        selected: list[dict[str, Any]],
        board_pool: list[dict[str, Any]],
        buy_pool: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        excluded = {
            str(symbol).strip().upper()
            for symbol in runtime.get("candidate_excluded") or []
        }
        eligible = [
            item for item in rows
            if str(item.get("symbol") or "").strip().upper() not in excluded
        ]
        pool_symbols = {
            str(item.get("symbol") or "").strip().upper()
            for item in [*board_pool, *buy_pool]
        }
        eligible = [
            item for item in eligible
            if str(item.get("symbol") or "").strip().upper() not in pool_symbols
        ]
        return self._candidate_pool(
            [item for item in eligible if "first_board" in item.get("source_modes", [])],
            [item for item in eligible if "rebound_board" in item.get("source_modes", [])],
            [
                item for item in selected
                if (
                    str(item.get("symbol") or "").strip().upper() not in excluded
                    and str(item.get("symbol") or "").strip().upper() not in pool_symbols
                )
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
            # 机构式板块评分需要覆盖 1/3/5/20 个交易日窗口。
            value = rps_rotation.build_rps_rotation(self.repo, 30, kind, level)
        except Exception:  # noqa: BLE001
            logger.warning("打板备选池读取板块轮动失败: %s", kind, exc_info=True)
            return {}
        if value.get("dates") and value.get("columns"):
            self._rotation_cache[key] = value
        return value

    def _candidate_stock_snapshot(
        self, now: datetime,
        *,
        with_date: bool = False,
    ) -> tuple[pl.DataFrame, dict[str, dict[str, Any]]] | tuple[
        pl.DataFrame, dict[str, dict[str, Any]], date | None
    ]:
        def result(
            frame: pl.DataFrame,
            rows: dict[str, dict[str, Any]],
            snapshot_date: date | None = None,
        ):
            return (frame, rows, snapshot_date) if with_date else (frame, rows)

        getter = getattr(self.quote_service, "get_enriched_today", None)
        if not callable(getter):
            return result(pl.DataFrame(), {})
        stock_df, stock_date = getter()
        if stock_date != now.date():
            # 日线锚定「最近已完成交易日」：交易时段内只接受当日实时快照；
            # 盘前/盘后/周末接受最近一个交易日的快照（如周六回看周五）。
            # 快照超过 7 天视为 pipeline 异常，保持 fail-closed。
            relaxed = now.weekday() >= 5 or not _is_trading_time(now)
            if not (
                relaxed
                and stock_date is not None
                and 0 <= (now.date() - stock_date).days <= 7
            ):
                return result(pl.DataFrame(), {})
        if stock_df is None or stock_df.is_empty():
            return result(pl.DataFrame(), {}, stock_date if isinstance(stock_date, date) else None)
        columns = [column for column in stock_df.columns if column in _SCORE_STOCK_COLUMNS]
        if "symbol" not in columns:
            return result(pl.DataFrame(), {}, stock_date if isinstance(stock_date, date) else None)
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
        return result(stock_df, rows, stock_date if isinstance(stock_date, date) else None)

    def _candidate_intraday_features(
        self, symbols: set[str], now: datetime,
    ) -> dict[str, dict[str, Any]]:
        getter = getattr(self.quote_service, "get_intraday_features", None)
        if not callable(getter) or not symbols:
            return {}
        try:
            if _is_trading_time(now) and now.weekday() < 5:
                freshness_seconds = 180
            elif now.weekday() >= 5:
                # 周末按最近一个交易日计算：周五的分钟数据在整个周末有效
                freshness_seconds = 7 * 24 * 60 * 60
            else:
                freshness_seconds = 24 * 60 * 60
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
    def _sector_candidate_score(value: dict[str, Any]) -> float:
        """Return the sector contribution used by candidate ranking.

        New snapshots carry an institutional score whose available maximum
        varies with data coverage. Normalize it to the sector's 50-point
        weight; old snapshots without the field keep their legacy score.
        """
        institutional = _finite(value.get("institutional_score"))
        institutional_max = _finite(value.get("institutional_max_score"))
        if institutional is not None and institutional_max and institutional_max > 0:
            return max(0.0, min(1.0, institutional / institutional_max)) * _SCORE_WEIGHTS["sector"]
        return float(_finite(value.get("score")) or 0.0)

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
            institutional_score = _finite(sector.get("institutional_score"))
            institutional_max = _finite(sector.get("institutional_max_score"))
            relative_strength = (
                f"机构强度 {institutional_score:.1f}/{institutional_max:.0f}"
                if institutional_score is not None and institutional_max
                else "机构强度待补"
            )
            reasons.append(
                f"{sector.get('name') or '板块'} {relative_strength}"
                f" · {sector.get('leadership') or 'follower'}"
            )
        if gene:
            reasons.append(f"涨停基因 {float(gene.get('score') or 0):.1f}/10")
        if technical:
            reasons.append(f"技术面 {float(technical.get('score') or 0):.1f}/5")
        return reasons

    @staticmethod
    def _entry_metrics(
        candidate: dict[str, Any],
        score: float | None,
        previous: dict[str, Any],
        detail: dict[str, dict[str, Any]],
        now: datetime,
    ) -> dict[str, Any]:
        """Build a buyability snapshot separately from the strength score."""
        now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
        current_score = _finite(score)
        previous_score = _finite(previous.get("candidate_score"))
        velocity = (
            current_score - previous_score
            if current_score is not None and previous_score is not None
            else None
        )
        previous_rounds = int(_finite(previous.get("candidate_score_rising_rounds")) or 0)
        rising_rounds = (
            min(3, previous_rounds + 1)
            if velocity is not None and velocity > _ENTRY_SCORE_RISING_DELTA
            else 0
        )
        gap = _finite(candidate.get("limit_gap_pct"))
        status = str(candidate.get("status") or "watching")
        quote_at = _quote_time(candidate.get("last_quote_at") or candidate.get("timestamp"))
        quote_age = (
            max(0.0, (now_aware - quote_at).total_seconds())
            if quote_at is not None and quote_at.date() == now_aware.date()
            else None
        )
        flow = detail.get("intraday_flow") or {}
        flow_score = _finite(flow.get("score")) or 0.0
        gap_score = (
            max(0.0, min(1.0, 1.0 - abs(gap - 0.015) / 0.015))
            if gap is not None else 0.0
        )
        velocity_score = (
            max(0.0, min(1.0, velocity / 3.0))
            if velocity is not None else 0.0
        )
        entry_score = (
            round(
                (current_score or 0.0) * 0.50
                + velocity_score * 20.0
                + max(0.0, min(1.0, flow_score / 15.0)) * 15.0
                + gap_score * 15.0,
                1,
            )
            if current_score is not None and gap is not None else None
        )

        market_time = now_aware.timetz().replace(tzinfo=None)
        if status in {"touched", "sealed", "broken", "resealed"} or (
            gap is not None and gap <= 0.0001
        ):
            state, reason = "limit_reached", "已触及涨停或出现封板状态"
        elif not _is_trading_time(now_aware):
            state, reason = "closed", "当前不在连续竞价时段"
        elif market_time < clock_time(9, 35):
            state, reason = "warming", "等待 09:35 后确认开盘强度"
        elif gap is None:
            state, reason = "unavailable", "涨停价或距涨停数据不可用"
        elif current_score is None:
            state, reason = "unavailable", "强势确认分尚未计算"
        elif quote_age is None or quote_age > _ENTRY_QUOTE_FRESH_SECONDS:
            state, reason = "stale", f"行情超过 {_ENTRY_QUOTE_FRESH_SECONDS:.0f} 秒未更新"
        elif gap < _ENTRY_MIN_LIMIT_GAP_PCT:
            state, reason = "too_close", "距涨停过近，成交空间不足"
        elif gap > _ENTRY_MAX_LIMIT_GAP_PCT:
            state, reason = "too_far", "尚未进入可交易的强势区间"
        elif velocity is None or rising_rounds < 2:
            state, reason = "warming", "等待强势分连续两轮上升"
        elif velocity <= _ENTRY_SCORE_RISING_DELTA:
            state, reason = "weakening", "强势分上升动能不足"
        else:
            state, reason = "tradable", "强势分上升且仍有成交空间"

        return {
            "entry_score": entry_score,
            "entry_rank": None,
            "candidate_score_velocity": round(velocity, 2) if velocity is not None else None,
            "candidate_score_rising_rounds": rising_rounds,
            "tradability_state": state,
            "tradability_reason": reason,
            "entry_score_detail": {
                "strength": round((current_score or 0.0) * 0.50, 2),
                "velocity": round(velocity_score * 20.0, 2),
                "intraday_flow": round(max(0.0, min(1.0, flow_score / 15.0)) * 15.0, 2),
                "limit_gap": round(gap_score * 15.0, 2),
                "quote_age_seconds": round(quote_age, 1) if quote_age is not None else None,
            },
            "entry_reasons": [reason],
        }

    @staticmethod
    def _rank_opportunities(
        candidates: list[dict[str, Any]],
        score_cache: dict[str, dict[str, Any]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
        result = []
        for candidate in candidates:
            symbol = str(candidate.get("symbol") or "").strip().upper()
            score = score_cache.get(symbol) or {}
            row = {**candidate, **score}
            state = str(row.get("tradability_state") or "unavailable")
            gap = _finite(row.get("limit_gap_pct"))
            status = str(row.get("status") or "watching")
            quote_at = _quote_time(row.get("last_quote_at") or row.get("timestamp"))
            quote_age = (
                (now_aware - quote_at).total_seconds()
                if quote_at is not None and quote_at.date() == now_aware.date()
                else None
            )
            if status in {"touched", "sealed", "broken", "resealed"} or (
                gap is not None and gap <= 0.0001
            ):
                continue
            if gap is None or gap < _ENTRY_MIN_LIMIT_GAP_PCT or gap > _ENTRY_MAX_LIMIT_GAP_PCT:
                continue
            if not _is_trading_time(now_aware) or now_aware.timetz().replace(tzinfo=None) < clock_time(9, 35):
                continue
            if quote_age is None or quote_age > _ENTRY_QUOTE_FRESH_SECONDS:
                continue
            if row.get("candidate_score_state") != "live" or state != "tradable":
                continue
            result.append(row)
        result.sort(key=lambda row: (
            -float(_finite(row.get("entry_score")) or 0.0),
            -float(_finite(row.get("candidate_score_velocity")) or 0.0),
            -float(_finite(row.get("candidate_score")) or 0.0),
            float(_finite(row.get("limit_gap_pct")) or float("inf")),
            str(row.get("symbol") or ""),
        ))
        for rank, row in enumerate(result, start=1):
            row["entry_rank"] = rank
        return result

    def _refresh_candidate_scores(
        self,
        runtime: dict[str, Any],
        candidates: list[dict[str, Any]],
        now: datetime,
        *,
        wait_for_lock: bool = False,
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
        acquired = (
            self._score_lock.acquire(timeout=_SCORE_ON_DEMAND_WAIT_SECONDS)
            if wait_for_lock
            else self._score_lock.acquire(blocking=False)
        )
        if not acquired:
            return False
        try:
            now_mono = time.monotonic()
            previous_cache = runtime.get("candidate_scores") or {}
            missing = any(symbol not in previous_cache for symbol in symbols)
            if not missing and now_mono - self._score_refresh_at < _SCORE_REFRESH_SECONDS:
                return False
            _, stock_rows, stock_date = self._candidate_stock_snapshot(now, with_date=True)
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
            realtime_sectors, realtime_anchor = self._sector_strength_snapshot(now.date())
            rotation_cutoff = (
                realtime_anchor
                if isinstance(realtime_anchor, date)
                else stock_date
                if isinstance(stock_date, date)
                else now.date()
            )
            rotations = {
                "concept": self._rotation("concept", None, rotation_cutoff),
                "industry": self._rotation("industry", 2, rotation_cutoff),
            } if sector_service is not None else {}

            valid_snapshots = dict(runtime.get("candidate_score_snapshots") or {})
            for symbol, value in previous_cache.items():
                if (
                    symbol not in valid_snapshots
                    and value.get("candidate_score") is not None
                    and _quote_time(value.get("candidate_score_as_of")) is not None
                    and _is_current_score_snapshot(value)
                ):
                    valid_snapshots[symbol] = dict(value)
            non_trading_cache = (
                not _is_trading_time(now)
                and now.timetz().replace(tzinfo=None) >= clock_time(11, 30)
            )
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
                prev_close = (stock_rows.get(symbol) or {}).get("prev_close") or candidate_quotes.get(symbol, {}).get("prev_close")
                # 涨停价优先取行情/候选行自带值；都没有时按板块规则从昨收推算，
                # 保证封板检测、封板前量能窗口在手动入口标的上同样生效。
                score_limit_up = (
                    _finite(candidate.get("limit_up"))
                    or _finite(candidate_quotes.get(symbol, {}).get("limit_up"))
                    or self._limit_up(
                        {"prev_close": prev_close},
                        symbol,
                        self._resolve_name(symbol, (stock_rows.get(symbol) or {}).get("name")),
                        now.date(),
                    )
                )
                intraday_flow = self._scale_score_detail(
                    intraday_flow_detail(
                        intraday_features.get(symbol),
                        previous_close=prev_close,
                        external_flow=flow_snapshots.get(symbol),
                        as_of=now.isoformat(),
                        limit_up=score_limit_up,
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
                            # 实时成员行情不可用（盘后/周末/重启）时，
                            # 用收盘快照的冻结值计算实时类组件。
                            sector_inputs = self._close_frozen_sector_inputs(
                                realtime, stock_rows, realtime_anchor,
                            )
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
                            today=rotation_cutoff,
                            realtime=realtime,
                            realtime_snapshot=sector_snapshot,
                        )
                        if value is not None:
                            value["as_of"] = now.isoformat()
                            value["data_source"] = sector_snapshot.get("data_source") or "kaipanla_socket"
                            value["close_frozen"] = sector_snapshot.get("data_source") == "daily_close"
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
                if sector is None:
                    # 实时板块行情不可用（周末/盘后/socket 断开）时，用日频
                    # 轮动数据降级出板块形态/过热；实时类组件返回 None，
                    # 由综合评分的数据门控显式标记为数据不足。
                    for kind in ("concept", "industry"):
                        targets = symbol_targets.get(kind) or []
                        if not targets:
                            continue
                        sector = rotation_only_detail(
                            targets[0], rotations.get(kind) or {}, rotation_cutoff,
                        )
                        if sector is not None:
                            sector["as_of"] = now.isoformat()
                            sector["data_source"] = "rps_rotation"
                            break
                sector = self._scale_score_detail(sector, _SCORE_WEIGHTS["sector"], 50.0)
                gene = self._scale_score_detail(
                    premium_gene_detail(self._premium_stats.get(symbol) or {}),
                    _SCORE_WEIGHTS["premium_gene"],
                    10.0,
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

                # 计算综合评分（100分制）
                board_quality_data = {
                    "break_count": candidate.get("break_count"),
                    "bid1_volume": candidate.get("bid1_volume"),
                }
                # TODO: 如果有四合一策略评分，可以传入 four_mode_score 参数
                comprehensive = comprehensive_score(
                    detail,
                    board_quality=board_quality_data,
                    four_mode_score=None,  # 暂时为空，后续可集成四合一策略
                )
                if comprehensive:
                    detail["comprehensive"] = comprehensive

                flow_detail = detail.get("intraday_flow") or {}
                complete = (
                    bool(flow_detail.get("capital_available"))
                    and all(detail.get(key) for key in _SCORE_WEIGHTS)
                    and bool((detail.get("sector") or {}).get("rotation_available", True))
                )
                base_score = (
                    sum(
                        LimitBoardService._sector_candidate_score(detail[key])
                        if key == "sector"
                        else float(detail[key]["score"])
                        for key in _SCORE_WEIGHTS
                    )
                    if complete else None
                )
                score = round(base_score, 1) if base_score is not None else None
                state = (
                    "cached"
                    if complete and (cached_component or non_trading_cache)
                    else "live" if complete else "unavailable"
                )
                entry = self._entry_metrics(
                    {**candidate, **candidate_quotes.get(symbol, {})},
                    score,
                    previous,
                    detail,
                    now,
                )
                current = {
                    "change_pct": change_pct,
                    "candidate_score": score,
                    "candidate_rank": None,
                    "candidate_score_state": state,
                    "candidate_score_as_of": now.isoformat(),
                    "candidate_score_detail": detail,
                    "candidate_reasons": self._score_reasons(candidate, detail),
                    **entry,
                }
                if complete:
                    valid_snapshots[symbol] = dict(current)
                    refreshed[symbol] = current
                    continue

                snapshot = valid_snapshots.get(symbol) or {}
                captured_at = _quote_time(snapshot.get("candidate_score_as_of"))
                cache_age = (
                    (now - captured_at).total_seconds()
                    if captured_at is not None and captured_at.date() == now.date()
                    else None
                )
                if (
                    snapshot.get("candidate_score") is not None
                    and _is_current_score_snapshot(snapshot)
                    and cache_age is not None
                    and cache_age >= 0
                    and (non_trading_cache or cache_age <= _SCORE_DISPLAY_CACHE_SECONDS)
                ):
                    snapshot_entry = self._entry_metrics(
                        {**candidate, **candidate_quotes.get(symbol, {})},
                        _finite(snapshot.get("candidate_score")),
                        previous,
                        snapshot.get("candidate_score_detail") or {},
                        now,
                    )
                    refreshed[symbol] = {
                        **snapshot,
                        "change_pct": change_pct,
                        "candidate_rank": None,
                        "candidate_score_state": "cached",
                        **snapshot_entry,
                    }
                else:
                    refreshed[symbol] = current
            changed = refreshed != previous_cache
            runtime["candidate_scores"] = refreshed
            runtime["candidate_score_snapshots"] = valid_snapshots
            self._score_refresh_at = now_mono
            return changed
        finally:
            self._score_lock.release()

    def view(self) -> dict[str, Any]:
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        self._refresh_sector_candidate_universe(cn_today())
        rows, selected, board_pool, buy_pool = self._view_collections(runtime, config)
        candidates = self._candidate_rows_for_runtime(
            runtime, rows, selected, board_pool, buy_pool,
        )
        if self._refresh_candidate_scores(
            runtime, self._scoring_rows(rows, board_pool), cn_now(),
        ):
            self._persist_runtime(runtime)
        # Keep both API projections on the same per-symbol score snapshot.
        score_cache = runtime.get("candidate_scores") or {}
        board_pool = [
            self._merge_candidate_score(row, score_cache)
            for row in board_pool
        ]
        premium_stats_provider = (
            lambda symbol: self._premium_stats.get(str(symbol).strip().upper()) or {}
        )
        candidate_pool = self._rank_candidates(
            candidates, score_cache, premium_stats_provider=premium_stats_provider,
        )
        first_board = self._rank_candidates(
            self._strong_rows(rows),
            score_cache,
            premium_stats_provider=premium_stats_provider,
        )
        rebound_board = self._rank_candidates(
            [item for item in self._strong_rows(rows) if "rebound_board" in item.get("source_modes", [])],
            score_cache,
            premium_stats_provider=premium_stats_provider,
        )
        opportunity_pool = self._rank_opportunities(
            self._strong_rows(rows),
            score_cache,
            cn_now(),
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
            *rows, *selected, *board_pool, *buy_pool, *candidate_pool,
            *opportunity_pool, *first_board, *rebound_board, *events,
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
        market_sentiment = self._market_sentiment_snapshot()
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
            "opportunity_pool": opportunity_pool,
            "board_pool": board_pool,
            "buy_pool": buy_pool,
            "blacklist": [
                symbol for symbol in runtime.get("blacklist", [])
                if not is_risk_warning_name(self._resolve_name(str(symbol).strip().upper()))
            ],
            "market_sentiment": market_sentiment,
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
                "limit_up_queue": self._queue_watcher.status(),
            },
        }

    def candidate_score_snapshot(self, symbol: str) -> dict[str, Any]:
        """On-demand v5 score for one symbol (confirm-dialog preview).

        Sector-strength / radar entries are not part of the recurring scoring
        scan set until they join the board pool, so the dialog triggers this
        per-symbol refresh. The full scan set is always passed because
        ``_refresh_candidate_scores`` replaces the whole candidate-score cache.
        """
        cleaned = str(symbol or "").strip().upper()
        if not cleaned:
            raise ValueError("缺少股票代码")
        config = self.store.load_config()
        runtime = self._runtime_for_today()
        rows, _selected, board_pool, _buy_pool = self._view_collections(runtime, config)
        scoring = self._scoring_rows(rows, board_pool)
        known = {
            str(item.get("symbol") or "").strip().upper() for item in scoring
        }
        if cleaned not in known:
            row = next(
                (
                    item for item in [*board_pool, *rows]
                    if str(item.get("symbol") or "").strip().upper() == cleaned
                ),
                None,
            )
            scoring = [*scoring, row or {"symbol": cleaned}]
        self._refresh_candidate_scores(
            runtime, scoring, cn_now(), wait_for_lock=True,
        )
        entry = (runtime.get("candidate_scores") or {}).get(cleaned) or {}
        return {
            "symbol": cleaned,
            "candidate_score": entry.get("candidate_score"),
            "candidate_score_state": entry.get("candidate_score_state") or "unavailable",
            "candidate_score_as_of": entry.get("candidate_score_as_of"),
            "candidate_score_detail": entry.get("candidate_score_detail") or {},
            "candidate_reasons": entry.get("candidate_reasons") or [],
        }

    def update_advanced_settings(
        self, settings: dict[str, Any], revision: int,
    ) -> dict[str, Any]:
        values = {
            "sweep_price_levels": int(settings["sweep_price_levels"]),
            "queue_wait_seconds": int(settings["queue_wait_seconds"]),
            "queue_confirm_snapshots": int(settings["queue_confirm_snapshots"]),
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

    @staticmethod
    def _pool_allocation(
        mode: str | None,
        value: float | None,
        *,
        default: str,
    ) -> tuple[str, float | None]:
        cleaned_mode = str(mode or default).strip().lower()
        if cleaned_mode in _LEGACY_POOL_ALLOCATION_MODES:
            legacy = "跟随全局" if cleaned_mode == "global" else "一手"
            raise ValueError(f"「{legacy}」资金方式已废弃，请选择其他方式")
        if cleaned_mode not in _POOL_ALLOCATION_MODES:
            raise ValueError("交易数量/金额方式无效")
        if cleaned_mode in {"available", "sixth", "fifth", "quarter"}:
            if value is not None:
                raise ValueError("当前金额或比例配置不应填写固定金额")
            return cleaned_mode, None
        if cleaned_mode == "fixed" and value is None:
            value = _DEFAULT_FIXED_ALLOCATION_VALUE
        if value is None or not float(value) > 0:
            raise ValueError("固定金额或固定数量必须大于 0")
        if cleaned_mode == "volume":
            integer_value = int(value)
            if integer_value != value or integer_value < 100 or integer_value % 100:
                raise ValueError("固定数量必须是 100 股的整数倍")
            return cleaned_mode, float(integer_value)
        return cleaned_mode, float(value)

    def _pool_quote(self, symbol: str) -> dict[str, Any]:
        payload = self._fresh_tickflow_quotes({symbol})
        raw = (payload.get("quotes") or {}).get(symbol)
        if not isinstance(raw, dict):
            with self._lock:
                raw = dict(self._quotes.get(symbol) or {})
        price = _finite(raw.get("last_price", raw.get("close"))) if isinstance(raw, dict) else None
        quote_at = _quote_time(raw.get("timestamp")) if isinstance(raw, dict) else None
        now = cn_now()
        now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
        if (
            not isinstance(raw, dict)
            or price is None
            or price <= 0
            or quote_at is None
            or quote_at.date() != now_aware.date()
            or (now_aware - quote_at).total_seconds() > _DEPTH_FRESH_SECONDS
        ):
            raise ValueError("缺少 30 秒内的 TickFlow 实时行情，无法立即挂单")
        return {
            **raw,
            "symbol": symbol,
            "last_price": price,
            "timestamp": quote_at.isoformat(),
        }

    def _qmt_ready(self) -> Any:
        qmt = self._qmt()
        status = qmt.status() if qmt is not None else {}
        if not (
            qmt is not None
            and status.get("configured")
            and status.get("state") == "ready"
            and status.get("trade_enabled")
        ):
            raise ValueError(str(status.get("reason") or "QMT 实盘交易未就绪"))
        return qmt

    def _buy_order_preview(
        self,
        qmt: Any,
        symbol: str,
        price: float,
        allocation_mode: str,
        allocation_value: float | None,
        credit_buy_mode: str = "collateral",
    ) -> dict[str, Any]:
        if allocation_mode == "volume":
            volume = int(allocation_value or 0)
            if volume < 100 or volume % 100:
                raise ValueError("固定数量必须是 100 股的整数倍")
            preview_getter = getattr(qmt, "preview_order", None)
            if not callable(preview_getter):
                raise RuntimeError("QMT 不支持金额预览，无法确认固定数量的可用资金")
            preview = preview_getter({
                "action": "BUY",
                "symbol": symbol,
                "price": price,
                "price_type": "LIMIT",
                "reference_price": price,
                "allocation_mode": "fixed",
                "allocation_value": price * volume,
                "credit_buy_mode": credit_buy_mode,
            })
            preview_volume = int(preview.get("volume") or 0)
            if preview_volume < volume:
                raise ValueError(str(preview.get("reason") or "QMT 可用资金不足，无法买入一手"))
            return {
                "volume": volume,
                "actual_amount": round(price * volume, 2),
                "target_amount": round(price * volume, 2),
                "capped": False,
                "credit_buy_mode": preview.get("credit_buy_mode") or credit_buy_mode,
                "requested_credit_buy_mode": preview.get("requested_credit_buy_mode") or credit_buy_mode,
                "credit_buy_mode_switched": bool(preview.get("credit_buy_mode_switched")),
                "credit_buy_mode_reason": preview.get("credit_buy_mode_reason"),
            }
        preview_getter = getattr(qmt, "preview_order", None)
        if not callable(preview_getter):
            raise RuntimeError("QMT 不支持金额预览，无法确认买入金额")
        preview = preview_getter({
            "action": "BUY",
            "symbol": symbol,
            "price": price,
            "price_type": "LIMIT",
            "reference_price": price,
            "allocation_mode": allocation_mode,
            "allocation_value": allocation_value,
            "credit_buy_mode": credit_buy_mode,
        })
        volume = int(preview.get("volume") or 0)
        if volume < 100:
            raise ValueError(str(preview.get("reason") or "金额不足一手"))
        return {
            "volume": volume,
            "actual_amount": round(float(preview.get("actual_amount") or price * volume), 2),
            "target_amount": round(float(preview.get("target_amount") or price * volume), 2),
            "capped": bool(preview.get("capped")),
            "credit_buy_mode": preview.get("credit_buy_mode") or credit_buy_mode,
            "requested_credit_buy_mode": preview.get("requested_credit_buy_mode") or credit_buy_mode,
            "credit_buy_mode_switched": bool(preview.get("credit_buy_mode_switched")),
            "credit_buy_mode_reason": preview.get("credit_buy_mode_reason"),
        }

    def add_pool(
        self,
        symbol: str,
        source: str,
        revision: int,
        allocation_mode: str = "fixed",
        allocation_value: float | None = None,
        credit_buy_mode: str = "collateral",
    ) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)
        allocation_mode, allocation_value = self._pool_allocation(
            allocation_mode,
            allocation_value,
            default="fixed",
        )
        if credit_buy_mode not in {"collateral", "financing"}:
            raise ValueError("信用账户买入方式无效")
        capacity_error = self._pool_websocket_capacity_error(
            self.store.load_config(), {cleaned},
        )
        if capacity_error:
            raise RuntimeError(capacity_error)

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value["board_pool"]):
                return
            if any(str(item.get("symbol")) == cleaned for item in value.get("buy_pool", [])):
                raise ValueError("该股票已在买入池中")
            entry = {
                "symbol": cleaned,
                "name": name,
                "source": source,
                "auto_trade": True,
                "order_mode": "sweep",
                "allocation_mode": allocation_mode,
                "credit_buy_mode": credit_buy_mode,
                "added_at": cn_now().isoformat(),
            }
            if allocation_value is not None:
                entry["allocation_value"] = allocation_value
            value["board_pool"].append({
                **entry,
            })

        saved = self.store.update(revision, update)
        self._schedule_pool_refresh(self._runtime_for_today(), saved)
        return saved

    def update_pool(
        self,
        symbol: str,
        auto_trade: bool,
        order_mode: str,
        revision: int,
        allocation_mode: str | None = None,
        allocation_value: float | None = None,
        credit_buy_mode: str | None = None,
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
            if allocation_mode is not None:
                mode, normalized_value = self._pool_allocation(
                    allocation_mode,
                    allocation_value,
                    default="fixed",
                )
                member["allocation_mode"] = mode
                if normalized_value is None:
                    member.pop("allocation_value", None)
                else:
                    member["allocation_value"] = normalized_value
            if credit_buy_mode is not None:
                if credit_buy_mode not in {"collateral", "financing"}:
                    raise ValueError("信用账户买入方式无效")
                member["credit_buy_mode"] = credit_buy_mode

        saved = self.store.update(revision, update)
        self._schedule_pool_refresh(None, saved)
        return saved

    def add_buy_pool(
        self,
        symbol: str,
        source: str,
        revision: int,
        allocation_mode: str = "fixed",
        allocation_value: float | None = None,
        credit_buy_mode: str = "collateral",
        order_price: float | None = None,
    ) -> dict[str, Any]:
        cleaned, name = self._validated_stock(symbol)
        allocation_mode, allocation_value = self._pool_allocation(
            allocation_mode,
            allocation_value,
            default="fixed",
        )
        if credit_buy_mode not in {"collateral", "financing"}:
            raise ValueError("信用账户买入方式无效")
        capacity_error = self._pool_websocket_capacity_error(
            self.store.load_config(), {cleaned},
        )
        if capacity_error:
            raise RuntimeError(capacity_error)
        now = cn_now()
        now_aware = now if now.tzinfo else now.replace(tzinfo=CN_TZ)
        requested_price = _finite(order_price)
        if _is_trading_time(now_aware):
            quote = self._pool_quote(cleaned)
            price = float(quote["last_price"])
        elif requested_price is not None:
            price = requested_price
        else:
            raise ValueError("盘后委托需要填写有效的页面价格")
        qmt = self._qmt_ready()
        preview = self._buy_order_preview(
            qmt,
            cleaned,
            price,
            allocation_mode,
            allocation_value,
            credit_buy_mode,
        )
        idempotency_key = f"limit-buy-{cn_today().strftime('%Y%m%d')}-{cleaned}"

        def update(value: dict[str, Any]) -> None:
            if any(str(item.get("symbol")) == cleaned for item in value.get("buy_pool", [])):
                raise ValueError("该股票已在买入池中")
            if any(str(item.get("symbol")) == cleaned for item in value["board_pool"]):
                raise ValueError("该股票已在打板池中")
            entry = {
                "symbol": cleaned,
                "name": name,
                "source": source,
                "allocation_mode": allocation_mode,
                "credit_buy_mode": credit_buy_mode,
                "order_price": price,
                "order_volume": preview["volume"],
                "order_amount": preview["actual_amount"],
                "order_idempotency_key": idempotency_key,
                "added_at": cn_now().isoformat(),
            }
            if allocation_value is not None:
                entry["allocation_value"] = allocation_value
            value.setdefault("buy_pool", []).append(entry)

        saved = self.store.update(revision, update)
        runtime = self._runtime_for_today()
        buy_orders = runtime.setdefault("buy_orders", {})
        buy_orders[cleaned] = {
            "order_status": "submitting",
            "order_idempotency_key": idempotency_key,
            "order_price": price,
            "order_volume": preview["volume"],
            "order_amount": preview["actual_amount"],
            "order_error": None,
            "order_at": cn_now().isoformat(timespec="milliseconds"),
        }
        self._persist_runtime(runtime)
        try:
            request: dict[str, Any] = {
                "idempotency_key": idempotency_key,
                "strategy_name": "limit_board",
                "action": "BUY",
                "symbol": cleaned,
                "price": price,
                "price_type": "LIMIT",
                "trigger_at": cn_now().isoformat(timespec="milliseconds"),
                "system_order_at": cn_now().isoformat(timespec="milliseconds"),
            }
            if allocation_mode == "volume":
                request["volume"] = int(allocation_value or 0)
            else:
                request["allocation_mode"] = allocation_mode
                request["allocation_value"] = allocation_value
            request["credit_buy_mode"] = credit_buy_mode
            order = qmt.submit_order(request)
            result = {
                "status": str(order.get("status") or "unknown"),
                "order_sys_id": order.get("order_sys_id"),
                "error": order.get("error"),
                "volume": order.get("volume") or preview["volume"],
                "estimated_amount": order.get("estimated_amount") or preview["actual_amount"],
                "qmt_submit_at": order.get("qmt_submit_at"),
                "qmt_accepted_at": order.get("qmt_accepted_at"),
                "broker_order_at": order.get("broker_order_at"),
                "credit_buy_mode": order.get("credit_buy_mode") or preview.get("credit_buy_mode") or credit_buy_mode,
                "requested_credit_buy_mode": order.get("requested_credit_buy_mode") or preview.get("requested_credit_buy_mode") or credit_buy_mode,
                "credit_buy_mode_switched": bool(order.get("credit_buy_mode_switched") or preview.get("credit_buy_mode_switched")),
                "credit_buy_mode_reason": order.get("credit_buy_mode_reason") or preview.get("credit_buy_mode_reason"),
            }
        except Exception as exc:  # noqa: BLE001
            result = {
                "status": "blocked" if isinstance(exc, (ValueError, QmtOrderPreflightError)) else "unknown",
                "order_sys_id": None,
                "error": str(exc),
                "volume": preview["volume"],
                "estimated_amount": preview["actual_amount"],
                "credit_buy_mode": preview.get("credit_buy_mode") or credit_buy_mode,
                "requested_credit_buy_mode": preview.get("requested_credit_buy_mode") or credit_buy_mode,
                "credit_buy_mode_switched": bool(preview.get("credit_buy_mode_switched")),
                "credit_buy_mode_reason": preview.get("credit_buy_mode_reason"),
            }
        buy_orders[cleaned].update({
            "order_status": result["status"],
            "order_sys_id": result.get("order_sys_id"),
            "order_error": result.get("error"),
            "order_volume": result.get("volume") or preview["volume"],
            "order_amount": result.get("estimated_amount") or preview["actual_amount"],
            "order_updated_at": cn_now().isoformat(timespec="milliseconds"),
            "order_qmt_submit_at": result.get("qmt_submit_at"),
            "order_qmt_accepted_at": result.get("qmt_accepted_at"),
            "order_broker_at": result.get("broker_order_at"),
            "credit_buy_mode": result.get("credit_buy_mode") or preview.get("credit_buy_mode") or credit_buy_mode,
            "requested_credit_buy_mode": result.get("requested_credit_buy_mode") or preview.get("requested_credit_buy_mode") or credit_buy_mode,
            "credit_buy_mode_switched": bool(result.get("credit_buy_mode_switched") or preview.get("credit_buy_mode_switched")),
            "credit_buy_mode_reason": result.get("credit_buy_mode_reason") or preview.get("credit_buy_mode_reason"),
        })
        self._persist_runtime(runtime)
        self._schedule_pool_refresh(runtime, saved)
        return {"config": saved, "order": {"symbol": cleaned, **result}}

    def remove_buy_pool(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        return self.remove_buy_pool_batch([cleaned], revision)

    def remove_buy_pool_batch(self, symbols: list[str], revision: int) -> dict[str, Any]:
        cleaned_symbols = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        if not cleaned_symbols:
            raise ValueError("至少选择一只股票")
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "buy_pool",
                [item for item in value.get("buy_pool", []) if str(item.get("symbol")).strip().upper() not in cleaned_symbols],
            ),
        )
        runtime = self._runtime_for_today()
        buy_orders = runtime.setdefault("buy_orders", {})
        for cleaned in cleaned_symbols:
            buy_orders.pop(cleaned, None)
        self._persist_runtime(runtime)
        self._refresh_symbol_consumer()
        self._sync_websocket(runtime, saved)
        self._on_market_fetch()
        self._notify_updated()
        return saved

    def remove_pool(self, symbol: str, revision: int) -> dict[str, Any]:
        cleaned = str(symbol).strip().upper()
        return self.remove_pool_batch([cleaned], revision)

    def remove_pool_batch(self, symbols: list[str], revision: int) -> dict[str, Any]:
        cleaned_symbols = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        if not cleaned_symbols:
            raise ValueError("至少选择一只股票")
        saved = self.store.update(
            revision,
            lambda value: value.__setitem__(
                "board_pool",
                [item for item in value["board_pool"] if str(item.get("symbol")).strip().upper() not in cleaned_symbols],
            ),
        )
        self._sync_websocket(self._runtime_for_today(), saved)
        self._refresh_symbol_consumer()
        self._notify_updated()
        return saved
