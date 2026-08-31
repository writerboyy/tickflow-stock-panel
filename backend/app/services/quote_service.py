"""全局实时行情服务。

集中管理全市场行情拉取 + enriched 缓存，供盘中选股、自选股等所有模块复用。

架构:
  - 后台线程轮询 TickFlow get_by_universes(["CN_Equity_A", "CN_Index"])
  - 拉取行情 → 写 kline_daily (不复权) + 增量计算 enriched → 写盘 + 更新缓存
  - _enriched_cache 是唯一的盘中数据源 (OHLCV + 全套技术指标)
  - _live_agg_cache 是递推状态 (只加载一次, 盘中不变)

数据流 (每轮 ~15s):
  1. API 拉取 → raw_records (临时变量)
  2. raw_records → 写 kline_daily (不复权原始价格)
  3. raw_records → 更新 _enriched_cache 的 OHLCV
  4. 增量计算 enriched 指标 (~50ms)
  5. 写 kline_daily_enriched + 替换 _enriched_cache
  6. 通知 SSE

生命周期:
  - 服务启动时读取 preferences，若 enabled 则自动启动线程
  - 运行中可通过 API 切换开关
  - 关闭时停止线程
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta
from typing import Any, Callable

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.parquet import scan_daily_parquet
from app.strategy.intraday_signals import IntradaySignalEvaluator

logger = logging.getLogger(__name__)


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") else None

# Webhook(飞书等)投递专用线程池 —— 与行情轮询线程隔离。
# send_feishu 内置重试(最坏 ~3×5s 超时 + 退避), 若在 _poll_loop 上同步投递,
# webhook 慢/宕机会逐条累加, 拖垮整条实时行情+告警轮询。这里 fire-and-forget,
# 失败由 webhook_adapter 记 WARNING(可见), 但绝不阻塞热路径。
_WEBHOOK_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="feishu-webhook")


class QuoteSubscriber:
    """一个 SSE 连接对应一个订阅者: 独立事件 + 独立队列。

    此前四个通道共用服务级 Event + pending 列表, pop 是「取走」语义:
    多客户端 (多标签页/多设备) 时告警只会被先醒来的连接消费, 其余永远
    收不到; 共享 Event 的 clear/wait 也存在互相吞信号的竞态。
    改为每连接独立订阅者后, 事件对所有客户端广播。
    """

    def __init__(self, max_alerts: int = 1000, max_reviews: int = 200) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._max_alerts = max_alerts
        self._max_reviews = max_reviews
        self._quote_updated = False
        self._strategy_results_updated = False
        self._depth_updated = False
        self._large_orders_updated = False
        self._position_risk_updated = False
        self._limit_board_updated = False
        self._alerts: list[dict] = []
        self._reviews: list[str] = []

    # ── 消费侧 (SSE generator 线程) ──────────────────────
    def wait(self, timeout: float = 5.0) -> bool:
        """阻塞等待任一通道有新信号。"""
        return self._event.wait(timeout=timeout)

    def pop(self) -> dict:
        """原子取走全部待推送内容并复位事件。"""
        with self._lock:
            out = {
                "quote_updated": self._quote_updated,
                "strategy_results_updated": self._strategy_results_updated,
                "depth_updated": self._depth_updated,
                "large_orders_updated": self._large_orders_updated,
                "position_risk_updated": self._position_risk_updated,
                "limit_board_updated": self._limit_board_updated,
                "alerts": self._alerts,
                "reviews": self._reviews,
            }
            self._quote_updated = False
            self._strategy_results_updated = False
            self._depth_updated = False
            self._large_orders_updated = False
            self._position_risk_updated = False
            self._limit_board_updated = False
            self._alerts = []
            self._reviews = []
            self._event.clear()
            return out

    # ── 生产侧 (行情轮询 / depth / 复盘线程) ─────────────
    def push_alerts(self, alerts: list[dict]) -> None:
        with self._lock:
            self._alerts.extend(alerts)
            if len(self._alerts) > self._max_alerts:  # 背压: 丢弃最旧
                self._alerts = self._alerts[-self._max_alerts:]
            self._event.set()

    def push_review(self, event_json: str) -> None:
        with self._lock:
            self._reviews.append(event_json)
            if len(self._reviews) > self._max_reviews:
                self._reviews = self._reviews[-self._max_reviews:]
            self._event.set()

    def clear_alerts(self) -> None:
        with self._lock:
            self._alerts = []
            if (
                not self._quote_updated
                and not self._strategy_results_updated
                and not self._depth_updated
                and not self._large_orders_updated
                and not self._position_risk_updated
                and not self._limit_board_updated
                and not self._reviews
            ):
                self._event.clear()

    def notify_quote(self) -> None:
        with self._lock:
            self._quote_updated = True
            self._event.set()

    def notify_strategy_results(self) -> None:
        with self._lock:
            self._strategy_results_updated = True
            self._event.set()

    def notify_depth(self) -> None:
        with self._lock:
            self._depth_updated = True
            self._event.set()

    def notify_large_orders(self) -> None:
        with self._lock:
            self._large_orders_updated = True
            self._event.set()

    def notify_position_risk(self) -> None:
        with self._lock:
            self._position_risk_updated = True
            self._event.set()

    def notify_limit_board(self) -> None:
        with self._lock:
            self._limit_board_updated = True
            self._event.set()


# 落盘节流间隔: last_fetch_ms 仅在进程重启后用于显示"最后获取时间"(运行中读内存值),
# 每 30s 持久化一次足够, 避免 expert 档每秒一轮的全量 preferences 重写磁盘。
_LAST_FETCH_WRITE_INTERVAL_MS = 30_000.0
_last_fetch_written_at_ms: float = 0.0


def _persist_last_fetch(fetched_at_ms: float) -> None:
    """把"最后获取"时间戳持久化到 preferences, 使进程重启后仍可显示。

    放在锁外调用 (IO); 失败不影响主流程 (内存值已更新, 下次 fetch 再写)。
    距上次成功落盘不足 30s 时跳过 (节流只影响落盘频率, 内存值不受影响)。
    """
    global _last_fetch_written_at_ms
    if (fetched_at_ms - _last_fetch_written_at_ms) < _LAST_FETCH_WRITE_INTERVAL_MS:
        return
    try:
        from app.services import preferences
        preferences.save({"last_fetch_ms": round(fetched_at_ms, 0)})
        _last_fetch_written_at_ms = fetched_at_ms
    except Exception as e:  # noqa: BLE001
        logger.debug("last_fetch_ms 持久化失败 (不影响行情): %s", e)


def _monitor_name_map(repo) -> dict[str, str]:
    """监控回填用的 symbol → name 映射 (股票 + ETF + 指数, 股票优先)。

    走 repo.get_name_map() 的进程内 memo (三份 instruments 维表刷新时失效),
    避免每轮监控对 ~7000 行维表 iter_rows 重建。过滤空名称与旧行为一致。
    """
    return {s: n for s, n in repo.get_name_map().items() if n}


class QuoteService:
    """全局实时行情服务 — 单例。"""

    CORE_INDEX_SYMBOLS = ("000001.SH", "399001.SZ", "399006.SZ", "000680.SH")

    # 档位 → 最小轮询间隔 (秒) — TickFlow 档位限速保护, 仅实时源为 tickflow 时适用
    TIER_MIN_INTERVAL = {
        "expert": 1.0,
        "pro": 3.0,
        "starter": 6.0,
        "free": 6.0,
    }
    # 插件/自定义源: 不受 TickFlow 档位保护约束, 通用下限 1s (默认间隔仍为 DEFAULT_INTERVAL)
    CUSTOM_PROVIDER_MIN_INTERVAL = 1.0
    DEFAULT_INTERVAL = 6.0
    MAX_INTERVAL = 60.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 串行化行情拉取: 手动 POST /refresh 与后台轮询线程可能并发调用
        # _fetch_quotes, 两者同时写同一批 parquet/缓存会互相覆盖
        self._fetch_lock = threading.Lock()
        self._running = False
        self._enabled = False      # 全局开关 (持久化到 preferences)
        # 暂停态: 盘后管道/数据修正运行期间临时暂停取数, 防止与管道写同一批 parquet 竞态。
        # 与 _enabled 不同 — pause 不改 preferences、不 stop 线程, 仅让轮询循环跳过取数;
        # 进程重启后 _paused 归零, 从 preferences 恢复真实开关态, 无"假关闭"副作用。
        self._paused = False
        self._interval = self.DEFAULT_INTERVAL
        self._thread: threading.Thread | None = None
        self._temporary_consumers = 0
        self._temporary_original: tuple[bool, bool, float] | None = None
        self._symbol_consumers: dict[str, set[str]] = {}
        self._symbol_consumer_revision = 0
        self._latest_quotes: dict[str, dict] = {}
        self._fetch_listeners: set[Callable[[], None]] = set()
        self._alert_listeners: set[Callable[[list[dict]], None]] = set()
        self._repo = None          # 延迟注入, 避免循环导入
        # SSE 订阅者集合: 每个 /stream 连接一个 QuoteSubscriber, 事件广播到所有订阅者
        self._subscribers: set[QuoteSubscriber] = set()
        self._strategy_monitor = None            # 延迟注入
        self._app_state = None                   # 延迟注入 (FastAPI app.state)
        # 异动边缘规则上次评估时间戳 (秒)。异动快照历史部分有 60s 缓存,
        # 但每次构建仍有全市场循环, 轮询线程里限频到 30s 一次。
        self._abnormal_last_eval = 0.0

        # 拉取元信息 (给 SSE / status 用)
        self._fetch_time: float = 0.0       # perf_counter (用于计算 quote_age_ms)
        self._fetch_ms: float = 0.0         # 拉取耗时 (毫秒)
        # _fetched_at 持久化到 preferences: 进程重启后仍能显示"最后获取"时间,
        # 不因关闭开关/重启而归零 (数据页卡片常驻显示, 方便判断上次拉取时刻)。
        try:
            from app.services import preferences as _prefs
            self._fetched_at: float = float(_prefs.load().get("last_fetch_ms", 0.0))
        except Exception:  # noqa: BLE001
            self._fetched_at = 0.0      # 拉取完成的 Unix 时间戳 (毫秒)
        self._symbol_count: int = 0
        self._index_symbol_count: int = 0
        self._etf_symbol_count: int = 0
        self._index_quotes_cache: pl.DataFrame | None = None
        self._intraday_signal_evaluator = IntradaySignalEvaluator()
        # Shared intraday input for the monitor center and position risk.  WS
        # quotes are accumulated here as closed one-minute bars; the minute
        # provider seeds the day and fills gaps, so neither consumer rebuilds
        # a private window from its own queue.
        self._intraday_rows: dict[tuple[str, str, datetime], dict] = {}
        self._intraday_last_quote: dict[tuple[str, str], tuple[datetime, float, float]] = {}
        self._intraday_gap_anchors: set[tuple[str, str]] = set()
        self._intraday_buckets: dict[tuple[str, str], dict] = {}
        self._intraday_seeded: set[tuple[str, str, date]] = set()
        self._intraday_fetch_bucket: dict[tuple[str, date], str] = {}
        self._intraday_fetch_symbols: dict[tuple[str, date], set[str]] = {}
        self._intraday_ws_seen: set[tuple[str, str, date]] = set()
        self._intraday_consumers: dict[str, tuple[str, set[str]]] = {}
        self._intraday_prev_close: dict[str, dict[str, float]] = {}
        self._intraday_signal_cache: dict[str, tuple[tuple, dict[str, dict], set[str]]] = {}
        self._opening_volume_baseline_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
        # 午休/收盘最终同步状态: 到边界后必须成功拉取一版行情, 再进入休盘态。
        self._final_sync_done: set[tuple[date, str]] = set()
        self._final_sync_failed: dict[tuple[date, str], str] = {}
        self._holiday_active = False  # 交易日探针当前是否判休市 (日志去重)
        # 轮询放量 (volume_delta 规则): 上一轮全市场股票快照的 (累计成交量[手], 累计成交额[元])。
        # 每轮全量快照后更新 (含非连续竞价时段, 保证 13:00 恢复时 prev 是 12:59
        # 而非 11:30); 跨交易日清空; cur < prev (数据源重置) 时丢弃该轮差值。
        self._prev_stock_volume: dict[str, tuple[float, float]] | None = None
        self._prev_volume_fetched_at: float | None = None   # epoch 毫秒
        self._prev_volume_date: date | None = None
        # 最近一轮的有效差值 (vol_delta[手], amt_delta[元]) - 仅连续竞价时段内、
        # prev 不早于本时段开盘时计算
        self._volume_delta: dict[str, tuple[float, float]] = {}
        self._volume_delta_span_s: float = 0.0

    # ================================================================
    # 生命周期
    # ================================================================

    def start(self, interval: float = 0.0) -> None:
        """启动后台行情轮询线程。"""
        if self._running:
            return
        if interval <= 0:
            from app.services import preferences
            interval = preferences.get_realtime_quote_interval()
        self._interval = self._clamp_interval(interval)
        self._running = True
        self._enabled = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        self._save_enabled(True)
        logger.info("行情服务已启动, 轮询间隔 %.1fs", self._interval)

    def stop(self) -> None:
        """停止后台行情轮询线程。"""
        self.shutdown()
        self._save_enabled(False)
        logger.info("行情服务已停止")

    def shutdown(self) -> None:
        """仅停止运行时线程，保留用户的实时行情开关偏好。"""
        self._running = False
        self._enabled = False
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        logger.info("行情服务运行时已关闭")

    def enable(self) -> bool:
        """开启自动行情 (不立即启动线程，等下一个交易时段)。

        none 档无实时行情权限,拒绝开启并返回 False;
        free 档开启自选股实时,starter+ 开启全市场实时。返回值表示是否真正开启。
        """
        if not self.is_realtime_allowed():
            logger.warning("实时行情开启被拒:当前档位(none)无实时行情权限")
            return False
        self._enabled = True
        self._save_enabled(True)
        if not self._running:
            from app.services import preferences
            self._interval = self._clamp_interval(preferences.get_realtime_quote_interval())
            self._running = True
            self._thread = threading.Thread(target=self._poll_loop, daemon=True)
            self._thread.start()
        logger.info("行情服务已启用, 轮询间隔 %.1fs", self._interval)
        return True

    def disable(self) -> None:
        """关闭自动行情。"""
        self.stop()
        logger.info("行情服务已关闭")

    # ================================================================
    # 临时暂停 (盘后管道/数据修正期间, 防止写盘竞态)
    # ================================================================

    def pause(self) -> None:
        """临时暂停行情轮询取数 (不关闭线程、不改 preferences)。

        用于盘后管道/数据修正运行期间, 防止实时行情覆写管道正在写的 parquet。
        与 stop() 的区别: 线程继续存活但跳过 _fetch_quotes; preferences 开关态不变,
        管道结束调用 resume() 即恢复。线程级检查, 即时生效, 无 join 等待。
        """
        self._paused = True
        logger.info("行情轮询已临时暂停 (管道/修正运行中)")

    def resume(self) -> None:
        """恢复暂停的行情轮询取数 (对应 pause)。"""
        self._paused = False
        logger.info("行情轮询已恢复")

    def is_paused(self) -> bool:
        """是否处于临时暂停态 (管道运行期间)。"""
        return self._paused

    @contextmanager
    def paused(self):
        """上下文管理器: 进入时暂停轮询取数, 退出时(含异常)自动恢复。

        供盘后管道/数据修正复用:
            with quote_service.paused():
                run_pipeline(...)
        无论正常结束还是异常/crash, finally 都会 resume (除非进程直接被 kill)。
        """
        self.pause()
        try:
            yield
        finally:
            self.resume()

    def boot_check(self) -> None:
        """启动时检查 preferences，若 enabled 则自动启动。

        none 档无实时行情权限:即使 preferences 标记为 enabled,
        也不启动,并同步 preferences 为关闭(避免 UI 误显示已开启)。
        """
        from app.services import preferences
        if not self.is_realtime_allowed():
            if preferences.get_realtime_quotes_enabled():
                self._save_enabled(False)
            logger.info("实时行情未启动:当前档位(none)无实时行情权限")
            return
        if preferences.get_realtime_quotes_enabled():
            self.start()

    def set_repo(self, repo) -> None:
        """注入 KlineRepository, 用于实时落盘。"""
        self._repo = repo
        with self._lock:
            self._opening_volume_baseline_cache.clear()

    def set_app_state(self, app_state) -> None:
        """注入 FastAPI app.state, 用于获取 strategy_monitor 等单例。"""
        self._app_state = app_state

    def set_interval(self, interval: float) -> float:
        """运行时更新轮询间隔（立即生效）。"""
        clamped = self._clamp_interval(interval)
        self._interval = clamped
        from app.services import preferences
        preferences.set_realtime_quote_interval(clamped)
        logger.info("轮询间隔已更新为 %.1fs", clamped)
        return clamped

    def get_min_interval(self) -> float:
        """返回当前档位允许的最小间隔。"""
        return self._tier_min_interval()

    def acquire_temporary_polling(self, interval: float) -> None:
        """临时启用/加速轮询，不写入用户偏好。"""
        requested = float(interval)
        minimum = self.get_min_interval()
        if requested < minimum:
            raise ValueError(f"当前套餐最小行情间隔为 {minimum:g} 秒")
        with self._lock:
            if self._temporary_consumers == 0:
                self._temporary_original = (self._running, self._enabled, self._interval)
            self._temporary_consumers += 1
            self._interval = min(self._interval, requested)
            self._enabled = True
            if not self._running:
                self._running = True
                self._thread = threading.Thread(target=self._poll_loop, daemon=True)
                self._thread.start()

    def release_temporary_polling(self) -> None:
        """释放临时行情租约，并恢复首个租约前的运行态。"""
        thread = None
        with self._lock:
            if self._temporary_consumers <= 0:
                return
            self._temporary_consumers -= 1
            if self._temporary_consumers:
                return
            original = self._temporary_original
            self._temporary_original = None
            if original is None:
                return
            was_running, was_enabled, original_interval = original
            self._interval = original_interval
            self._enabled = was_enabled
            if not was_running:
                self._running = False
                thread = self._thread
                self._thread = None
        if thread and thread is not threading.current_thread():
            thread.join(timeout=10)

    def add_fetch_listener(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._fetch_listeners.add(callback)

    def remove_fetch_listener(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._fetch_listeners.discard(callback)

    def get_latest_quotes(self, symbols: set[str] | None = None) -> list[dict]:
        """返回行情轮询已经缓存的快照，不触发任何数据源请求。"""
        requested = {str(symbol).strip().upper() for symbol in symbols or set() if str(symbol).strip()}
        with self._lock:
            values = list(self._latest_quotes.values())
        rows = []
        for quote in values:
            symbol = str(quote.get("symbol") or "").strip().upper()
            if not symbol or requested and symbol not in requested:
                continue
            rows.append({key: value for key, value in quote.items() if not key.startswith("_")})
        return rows

    def notify_large_orders_updated(self) -> None:
        """大单后台聚合完成后触发独立 SSE 事件。"""
        for sub in self._snapshot_subscribers():
            sub.notify_large_orders()

    def notify_position_risk_updated(self) -> None:
        for sub in self._snapshot_subscribers():
            sub.notify_position_risk()

    def notify_limit_board_updated(self) -> None:
        for sub in self._snapshot_subscribers():
            sub.notify_limit_board()

    def add_alert_listener(self, callback: Callable[[list[dict]], None]) -> None:
        with self._lock:
            self._alert_listeners.add(callback)

    def remove_alert_listener(self, callback: Callable[[list[dict]], None]) -> None:
        with self._lock:
            self._alert_listeners.discard(callback)

    def set_symbol_consumer(self, consumer_id: str, symbols: set[str]) -> None:
        """注册需要随现有轮询补拉的少量标的。"""
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        with self._lock:
            previous_symbols = set().union(*self._symbol_consumers.values()) if self._symbol_consumers else set()
            if cleaned:
                self._symbol_consumers[consumer_id] = cleaned
            else:
                self._symbol_consumers.pop(consumer_id, None)
            current_symbols = set().union(*self._symbol_consumers.values()) if self._symbol_consumers else set()
            if current_symbols - previous_symbols:
                self._symbol_consumer_revision += 1
                final_key = self._final_sync_key(self._market_phase())
                if final_key:
                    self._final_sync_done.discard(final_key)

    def remove_symbol_consumer(self, consumer_id: str) -> None:
        with self._lock:
            self._symbol_consumers.pop(consumer_id, None)

    def get_fresh_quotes(self, symbols: set[str]) -> dict:
        """返回当前交易日的最新报价快照，不触发网络请求。"""
        requested = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        with self._lock:
            cached = {symbol: dict(self._latest_quotes[symbol]) for symbol in requested if symbol in self._latest_quotes}
            interval = self._interval
        today = cn_today()
        active_polling = self._should_poll_for_phase(self._market_phase())
        max_age_s = max(interval * 2, 30.0)
        now_mono = time.monotonic()
        quotes: dict[str, dict] = {}
        for symbol, quote in cached.items():
            if quote.get("_quote_date") != today:
                continue
            if active_polling and now_mono - float(quote.get("_received_at") or 0) > max_age_s:
                continue
            quotes[symbol] = {key: value for key, value in quote.items() if not key.startswith("_")}
        missing = sorted(requested - quotes.keys())
        timestamps = [str(quote.get("timestamp")) for quote in quotes.values() if quote.get("timestamp")]
        return {
            "live": not missing,
            "quotes": quotes,
            "missing_symbols": missing,
            "as_of": max(timestamps) if timestamps else None,
            "date": today.isoformat(),
        }

    def _consumer_symbols(self) -> set[str]:
        with self._lock:
            return set().union(*self._symbol_consumers.values()) if self._symbol_consumers else set()

    @staticmethod
    def _quote_datetime(value) -> datetime | None:
        if isinstance(value, datetime):
            return value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=CN_TZ)
        if isinstance(value, (int, float)):
            seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=CN_TZ)
        if value:
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                return parsed.astimezone(CN_TZ) if parsed.tzinfo else parsed.replace(tzinfo=CN_TZ)
            except ValueError:
                return None
        return None

    def _cache_latest_quotes(self, records: list[dict]) -> None:
        received_at = time.monotonic()
        fallback_time = cn_now()
        updates: dict[str, dict] = {}
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            price = raw.get("last_price", raw.get("close"))
            if not symbol or price is None:
                continue
            quote_time = self._quote_datetime(raw.get("timestamp")) or fallback_time
            updates[symbol] = {
                **raw,
                "symbol": symbol,
                "last_price": float(price),
                "timestamp": quote_time.isoformat(),
                "_quote_date": quote_time.date(),
                "_received_at": received_at,
            }
        if updates:
            with self._lock:
                self._latest_quotes.update(updates)

    def record_quotes(self, records: list[dict]) -> None:
        """把其他共享行情通道收到的报价并入只读快照。"""
        self._cache_latest_quotes(records)
        self._record_intraday_quotes(records)

    def mark_intraday_gap(self, symbols: set[str] | None = None, asset_type: str | None = None) -> None:
        """让断线后的第一条累计行情只做基线，不把缺口成交量并入当前分钟。"""
        with self._lock:
            selected = {str(symbol).strip().upper() for symbol in (symbols or set()) if str(symbol).strip()}
            if not selected:
                selected = {symbol for _, symbol in self._intraday_last_quote}
            kinds = {asset_type} if asset_type else {kind for kind, _ in self._intraday_last_quote}
            self._intraday_gap_anchors.update((kind, symbol) for kind in kinds for symbol in selected)
            self._intraday_signal_evaluator.mark_gap(selected, asset_type)

    def set_intraday_consumer(
        self, consumer_id: str, symbols: set[str], asset_type: str = "stock",
    ) -> None:
        """注册共享分时输入消费者；WS 和公共监控使用同一份分钟快照。"""
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        with self._lock:
            if cleaned:
                self._intraday_consumers[consumer_id] = (asset_type, cleaned)
            else:
                self._intraday_consumers.pop(consumer_id, None)
        if consumer_id.startswith("monitor:"):
            hub = getattr(getattr(self._app_state, "paper_supervisor", None), "hub", None)
            setter = getattr(hub, "set_intraday_consumer", None)
            capset = getattr(self._app_state, "capabilities", None)
            try:
                from app.tickflow.capabilities import Cap
                websocket_allowed = bool(capset and capset.has(Cap.WEBSOCKET))
            except Exception:  # noqa: BLE001
                websocket_allowed = False
            if callable(setter) and websocket_allowed:
                try:
                    setter(consumer_id, cleaned, asset_type)
                except (ValueError, RuntimeError) as exc:
                    logger.info("分时 WS 容量不足，%s 降级到分钟接口: %s", consumer_id, exc)

    def remove_intraday_consumer(self, consumer_id: str) -> None:
        with self._lock:
            self._intraday_consumers.pop(consumer_id, None)
        if consumer_id.startswith("monitor:"):
            hub = getattr(getattr(self._app_state, "paper_supervisor", None), "hub", None)
            remover = getattr(hub, "remove_intraday_consumer", None)
            if callable(remover):
                try:
                    remover(consumer_id)
                except (ValueError, RuntimeError):
                    logger.debug("移除分时 WS 消费者失败", exc_info=True)

    @staticmethod
    def _intraday_datetime(value: object) -> datetime | None:
        parsed = QuoteService._quote_datetime(value)
        return parsed.replace(tzinfo=None) if parsed else None

    def _record_intraday_quotes(self, records: list[dict]) -> None:
        """把实时 WS/轮询快照按累计成交量聚合为闭合 1 分钟 K。"""
        with self._lock:
            consumers = {
                (asset_type, symbol)
                for asset_type, symbols in self._intraday_consumers.values()
                for symbol in symbols
            }
        if not consumers:
            return
        for raw in records:
            symbol = str(raw.get("symbol") or "").strip().upper()
            matching_types = {kind for kind, candidate in consumers if candidate == symbol}
            if not matching_types:
                continue
            asset_type = next(iter(matching_types))
            if (asset_type, symbol) not in consumers:
                continue
            point = self._intraday_datetime(raw.get("timestamp"))
            price = _finite(raw.get("last_price", raw.get("close")))
            if point is None or price is None or price <= 0:
                continue
            if not (
                dt_time(9, 30) <= point.time() < dt_time(11, 30)
                or dt_time(13, 0) <= point.time() < dt_time(15, 0)
            ):
                continue
            current_volume = _finite(raw.get("volume")) or 0.0
            current_amount = _finite(raw.get("amount")) or 0.0
            key = (asset_type, symbol)
            with self._lock:
                self._intraday_ws_seen.add((asset_type, symbol, point.date()))
                gap_anchor = key in self._intraday_gap_anchors
                if gap_anchor:
                    self._intraday_gap_anchors.discard(key)
                previous = None if gap_anchor else self._intraday_last_quote.get(key)
                if previous is not None and point <= previous[0]:
                    continue
                volume_delta = max(0.0, current_volume - previous[1]) if previous else 0.0
                amount_delta = max(0.0, current_amount - previous[2]) if previous else 0.0
                self._intraday_last_quote[key] = (point, current_volume, current_amount)
                bucket_time = point.replace(second=0, microsecond=0)
                bucket = self._intraday_buckets.get(key)
                if bucket is not None and bucket["datetime"] != bucket_time:
                    row = dict(bucket)
                    self._intraday_rows[(asset_type, symbol, bucket["datetime"])] = row
                    bucket = None
                if bucket is None:
                    bucket = {
                        "symbol": symbol, "datetime": bucket_time,
                        "open": price, "high": price, "low": price, "close": price,
                        "volume": volume_delta, "amount": amount_delta,
                    }
                    self._intraday_buckets[key] = bucket
                else:
                    bucket["high"] = max(bucket["high"], price)
                    bucket["low"] = min(bucket["low"], price)
                    bucket["close"] = price
                    bucket["volume"] += volume_delta
                    bucket["amount"] += amount_delta

    def _seed_intraday_rows(self, asset_type: str, symbols: set[str], now: datetime) -> None:
        """用本地分钟分区和实时分钟能力补齐当天历史，WS 行保持权威。"""
        if not symbols:
            return
        trade_date = now.date()
        key = (asset_type, trade_date)
        bucket = now.strftime("%Y%m%d%H%M")
        with self._lock:
            if self._intraday_fetch_bucket.get(key) != bucket:
                self._intraday_fetch_bucket[key] = bucket
                self._intraday_fetch_symbols[key] = {
                    symbol for symbol in symbols
                    if (asset_type, symbol, trade_date) in self._intraday_seeded
                    and (asset_type, symbol, trade_date) in self._intraday_ws_seen
                }
            already_fetched = self._intraday_fetch_symbols.setdefault(key, set())
            seeded = set(symbols) - already_fetched
            if not seeded:
                return
        from app.services.kline_sync import fetch_intraday_monitor_batch, intraday_monitor_support
        from app.services.minute_quality import normalize_minute_clock

        frames: list[pl.DataFrame] = []
        local_getter = getattr(self._repo, "get_minute_range", None)
        if callable(local_getter):
            try:
                local = local_getter(
                    sorted(seeded), trade_date, trade_date, asset_type,
                )
                if local is not None and not local.is_empty():
                    local, _basis, _shifted = normalize_minute_clock(local)
                    frames.append(local)
            except Exception:  # noqa: BLE001
                logger.debug("共享分时读取本地分钟数据失败", exc_info=True)

        capset = getattr(self._app_state, "capabilities", None)
        support = intraday_monitor_support(capset)
        if support["available"]:
            max_symbols = max(1, int(support["max_symbols"]))
            ordered = sorted(seeded)
            for offset in range(0, len(ordered), max_symbols):
                frame = fetch_intraday_monitor_batch(
                    ordered[offset:offset + max_symbols], capset, now=now,
                )
                if not frame.is_empty():
                    frames.append(frame)
        with self._lock:
            self._intraday_fetch_symbols.setdefault(key, set()).update(seeded)
        if not frames:
            return
        # 本地帧先加入、实时帧后加入；相同分钟由实时帧覆盖，WS 聚合行仍由下方
        # setdefault 保留为最高优先级。
        minute_df = pl.concat(frames, how="diagonal_relaxed").unique(
            subset=["symbol", "datetime"], keep="last",
        )
        cutoff = now.replace(second=0, microsecond=0)
        rows = minute_df.filter(
            pl.col("datetime").dt.date() == trade_date,
            pl.col("datetime") < cutoff,
            pl.col("volume").fill_null(0) >= 0,
        )
        with self._lock:
            for row in rows.to_dicts():
                symbol = str(row.get("symbol") or "").strip().upper()
                if not symbol or (asset_type, symbol) not in {(asset_type, s) for s in seeded}:
                    continue
                dt = row.get("datetime")
                if isinstance(dt, datetime):
                    self._intraday_rows.setdefault((asset_type, symbol, dt.replace(tzinfo=None)), row)
            self._intraday_seeded.update((asset_type, symbol, trade_date) for symbol in seeded)

    def get_intraday_snapshot(
        self, symbols: set[str], *, asset_type: str = "stock", now: datetime | None = None,
    ) -> dict[str, Any]:
        """返回共享分钟行、完整日内 VWAP 和数据能力状态。"""
        now = now or cn_now()
        if now.tzinfo:
            now = now.astimezone(CN_TZ).replace(tzinfo=None)
        cleaned = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        with self._lock:
            stale_rows = [key for key in self._intraday_rows if key[2].date() < now.date()]
            for key in stale_rows:
                self._intraday_rows.pop(key, None)
            stale_days = [key for key in self._intraday_seeded if key[2] < now.date()]
            self._intraday_seeded.difference_update(stale_days)
        self._seed_intraday_rows(asset_type, cleaned, now)
        cutoff = now.replace(second=0, microsecond=0)
        with self._lock:
            row_map = {
                (symbol, dt): dict(row) for (kind, symbol, dt), row in self._intraday_rows.items()
                if kind == asset_type and symbol in cleaned and dt.date() == now.date() and dt < cutoff
            }
            for symbol in cleaned:
                bucket = self._intraday_buckets.get((asset_type, symbol))
                if bucket and bucket["datetime"].date() == now.date() and bucket["datetime"] < cutoff:
                    key = (symbol, bucket["datetime"])
                    existing = row_map.get(key)
                    if existing is None or float(bucket.get("volume") or 0) > 0 or float(bucket.get("amount") or 0) > 0:
                        row_map[key] = dict(bucket)
            rows = list(row_map.values())
        rows.sort(key=lambda row: (str(row.get("symbol")), row.get("datetime")))
        vwap: dict[str, float] = {}
        for symbol in cleaned:
            points = [row for row in rows if row.get("symbol") == symbol]
            volume = sum(float(row.get("volume") or 0) for row in points)
            amount = sum(float(row.get("amount") or 0) for row in points)
            if volume > 0 and amount > 0:
                vwap[symbol] = amount / (volume * 100.0)
        return {
            "rows": rows,
            "vwap": vwap,
            "as_of": max((row["datetime"] for row in rows), default=None),
            "source": "websocket_aggregate+minute_seed",
            "available": bool(rows),
        }

    @staticmethod
    def _ema(values: list[float], period: int) -> float | None:
        if not values:
            return None
        alpha = 2.0 / (period + 1.0)
        result = values[0]
        for value in values[1:]:
            result = alpha * value + (1.0 - alpha) * result
        return result

    @staticmethod
    def _atr(bars: list[dict[str, float]], period: int) -> float | None:
        if not bars:
            return None
        true_ranges: list[float] = []
        previous_close: float | None = None
        for bar in bars:
            high = _finite(bar.get("high"))
            low = _finite(bar.get("low"))
            close = _finite(bar.get("close"))
            if high is None or low is None or close is None:
                continue
            true_ranges.append(
                max(high - low, abs(high - previous_close), abs(low - previous_close))
                if previous_close is not None else high - low,
            )
            previous_close = close
        if not true_ranges:
            return None
        window = true_ranges[-max(1, int(period)):]
        return sum(window) / len(window)

    def _previous_day_levels(
        self,
        symbols: set[str],
        now: datetime,
        asset_type: str,
    ) -> dict[str, dict[str, float | None]]:
        """读取最近一个可用交易日的高低点，不把自然日当作交易日。"""
        if not symbols or self._repo is None:
            return {}
        getter = getattr(self._repo, "get_daily_asset_batch", None)
        if not callable(getter):
            return {}
        end = now.date() - timedelta(days=1)
        start = end - timedelta(days=14)
        try:
            frame = getter(
                asset_type,
                sorted(symbols),
                start,
                end,
                columns=["symbol", "date", "raw_high", "raw_low", "high", "low"],
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            logger.warning("前一交易日高低点读取失败", exc_info=True)
            return {}
        if frame is None or frame.is_empty():
            return {}
        try:
            rows = frame.sort(["symbol", "date"]).to_dicts()
        except (AttributeError, pl.exceptions.PolarsError):
            return {}
        levels: dict[str, dict[str, float | None]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            high = _finite(row.get("raw_high"))
            low = _finite(row.get("raw_low"))
            if high is None:
                high = _finite(row.get("high"))
            if low is None:
                low = _finite(row.get("low"))
            levels[symbol] = {"high": high, "low": low}
        return levels

    def get_intraday_features(
        self,
        symbols: set[str],
        *,
        asset_type: str = "stock",
        now: datetime | None = None,
        freshness_seconds: int = 180,
    ) -> dict[str, dict[str, Any]]:
        """返回共享的闭合 1/5 分钟特征；缺少新鲜分钟数据时显式 fail-closed。"""
        now = now or cn_now()
        if now.tzinfo:
            now = now.astimezone(CN_TZ).replace(tzinfo=None)
        snapshot = self.get_intraday_snapshot(symbols, asset_type=asset_type, now=now)
        rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for row in snapshot.get("rows") or []:
            symbol = str(row.get("symbol") or "").strip().upper()
            dt = row.get("datetime")
            if symbol and isinstance(dt, datetime):
                rows_by_symbol.setdefault(symbol, []).append(row)
        cutoff = now.replace(second=0, microsecond=0)
        selected_symbols = {
            str(value).strip().upper() for value in symbols if str(value).strip()
        }
        previous_levels = self._previous_day_levels(selected_symbols, now, asset_type)
        result: dict[str, dict[str, Any]] = {}
        for symbol in selected_symbols:
            rows = sorted(rows_by_symbol.get(symbol, []), key=lambda row: row["datetime"])
            latest_dt = rows[-1]["datetime"] if rows else None
            closed_at = latest_dt + timedelta(minutes=1) if latest_dt else None
            age_seconds = max(0.0, (now.replace(tzinfo=None) - closed_at).total_seconds()) if closed_at else None
            fresh = bool(
                rows and latest_dt < cutoff and closed_at is not None
                and age_seconds is not None and age_seconds <= freshness_seconds
            )
            bars_1m = [
                {
                    "datetime": row["datetime"],
                    "open": float(row.get("open") or row.get("close") or 0),
                    "high": float(row.get("high") or row.get("close") or 0),
                    "low": float(row.get("low") or row.get("close") or 0),
                    "close": float(row.get("close") or 0),
                    "volume": float(row.get("volume") or 0),
                    "amount": float(row.get("amount") or 0),
                }
                for row in rows
                if _finite(row.get("close")) is not None
            ]
            grouped: dict[datetime, list[dict[str, Any]]] = {}
            for bar in bars_1m:
                dt = bar["datetime"]
                bucket = dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
                if bucket + timedelta(minutes=5) <= cutoff:
                    grouped.setdefault(bucket, []).append(bar)
            bars_5m: list[dict[str, Any]] = []
            obv = 0.0
            previous_close_5m: float | None = None
            for bucket, values in sorted(grouped.items()):
                close = values[-1]["close"]
                if previous_close_5m is not None:
                    if close > previous_close_5m:
                        obv += sum(item["volume"] for item in values)
                    elif close < previous_close_5m:
                        obv -= sum(item["volume"] for item in values)
                previous_close_5m = close
                bars_5m.append({
                    "datetime": bucket,
                    "open": values[0]["open"],
                    "high": max(item["high"] for item in values),
                    "low": min(item["low"] for item in values),
                    "close": values[-1]["close"],
                    "volume": sum(item["volume"] for item in values),
                    "amount": sum(item["amount"] for item in values),
                    "source_bars": len(values),
                    "closed": len(values) == 5,
                    "obv": obv,
                })
            closes_1m = [bar["close"] for bar in bars_1m if bar["close"] > 0]
            complete_bars_5m = [bar for bar in bars_5m if bar["closed"]]
            closes_5m = [bar["close"] for bar in complete_bars_5m if bar["close"] > 0]
            opening = [
                bar for bar in bars_1m
                if dt_time(9, 31) <= bar["datetime"].time() < dt_time(10, 0)
            ]
            auction_bar = next(
                (bar for bar in bars_1m if bar["datetime"].time() == dt_time(9, 30)),
                None,
            )
            opening_five_minute_bars = [
                bar for bar in bars_1m
                if dt_time(9, 31) <= bar["datetime"].time() <= dt_time(9, 35)
            ]
            opening_five_minute_complete = (
                len(opening_five_minute_bars) == 5
                and {bar["datetime"].time() for bar in opening_five_minute_bars}
                == {dt_time(9, minute) for minute in range(31, 36)}
            )
            opening_volume_valid = opening_five_minute_complete and all(
                _finite(bar.get("volume")) is not None and float(bar["volume"]) >= 0
                for bar in opening_five_minute_bars
            )
            previous_volumes = [bar["volume"] for bar in bars_1m[-21:-1] if bar["volume"] > 0]
            latest_volume = bars_1m[-1]["volume"] if bars_1m else 0.0
            average_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else None
            latest_close = closes_1m[-1] if closes_1m else None
            momentum_1m = closes_1m[-1] / closes_1m[-2] - 1 if len(closes_1m) >= 2 and closes_1m[-2] else None
            momentum_5m = closes_5m[-1] / closes_5m[-2] - 1 if len(closes_5m) >= 2 and closes_5m[-2] else None
            vwap = _finite((snapshot.get("vwap") or {}).get(symbol))
            reason = "" if fresh else ("分钟数据为空" if not rows else "分钟数据过期")
            opening_baseline = self._opening_volume_baseline(
                symbol, now.date(), asset_type,
            )
            result[symbol] = {
                "symbol": symbol,
                "available": bool(fresh),
                "fresh": bool(fresh),
                "reason": reason,
                "source": snapshot.get("source"),
                "as_of": latest_dt.isoformat() if latest_dt else None,
                "age_seconds": age_seconds,
                "bars_1m": len(bars_1m),
                "bars_5m": len(complete_bars_5m),
                "last_price": latest_close,
                "session_vwap": vwap,
                "opening_range_high": max((bar["high"] for bar in opening), default=None),
                "opening_range_low": min((bar["low"] for bar in opening), default=None),
                "ema9_1m": self._ema(closes_1m, 9),
                "ema20_1m": self._ema(closes_1m, 20),
                "ema9_5m": self._ema(closes_5m, 9),
                "ema20_5m": self._ema(closes_5m, 20),
                "atr14_1m": self._atr(bars_1m, 14),
                "atr14_5m": self._atr(complete_bars_5m, 14),
                "five_minute_high": max((bar["high"] for bar in complete_bars_5m[-3:]), default=None),
                "five_minute_low": min((bar["low"] for bar in complete_bars_5m[-3:]), default=None),
                "momentum_1m": momentum_1m,
                "momentum_5m": momentum_5m,
                "relative_volume": latest_volume / average_volume if average_volume else None,
                "auction": {
                    "available": bool(
                        auction_bar
                        and auction_bar["close"] > 0
                        and (auction_bar["volume"] > 0 or auction_bar["amount"] > 0)
                    ),
                    "as_of": auction_bar["datetime"].isoformat() if auction_bar else None,
                    "price": auction_bar["close"] if auction_bar else None,
                    "volume": auction_bar["volume"] if auction_bar else None,
                    "amount": auction_bar["amount"] if auction_bar else None,
                },
                "opening_five_minute": {
                    "available": opening_five_minute_complete,
                    "volume_valid": opening_volume_valid,
                    "as_of": opening_five_minute_bars[-1]["datetime"].isoformat()
                    if opening_five_minute_bars else None,
                    "open": opening_five_minute_bars[0]["open"] if opening_five_minute_bars else None,
                    "close": opening_five_minute_bars[-1]["close"] if opening_five_minute_bars else None,
                    "high": max((bar["high"] for bar in opening_five_minute_bars), default=None),
                    "low": min((bar["low"] for bar in opening_five_minute_bars), default=None),
                    "volume": sum(bar["volume"] for bar in opening_five_minute_bars),
                    "amount": sum(bar["amount"] for bar in opening_five_minute_bars),
                    **opening_baseline,
                },
                "previous_day_high": (previous_levels.get(symbol) or {}).get("high"),
                "previous_day_low": (previous_levels.get(symbol) or {}).get("low"),
                "session_bars": bars_1m,
                "closed_bars": bars_1m[-20:],
                # Dynamic exits need 24 bars for the double-peak scan.  Expose
                # only complete buckets so a delayed one-minute bar cannot be
                # mistaken for a closed five-minute confirmation.
                "closed_bars_5m": complete_bars_5m[-60:],
                "latest_closed_5m_token": complete_bars_5m[-1]["datetime"].isoformat() if complete_bars_5m else None,
            }
        return result

    def _opening_volume_baseline(
        self,
        symbol: str,
        trading_date: date,
        asset_type: str,
        *,
        sessions: int = 20,
    ) -> dict[str, Any]:
        """读取过去完整交易日的 09:31-09:35 成交量中位数，按资产类型缓存。"""
        key = (asset_type, symbol, trading_date.isoformat())
        with self._lock:
            cached = self._opening_volume_baseline_cache.get(key)
        if cached is not None:
            return dict(cached)
        unavailable = {"baseline_median_volume": None, "baseline_samples": 0, "baseline_available": False}
        if self._repo is None:
            return unavailable
        getter = getattr(self._repo, "get_minute_range", None)
        if not callable(getter):
            return unavailable
        try:
            frame = getter(
                [symbol],
                trading_date - timedelta(days=90),
                trading_date - timedelta(days=1),
                asset_type,
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            logger.debug("早盘量能基准读取失败", exc_info=True)
            return unavailable
        if frame is None or frame.is_empty() or "datetime" not in frame.columns:
            return unavailable
        samples: dict[str, dict[str, float]] = {}
        invalid_days: set[str] = set()
        expected_times = {dt_time(9, minute) for minute in range(31, 36)}
        for row in frame.to_dicts():
            raw_dt = row.get("datetime") or row.get("date")
            if not isinstance(raw_dt, datetime):
                try:
                    raw_dt = datetime.fromisoformat(str(raw_dt).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    continue
            # 仓库分钟契约是北京时间墙钟；仅显式标记的旧 UTC 仓库需要平移。
            if raw_dt.tzinfo:
                local_dt = raw_dt.astimezone(CN_TZ).replace(tzinfo=None)
            elif getattr(self._repo, "intraday_datetime_basis", "beijing_naive") == "utc_naive":
                local_dt = raw_dt + timedelta(hours=8)
            else:
                local_dt = raw_dt
            minute = local_dt.time().replace(second=0, microsecond=0)
            if minute not in expected_times:
                continue
            volume = _finite(row.get("volume"))
            day = local_dt.date().isoformat()
            if local_dt.time().second != 0 or local_dt.time().microsecond != 0:
                invalid_days.add(day)
                continue
            if volume is None or volume < 0:
                invalid_days.add(day)
                continue
            day_values = samples.setdefault(day, {})
            if minute.strftime("%H:%M") in day_values:
                invalid_days.add(day)
                continue
            day_values[minute.strftime("%H:%M")] = volume
        complete = [
            sum(values.values())
            for day, values in sorted(samples.items())
            if day not in invalid_days and set(values) == {value.strftime("%H:%M") for value in expected_times}
        ][-sessions:]
        if len(complete) < sessions:
            result = unavailable
        else:
            middle = len(complete) // 2
            median = complete[middle] if len(complete) % 2 else (complete[middle - 1] + complete[middle]) / 2
            result = {
                "baseline_median_volume": median,
                "baseline_samples": len(complete),
                "baseline_available": True,
            }
        with self._lock:
            self._opening_volume_baseline_cache[key] = dict(result)
        return result

    def get_intraday_signals(
        self,
        symbols: set[str],
        *,
        prev_close: dict[str, float],
        asset_type: str = "stock",
        now: datetime | None = None,
        consumer_id: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        """用共享分钟快照计算一次闭合分钟边沿事件，供所有消费者读取。"""
        now = now or cn_now()
        with self._lock:
            active_symbols = set(symbols)
            active_symbols.update(
                candidate
                for kind, candidates in self._intraday_consumers.values()
                if kind == asset_type
                for candidate in candidates
            )
            self._intraday_prev_close.setdefault(asset_type, {}).update(prev_close)
            prev_close_all = dict(self._intraday_prev_close[asset_type])
        snapshot = self.get_intraday_snapshot(active_symbols, asset_type=asset_type, now=now)
        rows = snapshot["rows"]
        if not rows:
            return {}
        latest = max(row["datetime"] for row in rows)
        cache_key = (
            now.date().isoformat(), now.strftime("%Y%m%d%H%M"), latest,
            tuple(sorted(active_symbols)),
        )
        with self._lock:
            cached = self._intraday_signal_cache.get(asset_type)
            if cached and cached[0] == cache_key:
                delivered = cached[2]
                if consumer_id and consumer_id in delivered:
                    return {}
                if consumer_id:
                    delivered.add(consumer_id)
                return {
                    symbol: dict(value) for symbol, value in cached[1].items()
                    if symbol in symbols
                }
        frame = pl.DataFrame(rows)
        signals = self._intraday_signal_evaluator.evaluate(
            frame,
            symbols=active_symbols,
            prev_close=prev_close_all,
            asset_type=asset_type,
            now=now,
        )
        by_symbol = {str(item["symbol"]): dict(item) for item in signals}
        with self._lock:
            delivered = {consumer_id} if consumer_id else set()
            self._intraday_signal_cache[asset_type] = (cache_key, by_symbol, delivered)
        return {symbol: dict(value) for symbol, value in by_symbol.items() if symbol in symbols}

    def _notify_fetch_listeners(self) -> None:
        with self._lock:
            listeners = list(self._fetch_listeners)
        for callback in listeners:
            try:
                callback()
            except Exception:  # noqa: BLE001
                logger.exception("行情监听器执行失败")

    # ================================================================
    # SSE 订阅管理 — 每个 /stream 连接一个订阅者, 事件广播
    # ================================================================

    def subscribe(self) -> QuoteSubscriber:
        """注册一个 SSE 订阅者 (连接建立时调用)。"""
        sub = QuoteSubscriber()
        with self._lock:
            self._subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: QuoteSubscriber) -> None:
        """注销订阅者 (连接断开时调用)。"""
        with self._lock:
            self._subscribers.discard(sub)

    def _snapshot_subscribers(self) -> list[QuoteSubscriber]:
        with self._lock:
            return list(self._subscribers)

    def _broadcast_quote_updated(self) -> None:
        # 实时行情刷新后清空总览聚合缓存, 使看板 (overview-market) 在 SSE 触发的
        # 重取中拿到最新指数/聚合值。与 _broadcast 同时进行, 与侧栏 /intraday/indices
        # (无缓存, 直读实时缓存) 行为对齐, 避免看板落后于侧栏。
        # 延迟导入规避 services <-> api 层循环依赖。
        from app.api.overview import invalidate_overview_cache

        invalidate_overview_cache()
        for sub in self._snapshot_subscribers():
            sub.notify_quote()

    def notify_strategy_results_updated(self) -> None:
        """策略监控完成实时结果更新后调用，仅刷新策略页结果缓存。"""
        for sub in self._snapshot_subscribers():
            sub.notify_strategy_results()

    def notify_depth_updated(self) -> None:
        """五档盘口修正完成后调用: 通知 SSE 推送 depth_updated, 触发连板梯队刷新。

        与行情/告警通道独立 — 只刷新连板梯队, 不连带刷新 watchlist 等。
        """
        for sub in self._snapshot_subscribers():
            sub.notify_depth()

    def _broadcast_alerts(self, alerts: list[dict]) -> None:
        with self._lock:
            listeners = list(self._alert_listeners)
        for listener in listeners:
            try:
                listener(alerts)
            except Exception:  # noqa: BLE001
                logger.exception("告警监听器执行失败")
        for sub in self._snapshot_subscribers():
            sub.push_alerts(alerts)

    def push_alerts(self, alerts: list[dict]) -> None:
        """推送当前已启用规则的公共监控事件。"""
        allowed = self._active_monitor_alerts(alerts)
        if allowed:
            self._broadcast_alerts(allowed)

    def _active_monitor_alerts(self, alerts: list[dict]) -> list[dict]:
        from app.services.alert_store import is_monitor_rule_event

        engine = getattr(getattr(self, "_app_state", None), "monitor_engine", None)
        rules = getattr(engine, "rules", {}) if engine is not None else {}
        active_rule_ids = set(rules) if isinstance(rules, dict) else set()
        return [
            event for event in alerts
            if is_monitor_rule_event(event)
            and str(event.get("rule_id") or "") in active_rule_ids
        ]

    def enrich_external_alerts(self, alerts: list[dict]) -> None:
        """为外部服务生成的告警补全监控中心配置的概念和行业。"""
        self._enrich_alerts_ext(alerts)

    @staticmethod
    def _format_alert_notification_body(event: dict) -> str:
        symbol = str(event.get("symbol") or "").strip()
        name = str(event.get("name") or "").strip()
        message = str(event.get("message") or "").strip()
        parts = [symbol] if symbol else []
        if name and name not in message:
            parts.append(name)
        if message:
            parts.append(message)
        elif name:
            parts.append(name)
        if event.get("source") == "limit_board":
            concept = event.get("concept")
            concept_values = concept if isinstance(concept, (list, tuple, set)) else [concept]
            concepts: list[str] = []
            for value in concept_values:
                for item in str(value or "").replace("；", ";").replace(",", ";").replace("，", ";").replace("、", ";").split(";"):
                    item = item.strip()
                    if item and item not in concepts:
                        concepts.append(item)
                    if len(concepts) == 3:
                        break
                if len(concepts) == 3:
                    break
            body = " ".join(parts)
            if concepts:
                return f"{body}\n概念：{'、'.join(concepts)}"
            return body
        return " ".join(parts)

    def publish_external_alerts(self, alerts: list[dict]) -> None:
        """发布外部领域事件的专属通知；公共监控流只接受规则引擎事件。"""
        if not alerts:
            return
        monitor_alerts = self._active_monitor_alerts(alerts)
        if monitor_alerts:
            self.enrich_external_alerts(monitor_alerts)
            self._broadcast_alerts(monitor_alerts)
            self._maybe_send_system_notifications(monitor_alerts)
        try:
            from app.services import preferences, webhook_adapter

            feishu_url = preferences.get_feishu_webhook_url()
            wecom_url = preferences.get_wecom_webhook_url()
            feishu_secret = preferences.get_feishu_webhook_secret()
            for event in alerts:
                if event.get("source") == "limit_board":
                    continue
                title = (
                    "TickFlow · 持仓风控"
                )
                body = self._format_alert_notification_body(event)
                if feishu_url:
                    _WEBHOOK_EXECUTOR.submit(
                        webhook_adapter.send_feishu, feishu_url, title, body, feishu_secret,
                    )
                if wecom_url:
                    _WEBHOOK_EXECUTOR.submit(webhook_adapter.send_wecom, wecom_url, title, body)
        except Exception as exc:  # noqa: BLE001
            logger.warning("持仓风控 Webhook 提交异常: %s", exc)

    def clear_pending_alerts(self) -> None:
        for sub in self._snapshot_subscribers():
            sub.clear_alerts()

    def push_review_event(self, event_json: str) -> None:
        """广播一条复盘进度事件(JSON 字符串), 唤醒所有 SSE generator。

        事件格式与 recap_market_stream 的产出一致(meta/delta/error/done),
        前端 reviewStore 直接消费。背压在订阅者队列内做 (丢弃最旧)。
        """
        for sub in self._snapshot_subscribers():
            sub.push_review(event_json)

    # ================================================================
    # 档位感知间隔限制
    # ================================================================

    @staticmethod
    def _current_tier() -> str:
        """获取当前档位名（小写）。"""
        from app.tickflow.policy import tier_label
        return tier_label().split()[0].split("+")[0].strip().lower()

    @classmethod
    def realtime_mode(cls) -> str:
        """当前实时行情模式: none / full_market。

        TickFlow 免费档不再提供"自选前 5 只"降级实时(自定义源 fuyao 的全市场
        快照已全面覆盖且免费); TickFlow 免费档 = 无实时, 接入自定义实时源
        (如 fuyao)或升级 TickFlow 后恢复全市场模式。
        """
        from app.services import preferences
        if preferences.get_realtime_data_provider() != "tickflow":
            return "full_market"
        tier = cls._current_tier()
        if tier in ("none", "free"):
            return "none"
        return "full_market"

    @staticmethod
    def realtime_provider() -> str:
        """返回共享实时行情当前选择的 provider。"""
        from app.services import preferences
        return preferences.get_realtime_data_provider()

    @classmethod
    def is_realtime_allowed(cls) -> bool:
        """当前档位是否允许使用实时行情。"""
        return cls.realtime_mode() != "none"

    @classmethod
    def _tier_min_interval(cls) -> float:
        # 实时源路由到插件/自定义源时, TickFlow 档位限速不适用 (中立能力原则):
        # 下限放宽到通用 1s, 默认/已保存间隔不变
        from app.services import preferences
        if preferences.get_realtime_data_provider() != "tickflow":
            return cls.CUSTOM_PROVIDER_MIN_INTERVAL
        tier = cls._current_tier()
        return cls.TIER_MIN_INTERVAL.get(tier, cls.DEFAULT_INTERVAL)

    def _clamp_interval(self, interval: float) -> float:
        return max(self._tier_min_interval(), min(self.MAX_INTERVAL, interval))

    # ================================================================
    # 行情数据访问
    # ================================================================

    def get_enriched_today(self) -> tuple[pl.DataFrame, date | None]:
        """返回今天 enriched 数据 + 日期 (线程安全)。

        所有页面统一通过此方法获取实时行情 + 技术指标。
        """
        if not self._repo:
            return pl.DataFrame(), None
        return self._repo.get_enriched_latest()

    def get_quotes_compat(self, asset_type: str = "stock") -> pl.DataFrame:
        """兼容接口: 返回行情 DataFrame (用于盘中选股等需要 last_price/prev_close 的场景)。

        从 _enriched_cache 取 today 的数据, 只选行情基础列, 补上 last_price 别名。
        不返回指标列, 避免 JOIN live_agg 时列名冲突。
        """
        if asset_type == "stock":
            df, _ = self.get_enriched_today()
        elif self._repo:
            df, _ = self._repo.get_enriched_latest_asset(asset_type)
        else:
            df = pl.DataFrame()
        if df.is_empty():
            return df

        # 只取盘中选股需要的行情基础列
        keep = [c for c in [
            "symbol", "close", "open", "high", "low", "volume", "amount",
            "prev_close", "change_pct", "change_amount", "amplitude", "turnover_rate",
        ] if c in df.columns]
        df = df.select(keep)

        # enriched 的 close 等价于 last_price
        if "close" in df.columns and "last_price" not in df.columns:
            df = df.with_columns(pl.col("close").alias("last_price"))
        return df

    def get_index_quotes(self, symbols: list[str] | None = None) -> pl.DataFrame:
        """返回实时指数行情缓存。不会触发 TickFlow 请求。"""
        with self._lock:
            df = self._index_quotes_cache.clone() if self._index_quotes_cache is not None else pl.DataFrame()
        if df.is_empty():
            return df
        if symbols:
            return df.filter(pl.col("symbol").is_in(symbols))
        return df

    def has_fresh_index_quotes(self) -> bool:
        """指数缓存是否来自当前仍有效的一轮实时行情。"""
        with self._lock:
            has_cache = self._index_quotes_cache is not None and not self._index_quotes_cache.is_empty()
            fetch_time = self._fetch_time
        if not has_cache or not fetch_time:
            return False

        phase = self._market_phase()
        if self._should_poll_for_phase(phase):
            age_s = time.perf_counter() - fetch_time
            return age_s <= max(self._interval * 2, 30.0)
        final_key = self._final_sync_key(phase)
        if final_key:
            return final_key in self._final_sync_done
        return False

    def status(self) -> dict:
        """返回行情服务状态。"""
        age = (time.perf_counter() - self._fetch_time) * 1000 if self._fetch_time else -1
        mode = self.realtime_mode()
        phase = self._market_phase()
        final_key = self._final_sync_key(phase)
        final_done = bool(final_key and final_key in self._final_sync_done)
        final_failed = self._final_sync_failed.get(final_key) if final_key else None
        return {
            "enabled": self._enabled,
            "running": self._running,
            "paused": self._paused,
            "mode": mode,
            "realtime_allowed": mode != "none",
            "interval_s": self._interval,
            "fetch_ms": round(self._fetch_ms, 0) if self._fetch_ms else None,
            "symbol_count": self._symbol_count,
            "index_symbol_count": self._index_symbol_count,
            "etf_symbol_count": self._etf_symbol_count,
            "quote_age_ms": round(age, 0) if age >= 0 else None,
            # 交易时段 = 连续竞价; polling_window 另行返回,避免午休/收盘缓冲误显示为交易中。
            "is_trading_hours": self._is_continuous_trading(),
            "is_polling_window": self._should_poll_for_phase(phase),
            "market_phase": phase,
            "final_sync_done": final_done,
            "final_sync_failed": final_failed,
            "last_fetch_ms": round(self._fetched_at, 0) if self._fetched_at else None,
        }

    def refresh(self) -> dict:
        """手动触发一次行情拉取。"""
        self._fetch_quotes()
        return self.status()

    # ================================================================
    # 后台轮询
    # ================================================================

    def _poll_loop(self) -> None:
        while self._running and self._enabled:
            cycle_started = time.monotonic()
            try:
                # 管道/数据修正运行期间临时暂停取数, 防止与管道写同一批 parquet 竞态。
                # 线程继续存活 + 分片 sleep, resume() 后即时恢复, 无需重启线程。
                if not self._paused:
                    phase = self._market_phase()
                    if self._should_fetch_for_phase(phase):
                        is_final = phase in {"morning_final", "close_final"}
                        ok = self._fetch_quotes(final=is_final)
                        if is_final:
                            key = self._final_sync_key(phase)
                            if key and ok:
                                self._final_sync_done.add(key)
                                self._final_sync_failed.pop(key, None)
                                logger.info("%s 最终行情同步完成, 进入休盘态", "午休" if phase == "morning_final" else "收盘")
                            elif key:
                                self._final_sync_failed[key] = "fetch_failed"
                                logger.warning("%s 最终行情同步失败, 将继续重试", "午休" if phase == "morning_final" else "收盘")
                    else:
                        logger.debug("非轮询阶段(%s), 跳过行情轮询", phase)
            except Exception as e:  # noqa: BLE001
                logger.warning("行情轮询异常: %s", e)

            deadline = cycle_started + self._interval
            while self._running and self._enabled:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(0.5, remaining))

    def _fetch_quotes(self, *, final: bool = False) -> bool:
        """拉取行情。加锁串行化 (后台轮询 vs 手动 refresh)。返回本轮是否成功更新。"""
        with self._fetch_lock:
            before = self._fetched_at
            with self._lock:
                consumer_revision = self._symbol_consumer_revision
            if final:
                logger.info("最终行情同步开始")
            if self.realtime_mode() == "watchlist":
                self._fetch_watchlist_quotes()
            else:
                self._fetch_full_market_quotes()
            updated = self._fetched_at > before
            with self._lock:
                consumers_unchanged = consumer_revision == self._symbol_consumer_revision
        if updated:
            self._notify_fetch_listeners()
        return updated and (not final or consumers_unchanged)

    def _fetch_full_market_quotes(self) -> None:
        """拉取全市场行情 → 写 daily + 计算 enriched + 更新缓存。"""
        from app.services import preferences

        provider_name = preferences.get_realtime_data_provider()
        if provider_name != "tickflow":
            from app.data_providers import custom as custom_sources
            if custom_sources.provider_has_dataset(provider_name, "realtime"):
                try:
                    t0 = time.perf_counter()
                    now_ts = time.perf_counter()
                    records = custom_sources.get_provider(provider_name).get_realtime()
                except Exception as e:  # noqa: BLE001
                    logger.warning("自定义实时行情拉取失败: %s", e)
                    return
                self._process_full_market_records(records, t0=t0, now_ts=now_ts)
                return
            # 自定义源未配置 realtime → 回退 TickFlow

        from app.tickflow.client import get_paid_realtime_client

        tf = get_paid_realtime_client()
        if tf is None:
            logger.warning("实时行情拉取失败:未配置付费服务器 API Key")
            return
        t0 = time.perf_counter()
        now_ts = time.perf_counter()

        try:
            from app.services import preferences
            all_index_symbols = set(self._repo.get_index_symbol_set()) if self._repo else set()
            core_index_symbols = set(preferences.get_realtime_index_symbols() or self.CORE_INDEX_SYMBOLS)
            all_index_symbols.update(core_index_symbols)
            # 指数监控规则标的并入轮询 (mode=core 时 quotes.get 显式拉取覆盖; mode=all 被 CN_Index 全覆盖)
            monitor_index_symbols: set[str] = set()
            engine = getattr(self._app_state, "monitor_engine", None) if self._app_state else None
            if engine:
                for _r in list(engine.rules.values()):
                    if _r.get("enabled", True) and _r.get("asset_type") == "index" and _r.get("scope") == "symbols":
                        monitor_index_symbols.update(s for s in _r.get("symbols", []) if s)
            all_index_symbols.update(monitor_index_symbols)
            all_etf_symbols = set()
            if self._repo:
                etf_inst = self._repo.get_etf_instruments()
                if not etf_inst.is_empty() and "symbol" in etf_inst.columns:
                    all_etf_symbols = set(etf_inst["symbol"].cast(pl.Utf8).to_list())

            universes: list[str] = []
            if preferences.get_realtime_pull_stock():
                universes.append("CN_Equity_A")
            if preferences.get_realtime_pull_etf() and all_etf_symbols:
                universes.append("CN_ETF")
            if preferences.get_realtime_pull_index() and preferences.get_realtime_index_mode() == "all":
                universes.append("CN_Index")

            resp = []
            if universes:
                _u0 = time.perf_counter()
                logger.info("拉取全市场行情 (universes=%s, SDK超时=30s×重试3)", universes)
                resp.extend(tf.quotes.get_by_universes(universes=universes) or [])
                logger.info("全市场行情拉取完成: %d 条 (%.2fs)", len(resp), time.perf_counter() - _u0)
            if preferences.get_realtime_pull_index() and preferences.get_realtime_index_mode() == "core":
                _i0 = time.perf_counter()
                _core_syms = sorted(core_index_symbols | monitor_index_symbols)
                resp.extend(tf.quotes.get(symbols=_core_syms) or [])
                logger.info("核心指数行情拉取完成: %d 只 (%.2fs)", len(_core_syms), time.perf_counter() - _i0)
            received_symbols = {str(item.get("symbol") or "").strip().upper() for item in resp}
            missing_symbols = sorted(self._consumer_symbols() - received_symbols)
            if missing_symbols:
                resp.extend(tf.quotes.get(symbols=missing_symbols) or [])
                logger.info("补拉模拟盘持仓行情: %d 只", len(missing_symbols))
        except Exception as e:  # noqa: BLE001
            logger.warning("行情拉取失败 (%.2fs): %s", time.perf_counter() - t0, e)
            return

        if not resp:
            logger.warning("行情数据为空")
            return

        # ---- 解析 API 响应 (临时变量, 用完丢弃) ----
        records = []
        for q in resp:
            ext = q.get("ext") or {}
            last_price = q.get("last_price")
            prev_close = q.get("prev_close")
            change_amount = ext.get("change_amount")
            change_pct = ext.get("change_pct")
            if change_amount is None and last_price is not None and prev_close is not None:
                change_amount = float(last_price) - float(prev_close)
            if change_pct is None and change_amount is not None and prev_close not in (None, 0):
                # 与 API ext.change_pct 同为小数制 (0.0366 = 3.66%),
                # enriched 全项目约定小数 (见 pipeline.py), 此处不可乘 100
                change_pct = float(change_amount) / float(prev_close)
            records.append({
                "symbol": q.get("symbol"),
                "name": q.get("name") or ext.get("name"),
                "last_price": last_price,
                "prev_close": prev_close,
                "open": q.get("open"),
                "high": q.get("high"),
                "low": q.get("low"),
                "volume": q.get("volume"),
                "amount": q.get("amount"),
                "change_pct": change_pct,
                "change_amount": change_amount,
                "amplitude": ext.get("amplitude"),
                "turnover_rate": ext.get("turnover_rate"),
                "timestamp": q.get("timestamp"),
                "session": q.get("session"),
            })

        sparse_assets = {
            asset_type
            for asset_type, enabled in (
                ("stock", preferences.get_realtime_pull_stock()),
                ("etf", preferences.get_realtime_pull_etf()),
            )
            if not enabled
        }
        self._process_full_market_records(records, t0=t0, now_ts=now_ts, merge_assets=sparse_assets)

    def _process_full_market_records(
        self,
        records: list[dict],
        *,
        t0: float,
        now_ts: float,
        merge_assets: set[str] | None = None,
    ) -> None:
        """把全市场 records 写盘并增量计算 enriched。"""
        from app.services import preferences
        merge_assets = merge_assets or set()
        all_index_symbols = set(self._repo.get_index_symbol_set()) if self._repo else set()
        core_index_symbols = set(preferences.get_realtime_index_symbols() or self.CORE_INDEX_SYMBOLS)
        all_index_symbols.update(core_index_symbols)
        all_etf_symbols = set()
        if self._repo:
            etf_inst = self._repo.get_etf_instruments()
            if not etf_inst.is_empty() and "symbol" in etf_inst.columns:
                all_etf_symbols = set(etf_inst["symbol"].cast(pl.Utf8).to_list())

        if not records:
            logger.warning("行情数据为空")
            return
        self._cache_latest_quotes(records)
        self._record_intraday_quotes(records)

        index_records = [r for r in records if r.get("symbol") in all_index_symbols]
        etf_records = [r for r in records if r.get("symbol") in all_etf_symbols]
        stock_records = [
            r for r in records
            if r.get("symbol") not in all_index_symbols and r.get("symbol") not in all_etf_symbols
        ]

        fetch_ms = (time.perf_counter() - t0) * 1000
        fetched_at = time.time() * 1000

        # ---- 更新元信息 ----
        with self._lock:
            self._fetch_time = now_ts
            self._fetch_ms = fetch_ms
            self._fetched_at = fetched_at
            self._symbol_count = len(stock_records)
            self._index_symbol_count = len(index_records)
            self._etf_symbol_count = len(etf_records)
            self._index_quotes_cache = self._build_index_quotes(index_records)

        _persist_last_fetch(fetched_at)
        logger.info("行情刷新: %d 只股票, %d 只ETF, %d 只指数, 耗时 %.0fms", len(stock_records), len(etf_records), len(index_records), fetch_ms)

        # 轮询放量状态更新 (volume_delta 规则的差值来源)
        self._update_volume_delta(stock_records, fetched_at)

        # ---- 写 kline_daily (不复权原始价格, 只有 OHLCV) ----
        daily_df = self._build_daily(stock_records)
        if not daily_df.is_empty() and self._repo:
            try:
                if "stock" in merge_assets:
                    self._repo.merge_live_daily_asset("stock", daily_df)
                else:
                    self._repo.flush_live_daily(daily_df)
            except Exception as e:  # noqa: BLE001
                logger.warning("日K写盘失败: %s", e)

        etf_daily_df = self._build_daily(etf_records)
        if not etf_daily_df.is_empty() and self._repo:
            try:
                if "etf" in merge_assets:
                    self._repo.merge_live_daily_asset("etf", etf_daily_df)
                else:
                    self._repo.flush_live_daily_asset("etf", etf_daily_df)
            except Exception as e:  # noqa: BLE001
                logger.warning("ETF 日K写盘失败: %s", e)

        # ---- 构建 API 直接值的补充表 (不写 daily, 只用于 enriched 计算) ----
        quote_extra = self._build_quote_extra(stock_records)
        etf_quote_extra = self._build_quote_extra(etf_records)

        # ---- 增量计算 enriched + 写盘 + 更新缓存 ----
        if not daily_df.is_empty() and self._repo:
            self._flush_live_enriched(daily_df, quote_extra, asset_type="stock", merge="stock" in merge_assets)
        if not etf_daily_df.is_empty() and self._repo:
            self._flush_live_enriched(etf_daily_df, etf_quote_extra, asset_type="etf", merge="etf" in merge_assets)
        # ---- 指数: 仅有指数监控规则时才写盘 (无规则零成本) ----
        # mode=all (完整 CN_Index universe) → flush 覆盖; mode=core (部分标的) → merge 不截断分区
        engine = getattr(self._app_state, "monitor_engine", None) if self._app_state else None
        if engine and engine.has_asset_rules("index") and self._repo:
            index_daily_df = self._build_daily(index_records)
            if not index_daily_df.is_empty():
                use_flush = preferences.get_realtime_index_mode() == "all"
                try:
                    if use_flush:
                        self._repo.flush_live_daily_asset("index", index_daily_df)
                    else:
                        self._repo.merge_live_daily_asset("index", index_daily_df)
                except Exception as e:  # noqa: BLE001
                    logger.warning("指数日K写盘失败: %s", e)
                self._flush_live_enriched(index_daily_df, self._build_quote_extra(index_records), asset_type="index", merge=not use_flush)

        # ---- 通知 SSE ----
        self._broadcast_quote_updated()

        # ---- 策略监控 + 告警评估 ----
        self._evaluate_monitors(daily_df, quote_extra)

    def _fetch_watchlist_quotes(self) -> None:
        """Free 档自选股实时: 按 capability batch 上限分批拉取。"""
        from app.services import preferences
        from app.tickflow.client import get_paid_realtime_client
        from app.tickflow.capabilities import Cap
        from app.tickflow.policy import detect_capabilities
        from app.tickflow.rate_limits import chunked, resolve_limit, sleep_between_batches

        configured_symbols = set(preferences.get_realtime_watchlist_symbols())
        symbols = sorted(configured_symbols | self._consumer_symbols())
        persistent_symbols = set(configured_symbols)
        # 指数监控规则标的并入轮询 (与股票共享 batch 额度)
        engine = getattr(self._app_state, "monitor_engine", None) if self._app_state else None
        if engine:
            for _r in list(engine.rules.values()):
                if _r.get("enabled", True) and _r.get("asset_type") == "index" and _r.get("scope") == "symbols":
                    for _s in _r.get("symbols", []):
                        if _s and _s not in symbols:
                            symbols.append(_s)
                        if _s:
                            persistent_symbols.add(_s)
        if not symbols:
            logger.info("自选实时未配置标的, 跳过行情拉取")
            return

        tf = get_paid_realtime_client()
        if tf is None:
            logger.warning("自选实时拉取失败:未配置付费服务器 API Key")
            return

        # 按 capability batch 上限分批: 股票+指数共享额度, 超过上限会导致整轮失败
        capset = detect_capabilities()
        lim = resolve_limit(capset, Cap.QUOTE_BY_SYMBOL, default_batch=5)
        batches = chunked(symbols, lim.batch)

        t0 = time.perf_counter()
        now_ts = time.perf_counter()
        resp = []
        for i, batch in enumerate(batches):
            sleep_between_batches(i, lim.rpm)
            try:
                resp.extend(tf.quotes.get(symbols=batch) or [])
            except Exception as e:  # noqa: BLE001
                logger.warning("自选实时批次 %d/%d 拉取失败: %s", i + 1, len(batches), e)

        if not resp:
            logger.warning("自选实时行情数据为空")
            return

        records = []
        for q in resp:
            ext = q.get("ext") or {}
            last_price = q.get("last_price")
            prev_close = q.get("prev_close")
            change_amount = ext.get("change_amount")
            change_pct = ext.get("change_pct")
            if change_amount is None and last_price is not None and prev_close is not None:
                change_amount = float(last_price) - float(prev_close)
            if change_pct is None and change_amount is not None and prev_close not in (None, 0):
                # 小数制, 与 ext.change_pct / enriched 口径一致 (不乘 100)
                change_pct = float(change_amount) / float(prev_close)
            records.append({
                "symbol": q.get("symbol"),
                "name": q.get("name") or ext.get("name"),
                "last_price": last_price,
                "prev_close": prev_close,
                "open": q.get("open"),
                "high": q.get("high"),
                "low": q.get("low"),
                "volume": q.get("volume"),
                "amount": q.get("amount"),
                "change_pct": change_pct,
                "change_amount": change_amount,
                "amplitude": ext.get("amplitude"),
                "turnover_rate": ext.get("turnover_rate"),
                "timestamp": q.get("timestamp"),
                "session": q.get("session"),
            })
        self._cache_latest_quotes(records)

        index_set = self._repo.get_index_symbol_set() if self._repo else set()
        etf_set = self._repo.get_etf_symbol_set() if self._repo else set()
        index_records, etf_records, stock_records = self._split_records_by_asset(records, index_set, etf_set)

        fetch_ms = (time.perf_counter() - t0) * 1000
        fetched_at = time.time() * 1000
        with self._lock:
            self._fetch_time = now_ts
            self._fetch_ms = fetch_ms
            self._fetched_at = fetched_at
            self._symbol_count = len(stock_records)
            self._index_symbol_count = len(index_records)
            self._etf_symbol_count = len(etf_records)
            self._index_quotes_cache = self._build_index_quotes(index_records) if index_records else None

        _persist_last_fetch(fetched_at)
        logger.info("自选实时刷新: %d 只股票, %d 只ETF, %d 只指数, 耗时 %.0fms",
                    len(stock_records), len(etf_records), len(index_records), fetch_ms)

        persistent_records = [row for row in records if row.get("symbol") in persistent_symbols]
        persistent_index_records, persistent_etf_records, persistent_stock_records = self._split_records_by_asset(
            persistent_records,
            index_set,
            etf_set,
        )
        daily_df = self._build_daily(persistent_stock_records)
        quote_extra = self._build_quote_extra(persistent_stock_records)
        if not daily_df.is_empty() and self._repo:
            try:
                self._repo.merge_live_daily_asset("stock", daily_df)
            except Exception as e:  # noqa: BLE001
                logger.warning("自选实时日K写盘失败: %s", e)
            self._flush_live_enriched(daily_df, quote_extra, asset_type="stock", merge=True)

        # 已配置的 ETF/指数按各自资产落盘，不污染股票表。
        etf_daily_df = self._build_daily(persistent_etf_records)
        if not etf_daily_df.is_empty() and self._repo:
            try:
                self._repo.merge_live_daily_asset("etf", etf_daily_df)
            except Exception as e:  # noqa: BLE001
                logger.warning("自选实时 ETF 日K写盘失败: %s", e)
            self._flush_live_enriched(etf_daily_df, self._build_quote_extra(persistent_etf_records), asset_type="etf", merge=True)
        index_daily_df = self._build_daily(persistent_index_records)
        if not index_daily_df.is_empty() and self._repo:
            try:
                self._repo.merge_live_daily_asset("index", index_daily_df)
            except Exception as e:  # noqa: BLE001
                logger.warning("自选实时指数日K写盘失败: %s", e)
            self._flush_live_enriched(index_daily_df, self._build_quote_extra(persistent_index_records), asset_type="index", merge=True)

        self._broadcast_quote_updated()
        self._evaluate_monitors(daily_df, quote_extra)

    # ================================================================
    # 工具
    # ================================================================

    @staticmethod
    def _build_daily(records: list[dict]) -> pl.DataFrame:
        """将 API records 转为日K格式 DataFrame (OHLCV + quote_ts, 写 kline_daily 用)。"""
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        cols_map = {
            "symbol": "symbol",
            "last_price": "close",
            "open": "open",
            "high": "high",
            "low": "low",
            "volume": "volume",
            "amount": "amount",
            "timestamp": "quote_ts",
        }
        select_exprs = []
        for src, dst in cols_map.items():
            if src in df.columns:
                select_exprs.append(pl.col(src).cast(pl.Int64, strict=False).alias(dst)
                                     if dst == "quote_ts" else pl.col(src).alias(dst))
        if not select_exprs:
            return pl.DataFrame()
        result = df.select(select_exprs).with_columns(
            pl.lit(cn_today()).cast(pl.Date).alias("date"),
        )
        # 修复: API 在非交易时段可能返回 open/high/low=0 或 null,
        # 导致蜡烛从 0 开始。用 close 填充这些异常值。
        for col in ("open", "high", "low"):
            if col in result.columns:
                result = result.with_columns(
                    pl.when((pl.col(col) == 0) | pl.col(col).is_null())
                    .then(pl.col("close"))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        return result

    @staticmethod
    def _build_quote_extra(records: list[dict]) -> pl.DataFrame:
        """构建 API 直接提供的补充字段 (不写 daily, 只传给 enriched 计算)。

        包含: prev_close, change_pct, change_amount, amplitude, turnover_rate。
        """
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        keep = [c for c in [
            "symbol", "prev_close", "change_pct", "change_amount",
            "amplitude", "turnover_rate",
        ] if c in df.columns]
        if not keep or "symbol" not in keep:
            return pl.DataFrame()
        out = df.select(keep)
        # 实时 API 的 turnover_rate 入口契约为小数制(0.05 = 5%).
        # enriched 内部统一存百分数值(5 = 5%), 后续页面/筛选直接展示和比较。
        if "turnover_rate" in out.columns:
            out = out.with_columns((pl.col("turnover_rate").cast(pl.Float64, strict=False) * 100).alias("turnover_rate"))
        return out

    @staticmethod
    def _build_index_quotes(records: list[dict]) -> pl.DataFrame:
        """构建指数实时行情缓存，不落股票 parquet。

        注意: API 返回的 change_pct/amplitude 是小数 (0.0366 = 3.66%),
        统一转成百分比输出, 与 _fallback_index_quotes_from_daily 口径一致
        (前端指数侧不×100, 直接 toFixed(2)% 展示)。
        """
        if not records:
            return pl.DataFrame()
        df = pl.DataFrame(records)
        keep = [c for c in [
            "symbol", "name", "last_price", "prev_close", "open", "high", "low",
            "volume", "amount", "change_pct", "change_amount", "amplitude", "timestamp", "session",
        ] if c in df.columns]
        if not keep or "symbol" not in keep:
            return pl.DataFrame()
        df = df.select(keep)
        # 自定义源可能不提供 change_pct/change_amount, 按 last_price/prev_close 补算
        # (TickFlow 路径在 _fetch_full_market_quotes 已算好, 此处只补缺失的)
        if "change_pct" not in df.columns and "last_price" in df.columns and "prev_close" in df.columns:
            # prev_close=0 → inf (非合法 JSON), prev_close=null → null; 用 when 守护
            df = df.with_columns(
                pl.when(pl.col("prev_close") != 0)
                .then((pl.col("last_price") - pl.col("prev_close")) / pl.col("prev_close"))
                .otherwise(None)
                .alias("change_pct")
            )
        if "change_amount" not in df.columns and "last_price" in df.columns and "prev_close" in df.columns:
            df = df.with_columns(
                (pl.col("last_price") - pl.col("prev_close")).alias("change_amount")
            )
        # change_pct / amplitude: 小数 → 百分比 (统一指数展示口径)
        for col in ("change_pct", "amplitude"):
            if col in df.columns:
                df = df.with_columns((pl.col(col).cast(pl.Float64) * 100).alias(col))
        if "last_price" in df.columns and "close" not in df.columns:
            df = df.with_columns(pl.col("last_price").alias("close"))
        return df

    @staticmethod
    def _market_phase() -> str:
        """A股行情轮询阶段(北京时间)。

        final 阶段用于午休/收盘定版: 需要至少成功拉取一版边界后的行情, 才算进入休盘。
        """
        now = cn_now()
        if now.weekday() >= 5:
            return "closed"
        t = now.time()
        if dt_time(9, 15) <= t < dt_time(9, 30):
            return "preopen"
        if dt_time(9, 30) <= t < dt_time(11, 30):
            return "morning"
        if dt_time(11, 30) <= t < dt_time(12, 55):
            return "morning_final"
        if dt_time(12, 55) <= t < dt_time(13, 0):
            return "pre_afternoon"
        if dt_time(13, 0) <= t < dt_time(15, 0):
            return "afternoon"
        if t >= dt_time(15, 0):
            return "close_final"
        return "closed"

    @staticmethod
    def _final_sync_key(phase: str) -> tuple[date, str] | None:
        if phase == "morning_final":
            return (cn_today(), "morning")
        if phase == "close_final":
            return (cn_today(), "close")
        return None

    def _holiday_gate(self) -> bool:
        """交易日探针门控: 确定休市 → False (停止轮询, 含 final 定版)。

        探针未知 (None, 未配置 fuyao 且 tickflow 不可用/开盘缓冲窗内) → True,
        维持周几近似现状行为。探针是纯读, 不落盘; 休市结论带 TTL 定期复探,
        误判自愈。首次判定变化打一条日志, 避免每拍刷屏。
        """
        from app.services import trading_day

        holiday = trading_day.is_trading_day() is False
        if holiday != self._holiday_active:
            self._holiday_active = holiday
            if holiday:
                logger.info("交易日探针判定休市, 行情轮询暂停 (30 分钟复探)")
        return not holiday

    def _should_poll_for_phase(self, phase: str) -> bool:
        """是否处于会主动拉行情的阶段。final 阶段成功后即停止。

        交易日探针会剔除工作日休市；常规轮询只允许发生在连续竞价时段。
        盘前和午后开盘前的集合竞价阶段不拉取实时行情，避免非交易时间持续消耗行情配额。
        """
        if not self._holiday_gate():
            return False
        if phase in {"morning", "afternoon"}:
            return True
        key = self._final_sync_key(phase)
        return bool(key and key not in self._final_sync_done)

    def _should_fetch_for_phase(self, phase: str) -> bool:
        return self._should_poll_for_phase(phase)

    def _is_trading_hours(self) -> bool:
        """行情轮询窗口(兼容旧调用): 包含盘前预热和未完成的午休/收盘定版。"""
        return self._should_poll_for_phase(self._market_phase())

    @staticmethod
    def _is_continuous_trading() -> bool:
        """A股连续竞价时段(北京时间): 9:30-11:30 / 13:00-15:00, 仅工作日。

        比 _is_trading_hours 严格: 排除 9:15-9:30 集合竞价(指示价, 非成交价)、
        午间与 15:00 后收盘缓冲。监控评估只在此窗口进行, 不对竞价/收盘后的陈旧价告警。
        (节假日由 _evaluate_monitors 里的「快照日期=当日」新鲜度判据兜底, 无需交易日历。)
        """
        now = cn_now()
        t = now.time()
        morning = dt_time(9, 30) <= t <= dt_time(11, 30)
        afternoon = dt_time(13, 0) <= t <= dt_time(15, 0)
        return now.weekday() < 5 and (morning or afternoon)

    @staticmethod
    def _save_enabled(enabled: bool) -> None:
        from app.services import preferences
        preferences.save({"realtime_quotes_enabled": enabled})

    # ================================================================
    # 策略监控
    # ================================================================

    def _evaluate_monitors(self, daily_df: pl.DataFrame, quote_extra: pl.DataFrame | None) -> None:
        """行情更新后评估统一监控规则引擎,并刷新策略结果缓存。"""
        try:
            # 仅在「交易日 + 连续竞价时段」评估监控 —— 避开集合竞价指示价、盘前/收盘后
            # 缓冲。轮询窗口(_is_trading_hours)更宽是为盘前预热/收盘捕捉, 但告警不应
            # 基于这些非连续竞价价格。
            if not self._is_continuous_trading():
                return
            # 获取 enriched 数据 (刚算好的)
            enriched_today, enriched_date = self.get_enriched_today()
            # 股票快照就绪 = 非空 + 日期为当日。未就绪时仅跳过股票轮,
            # ETF/指数轮有各自的空表+日期守卫, 不受影响 (纯指数行情/自选场景可独立评估)。
            stock_ready = (not enriched_today.is_empty()) and (enriched_date == cn_today())
            if not stock_ready:
                logger.debug("股票快照未就绪(空=%s, 日期=%s), 跳过股票轮",
                             enriched_today.is_empty(), enriched_date)

            all_alerts: list[dict] = []
            rule_events: list[dict] = []
            engine = None

            # 通用监控规则评估 (统一引擎: signal/price/market/strategy)
            if self._app_state:
                engine = getattr(self._app_state, "monitor_engine", None)
                if engine and engine.rule_count > 0:
                    # 预构建 symbol → name 映射 (enriched 已 drop name 列, 引擎触发时回填用)。
                    # 股票 + ETF + 指数三表合并走 _monitor_name_map -> repo.get_name_map()
                    # 的进程内 memo, 避免每轮监控对 ~7000 行维表 iter_rows 重建。
                    try:
                        name_map = _monitor_name_map(self._app_state.repo)
                        if name_map:
                            engine.set_name_map(name_map)
                    except Exception as e:  # noqa: BLE001
                        logger.debug("name_map 构建失败 (不影响监控): %s", e)
                    # 股票轮: 快照未就绪时跳过 (ladder 封单也依赖股票快照日期, 一并跳过)
                    if stock_ready:
                        eval_df = enriched_today
                        if engine.has_rule_type("ladder"):
                            eval_df = self._inject_sealed_vol(enriched_today, enriched_date)
                        if engine.has_rule_type("volume_delta"):
                            eval_df = self._inject_volume_delta(eval_df)
                        eval_df = self._inject_intraday_signals(eval_df, engine, "stock")
                        rule_events = engine.evaluate(eval_df, asset_type="stock")
                        if engine.consume_strategy_result_updates():
                            self.notify_strategy_results_updated()
                    if engine.has_rule_type("sector"):
                        rule_events += engine.evaluate_sectors(
                            enriched_today if stock_ready else pl.DataFrame(),
                            self.get_index_quotes(),
                        )
                    # 异动边缘规则轮: 快照 (enriched 偏离列 + 实时叠加) 由
                    # abnormal_moves.build_overview 统一构建, 引擎只做边缘触发判定。
                    # 30s 限频 —— 快照历史部分 60s 缓存, 无需跟行情轮询同频重算。
                    if engine.has_rule_type("abnormal") and self._repo is not None:
                        _now_ts = time.time()
                        if _now_ts - self._abnormal_last_eval >= 30.0:
                            self._abnormal_last_eval = _now_ts
                            try:
                                from app.services import abnormal_moves
                                _overview = abnormal_moves.build_overview(
                                    self._repo, self,
                                    min_closeness=engine.min_abnormal_closeness(),
                                    limit=1000,
                                )
                                rule_events += engine.evaluate_abnormal(_overview.get("rows") or [])
                            except Exception as e:  # noqa: BLE001
                                logger.warning("异动监控规则评估失败 (不影响其他告警): %s", e)
                    # ETF 规则轮: 股票快照不含 ETF, 用 ETF enriched 快照单独评估。
                    # 独立 try —— ETF 轮任何异常都不得丢弃本轮已算出的股票告警。
                    # refresh=False —— 不在轮询线程上触发 ETF 冷缓存的同步重算 (缓存由 ETF 实时
                    # flush 焐热; 未焐热说明无 ETF 实时数据, 跳过本轮 ETF 评估)。
                    if engine.has_asset_rules("etf") and self._repo is not None:
                        try:
                            etf_enriched, _ = self._repo.get_enriched_latest_asset("etf", refresh=False)
                            if not etf_enriched.is_empty():
                                etf_enriched = self._inject_intraday_signals(etf_enriched, engine, "etf")
                                rule_events = rule_events + engine.evaluate(
                                    etf_enriched, asset_type="etf", reset_strategy_results=False,
                                )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("ETF 监控评估失败 (不影响股票告警): %s", e)
                    # 指数规则轮: 复刻 ETF 轮。快照由指数实时 flush 焐热;
                    # refresh=False 冷缓存不同步重算; 显式日期守卫防陈旧 parquet 误告警
                    # (ETF 轮靠空表隐式跳过, 指数轮更显式, 行为等价)。
                    if engine.has_asset_rules("index") and self._repo is not None:
                        try:
                            index_enriched, index_date = self._repo.get_enriched_latest_asset("index", refresh=False)
                            if not index_enriched.is_empty() and index_date == cn_today():
                                index_enriched = self._inject_intraday_signals(index_enriched, engine, "index")
                                rule_events = rule_events + engine.evaluate(
                                    index_enriched, asset_type="index", reset_strategy_results=False,
                                )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("指数监控评估失败 (不影响股票/ETF 告警): %s", e)
                    if rule_events:
                        rule_events = self._format_extension_notifications(rule_events)
                        # 落盘到 alerts.jsonl
                        try:
                            from app.services import alert_store
                            alert_store.append_many(
                                self._app_state.repo.store.data_dir, rule_events,
                            )
                        except Exception as e:  # noqa: BLE001
                            logger.warning("告警落盘失败: %s", e)
                        # 转为 SSE 推送格式 (兼容旧 alert schema)
                        for ev in rule_events:
                            alert = {
                                "source": ev["source"],
                                "type": ev["type"],
                                "rule_id": ev.get("rule_id"),
                                "strategy_id": ev.get("strategy_id") if ev["source"] == "strategy" else None,
                                "symbol": ev["symbol"],
                                "name": ev["name"],
                                "message": ev["message"],
                                "price": ev["price"],
                                "change_pct": ev["change_pct"],
                                "signals": ev["signals"],
                                "severity": ev.get("severity", "info"),
                                "conditions": ev.get("conditions") or [],
                                "logic": ev.get("logic") or "and",
                            }
                            for key in (
                                "sector_kind", "sector_key", "sector_name",
                                "sector_source_field", "sector_value", "sector_level",
                                "window_change_pct", "coverage_ratio", "valid_count",
                                "total_count", "up_count", "down_count", "leader",
                                "abnormal_window", "abnormal_value", "abnormal_threshold",
                                "abnormal_closeness", "volume_delta", "volume_delta_span",
                                "volume_delta_amount",
                            ):
                                if key in ev:
                                    alert[key] = ev[key]
                            all_alerts.append(alert)

            # 策略页实时回显: 不写文件 (实时行情每轮更新 enriched, 写文件会被 read_cache
            # 的 mtime 校验判过期, 反复读不到)。监控引擎本轮已算出的结果存在内存
            # (latest_strategy_results), 由 /api/screener/cached 端点直接叠加读取。

            # 广播到所有 SSE 订阅者 (背压保护在订阅者队列内做)
            if all_alerts:
                # 按 symbol 富化行业/概念 ext 字段, 使 toast + 触发记录统一展示板块标签。
                self._enrich_alerts_ext(all_alerts)
                self._broadcast_alerts(all_alerts)
                logger.info("监控评估完成: %d 条通知", len(all_alerts))

                # 系统通知 (可选通道, 由 preferences 开关控制)。
                # cooldown 去重已在 MonitorRuleEngine 做过, 这里只负责转发。
                self._maybe_send_system_notifications(all_alerts)

            # Webhook 推送 (飞书等外部 IM, 由规则 webhook_channels 指定渠道)。
            # 紧随系统通知, 同样静默降级不阻断主流程。
            if rule_events:
                self._maybe_send_webhook(rule_events, engine)

        except Exception as e:  # noqa: BLE001
            logger.warning("监控评估失败: %s", e)

    def _format_extension_notifications(self, events: list[dict]) -> list[dict]:
        """Apply optional copy formatters after evaluation and before every output channel."""
        registry = (
            getattr(self._app_state, "extension_registry", None)
            if self._app_state is not None
            else None
        )
        if registry is None or not registry.has_notification_formatters:
            return events

        from app.extensions.contracts import (
            BACKEND_EXTENSION_API_VERSION,
            NotificationFormatContext,
        )

        formatted_events: list[dict] = []
        for event in events:
            formatted = dict(event)
            context = NotificationFormatContext(
                api_version=BACKEND_EXTENSION_API_VERSION,
            )
            for registered in registry.notification_formatters():
                try:
                    message = registered.implementation.format_message(dict(formatted), context)
                    if not isinstance(message, str):
                        raise TypeError("notification formatter must return str")
                    formatted["message"] = message
                except Exception as exc:
                    logger.warning(
                        "notification formatter failed %s: %s",
                        registered.implementation_id,
                        exc,
                    )
            formatted_events.append(formatted)
        return formatted_events

    def _enrich_alerts_ext(self, alerts: list[dict]) -> None:
        """就地给告警事件按 symbol 追加行业/概念 ext 字段。

        读 preferences.get_monitor_ext_fields() 取字段配置, 用 screener._load_ext_value_maps
        (带 parquet mtime 缓存) 富化。富化失败静默降级 (告警照常推送, 只是没标签)。
        每条事件新增 {configId}__{fieldName} 键 (与 watchlist/screener 输出约定一致)。
        """
        if not alerts or not self._app_state or self._repo is None:
            return
        try:
            from app.services import preferences
            fields = preferences.get_monitor_ext_fields()
            # 新结构 {field, maxTags, hiddenIndices}, 后端只需 .field
            parts = []
            aliases: dict[str, str] = {}
            for key in ("concept", "industry"):
                item = fields.get(key)
                if isinstance(item, dict) and item.get("field"):
                    field = item["field"]
                    parts.append(field)
                elif isinstance(item, str) and item:
                    field = item
                    parts.append(field)  # 兼容旧格式
                else:
                    continue
                if "." in field:
                    config_id, field_name = field.split(".", 1)
                    aliases[f"{config_id}__{field_name}"] = key
            if not parts:
                return
            ext_columns = ",".join(parts)
            from app.api.screener import _load_ext_value_maps
            value_maps = _load_ext_value_maps(self._repo, ext_columns)
            if not value_maps:
                return
            for ev in alerts:
                sym = ev.get("symbol")
                if not sym:
                    continue
                for out_col, vmap in value_maps.items():
                    value = vmap.get(str(sym))
                    ev[out_col] = value
                    if aliases.get(out_col) == "concept" and value not in (None, ""):
                        ev["concept"] = value
        except Exception as e:  # noqa: BLE001
            logger.debug("告警 ext 富化失败 (不影响推送): %s", e)

    def _inject_intraday_signals(self, enriched: pl.DataFrame, engine, asset_type: str) -> pl.DataFrame:
        """从共享分钟快照注入分时信号，监控中心与持仓风控使用同一输入。"""
        get_symbols = getattr(engine, "intraday_signal_symbols", None)
        if not callable(get_symbols):
            return enriched
        symbols = get_symbols(asset_type)
        if not symbols:
            self.remove_intraday_consumer(f"monitor:{asset_type}")
            return enriched

        now = cn_now()
        self.set_intraday_consumer(f"monitor:{asset_type}", symbols, asset_type)
        prev_close: dict[str, float] = {}
        available_cols = set(enriched.columns)
        scoped = enriched.filter(pl.col("symbol").is_in(sorted(symbols)))
        for row in scoped.iter_rows(named=True):
            symbol = str(row.get("symbol") or "")
            reference = row.get("prev_close") if "prev_close" in available_cols else None
            if reference is None and "close" in available_cols and "change_pct" in available_cols:
                close = row.get("close")
                change_pct = row.get("change_pct")
                if close is not None and change_pct is not None and float(change_pct) > -1:
                    reference = float(close) / (1.0 + float(change_pct))
            if symbol and reference is not None:
                prev_close[symbol] = float(reference)

        signal_map = self.get_intraday_signals(
            symbols,
            prev_close=prev_close,
            asset_type=asset_type,
            now=now,
            consumer_id=f"monitor:{asset_type}",
        )
        signals = list(signal_map.values())
        # 涨停/封板是更高优先级状态；同一涨停价附近的均价穿越不重复提醒。
        limit_prices: dict[str, float] = {}
        if "limit_up" in enriched.columns:
            for row in scoped.iter_rows(named=True):
                limit = _finite(row.get("limit_up"))
                if limit and limit > 0:
                    limit_prices[str(row.get("symbol"))] = limit
        with self._lock:
            for symbol in symbols:
                quote = self._latest_quotes.get(symbol)
                limit = _finite(quote.get("limit_up")) if quote else None
                if limit and limit > 0:
                    limit_prices.setdefault(symbol, limit)
        filtered: list[dict[str, Any]] = []
        for signal in signals:
            symbol = str(signal.get("symbol") or "")
            limit = limit_prices.get(symbol)
            quote = self._latest_quotes.get(symbol)
            price = _finite(quote.get("last_price")) if quote else None
            if limit and price and price >= limit - max(0.001, limit * 1e-6):
                signal = dict(signal)
                signal["signal_intraday_avg_cross_up"] = False
                signal["signal_intraday_avg_cross_down"] = False
            filtered.append(signal)
        return self._intraday_signal_evaluator.inject(enriched, filtered)

    @staticmethod
    def _continuous_session_start_ms() -> float:
        """当前连续竞价时段的起点 (北京时间 9:30 或 13:00) 的 epoch 毫秒。"""
        now = cn_now()
        start_time = dt_time(13, 0) if now.time() >= dt_time(13, 0) else dt_time(9, 30)
        return datetime.combine(now.date(), start_time, tzinfo=now.tzinfo).timestamp() * 1000.0

    def _update_volume_delta(self, stock_records: list[dict], fetched_at_ms: float) -> None:
        """全市场相邻两次快照的股票累计成交量差值 (手), 供 volume_delta 规则。

        - prev 每轮都更新 (含非连续竞价时段); 差值只在连续竞价时段内计算
        - 开盘保护: prev 早于本时段起点 (9:30/13:00) 时本轮差值无效 -- 避免
          9:25 集合竞价撮合量 / 午休缺口被当成"突然放量"
        - cur < prev (数据源重置/口径跳变) 的个股丢弃差值; 跨交易日清空
        """
        today = cn_today()
        if self._prev_volume_date != today:
            self._prev_stock_volume = None
            self._prev_volume_fetched_at = None
            self._prev_volume_date = today
            self._volume_delta = {}

        cur: dict[str, tuple[float, float]] = {}
        for r in stock_records:
            sym = r.get("symbol")
            vol = r.get("volume")
            amt = r.get("amount")
            if not sym or not isinstance(vol, (int, float)):
                continue
            cur[str(sym)] = (
                float(vol),
                float(amt) if isinstance(amt, (int, float)) else 0.0,
            )

        prev = self._prev_stock_volume
        prev_ts = self._prev_volume_fetched_at
        if (
            prev is not None
            and prev_ts is not None
            and self._is_continuous_trading()
            and prev_ts >= self._continuous_session_start_ms()
        ):
            delta = {
                sym: (v - prev[sym][0], a - prev[sym][1])
                for sym, (v, a) in cur.items()
                if sym in prev and v >= prev[sym][0] and a >= prev[sym][1] and v - prev[sym][0] > 0
            }
            self._volume_delta = delta
            self._volume_delta_span_s = max((fetched_at_ms - prev_ts) / 1000.0, 0.001)
        else:
            self._volume_delta = {}

        self._prev_stock_volume = cur
        self._prev_volume_fetched_at = fetched_at_ms

    def _inject_volume_delta(self, enriched_today: pl.DataFrame) -> pl.DataFrame:
        """把最近一轮快照差值作为临时列注入 enriched 副本。

        _volume_delta (手) / _volume_delta_amount (元) / _volume_delta_span (秒, 快照间隔)。
        无有效差值 (首轮/开盘保护/暂停后恢复) 时返回原 df, 规则安全降级不触发。
        """
        try:
            delta = self._volume_delta
            if not delta:
                return enriched_today
            span = self._volume_delta_span_s
            delta_df = pl.DataFrame({
                "symbol": list(delta.keys()),
                "_volume_delta": [v for v, _ in delta.values()],
                "_volume_delta_amount": [a for _, a in delta.values()],
                "_volume_delta_span": [span] * len(delta),
            })
            drop_cols = [
                c for c in ("_volume_delta", "_volume_delta_amount", "_volume_delta_span")
                if c in enriched_today.columns
            ]
            df = enriched_today.drop(drop_cols) if drop_cols else enriched_today
            return df.join(delta_df, on="symbol", how="left")
        except Exception as e:  # noqa: BLE001
            logger.debug("快照差值注入失败 (volume_delta 规则将不触发): %s", e)
            return enriched_today

    def _inject_sealed_vol(self, enriched_today: pl.DataFrame, enriched_date) -> pl.DataFrame:
        """从 depth_service 取封单量, 作为临时列 _sealed_vol 注入 enriched 副本。

        涨停封单(买一量) + 跌停封单(卖一量)合并, 供 ladder 规则评估。
        depth 未就绪时返回原 df (不注入, ladder 规则安全降级不触发)。
        """
        try:
            depth_svc = getattr(self._app_state, "depth_service", None)
            if not depth_svc:
                return enriched_today
            # enriched_date 可能是 date 或字符串, 统一为 date
            from datetime import date as date_cls
            target_date = enriched_date if isinstance(enriched_date, date_cls) else date_cls.fromisoformat(str(enriched_date))
            # 取涨停 + 跌停封单, 合并 {symbol: vol}
            up_map = depth_svc.get_sealed_map(target_date, is_down=False)
            down_map = depth_svc.get_sealed_map(target_date, is_down=True)
            sealed: dict[str, int] = {}
            for m in (up_map, down_map):
                for sym, info in m.items():
                    vol = (info or {}).get("vol")
                    if vol and vol > 0:
                        sealed[sym] = vol  # 后者覆盖前者 (同 symbol 不可能在涨跌停都封单)
            if not sealed:
                return enriched_today
            # 构造 (symbol, _sealed_vol) DataFrame, join 到 enriched 副本
            sealed_df = pl.DataFrame({
                "symbol": list(sealed.keys()),
                "_sealed_vol": list(sealed.values()),
            })
            # 若已有残留列先移除 (避免重复 join 报错)
            df = enriched_today.drop("_sealed_vol") if "_sealed_vol" in enriched_today.columns else enriched_today
            return df.join(sealed_df, on="symbol", how="left")
        except Exception as e:  # noqa: BLE001
            logger.debug("封单注入失败 (ladder 规则将不触发): %s", e)
            return enriched_today

    def _maybe_send_webhook(self, rule_events: list[dict], engine) -> None:
        """把告警通过 Webhook 推送到外部 IM (由规则 webhook_channels 指定渠道)。

        - 飞书 / 企业微信任一已配置即生效 (两个都没配才跳过)
        - 仅推送 webhook_channels 非空的规则触发的告警, 且只投递被勾选的渠道
        - 失败静默, 不阻断主流程
        - 去重: 复用 MonitorRuleEngine 的 cooldown, 此处不重复去重

        注意: 用 rule_events (含 rule_id) 而非重建后的 all_alerts,
        以便反查引擎规则判断是否启用推送。
        """
        try:
            from app.services import preferences
            from app.services import webhook_adapter

            feishu_url = preferences.get_feishu_webhook_url()
            feishu_secret = preferences.get_feishu_webhook_secret()
            wecom_url = preferences.get_wecom_webhook_url()
            # 两个通道都没配置才跳过
            if not feishu_url and not wecom_url:
                return

            # 反查规则, 过滤出启用推送的事件
            source_labels = {
                "strategy": "策略", "signal": "信号",
                "price": "价格", "market": "异动", "ladder": "连板梯队",
                "sector": "板块", "volume_delta": "放量",
            }
            rules = engine.rules if engine is not None else {}
            enqueued = 0
            for ev in rule_events:
                rule = rules.get(ev.get("rule_id"))
                # webhook_channels 指定命中的渠道 (['feishu'] / ['wecom'] / ['feishu','wecom'] / []).
                # 空列表 = 该规则不推送。仅推送「渠道已选 + 对应地址已配置」的组合。
                channels = rule.get("webhook_channels") if rule else None
                if not channels:
                    continue
                source = ev.get("source", "")
                source_label = source_labels.get(source, source or "通知")
                symbol = ev.get("symbol") or ""
                name = ev.get("name") or ""
                message = ev.get("message") or ""
                title = source_label
                body = f"{symbol} {name} {message}".strip() if symbol else (message or name)
                # 提交到独立线程池, 不阻塞行情轮询线程 (webhook 慢/重试不拖累实时行情+告警)。
                # 按渠道独立投递: 飞书 / 企业微信谁被勾选且已配置就推谁。
                # 应用内 alerts.jsonl 记录与 SSE 已在前面完成, 不依赖 webhook 成败,
                # 失败由 webhook_adapter 记 WARNING(可见)。
                if feishu_url and "feishu" in channels:
                    _WEBHOOK_EXECUTOR.submit(webhook_adapter.send_feishu, feishu_url, title, body, feishu_secret)
                    enqueued += 1
                if wecom_url and "wecom" in channels:
                    _WEBHOOK_EXECUTOR.submit(webhook_adapter.send_wecom, wecom_url, title, body)
                    enqueued += 1
            if enqueued:
                logger.info("Webhook 已提交 %d 条 (异步投递, 按渠道独立投递, 失败记 WARNING)", enqueued)
        except Exception as e:  # noqa: BLE001
            logger.warning("Webhook 提交异常 (不影响告警主流程): %s", e)

    def _maybe_send_system_notifications(self, all_alerts: list[dict]) -> None:
        """把告警转发到操作系统通知中心 (由 preferences 开关控制)。

        - 开关关闭: 直接返回
        - 开关开启: 逐条发系统通知; 失败静默, 不阻断主流程
        - 去重: 复用 MonitorRuleEngine 的 cooldown, 此处不重复去重
        - 批量策略事件 (symbol="") 聚合为一条通知, 避免刷屏
        """
        try:
            from app.services import preferences
            from app.services import notify_adapter

            if not preferences.get_system_notify_enabled():
                return

            for ev in all_alerts:
                # 通知标题: 用 source 分类 (策略/信号/价格/异动)
                source = ev.get("source", "")
                source_label = {
                    "strategy": "策略", "signal": "信号",
                    "price": "价格", "market": "异动", "sector": "板块",
                    "position_risk": "持仓风控",
                    "limit_board": "打板专区",
                }.get(source, source or "通知")

                body = self._format_alert_notification_body(ev)

                title = f"TickFlow · {source_label}"
                notify_adapter.notify(title, body)
        except Exception as e:  # noqa: BLE001
            logger.debug("系统通知发送异常 (不影响告警主流程): %s", e)

    @staticmethod
    def _get_strategy_monitor():
        """获取 StrategyMonitorService — 不再使用, 改用 _app_state 注入。"""
        return None

    # ================================================================
    # enriched 增量计算
    # ================================================================

    def _flush_live_enriched(self, daily_df: pl.DataFrame, quote_extra: pl.DataFrame = None, asset_type: str = "stock", merge: bool = False) -> None:
        """增量计算今天的 enriched: 用昨天的递推状态 + 今天 OHLCV → 只算今天 5500 行。

        quote_extra: API 直接提供的补充字段 (prev_close, change_pct 等),
                     不写 daily, 直接传给 compute_enriched_today 避免重复计算。
        """
        try:
            today = cn_today()
            t0 = time.perf_counter()

            # ---- 尝试增量路径 ----
            live_agg = self._repo.get_live_agg() if asset_type == "stock" else pl.DataFrame()
            prev_enriched, prev_date = (
                self._repo.get_enriched_latest()
                if asset_type == "stock"
                else self._repo.get_enriched_latest_asset(asset_type)
            )

            use_incremental = (
                asset_type == "stock"
                and not live_agg.is_empty()
                and not prev_enriched.is_empty()
                and prev_date is not None
            )

            if use_incremental:
                from app.indicators.pipeline import compute_enriched_today
                from app.market_time import trading_minutes_elapsed_from_ts, trading_minutes_elapsed
                instruments = self._repo.get_instruments()
                # 将 API 直接提供的补充字段 JOIN 到 daily_df
                today_ohlcv = daily_df
                if quote_extra is not None and not quote_extra.is_empty():
                    today_ohlcv = daily_df.join(quote_extra, on="symbol", how="left")
                # 量比时间折算: 优先用行情 quote_ts (真实成交时间), 缺失则兜底服务端时间
                elapsed_minutes: float | None = None
                if "quote_ts" in daily_df.columns and not daily_df.is_empty():
                    valid_ts = daily_df["quote_ts"].drop_nulls()
                    if not valid_ts.is_empty():
                        elapsed_minutes = trading_minutes_elapsed_from_ts(valid_ts.median())
                if elapsed_minutes is None:
                    elapsed_minutes = trading_minutes_elapsed()
                enriched_today = compute_enriched_today(
                    live_agg=live_agg,
                    prev_enriched=prev_enriched,
                    today_ohlcv=today_ohlcv,
                    instruments=instruments,
                    elapsed_minutes=elapsed_minutes,
                )
                if enriched_today.is_empty():
                    logger.warning("增量计算结果为空, 回退到全量计算")
                    use_incremental = False

            # ---- 全量回退路径 ----
            if not use_incremental:
                from datetime import timedelta
                from app.indicators.pipeline import compute_enriched

                logger.info("enriched 全量计算 (live_agg=%s, 上次日期=%s)",
                            "ok" if not live_agg.is_empty() else "空", prev_date)

                cutoff = today - timedelta(days=90)
                table = {"etf": "kline_etf_daily", "index": "kline_index_daily"}.get(asset_type, "kline_daily")
                daily_glob = str(self._repo.store.data_dir / table / "**" / "*.parquet")
                ohlcv_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "quote_ts"]
                hist_df = (
                    scan_daily_parquet(daily_glob)
                    .filter(pl.col("date") >= cutoff)
                    .sort(["symbol", "date"])
                    .collect()
                )
                if hist_df.is_empty():
                    return

                hist_cols = [c for c in ohlcv_cols if c in hist_df.columns]
                hist_df = hist_df.select(hist_cols).filter(pl.col("date") != today)
                daily_ohlcv = daily_df.select([c for c in ohlcv_cols if c in daily_df.columns])
                full_df = pl.concat([hist_df, daily_ohlcv], how="diagonal_relaxed")
                full_df = full_df.sort(["symbol", "date"])

                factor_dir = {"stock": "adj_factor", "etf": "adj_factor_etf"}.get(asset_type)
                factor_path = self._repo.store.data_dir / factor_dir / "all.parquet" if factor_dir else None
                factors = pl.DataFrame()
                if factor_path and factor_path.exists():
                    try:
                        factors = pl.read_parquet(factor_path)
                    except Exception:
                        pass
                instruments = self._repo.get_instruments() if asset_type == "stock" else None

                enriched_full = compute_enriched(
                    full_df,
                    factors=factors,
                    instruments=instruments,
                    historical_shares=(
                        self._repo.get_historical_shares()
                        if asset_type == "stock"
                        else None
                    ),
                )
                # momentum_3d 不在指标全集里, 但 deviate_3d 需要; 多日帧上 shift 补算
                enriched_full = enriched_full.sort(["symbol", "date"]).with_columns(
                    (pl.col("close") / pl.col("close").shift(3).over("symbol") - 1).alias("momentum_3d")
                )
                enriched_today = enriched_full.filter(pl.col("date") == today)

            if enriched_today.is_empty():
                return

            # 异动偏离列: 盘中路径不经过 _refresh_enriched 冷刷新,
            # 需在此附着 (基准 = 历史帧 + 指数实时外推), 否则盘中异动列表为空
            if asset_type == "stock":
                from app.indicators.pipeline import attach_deviation_columns_today
                try:
                    index_quotes = self.get_index_quotes()
                except Exception:
                    index_quotes = None
                enriched_today = attach_deviation_columns_today(
                    enriched_today, self._repo.store.data_dir, index_quotes
                )

            # ---- 写盘 + 更新缓存 ----
            if merge:
                self._repo.merge_live_enriched_asset(asset_type, enriched_today)
            else:
                self._repo.flush_live_enriched_asset(asset_type, enriched_today)

            elapsed = time.perf_counter() - t0
            mode_label = "增量" if use_incremental else "全量"
            logger.info("enriched %s: %d 只, %s, 耗时 %.0fms",
                        mode_label, len(enriched_today), today, elapsed * 1000)
        except Exception as e:  # noqa: BLE001
            logger.warning("enriched 计算失败: %s", e)
