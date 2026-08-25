"""Optional D202 limit-up queue integration.

``limit_up_watcher`` connects to the user's local D202 service and keeps a
best-effort estimate of the queue at the limit-up price.  The adapter keeps
that optional connection isolated from TickFlow/QMT so a missing D202 service
never affects the normal limit-board workflow.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from app.market_time import CN_TZ

logger = logging.getLogger(__name__)

_DEFAULT_D202_URL = "ws://127.0.0.1:8080/d202"


def _watcher_url(value: str | None) -> str:
    """Use D202 by default while allowing a complete future endpoint override."""
    raw = str(value or _DEFAULT_D202_URL).strip()
    parts = urlsplit(raw)
    if not parts.scheme or not parts.netloc:
        return _DEFAULT_D202_URL
    path = parts.path or "/d202"
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def d202_code(symbol: str) -> str | None:
    """Convert the canonical ``600000.SH`` form to D202's ``SH600000``."""
    value = str(symbol or "").strip().upper()
    if value.endswith((".SH", ".SZ")):
        ticker, market = value.rsplit(".", 1)
        return f"{market}{ticker}"
    return None


def _epoch_ms_iso(value: object) -> str | None:
    try:
        milliseconds = int(value or 0)
    except (TypeError, ValueError):
        return None
    if milliseconds <= 0:
        return None
    return datetime.fromtimestamp(milliseconds / 1000, CN_TZ).isoformat()


def _side_snapshot(side: Any) -> dict[str, int]:
    return {
        "volume": int(getattr(side, "volume", 0) or 0),
        "count": int(getattr(side, "count", 0) or 0),
        "amount": int(getattr(side, "amount", 0) or 0),
        "last_reduction": int(getattr(side, "last_reduction", 0) or 0),
    }


class _Side:
    def __init__(self) -> None:
        self.volume = 0
        self.count = 0
        self.amount = 0
        self.last_reduction = 0


class _Entry:
    def __init__(self, hand_count: int) -> None:
        self.timestamp = 0
        self.hand_count = hand_count
        self.status = 1
        self.found_id = 0
        self.found_index = -1
        self.front = _Side()
        self.back = _Side()
        self.queue_elapsed_ms = 0


class _Aggregate:
    def __init__(self) -> None:
        self.count = 0
        self.volume = 0
        self.amount = 0


class _D202Watcher:
    """Small in-process implementation of the upstream D202 observer contract."""

    def __init__(self, code: str, price_li: int) -> None:
        self.code = code
        self.price_li = price_li
        self.price_yuan = price_li / 1000.0
        self._queue_volumes: list[int] = []
        self._queue_initialized = False
        self._queue_generation = 0
        self._next_order_id = 1
        self._timestamp = 0
        self._current = _Aggregate()
        self._first = _Aggregate()
        self._new_add = _Aggregate()
        self._cancelled = _Aggregate()
        self._executed = _Aggregate()
        self._my_orders: list[_Entry] = []
        self._callbacks: dict[str, list[Callable[..., Any]]] = {
            "tick": [], "snapshot": [], "gone": [], "fill": [],
        }
        self.limit_up_gone = False
        self.limit_up_may_gone = False
        self.inflow_streak = 0
        self.outflow_streak = 0
        self.net_change_amt = 0

    @property
    def current(self) -> _Aggregate:
        return self._copy_aggregate(self._current)

    @property
    def first(self) -> _Aggregate:
        return self._copy_aggregate(self._first)

    @property
    def new_add(self) -> _Aggregate:
        return self._copy_aggregate(self._new_add)

    @property
    def cancelled(self) -> _Aggregate:
        return self._copy_aggregate(self._cancelled)

    @property
    def executed(self) -> _Aggregate:
        return self._copy_aggregate(self._executed)

    @property
    def my_orders(self) -> list[_Entry]:
        return list(self._my_orders)

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @staticmethod
    def _copy_aggregate(value: _Aggregate) -> _Aggregate:
        result = _Aggregate()
        result.count, result.volume, result.amount = value.count, value.volume, value.amount
        return result

    def _amount(self, volume: int) -> int:
        return (self.price_li // 10 * volume) // 10000

    def _emit(self, key: str, *args: Any) -> None:
        for callback in self._callbacks[key]:
            try:
                callback(self, *args)
            except Exception:
                logger.debug("D202 watcher callback failed", exc_info=True)

    def on_tick(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        self._callbacks["tick"].append(callback)
        return callback

    def on_snapshot(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        self._callbacks["snapshot"].append(callback)
        return callback

    def on_limit_gone(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        self._callbacks["gone"].append(callback)
        return callback

    def on_fill(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        self._callbacks["fill"].append(callback)
        return callback

    def queue(self, hand_count: int) -> int:
        entry = _Entry(int(hand_count))
        entry.timestamp = self._timestamp
        entry.queue_generation = self._queue_generation
        self._my_orders.append(entry)
        return len(self._my_orders) - 1

    def feed_queue(self, data: dict[str, Any], timestamp: int) -> None:
        """Consume a D202 ``queue`` response for the first buy level.

        D202 exposes the ordered volume distribution but not exchange order
        IDs.  The queue position is therefore matched against a newly added
        volume with the same hand count, which is the closest estimate the
        protocol permits.
        """
        self._timestamp = int(timestamp or 0)
        volumes: list[int] = []
        for value in data.get("volumes") or []:
            try:
                volume = int(value or 0)
            except (TypeError, ValueError):
                continue
            if volume >= 0:
                volumes.append(volume)

        previous = self._queue_volumes
        had_snapshot = self._queue_initialized
        self._queue_volumes = volumes
        self._queue_initialized = True
        self._queue_generation += 1
        self._current.count = int(data.get("totalCount") or len(volumes))
        self._current.volume = sum(volumes)
        self._current.amount = self._amount(self._current.volume)
        if not had_snapshot:
            self._first = self._copy_aggregate(self._current)
            self._new_add = self._copy_aggregate(self._current)
            self._emit("snapshot")
            # A queue packet has no order IDs. On the first response, use
            # the current distribution as the best available match for an
            # already accepted order; later responses only match additions.
            added_indices = list(range(len(volumes)))
        else:
            added, removed = self._queue_delta(previous, volumes)
            self._new_add.count = len(added)
            self._new_add.volume = sum(added)
            self._cancelled.count = len(removed)
            self._cancelled.volume = sum(removed)
            added_indices = self._added_indices(previous, volumes)
        self._match_queue_orders(volumes, added_indices)
        self._finish()

    @staticmethod
    def _queue_delta(previous: list[int], current: list[int]) -> tuple[list[int], list[int]]:
        before = Counter(previous)
        after = Counter(current)
        added: list[int] = []
        removed: list[int] = []
        for volume, count in (after - before).items():
            added.extend([volume] * count)
        for volume, count in (before - after).items():
            removed.extend([volume] * count)
        return added, removed

    @staticmethod
    def _added_indices(previous: list[int], current: list[int]) -> list[int]:
        available = Counter(previous)
        indices: list[int] = []
        for index, volume in enumerate(current):
            if available[volume]:
                available[volume] -= 1
            else:
                indices.append(index)
        return indices

    def _match_queue_orders(self, volumes: list[int], added_indices: list[int]) -> None:
        available = set(added_indices)
        for entry in self._my_orders:
            if entry.status in {3, 100}:
                continue
            if entry.found_id == 0:
                index = next(
                    (index for index in sorted(available) if volumes[index] == entry.hand_count),
                    None,
                )
                if index is None:
                    continue
                available.remove(index)
                entry.found_id = self._next_order_id
                self._next_order_id += 1
                entry.found_index = index
                entry.status = 2
            else:
                index = self._locate_order(entry, volumes)
                if index is None:
                    entry.status = 100
                    continue
                entry.found_index = index
            self._update_order_position(entry, volumes)

    @staticmethod
    def _locate_order(entry: _Entry, volumes: list[int]) -> int | None:
        candidates = [
            index for index, volume in enumerate(volumes)
            if volume == entry.hand_count
        ]
        if candidates:
            return min(candidates, key=lambda index: abs(index - entry.found_index))
        if 0 <= entry.found_index < len(volumes) and volumes[entry.found_index] > 0:
            return entry.found_index
        return None

    def _update_order_position(self, entry: _Entry, volumes: list[int]) -> None:
        index = entry.found_index
        front_volume = sum(volumes[:index])
        back_volume = sum(volumes[index + 1:])
        previous_front = entry.front.volume
        entry.front.volume = front_volume
        entry.front.count = index
        entry.back.volume = back_volume
        entry.back.count = max(0, len(volumes) - index - 1)
        entry.front.last_reduction = max(0, previous_front - front_volume)

    def _finish(self) -> None:
        self._current.amount = self._amount(self._current.volume)
        self._new_add.amount = self._amount(self._new_add.volume)
        self._cancelled.amount = self._amount(self._cancelled.volume)
        self._executed.amount = self._amount(self._executed.volume)
        self.net_change_amt = self._new_add.amount - self._cancelled.amount
        if self._new_add.count:
            self.inflow_streak += 1
            self.outflow_streak = 0
        else:
            self.outflow_streak += 1
            self.inflow_streak = 0
        had_records = self._queue_initialized and self._current.count > 0
        self.limit_up_may_gone = had_records and self._cancelled.amount * 5 > self._current.amount
        self.limit_up_gone = self._queue_initialized and self._current.count == 0
        for entry in self._my_orders:
            if entry.found_id:
                entry.front.amount = self._amount(entry.front.volume)
                entry.back.amount = self._amount(entry.back.volume)
                entry.queue_elapsed_ms = max(0, self._timestamp - entry.timestamp)
                if entry.status == 100:
                    self._emit("fill", entry)
        self._emit("gone" if self.limit_up_gone else "tick")
        self._new_add, self._cancelled, self._executed = _Aggregate(), _Aggregate(), _Aggregate()


class _D202WebSocketSource:
    def __init__(self, url: str) -> None:
        self.url = url
        self._watchers: list[_D202Watcher] = []
        self._ws: Any | None = None

    def add_watcher(self, watcher: _D202Watcher) -> None:
        if watcher not in self._watchers:
            self._watchers.append(watcher)

    def connect(self, block: bool = False) -> None:
        import websocket  # type: ignore
        self._ws = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=lambda _ws, error: logger.warning("D202 WebSocket 错误: %s", error),
        )
        def runner() -> None:
            self._ws.run_forever(ping_interval=30, ping_timeout=10)
        if block:
            runner()
        else:
            threading.Thread(target=runner, name="limit-up-d202", daemon=True).start()

    def disconnect(self) -> None:
        if self._ws is not None:
            self._ws.close()

    def _on_open(self, ws: Any) -> None:
        ws.send(json.dumps([
            {
                "type": "queue",
                "code": watcher.code,
                "enable": 1,
                "dir": "B",
                "level": 0,
            }
            for watcher in self._watchers
        ]))

    def _on_message(self, _ws: Any, text: str) -> None:
        try:
            message = json.loads(text)
        except (TypeError, json.JSONDecodeError):
            return
        timestamp = int(message.get("ts") or 0)
        by_code = {
            str(getattr(watcher, "code", "") or ""): watcher
            for watcher in self._watchers
        }
        for item in message.get("list") or []:
            if item.get("type") != "queue":
                continue
            data = item.get("data") or {}
            watcher = by_code.get(str(data.get("code") or ""))
            if watcher is not None:
                watcher.feed_queue(data, timestamp)


def watcher_snapshot(watcher: Any) -> dict[str, Any]:
    """Return a JSON-safe projection of one upstream watcher."""
    current = watcher.current
    first = watcher.first
    new_add = watcher.new_add
    cancelled = watcher.cancelled
    executed = watcher.executed
    entries = list(watcher.my_orders or [])
    entry = entries[-1] if entries else None
    status = {
        1: "queueing_unmatched",
        2: "queueing",
        3: "cancelled",
        100: "filled_estimate",
    }.get(int(getattr(entry, "status", 0) or 0), "watching") if entry else "watching"
    return {
        "state": "live",
        "code": str(getattr(watcher, "code", "") or ""),
        "price": float(getattr(watcher, "price_yuan", 0.0) or 0.0) or None,
        "as_of": _epoch_ms_iso(getattr(watcher, "timestamp", 0)),
        "first": {
            "count": int(getattr(first, "count", 0) or 0),
            "volume": int(getattr(first, "volume", 0) or 0),
            "amount": int(getattr(first, "amount", 0) or 0),
        },
        "current": {
            "count": int(getattr(current, "count", 0) or 0),
            "volume": int(getattr(current, "volume", 0) or 0),
            "amount": int(getattr(current, "amount", 0) or 0),
        },
        "new_add": {
            "count": int(getattr(new_add, "count", 0) or 0),
            "volume": int(getattr(new_add, "volume", 0) or 0),
            "amount": int(getattr(new_add, "amount", 0) or 0),
        },
        "cancelled": {
            "count": int(getattr(cancelled, "count", 0) or 0),
            "volume": int(getattr(cancelled, "volume", 0) or 0),
            "amount": int(getattr(cancelled, "amount", 0) or 0),
        },
        "executed": {
            "count": int(getattr(executed, "count", 0) or 0),
            "volume": int(getattr(executed, "volume", 0) or 0),
            "amount": int(getattr(executed, "amount", 0) or 0),
        },
        "net_change_amount": int(getattr(watcher, "net_change_amt", 0) or 0),
        "inflow_streak": int(getattr(watcher, "inflow_streak", 0) or 0),
        "outflow_streak": int(getattr(watcher, "outflow_streak", 0) or 0),
        "limit_up_gone": bool(getattr(watcher, "limit_up_gone", False)),
        "limit_up_may_gone": bool(getattr(watcher, "limit_up_may_gone", False)),
        "order_status": status,
        "order": {
            "hand_count": int(getattr(entry, "hand_count", 0) or 0),
            "front": _side_snapshot(getattr(entry, "front", None)),
            "back": _side_snapshot(getattr(entry, "back", None)),
            "elapsed_ms": int(getattr(entry, "queue_elapsed_ms", 0) or 0),
        } if entry else None,
    }


class LimitUpQueueService:
    """Own the optional D202 WebSocket and expose immutable snapshots."""

    def __init__(
        self,
        on_update: Callable[[], None] | None = None,
        url: str | None = None,
        watcher_factory: Callable[..., Any] | None = None,
        source_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.url = _watcher_url(url or os.getenv("LIMIT_UP_WATCHER_URL"))
        self._on_update = on_update
        self._watcher_factory = watcher_factory
        self._source_factory = source_factory
        self._lock = threading.RLock()
        self._source: Any | None = None
        self._watchers: dict[str, Any] = {}
        self._queue_keys: dict[str, str] = {}
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._specs: dict[str, dict[str, Any]] = {}
        self._started = False
        self._state = "idle"
        self._last_error: str | None = None

    def start(self) -> None:
        with self._lock:
            self._started = True
            if self._watcher_factory is None or self._source_factory is None:
                # The upstream package targets a different price-packet
                # protocol. D202 exposes the queue distribution through its
                # own queue packet, so this adapter owns that boundary.
                self._watcher_factory = _D202Watcher
                self._source_factory = _D202WebSocketSource
            self._state = "idle"
            self._last_error = None

    def stop(self) -> None:
        with self._lock:
            source = self._source
            self._source = None
            self._watchers.clear()
            self._queue_keys.clear()
            self._specs.clear()
            self._started = False
            self._state = "idle"
        if source is not None:
            try:
                source.disconnect()
            except Exception:
                logger.debug("D202 排队监听断开失败", exc_info=True)

    def sync(self, specs: dict[str, dict[str, Any]]) -> None:
        """Subscribe to the current board pool and register accepted orders."""
        with self._lock:
            if not self._started:
                return
            normalized = {
                str(symbol).strip().upper(): dict(spec)
                for symbol, spec in specs.items()
                if d202_code(symbol) and float(spec.get("limit_up") or 0) > 0
            }
            signature = {
                symbol: (
                    d202_code(symbol),
                    round(float(spec.get("limit_up") or 0), 3),
                )
                for symbol, spec in normalized.items()
            }
            current_signature = {
                symbol: (
                    d202_code(symbol),
                    round(float(spec.get("limit_up") or 0), 3),
                )
                for symbol, spec in self._specs.items()
            }
            self._specs = normalized
            self._snapshots = {
                symbol: snapshot
                for symbol, snapshot in self._snapshots.items()
                if symbol in normalized
            }
            if signature != current_signature:
                self._restart_locked()
            else:
                self._sync_orders_locked()

    def snapshot(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._snapshots.get(str(symbol).strip().upper())
            return dict(value) if value else None

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self._state,
                "url": self.url,
                "symbols": len(self._watchers),
                "last_error": self._last_error,
            }

    def _restart_locked(self) -> None:
        old_source = self._source
        self._source = None
        self._watchers = {}
        self._queue_keys = {}
        if old_source is not None:
            try:
                old_source.disconnect()
            except Exception:
                logger.debug("D202 排队监听重连断开失败", exc_info=True)
        if not self._specs:
            self._state = "idle"
            self._last_error = None
            return
        try:
            source = self._source_factory(self.url)
            for symbol, spec in self._specs.items():
                code = d202_code(symbol)
                price = float(spec["limit_up"])
                watcher = self._watcher_factory(code, price_li=round(price * 1000))
                watcher.on_tick(self._handle_tick)
                watcher.on_snapshot(self._handle_tick)
                watcher.on_limit_gone(self._handle_tick)
                watcher.on_fill(self._handle_tick)
                queue_key = str(spec.get("queue_key") or "")
                queue_volume = int(spec.get("queue_volume") or 0)
                if queue_key and queue_volume >= 100 and queue_volume % 100 == 0:
                    watcher.queue(queue_volume // 100)
                    self._queue_keys[symbol] = queue_key
                source.add_watcher(watcher)
                self._watchers[symbol] = watcher
            self._source = source
            self._state = "connecting"
            self._last_error = None
            source.connect(block=False)
        except Exception as exc:
            self._state = "unavailable"
            self._last_error = f"D202 排队监听启动失败: {exc}"
            logger.warning("D202 排队监听启动失败: %s", exc)

    def _sync_orders_locked(self) -> None:
        """Register newly accepted orders without resetting D202 baselines."""
        for symbol, spec in self._specs.items():
            queue_key = str(spec.get("queue_key") or "")
            queue_volume = int(spec.get("queue_volume") or 0)
            watcher = self._watchers.get(symbol)
            if (
                watcher is not None
                and queue_key
                and queue_volume >= 100
                and queue_volume % 100 == 0
                and self._queue_keys.get(symbol) != queue_key
            ):
                watcher.queue(queue_volume // 100)
                self._queue_keys[symbol] = queue_key

    def _handle_tick(self, watcher: Any, *_args: Any) -> None:
        symbol = str(getattr(watcher, "code", "") or "").strip().upper()
        with self._lock:
            canonical = next(
                (item for item in self._watchers if d202_code(item) == symbol),
                None,
            )
            if canonical is None:
                return
            self._snapshots[canonical] = watcher_snapshot(watcher)
            self._state = "connected"
        if self._on_update is not None:
            try:
                self._on_update()
            except Exception:
                logger.debug("D202 排队状态刷新通知失败", exc_info=True)
