"""QMT ZMQ RPC 交易网关。

浏览器永远只访问本地 API；本模块是主项目与 QMT 之间的唯一边界。
连接地址和账户从环境变量读取，当前连接模式可在运行时切换。
"""
from __future__ import annotations

import base64
import json
import math
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
try:
    import zmq
except ImportError:  # pragma: no cover - dependency is installed with the backend
    zmq = None


class QmtRpcError(RuntimeError):
    pass


class QmtOrderPreflightError(QmtRpcError):
    """The order was rejected before a submit request was sent to QMT."""


def _is_uncertain_passorder_error(error: BaseException) -> bool:
    """识别桥接层已经调用 passorder、但暂未回查到委托号的结果。"""
    message = str(error).casefold()
    return "passorder" in message and "order not found" in message


_CN_TZ = ZoneInfo("Asia/Shanghai")
_SAFE_B64_PREFIX = "b64s:"
_SAFE_B64_DIGIT_ENCODE = str.maketrans("0123456789", "!#$%&()*~?")
_SAFE_B64_DIGIT_DECODE = str.maketrans("!#$%&()*~?", "0123456789")
_BROKER_ORDER_TIME_FIELDS = (
    "broker_order_at",
    "order_time",
    "entrust_time",
    "order_datetime",
    "entrust_datetime",
    "order_timestamp",
)
_QMT_CANCELABLE_ORDER_STATUSES = frozenset({"48", "49", "50", "55"})
_QMT_CANCEL_PENDING_ORDER_STATUSES = frozenset({"51", "52"})
_QMT_TERMINAL_ORDER_STATUS_LABELS = {
    "53": "部撤",
    "54": "已撤",
    "56": "已成",
    "57": "废单",
    "rejected": "已拒绝",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _smallest(*values: float | None) -> float | None:
    """The smallest of the given amounts, ignoring the unknown ones."""
    known = [value for value in values if value is not None]
    return min(known) if known else None


def _utc_iso(seconds_ago: float = 0.0) -> str:
    """ISO timestamp for ``seconds_ago`` in the past, matching ``_now()``."""
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


def _age_seconds(stamp: str | None) -> float:
    """Seconds since an ISO timestamp, or ``inf`` when it cannot be read."""
    if not stamp:
        return float("inf")
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return float("inf")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def _encode_zmq_payload(value: dict[str, Any]) -> bytes:
    raw = json.dumps(value, ensure_ascii=False).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii").translate(_SAFE_B64_DIGIT_ENCODE)
    return (_SAFE_B64_PREFIX + encoded).encode("utf-8")


def _decode_zmq_payload(value: bytes | str) -> Any:
    text = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)
    if text.startswith(_SAFE_B64_PREFIX):
        encoded = text[len(_SAFE_B64_PREFIX):].translate(_SAFE_B64_DIGIT_DECODE)
        text = base64.b64decode(encoded.encode("ascii")).decode("utf-8")
    return json.loads(text)


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    # Big QMT uses the largest representable float as an unset-field sentinel.
    return result if math.isfinite(result) and abs(result) < 1e307 else None


# QMT CREDIT account rows expose buying power separately from ordinary cash.
# Keep the vendor field names at this boundary so cash is never mistaken for
# financing buying power when a credit account is configured.
_CREDIT_ASSURE_BUYING_POWER_FIELDS = (
    "m_dAssureEnbuyBalance",
    "assure_enbuy_balance",
    "credit_assure_buying_power",
)
_CREDIT_FINANCING_BUYING_POWER_FIELDS = (
    "m_dFinEnbuyBalance",
    "fin_enbuy_balance",
)
_CREDIT_FINANCING_AVAILABLE_FIELDS = (
    "m_dFinEnableBalance",
    "fin_enable_balance",
    "financing_available_amount",
)
_CREDIT_FINANCING_QUOTA_FIELDS = (
    "m_dFinEnableQuota",
    "fin_enable_quota",
)
_CREDIT_ASSET_ALIASES = {
    "assure_enbuy_balance": _CREDIT_ASSURE_BUYING_POWER_FIELDS,
    "fin_enbuy_balance": _CREDIT_FINANCING_BUYING_POWER_FIELDS,
    "fin_enable_balance": _CREDIT_FINANCING_AVAILABLE_FIELDS,
    "fin_enable_quota": _CREDIT_FINANCING_QUOTA_FIELDS,
}
_CREDIT_ASSET_FIELDS = frozenset(
    (*_CREDIT_ASSURE_BUYING_POWER_FIELDS,
     *_CREDIT_FINANCING_BUYING_POWER_FIELDS,
     *_CREDIT_FINANCING_AVAILABLE_FIELDS,
     *_CREDIT_FINANCING_QUOTA_FIELDS),
)
_CREDIT_BUY_MODES = frozenset({"collateral", "financing"})
_CREDIT_SUBJECT_CACHE_SECONDS = 180.0
_CREDIT_OPVOLUME_BACKGROUND_TIMEOUT_SECONDS = 8.0
_CREDIT_SUBJECT_ERROR_CACHE_SECONDS = 5.0
# Budget for one background poll. The broker's own kick-off query is slower
# than this, so only the cheap status polls use it.
_CREDIT_BACKGROUND_RPC_TIMEOUT_SECONDS = 3.0
# A failed query is retried after this long instead of poisoning the symbol
# for the rest of the session.
_CREDIT_OPVOLUME_ERROR_CACHE_SECONDS = 30.0
# How long a stored per-symbol limit may be shown as-is. The broker query costs
# ~1.7s, so the limit is computed once, kept here and refreshed in the
# background rather than recomputed on every preview.
_CREDIT_SYMBOL_LIMIT_TTL_SECONDS = 60.0
# The stored limit belongs to the price it was computed at; re-ask when the
# quote drifts further than this share of it.
_CREDIT_SYMBOL_LIMIT_PRICE_TOLERANCE = 0.005
# The broker's financing subject list changes rarely, so it is persisted and
# re-read on this cadence instead of per session.
_CREDIT_SUBJECT_LIST_SYNC_SECONDS = 7 * 24 * 3600.0
# Background renewal cadence and lead time. Renewal keeps an active symbol
# fresh so a click does not land on an expiring entry.
_CREDIT_SYMBOL_RENEW_INTERVAL_SECONDS = 15.0
_CREDIT_SYMBOL_RENEW_LEAD_SECONDS = 15.0
# A symbol stops being renewed this long after its last preview.
_CREDIT_SYMBOL_ACTIVE_SECONDS = 1800.0
# Account refresh cadence. ``get_asset`` is slow, so it runs on the background
# socket often enough to stay inside ``auto_sync_interval``.
_ACCOUNT_CACHE_REFRESH_SECONDS = 10.0

_CREDIT_RAW_FIELD_TARGETS = {
    "m_dFinEnbuyBalance": ("fin_enbuy_balance",),
    "m_dFinEnableBalance": ("fin_enable_balance", "financing_available_amount"),
    "m_dFinEnableQuota": ("fin_enable_quota",),
}


def _first_number(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    for field in fields:
        value = _float(row.get(field))
        if value is not None and value >= 0 and value < 1e307:
            return value
    return None


def _credit_buying_power(
    account: dict[str, Any], credit_buy_mode: str = "collateral",
) -> tuple[float | None, str | None, float | None]:
    """Return the selected buying power, label and actual financing amount."""
    if credit_buy_mode not in _CREDIT_BUY_MODES:
        raise ValueError("信用账户买入方式必须是担保品买入或融资买入")
    financing_available = _first_number(account, _CREDIT_FINANCING_AVAILABLE_FIELDS)
    if credit_buy_mode == "financing":
        financing_buying_power = _first_number(account, _CREDIT_FINANCING_BUYING_POWER_FIELDS)
        return (
            financing_buying_power
            if financing_buying_power is not None
            else financing_available,
            "可买融资标的资金",
            financing_available,
        )
    return (
        _first_number(account, _CREDIT_ASSURE_BUYING_POWER_FIELDS),
        "可买担保品资金",
        financing_available,
    )


def _fallback_credit_buy_mode(mode: str) -> str:
    return "collateral" if mode == "financing" else "financing"


def _normalise_account(asset: dict[str, Any], account_type: str | None = None) -> dict[str, Any]:
    account = {
        "cash": _float(asset.get("cash")),
        "total_asset": _float(asset.get("total_asset")),
        "market_value": _float(asset.get("market_value")),
    }
    for field in _CREDIT_ASSET_FIELDS:
        value = _float(asset.get(field))
        if value is not None and value >= 0:
            account[field] = value
    for canonical, aliases in _CREDIT_ASSET_ALIASES.items():
        value = _first_number(asset, aliases)
        if value is not None:
            account[canonical] = value
    account["account_type"] = str(account_type or asset.get("account_type") or "STOCK").upper()
    # Some Big QMT account rows expose financing buying power as an unset
    # sentinel while still reporting the usable financing amount. Preserve
    # that positive amount; a financing quota is not buying power.
    if account["account_type"] == "CREDIT" and _first_number(account, _CREDIT_FINANCING_BUYING_POWER_FIELDS) is None:
        financing_available = _first_number(account, _CREDIT_FINANCING_AVAILABLE_FIELDS)
        if financing_available is not None:
            account["fin_enbuy_balance"] = financing_available
    return account


_ALLOCATION_RATIOS = {
    "sixth": 1 / 6,
    "fifth": 0.2,
    "quarter": 0.25,
    "third": 1 / 3,
    "half": 0.5,
    "available": 1.0,
}
# 一手模式 (lot) 已废弃: 打板池不再提供该选项, 零金额也不再代表一手。
_ALLOCATION_MODES = frozenset((*_ALLOCATION_RATIOS, "fixed"))


def _parse_broker_time(value: Any, anchor: str | None) -> str | None:
    anchor_value = datetime.now(_CN_TZ)
    if anchor:
        try:
            parsed_anchor = datetime.fromisoformat(str(anchor).replace("Z", "+00:00"))
        except ValueError:
            pass
        else:
            if parsed_anchor.tzinfo is None:
                parsed_anchor = parsed_anchor.replace(tzinfo=_CN_TZ)
            anchor_value = parsed_anchor.astimezone(_CN_TZ)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number >= 1_000_000_000_000:
            parsed = datetime.fromtimestamp(number / 1000, timezone.utc)
        elif number >= 1_000_000_000:
            parsed = datetime.fromtimestamp(number, timezone.utc)
        else:
            value = str(int(number)).zfill(6)
            parsed = None
    else:
        parsed = None
    if parsed is None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None:
            formats = (
                "%Y%m%d%H%M%S.%f",
                "%Y%m%d%H%M%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%H:%M:%S.%f",
                "%H:%M:%S",
                "%H%M%S%f",
                "%H%M%S",
            )
            for fmt in formats:
                try:
                    candidate = datetime.strptime(text, fmt)
                except ValueError:
                    continue
                if fmt.startswith("%H"):
                    candidate = candidate.replace(
                        year=anchor_value.year,
                        month=anchor_value.month,
                        day=anchor_value.day,
                    )
                parsed = candidate
                break
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_CN_TZ)
    return parsed.isoformat(timespec="milliseconds")


def _broker_time(order: dict[str, Any], anchor: str | None) -> tuple[str | None, Any, str | None]:
    for field in _BROKER_ORDER_TIME_FIELDS:
        raw = order.get(field)
        if raw in (None, ""):
            continue
        raw_value = raw if isinstance(raw, (str, int, float, bool)) else str(raw)
        return _parse_broker_time(raw, anchor), raw_value, field
    return None, None, None


class QmtZmqRpcClient:
    """调用 bigqmt_signal_trader 的 ZMQ ROUTER/DEALER RPC 协议。"""

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.enabled = bool(getattr(settings, "qmt_enabled", False))
        self.connection_mode = self._normalise_connection_mode(
            getattr(settings, "qmt_connection_mode", "remote"),
        )
        self.remote_connect_address = str(
            getattr(settings, "qmt_zmq_connect_address", "") or "",
        ).strip()
        self.local_connect_address = str(
            getattr(settings, "qmt_local_zmq_connect_address", "tcp://127.0.0.1:15648")
            or "",
        ).strip()
        self.connect_address = self._address_for_mode(self.connection_mode)
        self.account_id = str(getattr(settings, "qmt_account_id", "") or "").strip()
        self.account_type = str(getattr(settings, "qmt_account_type", "STOCK") or "STOCK").upper()
        self.timeout = max(1.0, float(getattr(settings, "qmt_rpc_timeout_seconds", 6.0)))
        self._context = None
        self._dealer = None
        self._background_dealer = None
        self._lock = threading.Lock()
        # Background credit queries use their own socket and lock. Sharing the
        # interactive one made a slow broker reply hold ``self._lock`` for the
        # whole round trip, which stalled order previews by 5-13 seconds.
        self._background_lock = threading.Lock()
        self._credit_state_lock = threading.Lock()
        self._retired = False
        self._credit_opvolume_cache: dict[tuple[str, float, str], dict[str, Any]] = {}
        self._credit_opvolume_futures: dict[tuple[str, float, str], Any] = {}
        self._credit_opvolume_error_until: dict[tuple[str, float, str], float] = {}
        self._credit_executor: ThreadPoolExecutor | None = None
        self._credit_subjects_cache: list[dict[str, Any]] | None = None
        self._credit_subjects_cache_at = 0.0
        self._credit_subject_future: Any | None = None
        self._credit_subject_error_until = 0.0

    @staticmethod
    def _normalise_connection_mode(value: Any) -> str:
        return "local" if str(value or "remote").strip().lower() == "local" else "remote"

    def _address_for_mode(self, mode: str) -> str:
        return self.local_connect_address if mode == "local" else self.remote_connect_address

    @property
    def configured(self) -> bool:
        return bool(
            not self._retired
            and self.enabled
            and self.connect_address
            and self.account_id
            and zmq is not None
        )

    @property
    def configuration_reason(self) -> str:
        if self._retired:
            return "QMT 连接已切换"
        if not self.enabled:
            return "QMT_ENABLED 未开启"
        missing = []
        if not self.connect_address:
            missing.append(
                "QMT_LOCAL_ZMQ_CONNECT_ADDRESS"
                if self.connection_mode == "local"
                else "QMT_ZMQ_CONNECT_ADDRESS",
            )
        if not self.account_id:
            missing.append("QMT_ACCOUNT_ID")
        if missing:
            return "缺少 " + ", ".join(missing)
        return "后端未安装 pyzmq 客户端" if zmq is None else "已配置"

    @property
    def remote_configured(self) -> bool:
        return bool(self.enabled and self.remote_connect_address and self.account_id and zmq is not None)

    @property
    def local_configured(self) -> bool:
        return bool(self.enabled and self.local_connect_address and self.account_id and zmq is not None)

    def _ensure_dealer(self):
        if not self.configured:
            raise QmtRpcError(self.configuration_reason)
        with self._lock:
            return self._ensure_dealer_locked()

    def _ensure_dealer_locked(self):
        if self._dealer is None:
            self._dealer = self._create_dealer()
        return self._dealer

    def _ensure_background_dealer_locked(self):
        if self._background_dealer is None:
            self._background_dealer = self._create_dealer()
        return self._background_dealer

    def _create_dealer(self):
        if zmq is None:  # pragma: no cover - guarded by configured
            raise QmtRpcError("后端未安装 pyzmq 客户端")
        if self._context is None:
            self._context = zmq.Context.instance()
        dealer = self._context.socket(zmq.DEALER)
        dealer.setsockopt(zmq.IDENTITY, uuid.uuid4().hex[:16].encode("ascii"))
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.connect(self.connect_address)
        return dealer

    def _close_dealer_locked(self) -> None:
        try:
            if self._dealer is not None:
                self._dealer.close(linger=0)
        finally:
            self._dealer = None

    def _close_background_dealer_locked(self) -> None:
        try:
            if self._background_dealer is not None:
                self._background_dealer.close(linger=0)
        finally:
            self._background_dealer = None

    def _credit_executor_locked(self) -> ThreadPoolExecutor:
        if self._credit_executor is None:
            self._credit_executor = ThreadPoolExecutor(
                max_workers=2,
                thread_name_prefix="qmt-credit-query",
            )
        return self._credit_executor

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        request = self._build_request(method, params)
        with self._lock:
            dealer = self._ensure_dealer_locked()
            try:
                response = self._roundtrip(dealer, request, self.timeout)
            except QmtRpcError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._close_dealer_locked()
                raise QmtRpcError(f"QMT ZMQ RPC 连接失败: {exc.__class__.__name__}") from exc
            if response is None:
                self._close_dealer_locked()
                raise QmtRpcError(f"QMT RPC 超时: {method}")
        return self._unwrap_response(method, response)

    def call_background(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> Any:
        """Send an RPC over the dedicated background socket.

        Credit contract and max-volume queries are slow and chatty. Keeping
        them off the interactive socket stops a hung broker reply from holding
        ``self._lock`` while an order preview waits for its turn.
        """
        # A replaced ``call`` is a test double or an older bridge adapter; keep
        # routing through it so existing behaviour is unchanged.
        if getattr(self.call, "__func__", None) is not QmtZmqRpcClient.call:
            return self.call(method, params)
        request = self._build_request(method, params)
        with self._background_lock:
            dealer = self._ensure_background_dealer_locked()
            try:
                response = self._roundtrip(
                    dealer, request, timeout_seconds or min(self.timeout, 8.0),
                )
            except QmtRpcError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._close_background_dealer_locked()
                raise QmtRpcError(f"QMT ZMQ RPC 连接失败: {exc.__class__.__name__}") from exc
            if response is None:
                self._close_background_dealer_locked()
                raise QmtRpcError(f"QMT RPC 超时: {method}")
        return self._unwrap_response(method, response)

    def _build_request(self, method: str, params: dict[str, Any] | None) -> dict[str, Any]:
        if not self.configured:
            raise QmtRpcError(self.configuration_reason)
        return {
            "schema_version": 1,
            "request_id": uuid.uuid4().hex,
            "account_id": self.account_id,
            "method": method,
            "params": params or {},
            "ttl_seconds": max(60, int(self.timeout) + 30),
        }

    def _roundtrip(
        self,
        dealer: Any,
        request: dict[str, Any],
        timeout_seconds: float,
    ) -> dict[str, Any] | None:
        """Send one request and return its reply, or ``None`` on timeout."""
        request_id = str(request["request_id"])
        dealer.send(_encode_zmq_payload(request))
        poller = zmq.Poller()
        poller.register(dealer, zmq.POLLIN)
        deadline = time.monotonic() + max(0.1, float(timeout_seconds))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            events = dict(poller.poll(timeout=max(1, math.ceil(remaining * 1000))))
            if dealer not in events:
                continue
            candidate = _decode_zmq_payload(dealer.recv())
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("request_id") or "") != request_id:
                continue
            return candidate

    def _unwrap_response(self, method: str, response: dict[str, Any]) -> Any:
        if not response.get("ok"):
            error = response.get("error") or f"QMT RPC {method} 失败"
            raise QmtRpcError(str(error))
        server_error = str(response.get("server_error") or "").strip()
        if server_error:
            raise QmtRpcError(f"QMT {method} server_error: {server_error}")
        return response.get("data")

    def close(self) -> None:
        executor = None
        with self._lock:
            self._retired = True
            self._close_dealer_locked()
        # A background credit or asset query can still be mid-flight for
        # several seconds. Shutdown must not wait for it; ``_retired`` already
        # makes every later call fail fast.
        if self._background_lock.acquire(timeout=1.0):
            try:
                self._close_background_dealer_locked()
            finally:
                self._background_lock.release()
        with self._credit_state_lock:
            executor = self._credit_executor
            self._credit_executor = None
            self._credit_opvolume_futures.clear()
            self._credit_subject_future = None
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def probe(self) -> dict[str, Any]:
        data = self.call("ping")
        if isinstance(data, dict) and data.get("account_id") and str(data["account_id"]) != self.account_id:
            raise QmtRpcError("QMT RPC 返回的账户与本地配置不一致")
        return {"server_time": data.get("server_time") if isinstance(data, dict) else None}

    def get_asset(self, background: bool = False) -> dict[str, Any]:
        """Read assets and disambiguate credit-account financing fields.

        Older bridge versions returned the quota as ``fin_enable_balance``.
        The raw ACCOUNT row is authoritative when available, so use it to
        remove that ambiguous value and preserve the actual financing fields.

        This query costs 3-4s on the broker side, so the periodic cache
        refresh uses ``background=True`` to keep the interactive socket free.
        """
        send = self.call_background if background else self.call
        asset = send("get_asset", {"account_id": self.account_id})
        if not isinstance(asset, dict):
            raise QmtRpcError("QMT 账户资产响应格式无效")
        if self.account_type != "CREDIT":
            return asset
        if any(field in asset for field in _CREDIT_RAW_FIELD_TARGETS):
            return asset

        try:
            rows = send("query_account_infos", {"account_id": self.account_id})
        except Exception:  # noqa: BLE001 - older bridges may not expose this query
            return asset
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return asset

        account_info = rows[0]
        merged = dict(asset)
        for raw_field, targets in _CREDIT_RAW_FIELD_TARGETS.items():
            if raw_field not in account_info:
                continue
            value = _float(account_info.get(raw_field))
            for target in targets:
                merged.pop(target, None)
            if value is not None:
                merged[targets[0]] = value
        return merged

    def _fetch_credit_opvolume(
        self,
        key: tuple[str, float, str],
        symbol: str,
        price: float,
        mode: str,
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        op_type = 27 if mode == "financing" else 33
        query_payload = {
            "account_id": self.account_id,
            "stock_code": symbol,
            "op_type": op_type,
            "price_type": 11,
            "price": float(price),
        }
        # The broker answers this query only after it has kicked off the
        # calculation, which measured 1.5-2.0s. Give it the full background
        # budget instead of the shorter poll timeout.
        query = self.call_background("query_credit_opvolume", query_payload)
        if not isinstance(query, dict):
            result = {"status": "unavailable", "stock_code": symbol, "max_volume": None, "max_amount": None}
            with self._credit_state_lock:
                self._cache_credit_opvolume_locked(key, result)
            return result
        if query.get("status") in {"error", "unavailable"}:
            with self._credit_state_lock:
                self._cache_credit_opvolume_locked(key, query)
            return query
        deadline = time.monotonic() + max(0.5, float(timeout_seconds or min(self.timeout, 4.0)))
        latest = {"status": "pending", "stock_code": symbol, "max_volume": None, "max_amount": None}
        while time.monotonic() < deadline:
            latest = self.call_background(
                "get_credit_opvolume",
                query_payload,
                _CREDIT_BACKGROUND_RPC_TIMEOUT_SECONDS,
            )
            if isinstance(latest, dict) and latest.get("status") in {"ready", "error", "unavailable"}:
                with self._credit_state_lock:
                    self._cache_credit_opvolume_locked(key, latest)
                return latest
            time.sleep(0.2)
        return latest if isinstance(latest, dict) else {"status": "pending", "stock_code": symbol, "max_volume": None, "max_amount": None}

    def _cache_credit_opvolume_locked(
        self,
        key: tuple[str, float, str],
        result: dict[str, Any],
    ) -> None:
        """Store a result and let a transient error expire on its own."""
        self._credit_opvolume_cache[key] = dict(result)
        if result.get("status") == "error":
            self._credit_opvolume_error_until[key] = (
                time.monotonic() + _CREDIT_OPVOLUME_ERROR_CACHE_SECONDS
            )
        else:
            self._credit_opvolume_error_until.pop(key, None)

    def _cached_credit_opvolume_locked(
        self,
        key: tuple[str, float, str],
    ) -> dict[str, Any] | None:
        """Return a cached result, dropping an expired error entry."""
        cached = self._credit_opvolume_cache.get(key)
        if cached is None:
            return None
        if cached.get("status") == "error" and time.monotonic() >= self._credit_opvolume_error_until.get(key, 0.0):
            self._credit_opvolume_cache.pop(key, None)
            self._credit_opvolume_error_until.pop(key, None)
            return None
        return cached

    def _resolve_credit_opvolume_background(
        self,
        key: tuple[str, float, str],
        symbol: str,
        price: float,
        mode: str,
    ) -> None:
        try:
            result = self._fetch_credit_opvolume(
                key,
                symbol,
                price,
                mode,
                _CREDIT_OPVOLUME_BACKGROUND_TIMEOUT_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - expose a stable status to the next poll
            result = {
                "status": "error",
                "stock_code": symbol,
                "max_volume": None,
                "max_amount": None,
                "reason": str(exc),
            }
        with self._credit_state_lock:
            self._cache_credit_opvolume_locked(key, result)
            self._credit_opvolume_futures.pop(key, None)

    def _schedule_credit_opvolume_locked(
        self,
        key: tuple[str, float, str],
        symbol: str,
        price: float,
        mode: str,
    ) -> None:
        """Start one background broker query. Caller holds the state lock."""
        self._credit_opvolume_cache[key] = {
            "status": "pending",
            "stock_code": symbol,
            "max_volume": None,
            "max_amount": None,
        }
        self._credit_opvolume_futures[key] = self._credit_executor_locked().submit(
            self._resolve_credit_opvolume_background,
            key,
            symbol,
            float(price),
            mode,
        )

    def get_credit_opvolume(
        self,
        stock_code: str,
        price: float,
        credit_buy_mode: str = "financing",
        timeout_seconds: float | None = None,
        background: bool = False,
    ) -> dict[str, Any]:
        """Return QMT's broker-calculated max volume for one credit buy.

        ``query_credit_opvolume`` is asynchronous in Big QMT. Preview requests
        use ``background=True`` so the first HTTP response only schedules the
        broker query and returns ``pending``; later polls read the in-memory
        result. Final order validation keeps the synchronous path.
        """
        symbol = str(stock_code or "").strip().upper()
        if not symbol or price <= 0:
            return {"status": "unavailable", "stock_code": symbol, "max_volume": None, "max_amount": None}
        mode = str(credit_buy_mode or "financing").strip().lower()
        key = (symbol, round(float(price), 6), mode)
        # A replaced ``call`` method is typically a test double or an older
        # bridge adapter. Keep that compatibility path synchronous instead of
        # returning pending for a response that is already available.
        if background and getattr(self.call, "__func__", None) is not QmtZmqRpcClient.call:
            background = False
        with self._credit_state_lock:
            cached = self._cached_credit_opvolume_locked(key)
            future = self._credit_opvolume_futures.get(key)
            if cached is not None and cached.get("status") in {"ready", "error", "unavailable"}:
                return dict(cached)
            if background:
                if future is None:
                    self._schedule_credit_opvolume_locked(key, symbol, price, mode)
                return dict(self._credit_opvolume_cache[key])
        if future is not None:
            try:
                result = future.result(timeout=max(self.timeout, 8.0))
            except FuturesTimeout:
                return {"status": "pending", "stock_code": symbol, "max_volume": None, "max_amount": None}
            except Exception as exc:  # noqa: BLE001 - preserve the preview contract
                return {"status": "error", "stock_code": symbol, "max_volume": None, "max_amount": None, "reason": str(exc)}
            return dict(result) if isinstance(result, dict) else {"status": "pending", "stock_code": symbol, "max_volume": None, "max_amount": None}
        return self._fetch_credit_opvolume(key, symbol, float(price), mode, timeout_seconds)

    def _fetch_credit_subjects(self) -> list[dict[str, Any]]:
        rows = self.call_background("query_credit_subjects", {"account_id": self.account_id})
        if not isinstance(rows, list) or not rows:
            raise QmtRpcError("QMT 融资标的列表为空")
        subjects = [dict(row) for row in rows if isinstance(row, dict)]
        if not subjects:
            raise QmtRpcError("QMT 融资标的列表为空")
        with self._credit_state_lock:
            self._credit_subjects_cache = subjects
            self._credit_subjects_cache_at = time.monotonic()
            self._credit_subject_error_until = 0.0
        return subjects

    def _resolve_credit_subjects_background(self) -> None:
        try:
            self._fetch_credit_subjects()
        except Exception:
            with self._credit_state_lock:
                self._credit_subject_error_until = time.monotonic() + _CREDIT_SUBJECT_ERROR_CACHE_SECONDS
        finally:
            with self._credit_state_lock:
                self._credit_subject_future = None

    def get_credit_subject(self, stock_code: str, background: bool = False) -> dict[str, Any]:
        """Return whether one security is currently a financing subject.

        QMT exposes this as the account's full ``get_assure_contract`` list;
        cache it briefly because the list is large and changes much less often
        than a preview dialog is opened.  An unavailable bridge is kept
        distinct from an empty match so callers can retain their compatibility
        fallback when the newer query is not supported.
        """
        symbol = str(stock_code or "").strip().upper()
        if not symbol:
            return {"status": "unavailable", "stock_code": symbol, "eligible": None}
        now = time.monotonic()
        if background and getattr(self.call, "__func__", None) is not QmtZmqRpcClient.call:
            background = False
        with self._credit_state_lock:
            subjects = self._credit_subjects_cache
            cache_age = now - self._credit_subjects_cache_at
            error_until = self._credit_subject_error_until
        if subjects is None or cache_age > _CREDIT_SUBJECT_CACHE_SECONDS:
            if background:
                if error_until > now:
                    return {"status": "unavailable", "stock_code": symbol, "eligible": None}
                with self._credit_state_lock:
                    if self._credit_subject_future is None:
                        self._credit_subject_future = self._credit_executor_locked().submit(
                            self._resolve_credit_subjects_background,
                        )
                return {"status": "pending", "stock_code": symbol, "eligible": None}
            try:
                subjects = self._fetch_credit_subjects()
            except Exception:  # noqa: BLE001 - older bridges may not expose the query
                return {"status": "unavailable", "stock_code": symbol, "eligible": None}

        code, _, exchange = symbol.partition(".")
        matched = None
        for row in subjects:
            instrument = str(
                row.get("m_strInstrumentID")
                or row.get("instrument_id")
                or row.get("stock_code")
                or row.get("symbol")
                or ""
            ).strip().upper()
            row_exchange = str(
                row.get("m_strExchangeID") or row.get("exchange_id") or ""
            ).strip().upper()
            if instrument == symbol or instrument == code and (not exchange or row_exchange == exchange):
                matched = row
                break
        if matched is None:
            return {
                "status": "ready",
                "stock_code": symbol,
                "eligible": False,
                "subject": None,
            }
        return {
            "status": "ready",
            "stock_code": symbol,
            "eligible": self._credit_subject_row_eligible(matched),
            "subject": matched,
        }

    @staticmethod
    def _credit_subject_row_symbol(row: dict[str, Any]) -> str:
        """Normalise one contract row into a ``CODE.EXCHANGE`` symbol."""
        instrument = str(
            row.get("m_strInstrumentID")
            or row.get("instrument_id")
            or row.get("stock_code")
            or row.get("symbol")
            or ""
        ).strip().upper()
        if not instrument:
            return ""
        if "." in instrument:
            return instrument
        exchange = str(row.get("m_strExchangeID") or row.get("exchange_id") or "").strip().upper()
        return f"{instrument}.{exchange}" if exchange else instrument

    @staticmethod
    def _credit_subject_row_eligible(row: dict[str, Any]) -> bool:
        raw_status = row.get("m_eFinStatus")
        try:
            return int(raw_status) == 48
        except (TypeError, ValueError):
            return str(raw_status or "").strip().upper() in {"48", "NORMAL", "OK"}

    def fetch_credit_subjects(self) -> list[dict[str, Any]]:
        """Read the broker's financing subject list over the background socket."""
        return self._fetch_credit_subjects()

    def snapshot(self) -> dict[str, Any]:
        """同一轮读取账户、持仓、委托和成交；任一步失败则整轮失败。"""
        self.probe()
        asset = self.get_asset()
        positions = self.call("get_positions", {"account_id": self.account_id})
        orders = self.call("query_orders", {"account_id": self.account_id, "strategy_name": ""})
        trades = self.call("query_trades", {"account_id": self.account_id, "strategy_name": ""})
        if not isinstance(asset, dict) or not isinstance(positions, dict):
            raise QmtRpcError("QMT 账户或持仓响应格式无效")
        entry_dates: dict[str, str] = {}
        if isinstance(trades, list):
            for trade in trades:
                if not isinstance(trade, dict):
                    continue
                action = str(
                    trade.get("action") or trade.get("direction")
                    or trade.get("entrust_bs") or trade.get("trade_type") or ""
                ).strip().upper()
                if action not in {"BUY", "B", "买入", "买", "1"}:
                    continue
                symbol = str(trade.get("stock_code") or trade.get("symbol") or "").strip().upper()
                raw_time = (
                    trade.get("trade_time") or trade.get("traded_at")
                    or trade.get("成交时间") or trade.get("time")
                )
                parsed = _parse_broker_time(raw_time, _now()) if raw_time not in (None, "") else None
                if not symbol or not parsed:
                    continue
                trade_date = parsed[:10]
                if trade_date > entry_dates.get(symbol, ""):
                    entry_dates[symbol] = trade_date
        normalized_positions = []
        for code, item in positions.items():
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("stock_code") or code or "").strip().upper()
            volume = int(item.get("volume") or 0)
            available = int(item.get("available") or item.get("can_use_volume") or 0)
            frozen_volume = int(item.get("frozen_volume") or item.get("frozen") or 0)
            on_road_volume = int(item.get("on_road_volume") or item.get("on_road") or 0)
            cost = _float(item.get("cost") or item.get("cost_price") or item.get("open_price"))
            if not symbol or volume < 0 or available < 0 or frozen_volume < 0 or on_road_volume < 0:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            if volume == 0:
                if available != 0:
                    raise QmtRpcError(f"QMT 空持仓可用数量不为零: {symbol}")
                continue
            if cost is None:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            if available > volume:
                raise QmtRpcError(f"QMT 可用数量大于持仓数量: {symbol}")
            market_value = _float(item.get("market_value"))
            price = _float(item.get("price") or item.get("last_price"))
            if (price is None or price <= 0) and market_value is not None and market_value > 0 and volume > 0:
                price = market_value / volume
            if market_value is None and price is not None and price > 0:
                market_value = price * volume
            normalized_positions.append({
                "symbol": symbol,
                "name": str(item.get("stock_name") or item.get("name") or symbol),
                "quantity": volume,
                "available": available,
                "frozen_volume": frozen_volume,
                "on_road_volume": on_road_volume,
                "cost_price": cost,
                "price": price if price is not None and price > 0 else None,
                "market_value": market_value,
                "asset_type": "etf" if symbol.startswith(("15", "16", "50", "51", "56", "58")) else "stock",
                "entry_date": entry_dates.get(symbol),
            })
        return {
            "account_id": self.account_id,
            "account": {
                "name": self.account_id,
                **_normalise_account(asset, self.account_type),
            },
            "positions": normalized_positions,
            "orders": orders if isinstance(orders, list) else [],
            "trades": trades if isinstance(trades, list) else [],
            "synced_at": _now(),
        }


class QmtTradingService:
    """本地交易控制面；不会把确认风险建议直接变成委托。"""

    def __init__(self, data_dir, settings: Any) -> None:
        self.settings = settings
        self.client = QmtZmqRpcClient(settings)
        self.trade_authorized = bool(getattr(settings, "qmt_trade_enabled", False))
        self.trade_enabled = self.trade_authorized and self.client.configured
        self.account_type = str(getattr(settings, "qmt_account_type", "STOCK") or "STOCK").upper()
        self.auto_sync_enabled = bool(getattr(settings, "qmt_auto_sync", True))
        self.auto_sync_interval = max(10.0, float(getattr(settings, "qmt_auto_sync_interval_seconds", 30.0)))
        self._lock = threading.RLock()
        self._submit_lock = threading.Lock()
        self._sync_write_lock = threading.Lock()
        self._auto_stop = threading.Event()
        self._auto_thread: threading.Thread | None = None
        self._auto_position_risk_service: Any | None = None
        self._connection_generation = 0
        self._last_status: dict[str, Any] = {}
        self._last_snapshot: dict[str, Any] | None = None
        self._last_snapshot_monotonic = 0.0
        self._last_account: dict[str, Any] | None = None
        self._last_account_monotonic = 0.0
        self._orders: dict[str, dict[str, Any]] = {}
        self._data_dir = data_dir
        self._db_path = data_dir / "user_data" / "position_risk" / "runtime.sqlite3"
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS qmt_orders (
                    idempotency_key TEXT PRIMARY KEY,
                    order_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )""",
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS qmt_credit_symbol_limit (
                    symbol TEXT NOT NULL,
                    credit_mode TEXT NOT NULL,
                    eligible INTEGER,
                    price REAL,
                    max_volume INTEGER,
                    max_amount REAL,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_accessed_at TEXT NOT NULL,
                    eligible_updated_at TEXT,
                    is_probe INTEGER,
                    PRIMARY KEY (symbol, credit_mode)
                )""",
            )
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(qmt_credit_symbol_limit)")
            }
            if "is_probe" not in columns:
                conn.execute("ALTER TABLE qmt_credit_symbol_limit ADD COLUMN is_probe INTEGER")

    def _connect(self) -> sqlite3.Connection:
        # Background threads write to the same file; give SQLite room to wait
        # for the write lock instead of failing fast.
        return sqlite3.connect(self._db_path, timeout=10.0)

    def _read_credit_limit(self, symbol: str, mode: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT eligible, price, max_volume, max_amount, status,
                          updated_at, last_accessed_at, eligible_updated_at
                   FROM qmt_credit_symbol_limit WHERE symbol = ? AND credit_mode = ?""",
                (symbol, mode),
            ).fetchone()
        if row is None:
            return None
        return {
            "eligible": None if row[0] is None else bool(row[0]),
            "price": row[1],
            "max_volume": row[2],
            "max_amount": row[3],
            "status": row[4],
            "updated_at": row[5],
            "last_accessed_at": row[6],
            "eligible_updated_at": row[7],
        }

    def _touch_credit_limit(self, symbol: str, mode: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE qmt_credit_symbol_limit
                   SET last_accessed_at = ? WHERE symbol = ? AND credit_mode = ?""",
                (_now(), symbol, mode),
            )

    def _write_credit_limit(
        self,
        symbol: str,
        mode: str,
        result: dict[str, Any],
        price: float,
        eligible: bool | None = None,
        is_probe: bool | None = None,
    ) -> None:
        status = str(result.get("status") or "unknown")
        max_volume = result.get("max_volume")
        max_amount = result.get("max_amount")
        now = _now()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO qmt_credit_symbol_limit (
                       symbol, credit_mode, eligible, price, max_volume, max_amount,
                       status, updated_at, last_accessed_at, eligible_updated_at, is_probe
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(symbol, credit_mode) DO UPDATE SET
                       eligible = COALESCE(excluded.eligible, eligible),
                       price = excluded.price,
                       max_volume = excluded.max_volume,
                       max_amount = excluded.max_amount,
                       status = excluded.status,
                       updated_at = excluded.updated_at,
                       is_probe = COALESCE(excluded.is_probe, is_probe)""",
                (
                    symbol,
                    mode,
                    None if eligible is None else int(eligible),
                    float(price),
                    int(max_volume) if isinstance(max_volume, (int, float)) else None,
                    float(max_amount) if isinstance(max_amount, (int, float)) else None,
                    status,
                    now,
                    now,
                    now if eligible is not None else None,
                    None if is_probe is None else int(is_probe),
                ),
            )

    def _read_credit_probe(self) -> dict[str, Any] | None:
        """The stored account-level financing balance measured by the probe."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT symbol, price, max_amount, updated_at
                   FROM qmt_credit_symbol_limit WHERE is_probe = 1""",
            ).fetchone()
        if row is None:
            return None
        return {
            "symbol": row[0],
            "price": row[1],
            "max_amount": row[2],
            "updated_at": row[3],
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            cached_account = (
                dict(self._last_account)
                if self._last_account is not None
                else dict(self._last_snapshot.get("account") or {}) if self._last_snapshot else None
            )
            account_age_ms = (
                round(max(0.0, time.monotonic() - self._last_account_monotonic) * 1000, 1)
                if self._last_account is not None else None
            )
            status = {
                "configured": self.client.configured,
                "trade_authorized": self.trade_authorized,
                "trade_enabled": self.trade_enabled,
                "account_id": self.client.account_id or None,
                "rpc_transport": "zmq",
                "rpc_address": self.client.connect_address or None,
                "connection_mode": self.client.connection_mode,
                "remote_rpc_address": self.client.remote_connect_address or None,
                "local_rpc_address": self.client.local_connect_address or None,
                "remote_configured": self.client.remote_configured,
                "local_configured": self.client.local_configured,
                "account_type": self.account_type,
                "auto_sync_enabled": self.auto_sync_enabled,
                "auto_sync_running": bool(self._auto_thread and self._auto_thread.is_alive()),
                "auto_sync_interval_seconds": self.auto_sync_interval,
                "last_probe_at": self._last_status.get("last_probe_at"),
                "last_sync_at": self._last_snapshot.get("synced_at") if self._last_snapshot else None,
                "account": cached_account or None,
                "account_age_ms": account_age_ms,
                "account_stale": account_age_ms is not None and account_age_ms > self.auto_sync_interval * 1000,
                "state": self._last_status.get("state", "not_configured" if not self.client.configured else "unknown"),
                "reason": self._last_status.get("reason") or self.client.configuration_reason,
                "latency_ms": self._last_status.get("latency_ms"),
            }
            return status

    def _remember_order(self, row: dict[str, Any]) -> None:
        key = str(row["idempotency_key"])
        with self._lock:
            self._orders[key] = row
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                "INSERT INTO qmt_orders(idempotency_key, order_json, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(idempotency_key) DO UPDATE SET order_json=excluded.order_json, updated_at=excluded.updated_at",
                (key, json.dumps(row, ensure_ascii=False), _now()),
            )

    def _remember_account(self, account: dict[str, Any]) -> None:
        with self._lock:
            self._last_account = dict(account)
            self._last_account_monotonic = time.monotonic()

    def _known_order(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            if key in self._orders:
                return dict(self._orders[key])
        with sqlite3.connect(self._db_path) as conn:
            row = conn.execute("SELECT order_json FROM qmt_orders WHERE idempotency_key=?", (key,)).fetchone()
        if not row:
            return None
        value = json.loads(row[0])
        with self._lock:
            self._orders[key] = value
        return value

    def _local_orders(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            rows = conn.execute("SELECT order_json FROM qmt_orders ORDER BY updated_at DESC LIMIT 100").fetchall()
        result = []
        for row in rows:
            try:
                value = json.loads(row[0])
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                result.append(value)
        return result

    def get_orders(self, idempotency_keys: set[str]) -> dict[str, dict[str, Any]]:
        keys = sorted({str(key).strip() for key in idempotency_keys if str(key).strip()})
        if not keys:
            return {}
        result: dict[str, dict[str, Any]] = {}
        with self._lock:
            for key in keys:
                if key in self._orders:
                    result[key] = dict(self._orders[key])
        missing = [key for key in keys if key not in result]
        if missing:
            placeholders = ",".join("?" for _key in missing)
            with sqlite3.connect(self._db_path) as conn:
                rows = conn.execute(
                    f"SELECT idempotency_key, order_json FROM qmt_orders WHERE idempotency_key IN ({placeholders})",  # noqa: S608
                    missing,
                ).fetchall()
            for key, raw in rows:
                try:
                    value = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    continue
                if not isinstance(value, dict):
                    continue
                result[str(key)] = value
                with self._lock:
                    self._orders[str(key)] = value
        return result

    @staticmethod
    def _normalize_remote_order(order: dict[str, Any], anchor: str | None = None) -> dict[str, Any]:
        value = dict(order)
        value["symbol"] = str(value.get("symbol") or value.get("stock_code") or "").strip().upper()
        value["order_sys_id"] = str(
            value.get("order_sys_id") or value.get("order_sysid") or value.get("order_id") or "",
        ) or None
        value["user_order_id"] = str(value.get("user_order_id") or value.get("remark") or "") or None
        if not value.get("error"):
            for field in ("status_msg", "status_message", "message", "reason", "msg"):
                message = str(value.get(field) or "").strip()
                if message:
                    value["error"] = message
                    break
        broker_at, broker_raw, broker_field = _broker_time(value, anchor or value.get("created_at"))
        if broker_raw is not None:
            value["broker_order_at"] = broker_at
            value["broker_order_time_raw"] = broker_raw
            value["broker_order_time_field"] = broker_field
        return value

    @staticmethod
    def _submit_result(response: Any) -> dict[str, Any] | None:
        """Accept the batch result shapes used by different QMT bridge builds."""
        candidates: Any = response
        if isinstance(candidates, dict):
            if any(key in candidates for key in ("success", "accepted", "explicit_failure")):
                return candidates
            for key in ("orders", "results", "items", "data"):
                if key in candidates:
                    candidates = candidates[key]
                    break
        if isinstance(candidates, dict):
            for key in ("order", "result"):
                nested = candidates.get(key)
                if isinstance(nested, dict):
                    return nested
            return None
        if not isinstance(candidates, list):
            return None
        for item in candidates:
            if not isinstance(item, dict):
                continue
            for key in ("order", "result"):
                nested = item.get(key)
                if isinstance(nested, dict):
                    return nested
            return item
        return None

    def _remote_order_for_submission(self, idempotency_key: str, order_tag: str) -> dict[str, Any] | None:
        try:
            remote_orders = self._query_remote_orders()
        except Exception:
            return None
        identifiers = {value for value in (idempotency_key, order_tag) if value}
        for order in remote_orders:
            order_identifiers = self._remote_order_identifiers(order)
            if identifiers & order_identifiers:
                return order
        return None

    @staticmethod
    def _remote_order_identifiers(order: dict[str, Any]) -> set[str]:
        identifiers = {
            str(order.get(field) or "").strip()
            for field in ("order_sys_id", "user_order_id", "signal_id", "remark")
        }
        return {value for value in identifiers if value}

    def _merge_remote_orders(self, remote_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        local = self._local_orders()
        by_identifier: dict[str, dict[str, Any]] = {}
        for item in local:
            key = str(item.get("idempotency_key") or "").strip()
            strategy = str(item.get("strategy_name") or "").strip()
            identifiers = self._remote_order_identifiers(item)
            if key:
                identifiers.add(key)
                if strategy:
                    identifiers.add(f"{strategy}:{key}")
            for identifier in identifiers:
                by_identifier[identifier] = item

        merged_remote: list[dict[str, Any]] = []
        matched_keys: set[str] = set()
        for raw in remote_orders:
            remote = self._normalize_remote_order(raw)
            current = next(
                (
                    by_identifier[identifier]
                    for identifier in self._remote_order_identifiers(remote)
                    if identifier in by_identifier
                ),
                None,
            )
            if current is None:
                merged_remote.append(remote)
                continue
            remote = self._normalize_remote_order(
                raw,
                str(current.get("created_at") or current.get("system_order_at") or "") or None,
            )
            merged = dict(current)
            merged.update({key: value for key, value in remote.items() if value is not None})
            merged["idempotency_key"] = current["idempotency_key"]
            if remote.get("order_sys_id") and not remote.get("error"):
                merged["error"] = None
            merged["updated_at"] = _now()
            self._remember_order(merged)
            matched_keys.add(str(current["idempotency_key"]))
            merged_remote.append(merged)
        return merged_remote + [
            item for item in local
            if str(item.get("idempotency_key") or "") not in matched_keys
        ]

    def _query_remote_orders(self) -> list[dict[str, Any]]:
        with self._lock:
            client = self.client
        result = client.call("query_orders", {"account_id": client.account_id, "strategy_name": ""})
        if not isinstance(result, list):
            raise QmtRpcError("QMT 委托响应格式无效")
        return [self._normalize_remote_order(item) for item in result if isinstance(item, dict)]

    def probe(self) -> dict[str, Any]:
        with self._lock:
            client = self.client
            generation = self._connection_generation
        started = time.monotonic()
        try:
            result = client.probe()
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                if generation == self._connection_generation and client is self.client:
                    self._last_status = {"state": "error", "reason": str(exc), "last_probe_at": _now()}
            raise
        with self._lock:
            if generation != self._connection_generation or client is not self.client:
                raise QmtRpcError("QMT 连接已切换，请重新检查")
            self._last_status = {
                "state": "ready", "reason": "QMT RPC 在线", "last_probe_at": _now(),
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
            }
        return {**self.status(), **result}

    def sync(self) -> dict[str, Any]:
        with self._lock:
            client = self.client
            generation = self._connection_generation
        started = time.monotonic()
        try:
            snapshot = client.snapshot()
            remote_orders = snapshot.get("orders") or []
            if isinstance(remote_orders, list):
                snapshot["orders"] = self._merge_remote_orders([
                    item for item in remote_orders if isinstance(item, dict)
                ])
        except Exception as exc:
            with self._lock:
                if generation == self._connection_generation and client is self.client:
                    self._last_status = {"state": "error", "reason": str(exc), "last_probe_at": _now()}
            raise
        with self._lock:
            if generation != self._connection_generation or client is not self.client:
                raise QmtRpcError("QMT 连接已切换，本次同步结果已丢弃")
            self._last_snapshot = snapshot
            self._last_snapshot_monotonic = time.monotonic()
            self._last_account = dict(snapshot.get("account") or {})
            self._last_account_monotonic = self._last_snapshot_monotonic
            self._last_status = {
                "state": "ready",
                "reason": "QMT账户正在自动同步" if self._auto_thread else "账户、持仓和委托已同步",
                "last_probe_at": _now(),
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
            }
        return snapshot

    def _sync_account_cache(self) -> dict[str, Any]:
        """Warm the account amount independently of the slower full snapshot."""
        with self._lock:
            client = self.client
            generation = self._connection_generation
        asset = client.get_asset(background=True)
        account = {
            "name": self.client.account_id,
            **_normalise_account(asset, self.account_type),
        }
        with self._lock:
            if generation != self._connection_generation or client is not self.client:
                raise QmtRpcError("QMT 连接已切换，本次账户缓存已丢弃")
            self._last_account = account
            self._last_account_monotonic = time.monotonic()
            if self._last_status.get("state") != "error":
                self._last_status = {
                    "state": "ready",
                    "reason": "QMT账户金额已更新，持仓正在同步",
                    "last_probe_at": _now(),
                    "latency_ms": self._last_status.get("latency_ms"),
                }
        return account

    def sync_into(self, position_risk_service: Any) -> dict[str, Any]:
        with self._sync_write_lock:
            snapshot = self.sync()
            try:
                portfolio = position_risk_service.replace_from_qmt(snapshot)
            except Exception as exc:
                with self._lock:
                    self._last_status = {"state": "error", "reason": str(exc), "last_probe_at": _now()}
                raise
            return {"portfolio": portfolio, "snapshot": snapshot}

    def start_auto_sync(self, position_risk_service: Any) -> bool:
        self._auto_position_risk_service = position_risk_service
        if not self.auto_sync_enabled or not self.client.configured:
            return False
        with self._lock:
            if self._auto_thread and self._auto_thread.is_alive():
                return True
            self._auto_stop.clear()

            def run() -> None:
                first_cycle = True
                while not self._auto_stop.is_set():
                    if first_cycle:
                        # Account cash is needed by the UI immediately; do not
                        # make it wait for positions, orders and trades.
                        threading.Thread(
                            target=self._account_cache_loop,
                            name="qmt-account-cache",
                            daemon=True,
                        ).start()
                        threading.Thread(
                            target=self._credit_limit_loop,
                            name="qmt-credit-limit",
                            daemon=True,
                        ).start()
                        first_cycle = False
                    try:
                        self.sync_into(position_risk_service)
                    except Exception:  # 状态由 sync 记录，下一轮自动重试
                        pass
                    if self._auto_stop.wait(self.auto_sync_interval):
                        break

            self._auto_thread = threading.Thread(target=run, name="qmt-auto-sync", daemon=True)
            self._auto_thread.start()
        return True

    def switch_connection(self, mode: str) -> dict[str, Any]:
        """Switch the QMT endpoint without restarting the API process."""
        mode = QmtZmqRpcClient._normalise_connection_mode(mode)
        with self._submit_lock:
            with self._lock:
                if mode == self.client.connection_mode:
                    return self.status()
                setattr(self.settings, "qmt_connection_mode", mode)
                self.client.close()
                self.client = QmtZmqRpcClient(self.settings)
                self._connection_generation += 1
                self.trade_enabled = False
                self._last_status = {
                    "state": "unknown" if self.client.configured else "not_configured",
                    "reason": f"已切换到{'本地' if mode == 'local' else '远程'} QMT，等待检查连接",
                    "last_probe_at": None,
                }
                self._last_snapshot = None
                self._last_snapshot_monotonic = 0.0
                self._last_account = None
                self._last_account_monotonic = 0.0
                position_risk_service = self._auto_position_risk_service
                auto_running = bool(self._auto_thread and self._auto_thread.is_alive())
                should_start_auto_sync = (
                    not auto_running
                    and self.auto_sync_enabled
                    and self.client.configured
                    and position_risk_service is not None
                )
        if should_start_auto_sync:
            self.start_auto_sync(position_risk_service)
        return self.status()

    @property
    def _account_cache_refresh_seconds(self) -> float:
        """Refresh faster than half the freshness window so it never lapses."""
        return max(4.0, min(_ACCOUNT_CACHE_REFRESH_SECONDS, self.auto_sync_interval / 2))

    def _account_cache_loop(self) -> None:
        """Keep the account amounts fresh without a preview ever paying for it.

        ``get_asset`` takes 3-4s on the broker side. A preview that finds the
        cache older than ``auto_sync_interval`` runs it inline, which showed
        up as a ~4.7s stall once per sync cycle. Refreshing on a fixed,
        shorter cadence keeps the interactive path on the cache.
        """
        while not self._auto_stop.is_set():
            try:
                self._sync_account_cache()
            except Exception:
                pass
            if self._auto_stop.wait(self._account_cache_refresh_seconds):
                break

    def _credit_limit_loop(self) -> None:
        """Keep stored symbol limits fresh and the subject list current.

        Renewal runs ahead of the TTL so a later dialog reads a value that is
        already there instead of waiting ~1.7s for the broker.
        """
        while not self._auto_stop.is_set():
            try:
                self._maintain_credit_limits()
            except Exception:
                pass
            if self._auto_stop.wait(_CREDIT_SYMBOL_RENEW_INTERVAL_SECONDS):
                break

    def _maintain_credit_limits(self) -> None:
        if self.account_type != "CREDIT":
            return
        if _age_seconds(self._credit_subject_list_synced_at()) > _CREDIT_SUBJECT_LIST_SYNC_SECONDS:
            self._sync_credit_subject_list()
        # The probe is the number every unwarmed symbol falls back to, so it is
        # refreshed before any per-symbol entry.
        probe = self._read_credit_probe()
        if probe is None or _age_seconds(probe.get("updated_at")) >= (
            _CREDIT_SYMBOL_LIMIT_TTL_SECONDS - _CREDIT_SYMBOL_RENEW_LEAD_SECONDS
        ):
            if self._refresh_credit_probe():
                return
        self._renew_one_credit_limit()

    def _select_credit_probe_symbol(self) -> tuple[str, float] | None:
        """Pick the financing subject with the smallest one-lot amount.

        The broker rounds a max-volume answer down to whole lots, so the
        cheaper the lot the tighter the measured account balance: a 119 yuan
        lot pins it to within one lot. Returns ``(symbol, price)``.
        """
        with self._connect() as conn:
            eligible = {
                row[0]
                for row in conn.execute(
                    "SELECT symbol FROM qmt_credit_symbol_limit WHERE eligible = 1",
                )
            }
        if not eligible:
            return None
        closes = self._latest_closes()
        candidates = [
            (price, symbol)
            for symbol, price in closes.items()
            if symbol in eligible and price > 0
        ]
        if not candidates:
            return None
        price, symbol = min(candidates)
        return symbol, price

    def _latest_closes(self) -> dict[str, float]:
        """Last close per symbol from the daily kline store, or ``{}``."""
        root = self._data_dir / "kline_daily"
        try:
            partitions = sorted(path.name for path in root.iterdir() if path.is_dir())
        except OSError:
            return {}
        if not partitions:
            return {}
        try:
            frame = pl.scan_parquet(root / partitions[-1] / "**/*.parquet").select(
                ["symbol", "close"],
            ).collect()
        except Exception:  # noqa: BLE001 - a missing store only disables the probe
            return {}
        if "symbol" not in frame.columns or "close" not in frame.columns:
            return {}
        return {
            symbol: close
            for symbol, close in frame.iter_rows()
            if symbol and close is not None
        }

    def _refresh_credit_probe(self) -> bool:
        """Re-measure the account balance through the probe symbol."""
        probe = self._read_credit_probe()
        if probe is None:
            selected = self._select_credit_probe_symbol()
            if selected is None:
                return False
            symbol, price = selected
        else:
            symbol, price = probe["symbol"], probe["price"] or 0.0
        if not symbol or not price or price <= 0:
            return False
        detail = self.client.get_credit_opvolume(
            symbol, price, "financing", timeout_seconds=_CREDIT_OPVOLUME_BACKGROUND_TIMEOUT_SECONDS,
        )
        if str(detail.get("status") or "") != "ready":
            return False
        with self._connect() as conn:
            conn.execute(
                "UPDATE qmt_credit_symbol_limit SET is_probe = 0 WHERE is_probe = 1",
            )
        self._write_credit_limit(symbol, "financing", detail, price, is_probe=True)
        return True

    def _credit_subject_list_synced_at(self) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT MAX(eligible_updated_at) FROM qmt_credit_symbol_limit",
            ).fetchone()
        return row[0] if row else None

    def _sync_credit_subject_list(self) -> int:
        """Persist which securities the broker allows financing buys on."""
        rows = self.client.fetch_credit_subjects()
        now = _now()
        payload = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = QmtZmqRpcClient._credit_subject_row_symbol(row)
            if not symbol:
                continue
            payload.append(
                (symbol, "financing", int(QmtZmqRpcClient._credit_subject_row_eligible(row))),
            )
        if not payload:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """INSERT INTO qmt_credit_symbol_limit (
                       symbol, credit_mode, eligible, price, max_volume, max_amount,
                       status, updated_at, last_accessed_at, eligible_updated_at
                   ) VALUES (?, ?, ?, NULL, NULL, NULL, 'unknown', ?, ?, ?)
                   ON CONFLICT(symbol, credit_mode) DO UPDATE SET
                       eligible = excluded.eligible,
                       eligible_updated_at = excluded.eligible_updated_at""",
                [(symbol, mode, eligible, now, now, now) for symbol, mode, eligible in payload],
            )
        return len(payload)

    def _renew_one_credit_limit(self) -> bool:
        """Refresh the oldest stored limit that is still in use."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT symbol, credit_mode, price FROM qmt_credit_symbol_limit
                   WHERE status = 'ready' AND max_amount IS NOT NULL
                     AND last_accessed_at > ? AND updated_at < ?
                   ORDER BY updated_at ASC LIMIT 1""",
                (
                    _utc_iso(_CREDIT_SYMBOL_ACTIVE_SECONDS),
                    _utc_iso(_CREDIT_SYMBOL_LIMIT_TTL_SECONDS - _CREDIT_SYMBOL_RENEW_LEAD_SECONDS),
                ),
            ).fetchone()
        if row is None:
            return False
        symbol, mode, price = row[0], row[1], row[2]
        if not price or price <= 0:
            return False
        detail = self.client.get_credit_opvolume(
            symbol, price, mode, timeout_seconds=_CREDIT_OPVOLUME_BACKGROUND_TIMEOUT_SECONDS,
        )
        self._write_credit_limit(symbol, mode, detail, price)
        return True

    def stop(self) -> None:
        self._auto_stop.set()
        thread = self._auto_thread
        if thread and thread.is_alive():
            thread.join(timeout=2.0)
        with self._lock:
            self._auto_thread = None
            self.trade_enabled = False
        self.client.close()

    def set_trade_enabled(self, enabled: bool) -> dict[str, Any]:
        with self._submit_lock:
            with self._lock:
                if enabled:
                    if not self.trade_authorized:
                        raise QmtRpcError("QMT_TRADE_ENABLED 未授权真实交易")
                    if not self.client.configured:
                        raise QmtRpcError(self.client.configuration_reason)
                    if self._last_snapshot is None or self._last_status.get("state") != "ready":
                        raise QmtRpcError("请先成功同步 QMT 权威账户，再开启真实交易")
                self.trade_enabled = bool(enabled)
        return self.status()

    def _buying_power(self, account: dict[str, Any], credit_buy_mode: str = "collateral") -> tuple[float | None, str, float | None]:
        """Resolve the amount that a BUY order may actually consume."""
        if self.account_type == "CREDIT":
            amount, label, financing_available = _credit_buying_power(account, credit_buy_mode)
            return amount, label or "信用账户可买额度", financing_available
        return _float(account.get("cash")), "可用资金", None

    def _credit_symbol_buying_power(
        self,
        symbol: str,
        price: float,
        credit_buy_mode: str,
        account: dict[str, Any],
        opvolume_timeout_seconds: float | None = None,
    ) -> tuple[float | None, str, float | None, dict[str, Any] | None]:
        """Resolve credit buying power without treating account cash as symbol credit.

        Financing buying power is a property of both the account and the
        security.  QMT's account row only contains the former, so always ask
        for the per-symbol limit in financing mode.  A ready result, including
        a zero-volume result, is authoritative; account-level fields are only
        a compatibility fallback when the bridge cannot provide that query.
        """
        amount, label, financing_available = self._buying_power(account, credit_buy_mode)
        if self.account_type != "CREDIT" or (
            credit_buy_mode != "financing" and amount not in (None, 0.0)
        ):
            return amount, label, financing_available, None
        # A stored answer beats any broker round trip: eligibility only moves
        # when the broker changes its list, and the limit is refreshed ahead of
        # its TTL by the background loop.
        stored = self._read_credit_limit(symbol, credit_buy_mode)
        if stored is not None:
            self._touch_credit_limit(symbol, credit_buy_mode)
            if stored.get("eligible") is False:
                return 0.0, "该股票不可融资买入", financing_available, {
                    "status": "ready",
                    "stock_code": symbol,
                    "max_volume": 0,
                    "max_amount": 0,
                    "reason": "not_financing_subject",
                    "cached": True,
                }
            if self._credit_limit_is_fresh(stored, price):
                return (
                    round(float(stored["max_amount"]), 2),
                    "该股票最大融资可买",
                    financing_available,
                    {
                        "status": "ready",
                        "stock_code": symbol,
                        "max_volume": stored.get("max_volume"),
                        "max_amount": stored.get("max_amount"),
                        "cached": True,
                    },
                )
        if credit_buy_mode == "financing":
            subject_getter = getattr(self.client, "get_credit_subject", None)
            if callable(subject_getter):
                try:
                    if opvolume_timeout_seconds is None:
                        subject = subject_getter(symbol)
                    else:
                        try:
                            subject = subject_getter(symbol, background=True)
                        except TypeError:
                            # Keep compatibility with test doubles and older
                            # clients that still expose the one-argument API.
                            subject = subject_getter(symbol)
                except Exception:  # noqa: BLE001 - fall through to opvolume compatibility path
                    subject = None
                if isinstance(subject, dict) and subject.get("status") == "ready" and subject.get("eligible") is False:
                    detail = {
                        "status": "ready",
                        "stock_code": symbol,
                        "max_volume": 0,
                        "max_amount": 0,
                        "reason": "not_financing_subject",
                    }
                    self._write_credit_limit(symbol, credit_buy_mode, detail, price, eligible=False)
                    return 0.0, "该股票不可融资买入", financing_available, detail
        getter = getattr(self.client, "get_credit_opvolume", None)
        if not callable(getter):
            return amount, label, financing_available, None
        try:
            if opvolume_timeout_seconds is None:
                detail = getter(symbol, price, credit_buy_mode)
            else:
                try:
                    detail = getter(symbol, price, credit_buy_mode, opvolume_timeout_seconds, True)
                except TypeError:
                    try:
                        detail = getter(symbol, price, credit_buy_mode, opvolume_timeout_seconds)
                    except TypeError:
                        # Keep compatibility with test doubles and older bridge
                        # clients that still expose the original three-argument
                        # method while allowing the built-in client to use a
                        # shorter preview polling window.
                        detail = getter(symbol, price, credit_buy_mode)
        except Exception:  # noqa: BLE001 - an unavailable bridge can use the fallback mode
            detail = {"status": "unavailable", "stock_code": symbol, "max_volume": None}
        if isinstance(detail, dict) and detail.get("status") in {"ready", "unavailable", "error"}:
            # Eligibility is decided by the broker's subject list, never by an
            # ``unavailable`` here: the bridge also returns it when its async
            # query is throttled, which must not brand a good stock for good.
            self._write_credit_limit(symbol, credit_buy_mode, detail, price)
        probe_amount = self._probe_derived_amount(price)
        if not isinstance(detail, dict) or detail.get("status") != "ready":
            if isinstance(detail, dict) and detail.get("status") == "pending":
                # The broker has not answered for this symbol yet. Fall back to
                # the tightest number we already have rather than the account
                # level financing field, which is a different, larger quantity.
                served = _smallest(
                    probe_amount,
                    float(stored["max_amount"]) if stored is not None and stored.get("max_amount") is not None else None,
                )
                if served is not None:
                    return (
                        round(served, 2),
                        "该股票最大融资可买",
                        financing_available,
                        {
                            "status": "ready",
                            "stock_code": symbol,
                            "max_volume": int(served / price / 100) * 100 if price > 0 else None,
                            "max_amount": served,
                            "cached": True,
                            "stale": True,
                            "from_probe": probe_amount is not None and served == probe_amount,
                        },
                    )
                return 0.0, label, financing_available, detail
            if probe_amount is not None:
                return (
                    probe_amount,
                    "该股票最大融资可买",
                    financing_available,
                    {
                        "status": "ready",
                        "stock_code": symbol,
                        "max_volume": int(probe_amount / price / 100) * 100,
                        "max_amount": probe_amount,
                        "cached": True,
                        "from_probe": True,
                    },
                )
            return amount, label, financing_available, detail if isinstance(detail, dict) else None
        try:
            max_volume = int(detail.get("max_volume") or 0)
        except (TypeError, ValueError):
            return amount, label, financing_available, detail
        if max_volume < 0:
            return amount, label, financing_available, detail
        max_amount = _float(detail.get("max_amount"))
        served = _smallest(
            max_amount if max_amount is not None else max_volume * price,
            probe_amount,
        )
        return (
            round(served, 2),
            "该股票最大融资可买",
            financing_available,
            detail,
        )

    def _probe_derived_amount(self, price: float) -> float | None:
        """Account financing balance from the probe, rounded down to lots.

        The broker answers in whole lots, so the cheapest lot measures the
        account balance most tightly. Any symbol is then bounded by that
        balance at its own price.
        """
        if price <= 0:
            return None
        probe = self._read_credit_probe()
        if probe is None or probe.get("max_amount") is None:
            return None
        if _age_seconds(probe.get("updated_at")) > _CREDIT_SYMBOL_LIMIT_TTL_SECONDS:
            return None
        lots = int(float(probe["max_amount"]) / price / 100)
        if lots <= 0:
            return None
        return round(lots * 100 * price, 2)

    @staticmethod
    def _credit_limit_is_fresh(stored: dict[str, Any], price: float) -> bool:
        """Whether a stored limit can still be shown for this price."""
        if stored.get("status") != "ready" or stored.get("max_amount") is None:
            return False
        if _age_seconds(stored.get("updated_at")) > _CREDIT_SYMBOL_LIMIT_TTL_SECONDS:
            return False
        stored_price = _float(stored.get("price"))
        if stored_price is None or stored_price <= 0:
            return False
        return abs(price - stored_price) / stored_price <= _CREDIT_SYMBOL_LIMIT_PRICE_TOLERANCE

    def _validate_order(self, request: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
        if not self.trade_enabled:
            raise QmtRpcError("真实交易开关未开启")
        action = str(request.get("action") or "").upper()
        if action not in {"BUY", "SELL"}:
            raise ValueError("交易方向必须是买入或卖出")
        symbol = str(request.get("symbol") or request.get("stock_code") or "").strip().upper()
        if not symbol:
            raise ValueError("证券代码不能为空")
        volume = int(request.get("volume") or 0)
        if volume <= 0 or volume % 100 != 0:
            raise ValueError("委托数量必须是正数且为 100 股整数手")
        price_type = str(request.get("price_type") or "LIMIT").upper()
        credit_buy_mode = str(request.get("credit_buy_mode") or "collateral").strip().lower()
        if action == "BUY" and credit_buy_mode not in _CREDIT_BUY_MODES:
            raise ValueError("信用账户买入方式必须是担保品买入或融资买入")
        if price_type not in {"LIMIT", "LATEST", "LATEST_PRICE"}:
            raise ValueError("暂仅支持限价或最新价")
        price = _float(request.get("price")) or 0.0
        if price_type == "LIMIT" and price <= 0:
            raise ValueError("限价必须大于 0")
        requested_credit_buy_mode = str(request.get("requested_credit_buy_mode") or credit_buy_mode).strip().lower()
        switched_credit_buy_mode = requested_credit_buy_mode != credit_buy_mode
        financing_symbol_unavailable = False
        if action == "SELL":
            positions = snapshot.get("positions") or []
            row = next((item for item in positions if item.get("symbol") == symbol), None)
            available = int((row or {}).get("available") or 0)
            if row is None or available < volume:
                frozen = int((row or {}).get("frozen_volume") or 0)
                quantity = int((row or {}).get("quantity") or 0)
                if frozen > 0:
                    raise ValueError(
                        f"QMT 可用持仓不足：总持仓 {quantity} 股，已冻结 {frozen} 股，"
                        "请在 QMT 确认撤单并等待冻结释放",
                    )
                raise ValueError("QMT 可用持仓不足，已拒绝卖出")
        elif price_type == "LIMIT":
            account = snapshot.get("account") or {}
            buying_power, _label, _financing_available, detail = self._credit_symbol_buying_power(
                symbol, price, credit_buy_mode, account,
            )
            financing_symbol_unavailable = (
                credit_buy_mode == "financing"
                and isinstance(detail, dict)
                and detail.get("status") == "ready"
                and buying_power == 0
            )
            if self.account_type == "CREDIT":
                fallback_mode = _fallback_credit_buy_mode(credit_buy_mode)
                fallback_power, _fallback_label, _, _fallback_detail = self._credit_symbol_buying_power(
                    symbol, price, fallback_mode, account,
                )
                if (
                    (buying_power is None or buying_power < price * volume)
                    and
                    fallback_power is not None
                    and fallback_power >= price * volume
                    and (buying_power is None or fallback_power > buying_power)
                ):
                    credit_buy_mode = fallback_mode
                    buying_power = fallback_power
                    switched_credit_buy_mode = True
            if buying_power is None:
                if self.account_type == "CREDIT":
                    status = str((detail or {}).get("status") or "unavailable")
                    raise QmtRpcError("QMT 未返回信用账户可买额度（该股票最大可买量：%s），已拒绝买入" % status)
            elif buying_power < price * volume:
                raise ValueError("QMT 可用资金不足，已拒绝买入")
        return {
            "action": action,
            "symbol": symbol,
            "volume": volume,
            "price": price,
            "price_type": price_type,
            "credit_buy_mode": credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
            "requested_credit_buy_mode": requested_credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
            "credit_buy_mode_switched": switched_credit_buy_mode,
            "credit_buy_mode_reason": (
                "该股票不可融资买入，已自动切换为担保品买入"
                if switched_credit_buy_mode and financing_symbol_unavailable
                else "首选买入额度不足，已自动切换"
                if switched_credit_buy_mode
                else None
            ),
        }

    def _allocation_preview(
        self,
        request: dict[str, Any],
        snapshot: dict[str, Any],
        opvolume_timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Calculate a lot-sized order from fresh cash/position availability."""
        action = str(request.get("action") or "").upper()
        symbol = str(request.get("symbol") or request.get("stock_code") or "").strip().upper()
        if action not in {"BUY", "SELL"} or not symbol:
            raise ValueError("交易方向和证券代码不能为空")
        price_type = str(request.get("price_type") or "LIMIT").upper()
        price = _float(request.get("price")) or 0.0
        if price_type in {"LATEST", "LATEST_PRICE"}:
            price = _float(request.get("reference_price")) or price
        if price <= 0:
            raise ValueError("金额下单需要有效的参考价格")
        mode = str(request.get("allocation_mode") or "").strip().lower()
        if mode not in _ALLOCATION_MODES:
            raise ValueError("金额分配方式必须是当前可用金额、可用金额六分之一、五分之一、四分之一、三分之一、二分之一或固定金额")

        financing_available = None
        credit_opvolume = None
        financing_buying_power_amount = None
        requested_credit_buy_mode = str(request.get("credit_buy_mode") or "collateral").strip().lower()
        credit_buy_mode = requested_credit_buy_mode
        if action == "BUY":
            account = snapshot.get("account") or {}
            basis_amount, basis_label, financing_available, credit_opvolume = self._credit_symbol_buying_power(
                symbol, price, credit_buy_mode, account, opvolume_timeout_seconds,
            )
            if basis_amount is None or basis_amount < 0:
                # A financing request may still fall back to collateral buying
                # when the broker has no account-level financing field. Keep a
                # zero primary candidate until that fallback is evaluated.
                if self.account_type == "CREDIT" and credit_buy_mode == "financing":
                    primary_basis_amount = 0.0
                elif self.account_type == "CREDIT":
                    status = str((credit_opvolume or {}).get("status") or "unavailable")
                    raise QmtRpcError("QMT 未返回信用账户可买额度（该股票最大可买量：%s），无法计算委托金额" % status)
                else:
                    raise QmtRpcError("QMT 可用资金无效，无法计算委托金额")
            else:
                primary_basis_amount = basis_amount
            if requested_credit_buy_mode == "financing":
                # Keep the requested financing result separate from the
                # effective candidate.  A collateral fallback must not make a
                # non-financing symbol look as though it had financing power.
                financing_buying_power_amount = (
                    basis_amount
                    if credit_opvolume and credit_opvolume.get("status") == "ready"
                    else None
                )
            available_volume = None
        else:
            row = next(
                (item for item in snapshot.get("positions") or [] if item.get("symbol") == symbol),
                None,
            )
            available_volume = int((row or {}).get("available") or 0)
            if row is None or available_volume <= 0:
                frozen = int((row or {}).get("frozen_volume") or 0)
                quantity = int((row or {}).get("quantity") or 0)
                if frozen > 0:
                    raise ValueError(
                        f"QMT 可用持仓不足：总持仓 {quantity} 股，已冻结 {frozen} 股，"
                        "请在 QMT 确认撤单并等待冻结释放",
                    )
                raise ValueError("QMT 可用持仓不足，无法计算卖出金额")
            basis_amount = available_volume * price
            primary_basis_amount = basis_amount
            basis_label = "可用持仓市值"

        fixed_amount = _float(request.get("allocation_value")) if mode == "fixed" else None
        if mode == "fixed" and (fixed_amount is None or fixed_amount <= 0):
            raise ValueError("固定金额必须大于 0")

        def build_candidate(candidate_mode: str, candidate_basis: float, candidate_label: str) -> dict[str, Any]:
            if mode == "fixed":
                candidate_requested_amount = fixed_amount or 0.0
            else:
                candidate_requested_amount = candidate_basis * _ALLOCATION_RATIOS[mode]
            candidate_target_amount = min(candidate_requested_amount, candidate_basis)
            candidate_volume = int(candidate_target_amount / price / 100) * 100
            if available_volume is not None:
                candidate_volume = min(candidate_volume, (available_volume // 100) * 100)
            candidate_actual_amount = round(candidate_volume * price, 2)
            return {
                "credit_buy_mode": candidate_mode if self.account_type == "CREDIT" and action == "BUY" else None,
                "allocation_value": candidate_requested_amount if mode == "fixed" else None,
                "basis_label": candidate_label,
                "basis_amount": round(candidate_basis, 2),
                "target_amount": round(candidate_target_amount, 2),
                "actual_amount": candidate_actual_amount,
                "volume": candidate_volume,
                "capped": candidate_target_amount < candidate_requested_amount or candidate_volume * price < candidate_target_amount,
                "reason": "金额不足一手" if candidate_volume < 100 else None,
            }

        candidate = build_candidate(credit_buy_mode, primary_basis_amount, basis_label)
        switched_credit_buy_mode = False
        if action == "BUY" and self.account_type == "CREDIT":
            fallback_mode = _fallback_credit_buy_mode(credit_buy_mode)
            fallback_basis, fallback_label, _, _fallback_opvolume = self._credit_symbol_buying_power(
                symbol, price, fallback_mode, account, opvolume_timeout_seconds,
            )
            if fallback_basis is not None and fallback_basis >= 0:
                fallback_candidate = build_candidate(fallback_mode, fallback_basis, fallback_label)
                primary_insufficient = (
                    primary_basis_amount <= 0
                    or candidate["capped"]
                    or candidate["volume"] < 100
                )
                if (
                    requested_credit_buy_mode == "financing"
                    and isinstance(credit_opvolume, dict)
                    and credit_opvolume.get("status") == "pending"
                ):
                    # Keep financing selected until the per-symbol result is
                    # ready; switching to collateral during the pending state
                    # would be a false negative for an eligible stock.
                    primary_insufficient = False
                if primary_insufficient and fallback_candidate["actual_amount"] > candidate["actual_amount"] and fallback_candidate["volume"] >= 100:
                    candidate = fallback_candidate
                    credit_buy_mode = fallback_mode
                    switched_credit_buy_mode = True

        if (
            action == "BUY"
            and self.account_type == "CREDIT"
            and (basis_amount is None or basis_amount < 0)
            and not switched_credit_buy_mode
        ):
            status = str((credit_opvolume or {}).get("status") or "unavailable")
            raise QmtRpcError("QMT 未返回信用账户可买额度（该股票最大可买量：%s），无法计算委托金额" % status)

        requested_amount = candidate["allocation_value"] if mode == "fixed" else candidate["target_amount"]
        return {
            "action": action,
            "symbol": symbol,
            "price": price,
            "price_type": price_type,
            "credit_buy_mode": credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
            "requested_credit_buy_mode": requested_credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
            "credit_buy_mode_switched": switched_credit_buy_mode,
            "credit_buy_mode_reason": (
                "该股票不可融资买入，已自动切换为担保品买入"
                if switched_credit_buy_mode
                and requested_credit_buy_mode == "financing"
                and financing_buying_power_amount == 0
                else "首选买入额度不足，已自动切换为" + ("担保品买入" if credit_buy_mode == "collateral" else "融资买入")
                if switched_credit_buy_mode
                else None
            ),
            "allocation_mode": mode,
            "allocation_value": requested_amount if mode == "fixed" else None,
            "basis_label": candidate["basis_label"],
            "basis_amount": candidate["basis_amount"],
            "cash_amount": _float((snapshot.get("account") or {}).get("cash")),
            "financing_available_amount": financing_available,
            "credit_opvolume": credit_opvolume,
            "financing_buying_power_amount": financing_buying_power_amount,
            "buying_power_amount": candidate["basis_amount"],
            "target_amount": candidate["target_amount"],
            "actual_amount": candidate["actual_amount"],
            "volume": candidate["volume"],
            "available_volume": available_volume,
            "capped": candidate["capped"],
            "reason": candidate["reason"],
        }

    def preview_order(self, request: dict[str, Any]) -> dict[str, Any]:
        action = str(request.get("action") or "").upper()
        # Preview and an immediately following submission share the recent
        # account snapshot for responsiveness. QMT still performs the final
        # acceptance and balance validation when the order is sent.
        with self._lock:
            cached_snapshot = self._last_snapshot
            snapshot_age = time.monotonic() - self._last_snapshot_monotonic
            cached_account = self._last_account
            account_age = time.monotonic() - self._last_account_monotonic
        if action == "BUY" and cached_account is not None and account_age <= self.auto_sync_interval:
            snapshot = {"account": dict(cached_account), "positions": []}
        elif cached_snapshot is not None and snapshot_age <= self.auto_sync_interval:
            if action == "BUY":
                snapshot = {
                    "account": dict(cached_snapshot.get("account") or {}),
                    "positions": [],
                }
            elif action == "SELL":
                snapshot = {
                    "account": {},
                    "positions": [
                        {
                            "symbol": item.get("symbol"),
                            "available": item.get("available", 0),
                            "quantity": item.get("quantity", 0),
                            "frozen_volume": item.get("frozen_volume", 0),
                            "on_road_volume": item.get("on_road_volume", 0),
                        }
                        for item in cached_snapshot.get("positions") or []
                    ],
                }
            else:
                snapshot = self._order_preflight(action)
        else:
            snapshot = self._order_preflight(action)
        return self._allocation_preview(request, snapshot, opvolume_timeout_seconds=0.5)

    def _order_preflight(self, action: str) -> dict[str, Any]:
        if action == "BUY":
            asset = self.client.get_asset()
            account = _normalise_account(asset, self.account_type)
            self._remember_account(account)
            return {"account": account, "positions": []}
        if action != "SELL":
            raise ValueError("交易方向必须是买入或卖出")
        raw_positions = self.client.call("get_positions", {"account_id": self.client.account_id})
        if not isinstance(raw_positions, dict):
            raise QmtRpcError("QMT 持仓响应格式无效")
        positions = []
        for code, item in raw_positions.items():
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("stock_code") or code or "").strip().upper()
            try:
                available = int(item.get("available") or item.get("can_use_volume") or 0)
                quantity = int(item.get("volume") or item.get("quantity") or 0)
                frozen_volume = int(item.get("frozen_volume") or item.get("frozen") or 0)
                on_road_volume = int(item.get("on_road_volume") or item.get("on_road") or 0)
            except (TypeError, ValueError) as exc:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}") from exc
            if not symbol or quantity < 0 or available < 0 or frozen_volume < 0 or on_road_volume < 0:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            positions.append({
                "symbol": symbol,
                "available": available,
                "quantity": quantity,
                "frozen_volume": frozen_volume,
                "on_road_volume": on_road_volume,
            })
        return {"account": {}, "positions": positions}

    def submit_order(self, request: dict[str, Any]) -> dict[str, Any]:
        idempotency_key = str(request.get("idempotency_key") or "").strip()
        if not idempotency_key:
            raise ValueError("缺少委托幂等键")
        strategy_name = str(request.get("strategy_name") or "position_risk").strip().lower()
        if strategy_name not in {"position_risk", "limit_board"}:
            raise ValueError("不支持的委托来源")
        with self._submit_lock:
            existing = self._known_order(idempotency_key)
            if existing:
                return existing
            action = str(request.get("action") or "").upper()
            try:
                with self._lock:
                    cached_account = self._last_account
                    account_age = time.monotonic() - self._last_account_monotonic
                if action == "BUY" and cached_account is not None and account_age <= self.auto_sync_interval:
                    snapshot = {"account": dict(cached_account), "positions": []}
                else:
                    snapshot = self._order_preflight(action)
                allocation = None
                if request.get("allocation_mode"):
                    allocation = self._allocation_preview(request, snapshot)
                    request = {
                        **request,
                        "volume": allocation["volume"],
                        "requested_credit_buy_mode": request.get("credit_buy_mode"),
                        "credit_buy_mode": allocation.get("credit_buy_mode") or request.get("credit_buy_mode"),
                    }
                    if allocation["volume"] < 100:
                        raise ValueError(allocation["reason"] or "金额不足一手")
                normalized = self._validate_order(request, snapshot)
            except QmtRpcError as exc:
                raise QmtOrderPreflightError(str(exc)) from exc
            order_tag = f"{strategy_name}:{idempotency_key}"
            params = {
                "stock_code": normalized["symbol"], "action": normalized["action"],
                "volume": normalized["volume"], "price": normalized["price"],
                "price_type": normalized["price_type"], "account_id": self.client.account_id,
                "strategy_name": strategy_name, "signal_id": idempotency_key,
                "remark": order_tag, "require_idempotency_check": True,
            }
            if self.account_type == "CREDIT" and normalized["action"] == "BUY":
                params["credit_buy_mode"] = normalized["credit_buy_mode"]
            created_at = _now()
            row = {
                "idempotency_key": idempotency_key, **normalized,
                "strategy_name": strategy_name,
                "status": "submitting", "order_sys_id": None, "user_order_id": order_tag,
                "created_at": created_at, "updated_at": created_at, "error": None,
                "trigger_at": request.get("trigger_at"),
                "system_order_at": request.get("system_order_at"),
                "qmt_submit_at": None,
                "qmt_response_at": None,
                "qmt_accepted_at": None,
                "broker_order_at": None,
                "broker_order_time_raw": None,
                "broker_order_time_field": None,
            }
            if allocation is not None:
                row.update({
                    "allocation_mode": allocation["allocation_mode"],
                    "allocation_value": allocation["allocation_value"],
                    "allocation_basis_amount": allocation["basis_amount"],
                    "allocation_target_amount": allocation["target_amount"],
                    "estimated_amount": allocation["actual_amount"],
                })
            row["requested_credit_buy_mode"] = request.get("requested_credit_buy_mode") or request.get("credit_buy_mode")
            row["credit_buy_mode_switched"] = bool(normalized.get("credit_buy_mode_switched"))
            row["credit_buy_mode_reason"] = normalized.get("credit_buy_mode_reason")
            self._remember_order(row)
            row["qmt_submit_at"] = _now()
            try:
                response = self.client.call(
                    "submit_orders_batch",
                    {"account_id": self.client.account_id, "strategy_name": strategy_name, "batch_id": idempotency_key, "orders": [params]},
                )
            except Exception as exc:
                # Some QMT bridges return an error after passorder succeeded
                # but before the order becomes visible to query_orders. Do a
                # final reconciliation and expose an explicit unknown state;
                # never let the caller retry the same idempotency key.
                if _is_uncertain_passorder_error(exc):
                    remote = self._remote_order_for_submission(idempotency_key, order_tag)
                    if remote is not None and (
                        remote.get("order_sys_id") or remote.get("user_order_id")
                    ):
                        response_time = _now()
                        row.update({key: value for key, value in remote.items() if value is not None})
                        row.update(
                            idempotency_key=idempotency_key,
                            strategy_name=str(row.get("strategy_name") or strategy_name),
                            status=str(remote.get("status") or "accepted_pending"),
                            error=None,
                            qmt_response_at=response_time,
                            qmt_accepted_at=response_time,
                            updated_at=response_time,
                        )
                        self._remember_order(row)
                        return row
                    response_time = _now()
                    row.update(
                        status="unknown",
                        qmt_response_at=response_time,
                        updated_at=response_time,
                        error="QMT 已调用 passorder，但暂未在委托列表找到订单；请在 QMT 核对，原幂等键禁止重发",
                    )
                    self._remember_order(row)
                    return row
                row.update(status="unknown", updated_at=_now(), error=str(exc))
                self._remember_order(row)
                raise
            result = self._submit_result(response)
            qmt_response_at = _now()
            if result is None:
                remote = self._remote_order_for_submission(idempotency_key, order_tag)
                if remote is not None and (
                    remote.get("order_sys_id") or remote.get("user_order_id")
                ):
                    row.update({
                        key: value for key, value in remote.items()
                        if value is not None
                    })
                    row.update(
                        idempotency_key=idempotency_key,
                        strategy_name=str(row.get("strategy_name") or strategy_name),
                        status=str(remote.get("status") or "accepted_pending"),
                        error=None,
                        qmt_response_at=qmt_response_at,
                        qmt_accepted_at=qmt_response_at,
                        updated_at=qmt_response_at,
                    )
                    self._remember_order(row)
                    return row
                row.update(
                    status="unknown",
                    qmt_response_at=qmt_response_at,
                    updated_at=qmt_response_at,
                    error="QMT 未返回可识别的委托结果；请在 QMT 委托列表核对",
                )
                self._remember_order(row)
                raise QmtRpcError("QMT 未返回可识别的委托结果；请在 QMT 委托列表核对，该幂等键不会自动重发")
            if not result.get("success") or not result.get("accepted"):
                uncertain = not bool(result.get("explicit_failure"))
                row.update(
                    status="unknown" if uncertain else "rejected",
                    qmt_response_at=qmt_response_at,
                    updated_at=qmt_response_at,
                    error=str(result.get("error") or "QMT 拒绝委托"),
                )
                self._remember_order(row)
                raise QmtRpcError(
                    f"{row['error']}；该幂等键不会自动重发" if uncertain else str(row["error"]),
                )
            result_order_sys_id = str(result.get("order_sys_id") or "").strip() or None
            result_user_order_id = str(result.get("user_order_id") or "").strip() or None
            if not result_order_sys_id and not result_user_order_id:
                remote = self._remote_order_for_submission(idempotency_key, order_tag)
                if remote is not None and (
                    remote.get("order_sys_id") or remote.get("user_order_id")
                ):
                    row.update({key: value for key, value in remote.items() if value is not None})
                    row.update(
                        idempotency_key=idempotency_key,
                        strategy_name=str(row.get("strategy_name") or strategy_name),
                        status=str(remote.get("status") or "accepted_pending"),
                        error=None,
                        qmt_response_at=qmt_response_at,
                        qmt_accepted_at=qmt_response_at,
                        updated_at=qmt_response_at,
                    )
                    self._remember_order(row)
                    return row
                row.update(
                    status="unknown",
                    qmt_response_at=qmt_response_at,
                    updated_at=qmt_response_at,
                    error="QMT 返回已受理但未提供委托号，且委托列表未找到对应订单",
                )
                self._remember_order(row)
                raise QmtRpcError(
                    "QMT 返回已受理但未提供委托号，且委托列表未找到对应订单；该幂等键不会自动重发",
                )
            broker_at, broker_raw, broker_field = _broker_time(result, created_at)
            row.update(
                status="accepted_pending",
                order_sys_id=result_order_sys_id,
                user_order_id=result_user_order_id,
                qmt_response_at=qmt_response_at,
                qmt_accepted_at=qmt_response_at,
                broker_order_at=broker_at,
                broker_order_time_raw=broker_raw,
                broker_order_time_field=broker_field,
                updated_at=qmt_response_at,
            )
            self._remember_order(row)
            return row

    def list_orders(self) -> list[dict[str, Any]]:
        local = self._local_orders()
        try:
            orders = self._query_remote_orders()
        except Exception:
            return local
        return self._merge_remote_orders(orders)

    def cancel_order(self, request: dict[str, Any]) -> dict[str, Any]:
        with self._submit_lock:
            return self._cancel_order(request)

    def _cancel_order(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.trade_enabled:
            raise QmtRpcError("真实交易开关未开启")
        order_sys_id = str(request.get("order_sys_id") or "").strip()
        if not order_sys_id:
            raise ValueError("缺少 QMT 委托号，无法撤单")
        self.client.probe()
        remote_order = next(
            (
                order
                for order in self._query_remote_orders()
                if str(order.get("order_sys_id") or "").strip() == order_sys_id
            ),
            None,
        )
        if remote_order is None:
            raise QmtRpcError(f"QMT 委托列表未找到 {order_sys_id}，已阻止撤单，请先刷新委托状态")
        status = str(remote_order.get("status") or "").strip().lower()
        if status in _QMT_CANCEL_PENDING_ORDER_STATUSES:
            raise QmtRpcError("撤单请求处理中，请等待 QMT 更新委托状态")
        if status in _QMT_TERMINAL_ORDER_STATUS_LABELS:
            label = _QMT_TERMINAL_ORDER_STATUS_LABELS[status]
            raise QmtRpcError(f"委托当前状态为“{label}”，无需重复撤单")
        if status not in _QMT_CANCELABLE_ORDER_STATUSES:
            raise QmtRpcError("QMT 委托状态未知，已阻止撤单，请在 QMT 核对")
        result = self.client.call("cancel_order", {"order_sys_id": order_sys_id, "account_id": self.client.account_id})
        return {"order_sys_id": order_sys_id, "status": "cancel_requested", "result": result}
