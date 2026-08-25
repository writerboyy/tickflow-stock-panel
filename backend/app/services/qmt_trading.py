"""云端 QMT ZMQ RPC 交易网关。

浏览器永远只访问本地 API；本模块是主项目与云端 QMT 之间的唯一边界。
ZMQ 连接地址和账户从环境变量读取，不写入运行日志或用户数据。
"""
from __future__ import annotations

import base64
import json
import math
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

try:
    import zmq
except ImportError:  # pragma: no cover - dependency is installed with the backend
    zmq = None


class QmtRpcError(RuntimeError):
    pass


class QmtOrderPreflightError(QmtRpcError):
    """The order was rejected before a submit request was sent to QMT."""


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
    "credit_financing_buying_power",
)
_CREDIT_FINANCING_AVAILABLE_FIELDS = (
    "m_dFinEnableBalance",
    "m_dFinEnableQuota",
    "fin_enable_balance",
    "financing_available_amount",
)
_CREDIT_ASSET_FIELDS = frozenset(
    (*_CREDIT_ASSURE_BUYING_POWER_FIELDS,
     *_CREDIT_FINANCING_BUYING_POWER_FIELDS,
     *_CREDIT_FINANCING_AVAILABLE_FIELDS),
)
_CREDIT_BUY_MODES = frozenset({"collateral", "financing"})


def _first_number(row: dict[str, Any], fields: tuple[str, ...]) -> float | None:
    for field in fields:
        value = _float(row.get(field))
        if value is not None and value >= 0 and value < 1e307:
            return value
    return None


def _credit_buying_power(
    account: dict[str, Any], credit_buy_mode: str = "collateral",
) -> tuple[float | None, str | None, float | None]:
    """Return the selected credit buying power, label and financing quota."""
    if credit_buy_mode not in _CREDIT_BUY_MODES:
        raise ValueError("信用账户买入方式必须是担保品买入或融资买入")
    financing_available = _first_number(account, _CREDIT_FINANCING_AVAILABLE_FIELDS)
    if credit_buy_mode == "financing":
        return (
            _first_number(account, _CREDIT_FINANCING_BUYING_POWER_FIELDS),
            "可买融资标的资金",
            financing_available,
        )
    return (
        _first_number(account, _CREDIT_ASSURE_BUYING_POWER_FIELDS),
        "可买担保品资金",
        financing_available,
    )


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
    account["account_type"] = str(account_type or asset.get("account_type") or "STOCK").upper()
    return account


_ALLOCATION_RATIOS = {
    "sixth": 1 / 6,
    "fifth": 0.2,
    "quarter": 0.25,
    "third": 1 / 3,
    "half": 0.5,
    "available": 1.0,
}
# ``lot`` is an internal compatibility mode for the old automatic board
# order setting where zero amount meant one 100-share lot.
_ALLOCATION_MODES = frozenset((*_ALLOCATION_RATIOS, "fixed", "lot"))


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
        self.enabled = bool(getattr(settings, "qmt_enabled", False))
        self.connect_address = str(
            getattr(settings, "qmt_zmq_connect_address", "") or "",
        ).strip()
        self.account_id = str(getattr(settings, "qmt_account_id", "") or "").strip()
        self.account_type = str(getattr(settings, "qmt_account_type", "STOCK") or "STOCK").upper()
        self.timeout = max(1.0, float(getattr(settings, "qmt_rpc_timeout_seconds", 6.0)))
        self._context = None
        self._dealer = None
        self._lock = threading.Lock()

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.connect_address and self.account_id and zmq is not None)

    @property
    def configuration_reason(self) -> str:
        if not self.enabled:
            return "QMT_ENABLED 未开启"
        missing = []
        if not self.connect_address:
            missing.append("QMT_ZMQ_CONNECT_ADDRESS")
        if not self.account_id:
            missing.append("QMT_ACCOUNT_ID")
        if missing:
            return "缺少 " + ", ".join(missing)
        return "后端未安装 pyzmq 客户端" if zmq is None else "已配置"

    def _ensure_dealer(self):
        if not self.configured:
            raise QmtRpcError(self.configuration_reason)
        with self._lock:
            return self._ensure_dealer_locked()

    def _ensure_dealer_locked(self):
        if self._dealer is not None:
            return self._dealer
        if zmq is None:  # pragma: no cover - guarded by configured
            raise QmtRpcError("后端未安装 pyzmq 客户端")
        if self._context is None:
            self._context = zmq.Context.instance()
        dealer = self._context.socket(zmq.DEALER)
        dealer.setsockopt(zmq.IDENTITY, uuid.uuid4().hex[:16].encode("ascii"))
        dealer.setsockopt(zmq.LINGER, 0)
        dealer.connect(self.connect_address)
        self._dealer = dealer
        return dealer

    def _close_dealer_locked(self) -> None:
        if self._dealer is None:
            return
        try:
            self._dealer.close(linger=0)
        finally:
            self._dealer = None

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        if not self.configured:
            raise QmtRpcError(self.configuration_reason)
        request_id = uuid.uuid4().hex
        request = {
            "schema_version": 1,
            "request_id": request_id,
            "account_id": self.account_id,
            "method": method,
            "params": params or {},
            "ttl_seconds": max(60, int(self.timeout) + 30),
        }
        with self._lock:
            dealer = self._ensure_dealer_locked()
            try:
                dealer.send(_encode_zmq_payload(request))
                poller = zmq.Poller()
                poller.register(dealer, zmq.POLLIN)
                deadline = time.monotonic() + self.timeout
                response = None
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    events = dict(poller.poll(timeout=max(1, math.ceil(remaining * 1000))))
                    if dealer not in events:
                        continue
                    candidate = _decode_zmq_payload(dealer.recv())
                    if not isinstance(candidate, dict):
                        continue
                    if str(candidate.get("request_id") or "") != request_id:
                        continue
                    response = candidate
                    break
            except QmtRpcError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._close_dealer_locked()
                raise QmtRpcError(f"QMT ZMQ RPC 连接失败: {exc.__class__.__name__}") from exc
            if response is None:
                self._close_dealer_locked()
                raise QmtRpcError(f"QMT RPC 超时: {method}")
        if not response.get("ok"):
            error = response.get("error") or f"QMT RPC {method} 失败"
            raise QmtRpcError(str(error))
        server_error = str(response.get("server_error") or "").strip()
        if server_error:
            raise QmtRpcError(f"QMT {method} server_error: {server_error}")
        return response.get("data")

    def close(self) -> None:
        with self._lock:
            self._close_dealer_locked()

    def probe(self) -> dict[str, Any]:
        data = self.call("ping")
        if isinstance(data, dict) and data.get("account_id") and str(data["account_id"]) != self.account_id:
            raise QmtRpcError("QMT RPC 返回的账户与本地配置不一致")
        return {"server_time": data.get("server_time") if isinstance(data, dict) else None}

    def snapshot(self) -> dict[str, Any]:
        """同一轮读取账户、持仓、委托和成交；任一步失败则整轮失败。"""
        self.probe()
        asset = self.call("get_asset", {"account_id": self.account_id})
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
            cost = _float(item.get("cost") or item.get("cost_price") or item.get("open_price"))
            if not symbol or volume < 0 or available < 0:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            if volume == 0:
                if available != 0:
                    raise QmtRpcError(f"QMT 空持仓可用数量不为零: {symbol}")
                continue
            if cost is None or cost <= 0:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            if available > volume:
                raise QmtRpcError(f"QMT 可用数量大于持仓数量: {symbol}")
            normalized_positions.append({
                "symbol": symbol,
                "name": str(item.get("stock_name") or item.get("name") or symbol),
                "quantity": volume,
                "available": available,
                "cost_price": cost,
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
        self._last_status: dict[str, Any] = {}
        self._last_snapshot: dict[str, Any] | None = None
        self._last_snapshot_monotonic = 0.0
        self._orders: dict[str, dict[str, Any]] = {}
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

    def status(self) -> dict[str, Any]:
        with self._lock:
            cached_account = dict(self._last_snapshot.get("account") or {}) if self._last_snapshot else None
            status = {
                "configured": self.client.configured,
                "trade_authorized": self.trade_authorized,
                "trade_enabled": self.trade_enabled,
                "account_id": self.client.account_id or None,
                "rpc_transport": "zmq",
                "rpc_address": self.client.connect_address or None,
                "account_type": self.account_type,
                "auto_sync_enabled": self.auto_sync_enabled,
                "auto_sync_running": bool(self._auto_thread and self._auto_thread.is_alive()),
                "auto_sync_interval_seconds": self.auto_sync_interval,
                "last_probe_at": self._last_status.get("last_probe_at"),
                "last_sync_at": self._last_snapshot.get("synced_at") if self._last_snapshot else None,
                "account": cached_account or None,
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
        result = self.client.call("query_orders", {"account_id": self.client.account_id, "strategy_name": ""})
        if not isinstance(result, list):
            raise QmtRpcError("QMT 委托响应格式无效")
        return [self._normalize_remote_order(item) for item in result if isinstance(item, dict)]

    def probe(self) -> dict[str, Any]:
        started = time.monotonic()
        try:
            result = self.client.probe()
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self._last_status = {"state": "error", "reason": str(exc), "last_probe_at": _now()}
            raise
        with self._lock:
            self._last_status = {
                "state": "ready", "reason": "QMT RPC 在线", "last_probe_at": _now(),
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
            }
        return {**self.status(), **result}

    def sync(self) -> dict[str, Any]:
        started = time.monotonic()
        try:
            snapshot = self.client.snapshot()
            remote_orders = snapshot.get("orders") or []
            if isinstance(remote_orders, list):
                snapshot["orders"] = self._merge_remote_orders([
                    item for item in remote_orders if isinstance(item, dict)
                ])
        except Exception as exc:
            with self._lock:
                self._last_status = {"state": "error", "reason": str(exc), "last_probe_at": _now()}
            raise
        with self._lock:
            self._last_snapshot = snapshot
            self._last_snapshot_monotonic = time.monotonic()
            self._last_status = {
                "state": "ready",
                "reason": "QMT账户正在自动同步" if self._auto_thread else "账户、持仓和委托已同步",
                "last_probe_at": _now(),
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
            }
        return snapshot

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
        if not self.auto_sync_enabled or not self.client.configured:
            return False
        with self._lock:
            if self._auto_thread and self._auto_thread.is_alive():
                return True
            self._auto_stop.clear()

            def run() -> None:
                while not self._auto_stop.is_set():
                    try:
                        self.sync_into(position_risk_service)
                    except Exception:  # 状态由 sync 记录，下一轮自动重试
                        pass
                    if self._auto_stop.wait(self.auto_sync_interval):
                        break

            self._auto_thread = threading.Thread(target=run, name="qmt-auto-sync", daemon=True)
            self._auto_thread.start()
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
        if enabled:
            if not self.trade_authorized:
                raise QmtRpcError("QMT_TRADE_ENABLED 未授权真实交易")
            if not self.client.configured:
                raise QmtRpcError(self.client.configuration_reason)
            with self._lock:
                if self._last_snapshot is None or self._last_status.get("state") != "ready":
                    raise QmtRpcError("请先成功同步 QMT 权威账户，再开启真实交易")
        with self._lock:
            self.trade_enabled = bool(enabled)
        return self.status()

    def _buying_power(self, account: dict[str, Any], credit_buy_mode: str = "collateral") -> tuple[float | None, str, float | None]:
        """Resolve the amount that a BUY order may actually consume."""
        if self.account_type == "CREDIT":
            amount, label, financing_available = _credit_buying_power(account, credit_buy_mode)
            return amount, label or "信用账户可买额度", financing_available
        return _float(account.get("cash")), "可用资金", None

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
        if action == "SELL":
            positions = snapshot.get("positions") or []
            row = next((item for item in positions if item.get("symbol") == symbol), None)
            if not row or int(row.get("available") or 0) < volume:
                raise ValueError("QMT 可用持仓不足，已拒绝卖出")
        elif price_type == "LIMIT":
            account = snapshot.get("account") or {}
            buying_power, _label, _financing_available = self._buying_power(account, credit_buy_mode)
            if buying_power is None:
                if self.account_type == "CREDIT":
                    raise QmtRpcError("QMT 未返回信用账户可买额度，已拒绝买入")
            elif buying_power < price * volume:
                raise ValueError("QMT 可用资金不足，已拒绝买入")
        return {
            "action": action,
            "symbol": symbol,
            "volume": volume,
            "price": price,
            "price_type": price_type,
            "credit_buy_mode": credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
        }

    def _allocation_preview(
        self,
        request: dict[str, Any],
        snapshot: dict[str, Any],
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
            raise ValueError("金额分配方式必须是当前可用金额、可用金额六分之一、五分之一、四分之一、三分之一、二分之一、固定金额或一手模式")

        financing_available = None
        credit_buy_mode = str(request.get("credit_buy_mode") or "collateral").strip().lower()
        if action == "BUY":
            account = snapshot.get("account") or {}
            basis_amount, basis_label, financing_available = self._buying_power(account, credit_buy_mode)
            if basis_amount is None or basis_amount < 0:
                if self.account_type == "CREDIT":
                    raise QmtRpcError("QMT 未返回信用账户可买额度，无法把现金余额当作融资额度")
                raise QmtRpcError("QMT 可用资金无效，无法计算委托金额")
            available_volume = None
        else:
            row = next(
                (item for item in snapshot.get("positions") or [] if item.get("symbol") == symbol),
                None,
            )
            available_volume = int((row or {}).get("available") or 0)
            if row is None or available_volume <= 0:
                raise ValueError("QMT 可用持仓不足，无法计算卖出金额")
            basis_amount = available_volume * price
            basis_label = "可用持仓市值"

        if mode == "lot":
            requested_amount = price * 100
        elif mode == "fixed":
            requested_amount = _float(request.get("allocation_value"))
            if requested_amount is None or requested_amount <= 0:
                raise ValueError("固定金额必须大于 0")
        else:
            requested_amount = basis_amount * _ALLOCATION_RATIOS[mode]
        target_amount = min(requested_amount, basis_amount)
        volume = int(target_amount / price / 100) * 100
        if available_volume is not None:
            volume = min(volume, (available_volume // 100) * 100)
        actual_amount = round(volume * price, 2)
        return {
            "action": action,
            "symbol": symbol,
            "price": price,
            "price_type": price_type,
            "credit_buy_mode": credit_buy_mode if self.account_type == "CREDIT" and action == "BUY" else None,
            "allocation_mode": mode,
            "allocation_value": requested_amount if mode == "fixed" else None,
            "basis_label": basis_label,
            "basis_amount": round(basis_amount, 2),
            "cash_amount": _float((snapshot.get("account") or {}).get("cash")),
            "financing_available_amount": financing_available,
            "buying_power_amount": round(basis_amount, 2),
            "target_amount": round(target_amount, 2),
            "actual_amount": actual_amount,
            "volume": volume,
            "available_volume": available_volume,
            "capped": target_amount < requested_amount or volume * price < target_amount,
            "reason": "金额不足一手" if volume < 100 else None,
        }

    def preview_order(self, request: dict[str, Any]) -> dict[str, Any]:
        action = str(request.get("action") or "").upper()
        # Preview reads the auto-sync snapshot for responsiveness; submission
        # still performs a fresh QMT preflight before sending the order.
        with self._lock:
            cached_snapshot = self._last_snapshot
            snapshot_age = time.monotonic() - self._last_snapshot_monotonic
        if cached_snapshot is not None and snapshot_age <= self.auto_sync_interval:
            if action == "BUY":
                snapshot = {
                    "account": dict(cached_snapshot.get("account") or {}),
                    "positions": [],
                }
            elif action == "SELL":
                snapshot = {
                    "account": {},
                    "positions": [
                        {"symbol": item.get("symbol"), "available": item.get("available", 0)}
                        for item in cached_snapshot.get("positions") or []
                    ],
                }
            else:
                snapshot = self._order_preflight(action)
        else:
            snapshot = self._order_preflight(action)
        return self._allocation_preview(request, snapshot)

    def _order_preflight(self, action: str) -> dict[str, Any]:
        if action == "BUY":
            asset = self.client.call("get_asset", {"account_id": self.client.account_id})
            if not isinstance(asset, dict):
                raise QmtRpcError("QMT 资产响应格式无效")
            return {"account": _normalise_account(asset, self.account_type), "positions": []}
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
            except (TypeError, ValueError) as exc:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}") from exc
            if not symbol or available < 0:
                raise QmtRpcError(f"QMT 持仓字段无效: {symbol or code}")
            positions.append({"symbol": symbol, "available": available})
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
                snapshot = self._order_preflight(action)
                allocation = None
                if request.get("allocation_mode"):
                    allocation = self._allocation_preview(request, snapshot)
                    request = {**request, "volume": allocation["volume"]}
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
                params["credit_buy_mode"] = str(request.get("credit_buy_mode") or "collateral").strip().lower()
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
            self._remember_order(row)
            row["qmt_submit_at"] = _now()
            try:
                response = self.client.call(
                    "submit_orders_batch",
                    {"account_id": self.client.account_id, "strategy_name": strategy_name, "batch_id": idempotency_key, "orders": [params]},
                )
            except Exception as exc:
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
