"""JoinQuant source facade backed by the canonical free-strategy engine.

This module deliberately adapts source-facing objects only.  It never changes
the engine's event order, account model, or matching implementation.
"""
from __future__ import annotations

import builtins
import copy
import math
import numbers
from collections.abc import Iterator, Mapping
from datetime import date, datetime, time as datetime_time, timedelta
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, Iterable

import pandas as pd

from .capabilities import (
    COMPATIBILITY_VERSION,
    analyze_source,
    ensure_executable,
)


_INTERNAL_SUFFIXES = {".XSHG": ".SH", ".XSHE": ".SZ", ".XBSE": ".BJ"}
_JQ_SUFFIXES = {".SH": ".XSHG", ".SZ": ".XSHE", ".BJ": ".XBSE"}
_TIME_ALIASES = {
    "before_open": "09:00",
    "open": "09:30",
    "market_open": "09:30",
    "close": "15:00",
    "market_close": "15:00",
    "after_close": "15:01",
}
_DEFAULT_PRICE_FIELDS = ("open", "close", "high", "low", "volume", "money")


class JoinQuantRuntimeError(ValueError):
    """An API is known but cannot be represented by the local runtime."""


def _normalize_symbol(value: Any) -> str:
    symbol = str(value).strip().upper()
    for source, target in _INTERNAL_SUFFIXES.items():
        if symbol.endswith(source):
            return f"{symbol[:-len(source)]}{target}"
    return symbol


def _jq_symbol(value: Any) -> str:
    symbol = _normalize_symbol(value)
    for source, target in _JQ_SUFFIXES.items():
        if symbol.endswith(source):
            return f"{symbol[:-len(source)]}{target}"
    return symbol


def _date_value(value: Any, *, end_of_day: bool = False) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=None)
    if isinstance(value, date):
        return datetime.combine(value, datetime_time(15 if end_of_day else 0, 0))
    text = str(value).strip()
    parsed = datetime.fromisoformat(text)
    if "T" not in text and " " not in text:
        return datetime.combine(parsed.date(), datetime_time(15 if end_of_day else 0, 0))
    return parsed.replace(tzinfo=None)


def _period(value: Any) -> str:
    normalized = str(value or "1d").strip().lower()
    aliases = {
        "1d": "1d", "d": "1d", "day": "1d", "daily": "1d",
        "1m": "1m", "minute": "1m", "1min": "1m",
        "5m": "5m", "5min": "5m",
        "30m": "30m", "30min": "30m",
    }
    if normalized not in aliases:
        raise JoinQuantRuntimeError(f"聚宽兼容层暂不支持 frequency/unit={value!r}")
    return aliases[normalized]


def _json_safe_state(value: Any, path: str = "g") -> None:
    """Reject non-persistable g state before a checkpoint silently loses it."""
    if value is None or isinstance(value, (str, int, float, bool, numbers.Real)):
        return
    if isinstance(value, (date, datetime)):
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _json_safe_state(item, f"{path}[{index}]")
        return
    if isinstance(value, tuple):
        for index, item in enumerate(value):
            _json_safe_state(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise JoinQuantRuntimeError(f"{path} 只允许字符串键以便检查点恢复")
            _json_safe_state(item, f"{path}.{key}")
        return
    raise JoinQuantRuntimeError(
        f"{path} 包含无法持久化的 {type(value).__name__}，请改为 dict/list/基础类型"
    )


_STATE_TYPE_KEY = "__tickflow_joinquant_type__"


def _encode_state(value: Any, path: str = "g") -> Any:
    _json_safe_state(value, path)
    if isinstance(value, datetime):
        return {_STATE_TYPE_KEY: "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {_STATE_TYPE_KEY: "date", "value": value.isoformat()}
    if isinstance(value, numbers.Integral) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, numbers.Real) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, list):
        return [_encode_state(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, tuple):
        return [_encode_state(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, dict):
        return {key: _encode_state(item, f"{path}.{key}") for key, item in value.items()}
    return value


def _decode_state(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_state(item) for item in value]
    if not isinstance(value, dict):
        return value
    marker = value.get(_STATE_TYPE_KEY)
    if marker == "datetime":
        return datetime.fromisoformat(str(value["value"]))
    if marker == "date":
        return date.fromisoformat(str(value["value"]))
    return {key: _decode_state(item) for key, item in value.items()}


class _GlobalState:
    def __init__(self, native_context: Any) -> None:
        object.__setattr__(self, "_native_context", native_context)

    def _values(self) -> dict[str, Any]:
        state = self._native_context.state
        values = state.setdefault("__joinquant_g__", {})
        if not isinstance(values, dict):
            raise JoinQuantRuntimeError("检查点中的 __joinquant_g__ 必须是对象")
        return values

    def __getattr__(self, name: str) -> Any:
        try:
            return self._values()[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        _json_safe_state(value, f"g.{name}")
        self._values()[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self._values()[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    @property
    def __dict__(self) -> dict[str, Any]:
        return self._values()


class _Position:
    def __init__(self, engine: Any, symbol: str) -> None:
        self._engine = engine
        self._symbol = symbol

    @property
    def security(self) -> str:
        return _jq_symbol(self._symbol)

    @property
    def total_amount(self) -> float:
        return float(self._engine.account.positions.get(self._symbol, 0.0))

    @property
    def closeable_amount(self) -> float:
        return float(self._engine.account.available.get(self._symbol, self.total_amount))

    @property
    def avg_cost(self) -> float:
        return float(self._engine.account.avg_cost.get(self._symbol, 0.0))

    @property
    def price(self) -> float:
        return float(self._engine._current_close_prices.get(self._symbol, self.avg_cost))  # noqa: SLF001

    @property
    def value(self) -> float:
        return self.total_amount * self.price

    @property
    def init_time(self) -> datetime | None:
        lots = self._engine._position_lots.get(self._symbol, [])  # noqa: SLF001
        if not lots:
            return None
        return datetime.combine(min(item["acquired"] for item in lots), datetime_time())


class _Positions(Mapping[str, _Position]):
    def __init__(self, engine: Any) -> None:
        self._engine = engine

    def __getitem__(self, symbol: str) -> _Position:
        normalized = _normalize_symbol(symbol)
        if self._engine.account.positions.get(normalized, 0.0) <= 0:
            raise KeyError(symbol)
        return _Position(self._engine, normalized)

    def __iter__(self) -> Iterator[str]:
        return iter(_jq_symbol(symbol) for symbol, quantity in self._engine.account.positions.items() if quantity > 0)

    def __len__(self) -> int:
        return sum(quantity > 0 for quantity in self._engine.account.positions.values())


class _Portfolio:
    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self._positions = _Positions(engine)

    @property
    def available_cash(self) -> float:
        return float(self._engine.account.cash)

    @property
    def cash(self) -> float:
        return self.available_cash

    @property
    def positions(self) -> Mapping[str, _Position]:
        return self._positions

    @property
    def long_positions(self) -> Mapping[str, _Position]:
        return self._positions

    @property
    def total_value(self) -> float:
        return float(self._engine.account.equity(self._engine._current_close_prices))  # noqa: SLF001

    @property
    def portfolio_value(self) -> float:
        return self.total_value


class _SecurityInfo(SimpleNamespace):
    pass


class _BarData:
    def __init__(self, runtime: "JoinQuantRuntime", symbol: str, bar: Any) -> None:
        self._runtime = runtime
        self._symbol = symbol
        self._bar = bar

    @property
    def code(self) -> str:
        return _jq_symbol(self._symbol)

    @property
    def open(self) -> float:
        return float(self._bar.open)

    @property
    def high(self) -> float:
        return float(self._bar.high)

    @property
    def low(self) -> float:
        return float(self._bar.low)

    @property
    def close(self) -> float:
        return float(self._bar.close)

    @property
    def last_price(self) -> float:
        return self.close

    @property
    def day_open(self) -> float:
        return self.open

    @property
    def volume(self) -> float:
        return float(self._bar.volume)

    @property
    def money(self) -> float:
        return float(self._bar.amount)

    @property
    def high_limit(self) -> float | None:
        return self._bar.limit_up

    @property
    def low_limit(self) -> float | None:
        return self._bar.limit_down

    @property
    def paused(self) -> bool:
        return bool(self._bar.suspended or not self._bar.tradable)

    @property
    def is_st(self) -> bool:
        name = self.name.upper()
        return "ST" in name or "退" in self.name

    @property
    def name(self) -> str:
        return self._runtime.instrument_name(self._symbol)

    @property
    def pre_close(self) -> float | None:
        return self._bar.previous_close

    @property
    def avg(self) -> float:
        return self.money / self.volume if self.volume > 0 else self.close


class _CurrentData(Mapping[str, _BarData]):
    def __init__(self, runtime: "JoinQuantRuntime", bars: Mapping[str, Any] | None = None) -> None:
        self._runtime = runtime
        self._bars = bars

    def _bar_for(self, symbol: str) -> Any:
        normalized = _normalize_symbol(symbol)
        values = self._bars if self._bars is not None else self._runtime.engine.context.current_bars()
        bar = values.get(normalized)
        if bar is None:
            raise KeyError(symbol)
        return bar

    def __getitem__(self, symbol: str) -> _BarData:
        normalized = _normalize_symbol(symbol)
        return _BarData(self._runtime, normalized, self._bar_for(normalized))

    def __iter__(self) -> Iterator[str]:
        values = self._bars if self._bars is not None else self._runtime.engine.context.current_bars()
        return iter(_jq_symbol(symbol) for symbol in values)

    def __len__(self) -> int:
        values = self._bars if self._bars is not None else self._runtime.engine.context.current_bars()
        return len(values)


class _BarDataView(_CurrentData):
    def current(self, security: str, field: str) -> Any:
        value = self[security]
        if field == "money":
            return value.money
        if field == "high_limit":
            return value.high_limit
        if field == "low_limit":
            return value.low_limit
        try:
            return getattr(value, field)
        except AttributeError as exc:
            raise JoinQuantRuntimeError(f"data.current 不支持字段 {field}") from exc


class _DataFacade:
    def __init__(self, runtime: "JoinQuantRuntime") -> None:
        self.runtime = runtime

    @property
    def engine(self) -> Any:
        return self.runtime.engine

    def _cutoff(self, end_date: Any | None) -> datetime:
        now = self.engine.context.now
        if now is None:
            raise JoinQuantRuntimeError("聚宽数据 API 只能在策略回调中调用")
        cutoff = now
        if end_date is not None:
            requested = _date_value(end_date, end_of_day=True)
            date_only = (
                isinstance(end_date, date) and not isinstance(end_date, datetime)
            ) or (
                isinstance(end_date, str)
                and "T" not in end_date
                and " " not in end_date
            )
            if requested > now:
                if date_only and requested.date() == now.date():
                    cutoff = now
                else:
                    raise JoinQuantRuntimeError("聚宽数据 API 不允许读取当前策略时点之后的行情")
            else:
                cutoff = requested
        return cutoff

    def _history(self, symbol: str, count: int, period: str, cutoff: datetime) -> list[Any]:
        if count <= 0:
            return []
        normalized = _normalize_symbol(symbol)
        loader = self.engine._history_loader  # noqa: SLF001
        if loader is not None:
            values = loader(normalized, count, period, cutoff)
        else:
            values = list(self.engine._history_by_period.get(period, {}).get(normalized, []))  # noqa: SLF001
            if period == self.engine.timeframe:
                values.extend(self.engine.history.get(normalized, []))
                values = sorted(
                    {bar.timestamp: bar for bar in values}.values(),
                    key=lambda bar: bar.timestamp,
                )
        visible = [bar for bar in values if bar.timestamp <= cutoff][-count:]
        self.engine.market_rows_consumed += len(visible)
        return visible

    @staticmethod
    def _field_value(bar: Any, field: str, fq: Any) -> Any:
        if field in {"open", "high", "low", "close"}:
            if fq in {None, "none"}:
                return bar.execution_price(field)
            return getattr(bar, field)
        if field == "volume":
            return bar.volume
        if field == "money":
            return bar.amount
        if field == "pre_close":
            return bar.previous_close
        if field == "avg":
            return bar.amount / bar.volume if bar.volume else bar.close
        if field == "high_limit":
            return bar.limit_up
        if field == "low_limit":
            return bar.limit_down
        if field == "paused":
            return bool(bar.suspended or not bar.tradable)
        raise JoinQuantRuntimeError(f"聚宽数据 API 不支持字段 {field}")

    @staticmethod
    def _fields(value: Any) -> tuple[str, ...]:
        if value is None:
            return _DEFAULT_PRICE_FIELDS
        if isinstance(value, str):
            return (value,)
        return tuple(str(item) for item in value)

    def get_price(
        self,
        security: str | Iterable[str],
        start_date: Any | None = None,
        end_date: Any | None = None,
        frequency: str = "daily",
        fields: str | Iterable[str] | None = None,
        skip_paused: bool = False,
        fq: str | None = "pre",
        count: int | None = None,
        panel: bool = True,
        fill_paused: bool | None = None,
        **_kwargs: Any,
    ) -> pd.DataFrame:
        del skip_paused, fill_paused
        if fq not in {"pre", None, "none"}:
            raise JoinQuantRuntimeError("聚宽兼容层仅支持 fq='pre' 或 fq=None")
        period = _period(frequency)
        selected_fields = self._fields(fields)
        cutoff = self._cutoff(end_date)
        symbols = [security] if isinstance(security, str) else list(security)
        if not symbols:
            return pd.DataFrame(columns=list(selected_fields))
        if start_date is not None:
            start = _date_value(start_date)
            if start > cutoff:
                raise JoinQuantRuntimeError("start_date 不能晚于 end_date")
            inferred = max(1, (cutoff.date() - start.date()).days * (240 if period == "1m" else 2) + 10)
            limit = max(int(count or 0), inferred)
        else:
            limit = int(count or 1)
        if limit <= 0:
            raise JoinQuantRuntimeError("count 必须是正整数")

        values_by_symbol = {
            _normalize_symbol(symbol): self._history(str(symbol), limit, period, cutoff)
            for symbol in symbols
        }
        if start_date is not None:
            values_by_symbol = {
                symbol: [bar for bar in values if bar.timestamp >= start]
                for symbol, values in values_by_symbol.items()
            }
        if len(values_by_symbol) == 1:
            values = next(iter(values_by_symbol.values()))
            records = {
                field: [self._field_value(bar, field, fq) for bar in values]
                for field in selected_fields
            }
            return pd.DataFrame(records, index=pd.DatetimeIndex([bar.timestamp for bar in values]))
        if panel:
            raise JoinQuantRuntimeError("多标的 get_price 仅支持 panel=False")
        rows = []
        for symbol, values in values_by_symbol.items():
            for bar in values:
                rows.append({
                    "time": bar.timestamp,
                    "code": _jq_symbol(symbol),
                    **{field: self._field_value(bar, field, fq) for field in selected_fields},
                })
        return pd.DataFrame(rows, columns=["time", "code", *selected_fields])

    def attribute_history(
        self,
        security: str,
        count: int,
        unit: str = "1d",
        fields: str | Iterable[str] | None = None,
        skip_paused: bool = True,
        df: bool = True,
        fq: str | None = "pre",
        **kwargs: Any,
    ) -> pd.DataFrame | dict[str, Any]:
        frame = self.get_price(
            security,
            frequency=unit,
            count=count,
            fields=fields,
            skip_paused=skip_paused,
            fq=fq,
            **kwargs,
        )
        if df:
            return frame
        return {name: frame[name].to_numpy() for name in frame.columns}

    def history(
        self,
        count: int,
        unit: str = "1d",
        field: str | Iterable[str] = "close",
        security_list: Iterable[str] | None = None,
        df: bool = True,
        skip_paused: bool = True,
        fq: str | None = "pre",
        **kwargs: Any,
    ) -> pd.DataFrame | dict[str, Any]:
        del skip_paused, kwargs
        if fq not in {"pre", None, "none"}:
            raise JoinQuantRuntimeError("聚宽兼容层仅支持 fq='pre' 或 fq=None")
        period = _period(unit)
        cutoff = self._cutoff(None)
        fields = self._fields(field)
        symbols = list(security_list or self.engine.context.universe)
        rows: dict[str, pd.Series[Any]] = {}
        for raw_symbol in symbols:
            symbol = _normalize_symbol(raw_symbol)
            values = self._history(symbol, count, period, cutoff)
            timestamps = pd.DatetimeIndex([bar.timestamp for bar in values])
            if len(fields) == 1:
                rows[_jq_symbol(symbol)] = pd.Series(
                    [self._field_value(bar, fields[0], fq) for bar in values],
                    index=timestamps,
                )
            else:
                for field_name in fields:
                    rows[f"{_jq_symbol(symbol)}:{field_name}"] = pd.Series(
                        [self._field_value(bar, field_name, fq) for bar in values],
                        index=timestamps,
                    )
        frame = pd.DataFrame(rows)
        if df:
            return frame
        return {name: frame[name].to_numpy() for name in frame.columns}

    def get_current_data(self) -> Mapping[str, _BarData]:
        return _CurrentData(self.runtime)

    def get_all_securities(
        self,
        types: str | Iterable[str] | None = None,
        date: Any | None = None,
    ) -> pd.DataFrame:
        del date
        requested = [types] if isinstance(types, str) else list(types or ["stock"])
        asset_types = {
            "fund": "etf",
            "stock": "stock",
            "index": "index",
        }
        accepted = {asset_types[item] for item in requested if item in asset_types}
        rows = []
        for item in self.engine.context.instruments():
            if accepted and item.get("asset_type") not in accepted:
                continue
            symbol = _normalize_symbol(item.get("symbol", ""))
            if not symbol:
                continue
            rows.append({
                "code": _jq_symbol(symbol),
                "display_name": str(item.get("name") or item.get("display_name") or symbol),
                "name": str(item.get("name") or item.get("display_name") or symbol),
                "start_date": item.get("list_date") or item.get("listing_date"),
                "end_date": item.get("delist_date") or datetime.max.date(),
                "type": "fund" if item.get("asset_type") == "etf" else item.get("asset_type"),
            })
        frame = pd.DataFrame(rows)
        return frame.set_index("code") if not frame.empty else pd.DataFrame(
            columns=["display_name", "name", "start_date", "end_date", "type"]
        )

    def get_security_info(self, security: str) -> _SecurityInfo:
        symbol = _normalize_symbol(security)
        item = next(
            (record for record in self.engine.context.instruments() if _normalize_symbol(record.get("symbol", "")) == symbol),
            None,
        )
        if item is None:
            raise JoinQuantRuntimeError(f"证券目录没有 {security}")
        asset_type = str(item.get("asset_type") or "stock")
        return _SecurityInfo(
            code=_jq_symbol(symbol),
            display_name=str(item.get("name") or item.get("display_name") or symbol),
            name=str(item.get("name") or item.get("display_name") or symbol),
            start_date=item.get("list_date") or item.get("listing_date"),
            end_date=item.get("delist_date") or datetime.max.date(),
            type="fund" if asset_type == "etf" else asset_type,
            parent=None,
        )


class _OrderResult:
    def __init__(self, order: Any | None) -> None:
        self._order = order

    @property
    def order_id(self) -> str | None:
        return self._order.id if self._order is not None else None

    @property
    def security(self) -> str | None:
        return _jq_symbol(self._order.symbol) if self._order is not None else None

    @property
    def amount(self) -> float | None:
        return self._order.requested_quantity if self._order is not None else None

    @property
    def status(self) -> str | None:
        return self._order.status if self._order is not None else None


class OrderCost:
    def __init__(self, **values: Any) -> None:
        self.values = values


class FixedSlippage:
    def __init__(self, value: float) -> None:
        self.value = float(value)
        if not math.isfinite(self.value) or self.value < 0:
            raise JoinQuantRuntimeError("FixedSlippage 必须是非负有限数")


class PriceRelatedSlippage:
    def __init__(self, rate: float) -> None:
        self.rate = float(rate)
        if not math.isfinite(self.rate) or self.rate < 0:
            raise JoinQuantRuntimeError("PriceRelatedSlippage 必须是非负有限数")


class _Orders:
    def __init__(self, runtime: "JoinQuantRuntime") -> None:
        self.runtime = runtime

    @property
    def engine(self) -> Any:
        return self.runtime.engine

    @staticmethod
    def _validate_style(style: Any, side: str, pindex: int) -> None:
        if style is not None:
            raise JoinQuantRuntimeError("聚宽兼容层暂不支持自定义 OrderStyle")
        if side not in {"long", None}:
            raise JoinQuantRuntimeError("聚宽兼容层暂不支持融资融券或空头订单")
        if pindex not in {0, None}:
            raise JoinQuantRuntimeError("聚宽兼容层只支持默认子账户 pindex=0")

    def _result(self, side: str, security: str, **kwargs: Any) -> _OrderResult:
        order = self.engine.submit_order(side, _normalize_symbol(security), **kwargs)
        return _OrderResult(order)

    def order(
        self,
        security: str,
        amount: float,
        style: Any = None,
        side: str = "long",
        pindex: int = 0,
        **_kwargs: Any,
    ) -> _OrderResult:
        self._validate_style(style, side, pindex)
        quantity = float(amount)
        if quantity == 0:
            return _OrderResult(None)
        return self._result("buy" if quantity > 0 else "sell", security, quantity=abs(quantity))

    def order_value(
        self,
        security: str,
        value: float,
        style: Any = None,
        side: str = "long",
        pindex: int = 0,
        **_kwargs: Any,
    ) -> _OrderResult:
        self._validate_style(style, side, pindex)
        amount = float(value)
        if amount == 0:
            return _OrderResult(None)
        return self._result("buy" if amount > 0 else "sell", security, value=abs(amount))

    def order_target(
        self,
        security: str,
        amount: float,
        style: Any = None,
        pindex: int = 0,
        **_kwargs: Any,
    ) -> _OrderResult:
        self._validate_style(style, "long", pindex)
        target = float(amount)
        if target < 0:
            raise JoinQuantRuntimeError("聚宽兼容层暂不支持负目标仓位")
        return self._result("target", security, target_quantity=target)

    def order_target_value(
        self,
        security: str,
        value: float,
        style: Any = None,
        pindex: int = 0,
        **_kwargs: Any,
    ) -> _OrderResult:
        self._validate_style(style, "long", pindex)
        target = float(value)
        if target < 0:
            raise JoinQuantRuntimeError("聚宽兼容层暂不支持负目标市值")
        return self._result("target", security, target_value=target)

    def order_target_percent(
        self,
        security: str,
        percent: float,
        style: Any = None,
        pindex: int = 0,
        **_kwargs: Any,
    ) -> _OrderResult:
        self._validate_style(style, "long", pindex)
        target = float(percent)
        if not 0 <= target <= 1:
            raise JoinQuantRuntimeError("目标仓位比例必须在 0 到 1 之间")
        return self._result("target", security, target_percent=target)

    def set_order_cost(self, cost: OrderCost, type: str = "stock") -> None:  # noqa: A002
        if not isinstance(cost, OrderCost):
            raise JoinQuantRuntimeError("set_order_cost 仅支持 OrderCost")
        expected = "fund" if self.engine.config.asset_type == "etf" else self.engine.config.asset_type
        if type not in {expected, "stock" if expected == "stock" else "fund"}:
            raise JoinQuantRuntimeError(f"订单费率类型 {type!r} 与当前资产类型不匹配")
        values = cost.values
        unsupported = set(values) - {
            "open_commission", "close_commission", "close_tax", "min_commission",
        }
        if unsupported:
            raise JoinQuantRuntimeError(f"OrderCost 包含不支持字段: {', '.join(sorted(unsupported))}")
        if "open_commission" in values:
            self.engine.config.commission_pct = float(values["open_commission"])
        if "close_commission" in values:
            self.engine.config.sell_commission_pct = float(values["close_commission"])
        if "close_tax" in values:
            self.engine.config.stamp_tax_pct = float(values["close_tax"])
        if "min_commission" in values:
            self.engine.config.min_commission = float(values["min_commission"])

    def set_slippage(self, model: Any, type: str = "stock") -> None:  # noqa: A002
        expected = "fund" if self.engine.config.asset_type == "etf" else self.engine.config.asset_type
        if type not in {expected, "stock" if expected == "stock" else "fund"}:
            raise JoinQuantRuntimeError(f"滑点类型 {type!r} 与当前资产类型不匹配")
        if isinstance(model, FixedSlippage):
            self.engine.config.fixed_slippage = model.value
            return
        if isinstance(model, PriceRelatedSlippage):
            self.engine.config.fixed_slippage = None
            self.engine.config.slippage_bps = model.rate * 10_000
            return
        raise JoinQuantRuntimeError("set_slippage 仅支持 FixedSlippage 或 PriceRelatedSlippage")

    def set_benchmark(self, security: str) -> None:
        self.engine.config.benchmark_symbol = _normalize_symbol(security)


class _Log:
    def __init__(self, runtime: "JoinQuantRuntime") -> None:
        self.runtime = runtime

    def _write(self, level: str, *values: Any) -> None:
        self.runtime.engine.context.log(" ".join(str(value) for value in values), level=level)

    def debug(self, *values: Any) -> None:
        self._write("DEBUG", *values)

    def info(self, *values: Any) -> None:
        self._write("INFO", *values)

    def warn(self, *values: Any) -> None:
        self._write("WARNING", *values)

    warning = warn

    def error(self, *values: Any) -> None:
        self._write("ERROR", *values)


class _Context:
    def __init__(self, runtime: "JoinQuantRuntime") -> None:
        self._runtime = runtime
        self._portfolio = _Portfolio(runtime.engine)

    @property
    def current_dt(self) -> datetime | None:
        return self._runtime.engine.context.now

    @property
    def previous_date(self) -> date | None:
        current = self.current_dt
        if current is None:
            return None
        dates = [value for value in self._runtime.engine._trading_dates if value < current.date()]  # noqa: SLF001
        if dates:
            return dates[-1]
        market_dates = [value for value in self._runtime.engine._market_dates if value < current.date()]  # noqa: SLF001
        if market_dates:
            return max(market_dates)
        candidate = current.date() - timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        return candidate

    @property
    def portfolio(self) -> _Portfolio:
        return self._portfolio

    @property
    def subportfolios(self) -> list[_Portfolio]:
        return [self._portfolio]

    @property
    def run_params(self) -> SimpleNamespace:
        return SimpleNamespace(
            type="backtest",
            frequency=self._runtime.engine.timeframe,
            start_date=self._runtime.engine.run_start,
            end_date=self._runtime.engine.run_end,
        )

    def set_universe(self, securities: Iterable[str]) -> None:
        self._runtime.engine.context.set_universe(securities)

    def run_daily(self, callback: Callable[..., Any], time: Any = "open", reference_security: str | None = None) -> None:
        self._runtime.run_daily(callback, time=time, reference_security=reference_security)

    def run_weekly(self, callback: Callable[..., Any], weekday: int, time: Any = "open", reference_security: str | None = None) -> None:
        self._runtime.run_weekly(callback, weekday=weekday, time=time, reference_security=reference_security)

    def run_monthly(self, callback: Callable[..., Any], monthday: int, time: Any = "open", reference_security: str | None = None) -> None:
        self._runtime.run_monthly(callback, monthday=monthday, time=time, reference_security=reference_security)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._runtime.engine.context, name)


class JoinQuantRuntime:
    dialect = "joinquant"

    def __init__(self, engine: Any, source: str) -> None:
        self.engine = engine
        self.source = source
        self.callbacks: dict[str, Any] = {}
        self.compatibility_report = analyze_source(source)
        self.context = _Context(self)
        self.g = _GlobalState(engine.context)
        self.data = _DataFacade(self)
        self.orders = _Orders(self)
        self.log = _Log(self)
        self.namespace: dict[str, Any] = {}
        self._scheduled_wrappers: dict[Callable[..., Any], Callable[..., Any]] = {}

    def instrument_name(self, symbol: str) -> str:
        record = next(
            (item for item in self.engine.context.instruments() if _normalize_symbol(item.get("symbol", "")) == symbol),
            {},
        )
        return str(record.get("name") or record.get("display_name") or symbol)

    def _schedule_time(self, value: Any) -> str:
        if isinstance(value, datetime_time):
            if value.second or value.microsecond:
                return self._ceil_second_time(value)
            return value.strftime("%H:%M")
        text = str(value).strip().lower()
        if text in _TIME_ALIASES:
            return _TIME_ALIASES[text]
        if text == "every_bar":
            return text
        parts = text.split(":")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            parsed = datetime_time(int(parts[0]), int(parts[1]), int(parts[2]))
            return self._ceil_second_time(parsed)
        return text

    @staticmethod
    def _ceil_second_time(value: datetime_time) -> str:
        current = datetime.combine(date(2000, 1, 1), value)
        if value.second or value.microsecond:
            current = current.replace(second=0, microsecond=0) + timedelta(minutes=1)
        return current.strftime("%H:%M")

    def _scheduled_wrapper(self, callback: Callable[..., Any]) -> Callable[..., Any]:
        wrapper = self._scheduled_wrappers.get(callback)
        if wrapper is None:
            def wrapper(_native_context: Any) -> Any:
                return callback(self.context)

            self._scheduled_wrappers[callback] = wrapper
        return wrapper

    @staticmethod
    def _reference_security(reference_security: str | None) -> None:
        if reference_security:
            raise JoinQuantRuntimeError("聚宽兼容层尚未支持 reference_security 独立交易日历")

    def run_daily(self, callback: Callable[..., Any], time: Any = "open", reference_security: str | None = None) -> None:
        self._reference_security(reference_security)
        self.engine.context.run_daily(self._scheduled_wrapper(callback), self._schedule_time(time))

    def run_weekly(self, callback: Callable[..., Any], weekday: int, time: Any = "open", reference_security: str | None = None) -> None:
        self._reference_security(reference_security)
        self.engine.context.run_weekly(
            self._scheduled_wrapper(callback),
            int(weekday),
            self._schedule_time(time),
        )

    def run_monthly(self, callback: Callable[..., Any], monthday: int, time: Any = "open", reference_security: str | None = None) -> None:
        self._reference_security(reference_security)
        self.engine.context.run_monthly(
            self._scheduled_wrapper(callback),
            int(monthday),
            self._schedule_time(time),
        )

    def _import(self, name: str, globals: Any = None, locals: Any = None, fromlist: Any = (), level: int = 0) -> Any:
        if name == "jqdata":
            return self._jqdata_module()
        if name == "jqfactor" or name.startswith("jqfactor."):
            raise JoinQuantRuntimeError("jqfactor 需要未接入的 PIT 因子数据")
        if name == "jqmt" or name.startswith("jqmt."):
            raise JoinQuantRuntimeError("jqmt 不能绕过系统订单、风控和 QMT 网关")
        if name == "jqlib" or name.startswith("jqlib."):
            raise JoinQuantRuntimeError("jqlib 技术指标需要显式迁移到系统指标契约")
        return builtins.__import__(name, globals, locals, fromlist, level)

    def _jqdata_module(self) -> ModuleType:
        module = ModuleType("jqdata")
        values = {
            "OrderCost": OrderCost,
            "FixedSlippage": FixedSlippage,
            "PriceRelatedSlippage": PriceRelatedSlippage,
            "attribute_history": self.data.attribute_history,
            "get_all_securities": self.data.get_all_securities,
            "get_current_data": self.data.get_current_data,
            "get_price": self.data.get_price,
            "get_security_info": self.data.get_security_info,
            "history": self.data.history,
            "log": self.log,
            "order": self.orders.order,
            "order_target": self.orders.order_target,
            "order_target_percent": self.orders.order_target_percent,
            "order_target_value": self.orders.order_target_value,
            "order_value": self.orders.order_value,
            "run_daily": self.run_daily,
            "run_monthly": self.run_monthly,
            "run_weekly": self.run_weekly,
            "set_benchmark": self.orders.set_benchmark,
            "set_order_cost": self.orders.set_order_cost,
            "set_slippage": self.orders.set_slippage,
            "set_universe": self.context.set_universe,
            "unschedule_all": self.engine.context.unschedule_all,
        }
        module.__dict__.update(values)
        module.__all__ = tuple(values)
        return module

    def load(self) -> None:
        ensure_executable(self.compatibility_report)
        jqdata = self._jqdata_module()
        runtime_builtins = dict(vars(builtins))
        runtime_builtins["__import__"] = self._import
        namespace: dict[str, Any] = {
            "__name__": "joinquant_strategy_snapshot",
            "__builtins__": runtime_builtins,
            "g": self.g,
            "log": self.log,
            "print": self.engine._strategy_print,  # noqa: SLF001
            "run_daily": self.run_daily,
            "run_weekly": self.run_weekly,
            "run_monthly": self.run_monthly,
            "unschedule_all": self.engine.context.unschedule_all,
            "set_universe": self.context.set_universe,
            "set_benchmark": self.orders.set_benchmark,
            "set_order_cost": self.orders.set_order_cost,
            "set_slippage": self.orders.set_slippage,
            "OrderCost": OrderCost,
            "FixedSlippage": FixedSlippage,
            "PriceRelatedSlippage": PriceRelatedSlippage,
            "attribute_history": self.data.attribute_history,
            "get_all_securities": self.data.get_all_securities,
            "get_current_data": self.data.get_current_data,
            "get_price": self.data.get_price,
            "get_security_info": self.data.get_security_info,
            "history": self.data.history,
            "order": self.orders.order,
            "order_target": self.orders.order_target,
            "order_target_percent": self.orders.order_target_percent,
            "order_target_value": self.orders.order_target_value,
            "order_value": self.orders.order_value,
            "jqdata": jqdata,
        }
        exec(compile(self.source, "<joinquant_strategy>", "exec"), namespace, namespace)
        self.namespace = namespace
        process_initialize = namespace.get("process_initialize")
        initialize = namespace.get("initialize")
        handle_data = namespace.get("handle_data")
        before_trading_start = namespace.get("before_trading_start")
        after_trading_end = namespace.get("after_trading_end")

        def initialize_wrapper(_native_context: Any) -> None:
            if callable(process_initialize):
                process_initialize(self.context)
            if callable(initialize):
                initialize(self.context)

        callbacks: dict[str, Any] = {}
        if callable(process_initialize) or callable(initialize):
            callbacks["initialize"] = initialize_wrapper
        if callable(before_trading_start):
            callbacks["before_trading_start"] = lambda _native_context: before_trading_start(self.context)
        if callable(handle_data):
            callbacks["on_bar"] = lambda _native_context, bars: handle_data(
                self.context,
                _BarDataView(self, bars),
            )
        if callable(after_trading_end):
            callbacks["after_trading_end"] = lambda _native_context: after_trading_end(self.context)
        self.callbacks = callbacks

    def runtime_snapshot(self) -> dict[str, Any]:
        values = self.engine.context.state.get("__joinquant_g__", {})
        _json_safe_state(values)
        return {
            "dialect": self.dialect,
            "compatibility_version": COMPATIBILITY_VERSION,
        }

    @staticmethod
    def serialize_state(state: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(state)
        if "__joinquant_g__" in result:
            result["__joinquant_g__"] = _encode_state(result["__joinquant_g__"])
        return result

    @staticmethod
    def restore_state(state: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(state)
        if "__joinquant_g__" in result:
            result["__joinquant_g__"] = _decode_state(result["__joinquant_g__"])
        return result
