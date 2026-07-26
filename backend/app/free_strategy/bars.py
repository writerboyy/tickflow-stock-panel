"""bar 数据模型、分钟线聚合和数据校验。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    amount: float = 0.0

    @property
    def date(self) -> date:
        return self.timestamp.date()

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol, "datetime": self.timestamp.isoformat(),
            "date": self.date.isoformat(), "open": self.open, "high": self.high,
            "low": self.low, "close": self.close, "volume": self.volume, "amount": self.amount,
        }


SESSION_OPEN = (time(9, 30), time(13, 0))
SESSION_CLOSE = (time(11, 30), time(15, 0))


def _session_start(value: datetime) -> datetime | None:
    t = value.time().replace(second=0, microsecond=0)
    if time(9, 30) <= t < time(11, 30):
        return value.replace(hour=9, minute=30, second=0, microsecond=0)
    if time(13, 0) <= t < time(15, 0):
        return value.replace(hour=13, minute=0, second=0, microsecond=0)
    return None


def aggregate_minute_bars(rows: Iterable[Bar], minutes: int) -> list[Bar]:
    """把 1 分钟 bar 聚合成 5/30 分钟 bar。

    午休被视为 session 边界，不能让 11:30 后的 bar 与 13:00 拼成一根；
    不完整的桶也会保留，策略可以看到当天真实存在的最后一根 bar。
    """
    if minutes <= 0:
        raise ValueError("聚合周期必须为正数")
    ordered = sorted(rows, key=lambda b: (b.timestamp, b.symbol))
    buckets: dict[tuple[str, datetime], list[Bar]] = {}
    for row in ordered:
        session = _session_start(row.timestamp)
        if session is None:
            continue
        elapsed = int((row.timestamp - session).total_seconds() // 60)
        anchor = session + timedelta(minutes=(elapsed // minutes) * minutes)
        buckets.setdefault((row.symbol, anchor), []).append(row)
    result: list[Bar] = []
    for (symbol, anchor), values in sorted(buckets.items(), key=lambda item: item[0][1]):
        values.sort(key=lambda b: b.timestamp)
        result.append(Bar(
            symbol=symbol, timestamp=anchor, open=values[0].open,
            high=max(v.high for v in values), low=min(v.low for v in values),
            close=values[-1].close, volume=sum(v.volume for v in values),
            amount=sum(v.amount for v in values),
        ))
    return result


def group_bars(rows: Iterable[Bar], timeframe: str) -> list[Bar]:
    if timeframe == "1m":
        return sorted(rows, key=lambda b: (b.timestamp, b.symbol))
    if timeframe in {"5m", "30m"}:
        return aggregate_minute_bars(rows, int(timeframe[:-1]))
    if timeframe == "1d":
        return sorted(rows, key=lambda b: (b.timestamp.date(), b.symbol))
    raise ValueError(f"不支持的周期: {timeframe}")


def validate_minute_history(rows: Iterable[Bar], symbols: list[str], start: date, end: date) -> None:
    values = list(rows)
    if not values:
        raise ValueError("没有可用的分钟K历史数据，请确认已开通分钟K能力并完成同步")
    found = {row.symbol for row in values}
    missing = [symbol for symbol in symbols if symbol not in found]
    if missing:
        raise ValueError(f"分钟K历史缺少标的: {', '.join(missing[:8])}")
    dates = {row.date for row in values if start <= row.date <= end}
    if not dates:
        raise ValueError(f"分钟K历史不覆盖 {start.isoformat()} 至 {end.isoformat()}")


def rows_to_bars(rows: Iterable[Mapping[str, object]]) -> list[Bar]:
    result: list[Bar] = []
    for row in rows:
        raw_dt = row.get("datetime", row.get("timestamp"))
        if isinstance(raw_dt, str):
            raw_dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
        if not isinstance(raw_dt, datetime):
            raise ValueError("bar 缺少 datetime")
        result.append(Bar(
            symbol=str(row["symbol"]), timestamp=raw_dt,
            open=float(row["open"]), high=float(row["high"]), low=float(row["low"]),
            close=float(row["close"]), volume=float(row.get("volume") or 0),
            amount=float(row.get("amount") or 0),
        ))
    return result
