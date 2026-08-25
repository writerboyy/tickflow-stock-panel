"""Deterministic scheduling rules for free strategies."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterable


_SESSION_OPEN = time(9, 30)
_SESSION_CLOSE = time(15, 0)
_EXPLICIT_PATTERN = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?")
_RELATIVE_PATTERN = re.compile(r"(open|close)([+-])(\d+)m")


def parse_time_expression(value: str | time) -> str:
    if isinstance(value, time):
        if value.microsecond:
            raise ValueError("定时任务时间不支持微秒精度")
        return value.strftime("%H:%M:%S" if value.second else "%H:%M")
    expression = str(value).strip().lower()
    if expression == "every_bar":
        return expression
    if _EXPLICIT_PATTERN.fullmatch(expression):
        return expression
    match = _RELATIVE_PATTERN.fullmatch(expression)
    if match is None:
        raise ValueError(
            "定时任务时间必须是 HH:MM[:SS]、every_bar、open±Nm 或 close±Nm"
        )
    anchor = _SESSION_OPEN if match.group(1) == "open" else _SESSION_CLOSE
    offset = timedelta(minutes=int(match.group(3)))
    if match.group(2) == "-":
        offset = -offset
    resolved = datetime.combine(date(2000, 1, 1), anchor) + offset
    if resolved.date() != date(2000, 1, 1):
        raise ValueError("开收盘偏移不能跨越自然日")
    return resolved.strftime("%H:%M")


def has_explicit_seconds(value: str | time) -> bool:
    """Return whether a schedule expression explicitly includes ``:SS``.

    ``09:31:00`` is different from ``09:31`` for second-precision replay even
    though both parse to the same wall-clock second.
    """
    if isinstance(value, time):
        return bool(value.second or value.microsecond)
    expression = str(value).strip().lower()
    return bool(re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d", expression))


@dataclass(frozen=True, slots=True)
class ScheduleRule:
    cadence: str
    time_expression: str
    ordinal: int | None = None
    reference_security: str | None = None

    def __post_init__(self) -> None:
        if self.cadence not in {"daily", "weekly", "monthly"}:
            raise ValueError(f"不支持的调度周期: {self.cadence}")
        if self.cadence != "daily" and (
            isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal == 0
        ):
            raise ValueError("周/月调度序号必须是非零整数")

    @property
    def resolved_time(self) -> str:
        return parse_time_expression(self.time_expression)

    def matches_date(self, day: date, trading_dates: Iterable[date]) -> bool:
        if self.cadence == "daily":
            return True
        dates = sorted(set(trading_dates))
        if self.cadence == "weekly":
            iso_year, iso_week, _ = day.isocalendar()
            dates = [
                value
                for value in dates
                if value.isocalendar()[:2] == (iso_year, iso_week)
            ]
        else:
            dates = [
                value
                for value in dates
                if (value.year, value.month) == (day.year, day.month)
            ]
        if not dates:
            return False
        index = int(self.ordinal or 0) - 1 if int(self.ordinal or 0) > 0 else int(self.ordinal or 0)
        if not -len(dates) <= index < len(dates):
            return False
        return dates[index] == day


@dataclass(slots=True)
class RegisteredSchedule:
    callback: Callable[..., Any]
    rule: ScheduleRule
    condition: Callable[[Any, datetime], bool] | None = None
    symbols: Iterable[str] | Callable[[Any, datetime], Iterable[str]] | None = None
    optional_symbols: Iterable[str] | Callable[[Any, datetime], Iterable[str]] | None = None
    done: bool = False

    @property
    def resolved_time(self) -> str:
        return self.rule.resolved_time
