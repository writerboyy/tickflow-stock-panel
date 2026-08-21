"""Strict health checks for canonical Tick backtest partitions."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from app.market_time import cn_naive_now

_BASE_REQUIRED = {
    "symbol", "datetime", "open", "high", "low", "prev_close", "volume",
    "amount", "limit_up", "limit_down", "suspended", "source", "source_order",
}
_REQUIRED_NUMERIC = {
    "open", "high", "low", "volume", "amount", "source_order",
}
_OPTIONAL_NUMERIC = {"prev_close", "limit_up", "limit_down"}


def _schema_errors(frame: pl.DataFrame, price_column: str) -> list[str]:
    schema = frame.schema
    errors: list[str] = []
    for column in ("symbol", "source"):
        if schema[column].base_type() != pl.String:
            errors.append(f"{column}={schema[column]}")
    if schema["datetime"].base_type() != pl.Datetime:
        errors.append(f"datetime={schema['datetime']}")
    if schema["suspended"].base_type() != pl.Boolean:
        errors.append(f"suspended={schema['suspended']}")
    for column in sorted({price_column, *_REQUIRED_NUMERIC}):
        if not schema[column].is_numeric():
            errors.append(f"{column}={schema[column]}")
    for column in sorted(_OPTIONAL_NUMERIC):
        if schema[column].base_type() != pl.Null and not schema[column].is_numeric():
            errors.append(f"{column}={schema[column]}")
    return errors


def _daily_dates(repo: Any, symbol: str, start: date, end: date) -> list[date]:
    getter = getattr(repo, "get_daily_asset", None)
    if not callable(getter):
        return []
    frame = getter("stock", symbol, start, end, ["date"])
    if frame is None or frame.is_empty() or "date" not in frame.columns:
        return []
    return sorted(set(frame["date"].to_list()))


def inspect_tick_data(
    repo: Any,
    symbols: Iterable[str],
    start: date,
    end: date,
    *,
    expected_dates: Iterable[date] | None = None,
) -> dict[str, Any]:
    normalized = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ))
    if not normalized:
        raise ValueError("Tick 数据预检没有可用标的")
    explicit_dates = sorted(set(expected_dates or ()))
    dates_by_symbol: dict[str, list[date]] = {}
    for symbol in normalized:
        dates = explicit_dates or _daily_dates(repo, symbol, start, end)
        if not dates:
            day = start
            dates = []
            while day <= end:
                if day.weekday() < 5:
                    dates.append(day)
                day += timedelta(days=1)
        dates_by_symbol[symbol] = dates
    issues: list[dict[str, Any]] = []
    rows = 0
    sources: set[str] = set()
    covered: dict[str, list[str]] = {symbol: [] for symbol in normalized}
    root = Path(repo.store.data_dir) / "tick"
    for day in sorted({value for dates in dates_by_symbol.values() for value in dates}):
        expected_symbols = [symbol for symbol, dates in dates_by_symbol.items() if day in dates]
        part = root / f"date={day.isoformat()}" / "part.parquet"
        if not part.exists():
            issues.append({
                "type": "missing_partition",
                "detail": f"{day.isoformat()} 缺少 Tick 分区",
                "action": "请先从 QMT 导入该交易日 Tick",
                "repairable": False,
                "missing_dates": [day.isoformat()],
            })
            continue
        try:
            frame = pl.read_parquet(part)
        except Exception as exc:  # noqa: BLE001
            issues.append({
                "type": "invalid_partition",
                "detail": f"{day.isoformat()} Tick 分区无法读取: {exc.__class__.__name__}",
                "action": "重新导入该交易日 Tick",
                "repairable": False,
            })
            continue
        missing = sorted(_BASE_REQUIRED - set(frame.columns))
        if "last_price" not in frame.columns and "close" not in frame.columns:
            missing.append("last_price/close")
        if missing:
            issues.append({
                "type": "missing_fields",
                "detail": f"{day.isoformat()} Tick 缺少字段: {', '.join(missing)}",
                "action": "按当前 Tick schema 重新导入",
                "repairable": False,
            })
            continue
        price_column = "last_price" if "last_price" in frame.columns else "close"
        schema_errors = _schema_errors(frame, price_column)
        if schema_errors:
            issues.append({
                "type": "invalid_schema",
                "detail": f"{day.isoformat()} Tick 字段类型无效: {', '.join(schema_errors)}",
                "action": "按当前 Tick schema 重新导入",
                "repairable": False,
            })
            continue
        try:
            day_frame = frame.filter(pl.col("datetime").dt.date() == day)
        except Exception as exc:  # noqa: BLE001
            issues.append({
                "type": "invalid_schema",
                "detail": f"{day.isoformat()} Tick schema 无法校验: {exc.__class__.__name__}",
                "action": "按当前 Tick schema 重新导入",
                "repairable": False,
            })
            continue
        if day_frame.is_empty():
            issues.append({
                "type": "wrong_partition_date",
                "detail": f"{day.isoformat()} 分区中没有该日 Tick",
                "action": "重新导入该交易日 Tick",
                "repairable": False,
            })
            continue
        for symbol in expected_symbols:
            selected = day_frame.filter(pl.col("symbol") == symbol)
            if selected.is_empty():
                issues.append({
                    "type": "missing_symbol_date",
                    "detail": f"{symbol} 缺少 {day.isoformat()} Tick",
                    "action": "从 QMT 重新导入该标的和交易日",
                    "repairable": False,
                    "missing_dates": [day.isoformat()],
                })
                continue
            invalid_expression = (
                pl.col("datetime").is_null()
                | pl.col(price_column).is_null()
                | ~pl.col(price_column).is_finite()
                | (pl.col(price_column) <= 0)
                | pl.col("open").is_null()
                | ~pl.col("open").is_finite()
                | (pl.col("open") <= 0)
                | pl.col("high").is_null()
                | ~pl.col("high").is_finite()
                | (pl.col("high") <= 0)
                | pl.col("low").is_null()
                | ~pl.col("low").is_finite()
                | (pl.col("low") <= 0)
                | pl.col("source").is_null()
                | (pl.col("source").str.strip_chars() == "")
                | pl.col("volume").is_null()
                | ~pl.col("volume").is_finite()
                | (pl.col("volume") < 0)
                | pl.col("amount").is_null()
                | ~pl.col("amount").is_finite()
                | (pl.col("amount") < 0)
                | pl.col("source_order").is_null()
                | (pl.col("source_order") < 0)
            )
            for column in _OPTIONAL_NUMERIC:
                invalid_expression |= pl.col(column).is_not_null() & (
                    ~pl.col(column).is_finite() | (pl.col(column) <= 0)
                )
            invalid = selected.filter(invalid_expression)
            if not invalid.is_empty():
                issues.append({
                    "type": "invalid_rows",
                    "detail": f"{symbol} {day.isoformat()} 有 {invalid.height} 条无效 Tick",
                    "action": "核对 QMT 原始响应并重新导入",
                    "repairable": False,
                })
            order_columns = [
                column for column in ("datetime", "source_order", "sequence", "trade_id")
                if column in selected.columns
            ]
            if selected.select(order_columns).rows() != selected.sort(
                order_columns, maintain_order=True,
            ).select(order_columns).rows():
                issues.append({
                    "type": "out_of_order",
                    "detail": f"{symbol} {day.isoformat()} Tick 顺序异常",
                    "action": "按原始序列重新导入",
                    "repairable": False,
                })
            rows += selected.height
            covered[symbol].append(day.isoformat())
            sources.update(str(value) for value in selected["source"].drop_nulls().unique())
    return {
        "scan_id": None,
        "status": "issues" if issues else "healthy",
        "checked_at": cn_naive_now().isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "symbols": normalized,
        "symbol_count": len(normalized),
        "timeframe": "tick",
        "rows": rows,
        "sources": sorted(sources),
        "coverage": covered,
        "issues": issues,
    }


def require_tick_data(*args: Any, **kwargs: Any) -> dict[str, Any]:
    report = inspect_tick_data(*args, **kwargs)
    if report["issues"]:
        detail = "; ".join(str(issue["detail"]) for issue in report["issues"][:5])
        raise ValueError(f"Tick 数据预检失败: {detail}")
    return report
