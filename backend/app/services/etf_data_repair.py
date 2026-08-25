"""Inspect and repair local ETF backtest market data through configured providers."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, time, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

import polars as pl

from app.parquet import scan_daily_parquet, scan_parquet_compat
from app.services import kline_sync
from app.services.kline_sync import _write_minute_partition
from app.services.minute_quality import assert_required_minute_coverage
from app.tickflow.repository import KlineRepository


logger = logging.getLogger(__name__)
MAX_SCAN_SYMBOLS = 2_000
MINUTE_BARS_PER_DAY = 240
MIN_HISTORY_START = date(1990, 1, 1)


def _load_factors(data_dir: Path) -> pl.DataFrame:
    path = data_dir / "adj_factor_etf" / "all.parquet"
    return pl.read_parquet(path) if path.exists() else pl.DataFrame()


def _scan_rows(path: Path, symbols: list[str], start: date, end: date, *, minute: bool) -> pl.DataFrame:
    try:
        scan = (
            scan_parquet_compat(str(path / "**" / "*.parquet"))
            if minute
            else scan_daily_parquet(str(path / "**" / "*.parquet"))
        )
        day_expr = pl.col("datetime").dt.date() if minute else pl.col("date")
        return (
            scan.filter(pl.col("symbol").is_in(symbols) & (day_expr >= start) & (day_expr <= end))
            .select("symbol", day_expr.alias("date"))
            .unique()
            .collect(engine="streaming")
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("ETF data scan skipped for %s: %s", path, exc)
        return pl.DataFrame({"symbol": [], "date": []}, schema={"symbol": pl.Utf8, "date": pl.Date})


def _latest_daily_date(path: Path, end: date) -> date | None:
    try:
        value = (
            scan_daily_parquet(str(path / "**" / "*.parquet"))
            .filter(pl.col("date") <= end)
            .select(pl.col("date").max())
            .collect(engine="streaming")
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("ETF latest daily scan skipped for %s: %s", path, exc)
        return None
    return value.item() if value.height and value.item() is not None else None


def _scan_minute_day_quality(
    path: Path,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[tuple[str, date], bool]:
    try:
        scan = scan_parquet_compat(str(path / "**" / "*.parquet"))
        timestamp = pl.col("datetime")
        hour = timestamp.dt.hour()
        minute = timestamp.dt.minute()
        continuous = (
            ((hour == 9) & (minute >= 31))
            | (hour == 10)
            | ((hour == 11) & (minute <= 30))
            | ((hour == 13) & (minute >= 1))
            | (hour == 14)
            | ((hour == 15) & (minute == 0))
        )
        allowed = continuous | ((hour == 9) & (minute == 30))
        valid_ohlc = pl.all_horizontal(
            pl.col(column).is_not_null()
            & pl.col(column).is_finite()
            & (pl.col(column) > 0)
            for column in ("open", "high", "low", "close")
        ) & (pl.col("high") >= pl.max_horizontal("open", "close")) & (
            pl.col("low") <= pl.min_horizontal("open", "close")
        )
        frame = (
            scan.filter(
                pl.col("symbol").is_in(symbols)
                & (timestamp.dt.date() >= start)
                & (timestamp.dt.date() <= end)
            )
            .with_columns(timestamp.dt.date().alias("date"))
            .group_by(["symbol", "date"])
            .agg(
                pl.len().alias("rows"),
                timestamp.n_unique().alias("unique_rows"),
                timestamp.filter(continuous).n_unique().alias("continuous_rows"),
                (~allowed).sum().alias("invalid_times"),
                (~valid_ohlc).sum().alias("invalid_ohlc"),
            )
            .collect(engine="streaming")
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("ETF minute quality scan skipped for %s: %s", path, exc)
        return {}
    return {
        (str(row["symbol"]), row["date"]): (
            int(row["continuous_rows"]) == MINUTE_BARS_PER_DAY
            and int(row["invalid_times"]) == 0
            and int(row["invalid_ohlc"]) == 0
            and int(row["unique_rows"]) == int(row["rows"])
        )
        for row in frame.iter_rows(named=True)
    }


def _issue(kind: str, symbol: str, start: date, end: date, **values: Any) -> dict[str, Any]:
    issue_id = sha256(f"{kind}|{symbol}|{start}|{end}".encode()).hexdigest()[:16]
    return {"id": issue_id, "type": kind, "symbol": symbol, "start": start.isoformat(), "end": end.isoformat(), **values}


def _local_issues(
    repo: KlineRepository,
    symbols: list[str],
    start: date,
    end: date,
    *,
    require_minute: bool,
    min_daily_bars: int,
) -> list[dict[str, Any]]:
    data_dir = repo.store.data_dir
    daily = _scan_rows(data_dir / "kline_etf_daily", symbols, start, end, minute=False)
    history = _scan_rows(
        data_dir / "kline_etf_daily", symbols, MIN_HISTORY_START, end, minute=False,
    )
    daily_by_symbol = {
        symbol: set(frame["date"].to_list())
        for symbol, frame in daily.partition_by("symbol", as_dict=True).items()
    } if not daily.is_empty() else {}
    history_by_symbol = {
        symbol: set(frame["date"].to_list())
        for symbol, frame in history.partition_by("symbol", as_dict=True).items()
    } if not history.is_empty() else {}
    expected_tail = _latest_daily_date(data_dir / "kline_etf_daily", end)
    minute_quality: dict[tuple[str, date], bool] = {}
    if require_minute:
        minute_quality = _scan_minute_day_quality(
            data_dir / "kline_etf_minute", symbols, start, end,
        )

    issues = []
    for symbol in symbols:
        expected = daily_by_symbol.get((symbol,), daily_by_symbol.get(symbol, set()))
        available_history = history_by_symbol.get(
            (symbol,), history_by_symbol.get(symbol, set()),
        )
        if not expected:
            issues.append(_issue(
                "daily_missing", symbol, start, end, severity="error", title="缺少 ETF 日K",
                detail="所选区间没有本地 ETF 日K，无法建立交易日基准。",
                action="请使用当前日K数据源同步后重新检查", repairable=False,
            ))
            continue
        latest = max(expected)
        if expected_tail is not None and latest < expected_tail:
            issues.append(_issue(
                "daily_tail_stale", symbol, latest, expected_tail,
                severity="error", title="ETF 日K尾部滞后",
                detail=f"本地最新交易日为 {latest.isoformat()}，市场基准为 {expected_tail.isoformat()}。",
                action="使用已校验的日K数据源补齐后重新计算", repairable=False,
                latest_date=latest.isoformat(), expected_date=expected_tail.isoformat(),
            ))
        if len(available_history) < min_daily_bars:
            issues.append(_issue(
                "daily_history_short", symbol,
                min(available_history) if available_history else start,
                latest,
                severity="error", title="ETF 日K预热不足",
                detail=f"本地只有 {len(available_history)} 根日K，策略至少需要 {min_daily_bars} 根。",
                action="补齐历史日K后重新检查", repairable=False,
                available_bars=len(available_history), required_bars=min_daily_bars,
            ))
        if require_minute:
            missing = sorted(
                day for day in expected
                if not minute_quality.get((symbol, day), False)
            )
            if missing:
                issues.append(_issue(
                    "minute_gap", symbol, missing[0], missing[-1], severity="error", title="分钟K存在缺口",
                    detail=f"缺少或不完整 {len(missing)} 个交易日的分钟K。",
                    action="使用当前分钟数据源补齐缺失交易日", repairable=True,
                    missing_dates=[day.isoformat() for day in missing],
                ))

    factors = _load_factors(data_dir)
    if not factors.is_empty() and {"symbol", "trade_date", "ex_factor"}.issubset(factors.columns):
        suspicious = factors.filter(
            pl.col("symbol").is_in(symbols)
            & (pl.col("trade_date") >= start)
            & (pl.col("trade_date") <= end)
            & (pl.col("ex_factor") >= 1.5)
        )
        for row in suspicious.iter_rows(named=True):
            observed = float(row["ex_factor"])
            nearest = round(observed)
            if nearest >= 2 and abs(observed - nearest) / nearest <= 0.02 and not abs(observed - nearest) <= 1e-9:
                day = row["trade_date"]
                issues.append(_issue(
                    "split_rounding", str(row["symbol"]), day, day, severity="warning", title="份额拆分比例不精确",
                    detail=f"本地复权因子 {observed:.6f} 接近 {nearest}:1 拆分，可能改变动量排名。",
                    action="请从可靠数据源更新复权因子后重新计算", repairable=False,
                ))
    return issues


def inspect_etf_data(
    repo: KlineRepository,
    symbols: list[str],
    start: date,
    end: date,
    *,
    require_minute: bool,
    min_daily_bars: int = 1,
    persist_scan: bool = False,
) -> dict[str, Any]:
    normalized = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not normalized:
        normalized = sorted(repo.get_etf_symbol_set())
    if not normalized:
        raise ValueError("没有可检查的 ETF 标的")
    if len(normalized) > MAX_SCAN_SYMBOLS:
        raise ValueError(f"单次最多检查 {MAX_SCAN_SYMBOLS} 只 ETF")
    if isinstance(min_daily_bars, bool) or min_daily_bars <= 0:
        raise ValueError("最小日K根数必须是正整数")

    unique = {
        issue["id"]: issue
        for issue in _local_issues(
            repo,
            normalized,
            start,
            end,
            require_minute=require_minute,
            min_daily_bars=min_daily_bars,
        )
    }
    result = {
        "scan_id": uuid.uuid4().hex[:12] if persist_scan else None,
        "status": "healthy" if not unique else "issues",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "start": start.isoformat(), "end": end.isoformat(), "symbols": normalized,
        "symbol_count": len(normalized), "require_minute": require_minute,
        "min_daily_bars": min_daily_bars,
        "issues": list(unique.values()),
    }
    if persist_scan:
        scan_dir = repo.store.data_dir / "etf_data_repairs" / "scans"
        scan_dir.mkdir(parents=True, exist_ok=True)
        (scan_dir / f"{result['scan_id']}.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return result


def _load_scan(data_dir: Path, scan_id: str) -> dict[str, Any]:
    if not scan_id or not scan_id.isalnum():
        raise ValueError("非法检查记录 ID")
    path = data_dir / "etf_data_repairs" / "scans" / f"{scan_id}.json"
    if not path.exists():
        raise FileNotFoundError("检查记录不存在，请重新检查")
    return json.loads(path.read_text(encoding="utf-8"))


def _append_record(data_dir: Path, record: dict[str, Any]) -> None:
    root = data_dir / "etf_data_repairs"
    root.mkdir(parents=True, exist_ok=True)
    with (root / "history.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def _issue_dates(issue: dict[str, Any], scan: dict[str, Any]) -> list[date]:
    raw_dates = issue.get("missing_dates")
    if not isinstance(raw_dates, list) or not raw_dates:
        raise ValueError("检查记录缺少可补齐的分钟K日期，请重新检查")
    try:
        dates = sorted({date.fromisoformat(str(value)) for value in raw_dates})
        scan_start = date.fromisoformat(str(scan["start"]))
        scan_end = date.fromisoformat(str(scan["end"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("检查记录格式无效，请重新检查") from exc
    if dates[0] < scan_start or dates[-1] > scan_end:
        raise ValueError("检查记录包含范围外日期，请重新检查")
    return dates


def validate_repair_request(
    data_dir: Path,
    scan_id: str,
    issue_ids: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    scan = _load_scan(data_dir, scan_id)
    allowed = {issue["id"]: issue for issue in scan.get("issues", [])}
    selected = [allowed[issue_id] for issue_id in issue_ids if issue_id in allowed]
    if not selected or len(selected) != len(set(issue_ids)):
        raise ValueError("修复项无效或已过期，请重新检查")
    if any(not issue.get("repairable") for issue in selected):
        raise ValueError("所选问题不能通过当前分钟数据源自动补齐")
    for issue in selected:
        if issue.get("type") != "minute_gap":
            raise ValueError("所选问题不能通过当前分钟数据源自动补齐")
        _issue_dates(issue, scan)
    return scan, selected


def _existing_minute_dates(data_dir: Path, symbol: str, dates: list[date]) -> set[date]:
    existing = _scan_rows(
        data_dir / "kline_etf_minute", [symbol], min(dates), max(dates), minute=True,
    )
    return set(existing["date"].to_list()) if not existing.is_empty() else set()


def _required_minute_rows(source: pl.DataFrame, symbol: str, dates: list[date]) -> pl.DataFrame:
    required = {"symbol", "datetime", "open", "high", "low", "close", "volume", "amount"}
    if source.is_empty() or not required.issubset(source.columns):
        raise RuntimeError(f"当前分钟数据源未返回 {symbol} 的完整分钟K")
    frame = source.filter(
        (pl.col("symbol") == symbol)
        & pl.col("datetime").dt.date().is_in(dates)
    ).select(sorted(required)).unique(subset=["symbol", "datetime"], keep="last")
    counts = {
        row["trade_date"]: int(row["len"])
        for row in frame.with_columns(pl.col("datetime").dt.date().alias("trade_date")).group_by("trade_date").len().iter_rows(named=True)
    }
    incomplete = [day.isoformat() for day in dates if counts.get(day, 0) < MINUTE_BARS_PER_DAY]
    if incomplete:
        raise RuntimeError(f"当前分钟数据源未返回完整交易日: {', '.join(incomplete)}")
    try:
        assert_required_minute_coverage(
            frame,
            ((symbol, day.isoformat()) for day in dates),
        )
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc
    return frame.sort(["symbol", "datetime"])


def repair_etf_data(
    repo: KlineRepository,
    scan_id: str,
    issue_ids: list[str],
    *,
    on_progress=None,
    should_cancel=None,
) -> dict[str, Any]:
    scan, selected = validate_repair_request(repo.store.data_dir, scan_id, issue_ids)
    dates_by_symbol: dict[str, set[date]] = {}
    for issue in selected:
        dates_by_symbol.setdefault(str(issue["symbol"]), set()).update(_issue_dates(issue, scan))

    symbols = sorted(dates_by_symbol)
    frames: list[pl.DataFrame] = []
    try:
        for index, symbol in enumerate(symbols, start=1):
            dates = sorted(dates_by_symbol[symbol])
            if on_progress:
                on_progress(index - 1, len(symbols), f"正在补齐 {symbol}")
            source = kline_sync.sync_minute_batch(
                [symbol],
                start_time=datetime.combine(dates[0], time(9, 25)),
                end_time=datetime.combine(dates[-1], time(15, 5)),
                asset_type="etf",
                should_cancel=should_cancel,
            )
            incoming = _required_minute_rows(source, symbol, dates)
            still_missing = sorted(set(dates) - _existing_minute_dates(repo.store.data_dir, symbol, dates))
            if still_missing:
                frames.append(incoming.filter(pl.col("datetime").dt.date().is_in(still_missing)))
            if on_progress:
                on_progress(index, len(symbols), f"已完成 {symbol}")

        incoming = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
        if not incoming.is_empty():
            _write_minute_partition(incoming, repo.store.data_dir / "kline_etf_minute")
        record = {
            "id": uuid.uuid4().hex[:12], "status": "succeeded",
            "started_at": datetime.now(timezone.utc).isoformat(), "source": "configured_minute_provider",
            "scan_id": scan_id, "symbols": symbols, "start": scan["start"], "end": scan["end"],
            "issue_types": sorted({issue["type"] for issue in selected}),
            "issues_repaired": len(selected), "minute_rows": incoming.height,
        }
        _append_record(repo.store.data_dir, record)
        return record
    except Exception as exc:
        _append_record(repo.store.data_dir, {
            "id": uuid.uuid4().hex[:12], "status": "failed",
            "started_at": datetime.now(timezone.utc).isoformat(), "source": "configured_minute_provider",
            "scan_id": scan_id, "symbols": symbols, "start": scan["start"], "end": scan["end"],
            "issue_types": sorted({issue["type"] for issue in selected}), "error": str(exc),
        })
        raise


def list_repair_records(data_dir: Path, limit: int = 50) -> list[dict[str, Any]]:
    path = data_dir / "etf_data_repairs" / "history.jsonl"
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records[-limit:][::-1]
