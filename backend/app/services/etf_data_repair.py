"""Inspect and explicitly repair ETF backtest market data with AxData."""
from __future__ import annotations

import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import polars as pl

from app.indicators.pipeline import compute_enriched
from app.services.kline_sync import _atomic_write_parquet, _write_minute_partition
from app.tickflow.repository import DataStore, KlineRepository


logger = logging.getLogger(__name__)
SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_FIELDS = [
    "instrument_id", "trade_time", "open", "high", "low", "close", "volume", "amount",
]
INTRADAY_FIELDS = [
    "instrument_id", "trade_date", "trade_time", "minute_index", "price", "volume", "prev_close",
]
DIVIDEND_FIELDS = ["instrument_id", "dividend_date", "accumulated_dividend"]
MAX_SCAN_SYMBOLS = 2_000
MAX_EXTERNAL_SYMBOLS = 20


def _request(
    base_url: str,
    interface: str,
    params: dict[str, Any],
    fields: list[str],
    *,
    retries: int,
) -> list[dict[str, Any]]:
    error: Exception | None = None
    url = f"{base_url.rstrip('/')}/v1/request/{interface}"
    for attempt in range(retries + 1):
        try:
            response = httpx.post(
                url,
                json={"params": params, "fields": fields},
                timeout=30.0,
            )
            response.raise_for_status()
            payload = response.json()
            if not payload.get("success"):
                raise RuntimeError(f"{interface} failed: {payload.get('error') or 'unknown AxData error'}")
            return list(payload.get("data") or [])
        except (httpx.HTTPError, RuntimeError, ValueError) as exc:
            error = exc
            if attempt < retries:
                time.sleep(0.5 * (2**attempt))
    assert error is not None
    raise error


def axdata_status(base_url: str) -> dict[str, Any]:
    try:
        response = httpx.get(base_url.rstrip("/"), timeout=2.0)
        return {
            "available": response.status_code < 500,
            "url": base_url,
            "message": "AxData 可用" if response.status_code < 500 else f"AxData 返回 {response.status_code}",
        }
    except httpx.HTTPError as exc:
        return {"available": False, "url": base_url, "message": f"AxData 不可用: {exc}"}


def _daily_frame(symbol: str, rows: list[dict[str, Any]], start: date, end: date) -> pl.DataFrame:
    normalized = []
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["trade_time"]))
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(SHANGHAI)
        day = timestamp.date()
        if start <= day <= end:
            normalized.append({
                "symbol": symbol,
                "date": day,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume") or 0.0),
                "amount": float(row.get("amount") or 0.0),
            })
    return pl.DataFrame(normalized).sort("date") if normalized else pl.DataFrame()


def _dividend_factors(
    symbol: str,
    daily: pl.DataFrame,
    dividend_rows: list[dict[str, Any]],
    existing: pl.DataFrame,
) -> pl.DataFrame:
    factors = existing.filter(pl.col("symbol") == symbol) if not existing.is_empty() else existing
    replacements: list[dict[str, Any]] = []
    previous_accumulated = 0.0
    for row in sorted(dividend_rows, key=lambda item: str(item["dividend_date"])):
        day = datetime.strptime(str(row["dividend_date"]), "%Y%m%d").date()
        accumulated = float(row["accumulated_dividend"])
        cash_dividend = accumulated - previous_accumulated
        previous_accumulated = accumulated
        if cash_dividend <= 0:
            continue
        previous = daily.filter(pl.col("date") < day).tail(1)
        if previous.is_empty():
            continue
        previous_close = float(previous["close"][0])
        if previous_close <= cash_dividend:
            raise ValueError(f"invalid dividend for {symbol} on {day}: {cash_dividend}")
        split_ratio = 1.0
        if not factors.is_empty():
            current = factors.filter(pl.col("trade_date") == day)
            if not current.is_empty():
                observed = float(current["ex_factor"][0])
                nearest = round(observed)
                if nearest >= 2 and abs(observed - nearest) / nearest <= 0.02:
                    split_ratio = float(nearest)
        replacements.append({
            "symbol": symbol,
            "trade_date": day,
            "ex_factor": split_ratio * previous_close / (previous_close - cash_dividend),
        })
    if not replacements:
        return factors
    replacement_frame = pl.DataFrame(replacements)
    if factors.is_empty():
        return replacement_frame.sort("trade_date")
    replaced_dates = replacement_frame["trade_date"].to_list()
    return pl.concat([
        factors.filter(~pl.col("trade_date").is_in(replaced_dates)),
        replacement_frame,
    ]).sort("trade_date")


def _normalize_pure_split_factors(
    factors: pl.DataFrame,
    dividend_rows: list[dict[str, Any]],
) -> pl.DataFrame:
    if factors.is_empty():
        return factors
    cash_dates: set[date] = set()
    previous_accumulated = 0.0
    for row in sorted(dividend_rows, key=lambda item: str(item["dividend_date"])):
        accumulated = float(row["accumulated_dividend"])
        if accumulated - previous_accumulated > 0:
            cash_dates.add(datetime.strptime(str(row["dividend_date"]), "%Y%m%d").date())
        previous_accumulated = accumulated
    normalized = []
    for row in factors.iter_rows(named=True):
        observed = float(row["ex_factor"])
        nearest = round(observed)
        day = row["trade_date"]
        if (
            day not in cash_dates
            and nearest >= 2
            and abs(observed - nearest) / nearest <= 0.02
        ):
            row["ex_factor"] = float(nearest)
        normalized.append(row)
    return pl.DataFrame(normalized, schema=factors.schema).sort("trade_date")


def _load_factors(data_dir: Path) -> pl.DataFrame:
    path = data_dir / "adj_factor_etf" / "all.parquet"
    return pl.read_parquet(path) if path.exists() else pl.DataFrame()


def _save_factors(data_dir: Path, symbol: str, factors: pl.DataFrame, existing: pl.DataFrame) -> None:
    if factors.is_empty():
        return
    other = existing.filter(pl.col("symbol") != symbol) if not existing.is_empty() else existing
    merged = factors if other.is_empty() else pl.concat([other, factors])
    path = data_dir / "adj_factor_etf" / "all.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_parquet(merged.sort(["symbol", "trade_date"]), path)


def _minute_frame(symbol: str, rows: list[dict[str, Any]], adjustment_ratio: float) -> pl.DataFrame:
    normalized = []
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["trade_time"]))
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(SHANGHAI).replace(tzinfo=None)
        raw_price = float(row["price"])
        volume = float(row.get("volume") or 0.0)
        adjusted_price = raw_price * adjustment_ratio
        normalized.append({
            "symbol": symbol,
            "datetime": timestamp,
            "open": adjusted_price,
            "high": adjusted_price,
            "low": adjusted_price,
            "close": adjusted_price,
            "volume": volume,
            "amount": raw_price * volume * 100.0,
        })
    return pl.DataFrame(normalized)


def _existing_minute_dates(data_dir: Path, symbol: str, dates: list[date]) -> set[date]:
    existing: set[date] = set()
    for day in dates:
        path = data_dir / "kline_etf_minute" / f"date={day.isoformat()}" / "part.parquet"
        if not path.exists():
            continue
        rows = pl.read_parquet(path, columns=["symbol"]).filter(pl.col("symbol") == symbol)
        if not rows.is_empty():
            existing.add(day)
    return existing


def _merge_instrument(repo: KlineRepository, symbol: str, name: str) -> None:
    instruments = repo.get_etf_instruments()
    incoming = pl.DataFrame({
        "symbol": [symbol], "name": [name], "code": [symbol.split(".", 1)[0]], "asset_type": ["etf"],
    })
    if not instruments.is_empty():
        incoming = pl.concat([instruments, incoming], how="diagonal_relaxed")
    repo.save_etf_instruments(incoming)


def import_symbol(
    *,
    symbol: str,
    name: str,
    start: date,
    end: date,
    data_dir: Path,
    axdata_url: str,
    workers: int,
    retries: int,
    replace_minute: bool = False,
) -> tuple[int, int]:
    daily_rows = _request(
        axdata_url, "etf_kline_tdx", {"code": symbol, "period": "day", "count": 800},
        DAILY_FIELDS, retries=retries,
    )
    daily = _daily_frame(symbol, daily_rows, start - timedelta(days=120), end)
    if daily.is_empty():
        raise RuntimeError(f"AxData returned no daily data for {symbol}")
    dividend_rows = _request(
        axdata_url, "fund_etf_dividend_sina", {"symbol": symbol, "limit": 5000},
        DIVIDEND_FIELDS, retries=retries,
    )
    existing_factors = _load_factors(data_dir)
    factors = _dividend_factors(symbol, daily, dividend_rows, existing_factors)
    factors = _normalize_pure_split_factors(factors, dividend_rows)
    enriched = compute_enriched(daily, factors=factors, instruments=None)
    trading_dates = daily.filter(pl.col("date") >= start)["date"].to_list()
    existing_dates = _existing_minute_dates(data_dir, symbol, trading_dates)
    requested_dates = trading_dates if replace_minute else [day for day in trading_dates if day not in existing_dates]
    intraday_by_date: dict[date, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _request, axdata_url, "etf_intraday_history_tdx",
                {"code": symbol, "trade_date": day.strftime("%Y%m%d")}, INTRADAY_FIELDS,
                retries=retries,
            ): day
            for day in requested_dates
        }
        for future in as_completed(futures):
            day = futures[future]
            rows = future.result()
            if not rows:
                raise RuntimeError(f"AxData returned no intraday data for {symbol} on {day}")
            intraday_by_date[day] = rows
    repo = KlineRepository(DataStore(data_dir))
    repo.append_etf_daily(daily)
    repo.append_etf_enriched(enriched)
    _save_factors(data_dir, symbol, factors, existing_factors)
    _merge_instrument(repo, symbol, name)
    ratios = {
        row["date"]: float(row["close"]) / float(row["raw_close"])
        for row in enriched.select("date", "close", "raw_close").iter_rows(named=True)
    }
    minute_frames = [
        _minute_frame(symbol, intraday_by_date[day], ratios.get(day, 1.0))
        for day in sorted(intraday_by_date)
    ]
    minute_rows = sum(frame.height for frame in minute_frames)
    if minute_frames:
        _write_minute_partition(pl.concat(minute_frames), data_dir / "kline_etf_minute")
    return daily.height, minute_rows


def _scan_rows(path: Path, symbols: list[str], start: date, end: date, *, minute: bool) -> pl.DataFrame:
    try:
        scan = pl.scan_parquet(str(path / "**" / "*.parquet"))
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
) -> list[dict[str, Any]]:
    data_dir = repo.store.data_dir
    daily = _scan_rows(data_dir / "kline_etf_daily", symbols, start, end, minute=False)
    daily_by_symbol = {
        symbol: set(frame["date"].to_list())
        for symbol, frame in daily.partition_by("symbol", as_dict=True).items()
    } if not daily.is_empty() else {}
    minute_by_symbol: dict[str, set[date]] = {}
    if require_minute:
        minute = _scan_rows(data_dir / "kline_etf_minute", symbols, start, end, minute=True)
        minute_by_symbol = {
            symbol: set(frame["date"].to_list())
            for symbol, frame in minute.partition_by("symbol", as_dict=True).items()
        } if not minute.is_empty() else {}
    issues = []
    for symbol in symbols:
        expected = daily_by_symbol.get((symbol,), daily_by_symbol.get(symbol, set()))
        if not expected:
            issues.append(_issue(
                "daily_missing", symbol, start, end, severity="error", title="缺少 ETF 日K",
                detail="所选区间没有本地 ETF 日K，无法建立交易日基准。", action="从 AxData 补齐日K与分钟K",
                missing_days=0, estimated_rows=0, requires_replace=False,
            ))
            continue
        if require_minute:
            actual = minute_by_symbol.get((symbol,), minute_by_symbol.get(symbol, set()))
            missing = sorted(expected - actual)
            if missing:
                issues.append(_issue(
                    "minute_gap", symbol, missing[0], missing[-1], severity="error", title="分钟K存在缺口",
                    detail=f"缺少 {len(missing)} 个交易日的分钟K。", action="从 AxData 补齐缺失交易日",
                    missing_days=len(missing), estimated_rows=len(missing) * 240, requires_replace=False,
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
                    action="用 AxData 分红记录重建复权并替换受影响分钟K", missing_days=0,
                    estimated_rows=240, requires_replace=True,
                ))
    return issues


def _external_factor_issues(
    data_dir: Path,
    symbols: list[str],
    start: date,
    end: date,
    axdata_url: str,
) -> list[dict[str, Any]]:
    existing = _load_factors(data_dir)
    issues = []
    for symbol in symbols:
        daily_rows = _request(
            axdata_url, "etf_kline_tdx", {"code": symbol, "period": "day", "count": 800}, DAILY_FIELDS, retries=1,
        )
        dividends = _request(
            axdata_url, "fund_etf_dividend_sina", {"symbol": symbol, "limit": 5000}, DIVIDEND_FIELDS, retries=1,
        )
        daily = _daily_frame(symbol, daily_rows, start - timedelta(days=120), end)
        if daily.is_empty():
            continue
        proposed = _dividend_factors(symbol, daily, dividends, existing)
        event_dates: set[date] = set()
        previous = 0.0
        for row in sorted(dividends, key=lambda item: str(item["dividend_date"])):
            current = float(row["accumulated_dividend"])
            if current - previous > 0:
                event_dates.add(datetime.strptime(str(row["dividend_date"]), "%Y%m%d").date())
            previous = current
        for day in sorted(event_dates):
            if not start <= day <= end:
                continue
            proposed_row = proposed.filter((pl.col("symbol") == symbol) & (pl.col("trade_date") == day))
            if proposed_row.is_empty():
                continue
            expected = float(proposed_row["ex_factor"][0])
            current_row = existing.filter((pl.col("symbol") == symbol) & (pl.col("trade_date") == day)) if not existing.is_empty() else pl.DataFrame()
            actual = float(current_row["ex_factor"][0]) if not current_row.is_empty() else None
            if actual is None or abs(actual - expected) > 1e-6:
                issues.append(_issue(
                    "factor_mismatch", symbol, day, day, severity="warning", title="分红复权因子不一致",
                    detail=f"本地 {actual if actual is not None else '缺失'}，AxData 推导值 {expected:.6f}。",
                    action="更新复权因子并替换受影响分钟K", missing_days=0, estimated_rows=240,
                    requires_replace=True,
                ))
    return issues


def inspect_etf_data(
    repo: KlineRepository,
    symbols: list[str],
    start: date,
    end: date,
    *,
    require_minute: bool,
    verify_axdata: bool,
    axdata_url: str,
    persist_scan: bool = False,
) -> dict[str, Any]:
    normalized = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if not normalized:
        normalized = sorted(repo.get_etf_symbol_set())
    if not normalized:
        raise ValueError("没有可检查的 ETF 标的")
    if len(normalized) > MAX_SCAN_SYMBOLS:
        raise ValueError(f"单次最多检查 {MAX_SCAN_SYMBOLS} 只 ETF")
    if verify_axdata and len(normalized) > MAX_EXTERNAL_SYMBOLS:
        raise ValueError(f"AxData 核对单次最多支持 {MAX_EXTERNAL_SYMBOLS} 只 ETF")
    issues = _local_issues(repo, normalized, start, end, require_minute=require_minute)
    source = (
        axdata_status(axdata_url)
        if verify_axdata or persist_scan
        else {"available": None, "url": axdata_url, "message": "未检测 AxData"}
    )
    if verify_axdata:
        if not source["available"]:
            raise RuntimeError(source["message"])
        issues.extend(_external_factor_issues(repo.store.data_dir, normalized, start, end, axdata_url))
    unique = {issue["id"]: issue for issue in issues}
    result = {
        "scan_id": uuid.uuid4().hex[:12] if persist_scan else None,
        "status": "healthy" if not unique else "issues",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "start": start.isoformat(), "end": end.isoformat(), "symbols": normalized,
        "symbol_count": len(normalized), "require_minute": require_minute,
        "verify_axdata": verify_axdata, "source": source, "issues": list(unique.values()),
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


def validate_repair_request(
    data_dir: Path,
    scan_id: str,
    issue_ids: list[str],
    *,
    replace_existing: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    scan = _load_scan(data_dir, scan_id)
    allowed = {issue["id"]: issue for issue in scan.get("issues", [])}
    selected = [allowed[issue_id] for issue_id in issue_ids if issue_id in allowed]
    if not selected or len(selected) != len(set(issue_ids)):
        raise ValueError("修复项无效或已过期，请重新检查")
    if any(issue.get("requires_replace") for issue in selected) and not replace_existing:
        raise PermissionError("所选问题需要替换已有分钟数据，请先明确确认覆盖")
    return scan, selected


def repair_etf_data(
    repo: KlineRepository,
    scan_id: str,
    issue_ids: list[str],
    *,
    replace_existing: bool,
    axdata_url: str,
    on_progress=None,
) -> dict[str, Any]:
    scan, selected = validate_repair_request(
        repo.store.data_dir, scan_id, issue_ids, replace_existing=replace_existing,
    )
    source = axdata_status(axdata_url)
    if not source["available"]:
        raise RuntimeError(source["message"])
    symbols = sorted({issue["symbol"] for issue in selected})
    names = repo.get_name_map(symbols)
    start = date.fromisoformat(scan["start"])
    end = date.fromisoformat(scan["end"])
    daily_rows = 0
    minute_rows = 0
    try:
        for index, symbol in enumerate(symbols, start=1):
            if on_progress:
                on_progress(index - 1, len(symbols), f"正在修复 {symbol}")
            symbol_issues = [issue for issue in selected if issue["symbol"] == symbol]
            replace_symbol = replace_existing and any(issue.get("requires_replace") for issue in symbol_issues)
            daily_count, minute_count = import_symbol(
                symbol=symbol, name=names.get(symbol, symbol), start=start, end=end,
                data_dir=repo.store.data_dir, axdata_url=axdata_url, workers=4, retries=2,
                replace_minute=replace_symbol,
            )
            daily_rows += daily_count
            minute_rows += minute_count
            if on_progress:
                on_progress(index, len(symbols), f"已完成 {symbol}")
        record = {
            "id": uuid.uuid4().hex[:12], "status": "succeeded",
            "started_at": datetime.now(timezone.utc).isoformat(), "source": "AxData",
            "scan_id": scan_id, "symbols": symbols, "start": scan["start"], "end": scan["end"],
            "issue_types": sorted({issue["type"] for issue in selected}),
            "issues_repaired": len(selected), "daily_rows": daily_rows, "minute_rows": minute_rows,
            "replace_existing": replace_existing,
        }
        _append_record(repo.store.data_dir, record)
        return record
    except Exception as exc:
        _append_record(repo.store.data_dir, {
            "id": uuid.uuid4().hex[:12], "status": "failed",
            "started_at": datetime.now(timezone.utc).isoformat(), "source": "AxData",
            "scan_id": scan_id, "symbols": symbols, "start": scan["start"], "end": scan["end"],
            "issue_types": sorted({issue["type"] for issue in selected}), "error": str(exc),
            "replace_existing": replace_existing,
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
