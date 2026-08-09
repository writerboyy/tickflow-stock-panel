"""Data-readiness contracts and fail-closed TickFlow preflight checks."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping

import polars as pl

from .financial_pit import FinancialPitUnavailable, load_financial_periods
from .industry import IndustryHistoryUnavailable, load_industry_history


@dataclass(frozen=True, slots=True)
class FinancialRequirement:
    table: str
    fields: tuple[str, ...]
    periods: int = 1
    comparison: str | None = None


@dataclass(frozen=True, slots=True)
class ReadinessRequirement:
    rebalance: str
    financials: tuple[FinancialRequirement, ...] = ()
    valuation_fields: tuple[str, ...] = ()
    industry_standard: str | None = None
    industry_level: str | int | None = None
    lifecycle: bool = False
    adjustment: str | None = None
    corporate_actions: bool = False


class ReadinessUnavailable(ValueError):
    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report
        gaps = report.get("gaps", [])
        detail = "; ".join(str(item.get("detail") or item.get("kind")) for item in gaps[:5])
        super().__init__(f"TickFlow readiness 检查失败 ({len(gaps)} 项): {detail}")


def make_requirement(
    *,
    rebalance: str,
    financials: Mapping[str, Mapping[str, Any]] | None = None,
    valuation_fields: Iterable[str] = (),
    industry_standard: str | None = None,
    industry_level: str | int | None = None,
    lifecycle: bool = False,
    adjustment: str | None = None,
    corporate_actions: bool = False,
) -> ReadinessRequirement:
    cadence = str(rebalance).strip().lower()
    if cadence not in {"daily", "weekly", "monthly"}:
        raise ValueError("readiness 调仓周期只支持 daily、weekly 或 monthly")
    normalized_financials = []
    for table, config in (financials or {}).items():
        fields = tuple(dict.fromkeys(str(value).strip() for value in config.get("fields", ()) if str(value).strip()))
        periods = config.get("periods", 1)
        if isinstance(periods, bool) or not isinstance(periods, int) or periods <= 0:
            raise ValueError(f"{table} 财务报告期数量必须是正整数")
        comparison = str(config.get("comparison") or "").strip().lower() or None
        if comparison not in {None, "consecutive", "yoy"}:
            raise ValueError(f"{table} comparison 只支持 consecutive 或 yoy")
        if comparison is not None and periods < 2:
            raise ValueError(f"{table} 比较报告期至少需要 2 期")
        normalized_financials.append(
            FinancialRequirement(str(table), fields, periods, comparison)
        )
    adjustment_value = str(adjustment).strip().lower() if adjustment is not None else None
    if adjustment_value not in {None, "pre", "post", "raw"}:
        raise ValueError("复权要求只支持 pre、post 或 raw")
    return ReadinessRequirement(
        rebalance=cadence,
        financials=tuple(normalized_financials),
        valuation_fields=tuple(dict.fromkeys(
            str(value).strip() for value in valuation_fields if str(value).strip()
        )),
        industry_standard=str(industry_standard).strip() if industry_standard else None,
        industry_level=industry_level,
        lifecycle=bool(lifecycle),
        adjustment=adjustment_value,
        corporate_actions=bool(corporate_actions),
    )


def _rebalance_dates(cadence: str, trading_dates: Iterable[date]) -> list[date]:
    dates = sorted(set(trading_dates))
    if cadence == "daily":
        return dates
    result = []
    seen: set[tuple[int, ...]] = set()
    for day in dates:
        key = (
            (day.isocalendar().year, day.isocalendar().week)
            if cadence == "weekly"
            else (day.year, day.month)
        )
        if key not in seen:
            seen.add(key)
            result.append(day)
    return result


def _active_parquet_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [
        path
        for path in sorted(root.rglob("*.parquet"))
        if not any(part.startswith(".") for part in path.relative_to(root).parts)
    ]


def _file_manifest(data_dir: Path, roots: Iterable[Path]) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for root in sorted(set(Path(value) for value in roots), key=str):
        target = data_dir / root
        candidates = (
            [target]
            if target.is_file()
            else _active_parquet_files(target)
        )
        if not candidates:
            files.append({"path": root.as_posix(), "missing": True})
            continue
        for path in candidates:
            stat = path.stat()
            files.append({
                "path": path.relative_to(data_dir).as_posix(),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            })
    encoded = json.dumps(files, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return {"sha256": sha256(encoded.encode("utf-8")).hexdigest(), "files": files}


def _load_lifecycle_events(
    data_dir: Path,
    symbols: list[str],
) -> dict[str, tuple[tuple[date, str], ...]]:
    path = data_dir / "pit_reference/history/instrument_lifecycle_events/part.parquet"
    if not path.exists():
        return {}
    frame = pl.read_parquet(path)
    required = {"symbol", "event_date", "event_type"}
    if not required <= set(frame.columns):
        return {}
    rows = (
        frame
        .filter(pl.col("symbol").is_in(symbols))
        .select("symbol", "event_date", "event_type")
        .sort(["symbol", "event_date"])
    )
    events: dict[str, list[tuple[date, str]]] = {}
    for row in rows.iter_rows(named=True):
        event_date = row["event_date"]
        if isinstance(event_date, datetime):
            event_date = event_date.date()
        if not isinstance(event_date, date):
            continue
        events.setdefault(str(row["symbol"]), []).append((
            event_date, str(row["event_type"]).lower(),
        ))
    return {symbol: tuple(values) for symbol, values in events.items()}


def _lifecycle_pool(
    events_by_symbol: Mapping[str, tuple[tuple[date, str], ...]],
    symbols: list[str],
    as_of: date,
) -> tuple[list[str], list[str], list[str]]:
    active: list[str] = []
    missing: list[str] = []
    delisted: list[str] = []
    for symbol in symbols:
        events = events_by_symbol.get(symbol, ())
        types = [event_type for event_date, event_type in events if event_date <= as_of]
        future_types = [event_type for event_date, event_type in events if event_date > as_of]
        if "listed" not in types and "listed" in future_types:
            continue
        if "listed" not in types:
            missing.append(symbol)
        elif "delisted" in types:
            delisted.append(symbol)
        else:
            active.append(symbol)
    return active, missing, delisted


def _previous_date(day: date, calendar: list[date]) -> date | None:
    previous = [value for value in calendar if value < day]
    return previous[-1] if previous else None


def _periods_match_comparison(rows: list[dict[str, Any]], comparison: str | None) -> bool:
    if comparison is None or len(rows) < 2:
        return True
    periods = [date.fromisoformat(str(row["period_end"])[:10]) for row in rows]
    for current, previous in zip(periods, periods[1:]):
        if comparison == "yoy":
            if (current.year - 1, current.month, current.day) != (
                previous.year,
                previous.month,
                previous.day,
            ):
                return False
            continue
        quarter = (current.month - 1) // 3
        expected_year = current.year if quarter > 0 else current.year - 1
        expected_month = quarter * 3 if quarter > 0 else 12
        expected_day = 31 if expected_month in {3, 12} else 30
        if previous != date(expected_year, expected_month, expected_day):
            return False
    return True


def build_readiness_manifest(
    data_dir: Path,
    requirements: Iterable[ReadinessRequirement],
    *,
    strategy_sha256: str,
    universe: Iterable[str],
    trading_dates: Iterable[date],
    calendar_dates: Iterable[date],
    benchmark_symbol: str,
    benchmark_dates: Iterable[date],
    dataset_roots: Iterable[Path] = (),
) -> dict[str, Any]:
    data_dir = Path(data_dir)
    values = tuple(requirements)
    symbols = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in universe if str(symbol).strip()
    ))
    calendar = sorted(set(calendar_dates))
    benchmark_days = set(benchmark_dates)
    gaps: list[dict[str, Any]] = []
    checked: list[dict[str, Any]] = []
    roots = list(dataset_roots)
    roots.append(Path("pit_reference/history/instrument_lifecycle_events/part.parquet"))
    lifecycle_events = (
        _load_lifecycle_events(data_dir, symbols)
        if any(requirement.lifecycle for requirement in values)
        else {}
    )
    for requirement in values:
        roots.extend(
            Path("financials") / item.table for item in requirement.financials
        )
        if requirement.valuation_fields:
            roots.append(Path("valuation_daily"))
        if requirement.industry_standard:
            roots.append(Path("pit_reference/history/industry_membership_history/part.parquet"))
        if requirement.corporate_actions:
            roots.append(Path("corporate_actions/stock_dividends.parquet"))
            if not (data_dir / "corporate_actions/stock_dividends.parquet").exists():
                gaps.append({
                    "kind": "corporate_actions",
                    "detail": "缺少 TickFlow corporate_actions/stock_dividends.parquet",
                })
        if requirement.adjustment:
            candidates = [
                path
                for root in dataset_roots
                for path in (
                    [data_dir / root]
                    if (data_dir / root).is_file()
                    else _active_parquet_files(data_dir / root)[:1]
                )
            ]
            if not candidates:
                gaps.append({
                    "kind": "adjustment",
                    "detail": f"缺少 {requirement.adjustment} 复权行情数据集",
                })
            else:
                schema = pl.read_parquet_schema(candidates[0])
                if not {"close", "raw_close"} <= set(schema):
                    gaps.append({
                        "kind": "adjustment",
                        "detail": (
                            f"{candidates[0].relative_to(data_dir)} 缺少 close/raw_close，"
                            f"不能证明 {requirement.adjustment} 复权"
                        ),
                    })
        for rebalance_day in _rebalance_dates(requirement.rebalance, trading_dates):
            as_of = _previous_date(rebalance_day, calendar)
            if as_of is None:
                gaps.append({
                    "kind": "calendar_warmup",
                    "rebalance_date": rebalance_day.isoformat(),
                    "detail": f"{rebalance_day.isoformat()} 缺少前一交易日",
                })
                continue
            if benchmark_symbol and as_of not in benchmark_days:
                gaps.append({
                    "kind": "benchmark",
                    "rebalance_date": rebalance_day.isoformat(),
                    "as_of": as_of.isoformat(),
                    "symbols": [benchmark_symbol],
                    "detail": f"基准 {benchmark_symbol} 在 {as_of.isoformat()} 缺少行情",
                })
            active = list(symbols)
            if requirement.lifecycle:
                active, missing_lifecycle, _delisted = _lifecycle_pool(
                    lifecycle_events, symbols, as_of,
                )
                if missing_lifecycle:
                    gaps.append({
                        "kind": "lifecycle",
                        "rebalance_date": rebalance_day.isoformat(),
                        "as_of": as_of.isoformat(),
                        "symbols": missing_lifecycle,
                        "detail": f"{as_of.isoformat()} 历史上市状态缺口 {len(missing_lifecycle)} 只",
                    })
            for financial in requirement.financials:
                try:
                    rows = load_financial_periods(
                        data_dir,
                        financial.table,
                        active,
                        as_of,
                        period_count=financial.periods,
                        required_fields=financial.fields,
                    )
                    missing_symbols = [
                        symbol
                        for symbol in active
                        if len(rows.get(symbol, ())) < financial.periods
                        or not _periods_match_comparison(
                            rows.get(symbol, []),
                            financial.comparison,
                        )
                    ]
                    if missing_symbols:
                        gaps.append({
                            "kind": "financial",
                            "table": financial.table,
                            "rebalance_date": rebalance_day.isoformat(),
                            "as_of": as_of.isoformat(),
                            "symbols": missing_symbols,
                            "detail": (
                                f"{financial.table} 在 {as_of.isoformat()} 缺少 "
                                f"{financial.periods} 个报告期或必需字段: {len(missing_symbols)} 只"
                            ),
                        })
                except FinancialPitUnavailable as exc:
                    gaps.append({
                        "kind": "financial_conflict",
                        "table": financial.table,
                        "rebalance_date": rebalance_day.isoformat(),
                        "as_of": as_of.isoformat(),
                        "detail": str(exc),
                    })
            if requirement.industry_standard and active:
                try:
                    load_industry_history(
                        data_dir,
                        active,
                        as_of,
                        requirement.industry_standard,
                        requirement.industry_level,
                    )
                except IndustryHistoryUnavailable as exc:
                    gaps.append({
                        "kind": "industry",
                        "rebalance_date": rebalance_day.isoformat(),
                        "as_of": as_of.isoformat(),
                        "detail": str(exc),
                    })
            if requirement.valuation_fields and active:
                from app.services.daily_valuation import load_latest_market_caps

                caps = load_latest_market_caps(data_dir, active, as_of)
                missing_caps = [symbol for symbol in active if symbol not in caps]
                if missing_caps:
                    gaps.append({
                        "kind": "valuation",
                        "rebalance_date": rebalance_day.isoformat(),
                        "as_of": as_of.isoformat(),
                        "symbols": missing_caps,
                        "detail": f"估值字段在 {as_of.isoformat()} 缺口 {len(missing_caps)} 只",
                    })
            checked.append({
                "rebalance_date": rebalance_day.isoformat(),
                "as_of": as_of.isoformat(),
                "universe_size": len(active),
            })
    file_manifest = _file_manifest(data_dir, roots)
    calendar_payload = [value.isoformat() for value in sorted(set(trading_dates))]
    calendar_sha = sha256("\n".join(calendar_payload).encode("ascii")).hexdigest()
    report = {
        "schema_version": 1,
        "status": "passed" if not gaps else "failed",
        "strategy_sha256": strategy_sha256,
        "tickflow_data_manifest_sha256": file_manifest["sha256"],
        "trading_calendar_sha256": calendar_sha,
        "benchmark_symbol": benchmark_symbol,
        "requirements": [asdict(value) for value in values],
        "universe_size": len(symbols),
        "checks": checked,
        "gaps": gaps,
        "source_proof": {
            "provider": "tickflow",
            "files": file_manifest["files"],
        },
    }
    if gaps:
        raise ReadinessUnavailable(report)
    return report


def persist_readiness_report(run_dir: Path, report: dict[str, Any]) -> Path:
    target = Path(run_dir).parent / "readiness_reports" / f"{Path(run_dir).name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return target
