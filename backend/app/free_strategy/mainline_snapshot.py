"""Point-in-time daily snapshots for the intraday mainline momentum strategy."""
from __future__ import annotations

import copy
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.services.security_dimensions import load_instrument_name_changes


SW_STANDARD = "申银万国行业分类标准"
BENCHMARK_SYMBOL = "000905.SH"
MIN_DAILY_AMOUNT = 100_000_000.0


def _calendar_start(day: date, lookback_days: int) -> date:
    return day - timedelta(days=max(120, lookback_days * 3))


def _percentile(column: str, *, group: str = "date") -> pl.Expr:
    return (
        pl.col(column).rank(method="average").over(group)
        / pl.len().over(group)
        * 100
    ).fill_null(0)


def _industry_key(prefix: str, standard: str, level: int) -> pl.Expr:
    code = pl.col(f"{prefix}_code").fill_null("").str.strip_chars()
    return pl.concat_str([
        pl.lit(f"{standard}|{level}|"),
        pl.when(code != "").then(code).otherwise(pl.col(f"{prefix}_name")),
    ])


def _name_events(repo: Any, instruments: pl.DataFrame) -> pl.DataFrame:
    changes = load_instrument_name_changes(repo)
    rows: list[dict[str, Any]] = []
    for symbol, current_name in instruments.select("symbol", "name").iter_rows():
        history = changes.get(str(symbol), ())
        initial_name = history[0][1] if history else str(current_name or "")
        rows.append({"symbol": str(symbol), "_name_date": date(1900, 1, 1), "pit_name": initial_name})
        rows.extend(
            {"symbol": str(symbol), "_name_date": changed, "pit_name": after or initial_name}
            for changed, _before, after in history
        )
    return pl.DataFrame(
        rows,
        schema={"symbol": pl.String, "_name_date": pl.Date, "pit_name": pl.String},
    ).sort(["symbol", "_name_date"])


def _join_industry(
    frame: pl.DataFrame,
    history: pl.DataFrame,
    *,
    standard: str,
    level: int,
    prefix: str,
) -> pl.DataFrame:
    selected = (
        history
        .filter(
            (pl.col("industry_standard") == standard)
            & (pl.col("industry_level") == level)
        )
        .select(
            pl.col("member_symbol").alias("symbol"),
            pl.col("effective_from").alias(f"_{prefix}_from"),
            pl.col("effective_to").alias(f"_{prefix}_to"),
            pl.col("industry_code").alias(f"{prefix}_code"),
            pl.col("industry_name").alias(f"{prefix}_name"),
        )
        .sort(["symbol", f"_{prefix}_from"])
    )
    if selected.is_empty():
        raise ValueError(f"PIT 行业历史缺少 {standard} {level} 级分类")
    joined = frame.sort(["symbol", "date"]).join_asof(
        selected,
        left_on="date",
        right_on=f"_{prefix}_from",
        by="symbol",
        strategy="backward",
        check_sortedness=False,
    )
    return joined.with_columns(
        pl.when(
            pl.col(f"_{prefix}_to").is_null()
            | (pl.col("date") < pl.col(f"_{prefix}_to"))
        )
        .then(pl.col(f"{prefix}_name"))
        .otherwise(None)
        .alias(f"{prefix}_name")
    ).drop(f"_{prefix}_from", f"_{prefix}_to")


class MainlineSnapshotCache:
    """Build compact daily candidate snapshots without loading all-market minutes."""

    def __init__(
        self,
        repo: Any,
        start: date,
        end: date,
        requirement: dict[str, Any],
        *,
        benchmark_symbol: str = BENCHMARK_SYMBOL,
    ) -> None:
        self.repo = repo
        self.start = start
        self.end = end
        self.requirement = dict(requirement)
        self.benchmark_symbol = benchmark_symbol
        self._snapshots: dict[date, dict[str, Any]] = {}
        self._all_symbols: set[str] = set()
        self._build()

    @property
    def all_symbols(self) -> list[str]:
        return sorted(self._all_symbols)

    @property
    def bootstrap_symbols(self) -> list[str]:
        for day in sorted(self._snapshots):
            symbols = [str(row["symbol"]) for row in self._snapshots[day]["candidates"]]
            if symbols:
                return symbols
        return []

    def snapshot(self, trading_day: date) -> dict[str, Any]:
        value = self._snapshots.get(trading_day)
        return copy.deepcopy(value) if value is not None else {
            "date": trading_day.isoformat(),
            "as_of": None,
            "coverage": 0.0,
            "industries": [],
            "subindustries": [],
            "candidates": [],
        }

    def _minute_available_symbols(self, trading_day: date) -> set[str] | None:
        path = (
            Path(self.repo.store.data_dir)
            / "kline_minute"
            / "_coverage"
            / f"date={trading_day.isoformat()}.json"
        )
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"分钟K覆盖文件损坏: {path}") from exc
        return {
            str(row.get("symbol"))
            for row in payload.get("groups", [])
            if row.get("symbol") and int(row.get("bars") or 0) > 0
        }

    def _load_daily(self, instruments: pl.DataFrame, load_start: date) -> pl.DataFrame:
        symbols = instruments["symbol"].to_list()
        columns = [
            "symbol", "date", "open", "high", "low", "close", "volume", "amount",
            "raw_close", "raw_high", "raw_low",
        ]
        frame = self.repo.get_daily_asset_batch(
            "stock", symbols, load_start, self.end, columns,
        )
        if frame.is_empty():
            raise ValueError("主线快照缺少全市场股票日线")
        required = {"symbol", "date", "close", "high", "amount"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"主线快照日线缺少字段: {', '.join(missing)}")
        return frame.sort(["symbol", "date"])

    def _eligible_rows(self, frame: pl.DataFrame, instruments: pl.DataFrame) -> pl.DataFrame:
        metadata_columns = [
            column for column in ("symbol", "listing_date", "list_date", "delist_date")
            if column in instruments.columns
        ]
        metadata = instruments.select(metadata_columns).unique("symbol")
        if "listing_date" not in metadata.columns and "list_date" in metadata.columns:
            metadata = metadata.rename({"list_date": "listing_date"})
        elif "listing_date" in metadata.columns and "list_date" in metadata.columns:
            metadata = metadata.with_columns(
                pl.coalesce("list_date", "listing_date").alias("listing_date")
            ).drop("list_date")
        joined = frame.join(metadata, on="symbol", how="left")
        names = _name_events(self.repo, instruments)
        joined = joined.sort(["symbol", "date"]).join_asof(
            names,
            left_on="date",
            right_on="_name_date",
            by="symbol",
            strategy="backward",
            check_sortedness=False,
        ).drop("_name_date")
        listing_ok = (
            pl.col("listing_date").is_not_null()
            & ((pl.col("date") - pl.col("listing_date")).dt.total_days() >= 120)
        )
        delisting_ok = (
            pl.col("delist_date").is_null() | (pl.col("date") < pl.col("delist_date"))
            if "delist_date" in joined.columns else pl.lit(True)
        )
        name = pl.col("pit_name").fill_null("").str.to_uppercase()
        return joined.with_columns(
            (listing_ok & delisting_ok & ~name.str.contains(r"ST|退市整理|退$")).alias("eligible")
        )

    def _features(self, frame: pl.DataFrame) -> pl.DataFrame:
        raw_close = pl.coalesce("raw_close", "close")
        previous_raw = raw_close.shift(1).over("symbol")
        code = pl.col("symbol").str.slice(0, 6)
        limit_pct = (
            pl.when(pl.col("pit_name").fill_null("").str.to_uppercase().str.contains("ST"))
            .then(0.05)
            .when(
                code.str.starts_with("300")
                | code.str.starts_with("301")
                | code.str.starts_with("688")
                | code.str.starts_with("689")
            )
            .then(0.20)
            .otherwise(0.10)
        )
        return (
            frame.sort(["symbol", "date"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(5).over("symbol") - 1).alias("ret5"),
                (pl.col("close") / pl.col("close").shift(20).over("symbol") - 1).alias("ret20"),
                pl.col("close").rolling_mean(20).over("symbol").alias("ma20"),
                pl.col("high").rolling_max(20).over("symbol").alias("high20"),
                pl.col("amount").rolling_mean(5).over("symbol").alias("amount5"),
                pl.col("amount").rolling_mean(20).over("symbol").alias("amount20"),
                pl.col("amount").rolling_median(20).over("symbol").alias("amount_median20"),
                raw_close.alias("raw_close"),
                previous_raw.alias("previous_raw_close"),
                limit_pct.alias("limit_pct"),
            )
            .with_columns(
                (pl.col("close") > pl.col("ma20")).cast(pl.Float64).alias("above_ma20"),
                (pl.col("ret5") > 0).cast(pl.Float64).alias("positive5"),
                (pl.col("amount5") / pl.col("amount20")).clip(0, 3).alias("amount_expansion"),
                (
                    pl.col("raw_close")
                    >= pl.col("previous_raw_close") * (1 + pl.col("limit_pct")) - 0.005
                ).cast(pl.Float64).alias("limit_hit"),
            )
            .with_columns(
                pl.col("limit_hit").rolling_sum(5).over("symbol").alias("limit_5d"),
                pl.col("limit_hit").rolling_sum(20).over("symbol").alias("limit_20d"),
            )
            .filter(
                pl.col("eligible")
                & pl.col("ret20").is_not_null()
                & (pl.col("amount_median20") >= MIN_DAILY_AMOUNT)
            )
        )

    @staticmethod
    def _industry_scores(frame: pl.DataFrame, keys: list[str], prefix: str) -> pl.DataFrame:
        grouped = (
            frame.group_by(["date", *keys])
            .agg(
                pl.len().alias("members"),
                pl.col("ret5").mean().alias("ret5"),
                pl.col("ret20").mean().alias("ret20"),
                pl.col("above_ma20").mean().alias("ma_breadth"),
                pl.col("positive5").mean().alias("positive_breadth"),
                pl.col("amount_expansion").mean().alias("amount_expansion"),
                (pl.col("limit_5d") > 0).mean().alias("limit_breadth"),
            )
            .sort([*keys, "date"])
        )
        ranked = grouped.with_columns(
            _percentile("ret5").alias("rps5"),
            _percentile("ret20").alias("rps20"),
            _percentile("ma_breadth").alias("ma_rank"),
            _percentile("positive_breadth").alias("positive_rank"),
            _percentile("amount_expansion").alias("amount_rank"),
            _percentile("limit_breadth").alias("limit_rank"),
        ).with_columns(
            (
                pl.col("rps5") * 0.25
                + pl.col("rps20") * 0.20
                + pl.col("ma_rank") * 0.15
                + pl.col("positive_rank") * 0.10
                + pl.col("amount_rank") * 0.15
                + pl.col("limit_rank") * 0.10
            ).alias("base_score")
        )
        return (
            ranked.sort([*keys, "date"])
            .with_columns(
                (_percentile("base_score") >= 75).cast(pl.Float64).alias("top_quartile")
            )
            .with_columns(
                pl.col("top_quartile").rolling_mean(3).over(keys).alias("persistence")
            )
            .with_columns(
                (pl.col("base_score") + pl.col("persistence").fill_null(0) * 5)
                .clip(0, 100)
                .round(2)
                .alias(f"{prefix}_score")
            )
        )

    def _build(self) -> None:
        standard = str(self.requirement.get("industry_standard") or SW_STANDARD)
        l1_level, l2_level = tuple(self.requirement.get("industry_levels") or (1, 2))
        lookback_days = int(self.requirement.get("lookback_days") or 60)
        min_coverage = float(self.requirement.get("min_coverage") or 0.95)
        load_start = _calendar_start(self.start, lookback_days)

        instruments = self.repo.get_instruments_asset("stock")
        if instruments.is_empty() or not {"symbol", "name"}.issubset(instruments.columns):
            raise ValueError("主线快照缺少股票标的目录")
        exchange = pl.col("exchange") if "exchange" in instruments.columns else pl.col("symbol").str.slice(-2)
        instruments = instruments.filter(exchange.is_in(["SH", "SZ"]))
        minute_symbols = self.repo.get_minute_symbols("stock", self.start, self.end)
        if minute_symbols:
            instruments = instruments.filter(pl.col("symbol").is_in(minute_symbols))

        daily = self._features(self._eligible_rows(self._load_daily(instruments, load_start), instruments))
        history_path = (
            Path(self.repo.store.data_dir)
            / "pit_reference/history/industry_membership_history/part.parquet"
        )
        if not history_path.exists():
            raise ValueError(f"PIT 行业历史不存在: {history_path}")
        industry = pl.read_parquet(history_path)
        joined = _join_industry(
            daily, industry, standard=standard, level=int(l1_level), prefix="l1",
        )
        joined = _join_industry(
            joined, industry, standard=standard, level=int(l2_level), prefix="l2",
        )

        coverage = (
            joined.filter(pl.col("date") >= self.start - timedelta(days=1))
            .group_by("date")
            .agg(
                pl.len().alias("eligible_count"),
                (pl.col("l1_name").is_not_null() & pl.col("l2_name").is_not_null())
                .sum().alias("classified_count"),
            )
            .with_columns(
                (pl.col("classified_count") / pl.col("eligible_count")).alias("coverage")
            )
        )
        measured_coverage = coverage.filter(pl.col("date") >= self.start)
        if (
            measured_coverage.is_empty()
            or float(measured_coverage["coverage"].min()) < min_coverage
        ):
            actual = (
                float(measured_coverage["coverage"].min())
                if not measured_coverage.is_empty() else 0.0
            )
            raise ValueError(
                f"PIT 申万一二级行业最低覆盖率 {actual:.2%}，低于 {min_coverage:.2%}"
            )
        classified = joined.filter(
            pl.col("l1_name").is_not_null() & pl.col("l2_name").is_not_null()
        ).with_columns(
            _industry_key("l1", standard, int(l1_level)).alias("l1_key"),
            _industry_key("l2", standard, int(l2_level)).alias("l2_key"),
        )

        benchmark = self.repo.get_daily_asset(
            "index", self.benchmark_symbol, load_start, self.end, ["date", "close"],
        )
        if benchmark.is_empty():
            raise ValueError(f"主线快照缺少基准 {self.benchmark_symbol} 日线")
        benchmark = benchmark.sort("date").with_columns(
            (pl.col("close") / pl.col("close").shift(5) - 1).alias("benchmark_ret5")
        ).select("date", "benchmark_ret5")

        l1 = self._industry_scores(classified, ["l1_key", "l1_name"], "l1").join(
            benchmark, on="date", how="left",
        ).with_columns((pl.col("ret5") - pl.col("benchmark_ret5")).alias("excess5"))
        selected_l1 = (
            l1.filter((pl.col("l1_score") >= 60) & (pl.col("excess5") > 0))
            .sort(["date", "l1_score"], descending=[False, True])
            .with_columns(pl.col("l1_score").rank("ordinal", descending=True).over("date").alias("rank"))
            .filter(pl.col("rank") <= 3)
        )
        l2 = self._industry_scores(classified, ["l1_key", "l1_name", "l2_key", "l2_name"], "l2")
        selected_l2 = (
            l2.join(
                selected_l1.select("date", "l1_key", "l1_score"),
                on=["date", "l1_key"],
                how="inner",
            )
            .filter((pl.col("l2_score") >= 55) & (pl.col("members") >= 5))
            .sort(["date", "l1_key", "l2_score"], descending=[False, False, True])
            .with_columns(
                pl.col("l2_score").rank("ordinal", descending=True)
                .over(["date", "l1_key"]).alias("rank")
            )
            .filter(pl.col("rank") <= 2)
        )

        candidates = (
            classified.join(
                selected_l2.select("date", "l1_key", "l2_key", "l1_score", "l2_score"),
                on=["date", "l1_key", "l2_key"],
                how="inner",
            )
            .join(
                l1.select("date", "l1_key", pl.col("ret20").alias("l1_ret20")),
                on=["date", "l1_key"],
                how="left",
            )
            .join(
                l2.select("date", "l2_key", pl.col("ret5").alias("l2_ret5")),
                on=["date", "l2_key"],
                how="left",
            )
            .with_columns(
                (pl.col("ret5") - pl.col("l2_ret5")).alias("l2_excess5"),
                (pl.col("ret20") - pl.col("l1_ret20")).alias("l1_excess20"),
                (pl.col("close") / pl.col("high20")).clip(0, 1.1).alias("near_high"),
            )
            .with_columns(
                _percentile("l2_excess5").alias("stock_rs5"),
                _percentile("l1_excess20").alias("stock_rs20"),
                _percentile("near_high").alias("stock_high_rank"),
                _percentile("amount_expansion").alias("stock_amount_rank"),
                _percentile("above_ma20").alias("stock_ma_rank"),
                _percentile("limit_20d").alias("stock_limit_rank"),
            )
            .with_columns(
                (
                    pl.col("stock_rs5") * 0.25
                    + pl.col("stock_rs20") * 0.20
                    + pl.col("stock_high_rank") * 0.15
                    + pl.col("stock_amount_rank") * 0.20
                    + pl.col("stock_ma_rank") * 0.10
                    + pl.col("stock_limit_rank") * 0.10
                ).round(2).alias("stock_score")
            )
            .sort(["date", "l2_key", "stock_score"], descending=[False, False, True])
            .with_columns(
                pl.col("stock_score").rank("ordinal", descending=True)
                .over(["date", "l2_key"]).alias("rank")
            )
            .filter(pl.col("rank") <= 5)
        )

        coverage_by_date = {row["date"]: row for row in coverage.to_dicts()}
        l1_by_date = {key[0]: part.to_dicts() for key, part in selected_l1.partition_by("date", as_dict=True).items()}
        l2_by_date = {key[0]: part.to_dicts() for key, part in selected_l2.partition_by("date", as_dict=True).items()}
        candidates_by_date = {key[0]: part.to_dicts() for key, part in candidates.partition_by("date", as_dict=True).items()}
        trading_dates = benchmark["date"].to_list()
        for index, as_of in enumerate(trading_dates[:-1]):
            trading_day = trading_dates[index + 1]
            if not self.start <= trading_day <= self.end:
                continue
            day_coverage = coverage_by_date.get(as_of, {})
            coverage_value = float(day_coverage.get("coverage") or 0)
            if coverage_value < min_coverage:
                self._snapshots[trading_day] = {
                    "date": trading_day.isoformat(),
                    "as_of": as_of.isoformat(),
                    "coverage": coverage_value,
                    "eligible_count": int(day_coverage.get("eligible_count") or 0),
                    "classified_count": int(day_coverage.get("classified_count") or 0),
                    "industries": [],
                    "subindustries": [],
                    "candidates": [],
                }
                continue
            available_symbols = self._minute_available_symbols(trading_day)
            candidate_rows = [
                row for row in candidates_by_date.get(as_of, [])
                if available_symbols is None or str(row["symbol"]) in available_symbols
            ][:30]
            clean_candidates = [
                {
                    "symbol": str(row["symbol"]),
                    "name": str(row.get("pit_name") or ""),
                    "l1_key": str(row["l1_key"]),
                    "l1_name": str(row["l1_name"]),
                    "l2_key": str(row["l2_key"]),
                    "l2_name": str(row["l2_name"]),
                    "l1_score": float(row["l1_score"]),
                    "l2_score": float(row["l2_score"]),
                    "stock_score": float(row["stock_score"]),
                    "previous_raw_close": float(row.get("raw_close") or row["close"]),
                }
                for row in candidate_rows
            ]
            self._all_symbols.update(row["symbol"] for row in clean_candidates)
            self._snapshots[trading_day] = {
                "date": trading_day.isoformat(),
                "as_of": as_of.isoformat(),
                "coverage": float(day_coverage.get("coverage") or 0),
                "eligible_count": int(day_coverage.get("eligible_count") or 0),
                "classified_count": int(day_coverage.get("classified_count") or 0),
                "industries": [
                    {"key": str(row["l1_key"]), "name": str(row["l1_name"]), "score": float(row["l1_score"])}
                    for row in l1_by_date.get(as_of, [])
                ],
                "subindustries": [
                    {
                        "key": str(row["l2_key"]), "name": str(row["l2_name"]),
                        "parent_key": str(row["l1_key"]), "score": float(row["l2_score"]),
                    }
                    for row in l2_by_date.get(as_of, [])
                ],
                "candidates": clean_candidates,
            }


def configure_mainline_snapshot(
    engine: Any,
    repo: Any,
    start: date,
    end: date,
) -> MainlineSnapshotCache | None:
    requirement = engine.mainline_snapshot_requirement
    if requirement is None:
        return None
    cache = MainlineSnapshotCache(
        repo,
        start,
        end,
        requirement,
        benchmark_symbol=engine.config.benchmark_symbol or BENCHMARK_SYMBOL,
    )
    engine.set_mainline_snapshot_loader(cache.snapshot)
    bootstrap = cache.bootstrap_symbols
    if not bootstrap:
        raise ValueError("主线快照在回测区间内没有可用候选股")
    engine.context.set_universe(bootstrap)
    return cache
