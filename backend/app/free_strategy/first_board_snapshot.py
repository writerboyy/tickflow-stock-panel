"""Point-in-time candidate snapshots for the large-turnover first-board strategy."""
from __future__ import annotations

import copy
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.price_limits import (
    polars_is_risk_warning_name,
    polars_limit_price,
    polars_price_limit_pct,
)

from .mainline_snapshot import _name_events


def _calendar_start(day: date, lookback_days: int) -> date:
    return day - timedelta(days=max(90, lookback_days * 3))


class FirstBoardSnapshotCache:
    """Build a daily limit-touch I/O index with D-1-only strategy features."""

    def __init__(
        self,
        repo: Any,
        start: date,
        end: date,
        requirement: dict[str, Any],
    ) -> None:
        self.repo = repo
        self.start = start
        self.end = end
        self.requirement = dict(requirement)
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
            "scan_index_only": "daily_high_limit_touch",
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

    @staticmethod
    def _eligible_rows(frame: pl.DataFrame, instruments: pl.DataFrame, repo: Any) -> pl.DataFrame:
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
        joined = joined.sort(["symbol", "date"]).join_asof(
            _name_events(repo, instruments),
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
            (listing_ok & delisting_ok & ~name.str.contains(r"ST|退市整理|退$"))
            .alias("eligible")
        )

    def _build(self) -> None:
        lookback_days = int(self.requirement.get("lookback_days") or 30)
        load_start = _calendar_start(self.start, lookback_days)
        instruments = self.repo.get_instruments_asset("stock")
        if instruments.is_empty() or not {"symbol", "name"}.issubset(instruments.columns):
            raise ValueError("首板扫描缺少股票标的目录")
        exchange = (
            pl.col("exchange")
            if "exchange" in instruments.columns
            else pl.col("symbol").str.slice(-2)
        )
        instruments = instruments.filter(exchange.is_in(["SH", "SZ"]))
        columns = [
            "symbol", "date", "open", "high", "low", "close", "volume", "amount",
            "raw_close", "raw_high", "raw_low", "total_shares",
        ]
        daily = self.repo.get_daily_asset_batch(
            "stock",
            instruments["symbol"].to_list(),
            load_start,
            self.end,
            columns,
        )
        if daily.is_empty():
            raise ValueError("首板扫描缺少全市场股票日线")
        required = {"symbol", "date", "close", "high", "amount"}
        missing = sorted(required - set(daily.columns))
        if missing:
            raise ValueError(f"首板扫描日线缺少字段: {', '.join(missing)}")
        market_days = sorted(daily["date"].unique().to_list())
        previous_market_day = {
            market_days[index]: market_days[index - 1]
            for index in range(1, len(market_days))
        }
        for column in ("raw_close", "raw_high", "total_shares"):
            if column not in daily.columns:
                daily = daily.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))

        frame = self._eligible_rows(daily, instruments, self.repo).sort(["symbol", "date"])
        raw_close = pl.coalesce("raw_close", "close")
        raw_high = pl.coalesce("raw_high", "high")
        limit_pct = polars_price_limit_pct(
            pl.col("symbol"),
            pl.col("date"),
            polars_is_risk_warning_name(pl.col("pit_name")),
        )
        frame = frame.with_columns(
            raw_close.alias("_raw_close"),
            raw_high.alias("_raw_high"),
            limit_pct.alias("_limit_pct"),
            (pl.col("close") / pl.col("close").shift(5).over("symbol") - 1).alias("ret5"),
            (pl.col("close") / pl.col("close").shift(20).over("symbol") - 1).alias("ret20"),
            pl.col("close").rolling_mean(20).over("symbol").alias("ma20"),
            pl.col("amount").rolling_mean(5).over("symbol").alias("amount5"),
            pl.col("amount").rolling_mean(20).over("symbol").alias("amount20"),
            pl.col("amount").rolling_median(20).over("symbol").alias("amount_median20"),
            (raw_close * pl.col("total_shares")).alias("market_cap"),
        ).with_columns(
            pl.col("_raw_close").shift(1).over("symbol").alias("previous_raw_close"),
            polars_limit_price(
                pl.col("_raw_close").shift(1).over("symbol"),
                pl.col("_limit_pct"),
                up=True,
            ).alias("limit_price"),
            (
                pl.col("_raw_close")
                >= polars_limit_price(
                    pl.col("_raw_close").shift(1).over("symbol"),
                    pl.col("_limit_pct"),
                    up=True,
                ) - 0.005
            ).cast(pl.Int8).alias("limit_close"),
        ).with_columns(
            pl.col("ret5").shift(1).over("symbol").alias("ret5_d1"),
            pl.col("ret20").shift(1).over("symbol").alias("ret20_d1"),
            (pl.col("close") > pl.col("ma20")).shift(1).over("symbol").alias("above_ma20_d1"),
            (pl.col("amount5") / pl.col("amount20")).shift(1).over("symbol")
            .alias("amount_expansion_d1"),
            pl.col("amount_median20").shift(1).over("symbol").alias("amount_median20_d1"),
            pl.col("market_cap").shift(1).over("symbol").alias("market_cap_d1"),
            pl.col("limit_close").shift(1).rolling_sum(5).over("symbol")
            .alias("prior_limit_close_5d"),
        ).filter(
            (pl.col("date") >= self.start)
            & (pl.col("date") <= self.end)
            & pl.col("eligible")
            & (pl.col("_raw_high") >= pl.col("limit_price") - 0.005)
            & pl.col("ret20_d1").is_not_null()
        )

        for trading_day, rows in frame.partition_by("date", as_dict=True).items():
            day = trading_day[0] if isinstance(trading_day, tuple) else trading_day
            minute_symbols = self._minute_available_symbols(day)
            candidates = []
            for row in rows.iter_rows(named=True):
                symbol = str(row["symbol"])
                if minute_symbols is not None and symbol not in minute_symbols:
                    continue
                candidates.append({
                    "symbol": symbol,
                    "name": str(row.get("pit_name") or ""),
                    "limit_price": float(row["limit_price"]),
                    "previous_raw_close": float(row["previous_raw_close"]),
                    "ret5_d1": float(row["ret5_d1"]),
                    "ret20_d1": float(row["ret20_d1"]),
                    "above_ma20_d1": bool(row.get("above_ma20_d1")),
                    "amount_expansion_d1": float(row.get("amount_expansion_d1") or 0),
                    "amount_median20_d1": float(row.get("amount_median20_d1") or 0),
                    "market_cap_d1": float(row.get("market_cap_d1") or 0),
                    "prior_limit_close_5d": int(row.get("prior_limit_close_5d") or 0),
                })
            candidates.sort(key=lambda item: item["symbol"])
            self._all_symbols.update(row["symbol"] for row in candidates)
            self._snapshots[day] = {
                "date": day.isoformat(),
                "as_of": (
                    previous_market_day[day].isoformat()
                    if day in previous_market_day else None
                ),
                "scan_index_only": "daily_high_limit_touch",
                "candidates": candidates,
            }


def configure_first_board_snapshot(
    engine: Any,
    repo: Any,
    start: date,
    end: date,
) -> FirstBoardSnapshotCache | None:
    requirement = engine.limit_board_snapshot_requirement
    if requirement is None:
        return None
    if engine.mainline_snapshot_requirement is not None:
        raise ValueError("同一策略不能同时声明主线快照和首板扫描快照")
    cache = FirstBoardSnapshotCache(repo, start, end, requirement)
    engine.set_limit_board_snapshot_loader(cache.snapshot)
    bootstrap = cache.bootstrap_symbols
    if not bootstrap:
        raise ValueError("首板扫描快照在回测区间内没有可用候选股")
    engine.context.set_universe(bootstrap)
    return cache
