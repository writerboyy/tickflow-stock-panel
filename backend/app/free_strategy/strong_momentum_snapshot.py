"""Point-in-time daily candidates for the minute strong-momentum strategy."""
from __future__ import annotations

import copy
from datetime import date, time, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from app.price_limits import (
    polars_is_risk_warning_name,
    polars_limit_price,
    polars_price_limit_pct,
)
from app.market_time import cn_today
from app.services.ingestion_manifest import load_ingestion_manifest

from .mainline_snapshot import _name_events


def _calendar_start(day: date, lookback_days: int) -> date:
    return day - timedelta(days=max(120, lookback_days * 4))


def _with_candidate_features(frame: pl.DataFrame) -> pl.DataFrame:
    """Compute every selection field from D-1 or earlier data."""
    raw_close = pl.coalesce("raw_close", "close")
    raw_high = pl.coalesce("raw_high", "high")
    raw_low = pl.coalesce("raw_low", "low")
    limit_pct = polars_price_limit_pct(
        pl.col("symbol"),
        pl.col("date"),
        polars_is_risk_warning_name(pl.col("pit_name")),
    )
    frame = frame.with_columns(
        raw_close.alias("_raw_close"),
        raw_high.alias("_raw_high"),
        raw_low.alias("_raw_low"),
        limit_pct.alias("_limit_pct"),
    ).with_columns(
        polars_limit_price(
            pl.col("_raw_close").shift(1).over("symbol"),
            pl.col("_limit_pct"),
            up=True,
        ).alias("_limit_up"),
        polars_limit_price(
            pl.col("_raw_close").shift(1).over("symbol"),
            pl.col("_limit_pct"),
            up=False,
        ).alias("_limit_down"),
    ).with_columns(
        (pl.col("_raw_close") >= pl.col("_limit_up") - 0.005).alias("_is_limit_up"),
        (pl.col("_raw_close") <= pl.col("_limit_down") + 0.005).alias("_is_limit_down"),
    )
    return frame.with_columns(
        pl.col("_raw_close").shift(1).over("symbol").alias("previous_raw_close"),
        pl.col("_limit_up").alias("limit_price"),
        (pl.col("close").shift(1).over("symbol") / pl.col("close").shift(2).over("symbol") - 1)
        .alias("previous_change"),
        (pl.col("close").shift(1).over("symbol") / pl.col("close").shift(4).over("symbol") - 1)
        .alias("previous_ret3"),
        (pl.col("close").shift(1).over("symbol") / pl.col("close").shift(6).over("symbol") - 1)
        .alias("previous_ret5"),
        (pl.col("close").shift(1).over("symbol") / pl.col("close").shift(21).over("symbol") - 1)
        .alias("previous_ret20"),
        (
            (pl.col("_raw_high").shift(1).over("symbol") - pl.col("_raw_low").shift(1).over("symbol"))
            / pl.col("_raw_close").shift(1).over("symbol")
        ).alias("previous_amplitude"),
        pl.col("turnover_rate").shift(1).over("symbol").alias("previous_turnover_rate"),
        (pl.col("volume").shift(1).over("symbol") / pl.col("volume").shift(2).over("symbol"))
        .alias("previous_volume_growth"),
        pl.col("_is_limit_up").shift(1).over("symbol").fill_null(False)
        .alias("previous_limit_up"),
        pl.col("_is_limit_down").shift(1).rolling_sum(3).over("symbol").fill_null(0)
        .alias("recent_limit_down_count"),
        (
            (pl.col("close").shift(1).over("symbol") > pl.col("close").shift(2).over("symbol"))
            & (pl.col("close").shift(2).over("symbol") > pl.col("close").shift(3).over("symbol"))
            & (pl.col("volume").shift(1).over("symbol") < pl.col("volume").shift(2).over("symbol"))
            & (pl.col("volume").shift(2).over("symbol") < pl.col("volume").shift(3).over("symbol"))
        ).fill_null(False).alias("previous_shrink_rise_3d"),
    )


def _filter_source_candidates(frame: pl.DataFrame) -> pl.DataFrame:
    """Apply the source strategy's board-specific static filters."""
    gem = (
        pl.col("symbol").str.starts_with("300")
        | pl.col("symbol").str.starts_with("301")
        | pl.col("symbol").str.starts_with("302")
    )
    return frame.with_columns(gem.alias("_is_gem")).filter(
        pl.when(pl.col("_is_gem"))
        .then(pl.col("previous_ret3") <= 0.72)
        .otherwise(pl.col("previous_ret5") <= 0.60)
        & (pl.col("previous_ret20") < 0.95)
        & pl.when(pl.col("_is_gem"))
        .then(pl.col("previous_amplitude") < 0.20)
        .otherwise(pl.col("previous_amplitude") < 0.16)
        & pl.when(pl.col("_is_gem"))
        .then(pl.col("previous_turnover_rate") < 28.0)
        .otherwise(pl.col("previous_turnover_rate") < 24.0)
        & (pl.col("previous_turnover_rate") >= 2.0)
        & (pl.col("recent_limit_down_count") == 0)
        & ~pl.col("previous_shrink_rise_3d")
    )


class StrongMomentumSnapshotCache:
    """Build compact D-1 strong-stock snapshots without future data."""

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
        self._auction_signatures: dict[date, tuple[Any, ...]] = {}
        self._build()

    @property
    def all_symbols(self) -> list[str]:
        return sorted(self._all_symbols)

    @property
    def bootstrap_symbols(self) -> list[str]:
        for day in sorted(self._snapshots):
            payload = self._snapshots[day]
            rows = payload.get("static_candidates") or payload.get("candidates") or []
            symbols = [str(row["symbol"]) for row in rows]
            if symbols:
                return symbols
        return []

    def snapshot(self, trading_day: date) -> dict[str, Any]:
        if trading_day in self._snapshots and self.requirement.get("require_auction"):
            signature = self._auction_signature(trading_day)
            if signature != self._auction_signatures.get(trading_day):
                self._build()
        value = self._snapshots.get(trading_day)
        return copy.deepcopy(value) if value is not None else {
            "date": trading_day.isoformat(),
            "as_of": None,
            "selection_mode": "strict",
            "candidates": [],
        }

    def _auction_signature(self, day: date) -> tuple[Any, ...]:
        data_dir = Path(self.repo.store.data_dir)
        manifest_path = (
            data_dir / "ext_data" / "_ingestion" / "kaipanla"
            / "auction_completion" / f"{day.isoformat()}.json"
        )
        try:
            stat = manifest_path.stat()
            manifest = (True, stat.st_mtime_ns, stat.st_size)
        except OSError:
            manifest = (False,)
        partition = data_dir / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
        files = []
        for path in sorted(partition.glob("*.parquet")):
            try:
                stat = path.stat()
            except OSError:
                continue
            files.append((path.name, stat.st_mtime_ns, stat.st_size))
        return (*manifest, tuple(files))

    def _auction_rows(self, day: date) -> tuple[dict[str, dict[str, Any]], list[str]]:
        data_dir = Path(self.repo.store.data_dir)
        manifest = load_ingestion_manifest(data_dir, "kaipanla", "auction_completion", day.isoformat())
        component = (manifest.get("components") or {}).get("strong_momentum_bid_detail")
        if day == cn_today() and self.requirement.get("require_auction"):
            if not isinstance(component, dict):
                return {}, ["缺少强者恒强 /31 竞价明细"]
            if component.get("status") == "valid_empty":
                return {}, []
            if component.get("status") != "complete":
                return {}, ["缺少强者恒强 /31 竞价明细"]
        root = data_dir / "ext_data" / "ext_kpl_auction" / "timeseries" / f"date={day.isoformat()}"
        files = sorted(root.glob("*.parquet"))
        if not files:
            return {}, []
        try:
            frame = pl.concat([pl.read_parquet(path) for path in files], how="diagonal_relaxed")
        except Exception:
            return {}, ["竞价 parquet 无法读取"]
        if frame.is_empty() or "symbol" not in frame.columns:
            return {}, []
        return {
            str(row.get("symbol") or "").strip().upper(): row
            for row in frame.to_dicts()
            if row.get("symbol")
        }, []

    def _tail_gains(self, symbols: list[str], trading_day: date) -> dict[str, float]:
        if not symbols:
            return {}
        frame = self.repo.get_minute_range(symbols, trading_day, trading_day, "stock")
        if frame.is_empty() or not {"symbol", "datetime", "close"}.issubset(frame.columns):
            return {}
        tail = (
            frame.filter(pl.col("datetime").dt.time() >= time(14, 30))
            .sort(["symbol", "datetime"])
            .group_by("symbol", maintain_order=True)
            .agg(
                pl.col("close").first().alias("start_close"),
                pl.col("close").last().alias("end_close"),
            )
            .filter(pl.col("start_close") > 0)
            .with_columns((pl.col("end_close") / pl.col("start_close") - 1).alias("gain"))
        )
        return {str(symbol): float(gain) for symbol, gain in tail.select("symbol", "gain").iter_rows()}

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
        joined = joined.sort(["symbol", "date"]).join_asof(
            _name_events(self.repo, instruments),
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
        risk_name = pl.col("pit_name").fill_null("").str.to_uppercase().str.contains(r"ST|退市整理|退$")
        return joined.with_columns((listing_ok & delisting_ok & ~risk_name).alias("eligible"))

    def _build(self) -> None:
        lookback_days = int(self.requirement.get("lookback_days") or 30)
        instruments = self.repo.get_instruments_asset("stock")
        if instruments.is_empty() or not {"symbol", "name"}.issubset(instruments.columns):
            raise ValueError("强者恒强快照缺少股票标的目录")
        instruments = instruments.filter(
            pl.col("symbol").str.contains(
                r"^(60\d{4}\.SH|00[01]\d{3}\.SZ|30[012]\d{3}\.SZ)$"
            )
        )
        columns = [
            "symbol", "date", "open", "high", "low", "close", "volume", "amount",
            "raw_close", "raw_high", "raw_low", "turnover_rate",
        ]
        daily = self.repo.get_daily_asset_batch(
            "stock",
            instruments["symbol"].to_list(),
            _calendar_start(self.start, lookback_days),
            self.end,
            columns,
        )
        required = {"symbol", "date", "high", "low", "close", "volume", "turnover_rate"}
        missing = sorted(required - set(daily.columns))
        if daily.is_empty() or missing:
            detail = f": {', '.join(missing)}" if missing else ""
            raise ValueError(f"强者恒强快照缺少全市场日线或必需字段{detail}")
        market_days = sorted(daily["date"].unique().to_list())
        previous_market_day = {
            market_days[index]: market_days[index - 1]
            for index in range(1, len(market_days))
        }
        frame = _filter_source_candidates(
            _with_candidate_features(
                self._eligible_rows(daily.sort(["symbol", "date"]), instruments)
            ).filter(
                (pl.col("date") >= self.start) & (pl.col("date") <= self.end) & pl.col("eligible")
            )
        )

        for key, rows in frame.partition_by("date", as_dict=True).items():
            day = key[0] if isinstance(key, tuple) else key
            strict = rows.filter(pl.col("previous_change") >= 0.06)
            selected = strict if not strict.is_empty() else rows.filter(pl.col("previous_change") >= 0.05)
            selection_mode = "strict" if not strict.is_empty() else "fallback"
            # Candidate membership comes from the source rules only.  Market-data
            # coverage is checked by the backtest/readiness path and must not
            # silently change the strategy's selected symbols.
            raw_rows = list(selected.iter_rows(named=True))
            as_of = previous_market_day.get(day)
            tail_gains = self._tail_gains(
                [str(row["symbol"]) for row in raw_rows],
                as_of,
            ) if as_of is not None else {}
            candidates = []
            for row in raw_rows:
                symbol = str(row["symbol"])
                tail_gain = tail_gains.get(symbol)
                tail_threshold = (
                    0.08
                    if str(row["symbol"]).startswith(("300", "301", "302"))
                    else 0.05
                )
                if tail_gain is not None and tail_gain > tail_threshold:
                    continue
                volume_growth = float(row.get("previous_volume_growth") or 0)
                candidates.append({
                    "symbol": symbol,
                    "name": str(row.get("pit_name") or ""),
                    "previous_raw_close": float(row["previous_raw_close"]),
                    "limit_price": float(row["limit_price"]),
                    "previous_change": float(row["previous_change"]),
                    "previous_ret5": float(row["previous_ret5"]),
                    "previous_ret20": float(row["previous_ret20"]),
                    "previous_amplitude": float(row["previous_amplitude"]),
                    "previous_turnover_rate": float(row["previous_turnover_rate"]),
                    "previous_volume_growth": volume_growth,
                    "previous_limit_up": bool(row.get("previous_limit_up")),
                    "previous_high_volume_limit": bool(row.get("previous_limit_up") and volume_growth >= 1.8),
                    "tail_gain_d1": tail_gain,
                })
            candidates.sort(key=lambda item: (item["previous_change"], item["symbol"]), reverse=True)
            static_candidates = copy.deepcopy(candidates)
            snapshot_state = "ready"
            data_gaps: list[str] = []
            if day == cn_today() and self.requirement.get("require_auction"):
                auction_rows, data_gaps = self._auction_rows(day)
                if data_gaps:
                    candidates = []
                    snapshot_state = "waiting_data"
                else:
                    confirmed = []
                    missing = 0
                    for candidate in candidates:
                        auction = auction_rows.get(candidate["symbol"], {})
                        value = auction.get("auction_change_pct_0925")
                        try:
                            value = float(value)
                        except (TypeError, ValueError):
                            value = None
                        if value is None or auction.get("source_0925") != "/31":
                            missing += 1
                            continue
                        confirmed.append({
                            **candidate,
                            "auction_change_pct_0925": value,
                            "auction_source": "/31",
                            "auction_required": True,
                        })
                    if missing:
                        data_gaps = [f"缺少强者恒强 /31 竞价明细（{missing}只）"]
                        candidates = []
                        snapshot_state = "waiting_data"
                    else:
                        candidates = confirmed
            self._all_symbols.update(row["symbol"] for row in candidates)
            self._snapshots[day] = {
                "date": day.isoformat(),
                "as_of": as_of.isoformat() if as_of is not None else None,
                "selection_mode": selection_mode,
                "candidates": candidates,
                "static_candidates": static_candidates,
                "state": snapshot_state,
                "data_gaps": data_gaps,
            }
            self._auction_signatures[day] = self._auction_signature(day)


def configure_strong_momentum_snapshot(
    engine: Any,
    repo: Any,
    start: date,
    end: date,
) -> StrongMomentumSnapshotCache | None:
    requirement = engine.strong_momentum_snapshot_requirement
    if requirement is None:
        return None
    if engine.mainline_snapshot_requirement is not None or engine.limit_board_snapshot_requirement is not None:
        raise ValueError("同一策略只能声明一种动态候选快照")
    cache = StrongMomentumSnapshotCache(repo, start, end, requirement)
    engine.set_strong_momentum_snapshot_loader(cache.snapshot)
    bootstrap = cache.bootstrap_symbols
    if not bootstrap:
        raise ValueError("强者恒强快照在回测区间内没有可用候选股")
    engine.context.set_universe(bootstrap)
    return cache


def strong_momentum_bid_symbols(
    repo: Any,
    trade_date: date,
    requirement: dict[str, Any] | None = None,
) -> list[str]:
    """Return static strong-momentum candidates for targeted /31 collection."""
    cache = StrongMomentumSnapshotCache(
        repo,
        trade_date,
        trade_date,
        requirement or {"lookback_days": 30, "require_auction": True},
    )
    snapshot = cache.snapshot(trade_date)
    return sorted({
        str(row.get("symbol") or "").strip().upper()
        for row in (snapshot.get("static_candidates") or snapshot.get("candidates") or [])
        if row.get("symbol")
    })
