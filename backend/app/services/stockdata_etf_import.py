"""Audited import of local StockData ETF parquet files.

The importer is intentionally narrow: it only fills currently known ETF
symbols, existing canonical rows always win, and publishing is opt-in.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
from uuid import uuid4

import duckdb
import polars as pl
import pyarrow.parquet as pq

from app.indicators.pipeline import ENRICHED_STORAGE_COLS, compute_enriched
from app.parquet import scan_daily_parquet
from app.services.storage_safety import assert_disk_reserve


class StockDataEtfImportBlocked(RuntimeError):
    """A source or publication safety check failed."""


@dataclass(frozen=True)
class StockDataEtfImportConfig:
    source_dir: Path
    data_dir: Path
    start: date = date(2019, 1, 1)
    end: date = field(default_factory=date.today)
    publish: bool = False
    run_id: str | None = None
    severe_close_tolerance: float = 0.005

    def normalized(self) -> StockDataEtfImportConfig:
        source_dir = self.source_dir.expanduser().resolve()
        data_dir = self.data_dir.expanduser().resolve()
        if self.start > self.end:
            raise ValueError("start must not be later than end")
        if self.severe_close_tolerance <= 0:
            raise ValueError("severe_close_tolerance must be positive")
        for path in (
            source_dir / "etf_daily.parquet",
            source_dir / "etf_1min",
            data_dir / "instruments_etf",
        ):
            if not path.exists():
                raise ValueError(f"required path does not exist: {path}")
        run_id = self.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8]
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", run_id):
            raise ValueError("run_id contains unsafe characters")
        return StockDataEtfImportConfig(
            source_dir=source_dir,
            data_dir=data_dir,
            start=self.start,
            end=self.end,
            publish=self.publish,
            run_id=run_id,
            severe_close_tolerance=self.severe_close_tolerance,
        )


def _sql_string(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _parquet_source(paths: list[Path], *, filename: bool = False) -> str:
    if not paths:
        raise StockDataEtfImportBlocked("no parquet files matched the import scope")
    values = ",".join(_sql_string(path) for path in paths)
    return (
        f"read_parquet([{values}], union_by_name=true"
        + (", filename=true" if filename else "")
        + ")"
    )


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _collapse_staged_partition(directory: Path) -> None:
    parts = sorted(directory.glob("*.parquet"))
    if not parts:
        raise StockDataEtfImportBlocked(f"staged partition is empty: {directory}")
    target = directory / "part.parquet"
    if len(parts) == 1:
        if parts[0] != target:
            parts[0].replace(target)
        return
    merged = pl.concat([pl.read_parquet(path) for path in parts], how="vertical_relaxed").sort(
        ["symbol", "datetime"]
    )
    temporary = directory / f".part.parquet.{uuid4().hex}.tmp"
    try:
        merged.write_parquet(temporary)
        for path in parts:
            path.unlink()
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def _file_fingerprint(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _tree_fingerprint(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = sha256()
    for item in sorted(path.rglob("*.parquet")):
        stat = item.stat()
        digest.update(str(item.relative_to(path)).encode())
        digest.update(f":{stat.st_size}:{stat.st_mtime_ns}\n".encode())
    return digest.hexdigest()


class StockDataEtfImporter:
    def __init__(
        self,
        config: StockDataEtfImportConfig,
        *,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config.normalized()
        self.progress = progress or (lambda _message: None)
        self.run_root = (
            self.config.data_dir
            / "backfill_state"
            / "stockdata_etf"
            / str(self.config.run_id)
        )
        self._temporary = tempfile.TemporaryDirectory(prefix="tickflow-stockdata-etf-")
        self.db = duckdb.connect(":memory:")
        self.db.execute("PRAGMA threads=4")
        self.db.execute("SET temp_directory = ?", [self._temporary.name])
        self.minute_end_exclusive: date | None = None
        self.minute_paths: list[Path] = []
        self.ignored_duplicate_files: list[str] = []
        self._views_ready = False

    def close(self) -> None:
        self.db.close()
        self._temporary.cleanup()

    def _register_inputs(self) -> None:
        if self._views_ready:
            return
        data_dir = self.config.data_dir
        source_dir = self.config.source_dir
        instrument_paths = sorted((data_dir / "instruments_etf").rglob("*.parquet"))
        instruments = pl.concat(
            [pl.read_parquet(path).select("symbol") for path in instrument_paths],
            how="vertical_relaxed",
        ).unique().sort("symbol")
        if instruments.is_empty():
            raise StockDataEtfImportBlocked("current ETF instrument set is empty")
        symbols = [str(value) for value in instruments["symbol"].to_list()]
        minute_root = source_dir / "etf_1min"
        self.minute_paths = [minute_root / f"{symbol}.parquet" for symbol in symbols]
        self.minute_paths = [path for path in self.minute_paths if path.is_file()]
        canonical_names = {path.name for path in self.minute_paths}
        self.ignored_duplicate_files = sorted(
            path.name
            for path in minute_root.glob("*.parquet")
            if "(" in path.stem and path.name not in canonical_names
        )

        self.db.register("instrument_frame", instruments)
        self.db.execute(
            "CREATE TEMP TABLE current_instruments AS SELECT symbol FROM instrument_frame"
        )
        file_map = pl.DataFrame({
            "filename": [str(path) for path in self.minute_paths],
            "expected_symbol": [path.stem for path in self.minute_paths],
        })
        self.db.register("source_file_map_frame", file_map)
        self.db.execute(
            "CREATE TEMP TABLE source_file_map AS SELECT * FROM source_file_map_frame"
        )
        self.db.execute(
            f"CREATE TEMP VIEW source_minute AS SELECT * FROM "
            f"{_parquet_source(self.minute_paths, filename=True)}"
        )
        self.db.execute(
            f"CREATE TEMP VIEW source_daily AS SELECT * FROM "
            f"read_parquet({_sql_string(source_dir / 'etf_daily.parquet')})"
        )

        current_daily_paths = sorted((data_dir / "kline_etf_daily").rglob("*.parquet"))
        self.db.execute(
            f"CREATE TEMP VIEW current_daily AS SELECT * FROM "
            f"{_parquet_source(current_daily_paths)}"
        )
        adjustment_paths = sorted((data_dir / "adj_factor_etf").rglob("*.parquet"))
        if adjustment_paths:
            self.db.execute(
                f"CREATE TEMP VIEW current_adjustments AS SELECT * FROM "
                f"{_parquet_source(adjustment_paths)}"
            )
        else:
            self.db.execute(
                "CREATE TEMP TABLE current_adjustments "
                "(symbol VARCHAR, trade_date DATE, ex_factor DOUBLE)"
            )

        current_minute_paths = sorted((data_dir / "kline_etf_minute").rglob("*.parquet"))
        current_first: date | None = None
        if current_minute_paths:
            value = self.db.execute(
                f"SELECT min(datetime) FROM {_parquet_source(current_minute_paths)}"
            ).fetchone()[0]
            if value is not None:
                current_first = value.date()
        requested_end_exclusive = self.config.end + timedelta(days=1)
        self.minute_end_exclusive = min(
            requested_end_exclusive,
            current_first or requested_end_exclusive,
        )
        self.db.execute(
            "CREATE TEMP TABLE import_bounds AS "
            "SELECT CAST(? AS DATE) AS start_date, CAST(? AS DATE) AS end_date, "
            "CAST(? AS DATE) AS minute_end_exclusive",
            [self.config.start, self.config.end, self.minute_end_exclusive],
        )
        self._create_normalized_views()
        self._views_ready = True

    def _create_normalized_views(self) -> None:
        self.db.execute("""
            CREATE TEMP VIEW normalized_source_daily AS
            SELECT
                ts_code AS symbol,
                CAST(trade_date AS DATE) AS date,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(vol AS DOUBLE) AS volume,
                CAST(amount * 1000.0 AS DOUBLE) AS amount,
                CAST(adj_factor AS DOUBLE) AS adj_factor
            FROM source_daily
        """)
        self.db.execute("""
            CREATE TEMP VIEW daily_latest_external_factor AS
            SELECT
                d.symbol,
                arg_max(d.adj_factor, d.date) FILTER (WHERE d.adj_factor > 0) AS latest_factor,
                max(d.date) FILTER (WHERE d.adj_factor > 0) AS latest_factor_date
            FROM normalized_source_daily d
            JOIN current_instruments i USING (symbol)
            CROSS JOIN import_bounds b
            WHERE d.date <= b.end_date
            GROUP BY d.symbol
        """)
        self.db.execute("""
            CREATE TEMP TABLE symbols_without_daily_factor AS
            SELECT f.expected_symbol AS symbol
            FROM source_file_map f
            LEFT JOIN daily_latest_external_factor d
              ON d.symbol = f.expected_symbol
            WHERE d.latest_factor IS NULL
        """)
        self.db.execute("""
            CREATE TEMP VIEW minute_latest_external_factor AS
            SELECT
                m.ts_code AS symbol,
                arg_max(CAST(m.adj_factor AS DOUBLE), m.trade_time)
                    FILTER (WHERE m.adj_factor > 0) AS latest_factor,
                max(CAST(m.trade_time AS DATE))
                    FILTER (WHERE m.adj_factor > 0) AS latest_factor_date
            FROM source_minute m
            JOIN symbols_without_daily_factor s ON s.symbol = m.ts_code
            CROSS JOIN import_bounds b
            WHERE CAST(m.trade_time AS DATE) <= b.end_date
            GROUP BY m.ts_code
        """)
        self.db.execute("""
            CREATE TEMP VIEW latest_external_factor AS
            SELECT symbol, latest_factor, latest_factor_date
            FROM daily_latest_external_factor
            WHERE latest_factor IS NOT NULL
            UNION ALL
            SELECT symbol, latest_factor, latest_factor_date
            FROM minute_latest_external_factor
            WHERE latest_factor IS NOT NULL
        """)
        self.db.execute("""
            CREATE TEMP VIEW filled_source_daily_factor AS
            SELECT
                symbol,
                date,
                last_value(adj_factor IGNORE NULLS) OVER (
                    PARTITION BY symbol
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS adj_factor
            FROM normalized_source_daily
        """)
        self.db.execute("""
            CREATE TEMP VIEW factor_scale AS
            SELECT
                l.symbol,
                l.latest_factor
                    * exp(coalesce(sum(ln(a.ex_factor)) FILTER (WHERE a.ex_factor > 0), 0.0))
                    AS latest_factor,
                l.latest_factor_date
            FROM latest_external_factor l
            CROSS JOIN import_bounds b
            LEFT JOIN current_adjustments a
              ON a.symbol = l.symbol
             AND a.trade_date > l.latest_factor_date
             AND a.trade_date <= b.end_date
            GROUP BY l.symbol, l.latest_factor, l.latest_factor_date
        """)
        self.db.execute("""
            CREATE TEMP VIEW minute_base_pre AS
            SELECT
                m.ts_code AS symbol,
                CAST(m.trade_time AS TIMESTAMP) AS datetime,
                CAST(m.trade_time AS DATE) AS date,
                CAST(m.open AS DOUBLE) AS open,
                CAST(m.high AS DOUBLE) AS high,
                CAST(m.low AS DOUBLE) AS low,
                CAST(m.close AS DOUBLE) AS close,
                CAST(m.vol AS DOUBLE) AS source_volume,
                CAST(m.amount AS DOUBLE) AS amount,
                coalesce(CAST(m.adj_factor AS DOUBLE), d.adj_factor) AS bar_factor,
                s.latest_factor AS latest_factor
            FROM source_minute m
            JOIN source_file_map f
              ON m.filename = f.filename
             AND m.ts_code = f.expected_symbol
            LEFT JOIN filled_source_daily_factor d
              ON d.symbol = m.ts_code
             AND d.date = CAST(m.trade_time AS DATE)
            LEFT JOIN factor_scale s ON s.symbol = m.ts_code
            CROSS JOIN import_bounds b
            WHERE CAST(m.trade_time AS DATE) >= b.start_date
              AND CAST(m.trade_time AS DATE) < b.minute_end_exclusive
        """)
        self.db.execute("""
            CREATE TEMP TABLE unresolved_factor_days AS
            SELECT symbol, date
            FROM minute_base_pre
            WHERE bar_factor IS NULL
            GROUP BY symbol, date
        """)
        self.db.execute("""
            CREATE TEMP VIEW prior_minute_factor AS
            SELECT
                u.symbol,
                u.date,
                arg_max(CAST(m.adj_factor AS DOUBLE), m.trade_time)
                    FILTER (WHERE m.adj_factor > 0) AS prior_factor
            FROM unresolved_factor_days u
            LEFT JOIN source_minute m
              ON m.ts_code = u.symbol
             AND CAST(m.trade_time AS DATE) < u.date
            GROUP BY u.symbol, u.date
        """)
        self.db.execute("""
            CREATE TEMP VIEW minute_base AS
            SELECT
                p.symbol, p.datetime, p.date, p.open, p.high, p.low, p.close,
                p.source_volume, p.amount,
                coalesce(p.bar_factor, f.prior_factor) AS bar_factor,
                p.latest_factor
            FROM minute_base_pre p
            LEFT JOIN prior_minute_factor f USING (symbol, date)
        """)
        self.progress("Auditing minute symbol-days")
        self.db.execute("""
            CREATE TEMP TABLE minute_groups AS
            SELECT
                symbol,
                date,
                count(*)::BIGINT AS bars,
                count(DISTINCT datetime)::BIGINT AS unique_bars,
                arg_max(close, datetime) AS minute_close,
                sum(CASE WHEN source_volume = 0 THEN 1 ELSE 0 END)::BIGINT AS zero_volume_rows,
                sum(CASE WHEN
                    open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                    OR NOT isfinite(open) OR NOT isfinite(high)
                    OR NOT isfinite(low) OR NOT isfinite(close)
                    OR open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
                    OR high < greatest(open, close) OR low > least(open, close)
                    OR source_volume IS NULL OR amount IS NULL
                    OR NOT isfinite(source_volume) OR NOT isfinite(amount)
                    OR source_volume < 0 OR amount < 0
                THEN 1 ELSE 0 END)::BIGINT AS invalid_value_rows,
                sum(CASE WHEN NOT (
                    (hour(datetime) = 9 AND minute(datetime) >= 30)
                    OR hour(datetime) = 10
                    OR (hour(datetime) = 11 AND minute(datetime) <= 30)
                    OR (hour(datetime) = 13 AND minute(datetime) >= 1)
                    OR hour(datetime) = 14
                    OR (hour(datetime) = 15 AND minute(datetime) = 0)
                ) THEN 1 ELSE 0 END)::BIGINT AS invalid_session_rows,
                sum(CASE WHEN bar_factor IS NULL OR bar_factor <= 0
                              OR latest_factor IS NULL OR latest_factor <= 0
                         THEN 1 ELSE 0 END)::BIGINT AS unresolved_factor_rows
            FROM minute_base
            GROUP BY symbol, date
        """)
        self.db.execute("""
            CREATE TEMP VIEW minute_groups_control AS
            SELECT
                g.*,
                coalesce(e.close, c.close) AS daily_close
            FROM minute_groups g
            LEFT JOIN normalized_source_daily e USING (symbol, date)
            LEFT JOIN current_daily c USING (symbol, date)
        """)
        self.db.execute("""
            CREATE TEMP TABLE excluded_minute_days AS
            SELECT g.symbol, g.date,
                   abs(g.minute_close - g.daily_close) / nullif(abs(g.daily_close), 0) AS relative_diff
            FROM minute_groups_control g
            CROSS JOIN import_bounds b
            WHERE g.daily_close IS NOT NULL
              AND abs(g.minute_close - g.daily_close) / nullif(abs(g.daily_close), 0) > ?
        """, [self.config.severe_close_tolerance])
        self.db.execute("""
            CREATE TEMP VIEW minute_groups_export AS
            SELECT g.*
            FROM minute_groups_control g
            LEFT JOIN excluded_minute_days x USING (symbol, date)
            WHERE x.symbol IS NULL
        """)
        self.db.execute("""
            CREATE TEMP VIEW minute_export AS
            SELECT m.*
            FROM minute_base m
            LEFT JOIN excluded_minute_days x USING (symbol, date)
            WHERE x.symbol IS NULL
        """)
        self.db.execute("""
            CREATE TEMP VIEW daily_missing AS
            SELECT
                d.symbol, d.date, d.open, d.high, d.low, d.close, d.volume, d.amount
            FROM normalized_source_daily d
            JOIN current_instruments i USING (symbol)
            LEFT JOIN current_daily c USING (symbol, date)
            CROSS JOIN import_bounds b
            WHERE d.date BETWEEN b.start_date AND b.end_date
              AND c.symbol IS NULL
        """)

    def audit(self) -> dict[str, Any]:
        self._register_inputs()
        mismatch = self.db.execute("""
            SELECT count(*)
            FROM source_minute m
            JOIN source_file_map f ON m.filename = f.filename
            WHERE m.ts_code <> f.expected_symbol
        """).fetchone()[0]
        group_summary = self.db.execute("""
            SELECT
                coalesce(sum(bars), 0),
                count(*),
                count(DISTINCT symbol),
                coalesce(sum(zero_volume_rows), 0),
                count(*) FILTER (WHERE bars <> 241 OR unique_bars <> bars),
                coalesce(sum(invalid_value_rows), 0),
                coalesce(sum(invalid_session_rows), 0),
                coalesce(sum(unresolved_factor_rows), 0),
                count(*) FILTER (WHERE daily_close IS NULL)
            FROM minute_groups_control
        """).fetchone()
        excluded = self.db.execute(
            "SELECT count(*), coalesce(sum(g.bars), 0) "
            "FROM excluded_minute_days x JOIN minute_groups g USING (symbol, date)"
        ).fetchone()
        exported = self.db.execute(
            "SELECT coalesce(sum(bars), 0), count(*), count(DISTINCT symbol), "
            "min(date), max(date) FROM minute_groups_export"
        ).fetchone()
        daily_missing = self.db.execute(
            "SELECT count(*), count(DISTINCT symbol), min(date), max(date) FROM daily_missing"
        ).fetchone()
        invalid_adjustments = self.db.execute("""
            SELECT count(*) FROM current_adjustments a
            JOIN latest_external_factor l USING (symbol)
            CROSS JOIN import_bounds b
            WHERE a.trade_date > l.latest_factor_date
              AND a.trade_date <= b.end_date
              AND (a.ex_factor IS NULL OR NOT isfinite(a.ex_factor) OR a.ex_factor <= 0)
        """).fetchone()[0]
        overlap_conflicts = self.db.execute("""
            SELECT count(*)
            FROM normalized_source_daily d
            JOIN current_daily c USING (symbol, date)
            JOIN current_instruments i USING (symbol)
            CROSS JOIN import_bounds b
            WHERE d.date BETWEEN b.start_date AND b.end_date
              AND (
                abs(d.open-c.open) > 1e-8 OR abs(d.high-c.high) > 1e-8
                OR abs(d.low-c.low) > 1e-8 OR abs(d.close-c.close) > 1e-8
                OR abs(d.volume-c.volume) > 1e-6 OR abs(d.amount-c.amount) > 0.1
              )
        """).fetchone()[0]
        existing_dates = self._existing_minute_target_dates()
        blockers: list[str] = []
        if mismatch:
            blockers.append(f"{mismatch} minute rows do not match their file symbol")
        if group_summary[4]:
            blockers.append(f"{group_summary[4]} minute symbol-days are not unique 241-bar grids")
        if group_summary[5]:
            blockers.append(f"{group_summary[5]} minute rows have invalid values")
        if group_summary[6]:
            blockers.append(f"{group_summary[6]} minute rows are outside the A-share session")
        if group_summary[7]:
            blockers.append(f"{group_summary[7]} minute rows have unresolved adjustment factors")
        if invalid_adjustments:
            blockers.append(f"{invalid_adjustments} future adjustment events are invalid")
        if existing_dates:
            blockers.append(
                "minute target partitions already exist in import range: "
                + ", ".join(existing_dates[:5])
            )
        exclusions = self.db.execute("""
            SELECT symbol, date, relative_diff
            FROM excluded_minute_days
            ORDER BY relative_diff DESC, symbol, date
            LIMIT 20
        """).fetchall()
        report = {
            "status": "blocked" if blockers else "ready",
            "run_id": self.config.run_id,
            "scope": {
                "asset_type": "etf",
                "symbols": "current_instruments_only",
                "start": self.config.start,
                "requested_end": self.config.end,
                "minute_end_exclusive": self.minute_end_exclusive,
                "existing_rows_win": True,
            },
            "source": {
                "canonical_minute_files": len(self.minute_paths),
                "ignored_duplicate_files": self.ignored_duplicate_files,
            },
            "minute": {
                "source_rows": int(group_summary[0]),
                "source_symbol_days": int(group_summary[1]),
                "source_symbols": int(group_summary[2]),
                "zero_volume_rows": int(group_summary[3]),
                "unverified_daily_control_symbol_days": int(group_summary[8]),
                "quarantined_symbol_days": int(excluded[0]),
                "quarantined_rows": int(excluded[1]),
                "publish_rows": int(exported[0]),
                "publish_symbol_days": int(exported[1]),
                "publish_symbols": int(exported[2]),
                "min_date": exported[3],
                "max_date": exported[4],
                "quarantine_sample": [
                    {"symbol": row[0], "date": row[1], "relative_close_diff": row[2]}
                    for row in exclusions
                ],
                "representation": "fixed_241_bar_grid",
                "zero_volume_tradeable": False,
            },
            "daily": {
                "missing_rows": int(daily_missing[0]),
                "missing_symbols": int(daily_missing[1]),
                "min_date": daily_missing[2],
                "max_date": daily_missing[3],
                "overlap_conflicts_preserved": int(overlap_conflicts),
            },
            "blockers": blockers,
        }
        return report

    def _existing_minute_target_dates(self) -> list[str]:
        assert self.minute_end_exclusive is not None
        result: list[str] = []
        root = self.config.data_dir / "kline_etf_minute"
        for path in root.glob("date=*"):
            try:
                value = date.fromisoformat(path.name.removeprefix("date="))
            except ValueError:
                continue
            if self.config.start <= value < self.minute_end_exclusive:
                result.append(value.isoformat())
        return sorted(result)

    def stage(self, audit: Mapping[str, Any]) -> dict[str, Any]:
        if audit.get("blockers"):
            raise StockDataEtfImportBlocked(str(audit["blockers"][0]))
        if self.run_root.exists():
            raise StockDataEtfImportBlocked(f"run directory already exists: {self.run_root}")
        assert_disk_reserve(self.config.data_dir)
        stage_root = self.run_root / "staging"
        stage_root.mkdir(parents=True)
        manifest_path = self.run_root / "manifest.json"
        _atomic_json(manifest_path, {"status": "audited", "audit": audit})
        self.progress("Staging daily gaps and ETF enriched data")
        daily_state = self._stage_daily(stage_root)
        enriched_state = self._stage_enriched(stage_root)
        self.progress("Staging minute partitions")
        minute_state = self._stage_minutes(stage_root)
        staged = {
            "daily": daily_state,
            "enriched": enriched_state,
            "minute": minute_state,
        }
        _atomic_json(manifest_path, {"status": "staged", "audit": audit, "staged": staged})
        return staged

    def _stage_daily(self, stage_root: Path) -> dict[str, Any]:
        incoming = self.db.sql(
            "SELECT symbol,date,open,high,low,close,volume,amount "
            "FROM daily_missing ORDER BY date,symbol"
        ).pl()
        targets: list[dict[str, Any]] = []
        for day_frame in incoming.partition_by("date", maintain_order=True):
            day = day_frame["date"][0]
            target = self.config.data_dir / "kline_etf_daily" / f"date={day}" / "part.parquet"
            existing = pl.read_parquet(target) if target.exists() else pl.DataFrame()
            merged = pl.concat([existing, day_frame], how="diagonal_relaxed").unique(
                subset=["symbol", "date"], keep="first"
            ).sort("symbol")
            staged = stage_root / "daily" / f"date={day}" / "part.parquet"
            _atomic_parquet(merged, staged)
            targets.append({
                "date": str(day),
                "target": str(target.relative_to(self.config.data_dir)),
                "fingerprint": _file_fingerprint(target),
            })
        return {"rows": incoming.height, "partitions": len(targets), "targets": targets}

    def _stage_enriched(self, stage_root: Path) -> dict[str, Any]:
        missing_rows = self.db.execute("SELECT count(*) FROM daily_missing").fetchone()[0]
        if not missing_rows:
            return {"rows": 0, "partitions": 0, "needed": False}
        daily_glob = self.config.data_dir / "kline_etf_daily" / "**" / "*.parquet"
        raw = scan_daily_parquet(str(daily_glob)).collect()
        incoming = self.db.sql(
            "SELECT symbol,date,open,high,low,close,volume,amount FROM daily_missing"
        ).pl()
        raw = pl.concat([raw, incoming], how="diagonal_relaxed").unique(
            subset=["symbol", "date"], keep="first"
        ).sort(["symbol", "date"])
        factor_path = self.config.data_dir / "adj_factor_etf" / "all.parquet"
        factors = pl.read_parquet(factor_path) if factor_path.exists() else None
        enriched = compute_enriched(raw, factors=factors, instruments=None)
        if enriched.height != raw.height or enriched.select("symbol", "date").n_unique() != raw.height:
            raise StockDataEtfImportBlocked("ETF enriched staging does not match daily keys")
        columns = [column for column in ENRICHED_STORAGE_COLS if column in enriched.columns]
        output = enriched.select(columns)
        target_root = self.config.data_dir / "kline_etf_enriched"
        staged_root = stage_root / "kline_etf_enriched"
        partitions = 0
        for day_frame in output.partition_by("date", maintain_order=True):
            day = day_frame["date"][0]
            _atomic_parquet(
                day_frame.sort("symbol"),
                staged_root / f"date={day}" / "part.parquet",
            )
            partitions += 1
        return {
            "rows": output.height,
            "partitions": partitions,
            "needed": True,
            "target_fingerprint": _tree_fingerprint(target_root),
        }

    def _stage_minutes(self, stage_root: Path) -> dict[str, Any]:
        years = [
            int(row[0])
            for row in self.db.execute(
                "SELECT DISTINCT year(date) FROM minute_groups_export ORDER BY 1"
            ).fetchall()
        ]
        result: dict[str, Any] = {"rows": 0, "partitions": 0, "years": {}}
        for year in years:
            assert_disk_reserve(self.config.data_dir)
            expected = self.db.execute(
                "SELECT coalesce(sum(bars), 0), count(*) "
                "FROM minute_groups_export WHERE year(date)=?",
                [year],
            ).fetchone()
            self.progress(f"Staging ETF minute year {year}: {int(expected[0])} rows")
            year_root = stage_root / "minute" / f"year={year}"
            year_root.mkdir(parents=True)
            self.db.execute(f"""
                COPY (
                    SELECT
                        symbol,
                        CAST(datetime AS TIMESTAMP) AS datetime,
                        CAST(open * bar_factor / latest_factor AS DOUBLE) AS open,
                        CAST(high * bar_factor / latest_factor AS DOUBLE) AS high,
                        CAST(low * bar_factor / latest_factor AS DOUBLE) AS low,
                        CAST(close * bar_factor / latest_factor AS DOUBLE) AS close,
                        CAST(source_volume / 100.0 AS DOUBLE) AS volume,
                        CAST(amount AS DOUBLE) AS amount,
                        date
                    FROM minute_export
                    WHERE year(date) = ?
                    ORDER BY date, symbol, datetime
                ) TO {_sql_string(year_root)} (
                    FORMAT PARQUET,
                    PARTITION_BY (date),
                    COMPRESSION ZSTD,
                    COMPRESSION_LEVEL 3,
                    PER_THREAD_OUTPUT false,
                    FILENAME_PATTERN 'part'
                )
            """, [year])
            date_dirs = sorted(year_root.glob("date=*"))
            for directory in date_dirs:
                _collapse_staged_partition(directory)
            rows = sum(pq.ParquetFile(path).metadata.num_rows for path in year_root.rglob("*.parquet"))
            if rows != int(expected[0]) or len(date_dirs) == 0:
                raise StockDataEtfImportBlocked(
                    f"staged minute year {year} mismatch: expected {expected[0]}, got {rows}"
                )
            self._stage_coverage(stage_root, year)
            result["rows"] += rows
            result["partitions"] += len(date_dirs)
            result["years"][str(year)] = {"rows": rows, "partitions": len(date_dirs)}
        return result

    def _stage_coverage(self, stage_root: Path, year: int) -> None:
        groups = self.db.execute("""
            SELECT symbol,date,bars,zero_volume_rows,daily_close IS NOT NULL AS daily_control
            FROM minute_groups_export
            WHERE year(date)=?
            ORDER BY date,symbol
        """, [year]).pl()
        for day_frame in groups.partition_by("date", maintain_order=True):
            day = day_frame["date"][0]
            records = day_frame.select(
                "symbol", "bars", "zero_volume_rows", "daily_control"
            ).to_dicts()
            payload = {
                "schema_version": 1,
                "expected_continuous_bars": 240,
                "optional_auction_bar": "09:30",
                "symbols": len(records),
                "complete_symbols": len(records),
                "incomplete_symbols": 0,
                "source": "stockdata",
                "ownership": "existing_tickflow_rows_win",
                "representation": "fixed_clock_grid",
                "zero_volume_rows": sum(int(row["zero_volume_rows"]) for row in records),
                "unverified_daily_control_symbols": sum(
                    not bool(row["daily_control"]) for row in records
                ),
                "groups": [
                    {"symbol": row["symbol"], "bars": int(row["bars"]), "complete": True}
                    for row in records
                ],
            }
            _atomic_json(
                stage_root / "coverage" / f"year={year}" / f"date={day}.json",
                payload,
            )

    def publish(self, audit: Mapping[str, Any], staged: Mapping[str, Any]) -> dict[str, Any]:
        assert_disk_reserve(self.config.data_dir)
        stage_root = self.run_root / "staging"
        backup_root = self.run_root / "backups"
        backup_root.mkdir(parents=True)
        new_targets: list[tuple[Path, Path]] = []
        replaced_daily: list[tuple[Path, Path, Path]] = []
        enriched_state: tuple[Path, Path, Path | None] | None = None
        try:
            for year_root in sorted((stage_root / "minute").glob("year=*")):
                for source in sorted(year_root.glob("date=*")):
                    target = self.config.data_dir / "kline_etf_minute" / source.name
                    if target.exists():
                        raise StockDataEtfImportBlocked(f"minute target appeared during publish: {target}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(source, target)
                    new_targets.append((target, source))
            for year_root in sorted((stage_root / "coverage").glob("year=*")):
                for source in sorted(year_root.glob("date=*.json")):
                    target = self.config.data_dir / "kline_etf_minute" / "_coverage" / source.name
                    if target.exists():
                        raise StockDataEtfImportBlocked(f"coverage target appeared during publish: {target}")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(source, target)
                    new_targets.append((target, source))

            for target_state in staged["daily"]["targets"]:
                target = self.config.data_dir / str(target_state["target"])
                expected = tuple(target_state["fingerprint"]) if target_state["fingerprint"] else None
                if _file_fingerprint(target) != expected:
                    raise StockDataEtfImportBlocked(f"daily target changed during staging: {target}")
                source = stage_root / "daily" / f"date={target_state['date']}" / "part.parquet"
                backup = backup_root / "daily" / f"date={target_state['date']}" / "part.parquet"
                backup.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    os.link(target, backup)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, target)
                replaced_daily.append((target, source, backup))

            if staged["enriched"].get("needed"):
                target = self.config.data_dir / "kline_etf_enriched"
                source = stage_root / "kline_etf_enriched"
                backup = backup_root / "kline_etf_enriched"
                if _tree_fingerprint(target) != staged["enriched"].get("target_fingerprint"):
                    raise StockDataEtfImportBlocked("ETF enriched target changed during staging")
                active_backup: Path | None = None
                if target.exists():
                    os.replace(target, backup)
                    active_backup = backup
                try:
                    os.replace(source, target)
                except Exception:
                    if active_backup is not None:
                        os.replace(active_backup, target)
                    raise
                enriched_state = (target, source, active_backup)
        except Exception:
            if enriched_state is not None:
                target, source, backup = enriched_state
                os.replace(target, source)
                if backup is not None:
                    os.replace(backup, target)
            for target, source, backup in reversed(replaced_daily):
                if target.exists():
                    source.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(target, source)
                if backup.exists():
                    os.replace(backup, target)
            for target, source in reversed(new_targets):
                if target.exists():
                    source.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(target, source)
            raise

        generation = uuid4().hex
        _atomic_json(
            self.config.data_dir / ".matrix_generation_etf.json",
            {"generation": generation, "updated_at_ns": time.time_ns()},
        )
        result = {
            "status": "published",
            "run_id": self.config.run_id,
            "daily_rows": int(staged["daily"]["rows"]),
            "minute_rows": int(staged["minute"]["rows"]),
            "minute_partitions": int(staged["minute"]["partitions"]),
            "enriched_rows": int(staged["enriched"]["rows"]),
            "matrix_generation": generation,
            "rollback_dir": str(backup_root),
        }
        _atomic_json(
            self.run_root / "manifest.json",
            {"status": "published", "audit": audit, "staged": staged, "publish": result},
        )
        return result


def run_stockdata_etf_import(
    config: StockDataEtfImportConfig,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    importer = StockDataEtfImporter(config, progress=progress)
    try:
        audit = importer.audit()
        if not importer.config.publish:
            return {"audit": audit, "publish": {"status": "dry_run"}}
        staged = importer.stage(audit)
        return {"audit": audit, "staged": staged, "publish": importer.publish(audit, staged)}
    finally:
        importer.close()
