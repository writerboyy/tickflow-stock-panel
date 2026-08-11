"""Audit and publish StockData day-partitioned stock minute history."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
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

from app.services.stockdata_etf_import import (
    _atomic_json,
    _atomic_parquet,
    _file_fingerprint,
    _parquet_source,
    _sql_string,
)
from app.services.tushare_history import assert_disk_reserve


class StockDataStockImportBlocked(RuntimeError):
    """A source or publication safety check failed."""


@dataclass(frozen=True)
class StockDataStockImportConfig:
    source_dir: Path
    data_dir: Path
    start: date = date(2019, 1, 1)
    end: date = date(2025, 12, 31)
    publish: bool = False
    run_id: str | None = None
    severe_close_tolerance: float = 0.005

    def normalized(self) -> StockDataStockImportConfig:
        source_dir = self.source_dir.expanduser().resolve()
        data_dir = self.data_dir.expanduser().resolve()
        if self.start > self.end:
            raise ValueError("start must not be later than end")
        if self.severe_close_tolerance <= 0:
            raise ValueError("severe_close_tolerance must be positive")
        for path in (
            source_dir,
            data_dir / "instruments" / "instruments.parquet",
            data_dir / "instruments_etf" / "instruments_etf.parquet",
            data_dir / "instruments_index" / "instruments_index.parquet",
            data_dir / "adj_factor" / "all.parquet",
            data_dir / "kline_daily",
            data_dir / "kline_daily_enriched",
        ):
            if not path.exists():
                raise ValueError(f"required path does not exist: {path}")
        for year in range(self.start.year, self.end.year + 1):
            if not (source_dir / str(year)).is_dir():
                raise ValueError(f"source year directory does not exist: {source_dir / str(year)}")
        run_id = self.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid4().hex[:8]
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", run_id):
            raise ValueError("run_id contains unsafe characters")
        return StockDataStockImportConfig(
            source_dir=source_dir,
            data_dir=data_dir,
            start=self.start,
            end=self.end,
            publish=self.publish,
            run_id=run_id,
            severe_close_tolerance=self.severe_close_tolerance,
        )


def _source_date(path: Path) -> date | None:
    if not re.fullmatch(r"\d{8}", path.stem):
        return None
    try:
        return datetime.strptime(path.stem, "%Y%m%d").date()
    except ValueError:
        return None


def _paths_for_dates(root: Path, start: date, end: date) -> list[Path]:
    result: list[Path] = []
    for path in sorted(root.glob("*.parquet")):
        value = _source_date(path)
        if value is not None and start <= value <= end:
            result.append(path)
    return result


def _partition_paths(root: Path, start: date, end: date) -> list[Path]:
    result: list[Path] = []
    for path in sorted(root.glob("date=*/part.parquet")):
        try:
            value = date.fromisoformat(path.parent.name.removeprefix("date="))
        except ValueError:
            continue
        if start <= value <= end:
            result.append(path)
    return result


_CONTINUOUS_SQL = """(
    (hour(datetime) = 9 AND minute(datetime) >= 31)
    OR hour(datetime) = 10
    OR (hour(datetime) = 11 AND minute(datetime) <= 30)
    OR (hour(datetime) = 13 AND minute(datetime) >= 1)
    OR hour(datetime) = 14
    OR (hour(datetime) = 15 AND minute(datetime) = 0)
)"""
_AUCTION_SQL = "(hour(datetime) = 9 AND minute(datetime) = 30)"
_SESSION_SQL = f"({_CONTINUOUS_SQL} OR {_AUCTION_SQL})"
_INVALID_VALUE_SQL = """(
    open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
    OR NOT isfinite(open) OR NOT isfinite(high)
    OR NOT isfinite(low) OR NOT isfinite(close)
    OR open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
    OR high < greatest(open, close) OR low > least(open, close)
    OR volume IS NULL OR amount IS NULL
    OR NOT isfinite(volume) OR NOT isfinite(amount)
    OR volume < 0 OR amount < 0
)"""


class _StockDataStockYearImporter:
    def __init__(
        self,
        config: StockDataStockImportConfig,
        year: int,
        run_root: Path,
        progress: Callable[[str], None],
    ) -> None:
        self.config = config
        self.year = year
        self.start = max(config.start, date(year, 1, 1))
        self.end = min(config.end, date(year, 12, 31))
        self.run_root = run_root / f"year={year}"
        self.progress = progress
        self.source_paths = _paths_for_dates(config.source_dir / str(year), self.start, self.end)
        self.current_paths = _partition_paths(config.data_dir / "kline_minute", self.start, self.end)
        self._temporary = tempfile.TemporaryDirectory(prefix=f"tickflow-stockdata-stock-{year}-")
        self.db = duckdb.connect(":memory:")
        self.db.execute("PRAGMA threads=2")
        self.db.execute("SET preserve_insertion_order=false")
        self.db.execute("SET memory_limit='12GB'")
        self.db.execute("SET temp_directory = ?", [self._temporary.name])
        self.target_states: dict[str, dict[str, tuple[int, int] | None]] = {}
        self._ready = False

    def close(self) -> None:
        self.db.close()
        self._temporary.cleanup()

    def _register_inputs(self) -> None:
        if self._ready:
            return
        if not self.source_paths:
            raise StockDataStockImportBlocked(f"no source parquet files for {self.start} through {self.end}")
        data_dir = self.config.data_dir
        self.db.execute(
            f"CREATE TEMP VIEW current_instruments AS SELECT DISTINCT symbol "
            f"FROM read_parquet({_sql_string(data_dir / 'instruments' / 'instruments.parquet')})"
        )
        self.db.execute(
            f"CREATE TEMP VIEW current_etfs AS SELECT DISTINCT symbol "
            f"FROM read_parquet({_sql_string(data_dir / 'instruments_etf' / 'instruments_etf.parquet')})"
        )
        self.db.execute(
            f"CREATE TEMP VIEW current_indices AS SELECT DISTINCT symbol "
            f"FROM read_parquet({_sql_string(data_dir / 'instruments_index' / 'instruments_index.parquet')})"
        )
        factor_path = data_dir / "adj_factor" / "all.parquet"
        self.db.execute(
            f"CREATE TEMP VIEW raw_factors AS SELECT symbol, CAST(trade_date AS DATE) trade_date, "
            f"CAST(ex_factor AS DOUBLE) ex_factor FROM read_parquet({_sql_string(factor_path)})"
        )
        self.db.execute("""
            CREATE TEMP VIEW invalid_factor_symbols AS
            SELECT DISTINCT symbol
            FROM raw_factors
            WHERE symbol IS NULL OR trade_date IS NULL OR ex_factor IS NULL
               OR NOT isfinite(ex_factor) OR ex_factor <= 0
        """)
        self.db.execute("""
            CREATE TEMP TABLE normalized_factors AS
            SELECT symbol, trade_date, arg_max(ex_factor, trade_date) AS ex_factor
            FROM raw_factors
            WHERE ex_factor IS NOT NULL AND isfinite(ex_factor) AND ex_factor > 0
            GROUP BY symbol, trade_date
        """)
        self.db.execute("""
            CREATE TEMP VIEW factor_events AS
            SELECT symbol, trade_date,
                   exp(sum(ln(ex_factor)) OVER (
                       PARTITION BY symbol ORDER BY trade_date
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   )) AS cum_factor
            FROM normalized_factors
        """)
        self.db.execute("""
            CREATE TEMP VIEW factor_totals AS
            SELECT symbol, arg_max(cum_factor, trade_date) AS total_factor
            FROM factor_events
            GROUP BY symbol
        """)

        daily_paths = _partition_paths(data_dir / "kline_daily", self.start, self.end)
        enriched_paths = _partition_paths(data_dir / "kline_daily_enriched", self.start, self.end)
        if not daily_paths or not enriched_paths:
            raise StockDataStockImportBlocked(f"daily controls are missing for {self.year}")
        self.db.execute(
            f"CREATE TEMP VIEW current_daily AS SELECT symbol, CAST(date AS DATE) AS date, "
            f"CAST(close AS DOUBLE) AS close FROM {_parquet_source(daily_paths)}"
        )
        self.db.execute(
            f"CREATE TEMP VIEW current_enriched AS SELECT symbol, CAST(date AS DATE) AS date, "
            f"CAST(close AS DOUBLE) AS close FROM {_parquet_source(enriched_paths)}"
        )
        if self.current_paths:
            self.db.execute(
                f"CREATE TEMP VIEW current_minute AS SELECT symbol, CAST(datetime AS TIMESTAMP) AS datetime, "
                f"CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, CAST(low AS DOUBLE) AS low, "
                f"CAST(close AS DOUBLE) AS close, CAST(volume AS DOUBLE) AS volume, "
                f"CAST(amount AS DOUBLE) AS amount FROM {_parquet_source(self.current_paths)}"
            )
        else:
            self.db.execute("""
                CREATE TEMP TABLE current_minute (
                    symbol VARCHAR, datetime TIMESTAMP, open DOUBLE, high DOUBLE,
                    low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE
                )
            """)
        self.db.execute("""
            CREATE TEMP VIEW current_minute_unique AS
            SELECT symbol, datetime,
                   arg_max(open, rowid) AS open, arg_max(high, rowid) AS high,
                   arg_max(low, rowid) AS low, arg_max(close, rowid) AS close,
                   arg_max(volume, rowid) AS volume, arg_max(amount, rowid) AS amount
            FROM (SELECT *, row_number() OVER () rowid FROM current_minute)
            GROUP BY symbol, datetime
        """)
        self.db.execute(f"""
            CREATE TEMP VIEW source_base AS
            SELECT
                CAST(code AS VARCHAR) AS symbol,
                try_cast(trade_time AS TIMESTAMP) AS datetime,
                CAST(try_cast(trade_time AS TIMESTAMP) AS DATE) AS date,
                try_strptime(CAST(date AS VARCHAR), '%Y%m%d')::DATE AS source_date,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(vol AS DOUBLE) AS volume,
                CAST(amount AS DOUBLE) AS amount
            FROM {_parquet_source(self.source_paths)}
        """)
        self._create_groups()
        self._ready = True

    def _create_groups(self) -> None:
        self.progress(f"Auditing stock minute source groups for {self.year}")
        self.db.execute("""
            CREATE TEMP TABLE source_duplicate_groups (
                symbol VARCHAR, date DATE, duplicate_rows BIGINT,
                continuous_duplicate_rows BIGINT, auction_duplicate_rows BIGINT
            )
        """)
        for path in self.source_paths:
            self.db.execute(f"""
                INSERT INTO source_duplicate_groups
                SELECT symbol, date,
                       sum(rows - 1)::BIGINT AS duplicate_rows,
                       sum(CASE WHEN {_CONTINUOUS_SQL} THEN rows - 1 ELSE 0 END)::BIGINT
                           AS continuous_duplicate_rows,
                       sum(CASE WHEN {_AUCTION_SQL} THEN rows - 1 ELSE 0 END)::BIGINT
                           AS auction_duplicate_rows
                FROM (
                    SELECT CAST(code AS VARCHAR) AS symbol,
                           CAST(try_cast(trade_time AS TIMESTAMP) AS DATE) AS date,
                           try_cast(trade_time AS TIMESTAMP) AS datetime,
                           count(*)::BIGINT AS rows
                    FROM read_parquet({_sql_string(path)})
                    GROUP BY code, trade_time
                    HAVING count(*) > 1
                ) duplicates
                GROUP BY symbol, date
            """)
        self.db.execute(f"""
            CREATE TEMP TABLE source_groups_raw AS
            SELECT
                source.symbol,
                source.date,
                count(*)::BIGINT bars,
                count(*) FILTER (WHERE {_CONTINUOUS_SQL})::BIGINT continuous_rows,
                count(*) FILTER (WHERE {_AUCTION_SQL})::BIGINT auction_rows,
                count(*) FILTER (WHERE NOT {_SESSION_SQL})::BIGINT invalid_session_rows,
                count(*) FILTER (WHERE {_SESSION_SQL} AND {_INVALID_VALUE_SQL})::BIGINT invalid_value_rows,
                count(*) FILTER (WHERE datetime IS NULL)::BIGINT invalid_timestamp_rows,
                count(*) FILTER (WHERE source.source_date IS NULL OR source.date IS NULL
                    OR source.source_date <> source.date)::BIGINT
                    source_date_mismatch_rows,
                count(*) FILTER (WHERE {_SESSION_SQL} AND volume = 0)::BIGINT zero_volume_rows,
                arg_max(source.close, source.datetime) FILTER (WHERE {_SESSION_SQL}) AS minute_close,
                coalesce(max(dupes.duplicate_rows), 0)::BIGINT AS duplicate_rows,
                coalesce(max(dupes.continuous_duplicate_rows), 0)::BIGINT AS continuous_duplicate_rows,
                coalesce(max(dupes.auction_duplicate_rows), 0)::BIGINT AS auction_duplicate_rows
            FROM source_base source
            LEFT JOIN source_duplicate_groups dupes
              ON source.symbol = dupes.symbol AND source.date = dupes.date
            GROUP BY source.symbol, source.date
        """)
        self.db.execute("""
            ALTER TABLE source_groups_raw ADD COLUMN unique_bars BIGINT;
            ALTER TABLE source_groups_raw ADD COLUMN continuous_unique BIGINT;
            ALTER TABLE source_groups_raw ADD COLUMN auction_unique BIGINT;
            UPDATE source_groups_raw
            SET unique_bars = bars - duplicate_rows,
                continuous_unique = continuous_rows - continuous_duplicate_rows,
                auction_unique = auction_rows - auction_duplicate_rows
        """)
        self.db.execute("""
            CREATE TEMP TABLE source_groups_control AS
            SELECT
                g.*,
                (
                    i.symbol IS NOT NULL
                    OR (
                        (d.symbol IS NOT NULL OR n.symbol IS NOT NULL)
                        AND e.symbol IS NULL AND x.symbol IS NULL
                    )
                ) AS is_stock,
                e.symbol IS NOT NULL AS is_etf,
                x.symbol IS NOT NULL AS is_index,
                d.close AS daily_close,
                n.close AS enriched_close,
                coalesce(f.cum_factor, 1.0) / coalesce(t.total_factor, 1.0) AS adjustment_ratio
            FROM source_groups_raw g
            ASOF LEFT JOIN factor_events f
              ON g.symbol = f.symbol AND g.date >= f.trade_date
            LEFT JOIN factor_totals t ON g.symbol = t.symbol
            LEFT JOIN current_instruments i ON g.symbol = i.symbol
            LEFT JOIN current_etfs e ON g.symbol = e.symbol
            LEFT JOIN current_indices x ON g.symbol = x.symbol
            LEFT JOIN current_daily d ON g.symbol = d.symbol AND g.date = d.date
            LEFT JOIN current_enriched n ON g.symbol = n.symbol AND g.date = n.date
        """)
        tolerance = self.config.severe_close_tolerance
        self.db.execute(f"""
            CREATE TEMP TABLE publish_groups AS
            SELECT source_group.*
            FROM source_groups_control AS source_group
            WHERE is_stock
              AND date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND invalid_timestamp_rows = 0
              AND source_date_mismatch_rows = 0
              AND continuous_rows = 240 AND continuous_unique = 240
              AND auction_rows = auction_unique AND auction_rows BETWEEN 0 AND 1
              AND invalid_value_rows = 0
              AND NOT EXISTS (
                  SELECT 1 FROM invalid_factor_symbols f
                  WHERE f.symbol = source_group.symbol
              )
              AND (
                  daily_close IS NULL
                  OR abs(minute_close - daily_close) / nullif(abs(daily_close), 0) <= {tolerance}
              )
              AND (
                  enriched_close IS NULL
                  OR abs(minute_close * adjustment_ratio - enriched_close)
                     / nullif(abs(enriched_close), 0) <= {tolerance}
              )
        """, [self.start, self.end])

    def _current_quality(self) -> tuple[int, int, int]:
        if not self.current_paths:
            return 0, 0, 0
        row = self.db.execute(f"""
            SELECT
                count(*) - count(DISTINCT (symbol, datetime)),
                count(*) FILTER (WHERE {_INVALID_VALUE_SQL}),
                count(*) FILTER (WHERE NOT {_SESSION_SQL})
            FROM current_minute
        """).fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    def _build_publish_dates(self) -> dict[str, int]:
        if not self.current_paths:
            self.db.execute("""
                CREATE TEMP TABLE publish_dates AS
                SELECT date, sum(continuous_rows + auction_rows)::BIGINT missing_rows,
                       0::BIGINT overlap_rows, 0::BIGINT price_conflicts,
                       0::BIGINT volume_conflicts, 0::BIGINT amount_conflicts
                FROM publish_groups
                GROUP BY date
            """)
        else:
            self.progress(f"Comparing {self.year} source keys with current stock minute rows")
            self.db.execute(f"""
                CREATE TEMP TABLE publish_dates_all AS
                SELECT
                    s.date,
                    count(*) FILTER (WHERE c.symbol IS NULL)::BIGINT missing_rows,
                    count(*) FILTER (WHERE c.symbol IS NOT NULL)::BIGINT overlap_rows,
                    count(*) FILTER (WHERE c.symbol IS NOT NULL AND (
                        abs(c.open - s.open * g.adjustment_ratio) > 0.005
                        OR abs(c.high - s.high * g.adjustment_ratio) > 0.005
                        OR abs(c.low - s.low * g.adjustment_ratio) > 0.005
                        OR abs(c.close - s.close * g.adjustment_ratio) > 0.005
                    ))::BIGINT price_conflicts,
                    count(*) FILTER (WHERE c.symbol IS NOT NULL
                        AND abs(c.volume - s.volume / 100.0) > 0.01)::BIGINT volume_conflicts,
                    count(*) FILTER (WHERE c.symbol IS NOT NULL
                        AND abs(c.amount - s.amount) > 0.1)::BIGINT amount_conflicts
                FROM source_base s
                JOIN publish_groups g USING (symbol, date)
                LEFT JOIN current_minute_unique c USING (symbol, datetime)
                WHERE {_SESSION_SQL}
                GROUP BY s.date
            """)
            self.db.execute("""
                CREATE TEMP TABLE publish_dates AS
                SELECT * FROM publish_dates_all WHERE missing_rows > 0
            """)
        row = self.db.execute("""
            SELECT
                coalesce(sum(missing_rows), 0),
                coalesce(sum(overlap_rows), 0),
                coalesce(sum(price_conflicts), 0),
                coalesce(sum(volume_conflicts), 0),
                coalesce(sum(amount_conflicts), 0),
                count(*)
            FROM publish_dates
        """).fetchone()
        dates = [value[0] for value in self.db.execute("SELECT date FROM publish_dates ORDER BY date").fetchall()]
        self.target_states = {}
        for day in dates:
            key = day.isoformat()
            self.target_states[key] = {
                "minute": _file_fingerprint(
                    self.config.data_dir / "kline_minute" / f"date={key}" / "part.parquet"
                ),
                "coverage": _file_fingerprint(
                    self.config.data_dir / "kline_minute" / "_coverage" / f"date={key}.json"
                ),
            }
        existing_stage_rows = self.db.execute("""
            SELECT count(*)
            FROM current_minute c
            JOIN publish_dates d ON CAST(c.datetime AS DATE) = d.date
        """).fetchone()[0]
        return {
            "missing_rows": int(row[0]),
            "overlap_rows": int(row[1]),
            "price_conflicts": int(row[2]),
            "volume_conflicts": int(row[3]),
            "amount_conflicts": int(row[4]),
            "partitions": int(row[5]),
            "existing_stage_rows": int(existing_stage_rows),
        }

    def audit(self) -> dict[str, Any]:
        self._register_inputs()
        invalid_factors = self.db.execute("""
            SELECT count(*) FROM raw_factors
            WHERE symbol IS NULL OR trade_date IS NULL OR ex_factor IS NULL
               OR NOT isfinite(ex_factor) OR ex_factor <= 0
        """).fetchone()[0]
        invalid_factor_symbols = self.db.execute(
            "SELECT count(*) FROM invalid_factor_symbols WHERE symbol IS NOT NULL"
        ).fetchone()[0]
        duplicate_factors = self.db.execute("""
            SELECT count(*) FROM (
                SELECT symbol, trade_date FROM raw_factors
                GROUP BY symbol, trade_date HAVING count(*) > 1
            )
        """).fetchone()[0]
        source = self.db.execute("""
            SELECT
                coalesce(sum(bars), 0), count(*), count(DISTINCT symbol),
                count(DISTINCT symbol) FILTER (WHERE is_stock),
                count(DISTINCT symbol) FILTER (WHERE is_etf),
                count(DISTINCT symbol) FILTER (WHERE is_index),
                count(DISTINCT symbol) FILTER (WHERE NOT is_stock AND NOT is_etf AND NOT is_index),
                coalesce(sum(invalid_session_rows), 0)
            FROM source_groups_control
        """).fetchone()
        eligible = self.db.execute("""
            SELECT
                coalesce(sum(continuous_rows + auction_rows), 0),
                count(*), count(DISTINCT symbol),
                coalesce(sum(zero_volume_rows), 0),
                count(*) FILTER (WHERE daily_close IS NULL),
                count(*) FILTER (WHERE enriched_close IS NULL)
            FROM publish_groups
        """).fetchone()
        quarantined = self.db.execute("""
            SELECT
                count(*),
                coalesce(sum(g.continuous_rows + g.auction_rows), 0),
                count(*) FILTER (WHERE g.continuous_rows <> 240 OR g.continuous_unique <> 240
                    OR g.auction_rows <> g.auction_unique OR g.auction_rows NOT BETWEEN 0 AND 1),
                count(*) FILTER (WHERE g.invalid_value_rows > 0),
                count(*) FILTER (WHERE g.source_date_mismatch_rows > 0 OR g.invalid_timestamp_rows > 0),
                count(*) FILTER (WHERE g.daily_close IS NOT NULL
                    AND abs(g.minute_close-g.daily_close)/nullif(abs(g.daily_close), 0) > ?),
                count(*) FILTER (WHERE g.enriched_close IS NOT NULL
                    AND abs(g.minute_close*g.adjustment_ratio-g.enriched_close)
                        /nullif(abs(g.enriched_close), 0) > ?),
                count(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM invalid_factor_symbols f WHERE f.symbol = g.symbol
                ))
            FROM source_groups_control g
            LEFT JOIN publish_groups p USING (symbol, date)
            WHERE g.is_stock AND g.date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND p.symbol IS NULL
        """, [
            self.config.severe_close_tolerance,
            self.config.severe_close_tolerance,
            self.start,
            self.end,
        ]).fetchone()
        publish = self._build_publish_dates()
        current_duplicates, current_invalid, current_session = self._current_quality()
        blockers: list[str] = []
        if duplicate_factors:
            blockers.append(f"{duplicate_factors} current adjustment factor keys are duplicated")
        if current_duplicates:
            blockers.append(f"{current_duplicates} current minute keys are duplicated")
        if current_invalid:
            blockers.append(f"{current_invalid} current minute rows have invalid values")
        if current_session:
            blockers.append(f"{current_session} current minute rows are outside the canonical session")
        sample = self.db.execute("""
            SELECT g.symbol, g.date, g.continuous_rows, g.auction_rows,
                   g.invalid_value_rows, g.invalid_session_rows,
                   CASE WHEN g.daily_close IS NULL THEN NULL
                        ELSE abs(g.minute_close-g.daily_close)/nullif(abs(g.daily_close), 0) END
            FROM source_groups_control g
            LEFT JOIN publish_groups p USING (symbol, date)
            WHERE g.is_stock AND g.date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND p.symbol IS NULL
            ORDER BY g.date, g.symbol
            LIMIT 20
        """, [self.start, self.end]).fetchall()
        report = {
            "status": "blocked" if blockers else "ready",
            "year": self.year,
            "scope": {
                "asset_type": "stock",
                "start": self.start,
                "end": self.end,
                "existing_rows_win": True,
            },
            "source": {
                "files": len(self.source_paths),
                "bytes": sum(path.stat().st_size for path in self.source_paths),
                "rows": int(source[0]),
                "symbol_days": int(source[1]),
                "symbols": int(source[2]),
                "stock_symbols": int(source[3]),
                "etf_symbols": int(source[4]),
                "index_symbols": int(source[5]),
                "unmatched_symbols": int(source[6]),
            },
            "minute": {
                "eligible_rows": int(eligible[0]),
                "eligible_symbol_days": int(eligible[1]),
                "eligible_symbols": int(eligible[2]),
                "zero_volume_rows": int(eligible[3]),
                "unverified_daily_symbol_days": int(eligible[4]),
                "unverified_enriched_symbol_days": int(eligible[5]),
                "rejected_out_of_session_rows": int(source[7]),
                "quarantined_symbol_days": int(quarantined[0]),
                "quarantined_rows": int(quarantined[1]),
                "quarantine_reasons": {
                    "incomplete_clock": int(quarantined[2]),
                    "invalid_values": int(quarantined[3]),
                    "invalid_date": int(quarantined[4]),
                    "daily_close_mismatch": int(quarantined[5]),
                    "adjusted_close_mismatch": int(quarantined[6]),
                    "invalid_adjustment_factor": int(quarantined[7]),
                },
                "publish_rows": publish["missing_rows"],
                "publish_partitions": publish["partitions"],
                "overlap_rows_preserved": publish["overlap_rows"],
                "overlap_conflicts_preserved": {
                    "price": publish["price_conflicts"],
                    "volume": publish["volume_conflicts"],
                    "amount": publish["amount_conflicts"],
                },
                "staged_final_rows": publish["existing_stage_rows"] + publish["missing_rows"],
                "representation": "fixed_clock_grid",
                "zero_volume_tradeable": False,
                "quarantine_sample": [
                    {
                        "symbol": row[0], "date": row[1], "continuous_rows": int(row[2]),
                        "auction_rows": int(row[3]), "invalid_value_rows": int(row[4]),
                        "invalid_session_rows": int(row[5]), "daily_close_relative_diff": row[6],
                    }
                    for row in sample
                ],
            },
            "adjustment": {
                "invalid_rows": int(invalid_factors),
                "invalid_symbols": int(invalid_factor_symbols),
            },
            "blockers": blockers,
        }
        return report

    def _normalize_partition(self, directory: Path) -> Path:
        parts = sorted(directory.glob("*.parquet"))
        if not parts:
            raise StockDataStockImportBlocked(f"staged partition is empty: {directory}")
        frame = pl.concat([pl.read_parquet(path) for path in parts], how="vertical_relaxed").sort(
            ["symbol", "datetime"]
        )
        target = directory / "part.parquet"
        _atomic_parquet(frame, target)
        for path in parts:
            if path != target and path.exists():
                path.unlink()
        return target

    def _validate_and_stage_coverage(self, stage_root: Path, expected_rows: int) -> dict[str, Any]:
        minute_paths = sorted((stage_root / "minute").glob("date=*/*.parquet"))
        if not minute_paths:
            raise StockDataStockImportBlocked(f"no staged minute partitions for {self.year}")
        self.db.execute("DROP VIEW IF EXISTS staged_minute")
        self.db.execute(
            f"CREATE TEMP VIEW staged_minute AS SELECT symbol, CAST(datetime AS TIMESTAMP) AS datetime, "
            f"CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, CAST(low AS DOUBLE) AS low, "
            f"CAST(close AS DOUBLE) AS close, CAST(volume AS DOUBLE) AS volume, CAST(amount AS DOUBLE) AS amount "
            f"FROM {_parquet_source(minute_paths)}"
        )
        summary = self.db.execute(f"""
            SELECT
                count(*), count(*) - count(DISTINCT (symbol, datetime)),
                count(*) FILTER (WHERE {_INVALID_VALUE_SQL}),
                count(*) FILTER (WHERE NOT {_SESSION_SQL})
            FROM staged_minute
        """).fetchone()
        if int(summary[0]) != expected_rows:
            raise StockDataStockImportBlocked(
                f"staged row mismatch for {self.year}: expected {expected_rows}, got {summary[0]}"
            )
        if summary[1] or summary[2] or summary[3]:
            raise StockDataStockImportBlocked(
                f"staged validation failed for {self.year}: duplicates={summary[1]}, "
                f"invalid={summary[2]}, session={summary[3]}"
            )
        groups = self.db.execute(f"""
            SELECT
                CAST(datetime AS DATE) date, symbol,
                count(*)::BIGINT bars,
                count(DISTINCT datetime)::BIGINT unique_bars,
                count(*) FILTER (WHERE {_CONTINUOUS_SQL})::BIGINT continuous_rows,
                count(DISTINCT datetime) FILTER (WHERE {_CONTINUOUS_SQL})::BIGINT continuous_unique,
                count(*) FILTER (WHERE {_AUCTION_SQL})::BIGINT auction_rows,
                count(*) FILTER (WHERE volume = 0)::BIGINT zero_volume_rows
            FROM staged_minute
            GROUP BY date, symbol
            ORDER BY date, symbol
        """).pl()
        coverage_root = stage_root / "coverage"
        complete_total = 0
        for day_frame in groups.partition_by("date", maintain_order=True):
            day = day_frame["date"][0]
            records: list[dict[str, Any]] = []
            for row in day_frame.iter_rows(named=True):
                complete = (
                    int(row["bars"]) == int(row["unique_bars"])
                    and int(row["continuous_rows"]) == 240
                    and int(row["continuous_unique"]) == 240
                    and int(row["auction_rows"]) in (0, 1)
                )
                complete_total += int(complete)
                records.append({
                    "symbol": row["symbol"],
                    "bars": int(row["bars"]),
                    "complete": complete,
                })
            payload = {
                "schema_version": 1,
                "expected_continuous_bars": 240,
                "optional_auction_bar": "09:30",
                "symbols": len(records),
                "complete_symbols": sum(bool(row["complete"]) for row in records),
                "incomplete_symbols": sum(not bool(row["complete"]) for row in records),
                "groups": records,
                "trade_date": str(day),
                "source": "stockdata_day_parquet",
                "ownership": "existing_tickflow_rows_win",
                "representation": "fixed_clock_grid",
                "zero_volume_rows": int(day_frame["zero_volume_rows"].sum()),
                "zero_volume_tradeable": False,
            }
            _atomic_json(coverage_root / f"date={day}.json", payload)
        return {
            "rows": int(summary[0]),
            "partitions": len(minute_paths),
            "symbol_days": groups.height,
            "complete_symbol_days": complete_total,
            "bytes": sum(path.stat().st_size for path in minute_paths),
        }

    def stage(self, audit: Mapping[str, Any]) -> dict[str, Any]:
        if audit.get("blockers"):
            raise StockDataStockImportBlocked(str(audit["blockers"][0]))
        publish_rows = int(audit["minute"]["publish_rows"])
        if not publish_rows:
            return {"status": "skipped", "rows": 0, "partitions": 0, "targets": []}
        assert_disk_reserve(self.config.data_dir)
        stage_root = self.run_root / "staging"
        if stage_root.exists():
            raise StockDataStockImportBlocked(f"staging directory already exists: {stage_root}")
        (stage_root / "minute").mkdir(parents=True)
        self.progress(f"Staging stock minute year {self.year}: {publish_rows} new rows")
        self.db.execute(f"""
            COPY (
                WITH incoming AS (
                    SELECT
                        s.symbol,
                        CAST(s.datetime AS TIMESTAMP) AS datetime,
                        CAST(s.open * g.adjustment_ratio AS DOUBLE) AS open,
                        CAST(s.high * g.adjustment_ratio AS DOUBLE) AS high,
                        CAST(s.low * g.adjustment_ratio AS DOUBLE) AS low,
                        CAST(s.close * g.adjustment_ratio AS DOUBLE) AS close,
                        CAST(s.volume / 100.0 AS DOUBLE) AS volume,
                        CAST(s.amount AS DOUBLE) AS amount,
                        s.date
                    FROM source_base s
                    JOIN publish_groups g USING (symbol, date)
                    JOIN publish_dates d USING (date)
                    LEFT JOIN current_minute_unique c USING (symbol, datetime)
                    WHERE {_SESSION_SQL} AND c.symbol IS NULL
                ),
                existing AS (
                    SELECT
                        c.symbol, CAST(c.datetime AS TIMESTAMP) AS datetime,
                        CAST(c.open AS DOUBLE) AS open, CAST(c.high AS DOUBLE) AS high,
                        CAST(c.low AS DOUBLE) AS low, CAST(c.close AS DOUBLE) AS close,
                        CAST(c.volume AS DOUBLE) AS volume, CAST(c.amount AS DOUBLE) AS amount,
                        CAST(c.datetime AS DATE) AS date
                    FROM current_minute c
                    JOIN publish_dates d ON CAST(c.datetime AS DATE) = d.date
                )
                SELECT * FROM existing
                UNION ALL
                SELECT * FROM incoming
            ) TO {_sql_string(stage_root / 'minute')} (
                FORMAT PARQUET,
                PARTITION_BY (date),
                COMPRESSION ZSTD,
                COMPRESSION_LEVEL 3,
                PER_THREAD_OUTPUT false,
                FILENAME_PATTERN 'part'
            )
        """)
        for directory in sorted((stage_root / "minute").glob("date=*")):
            self._normalize_partition(directory)
        expected = int(audit["minute"]["staged_final_rows"])
        state = self._validate_and_stage_coverage(stage_root, expected)
        state.update({
            "status": "staged",
            "new_rows": publish_rows,
            "targets": [
                {"date": day, "fingerprints": values}
                for day, values in sorted(self.target_states.items())
            ],
        })
        _atomic_json(self.run_root / "manifest.json", {
            "status": "staged", "audit": audit, "staged": state,
        })
        return state

    def publish(self, audit: Mapping[str, Any], staged: Mapping[str, Any]) -> dict[str, Any]:
        if staged.get("status") == "skipped":
            return {"status": "skipped", "year": self.year, "minute_rows": 0}
        assert_disk_reserve(self.config.data_dir)
        stage_root = self.run_root / "staging"
        backup_root = self.run_root / "backups"
        prepared: list[tuple[Path, Path, Path | None]] = []
        for day, fingerprints in sorted(self.target_states.items()):
            minute_target = self.config.data_dir / "kline_minute" / f"date={day}" / "part.parquet"
            coverage_target = self.config.data_dir / "kline_minute" / "_coverage" / f"date={day}.json"
            if _file_fingerprint(minute_target) != fingerprints["minute"]:
                raise StockDataStockImportBlocked(f"minute target changed during staging: {minute_target}")
            if _file_fingerprint(coverage_target) != fingerprints["coverage"]:
                raise StockDataStockImportBlocked(f"coverage target changed during staging: {coverage_target}")
            for target, source, kind in (
                (minute_target, stage_root / "minute" / f"date={day}" / "part.parquet", "minute"),
                (coverage_target, stage_root / "coverage" / f"date={day}.json", "coverage"),
            ):
                if not source.exists():
                    raise StockDataStockImportBlocked(f"staged {kind} file is missing: {source}")
                backup = backup_root / kind / f"date={day}" / target.name if target.exists() else None
                prepared.append((target, source, backup))

        published: list[tuple[Path, Path, Path | None]] = []
        try:
            for target, source, backup in prepared:
                target.parent.mkdir(parents=True, exist_ok=True)
                if backup is not None:
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    os.link(target, backup)
                os.replace(source, target)
                published.append((target, source, backup))
        except Exception:
            for target, source, backup in reversed(published):
                if target.exists():
                    source.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(target, source)
                if backup is not None and backup.exists():
                    os.replace(backup, target)
            raise

        generation = uuid4().hex
        _atomic_json(
            self.config.data_dir / ".matrix_generation_stock.json",
            {"generation": generation, "updated_at_ns": time.time_ns()},
        )
        result = {
            "status": "published",
            "year": self.year,
            "minute_rows": int(staged["new_rows"]),
            "minute_partitions": int(staged["partitions"]),
            "matrix_generation": generation,
            "rollback_dir": str(backup_root),
        }
        _atomic_json(self.run_root / "manifest.json", {
            "status": "published", "audit": audit, "staged": staged, "publish": result,
        })
        return result


def _aggregate_audits(audits: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "status": "blocked" if any(audit.get("blockers") for audit in audits) else "ready",
        "years": len(audits),
        "source_files": sum(int(audit["source"]["files"]) for audit in audits),
        "source_bytes": sum(int(audit["source"]["bytes"]) for audit in audits),
        "source_rows": sum(int(audit["source"]["rows"]) for audit in audits),
        "eligible_rows": sum(int(audit["minute"]["eligible_rows"]) for audit in audits),
        "publish_rows": sum(int(audit["minute"]["publish_rows"]) for audit in audits),
        "publish_partitions": sum(int(audit["minute"]["publish_partitions"]) for audit in audits),
        "overlap_rows_preserved": sum(
            int(audit["minute"]["overlap_rows_preserved"]) for audit in audits
        ),
        "quarantined_symbol_days": sum(
            int(audit["minute"]["quarantined_symbol_days"]) for audit in audits
        ),
        "rejected_out_of_session_rows": sum(
            int(audit["minute"]["rejected_out_of_session_rows"]) for audit in audits
        ),
        "zero_volume_rows": sum(int(audit["minute"]["zero_volume_rows"]) for audit in audits),
    }


def run_stockdata_stock_import(
    config: StockDataStockImportConfig,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    config = config.normalized()
    progress = progress or (lambda _message: None)
    run_root = config.data_dir / "backfill_state" / "stockdata_stock" / str(config.run_id)
    if config.publish:
        if run_root.exists():
            raise StockDataStockImportBlocked(f"run directory already exists: {run_root}")
        run_root.mkdir(parents=True)
        _atomic_json(run_root / "manifest.json", {
            "status": "running",
            "run_id": config.run_id,
            "scope": {"start": config.start, "end": config.end, "asset_type": "stock"},
            "years": {},
        })

    audits: list[dict[str, Any]] = []
    publishes: list[dict[str, Any]] = []
    try:
        for year in range(config.start.year, config.end.year + 1):
            importer = _StockDataStockYearImporter(config, year, run_root, progress)
            try:
                audit = importer.audit()
                audits.append(audit)
                if config.publish:
                    _atomic_json(run_root / "manifest.json", {
                        "status": "running",
                        "run_id": config.run_id,
                        "summary": _aggregate_audits(audits),
                        "years": {str(item["year"]): item for item in audits},
                        "published": publishes,
                    })
                    staged = importer.stage(audit)
                    publishes.append(importer.publish(audit, staged))
            finally:
                importer.close()
    except Exception as exc:
        if config.publish:
            _atomic_json(run_root / "manifest.json", {
                "status": "failed",
                "run_id": config.run_id,
                "summary": _aggregate_audits(audits),
                "years": {str(item["year"]): item for item in audits},
                "published": publishes,
                "error": str(exc),
            })
        raise

    summary = _aggregate_audits(audits)
    if not config.publish:
        return {
            "run_id": config.run_id,
            "audit": {"summary": summary, "years": audits},
            "publish": {"status": "dry_run"},
        }
    publish_result = {
        "status": "published",
        "minute_rows": sum(int(item.get("minute_rows", 0)) for item in publishes),
        "minute_partitions": sum(int(item.get("minute_partitions", 0)) for item in publishes),
        "years": publishes,
        "manifest": str(run_root / "manifest.json"),
    }
    _atomic_json(run_root / "manifest.json", {
        "status": "published",
        "run_id": config.run_id,
        "summary": summary,
        "years": {str(item["year"]): item for item in audits},
        "publish": publish_result,
    })
    return {
        "run_id": config.run_id,
        "audit": {"summary": summary, "years": audits},
        "publish": publish_result,
    }
