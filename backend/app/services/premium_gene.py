"""盘后计算个股溢价基因统计。"""
from __future__ import annotations

from datetime import date, timedelta
import logging
import os
from pathlib import Path
import time
from uuid import uuid4

import polars as pl

from app.indicators.pipeline import compute_indicators, compute_limit_signals
from app.parquet import ENRICHED_STORAGE_SCHEMA, scan_parquet_compat
from app.plugins.kaipanla.client import KaipanlaClient, KaipanlaRequestError
from app.plugins.kaipanla.credentials import load_credentials
from app.plugins.kaipanla.parsers import ResponseShapeError, parse_premium_gene

logger = logging.getLogger(__name__)

WINDOW_DAYS = 200
_HISTORY_CALENDAR_DAYS = 400
_DIR_NAME = "premium_gene"
_FILE_NAME = "part.parquet"
_LIVE_CACHE_TTL = 6 * 60 * 60
_live_cache: dict[tuple[str, date], tuple[float, dict]] = {}
_HISTORY_SCHEMA = {
    **ENRICHED_STORAGE_SCHEMA,
    # 兼容早期 enriched 分区把递推列写成 Int64 的数据。
    "consecutive_limit_ups": pl.Int64,
    "consecutive_limit_downs": pl.Int64,
}

SNAPSHOT_COLUMNS = [
    "symbol",
    "as_of",
    "window_days",
    "limit_up_count",
    "premium_5_count",
    "next_day_observation_count",
    "next_day_red_count",
    "next_day_red_rate",
    "first_board_attempt_count",
    "first_board_sealed_count",
    "first_board_broken_count",
    "first_board_seal_rate",
    "first_board_broken_rate",
    "consecutive_limit_up_count",
    "consecutive_rate",
]

_SNAPSHOT_SCHEMA = {
    "symbol": pl.String,
    "as_of": pl.Date,
    "window_days": pl.Int64,
    "limit_up_count": pl.Int64,
    "premium_5_count": pl.Int64,
    "next_day_observation_count": pl.Int64,
    "next_day_red_count": pl.Int64,
    "next_day_red_rate": pl.Float64,
    "first_board_attempt_count": pl.Int64,
    "first_board_sealed_count": pl.Int64,
    "first_board_broken_count": pl.Int64,
    "first_board_seal_rate": pl.Float64,
    "first_board_broken_rate": pl.Float64,
    "consecutive_limit_up_count": pl.Int64,
    "consecutive_rate": pl.Float64,
}


def snapshot_path(data_dir: Path) -> Path:
    """返回最新溢价基因快照路径。"""
    return data_dir / _DIR_NAME / _FILE_NAME


def _empty_snapshot() -> pl.DataFrame:
    return pl.DataFrame(schema=_SNAPSHOT_SCHEMA)


def _percent_to_fraction(value: float | None) -> float | None:
    return None if value is None else value / 100.0


async def _fetch_kaipanla(symbol: str) -> dict:
    """按开盘啦 /76 的六项数组映射为个股分析 API 字段。"""
    credentials = load_credentials()
    if credentials is None:
        raise KaipanlaRequestError("开盘啦凭据未配置")
    code = symbol.split(".", 1)[0]
    async with KaipanlaClient(credentials=credentials, attempts=1) as client:
        payload = await client.request(76, {"StockID": code})
    parsed = parse_premium_gene(payload, code)
    return {
        "limit_up_count": parsed["limit_up_count"],
        "premium_5_count": parsed["premium_5_count"],
        "next_day_red_rate": _percent_to_fraction(parsed["next_day_red_rate_pct"]),
        "first_board_seal_rate": _percent_to_fraction(parsed["first_board_seal_rate_pct"]),
        "first_board_broken_rate": _percent_to_fraction(parsed["first_board_broken_rate_pct"]),
        "consecutive_rate": _percent_to_fraction(parsed["consecutive_rate_pct"]),
    }


def load_snapshot(data_dir: Path) -> pl.DataFrame:
    """读取最新快照；文件不存在或损坏时返回空表。"""
    path = snapshot_path(data_dir)
    if not path.exists():
        return _empty_snapshot()
    try:
        frame = pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.warning("load premium gene snapshot failed: %s", exc)
        return _empty_snapshot()

    # 允许未来新增列，同时为旧快照补出新增字段，保持 API 可用。
    for column, dtype in _SNAPSHOT_SCHEMA.items():
        if column not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(column))
    return frame.select(SNAPSHOT_COLUMNS)


def persist_snapshot(data_dir: Path, rows: pl.DataFrame) -> None:
    """原子替换最新快照，避免 API 读到半写文件。"""
    if rows.is_empty():
        return
    path = snapshot_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = rows.select(SNAPSHOT_COLUMNS)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        rows.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def calculate(df: pl.DataFrame, *, window_days: int = WINDOW_DAYS,
              as_of: date | None = None) -> pl.DataFrame:
    """按股票计算近 ``window_days`` 个交易日的溢价基因统计。

    口径:
    - 涨停次数: ``signal_limit_up`` 为真的交易日数。
    - 溢价 5% 次数/次日红盘率: 涨停日的下一条交易记录涨跌幅分别大于
      5%/0%；没有下一交易日的涨停日不进入次日指标分母。
    - 首板: 前一交易日 ``consecutive_limit_ups`` 为 0，且当日封板或炸板。
    - 连板率: 首板封板后下一交易日晋级二板的次数 / 首板封板次数；
      同一条三板及以上连板只按一次晋级计数。

    ``df`` 应按股票包含窗口前至少一条历史记录，以便首板判断不丢失前一日状态。
    """
    required = {"symbol", "date"}
    if df.is_empty() or not required <= set(df.columns) or window_days <= 0:
        return _empty_snapshot()

    available = ["symbol", "date", "change_pct", "signal_limit_up",
                 "signal_broken_limit_up", "consecutive_limit_ups"]
    frame = df.select([column for column in available if column in df.columns]).with_columns([
        pl.col("symbol").cast(pl.String),
        pl.col("date").cast(pl.Date, strict=False),
    ]).drop_nulls(["symbol", "date"]).sort(["symbol", "date"])
    if frame.is_empty():
        return _empty_snapshot()
    if as_of is None:
        as_of = frame["date"].max()
    frame = frame.filter(pl.col("date") <= as_of)
    if frame.is_empty():
        return _empty_snapshot()

    # 旧 enriched 版本可能没有动态信号列；连续板列仍可作为封板信号的兼容回退。
    if "signal_limit_up" not in frame.columns:
        if "consecutive_limit_ups" in frame.columns:
            frame = frame.with_columns(
                (pl.col("consecutive_limit_ups").cast(pl.Int64, strict=False).fill_null(0) > 0)
                .alias("signal_limit_up")
            )
        else:
            frame = frame.with_columns(pl.lit(False).alias("signal_limit_up"))
    if "signal_broken_limit_up" not in frame.columns:
        frame = frame.with_columns(pl.lit(False).alias("signal_broken_limit_up"))
    if "consecutive_limit_ups" not in frame.columns:
        frame = frame.with_columns(
            pl.col("signal_limit_up").cast(pl.Int64).alias("consecutive_limit_ups")
        )
    if "change_pct" not in frame.columns:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("change_pct"))

    frame = frame.with_columns([
        pl.col("signal_limit_up").fill_null(False).cast(pl.Boolean).alias("_is_limit_up"),
        pl.col("signal_broken_limit_up").fill_null(False).cast(pl.Boolean).alias("_is_broken"),
        pl.col("consecutive_limit_ups").cast(pl.Int64, strict=False).fill_null(0).alias("_consecutive"),
        pl.col("change_pct").cast(pl.Float64, strict=False).alias("_change_pct"),
    ]).with_columns([
        pl.col("_change_pct").shift(-1).over("symbol").alias("_next_change_pct"),
        pl.col("_consecutive").shift(-1).over("symbol").alias("_next_consecutive"),
        pl.col("_consecutive").shift(1).over("symbol").fill_null(0).alias("_prev_consecutive"),
        pl.int_range(0, pl.len()).over("symbol").alias("_position"),
        pl.len().over("symbol").cast(pl.Int64).alias("_symbol_rows"),
    ])

    # 先在完整输入上计算 next/prev，再裁剪到每只股票最后 N 个交易日。
    frame = frame.filter(pl.col("_position") >= pl.col("_symbol_rows") - window_days)
    if frame.is_empty():
        return _empty_snapshot()

    is_limit = pl.col("_is_limit_up")
    is_broken = pl.col("_is_broken")
    has_next = is_limit & pl.col("_next_change_pct").is_not_null()
    first_board = (pl.col("_prev_consecutive") <= 0) & (is_limit | is_broken)
    aggregated = frame.group_by("symbol").agg([
        is_limit.cast(pl.Int64).sum().alias("limit_up_count"),
        (is_limit & has_next & (pl.col("_next_change_pct") > 0.05))
            .cast(pl.Int64).sum().alias("premium_5_count"),
        has_next.cast(pl.Int64).sum().alias("next_day_observation_count"),
        (is_limit & has_next & (pl.col("_next_change_pct") > 0))
            .cast(pl.Int64).sum().alias("next_day_red_count"),
        first_board.cast(pl.Int64).sum().alias("first_board_attempt_count"),
        (first_board & is_limit).cast(pl.Int64).sum().alias("first_board_sealed_count"),
        (first_board & is_broken).cast(pl.Int64).sum().alias("first_board_broken_count"),
        (
            first_board
            & is_limit
            & (pl.col("_next_consecutive") >= 2)
        ).cast(pl.Int64).sum().alias("consecutive_advance_count"),
        (is_limit & (pl.col("_consecutive") >= 2)).cast(pl.Int64)
            .sum().alias("consecutive_limit_up_count"),
    ]).with_columns([
        pl.when(pl.col("next_day_observation_count") > 0)
          .then(pl.col("next_day_red_count") / pl.col("next_day_observation_count"))
          .otherwise(0.0).alias("next_day_red_rate"),
        pl.when(pl.col("first_board_attempt_count") > 0)
          .then(pl.col("first_board_sealed_count") / pl.col("first_board_attempt_count"))
          .otherwise(0.0).alias("first_board_seal_rate"),
        pl.when(pl.col("first_board_attempt_count") > 0)
          .then(pl.col("first_board_broken_count") / pl.col("first_board_attempt_count"))
          .otherwise(0.0).alias("first_board_broken_rate"),
        pl.when(pl.col("first_board_sealed_count") > 0)
          .then(pl.col("consecutive_advance_count") / pl.col("first_board_sealed_count"))
          .otherwise(0.0).alias("consecutive_rate"),
        pl.lit(as_of, dtype=pl.Date).alias("as_of"),
        pl.lit(window_days, dtype=pl.Int64).alias("window_days"),
    ])
    return aggregated.select(SNAPSHOT_COLUMNS).sort("symbol")


def _load_history(repo, as_of: date) -> pl.DataFrame:
    """读取足够覆盖 200 个交易日的窄 enriched 数据。"""
    root = repo.store.data_dir / "kline_daily_enriched"
    if not root.exists():
        return pl.DataFrame()
    try:
        frame = (
            scan_parquet_compat(
                str(root / "**" / "*.parquet"),
                schema=_HISTORY_SCHEMA,
                cast_options=pl.ScanCastOptions(integer_cast="upcast"),
            )
            .filter(
                (pl.col("date") >= as_of - timedelta(days=_HISTORY_CALENDAR_DAYS))
                & (pl.col("date") <= as_of)
            )
            .select([
                "symbol", "date", "open", "high", "low", "close", "volume",
                "raw_close", "raw_high", "raw_low", "consecutive_limit_ups",
            ])
            .sort(["symbol", "date"])
            .collect()
        )
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.warning("load premium gene history failed: %s", exc)
        return pl.DataFrame()
    if frame.is_empty():
        return frame

    # 涨跌幅和涨停/炸板信号使用 enriched 统一口径；只计算本指标需要的列。
    try:
        frame = compute_indicators(frame, needed={"change_pct"})
        instruments = repo.get_instruments()
        if instruments.is_empty():
            frame = frame.with_columns([
                (pl.col("consecutive_limit_ups").cast(pl.Int64, strict=False).fill_null(0) > 0)
                .alias("signal_limit_up"),
                pl.lit(False).alias("signal_broken_limit_up"),
            ])
        else:
            frame = compute_limit_signals(
                frame,
                instruments,
                needed={"signal_limit_up", "signal_broken_limit_up"},
                historical_shares=repo.get_historical_shares(),
                historical_names=repo.get_instrument_name_history(),
            )
    except (OSError, pl.exceptions.PolarsError, ValueError, KeyError) as exc:
        logger.warning("compute premium gene signals failed: %s", exc)
        return pl.DataFrame()
    return frame


def refresh(repo, *, window_days: int = WINDOW_DAYS, force: bool = False) -> pl.DataFrame:
    """盘后刷新最新全市场快照并持久化。"""
    as_of = repo.latest_enriched_date("stock")
    if as_of is None:
        return _empty_snapshot()
    old = load_snapshot(repo.store.data_dir)
    if (
        not force
        and not old.is_empty()
        and "as_of" in old.columns
        and old["as_of"].drop_nulls().len() > 0
        and old["as_of"].drop_nulls().max() == as_of
        and old["window_days"].drop_nulls().len() > 0
        and old["window_days"].drop_nulls().max() == window_days
    ):
        return old

    history = _load_history(repo, as_of)
    rows = calculate(history, window_days=window_days, as_of=as_of)
    if not rows.is_empty():
        persist_snapshot(repo.store.data_dir, rows)
    return rows


def get_for_symbol(repo, symbol: str, *, window_days: int = WINDOW_DAYS) -> dict:
    """获取单只股票的最新指标，快照过期时按需补算。"""
    as_of = repo.latest_enriched_date("stock")
    if as_of is None:
        return {
            "available": False,
            "symbol": symbol,
            "as_of": None,
            "window_days": window_days,
        }
    rows = load_snapshot(repo.store.data_dir)
    snapshot_date = rows["as_of"].drop_nulls().max() if not rows.is_empty() else None
    snapshot_window = rows["window_days"].drop_nulls().max() if not rows.is_empty() else None
    if snapshot_date != as_of or snapshot_window != window_days:
        rows = refresh(repo, window_days=window_days)

    match = rows.filter(pl.col("symbol") == symbol) if not rows.is_empty() else _empty_snapshot()
    if match.is_empty():
        return {
            "available": False,
            "symbol": symbol,
            "as_of": str(as_of) if as_of else None,
            "window_days": window_days,
        }
    result = match.row(0, named=True)
    result["available"] = True
    result["as_of"] = str(result["as_of"])
    return result


async def get_for_symbol_async(repo, symbol: str, *, window_days: int = WINDOW_DAYS) -> dict:
    """优先返回开盘啦 /76 结果，未配置或失败时回退本地快照。"""
    normalized = symbol.strip().upper()
    as_of = repo.latest_enriched_date("stock")
    if as_of is None:
        return get_for_symbol(repo, normalized, window_days=window_days)

    if load_credentials() is not None:
        cache_key = (normalized, as_of)
        cached = _live_cache.get(cache_key)
        now = time.monotonic()
        if cached is not None and now - cached[0] < _LIVE_CACHE_TTL:
            return dict(cached[1])
        try:
            live = await _fetch_kaipanla(normalized)
            result = {
                "available": True,
                "symbol": normalized,
                "as_of": str(as_of),
                "window_days": window_days,
                **live,
            }
            _live_cache[cache_key] = (now, result)
            return dict(result)
        except (KaipanlaRequestError, ResponseShapeError, OSError, ValueError) as exc:
            logger.warning("load premium gene from kaipanla failed for %s: %s", normalized, exc)

    return get_for_symbol(repo, normalized, window_days=window_days)
