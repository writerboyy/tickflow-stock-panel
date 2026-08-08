"""标的维表同步服务。

盘前 9:10 先通过 exchanges.list + universes.get/instruments.batch 获取
全量标的元数据，失败时降级到 exchanges.get_instruments("SH"/"SZ"/"BJ")。

Starter+ 盘后可用 quotes.get(universes) 顺便补充 name。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from app.plugins.pit_history.storage import INSTRUMENT_LIFECYCLE_EVENTS_TABLE, read_history_table
from app.tickflow.catalog import DEFAULT_CN_EXCHANGES, fetch_instrument_details, list_cn_exchanges
from app.tickflow.client import get_client

logger = logging.getLogger(__name__)

_EXCHANGES = list(DEFAULT_CN_EXCHANGES)


def _flatten_instruments(items: list[dict]) -> list[dict]:
    """把 SDK 返回的 Instrument 列表 flatten 成扁平行。"""
    rows = []
    for item in items:
        row = {
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "code": item.get("code"),
            "exchange": item.get("exchange"),
            "region": item.get("region"),
            "type": item.get("type"),
        }
        ext = item.get("ext") or {}
        row["listing_date"] = ext.get("listing_date")
        row["total_shares"] = ext.get("total_shares")
        row["float_shares"] = ext.get("float_shares")
        row["tick_size"] = ext.get("tick_size")
        row["limit_up"] = ext.get("limit_up")
        row["limit_down"] = ext.get("limit_down")
        rows.append(row)
    return rows


def _universe_symbols(tf: object, universe_id: str = "CN_Equity_A") -> list[str]:
    universes = getattr(tf, "universes", None)
    get = getattr(universes, "get", None)
    if not callable(get):
        return []
    try:
        detail = get(universe_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("universes.get(%s) failed: %s", universe_id, exc)
        return []
    raw_symbols = detail.get("symbols") if isinstance(detail, dict) else getattr(detail, "symbols", None)
    symbols: list[str] = []
    for raw in raw_symbols or []:
        if isinstance(raw, str):
            symbol = raw
        elif isinstance(raw, dict):
            symbol = raw.get("symbol")
        else:
            symbol = getattr(raw, "symbol", None)
        normalized = str(symbol or "").strip().upper()
        if normalized:
            symbols.append(normalized)
    return list(dict.fromkeys(symbols))


def _fetch_instruments_via_catalog(tf: object) -> list[dict]:
    symbols = _universe_symbols(tf)
    if not symbols:
        return []
    items = fetch_instrument_details(tf, symbols)
    found = {
        str(item.get("symbol") or "").strip().upper()
        for item in items
        if item.get("symbol")
    }
    if not set(symbols).issubset(found):
        logger.warning(
            "instruments.batch returned incomplete CN_Equity_A catalog (%d/%d), using exchange fallback",
            len(found), len(symbols),
        )
        return []
    return items


def _fetch_instruments_via_provider() -> list[dict] | None:
    """若当前日K数据源不是 tickflow 且该 provider 提供 get_instruments, 用它拉标的维表。

    返回 flatten 行列表; 未命中(仍应走 tickflow)时返回 None。
    标的维表跟随日K数据源(二者天然耦合, 无独立偏好项)。
    """
    from app.services import preferences

    provider_name = preferences.get_daily_data_provider()
    if provider_name == "tickflow":
        return None
    from app.data_providers import custom as custom_sources

    if not custom_sources.is_custom_provider(provider_name):
        return None
    provider = custom_sources.get_provider(provider_name)
    if not hasattr(provider, "get_instruments"):
        return None
    try:
        items = provider.get_instruments("stock") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("provider %s get_instruments 失败: %s", provider_name, e)
        return None
    rows = _flatten_instruments(items)
    logger.info("instruments via %s: %d stocks", provider_name, len(rows))
    return rows


def sync_instruments(data_dir: Path) -> int:
    """全量同步标的维表 → data/instruments/instruments.parquet。

    返回写入的行数。
    """
    all_rows = _fetch_instruments_via_provider()
    if all_rows is None:
        # 未命中非 tickflow provider → 走 tickflow 直连
        tf = get_client()
        # Always discover the available CN exchanges first; the result is also
        # used by the compatibility path below.
        exchanges = list_cn_exchanges(tf)
        all_rows = _flatten_instruments(_fetch_instruments_via_catalog(tf))
        if not all_rows:
            all_rows = []
            for ex in exchanges:
                try:
                    items = tf.exchanges.get_instruments(ex, instrument_type="stock")
                    if items:
                        all_rows.extend(_flatten_instruments(items))
                        logger.info("instruments %s: %d stocks", ex, len(items))
                except Exception as e:
                    logger.warning("get_instruments(%s) failed: %s", ex, e)

    if not all_rows:
        return 0

    df = pl.DataFrame(all_rows)
    df = df.with_columns(pl.lit(date.today()).alias("as_of"))

    out = data_dir / "instruments" / "instruments.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    supplement = apply_lifecycle_supplement(data_dir)

    logger.info(
        "instruments synced: %d rows → %s (lifecycle matched=%d appended=%d)",
        supplement["rows"],
        out,
        supplement["matched_symbols"],
        supplement["appended_symbols"],
    )
    return supplement["rows"]


def _exchange_from_symbol(symbol: str) -> str:
    parts = str(symbol or "").strip().upper().split(".")
    return parts[1] if len(parts) == 2 else ""


def _code_from_symbol(symbol: str) -> str:
    return str(symbol or "").split(".", 1)[0]


def _ensure_column(df: pl.DataFrame, name: str, dtype: pl.DataType) -> pl.DataFrame:
    if name in df.columns:
        return df.with_columns(pl.col(name).cast(dtype, strict=False).alias(name))
    return df.with_columns(pl.lit(None, dtype=dtype).alias(name))


def _lifecycle_instruments(data_dir: Path) -> pl.DataFrame:
    events = read_history_table(data_dir, INSTRUMENT_LIFECYCLE_EVENTS_TABLE)
    required = {"symbol", "event_type", "event_date"}
    if events.is_empty() or not required.issubset(events.columns):
        return pl.DataFrame()
    frame = events.group_by("symbol").agg(
        pl.col("event_date").filter(pl.col("event_type") == "listed").min().alias("list_date"),
        pl.col("event_date").filter(pl.col("event_type") == "delisted").max().alias("delist_date"),
        pl.col("name")
        .filter(pl.col("name").is_not_null() & (pl.col("name") != ""))
        .last()
        .alias("name"),
        pl.col("exchange")
        .filter(pl.col("exchange").is_not_null() & (pl.col("exchange") != ""))
        .last()
        .alias("exchange"),
        pl.col("source")
        .filter(pl.col("source").is_not_null() & (pl.col("source") != ""))
        .last()
        .alias("source"),
    )
    if frame.is_empty():
        return frame
    rows: list[dict[str, object]] = []
    for row in frame.to_dicts():
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        exchange = str(row.get("exchange") or "").strip() or _exchange_from_symbol(symbol)
        delist_date = row.get("delist_date")
        rows.append({
            "symbol": symbol,
            "name": row.get("name") or symbol,
            "code": _code_from_symbol(symbol),
            "exchange": exchange,
            "region": None,
            "type": "stock",
            "asset_type": "stock",
            "source": row.get("source") or "pit_history",
            "listing_date": row.get("list_date"),
            "list_date": row.get("list_date"),
            "delist_date": delist_date,
            "status": "delisted" if delist_date is not None else "active",
        })
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).select([
        pl.col("symbol").cast(pl.String),
        pl.col("name").cast(pl.String),
        pl.col("code").cast(pl.String),
        pl.col("exchange").cast(pl.String),
        pl.col("region").cast(pl.String),
        pl.col("type").cast(pl.String),
        pl.col("asset_type").cast(pl.String),
        pl.col("source").cast(pl.String),
        pl.col("listing_date").cast(pl.Date),
        pl.col("list_date").cast(pl.Date),
        pl.col("delist_date").cast(pl.Date),
        pl.col("status").cast(pl.String),
    ]).unique(subset=["symbol"], keep="last").sort("symbol")


def apply_lifecycle_supplement(data_dir: Path) -> dict[str, int]:
    """Apply PIT lifecycle list/delist dates to data/instruments/instruments.parquet."""
    data_dir = Path(data_dir)
    inst_path = data_dir / "instruments" / "instruments.parquet"
    lifecycle = _lifecycle_instruments(data_dir)
    if lifecycle.is_empty():
        rows = pl.read_parquet(inst_path).height if inst_path.exists() else 0
        return {"rows": rows, "matched_symbols": 0, "appended_symbols": 0}

    if inst_path.exists():
        instruments = pl.read_parquet(inst_path)
    else:
        instruments = pl.DataFrame()

    if instruments.is_empty():
        out = lifecycle.filter(pl.col("delist_date").is_not_null()).with_columns(
            pl.lit(date.today()).alias("as_of")
        )
        inst_path.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(inst_path)
        return {"rows": out.height, "matched_symbols": 0, "appended_symbols": out.height}

    instruments = _ensure_column(instruments, "listing_date", pl.Date)
    instruments = _ensure_column(instruments, "list_date", pl.Date)
    instruments = _ensure_column(instruments, "delist_date", pl.Date)
    instruments = _ensure_column(instruments, "status", pl.String)
    instruments = _ensure_column(instruments, "asset_type", pl.String)
    instruments = _ensure_column(instruments, "source", pl.String)
    lifecycle_sources = set(lifecycle["source"].drop_nulls().to_list())
    if lifecycle_sources:
        instruments = instruments.filter(
            ~(
                pl.col("source").is_in(lifecycle_sources).fill_null(False)
                & pl.col("delist_date").is_null()
            )
        )
    updates = lifecycle.select([
        pl.col("symbol"),
        pl.col("list_date").alias("_life_list_date"),
        pl.col("delist_date").alias("_life_delist_date"),
    ])
    existing_symbols = set(instruments["symbol"].to_list()) if "symbol" in instruments.columns else set()
    joined = instruments.join(updates, on="symbol", how="left")
    matched = joined.filter(
        pl.col("_life_list_date").is_not_null() | pl.col("_life_delist_date").is_not_null()
    ).height
    joined = joined.with_columns(
        pl.coalesce([pl.col("listing_date"), pl.col("_life_list_date")]).alias("listing_date"),
        pl.coalesce([pl.col("delist_date"), pl.col("_life_delist_date")]).alias("delist_date"),
    ).with_columns(
        pl.coalesce([pl.col("list_date"), pl.col("listing_date")]).alias("list_date"),
        pl.when(pl.col("delist_date").is_not_null())
        .then(pl.lit("delisted"))
        .otherwise(pl.coalesce([pl.col("status"), pl.lit("active")]))
        .alias("status"),
        pl.coalesce([pl.col("asset_type"), pl.lit("stock")]).alias("asset_type"),
    ).drop(["_life_list_date", "_life_delist_date"])

    missing = lifecycle.filter(
        (~pl.col("symbol").is_in(existing_symbols)) & pl.col("delist_date").is_not_null()
    )
    if "as_of" in joined.columns and "as_of" not in missing.columns:
        missing = missing.with_columns(pl.lit(date.today()).alias("as_of"))
    combined = (
        pl.concat([joined, missing], how="diagonal_relaxed")
        if not missing.is_empty()
        else joined
    ).unique(subset=["symbol"], keep="last").sort("symbol")
    inst_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(inst_path)
    return {
        "rows": combined.height,
        "matched_symbols": matched,
        "appended_symbols": missing.height,
    }


def enrich_names_from_quotes(
    data_dir: Path,
    quotes_data: list[dict],
) -> int:
    """从 quotes 响应中提取 name，更新 instruments 维表（兜底补充）。

    盘后 quotes.get(universes) 返回的数据中包含 ext.name，
    用来补充 instruments 中可能缺失的 name。
    """
    if not quotes_data:
        return 0

    # 构建 symbol → name 映射
    name_map: dict[str, str] = {}
    for q in quotes_data:
        symbol = q.get("symbol", "")
        ext = q.get("ext") or {}
        name = ext.get("name") or q.get("name", "")
        if symbol and name:
            name_map[symbol] = name

    if not name_map:
        return 0

    inst_path = data_dir / "instruments" / "instruments.parquet"
    if not inst_path.exists():
        return 0

    df = pl.read_parquet(inst_path)

    # 只更新空 name 的行
    updates = pl.DataFrame({
        "symbol": list(name_map.keys()),
        "_new_name": list(name_map.values()),
    })
    df = df.join(updates, on="symbol", how="left")
    df = df.with_columns(
        pl.when(pl.col("name").is_null() | (pl.col("name") == ""))
        .then(pl.col("_new_name"))
        .otherwise(pl.col("name"))
        .alias("name"),
    ).drop("_new_name")

    df.write_parquet(inst_path)
    logger.info("instruments name enriched from quotes: %d names", len(name_map))
    return len(name_map)
