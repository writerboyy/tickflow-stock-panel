"""BaoStock historical CSI index membership collection."""
from __future__ import annotations

from bisect import bisect_left
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date
import importlib
from pathlib import Path
import socket
import threading
import time
from typing import Any

import polars as pl

from app.plugins.baostock.socket_proxy import (
    force_proxy_enabled,
    open_baostock_proxy_connection,
)
from app.plugins.pit_history.storage import (
    INDEX_MEMBERSHIP_HISTORY_TABLE,
    STRICT_INDEX_EXPECTATIONS,
    merge_index_membership_history,
    normalize_symbol,
    read_history_table,
    validate_index_membership_history,
)
from app.services.ingestion_manifest import (
    archive_source_payload,
    stable_content_hash,
    update_ingestion_manifest,
)


SOURCE = "baostock"
PARSER_VERSION = "baostock_index_membership_v2"
DEFAULT_INDEX_NAMES = {
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000906.SH": "中证800",
}
_INDEX_QUERY_METHODS = {
    "000300.SH": "query_hs300_stocks",
    "000905.SH": "query_zz500_stocks",
}
_BAOSTOCK_BLACKLIST_ERROR_CODE = "10001011"
_BAOSTOCK_LOCK = threading.Lock()
_HISTORICAL_SESSION_QUERY_LIMIT = 20
_QUERY_RETRIES = 3


class _SocketWithTimeout:
    def __init__(self, value: Any, timeout: float) -> None:
        self._value = value
        self._timeout = timeout
        self._value.settimeout(timeout)

    def connect(self, address: tuple[str, int]) -> None:
        host, port = address
        if force_proxy_enabled():
            self._replace_with_proxy(host, port)
            return
        try:
            self._value.connect(address)
        except OSError as direct_error:
            try:
                self._replace_with_proxy(host, port)
            except OSError as proxy_error:
                raise direct_error from proxy_error

    def _replace_with_proxy(self, host: str, port: int) -> None:
        proxy_socket = open_baostock_proxy_connection(host, port, timeout=self._timeout)
        try:
            self._value.close()
        finally:
            self._value = proxy_socket

    def recv(self, *args: Any, **kwargs: Any) -> bytes:
        data = self._value.recv(*args, **kwargs)
        if data == b"":
            raise ConnectionError("BaoStock socket closed")
        return data

    def __getattr__(self, name: str) -> Any:
        return getattr(self._value, name)


class _SocketModuleWithTimeout:
    def __init__(self, original: Any, timeout: float) -> None:
        self._original = original
        self._timeout = timeout

    def socket(self, *args: Any, **kwargs: Any):
        value = self._original.socket(*args, **kwargs)
        return _SocketWithTimeout(value, self._timeout)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._original, name)


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null", "<na>"} else text


def _parse_date(value: object) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _member_code(symbol: str) -> str:
    return symbol.split(".", 1)[0] if "." in symbol else symbol


def _login_error_message(login: Any) -> str:
    error_code = str(getattr(login, "error_code", ""))
    error_msg = str(getattr(login, "error_msg", ""))
    message = f"BaoStock login failed: error_code={error_code} error_msg={error_msg}"
    if error_code == _BAOSTOCK_BLACKLIST_ERROR_CODE:
        message += "; IP is in BaoStock blacklist, contact BaoStock QQ group administrator"
    return message


def normalize_index_membership_snapshot(
    rows: Iterable[dict[str, Any]],
    *,
    index_symbol: str,
    index_name: str,
    snapshot_date: date,
) -> pl.DataFrame:
    raw_rows = list(rows)
    output: list[dict[str, Any]] = []
    normalized_index = normalize_symbol(index_symbol).upper()
    snapshot_hash = stable_content_hash(
        {
            "dataset": INDEX_MEMBERSHIP_HISTORY_TABLE,
            "index_symbol": normalized_index,
            "snapshot_date": snapshot_date.isoformat(),
            "rows": raw_rows,
        }
    )
    for row in raw_rows:
        member_symbol = normalize_symbol(
            row.get("member_symbol")
            or row.get("code")
            or row.get("证券代码")
            or row.get("股票代码")
        )
        if not member_symbol:
            continue
        output.append(
            {
                "index_symbol": normalized_index,
                "index_name": _text(index_name),
                "member_symbol": member_symbol,
                "member_code": _member_code(member_symbol),
                "member_name": _text(
                    row.get("member_name")
                    or row.get("code_name")
                    or row.get("证券简称")
                    or row.get("股票简称")
                    or row.get("name")
                ),
                "snapshot_date": snapshot_date,
                "source_update_date": _parse_date(row.get("updateDate") or row.get("date")),
                "source": SOURCE,
                "provenance": "source_dated_snapshot",
                "snapshot_hash": snapshot_hash,
            }
        )
    if not output:
        return pl.DataFrame()
    return (
        pl.DataFrame(output)
        .select(
            pl.col("index_symbol").cast(pl.String),
            pl.col("index_name").cast(pl.String),
            pl.col("member_symbol").cast(pl.String),
            pl.col("member_code").cast(pl.String),
            pl.col("member_name").cast(pl.String),
            pl.col("snapshot_date").cast(pl.Date),
            pl.col("source_update_date").cast(pl.Date),
            pl.col("source").cast(pl.String),
            pl.col("provenance").cast(pl.String),
            pl.col("snapshot_hash").cast(pl.String),
        )
        .unique(subset=["index_symbol", "member_symbol", "snapshot_date"], keep="last")
        .sort(["index_symbol", "snapshot_date", "member_symbol"])
    )


class BaoStockIndexMembershipCollector:
    def __init__(
        self,
        data_dir: Path,
        bs_module: Any | None = None,
        *,
        timeout: float = 5.0,
        query_delay_seconds: float = 0.0,
    ) -> None:
        self.data_dir = Path(data_dir)
        self._bs_module = bs_module
        self._timeout = timeout
        self._query_delay_seconds = query_delay_seconds
        self._historical_gaps: dict[str, list[dict[str, Any]]] = {}

    @contextmanager
    def _session(self) -> Iterator[Any]:
        with _BAOSTOCK_LOCK:
            bs = self._baostock()
            socket_util = None
            original_socket_module = None
            if self._bs_module is None:
                try:
                    socket_util = importlib.import_module("baostock.util.socketutil")
                    original_socket_module = socket_util.socket
                    socket_util.socket = _SocketModuleWithTimeout(socket, self._timeout)
                except (AttributeError, ModuleNotFoundError):
                    pass
            try:
                login = bs.login()
                if getattr(login, "error_code", "0") != "0":
                    raise RuntimeError(_login_error_message(login))
                try:
                    yield bs
                finally:
                    try:
                        bs.logout()
                    except Exception:  # noqa: BLE001
                        pass
            finally:
                if socket_util is not None and original_socket_module is not None:
                    socket_util.socket = original_socket_module

    def fetch_index_snapshots(
        self,
        indices: Iterable[str],
        *,
        snapshot_dates: Iterable[date],
        index_names: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        dates = sorted(set(snapshot_dates))
        if not dates:
            return pl.DataFrame()
        names = {**DEFAULT_INDEX_NAMES, **(index_names or {})}
        normalized_indices = sorted({normalize_symbol(item).upper() for item in indices if item})
        unsupported = [item for item in normalized_indices if item not in _INDEX_QUERY_METHODS]
        if unsupported:
            raise ValueError(f"BaoStock index membership is unavailable for: {unsupported}")

        frames: list[pl.DataFrame] = []
        raw_payloads: dict[str, Any] = {}
        with self._session() as bs:
            query_count = len(normalized_indices) * len(dates)
            completed = 0
            for normalized_index in normalized_indices:
                query = getattr(bs, _INDEX_QUERY_METHODS[normalized_index])
                for snapshot_date in dates:
                    result = query(date=snapshot_date.isoformat())
                    rows = _result_rows(result)
                    raw_payloads[f"{normalized_index}:{snapshot_date.isoformat()}"] = {
                        "index_symbol": normalized_index,
                        "fields": list(getattr(result, "fields", [])),
                        "rows": rows,
                    }
                    frame = normalize_index_membership_snapshot(
                        rows,
                        index_symbol=normalized_index,
                        index_name=names.get(normalized_index, ""),
                        snapshot_date=snapshot_date,
                    )
                    if not frame.is_empty():
                        frames.append(frame)
                    completed += 1
                    if self._query_delay_seconds > 0 and completed < query_count:
                        time.sleep(self._query_delay_seconds)

        logical_snapshot = f"{dates[0].isoformat()}_{dates[-1].isoformat()}"
        archive_source_payload(
            self.data_dir,
            SOURCE,
            INDEX_MEMBERSHIP_HISTORY_TABLE,
            logical_snapshot,
            "all-indices",
            raw_payloads,
            parser_version=PARSER_VERSION,
        )
        if not frames:
            return pl.DataFrame()
        merged = pl.concat(frames, how="diagonal_relaxed").sort(
            ["index_symbol", "snapshot_date", "member_symbol"]
        )
        validation = validate_index_membership_history(merged)
        if not validation["usable"]:
            raise ValueError(f"BaoStock index membership failed strict validation: {validation}")
        return merged

    def fetch_historical_membership(
        self,
        index_symbol: str,
        *,
        trading_dates: Iterable[date],
        index_name: str = "",
        allow_incomplete_snapshots: bool = False,
    ) -> pl.DataFrame:
        dates = sorted(set(trading_dates))
        if not dates:
            return pl.DataFrame()
        normalized_index = normalize_symbol(index_symbol).upper()
        method_name = _INDEX_QUERY_METHODS.get(normalized_index)
        if method_name is None:
            raise ValueError(f"BaoStock index membership is unavailable for {normalized_index}")
        expected = int(STRICT_INDEX_EXPECTATIONS[normalized_index]["expected_members"])
        frames: list[pl.DataFrame] = []
        raw_payloads: dict[str, Any] = {}
        gaps: list[dict[str, Any]] = []
        target_pos = len(dates) - 1
        failures = 0
        while target_pos >= 0:
            try:
                with self._session() as bs:
                    query = getattr(bs, method_name)
                    session_queries = 0
                    while target_pos >= 0 and session_queries < _HISTORICAL_SESSION_QUERY_LIMIT:
                        query_date = dates[target_pos]
                        result = query(date=query_date.isoformat())
                        rows = _result_rows(result)
                        raw_payloads[query_date.isoformat()] = {
                            "index_symbol": normalized_index,
                            "fields": list(getattr(result, "fields", [])),
                            "rows": rows,
                        }
                        frame = normalize_index_membership_snapshot(
                            rows,
                            index_symbol=normalized_index,
                            index_name=index_name
                            or DEFAULT_INDEX_NAMES.get(normalized_index, ""),
                            snapshot_date=query_date,
                        )
                        if frame.height != expected:
                            if not allow_incomplete_snapshots:
                                raise ValueError(
                                    f"{normalized_index} {query_date} returned {frame.height} "
                                    f"members, expected {expected}"
                                )
                            update_dates = (
                                frame["source_update_date"].drop_nulls().unique().to_list()
                                if not frame.is_empty()
                                else []
                            )
                            if len(update_dates) == 1 and update_dates[0] <= query_date:
                                gap_start_pos = bisect_left(
                                    dates, update_dates[0], 0, target_pos + 1
                                )
                                source_update_date = update_dates[0].isoformat()
                            else:
                                gap_start_pos = target_pos
                                source_update_date = None
                            missing_dates = dates[gap_start_pos : target_pos + 1]
                            gaps.append(
                                {
                                    "index_symbol": normalized_index,
                                    "query_date": query_date.isoformat(),
                                    "source_update_date": source_update_date,
                                    "returned_members": frame.height,
                                    "expected_members": expected,
                                    "missing_date_start": missing_dates[0].isoformat(),
                                    "missing_date_end": missing_dates[-1].isoformat(),
                                    "missing_snapshot_dates": len(missing_dates),
                                }
                            )
                            target_pos = gap_start_pos - 1
                            session_queries += 1
                            failures = 0
                            if self._query_delay_seconds > 0 and target_pos >= 0:
                                time.sleep(self._query_delay_seconds)
                            continue
                        update_dates = (
                            frame["source_update_date"].drop_nulls().unique().to_list()
                        )
                        if len(update_dates) != 1 or update_dates[0] > query_date:
                            raise ValueError(
                                f"{normalized_index} {query_date} has invalid source update "
                                f"dates: {update_dates}"
                            )
                        update_date = update_dates[0]
                        start_pos = bisect_left(dates, update_date, 0, target_pos + 1)
                        for snapshot_date in dates[start_pos : target_pos + 1]:
                            frames.append(
                                frame.with_columns(
                                    pl.lit(snapshot_date)
                                    .cast(pl.Date)
                                    .alias("snapshot_date"),
                                    pl.lit("source_effective_snapshot").alias("provenance"),
                                )
                            )
                        target_pos = start_pos - 1
                        session_queries += 1
                        failures = 0
                        if self._query_delay_seconds > 0 and target_pos >= 0:
                            time.sleep(self._query_delay_seconds)
            except (ConnectionError, OSError, RuntimeError) as exc:
                failures += 1
                if failures >= _QUERY_RETRIES:
                    raise RuntimeError(
                        f"BaoStock historical query failed after {_QUERY_RETRIES} attempts "
                        f"for {normalized_index} {dates[target_pos]}: {exc}"
                    ) from exc
                time.sleep(max(self._query_delay_seconds, 1.0))

        logical_snapshot = f"{normalized_index}_{dates[0].isoformat()}_{dates[-1].isoformat()}"
        archive_source_payload(
            self.data_dir,
            SOURCE,
            INDEX_MEMBERSHIP_HISTORY_TABLE,
            logical_snapshot,
            normalized_index,
            raw_payloads,
            parser_version=PARSER_VERSION,
        )
        if not frames:
            raise ValueError(
                f"BaoStock returned no complete historical snapshots for {normalized_index}"
            )
        merged = pl.concat(frames, how="diagonal_relaxed").sort(
            ["index_symbol", "snapshot_date", "member_symbol"]
        )
        validation = validate_index_membership_history(merged, index_symbol=normalized_index)
        missing_snapshot_dates = sum(
            int(item["missing_snapshot_dates"]) for item in gaps
        )
        if (
            not validation["usable"]
            or validation["snapshot_dates"] != len(dates) - missing_snapshot_dates
        ):
            raise ValueError(f"BaoStock historical membership failed validation: {validation}")
        self._historical_gaps[normalized_index] = gaps
        return merged

    def collect_historical_membership(
        self,
        *,
        trading_dates: Iterable[date],
        indices: Iterable[str] = ("000300.SH", "000905.SH"),
    ) -> dict[str, Any]:
        dates = sorted(set(trading_dates))
        if not dates:
            raise ValueError("historical membership requires at least one trading date")
        normalized_indices = tuple(normalize_symbol(item).upper() for item in indices)
        frames = [
            self.fetch_historical_membership(
                index_symbol,
                trading_dates=dates,
                index_name=DEFAULT_INDEX_NAMES.get(index_symbol, ""),
                allow_incomplete_snapshots=True,
            )
            for index_symbol in normalized_indices
        ]
        source = pl.concat(frames, how="diagonal_relaxed")
        existing = read_history_table(self.data_dir, INDEX_MEMBERSHIP_HISTORY_TABLE)
        existing_keys = (
            existing.select("index_symbol", "snapshot_date").unique()
            if not existing.is_empty()
            else pl.DataFrame(
                schema={"index_symbol": pl.String, "snapshot_date": pl.Date}
            )
        )
        conflicts: list[dict[str, Any]] = []
        if not existing.is_empty():
            for snapshot in source.join(
                existing_keys,
                on=["index_symbol", "snapshot_date"],
                how="semi",
            ).partition_by(["index_symbol", "snapshot_date"], maintain_order=True):
                index_symbol = str(snapshot["index_symbol"][0])
                snapshot_date = snapshot["snapshot_date"][0]
                stored = existing.filter(
                    (pl.col("index_symbol") == index_symbol)
                    & (pl.col("snapshot_date") == snapshot_date)
                )
                stored_members = set(stored["member_symbol"].to_list())
                source_members = set(snapshot["member_symbol"].to_list())
                if stored_members != source_members:
                    conflicts.append(
                        {
                            "index_symbol": index_symbol,
                            "snapshot_date": snapshot_date.isoformat(),
                            "canonical_only": sorted(stored_members - source_members)[:20],
                            "baostock_only": sorted(source_members - stored_members)[:20],
                        }
                    )

        additions = source.join(
            existing_keys,
            on=["index_symbol", "snapshot_date"],
            how="anti",
        )
        derivation_source = additions
        if not existing.is_empty():
            retained = existing.join(
                source.select("index_symbol", "snapshot_date").unique(),
                on=["index_symbol", "snapshot_date"],
                how="semi",
            ).filter(pl.col("index_symbol").is_in(["000300.SH", "000905.SH"]))
            derivation_source = pl.concat(
                [retained, additions], how="diagonal_relaxed"
            )
        publish_frames = [additions]
        derived_missing_dates: list[date] = []
        if {"000300.SH", "000905.SH"}.issubset(set(normalized_indices)):
            derivation_source = derivation_source.filter(
                pl.col("index_symbol").is_in(["000300.SH", "000905.SH"])
            )
            complete_dates = (
                derivation_source.group_by("snapshot_date")
                .agg(pl.col("index_symbol").n_unique().alias("source_indices"))
                .filter(pl.col("source_indices") == 2)["snapshot_date"]
                .to_list()
            )
            complete_date_set = set(complete_dates)
            derived_missing_dates = [item for item in dates if item not in complete_date_set]
            if complete_dates:
                derived = derive_csi800(
                    derivation_source.filter(
                        pl.col("snapshot_date").is_in(complete_dates)
                    )
                )
                publish_frames.append(
                    derived.join(
                        existing_keys,
                        on=["index_symbol", "snapshot_date"],
                        how="anti",
                    )
                )
        incoming = pl.concat(publish_frames, how="diagonal_relaxed").sort(
            ["index_symbol", "snapshot_date", "member_symbol"]
        )
        result = merge_index_membership_history(self.data_dir, incoming)
        result["preserved_conflicts"] = len(conflicts)
        result["conflict_samples"] = conflicts[:20]
        gap_groups = [
            item
            for index_symbol in normalized_indices
            for item in self._historical_gaps.get(index_symbol, [])
        ]
        result["source_gap_groups"] = len(gap_groups)
        result["missing_source_snapshot_dates"] = sum(
            int(item["missing_snapshot_dates"]) for item in gap_groups
        )
        result["source_gap_samples"] = gap_groups[:20]
        result["missing_csi800_snapshot_dates"] = len(derived_missing_dates)
        result["csi800_gap_samples"] = [item.isoformat() for item in derived_missing_dates[:20]]
        update_ingestion_manifest(
            self.data_dir,
            SOURCE,
            INDEX_MEMBERSHIP_HISTORY_TABLE,
            f"historical_{dates[0].isoformat()}_{dates[-1].isoformat()}",
            status="published_with_gaps" if gap_groups else "published",
            parser_version=PARSER_VERSION,
            schema_version=1,
            source_content_hash=stable_content_hash(incoming.to_dicts()),
            content_hash=stable_content_hash(incoming.to_dicts()),
            published_rows=int(result["added_rows"]),
            provenance="source_effective_snapshot",
        )
        return result

    def _baostock(self):
        if self._bs_module is not None:
            return self._bs_module
        import baostock as bs  # noqa: PLC0415

        return bs


def derive_csi800(
    frame: pl.DataFrame,
    *,
    source_name: str = SOURCE,
) -> pl.DataFrame:
    source = frame.filter(pl.col("index_symbol").is_in(["000300.SH", "000905.SH"]))
    expected_dates = source.select("snapshot_date").unique().height
    rows: list[pl.DataFrame] = []
    for snapshot in source.partition_by("snapshot_date", maintain_order=True):
        snapshot_date = snapshot["snapshot_date"][0]
        if set(snapshot["index_symbol"].unique().to_list()) != {"000300.SH", "000905.SH"}:
            raise ValueError(f"cannot derive CSI 800 on {snapshot_date}: missing source index")
        members = snapshot.unique(subset="member_symbol", keep="first")
        if members.height != 800:
            raise ValueError(
                f"cannot derive CSI 800 on {snapshot_date}: union has {members.height} members"
            )
        snapshot_hash = stable_content_hash(sorted(members["member_symbol"].to_list()))
        rows.append(
            members.with_columns(
                pl.lit("000906.SH").alias("index_symbol"),
                pl.lit(DEFAULT_INDEX_NAMES["000906.SH"]).alias("index_name"),
                pl.lit(source_name).alias("source"),
                pl.lit("derived_union_csi300_csi500").alias("provenance"),
                pl.lit(snapshot_hash).alias("snapshot_hash"),
            )
        )
    if not rows:
        return pl.DataFrame()
    derived = pl.concat(rows, how="diagonal_relaxed").sort(["snapshot_date", "member_symbol"])
    if derived.select("snapshot_date").unique().height != expected_dates:
        raise ValueError("CSI 800 derivation did not cover every source snapshot date")
    return derived


def _result_rows(result: Any) -> list[dict[str, str]]:
    if getattr(result, "error_code", "0") != "0":
        raise RuntimeError(
            f"BaoStock index constituents query failed: {getattr(result, 'error_msg', '')}"
        )
    fields = list(getattr(result, "fields", []))
    rows: list[dict[str, str]] = []
    while result.next():
        values = result.get_row_data()
        rows.append({field: values[pos] for pos, field in enumerate(fields)})
    return rows
