"""Read-only TickFlow/QMT realtime observation and latency statistics."""
from __future__ import annotations

import json
import math
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from app.data_providers.normalizer import normalize_tick
from app.services.qmt_trading import QmtZmqRpcClient

_CN_TZ = ZoneInfo("Asia/Shanghai")


def _quote_address(rpc_address: str, explicit: str = "", advertised: str = "") -> str:
    if explicit.strip():
        return explicit.strip()
    rpc = urlsplit(rpc_address)
    advertised_value = urlsplit(advertised) if advertised else None
    port = advertised_value.port if advertised_value and advertised_value.port else (rpc.port or 0) + 1
    if not rpc.hostname or port <= 0:
        raise ValueError("QMT 全推行情地址无法从 RPC 地址推导")
    host = f"[{rpc.hostname}]" if ":" in rpc.hostname else rpc.hostname
    return urlunsplit((rpc.scheme or "tcp", f"{host}:{port}", "", "", ""))


def _decode_push_payload(blob: bytes) -> Any:
    try:
        import msgpack

        return msgpack.unpackb(blob, raw=False)
    except Exception:
        return json.loads(blob.decode("utf-8"))


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _normalized_observation(
    source: str,
    raw: dict[str, Any],
    received_wall: datetime,
    received_monotonic: float,
) -> dict[str, Any] | None:
    default_symbol = str(
        raw.get("symbol") or raw.get("stock_code") or raw.get("stockCode") or ""
    ).strip().upper() or None
    frame = normalize_tick(raw, default_symbol=default_symbol, source=source)
    if frame.is_empty():
        return None
    row = frame.row(0, named=True)
    source_time = row["datetime"].replace(tzinfo=_CN_TZ)
    sequence = row.get("sequence")
    trade_id = row.get("trade_id")
    source_event_id = sequence or trade_id
    stable_id = "|".join((
        str(row["symbol"]),
        str(source_event_id) if source_event_id is not None else source_time.isoformat(),
        "" if source_event_id is not None else str(row["last_price"]),
        "" if source_event_id is not None else str(row.get("volume") or 0),
        "" if source_event_id is not None else str(row.get("amount") or 0),
    ))
    return {
        "source": source,
        "symbol": str(row["symbol"]),
        "source_timestamp": source_time.isoformat(),
        "observer_wall_timestamp": received_wall.isoformat(),
        "observer_monotonic_timestamp": received_monotonic,
        "price": float(row["last_price"]),
        "volume": float(row.get("volume") or 0),
        "amount": float(row.get("amount") or 0),
        "sequence": sequence,
        "trade_id": trade_id,
        "event_id": stable_id,
    }


class QmtWholeQuoteSource:
    """QMT whole-quote source using only subscription lifecycle RPC methods."""

    def __init__(
        self,
        client: QmtZmqRpcClient,
        symbols: Iterable[str],
        *,
        quote_address: str = "",
    ) -> None:
        self.client = client
        self.symbols = {
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        }
        self.markets = sorted({
            symbol.rsplit(".", 1)[-1]
            for symbol in self.symbols
            if "." in symbol
        })
        self.client_id = f"tick-latency-{uuid.uuid4().hex[:12]}"
        self.sub_id = uuid.uuid4().hex[:12]
        self.quote_address = quote_address
        self._socket = None
        self._topic = ""

    def collect(self, callback: Callable[[dict[str, Any]], None], stop: threading.Event) -> None:
        if not self.markets:
            raise ValueError("QMT 全推订阅需要带交易所后缀的股票代码")
        result = self.client.call("subscribe_whole_quote", {
            "client_id": self.client_id,
            "sub_id": self.sub_id,
            "codes": self.markets,
        })
        if not isinstance(result, dict):
            raise ValueError("QMT 全推订阅响应格式无效")
        self._topic = str(result.get("topic") or result.get("combo_key") or "")
        if not self._topic:
            raise ValueError("QMT 全推订阅未返回 topic")
        address = _quote_address(
            self.client.connect_address,
            self.quote_address,
            str(result.get("push_endpoint") or ""),
        )
        try:
            import zmq

            socket = zmq.Context.instance().socket(zmq.SUB)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.SUBSCRIBE, self._topic.encode("utf-8"))
            socket.connect(address)
            self._socket = socket
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            last_keepalive = time.monotonic()
            while not stop.is_set():
                events = dict(poller.poll(200))
                now = time.monotonic()
                if now - last_keepalive >= 10:
                    self.client.call("quote_keepalive", {
                        "client_id": self.client_id,
                        "sub_id": self.sub_id,
                    })
                    last_keepalive = now
                if socket not in events:
                    continue
                frames = socket.recv_multipart()
                if len(frames) < 2:
                    continue
                payload = _decode_push_payload(frames[-1])
                data = payload.get("data") if isinstance(payload, dict) else payload
                if isinstance(data, dict):
                    for symbol, raw in data.items():
                        normalized_symbol = str(symbol).strip().upper()
                        if normalized_symbol not in self.symbols or not isinstance(raw, dict):
                            continue
                        callback({**raw, "symbol": normalized_symbol})
        finally:
            if self._socket is not None:
                self._socket.close(linger=0)
                self._socket = None
            try:
                self.client.call("unsubscribe_whole_quote", {
                    "client_id": self.client_id,
                    "sub_id": self.sub_id,
                })
            finally:
                self.client.close()


class TickFlowStreamSource:
    def __init__(self, symbols: Iterable[str]) -> None:
        self.symbols = sorted({
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        })
        self._stream = None

    def collect(self, callback: Callable[[dict[str, Any]], None], stop: threading.Event) -> None:
        from app.tickflow.client import get_paid_realtime_client

        client = get_paid_realtime_client()
        if client is None:
            raise ValueError("未配置可用的 TickFlow Key")
        from tickflow.resources.stream import MarketStream

        stream = MarketStream(client._client)  # noqa: SLF001 - SDK exposes no stream factory
        self._stream = stream
        stream.on_quotes(lambda records: [callback(row) for row in records])
        stream.subscribe("quotes", self.symbols)
        stream.connect(block=False)
        try:
            stop.wait()
        finally:
            try:
                stream.unsubscribe("quotes", self.symbols)
            finally:
                stream.close()
                self._stream = None


def _percentiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None}
    ordered = sorted(values)

    def value_at(percent: float) -> float:
        return ordered[round((len(ordered) - 1) * percent)]

    return {"p50": value_at(0.50), "p95": value_at(0.95), "p99": value_at(0.99)}


def summarize_observations(
    observations: list[dict[str, Any]],
    *,
    clocks_synchronized: bool,
) -> dict[str, Any]:
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in observations:
        by_source.setdefault(str(row["source"]), []).append(row)
    sources: dict[str, Any] = {}
    unique_by_source: dict[str, dict[tuple[str, str, float, float, float], dict[str, Any]]] = {}
    for source, rows in sorted(by_source.items()):
        seen: set[str] = set()
        duplicates = 0
        out_of_order = 0
        previous_source_time: dict[str, datetime] = {}
        intervals: list[float] = []
        previous_observer: dict[str, float] = {}
        receive_delays: list[float] = []
        unique: dict[tuple[str, str, float, float, float], dict[str, Any]] = {}
        for row in rows:
            event_id = str(row["event_id"])
            if event_id in seen:
                duplicates += 1
            seen.add(event_id)
            source_time = datetime.fromisoformat(str(row["source_timestamp"]))
            previous = previous_source_time.get(str(row["symbol"]))
            if previous is not None and source_time < previous:
                out_of_order += 1
            previous_source_time[str(row["symbol"])] = source_time
            observer = float(row["observer_monotonic_timestamp"])
            previous_value = previous_observer.get(str(row["symbol"]))
            if previous_value is not None:
                intervals.append((observer - previous_value) * 1000)
            previous_observer[str(row["symbol"])] = observer
            if clocks_synchronized:
                observed_wall = datetime.fromisoformat(str(row["observer_wall_timestamp"]))
                receive_delays.append((observed_wall - source_time).total_seconds() * 1000)
            key = (
                str(row["symbol"]), str(row["source_timestamp"]),
                float(row["price"]), float(row["volume"]), float(row["amount"]),
            )
            unique.setdefault(key, row)
        unique_by_source[source] = unique
        gaps = [value for value in intervals if value > 5_000]
        sources[source] = {
            "events": len(rows),
            "unique_events": len(unique),
            "duplicates": duplicates,
            "out_of_order": out_of_order,
            "event_interval_ms": _percentiles(intervals),
            "gap_count_over_5s": len(gaps),
            "gap_duration_ms": sum(gaps),
            "queue_delay_ms": _percentiles([
                float(row["queue_delay_ms"]) for row in rows
            ]),
            "strategy_processing_delay_ms": _percentiles([
                float(row["strategy_processing_delay_ms"]) for row in rows
            ]),
            "source_to_observer_delay_ms": (
                _percentiles(receive_delays) if clocks_synchronized else None
            ),
        }
    source_names = sorted(unique_by_source)
    comparison: dict[str, Any] = {
        "clock_basis": "synchronized_source_clock" if clocks_synchronized else "observer_match_only",
        "matched_events": 0,
        "effective_coverage": 0.0,
    }
    if len(source_names) == 2:
        left, right = source_names
        matched = set(unique_by_source[left]) & set(unique_by_source[right])
        deltas = [
            (
                float(unique_by_source[right][key]["observer_monotonic_timestamp"])
                - float(unique_by_source[left][key]["observer_monotonic_timestamp"])
            ) * 1000
            for key in matched
        ]
        comparison.update({
            "left_source": left,
            "right_source": right,
            "matched_events": len(matched),
            "effective_coverage": (
                len(matched) / max(len(unique_by_source[left]), len(unique_by_source[right]), 1)
            ),
            "left_unmatched_ratio": 1 - len(matched) / max(len(unique_by_source[left]), 1),
            "right_unmatched_ratio": 1 - len(matched) / max(len(unique_by_source[right]), 1),
            "right_minus_left_arrival_ms": _percentiles(deltas),
            "left_faster": sum(value > 0 for value in deltas),
            "right_faster": sum(value < 0 for value in deltas),
            "ties": sum(value == 0 for value in deltas),
        })
    return {"sources": sources, "comparison": comparison}


def run_latency_probe(
    sources: dict[str, Any],
    duration_seconds: float,
    output_dir: Path,
    *,
    clocks_synchronized: bool = False,
    strategy_processor: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Collect both sources on one observer and persist raw JSONL plus a report."""
    stop = threading.Event()
    events: queue.Queue[tuple[str, dict[str, Any], datetime, float]] = queue.Queue(maxsize=100_000)
    observations: list[dict[str, Any]] = []
    errors: dict[str, str] = {}

    def enqueue(source: str, raw: dict[str, Any]) -> None:
        received_monotonic = time.monotonic()
        received_wall = datetime.now(timezone.utc).astimezone(_CN_TZ)
        try:
            events.put_nowait((source, raw, received_wall, received_monotonic))
        except queue.Full:
            errors[source] = "观察队列已满"
            stop.set()

    def consume() -> None:
        while not stop.is_set() or not events.empty():
            try:
                source, raw, received_wall, received_monotonic = events.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                processing_started = time.monotonic()
                row = _normalized_observation(
                    source, raw, received_wall, received_monotonic,
                )
                if row is not None:
                    if strategy_processor is not None:
                        strategy_processor(row)
                    processing_finished = time.monotonic()
                    row["queue_delay_ms"] = (processing_started - received_monotonic) * 1000
                    row["strategy_processing_delay_ms"] = (
                        processing_finished - processing_started
                    ) * 1000
                    observations.append(row)
            except Exception as exc:  # noqa: BLE001
                errors[source] = f"观测处理失败: {exc.__class__.__name__}: {exc}"
                stop.set()
            finally:
                events.task_done()

    consumer = threading.Thread(target=consume, name="tick-latency-consumer", daemon=True)
    consumer.start()
    workers: list[threading.Thread] = []

    def collect_source(name: str, source: Any) -> None:
        try:
            source.collect(lambda raw: enqueue(name, raw), stop)
            if not stop.is_set():
                errors[name] = "行情源在观测结束前提前退出"
                stop.set()
        except Exception as exc:  # noqa: BLE001
            errors[name] = f"{exc.__class__.__name__}: {exc}"
            stop.set()

    started_at = datetime.now(_CN_TZ)
    for name, source in sources.items():
        thread = threading.Thread(
            target=collect_source,
            args=(name, source),
            name=f"tick-latency-{name}",
            daemon=True,
        )
        workers.append(thread)
        thread.start()
    stop.wait(max(0.1, duration_seconds))
    stop.set()
    for name, thread in zip(sources, workers, strict=True):
        thread.join(5)
        if thread.is_alive():
            errors.setdefault(name, "采集线程未在 5 秒内停止")
    consumer.join(5)
    if consumer.is_alive():
        errors.setdefault("consumer", "观测处理线程未在 5 秒内停止")
    ended_at = datetime.now(_CN_TZ)

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "observations.jsonl"
    with raw_path.open("w", encoding="utf-8") as stream:
        for row in observations:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    report = {
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "clocks_synchronized": clocks_synchronized,
        "errors": errors,
        **summarize_observations(
            observations,
            clocks_synchronized=clocks_synchronized,
        ),
    }
    (output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report
