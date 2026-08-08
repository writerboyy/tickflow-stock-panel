"""Small adapters for TickFlow exchange and instrument catalog endpoints."""
from __future__ import annotations

import logging
from collections.abc import Mapping

logger = logging.getLogger(__name__)

DEFAULT_CN_EXCHANGES = ("SH", "SZ", "BJ")
INSTRUMENT_BATCH_SIZE = 1000


def _field(value: object, name: str) -> object:
    if isinstance(value, Mapping):
        return value.get(name)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump().get(name)
        except Exception:  # noqa: BLE001
            pass
    return getattr(value, name, None)


def _record(value: object) -> dict | None:
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump()
            return dict(dumped) if isinstance(dumped, Mapping) else None
        except Exception:  # noqa: BLE001
            return None
    symbol = _field(value, "symbol")
    return {"symbol": symbol} if symbol else None


def list_cn_exchanges(tf: object, fallback: tuple[str, ...] = DEFAULT_CN_EXCHANGES) -> list[str]:
    """Return supported CN exchange codes from ``GET /v1/exchanges``.

    The static fallback keeps older/free SDK servers usable when the catalog
    endpoint is unavailable; callers still fail closed on an empty response.
    """
    exchanges = getattr(tf, "exchanges", None)
    list_exchanges = getattr(exchanges, "list", None)
    if not callable(list_exchanges):
        return list(fallback)
    try:
        rows = list_exchanges() or []
    except Exception as exc:  # noqa: BLE001
        logger.warning("exchanges.list failed, using configured CN exchanges: %s", exc)
        return list(fallback)

    known = set(fallback)
    found: set[str] = set()
    for row in rows:
        code = str(_field(row, "exchange") or "").strip().upper()
        region = str(_field(row, "region") or "").strip().upper()
        if code in known and (not region or region in {"CN", "CHN"}):
            found.add(code)
    return [code for code in fallback if code in found] or list(fallback)


def _records(response: object) -> list[dict]:
    if response is None:
        return []
    if isinstance(response, Mapping):
        if "symbol" in response:
            row = _record(response)
            return [row] if row else []
        response = response.get("data", response.values())
    if isinstance(response, (str, bytes)):
        return []
    rows: list[dict] = []
    try:
        for value in response:
            row = _record(value)
            if row:
                rows.append(row)
    except TypeError:
        return []
    return rows


def fetch_instrument_details(tf: object, symbols: list[str]) -> list[dict]:
    """Fetch instrument metadata via ``instruments.batch`` or single ``get``.

    A failed batch is returned as an empty result so the caller can choose its
    existing exchange-level fallback instead of publishing a partial catalog.
    """
    normalized = list(dict.fromkeys(
        str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
    ))
    if not normalized:
        return []
    instruments = getattr(tf, "instruments", None)
    batch = getattr(instruments, "batch", None)
    if callable(batch):
        rows: list[dict] = []
        batch_failed = False
        for start in range(0, len(normalized), INSTRUMENT_BATCH_SIZE):
            chunk = normalized[start:start + INSTRUMENT_BATCH_SIZE]
            try:
                part = _records(batch(chunk))
            except Exception as exc:  # noqa: BLE001
                logger.warning("instruments.batch failed (%d symbols): %s", len(chunk), exc)
                batch_failed = True
                break
            if not part:
                batch_failed = True
                break
            rows.extend(part)
        if not batch_failed:
            return rows

    get = getattr(instruments, "get", None)
    if callable(get) and len(normalized) == 1:
        try:
            return _records(get(normalized[0]))
        except Exception as exc:  # noqa: BLE001
            logger.warning("instruments.get failed (%s): %s", normalized[0], exc)
    return []
