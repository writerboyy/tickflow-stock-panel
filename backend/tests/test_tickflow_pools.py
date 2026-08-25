from types import SimpleNamespace
from unittest.mock import MagicMock

from app.tickflow import pools


def _client(*, batch=None, get=None, quote_rows=None, universes=None):
    return SimpleNamespace(
        universes=SimpleNamespace(
            list=lambda: universes or [{"id": "CSI300", "name": "沪深300"}],
            batch=batch,
            get=get,
        ),
        quotes=SimpleNamespace(
            get_by_universes=MagicMock(return_value=quote_rows or []),
        ),
    )


def test_pool_uses_universe_batch_details_before_quote_pool(monkeypatch):
    quotes = MagicMock(return_value=[])
    tf = _client(
        batch=MagicMock(return_value={"CSI300": {"symbols": ["600000.SH", "600000.SH", "000001.SZ"]}}),
        quote_rows=[],
    )
    tf.quotes.get_by_universes = quotes
    monkeypatch.setattr(pools, "get_client", lambda: tf)

    assert pools._fetch_pool("CSI300") == ["600000.SH", "000001.SZ"]  # noqa: SLF001
    tf.universes.batch.assert_called_once_with(["CSI300"])
    quotes.assert_not_called()


def test_pool_falls_back_to_universe_get_when_batch_is_unavailable(monkeypatch):
    get = MagicMock(return_value=SimpleNamespace(symbols=["600519.SH"]))
    tf = _client(batch=None, get=get)
    monkeypatch.setattr(pools, "get_client", lambda: tf)

    assert pools._fetch_pool("CSI300") == ["600519.SH"]  # noqa: SLF001
    get.assert_called_once_with("CSI300")


def test_pool_falls_back_to_quote_pool_for_old_sdk(monkeypatch):
    tf = _client(batch=None, get=None, quote_rows=[{"symbol": "600036.SH"}])
    monkeypatch.setattr(pools, "get_client", lambda: tf)

    assert pools._fetch_pool("CSI300") == ["600036.SH"]  # noqa: SLF001
    tf.quotes.get_by_universes.assert_called_once_with(["CSI300"], as_dataframe=True)
