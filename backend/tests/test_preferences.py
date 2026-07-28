from app.services import preferences
from app.services.quote_service import QuoteService


def test_realtime_quotes_default_to_enabled_when_allowed(monkeypatch):
    monkeypatch.setattr(preferences, "load", lambda: {})
    monkeypatch.setattr(QuoteService, "is_realtime_allowed", classmethod(lambda cls: True))

    assert preferences.get_realtime_quotes_enabled() is True


def test_realtime_quotes_preserve_explicit_disabled_value(monkeypatch):
    monkeypatch.setattr(preferences, "load", lambda: {"realtime_quotes_enabled": False})
    monkeypatch.setattr(QuoteService, "is_realtime_allowed", classmethod(lambda cls: True))

    assert preferences.get_realtime_quotes_enabled() is False


def test_quote_service_shutdown_preserves_realtime_preference(monkeypatch):
    saved = []
    service = QuoteService()
    service._running = True  # noqa: SLF001
    service._enabled = True  # noqa: SLF001
    monkeypatch.setattr(service, "_save_enabled", saved.append)

    service.shutdown()

    assert saved == []
    assert service._running is False  # noqa: SLF001
    assert service._enabled is False  # noqa: SLF001


def test_quote_service_stop_persists_explicit_disabled_value(monkeypatch):
    saved = []
    service = QuoteService()
    monkeypatch.setattr(service, "_save_enabled", saved.append)

    service.stop()

    assert saved == [False]
