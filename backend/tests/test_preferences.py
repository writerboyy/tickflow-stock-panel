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
