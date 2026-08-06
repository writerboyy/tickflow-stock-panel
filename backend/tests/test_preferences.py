import pytest
from pydantic import ValidationError

from app.api.settings import LargeOrderPreferencesIn
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


def test_large_order_limit_gap_defaults_and_persists_as_decimal(monkeypatch):
    stored = {}
    monkeypatch.setattr(preferences, "load", lambda: dict(stored))
    monkeypatch.setattr(preferences, "save", lambda values: stored.update(values))

    defaults = preferences.get_large_orders_preferences()
    assert defaults["min_limit_up_gap_pct"] == 0.02
    assert defaults["version"] == "large_orders_v2"

    updated = preferences.set_large_orders_preferences({"min_limit_up_gap_pct": 0.035})
    assert stored["large_orders_min_limit_up_gap_pct"] == 0.035
    assert updated["min_limit_up_gap_pct"] == 0.035


def test_large_order_market_segments_default_and_persist(monkeypatch):
    stored = {}
    monkeypatch.setattr(preferences, "load", lambda: dict(stored))
    monkeypatch.setattr(preferences, "save", lambda values: stored.update(values))

    defaults = preferences.get_large_orders_preferences()
    assert defaults["market_segments"] == ["main", "star", "chinext"]
    assert defaults["exclude_bse"] is True
    assert defaults["exclude_st"] is True

    updated = preferences.set_large_orders_preferences({
        "market_segments": ["main", "star", "chinext", "bse", "st"],
    })
    assert updated["market_segments"] == ["main", "star", "chinext", "bse", "st"]
    assert updated["exclude_bse"] is False
    assert updated["exclude_st"] is False
    assert stored["large_orders_market_segments"] == ["main", "star", "chinext", "bse", "st"]


def test_large_order_market_segments_migrate_legacy_exclusions(monkeypatch):
    stored = {"large_orders_exclude_bse": False, "large_orders_exclude_st": True}
    monkeypatch.setattr(preferences, "load", lambda: dict(stored))
    monkeypatch.setattr(preferences, "save", lambda values: stored.update(values))

    current = preferences.get_large_orders_preferences()

    assert current["market_segments"] == ["main", "star", "chinext", "bse"]
    updated = preferences.set_large_orders_preferences({"exclude_st": False})
    assert updated["market_segments"] == ["main", "star", "chinext", "bse", "st"]
    assert stored["large_orders_market_segments"] == ["main", "star", "chinext", "bse", "st"]


def test_large_order_market_segments_reject_unknown_value():
    with pytest.raises(ValidationError):
        LargeOrderPreferencesIn(market_segments=["main", "unknown"])
