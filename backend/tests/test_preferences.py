import pytest
from pydantic import ValidationError

from app.api.large_orders import LargeOrderPreferencesIn
from app.services import preferences
from app.services.quote_service import QuoteService


def test_realtime_quotes_default_to_enabled_when_allowed(monkeypatch):
    monkeypatch.setattr(preferences, "load", lambda: {})
    monkeypatch.setattr(QuoteService, "is_realtime_allowed", classmethod(lambda cls: True))

    assert preferences.get_realtime_quotes_enabled() is True


def test_realtime_quotes_default_to_enabled_when_capability_lookup_fails(monkeypatch):
    monkeypatch.setattr(preferences, "load", lambda: {})

    def raise_capability_error(cls):
        raise RuntimeError("unavailable")

    monkeypatch.setattr(
        QuoteService,
        "is_realtime_allowed",
        classmethod(raise_capability_error),
    )

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


def test_qmt_quick_amount_presets_default(monkeypatch):
    monkeypatch.setattr(preferences, "load", lambda: {})

    assert preferences.get_qmt_quick_amount_presets() == [10_000, 20_000, 30_000, 40_000]


def test_qmt_quick_amount_presets_persist(monkeypatch):
    stored = {}
    monkeypatch.setattr(preferences, "load", lambda: dict(stored))
    monkeypatch.setattr(preferences, "save", lambda values: stored.update(values))

    saved = preferences.set_qmt_quick_amount_presets([5_000, 15_000, 25_000, 50_000])

    assert saved == [5_000, 15_000, 25_000, 50_000]
    assert stored["qmt_quick_amount_presets"] == [5_000, 15_000, 25_000, 50_000]
    # 重新读取(模拟重启面板/应用)仍拿到用户值, 而不是内置默认
    assert preferences.get_qmt_quick_amount_presets() == [5_000, 15_000, 25_000, 50_000]


def test_qmt_quick_amount_presets_normalize_invalid_values(monkeypatch):
    stored = {}
    monkeypatch.setattr(preferences, "load", lambda: dict(stored))
    monkeypatch.setattr(preferences, "save", lambda values: stored.update(values))

    # 非法/越界值: 脏值回退到对应位置的默认值, 越界值被 clamp
    saved = preferences.set_qmt_quick_amount_presets(["abc", 50, 999_999_999, 12_345])

    assert saved == [10_000, 100, 10_000_000, 12_345]
    # 不足 4 个时补齐默认档位
    assert preferences.set_qmt_quick_amount_presets([1_000]) == [1_000, 20_000, 30_000, 40_000]
    # 存进去的脏数据读出来同样被规范化, 长度恒为 4
    stored["qmt_quick_amount_presets"] = [None, "x", 3, 4, 5]
    assert preferences.get_qmt_quick_amount_presets() == [10_000, 20_000, 100, 100]


def test_update_qmt_quick_amount_presets_endpoint_calls_setter_once(monkeypatch):
    from app.api import settings

    calls = []
    monkeypatch.setattr(
        "app.services.preferences.set_qmt_quick_amount_presets",
        lambda presets: calls.append(list(presets)) or [5_000, 15_000, 25_000, 35_000],
    )
    req = settings.QmtQuickAmountPresetsIn(presets=[5_000, 15_000, 25_000, 35_000])

    result = settings.update_qmt_quick_amount_presets(req)

    assert calls == [[5_000, 15_000, 25_000, 35_000]]
    assert result == {"qmt_quick_amount_presets": [5_000, 15_000, 25_000, 35_000]}


def test_get_preferences_exposes_qmt_quick_amount_presets(monkeypatch):
    from app.api import settings

    monkeypatch.setattr(
        "app.services.preferences.get_qmt_quick_amount_presets",
        lambda: [5_000, 15_000, 25_000, 35_000],
    )
    payload = settings.get_preferences()
    assert payload["qmt_quick_amount_presets"] == [5_000, 15_000, 25_000, 35_000]


def test_qmt_quick_amount_presets_http_round_trip(tmp_path, monkeypatch):
    """HTTP 往返: 默认档位 → PUT 保存 → 重新 GET 仍是用户值(模拟重启后仍在)。"""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from app.api.settings import router

    def fake_path():
        p = tmp_path / "user_data" / "preferences.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    monkeypatch.setattr(preferences, "_path", fake_path)
    monkeypatch.setattr(preferences, "_cache", None)
    monkeypatch.setattr(preferences, "_cache_sig", None)

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    assert client.get("/api/settings/preferences").json()["qmt_quick_amount_presets"] == [
        10_000, 20_000, 30_000, 40_000,
    ]

    resp = client.put(
        "/api/settings/preferences/qmt-quick-amount-presets",
        json={"presets": [8_000, 18_000, 28_000, 38_000]},
    )
    assert resp.status_code == 200
    assert resp.json() == {"qmt_quick_amount_presets": [8_000, 18_000, 28_000, 38_000]}
    assert client.get("/api/settings/preferences").json()["qmt_quick_amount_presets"] == [
        8_000, 18_000, 28_000, 38_000,
    ]
