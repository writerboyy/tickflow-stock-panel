from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from app.services.position_risk_context import (
    PositionRiskContextService,
    correlation_snapshot,
    emotion_phase,
)


def test_emotion_phase_maps_all_market_cycle_stages():
    assert emotion_phase([]) == "数据不足"
    assert emotion_phase([20, 21, 22]) == "冰点"
    assert emotion_phase([20, 24, 29]) == "修复"
    assert emotion_phase([50, 52, 54]) == "启动"
    assert emotion_phase([55, 60, 65]) == "发酵"
    assert emotion_phase([70, 74, 78]) == "高潮"
    assert emotion_phase([70, 65, 60]) == "分化"
    assert emotion_phase([44, 40, 35]) == "退潮"


def _correlation_history(days: int) -> pl.DataFrame:
    rows = []
    start = date(2026, 7, 1)
    members = ["HOLD", "A", "B", "C", "D", "LEADER"]
    for index in range(days):
        day = start + timedelta(days=index)
        base = (index - days / 2) / 100
        for offset, symbol in enumerate(members):
            rows.append({
                "symbol": symbol,
                "date": day,
                "change_pct": base + offset / 10_000,
            })
    return pl.DataFrame(rows)


def test_correlation_snapshot_requires_ten_overlapping_trading_days():
    members = {"HOLD", "A", "B", "C", "D", "LEADER"}
    insufficient = correlation_snapshot(_correlation_history(9), "HOLD", members, "LEADER")
    assert insufficient == {"sector": None, "leader": None, "samples": 9, "leader_samples": 9}

    result = correlation_snapshot(_correlation_history(20), "HOLD", members, "LEADER")
    assert result["samples"] == 20
    assert result["leader_samples"] == 20
    assert result["sector"] == pytest.approx(1.0)
    assert result["leader"] == pytest.approx(1.0)


class _SectorService:
    def __init__(self) -> None:
        self.concept = {"key": "concept-ai", "kind": "concept", "name": "人工智能"}
        self.industry = {"key": "industry-tech", "kind": "industry", "name": "电子 / 计算机", "level": 2}

    def targets_for_symbol(self, _symbol, *, kind=None, industry_level=None):
        if kind == "concept":
            return [self.concept]
        if kind == "industry" and industry_level == 2:
            return [self.industry]
        return []

    def member_symbols(self, _target_key):
        return {"HOLD", "A", "B", "C", "D", "LEADER"}

    def build_snapshots(self, _stock_df, _index_df, targets, _windows, *, now):
        return {
            target["key"]: {
                **target,
                "valid": True,
                "change_pct": 0.01,
                "coverage_ratio": 1.0,
                "leader": {"symbol": "LEADER", "name": "龙头", "change_pct": 0.05},
            }
            for target in targets
        }


class _Repo:
    def __init__(self, root: Path) -> None:
        self.store = SimpleNamespace(data_dir=root)
        self.history = _correlation_history(20)

    def enriched_latest_date(self):
        return self.history["date"].max()

    def get_enriched_range(self, _start, _end, symbols=None, columns=None):
        frame = self.history
        if symbols:
            frame = frame.filter(pl.col("symbol").is_in(symbols))
        return frame.select(columns) if columns else frame


class _Quotes:
    def get_enriched_today(self):
        return pl.DataFrame({
            "symbol": ["HOLD", "A", "B", "C", "D", "LEADER"],
            "name": ["持仓", "A", "B", "C", "D", "龙头"],
            "change_pct": [0.02, 0.01, 0.0, 0.03, -0.01, 0.05],
        }), date(2026, 8, 17)

    def get_index_quotes(self):
        return pl.DataFrame()


def test_context_prefers_valid_concept_and_strictly_gates_missing_auction(monkeypatch, tmp_path: Path):
    sector_service = _SectorService()
    service = PositionRiskContextService(
        _Repo(tmp_path),
        _Quotes(),
        SimpleNamespace(sector_monitor_service=sector_service, depth_service=None),
    )
    monkeypatch.setattr(
        "app.services.position_risk_context.build_market_overview",
        lambda *_args, **_kwargs: {
            "as_of": "2026-08-17",
            "indices": [{"symbol": "000001.SH"}],
            "breadth": {"total": 5000},
            "emotion": {"score": 60, "label": "偏暖"},
        },
    )
    rotation = {
        "dates": ["2026-08-17", "2026-08-14", "2026-08-13", "2026-08-12", "2026-08-11"],
        "columns": {
            day: [["人工智能", value], ["计算机", value / 2]]
            for day, value in zip(
                ["2026-08-17", "2026-08-14", "2026-08-13", "2026-08-12", "2026-08-11"],
                [0.03, 0.02, 0.01, 0.01, -0.01],
                strict=True,
            )
        },
    }
    monkeypatch.setattr(
        "app.services.position_risk_context.rps_rotation.build_rps_rotation",
        lambda *_args, **_kwargs: rotation,
    )
    monkeypatch.setattr(
        "app.services.position_risk_context.regime_builder.load_regime_history",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    features = {
        "HOLD": {
            "auction": {"available": False},
            "opening_five_minute": {"available": True, "volume": 10_000},
            "relative_volume": 1.2,
            "buy_ratio": 0.60,
            "sell_ratio": 0.40,
            "flow_samples": 10,
        },
    }
    now = datetime(2026, 8, 17, 9, 36)
    first = service.build(
        {"HOLD"}, features, {"HOLD": {"change_pct": 0.02}}, {"HOLD": {}}, now,
    )["HOLD"]
    assert first["sector_kind"] == "concept"
    assert first["sector_name"] == "人工智能"
    assert first["sector_five_day_change_pct"] == pytest.approx(0.06)
    assert first["sector_yesterday_change_pct"] == pytest.approx(0.02)
    assert first["state"] == "unavailable"
    assert first["missing"] == ["auction"]

    features["HOLD"]["auction"] = {"available": True, "price": 10.0, "volume": 1000}
    second = service.build(
        {"HOLD"}, features, {"HOLD": {"change_pct": 0.02}}, {"HOLD": {}}, now,
    )["HOLD"]
    assert second["state"] == "supportive"
    assert second["gate_open"] is True


def test_context_falls_back_to_industry_when_all_concepts_are_invalid(monkeypatch, tmp_path: Path):
    class FallbackSectorService(_SectorService):
        def build_snapshots(self, _stock_df, _index_df, targets, _windows, *, now):
            snapshots = super().build_snapshots(_stock_df, _index_df, targets, _windows, now=now)
            snapshots["concept-ai"].update(valid=False, coverage_ratio=0.5)
            return snapshots

    sector_service = FallbackSectorService()
    service = PositionRiskContextService(
        _Repo(tmp_path),
        _Quotes(),
        SimpleNamespace(sector_monitor_service=sector_service, depth_service=None),
    )
    monkeypatch.setattr(
        "app.services.position_risk_context.build_market_overview",
        lambda *_args, **_kwargs: {
            "as_of": "2026-08-17",
            "indices": [{"symbol": "000001.SH"}],
            "breadth": {"total": 5000},
            "emotion": {"score": 60, "label": "偏暖"},
        },
    )
    monkeypatch.setattr(
        "app.services.position_risk_context.rps_rotation.build_rps_rotation",
        lambda _repo, _days, kind, _level: {
            "dates": ["2026-08-17", "2026-08-14", "2026-08-13", "2026-08-12", "2026-08-11"],
            "columns": {
                day: [["人工智能" if kind == "concept" else "计算机", value]]
                for day, value in zip(
                    ["2026-08-17", "2026-08-14", "2026-08-13", "2026-08-12", "2026-08-11"],
                    [0.03, 0.02, 0.01, 0.01, -0.01],
                    strict=True,
                )
            },
        },
    )
    monkeypatch.setattr(
        "app.services.position_risk_context.regime_builder.load_regime_history",
        lambda *_args, **_kwargs: pl.DataFrame(),
    )
    feature = {
        "auction": {"available": True, "price": 10.0, "volume": 1000},
        "opening_five_minute": {"available": True, "volume": 10_000},
        "relative_volume": 1.2,
        "buy_ratio": 0.60,
        "sell_ratio": 0.40,
        "flow_samples": 10,
    }
    context = service.build(
        {"HOLD"}, {"HOLD": feature}, {"HOLD": {"change_pct": 0.02}}, {"HOLD": {}},
        datetime(2026, 8, 17, 9, 36),
    )["HOLD"]
    assert context["sector_kind"] == "industry"
    assert context["sector_name"] == "电子 / 计算机"
