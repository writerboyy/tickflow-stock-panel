from datetime import date

import pytest

from app.services.limit_board_scoring import (
    premium_gene_detail,
    rotation_detail,
    sector_detail,
    technical_detail,
)


def _rotation() -> dict:
    dates = [
        "2026-08-17",
        "2026-08-14",
        "2026-08-13",
        "2026-08-12",
        "2026-08-11",
        "2026-08-10",
    ]
    changes = [0.20, 0.03, 0.02, 0.01, 0.0, -0.01]
    ranks = [1, 1, 2, 3, 4, 5]
    columns = {}
    for day, change, rank in zip(dates, changes, ranks, strict=True):
        rows = [[f"板块{index}", 0.0] for index in range(1, 11)]
        rows[rank - 1] = ["人工智能", change]
        columns[day] = rows
    return {"dates": dates, "columns": columns, "concept_count": 10}


def _rotation_series(changes: list[float], ranks: list[int]) -> dict:
    dates = [f"2026-08-{day:02d}" for day in (10, 11, 12, 13, 14)]
    columns = {}
    for trading_date, change, rank in zip(dates, changes, ranks, strict=True):
        rows = [[f"板块{index}", 0.0] for index in range(1, 11)]
        rows[rank - 1] = ["人工智能", change]
        columns[trading_date] = rows
    return {"dates": dates, "columns": columns, "concept_count": 10}


def test_premium_gene_score_uses_sample_confidence():
    detail = premium_gene_detail({
        "as_of": "2026-08-14",
        "window_days": 200,
        "limit_up_count": 12,
        "premium_5_count": 5,
        "next_day_observation_count": 10,
        "next_day_red_rate": 0.8,
        "first_board_attempt_count": 10,
        "first_board_sealed_count": 8,
        "first_board_seal_rate": 0.8,
        "first_board_broken_rate": 0.2,
        "consecutive_rate": 0.5,
    })

    assert detail is not None
    assert detail["score"] == pytest.approx(21.9)
    assert detail["premium_5_rate"] == pytest.approx(0.5)
    assert detail["components"]["consecutive"] == pytest.approx(2.0)


def test_technical_score_combines_all_configured_indicators():
    detail = technical_detail({
        "close": 12.0,
        "ma5": 11.0,
        "ma10": 10.0,
        "ma20": 9.0,
        "ma60": 8.0,
        "momentum_5d": 0.10,
        "momentum_20d": 0.30,
        "vol_ratio_5d": 2.5,
        "macd_dif": 0.3,
        "macd_dea": 0.2,
        "macd_hist": 0.1,
        "rsi_14": 70.0,
    })

    assert detail is not None
    assert detail["score"] == pytest.approx(20.0)
    assert detail["components"] == {
        "trend": 7.0,
        "momentum": 5.0,
        "volume": 3.0,
        "macd": 3.0,
        "rsi": 2.0,
    }


def test_rotation_uses_five_completed_trading_days_and_marks_rising():
    detail = rotation_detail(_rotation(), "人工智能", date(2026, 8, 17))

    assert detail is not None
    assert [row["date"] for row in detail["days"]] == [
        "2026-08-10",
        "2026-08-11",
        "2026-08-12",
        "2026-08-13",
        "2026-08-14",
    ]
    assert detail["days"][-1]["change_pct"] == pytest.approx(0.03)
    assert detail["trend_slope"] == pytest.approx(0.01)
    assert detail["rotation_label"] == "上升"
    assert detail["score"] == pytest.approx(16.22)


def test_rotation_sorts_dates_before_selecting_latest_five_completed_days():
    rotation = _rotation()
    rotation["dates"] = [
        "2026-08-11", "2026-08-17", "2026-08-10",
        "2026-08-14", "2026-08-12", "2026-08-13",
    ]

    detail = rotation_detail(rotation, "人工智能", date(2026, 8, 17))

    assert detail is not None
    assert [row["date"] for row in detail["days"]] == [
        "2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13", "2026-08-14",
    ]


@pytest.mark.parametrize(
    ("changes", "ranks", "expected"),
    [
        ([0.01, 0.01, 0.00, 0.01, 0.02], [1, 2, 5, 1, 2], "主线"),
        ([-0.02, -0.01, 0.00, 0.01, 0.02], [8, 7, 6, 4, 2], "上升"),
        ([0.02, 0.01, 0.00, -0.01, -0.02], [2, 4, 6, 7, 8], "退潮"),
        ([0.00, 0.01, 0.00, -0.01, 0.00], [5, 5, 5, 5, 5], "震荡"),
    ],
)
def test_rotation_labels_cover_all_defined_states(changes, ranks, expected):
    detail = rotation_detail(
        _rotation_series(changes, ranks), "人工智能", date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["rotation_label"] == expected


def test_sector_score_recognizes_co_leader_by_price_change():
    stock_rows = {
        "A": {"symbol": "A", "name": "候选", "change_pct": 0.10, "amount": 100},
        "B": {"symbol": "B", "name": "代表龙头", "change_pct": 0.0995, "amount": 200},
        "C": {"symbol": "C", "change_pct": 0.095, "amount": 100},
        "D": {"symbol": "D", "change_pct": 0.04, "amount": 100},
        "E": {"symbol": "E", "change_pct": 0.01, "amount": 100},
        "F": {"symbol": "F", "change_pct": -0.01, "amount": 100},
    }
    detail = sector_detail(
        symbol="A",
        target={"kind": "concept", "name": "人工智能"},
        snapshot={
            "valid": True,
            "change_pct": 0.02,
            "coverage_ratio": 1.0,
            "up_count": 5,
            "down_count": 1,
            "valid_count": 6,
            "total_count": 6,
        },
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols=set(stock_rows),
        today=date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["leader"]["symbol"] == "B"
    assert detail["stock_rank"] == 1
    assert detail["leadership"] == "leader"
    assert detail["is_sector_leader"] is True
    assert detail["current_components"]["leadership"] == 10.0
    assert detail["current_score"] == pytest.approx(25.95)
    assert detail["rotation_score"] == pytest.approx(16.22)
    assert detail["score"] == pytest.approx(42.17)


def test_sector_score_marks_close_third_place_as_front():
    stock_rows = {
        "A": {"symbol": "A", "change_pct": 0.10, "amount": 300},
        "B": {"symbol": "B", "change_pct": 0.099, "amount": 200},
        "C": {"symbol": "C", "change_pct": 0.095, "amount": 100},
        "D": {"symbol": "D", "change_pct": 0.04, "amount": 100},
        "E": {"symbol": "E", "change_pct": 0.01, "amount": 100},
    }
    detail = sector_detail(
        symbol="C",
        target={"kind": "concept", "name": "人工智能"},
        snapshot={
            "valid": True,
            "change_pct": 0.02,
            "coverage_ratio": 1.0,
            "up_count": 5,
            "down_count": 0,
            "valid_count": 5,
            "total_count": 5,
        },
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols=set(stock_rows),
        today=date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["stock_rank"] == 3
    assert detail["leadership"] == "front"
    assert detail["current_components"]["leadership"] == 5.0


def test_sector_breadth_uses_valid_members_and_accepts_eighty_percent_coverage():
    stock_rows = {
        "A": {"symbol": "A", "change_pct": 0.03, "amount": 100},
        "B": {"symbol": "B", "change_pct": 0.02, "amount": 100},
        "C": {"symbol": "C", "change_pct": 0.00, "amount": 100},
        "D": {"symbol": "D", "change_pct": -0.01, "amount": 100},
    }
    detail = sector_detail(
        symbol="A",
        target={"kind": "concept", "name": "人工智能"},
        snapshot={
            "valid": True,
            "change_pct": 0.01,
            "coverage_ratio": 0.8,
            "up_count": 2,
            "down_count": 1,
            "valid_count": 4,
            "total_count": 5,
        },
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols={"A", "B", "C", "D", "E"},
        today=date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["up_ratio"] == pytest.approx(0.5)
    assert detail["current_components"]["breadth"] == pytest.approx(2.5)
    assert detail["valid_count"] == 4
    assert detail["member_count"] == 5


def test_sector_score_handles_an_all_declining_sector():
    stock_rows = {
        symbol: {
            "symbol": symbol,
            "change_pct": change,
            "amount": 100,
        }
        for symbol, change in zip("ABCDE", [-0.01, -0.02, -0.03, -0.04, -0.05], strict=True)
    }
    detail = sector_detail(
        symbol="A",
        target={"kind": "concept", "name": "人工智能"},
        snapshot={
            "valid": True,
            "change_pct": -0.02,
            "coverage_ratio": 1.0,
            "up_count": 0,
            "down_count": 5,
            "valid_count": 5,
            "total_count": 5,
        },
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols=set(stock_rows),
        today=date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["leader"]["symbol"] == "A"
    assert detail["is_sector_leader"] is False
    assert detail["current_components"]["leader_change"] == 0.0
