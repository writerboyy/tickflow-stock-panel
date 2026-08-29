from datetime import date

import pytest

from app.services.limit_board_scoring import (
    comprehensive_score,
    intraday_flow_detail,
    premium_gene_detail,
    rotation_detail,
    rotation_only_detail,
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


def test_premium_gene_score_uses_three_gate_metrics():
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
    assert detail["score"] == pytest.approx(8.67)
    assert detail["max_score"] == pytest.approx(10.0)
    assert detail["premium_5_rate"] == pytest.approx(0.5)
    assert detail["components"]["first_board_broken"] == pytest.approx(2.67)
    assert detail["passed"] is True
    assert detail["criteria"]["limit_up_count"]["passed"] is True
    assert detail["criteria"]["next_day_red_rate"]["passed"] is True
    assert detail["criteria"]["first_board_broken_rate"]["passed"] is True


def test_premium_gene_score_accepts_live_gate_snapshot_without_optional_counts():
    detail = premium_gene_detail({
        "limit_up_count": 4,
        "next_day_red_rate": 0.80,
        "first_board_broken_rate": 0.75,
    })

    assert detail is not None
    assert detail["score"] == pytest.approx(4.61)
    assert detail["passed"] is True
    assert detail["criteria"]["limit_up_count"]["passed"] is True


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


def test_intraday_flow_score_makes_persistent_underwater_outflow_lowest_weighted_signal():
    rising = intraday_flow_detail({
        "available": True,
        "session_vwap": 10.30,
        "closed_bars": [
            {"open": 10.0, "close": close, "amount": amount}
            for close, amount in zip(
                (10.1, 10.2, 10.3, 10.4, 10.5),
                (1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000),
                strict=True,
            )
        ],
    }, previous_close=10.0, external_flow={"buy_ratio": 1.0, "sell_ratio": 0.0})
    underwater = intraday_flow_detail({
        "available": True,
        "session_vwap": 9.70,
        "closed_bars": [
            {"open": 10.0, "close": close, "amount": 1_000_000}
            for close in (9.9, 9.8, 9.7, 9.6, 9.5)
        ],
    }, previous_close=10.0, external_flow={"buy_ratio": 0.0, "sell_ratio": 1.0})

    assert rising is not None and underwater is not None
    assert rising["max_score"] == 50.0
    assert rising["score"] > 40.0
    assert rising["trend_score"] > 20.0
    assert rising["trend_state"] == "strong"
    assert rising["price_volume_rising"] is True
    assert rising["capital_score"] == pytest.approx(25.0)
    assert rising["capital_source_label"] == "实时主动大单"
    assert rising["components"]["price_volume"] == pytest.approx(5.0)
    assert underwater["score"] < 5.0
    assert underwater["trend_state"] == "weak"
    assert underwater["price_volume_rising"] is False
    assert underwater["flow_state"] == "outflow"
    assert underwater["underwater_ratio"] == pytest.approx(1.0)
    assert underwater["net_flow_ratio"] == pytest.approx(-1.0)
    assert underwater["outflow_streak"] == 4


def test_intraday_flow_prefers_realtime_large_order_ratios_and_requires_live_minutes():
    detail = intraday_flow_detail({
        "available": True,
        "session_vwap": 10.0,
        "closed_bars": [{"open": 10.0, "close": 10.1, "amount": 1_000_000}],
    }, previous_close=10.0, external_flow={"buy_ratio": 0.25, "sell_ratio": 0.75})

    assert detail is not None
    assert detail["flow_source"] == "large_order"
    assert detail["net_flow_ratio"] == pytest.approx(-0.5)
    assert intraday_flow_detail({"available": False}, previous_close=10.0) is None


def test_intraday_flow_scores_kaipanla_main_net_speed_without_claiming_active_trades():
    detail = intraday_flow_detail({
        "available": True,
        "session_vwap": 10.0,
        "closed_bars": [
            {"open": 10.0, "close": 10.1, "amount": 1_000_000},
            {"open": 10.1, "close": 10.2, "amount": 1_000_000},
        ],
    }, previous_close=10.0, external_flow={
        "source": "kaipanla_net_flow",
        "data_quality": "net_flow",
        "buy_ratio": 0.0,
        "sell_ratio": 1.0,
        "net_flow_amount": 8_000_000,
        "net_flow_delta": 1_000_000,
        "net_flow_speed": 200_000,
        "net_flow_window_minutes": 5,
        "net_flow_as_of": "2026-08-18T10:00:00+08:00",
    })

    assert detail is not None
    assert detail["capital_available"] is True
    assert detail["flow_source"] == "kaipanla_net_flow"
    assert detail["flow_metric"] == "main_net_speed"
    assert detail["capital_source_label"] == "开盘啦主力净额涨速"
    assert detail["buy_ratio"] is None
    assert detail["sell_ratio"] is None
    assert detail["net_flow_speed_ratio"] == pytest.approx(0.2)
    assert detail["net_flow_ratio"] == pytest.approx(0.2)
    assert detail["flow_state"] == "inflow"


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
    assert detail["institutional_score"] == pytest.approx(32.79, abs=0.01)
    assert detail["institutional_max_score"] == pytest.approx(43.0)
    assert detail["momentum_20d_percentile"] is None


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


def test_sector_rank_normalizes_growth_board_progress():
    """主板涨停（进度 1.0）必须排在创业板 +15%（进度 0.75）前面。"""
    stock_rows = {
        "600000.SH": {"symbol": "600000.SH", "name": "主板票", "change_pct": 0.10, "amount": 100},
        "300001.SZ": {"symbol": "300001.SZ", "name": "创业票", "change_pct": 0.15, "amount": 200},
        "600002.SH": {"symbol": "600002.SH", "change_pct": 0.05, "amount": 100},
        "600003.SH": {"symbol": "600003.SH", "change_pct": 0.02, "amount": 100},
        "600004.SH": {"symbol": "600004.SH", "change_pct": -0.01, "amount": 100},
    }
    snapshot = {
        "valid": True,
        "change_pct": 0.02,
        "coverage_ratio": 1.0,
        "up_count": 4,
        "down_count": 1,
        "valid_count": 5,
        "total_count": 5,
    }
    main_board = sector_detail(
        symbol="600000.SH",
        target={"kind": "concept", "name": "人工智能"},
        snapshot=snapshot,
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols=set(stock_rows),
        today=date(2026, 8, 17),
    )

    assert main_board is not None
    assert main_board["stock_rank"] == 1
    assert main_board["leader"]["symbol"] == "600000.SH"
    assert main_board["leadership"] == "leader"
    assert main_board["stock_progress_pct"] == pytest.approx(1.0)
    assert main_board["rank_method"] == "intraday_progress_normalized"

    growth = sector_detail(
        symbol="300001.SZ",
        target={"kind": "concept", "name": "人工智能"},
        snapshot=snapshot,
        rotation=_rotation(),
        stock_rows=stock_rows,
        member_symbols=set(stock_rows),
        today=date(2026, 8, 17),
    )

    assert growth is not None
    assert growth["stock_rank"] == 2
    assert growth["stock_progress_pct"] == pytest.approx(0.75)
    assert growth["leadership"] == "front"
    # 与龙头的进度差：1.0 - 0.75 = 0.25
    assert growth["leader_gap_pct"] == pytest.approx(0.25)


def _sealed_pullup_intraday() -> dict:
    """昨收 10.0，启动后 4 分钟放量一波拉到涨停 11.0 并封死。"""
    closes = [10.05, 10.10, 10.32, 10.55, 10.80, 11.00]
    amounts = [1_000_000, 1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000]
    return {
        "available": True,
        "session_bars": [
            {"open": close, "high": close, "low": close, "close": close, "amount": amount}
            for close, amount in zip(closes, amounts, strict=True)
        ],
    }


def test_intraday_flow_reports_pullup_metrics_and_sealed_state():
    detail = intraday_flow_detail(
        _sealed_pullup_intraday(), previous_close=10.0, limit_up=11.0,
    )

    assert detail is not None
    assert detail["limit_up"] == pytest.approx(11.0)
    assert detail["touch_index"] == 5
    assert detail["sealed_now"] is True
    assert detail["pull_up_start_index"] == 2
    assert detail["pull_up_minutes"] == 4
    assert detail["pull_up_max_drawdown"] == pytest.approx(0.0)
    assert detail["pull_up_gain"] == pytest.approx(11.0 / 10.32 - 1.0)
    # 封板前量能：最近 3 根均值 4M，之前 3 根均值约 1.33M → +200%
    assert detail["pre_seal_amount_growth"] == pytest.approx(2.0)
    assert detail["day_open"] == pytest.approx(10.05)
    assert detail["day_high"] == pytest.approx(11.0)
    assert detail["day_low"] == pytest.approx(10.05)
    assert detail["last_price"] == pytest.approx(11.0)


def test_intraday_flow_pullup_drawdown_and_unsealed_volume_window():
    intraday = {
        "available": True,
        "session_bars": [
            {"open": close, "high": close, "low": close, "close": close, "amount": 1_000_000}
            for close in (10.0, 10.3, 10.6, 10.4, 10.7)
        ],
    }
    detail = intraday_flow_detail(intraday, previous_close=10.0, limit_up=11.0)

    assert detail is not None
    assert detail["touch_index"] is None
    assert detail["sealed_now"] is False
    assert detail["pull_up_minutes"] == 4
    assert detail["pull_up_max_drawdown"] == pytest.approx((10.6 - 10.4) / 10.6)
    # 未触板：量能窗口覆盖全部 bar，均额持平 → 0
    assert detail["pre_seal_amount_growth"] == pytest.approx(0.0)


def test_intraday_flow_without_limit_price_keeps_pullup_fields_empty():
    detail = intraday_flow_detail(
        {
            "available": True,
            "session_bars": [
                {"open": 10.0, "close": 10.5, "amount": 1_000_000},
                {"open": 10.5, "close": 10.8, "amount": 1_000_000},
            ],
        },
        previous_close=10.0,
    )

    assert detail is not None
    assert detail["limit_up"] is None
    assert detail["touch_index"] is None
    assert detail["sealed_now"] is False
    # 启动点不依赖涨停价，仍然可算
    assert detail["pull_up_minutes"] == 2


def test_comprehensive_score_marks_all_missing_data_as_unavailable():
    result = comprehensive_score({})

    assert result["comprehensive_score"] == 0.0
    assert result["grade"] == "D"
    assert result["grade_label"] == "数据不足"
    assert result["data_completeness"] == 0.0
    for dimension in result["dimensions"].values():
        assert dimension["max_score"] == 0.0
        assert dimension["components"] == {}
        assert dimension["unavailable_components"]
    # 缺数据不允许产生任何警示/优势（旧版会报「板块涨幅不大，安全」等假信号）
    assert result["warnings"] == []
    assert result["strengths"] == []


def test_comprehensive_score_gene_only_scales_and_caps_grade():
    result = comprehensive_score({
        "premium_gene": {
            "next_day_red_rate": 0.5,
            "first_board_seal_rate": 1.0,
        },
    })

    history = result["dimensions"]["history"]
    assert history["score"] == pytest.approx(18.0)
    assert history["max_score"] == pytest.approx(24.0)
    assert history["full_max_score"] == pytest.approx(30.0)
    assert history["unavailable_components"] == ["consecutive_ability"]
    # 18/24 = 75 → A 档，但数据完整度 0.24 → 封顶 B
    assert result["comprehensive_score"] == pytest.approx(75.0)
    assert result["grade"] == "B"
    assert result["data_completeness"] == pytest.approx(0.24)
    # 真实数据驱动的警示保留；缺项的假信号全部消失
    assert "次日收红率偏低" in result["warnings"]
    assert "板块涨幅不大，安全" not in result["strengths"]
    assert "主力资金流出" not in result["warnings"]
    assert "均线压制" not in result["warnings"]
    assert "非板块龙头" not in result["warnings"]


def test_comprehensive_score_sector_overheat_uses_real_data_only():
    sector = {
        "rotation_available": True,
        "rotation_score": 16.22,
        "rotation_label": "主线",
        "five_day_change_pct": 0.18,
        "days": [{"change_pct": 0.02} for _ in range(5)],
        "realtime_rank": 1,
        "realtime_rank_count": 50,
        "change_pct": 0.05,
        "up_ratio": 0.9,
        "leadership": "leader",
        "is_sector_leader": True,
        # 归一化后的涨停进度差距：0.12 = 领先第二名 12% 涨停进度
        "leader_gap_pct": 0.12,
        "stock_rank": 1,
    }
    result = comprehensive_score({"sector": sector})

    sentiment = result["dimensions"]["sentiment"]
    assert sentiment["max_score"] == pytest.approx(30.0)
    assert sentiment["components"]["sector_pattern"] == pytest.approx(12.2, abs=0.05)
    # 过热：5日涨幅 18% → 1 分；连涨 5 天 → 1 分；排名 1/50 顶部 → 0 分
    assert sentiment["components"]["overheat_risk"] == pytest.approx(2.0)
    assert sentiment["components"]["sector_current"] == pytest.approx(5.0)
    health = result["dimensions"]["health"]
    assert health["components"]["sector_position"] == pytest.approx(15.0)
    assert "板块绝对龙头" in result["strengths"]
    assert "主线板块" in result["strengths"]
    assert "板块涨幅过大，注意回调风险" in result["warnings"]


def test_comprehensive_score_sealed_stock_scores_pullup_not_capital():
    flow = intraday_flow_detail(
        _sealed_pullup_intraday(), previous_close=10.0, limit_up=11.0,
        external_flow={"buy_ratio": 0.0, "sell_ratio": 1.0},
    )
    result = comprehensive_score({"intraday_flow": flow})

    health = result["dimensions"]["health"]
    # 一波流放量拉升：用时 4 + 流畅 3 + 封板前量能 4 = 11/11
    assert health["components"]["pullup_form"] == pytest.approx(11.0)
    # 封板后资金流向失真 → 数据不足，不再拿「主力流出」扣分
    assert "capital_flow" in health["unavailable_components"]
    # 日K：实体 1.5 + 影线 0.3（无均线数据 → max 2）
    assert health["components"]["daily_k_pattern"] == pytest.approx(1.8)
    assert health["max_score"] == pytest.approx(13.0)
    assert "一波流拉升" in result["strengths"]
    assert "主力资金流出" not in result["warnings"]


def test_comprehensive_score_flags_choppy_pullup():
    intraday = {
        "available": True,
        "session_bars": [
            {"open": close, "high": close, "low": close, "close": close, "amount": 1_000_000}
            for close in (10.0, 10.3, 10.6, 10.0, 10.7)
        ],
    }
    flow = intraday_flow_detail(intraday, previous_close=10.0, limit_up=11.0)
    result = comprehensive_score({"intraday_flow": flow})

    health = result["dimensions"]["health"]
    # 最大回撤 (10.6-10.0)/10.6 ≈ 5.7% > 5% → 流畅度 0 分
    assert flow["pull_up_max_drawdown"] == pytest.approx(0.0566, abs=0.001)
    assert health["components"]["pullup_form"] < 11.0
    assert "拉升反复，分歧大" in result["warnings"]


def test_rotation_only_detail_scores_daily_rotation_and_gates_realtime_parts():
    detail = rotation_only_detail(
        {"kind": "concept", "name": "人工智能"}, _rotation(), date(2026, 8, 17),
    )

    assert detail is not None
    assert detail["rotation_available"] is True
    assert detail["realtime_available"] is False
    assert detail["rotation_score"] == pytest.approx(16.22)
    assert detail["max_score"] == pytest.approx(50.0)
    assert detail["days"]
    assert detail["five_day_change_pct"] is not None
    # 实时类组件全部为 None，交给综合评分的数据门控
    assert detail["change_pct"] is None
    assert detail["up_ratio"] is None
    assert detail["leadership"] is None
    assert detail["stock_rank"] is None

    result = comprehensive_score({"sector": detail})
    sentiment = result["dimensions"]["sentiment"]
    # 日频组件正常计分：板块形态 + 过热（5日涨幅 + 连涨天数，排名缺）
    assert sentiment["components"]["sector_pattern"] == pytest.approx(4.9, abs=0.05)
    assert sentiment["components"]["overheat_risk"] == pytest.approx(6.0)
    assert "sector_current" in sentiment["unavailable_components"]
    health = result["dimensions"]["health"]
    assert "sector_position" in health["unavailable_components"]
    assert "板块相对强度高" in result["strengths"]


def test_rotation_only_detail_returns_none_without_five_completed_days():
    detail = rotation_only_detail(
        {"kind": "concept", "name": "人工智能"},
        {"dates": [], "columns": {}},
        date(2026, 8, 17),
    )

    assert detail is None
