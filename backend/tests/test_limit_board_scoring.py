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


def _technical_values() -> dict:
    return {
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
    }


def test_technical_detail_exposes_kdj_when_present():
    detail = technical_detail({**_technical_values(), "kdj_k": 82.3, "kdj_d": 75.1, "kdj_j": 96.7})

    assert detail is not None
    assert detail["kdj_k"] == pytest.approx(82.3)
    assert detail["kdj_d"] == pytest.approx(75.1)
    assert detail["kdj_j"] == pytest.approx(96.7)


def test_technical_detail_survives_missing_kdj():
    """KDJ 是纯展示字段，缺失不能让整个技术面明细消失。"""
    detail = technical_detail(_technical_values())

    assert detail is not None
    assert detail["score"] == pytest.approx(20.0)
    assert detail["kdj_k"] is None
    assert detail["kdj_d"] is None
    assert detail["kdj_j"] is None


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


def test_intraday_flow_keeps_precise_capital_signal_without_minute_bars():
    detail = intraday_flow_detail(
        None,
        previous_close=10.0,
        external_flow={
            "data_quality": "precise",
            "buy_ratio": 0.8,
            "sell_ratio": 0.2,
        },
    )

    assert detail is not None
    assert detail["bars"] == 0
    assert detail["capital_available"] is True
    assert detail["net_flow_ratio"] == pytest.approx(0.6)
    assert detail["pull_up_minutes"] is None

    health = comprehensive_score({"intraday_flow": detail})["dimensions"]["health"]
    assert "capital_flow" in health["components"]
    assert "capital_flow" not in health["unavailable_components"]
    assert "pullup_form" in health["unavailable_components"]


def test_intraday_flow_rejects_proxy_capital_signal_without_minute_bars():
    assert intraday_flow_detail(
        None,
        external_flow={
            "data_quality": "proxy_only",
            "buy_ratio": 0.8,
            "sell_ratio": 0.2,
        },
    ) is None


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
        assert set(dimension["unavailable_reasons"]) == set(
            dimension["unavailable_components"]
        )
        assert all(dimension["unavailable_reasons"].values())
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

    # 历史基因占 60%：内部 100 分制中的次日收红 40 分 ×0.5 + 封板成功 40 分 ×1.0，外层缩放到 60 分
    history = result["dimensions"]["history"]
    assert history["score"] == pytest.approx(36.0)
    assert history["max_score"] == pytest.approx(48.0)
    assert history["full_max_score"] == pytest.approx(60.0)
    assert history["unavailable_components"] == ["consecutive_ability"]
    # 36/48 = 75 → A 档，但数据完整度不足 → 封顶 B
    assert result["comprehensive_score"] == pytest.approx(75.0)
    assert result["grade"] == "B"
    # 满分基数 = 60（历史基因）+ 20（板块强度）+ 20（拉升健康度）
    assert result["data_completeness"] == pytest.approx(0.48)
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
    assert sentiment["max_score"] == pytest.approx(20.0)
    # 板块形态：rotation 16.22/20 × 50 内部点，再按 20/100 缩放
    assert sentiment["components"]["sector_pattern"] == pytest.approx(8.1, abs=0.05)
    # 过热：5日涨幅 18% → 0.64；连涨 5 天 → 0.64；排名 1/50 顶部 → 0
    assert sentiment["components"]["overheat_risk"] == pytest.approx(1.3, abs=0.05)
    assert sentiment["components"]["sector_current"] == pytest.approx(3.4, abs=0.05)
    health = result["dimensions"]["health"]
    assert health["components"]["sector_position"] == pytest.approx(7.7)
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
    # 一波流放量拉升：内部 28.4/28.4，外层按 20/100 缩放为 5.7/5.7
    assert health["components"]["pullup_form"] == pytest.approx(5.7)
    # 封板后资金流向失真 → 数据不足，不再拿「主力流出」扣分
    assert "capital_flow" in health["unavailable_components"]
    # 拉升健康度内部满分 = 板块地位 38.4 + 拉升形态 28.4 + 资金强度 25.6 + 均线形态 7.6
    assert "daily_k_pattern" not in health["components"]
    assert health["max_score"] == pytest.approx(5.7)
    assert health["full_max_score"] == pytest.approx(20.0)
    assert "一波流拉升" in result["strengths"]
    assert "主力资金流出" not in result["warnings"]


def test_dimension_weights_history_heavy_with_rest_split():
    """历史涨停基因、板块强度、拉升健康度的外层权重为 60/20/20，合计 100。

    data_completeness 的分母取各维度 full_max_score 之和，三者必须合计 100，
    否则完整度永远到不了 1.0，「数据不完整封顶 B」会把所有评级静默压死。
    """
    result = comprehensive_score({})
    dimensions = result["dimensions"]

    assert dimensions["history"]["full_max_score"] == pytest.approx(60.0)
    assert dimensions["sentiment"]["full_max_score"] == pytest.approx(20.0)
    assert dimensions["health"]["full_max_score"] == pytest.approx(20.0)
    assert sum(item["full_max_score"] for item in dimensions.values()) == pytest.approx(100.0)


def test_dimension_internal_hundreds_scale_to_outer_weights():
    result = comprehensive_score({
        "premium_gene": {
            "next_day_red_rate": 1.0,
            "first_board_seal_rate": 1.0,
            "consecutive_rate": 1.0,
        },
        "sector": {
            "institutional_components": {
                "relative_momentum": 20.0,
                "trend": 10.0,
                "persistence": 10.0,
                "stability": 10.0,
                "breadth": 20.0,
                "money_flow": 15.0,
                "liquidity": 5.0,
            },
            "institutional_component_max": {
                "relative_momentum": 20.0,
                "trend": 10.0,
                "persistence": 10.0,
                "stability": 10.0,
                "breadth": 20.0,
                "money_flow": 15.0,
                "liquidity": 5.0,
            },
            "leadership": "leader",
            "is_sector_leader": True,
            "leader_gap_pct": 0.10,
            "stock_rank": 1,
        },
        "intraday_flow": {
            "pull_up_minutes": 15,
            "pull_up_max_drawdown": 0.0,
            "pre_seal_amount_growth": 0.60,
            "sealed_now": False,
            "net_flow_ratio": 0.40,
        },
        "technical": {
            "price": 12.0,
            "ma5": 11.0,
            "ma10": 10.0,
            "ma20": 9.0,
            "ma60": 8.0,
        },
    })

    dimensions = result["dimensions"]
    assert dimensions["history"]["score"] == pytest.approx(60.0)
    assert dimensions["sentiment"]["score"] == pytest.approx(20.0)
    assert dimensions["health"]["score"] == pytest.approx(20.0)
    assert result["comprehensive_score"] == pytest.approx(100.0)
    assert result["data_completeness"] == pytest.approx(1.0)


def test_ma_alignment_scores_but_emits_no_text_signal():
    """均线形态已恢复为拉升健康度的评分项，但不再产出文字优势/警示。"""
    flow = intraday_flow_detail(
        _sealed_pullup_intraday(), previous_close=10.0, limit_up=11.0,
        external_flow={"buy_ratio": 0.70, "sell_ratio": 0.30},
    )
    result = comprehensive_score({"intraday_flow": flow})

    assert "perfect_ma" not in result["strengths"]
    assert "完美多头排列" not in result["strengths"]
    assert "均线压制" not in result["warnings"]


def test_health_scores_ma_alignment_by_strict_ma_ladder():
    """均线形态内部 100 分制五档，外层按拉升健康度 20 分缩放。"""
    def health_for(price: float, ma5: float, ma10: float, ma20: float, ma60: float) -> float:
        technical = technical_detail({
            "close": price, "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60,
            "momentum_5d": 0.0, "momentum_20d": 0.0, "vol_ratio_5d": 1.0,
            "macd_dif": 0.0, "macd_dea": 0.0, "macd_hist": 0.0, "rsi_14": 60.0,
        })
        return comprehensive_score({"technical": technical})["dimensions"]["health"]

    assert health_for(12.0, 11.0, 10.0, 9.0, 8.0)["components"]["ma_alignment"] == pytest.approx(1.5)
    assert health_for(12.0, 11.0, 10.0, 9.0, 9.5)["components"]["ma_alignment"] == pytest.approx(1.3)
    assert health_for(12.0, 11.0, 10.0, 10.5, 9.0)["components"]["ma_alignment"] == pytest.approx(1.0)
    assert health_for(12.0, 11.0, 11.5, 10.0, 9.0)["components"]["ma_alignment"] == pytest.approx(0.5)
    assert health_for(10.0, 11.0, 10.0, 9.0, 8.0)["components"]["ma_alignment"] == pytest.approx(0.0)


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
    # 剩余日频分项先归一到内部 100 分，再按板块强度 20 分缩放；实时分项缺失时不计分。
    assert sentiment["components"] == {
        "relative_momentum": pytest.approx(2.9, abs=0.05),
        "trend": pytest.approx(2.2, abs=0.05),
        "persistence": pytest.approx(0.9, abs=0.05),
        "stability": pytest.approx(1.3, abs=0.05),
    }
    assert sentiment["max_score"] == pytest.approx(9.6, abs=0.05)
    assert sentiment["unavailable_components"] == [
        "breadth", "money_flow", "liquidity",
    ]
    assert sentiment["unavailable_reasons"] == {
        "breadth": "实时板块成分涨跌数据未返回",
        "money_flow": "实时板块主力净额或成交额未返回",
        "liquidity": "实时板块量比未返回",
    }
    health = result["dimensions"]["health"]
    assert "sector_position" in health["unavailable_components"]
    assert "板块相对强度高" in result["strengths"]


def test_comprehensive_score_uses_institutional_sector_components_without_leadership():
    components = {
        "relative_momentum": 20.0,
        "trend": 10.0,
        "persistence": 5.0,
        "stability": 7.0,
        "breadth": 20.0,
        "money_flow": 15.0,
        "liquidity": 5.0,
    }
    result = comprehensive_score({
        "sector": {
            "institutional_components": components,
            "institutional_component_max": {
                "relative_momentum": 20.0,
                "trend": 10.0,
                "persistence": 10.0,
                "stability": 10.0,
                "breadth": 20.0,
                "money_flow": 15.0,
                "liquidity": 5.0,
            },
        },
    })

    sentiment = result["dimensions"]["sentiment"]
    assert sentiment["score"] == pytest.approx(18.2)
    assert sentiment["max_score"] == pytest.approx(20.0)
    assert sentiment["unavailable_components"] == []
    assert sentiment["components"]["relative_momentum"] == pytest.approx(4.4)
    assert sentiment["components"]["money_flow"] == pytest.approx(3.3)
    assert "leadership" not in sentiment["components"]


def test_comprehensive_score_explains_missing_rotation_components():
    result = comprehensive_score({
        "sector": {
            "rotation_available": False,
            "rotation_matrix_available": False,
            "rotation_history_sessions": 0,
            "institutional_components": {
                "breadth": 12.0,
                "money_flow": 8.0,
                "liquidity": 3.0,
            },
            "institutional_component_max": {
                "breadth": 20.0,
                "money_flow": 15.0,
                "liquidity": 5.0,
            },
        },
    })

    sentiment = result["dimensions"]["sentiment"]
    for key in ("relative_momentum", "trend", "persistence", "stability"):
        assert sentiment["unavailable_reasons"][key] == "无板块历史轮动矩阵"


def test_comprehensive_score_explains_short_rotation_history_and_live_gaps():
    result = comprehensive_score({
        "sector": {
            "rotation_available": False,
            "rotation_matrix_available": True,
            "rotation_history_sessions": 2,
            "institutional_components": {"breadth": 12.0},
            "institutional_component_max": {"breadth": 20.0},
        },
    })

    reasons = result["dimensions"]["sentiment"]["unavailable_reasons"]
    assert reasons["relative_momentum"] == "板块历史仅 2 个交易日，至少需要 3 个"
    assert reasons["persistence"] == "板块历史仅 2 个交易日，至少需要 5 个"
    assert reasons["money_flow"] == "实时板块主力净额或成交额未返回"
    assert reasons["liquidity"] == "实时板块量比未返回"


def test_rotation_only_detail_returns_none_without_five_completed_days():
    detail = rotation_only_detail(
        {"kind": "concept", "name": "人工智能"},
        {"dates": [], "columns": {}},
        date(2026, 8, 17),
    )

    assert detail is None
