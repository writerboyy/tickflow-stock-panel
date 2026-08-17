from __future__ import annotations

import json
from datetime import date

import pytest

from app.plugins.kaipanla.parsers import (
    ResponseShapeError,
    parse_auction,
    parse_bid_detail,
    parse_capital_net,
    parse_dragon_tiger_details,
    parse_dragon_tiger_movement,
    parse_interval_stock,
    parse_large_order_statistics,
    parse_large_order_trades,
    parse_large_order_intents,
    parse_lhb_detail,
    parse_lhb_list,
    parse_limit_up_expression,
    parse_limit_up_ladder_height,
    parse_limitup,
    parse_premium_gene,
    parse_northbound_sector,
    parse_northbound_stocks,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
    parse_sector_constituents,
    parse_sector_strength,
    parse_shareholder_changes,
    parse_shareholder_count_changes,
)


def test_large_order_trade_parser_maps_direction_and_deduplicates():
    rows = parse_large_order_trades(
        {
            "List": [
                ["2", "1778651941", "1075", "1011575", "9.41", "2026-05-13 13:59:01"],
                ["2", "1778651941", "1075", "1011575", "9.41", "2026-05-13 13:59:01"],
                ["4", "1778651942", "200", "188200", "9.41", "2026-05-13 13:59:02"],
            ]
        },
        "600126",
    )
    assert len(rows) == 2
    assert rows[0]["direction"] == "active_buy"
    assert rows[1]["direction"] == "active_sell"
    assert rows[0]["event_id"].startswith("600126:")
    with pytest.raises(ResponseShapeError, match="6 列"):
        parse_large_order_trades({"List": [["2"]]}, "600126")


def test_large_order_intent_parser_keeps_cancel_and_limit_flags():
    rows = parse_large_order_intents(
        {
            "List": [["09:30:01", "123", "10.5", "1000", "1050000", "1", "unknown", "1", "0", "1778651941"]]
        },
        "000001",
    )
    assert rows[0]["side"] == "buy"
    assert rows[0]["limit_flag"] is True
    assert rows[0]["cancel_flag"] is False
    assert rows[0]["raw_tail"] == "unknown"
    with pytest.raises(ResponseShapeError, match="撤单标记"):
        parse_large_order_intents(
            {"List": [["09:30:01", "123", "10.5", "1000", "1050000", "1", "x", "1", "2", "1778651941"]]},
            "000001",
        )


AUCTION_ROW = [
    "002969",
    "嘉美包装",
    6.07,
    9.96,
    1_381_972_304,
    9.96,
    2_222_228,
    0.16,
    4_058_402,
    37_199_550,
    4_058_402,
    "实控人变更、酿酒",
    2_547_574_181,
    12_878_114,
    17_454_281,
    -4_576_167,
    "6天4板",
]


def test_auction_parser_maps_only_documented_17_columns_and_preserves_percent_units():
    row = parse_auction({"info": [AUCTION_ROW]})[0]
    assert row["auction_change_pct"] == 9.96
    assert row["auction_turnover_pct"] == 0.16
    assert row["float_market_cap"] == 2_547_574_181
    assert set(row) == {
        "symbol",
        "code",
        "name",
        "realtime_change_pct",
        "limit_buy_amount",
        "auction_change_pct",
        "auction_net_amount",
        "auction_turnover_pct",
        "auction_amount",
        "post_0920_buy_amount",
        "themes",
        "float_market_cap",
        "board_label",
    }

    with pytest.raises(ResponseShapeError, match="17 列"):
        parse_auction({"info": [AUCTION_ROW[:-1]]})


def test_bid_parser_omits_unknown_direction_position_from_standard_json():
    row = parse_bid_detail(
        {
            "code": "000785",
            "bid": [["09:15", 3.03, 99, 134], ["09:25", 3.06, 0, 1281]],
            "preclose_px": 3.06,
            "hprice": 3.06,
            "lprice": 3.03,
            "openpx": 3.06,
        }
    )
    points = json.loads(row["bid_points_json"])
    assert points == [
        {"time": "09:15", "price": 3.03, "volume": 134.0},
        {"time": "09:25", "price": 3.06, "volume": 1281.0},
    ]
    assert row["bid_last_price"] == 3.06


def test_limitup_parser_flattens_nested_plates_to_one_row_per_stock():
    stock = [
        "000777",
        "中核科技",
        0,
        "",
        0,
        0,
        1_759_973_841,
        0,
        70_447_232,
        "首板",
        1,
        "可控核聚变、核电",
        239_159_833,
        682_199_256,
        11.73,
        5_877_917_502,
        "可控核聚变",
        "详细原因",
        1,
    ]
    payload = {
        "nums": {"SZJS": 2990, "XDJS": 2036, "ZT": 97, "DT": 24, "ZBL": 28.3333, "yestRase": 1.389},
        "list": [
            {"ZSCode": "801074", "ZSName": "核电", "StockList": [stock]},
            {"ZSCode": "801075", "ZSName": "聚变", "StockList": [stock]},
        ],
    }
    rows = parse_limitup(payload)
    assert len(rows) == 1
    assert rows[0]["plate_names"] == "核电;聚变"
    assert rows[0]["reason_detail"] == "详细原因"
    assert rows[0]["market_limitup_count"] == 97


def test_limit_up_expression_parser_maps_kaipanla_sentiment_fields():
    row = parse_limit_up_expression(
        {"info": [52, 5, 5, 1, 13.5135, 38.4615, 11.1111, 23.1707, -0.094, -0.416, -2.042, "题材存在炒作机会"]},
        date(2026, 8, 14),
    )

    assert row["as_of"] == "2026-08-14"
    assert row["first_board_count"] == 52
    assert row["market_broken_rate_pct"] == 23.1707
    assert row["yesterday_consecutive_change_pct"] == -0.416
    assert row["market_evaluation"] == "题材存在炒作机会"


def test_limit_up_ladder_parser_ignores_special_zero_level():
    assert parse_limit_up_ladder_height(
        {"Date": "2026-08-14", "List": [{"Tip": 0}, {"Tip": 3}, {"Tip": 5}, {"Tip": -1}]},
    ) == {"as_of": "2026-08-14", "max_consecutive": 5}


def test_premium_gene_parser_maps_documented_six_values_as_percentages():
    row = parse_premium_gene(
        {"List": [9, 3, 100, 88.8889, 11.1111, 12.5]},
        "001330",
    )
    assert row == {
        "symbol": "001330",
        "code": "001330",
        "limit_up_count": 9,
        "premium_5_count": 3,
        "next_day_red_rate_pct": 100.0,
        "first_board_seal_rate_pct": 88.8889,
        "first_board_broken_rate_pct": 11.1111,
        "consecutive_rate_pct": 12.5,
    }
    with pytest.raises(ResponseShapeError, match="6 列"):
        parse_premium_gene({"List": [9]}, "001330")


def test_lhb_object_and_detail_parsers_keep_daily_grain_with_summaries():
    trade_date, rows = parse_lhb_list(
        {
            "Time": "2026-05-15",
            "list": [
                {
                    "ID": "002208",
                    "Name": "合肥城建",
                    "IncreaseAmount": "4.85%",
                    "BuyIn": "8291254",
                    "JoinNum": 2,
                    "Turnover": "3244473547",
                    "CircPrice": 16671780262.92,
                    "Amplitude": "15.35",
                    "TurnoverRatio": "19.99",
                    "Capitalization": 16676339719.44,
                }
            ],
        }
    )
    assert str(trade_date) == "2026-05-15"
    assert rows[0]["increase_pct"] == 4.85

    detail = parse_lhb_detail(
        {
            "BuyList": [{"Name": "席位甲", "Buy": "100", "Sell": "20"}],
            "SellList": [{"Name": "席位乙", "Buy": "10", "Sell": "80"}],
        },
        "002208",
    )
    assert detail["buy_seat_count"] == 1
    assert detail["buy_list_buy_amount"] == 100
    assert detail["sell_list_sell_amount"] == 80


def test_regulatory_array_lengths_and_fields_are_strict():
    monitor = parse_regulatory_monitor(
        {
            "List": [["301319", "唯特偶", "2026-05-21", "2026-06-03", 2]],
        }
    )[0]
    anomaly = parse_regulatory_anomaly(
        {
            "List": [
                [
                    "002208",
                    "合肥城建",
                    1,
                    "10日内2次异动个股",
                    3,
                    7,
                    3.76,
                    27.67,
                    6.92,
                    23.96,
                    -7.42,
                ]
            ],
        }
    )[0]
    assert monitor["monitor_category"] == 2
    assert anomaly["trigger_change_pct"] == 6.92

    with pytest.raises(ResponseShapeError, match="11 列"):
        parse_regulatory_anomaly({"List": [["002208", "合肥城建"]]})


def test_fund_parsers_keep_main_flow_and_big_order_contracts_separate():
    interval = parse_interval_stock(
        {"List": [["600126", "杭钢股份", 9.2, 1.5, 100, 40, 60, 3.2, 1000, 2000, "算力", "", "流入", 3]]}
    )[0]
    capital = parse_capital_net(
        {"trend": [["09:30", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]},
        "600126",
    )
    statistics = parse_large_order_statistics(
        {"Date": ["20260710"], "TDJL": [30], "DDJL": [20], "ZDJL": [10], "XDJL": [-5]},
        "600126",
        date(2026, 7, 10),
    )
    assert interval["main_net"] == 60
    assert capital["capital_net_close"] == 2
    assert capital["capital_net_points"] == 1
    assert statistics is not None
    assert statistics["main_net_amount_over_300k"] == 50


def test_reference_parsers_keep_report_periods_and_composite_rows():
    report_date, sectors = parse_northbound_sector(
        {"Date": "20260630", "Sum_ZCJE": 10, "Sum_ZCC": 20, "List": [["P1", "板块", 1, 2, 3, 4, 5, 6, 7]]}
    )
    _, stocks = parse_northbound_stocks(
        {"Date": "20260630", "List": [["600126", "杭钢股份", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]},
        "P1",
    )
    shareholders = parse_shareholder_changes(
        {
            "LTGDData": [{"JGID": "H1", "JG": "股东甲", "CYSL": 2, "ZLTBL": 3, "SJJZC": "新进", "NiuSan": 1, "Color": 2}],
            "LTGDData_SQ": [],
        },
        "600126",
        date(2026, 6, 30),
    )
    counts = parse_shareholder_count_changes(
        {"List": [{"Day": "20260630", "StockID": "600126", "Name": "杭钢股份", "LTZB": 1, "CMJZ": 2, "JSQBH": 3, "UpdateDay": "20260701", "IsNew": 1}]}
    )
    movements = parse_dragon_tiger_movement(
        {"List": [{"BID": "P", "BName": "席位", "Buy": [{"Sto": "600126", "StoN": "杭钢股份", "Money": 1, "Three": 0}], "Sell": []}]},
        date(2026, 7, 10),
    )
    constituent_row = [None] * 41
    constituent_row[0], constituent_row[1], constituent_row[40] = "600126", "杭钢股份", 2
    constituents = parse_sector_constituents({"list": [constituent_row]}, "P1")
    strength_row = ["P1", "板块", 88.5, 3.2, 0.6, 100, 12, 60, 48, 1.4, 500, 20, 900, 2.1, 35, 30]
    child_strength_row = ["P2", "子板块", 77.0, 2.1, 0.3, 80, 8, 40, 32, 1.2, 400]
    strengths = parse_sector_strength({
        "list": [strength_row],
        "list_son": ["P1"],
        "list_soninfo": [child_strength_row],
    })

    assert report_date == date(2026, 6, 30)
    assert sectors[0]["holding_amount"] == 3
    assert stocks[0]["symbol"] == "600126"
    assert shareholders[0]["holding_change_pct"] is None
    assert counts[0]["updated_date"] == "2026-07-01"
    assert movements[0]["side"] == "buy"
    assert constituents[0]["limit_count"] == 2
    assert strengths[0]["plate_id"] == "P1"
    assert strengths[0]["strength"] == 88.5
    assert strengths[0]["change_pct_pct"] == 3.2
    assert strengths[0]["main_net"] == 12.0
    assert strengths[0]["rank"] == 1
    assert strengths[0]["rank_count"] == 2
    assert strengths[1]["plate_name"] == "子板块"
    assert strengths[1]["parent_plate_id"] == "P1"
    assert strengths[1]["is_child"] is True
    assert strengths[1]["rank"] == 2


def test_sector_strength_parser_uses_documented_live_money_columns():
    row = [
        "801001", "芯片", 16807, 3.254, 0.54,
        1_178_079_082_397, 38_070_711_178, 297_956_763_365, -259_886_052_187,
        1.086, 28_956_977_205_536, 2.9, 18_355_851_969, 39_545_448_949_952,
        212_572_237_165, 67.7139, 45.5848, 16807, 3.254,
    ]

    parsed = parse_sector_strength({"list": [row]})[0]

    assert parsed["main_net"] == 38_070_711_178
    assert parsed["large_order_amount_3m"] == 18_355_851_969
    assert parsed["market_value"] == 39_545_448_949_952
    assert parsed["institution_increase"] == 212_572_237_165
    assert parsed["pe_current"] == 67.7139
    assert parsed["pe_forward"] == 45.5848


def test_lhb_detail_parser_preserves_source_identity_across_reason_groups():
    def item(log_id: str, reason_type: str) -> dict:
        return {
            "ID": "D1",
            "Name": "席位甲",
            "Buy": 100,
            "Sell": 20,
            "PX": 1,
            "LogID": log_id,
            "ReasonType": reason_type,
            "GroupIcon": [],
        }

    rows = parse_dragon_tiger_details(
        {"List": [
            {"BuyList": [item("log-0", "0")], "SellList": []},
            {"BuyList": [item("log-1", "1")], "SellList": []},
        ]},
        "600126",
    )

    assert [(row["log_id"], row["reason_type"], row["rank"]) for row in rows] == [
        ("log-0", "0", 1),
        ("log-1", "1", 1),
    ]
