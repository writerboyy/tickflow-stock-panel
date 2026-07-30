from __future__ import annotations

import json

import pytest

from app.plugins.kaipanla.parsers import (
    ResponseShapeError,
    parse_auction,
    parse_bid_detail,
    parse_lhb_detail,
    parse_lhb_list,
    parse_limitup,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
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
