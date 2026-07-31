from __future__ import annotations

import json
from datetime import date

import pytest

from app.plugins.kaipanla.parsers import (
    ResponseShapeError,
    parse_auction,
    parse_bid_detail,
    parse_capital_net,
    parse_dragon_tiger_movement,
    parse_interval_stock,
    parse_large_order_statistics,
    parse_lhb_detail,
    parse_lhb_list,
    parse_limitup,
    parse_northbound_sector,
    parse_northbound_stocks,
    parse_regulatory_anomaly,
    parse_regulatory_monitor,
    parse_sector_constituents,
    parse_sector_strength,
    parse_shareholder_changes,
    parse_shareholder_count_changes,
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
    strength_row = ["P1", "板块"] + [0] * 9
    strengths = parse_sector_strength({"list": [strength_row]})

    assert report_date == date(2026, 6, 30)
    assert sectors[0]["holding_amount"] == 3
    assert stocks[0]["symbol"] == "600126"
    assert shareholders[0]["holding_change_pct"] is None
    assert counts[0]["updated_date"] == "2026-07-01"
    assert movements[0]["side"] == "buy"
    assert constituents[0]["limit_count"] == 2
    assert strengths[0]["plate_id"] == "P1"
