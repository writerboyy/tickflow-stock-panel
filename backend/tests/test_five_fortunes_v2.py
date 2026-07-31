from datetime import datetime

from app.free_strategy import five_fortunes_v2 as five
from app.free_strategy.engine import FreeStrategyEngine
from app.free_strategy.templates import TEMPLATES


def metric(**overrides):
    row = {
        "score": 4.0,
        "r2": 0.5,
        "passed_momentum": True,
        "passed_r2": True,
        "passed_volume": False,
        "passed_loss": False,
        "passed_volume_divergence": False,
    }
    row.update(overrides)
    return row


def test_weak_regime_keeps_only_momentum_and_r2_filters():
    row = metric()

    assert five._passes_filters(row, "走弱期") is True
    assert five._filter_failures(row, "走弱期") == []
    assert five._passes_filters(row, "正常期") is False


def test_choppy_regime_applies_volume_price_divergence_filter():
    row = metric(passed_volume=True, passed_loss=True, passed_volume_divergence=False)

    assert five._passes_filters(row, "震荡期") is False
    assert five._filter_failures(row, "震荡期") == ["volume_divergence"]


def test_intraday_trend_confirmation_uses_price_only():
    state = {
        "intraday": {
            "minute_closes": {
                "510300.SH": [1.0 + index * 0.002 for index in range(30)],
                "159915.SZ": [1.0] * 4,
            },
        },
    }

    assert five._intraday_trend_confirmed(state, "510300.SH") is True
    assert five._intraday_trend_confirmed(state, "159915.SZ") is False
    assert five._intraday_trend_confirmed(state, "511880.SH") is True


def test_template_uses_wufu2_scheduled_buy_retry_times():
    template = TEMPLATES["five_fortunes_v2"]
    engine = FreeStrategyEngine(
        template["source"],
        timeframe=template["config"]["timeframe"],
    )

    assert engine.execution_mode == "full_bar"
    assert engine.scheduled_times == [
        "09:00",
        "09:40",
        "13:10",
        "13:40",
        "14:00",
        "14:10",
        "14:30",
        "14:40",
        "14:55",
        "15:10",
    ]
    assert engine.context.state["five_fortunes_v2"]["version"] == "2.0"
    assert "five_fortunes" not in engine.context.state


def test_volume_price_divergence_matches_reference_rule():
    closes = [1.0, 1.01, 1.02, 1.025, 1.03, 1.04]
    volumes = [100, 100, 100, 80, 80, 80]

    passed, info = five._volume_price_divergence(closes, volumes)

    assert passed is False
    assert info["reason"] == "divergence"


def test_choose_targets_retains_existing_candidate_without_correlation_guard():
    class Portfolio:
        positions = {"A": 100}

    class Context:
        now = datetime(2026, 7, 28, 13, 10)
        portfolio = Portfolio()
        state = {"five_fortunes_v2": {"decision": {}, "rank_streak": {}}}

    rows = [{"symbol": "B", "score": 5.0}, {"symbol": "A", "score": 4.9}]

    assert five._choose_targets(Context(), rows, rows) == ["A"]


def test_choose_targets_uses_reference_score_without_stale_quote_adjustment():
    class Portfolio:
        positions = {}

    class Context:
        now = datetime(2025, 8, 21, 13, 10)
        portfolio = Portfolio()
        state = {"five_fortunes_v2": {"decision": {}, "rank_streak": {}}}

    rows = [
        {"symbol": "588890.SH", "score": 4.997, "entry_score": 5.001},
        {"symbol": "588200.SH", "score": 4.901, "entry_score": 4.950},
    ]

    assert five._choose_targets(Context(), rows, rows) == ["588890.SH"]


def test_daily_decision_signal_uses_the_v2_identity(monkeypatch):
    emitted = []

    class Portfolio:
        cash = 100_000
        positions = {}

    class Context:
        now = datetime(2026, 7, 28, 13, 10)
        portfolio = Portfolio()
        state = {"five_fortunes_v2": {
            "regime": "正常期",
            "raw_regime": "正常期",
            "regime_changed_today": False,
            "decision": {},
            "intraday": {"raw_close": {}},
        }}

        @staticmethod
        def order_target_percent(_symbol, _target):
            pass

        @staticmethod
        def emit_signal(signal_type, payload, *, event_id):
            emitted.append((signal_type, payload, event_id))

        @staticmethod
        def log(_message):
            pass

    rows = [{"symbol": "159985.SZ", "score": 1.5}]
    monkeypatch.setattr(five, "_rank_candidates", lambda _context: rows)
    monkeypatch.setattr(five, "_candidate_pool", lambda _rows, _regime: rows)
    monkeypatch.setattr(five, "_choose_targets", lambda *_args: ["159985.SZ"])
    monkeypatch.setattr(five, "_held_symbols", lambda _context: [])
    monkeypatch.setattr(five, "_buy_targets", lambda *_args, **_kwargs: None)

    five._prepare_and_sell(Context())

    signal_type, payload, event_id = emitted[0]
    assert signal_type == "daily_decision"
    assert payload["strategy"] == "five_fortunes_v2"
    assert event_id == "five_fortunes_v2:2026-07-28:decision"


def test_historical_names_keep_reference_medical_etfs_in_separate_groups():
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588700.SH"]) == "科创组:生物"
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588860.SH"]) == "科创组:医药"


def test_historical_name_keeps_renamed_star_market_etf_in_dynamic_pool():
    assert five._dynamic_group(five.WUFU_GROUP_NAME_OVERRIDES["588040.SH"]) == "科创组:指"


def test_market_catalog_restores_source_lofs_missing_from_etf_dimension():
    class Context:
        @staticmethod
        def instruments(_asset=None):
            return [{"symbol": "510300.SH", "name": "沪深300ETF", "has_minute": True}]

    market_symbols, names, dynamic_groups, minute_symbols = five._market_catalog(Context())

    assert market_symbols == ["510300.SH"]
    assert names["161226.SZ"] == "国投白银LOF"
    assert names["501018.SH"] == "南方原油LOF"
    assert {"161226.SZ", "501018.SH"} <= minute_symbols
    assert "161226.SZ" not in dynamic_groups


def test_buy_target_uses_reference_commission_and_slippage_sizing():
    class Portfolio:
        cash = 100_716.0
        positions = {}

    class Context:
        now = datetime(2025, 8, 13, 13, 10)
        portfolio = Portfolio()
        orders = []
        state = {
            "five_fortunes_v2": {
                "target": ["588760.SH"],
                "rebuy_cooldown": {},
                "decision": {},
                "intraday": {
                    "raw_close": {"588760.SH": 0.623},
                    "minute_closes": {"588760.SH": []},
                    "suspended": {},
                    "tradable": {},
                    "limit_up": {},
                    "limit_down": {},
                },
            },
        }

        @classmethod
        def order_target(cls, symbol, quantity):
            cls.orders.append((symbol, quantity))

        @staticmethod
        def log(_message):
            pass

    five._buy_targets(Context(), force=True)

    assert Context.orders == [("588760.SH", 161_600)]


def test_minute_stop_loss_uses_reference_fixed_threshold():
    class Bar:
        @staticmethod
        def execution_price(_field):
            return 0.95

    class Portfolio:
        positions = {"510300.SH": 100}
        avg_cost = {"510300.SH": 1.0}

    class Context:
        now = datetime(2025, 10, 10, 10, 0)
        portfolio = Portfolio()
        state = {"five_fortunes_v2": {"regime": "正常期", "rebuy_cooldown": {}}}
        orders = []

        @classmethod
        def order_target_percent(cls, symbol, target):
            cls.orders.append((symbol, target))

        @staticmethod
        def log(_message):
            pass

    five._minute_stop_loss(Context(), {"510300.SH": Bar()})

    assert Context.orders == [("510300.SH", 0.0)]
    assert Context.state["five_fortunes_v2"]["rebuy_cooldown"] == {}


def test_minute_stop_loss_stops_after_reference_trading_window():
    class Bar:
        @staticmethod
        def execution_price(_field):
            return 0.90

    class Portfolio:
        positions = {"510300.SH": 100}
        avg_cost = {"510300.SH": 1.0}

    class Context:
        now = datetime(2025, 10, 10, 15, 0)
        portfolio = Portfolio()
        state = {"five_fortunes_v2": {"regime": "正常期", "rebuy_cooldown": {}}}
        orders = []

        @classmethod
        def order_target_percent(cls, symbol, target):
            cls.orders.append((symbol, target))

        @staticmethod
        def log(_message):
            pass

    five._minute_stop_loss(Context(), {"510300.SH": Bar()})

    assert Context.orders == []


def test_dynamic_weak_lookback_signal_updates_outside_weak_regime(monkeypatch):
    class Context:
        now = datetime(2026, 1, 23, 9, 40)
        state = {
            "five_fortunes_v2": {
                "is_a_share_weak": False,
                "is_choppy": False,
                "weak_enter_streak": 0,
                "weak_exit_streak": 0,
                "weak_days_count": 0,
                "weak_confirm_days": 1,
                "max_weak_days": 20,
            },
        }

    calls = []
    monkeypatch.setattr(five, "_proxy_votes", lambda *_args: (4, 0, 4))
    monkeypatch.setattr(five, "_adjust_weak_momentum_lookback", lambda _context: calls.append("adjust"))
    monkeypatch.setattr(five, "_check_choppy_market", lambda _context: False)
    monkeypatch.setattr(five, "_set_regime_from_flags", lambda *_args: None)
    monkeypatch.setattr(five, "_refresh_liquidity_pools", lambda _context: None)

    five._check_weak_period_daily(Context())

    assert calls == ["adjust"]


def test_liquidity_refresh_keeps_source_lofs_subscribed_for_history(monkeypatch):
    days = ["2026-01-19", "2026-01-20", "2026-01-21"]

    class Portfolio:
        positions = {}

    class Context:
        portfolio = Portfolio()
        universe = []
        state = {
            "five_fortunes_v2": {
                "market_symbols": ["510300.SH"],
                "fixed_pool": ["501018.SH"],
                "global_pool": ["501018.SH"],
                "dynamic_groups": {},
                "dynamic_pool_ready": True,
                "regime": "正常期",
            },
        }

        @classmethod
        def set_universe(cls, symbols):
            cls.universe = symbols

        @staticmethod
        def log(_message):
            pass

    history = {
        "510300.SH": [
            {"date": day, "close": 1.0, "volume": 1.0, "amount": 100_000_000.0}
            for day in days
        ],
        "501018.SH": [
            {"date": day, "close": 1.0, "volume": 1.0, "amount": 0.0}
            for day in days
        ],
    }
    monkeypatch.setattr(
        five,
        "_history_rows_batch",
        lambda _context, symbols, _count: {symbol: history.get(symbol, []) for symbol in symbols},
    )

    five._refresh_liquidity_pools(Context())

    assert "501018.SH" not in Context.state["five_fortunes_v2"]["normal_liquidity_pool"]
    assert "501018.SH" in Context.universe


def test_liquidity_refresh_caps_dynamic_pool_to_source_configured_limit(monkeypatch):
    days = ["2026-04-01", "2026-04-02", "2026-04-03"]
    symbols = [f"159{i:03d}.SZ" for i in range(five.DYNAMIC_POOL_TOP_N + 1)]

    class Portfolio:
        positions = {}

    class Context:
        portfolio = Portfolio()
        universe = []
        state = {
            "five_fortunes_v2": {
                "market_symbols": symbols,
                "fixed_pool": [],
                "global_pool": [],
                "dynamic_groups": {
                    symbol: f"普通组:{index:03d}" for index, symbol in enumerate(symbols)
                },
                "dynamic_pool_ready": True,
                "regime": "正常期",
            },
        }

        @classmethod
        def set_universe(cls, symbols):
            cls.universe = symbols

        @staticmethod
        def log(_message):
            pass

    def history_for(symbol):
        amount = 200_000_000.0 - symbols.index(symbol) if symbol in symbols else 1.0
        return [
            {"date": day, "close": 1.0, "volume": 1.0, "amount": amount}
            for day in days
        ]

    monkeypatch.setattr(
        five,
        "_history_rows_batch",
        lambda _context, requested, _count: {symbol: history_for(symbol) for symbol in requested},
    )

    five._refresh_liquidity_pools(Context())

    dynamic_pool = Context.state["five_fortunes_v2"]["dynamic_pool"]
    assert len(dynamic_pool) == five.DYNAMIC_POOL_TOP_N
    assert symbols[:five.DYNAMIC_POOL_TOP_N] == dynamic_pool
    assert symbols[five.DYNAMIC_POOL_TOP_N] not in dynamic_pool
