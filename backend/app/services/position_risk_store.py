"""持仓风控配置和运行状态的持久化。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
def _now() -> str:
    return datetime.now().astimezone().isoformat()


def default_rule_options() -> dict[str, Any]:
    return {
        "rules": {
            "market_context": {
                "enabled": False,
                "notify": False,
                "min_correlation": 0.50,
                "sector_weakening": -0.005,
                "underperform_threshold": -0.01,
                "min_flow_samples": 3,
                "normal_action_pct": 25,
                "strong_action_pct": 50,
            },
            "stop_loss": {
                "enabled": False, "notify": False, "threshold": -0.10,
                "mode": "max_fixed_atr", "atr_multiple": 1.5,
                "fees_buffer": 0.002, "action_pct": 100, "auto_execute": False,
            },
            "take_profit": {
                "enabled": False, "notify": False, "threshold": 0.10,
                "fees_buffer": 0.002, "action_pct": 100, "auto_execute": False,
            },
            "trailing_drawdown": {
                "enabled": False, "notify": False, "activation_gain": 0.05,
                "threshold": 0.08, "action_pct": 50, "auto_execute": False,
            },
            "take_profit_ladder": {
                "enabled": False, "active": False, "notify": False,
                "first_r": 1.0, "first_action_pct": 30,
                "second_r": 1.5, "second_action_pct": 30,
                "runner_pct": 40, "fees_buffer": 0.002,
                "break_even_r": 1.0, "lock_profit_r": 0.5,
                "runner_atr_multiple": 1.5, "auto_execute": False,
            },
            "t_trading": {
                "enabled": False, "notify": False, "buy_pct": 10, "sell_pct": 25,
                "confirm_bars": 2, "cooldown_minutes": 10,
                "min_expected_return": 0.005, "max_daily_trades": 3,
            },
            "structure_stop": {
                "enabled": False, "active": False, "notify": False, "reference": "vwap",
                "buffer": 0.002, "confirm_bars": 2, "action_pct": 50,
                "auto_execute": False,
            },
            "atr_protection": {
                "enabled": False, "active": False, "notify": False, "activation_gain": 0.02,
                "atr_multiple": 1.5, "action_pct": 50, "auto_execute": False,
            },
            "time_stop": {
                "enabled": False, "active": False, "notify": False, "max_minutes": 120,
                "min_gain": 0.0, "close_before_minutes": 15, "action_pct": 25,
                "auto_execute": False,
            },
            "ma5_breakdown": {"enabled": False, "notify": False, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 0},
            "ma10_breakdown": {"enabled": False, "notify": False, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 25},
            "ma20_breakdown": {"enabled": False, "notify": False, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 50},
            "five_minute_drawdown": {"enabled": False, "notify": False, "threshold": 0.03, "action_pct": 25, "auto_execute": False},
            "vwap_breakdown": {"enabled": False, "notify": False, "buffer": 0.01, "sustain_seconds": 30, "action_pct": 25, "auto_execute": False},
            "broken_limit_up": {"enabled": False, "notify": False, "action_pct": 50, "auto_execute": False},
            "resealed_limit_up": {"enabled": False, "notify": False, "action_pct": 0},
            "sealed_order_shrink_50": {"enabled": False, "notify": False, "threshold": 0.50, "action_pct": 25},
            "sealed_order_shrink_80": {"enabled": False, "notify": False, "threshold": 0.80, "action_pct": 50},
            "limit_down": {"enabled": False, "notify": False, "action_pct": 100, "auto_execute": False},
            "intraday_peak_pullback": {
                "enabled": True, "active": True, "notify": True,
                "activation_r": 1.0, "pullback_atr_multiple": 1.5, "confirm_bars": 2,
                "activation_gain": 0.05, "threshold": 0.03, "confirm_seconds": 5, "action_pct": 100,
                "cooldown_seconds": 300, "auto_execute": False,
            },
            "sector_leader_weakening": {
                "enabled": True, "notify": True, "auto_execute": False, "action_pct": 100,
                "correlation_window_days": 20, "min_correlation_samples": 20,
                "min_correlation": 0.50, "decline_delta": 0.20,
                "confirm_bars": 2, "underperformance_gap": -0.003,
            },
            "volume_price_divergence": {
                "enabled": True, "notify": True, "auto_execute": False, "action_pct": 100,
                "lookback_bars": 24, "min_peak_separation": 2,
                "min_peak_prominence_atr": 0.5, "max_peak_volume_ratio": 0.80,
                "confirm_bars": 2,
            },
            "opening_volume_selloff": {
                "enabled": True, "notify": True, "auto_execute": False, "action_pct": 100,
                "baseline_sessions": 20, "volume_multiple": 2.0,
                "price_confirmations": 2, "window_end": "10:00",
            },
            "next_day_gap_down": {
                "enabled": True, "active": False, "notify": False, "threshold": -0.03,
                "confirm_minutes": 1, "action_pct": 50, "auto_execute": False,
            },
            "next_day_gap_up_take_profit": {
                "enabled": True, "active": False, "notify": False, "threshold": 0.04,
                "confirm_minutes": 1, "action_pct": 50, "fees_buffer": 0.002,
                "auto_execute": False,
            },
            "opening_range_failure": {
                "enabled": True, "active": False, "notify": False, "window_minutes": 5,
                "buffer": 0.002, "confirm_bars": 1, "reference": "opening_range_low",
                "action_pct": 50, "auto_execute": False,
            },
            "t_plus_one_exit": {
                "enabled": True, "active": False, "notify": False, "max_holding_days": 1,
                "close_before_minutes": 15, "min_gain": -1.0,
                "action_pct": 100, "auto_execute": False,
            },
            "large_buy": {
                "enabled": True, "notify": False, "action_pct": 0, "window_seconds": 60,
                "min_samples": 7, "min_amount": 1_000_000,
                "mad_multiplier": 3.0, "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "large_sell": {
                "enabled": True, "notify": False, "action_pct": 0, "window_seconds": 60,
                "min_samples": 7, "min_amount": 1_000_000,
                "mad_multiplier": 3.0, "min_z_score": 2.5, "direction_ratio": 0.65,
            },
            "continuous_outflow": {"enabled": True, "notify": False, "direction_ratio": 0.65, "sustain_seconds": 10, "action_pct": 0},
            "orderbook_imbalance": {"enabled": True, "notify": False, "threshold": -0.35, "sustain_seconds": 10, "action_pct": 0},
            "fund_flow_pressure": {
                "enabled": True, "notify": False, "min_evidence": 2,
                "sustain_seconds": 30, "recovery_seconds": 60,
                "cooldown_seconds": 900, "recovery_sell_ratio": 0.55,
                "recovery_imbalance": -0.15, "price_buffer": 0.002,
                "strong_price_drop": 0.01,
                "action_pct": 25, "strong_action_pct": 50,
            },
            "daily_equity_loss": {"enabled": True, "notify": False, "threshold": 0.03, "action_pct": 50},
            "equity_drawdown": {"enabled": True, "notify": False, "threshold": 0.08, "action_pct": 50},
            "unrealized_loss": {"enabled": True, "notify": False, "threshold": 0.08, "action_pct": 50},
            "total_exposure": {"enabled": True, "notify": False, "threshold": 0.95, "action_pct": 25},
            "symbol_concentration": {"enabled": True, "notify": False, "threshold": 0.30, "target_pct": 30},
            "clustered_severe_events": {"enabled": True, "notify": False, "count": 3, "window_seconds": 300, "action_pct": 50},
            "quote_interruption": {"enabled": True, "notify": False, "threshold_seconds": 30, "action_pct": 0},
        },
        "signals": {"builtin": {}, "custom": {}, "monitor_rules": {}},
    }


def default_portfolio() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "revision": 0,
        "account": {
            "name": "手工账户",
            "cash": None,
            "total_asset": None,
            "previous_close_total_asset": None,
            "high_watermark": None,
        },
        "positions": [],
        "overrides": {},
        "imported_at": None,
        "updated_at": None,
    }


class RevisionConflict(ValueError):
    pass


class PositionRiskStore:
    def __init__(self, data_dir: Path) -> None:
        self.root = Path(data_dir) / "user_data" / "position_risk"
        self.root.mkdir(parents=True, exist_ok=True)
        self.portfolio_path = self.root / "portfolio.json"
        self.db_path = self.root / "runtime.sqlite3"
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runtime_state (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                DROP TABLE IF EXISTS recommendations;
            """)

    @staticmethod
    def _merge_defaults(value: dict[str, Any]) -> dict[str, Any]:
        result = default_portfolio()
        result.update({key: deepcopy(item) for key, item in value.items() if key not in {"account", "template", "schema_version"}})
        result["account"].update(value.get("account") or {})
        result["schema_version"] = SCHEMA_VERSION
        return result

    def load(self) -> dict[str, Any]:
        with self._lock:
            if not self.portfolio_path.exists():
                return default_portfolio()
            try:
                raw = json.loads(self.portfolio_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError("持仓风控配置损坏，已拒绝覆盖") from exc
            if not isinstance(raw, dict):
                raise RuntimeError("持仓风控配置格式无效，已拒绝覆盖")
            normalized = self._merge_defaults(raw)
            if raw != normalized:
                self._write(normalized)
            return normalized

    def _write(self, value: dict[str, Any]) -> None:
        temporary = self.portfolio_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, self.portfolio_path)

    def replace(self, value: dict[str, Any], revision: int) -> dict[str, Any]:
        with self._lock:
            current = self.load()
            if int(revision) != int(current["revision"]):
                raise RevisionConflict(f"配置已更新，请刷新后重试（当前 revision={current['revision']}）")
            next_value = self._merge_defaults(value)
            next_value["revision"] = int(current["revision"]) + 1
            next_value["updated_at"] = _now()
            self._write(next_value)
            return deepcopy(next_value)

    def update(self, revision: int, updater) -> dict[str, Any]:
        with self._lock:
            current = self.load()
            if int(revision) != int(current["revision"]):
                raise RevisionConflict(f"配置已更新，请刷新后重试（当前 revision={current['revision']}）")
            next_value = deepcopy(current)
            updater(next_value)
            next_value["revision"] = int(current["revision"]) + 1
            next_value["updated_at"] = _now()
            self._write(next_value)
            return deepcopy(next_value)

    def update_system(self, updater) -> dict[str, Any]:
        """更新运行账户字段但不抢占用户配置 revision。"""
        with self._lock:
            current = self.load()
            next_value = deepcopy(current)
            updater(next_value)
            next_value = self._merge_defaults(next_value)
            next_value["revision"] = int(current["revision"])
            next_value["updated_at"] = _now()
            self._write(next_value)
            return deepcopy(next_value)

    def get_runtime(self, key: str, default: Any = None) -> Any:
        with self._connect() as conn:
            row = conn.execute("SELECT value_json FROM runtime_state WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value_json"]) if row else default

    def set_runtime(self, key: str, value: Any) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO runtime_state(key, value_json, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at""",
                (key, json.dumps(value, ensure_ascii=False), _now()),
            )
