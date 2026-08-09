"""持仓风控配置、运行状态和待确认建议的持久化。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
RECOMMENDATION_STATUSES = {"pending", "confirmed", "dismissed", "superseded", "stale"}


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def default_template() -> dict[str, Any]:
    return {
        "rules": {
            "stop_loss": {"enabled": True, "threshold": -0.10, "action_pct": 100},
            "trailing_drawdown": {"enabled": True, "activation_gain": 0.05, "threshold": 0.08, "action_pct": 50},
            "ma5_breakdown": {"enabled": True, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 0},
            "ma10_breakdown": {"enabled": True, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 25},
            "ma20_breakdown": {"enabled": True, "buffer": 0.002, "sustain_seconds": 5, "action_pct": 50},
            "five_minute_drawdown": {"enabled": True, "threshold": 0.03, "action_pct": 25},
            "vwap_breakdown": {"enabled": True, "buffer": 0.01, "sustain_seconds": 30, "action_pct": 25},
            "broken_limit_up": {"enabled": True, "action_pct": 50},
            "resealed_limit_up": {"enabled": True, "action_pct": 0},
            "sealed_order_shrink_50": {"enabled": True, "threshold": 0.50, "action_pct": 25},
            "sealed_order_shrink_80": {"enabled": True, "threshold": 0.80, "action_pct": 50},
            "limit_down": {"enabled": True, "action_pct": 100},
            "large_buy": {"enabled": True, "action_pct": 0},
            "large_sell": {"enabled": True, "action_pct": 25},
            "continuous_outflow": {"enabled": True, "direction_ratio": 0.65, "sustain_seconds": 10, "action_pct": 25},
            "orderbook_imbalance": {"enabled": True, "threshold": -0.35, "sustain_seconds": 10, "action_pct": 25},
            "daily_equity_loss": {"enabled": True, "threshold": 0.03, "action_pct": 50},
            "equity_drawdown": {"enabled": True, "threshold": 0.08, "action_pct": 50},
            "unrealized_loss": {"enabled": True, "threshold": 0.08, "action_pct": 50},
            "total_exposure": {"enabled": True, "threshold": 0.95, "action_pct": 25},
            "symbol_concentration": {"enabled": True, "threshold": 0.30, "target_pct": 30},
            "clustered_severe_events": {"enabled": True, "count": 3, "window_seconds": 300, "action_pct": 50},
            "quote_interruption": {"enabled": True, "threshold_seconds": 30, "action_pct": 0},
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
        "template": default_template(),
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
                CREATE TABLE IF NOT EXISTS recommendations (
                    id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL UNIQUE,
                    symbol TEXT,
                    scope TEXT NOT NULL DEFAULT 'symbol',
                    rule_id TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    risk_score INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    reduction_pct INTEGER NOT NULL,
                    reasons_json TEXT NOT NULL,
                    source_ids_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    portfolio_revision INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_recommendations_status
                    ON recommendations(status, created_at DESC);
            """)

    @staticmethod
    def _merge_defaults(value: dict[str, Any]) -> dict[str, Any]:
        result = default_portfolio()
        result.update({key: deepcopy(item) for key, item in value.items() if key not in {"account", "template"}})
        result["account"].update(value.get("account") or {})
        incoming_template = value.get("template") or {}
        result["template"]["rules"].update(incoming_template.get("rules") or {})
        result["template"]["signals"].update(incoming_template.get("signals") or {})
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
            return self._merge_defaults(raw)

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

    def stale_pending(self) -> int:
        with self._connect() as conn:
            result = conn.execute(
                "UPDATE recommendations SET status='stale', updated_at=? WHERE status='pending'",
                (_now(),),
            )
            return int(result.rowcount)

    def add_recommendation(self, item: dict[str, Any]) -> dict[str, Any]:
        severity_rank = {"info": 1, "warn": 2, "critical": 3}
        now = _now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM recommendations WHERE fingerprint = ?",
                (item["fingerprint"],),
            ).fetchone()
            if existing:
                return self._row(existing)
            pending = conn.execute(
                """SELECT * FROM recommendations
                   WHERE status='pending' AND scope=? AND COALESCE(symbol, '')=COALESCE(?, '')
                   ORDER BY risk_score DESC, created_at DESC LIMIT 1""",
                (item.get("scope", "symbol"), item.get("symbol")),
            ).fetchone()
            if pending:
                stronger = (
                    int(item["risk_score"]), severity_rank.get(item["severity"], 0), int(item["reduction_pct"])
                ) > (
                    int(pending["risk_score"]), severity_rank.get(pending["severity"], 0), int(pending["reduction_pct"])
                )
                if not stronger:
                    return self._row(pending)
                conn.execute(
                    "UPDATE recommendations SET status='superseded', updated_at=? WHERE id=?",
                    (now, pending["id"]),
                )
            recommendation_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO recommendations(
                    id, fingerprint, symbol, scope, rule_id, severity, risk_score, action,
                    reduction_pct, reasons_json, source_ids_json, status, portfolio_revision,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
                (
                    recommendation_id, item["fingerprint"], item.get("symbol"), item.get("scope", "symbol"),
                    item["rule_id"], item["severity"], int(item["risk_score"]), item["action"],
                    int(item["reduction_pct"]), json.dumps(item.get("reasons", []), ensure_ascii=False),
                    json.dumps(item.get("source_ids", []), ensure_ascii=False), int(item["portfolio_revision"]),
                    now, now,
                ),
            )
            row = conn.execute("SELECT * FROM recommendations WHERE id=?", (recommendation_id,)).fetchone()
        return self._row(row)

    @staticmethod
    def _row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["reasons"] = json.loads(value.pop("reasons_json"))
        value["source_ids"] = json.loads(value.pop("source_ids_json"))
        return value

    def list_recommendations(self, status: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        query = "SELECT * FROM recommendations"
        params: list[Any] = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, min(int(limit), 5000)))
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row(row) for row in rows]

    def set_recommendation_status(self, recommendation_id: str, status: str) -> dict[str, Any]:
        if status not in {"confirmed", "dismissed"}:
            raise ValueError("建议只能确认或忽略")
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM recommendations WHERE id=?", (recommendation_id,)).fetchone()
            if row is None:
                raise FileNotFoundError(recommendation_id)
            if row["status"] != "pending":
                raise ValueError("建议已处理或已失效")
            conn.execute(
                "UPDATE recommendations SET status=?, updated_at=? WHERE id=?",
                (status, _now(), recommendation_id),
            )
            updated = conn.execute("SELECT * FROM recommendations WHERE id=?", (recommendation_id,)).fetchone()
        return self._row(updated)
