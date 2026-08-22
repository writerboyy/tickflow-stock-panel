"""自由策略和模拟账户的 JSON 文件存储。"""
from __future__ import annotations

import ast
import fcntl
import json
import os
import shutil
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Iterator

from app.free_strategy.jq_compat.capabilities import analyze_source
from app.market_time import cn_today


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def infer_execution_mode(source: str) -> str | None:
    try:
        module = ast.parse(source)
    except SyntaxError:
        return None
    callbacks = {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if "on_quote" in callbacks:
        return "quote"
    if "on_bar" in callbacks:
        return "full_bar"
    return "scheduled"


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class FreeStrategyStore:
    def __init__(self, data_dir: Path):
        self.root = Path(data_dir) / "free_strategies"
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, strategy_id: str) -> Path:
        if not strategy_id or strategy_id in {".", ".."} or "/" in strategy_id or "\\" in strategy_id:
            raise ValueError("非法策略 ID")
        return self.root / strategy_id

    def list(self) -> list[dict[str, Any]]:
        result = []
        for path in sorted(self.root.iterdir()):
            manifest = path / "manifest.json"
            if path.is_dir() and manifest.exists():
                result.append(self._normalize_manifest(json.loads(manifest.read_text(encoding="utf-8"))))
        return result

    def get(self, strategy_id: str) -> dict[str, Any]:
        path = self._path(strategy_id)
        manifest = self._normalize_manifest(json.loads((path / "manifest.json").read_text(encoding="utf-8")))
        manifest["source"] = (path / "strategy.py").read_text(encoding="utf-8")
        manifest["execution_mode_hint"] = infer_execution_mode(manifest["source"])
        if manifest["dialect"] == "joinquant" and not isinstance(manifest.get("compatibility_report"), dict):
            manifest["compatibility_report"] = analyze_source(manifest["source"])
        return manifest

    @staticmethod
    def _normalize_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
        result = dict(manifest)
        dialect = str(result.get("dialect") or "native").strip().lower()
        result["dialect"] = dialect if dialect in {"native", "joinquant"} else "native"
        return result

    def save(
        self,
        strategy_id: str | None,
        name: str,
        source: str,
        config: dict[str, Any] | None = None,
        *,
        dialect: str | None = None,
    ) -> dict[str, Any]:
        strategy_id = strategy_id or uuid.uuid4().hex[:12]
        path = self._path(strategy_id)
        path.mkdir(parents=True, exist_ok=True)
        manifest_path = path / "manifest.json"
        previous = self._normalize_manifest(json.loads(manifest_path.read_text(encoding="utf-8"))) if manifest_path.exists() else {}
        resolved_dialect = str(dialect or previous.get("dialect") or "native").strip().lower()
        if resolved_dialect not in {"native", "joinquant"}:
            raise ValueError("策略运行方言只支持 native 或 joinquant")
        revision = int(previous.get("revision", 0)) + 1
        (path / "revisions").mkdir(exist_ok=True)
        (path / "revisions" / f"{revision:04d}.py").write_text(source, encoding="utf-8")
        (path / "strategy.py").write_text(source, encoding="utf-8")
        compatibility_report = (
            analyze_source(source)
            if resolved_dialect == "joinquant"
            else {
                "version": None,
                "dialect": "native",
                "summary_status": "supported",
                "apis": [],
            }
        )
        manifest = {
            "id": strategy_id,
            "name": name.strip() or strategy_id,
            "config": config or previous.get("config", {}),
            "dialect": resolved_dialect,
            "compatibility_report": compatibility_report,
            "revision": revision,
            "updated_at": now_iso(),
            "created_at": previous.get("created_at", now_iso()),
        }
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            **manifest,
            "source": source,
            "execution_mode_hint": infer_execution_mode(source),
        }

    def rename(self, strategy_id: str, name: str) -> dict[str, Any]:
        path = self._path(strategy_id)
        manifest_path = path / "manifest.json"
        manifest = self._normalize_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
        manifest["name"] = name
        manifest["updated_at"] = now_iso()
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        source = (path / "strategy.py").read_text(encoding="utf-8")
        return {
            **manifest,
            "source": source,
            "execution_mode_hint": infer_execution_mode(source),
        }

    def delete(self, strategy_id: str) -> None:
        shutil.rmtree(self._path(strategy_id))


class PaperAccountStore:
    _locks: dict[str, threading.RLock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, data_dir: Path):
        self.root = Path(data_dir) / "paper_accounts"
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, account_id: str) -> Path:
        if not account_id or "/" in account_id or "\\" in account_id:
            raise ValueError("非法模拟账户 ID")
        return self.root / account_id

    def list(self) -> list[dict[str, Any]]:
        result = []
        for path in sorted(self.root.iterdir()):
            state = path / "state.json"
            if path.is_dir() and state.exists():
                result.append(json.loads(state.read_text(encoding="utf-8")))
        return result

    def get(self, account_id: str) -> dict[str, Any]:
        return json.loads((self._path(account_id) / "state.json").read_text(encoding="utf-8"))

    def save(self, state: dict[str, Any]) -> dict[str, Any]:
        account_id = str(state["id"])
        with self._state_guard(account_id):
            return self._save_state_unlocked(state)

    def update(
        self,
        account_id: str,
        updater: Callable[[dict[str, Any]], dict[str, Any] | None],
    ) -> dict[str, Any]:
        """在同一跨进程锁内重新读取并更新账户状态。"""
        with self._state_guard(account_id):
            current = self.get(account_id)
            updated = updater(current)
            return self._save_state_unlocked(updated if updated is not None else current)

    def update_fields(self, account_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        return self.update(account_id, lambda current: {**current, **fields})

    def _save_state_unlocked(self, state: dict[str, Any]) -> dict[str, Any]:
        path = self._path(str(state["id"]))
        path.mkdir(parents=True, exist_ok=True)
        saved = {"schema_version": 2, **state, "updated_at": now_iso()}
        _atomic_json_write(path / "state.json", saved)
        return saved

    @contextmanager
    def _state_guard(self, account_id: str) -> Iterator[None]:
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        with self._account_lock(account_id), (path / ".state.lock").open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def replace_equity_curve(self, account_id: str, rows: list[dict[str, Any]]) -> None:
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        database = path / "equity.sqlite3"
        with self._account_lock(account_id), sqlite3.connect(database) as connection:
            self._ensure_equity_table(connection)
            connection.execute("DELETE FROM equity_curve")
            self._upsert_equity_rows(connection, rows)

    def upsert_equity_curve(self, account_id: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        database = path / "equity.sqlite3"
        with self._account_lock(account_id), sqlite3.connect(database) as connection:
            self._ensure_equity_table(connection)
            self._upsert_equity_rows(connection, rows)
            cutoff = (cn_today() - timedelta(days=365)).isoformat()
            connection.execute("DELETE FROM equity_curve WHERE timestamp < ?", (cutoff,))

    def equity_curve(self, account_id: str, *, days: int = 365) -> list[dict[str, Any]]:
        database = self._path(account_id) / "equity.sqlite3"
        if not database.exists():
            return []
        cutoff = (cn_today() - timedelta(days=max(1, days))).isoformat()
        with self._account_lock(account_id), sqlite3.connect(database) as connection:
            self._ensure_equity_table(connection)
            rows = connection.execute(
                """
                SELECT timestamp, equity, cash, nav, drawdown_pct, positions, avg_cost
                FROM equity_curve
                WHERE timestamp >= ?
                ORDER BY timestamp
                """,
                (cutoff,),
            ).fetchall()
        return [
            {
                "timestamp": timestamp,
                "equity": equity,
                "cash": cash,
                "nav": nav,
                "drawdown_pct": drawdown_pct,
                "positions": json.loads(positions),
                "avg_cost": json.loads(avg_cost),
            }
            for timestamp, equity, cash, nav, drawdown_pct, positions, avg_cost in rows
        ]

    def max_drawdown_pct(self, account_id: str) -> float:
        database = self._path(account_id) / "equity.sqlite3"
        if not database.exists():
            return 0.0
        with self._account_lock(account_id), sqlite3.connect(database) as connection:
            self._ensure_equity_table(connection)
            row = connection.execute(
                "SELECT COALESCE(MAX(drawdown_pct), 0) FROM equity_curve"
            ).fetchone()
        return float(row[0] if row else 0.0)

    @staticmethod
    def _ensure_equity_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_curve (
                timestamp TEXT PRIMARY KEY,
                equity REAL NOT NULL,
                cash REAL NOT NULL,
                nav REAL NOT NULL,
                drawdown_pct REAL NOT NULL,
                positions TEXT NOT NULL,
                avg_cost TEXT NOT NULL DEFAULT '{}',
                source TEXT NOT NULL
            )
            """
        )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(equity_curve)")
        }
        if "avg_cost" not in columns:
            connection.execute(
                "ALTER TABLE equity_curve ADD COLUMN avg_cost TEXT NOT NULL DEFAULT '{}'"
            )

    @staticmethod
    def _upsert_equity_rows(
        connection: sqlite3.Connection,
        rows: list[dict[str, Any]],
    ) -> None:
        connection.executemany(
            """
            INSERT INTO equity_curve (
                timestamp, equity, cash, nav, drawdown_pct, positions, avg_cost, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(timestamp) DO UPDATE SET
                equity = excluded.equity,
                cash = excluded.cash,
                nav = excluded.nav,
                drawdown_pct = excluded.drawdown_pct,
                positions = excluded.positions,
                avg_cost = excluded.avg_cost,
                source = excluded.source
            """,
            [
                (
                    str(row["timestamp"]),
                    float(row["equity"]),
                    float(row["cash"]),
                    float(row["nav"]),
                    float(row["drawdown_pct"]),
                    json.dumps(row.get("positions", {}), ensure_ascii=False),
                    json.dumps(row.get("avg_cost", {}), ensure_ascii=False),
                    str(row.get("source") or "paper"),
                )
                for row in rows
            ],
        )

    @classmethod
    def _account_lock(cls, account_id: str) -> threading.RLock:
        with cls._locks_guard:
            return cls._locks.setdefault(account_id, threading.RLock())

    def append_event(self, account_id: str, event: dict[str, Any]) -> None:
        with self._event_connection(account_id) as connection:
            self._insert_event(connection, event)

    def append_event_once(self, account_id: str, event: dict[str, Any]) -> bool:
        event_id = str(event.get("id") or "").strip()
        if not event_id:
            raise ValueError("幂等事件必须提供 id")
        with self._event_connection(account_id) as connection:
            return self._insert_event(connection, event, ignore_duplicate=True)

    def append_strategy_logs(
        self,
        account_id: str,
        logs: list[dict[str, Any]],
    ) -> None:
        for item in logs:
            if item.get("source") != "strategy":
                continue
            raw = ":".join(
                str(item.get(key) or "")
                for key in ("timestamp", "level", "source", "message")
            )
            self.append_event_once(account_id, {
                "id": f"log:{sha256(raw.encode('utf-8')).hexdigest()[:24]}",
                "type": "log",
                **item,
            })

    def events(self, account_id: str, limit: int = 500) -> list[dict[str, Any]]:
        with self._event_connection(account_id) as connection:
            rows = connection.execute(
                "SELECT payload FROM paper_events ORDER BY sequence DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
        return [json.loads(row[0]) for row in reversed(rows)]

    def events_page(
        self,
        account_id: str,
        *,
        cursor: int | None = None,
        limit: int = 100,
        event_types: set[str] | None = None,
    ) -> dict[str, Any]:
        page_size = max(1, min(limit, 500))
        clauses: list[str] = []
        parameters: list[Any] = []
        if cursor is not None:
            clauses.append("sequence < ?")
            parameters.append(cursor)
        if event_types:
            placeholders = ",".join("?" for _ in event_types)
            clauses.append(f"type IN ({placeholders})")
            parameters.extend(sorted(event_types))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._event_connection(account_id) as connection:
            rows = connection.execute(
                f"SELECT payload FROM paper_events {where} ORDER BY sequence DESC LIMIT ?",  # noqa: S608
                (*parameters, page_size),
            ).fetchall()
        page = [json.loads(row[0]) for row in rows]
        next_cursor = int(page[-1]["sequence"]) if len(page) == page_size else None
        return {"events": page, "next_cursor": next_cursor}

    @contextmanager
    def _event_connection(self, account_id: str) -> Iterator[sqlite3.Connection]:
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        with self._account_lock(account_id), (path / ".ledger.lock").open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                with sqlite3.connect(path / "ledger.sqlite3", timeout=30) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    self._ensure_event_table(connection)
                    self._migrate_jsonl_events(connection, path / "ledger.jsonl")
                    yield connection
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _ensure_event_table(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_events (
                sequence INTEGER PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_paper_events_type_sequence "
            "ON paper_events(type, sequence)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS paper_event_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )

    @staticmethod
    def _migrate_jsonl_events(connection: sqlite3.Connection, ledger: Path) -> None:
        migrated = connection.execute(
            "SELECT 1 FROM paper_event_meta WHERE key = 'jsonl_migrated'"
        ).fetchone()
        if migrated:
            return
        maximum = int(connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM paper_events"
        ).fetchone()[0])
        if ledger.exists():
            for line in ledger.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                event_id = str(payload.get("id") or uuid.uuid4().hex)
                if connection.execute(
                    "SELECT 1 FROM paper_events WHERE id = ?", (event_id,)
                ).fetchone():
                    continue
                try:
                    sequence = int(payload.get("sequence") or maximum + 1)
                except (TypeError, ValueError):
                    sequence = maximum + 1
                if sequence <= maximum or connection.execute(
                    "SELECT 1 FROM paper_events WHERE sequence = ?", (sequence,)
                ).fetchone():
                    sequence = maximum + 1
                maximum = sequence
                payload.update({
                    "id": event_id,
                    "timestamp": str(payload.get("timestamp") or now_iso()),
                    "sequence": sequence,
                })
                connection.execute(
                    "INSERT INTO paper_events(sequence, id, timestamp, type, payload) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        sequence,
                        event_id,
                        payload["timestamp"],
                        str(payload.get("type") or "event"),
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
        connection.execute(
            "INSERT INTO paper_event_meta(key, value) VALUES ('jsonl_migrated', ?)",
            (now_iso(),),
        )

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        event: dict[str, Any],
        *,
        ignore_duplicate: bool = False,
    ) -> bool:
        event_id = str(event.get("id") or uuid.uuid4().hex)
        if ignore_duplicate and connection.execute(
            "SELECT 1 FROM paper_events WHERE id = ?", (event_id,)
        ).fetchone():
            return False
        sequence = int(connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM paper_events"
        ).fetchone()[0])
        payload = {
            "id": event_id,
            "timestamp": now_iso(),
            **event,
            "sequence": sequence,
        }
        try:
            connection.execute(
                "INSERT INTO paper_events(sequence, id, timestamp, type, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    sequence,
                    event_id,
                    str(payload["timestamp"]),
                    str(payload.get("type") or "event"),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        except sqlite3.IntegrityError:
            if ignore_duplicate:
                return False
            raise
        return True

    def delete(self, account_id: str) -> None:
        shutil.rmtree(self._path(account_id))
