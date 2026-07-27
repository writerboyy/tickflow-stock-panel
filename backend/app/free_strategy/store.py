"""自由策略和模拟账户的 JSON 文件存储。"""
from __future__ import annotations

import fcntl
import json
import os
import shutil
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
                result.append(json.loads(manifest.read_text(encoding="utf-8")))
        return result

    def get(self, strategy_id: str) -> dict[str, Any]:
        path = self._path(strategy_id)
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        manifest["source"] = (path / "strategy.py").read_text(encoding="utf-8")
        return manifest

    def save(self, strategy_id: str | None, name: str, source: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        strategy_id = strategy_id or uuid.uuid4().hex[:12]
        path = self._path(strategy_id)
        path.mkdir(parents=True, exist_ok=True)
        manifest_path = path / "manifest.json"
        previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        revision = int(previous.get("revision", 0)) + 1
        (path / "revisions").mkdir(exist_ok=True)
        (path / "revisions" / f"{revision:04d}.py").write_text(source, encoding="utf-8")
        (path / "strategy.py").write_text(source, encoding="utf-8")
        manifest = {"id": strategy_id, "name": name.strip() or strategy_id, "config": config or previous.get("config", {}),
                    "revision": revision, "updated_at": now_iso(), "created_at": previous.get("created_at", now_iso())}
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**manifest, "source": source}

    def rename(self, strategy_id: str, name: str) -> dict[str, Any]:
        path = self._path(strategy_id)
        manifest_path = path / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["name"] = name
        manifest["updated_at"] = now_iso()
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**manifest, "source": (path / "strategy.py").read_text(encoding="utf-8")}

    def delete(self, strategy_id: str) -> None:
        shutil.rmtree(self._path(strategy_id))


class PaperAccountStore:
    _locks: dict[str, threading.Lock] = {}
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
        path = self._path(str(state["id"]))
        path.mkdir(parents=True, exist_ok=True)
        state = {"schema_version": 2, **state, "updated_at": now_iso()}
        _atomic_json_write(path / "state.json", state)
        return state

    @classmethod
    def _account_lock(cls, account_id: str) -> threading.Lock:
        with cls._locks_guard:
            return cls._locks.setdefault(account_id, threading.Lock())

    def append_event(self, account_id: str, event: dict[str, Any]) -> None:
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        ledger = path / "ledger.jsonl"
        with self._account_lock(account_id):
            with ledger.open("a+", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    handle.seek(0)
                    lines = handle.read().splitlines()
                    last = next((line for line in reversed(lines) if line.strip()), "")
                    try:
                        sequence = int(json.loads(last).get("sequence", 0)) + 1 if last else 1
                    except (ValueError, json.JSONDecodeError):
                        sequence = len(lines) + 1
                    payload = {
                        "id": str(event.get("id") or uuid.uuid4().hex),
                        "timestamp": now_iso(),
                        **event,
                        "sequence": sequence,
                    }
                    handle.seek(0, os.SEEK_END)
                    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def events(self, account_id: str, limit: int = 500) -> list[dict[str, Any]]:
        path = self._path(account_id) / "ledger.jsonl"
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
        result = []
        for line in lines:
            if not line.strip():
                continue
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return result

    def events_page(
        self,
        account_id: str,
        *,
        cursor: int | None = None,
        limit: int = 100,
        event_types: set[str] | None = None,
    ) -> dict[str, Any]:
        rows = self.events(account_id, limit=100_000)
        if event_types:
            rows = [row for row in rows if row.get("type") in event_types]
        if cursor is not None:
            rows = [row for row in rows if int(row.get("sequence", 0)) < cursor]
        page = rows[-max(1, min(limit, 500)):]
        page.reverse()
        next_cursor = int(page[-1].get("sequence", 0)) if len(page) == max(1, min(limit, 500)) else None
        return {"events": page, "next_cursor": next_cursor}

    def delete(self, account_id: str) -> None:
        shutil.rmtree(self._path(account_id))
