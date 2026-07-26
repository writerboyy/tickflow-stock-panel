"""自由策略和模拟账户的 JSON 文件存储。"""
from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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

    def delete(self, strategy_id: str) -> None:
        shutil.rmtree(self._path(strategy_id))


class PaperAccountStore:
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
        state = {**state, "updated_at": now_iso()}
        (path / "state.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        return state

    def append_event(self, account_id: str, event: dict[str, Any]) -> None:
        path = self._path(account_id)
        path.mkdir(parents=True, exist_ok=True)
        with (path / "ledger.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"timestamp": now_iso(), **event}, ensure_ascii=False) + "\n")

    def events(self, account_id: str, limit: int = 500) -> list[dict[str, Any]]:
        path = self._path(account_id) / "ledger.jsonl"
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()[-limit:]
        return [json.loads(line) for line in lines if line.strip()]
