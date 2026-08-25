"""Persistent configuration and intraday state for the limit-board workspace."""
from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2


def default_config() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "revision": 0,
        "settings": {
            "sweep_price_levels": 5,
            "queue_wait_seconds": 0,
            "queue_confirm_snapshots": 0,
            "order_allocation_mode": "fixed",
            "order_amount_per_board": 0.0,
            "max_auto_board_count": 0,
            "max_market_broken_rate_pct": 40.0,
            "main_board_only": False,
            "near_limit_pct": 0.02,
            "exit_limit_pct": 0.03,
            "exit_sustain_seconds": 30,
            "first_board_lookback_days": 10,
            "blacklist_after_breaks": 3,
        },
        "selected": [],
        "board_pool": [],
        "buy_pool": [],
    }


class RevisionConflict(ValueError):
    pass


class LimitBoardStore:
    def __init__(self, data_dir: Path) -> None:
        self.root = Path(data_dir) / "user_data" / "limit_board"
        self.root.mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "config.json"
        self.runtime_path = self.root / "runtime.json"
        self.events_path = self.root / "events.jsonl"
        self._lock = threading.RLock()

    def load_config(self) -> dict[str, Any]:
        with self._lock:
            result = default_config()
            if not self.config_path.exists():
                return result
            try:
                raw = json.loads(self.config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError("打板专区配置损坏，已拒绝覆盖") from exc
            if not isinstance(raw, dict):
                raise RuntimeError("打板专区配置格式无效，已拒绝覆盖")
            result["revision"] = int(raw.get("revision") or 0)
            result["settings"].update(raw.get("settings") or {})
            # 旧版的打板通知开关已废弃，公共提醒统一由监控规则控制。
            result["settings"].pop("notifications", None)
            result["selected"] = [
                item for item in (raw.get("selected") or [])
                if isinstance(item, dict) and item.get("symbol")
            ]
            result["board_pool"] = [
                item for item in (raw.get("board_pool") or [])
                if isinstance(item, dict) and item.get("symbol")
            ]
            result["buy_pool"] = [
                item for item in (raw.get("buy_pool") or [])
                if isinstance(item, dict) and item.get("symbol")
            ]
            return result

    def update(self, revision: int, updater) -> dict[str, Any]:
        with self._lock:
            current = self.load_config()
            if int(revision) != int(current["revision"]):
                raise RevisionConflict(
                    f"配置已更新，请刷新后重试（当前 revision={current['revision']}）"
                )
            value = deepcopy(current)
            updater(value)
            value["schema_version"] = SCHEMA_VERSION
            value["revision"] = int(current["revision"]) + 1
            self._atomic_write(self.config_path, value)
            return deepcopy(value)

    def load_runtime(self) -> dict[str, Any]:
        with self._lock:
            if not self.runtime_path.exists():
                return {"trading_date": None, "symbols": {}, "blacklist": []}
            try:
                value = json.loads(self.runtime_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {"trading_date": None, "symbols": {}, "blacklist": []}
            return value if isinstance(value, dict) else {"trading_date": None, "symbols": {}, "blacklist": []}

    def save_runtime(self, value: dict[str, Any]) -> None:
        with self._lock:
            self._atomic_write(self.runtime_path, value)

    @staticmethod
    def event_identity(value: dict[str, Any]) -> str:
        trading_date = str(value.get("trading_date") or "").strip()
        symbol = str(value.get("symbol") or "").strip().upper()
        event_type = str(value.get("type") or "").strip()
        if not trading_date or not symbol or not event_type:
            return f"legacy:{int(value.get('ts') or 0)}"
        break_count = 0 if event_type == "touched" else int(value.get("break_count") or 0)
        return f"limit_board:{trading_date}:{symbol}:{event_type}:{break_count}"

    def append_event_once(self, value: dict[str, Any]) -> bool:
        identity = self.event_identity(value)
        with self._lock:
            if self.events_path.exists():
                try:
                    with self.events_path.open("r", encoding="utf-8") as stream:
                        for line in stream:
                            try:
                                existing = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if self.event_identity(existing) == identity:
                                return False
                except OSError:
                    pass
            with self.events_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(value, ensure_ascii=False) + "\n")
        return True

    def append_event(self, value: dict[str, Any]) -> None:
        with self._lock, self.events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, ensure_ascii=False) + "\n")

    def events(self, trading_date: str, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock:
            if not self.events_path.exists():
                return []
            rows_by_identity: dict[str, dict[str, Any]] = {}
            try:
                with self.events_path.open("r", encoding="utf-8") as stream:
                    for line in stream:
                        try:
                            value = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if str(value.get("trading_date")) == trading_date:
                            identity = self.event_identity(value)
                            current = rows_by_identity.get(identity)
                            if current is None or int(value.get("ts") or 0) < int(current.get("ts") or 0):
                                rows_by_identity[identity] = value
            except OSError:
                return []
        rows = list(rows_by_identity.values())
        rows.sort(key=lambda item: int(item.get("ts") or 0), reverse=True)
        return rows[: max(1, min(int(limit), 2000))]

    @staticmethod
    def _atomic_write(path: Path, value: dict[str, Any]) -> None:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, path)
