"""告警触发记录存储 — JSONL 追加写 + 滚动清理。

职责:
  - 把每次触发的 AlertEvent 追加写入 data/user_data/alerts.jsonl
  - 提供查询 (按来源/类型过滤、时间倒序、限量)
  - 滚动清理: 保留近 N 天 + 上限 M 条 (取交集)

设计:
  - JSONL 每行一个 JSON 对象,便于增量追加和流式读取
  - 清理策略: 追加后按需 prune (按 ts 删旧),避免文件无限膨胀
  - 读时全量加载到内存过滤 (记录量受上限约束, 5000 条量级无压力)
"""
from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# 保留策略
MAX_DAYS = 7
MAX_RECORDS = 5000
# 每隔多少次写入触发一次清理 (避免每次写都 prune)
PRUNE_EVERY = 20

# 监控中心的公共告警只能来自 MonitorRuleEngine。
# position_risk、limit_board 等领域服务保留各自的事件存储/时间线，不能混入公共监控记录。
MONITOR_RULE_SOURCES = frozenset({
    "strategy", "signal", "price", "market", "ladder", "sector",
})

_lock = threading.Lock()
_write_count = 0


def _path(data_dir: Path) -> Path:
    p = data_dir / "user_data" / "alerts.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def append(data_dir: Path, event: dict) -> None:
    """追加一条触发记录。event 应含 ts(毫秒)、rule_id、source 等字段。"""
    line = json.dumps(event, ensure_ascii=False)
    with _lock:
        p = _path(data_dir)
        with p.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        global _write_count
        _write_count += 1
        if _write_count >= PRUNE_EVERY:
            _write_count = 0
            _prune_locked(p)


def append_many(data_dir: Path, events: list[dict]) -> None:
    """批量追加。"""
    if not events:
        return
    with _lock:
        p = _path(data_dir)
        with p.open("a", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        global _write_count
        _write_count += len(events)
        if _write_count >= PRUNE_EVERY:
            _write_count = 0
            _prune_locked(p)


def is_monitor_rule_event(event: dict) -> bool:
    """判断事件是否属于监控规则引擎可写入监控中心的来源。"""
    rule_id = str(event.get("rule_id") or "").strip()
    return bool(rule_id) and event.get("source") in MONITOR_RULE_SOURCES


def list_recent(
    data_dir: Path,
    days: int = MAX_DAYS,
    limit: int = MAX_RECORDS,
    source: str | None = None,
    type: str | None = None,
) -> list[dict]:
    """读取近 N 天记录,按时间倒序,支持按 source/type 过滤。

    持锁读: prune/delete/clear 会整文件重写, 无锁读可能读到截断内容。
    """
    import time
    cutoff = (time.time() - days * 86400) * 1000  # 毫秒
    out: list[dict] = []
    p = _path(data_dir)
    if not p.exists():
        return []
    try:
        with _lock, p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("ts", 0) < cutoff:
                    continue
                if source and ev.get("source") != source:
                    continue
                if type and ev.get("type") != type:
                    continue
                out.append(ev)
    except Exception as e:
        logger.warning("alert_store read failed: %s", e)
        return []
    # 时间倒序 + 截断
    out.sort(key=lambda x: x.get("ts", 0), reverse=True)
    return out[:limit]


def list_monitor_events(
    data_dir: Path,
    days: int = MAX_DAYS,
    limit: int = MAX_RECORDS,
    source: str | None = None,
    type: str | None = None,
) -> list[dict]:
    """读取监控规则引擎产生的记录，供公共监控中心使用。"""
    events = list_recent(data_dir, days=days, limit=MAX_RECORDS, source=source, type=type)
    filtered = [event for event in events if is_monitor_rule_event(event)]
    return filtered[:limit]


def clear(data_dir: Path) -> int:
    """清空全部记录,返回清除的条数。"""
    with _lock:
        p = _path(data_dir)
        if not p.exists():
            return 0
        count = 0
        try:
            with p.open("r", encoding="utf-8") as f:
                count = sum(1 for line in f if line.strip())
        except Exception:
            pass
        p.write_text("", encoding="utf-8")
        return count


def clear_monitor(data_dir: Path) -> int:
    """清空监控规则记录，保留其它领域服务的独立事件。"""
    with _lock:
        p = _path(data_dir)
        if not p.exists():
            return 0
        kept: list[dict] = []
        cleared = 0
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except Exception:
                        continue
                    if is_monitor_rule_event(event):
                        cleared += 1
                    else:
                        kept.append(event)
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store clear_monitor read failed: %s", exc)
            return 0
        try:
            with p.open("w", encoding="utf-8") as f:
                for event in kept:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store clear_monitor write failed: %s", exc)
            return 0
        return cleared


def delete_one(data_dir: Path, ts: int) -> bool:
    """删除指定 ts 的单条记录,返回是否删除成功。

    JSONL 无主键, 用 ts(毫秒时间戳) 作为标识。
    若存在多条同 ts, 只删第一条。
    """
    with _lock:
        p = _path(data_dir)
        if not p.exists():
            return False
        kept: list[dict] = []
        deleted = False
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except Exception:
                        continue
                    if not deleted and ev.get("ts") == ts:
                        deleted = True
                        continue
                    kept.append(ev)
        except Exception as e:
            logger.warning("alert_store delete_one read failed: %s", e)
            return False
        if not deleted:
            return False
        try:
            with p.open("w", encoding="utf-8") as f:
                for ev in kept:
                    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning("alert_store delete_one write failed: %s", e)
            return False
        return True


def delete_monitor_one(data_dir: Path, ts: int) -> bool:
    """删除指定时间戳的监控规则记录，不触碰其它领域事件。"""
    with _lock:
        p = _path(data_dir)
        if not p.exists():
            return False
        kept: list[dict] = []
        deleted = False
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except Exception:
                        continue
                    if not deleted and event.get("ts") == ts and is_monitor_rule_event(event):
                        deleted = True
                        continue
                    kept.append(event)
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store delete_monitor_one read failed: %s", exc)
            return False
        if not deleted:
            return False
        try:
            with p.open("w", encoding="utf-8") as f:
                for event in kept:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store delete_monitor_one write failed: %s", exc)
            return False
        return True


def sanitize_position_risk_events(data_dir: Path) -> int:
    """Remove legacy score/recommendation/evidence fields from position-risk events."""
    with _lock:
        p = _path(data_dir)
        if not p.exists():
            return 0
        removed = 0
        changed = False
        kept: list[dict] = []
        fields = {
            "risk_score", "risk_level", "reasons", "source_ids", "signals",
            "conditions", "logic", "evidence", "evidence_coverage",
        }
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                    except Exception:
                        logger.warning("alert_store position-risk migration found malformed JSON; keeping file unchanged")
                        return 0
                    if event.get("source") == "position_risk":
                        if "suggestion_pct" in event:
                            event["action_pct"] = event.pop("suggestion_pct")
                            changed = True
                        for field in fields:
                            if field in event:
                                event.pop(field, None)
                                removed += 1
                                changed = True
                    kept.append(event)
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store position-risk migration read failed: %s", exc)
            return 0
        if not changed:
            return 0
        temporary = p.with_name(f"{p.name}.position-risk-migration.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as f:
                for event in kept:
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
            os.replace(temporary, p)
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert_store position-risk migration write failed: %s", exc)
            return 0
        return removed


def count(data_dir: Path) -> int:
    """返回当前记录总数。持锁读, 防与整文件重写并发。"""
    p = _path(data_dir)
    if not p.exists():
        return 0
    try:
        with _lock, p.open("r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


def count_monitor(data_dir: Path) -> int:
    """返回监控规则记录总数。"""
    p = _path(data_dir)
    if not p.exists():
        return 0
    try:
        with _lock, p.open("r", encoding="utf-8") as f:
            return sum(
                1
                for line in f
                if line.strip() and _is_monitor_json_line(line)
            )
    except Exception:
        return 0


def _is_monitor_json_line(line: str) -> bool:
    try:
        return is_monitor_rule_event(json.loads(line))
    except Exception:
        return False


def _prune_locked(p: Path) -> None:
    """(调用方需持锁) 保留近 MAX_DAYS 天 + 上限 MAX_RECORDS 条。"""
    import time
    cutoff = (time.time() - MAX_DAYS * 86400) * 1000
    kept: list[dict] = []
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("ts", 0) >= cutoff:
                    kept.append(ev)
    except FileNotFoundError:
        return
    except Exception as e:
        logger.warning("alert_store prune read failed: %s", e)
        return
    # 上限截断 (保留最新的)
    if len(kept) > MAX_RECORDS:
        kept.sort(key=lambda x: x.get("ts", 0))
        kept = kept[-MAX_RECORDS:]
    # 重写文件
    try:
        with p.open("w", encoding="utf-8") as f:
            for ev in kept:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("alert_store prune write failed: %s", e)
