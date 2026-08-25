import json
import time
from types import SimpleNamespace

from app.services import alert_store
from app.services.quote_service import QuoteService


def _write_events(data_dir, events):
    path = data_dir / "user_data" / "alerts.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events),
        encoding="utf-8",
    )


def _recent_events(*sources):
    now = int(time.time() * 1000)
    return [
        {
            "ts": now - index,
            "rule_id": f"rule-{index}",
            "source": source,
            "type": source,
        }
        for index, source in enumerate(sources)
    ]


def test_monitor_queries_exclude_domain_events(tmp_path):
    events = _recent_events("market", "limit_board", "position_risk")
    events.append({"ts": events[0]["ts"] - 10, "source": "market", "type": "market"})
    _write_events(tmp_path, events)

    assert [event["source"] for event in alert_store.list_monitor_events(tmp_path)] == ["market"]
    assert alert_store.count_monitor(tmp_path) == 1


def test_monitor_clear_and_delete_preserve_domain_events(tmp_path):
    events = _recent_events("market", "limit_board", "signal")
    _write_events(tmp_path, events)

    assert alert_store.delete_monitor_one(tmp_path, events[1]["ts"]) is False
    assert alert_store.delete_monitor_one(tmp_path, events[0]["ts"]) is True
    assert alert_store.clear_monitor(tmp_path) == 1
    assert [event["source"] for event in alert_store.list_recent(tmp_path)] == ["limit_board"]


def test_public_push_requires_an_active_monitor_rule():
    service = QuoteService.__new__(QuoteService)
    service._app_state = SimpleNamespace(
        monitor_engine=SimpleNamespace(rules={"active-market-rule": {}}),
    )
    broadcast = []
    service._broadcast_alerts = broadcast.extend

    service.push_alerts([
        {"source": "market", "rule_id": "active-market-rule", "message": "ok"},
        {"source": "market", "rule_id": "missing-rule", "message": "blocked"},
        {"source": "depth", "rule_id": "active-market-rule", "message": "blocked"},
        {"source": "limit_board", "rule_id": "active-market-rule", "message": "blocked"},
    ])

    assert [event["message"] for event in broadcast] == ["ok"]
