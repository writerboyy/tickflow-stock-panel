from __future__ import annotations

import json

from app.services.tushare_automation import (
    INCREMENTAL_JOB_ID,
    WEEKLY_AUDIT_JOB_ID,
    TushareAutomation,
)
from app.services.tushare_history import save_tushare_key


class _Scheduler:
    def __init__(self) -> None:
        self.jobs = []

    def add_job(self, func, **kwargs) -> None:
        self.jobs.append((func, kwargs))


class _RunnerFactory:
    def __init__(self, results):
        self.results = list(results)
        self.configs = []

    def __call__(self, config, _client):
        self.configs.append(config)
        result = self.results.pop(0)

        class Runner:
            def run(self):
                return result

        return Runner()


def test_automation_reuses_shared_scheduler_and_requires_key(tmp_path):
    scheduler = _Scheduler()
    automation = TushareAutomation(tmp_path)
    assert automation.start(scheduler) is False
    assert scheduler.jobs == []

    save_tushare_key("secret", data_dir=tmp_path)
    assert automation.start(scheduler) is True
    assert {item[1]["id"] for item in scheduler.jobs} == {
        INCREMENTAL_JOB_ID,
        WEEKLY_AUDIT_JOB_ID,
    }


def test_automation_enables_publish_after_two_incrementals_and_healthy_audit(tmp_path):
    save_tushare_key("secret", data_dir=tmp_path)
    factory = _RunnerFactory([
        {"status": "completed"},
        {"status": "completed"},
        {"status": "completed", "dataset_audit": {"status": "healthy"}},
        {"status": "completed"},
    ])
    automation = TushareAutomation(tmp_path, runner_factory=factory)

    automation.run_incremental()
    state = json.loads(automation.state_path.read_text())
    state["last_successful_incremental_date"] = "2000-01-01"
    automation._save_state(state)
    second = automation.run_incremental()
    assert second["qualification"]["auto_publish_enabled"] is False
    audited = automation.run_weekly_audit()
    assert audited["qualification"]["auto_publish_enabled"] is True
    automation.run_incremental()

    assert [config.publish for config in factory.configs] == [False, False, False, True]
    state = json.loads(automation.state_path.read_text())
    assert state["consecutive_incremental_successes"] == 2


def test_failed_incremental_resets_consecutive_successes(tmp_path):
    save_tushare_key("secret", data_dir=tmp_path)
    factory = _RunnerFactory([{"status": "completed"}, {"status": "incomplete"}])
    automation = TushareAutomation(tmp_path, runner_factory=factory)

    automation.run_incremental()
    result = automation.run_incremental()

    assert result["qualification"]["consecutive_incremental_successes"] == 0
    assert result["qualification"]["auto_publish_enabled"] is False
