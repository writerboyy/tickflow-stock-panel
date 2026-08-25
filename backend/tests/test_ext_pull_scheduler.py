from __future__ import annotations

import pytest

from app.services.ext_data import ExtConfig, PullConfig
from app.services.ext_pull import PullScheduler


@pytest.mark.asyncio
async def test_scheduler_can_delay_first_pull_in_maintenance_mode(tmp_path, monkeypatch):
    scheduler = PullScheduler()
    config = ExtConfig(
        id="test_source",
        label="test",
        mode="snapshot",
        fields=[],
        pull=PullConfig(
            url="https://example.invalid/data",
            enabled=True,
            schedule_minutes=7,
        ),
    )
    sleeps: list[int] = []

    async def stop_after_initial_delay(seconds: int) -> None:
        sleeps.append(seconds)
        scheduler._running = False

    monkeypatch.setattr("app.services.ext_pull.asyncio.sleep", stop_after_initial_delay)

    scheduler.start(tmp_path, run_immediately=False)
    await scheduler._run_loop(config)

    assert sleeps == [420]
