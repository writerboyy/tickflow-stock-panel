"""Daily incremental and weekly audit jobs for the local Tushare gap fill."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import threading
from typing import Any, Callable
from uuid import uuid4

from apscheduler.triggers.cron import CronTrigger

from app.services.tushare_history import (
    BackfillConfig,
    GlobalRateLimiter,
    TushareHistoryBackfill,
    TushareProxyClient,
    load_tushare_key,
)


logger = logging.getLogger(__name__)

INCREMENTAL_JOB_ID = "tushare_gap_fill_incremental"
WEEKLY_AUDIT_JOB_ID = "tushare_gap_fill_weekly_audit"
_AUTOMATED_PHASES = (
    "universe",
    "reference",
    "daily",
    "financials",
    "factors",
    "adjustment",
    "stock_minute",
    "etf_minute",
    "publish_minute",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TushareAutomation:
    """Register Tushare jobs on the application's existing APScheduler."""

    def __init__(
        self,
        data_dir: Path,
        *,
        repo: Any | None = None,
        runner_factory: Callable[[BackfillConfig, Any], Any] = TushareHistoryBackfill,
    ) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.repo = repo
        self.runner_factory = runner_factory
        self.state_path = (
            self.data_dir
            / "backfill_state"
            / "tushare_proxy"
            / "automation_state.json"
        )
        self._lock = threading.Lock()

    def _load_state(self) -> dict[str, Any]:
        try:
            value = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            value = {}
        return {
            "schema_version": 1,
            "consecutive_incremental_successes": int(
                value.get("consecutive_incremental_successes") or 0
            ),
            "last_weekly_audit_healthy_at": value.get("last_weekly_audit_healthy_at"),
            "auto_publish_enabled": bool(value.get("auto_publish_enabled", False)),
            **value,
        }

    def _save_state(self, state: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name(
            f".{self.state_path.name}.{uuid4().hex}.tmp"
        )
        try:
            temporary.write_text(
                json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            os.replace(temporary, self.state_path)
        finally:
            if temporary.exists():
                temporary.unlink()

    @staticmethod
    def _qualify(state: dict[str, Any]) -> None:
        state["auto_publish_enabled"] = bool(
            int(state.get("consecutive_incremental_successes") or 0) >= 2
            and state.get("last_weekly_audit_healthy_at")
        )

    def _client(self) -> TushareProxyClient:
        token = load_tushare_key(data_dir=self.data_dir)
        if not token:
            raise RuntimeError("Tushare automation skipped: API key is not configured")
        return TushareProxyClient(token, limiter=GlobalRateLimiter())

    def start(self, scheduler: Any) -> bool:
        """Attach jobs to the shared scheduler when a local key is available."""
        if scheduler is None or not load_tushare_key(data_dir=self.data_dir):
            logger.info("Tushare automation disabled: shared scheduler or API key unavailable")
            return False
        scheduler.add_job(
            self.run_incremental,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=18,
                minute=40,
                timezone="Asia/Shanghai",
            ),
            id=INCREMENTAL_JOB_ID,
            misfire_grace_time=7200,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        scheduler.add_job(
            self.run_weekly_audit,
            trigger=CronTrigger(
                day_of_week="sun",
                hour=2,
                minute=0,
                timezone="Asia/Shanghai",
            ),
            id=WEEKLY_AUDIT_JOB_ID,
            misfire_grace_time=14400,
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        logger.info("Tushare automation registered on the shared scheduler")
        return True

    def run_incremental(self) -> dict[str, Any]:
        with self._lock:
            state = self._load_state()
            publish = bool(state.get("auto_publish_enabled"))
            today = date.today()
            run_id = f"auto-incremental-{today.isoformat()}"
            config = BackfillConfig(
                data_dir=self.data_dir,
                run_id=run_id,
                phases=_AUTOMATED_PHASES,
                start=today - timedelta(days=10),
                end=today,
                incremental=True,
                publish=publish,
            )
            try:
                result = self.runner_factory(config, self._client()).run()
                success = result.get("status") == "completed"
            except Exception as exc:  # noqa: BLE001
                logger.warning("Tushare incremental failed: %s", type(exc).__name__)
                result = {"status": "failed", "error": type(exc).__name__}
                success = False
            if success:
                if state.get("last_successful_incremental_date") != today.isoformat():
                    state["consecutive_incremental_successes"] = int(
                        state.get("consecutive_incremental_successes") or 0
                    ) + 1
                state["last_successful_incremental_date"] = today.isoformat()
            else:
                state["consecutive_incremental_successes"] = 0
            state["last_incremental_at"] = _utc_now()
            state["last_incremental_status"] = result.get("status")
            state["last_incremental_run_id"] = run_id
            self._qualify(state)
            self._save_state(state)
            if success and publish and self.repo is not None:
                self.repo.rebuild_views()
                self.repo.refresh_cache()
            return {"run": result, "qualification": state}

    def run_weekly_audit(self) -> dict[str, Any]:
        with self._lock:
            state = self._load_state()
            today = date.today()
            run_id = f"auto-audit-{today.isoformat()}"
            config = BackfillConfig(
                data_dir=self.data_dir,
                run_id=run_id,
                phases=("audit",),
                start=date(2010, 1, 1),
                end=today,
            )
            try:
                result = self.runner_factory(config, self._client()).run()
                audit = result.get("dataset_audit") or {}
                healthy = result.get("status") == "completed" and audit.get("status") == "healthy"
            except Exception as exc:  # noqa: BLE001
                logger.warning("Tushare weekly audit failed: %s", type(exc).__name__)
                result = {"status": "failed", "error": type(exc).__name__}
                healthy = False
            state["last_weekly_audit_at"] = _utc_now()
            state["last_weekly_audit_status"] = "healthy" if healthy else "unhealthy"
            state["last_weekly_audit_run_id"] = run_id
            if healthy:
                state["last_weekly_audit_healthy_at"] = state["last_weekly_audit_at"]
            self._qualify(state)
            self._save_state(state)
            return {"run": result, "qualification": state}
