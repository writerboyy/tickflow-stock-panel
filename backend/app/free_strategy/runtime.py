"""Strategy source runtimes sharing the canonical FreeStrategyEngine."""
from __future__ import annotations

import copy
from typing import Any, Protocol


class LoadedRuntime(Protocol):
    dialect: str
    callbacks: dict[str, Any]
    compatibility_report: dict[str, Any]

    def load(self) -> None: ...

    def serialize_state(self, state: dict[str, Any]) -> dict[str, Any]: ...

    def restore_state(self, state: dict[str, Any]) -> dict[str, Any]: ...

    def runtime_snapshot(self) -> dict[str, Any]: ...


class NativeRuntime:
    dialect = "native"

    def __init__(self, engine: Any, source: str) -> None:
        self.engine = engine
        self.source = source
        self.callbacks: dict[str, Any] = {}
        self.compatibility_report = {
            "version": None,
            "dialect": self.dialect,
            "summary_status": "supported",
            "apis": [],
        }

    def load(self) -> None:
        namespace: dict[str, Any] = {
            "__name__": "free_strategy_snapshot",
            "print": self.engine._strategy_print,  # noqa: SLF001
            "run_daily": self.engine.context.run_daily,
            "run_weekly": self.engine.context.run_weekly,
            "run_monthly": self.engine.context.run_monthly,
            "unschedule_all": self.engine.context.unschedule_all,
        }
        exec(compile(self.source, "<free_strategy>", "exec"), namespace, namespace)
        callback_names = (
            "initialize",
            "before_trading_start",
            "on_bar",
            "on_quote",
            "after_trading_end",
        )
        self.callbacks = {
            name: namespace[name]
            for name in callback_names
            if callable(namespace.get(name))
        }

    @staticmethod
    def serialize_state(state: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(state)

    @staticmethod
    def restore_state(state: dict[str, Any]) -> dict[str, Any]:
        return state

    def runtime_snapshot(self) -> dict[str, Any]:
        return {"dialect": self.dialect, "compatibility_version": None}


def create_runtime(dialect: str, engine: Any, source: str) -> LoadedRuntime:
    normalized = str(dialect or "native").strip().lower()
    if normalized == "native":
        return NativeRuntime(engine, source)
    if normalized == "joinquant":
        from .jq_compat.runtime import JoinQuantRuntime

        return JoinQuantRuntime(engine, source)
    raise ValueError(f"不支持的策略运行方言: {dialect}")
