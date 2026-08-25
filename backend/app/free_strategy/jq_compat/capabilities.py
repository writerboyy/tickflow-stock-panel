"""Static capability report for JoinQuant strategy source.

The report is intentionally conservative.  A source that names an unavailable
JoinQuant capability is saved with diagnostics, but cannot start a backtest or
paper account until that capability has a canonical TickFlow implementation.
"""
from __future__ import annotations

import ast
from hashlib import sha256
from typing import Any


COMPATIBILITY_VERSION = "jq-v1"

_API_CAPABILITIES: dict[str, tuple[str, str]] = {
    "attribute_history": ("emulated", "基于当前回测截止时点的历史 K 线返回 pandas 数据"),
    "after_trading_end": ("supported", "映射到系统盘后生命周期"),
    "before_trading_start": ("supported", "映射到系统盘前生命周期"),
    "get_all_securities": ("emulated", "使用系统已加载的标的目录"),
    "get_current_data": ("emulated", "使用当前 bar 或报价快照，不包含盘口"),
    "get_price": ("emulated", "基于系统历史 K 线返回 pandas 数据"),
    "get_security_info": ("emulated", "使用系统已加载的证券元数据"),
    "g": ("supported", "状态保存在检查点，可由模拟盘恢复"),
    "handle_data": ("supported", "映射到系统完整 bar 回放事件"),
    "history": ("emulated", "基于系统历史 K 线返回 pandas 数据"),
    "initialize": ("supported", "映射到系统初始化生命周期"),
    "order": ("supported", "映射到系统订单、风控和撮合"),
    "order_target": ("supported", "映射到系统订单、风控和撮合"),
    "order_target_percent": ("supported", "映射到系统订单、风控和撮合"),
    "order_target_value": ("supported", "映射到系统订单、风控和撮合"),
    "order_value": ("supported", "映射到系统订单、风控和撮合"),
    "process_initialize": ("emulated", "在每次运行时初始化阶段调用"),
    "run_daily": ("emulated", "映射到系统交易日调度"),
    "run_monthly": ("emulated", "映射到系统交易日调度"),
    "run_weekly": ("emulated", "映射到系统交易日调度"),
    "set_benchmark": ("supported", "更新系统基准标的"),
    "set_order_cost": ("emulated", "映射可表达的佣金、印花税和最低佣金"),
    "set_slippage": ("emulated", "映射 FixedSlippage 或 PriceRelatedSlippage 到双边滑点"),
    "unschedule_all": ("supported", "清除系统已注册调度"),
}

_UNAVAILABLE_CAPABILITIES: dict[str, str] = {
    "after_code_changed": "代码热更新生命周期没有等价运行环境",
    "get_concept": "缺少 PIT 概念历史数据契约",
    "get_current_tick": "当前系统没有逐笔或五档快照",
    "get_factor_values": "缺少可验证的因子历史数据契约",
    "get_fundamentals": "查询 DSL 与 PIT 字段映射尚未实现",
    "get_industry": "缺少 PIT 行业分类的聚宽对象映射",
    "get_index_stocks": "缺少已接入的 PIT 指数成分 loader",
    "get_industry_stocks": "缺少已接入的 PIT 行业成分 loader",
    "get_money_flow": "缺少同口径的 PIT 资金流历史",
    "get_valuation": "缺少 PIT 估值字段的聚宽 DataFrame 映射",
    "get_ticks": "当前系统没有 Tick/L2 历史数据",
    "jqfactor": "因子模块依赖未接入的 PIT 因子数据",
    "jqmt": "策略源码不得绕过系统订单、风控和 QMT 网关",
    "jqlib.technical_analysis": "聚宽技术指标模块尚未映射到系统指标契约",
    "query": "聚宽财务查询 DSL 尚未实现",
}


class JoinQuantCapabilityError(ValueError):
    """Raised before execution when a strategy requires an unavailable API."""

    def __init__(self, report: dict[str, Any]) -> None:
        self.report = report
        blocked = [item["name"] for item in report.get("apis", []) if item["status"] == "unavailable"]
        super().__init__(f"聚宽策略包含当前不可用能力: {', '.join(blocked)}")


def _call_name(node: ast.Call, module_aliases: set[str]) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        if isinstance(node.func.value, ast.Name) and node.func.value.id in module_aliases:
            return node.func.attr
    return None


def _literal_argument(call: ast.Call, name: str, position: int | None = None) -> Any:
    for keyword in call.keywords:
        if keyword.arg == name and isinstance(keyword.value, ast.Constant):
            return keyword.value.value
    if position is not None and len(call.args) > position and isinstance(call.args[position], ast.Constant):
        return call.args[position].value
    return None


def _schedule_time(call: ast.Call, name: str) -> str | None:
    positions = {"run_daily": 1, "run_weekly": 2, "run_monthly": 2}
    value = _literal_argument(call, "time", positions.get(name))
    return value if isinstance(value, str) else None


def _has_second_precision(value: str) -> bool:
    parts = value.strip().split(":")
    return len(parts) == 3 and parts[-1] not in {"", "00"}


def analyze_source(source: str) -> dict[str, Any]:
    """Return a deterministic report without executing untrusted source."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {
            "version": COMPATIBILITY_VERSION,
            "dialect": "joinquant",
            "source_sha256": sha256(source.encode("utf-8")).hexdigest(),
            "summary_status": "unavailable",
            "apis": [{
                "name": "syntax",
                "status": "unavailable",
                "detail": f"源码语法错误: {exc.msg}",
            }],
        }

    found: dict[str, tuple[str, str]] = {}
    module_aliases = {"jqdata", "jqfactor"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Import):
            continue
        for alias in node.names:
            if alias.name in {"jqdata", "jqfactor"}:
                module_aliases.add(alias.asname or alias.name)

    def add(name: str, status: str, detail: str) -> None:
        previous = found.get(name)
        priority = {"supported": 0, "emulated": 1, "degraded": 2, "unavailable": 3}
        if previous is None or priority[status] > priority[previous[0]]:
            found[name] = (status, detail)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "jqdata":
                    add("jqdata", "supported", "提供核心聚宽 API 门面")
                elif alias.name == "jqfactor" or alias.name.startswith("jqfactor."):
                    add("jqfactor", "unavailable", _UNAVAILABLE_CAPABILITIES["jqfactor"])
                elif alias.name == "jqmt" or alias.name.startswith("jqmt."):
                    add("jqmt", "unavailable", _UNAVAILABLE_CAPABILITIES["jqmt"])
                elif alias.name == "jqlib" or alias.name.startswith("jqlib."):
                    add(
                        "jqlib.technical_analysis",
                        "unavailable",
                        _UNAVAILABLE_CAPABILITIES["jqlib.technical_analysis"],
                    )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "jqdata":
                add("jqdata", "supported", "提供核心聚宽 API 门面")
                for alias in node.names:
                    if alias.name in _API_CAPABILITIES:
                        add(alias.name, *_API_CAPABILITIES[alias.name])
                    elif alias.name in _UNAVAILABLE_CAPABILITIES:
                        add(alias.name, "unavailable", _UNAVAILABLE_CAPABILITIES[alias.name])
            elif module == "jqfactor":
                add("jqfactor", "unavailable", _UNAVAILABLE_CAPABILITIES["jqfactor"])
            elif module == "jqlib" or module.startswith("jqlib."):
                add(
                    "jqlib.technical_analysis",
                    "unavailable",
                    _UNAVAILABLE_CAPABILITIES["jqlib.technical_analysis"],
                )
            elif module == "jqmt" or module.startswith("jqmt."):
                add("jqmt", "unavailable", _UNAVAILABLE_CAPABILITIES["jqmt"])
        elif isinstance(node, ast.FunctionDef) and node.name in {"initialize", "handle_data", "before_trading_start", "after_trading_end", "process_initialize", "after_code_changed"}:
            if node.name in _API_CAPABILITIES:
                add(node.name, *_API_CAPABILITIES[node.name])
            elif node.name in _UNAVAILABLE_CAPABILITIES:
                add(node.name, "unavailable", _UNAVAILABLE_CAPABILITIES[node.name])
        elif isinstance(node, ast.Name) and node.id == "g":
            add("g", *_API_CAPABILITIES["g"])
        elif isinstance(node, ast.Call):
            name = _call_name(node, module_aliases)
            if name in _API_CAPABILITIES:
                add(name, *_API_CAPABILITIES[name])
            elif name in _UNAVAILABLE_CAPABILITIES:
                add(name, "unavailable", _UNAVAILABLE_CAPABILITIES[name])
            if name in {"run_daily", "run_weekly", "run_monthly"}:
                schedule_time = _schedule_time(node, name)
                if schedule_time and _has_second_precision(schedule_time):
                    add(
                        "second_precision_schedule",
                        "supported",
                        "保留 HH:MM:SS，并由回测与实时 Quote 事件使用实际秒级时间触发",
                    )
                if _literal_argument(node, "reference_security"):
                    add(
                        "reference_security_calendar",
                        "unavailable",
                        "系统尚未按 reference_security 维护独立交易日历",
                    )
            if name == "get_price":
                fq = _literal_argument(node, "fq")
                if isinstance(fq, str) and fq.lower() == "post":
                    add("post_adjustment", "unavailable", "系统当前没有后复权历史数据契约")

    apis = [
        {"name": name, "status": status, "detail": detail}
        for name, (status, detail) in sorted(found.items())
    ]
    statuses = {item["status"] for item in apis}
    summary_status = (
        "unavailable" if "unavailable" in statuses
        else "degraded" if "degraded" in statuses
        else "supported"
    )
    return {
        "version": COMPATIBILITY_VERSION,
        "dialect": "joinquant",
        "source_sha256": sha256(source.encode("utf-8")).hexdigest(),
        "summary_status": summary_status,
        "apis": apis,
    }


def ensure_executable(report: dict[str, Any]) -> None:
    if any(item.get("status") == "unavailable" for item in report.get("apis", [])):
        raise JoinQuantCapabilityError(report)
