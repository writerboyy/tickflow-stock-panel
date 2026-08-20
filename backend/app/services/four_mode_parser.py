"""Read-only parser for the archived JoinQuant four-mode strategy.

The strategy is deliberately parsed as source text/AST.  It imports JoinQuant
and QMT modules that are not an equivalent runtime dependency of the limit-board
workspace, so this module must never import or execute the strategy itself.
"""
from __future__ import annotations

import ast
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any

from app.market_time import CN_TZ


_SOURCE_RELATIVE_PATH = Path("docs") / "聚宽策略" / "四合一打板.py"

_MODE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "id": "yje",
        "name": "一进二",
        "summary": "从昨日涨停股中筛选次日可能晋级二板的竞价强势标的。",
        "runtime": "竞价确认 + 委比/盘口买入确认",
        "functions": (
            "calculate_yje_score",
            "finalize_yje_stock_list",
            "check_weibi_signal",
            "buy_yje_mode",
        ),
        "state_prefixes": ("yje_",),
        "config_keys": ("yje_max_positions", "weibi_threshold"),
    },
    {
        "id": "rzq",
        "name": "弱转强",
        "summary": "从炸板和近期强势形态中寻找由弱转强的早盘候选。",
        "runtime": "竞价过滤 + 分钟级盘中买入/卖出检查",
        "functions": (
            "finalize_rzq_stock_list",
            "handle_rzq_qs_sb_buy",
            "check_compatibility_buy",
        ),
        "state_prefixes": ("rzq_",),
        "config_keys": ("priority_config",),
    },
    {
        "id": "qs",
        "name": "趋势股",
        "summary": "在中证 1000 成分范围内按均线、动量、ATR 和概念组合评分。",
        "runtime": "日线趋势筛选 + ATR 止损与持有日管理",
        "functions": (
            "prepare_qs_stock_list",
            "finalize_qs_stock_list",
            "get_qs_hold_days_and_target",
            "log_qs_daily_status",
        ),
        "state_prefixes": ("qs_",),
        "config_keys": (
            "qs_max_ratio",
            "qs_max_count",
            "qs_max_hold_days",
            "qs_atr_period",
            "qs_atr_multiplier",
        ),
    },
    {
        "id": "sb",
        "name": "首板",
        "summary": "先做历史、估值和流动性预选，再用盘中三维信号确认首板。",
        "runtime": "盘前静态池 + 触板/均价线/日内强度确认",
        "functions": (
            "get_sb_candidate_pool",
            "prepare_sb_stock_list",
            "finalize_sb_stock_list",
            "check_sb_weibi_signal",
        ),
        "state_prefixes": ("sb_",),
        "config_keys": ("sb_max_positions", "sb_buy_max_positions", "sb_max_ratio"),
    },
)

_SCHEDULE_DEFINITIONS: tuple[dict[str, str], ...] = (
    {"time": "09:05:00", "function": "prepare_stock_candidates", "description": "昨日数据完成四模式静态预选"},
    {"time": "09:24:00", "function": "check_market_risk_microstructure", "description": "盘前市场微观结构风控"},
    {"time": "09:25:45", "function": "get_stock_list", "description": "最终竞价数据确认并发布候选池"},
    {"time": "09:27:00", "function": "sell_check_open", "description": "集合竞价后卖出检查"},
    {"time": "09:28:00", "function": "buy", "description": "按模式优先级启动买入流水线"},
    {"time": "10:00:00", "function": "sell_30min_check", "description": "开盘 30 分钟卖出检查"},
    {"time": "11:30:00", "function": "midday_sentiment_update", "description": "午间情绪和模式优先级更新"},
    {"time": "14:30:00", "function": "sell_check_afternoon", "description": "午后卖出检查"},
    {"time": "15:01:00", "function": "log_qs_daily_status", "description": "收盘记录趋势股状态"},
    {"time": "15:02:00", "function": "print_daily_audit", "description": "日终资产与持仓审计"},
    {"time": "15:05:00", "function": "daily_reset", "description": "重置日内状态"},
)

_DEPENDENCY_NOTES: dict[str, tuple[str, bool, str]] = {
    "jqdata": ("聚宽行情、证券目录与订单 API", False, "系统没有等价的聚宽运行时契约"),
    "jqfactor": ("VOL5 等聚宽因子", False, "因子口径和 PIT 覆盖尚未验证"),
    "jqlib.technical_analysis": ("聚宽技术分析辅助模块", False, "未接入兼容实现"),
    "talib": ("ATR 等技术指标", False, "当前后端未形成可验证的 TA-Lib 技术指标契约"),
    "get_current_tick": ("竞价、盘口和 Tick", False, "历史 K 线不能替代实时盘口"),
    "get_index_stocks": ("中证 1000 历史成分", False, "趋势股模式需要按日期的 PIT 成分快照"),
    "jqmt": ("可选 QMT 实盘订单桥", False, "源码 ENABLE_LIVE_TRADING 默认关闭，系统委托不由源码直推"),
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _function_lines(tree: ast.AST) -> dict[str, int]:
    return {
        node.name: node.lineno
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _config_values(tree: ast.AST) -> dict[str, Any]:
    values: dict[str, Any] = {}
    config_functions = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_apply_config"
    ]
    nodes = ast.walk(config_functions[0]) if config_functions else ast.walk(tree)
    assignments = sorted(
        (node for node in nodes if isinstance(node, ast.Assign)),
        key=lambda node: node.lineno,
    )
    for node in assignments:
        for target in node.targets:
            if not isinstance(target, ast.Attribute) or not isinstance(target.value, ast.Name):
                continue
            if target.value.id != "g" or (
                target.attr != "priority_config"
                and not target.attr.endswith((
                    "_time", "_count", "_ratio", "_period", "_multiplier",
                    "_positions", "_threshold", "_days",
                ))
            ):
                continue
            try:
                values[target.attr] = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
    return values


def _state_fields(tree: ast.AST) -> list[str]:
    fields: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute) or not isinstance(node.value, ast.Name) or node.value.id != "g":
            continue
        if node.attr.startswith(("yje_", "rzq_", "qs_", "sb_")):
            fields.add(node.attr)
    return sorted(fields)


def parse_four_mode_strategy(source_path: Path | None = None) -> dict[str, Any]:
    """Return a JSON-safe, execution-free report for the archived strategy."""
    path = source_path or (_repo_root() / _SOURCE_RELATIVE_PATH)
    if not path.exists():
        return {
            "state": "unavailable",
            "reason": f"策略源码不存在：{_SOURCE_RELATIVE_PATH.as_posix()}",
            "source": {"path": _SOURCE_RELATIVE_PATH.as_posix()},
            "modes": [],
            "schedule": [],
            "dependencies": [],
            "state_fields": [],
        }
    try:
        raw = path.read_bytes()
        tree = ast.parse(raw.decode("utf-8"), filename=str(path))
    except (OSError, UnicodeDecodeError, SyntaxError) as exc:
        return {
            "state": "unavailable",
            "reason": f"策略源码解析失败：{exc}",
            "source": {"path": _SOURCE_RELATIVE_PATH.as_posix()},
            "modes": [],
            "schedule": [],
            "dependencies": [],
            "state_fields": [],
        }

    functions = _function_lines(tree)
    configs = _config_values(tree)
    state_fields = _state_fields(tree)
    modes = []
    for definition in _MODE_DEFINITIONS:
        modes.append({
            **definition,
            "functions": [
                {"name": name, "line": functions.get(name)}
                for name in definition["functions"]
                if name in functions
            ],
            "config": [
                {"key": key, "value": configs[key]}
                for key in definition["config_keys"]
                if key in configs
            ],
            "state_fields": [
                field for field in state_fields
                if any(field.startswith(prefix) for prefix in definition["state_prefixes"])
            ],
        })

    imported = {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    imported.update(
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    referenced_names = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    }
    dependencies = []
    for name, (kind, available, note) in _DEPENDENCY_NOTES.items():
        dependencies.append({
            "name": name,
            "kind": kind,
            "available": available,
            "referenced": (
                name in imported
                or name in referenced_names
                or any(name in item for item in imported)
            ),
            "note": note,
        })
    return {
        "state": "available",
        "reason": "仅完成源码结构解析；未执行 JoinQuant/QMT 策略，也未生成实时候选。",
        "source": {
            "path": _SOURCE_RELATIVE_PATH.as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "title": "年华90+回撤9四模式策略一进二、弱转强、趋势股、首板",
            "source_url": "https://www.joinquant.com/post/78265",
            "parsed_at": datetime.now(CN_TZ).isoformat(),
        },
        "modes": modes,
        "schedule": list(_SCHEDULE_DEFINITIONS),
        "dependencies": dependencies,
        "state_fields": state_fields,
        "live_trading_enabled": False,
        "execution_state": "read_only",
    }
