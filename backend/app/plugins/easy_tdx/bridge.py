"""EasyTDX 可选依赖检测。"""

from __future__ import annotations

import importlib.util


INSTALL_HINT = "未安装 easy-tdx，运行: uv sync --extra easy-tdx"


def availability() -> tuple[bool, str]:
    try:
        installed = importlib.util.find_spec("easy_tdx") is not None
    except (ImportError, ValueError):
        installed = False
    return (True, "ok") if installed else (False, INSTALL_HINT)
