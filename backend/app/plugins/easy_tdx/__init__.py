"""EasyTDX 行业维度辅助采集插件。"""

from app.plugins.easy_tdx.collector import EasyTdxCollector
from app.plugins.easy_tdx.storage import ensure_config

__all__ = ["EasyTdxCollector", "ensure_config"]
