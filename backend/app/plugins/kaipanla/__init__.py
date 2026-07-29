"""开盘啦辅助扩展数据采集插件。"""

from app.plugins.kaipanla.collector import KaipanlaCollector
from app.plugins.kaipanla.storage import ensure_configs

__all__ = ["KaipanlaCollector", "ensure_configs"]
