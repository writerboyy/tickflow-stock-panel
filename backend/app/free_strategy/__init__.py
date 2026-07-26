"""独立自由策略运行时。

自由策略故意不复用 ``app.backtest`` 的矩阵信号模型。这里的公共核心只依赖
bar、策略源码和一个可持久化的账户状态，因此历史回测和模拟盘可以共享撮合规则。
"""

from .bars import Bar, aggregate_minute_bars, group_bars
from .engine import FreeStrategyConfig, FreeStrategyEngine

__all__ = ["Bar", "aggregate_minute_bars", "group_bars", "FreeStrategyConfig", "FreeStrategyEngine"]
