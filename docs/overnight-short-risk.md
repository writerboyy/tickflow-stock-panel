# 隔日短线止盈止损

隔日短线预设面向 A 股 T+1 持仓，默认只写入保守参数，所有 `auto_execute` 均为 `false`。启用真实委托前应逐条确认规则、执行比例、可用数量和 QMT 状态。

## 参数口径

- 初始止损支持 `fixed`、`atr`、`max_fixed_atr`。预设使用 `max_fixed_atr`，在固定亏损价和 `成本价 - ATR14 × 倍数` 中取更高保护价。实际初始保护价定义为 `1R`，后续保护价只能上移。
- 分段止盈默认在 `1R` 减仓 30%，将保护价抬到成本加费用滑点缓冲；`1.5R` 再减仓 30%，至少锁定 `0.5R`；`2R` 后剩余仓位使用 ATR/Chandelier 跟踪。
- 盘中冲高回落只有在达到启动收益后才记录盘中高点回撤，要求闭合行情持续确认；断线恢复的第一条报价只建立基线。
- 次日跳空低开、高开止盈和 5/15 分钟开盘区间失败只使用已经闭合的 1 分钟 K 线。跳空规则等待首个有效分钟确认，不把跌停价或过期行情当成交。
- T+1 按交易日计算。`entry_date` 优先来自 QMT 成交记录，手工导入可补填；缺少建仓日时只显示数据不足，禁止自动清仓。

## A 股交易边界

自动委托只能卖出，数量按可用持仓向下取整为 100 股/份整手，并复用行情新鲜度、跌停、QMT 交易开关和幂等门禁。`accepted_pending` 是 QMT 受理状态，不是成交保证；超时或未知状态不会自动重发。涨停、跌停、停牌、无可用数量和行情中断都可能导致规则触发但无法成交。

## 研究与实现参考

- Zambelli (2016), [Determining Optimal Stop-Loss Thresholds](https://arxiv.org/abs/1609.00869)：阈值应由历史回撤分布校准。
- Leung & Li (2015), [transaction costs and stop-loss exit](https://arxiv.org/abs/1411.5062)：费用和滑点应纳入退出优化。
- He & Zhi (2011), [optimal stopping](https://arxiv.org/abs/1103.1755)：亏损截断、分段兑现和剩余仓位跟踪。
- [Freqtrade stoploss](https://github.com/freqtrade/freqtrade/blob/develop/docs/stoploss.md)、[Backtrader StopTrail](https://github.com/mementum/backtrader/blob/master/backtrader/strategy.py)、[VectorBT stop enums](https://github.com/polakowo/vectorbt/blob/master/vectorbt/portfolio/enums.py)：初始止损、只收紧跟踪和事件幂等的工程参考。
- [Overnight Returns](https://arxiv.org/abs/2507.04481) 与 [Overnight Tail Risk](https://arxiv.org/abs/2402.07134)：隔夜收益和隔夜尾部风险具有独立特征。

以上研究来自不同市场，不能直接视为 A 股收益承诺。回测应使用真实交易日、复权/未复权口径、手续费、印花税、滑点、涨跌停和 T+1 可用数量约束，并对跳空无法成交进行压力测试。
