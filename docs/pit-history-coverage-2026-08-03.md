# PIT 历史覆盖审计（2026-08-03，2026-08-07 更新）

本报告只记录当前数据湖中的可审计证据，不把当前快照推断成过去的生效事件。

## 行业历史

原始来源为 `akshare_cninfo`，对应入库 manifest 为
`data/ext_data/_ingestion/pit_history/industry_membership_history/2026-08-02.json`。
规范化表 `data/pit_reference/history/industry_membership_history/part.parquet` 共 52,630 行、12 套行业标准，覆盖 5,203 只标的。

按已发布股票日线 universe，在四个代表交易日对各标准进行 PIT 区间对账。覆盖率定义为：当日有日线的标的中，能由该标准的有效区间命中的标的比例。

| 行业标准 | 2010-01-04 | 2021-08-02 | 2024-01-02 | 2026-07-31 |
| --- | ---: | ---: | ---: | ---: |
| 申银万国行业分类标准 | 285/1,587 (17.96%) | 4,217/4,465 (94.45%) | 4,991/5,328 (93.67%) | 5,202/5,534 (94.00%) |
| 证监会行业分类标准（2012） | 1,468/1,587 (92.50%) | 4,218/4,465 (94.47%) | 4,991/5,328 (93.67%) | 5,156/5,534 (93.17%) |

没有标准在全部代表日期达到当前严格的 95% PIT 门槛。因此申万标准可以作为“允许缺失的行业中性化参考”，不能宣称为全市场、全历史覆盖。`ext_industry_tdx` 和 TickFlow 当前行业 universe 只能补当前快照，不能补这些历史缺口。

## 指数成分

旧表 `data/pit_reference/history/index_membership_events/part.parquet` 只有 262 条
Sina 事件，无法表示完整成分，已判定为不可用旧数据。现行唯一正式表为
`data/pit_reference/history/index_membership_history/part.parquet`，主键是
`(index_symbol, snapshot_date, member_symbol)`。

BaoStock 候选快照位于 `data/pit_reference/baostock/index_constituent_candidates`：

- 2021-08-04 至 2026-08-07，共 1,214 个交易日；每个快照恰好 300 只。
- `provenance=candidate_snapshot`，仅作交叉核验，不覆盖 JoinQuant 正式数据。

JoinQuant 候选快照位于 `data/pit_reference/joinquant/joinquant_index_constituent_candidates`：

- 2025-04-25 至 2026-04-30，共 246 个交易日、73,800 行；每个快照恰好 300 只。
- 与 BaoStock 的 246 个重叠日期中，240 日成员集合完全一致；6 日各有 1 只差异（`601298.SH`/`601989.SH` 互换）。

当前结论：将已验证的 JoinQuant 快照一次性迁入正式单表，发布
2025-04-25 至 2026-04-30 的 246 个日期，共 73,800 行。运行时不依赖 JoinQuant
目录或供应商标签。日期缺失或当日不是恰好 300 只时必须关闭使用，不用
BaoStock 静默补齐。沪深 500、800、1000 仍没有可发布数据，保持不可用。

## 决定

采用单表日快照模型，不再从快照推导纳入/剔除区间。迁移和发布都需通过
索引族精确成分数、主键唯一性和日期覆盖校验；不满足时 fail closed。
