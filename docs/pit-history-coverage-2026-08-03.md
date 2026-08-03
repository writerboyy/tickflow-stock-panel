# PIT 历史覆盖审计（2026-08-03）

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

严格历史表 `data/pit_reference/history/index_membership_events/part.parquet` 当前只有 `000300.SH`：262 条事件、243 只标的，事件区间只覆盖到 2010 年，严格代表日期校验失败。

BaoStock 候选快照位于 `data/pit_reference/baostock/index_constituent_candidates`：

- 2021-08-04 至 2026-08-03，共 1,210 个交易日、363,000 行；每个快照恰好 300 只。
- `provenance=candidate_snapshot`，没有纳入/剔除生效日期，不能写入严格 `index_membership_events`。

JoinQuant 候选快照位于 `data/pit_reference/joinquant/joinquant_index_constituent_candidates`：

- 2025-04-25 至 2026-04-30，共 246 个交易日、73,800 行；每个快照恰好 300 只。
- 与 BaoStock 的 246 个重叠日期中，240 日成员集合完全一致；6 日各有 1 只差异（`601298.SH`/`601989.SH` 互换），只能作为交叉核验，不能证明历史生效事件。

因此当前结论为：沪深 300 可发布为“2021-08-04 起的带日期候选观测”，不可发布为 2021 年以前或完整历史 PIT 成分；沪深 500、800、1000 没有达到严格门槛的历史事件证据，保持不可用。TickFlow 的付费接口可以补指数日线和当前指数列表，但没有历史成分生效区间接口。

## 决定

本阶段不修改严格 PIT canonical 表，不用当前行业/指数快照回填历史。严格校验和边界测试保留在 `backend/app/plugins/pit_history/storage.py` 及其测试中，后续只有取得带有效日期的历史事件源才允许升级发布。
