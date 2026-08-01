# 全量数据湖审计（2026-08-02）

## 1. 审计结论

结论：**部分通过**。当前数据湖的股票、ETF 日线主链路结构合理，原始日线、enriched 和日度估值逐 `(symbol, date)` 对齐；复权、换手率、市值、TTM 与估值公式在可验证范围内正确。开盘啦与 EasyTDX 的扩展数据基本按各自独立语义入库，没有替代 TickFlow 权威行情或正式财务数据。

但当前不能把全部历史派生结果和盘中指标视为可靠，存在 4 组 P1 问题：

1. 历史 ST 名称没有进入涨跌停和连板计算，造成 212 个标的的 7,433 行涨停连板、6,672 行跌停连板与历史名称口径不符，属于未来信息污染。
2. 盘中增量指标与批量公式不一致。30 个真实标的中 MACD、BOLL、年化波动率、60 日极值和 RSI 全部或大部分不一致；窗口未满时还会提前产生 MA/BOLL/极值。
3. EasyTDX 分红历史有 7,259 / 51,390 行 `cash_per_share` 不能由当前分红方案解析公式复现，影响 3,421 个标的。该表只能作为辅助记录日上下文，不能用于权威复权或事件回放。
4. 指数日线有 495 行负成交量，覆盖 7 个指数；`000691.SH` 同时出现约 `1e18` 的异常成交额，符合有符号 32 位溢出/字段解码错误特征。

此外，财务原表存在重复键和 8 组同键不同值；当前 enriched/valuation 与彼此一致，但最新股本原表已不能完全复现部分近期分区。股票和 ETF 分钟数据的历史覆盖与最近两个交易日完整度也没有达到全市场连续覆盖。

本次只读审计没有启动应用、collector、刷新或重建任务。原因是应用启动会注册并可能触发后台采集，不符合“审计前后市场数据不变”的约束。

## 2. 范围与判定

审计数据目录为运行中的 `data/`，基线截止到 `2026-07-31`。审计包括：

- 股票、ETF、指数日线与分钟线；
- 复权因子、财务、股本、估值、分红、深度、基金净值和维表；
- `ext_data` 下全部 20 张扩展表及开盘啦原始归档；
- 批量历史指标、盘中增量指标、涨跌停/连板、换手率、复权与估值；
- 数据源所有权、能力缺失和下游消费边界。

状态含义：

| 状态 | 含义 |
| --- | --- |
| 通过 | 已有数据、代码契约和独立复算均未发现实质问题 |
| 部分通过 | 主体可用，但存在覆盖、演进、重现性或局部质量问题 |
| 不通过 | 已确认数据或计算结果错误，可能影响业务结果 |
| 无数据不可验证 | 当前没有该数据集，不能据此证明正确或错误 |

审计容差：价格和技术指标使用 `abs(a-b) <= 1e-8 + 1e-8*abs(a)`；金额/财务字段按公式使用 `1e-6 + 1e-9*abs(a)`。分钟 OHLC 边界另按浮点 epsilon 复核，不能把 `1e-14` 量级误差判为真实越界。

## 3. 数据目录与表级矩阵

### 3.1 权威行情与派生表

| 数据集 | 粒度、主键、分区 | 覆盖与规模 | 来源、单位、消费者 | 结论 |
| --- | --- | --- | --- | --- |
| `kline_daily` | 日频；`symbol,date`；`date=YYYY-MM-DD` | 1,211 文件，6,160,370 行，`2021-08-02..2026-07-31` | TickFlow；价格元/股、成交量股、成交额元；enriched、回测、筛选、行情 API | 通过 |
| `kline_daily_enriched` | 日频；`symbol,date`；按日分区 | 6,160,370 行，与日线逐键对齐 | TickFlow 派生；前复权 OHLC、原始价、股本、换手率、连板；策略/回测/监控 | **不通过**：历史 ST 连板错误；近期股本重现漂移 |
| `valuation_daily` | 日频；`symbol,date`；按日分区 | 6,160,370 行，与 enriched 逐键对齐 | TickFlow 派生；金额元、比率倍数；估值 API/策略 | **部分通过**：公式正确，但依赖的财务去重和最新股本输入不可完全重现 |
| `kline_index_daily` | 日频；`symbol,date`；按日分区 | 704,552 行，`2021-08-02..2026-07-31` | TickFlow 指数行情；指数看板/市场背景 | **不通过**：495 行负成交量，`000691.SH` 成交额异常 |
| `kline_index_enriched` | 日频；`symbol,date`；按日分区 | 704,552 行，与指数日线逐键对齐 | 指数派生；监控/市场概览 | **不通过**：完整继承指数原表异常 |
| `kline_etf_daily` | 日频；`symbol,date`；按日分区 | 1,123,552 行，`2021-08-02..2026-07-31` | TickFlow ETF 行情；ETF 回测/策略/行情 API | 通过 |
| `kline_etf_enriched` | 日频；`symbol,date`；按日分区 | 1,123,552 行，与 ETF 日线逐键对齐 | TickFlow 派生；ETF 策略/监控 | 通过 |
| `kline_minute` | 分钟；`symbol,datetime`；按交易日分区 | 327 文件，386,812,351 行，`2025-03-28..2026-07-31` | TickFlow；北京时间左开右闭，`09:30` 为竞价例外；分钟回测/分时 | **部分通过**：覆盖不足及 149,179 行空 OHLC |
| `kline_etf_minute` | 分钟；`symbol,datetime`；按交易日分区 | 248 文件，83,741,815 行，`2025-07-24..2026-07-31` | TickFlow ETF 分钟行情；ETF 回测/分时 | **部分通过**：最近两日覆盖不足 |
| `kline_index_minute` | 预期分钟；`symbol,datetime` | 数据集不存在 | 当前未声明指数分钟权威表 | 无数据不可验证 |
| `adj_factor` | 事件序列；`symbol,trade_date`；单文件 | 20,986 行，`2021-08-02..2026-07-31` | TickFlow 复权事件；`ex_factor` 为事件乘数；股票前复权 | 通过 |
| `adj_factor_etf` | 事件序列；`symbol,trade_date`；单文件 | 895 行，`2021-08-02..2026-07-28` | TickFlow ETF 复权事件；ETF 前复权 | 通过 |

### 3.2 财务、事件和维表

| 数据集 | Schema/粒度/主键 | 规模与质量 | 语义与消费者 | 结论 |
| --- | --- | --- | --- | --- |
| `financials/metrics` | 报告；`symbol,period_end,announce_date` | 303,549 行；376 个重复余量，其中 5 组同键不同值 | TickFlow 正式财务指标；财务分析 | **不通过** |
| `financials/income` | 累计报表；同上 | 404,545 行；352 个重复余量，其中 3 组同键不同值 | TickFlow 正式利润表；TTM、PE/PS | **不通过** |
| `financials/balance_sheet` | 时点报表；同上 | 344,741 行；340 个完全重复余量 | TickFlow 正式资产负债表；PB | 部分通过 |
| `financials/cash_flow` | 累计报表；同上 | 328,697 行；113 个完全重复余量 | TickFlow 正式现金流量表；PCF | 部分通过 |
| `financials/shares` | 生效事件；`symbol,period_end,announce_date` | 422,834 行；无重复键；62,838 行 `period_end > announce_date` | `announce_date` 为生效日、`period_end` 为对应股本日期；换手率/市值 | 部分通过：语义合理，但源刷新后未重建部分派生分区 |
| `corporate_actions` | 事件；`symbol,event_date` | 53,538 行，`1991-05-02..2026-08-07`；无重复/非正现金分红 | TickFlow 权威除权事件；复权和历史回放 | 通过 |
| `depth5` | 日内快照；`symbol` + 分区日 | 646 行，仅 6 个交易日 `2026-07-24..2026-07-31` | TickFlow 实时五档；封板状态 | 部分通过：现有数据合法，历史能力不可回补 |
| `fund_nav` | 基金日净值；`symbol,date`；按 symbol 分区 | 773,022 行、695 只，`2004-12-29..2026-07-31` | 基金净值；免费策略净值基准 | 部分通过：36 文件新增 `date_timezone`，659 文件为旧 schema |
| `instruments` | 当前股票快照；`symbol` | 5,537 行，无重复/非法代码 | TickFlow 维表；当前名称、股本、涨跌停价 | 通过（仅当前快照） |
| `instruments_index` | 当前指数快照；`symbol` | 612 行 | 指数路由与名称 | 通过 |
| `instruments_etf` | 当前 ETF 快照；`symbol` | 1,648 行 | ETF 路由、名称和基金元数据 | 通过 |
| `instrument_name_history` | 名称事件；`symbol,change_date` | 7,493 行 | 历史 ST/名称判定 | **部分通过**：表本身有效，但连板生产链路未消费 |
| `pools` | 当前池快照；`symbol,as_of` | 5,530 行 | 当前股票池筛选 | 通过（不能作为历史 PIT 成分） |
| `instruments_ext` | 扩展维表 | 0 行 | 未启用 | 无数据不可验证 |
| `kline_ext` | 扩展 K 线 | 0 行 | 未启用 | 无数据不可验证 |

### 3.3 全部扩展表

扩展表均使用 `config.json` 声明字段和单位。非空表的 Parquet schema 与配置一致，主键空值、重复键、分区日期错位和非法标准化 symbol 均为 0。

| 表 | 模式、主键、规模 | 来源和边界 | 结论 |
| --- | --- | --- | --- |
| `ext_gn_ths` | 快照；`symbol`；5,547 行 | 同花顺概念维度，仅展示/筛选 | 通过 |
| `ext_hy_ths` | 快照；`symbol`；5,542 行 | 同花顺行业维度，仅展示/筛选 | 通过 |
| `ext_industry_tdx` | 快照；`symbol`；5,209 行 | EasyTDX 申万/通达信行业；不提供行情；当前不映射北交所 | 通过（能力边界明确） |
| `ext_money_flow` | 快照；`symbol`；5,546 行 | 旧扩展资金流；`change_pct` 与 TickFlow 实时行情重叠 | 部分通过：只允许上下文使用 |
| `ext_kpl_auction` | 日频事件；`symbol`；0 行 | 开盘啦竞价，不是权威行情 | 通过（合理空数据） |
| `ext_kpl_funds` | 日频；`symbol`；5,537 行 | 开盘啦资金结构；价格/涨幅/成交额/市值不得替代 TickFlow | 通过 |
| `ext_kpl_limitup` | 日频；`symbol`；100 行 | 开盘啦涨停原因上下文；连板/市值不得替代 enriched | 通过 |
| `ext_kpl_lhb` | 日频；`symbol`；72 行 | 开盘啦龙虎榜汇总 | 通过 |
| `ext_kpl_lhb_detail` | 日频；`symbol,side,rank`；720 行 | 开盘啦席位明细 | 通过 |
| `ext_kpl_lhb_movement` | 日频；`participant_id,side,symbol`；69 行 | 开盘啦游资/机构动向 | 通过 |
| `ext_kpl_regulatory` | 日频；`symbol`；14 行 | 开盘啦监管事件 | 通过 |
| `ext_kpl_northbound_sector` | 季频；`report_date,plate_id`；96 行 | 开盘啦季度北向持仓，不是每日净流入 | 通过 |
| `ext_kpl_northbound_stock` | 季频；`report_date,plate_id,symbol`；3,807 行 | 开盘啦季度个股持仓；股本/市值只作上下文 | 通过 |
| `ext_kpl_sector_constituents` | 日频；`plate_id,symbol`；30,667 行 | 开盘啦板块历史成分；伴随行情字段不权威 | 通过 |
| `ext_kpl_shareholder_changes` | 报告期；`report_date,symbol,snapshot_kind,shareholder_id`；54,662 行 | 开盘啦十大流通股东 | 通过 |
| `ext_kpl_shareholder_counts` | 事件；`report_date,symbol`；17,614 行 | 开盘啦股东人数变更 | 通过 |
| `ext_tdx_margin` | 日频；`symbol,report_date`；49,493 行 | EasyTDX F10，两融金额万元、融券卖出量万股 | 通过 |
| `ext_tdx_forecast` | 公告；`symbol,announcement_date`；1,750 行 | EasyTDX 正式“业绩预告”章节；不等于快报/正式报表 | 通过 |
| `ext_tdx_express` | 公告；`symbol,announcement_date`；0 行 | EasyTDX 正式“业绩快报”章节 | 通过（合理空数据） |
| `ext_tdx_dividend_history` | 记录日；`symbol,record_date,plan`；51,390 行 | EasyTDX 7615 已实施分红；不替代 TickFlow 除权事件 | **不通过**：7,259 行每股现金分红错误 |

## 4. Schema、单位和分区审计

### 4.1 行情 Schema

- 日线基础字段为 `symbol,date,open,high,low,close,volume,amount`；股票/ETF 新分区可增加 `quote_ts`。兼容扫描使用 `union_by_name` 和显式类型对齐，旧分区可读。
- enriched 磁盘表是窄表，不持久化 MA/MACD 等技术指标。股票额外持久化 `raw_close/raw_high/raw_low,turnover_rate,total_shares,float_shares,consecutive_limit_ups,consecutive_limit_downs,quote_ts`；技术指标在运行时重算。
- `open/high/low/close` 在 enriched 中为前复权价格；`raw_*` 为未复权价格。涨跌停使用 `raw_*`，技术指标使用前复权价格。
- 分钟表使用无时区 `timestamp[us]` 表示北京时间墙钟。全表时间复核后，交易时段外记录为 0；`09:30` 竞价根和 `09:31..11:30,13:01..15:00` 连续竞价根合计最多 241 根。
- `fund_nav.date_timezone` 是可兼容的加列演进，但同一逻辑表目前有 2 种物理 schema，建议统一回填或在元数据中正式登记 schema version。

### 4.2 财务和估值单位

- 正式财务、股本、市值和成交额均按元/股/股数的基础单位计算；EasyTDX F10 两融字段明确保留“万元/万股”，不混入正式财务。
- `turnover_rate = volume / float_shares * 100`，结果为百分数值，例如 `5` 表示 `5%`。
- `market_cap = raw_close * total_shares`，`float_market_cap = raw_close * float_shares`。
- `PE/PB/PS/PCF` 只在分子、分母均为有限正数时输出，否则为 null，不把负利润伪装成负 PE。
- `financials/shares.period_end > announce_date` 不能直接判错。该表使用 `announce_date` 作为股本生效/可用日，与正式报表“报告期不得晚于公告日”的规则不同。

### 4.3 PIT 备份隔离

旧备份目录 `.kline_daily_enriched.pre-pit-20260728` 位于现行 `kline_daily_enriched` 的同级隐藏目录。生产扫描均以 `data/kline_daily_enriched/**/*.parquet` 为根，DuckDB 视图也使用同一显式根；不会命中该备份。现行表扫描行数仍为 6,160,370，没有出现备份倍增。

## 5. 全表质量检查

### 5.1 日线和资产隔离

- 股票、ETF、指数日线均无重复 `(symbol,date)`、必需字段空值、非正价格、OHLC 关系错误或分区日期错位。
- 股票/ETF/指数代码分别通过独立目录和维表路由，没有发现跨资产主键串表。
- 股票、ETF 日线和各自 enriched 的键、原始 OHLCV 完全一致；指数日线/enriched 也逐键一致，但共同包含指数源异常。
- 指数异常为 495 行、7 个 symbol：`000011.SH` 5 行、`000691.SH` 298 行、`000902.SH` 6 行、`000985.SH` 4 行、`399317.SZ` 6 行、`399379.SZ` 93 行、`399380.SZ` 83 行。范围为 `2021-08-02..2026-07-30`。

### 5.2 分钟数据

股票分钟：

- 无重复键、非正价格、分区错位或交易时段外记录。
- 实际全市场覆盖从 `2025-05-19` 才开始，不能把名义起点 `2025-03-28` 当作全市场起点。
- 相对当日日线 universe，有 33 个交易日的完整 241 根覆盖率低于 95%。
- `2026-07-30` 只有 4,813 / 5,528 个分钟标的完整，完整率 86.97%；`2026-07-31` 缺 6 个日线标的。
- 149,179 行 OHLC 全空，集中在 `2026-04-20..2026-07-22` 的遗留标的日；当前 sanitizer 会丢弃这些行，但历史文件没有清理。
- 3,341 个表面 OHLC 越界均为浮点 epsilon；41,086 个负 `amount` 也只是约 `1e-10` 的计算残差，均不是经济意义上的负值。
- `2026-07-31` 可比标的分钟收盘与日线收盘 0 实质差异。开盘、高低、成交量/额受竞价根、缺根和供应商聚合语义影响，不据此直接判错。

ETF 分钟：

- 无重复键、空 OHLC、非正价格、OHLC 越界、分区错位或交易时段外记录。
- `2026-07-30` 仅 1,436 / 1,593 只完整，完整率 90.14%；`2026-07-31` 缺 200 只 ETF，完整率 87.08%。
- 52,520 个负 `amount` 同样是微小浮点残差。
- 最近一个可完整对账日 `2026-07-29` 的分钟收盘与日线收盘无实质差异。

结论：覆盖不足是能力/采集窗口问题，历史 depth5 不可回补也是能力边界；二者不能伪装成“价格字段错误”。但使用分钟回测前必须按交易日、标的验证完整度并 fail-closed。

### 5.3 小文件

- 日线和分钟均为一日一文件，文件大小与扫描方式合理。
- `ext_tdx_dividend_history` 51,390 行分散在 4,005 个日期分区，中位数仅少量记录，是明确的小文件开销。
- `fund_nav` 695 个 symbol 分区符合按基金读取场景，但全市场扫描时仍应走 dataset scanner，避免逐文件 Python 循环。

## 6. 派生链路和 PIT 对账

### 6.1 复权、股本、换手率和市值

- 使用 `adj_factor.ex_factor` 独立构造累计因子，对全部股票和 ETF 日线重算前复权 OHLC，股票与 ETF 四价错配均为 0。
- 股票日线、enriched、valuation 共 6,160,370 个键完全一致。
- stored enriched 与 valuation 的 `raw_close,total_shares,float_shares` 一致；换手率、市值、流通市值、PE/PB/PS/PCF 公式错配均为 0。
- 使用当前 `financials/shares` 和 `instruments` 重新按 PIT 规则选择股本时，`total_shares` 有 134 行不一致，`float_shares` 有 236 行不一致。主要集中于最近交易日，说明源表更新后没有同步重建受影响的 enriched/valuation 分区。

### 6.2 TTM 和公告日可见性

冻结公式：

```text
年报 TTM = 当期累计值
一季报/半年报/三季报 TTM = 本期累计 + 上年年报 - 上年同期累计
```

正式财务先按 `(symbol, period_end, announce_date)` 归一，再按公告日生成事件；日估值只能 backward as-of join `announce_date <= trade_date`。全表检查没有未来公告泄漏。对 30 个沪深北真实标的，以不调用生产 TTM 函数的独立事件循环重算 `2026-07-31` 的利润、收入、现金流 TTM、权益及全部公告日/报告期字段，10 个字段均 0 错配。

当前风险不在 TTM 公式，而在原表去重：

- `metrics`：371 个完全重复余量，5 组同键不同值；
- `income`：349 个完全重复余量，3 组同键不同值；
- `balance_sheet`：340 个完全重复余量；
- `cash_flow`：113 个完全重复余量。

8 个冲突键为：

```text
metrics: 002010.SZ/2024-12-31/2026-04-21
         002462.SZ/2022-12-31/2026-03-14
         300205.SZ/2024-12-31/2026-04-23
         688132.SH/2024-12-31/2026-03-20
         920249.BJ/2021-12-31/2026-01-30
income:  300500.SZ/2024-12-31/2025-12-31
         600169.SH/2021-12-31/2025-11-01
         601118.SH/2024-12-31/2026-04-18
```

当前 `_normalize_financial_events` 以 Parquet 读取顺序生成 `_source_order`，同键 `keep="last"`。原表没有供应商 revision ID、更新时间或更正序号，因此“最后一条”不能证明是正确版本；文件重写或合并顺序变化还可能改变结果。

### 6.3 历史 ST 涨跌停和连板

`instrument_name_history` 显示 304 个标的、213,608 个交易日的历史名称状态与当前名称不同。生产 enriched 的历史连板值与“当前名称套用全部历史”结果完全一致，证明历史名称没有进入调用链。

- 按历史名称重算，`consecutive_limit_ups` 有 7,433 行、212 个标的不一致；
- `consecutive_limit_downs` 有 6,672 行、212 个标的不一致；
- ST/`*ST` 的 5% 价格限制被错误按当前 10% 或反向错误套用于历史，直接影响连板策略、涨跌停筛选和历史回测。

## 7. 数据源入库与所有权

### 7.1 所有权规则

`backend/app/services/data_authority.py` 已把股票、ETF、指数日线/分钟线、实时行情、正式财务和公司行动定义为 TickFlow primary；enriched 和 valuation 定义为 TickFlow derived。扩展表的重叠字段在 canonical usage 下会 fail-closed。

确认以下语义保持独立：

- EasyTDX `业绩预告` 是区间/方向预估，`业绩快报` 是正式报告前的初步实际结果，二者都不覆盖 TickFlow 正式财务报表；
- EasyTDX 分红历史按股权登记日组织，TickFlow `corporate_actions.event_date` 才是权威除权/回放事件；
- 开盘啦季度北向持仓不是每日北向净流入；板块成分伴随的价格/成交额不是权威行情；
- 开盘啦资金、涨停、北向和板块表中的重叠价格、换手率、股本、市值字段均只能作展示/筛选上下文；
- EasyTDX 行业只提供维度，不提供行情、股本或正式财务替代能力。

自定义 provider 路径允许“未配置/调用失败时回到 TickFlow”，这是回到权威源，不是 EasyTDX/开盘啦静默替代 TickFlow。没有发现 EasyTDX 或开盘啦扩展字段进入 canonical 行情、复权、估值或正式财务链路。

### 7.2 开盘啦原始归档回放

只读回放 `data/ext_data/_kaipanla_raw` 下 20,112 份、153 MB 归档，不调用 client、collector 或 storage：

- 所有 JSON/Gzip 均可读取；20,112 / 20,112 可由对应 parser 解析，异常 0；
- `/30` 共 2,681 份响应，全部显式包含 list 类型的空 `info`，parser 输出 0 行。因此 `ext_kpl_auction` 为空是合理空数据，不是采集或落盘缺口；
- 其余表重放并按声明主键合并后，主键集合与 Parquet 完全一致，字段值错配为 0；
- 典型对账：资金 5,537/5,537、涨停 100/100、龙虎榜 72/72、北向个股 3,807/3,807、十大股东 54,662/54,662、股东人数 17,614/17,614、板块成分 30,667/30,667。

### 7.3 EasyTDX 在线只读抽样

按稳定排序选取 30 个标的：沪 12、深 12、北 6；沪深各含 6 个已有预告记录和 6 个无预告记录，北交所覆盖当前无行业/预告记录场景。直接调用 client/parser，不调用 collector，不写结果：

- 行业列表返回 5,209 行；24 个沪深样本与落盘行业编码 0 差异。6 个北交所样本不在当前 `_symbol()` 行业映射中，属于明确能力缺失；
- F10 30/30 返回：10 个标的含两融表，共 204 行，与落盘 0 差异；12 个含正式业绩预告，与落盘 0 差异；30 个均无正式业绩快报章节；
- 分红历史返回 283 行、失败 0；主键均已落盘，但 41 行 `cash_per_share` 与当前方案解析不同；
- 将分红全表按当前 `cash_per_share_from_plan(plan)` 重算，7,259 行、3,421 个 symbol 不一致。典型错误如 `10转增3股派3元` 被存成 `1.0`，正确每股现金为 `0.3`。

开盘啦另外只读复核 `/15` 和 `/100`：均成功，分别返回 100 行涨停列表和 `2026-07-31` 的 72 行龙虎榜，与归档/落盘规模一致。没有调用在线竞价 collector。

### 7.4 EasyTDX 与开盘啦入库优化方案

以下内容是基于本次审计证据形成的实施方案。审计阶段保持只读，没有调用 collector、刷新任务或在线结果落盘；后续整改分支已实现其中一部分代码门禁，具体状态见 7.4.5。代码实现不等于现有数据已修复，所有影子重算、归档回放和生产发布仍需单独验收。

#### 7.4.1 目标架构与数据边界

两类辅助源统一采用以下流水线，但按数据集独立发布，避免一个端点或章节失败连带覆盖其他有效数据：

```text
fetch
  -> immutable raw archive / staging
  -> parse
  -> normalize
  -> quality gate
  -> atomic publish
  -> manifest + checkpoint + metrics
```

- TickFlow 继续唯一拥有股票、ETF、指数行情、复权因子、正式财务、公司行动及其派生链路；EasyTDX 和开盘啦只补充行业、两融、预告/快报、分红记录日、资金、题材、榜单和股东等辅助上下文。
- 两个辅助源响应中的价格、涨跌幅、成交量/额、换手率、股本和市值保留在扩展表时必须标记为 source context；任何 canonical consumer 请求这些字段都应由 authority gate 拒绝，不能通过字段同名、非空优先或静默 fallback 进入 enriched、估值、复权、回测和正式财务链路。
- 业绩预告、业绩快报、正式财务报表、股权登记日和除权事件分别建模，不互相填充或覆盖。缺少正式章节、端点能力或完整分页时返回显式状态，不用近似字段生成伪记录。
- 每个发布单元至少记录 `dataset`、`source`、`logical_snapshot`、请求范围、原始内容 hash、parser/schema version、分区、主键、单位、抓取时间、源业务日期、原始/解析/拒绝/发布行数、空数据原因、重试次数和失败清单。

质量门禁应在 publish 之前完成，且失败时保留上一份有效快照：

| 门禁 | 通过条件 | 失败行为 |
| --- | --- | --- |
| Schema 与版本 | 必需字段、类型、枚举和 schema version 符合数据集契约 | 隔离到 staging/dead-letter，不改变现行分区 |
| 主键与日期 | 批内、目标分区和跨分页主键唯一；业务日期、公告日、报告期和分区日期可解释 | 输出冲突键和来源页，整发布单元失败 |
| 单位与数值 | 单位显式；金额、股数、比例不使用数值大小猜测转换；范围校验通过 | 拒绝异常行，拒绝数超过阈值则整批失败 |
| 完整性 | 请求批次/分页全部完成，预期标的和页码可由 manifest 对账 | 标为 `incomplete`，不发布部分快照 |
| 空数据 | 仅接受 `valid_empty`、`unsupported`、`section_absent` 等已定义原因 | 网络失败、解析失败不得写成空表或覆盖旧数据 |
| 幂等性 | 相同 source snapshot、parser version 和输入 hash 重跑结果一致 | hash/行数漂移进入人工复核，不自动替换 |

#### 7.4.2 EasyTDX 专项改造

1. **有界批次与断点续跑**：全市场按稳定的交易所、代码顺序切成约 50 个标的一批；checkpoint 记录每批的请求标的、完成数据集、失败标的、重试次数和 source snapshot。重跑只处理未完成或明确失败批次，不再次覆盖已验证分区。
2. **按数据集独立发布**：`ext_industry_tdx`、`ext_tdx_margin`、`ext_tdx_forecast`、`ext_tdx_express` 和 `ext_tdx_dividend_history` 分别构建 manifest 和原子提交。某标的 F10 某章节失败不能使其他章节变空，也不能用同一次全市场任务的一次性写入掩盖部分失败。
3. **正式章节门禁**：两融必须出现正式表头 `融资余额(万元)`；预告和快报必须分别出现 `●业绩预告:`、`●业绩快报:` 等正式章节 marker。问答、新闻或其他章节中的同名文本只能进入拒绝原因，不能产生记录。
4. **能力三态**：北交所行业当前明确记录为 `unsupported`，不能伪装成“无行业”；快报无正式章节记录为 `section_absent`，请求失败为 `source_error`，解析被拒绝为 `parse_rejected`。只有前两类可形成合理空数据结论，后两类必须进入失败清单并可重试。
5. **分红专项修复**：先把 7,259 行差异键及原始 `plan` 写入不对外服务的修复清单；使用冻结后的 `cash_per_share_from_plan` 在影子表重算，逐 `(symbol,record_date,plan)` 对账主键、单位和行数。只有差异归零后才原子切换 `ext_tdx_dividend_history`；回滚方式是恢复旧 manifest/分区。修复期间及完成后，权威复权仍只使用 TickFlow `corporate_actions`。
6. **可追溯原文**：对 F10 内容保存 source snapshot、category metadata、内容 hash 和 parser version；敏感连接信息不得进入归档或日志。原文发生变化时生成新 revision，不依赖文件遍历顺序选择“最后一条”。
7. **小文件治理**：分红表从当前 4,005 个小分区迁移为按年或月 compact 的物理布局，同时保留 `record_date` 列和分区统计以支持逻辑查询。compaction 只在新文件校验成功后更新 manifest，不与采集任务原地改写同一批文件。

#### 7.4.3 开盘啦专项改造

1. **先归档、后解析**：请求成功后先以端点、业务日期、页码和抓取批次归档不可变原始响应，再从归档解析到 staging。归档写入、hash 校验或解压验证失败时不得继续发布。
2. **完整快照发布**：同一逻辑快照的全部分页和关联请求成功后才合并、去重并原子发布；任一页失败时保留上一份完整分区，禁止用已成功的部分页覆盖完整历史。分页达到安全上限时标为 `page_limit_reached`，不能按正常完成处理。
3. **端点级 checkpoint 与失败隔离**：每个端点独立记录页游标、限流等待、重试和 dead-letter。`/15`、`/100` 等端点失败互不影响；重试从失败页恢复，并通过请求指纹和主键保证幂等。
4. **竞价完成状态**：09:15、09:20、09:25 快照和 `/31` 明细分别记录完成状态，只有契约要求的组成部分齐全才将该交易日标为完整。`/30` 的 2,681 份历史响应均为空 `info`，应记录为供应商明确返回的 `valid_empty`；不得据此生成零值竞价行，也不能把未来的超时或鉴权失败归为合理空数据。
5. **回放作为发布前回归**：现有 20,112 份原始归档作为固定回放基线；parser 或 schema 变化时，在影子目录重放并比较表级行数、主键集合、单位和字段 hash。只有预期差异有迁移说明且 authority 测试通过后才升级 parser version。
6. **重叠字段隔离**：资金、涨停、北向、板块及股东数据中的伴随行情字段保留来源前缀或 source-context metadata；标准化层禁止映射为 TickFlow canonical 字段。消费者需要权威价格、换手率、股本或市值时必须另行读取 TickFlow 数据并按键关联。
7. **归档与 Parquet 生命周期**：原始响应按端点/日期分层保留，定期压缩并生成校验清单；不得在 parser 发布后立即删除原文。Parquet 根据查询粒度做月度/年度 compaction，manifest 切换后再回收已确认不被读取的旧物理文件。

#### 7.4.4 分阶段实施、监控与验收

| 阶段 | 优先级 | 实施内容 | 完成标准 |
| --- | --- | --- | --- |
| A | P1 | 修复 EasyTDX 分红解析并影子重算 | 7,259 行差异归零；重复键 0；权威复权结果不变；可按旧 manifest 回滚 |
| B | P2 | EasyTDX 有界批次、checkpoint、章节门禁、三态空数据和数据集独立发布 | 人为中断后可从失败批恢复；快报空、北交所 unsupported 和请求失败可区分；重跑幂等 |
| C | P2 | 开盘啦完整分页发布、端点 checkpoint、dead-letter 和归档回放门禁 | 部分页不会覆盖完整分区；20,112 份基线重放主键/字段无非预期差异；`/30` 合理空可证明 |
| D | P3 | 两源 manifest、质量指标、告警和小文件 compaction | 发布批次可追溯；异常能定位到源/端点/标的/页；小文件数显著下降且查询结果不变 |

上线前必须在隔离目录或影子表完成故障注入：网络超时、单页失败、F10 缺章节、schema 增列/缺列、重复键、单位异常、任务中断和重复执行。验收指标统一为：发布表重复键 0、非法单位 0、未解释空数据 0、失败批次可恢复、同输入重跑 hash 一致、部分响应不覆盖上一有效快照、canonical authority 负向测试全部通过、非目标表文件 hash/mtime 不变。发布后按批次监控成功率、有效空/失败原因分布、拒绝行数、源到湖延迟、主键冲突数、重试次数和小文件数量；任一质量门禁失败时停止该数据集发布，不影响 TickFlow 权威主链路和其他辅助表。

#### 7.4.5 整改分支落地状态

本节记录 `codex/data-audit-remediation-20260802` 的代码实现边界。整改只修改代码和测试，没有运行 EasyTDX/开盘啦 collector，没有把在线样本写入数据湖，也没有切换任何生产影子表。

| 能力 | 已落地 | 尚未完成/不能宣称 |
| --- | --- | --- |
| EasyTDX 批次与恢复 | 50 标的稳定批次；数据集独立 manifest/checkpoint；失败批次重试；staging 与原始内容 hash；相同输入恢复；manifest 损坏时 fail-closed | 尚未对全市场真实任务做中断恢复演练；尚未建立运维告警面板 |
| EasyTDX 空数据判定 | F10 批次必须返回全部请求标的，缺标的记 `source_error`；分红底层逐标的失败会使整批失败；正式章节缺失才可记 `section_absent` | 北交所行业仍是既有能力边界，未新增其数据；真实快报首次出现后的落盘仍待在线验收 |
| EasyTDX 分红修复 | 新增只读默认的影子重算/按年 compaction 工具；校验行数、主键和冻结公式；`apply` 时保留旧目录用于回滚 | 未对现有 51,390 行执行影子重算或切换；因此 7,259 行历史差异仍存在，权威复权仍只使用 TickFlow `corporate_actions` |
| 开盘啦分页与竞价 | `/115`、`/30`、`/100` 统一分页上限、逐页归档和 manifest；分页失败不发布；竞价三检查点与 `/31` 记录组件完成状态；`/31` 任一标的失败时不发布该批明细 | 其他逐股扩展端点仍需逐项接入同等级 checkpoint/dead-letter；20,112 份归档尚未在本整改分支重新全量回放 |
| 数据所有权 | 扩展配置持久化 `schema_version`、authority、canonical dataset、overlap policy 与 allowed usage；负向测试保持 EasyTDX/开盘啦不能替代 TickFlow 权威字段 | 未改变公开 API 或 canonical schema；消费者新增扩展用途时仍需同步增加 authority 测试 |

EasyTDX/开盘啦的上线顺序保持 A-D 不变。当前可判定为：阶段 B 的核心采集骨架和阶段 C 的竞价/龙虎榜主分页门禁已实现并通过模拟故障测试；阶段 A 只有修复工具、没有数据执行；阶段 C 的全归档回放和阶段 D 的监控、全端点治理尚未完成。

## 8. 指标公式与独立验证

### 8.1 项目冻结公式

| 指标 | 当前批量公式 | 初始化/窗口/口径 |
| --- | --- | --- |
| MA(N) | `mean(close, N)` | 前复权 close；窗口满 N 日才输出 |
| EMA(N) | `alpha=2/(N+1), adjust=False` | 首个有效 close 为状态初值 |
| MACD | `DIF=EMA12-EMA26`; `DEA=EMA9(DIF)`; `HIST=2*(DIF-DEA)` | 前复权 close |
| BOLL | `MA20 +/- 2*rolling_std(close,20)` | 批量 `ddof=1`；窗口满才输出 |
| KDJ | 9 日 high/low 得 RSV；K、D 各 `alpha=1/3`; `J=3K-2D` | K 从首个 RSV 初始化，不使用固定 50 |
| ATR(14) | `TR=max(H-L,abs(H-prevC),abs(L-prevC))`; `EMA(alpha=1/14)` | Wilder 型递推 |
| 量比 | `volume / mean(volume.shift(1),5)` | 分母为前 5 日，不含当日；盘中按已交易分钟折算量 |
| 动量(N) | `close/close.shift(N)-1` | N 为交易日，包含当日终点 |
| 年化波动率 | `std(daily_return,20)*sqrt(252)` | 批量 `ddof=1`，需 20 个收益率 |
| RSI(N) | 涨跌额正负拆分后分别 `EMA(alpha=1/N)` | 首日 gain/loss 置 0；loss=0 用 `1e-12` |
| 涨跌幅/额 | `close/prev_close-1`; `close-prev_close` | 前复权价 |
| 振幅 | `(high-low)/prev_close` | 前复权价；前收必须正 |
| 60 日极值 | `rolling_max/min(close,60)` | 当前批量公式是收盘极值，不是日内 high/low |
| 涨跌停/连板 | 交易板块、日期、ST 状态决定比例，按 tick size 舍入 | 必须使用原始价和历史名称；当前历史名称调用链有 bug |
| 换手率 | `volume/float_shares*100` | 历史股本优先，缺失才回退当前维表 |
| 前复权 | 日 OHLC 乘基于事件因子的累计比例 | `raw_*` 保持原价 |
| 估值 | 市值/TTM 或时点权益 | 公告日 PIT；负/零分母输出 null |

### 8.2 30 标的真实数据重算

使用前 300 个自然日（约 210 个交易日）的原始 OHLCV，独立 Python 循环实现上述公式，不调用生产指标函数作为 expected。样本为沪 12、深 12、北 6，且每只至少 180 条记录。

批量路径：34 个输出列全部 0 错配，包括 MA/EMA/MACD/BOLL/KDJ/ATR/量比/动量/波动率/RSI/涨跌幅/振幅/极值。

盘中增量路径对同一 `2026-07-31` 输入：

| 字段 | 30 标的错配数 | 最大绝对差 | 根因 |
| --- | ---: | ---: | --- |
| `macd_dif/dea/hist` | 30/30 | 0.03809 / 0.00762 / 0.06095 | `_ema12/_ema26` 只用近 90 自然日重新初始化 |
| `boll_upper/lower` | 30/30 | 0.24938 | 增量用总体方差 `/20`，批量用样本标准差 `ddof=1` |
| `annual_vol_20d` | 30/30 | 0.02471 | 增量用总体方差，批量用 `ddof=1` |
| `high_60d` | 28/30 | 9.55 | 增量用日 high，批量用 close |
| `low_60d` | 30/30 | 2.46 | 增量用日 low，批量用 close |
| `rsi_6/14/24` | 30/30 | 0.00203 / 0.54616 / 1.94280 | RSI 状态只用近 90 自然日重新初始化 |

MA、EMA5/10/20/30/60、KDJ、ATR、量比、动量、涨跌幅/额和振幅均 0 错配。

### 8.3 固定边界样本

- 80 日恒定价格：BOLL 上下轨 10、ATR 0、年化波动率 0、量比 1、RSI 0；K/D/J 为 NaN，因为代码只对 null 分母填 `1e-12`，没有处理实际的 0 区间。
- 只有 10 日历史后计算第 11 日：批量 `ma20/30/60,boll,high_60d` 均为 null；增量却分别输出 `5.0/3.3333/1.6667`、BOLL 上轨 15、`high_60d=10`。`_window_len` 已构造但没有用于 gate。
- 除权：股票和 ETF 全表独立复权重算 0 错配；涨跌停测试覆盖 ST、主板、创业板/科创板、停牌和原始价边界。
- 缺历史股本：生产规则与测试均明确“历史股本优先、无可用历史记录时回退当前维表”，但回退数据不是历史 PIT 事实，报告和策略应能识别该状态。

### 8.4 与常见平台的口径差异

以下差异本身不直接判 bug，只有批量/增量违反项目冻结公式才判错：

- BOLL 常见实现既有 `ddof=0` 也有 `ddof=1`；项目批量冻结为 `ddof=1`。
- RSI 常见实现有 Wilder 初始 SMA、递推平滑和简单滚动均值；项目使用从首日 0 gain/loss 开始的 `alpha=1/N` EWM，恒定价格得到 0 而不是部分平台的 50。
- KDJ 常见实现从 K/D=50 开始；项目从首个有效 RSV 开始。零波动区间得到 NaN 是项目边界缺陷，不应解释为正常平台差异。
- 60 日“新高/新低”可以定义为收盘极值或日内 high/low；项目批量冻结为 close 极值，因此增量路径必须一致。

## 9. 已确认问题与修复顺序

本次没有发现 P0 数据破坏或凭据泄漏问题。

| 优先级 | 问题与影响 | 修复/重算方案 |
| --- | --- | --- |
| P1 | 历史 ST 状态未进入涨跌停/连板；14,105 个字段行差异，影响 212 个标的和历史策略信号 | 在历史流水线按 `(symbol,date)` as-of join `instrument_name_history`；增加 ST 变更日前后测试；全量重建股票 enriched、valuation 受影响列和依赖策略缓存 |
| P1 | 盘中 MACD/BOLL/波动率/极值/RSI 与批量不一致，窗口未满提前输出 | 以批量冻结公式为唯一契约；保存/构造完整递推状态，BOLL/波动率统一 `ddof=1`，极值统一 close，所有窗口使用 `_window_len` gate；增加批量-增量逐列一致性测试 |
| P1 | EasyTDX 分红 `cash_per_share` 7,259 行错误 | 不改变该表的辅助所有权；用当前 `cash_per_share_from_plan` 对原始 `plan` 生成修复候选，逐键校验后仅重建该扩展表；权威复权继续只读 `corporate_actions` |
| P1 | 指数 495 行负成交量和 `000691.SH` 异常成交额 | 从 TickFlow 重新拉取 7 个指数的受影响日期；在标准化入口增加 int32 overflow/负量/异常金额校验；重建指数 enriched 和市场背景缓存 |
| P2 | 财务原表重复及 8 组冲突，“最后一条”依赖文件顺序 | 保存供应商 record/revision/update ID；按确定性 revision 规则去重；对 8 组人工核源；重建 valuation 并逐日对账 |
| P2 | 当前股本源不能复现 enriched 的 134/236 行 | 记录派生构建 source snapshot/version；股本刷新后按最小受影响日期重建 enriched/valuation；增加 source hash 到 metadata |
| P2 | 股票分钟 149,179 行空 OHLC，33 日低覆盖；ETF 最近两日低覆盖 | 清理历史空 OHLC 前先生成修复清单；以标的日 241/240 根和日线 universe 建 coverage manifest；分钟回测按所需窗口 fail-closed |
| P2 | KDJ 零区间为 NaN | 对 `high_9 == low_9` 冻结明确值（建议延续前 K/D 或使用中性 RSV=50），批量与增量同时修改并补恒定价格测试 |
| P3 | `fund_nav` 两种 schema、旧扩展配置缺 authority 持久字段 | 在不改变公开 schema 的前提下登记 schema version；迁移配置元数据或在审计中持续校验 runtime 补全 |
| P3 | EasyTDX 分红 4,005 个小分区 | 在保持逻辑按记录日查询的前提下按年/月 compaction，维护 manifest/统计信息 |

推荐重算顺序：先修生产公式和确定性去重规则，再修原表，最后依次重建 `kline_daily_enriched -> valuation_daily -> 指数 enriched -> 依赖缓存/策略结果`。不能先重算后修公式，否则会把已确认错误扩大到全历史。

## 10. 不可验证项和剩余边界

- 没有指数分钟表，无法验证指数 1 分钟聚合或覆盖。
- depth5 只有 6 个交易日，历史深度不能从日线/分钟线推导，不能验证更早封单状态。
- 当前 pools、行业和 instruments 是快照，不能证明历史 PIT 成分或历史行业归属。
- EasyTDX 业绩快报全表为空；全市场采集结果和 30 标的在线样本均没有正式快报章节，因此判为合理空数据，但不能证明未来出现快报时的实际落盘行为；parser/collector 定向测试承担该契约验证。
- 在线抽样只能证明审计时点的 30 个标的和两个开盘啦端点，不等于供应商所有代码、所有历史日期的 SLA。
- 审计和整改验证均没有刷新生产数据，因此已确认缺口仍保留在数据湖中；报告中的“代码已落地”不是“数据已修复”证明。

## 11. 证据与验证记录

### 11.1 只读证据命令

```bash
# 表、文件和 schema 目录
find "$DATA_DIR" -maxdepth 2 -type d

# 生产扫描根，证明隐藏 PIT 备份不被 glob 命中
rg -n 'kline_daily_enriched.*/\*\*|_enriched_glob|scan_enriched_parquet' backend/app

# 数据所有权与扩展重叠字段 fail-closed 规则
sed -n '1,430p' backend/app/services/data_authority.py

# 生产指标批量/增量公式和 live-state 构造
sed -n '333,535p' backend/app/indicators/pipeline.py
sed -n '1367,1662p' backend/app/indicators/pipeline.py
sed -n '701,902p' backend/app/tickflow/repository.py
```

全量扫描、开盘啦回放、30 标的独立指标和在线抽样使用 `/tmp/tickflow_*_audit.py` 临时只读程序执行；这些程序没有调用 storage/collector，也没有写入 `data/`，不作为产品代码提交。

### 11.2 完整性基线

审计前对 34,096 个市场数据文件记录 SHA-256、size、mtime 和 ctime：

```bash
shasum -a 256 <market-data-files> > /tmp/tickflow-data-audit-pre.sha256
stat -f '%N|%z|%m|%c' <market-data-files> > /tmp/tickflow-data-audit-pre.stat
```

审计完成后用相同排序和文件集合生成 post 清单。实际结果：

```text
SHA-256: 34,096 / 34,096 文件一致，cmp exit 0
size/mtime/ctime: 34,096 / 34,096 文件一致，cmp exit 0
```

### 11.3 测试

审计基线的定向测试结果为：

```text
67 passed in 1.32s
```

提交前复跑命令：

```bash
cd backend
PYTHONPATH=. .venv/bin/python -m pytest \
  tests/test_data_authority.py tests/test_parquet_schema_compat.py \
  tests/test_enriched_full_rebuild.py tests/test_financial_shares.py \
  tests/test_daily_valuation.py tests/test_indicator_needed.py \
  tests/test_pipeline_and_monitor_fixes.py tests/test_price_limits.py \
  tests/test_realtime_enriched_resume.py tests/test_realtime_turnover_rate.py \
  tests/test_st_limit_and_sharpe.py tests/test_kaipanla_credentials.py \
  tests/test_kaipanla_parsers.py tests/test_kaipanla_storage.py \
  tests/test_kaipanla_collector.py tests/test_easy_tdx_collector.py \
  tests/test_backfill_easy_tdx_dividends.py -q
```

实际结果：`134 passed, 4 warnings in 5.49s`。4 条 warning 均为既有 Polars `collect(streaming=True)` 参数弃用提示，没有测试失败。`git diff --check` 通过。该结果对应只读审计报告提交，报告本身不改变公开 API、schema、类型、采集代码或运行时行为。

### 11.4 整改实现验证

整改分支新增并验证了以下失败路径：历史名称 PIT、批量/盘中指标逐列一致、KDJ 零波动、财务同键冲突拒绝、派生 source snapshot、EasyTDX 批次恢复/缺标的响应/损坏 manifest、开盘啦分页中断/竞价组件/部分 `/31` 明细、指数异常批次、分钟完整度与影子清理、分红影子修复、enriched/valuation 原子发布和回滚目录。

实际执行命令：

```bash
cd backend
PYTHONPATH=. .venv/bin/python -m pytest -q
PYTHONPATH=. .venv/bin/python -m ruff check <本分支全部 Python 变更文件>
git diff --check custom...HEAD
```

隔离工作树复用了主工作区现有 `.venv` 解释器。实际结果：`954 passed, 13 warnings in 11.93s`；Ruff 全部通过；`git diff --check` 通过。13 条 warning 均为既有 Polars streaming 参数或 `datetime.utcnow()` 弃用提示。测试只使用临时目录，没有调用真实 collector 或改写 `data/`；生产数据修复仍必须按 7.4.4 的影子验收顺序单独执行。
