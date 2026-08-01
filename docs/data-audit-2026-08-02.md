# 全量数据湖审计（2026-08-02）

## 1. 审计结论

原始审计结论：**部分通过**。当前数据湖的股票、ETF 日线主链路结构合理，原始日线、enriched 和日度估值逐 `(symbol, date)` 对齐；复权、换手率、市值、TTM 与估值公式在可验证范围内正确。开盘啦与 EasyTDX 的扩展数据基本按各自独立语义入库，没有替代 TickFlow 权威行情或正式财务数据。

原始基线存在 4 组 P1 问题：

1. 历史 ST 名称没有进入涨跌停和连板计算，造成 212 个标的的 7,433 行涨停连板、6,672 行跌停连板与历史名称口径不符，属于未来信息污染。
2. 盘中增量指标与批量公式不一致。30 个真实标的中 MACD、BOLL、年化波动率、60 日极值和 RSI 全部或大部分不一致；窗口未满时还会提前产生 MA/BOLL/极值。
3. EasyTDX 分红历史有 7,259 / 51,390 行 `cash_per_share` 不能由当前分红方案解析公式复现，影响 3,421 个标的。该表只能作为辅助记录日上下文，不能用于权威复权或事件回放。
4. 指数日线有 495 行负成交量，覆盖 7 个指数；`000691.SH` 同时出现约 `1e18` 的异常成交额，符合有符号 32 位溢出/字段解码错误特征。

整改后当前状态：前 3 组 P1 已完成代码修复和生产重算；股票 enriched、valuation 仍各为 6,160,370 个唯一键，EasyTDX 分红 7,259 行差异已归零并从 4,005 个文件压缩为 27 个年度文件，股票分钟 149,179 行空 OHLC 已清除。严格强化开盘啦归档回放后又发现龙虎榜席位明细旧主键会覆盖同股同方向不同上榜原因的记录，现已将主键升级为 `(symbol,side,log_id)` 并从原始归档恢复 78 行，720 行修复为 798 行。指数异常仍存在：同源在线重拉的 8,477 行与本地逐字段一致，远端本身仍返回异常值，现行入口门禁会拒绝再次发布。财务 8 组冲突也仍存在，当前账号没有 TickFlow financial 能力，无法取得 revision 证据，禁止按文件顺序猜测修复。分钟历史覆盖不足、指数分钟缺失和 depth5 历史不可回补仍是能力边界。

原始只读审计阶段没有启动应用、collector、刷新或重建任务。原因是应用启动会注册并可能触发后台采集，不符合“审计前后市场数据不变”的约束。后续生产整改及其数据变更单独记录在 11.2.1 和 11.5，不能与原始只读基线混为一谈。

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
| `kline_daily_enriched` | 日频；`symbol,date`；按日分区 | 6,160,370 行，与日线逐键对齐 | TickFlow 派生；前复权 OHLC、原始价、股本、换手率、连板；策略/回测/监控 | 通过：已按历史名称和当前股本 source snapshot 全量重建 |
| `valuation_daily` | 日频；`symbol,date`；按日分区 | 6,160,370 行，与 enriched 逐键对齐 | TickFlow 派生；金额元、比率倍数；估值 API/策略 | **部分通过**：公式和股本输入已重建，但财务 8 组冲突仍未取得 revision 证据 |
| `kline_index_daily` | 日频；`symbol,date`；按日分区 | 704,552 行，`2021-08-02..2026-07-31` | TickFlow 指数行情；指数看板/市场背景 | **不通过**：495 行负成交量，`000691.SH` 成交额异常 |
| `kline_index_enriched` | 日频；`symbol,date`；按日分区 | 704,552 行，与指数日线逐键对齐 | 指数派生；监控/市场概览 | **不通过**：完整继承指数原表异常 |
| `kline_etf_daily` | 日频；`symbol,date`；按日分区 | 1,123,552 行，`2021-08-02..2026-07-31` | TickFlow ETF 行情；ETF 回测/策略/行情 API | 通过 |
| `kline_etf_enriched` | 日频；`symbol,date`；按日分区 | 1,123,552 行，与 ETF 日线逐键对齐 | TickFlow 派生；ETF 策略/监控 | 通过 |
| `kline_minute` | 分钟；`symbol,datetime`；按交易日分区 | 327 文件，386,663,172 行，`2025-03-28..2026-07-31` | TickFlow；北京时间左开右闭，`09:30` 为竞价例外；分钟回测/分时 | **部分通过**：空 OHLC 已清除，历史覆盖仍不足 |
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
| `financials/shares` | 生效事件；`symbol,period_end,announce_date` | 422,834 行；无重复键；62,838 行 `period_end > announce_date` | `announce_date` 为生效日、`period_end` 为对应股本日期；换手率/市值 | 通过：语义合理，派生表已按冻结 source snapshot 重建 |
| `corporate_actions` | 事件；`symbol,event_date` | 53,538 行，`1991-05-02..2026-08-07`；无重复/非正现金分红 | TickFlow 权威除权事件；复权和历史回放 | 通过 |
| `depth5` | 日内快照；`symbol` + 分区日 | 646 行，仅 6 个交易日 `2026-07-24..2026-07-31` | TickFlow 实时五档；封板状态 | 部分通过：现有数据合法，历史能力不可回补 |
| `fund_nav` | 基金日净值；`symbol,date`；按 symbol 分区 | 773,022 行、695 只，`2004-12-29..2026-07-31` | 基金净值；免费策略净值基准 | 部分通过：36 文件新增 `date_timezone`，659 文件为旧 schema |
| `instruments` | 当前股票快照；`symbol` | 5,537 行，无重复/非法代码 | TickFlow 维表；当前名称、股本、涨跌停价 | 通过（仅当前快照） |
| `instruments_index` | 当前指数快照；`symbol` | 612 行 | 指数路由与名称 | 通过 |
| `instruments_etf` | 当前 ETF 快照；`symbol` | 1,648 行 | ETF 路由、名称和基金元数据 | 通过 |
| `instrument_name_history` | 名称事件；`symbol,change_date` | 7,493 行 | 历史 ST/名称判定 | 通过：历史连板生产链路已消费并重建 |
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
| `ext_kpl_lhb_detail` | 日频；`symbol,side,log_id`；798 行；schema v2 | 开盘啦席位明细；`reason_type`、`rank` 保留为属性 | 通过：从 72 份归档恢复旧主键覆盖的 78 行 |
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
| `ext_tdx_dividend_history` | 记录日；`symbol,record_date,plan`；51,390 行 | EasyTDX 7615 已实施分红；不替代 TickFlow 除权事件 | 通过：7,259 行已修正，冻结公式复核差异 0 |

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
- 原始基线的 149,179 行空 OHLC 已由影子修复清除；修复后 386,663,172 行，空 OHLC 为 0，原 327 个文件中 64 个重写、263 个硬链接，旧目录保留用于回滚。
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
- `ext_tdx_dividend_history` 原 51,390 行分散在 4,005 个日期分区；整改后按年压缩为 27 个文件，主键和行数不变。
- `fund_nav` 695 个 symbol 分区符合按基金读取场景，但全市场扫描时仍应走 dataset scanner，避免逐文件 Python 循环。

## 6. 派生链路和 PIT 对账

### 6.1 复权、股本、换手率和市值

- 使用 `adj_factor.ex_factor` 独立构造累计因子，对全部股票和 ETF 日线重算前复权 OHLC，股票与 ETF 四价错配均为 0。
- 股票日线、enriched、valuation 共 6,160,370 个键完全一致。
- stored enriched 与 valuation 的 `raw_close,total_shares,float_shares` 一致；换手率、市值、流通市值、PE/PB/PS/PCF 公式错配均为 0。
- 原始基线使用当前 `financials/shares` 和 `instruments` 重算时，`total_shares` 有 134 行、`float_shares` 有 236 行漂移。全量重建后，与旧表相比对应列分别改动 134 和 169 行；当前 metadata 固化了 `shares`、`instruments`、历史名称、复权和日线的逐文件 hash。仅用历史股本、不带 `instruments._instrument_as_of` 复核时剩余 4/46 行差异全部位于 `2026-07-31`，属于当日安全使用当前维表快照，不是历史未来数据回退。

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

原始审计时，`instrument_name_history` 显示 304 个标的、213,608 个交易日的历史名称状态与当前名称不同；旧生产 enriched 的历史连板值与“当前名称套用全部历史”结果完全一致，证明当时历史名称没有进入调用链。

- 按历史名称重算，`consecutive_limit_ups` 有 7,433 行、212 个标的不一致；
- `consecutive_limit_downs` 有 6,672 行、212 个标的不一致；
- ST/`*ST` 的 5% 价格限制被错误按当前 10% 或反向错误套用于历史，直接影响连板策略、涨跌停筛选和历史回测。

整改后历史名称已进入批量和盘中路径，全量重建相对旧表实际改动上述 7,433/6,672 行，行数和主键保持不变。

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
- 初版回放只比较 Parquet 已存在字段，不能发现落盘 schema 丢列；强化后同时校验配置字段/类型/label hash、Parquet 缺失或额外字段、分区日期、重复主键、回放缺值和确定性 revision；
- 强化回放发现 `dragon_tiger_details` 原始 parser 产生 798 行，旧表只有 720 行。8 份归档内出现相同 `(symbol,side,rank)`，因为同一股票的多个上榜原因组可各自从 rank 1 开始；旧 `atomic_upsert_records` 因此静默覆盖 78 行，并带来 218 个字段值错配；
- 798 个上游 `LogID` 全部唯一。修复将主键改为 `(symbol,side,log_id)`，新增 `reason_type`，以 72 份原始归档影子重建并原子发布 schema v2；旧 Parquet 和配置保留为回滚副本；
- 修复后严格回放状态为 `passed`：12 张扩展表均为 schema/类型/分区/重复主键/缺失主键/额外主键/字段值错配 0。典型对账：资金 5,537/5,537、涨停 100/100、龙虎榜汇总 72/72、龙虎榜席位 798/798、北向个股 3,807/3,807、十大股东 54,662/54,662、股东人数 17,614/17,614、板块成分 30,667/30,667。

整改实现已将该检查产品化为只读门禁：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/replay_kaipanla_archives.py \
  --data-dir ../data --output /tmp/tickflow-kaipanla-replay.json
```

实际结果为 `status=passed`、20,112/20,112 解析成功、错误 0；12 张表全部通过，其中竞价表为有原始证据的合理空表。输出只写显式指定的隔离路径，不调用在线 client、collector 或生产 storage。

### 7.3 EasyTDX 在线只读抽样

审计基线按稳定排序选取 30 个标的：沪 12、深 12、北 6；沪深各含 6 个已有预告记录和 6 个无预告记录，北交所覆盖当前无行业/预告记录场景。直接调用 client/parser，不调用 collector，不写结果：

- 行业列表返回 5,209 行；24 个沪深样本与落盘行业编码 0 差异。6 个北交所样本不在当前 `_symbol()` 行业映射中，属于明确能力缺失；
- F10 30/30 返回：10 个标的含两融表，共 204 行，与落盘 0 差异；12 个含正式业绩预告，与落盘 0 差异；30 个均无正式业绩快报章节；
- 分红历史返回 283 行、失败 0；主键均已落盘，但 41 行 `cash_per_share` 与当前方案解析不同；
- 将分红全表按当前 `cash_per_share_from_plan(plan)` 重算，7,259 行、3,421 个 symbol 不一致。典型错误如 `10转增3股派3元` 被存成 `1.0`，正确每股现金为 `0.3`。

开盘啦另外只读复核 `/15` 和 `/100`：均成功，分别返回 100 行涨停列表和 `2026-07-31` 的 72 行龙虎榜，与归档/落盘规模一致。没有调用在线竞价 collector。

### 7.4 EasyTDX 与开盘啦入库优化方案

以下内容是基于本次审计证据形成的实施方案。审计阶段保持只读，没有调用 collector、刷新任务或在线结果落盘；后续整改按 7.4.4 的顺序执行，当前代码和数据验收状态见 7.4.5 与 11.5。任何“已修复”结论同时要求代码门禁、影子校验和生产数据证据。

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
7. **小文件治理**：分红表从原始 4,005 个小分区迁移为按年或月 compact 的物理布局，同时保留 `record_date` 列和分区统计以支持逻辑查询。compaction 只在新文件校验成功后更新 manifest，不与采集任务原地改写同一批文件。

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

本节记录已进入 `custom` 的前序整改、本轮 `codex/data-audit-implementation-20260802` 实现及生产数据验收。隔离分支开发和测试没有运行 EasyTDX/开盘啦 collector；龙虎榜修复只使用已有原始归档走影子校验和原子切换。首次整改合并到仍运行且启用 `uvicorn --reload` 的 `custom` 后曾触发既有启动任务，例外见 11.2.1；本轮执行前确认应用端口无监听，并先合入维护启动开关。

| 能力 | 状态 | 已验证内容 | 尚未完成/不能宣称 |
| --- | --- | --- | --- |
| EasyTDX 批次与恢复 | `custom` 已落地 | 50 标的稳定批次；数据集独立 manifest/checkpoint；失败批次重试；staging 与原始内容 hash；相同输入恢复；manifest 损坏时 fail-closed | 尚未对全市场真实任务做中断恢复演练；尚未建立运维告警面板 |
| EasyTDX 空数据判定 | `custom` 已落地 | F10 批次必须返回全部请求标的，缺标的记 `source_error`；分红底层逐标的失败会使整批失败；正式章节缺失才可记 `section_absent` | 北交所行业仍是既有能力边界，未新增其数据；真实快报首次出现后的落盘仍待在线验收 |
| EasyTDX 分红修复 | 生产数据已验收 | 51,390 行影子重算后原子切换；7,259 行差异归零；4,005 个日期文件压缩为 27 个年度文件；旧目录和 manifest 可回滚 | 权威复权仍只使用 TickFlow `corporate_actions`；未把辅助分红记录升级为除权事件 |
| 开盘啦分页与竞价 | `custom` 已落地 | `/115`、`/30`、`/100` 统一分页上限、逐页归档和 manifest；分页失败不发布；竞价三检查点与 `/31` 记录组件完成状态；`/31` 任一标的失败时不发布该批明细 | 竞价仍无业务行，但 2,681 份 `/30` 原始响应均已证明为 `valid_empty` |
| 开盘啦扩展端点 | 本轮已落地 | 资金榜/分时/日度、北向板块/个股、股东人数窗口/分页、十大股东、板块发现/成分、涨停、龙虎榜主/扩展明细和监管端点均记录独立批次；请求失败、解析拒绝、合理空、分页上限和完整发布可区分；不完整组件不覆盖上一有效数据 | 尚未进行全市场真实故障恢复演练 |
| 开盘啦归档回放 | 本轮已落地并验收 | 产品化只读回放器兼容旧 envelope 和带 hash/parser version 的新 envelope；20,112/20,112 解析成功，严格校验 12 张表 schema、类型、分区、主键和字段值 | 现存旧归档没有 parser version/hash 字段，只能在后续新归档中逐步建立版本链 |
| 开盘啦龙虎榜修复 | 本轮生产数据已验收 | 识别旧 `(symbol,side,rank)` 冲突；以 72 份归档恢复 78 行；schema v2 的 798 行主键唯一，严格回放全部通过 | 这是扩展表内部 schema 升级；旧配置和旧 Parquet 保留用于回滚，不改变 canonical schema |
| 维护启动 | 本轮已落地 | `TICKFLOW_SKIP_COLLECTOR_BOOTSTRAP=1` 跳过 EasyTDX bootstrap、开盘啦 catch-up 和通用扩展即时首拉，同时保留定时任务注册 | 该开关不禁用到点执行的正常 cron，维护窗口仍需避开 cron 或停止调度器 |
| 入库健康门禁 | 本轮已落地 | 只读 CLI 汇总每源/数据集最新 manifest，输出批次状态、失败批、发布/拒绝行数；损坏、路径契约错位和失败状态均非零退出；没有新增公开 API | 活动 `_ingestion` 目录在只读审计后已清理，当前结果为 `no_data`；待下一次受控采集生成 manifest 后接入外部告警 |
| 数据所有权 | `custom` 已落地 | 扩展配置持久化 `schema_version`、authority、canonical dataset、overlap policy 与 allowed usage；负向测试保持 EasyTDX/开盘啦不能替代 TickFlow 权威字段 | 未改变公开 API 或 canonical schema；消费者新增扩展用途时仍需同步增加 authority 测试 |

EasyTDX/开盘啦的上线顺序保持 A-D 不变。当前可判定为：阶段 A 已完成生产修复和回滚留存；阶段 B 的核心采集骨架已落地；阶段 C 的逐端点隔离、完整分页、严格全归档回放和龙虎榜迁移已完成代码及生产验收；阶段 D 已具备 manifest、机器可读健康门禁和 EasyTDX 分红 compaction，但外部告警接入、开盘啦小文件 compaction 和全市场真实中断恢复演练仍未完成。

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
| 涨跌停/连板 | 交易板块、日期、ST 状态决定比例，按 tick size 舍入 | 必须使用原始价和历史名称；整改后批量与盘中路径一致 |
| 换手率 | `volume/float_shares*100` | 历史股本优先，缺失才回退当前维表 |
| 前复权 | 日 OHLC 乘基于事件因子的累计比例 | `raw_*` 保持原价 |
| 估值 | 市值/TTM 或时点权益 | 公告日 PIT；负/零分母输出 null |

### 8.2 30 标的真实数据重算

以下数字为原始审计基线。使用前 300 个自然日（约 210 个交易日）的原始 OHLCV，独立 Python 循环实现上述公式，不调用生产指标函数作为 expected。样本为沪 12、深 12、北 6，且每只至少 180 条记录。

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

MA、EMA5/10/20/30/60、KDJ、ATR、量比、动量、涨跌幅/额和振幅均 0 错配。整改后盘中路径复用批量公式和完整历史窗口，新增逐列一致性测试覆盖 MACD、BOLL、波动率、60 日极值、RSI 和窗口 gate；原始错配不再代表当前代码状态。

### 8.3 固定边界样本

以下前三项同样记录原始缺陷；整改后 KDJ 零区间冻结为中性 RSV、窗口未满保持 null，除权和股本边界继续保持原契约。

- 80 日恒定价格：BOLL 上下轨 10、ATR 0、年化波动率 0、量比 1、RSI 0；K/D/J 为 NaN，因为代码只对 null 分母填 `1e-12`，没有处理实际的 0 区间。
- 只有 10 日历史后计算第 11 日：批量 `ma20/30/60,boll,high_60d` 均为 null；增量却分别输出 `5.0/3.3333/1.6667`、BOLL 上轨 15、`high_60d=10`。`_window_len` 已构造但没有用于 gate。
- 除权：股票和 ETF 全表独立复权重算 0 错配；涨跌停测试覆盖 ST、主板、创业板/科创板、停牌和原始价边界。
- 缺历史股本：生产规则与测试均明确“历史股本优先、无可用历史记录时回退当前维表”，但回退数据不是历史 PIT 事实，报告和策略应能识别该状态。

### 8.4 与常见平台的口径差异

以下差异本身不直接判 bug，只有批量/增量违反项目冻结公式才判错：

- BOLL 常见实现既有 `ddof=0` 也有 `ddof=1`；项目批量冻结为 `ddof=1`。
- RSI 常见实现有 Wilder 初始 SMA、递推平滑和简单滚动均值；项目使用从首日 0 gain/loss 开始的 `alpha=1/N` EWM，恒定价格得到 0 而不是部分平台的 50。
- KDJ 常见实现从 K/D=50 开始；项目从首个有效 RSV 开始。整改前零波动区间得到 NaN，现已冻结为中性 RSV=50，窗口未满仍保持 null。
- 60 日“新高/新低”可以定义为收盘极值或日内 high/low；项目批量冻结为 close 极值，因此增量路径必须一致。

## 9. 已确认问题与修复顺序

本次没有发现 P0 数据破坏或凭据泄漏问题。

| 优先级 | 问题与影响 | 修复/重算方案 |
| --- | --- | --- |
| 已解决 P1 | 历史 ST 连板 14,105 个字段行差异 | 已接入历史名称、全量重建并保留旧目录；新旧表对应列实际改动 7,433/6,672 行 |
| 已解决 P1 | 盘中 MACD/BOLL/波动率/极值/RSI 和窗口 gate | 已统一批量公式、完整历史窗口和逐列一致性测试 |
| 已解决 P1 | EasyTDX 分红 7,259 行错误 | 已影子校验、原子发布并保留 4,005 文件旧目录；冻结公式剩余差异 0 |
| 阻塞 P1 | 指数负量/异常成交额 | 入口已 fail-closed；同源在线 8,477 行与本地逐字段一致且同样异常，等待 TickFlow 修正或提供可验证 revision，不能本地猜值 |
| 阻塞 P2 | 财务原表重复及 8 组冲突 | 下游已拒绝同键冲突；当前无 financial 能力，无法在线核源，必须取得供应商 revision/update ID 后修复并重建 valuation |
| 已解决 P2 | 股本源刷新后派生漂移 | enriched/valuation 已按固定 source snapshot 全量重建并保留旧目录 |
| 部分解决 P2 | 股票分钟空 OHLC 和分钟覆盖不足 | 149,179 行空 OHLC 已清除；覆盖 manifest/fail-closed 已落地，历史覆盖不足仍保留为能力边界 |
| 已解决 P2 | KDJ 零区间为 NaN | 批量与增量统一中性 RSV 并补恒定价格测试 |
| 已解决 P2 | 开盘啦龙虎榜明细旧主键覆盖 78 行 | 主键升级为 `symbol,side,log_id`，保留上榜原因和 rank；72 份归档影子重建为 798 行并严格回放 |
| P3 | `fund_nav` 两种 schema、旧扩展配置缺 authority 持久字段 | 在不改变公开 schema 的前提下登记 schema version；迁移配置元数据或在审计中持续校验 runtime 补全 |
| 已解决 P3 | EasyTDX 分红 4,005 个小分区 | 已按年 compaction 为 27 个文件并维护 repair manifest，逻辑主键和行数不变 |

推荐重算顺序：先修生产公式和确定性去重规则，再修原表，最后依次重建 `kline_daily_enriched -> valuation_daily -> 指数 enriched -> 依赖缓存/策略结果`。不能先重算后修公式，否则会把已确认错误扩大到全历史。

## 10. 不可验证项和剩余边界

- 没有指数分钟表，无法验证指数 1 分钟聚合或覆盖。
- depth5 只有 6 个交易日，历史深度不能从日线/分钟线推导，不能验证更早封单状态。
- 当前 pools、行业和 instruments 是快照，不能证明历史 PIT 成分或历史行业归属。
- EasyTDX 业绩快报全表为空；全市场采集结果和 30 标的在线样本均没有正式快报章节，因此判为合理空数据，但不能证明未来出现快报时的实际落盘行为；parser/collector 定向测试承担该契约验证。
- 在线抽样只能证明审计时点的 30 个标的和两个开盘啦端点，不等于供应商所有代码、所有历史日期的 SLA。
- 入库健康 CLI 当前为 `no_data`，因为活动 `_ingestion` 在只读审计后已清理；门禁行为已由故障注入测试证明，但真实告警必须等下一次受控采集产生 manifest 后验收。
- 财务 8 组冲突和指数源异常仍保留在数据湖中；其余“已解决”项均有生产 repair manifest、source snapshot 或回滚目录证明，不能仅以测试通过替代数据验收。

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

#### 11.2.1 整改合并时的热重载例外

`custom` 合并后，正在运行的 `uvicorn --reload` 于 `2026-08-02 03:38:11 +08:00` 重启应用，并触发既有启动采集：

- 开盘啦历史竞价回补调用 `/30`，新增 60 份空响应原始归档和 120 份 manifest；确认没有生成竞价业务行后，已将这些文件移出活动数据湖，`_kaipanla_raw` 文件数恢复为审计基线 20,112，活动数据湖中不保留 `_ingestion` 目录；
- `ext_gn_ths`、`ext_hy_ths`、`ext_money_flow` 三张已配置的辅助快照被启动任务刷新，行数仍分别为 5,547、5,542、5,546；旧 Parquet 字节版本没有可用备份，因此不能宣称这 6 个 Parquet/config 文件的 hash/mtime 未变化；
- canonical 股票/ETF/指数行情、分钟、复权、财务、股本、估值、公司行动及本报告新增的影子修复目标均未被该重载改写；运行中的模拟账户文件继续按其既有任务更新，不归因于审计重算。

因此，11.2 的 34,096 文件一致性结论只对应原始只读审计提交；整改合并后的结论是“canonical 市场数据未变，但 3 张辅助扩展快照发生一次启动刷新”。

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

原只读审计报告提交时的实际结果为 `134 passed, 4 warnings in 5.49s`。最终报告合入后在 `custom` 复跑同一组命令，实际结果为 `154 passed, 6 warnings in 4.73s`；新增用例来自当前测试文件扩充，6 条 warning 均为既有 Polars `collect(streaming=True)` 参数弃用提示，没有测试失败。`git diff --check` 通过。该结果对应原报告提交，不代表后续整改没有运行时改动；本轮实现验证见 11.4。

### 11.4 整改实现验证

整改覆盖历史名称 PIT、批量/盘中指标逐列一致、KDJ 零波动、财务同键冲突拒绝、派生 source snapshot、EasyTDX 批次恢复/缺标的响应/损坏 manifest、开盘啦全部基础及逐股/逐板块组件、指数异常批次、分钟完整度与影子清理、分红影子修复、enriched/valuation 原子发布和回滚目录。本轮另外落地 20,112 份归档严格回放、龙虎榜明细 schema v2 迁移、机器可读入库健康门禁，以及禁止启动时立即触发辅助采集的维护模式。

实际执行命令：

```bash
cd backend
PYTHONPATH=. .venv/bin/python -m pytest -q
PYTHONPATH=. .venv/bin/python -m ruff check app tests scripts
PYTHONPATH=. .venv/bin/python scripts/replay_kaipanla_archives.py \
  --data-dir ../data --output /tmp/tickflow-kaipanla-replay-post-repair.json
PYTHONPATH=. .venv/bin/python scripts/check_ingestion_health.py \
  --data-dir ../data --source easy_tdx --source kaipanla
git diff --check custom...HEAD
```

隔离工作树使用 `uv sync --extra dev --extra easy-tdx` 建立独立环境。已进入 `custom` 的上一整改提交结果为 `954 passed, 13 warnings in 11.93s`；补充维护启动、开盘啦全端点门禁和初版归档回放后为 `963 passed, 13 warnings in 23.72s`；本轮强化 schema/主键回放、龙虎榜修复、剩余基础端点 manifest 和健康门禁后为 `978 passed, 13 warnings in 23.95s`，快进到 `custom` 后复跑为 `978 passed, 13 warnings in 16.67s`。Ruff 对 `app tests scripts` 全部通过，`git diff --check` 通过，合并后严格回放仍为 20,112/20,112 归档和 12/12 表通过。13 条 warning 均为既有 Polars streaming 参数或 `datetime.utcnow()` 弃用提示。没有新增公开 API；canonical schema 和前端类型不变，唯一 schema 变更是辅助扩展表 `ext_kpl_lhb_detail` 从 v1 升级到 v2。

### 11.5 生产整改与外部阻塞证据

| 项目 | 当前结果 | 回滚/边界 |
| --- | --- | --- |
| EasyTDX 分红 | repair manifest：51,390 行、修正 7,259 行、差异归零、4,005 文件变 27 文件 | 旧目录 `timeseries.pre-repair-20260801T185338Z-a9caa86b`；`corporate_actions` 聚合 hash 前后保持 `e861b74e...d99` |
| 股票分钟 | 386,812,351 -> 386,663,172 行；拒绝 149,179 行；64 文件重写、263 文件硬链接；空 OHLC 复核 0 | 旧目录 `.kline_minute.pre-repair-20260801T190525Z-54f52795`；覆盖不足未伪装修复 |
| ETF 分钟 | 83,741,815 行；拒绝 0；248 文件全部硬链接，仅生成 coverage manifest | 旧目录 `.kline_etf_minute.pre-repair-20260801T192030Z-b081bd8a`；最近两日覆盖不足仍存在 |
| 股票 enriched | 6,160,370 行和唯一键不变；相对旧表连板列改动 7,433/6,672 行，股本列改动 134/169 行；metadata 固化全部输入 hash | 旧目录 `.kline_daily_enriched.pre-rebuild-20260801T185833Z` |
| valuation | 6,160,370 行和唯一键不变；按新 enriched 和固定财务 source snapshot 重建 | 旧目录 `.valuation_daily.pre-rebuild-20260801T190039Z`；财务原表 8 组冲突仍需供应商核源后再次重建 |
| 开盘啦龙虎榜明细 | 720 -> 798 行；恢复 78 行；`(symbol,side,log_id)` 798 个唯一键；schema v2；字段 contract hash `2481d895...53c0` | 旧目录 `timeseries.pre-repair-20260801T205054Z-84730081` 和旧配置可回滚；修复仅修改该表 3 个活动文件 |
| 开盘啦回放 | 20,112/20,112 归档解析成功；错误 0；12 张表 schema/类型/分区/主键/字段值全部通过；`/30` 2,681 份均为 `valid_empty` | 报告写到 `/tmp`；除已声明的龙虎榜修复外不修改数据湖 |
| 指数在线核源 | 7 个指数、8,477 行在线 TickFlow 与本地逐 OHLCVA 字段一致；在线结果仍触发质量门禁，本地异常未改 | 等待上游修正或提供 revision；不得用其他辅助源静默替代 |
| 财务在线核源 | 当前 `financial_capability=False`，8 组冲突无法在线取证 | 保留 fail-closed；取得 Expert 能力或供应商 revision 后再修原表和 valuation |

本轮数据执行前确认 `3011`、`3018` 无监听进程；没有调用 EasyTDX/开盘啦在线 collector。龙虎榜修复从已有 72 份归档重建，执行窗口内 `data/` 新 mtime 仅出现在该表的 `config.json`、`part.parquet` 和 `repair-manifest.json`；备份文件 hash 与修复前活动文件逐字节一致。`TICKFLOW_SKIP_COLLECTOR_BOOTSTRAP=1` 已随本轮实现落地，但它只跳过启动首拉，不禁用到点 cron。
