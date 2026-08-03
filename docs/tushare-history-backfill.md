# Tushare 缺口落盘运行手册

本流程使用 `https://teajoin.com/` 的 Tushare 兼容协议，补齐 TickFlow 本地
Parquet 中缺失的 A 股、ETF、指数历史。TickFlow 已有主键始终保留；重叠值超出
容差时整张数据集阻断发布。应用运行时只读取本地 Parquet，不会把 Tushare 作为
网络查询 fallback，也不安装 Tushare SDK。

## macOS Apple Silicon 安装

支持平台为 `osx-arm64`（Apple Silicon）。在仓库的 `backend/` 目录执行：

```bash
uname -m                         # 必须输出 arm64
uv sync --extra dev --frozen
uv run python -c 'import platform; print(platform.system(), platform.machine())'
```

客户端使用 Python 标准库 HTTP，不引入额外原生 SDK。发布前必须在同一台
`osx-arm64` 机器完成上述安装、定向测试和 smoke。

## 密钥与目录

密钥只允许从 stdin 写入 `DATA_DIR/user_data/secrets.json`，文件权限为 `0600`：

```bash
cd backend
printf '%s\n' "$TUSHARE_PROXY_API_KEY" | uv run python scripts/backfill_tushare_history.py \
  --data-dir /path/to/data --key-stdin --preflight --run-id preflight-20260803
```

不要把 key 放在命令参数、环境转储、日志或 manifest 中。原始响应以 gzip 归档在
`ext_data/_tushare_proxy_raw/snapshot=<run-id>/`；staging、manifest 和发布备份位于
`backfill_state/tushare_proxy/<run-id>/`。

## 1. 预检

先验证端点权限、字段、页长和空响应语义：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py \
  --data-dir /path/to/data --run-id preflight-20260803 --preflight \
  --datasets reference,daily,financials,factors
```

`ok` 表示端点返回过字段和数据；`empty_unconfirmed` 不能当作合法空数据；权限、
协议或网络错误分别记为 `blocked/error`。当前只有已确认不存在记录的 `express`
允许全端点 `valid_empty`，其他数据集全空都会阻断。

## 2. 5+1+1 staging smoke

先用 5 只股票、1 只 ETF、1 个指数做短窗口 staging，不加 `--publish`：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py \
  --data-dir /path/to/smoke-data --run-id smoke-20260803 \
  --datasets stock_basic,etf_basic,index_basic,trade_cal,namechange,index_member_all,index_weight,daily,daily_basic,fund_daily,index_daily,income,dividend,moneyflow,forecast \
  --symbols 000001.SZ,000002.SZ,600000.SH,600519.SH,300750.SZ \
  --etfs 510300.SH --indexes 000300.SH \
  --start 2025-01-01 --end 2025-12-31
```

检查以下文件：

- `manifest.json`：失败/阻断批次、请求参数 hash、行数和发布状态；
- `capability_matrix.json`：日期、标的、字段非空率、失败和未确认空批次；
- `ext_data/_ingestion/tushare_proxy/<dataset>/<run-id>.json`：数据集级审计；
- `datasets/<dataset>/*.parquet`：标准化 staging；
- 原始响应归档：确认 token 未出现。

## 3. 历史回填与发布

推荐按依赖顺序使用独立 run；日线、财务和参考数据从 `2010-01-01` 开始：

```bash
cd backend

# 参考数据和 PIT 历史
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id reference-2010 --datasets reference --start 2010-01-01

# 股票、ETF、指数日线和 daily_basic
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id daily-2010 --datasets daily --start 2010-01-01

# 财务、历史股本与现金分红
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id financials-2010 --datasets financials --start 2010-01-01

# 因子扩展表
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id factors-2010 --datasets factors --start 2010-01-01
```

确认 staging 和冲突报告后，用相同参数和 run id 加 `--resume --publish`。发布会先
审计整张数据集的所有分区，再原子替换；canonical 重叠行由 TickFlow 保留。成功后
重建 `kline_daily_enriched`、ETF/指数 enriched 和 `valuation_daily`。扩展表只有发布
成功后，`config.json` 才会出现 `factor-input`。

分钟历史使用现有阶段，反向游标每页最多 8,000 行，实际最早日期写入能力矩阵：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id minute-history --phases universe,adjustment,stock_minute,etf_minute,publish_minute \
  --publish
```

只请求和保存 1 分钟数据；5/15/30/60 分钟由本地读取层聚合。股票与 ETF 分别发布
到 `kline_minute` 和 `kline_etf_minute`，价格按复权因子处理，成交量和成交额不复权。

## 4. 恢复与现有 run

读取状态不访问网络：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --status --run-id 019fc69b-b41a-7311-9407-9c30c5a1f1bc
```

恢复时必须使用与原 manifest 相同的阶段和样本范围。现有
`019fc69b-b41a-7311-9407-9c30c5a1f1bc` 的原始归档和已完成复权批次不得删除；恢复
会跳过已有完整 batch：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id 019fc69b-b41a-7311-9407-9c30c5a1f1bc --resume \
  --phases universe,adjustment,stock_minute,etf_minute
```

失败批次可用相同 run id 重试。`failed/blocked` 不会被解释为空数据，也不会覆盖上次
有效发布。

## 5. 增量、周审计与资格门槛

应用启动时，如果本地存在 key，会把以下任务注册到现有 APScheduler：

- 交易日北京时间 18:40：最近 10 个自然日增量；日线和事件按 `trade_date` 或
  `ann_date` 全市场请求，分钟按已有最大时间向后补；
- 周日北京时间 02:00：全表缺口、主键、空键和 PIT 审计。

周审计会检查日线覆盖区间内的交易日缺口、财务公告日早于报告期、指数/行业成分
区间非法或重叠，以及 `valuation_daily` 中公告日在行情日之后的前视记录。供应商已
明确确认且成功发布的合法空集保留为 `valid_empty`，不会误报为文件缺失。

自动任务初始只 staging。连续两次增量成功且至少一次周审计为 `healthy` 后，
`automation_state.json` 才启用自动发布；之后任一增量失败会把连续成功次数清零。
单张数据集失败不阻断其他数据集，上一次有效 Parquet 保持不变。

## 6. 冲突、回滚和审计

- 行情比较 OHLC、成交量和成交额容差；超容差阻断整张数据集；
- 财务主键为 `(symbol, period_end, announce_date)`，优先使用实际披露日
  `f_ann_date`；canonical 选择最高 `update_flag`，全部供应商版本按修订 hash 留在
  `financials/_revisions/`，同版本仍有数值冲突时阻断发布；
- 指数和行业成分包含 `effective_from`、`effective_to`、`source_snapshot_date`；
- 分红只有除权除息日和正的税前每股现金金额都有效时才进入回放表；
- 发布前版本保存在 `<run-id>/backups/`，发布 staging 在 `<run-id>/publish_staging/`。

发生异常时先停止自动任务，保留 manifest、原始归档和冲突列表。可以把
`backups/` 中对应相对路径恢复到数据目录；不要删除整个 run。恢复后执行：

```bash
cd backend
uv run python scripts/backfill_tushare_history.py --data-dir /path/to/data \
  --run-id weekly-audit-manual --phases audit
```

## 7. 验收

在 `osx-arm64` 上执行：

```bash
cd backend
uv sync --extra dev --frozen
uv run pytest tests/test_tushare_ingestion.py tests/test_tushare_history.py \
  tests/test_tushare_automation.py tests/test_tushare_history_cli.py \
  tests/test_data_authority.py -q
uv run ruff check app scripts tests
```

验收要求：主键唯一、无未审计发布分区、无超容差冲突、财务和事件无公告日前可见、
失败批次可恢复、能力矩阵与实际覆盖一致。期货、期权、港股和美股不在本流程范围，
期货仍明确为无数据源。
