# Tick 回测与实时对比

## 能力边界

- Tick 回测只接入「自定义 → 量化策略」主链路，旧因子/信号回测不变。
- 首版只支持股票；ETF 选择 Tick 会直接拒绝，不会降级到分钟 K。
- 历史 Tick 来自 QMT ZMQ RPC，回测只读取本地 `data/tick/date=YYYY-MM-DD/part.parquet`。
- 任一标的或交易日缺数据时 fail-closed。

## 导入 QMT 历史 Tick

QMT 地址和账户仅通过本地 `.env` 配置：

```dotenv
QMT_ENABLED=true
QMT_ZMQ_CONNECT_ADDRESS=tcp://qmt-host.example:15648
QMT_ACCOUNT_ID=your-account-id
```

```bash
cd backend
uv run python scripts/import_qmt_ticks.py \
  --symbols 600000.SH,000001.SZ \
  --start 2026-08-03 \
  --end 2026-08-07
```

每个交易日的全部请求都成功并通过 schema 检查后，才会原子替换当日分区。

## TickFlow/QMT 实时对比

QMT 全推 PUB 默认使用 RPC 端口 `+1`。如服务端另行配置，在本地 `.env` 设置 `QMT_QUOTE_ZMQ_CONNECT_ADDRESS`。

```bash
cd backend
uv run python scripts/compare_realtime_ticks.py \
  --symbols 600000.SH,000001.SZ \
  --duration 1800
```

原始观测和报告默认写入 `data/user_data/tick_latency/`，不进入 Git。工具只调用 QMT 的 `subscribe_whole_quote` / `quote_keepalive` / `unsubscribe_whole_quote` 和 TickFlow WebSocket 行情订阅，不调用账户、下单或撤单接口。

默认报告只根据同一观察机的接收时间比较匹配事件。只有确认 QMT、TickFlow 与观察机时钟已同步时，才使用 `--clocks-synchronized` 输出源时间到观察机的延迟。
