# 数据源插件开发指南

数据源插件是可选的行情数据来源(stock-sdk、akshare 等),作为独立模块放在
`backend/app/plugins/` 下。用户**手动安装依赖**后才可用(开发模式);不安装完全不影响主功能。

> ⚠️ **Docker 默认不打包 stock-sdk**(合规考虑:它抓取第三方财经网站接口,存在版权与反爬风险)。如需在 Docker 中启用,构建时传 `--build-arg INCLUDE_STOCKSDK=1`,使用风险自负。下方"手动安装依赖"适用于开发模式及自定义 Docker 构建。

## 快速上手

一个插件 = 一个目录 + 一个 `plugin.yaml` 清单:

```
backend/app/plugins/<your_plugin>/
├── plugin.yaml          # 清单(必需)
├── provider.py          # Provider 实现(必需)
├── ...                  # 桥接/依赖文件(按需)
```

### plugin.yaml 字段

```yaml
name: my_source                          # 唯一标识, 只允许 [a-z0-9_], 也是 provider name
display_name: "我的数据源"                 # 设置页显示名
runtime: python                          # 运行时类型: node | python | none
entry: app.plugins.my_source.provider:MyProvider   # provider 类的导入路径
check: app.plugins.my_source.bridge:availability   # 可用性检测函数(可选)
datasets: [daily, adj_factor, minute, realtime]     # 支持的数据集
description: "数据源描述"
install_hint: "pip install xxx"          # 未装依赖时显示的安装提示
```

### runtime 字段说明

| runtime | 含义 | 典型场景 |
|---|---|---|
| `python` | 纯 Python 依赖, `pip install` | akshare、tushare |
| `node` | 需要 Node.js 运行时, `npm install` | stock-sdk(Docker 默认不打包,见 [deployment.md](./deployment.md)) |

> stock-sdk 在 Docker 中默认不打包(合规考虑);如需启用,构建时传 `--build-arg INCLUDE_STOCKSDK=1`,开发模式下需手动 `npm install`。
| `none` | 无额外依赖 | 纯 HTTP API 源 |

`runtime` 字段当前仅用于 UI 展示, 实际依赖检测由 `check` 函数负责。

### check 函数

插件自己负责检测依赖是否已安装。后端启动时会调用此函数:

```python
# app/plugins/my_source/bridge.py
def availability() -> tuple[bool, str]:
    """返回 (是否可用, 原因)。不抛异常。"""
    try:
        import akshare  # noqa: F401
        return True, "ok"
    except ImportError:
        return False, "未安装 akshare, 运行: pip install akshare"
```

- **可用** → 插件注册进路由表, 设置页可切换
- **不可用** → 设置页显示插件卡片但灰显, 展示 `install_hint`

## Provider 接口契约

Provider 是一个普通 Python 类(无需继承基类), 实现以下方法签名。方法签名对齐
`GenericHTTPProvider`, 这样 services 层(kline_sync / quote_service 等)的路由逻辑
零改动即可路由到插件。

```python
class MyProvider:
    name = "my_source"
    builtin = True  # 标记为内置(不可被用户编辑/删除)

    def __init__(self):
        self.config = MyConfig()  # 需有 .datasets 属性(dict, key 是数据集名)

    def close(self) -> None:
        """清理资源(load_all 重建注册表时会调)。"""

    def get_daily(self, symbols, start_time, end_time, asset_type="stock", on_chunk_done=None) -> pl.DataFrame:
        """日K: 返回 schema [symbol, date, open, high, low, close, volume, amount]"""

    def get_adj_factors(self, symbols, start_time, end_time, asset_type="stock", on_chunk_done=None) -> pl.DataFrame:
        """除权因子: 返回 schema [symbol, trade_date, ex_factor]"""

    def get_minute(self, symbols, start_time, end_time, asset_type="stock", on_chunk_done=None, freq="1m") -> pl.DataFrame:
        """分钟K: 返回 schema [symbol, datetime, open, high, low, close, volume, amount]"""

    def get_realtime(self) -> list[dict]:
        """全市场实时快照: 返回 list[dict], 每行含 symbol/last_price/prev_close/open/high/low/volume"""

    def get_instruments(self, asset_type="stock") -> list[dict]:
        """标的维表(可选): 返回 tickflow Instrument 形状的行, 供 instrument_sync 复用 flatten"""
```

### config.datasets 的作用

`provider_has_dataset(name, dataset)` 通过 `dataset in provider.config.datasets` 判断。
这是 services 层路由的关键: 用户在设置页选了插件, 但某数据集未声明时, 该数据集
自动回退 TickFlow。

```python
class MyConfig:
    datasets = {"daily": ..., "realtime": ...}  # key 是数据集名, value 任意
```

## 现有插件参考

- **`backend/app/plugins/stocksdk/`** — Node 型插件, 通过 subprocess 桥接调用 stock-sdk
  - `bridge.py` — Python↔Node 桥接 + availability 检测
  - `bridge.mjs` — Node 端(并发池、重试、SDK 解析)
  - `provider.py` — Provider 实现(归一化、分批、错误降级)
- **`backend/app/plugins/tushare/`** — 无额外依赖的 Tushare 历史行情插件
  - 通过固定的 HTTPS 代理调用标准 Tushare HTTP 协议,不安装 Tushare SDK。
  - 在「设置 -> 数据源」卡片中配置并验证 Key,可选择股票/ETF/指数日K、股票/ETF
    复权因子及股票/ETF 1分钟K；实时行情继续使用 TickFlow。
  - 日线保持系统的手口径、成交额从千元转换为元；分钟原始归档保留供应商的股口径，进入 provider 或 canonical 发布前转换为手，成交额保持元。
  - 分钟价格在写入 canonical 表前按累计复权因子转换为前复权价格。Key 只保存在本机
    `data/user_data/secrets.json`；清除 Key 后未来拉取回到 TickFlow,已落盘 Parquet 不受影响。

## 路由机制(无需关心, 仅参考)

后端启动时, `loader.py` 的 `_load_builtin_plugins()` 扫描 `plugins/` 目录:
1. 读每个子目录的 `plugin.yaml`
2. 调 `check` 函数检测可用性
3. 可用 → 动态 import `entry` 指向的 Provider 类 → 注册进 `_PROVIDERS`
4. 不可用 → 记录状态, 设置页显示但不可切换

注册后, 插件和用户 YAML 自定义源走**完全相同的路由路径**(services 层的
`provider_has_dataset` / `get_provider` 调用), 无需额外集成代码。

## 辅助采集插件

并非所有外部数据都能替代 `daily`、`minute`、`realtime` 等标准行情数据集。只提供
竞价、榜单、监管标签等扩展字段的来源，应作为辅助采集插件接入，而不是声明成
`MarketDataProvider`：

- 不创建 `plugin.yaml`，不进入 provider discovery，也不出现在主行情切换列表中。
- 在 `app/plugins/<name>/` 内封装固定端点客户端、供应商字段解析、采集调度和失败隔离。
- 输出通过 `ExtConfigStore` 注册为普通扩展表，配置的 `pull` 为空；不得为供应商需求修改
  通用 `ext_data` 拉取器或映射器。
- 复用应用现有调度器，不启动第二个 scheduler；未配置凭据或插件失败不得影响应用启动。
- 凭据使用 `secrets_store` 分字段保存，请求主机和路径使用代码白名单，日志和响应不得包含
  完整 URL 或凭据。
- 多时点写入同一日行时，插件负责按稳定主键进行非空字段原子合并。

`backend/app/plugins/kaipanla/` 是这类插件的现有参考实现，具体表契约和调度见
[开盘啦扩展数据接入](./plans/kaipanla-auction.md)。

### EasyTDX 辅助数据

EasyTDX 按辅助采集插件接入，不创建 `plugin.yaml`，不会出现在主行情数据源列表。
上游参考为 [handsomejustin/easy_tdx](https://github.com/handsomejustin/easy_tdx)
`v1.20.4`（MIT），调用公开接口 `TdxClient.get_security_list_all()`。

安装可选依赖：

```bash
cd backend
uv sync --extra easy-tdx
```

行业快照写入 `ext_industry_tdx`：

| 字段 | 含义 |
| --- | --- |
| `symbol` / `code` | 标准化证券代码 / 6 位代码 |
| `industry_sw` | EasyTDX 从 `tdxhy.cfg` 读取的申万行业代码 |
| `industry_tdx` | EasyTDX 从 `tdxhy.cfg` 读取的通达信行业代码 |
| `source` / `collected_at` | 来源标识 / 采集时间 |

F10 参考数据在每个交易日 `18:40` 采集，写入 `ext_tdx_margin`、
`ext_tdx_forecast`、`ext_tdx_express` 和 `ext_tdx_dividend_history`。分红历史来自
通达信 7615 F10 页，仅保留已实施、登记日和每股现金额明确的方案，按股权登记日分区；
两融使用 F10 正式日度表；预告和快报仅解析正式栏目，互动问答、新闻或未匹配的正文
不会写入。预告与快报按公告日分表，不互相覆盖，也不代替正式财报。

全市场分红历史首次回填使用可恢复脚本：

```bash
cd backend
uv run python scripts/backfill_easy_tdx_dividends.py
```

默认每批 50 只股票、4 个并发请求，进度写入
`data/backfill_state/easy_tdx_dividends.json`。请求失败的代码不会被标记完成，重跑只会重试
这些代码；全部代码完成前脚本以非零状态退出。

数据边界：

- 行业快照只保留两个行业代码。EasyTDX 返回的名称、行情等字段不落库，避免与 TickFlow 重复。
- 不采集竞价、涨停复盘、龙虎榜和监管数据，避免与开盘啦四张扩展表重复。
- `get_security_list_all()` 当前只覆盖沪深 A 股；北交所不在快照内。
- `industry_sw` 保留上游原始代码，不推断行业名称或自行截断层级。
- 历史简称变更仍读取交易所权威快照 `instrument_name_history`，EasyTDX 不作为替代。

运行时行为：

- 复用主 scheduler，每个工作日 `08:30` 更新；首次启动或快照超过 24 小时时后台补采。
- 同步 EasyTDX 调用通过工作线程执行，不阻塞应用事件循环。
- 新快照先写入临时文件，再原子替换 `part.parquet`；空结果不会覆盖上一份有效快照。
- 未安装依赖、连接失败或上游返回空数据都只记录日志，不影响应用启动和主行情数据源。
- 依赖行业去重的“涨停基因小市值”策略在无有效快照或候选股缺少 `industry_sw`
  时明确报不可计算，不会默认改成“每只股一个行业”。
