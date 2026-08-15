# Python 股票策略界面 AI 开发接口文档

本文档供 AI 或 Python 开发者直接编写股票策略、数据分析程序和策略界面。当前 Python SDK 实际行为为准。

Python 版的核心入口是：

```python
from stock_sdk import rd, bk, zb
```

常用在线接口也由 `stock_sdk` 导出：

```python
from stock_sdk import (
    get_price,
    get_bars,
    get_ticks,
    get_last_tick,
    get_fundamentals,
    query,
    cash_flow,
)
```

本地行情优先使用 `rd`。在线接口只用于本地数据库没有的数据，不能用在线接口暴力替代本地批量行情。


推荐方式是：

```python
from stock_sdk import *
```


---

## 1. AI 必须遵守的规则

1. `rd` 是分层 K-V 数据库，不是 SQL，也不是 REST 路由猜测器。先确定 `table + K1 + K2...`，再调用。
2. 日K和分钟K的固定模板是 `rd.get/vals("日k或分钟k", code, date_query)`，不能省略 `code` 或 `date_query`。
3. 禁止用 `rd.get("日k")`、`rd.get("日k", "all")`、`rd.get("日k", code)` 反复试错。
4. 字符串 `"all"` 不是通配符。整层匹配使用 `"*"`；前缀匹配使用 `"6*"`、`"202606*"`。
5. 精确读取单条值用 `rd.get()`；读取匹配结果中的值集合优先用 `rd.vals()`；只要完整键名用 `rd.keys()`。
6. `rd.vals(...).get("date,code,close")` 是服务端字段投影，不是 Python 端二次筛选。
7. 对查询对象先切片、再执行。`query[:100]`、`query[-100:]` 会进入请求参数；不要先全量下载再切片。
8. 大范围查询使用代码前缀，让服务器筛选，例如 `rd.vals("日k", "6*", date_query)`；不得循环五千个代码逐股请求。
9. 需要复权、周/月K或分钟聚合时使用 `rd.get_data()`，不要重新手写复权和周期合成。
10. Python 版 `zb.get()` 既支持股票代码，也支持 `rd.get_data(fields=None)` 的对象行；传入原始行情时直接计算，不得再次请求或二次复权。
11. 在线 `get_price/get_ticks/...` 默认按单标的小数据量使用。当前在线服务会限制批量代码请求。
12. 在线接口可能以 `{"error": "..."}` 返回业务错误，不一定抛异常；必须检查返回值。
13. 同一查询结果应缓存。界面排序、筛选、翻页和重绘不得重复拉取数据。
14. 股票代码始终使用字符串，例如 `'000001'`，不能写成整数 `1`。
15. 所有策略持久化、动态因子、实时缓存、私有数据和自定义数据源必须写入 `rd` 的 `./mydb`，严禁自行创建 SQLite、MySQL、DuckDB 或其他第二套数据库。
16. `./data～./dataN` 是系统历史数据，`./mydb` 是私有写入空间。只能通过 `rd` 接口访问，不得绕过 SDK 直接操作底层文件。

---

## 2. 安装、导入和连接

### 2.1 文件与运行环境

Python 调用所需文件：

- `stockdb.pyd`：Python 3.8+ 非自由线程版本。
- `3.14t+stockdb.pyd`：Python 3.14t+ 自由线程版本，使用时按部署说明改名为 `stockdb.pyd`。
- `stock_sdk.py`：成品 SDK。
- `stockdb.exe`：本地数据库服务。

本地使用时先更新并启动 `stockdb.exe`。默认地址是 `127.0.0.1:7899`。此步骤用户测完成数据更新和启动。

### 2.2 推荐导入

```python
from stock_sdk import *
```

需要修改本地数据库连接时：

```python
from stock_sdk import *

init(
    host="127.0.0.1",
    port=7899,
    socket_timeout=None,
    password=None,
    warm=True,
)
```

`init()` 配置的是本地/StockDB 数据端点。通常使用默认本地地址时无需调用。

### 2.3 在线接口配置与返回模式

`set_init()` 配置的是在线 API，不是 `rd` 的本地数据库连接：

```python
from stock_sdk import set_init

# 在线接口保留 pandas DataFrame 和日期索引
set_init(df=True)

# 在线接口返回普通 list/dict（默认，推荐）
set_init(df=False)
```

当部署方提供独立在线 API 地址时才传 `ip`：

```python
set_init("host:port", df=True)
```

远程体验服务不得进行大批量分钟行情拉取。批量历史行情必须使用本地 `stockdb.exe + rd`。

---

## 私有存储强制规范（最高优先级）

### 存储位置与职责

```text
./data ～ ./dataN  系统历史数据
./mydb             用户私有存储
```

`./mydb` 适用于：

- 个性化数据和用户配置。
- 动态因子、计算结果和策略状态。
- 实时缓存、任务断点和增量状态。
- 私有策略、私有数据、私有数据源。
- 其他小众但需要永久保存的数据。

一次写入后本地永久保存。写入或修改完成后立即可读，无缓存延迟；WAL 负责异常情况下的数据可靠性。

AI 实现任何持久化需求时，默认且唯一的存储方案是 `rd + ./mydb`。严禁自行引入或创建：

```text
sqlite3 / SQLite / SQLAlchemy
MySQL / pymysql / mysqlclient
DuckDB
PostgreSQL
LMDB / LevelDB
shelve 或其他并行本地数据库
```

CSV、JSON、Excel、Parquet 只能在用户明确要求“导入/导出文件”时使用，不能作为策略内部主数据库或缓存数据库。

### 单条写入

`rd.set()` 的最后一个参数是 Value，前面的参数依次组成 Key：

```python
# table -> value
await rd.set(table, value)

# table:key -> value
await rd.set(table, key, value)

# table:key:key2 -> value
await rd.set(table, key, key2, value)
```

Key 层级限于0到2个，多余k会被忽略；表结构由业务自行定义，但写入和读取必须使用完全相同的 Key 顺序。
推荐定义：
table:name
key:code
key2:date/time

同步程序可显式执行：

```python
rd.set(table, key, key2, value).do()
```

### 批量写入必须使用 Pipe

禁止在循环里逐条 `await rd.set(...)`。批量写入统一先组装 Pipe，再一次提交：

```python
pipeline = rd.pipe()

for item in items:
    pipeline.mset(
        "策略因子",
        item["code"],
        item["date"],
        item["value"],
    )

await pipeline
```

同步提交：

```python
pipeline.do()
```

接口支持超大批量写入；如果上层数据生成本身占用大量内存，可按业务批次建立多个 Pipe，但不能退化为逐条网络请求。

### 完整 Value 类型示例

```python
value = {
    "score": 83.25,
    "enabled": True,
    "tags": ["放量", "突破"],
    "raw": b"binary-data",
    "params": {"n": 20, "threshold": 3.0},
    "note": None,
    # "frame": pandas_dataframe,  # DataFrame可直接作为Value
}

await rd.set("私有策略", "strategy_01", "600633", value)
```

不要因为 Value 中包含列表、字典、二进制或 DataFrame，就把数据拆出去写入其他数据库。

### 字段和子节点修改

覆盖完整 Value：

```python
await rd.set(table, key, key2, new_value)
```

只修改嵌套字段：

```python
await (
    rd.get(table, key, key2)
      .get("sub_key")
      .set("sub_sub_key")
      .val("new_value")
)
```

这不是先读取、在 Python 中修改、再整条写回。链式表达式会组成一次服务端更新请求：

```text
ap=get.sub_key/set.sub_sub_key
```

因此修改局部字段时应优先使用链式更新，避免读改写竞争和不必要的数据传输。

### 批量读取和子节点读取

```python
value = await rd.get(table, key, key2)

latest = await (
    rd.get(table_exp, key_exp, key2_exp)
      .get("sub_key")
      .all()
      [-3:]
)
```

`table_exp/key_exp/key2_exp` 均可使用对应层级支持的匹配、范围和前缀表达式。字段遍历、子节点操作和 `[-3:]` 会写入同一个服务端查询，不应改造成 Python 全量读取后再筛选。

根据目标可选择顶层命令：

```text
rd.get / rd.vals / rd.keys / rd.len / rd.delete
rd.set / rd.setr / rd.setl
```

根据 Value 结构可在 QueryResult 上继续使用：

```text
.get(...) / .keys() / .vals() / .len() / .all()
.set(...) / .val(...)
```

不得通过名称猜测操作语义。复杂表达式先调用 `.url()`，确认命令、Key层级、子节点路径和服务端切片都正确后再执行。

### 推荐的私有表结构

```text
策略配置 : strategy_id                         -> dict
策略状态 : strategy_id : trade_date            -> dict
动态因子 : factor_name : code : datetime        -> int/float/dict
实时缓存 : data_source : code : datetime        -> dict/bytes/DataFrame
用户标记 : user_id : code : datetime            -> dict/list
任务断点 : task_name : partition                 -> dict
```

表名和 Key 要表达业务含义，不能把全部数据塞进一个无结构的大字典，也不能为每种策略另建 SQLite 文件。

---

## 3. 先理解 rd：它是分层 K-V

### 3.1 基本模型

逻辑结构：

```text
table : key1 : key2 : ... : keyN  ->  value
```

股票数据常用结构：

```text
日k   : code : YYYYMMDD        -> dict
分钟k : code : YYYYMMDDhhmmss  -> dict
```

示例：

```text
日k:600633:20260625
分钟k:600422:20260625145200
```

其他表不一定有两个键层级。例如 `股票代码` 本身直接对应一个字典：

```python
codes_by_prefix = rd.get("股票代码")
codes_6 = codes_by_prefix["6"]
```

所以调用参数数量由数据的 K-V 结构决定，不能看到表名后随意省略键。

### 3.2 推荐写法

```python
row = rd.get("日k", "600633", "20260625")
minute = rd.get("分钟k", "600422", "20260625145200")
```

完整键拼接也可用：

```python
row = rd.get("日k:600633:20260625")
```

策略代码统一推荐分离参数写法，更容易看出 `table/K1/K2` 是否完整。

### 3.3 Value 支持类型

Value 支持常用 Python 标量和容器类型，包括 `list/dict/int/float/str/bytes/DataFrame`、布尔值、`None` 及嵌套组合。存储自定义策略结果时优先保留其自然 Python 类型；类实例、函数、文件句柄等运行时对象应先转换为字典、列表或基础类型。

```python
value = {
    "score": 81.5,
    "enabled": True,
    "tags": ["突破", "放量"],
    "params": {"n": 20, "threshold": 3.0},
    "note": None,
}

rd.set("我的数据", "600633", "20270101", value)
loaded = rd.get("我的数据", "600633", "20270101")
```

最后一个参数是 Value；前面的参数依次构成 Key。

---

## 4. rd.get / rd.vals / rd.keys 的区别

### 4.1 精确键

精确 `rd.get()` 返回该键对应的原始 Value：

```python
row = rd.get("日k", "600633", "20260625")
# row 等价于一个 dict
# {'date': 20260625, 'code': '600633', 'open': ..., 'close': ...}
```

精确 `rd.vals()` 仍然按“值集合”返回，因此外层是列表：

```python
rows = rd.vals("日k", "600633", "20260625")
# [ {...} ]
```

### 4.2 匹配多个键

当通配符或范围匹配多条记录时：

```python
pairs = rd.get("日k", "600633", "2026062*")
# [["日k:600633:20260622", {...}], ...]

rows = rd.vals("日k", "600633", "2026062*")
# [{...}, {...}, ...]

keys = rd.keys("日k", "600633", "2026062*")
# ["日k:600633:20260622", ...]
```

策略通常只需要行情字典，因此集合查询优先使用 `rd.vals()`。

| 目标 | 正确接口 |
|---|---|
| 精确取得一个 Value | `rd.get(table, *keys)` |
| 匹配并只返回 Value | `rd.vals(table, *key_queries)` |
| 匹配并只返回完整键 | `rd.keys(table, *key_queries)` |
| 同时保留完整键和值 | `rd.get(table, *key_queries)` |

### 4.3 `all` 不是查询语法

以下调用错误：

```python
rd.get("日k", "all")       # "all" 只是一个普通字符串键
rd.get("日k", "600633")   # 日k还缺少日期这一层
```

整层匹配使用 `"*"`：

```python
rd.vals("日k", "600633", "*")
```

`QueryResult.all()` 只是查询结果的后处理操作，通常无需使用；它与键查询中的通配符无关。字典字段应优先使用 `.get(fields)`、`.keys()` 或 `.vals()` 明确投影。

---

## 5. Key 查询语法

### 5.1 精确匹配

```python
rd.get("日k", "600633", "20260625")
rd.get("分钟k", "600422", "20260625145200")
```

### 5.2 整层与前缀匹配

```python
rd.vals("日k", "600633", "*")             # 该股票全部日K
rd.vals("日k", "600633", "202606*")       # 2026年6月
rd.vals("日k", "6*", "20260625")          # 6开头证券的某日数据
rd.vals("分钟k", "60042*", "20260625145200")
rd.vals("分钟k", "600422", "20260625093*")
rd.vals("退市*")                            # 表名前缀匹配
```

这里的 `*` 是整层或后缀前缀匹配，不要自行假设复杂正则或任意位置 glob 语法。

### 5.3 范围与顺序

`<` 表示区间正序，`>` 表示同一区间反序。符号是查询字符串的一部分，不是 Python 比较运算：

```python
asc = rd.vals("日k", "600633", "20260620<20260626")
desc = rd.vals("日k", "600633", "20260620>20260626")
```

开放区间使用 `N`：

```python
after = rd.vals("日k", "600633", "20260620<N")
before_desc = rd.vals("日k", "600633", "N>20260626")
```

推荐先生成查询并检查 URL：

```python
q = rd.vals("日k", "600633", "20260620<20260626")
print(q.url())
# /?cmd=vals&t=日k&k1=key:600633&k2=fwd:20260620,20260626
```

---

## 6. QueryResult：先组装，后执行

`rd.get/vals/keys()` 返回的对象是 `QueryResult`。它同时具备：

- 查询计划：可以继续字段投影、切片并查看 `.url()`。
- 同步值代理：打印、遍历、索引时可自动执行。
- 显式同步执行：调用 `.do()`。
- 异步执行：直接 `await query`。

### 6.1 推荐的清晰写法

```python
q = rd.vals("日k", "600633", "2026062*")
print(q.url())
rows = q.do()
```

### 6.2 字段投影在服务器完成

```python
q = rd.vals("日k", "600633", "2026062*")

dates = q.get("date").do()
# [20260622, 20260623, ...]

matrix = q.get("code,date,amount,high").do()
# [['600633', 20260622, 258471424, 11.08], ...]
```

必须理解：

```python
rd.vals("日k", "600633", query).get("date")
```

等于在同一个服务端请求中追加字段投影 `ap=get.date`，不是下载全部字典后在 Python 中再次筛选。

如果已经执行得到 Python 字典，才使用普通字典的 `.get()`：

```python
rows = rd.vals("日k", "600633", "2026062*").do()
dates = [row.get("date") for row in rows]
```

### 6.3 切片在服务器完成

```python
q = rd.vals("日k", "600633", "20260620<20260629")

first_three = q[:3]
last_three = q[-3:]

print(first_three.url())  # ...&num=3
print(last_three.url())   # ...&num=-3
```

可以继续组合投影：

```python
dates = rd.vals("日k", "600633", "20260620<N")[-20:].get("date").do()
```

### 6.4 同步与异步使用同一套表达式

同步：

```python
rows = rd.vals("日k", "600633", "2026062*")[:10].do()
```

异步：

```python
import asyncio

async def main():
    rows = await rd.vals("日k", "600633", "2026062*")[:10]
    print(rows)

asyncio.run(main())
```

不要为了异步重新发明另一套 `rd` 查询语法。

---

## 7. 全市场/大范围原生 rd 查询

### 7.1 服务器按前缀筛选

错误方式：

```python
# 禁止：数千次独立请求
for code in all_codes:
    row = rd.get("日k", code, "20260625")
```

正确方式：

```python
prefixes = ["0*", "3*", "6*", "920*"]

rows = []
for prefix in prefixes:
    part = rd.vals("日k", prefix, "20260625").do()
    rows.extend(part)
```

这只有少量前缀请求。`1*`、`5*` 等范围包含基金、债券等非股票证券，是否加入必须由策略股票池定义决定。

日期范围同样由服务器直接处理：

```python
rows_6 = rd.vals(
    "日k",
    "6*",
    "20260507<20260508",
).do()
```

### 7.2 只取需要的字段

```python
matrix = rd.vals(
    "日k",
    "6*",
    "20260507<20260508",
).get("date,code,volume,close").do()
```

列顺序严格等于字段字符串顺序：

```text
[date, code, volume, close]
```

字段投影能显著减少网络传输和 Python 对象数量。

### 7.3 股票代码全集

```python
groups = rd.get("股票代码")

# 按产品定义选择范围，不要盲目拼接所有前缀
codes = groups.get("0", []) + groups.get("3", []) + groups.get("6", [])
```

北交所和其他9开头证券是否加入，应检查当前数据库的 `groups.get("9", [])` 并按业务规则筛选。

---

## 8. rd.pipe：批量读取和批量写入

当查询目标是许多互不连续的精确键，无法用一个前缀或范围表达式覆盖时，使用 Pipe：

```python
pipe = rd.pipe()

for code in ["600633", "600422", "000001"]:
    pipe.mget("分钟k", code, "20260625145200")

rows = pipe.do()       # 同步
```

异步：

```python
async def main():
    pipe = rd.pipe()
    for code in ["600633", "600422", "000001"]:
        pipe.mget("分钟k", code, "20260625145200")
    rows = await pipe
    return rows
```

批量写入私有存储使用 `mset`，不能循环逐条执行 `rd.set`：

```python
async def save_factors(items):
    pipe = rd.pipe()
    for item in items:
        pipe.mset(
            "动态因子",
            item["factor"],
            item["code"],
            item["date"],
            item["value"],
        )
    await pipe
```

同一个 Pipe 可以承载大量写入任务；提交成功后数据立即可以通过相同的 `table + keys` 读取。

优先级：

1. 能用一个前缀/范围查询：直接 `rd.vals()`。
2. 多个离散精确键：`rd.pipe()`。
3. 需要复权和周期加工：`rd.get_data()`。
4. 多条私有数据写入：`rd.pipe()` + `mset()`。

---

## 9. rd.get_data：加工后的行情接口

### 9.1 完整签名

```python
data = rd.get_data(
    code,                  # 必须：'600633' 或 ['600633', '600422']
    start=None,            # 可选：8位或14位字符串
    end=None,              # 可选：8位、14位或 'N'
    frequency="1d",       # 1d/1m/5m/15m/30m/60m/1w/1M
    fields=None,           # None、逗号字符串或字段列表
    limit=None,            # 每个查询结果最大记录数
    desc=False,            # False升序，True降序
    as_df=False,           # False返回list/dict，True返回DataFrame
    fq="qfq",             # qfq/hfq/None
)
```

异步接口参数相同：

```python
data = await rd.get_data_async(...)
```

### 9.2 日期语义

- `start=None, end=None`：查询全量。
- 日K只传8位 `start`：查询该精确交易日。
- 分钟K只传14位 `start`：查询该精确分钟键。
- 分钟K查询整天：同时传入相同的8位 `start/end`。
- 查询从某日到最新：`start='20260625', end='N'`。
- 不要只传 `end`；当前接口不会把它自动解释为“从最早到 end”。

```python
day = rd.get_data(
    "600633",
    start="20260625",
    end="20260625",
    frequency="1d",
)

minutes = rd.get_data(
    "600633",
    start="20260625",
    end="20260625",
    frequency="1m",
)
```

### 9.3 返回结构

单代码、不传 `fields`：

```python
rows = rd.get_data("600633", start="20260625", end="20260626", fq=None)
# list[dict]
```

多代码、不传 `fields`：

```python
rows_by_code = rd.get_data(
    ["600633", "600422"],
    start="20260625",
    end="20260626",
    fq=None,
)
# {'600633': [dict, ...], '600422': [dict, ...]}
```

传 `fields` 后返回位置数组；即使只有一个字段，也保留二维结构：

```python
dates = rd.get_data(
    "600633",
    start="20260625",
    end="20260626",
    fields="date",
    fq=None,
)
# [[20260625], [20260626]]

matrix = rd.get_data(
    "600633",
    start="20260625",
    end="20260626",
    fields="date,code,close",
    fq=None,
)
# [[20260625, '600633', 10.45], ...]
```

这与浏览器版单字段返回标量数组的行为不同，AI 不得混用两份文档的返回结构。

多代码加 `fields`：

```python
# {'600633': [[...], ...], '600422': [[...], ...]}
```

### 9.4 DataFrame

```python
df = rd.get_data(
    ["600633", "600422"],
    start="20260625",
    end="20260626",
    fields="date,code,close",
    as_df=True,
    fq=None,
)
```

多代码 `as_df=True` 返回合并后的 `pandas.DataFrame`，不是 `{code: DataFrame}`。

### 9.5 周期、排序和限制

```python
latest_20 = rd.get_data(
    "600633",
    frequency="1d",
    fields="date,close",
    limit=20,
    desc=True,
)

bars_5m = rd.get_data(
    "600633",
    start="20260625",
    end="20260625",
    frequency="5m",
)
```

`5m/15m/30m/60m/1w/1M` 和复权由 SDK 处理。不要再对结果手动聚合或二次复权。

### 9.6 大批量使用

`rd.get_data()` 支持代码列表。单次传代码列表或按内存能力分成少量大块；禁止在循环中对每只股票分别调用：

```python
rows_by_code = rd.get_data(
    codes,
    start="20260701",
    end="20260807",
    frequency="1d",
    fq="qfq",
)
```

如果只需要原始、不复权、固定日期范围的数据，前缀 `rd.vals()` 通常更直接、更省请求。

---

## 10. bk.get：板块与股票双向映射

### 10.1 签名

```python
result = bk.get(
    x=None,          # 股票代码、板块代码、板块名称、代码列表或None
    category=None,   # 0/1/2/3 或中文类别
    fields=None,     # 单字段或逗号字段串
)
```

类别：

| 值 | 类别 |
|---|---|
| `0` | 概念 |
| `1` | 申万一级 |
| `2` | 申万二级 |
| `3` | 申万三级 |

板块索引首次使用时加载并缓存，后续查询不应重新请求。

### 10.2 股票查板块

```python
boards = bk.get("600633", 1)
# [{'code': '801760.SL', 'name': '传媒', ...}]

names = bk.get("600633", 0, "name")
# ['AIGC概念', 'AI应用', ...]

values = bk.get("600633", 1, "group,name")
# [['申万行业指数列表', '传媒']]
```

股票可能属于多个板块，因此股票查询始终按列表处理。

批量股票：

```python
mapping = bk.get(["600633", "000007"], 1, "name")
# {'600633': ['传媒'], '000007': ['商业贸易']}
```

### 10.3 板块查股票

```python
symbols = bk.get("5G", 0, "symbols")
code = bk.get("5G", 0, "code")

board = bk.get("801170.SL")
# 精确板块代码返回包含 symbols 的dict
```

### 10.4 名称模糊匹配与分类全集

```python
names = bk.get("电池", 0, "name")
# ['BC电池', 'HJT电池', ...]

boards = bk.get(category=1, fields="name,code")
# [['交通运输', '801170.SL'], ['休闲服务', '801210.SL'], ...]
```

注意：多字段始终按字段顺序返回位置数组。不要把 `fields="name,code"` 的结果误写成纯名称列表。

允许字段：

```text
code,name,source,type,group,category,symbols
```

---

## 11. zb.get：批量技术指标

### 11.1 Python 版签名

```python
result = zb.get(
    name,
    codes=None,
    original=None,
    start=None,
    end=None,
    frequency="day",
    method=1,
    base=1000.0,
    fq="qfq",
    fields=None,
    n=None,
    cross=False,
)
```

代码取数模式：

- 单代码字符串：`"600633"`
- 多代码列表：`["600633", "000007"]`

```python
macd = zb.get(
    "macd",
    ["600633", "000007"],
    start="20260601",
    end="20260630",
)
```

内存数据模式：

```python
kdata = rd.get_data(
    ["600633", "000007"],
    start="20260601",
    end="20260630",
    fields=None,
    fq="qfq",
)

macd = zb.get("macd", original=kdata)

# 也支持自动识别第二个位置参数
macd = zb.get("macd", kdata)
```

`orgainal` 作为历史拼写兼容别名也可使用，但新代码统一写正确的 `original`。

支持的原始数据形态：

- 单股 `list[dict]`，每行通常包含 `date/code/open/high/low/close/volume`。
- 多股 `{code: list[dict]}`。
- 包含命名列的 pandas DataFrame。
- 返回对象字典行的 `rd.vals(...)` QueryResult。

不接受 `rd.get_data(..., fields="date,open,...")` 返回的位置矩阵，因为矩阵本身不携带列名。需要传指标时应保留 `fields=None`：

```python
# 正确
kdata = rd.get_data(codes, start=start, end=end, fields=None, fq="qfq")
result = zb.get("macd", original=kdata)
```

传入 `original` 后，`start/end/frequency/fq` 不会再次应用；原始数据采用什么周期和复权方式，指标就基于什么数据计算。`n/fields/cross/method/base` 仍是指标计算参数。

### 11.2 代码取数模式必须明确日期

只有传代码并让 `zb.get()` 自行取数时才使用日期参数。当前默认 `start` 是固定值 `20260302`；如果不传 `end`，只会读取默认开始日这一精确日，不代表全量历史。传 `original` 时不需要日期参数。

正式策略必须明确传入 `start/end`：

```python
macd = zb.get(
    "macd",
    "600633",
    start="20260601",
    end="20260630",
    frequency="1d",
    fq="qfq",
)
```

需要从开始日算到最新：

```python
macd = zb.get("macd", "600633", start="20260601", end="N")
```

### 11.3 返回结构

单代码返回列表：

```python
[
    {'date': 20260625, 'dif': ..., 'dea': ..., 'macd': ...},
    ...
]
```

多代码返回字典：

```python
{
    '600633': [dict, ...],
    '000007': [dict, ...],
}
```

内存数据模式遵循数据外形：单股 `list[dict]` 返回指标列表；`{code: rows}` 映射返回 `{code: 指标列表}`，即使映射中只有一个代码。若需要单股列表，可传单股行列表，或同时显式传入代码字符串：

```python
result = zb.get("macd", "600633", original=rows_by_code)
```

每只股票内部按 `date` 升序独立计算，不要求不同股票日期完全对齐。

### 11.4 多指标与参数对应

```python
studies = zb.get(
    "ma,kdj,macd",
    ["600633", "000007"],
    start="20260601",
    end="20260630",
    frequency="1d",
    n=["5,10,20", None, "12,26,9"],
)
```

也支持名称列表：

```python
zb.get(["ma", "kdj", "macd"], "600633", ...)
```

多指标时，`n` 必须与指标名称一一对应。不能把 `n="5,10,20"` 直接用于三个不同指标。

### 11.5 cross

```python
signals = zb.get(
    "ma,kdj,macd",
    ["600633", "000007"],
    start="20260601",
    end="20260630",
    n=["5,10,20", None, "12,26,9"],
    cross=True,
)

with_values = zb.get(
    "ma,kdj,macd",
    ["600633", "000007"],
    start="20260601",
    end="20260630",
    n=["5,10,20", None, "12,26,9"],
    cross="with_value",
)
```

- `cross=False`：返回指标值。
- `cross=True`：只返回交叉信号。
- `cross="with_value"`：指标值和交叉信号一起返回。
- 单项信号：`1` 金叉、`-1` 死叉、`0` 无交叉。
- 多指标会返回 `macd_cross/kdj_cross/...` 等整数信号。
- 多指标综合 `cross` 是布尔值：所有单项同一根K线都为金叉时为 `True`，否则为 `False`；它不是综合死叉字段。

### 11.6 基础指标 fields

只有以下基础指标支持 `fields`：

```text
ma,ema,sma,wma,dma,std,sum,hhv,llv,ref
```

可选输入字段：

```text
close,high,low,open,volume,amount,float_mv,total_mv
```

```python
volume_ma = zb.get(
    "ma",
    "600633",
    start="20260601",
    end="20260630",
    n="5,10",
    fields="volume",
)
```

### 11.7 支持的指标

```text
ma, ema, sma, wma, dma, std, sum, hhv, llv, ref,
macd, kdj, rsi, wr, bias, boll, psy, cci, atr, bbi,
dmi, taq, ktn, trix, vr, cr, emv, dpo, brar, dfma,
mtm, mass, roc, expma, obv, mfi, asi, xsii, zhishu
```

常用参数：

| 指标 | n 示例 |
|---|---|
| `ma` | `5` 或 `'5,10,20'` |
| `macd` | `None`，默认12/26/9 |
| `kdj` | `None`，默认9/3/3 |
| `rsi` | `24` |
| `wr` | `'10,6'` |
| `boll` | `'20,2'` |
| `bias` | `'6,12,24'` |
| `xsii` | `'102,7'` |

### 11.8 指数 zhishu

```python
index_rows = zb.get(
    "zhishu",
    ["600633", "000007"],
    start="20260601",
    end="N",
    frequency="1d",
    method=1,
    base=1000,
)
```

| method | 权重 |
|---|---|
| `1` | 等权 |
| `2` | 流通市值 |
| `3` | 成交额 |
| `4` | 成交量 |
| `5` | 总市值 |

当前实现只有 `frequency='1d'` 支持 `method=2/5`；其他周期使用 `1/3/4`。指数需要前一根有效收盘价，因此结果会去掉无法形成收益的第一期，必须按 `date` 对齐。

### 11.9 直接数组函数

```python
close = [10.0, 10.2, 10.1, 10.5]
high = [10.1, 10.3, 10.2, 10.6]
low = [9.9, 10.0, 10.0, 10.2]

dif, dea, macd = zb.MACD(close, 12, 26, 9)
k, d, j = zb.KDJ(close, high, low, 9, 3, 3)
cross = zb.CROSS(dif, dea)
```

低层函数是同步计算，不会读取数据库。初始周期可能出现 `nan`，统计前使用 `math.isfinite()` 过滤。

---

## 12. 在线 API

### 12.1 使用边界

在线 API 用于：

- 最新 Tick 或在线行情补充。
- 财务报表和基本面查询。
- 指数成分、行业、概念、交易日等参考数据。

在线 API 不用于：

- 全市场逐股历史K线。
- 高频轮询五千只股票。
- 替代本地 `rd` 的大批量分钟数据。

当前在线服务对批量代码有限制。实测 `get_price([code1, code2], ...)` 会返回批量限制错误，因此在线行情按单代码、小 `count` 使用。

统一错误检查：

```python
def require_online_result(value):
    if isinstance(value, dict) and value.get("error"):
        raise RuntimeError(value["error"])
    return value
```

### 12.2 get_price

经过验证的单代码模板：

```python
from stock_sdk import set_init, get_price

set_init(df=True)

df = require_online_result(get_price(
    "000001",
    end_date="2026-08-06",
    count=20,
    frequency="daily",
    fields=["open", "high", "low", "close", "volume", "money"],
))
```

`df=True` 时返回 `pandas.DataFrame`，日期保存在索引中。

`df=False/None` 时返回 `list[dict]`，`get_price` 的日期索引不会自动成为字典字段：

```python
set_init(df=False)
rows = get_price(
    "000001",
    end_date="2026-08-06",
    count=2,
    frequency="daily",
    fields=["open", "close"],
)
# [{'open': 11.41, 'close': 11.25}, ...]
```

如果普通列表结果必须包含日期，优先使用 `get_bars(..., fields=['date', ...])`，或使用 `df=True` 后重置索引。

### 12.3 get_bars

```python
from stock_sdk import get_bars

rows = require_online_result(get_bars(
    "000001",
    count=20,
    unit="1d",
    fields=["date", "open", "high", "low", "close", "volume"],
    end_dt="2026-08-06",
))
```

普通返回示例：

```python
[
    {'date': '2026-08-05', 'open': 11.41, 'close': 11.25},
    ...
]
```

### 12.4 get_ticks / get_last_tick

历史 Tick 小样本：

```python
from stock_sdk import get_ticks

ticks = require_online_result(get_ticks(
    "000001",
    end_dt="2026-08-06 15:00:00",
    count=20,
    fields=["time", "current", "volume"],
))
```

最新 Tick：

```python
from stock_sdk import get_last_tick

ticks = require_online_result(get_last_tick("000001", count=10))
```

常见返回字段：

```text
time,current,high,low,volume,money
```

`time` 在普通列表模式下可能是 `20260807152700.0` 形式的数字，使用前按14位日期时间规范化。

### 12.5 交易日和证券信息

```python
from stock_sdk import (
    set_init,
    get_trade_days,
    get_all_securities,
    get_security_info,
)

days = require_online_result(get_trade_days(
    end_date="2026-08-06",
    count=20,
))

info = require_online_result(get_security_info("000001"))

set_init(df=True)
stocks = require_online_result(get_all_securities(
    types=["stock"],
    date="2026-08-06",
))
```

`get_all_securities(..., df=True)` 的股票代码位于 DataFrame 索引；普通列表模式会丢失该索引，不适合直接建立代码映射。

### 12.6 财务数据

```python
from stock_sdk import get_fundamentals, query, cash_flow

q = query(cash_flow).filter(cash_flow.code == "000001.XSHE")
data = require_online_result(get_fundamentals(q, statDate="2024q4"))
```

财务查询中的代码常用带交易所后缀形式：

```text
000001.XSHE
600633.XSHG
```

`date` 与 `statDate` 含义不同：`date` 表示查询日期，`statDate` 表示财报统计期；按策略需求选择，不要同时随意猜测。

### 12.7 其他已导出的在线接口

证券与分类：

```text
get_all_trade_days, get_all_securities, get_security_info,
get_industry, get_concepts, get_industries,
get_concept_stocks, get_industry_stocks,
get_index_stocks, get_index_weights
```

行情与交易数据：

```text
get_price, get_bars, get_call_auction, get_ticks, get_last_tick,
get_extras, get_money_flow, get_mtss,
get_margincash_stocks, get_marginsec_stocks
```

财务、因子与衍生品：

```text
get_fundamentals, get_fundamentals_continuously,
get_fund_info, get_valuation,
get_billboard_list, get_locked_shares,
get_factor_values, get_factor_kanban_values,
get_index_style_exposure,
get_dominant_future, get_future_contracts
```

这些接口由在线 `RemoteProxy` 提供，Python 无法可靠地通过 `inspect.signature()` 取得完整签名。AI 不得对未使用的接口进行几十次参数试探；只有策略明确需要时，按已有示例或部署方接口说明构造一次小查询。

---

## 13. 完整纯 Python 策略流水线

### 13.1 少量股票：get_data + zb + bk

```python
from stock_sdk import rd, bk, zb

CODES = ["600633", "600422", "000001"]
START = "20260701"
END = "20260807"

# 1. 一次批量加载行情
rows_by_code = rd.get_data(
    CODES,
    start=START,
    end=END,
    frequency="1d",
    fq="qfq",
)

# 2. 直接复用已加载行情，不再访问数据库
macd_by_code = zb.get(
    "macd",
    original=rows_by_code,
    cross="with_value",
)

# 3. 板块批量映射
boards_by_code = bk.get(CODES, 1, "name")
```

`zb.get(original=rows_by_code)` 直接复用内存行情，不会再次调用 `get_data()`。页面层缓存一次 `rows_by_code` 后，可以基于它计算多个指标。

### 13.2 大范围原始日K：前缀服务端查询

```python
from collections import defaultdict
from stock_sdk import rd

PREFIXES = ["0*", "3*", "6*", "9*"]
DATE_QUERY = "20260701<20260807"

rows = []
for prefix in PREFIXES:
    rows.extend(
        rd.vals("日k", prefix, DATE_QUERY).do()
    )

rows_by_code = defaultdict(list)
for row in rows:
    if not isinstance(row, dict):
        continue
    code = str(row.get("code", ""))
    if len(code) != 6:
        continue
    rows_by_code[code].append(row)

for values in rows_by_code.values():
    values.sort(key=lambda row: int(row["date"]))
```

如果只需要少数字段，把 `.get("date,code,close,volume")` 放在 `.do()` 之前，并按位置数组解析。

### 13.3 有效交易记录

```python
from math import isfinite

def valid_trading_rows(rows):
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            close = float(row["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if not isfinite(close) or close <= 0:
            continue

        if "pct_chg" in row:
            pct = row.get("pct_chg")
            if pct in ("-", None, ""):
                continue
            try:
                if not isfinite(float(pct)):
                    continue
            except (TypeError, ValueError):
                continue

        if "volume" in row:
            try:
                if float(row["volume"]) <= 0:
                    continue
            except (TypeError, ValueError):
                continue
        result.append(row)

    return sorted(result, key=lambda row: int(row["date"]))
```

停牌或无效行情不能作为零涨幅参与 N 日收益、连续涨跌和平均值。

### 13.4 缓存

```python
from functools import lru_cache

@lru_cache(maxsize=32)
def load_one(code, start, end, frequency="1d", fq="qfq"):
    return rd.get_data(
        code,
        start=start,
        end=end,
        frequency=frequency,
        fq=fq,
    )
```

代码列表不能直接作为 `lru_cache` 键，需转换为元组，或使用自定义字典缓存。缓存键至少包含：

```text
codes/start/end/frequency/fields/limit/desc/fq
```

在线接口缓存键还应包含接口名称和所有在线参数。

`lru_cache` 只适合当前进程的热点数据。需要跨重启保留的行情缓存、在线结果、计算因子或界面状态必须批量写入 `rd + ./mydb`，不能改用 SQLite、DuckDB 或磁盘散文件。

---

## 14. 策略界面性能规则

无论使用 PySide/PyQt、Tkinter、Streamlit、NiceGUI、Flask 或其他 Python 界面，数据规则不变：

1. 数据读取和指标计算与界面渲染分离。
2. 窗口重绘、排序、筛选和翻页只操作内存结果。
3. 大范围原始行情优先使用 `rd` 前缀查询。
4. 离散精确键使用 `rd.pipe()`。
5. 复权和周期数据使用 `rd.get_data()` 代码列表接口。
6. 在线 API 只做单标的、小数据补充并缓存。
7. 后台线程或异步任务完成后再更新 UI，不能在鼠标移动事件中同步请求网络。
8. 平均值、金叉数量等汇总对完整筛选结果计算，不能只计算当前可见控件。
9. 所有需要跨进程、跨重启保留的数据都写入 `rd + ./mydb`。
10. 多条私有数据使用 `rd.pipe().mset(...)` 批量提交，不能逐条网络写入。
11. 不得为缓存、配置、因子或策略结果引入 SQLite、MySQL、DuckDB 等第二套数据库。

---

## 15. 常见错误清单

### 严重错误：自行创建其他数据库

```python
import sqlite3
import duckdb
from sqlalchemy import create_engine
```

以上方式在本系统中禁止用于策略存储、缓存、因子和私有数据。统一改为 `rd.set()`、`rd.pipe().mset()` 和 `rd.get/vals()`，数据保存到 `./mydb`。

### 严重错误：批量数据逐条写入

```python
for item in items:
    await rd.set("动态因子", item["code"], item["date"], item)
```

正确方式：

```python
pipe = rd.pipe()
for item in items:
    pipe.mset("动态因子", item["code"], item["date"], item)
await pipe
```

### 错误：把 rd 当作只有 table 的数据库

```python
rd.get("日k")
```

日K必须提供 `code` 和 `date_query`。

### 错误：把 all 当通配符

```python
rd.get("日k", "all")
```

`"all"` 是普通键。使用 `"*"`，并补齐全部键层级。

### 错误：少一个日期参数

```python
rd.get("日k", "600633")
```

查询全部日期应写：

```python
rd.vals("日k", "600633", "*")
```

### 错误：全市场逐股请求

```python
for code in codes:
    rd.get("日k", code, date)
```

同一日期按代码前缀查询，或使用 Pipe/批量 `get_data`。

### 错误：误解 QueryResult.get

```python
rd.vals("日k", "600633", query).get("date")
```

这是正确的服务端字段投影，不是“对结果手动二次筛选”。不要把它改写成先全量下载再循环。

### 错误：把 Python get_data 单字段当标量数组

```python
dates = rd.get_data(..., fields="date")
# dates 是 [[date], ...]，不是 [date, ...]
```

### 错误：给 zb 传无列名的位置矩阵

```python
kdata = rd.get_data(..., fields="date,open,high,low,close,volume")
zb.get("macd", original=kdata)  # 错误：无法知道每一列对应的字段
```

正确：

```python
kdata = rd.get_data(codes, start=start, end=end, fields=None)
zb.get("macd", original=kdata)

# 或让zb按代码自行取数
zb.get("macd", codes, start=start, end=end)
```

### 错误：省略 zb 日期

```python
zb.get("macd", "600633")
```

这只会使用固定默认开始日，不是全量。正式策略明确传 `start/end`。

### 错误：在线 get_price 批量代码

```python
get_price(["000001", "600633"], ...)
```

当前在线服务限制批量行情。历史批量使用本地 `rd`；在线按单代码、小 `count` 调用。

### 错误：不检查在线错误字典

```python
rows = get_price(...)
```

必须检查：

```python
rows = require_online_result(get_price(...))
```

---

## 16. 给 AI 的 Python 策略需求模板

```text
使用 Python stock_sdk 开发策略或策略界面。

运行方式：本地 stockdb.exe / 部署方远程体验
证券范围：指定代码 / A股 / 自定义前缀 / 某个板块
日期范围：YYYYMMDD 到 YYYYMMDD，或结束使用 N
周期：1d / 1m / 5m / 15m / 30m / 60m / 1w / 1M
复权：qfq / hfq / None
指标：例如 MACD、KDJ、MA、cross
在线补充：是否需要 get_price、tick、财务、指数成分
私有存储：需要保存的配置、因子、缓存、策略状态及table/key结构
输出：普通Python结构 / pandas DataFrame
界面：PySide6 / Tkinter / Streamlit / 其他指定框架

硬性要求：
1. 严格按 table + key1 + key2 的KV模板调用rd。
2. 不得使用 rd.get("日k")、rd.get("日k", "all") 等试错调用。
3. 大范围原始行情使用代码前缀服务端查询，不逐股请求。
4. 复权和周期加工使用 rd.get_data，不重复实现。
5. Python版 zb.get 可传代码，也可直接传 fields=None 的对象行情；已有行情时必须复用，不能重复拉取。
6. 在线行情只允许单代码、小数据量，并检查 error 字典。
7. 查询、指标和在线结果全部缓存，界面重绘不得重新请求。
8. 所有持久化数据只能使用rd的./mydb；严禁SQLite、MySQL、DuckDB等其他数据库。
9. 批量写入必须使用rd.pipe()+mset，批量读取必须使用表达式、字段投影和服务端切片。
10. 直接给出可运行的纯Python代码。
```

当需求存在歧义时，优先明确：

- “全市场”是否包含基金、债券、北交所和其他9开头证券？
- 需要原始行情还是前/后复权行情？
- N日收益是N根K线还是N个完整收益区间？
- 在线数据是否确实无法从本地 `rd` 获得？
- 普通列表还是 DataFrame，是否必须保留日期索引？
- 界面使用哪个 Python 框架？

用户已经明确时，不要重复询问，不要通过大量无效调用探索接口。
