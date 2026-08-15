# 股票策略界面 AI 开发接口文档

本文档供 AI 或前端开发者直接生成股票策略网页。运行环境是浏览器，底层数据服务默认为 `127.0.0.1:7899`，不需要启动 Python 网页服务器，不依赖 ECharts。

SDK 对外只使用四个公共全局对象：

| 文件 | 全局对象 | 用途 |
|---|---|---|
| `gp.js` | `gp` | 日K、分钟K、周K、月K和复权数据 |
| `bk.js` | `bk` | 板块与股票的双向映射 |
| `zb.js` | `zb` | 技术指标、交叉信号和市场指数 |
| `tu.js` | `tu` | Canvas K线、分时、成交量、指标和标记绘图 |

每个 JS 都内置所需的授权与基础代码，不需要额外引入 `license_core.js` 等公共文件。策略页面通常同时引入四个文件。`zb_core`、`__pxLicense` 等属于兼容或内部对象，策略代码不得依赖。

---

## 1. AI 必须遵守的规则

1. 设置 `window.baseurl` 必须发生在加载 SDK 之前。未设置时使用 `127.0.0.1:7899`。
2. `gp.get()`、`bk.get()`、`zb.get()` 都是异步接口，必须 `await`。
3. `tu.get()` 在授权完成后是同步绘图接口，指标 Promise 必须先 `await`。标准策略流水线应先完成 `gp.get()`/`zb.get()`，再调用 `tu.get()`。
4. 大范围市场不得逐股请求五千多次，必须使用代码前缀分片；但“全部证券”和“A股股票”的前缀范围不同，不能混为一谈。
5. 相同查询只拉取一次并缓存。筛选、排序、翻页、板块切换不得重复请求；日期、周期或复权方式改变时应使用新的缓存键。
6. 下游还要计算指标或绘图时，`gp.get()` 不要传 `fields`，保持对象行结构。
7. 停牌或无有效价格的记录必须排除，不能把 `-`、`null`、`NaN` 当作零。
8. 指标与K线按 `date` 对齐，不能假定不同股票或不同数据源的数组索引天然一致。
9. “全部”模式按当前选定的完整证券范围排序；点击具体板块后，才在该板块股票集合内排序。
10. 平均值、金叉数量等汇总必须对完整结果计算，不能只计算当前可见窗口或当前页。
11. 图表调用固定为 `tu.get(data, target, options)`，前两个参数是必传位置参数。
12. JavaScript 不支持 Python 的 `title="xxx"` 命名参数写法，所有可选项都放入对象。

---

## 2. 最小可运行页面

```html
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>策略页面</title>
  <style>
    html, body { margin: 0; width: 100%; height: 100%; background: #fff; }
    #chart { width: 100%; min-height: 360px; }
  </style>
</head>
<body>
  <div id="chart"></div>

  <script>window.baseurl = '127.0.0.1:7899';</script>
  <script src="gp.js"></script>
  <script src="bk.js"></script>
  <script src="zb.js"></script>
  <script src="tu.js"></script>
  <script>
    async function main() {
      const kdata = await gp.get({
        code: '600633',
        start: '20260101',
        end: '20260807',
        frequency: '1d',
        fq: 'qfq'
      });

      const zhibiao = await zb.get('macd', kdata);

      tu.get(kdata, '#chart', {
        title: '600633 日K · MACD',
        zhibiao,
        biaoji: []
      });
    }

    main().catch(error => {
      document.body.textContent = `初始化失败：${error.message}`;
      console.error(error);
    });
  </script>
</body>
</html>
```

对象简写 `{ zhibiao }` 等同于 `{ zhibiao: zhibiao }`。

---

## 3. 统一数据约定

### 3.1 标准K线对象

`gp.get()` 在不传 `fields` 时返回对象行，常用字段如下：

```js
{
  date: 20260807,          // 日K通常8位；分钟K通常14位
  code: '600633',
  name: '浙数文化',
  open: 15.20,
  high: 15.68,
  low: 15.10,
  close: 15.55,
  volume: 123456,
  amount: 192000000,
  pre_close: 15.18,
  pct_chg: 2.437,
  amplitude: 3.821,
  float_mv: 12300000000,
  total_mv: 15000000000
}
```

字段是否存在取决于底层数据。策略代码必须使用 `Number.isFinite(Number(row.field))` 检查。

### 3.2 时间格式必须按接口区分

`gp.get()` 的查询参数 `start/end` 只接受：

- 8位日期：`20260807` 或 `'20260807'`
- 14位日期时间：`20260807153900` 或 `'20260807153900'`

`gp.get()` 不接受 Unix 秒、Unix 毫秒、`Date` 对象或 `2026-08-07` 这类字符串。传入这些值会抛出 `RangeError`。

`tu.get()` 图表数据中的 `date/time` 支持范围更宽：8位或12～14位数字日期、Unix秒、Unix毫秒、`Date` 对象、纯时间字符串以及可被 `Date.parse()` 解析的字符串。

`zb.get()` 对对象行计算时会把 `row.date` 转为数字并按日期排序。为确保与 `gp.get()` 和 `tu.get()` 对齐，策略数据统一使用8位或14位数字日期，不要混用格式。

### 3.3 排序约定

- `gp.get()` 默认 `desc: false`，即时间升序。
- 指标计算要求时间升序。
- 如果为了列表展示使用降序数据，计算指标前应恢复升序，或者让 `zb.get()` 自己按日期整理对象行。

---

## 4. gp.js：行情数据

### 4.1 推荐签名

```js
const result = await gp.get({
  code,                 // 必须：单个代码、代码数组、前缀或前缀数组
  start: null,          // 可选：8位或14位
  end: null,            // 可选：8位或14位
  frequency: '1d',     // 1d/1m/5m/15m/30m/60m/1w/1M
  fields: null,         // null、逗号字符串或数组
  limit: null,          // 非负整数
  desc: false,          // false升序，true降序
  fq: 'qfq',            // qfq/hfq/null/'none'
  onProgress: null      // 可选进度回调
});
```

也支持旧式位置参数，但 AI 生成代码时统一使用配置对象：

```js
await gp.get(code, start, end, frequency, fields, limit, desc, fq);
```

### 4.2 code 支持形式

```js
await gp.get({ code: '600633' });
await gp.get({ code: ['600633', '000001'] });
await gp.get({ code: '6*' });
await gp.get({ code: ['0*', '1*', '3*', '5*', '6*'] });
await gp.get({ code: '*' }); // 可用；拉完整K线时数据很大，不推荐
```

代码可带 `sh`、`sz` 前缀，最终会规范为六位数字。

股票代码应始终使用字符串。`600633` 作为数字仍可识别，但 `000001` 写成数字会变成 `1` 并校验失败；因此不要在策略代码中使用数字型股票代码。

### 4.3 返回结构

单个代码或单个前缀返回数组：

```js
const rows = await gp.get({ code: '600633' });
// rows: [{date, code, open, high, low, close, ...}, ...]
```

传入数组时返回以输入项为键的对象：

```js
const result = await gp.get({ code: ['600633', '000001'] });
// {
//   '600633': [...],
//   '000001': [...]
// }
```

前缀数组同理：

```js
const chunks = await gp.get({ code: ['0*', '3*', '6*'] });
const allRows = Object.values(chunks).flat();
```

注意：`chunks['6*']` 是六开头股票的扁平K线数组，真实股票代码读取每行的 `row.code`。

### 4.4 fields 的重要歧义

不传 `fields`：返回对象数组，适合策略、指标和绘图。

```js
const rows = await gp.get({ code: '600633', fields: null });
```

传一个字段：返回标量数组。

```js
const dates = await gp.get({ code: '600633', fields: 'date' });
// [20260801, 20260804, ...]
```

传多个字段：返回二维位置数组，列顺序与 `fields` 一致，不是对象数组。

```js
const rows = await gp.get({
  code: '600633',
  fields: 'date,code,volume,close'
});
// [[20260801, '600633', 123456, 15.2], ...]
```

因此，只要后面还要调用 `zb.get()` 或 `tu.get()`，推荐 `fields: null`。

### 4.5 日期范围

```js
// 单日
await gp.get({ code: '600633', start: '20260807', end: '20260807' });

// 日期区间
await gp.get({ code: '600633', start: '20260701', end: '20260807' });

// 某日全部分钟数据
await gp.get({
  code: '600633',
  start: '20260807',
  end: '20260807',
  frequency: '1m'
});
```

只传 `start` 时需区分周期：

- 日K/周K/月K传8位日期：查询该交易日。
- 分钟K传8位日期：自动展开为当天 `00:00:00～23:59:59`，即查询整天分钟数据。
- 分钟K传14位日期时间：查询该精确时间键。
- 只传 `end`：查询数据库起点至 `end`。

“从 start 到现在”不是隐含语义。需要区间时应同时传 `start` 和 `end`。

### 4.6 frequency

| 值 | 含义 |
|---|---|
| `1d` | 日K |
| `1m` | 1分钟 |
| `5m` | 5分钟聚合 |
| `15m` | 15分钟聚合 |
| `30m` | 30分钟聚合 |
| `60m` | 60分钟聚合 |
| `1w` | 周K聚合 |
| `1M` | 月K聚合，注意大写 M |

5/15/30/60分钟、周K和月K会在客户端聚合，因此大区间应谨慎控制日期长度。

### 4.7 复权

```js
fq: 'qfq'   // 默认，前复权
fq: 'hfq'   // 后复权
fq: null    // 不复权
fq: 'none'  // 不复权
```

复权因子首次加载后由 `gp.js` 全量缓存，不会每只股票重复请求。

### 4.8 进度

```js
const rows = await gp.get({
  code: ['0*', '3*', '6*'],
  start: '20260701',
  end: '20260807',
  onProgress(info) {
    // info.kind: 'market' 或 'factor'
    // info.loaded: 已接收字节
    // info.total: 总字节；服务器未返回 Content-Length 时可能为 null
    // info.percent: 0~100；无法确定总量时可能为 null
    // info.done: 是否完成
    updateProgressUI(info);
  }
});
```

不能伪造百分比。`percent === null` 时显示不确定进度动画和真实已加载字节。

### 4.9 其他方法

```js
gp.url('600633', '20260801', '20260807', '1d'); // 只生成请求URL
await gp.ready();                                // 预加载/取得复权因子缓存
gp.configure({ baseUrl: '192.168.1.1:7899' });
gp.clearCache();
await gp.refresh();
```

---

## 5. 大范围市场批量拉取标准方式

### 5.1 先明确证券范围

代码前缀代表数据库范围，不天然等于“全部股票”：

```js
// 沪深A股 + 当前数据库中的北交所920代码
const A_SHARE_PREFIXES = ['0*', '3*', '6*', '920*'];

// 数据库主要证券前缀；包含股票、基金、债券等，不可直接当作纯股票池
const ALL_SECURITY_PREFIXES = ['0*', '1*', '3*', '5*', '6*', '9*'];
```

- `1*`、`5*` 中包含基金、债券等非股票证券。
- `9*` 包含 `920xxx` 北交所股票，也可能包含其他9开头证券。
- 如果用户指定 `0* / 1* / 3* / 5* / 6*`，应称为“指定前缀范围”，不能自动称为完整A股市场。
- 策略页面必须把实际使用的代码范围写入配置和界面标题。

需要检查某个交易日数据库实际包含哪些代码时，可以只投影 `code` 字段；这种单日标量查询数据较小：

```js
const availableCodes = await gp.get({
  code: '*',
  start: '20260807',
  end: '20260807',
  frequency: '1d',
  fields: 'code',
  fq: null
});
```

该查询用于发现代码范围，不应用它替代后续按前缀分片拉取完整K线。

### 5.2 禁止逐股循环

错误方式：

```js
// 禁止：可能产生五千多次 HTTP 请求
for (const code of allCodes) {
  await gp.get({ code, start, end });
}
```

正确方式：按已明确的范围分片请求。

```js
const chunks = await gp.get({
  code: A_SHARE_PREFIXES,
  start: '20260701',
  end: '20260807',
  frequency: '1d',
  fields: null,
  desc: false,
  fq: 'qfq'
});

const allRows = Object.values(chunks).flat();
```

HTTP行情请求数约等于前缀分片数，而不是股票数量。首次使用前/后复权时，`gp.js` 还会额外共享一次全量复权因子请求。

### 5.3 按股票分组

```js
function groupByCode(rows) {
  const output = Object.create(null);
  for (const row of rows) {
    const code = String(row.code || '');
    if (!/^\d{6}$/.test(code)) continue;
    if (!output[code]) output[code] = [];
    output[code].push(row);
  }
  for (const values of Object.values(output)) {
    values.sort((a, b) => Number(a.date) - Number(b.date));
  }
  return output;
}

const rowsByCode = groupByCode(allRows);
```

### 5.4 页面级 Promise 缓存

`gp.js` 缓存复权因子，但不会自动缓存每次行情查询。策略页面必须缓存行情结果。

```js
const marketCache = new Map();

function marketCacheKey(options) {
  const code = Array.isArray(options.code)
    ? options.code.map(String).slice().sort()
    : String(options.code);
  const fields = Array.isArray(options.fields)
    ? options.fields.map(String)
    : (options.fields ?? null);
  return JSON.stringify({
    code,
    start: options.start ?? null,
    end: options.end ?? null,
    frequency: options.frequency ?? '1d',
    fields,
    limit: options.limit ?? null,
    desc: Boolean(options.desc),
    fq: options.fq === undefined ? 'qfq' : options.fq
  });
}

function getMarketCached(options) {
  const key = marketCacheKey(options);
  if (!marketCache.has(key)) {
    const task = gp.get(options).catch(error => {
      marketCache.delete(key);
      throw error;
    });
    marketCache.set(key, task); // 缓存 Promise，可合并并发请求
  }
  return marketCache.get(key);
}
```

---

## 6. bk.js：板块双向映射

`bk.js` 加载并通过授权后会自动请求一次全部板块数据并构建内存索引。`bk.get()`、`bk.ready()` 会复用同一个初始化 Promise；后续按股票、板块或名称查询不会再次逐项发送 HTTP 请求。

### 6.1 板块对象

```js
{
  code: '309265.TI',
  name: '文化传媒',
  source: '...',
  type: '...',
  group: '...',
  category: '概念',
  symbols: ['600633', '600xxx', ...]
}
```

类别可使用中文或数字：

| 数字 | 类别 |
|---|---|
| `0` | 概念 |
| `1` | 申万一级 |
| `2` | 申万二级 |
| `3` | 申万三级 |

### 6.2 查询形式

```js
await bk.get();
// 全部板块对象数组，包含 symbols

await bk.get('309265.TI');
// 单个板块对象，包含 symbols

await bk.get('309265.TI', null, 'symbols');
// 只返回股票代码数组

await bk.get('600633');
// 返回该股票所属的全部板块数组，不包含每个板块的 symbols

await bk.get('600633', '申万一级');
// 只返回该股票所属的申万一级板块

await bk.get('文化传媒', '概念');
// 按名称查询；先精确匹配，未命中时模糊包含匹配

await bk.get(['600633', '000001']);
// {'600633': [...], '000001': [...]}
```

配置对象写法：

```js
await bk.get('600633', {
  category: '概念',
  fields: 'code,name,category'
});
```

### 6.3 fields 返回规则

允许字段：

```text
code,name,source,type,group,category,symbols
```

返回形态不仅取决于 `fields`，还取决于查询类型：

| 查询形式 | 不传 fields | 单字段 | 多字段 | 未命中 |
|---|---|---|---|---|
| 精确板块代码 | 板块对象 | 标量；`symbols` 本身是数组 | 单个位置数组 | 通常进入名称查询逻辑 |
| 股票代码 | 板块对象数组 | 标量数组 | 二维位置数组 | `[]` |
| 指定类别的精确板块名称 | 板块对象 | 标量 | 单个位置数组 | 未精确命中时继续模糊查询 |
| 模糊名称或未指定类别的名称 | 板块对象数组 | 标量数组 | 二维位置数组 | `[]` |
| `bk.get()` 全量查询 | 板块对象数组 | 标量数组 | 二维位置数组 | `[]` |

因此，不能只根据“匹配数量看起来为1”判断返回值是否为数组。股票代码查询和未指定类别的名称查询始终按多项结果处理。

需要统一成数组时，仅对对象查询结果使用下面的辅助函数；不要对 `fields:'symbols'` 的结果使用它，因为该标量本身就是股票代码数组：

```js
function boardObjects(value) {
  if (Array.isArray(value)) return value;
  return value && typeof value === 'object' && Object.keys(value).length ? [value] : [];
}
```

### 6.4 一只股票可能属于多个板块

“所属板块”不是唯一值。AI 在做分组界面时必须明确主板块选择规则，例如：

```js
function choosePrimaryBoard(boards, preferredCategory = '申万一级') {
  return boards.find(item => item.category === preferredCategory)
    || boards[0]
    || null;
}
```

如果用户点击了具体板块，应直接以该板块为当前上下文，不要重新选择主板块。

### 6.5 其他方法

```js
await bk.ready();
bk.configure({ baseUrl: '192.168.1.1:7899' });
bk.reset();
await bk.refresh();
```

---

## 7. zb.js：指标与信号

### 7.1 高层接口

```js
const result = await zb.get(name, codesOrData, options);
```

`codesOrData` 可以是：

- 单个股票代码
- 股票代码数组
- `gp.get()` 返回的对象行数组
- `{code: rows}` 映射
- `Map<code, rows>`
- 尚未完成的 `gp.get()` Promise

### 7.2 推荐：先拉行情，再计算

```js
const kdata = await gp.get({
  code: '600633',
  start: '20260101',
  end: '20260807',
  frequency: '1d',
  fields: null
});

const macd = await zb.get('macd', kdata);
// [{date, dif, dea, macd}, ...]
```

也可以直接传 Promise：

```js
const macd = await zb.get('macd', gp.get({
  code: '600633',
  start: '20260101',
  end: '20260807'
}));
```

也可以让 `zb.js` 根据股票代码自行取数：

```js
const macdByCode = await zb.get('macd', ['600633', '000001'], {
  start: '20260101',
  end: '20260807',
  frequency: '1d',
  fq: 'qfq'
});
```

直接传股票代码而未提供 `start` 时，当前 `zb.js` 会使用固定默认值 `20260302`，并不是“查询全量”。正式策略不得依赖这个内部默认值，必须明确传入 `start/end`。直接传已经取得的行情对象行时不受该默认值影响。

但大范围策略禁止把五千多个代码交给这种逐代码模式。必须先用 `gp.get()` 的前缀分片取得行情，再把 `rowsByCode` 交给 `zb.get()`。

如果行情行没有 `code`，单股数据仍可计算；批量数据必须有 `code` 或使用 `{code: rows}` 映射。

### 7.3 多指标一次计算

```js
const studies = await zb.get('bbi,macd', kdata);
// [{date, bbi, dif, dea, macd}, ...]
```

自定义参数：

```js
const studies = await zb.get(['bbi', 'macd'], kdata, {
  n: [
    [3, 6, 12, 20],
    [12, 26, 9]
  ]
});
```

多指标时，`n` 必须与指标名称一一对应。

### 7.4 批量股票

```js
const indicatorsByCode = await zb.get('macd', rowsByCode);
// {
//   '600633': [{date,dif,dea,macd}, ...],
//   '000001': [{date,dif,dea,macd}, ...]
// }
```

`zb.get()` 会按股票和日期独立计算，不会把不同股票的数据串在一起。

### 7.5 支持的指标及输出字段

| 指标 | 默认输出字段 |
|---|---|
| `macd` | `dif, dea, macd` |
| `kdj` | `k, d, j` |
| `rsi` | `rsi` |
| `wr` | `wr, wr1` |
| `bias` | `bias1, bias2, bias3` |
| `boll` | `upper, mid, lower` |
| `psy` | `psy, psyma` |
| `cci` | `cci` |
| `atr` | `atr` |
| `bbi` | `bbi` |
| `dmi` | `pdi, mdi, adx, adxr` |
| `taq` | `taq_up, taq_mid, taq_down` |
| `ktn` | `ktn_up, ktn_mid, ktn_down` |
| `trix` | `trix, trma` |
| `vr` | `vr` |
| `cr` | `cr` |
| `emv` | `emv, maemv` |
| `dpo` | `dpo, madpo` |
| `brar` | `ar, br` |
| `dfma` | `dfma_dif, dfma_difma` |
| `mtm` | `mtm, mtmma` |
| `mass` | `mass, ma` |
| `roc` | `roc, maroc` |
| `expma` | `exp1, exp2` |
| `obv` | `obv` |
| `mfi` | `mfi` |
| `asi` | `asi, asit` |
| `xsii` | `td1, td2, td3, td4` |

基础指标：`ma, ema, sma, wma, dma, std, sum, hhv, llv, ref`。

```js
const ma = await zb.get('ma', kdata, { n: [5, 10, 20] });
// [{date, ma5, ma10, ma20}, ...]
```

基础指标可以指定输入字段：

```js
const volumeMA = await zb.get('ma', kdata, {
  n: [5, 10],
  fields: 'volume'
});
// [{date, ma5, ma10}, ...]
```

多个输入字段会增加字段前缀：

```js
const values = await zb.get('ma', kdata, {
  n: [5],
  fields: 'close,volume'
});
// [{date, close_ma5, volume_ma5}, ...]
```

### 7.6 金叉和死叉

```js
const signals = await zb.get('macd', kdata, { cross: true });
// [{date, cross}, ...]
```

`cross` 数值：

| 值 | 含义 | 推荐界面显示 |
|---|---|---|
| `1` | 金叉 | `*` |
| `-1` | 死叉 | `×` |
| `0` | 无信号 | 空白 |

同时保留指标值：

```js
const signals = await zb.get('macd', kdata, {
  cross: 'with_value'
});
// [{date, dif, dea, macd, cross}, ...]
```

多指标交叉：

```js
const signals = await zb.get('macd,kdj', kdata, {
  cross: 'with_value'
});
// 包含 macd_cross、kdj_cross 和综合 cross
```

各个 `*_cross` 使用 `1/-1/0` 表示金叉、死叉、无信号。多指标综合 `cross` 只在所有指标同一根K线同时金叉时为 `1`，其他情况为 `0`；不要把综合 `cross` 当作综合死叉字段。

基础均线交叉必须正好提供两个周期：

```js
await zb.get('ma', kdata, {
  n: [5, 10],
  cross: 'with_value'
});
```

### 7.7 市场指数 ZHISHU

```js
const indexRows = await zb.get('zhishu', rowsByCode, {
  method: 1,
  base: 1000,
  frequency: '1d'
});
```

权重方式：

| method | 权重 |
|---|---|
| `1` | 等权 |
| `2` | 流通市值 |
| `3` | 成交额 |
| `4` | 成交量 |
| `5` | 总市值 |

当前实现只有 `frequency:'1d'` 允许 `method=2/5`。任何非 `1d` 周期（包括 `1m/5m/.../1w/1M`）使用 `method=2/5` 都会抛出 `RangeError`，即使输入行中存在市值字段。非日线周期使用 `method=1/3/4`。

`ZHISHU` 需要每只股票的上一根有效收盘价才能计算收益，因此高层接口会删除无法形成指数收益的第一期，返回结果通常比输入日期序列少第一天。不要假定指数结果与输入日期数量完全相同，应始终按 `date` 对齐。

### 7.8 直接数组函数

低层指标是同步函数，适合自定义策略：

```js
const close = kdata.map(row => Number(row.close));
const high = kdata.map(row => Number(row.high));
const low = kdata.map(row => Number(row.low));

const [dif, dea, macd] = zb.MACD(close, 12, 26, 9);
const [k, d, j] = zb.KDJ(close, high, low, 9, 3, 3);
const cross = zb.CROSS(dif, dea);
```

初始周期可能产生 `NaN`。排序和统计前必须过滤 `Number.isFinite(value)`。

全局配置：

```js
zb.configure({
  baseUrl: '192.168.1.1:7899',
  fetch: window.fetch.bind(window), // 通常不需要手动设置
  loader: null                     // 可选自定义行情加载器
});
```

---

## 8. tu.js：绘图

`tu.js` 是零依赖 Canvas 图表，不使用 ECharts。默认白色背景、无网格、自适应尺寸。

`tu.get()` 的绘图过程本身是同步的，但浏览器版 SDK 必须先完成授权。标准策略页面应先 `await gp.get()`、`await bk.get()` 或 `await zb.get()` 完成异步初始化，再调用 `tu.get()`。不要在 `<script src="tu.js"></script>` 后紧接着用静态数据同步调用，否则授权仍在进行时可能抛出“SDK尚未通过授权验证”。当前 `tu.js` 没有公共 `tu.ready()`，策略代码也不得调用内部 `__pxLicense`。

### 8.1 固定签名

```js
const chart = tu.get(data, target, options);
```

- `data`：K线或分时数据
- `target`：HTMLElement、`'#id'`、`'.class'` 或纯 id
- `options`：配置对象

`data` 还可以直接是有效 JSON 字符串、列式对象，或带内联配置的包装对象：

```js
tu.get('[{"price":15.2,"time":"09:30"}]', '#chart', {
  type: 'line'
});

tu.get({
  data: [{ price: 15.2, time: '09:30' }],
  options: { type: 'line', title: '今日分时' }
}, '#chart');

tu.get({
  price: [15.2, 15.3],
  time: ['09:30', '09:31'],
  volume: [1000, 1200]
}, '#chart', { type: 'line' });
```

兼容旧式：

```js
tu.get(data, '#chart', '今日分时', 15.20);
```

AI 生成代码时统一使用配置对象。

### 8.2 K线数据

二维数组格式：

```js
[
  [open, close, high, low, volume, time],
  ...
]
```

对象格式：

```js
[
  { open, close, high, low, volume, date },
  ...
]
```

`gp.get()` 的完整对象行可以直接传给 `tu.get()`。

### 8.3 分时/Tick 数据

```js
[
  [price, time, volume],
  ...
]
```

或：

```js
[
  { price, time, volume },
  ...
]
```

分钟行情来自 `gp.get()` 时通常仍含 OHLC，自动识别会认为是蜡烛图。要画分时线必须强制 `type: 'line'`：

```js
const minuteRows = await gp.get({
  code: '600633',
  start: '20260807',
  end: '20260807',
  frequency: '1m'
});

tu.get(minuteRows, '#minute-chart', {
  type: 'line',
  title: '20260807 · 文化传媒 · 600633',
  preclose: 15.20
});
```

`line` 类型会从对象的 `price` 或 `close` 读取价格。

### 8.4 preclose

- 分时图传入 `preclose` 时，以它为昨收中线并计算右侧涨跌幅。
- 分时图未传 `preclose` 时，以第一根价格为基准。
- K线从第二根开始自动按上一根收盘计算涨幅；第一根可使用传入的 `preclose`。

### 8.5 常用配置

```js
tu.get(kdata, '#chart', {
  type: 'auto',          // auto/kline/line
  title: '带MACD的日K',
  theme: 'light',
  height: 'auto',
  minHeight: 240,
  maxHeight: 620,
  aspectRatio: 2.15,
  showHeader: true,
  showTooltip: true,
  showVolume: true,
  showGrid: false,
  showCrosshair: true,
  showMA: false,
  ma: [5, 10, 20],
  preclose: null,
  zhibiao: null,
  biaoji: [],
  visibleCount: null,
  pricePrecision: null,
  wheelZoom: true,
  dragPan: true,
  paddingLeft: 0,
  paddingRight: 0,
  axisInside: true,
  sort: true
});
```

### 8.6 指标接图

```js
const kdata = await gp.get({
  code: '600633',
  start: '20260101',
  end: '20260807'
});

const zhibiao = await zb.get('bbi,macd', kdata);

tu.get(kdata, '#chart', {
  title: '600633 · BBI + MACD',
  zhibiao
});
```

默认绘图规则：

- `bbi/boll/taq/ktn/expma/ma*` 等叠加到主图。
- `macd/kdj/rsi/...` 自动建立下方指标面板。
- `macd` 自动使用柱状图，其余通常为线。
- 指标按 `date/time` 与主图对齐。

批量指标传给单只图时应选择对应代码：

```js
tu.get(rowsByCode[code], '#chart', {
  zhibiao: indicatorsByCode[code]
});
```

不能直接传尚未完成的 Promise：

```js
// 错误
tu.get(kdata, '#chart', { zhibiao: zb.get('macd', kdata) });

// 正确
const zhibiao = await zb.get('macd', kdata);
tu.get(kdata, '#chart', { zhibiao });
```

### 8.7 标记线 biaoji

```js
tu.get(kdata, '#chart', {
  biaoji: [20260801, 20260807]
});
```

带备注：

```js
tu.get(kdata, '#chart', {
  biaoji: [
    [20260801, '买点'],
    [20260807, '卖点', '#1677d2']
  ]
});
```

对象格式：

```js
biaoji: [
  { time: 20260801, label: '突破', color: '#e53935' }
]
```

### 8.8 自定义指标图形

只要自定义结果包含时间字段和数值字段，就能直接交给 `zhibiao`：

```js
const customRows = kdata.map((row, index) => ({
  date: row.date,
  my_score: calculateScore(kdata, index)
}));

tu.get(kdata, '#chart', {
  title: '自定义策略分数',
  zhibiao: customRows
});
```

需要控制名称、面板、颜色或图形类型时：

```js
tu.get(kdata, '#chart', {
  zhibiao: {
    data: customRows,
    series: [
      {
        field: 'my_score',
        name: '策略分数',
        pane: '策略',       // 'main' 表示叠加到K线主图
        type: 'line',      // line 或 bar
        color: '#1677d2',
        width: 1.5
      }
    ]
  }
});
```

因此不要发明 `chart.setIndicators()`、`chart.addIndicator()` 等不存在的接口。初始化时使用 `options.zhibiao`，后续修改使用 `chart.setOptions({ zhibiao })`。

### 8.9 更新和销毁

```js
const chart = tu.get(kdata, '#chart', options);

chart.update(newRow);       // 同时间更新最后一根，否则追加
chart.append(moreRows);     // 追加
chart.setData(allRows);     // 全量替换
chart.setOptions(options);  // 修改配置
chart.getData();
chart.resize();
chart.toDataURL();
chart.destroy();

tu.update('#chart', newRow);
tu.destroy('#chart');
tu.getInstance('#chart');
```

一个容器只能保留一个有效图表实例。`tu.get()` 不会自动销毁同一容器中已有的实例，反复调用会叠加 Canvas 和事件监听器。正确方式是首次调用一次 `tu.get()`，之后复用返回实例：

```js
let chart = tu.get(kdata, '#chart', options);

// 切换股票或周期
chart.setData(nextData, nextOptions);

// 只修改标题、指标、标记等配置
chart.setOptions({ title, zhibiao, biaoji });

// 确实需要重建时
chart.destroy();
chart = tu.get(nextData, '#chart', nextOptions);
```

`chart.setOptions()` 不会重新识别图表类型。K线与分时线互相切换时应调用 `chart.setData(nextData, { type:'kline' })` 或 `{ type:'line' }`。

### 8.10 事件

配置回调：

```js
tu.get(kdata, '#chart', {
  onHover(detail, chart) {},
  onClick(detail, chart) {},
  onRangeChange(detail, chart) {}
});
```

DOM 事件：

```js
const element = document.querySelector('#chart');
element.addEventListener('tu:hover', event => console.log(event.detail));
element.addEventListener('tu:click', event => console.log(event.detail));
element.addEventListener('tu:rangeChange', event => console.log(event.detail));
```

---

## 9. 完整策略流水线

### 9.1 指定A股范围的行情、指标、板块

```js
// 示例股票池：沪深A股 + 当前数据库的北交所920代码。
// 如果产品需要基金、债券或其他证券，应由界面配置明确增加对应前缀。
const PREFIXES = ['0*', '3*', '6*', '920*'];

async function loadStrategyData(start, end) {
  const [chunks] = await Promise.all([
    getMarketCached({
      code: PREFIXES,
      start,
      end,
      frequency: '1d',
      fields: null,
      desc: false,
      fq: 'qfq'
    }),
    bk.ready()
  ]);

  const rowsByCode = groupByCode(Object.values(chunks).flat());
  const codes = Object.keys(rowsByCode);

  const [macdByCode, boardsByCode] = await Promise.all([
    zb.get('macd', rowsByCode, { cross: 'with_value' }),
    bk.get(codes)
  ]);

  return { rowsByCode, macdByCode, boardsByCode };
}
```

### 9.2 停牌与无效数据

```js
function validTradingRows(rows) {
  return rows
    .filter(row => {
      if (!row) return false;
      const close = Number(row.close);
      if (!Number.isFinite(close) || close <= 0) return false;

      // 数据源明确给出涨幅但值为 '-'、null、NaN 时，按无效/停牌记录排除。
      if (Object.prototype.hasOwnProperty.call(row, 'pct_chg')) {
        if (row.pct_chg === '-' || row.pct_chg === null || row.pct_chg === '') return false;
        if (!Number.isFinite(Number(row.pct_chg))) return false;
      }

      // 日K存在成交量字段时，零成交量通常表示未发生有效交易。
      if (Object.prototype.hasOwnProperty.call(row, 'volume')) {
        const volume = Number(row.volume);
        if (!Number.isFinite(volume) || volume <= 0) return false;
      }
      return true;
    })
    .slice()
    .sort((a, b) => Number(a.date) - Number(b.date));
}
```

缺少某个交易日表示该股票当日没有有效记录。不要自动补零，也不要生成零涨幅记录。若具体数据源对停牌有独立字段，应优先使用该字段；上面的规则是当前常用字段下的保守过滤。

### 9.3 N日涨幅必须明确口径

“最近 N 条有效K线涨幅”示例：

```js
function nBarReturn(rows, n) {
  const values = validTradingRows(rows).slice(-n);
  if (values.length < 2) return null;
  const first = Number(values[0].close);
  const last = Number(values[values.length - 1].close);
  return first ? (last / first - 1) * 100 : null;
}
```

注意：N条K线只有 N-1 个收盘间隔。如果用户要求“完整 N 个交易日收益”，应取 N+1 个有效收盘价。AI 必须在界面说明采用哪一种口径。

### 9.4 连续上涨/下跌

```js
function trailingDirectionCount(rows) {
  const values = validTradingRows(rows);
  if (values.length < 2) return 0;
  let count = 0;
  const lastDirection = Math.sign(
    Number(values.at(-1).close) - Number(values.at(-2).close)
  );
  if (!lastDirection) return 0;
  for (let i = values.length - 1; i > 0; i -= 1) {
    const direction = Math.sign(Number(values[i].close) - Number(values[i - 1].close));
    if (direction !== lastDirection) break;
    count += lastDirection;
  }
  return count; // 正数连续上涨，负数连续下跌
}
```

### 9.5 金叉显示

```js
function crossText(value) {
  if (value === 1) return '*';
  if (value === -1) return '×';
  return '';
}
```

### 9.6 汇总必须基于完整集合

```js
function summarize(allMetrics) {
  const returns = allMetrics
    .map(item => item.returnPct)
    .filter(Number.isFinite);
  return {
    avg: returns.length
      ? returns.reduce((sum, value) => sum + value, 0) / returns.length
      : null,
    goldCrossCount: allMetrics.filter(item => item.cross === 1).length
  };
}
```

先对完整股票集合计算，再把结果交给虚拟列表显示。不能对当前屏幕中的几十个单元格计算平均值。

---

## 10. 策略界面性能规范

### 10.1 必须缓存的内容

- 行情窗口：`start/end/frequency/fq/prefixes`
- `rowsByCode`
- 板块全量索引
- 每只股票计算后的指标
- 悬浮分时图：`code/date/frequency`

### 10.2 不得在渲染循环中请求

以下事件只能使用缓存数据：

- 排序方式切换
- 天数 N 修改后的重复渲染
- 板块切换
- 横向拖动和虚拟窗口更新
- 页面翻页或滚动

如果 N 改变需要重新计算，可以使用已缓存的K线重新计算，不应重新拉行情。

### 10.3 悬浮图

```js
const hoverCache = new Map();

function getMinuteCached(code, date) {
  const key = `${code}:${date}`;
  if (!hoverCache.has(key)) {
    hoverCache.set(key, gp.get({
      code,
      start: date,
      end: date,
      frequency: '1m',
      fq: 'qfq'
    }).catch(error => {
      hoverCache.delete(key);
      throw error;
    }));
  }
  return hoverCache.get(key);
}
```

悬浮层应延迟关闭约 100~200ms。鼠标从股票单元格移动到悬浮层时保持显示，只切换数据，不反复销毁和创建容器。
### 10.4 大规模列表
选定范围达到五千多只股票时：
- 数据与 DOM 分离。
- 只渲染可见范围和少量缓冲区。
- 拖动导航时按单只股票连续移动，不按整页跳变。
- 编号是全局排序位置，不是当前虚拟窗口序号。
- 汇总指标仍使用完整数据集。

---

## 11. AI 生成页面时的界面契约

用户给出布局和策略需求后，AI 应按以下顺序实现：

1. 明确证券范围：全部证券、A股股票、自定义代码前缀或某个板块。
2. 明确周期：日K、分钟、周K或月K。
3. 明确日期区间和 N 的统计口径。
4. 一次性批量拉取并建立缓存。
5. 按股票分组并计算完整指标与汇总。
6. 再实现筛选、排序和虚拟化渲染。
7. 最后接入 `tu.get()` 的主图或悬浮图。

默认视觉要求：

- 白色背景。
- 紧凑布局。
- 不依赖 ECharts。
- 图表无背景网格。
- 图表和列表自适应窗口。
- 不出现无意义的浏览器整体滚动条。
- 加载进度使用真实字节或不确定状态，不伪造百分比。
- 单列最大宽度由界面需求控制，密集股票矩阵通常不超过 120px。
- 悬浮层必须检查四个屏幕边缘并自动换向。

---

## 12. 常见错误清单

### 错误：逐股拉全市场

```js
await Promise.all(codes.map(code => gp.get({ code })));
```

改为前缀分片。

### 错误：fields 后仍当对象使用

```js
const rows = await gp.get({ code, fields: 'date,close' });
console.log(rows[0].close); // 错误，rows[0] 是 [date, close]
```

### 错误：未 await 指标

```js
tu.get(kdata, '#chart', { zhibiao: zb.get('macd', kdata) });
```

### 错误：把 tu.get 写成单个配置对象

```js
// 错误：tu.get 不接受这种签名
tu.get({ target: '#chart', data: kdata, title: '日K' });

// 正确：data 和 target 是前两个位置参数
tu.get(kdata, '#chart', { title: '日K' });
```

### 错误：分钟 OHLC 自动画成蜡烛

```js
tu.get(minuteRows, '#chart');
```

如需分时线应设置 `{ type: 'line' }`。

### 错误：把停牌当成零涨幅

无有效 `close`、`pct_chg` 为 `-`/无效值，或存在 `volume` 且成交量不大于0的日K记录应排除，不能参与平均值、连续涨跌和 N 日涨幅。

### 错误：板块内排序忽略当前参数

切换板块后仍应读取当前排序方式、统计天数和其他筛选条件，只改变股票集合。

### 错误：统计只计算可见窗口

平均涨幅和金叉数必须对完整筛选结果计算。

---

## 13. 给 AI 的需求模板

用户可以直接按以下方式描述页面：

```text
使用 gp.js、bk.js、zb.js、tu.js 开发一个纯前端策略页面。

证券范围：全部证券 / A股股票 / 自定义代码前缀 / 某个板块 / 指定代码
日期范围：YYYYMMDD 到 YYYYMMDD
周期：1d / 1m / 5m / 1w / 1M
策略：例如最近20根有效K线涨幅、连续涨跌、MACD金叉
排序：例如全市场按20日涨幅降序
布局：描述顶部、左侧、股票矩阵、底部导航和悬浮图
主图：日K或分时，显示哪些指标和标记
交互：板块切换、筛选、拖动、悬浮、点击
视觉：颜色、字号、间距、最大列宽

要求：
1. 大范围市场使用前缀分片批量请求，不逐股请求；明确是否包含基金、债券和北交所。
2. 行情、板块、指标和悬浮分钟数据全部缓存。
3. 停牌和无效价格不参与统计。
4. 汇总基于完整结果，不基于当前可见窗口。
5. 直接给出可运行的 HTML/CSS/JS。
```

当需求存在歧义时，AI 优先明确以下问题：

- N 日是 N 条K线还是 N 个完整收益区间？
- 一只股票属于多个板块时采用哪个主板块？
- 金叉是只看当天，还是统计区间内出现次数？
- 分钟行情画蜡烛还是分时线？
- “全部”排序是当前选定证券范围的整体排序，还是按板块分组后排序？

如果用户已经明确这些口径，AI 不应重复询问，应直接实现。
