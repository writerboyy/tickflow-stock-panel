# 小石金融信息与量化专家接入

本项目将小石作为**独立的可信只读数据服务**接入，不会自动替换 TickFlow
canonical provider，也不会把在线接口作为回测循环的数据源。

## 配置

将用户授权的 Key 注入后端进程环境：

```bash
XIAOSHI_API_KEY=... uv run uvicorn app.main:app --reload --port 3018
```

Key 只从 `XIAOSHI_API_KEY` 读取，不写入 `data/user_data/secrets.json`，也不会出现在
状态接口、异常上下文或日志中。

## 资源版本

- `GET /api/settings/xiaoshi`：查看只读状态、已验证的 Manifest/Prompt/Skill 版本和
  `xiaoshi-data` 是否可用；不返回 Key。
- `POST /api/settings/xiaoshi/refresh`：按任务开始规则读取一次 Manifest。版本和 SHA-256
  未变化时不下载正文。
- `POST /api/settings/xiaoshi/update-prompt`：用户明确要求更新时，下载 Prompt、Skill
  全部 `skill_files` 和 API schema，逐文件校验 SHA-256/大小，再校验整体 Skill 包；
  最后原子切换版本。失败保留最后一个已验证版本。

## 当前只读行情

`GET /api/xiaoshi/quote/{symbol}?market=CN&instrument=stock` 只代理小石统一行情接口。
必须显式传 `market` 和 `instrument`，返回值中的标的身份由小石响应确认。

## 历史数据

历史读取必须经过 `xiaoshi-data`，并在每次查询前依次运行：

```text
xiaoshi-data catalog
xiaoshi-data schema --dataset <dataset>
xiaoshi-data coverage --dataset <dataset>
xiaoshi-data query --dataset <dataset>
```

查询前还会读取当前历史 Manifest，并只接受同时声明了 `publication_scope` 和
`coverage` 且覆盖目标市场、标的和时间范围的记录。不会枚举 R2 key。

`financial-as-reported` 必须传带时区的 `as_of`，每条结果都必须有
`available_at <= as_of`；缺失或无效时间会阻断结果。

`429` / `bulk_download_required` 被作为受控保护响应返回，包含脱敏参数、版本和错误
指纹；客户端不会无限重试。空结果、缺失、停牌、非交易日和源不可用不会被转换为零。
