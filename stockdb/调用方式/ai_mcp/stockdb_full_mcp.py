import asyncio
import datetime
import json
import re
import stock_sdk as sdk
from stock_sdk import *
from native_mcp import NativeMCP

# 1. 实例化本地纯异步 NativeMCP 服务器
mcp = NativeMCP("StockDB-Native-Server")
SAFE_ROOTS = {
    "valuation": valuation,
    "income": income,
    "cash_flow": cash_flow,
    "indicator": indicator,
    "balance": balance,
    "bond": bond,
    "finance": finance,
    "opt": opt,
}
RUN_QUERY_TABLES = {
    "bond": [
        "bond.BOND_BASIC_INFO",
        "bond.BOND_COUPON",
        "bond.BOND_INTEREST_PAYMENT",
        "bond.CONBOND_BASIC_INFO",
        "bond.CONBOND_CONVERT_PRICE_ADJUST",
        "bond.CONBOND_DAILY_CONVERT",
        "bond.CONBOND_DAILY_PRICE",
        "bond.REPO_DAILY_PRICE",
    ],
    "finance": [
        "finance.CCTV_NEWS",
        "finance.FINANCE_BALANCE_SHEET_PARENT",
        "finance.FINANCE_CASHFLOW_STATEMENT",
        "finance.FINANCE_INCOME_STATEMENT",
        "finance.FUND_DIVIDEND",
        "finance.FUND_FIN_INDICATOR",
        "finance.FUND_INVEST_TARGET",
        "finance.FUND_MAIN_INFO",
        "finance.FUND_MF_DAILY_PROFIT",
        "finance.FUND_NET_VALUE",
        "finance.FUND_PORTFOLIO",
        "finance.FUND_PORTFOLIO_BOND",
        "finance.FUND_PORTFOLIO_STOCK",
        "finance.FUND_SHARE_DAILY",
        "finance.FUT_CHARGE",
        "finance.FUT_GLOBAL_DAILY",
        "finance.FUT_MARGIN",
        "finance.FUT_MEMBER_POSITION_RANK",
        "finance.FUT_WAREHOUSE_RECEIPT",
        "finance.STK_AH_PRICE_COMP",
        "finance.STK_AUDIT_OPINION",
        "finance.STK_CAPITAL_CHANGE",
        "finance.STK_COMPANY_INFO",
        "finance.STK_EL_CONST_CHANGE",
        "finance.STK_EL_TOP_ACTIVATE",
        "finance.STK_EMPLOYEE_INFO",
        "finance.STK_EXCHANGE_LINK_CALENDAR",
        "finance.STK_EXCHANGE_LINK_RATE",
        "finance.STK_EXCHANGE_TRADE_INFO",
        "finance.STK_FIN_FORCAST",
        "finance.STK_HK_HOLD_INFO",
        "finance.STK_HOLDER_NUM",
        "finance.STK_LIMITED_SHARES_LIST",
        "finance.STK_LIMITED_SHARES_UNLIMIT",
        "finance.STK_LIST",
        "finance.STK_MANAGEMENT_INFO",
        "finance.STK_ML_QUOTA",
        "finance.STK_MT_TOTAL",
        "finance.STK_NAME_HISTORY",
        "finance.STK_PERFORMANCE_LETTERS",
        "finance.STK_REPORT_DISCLOSURE",
        "finance.STK_SHAREHOLDER_FLOATING_TOP10",
        "finance.STK_SHAREHOLDER_TOP10",
        "finance.STK_SHAREHOLDERS_SHARE_CHANGE",
        "finance.STK_SHARES_FROZEN",
        "finance.STK_SHARES_PLEDGE",
        "finance.STK_STATUS_CHANGE",
        "finance.STK_XR_XD",
    ],
    "opt": [
        "opt.OPT_ADJUSTMENT",
        "opt.OPT_CONTRACT_INFO",
        "opt.OPT_DAILY_PREOPEN",
        "opt.OPT_DAILY_PRICE",
        "opt.OPT_EXERCISE_INFO",
        "opt.OPT_RISK_INDICATOR",
        "opt.OPT_TRADE_RANK_STK",
    ],
}
FUNDAMENTAL_ALLOWED_ROOTS = frozenset({"valuation", "income", "cash_flow", "indicator", "balance"})
RUN_QUERY_ALLOWED_ROOTS = frozenset({"bond", "finance", "opt"})
RUN_QUERY_ALLOWED_TABLES = frozenset(
    table_name
    for table_names in RUN_QUERY_TABLES.values()
    for table_name in table_names
)
SAFE_PATH_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
BK_CATEGORY_ALIASES = {
    "0": 0,
    "concept": 0,
    "概念": 0,
    "1": 1,
    "sw_l1": 1,
    "sw1": 1,
    "申万一级": 1,
    "2": 2,
    "sw_l2": 2,
    "sw2": 2,
    "申万二级": 2,
    "3": 3,
    "sw_l3": 3,
    "sw3": 3,
    "申万三级": 3,
}


def _format_result(result) -> str:
    if hasattr(result, "to_json"):
        try:
            return result.to_json(orient="records", force_ascii=False, date_format="iso")
        except TypeError:
            try:
                return result.to_json(force_ascii=False)
            except Exception:
                pass
        except Exception:
            pass

    if isinstance(result, (list, tuple, dict, str, int, float, bool)) or result is None:
        try:
            return json.dumps(result, ensure_ascii=False, default=str)
        except TypeError:
            pass

    return str(result)


def _ensure_list(value, arg_name: str) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    raise ValueError(f"{arg_name} 必须是 list/tuple。")


def _validate_path(path: str, arg_name: str) -> str:
    if not isinstance(path, str) or not SAFE_PATH_RE.fullmatch(path):
        raise ValueError(f"{arg_name} 非法: {path!r}")
    return path


def _resolve_path(path: str, arg_name: str, allowed_roots=None):
    path = _validate_path(path, arg_name)
    parts = path.split(".")
    root_name = parts[0]
    if root_name not in SAFE_ROOTS:
        raise ValueError(f"{arg_name} 不在允许范围内: {path}")
    if allowed_roots and root_name not in allowed_roots:
        allow_text = ", ".join(sorted(allowed_roots))
        raise ValueError(f"{arg_name} 只允许使用: {allow_text}")

    obj = SAFE_ROOTS[root_name]
    for part in parts[1:]:
        if not hasattr(obj, part):
            raise ValueError(f"{arg_name} 不存在: {path}")
        obj = getattr(obj, part)
    return obj


def _resolve_table(table: str, allowed_roots=None):
    if allowed_roots == FUNDAMENTAL_ALLOWED_ROOTS and table not in FUNDAMENTAL_ALLOWED_ROOTS:
        raise ValueError("财务 table 只允许使用: valuation, income, cash_flow, indicator, balance")
    if allowed_roots == RUN_QUERY_ALLOWED_ROOTS and table not in RUN_QUERY_ALLOWED_TABLES:
        raise ValueError("table 不在 stockdb_list_query_tables 返回的白名单中。")
    table_obj = _resolve_path(table, "table", allowed_roots=allowed_roots)
    return table_obj, table.split(".")[0]


def _resolve_field(field: str, table_obj=None, table_path: str = None, allowed_roots=None):
    field = _validate_path(field, "field")
    field_name = field
    if "." in field:
        if table_path is None or not field.startswith(f"{table_path}."):
            raise ValueError(f"字段必须属于当前 table: {field}")
        field_name = field[len(table_path) + 1:]
        if "." in field_name:
            raise ValueError(f"字段路径层级非法: {field}")

    if table_obj is None or field_name.startswith("_") or not hasattr(table_obj, field_name):
        raise ValueError(f"字段不存在: {field}")
    return getattr(table_obj, field_name)


def _build_filter_expr(table_obj, table_path: str, filter_item: dict, allowed_roots=None):
    if not isinstance(filter_item, dict):
        raise ValueError("filters 内每一项都必须是对象。")

    field_name = filter_item.get("field")
    op = str(filter_item.get("op", "==")).lower()
    value = filter_item.get("value")
    field_expr = _resolve_field(
        field_name,
        table_obj=table_obj,
        table_path=table_path,
        allowed_roots=allowed_roots,
    )

    if op in {"==", "eq"}:
        return field_expr == value
    if op in {"!=", "ne"}:
        return field_expr != value
    if op in {">", "gt"}:
        return field_expr > value
    if op in {">=", "ge"}:
        return field_expr >= value
    if op in {"<", "lt"}:
        return field_expr < value
    if op in {"<=", "le"}:
        return field_expr <= value
    if op in {"in", "in_"}:
        value = _ensure_list(value, "filters.value")
        return field_expr.in_(value)
    if op == "like":
        if not isinstance(value, str):
            raise ValueError("like 操作的 value 必须是字符串。")
        return field_expr.like(value)
    if op == "between":
        value = _ensure_list(value, "filters.value")
        if len(value) != 2:
            raise ValueError("between 操作必须提供两个值。")
        return field_expr.between(value[0], value[1])

    raise ValueError(f"不支持的过滤操作符: {op}")


def _build_order_expr(table_obj, table_path: str, order_item, allowed_roots=None):
    if isinstance(order_item, str):
        field_name = order_item
        direction = "asc"
    elif isinstance(order_item, dict):
        field_name = order_item.get("field")
        direction = str(order_item.get("direction", "asc")).lower()
    else:
        raise ValueError("order_by 内每一项都必须是字符串或对象。")

    field_expr = _resolve_field(
        field_name,
        table_obj=table_obj,
        table_path=table_path,
        allowed_roots=allowed_roots,
    )
    if direction == "asc":
        return field_expr.asc()
    if direction == "desc":
        return field_expr.desc()
    raise ValueError(f"不支持的排序方向: {direction}")


def _build_query(
    table: str,
    fields=None,
    filters=None,
    order_by=None,
    limit: int = None,
    offset: int = None,
    allowed_roots=None,
):
    table_obj, root_name = _resolve_table(table, allowed_roots=allowed_roots)

    field_items = _ensure_list(fields, "fields") if fields is not None else []
    if field_items:
        selected_fields = [
            _resolve_field(
                field_name,
                table_obj=table_obj,
                table_path=table,
                allowed_roots=allowed_roots,
            )
            for field_name in field_items
        ]
        query_obj = query(*selected_fields)
    else:
        query_obj = query(table_obj)

    for filter_item in _ensure_list(filters, "filters"):
        query_obj = query_obj.filter(
            _build_filter_expr(table_obj, table, filter_item, allowed_roots=allowed_roots)
        )

    order_items = _ensure_list(order_by, "order_by") if order_by is not None else []
    if order_items:
        order_exprs = [
            _build_order_expr(table_obj, table, order_item, allowed_roots=allowed_roots)
            for order_item in order_items
        ]
        query_obj = query_obj.order_by(*order_exprs)

    if limit is not None:
        limit = int(limit)
        if limit <= 0:
            raise ValueError("limit 必须大于 0。")
        query_obj = query_obj.limit(limit)

    if offset is not None:
        offset = int(offset)
        if offset < 0:
            raise ValueError("offset 不能小于 0。")
        query_obj = query_obj.offset(offset)

    return query_obj, root_name


def _resolve_financial_fields(fields) -> list:
    resolved = []
    for field_name in _ensure_list(fields, "fields"):
        field_name = _validate_path(field_name, "fields")
        parts = field_name.split(".")
        if len(parts) != 2 or parts[0] not in FUNDAMENTAL_ALLOWED_ROOTS:
            raise ValueError("fields 必须使用 table.field 格式的财务字段。")
        table_obj = SAFE_ROOTS[parts[0]]
        resolved.append(
            _resolve_field(
                field_name,
                table_obj=table_obj,
                table_path=parts[0],
                allowed_roots=FUNDAMENTAL_ALLOWED_ROOTS,
            )
        )
    return resolved


def _call_fundamentals_from_spec(
    table: str,
    filters=None,
    fields=None,
    order_by=None,
    limit: int = None,
    date: str = None,
    stat_date: str = None,
):
    query_obj, _ = _build_query(
        table=table,
        fields=fields,
        filters=filters,
        order_by=order_by,
        limit=limit,
        allowed_roots=FUNDAMENTAL_ALLOWED_ROOTS,
    )
    kwargs = {}
    if date is not None:
        kwargs["date"] = date
    if stat_date is not None:
        kwargs["statDate"] = stat_date
    return get_fundamentals(query_obj, **kwargs)


def _is_sdk_error(result) -> bool:
    return isinstance(result, dict) and bool(result.get("error"))


def _future_contract_names(underlying_symbol: str, date_value: str = None) -> list:
    if not isinstance(underlying_symbol, str) or not re.fullmatch(r"[A-Za-z]{1,8}", underlying_symbol):
        raise ValueError("underlying_symbol 必须是 1-8 位英文字母。")

    query_date = date_value or datetime.date.today().strftime("%Y-%m-%d")
    rows = get_all_securities(types=["futures"], date=query_date)
    if not isinstance(rows, list):
        return []

    prefix = underlying_symbol.upper()
    contracts = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).upper()
        if not name.startswith(prefix) or name.endswith(("8888", "9999")):
            continue
        contracts.append(name)
    return contracts


def _normalize_alpha_factors(alpha_values, max_factor: int) -> list:
    if alpha_values is None:
        numbers = range(1, max_factor + 1)
    else:
        numbers = []
        for value in _ensure_list(alpha_values, "alpha"):
            if isinstance(value, int):
                number = value
            elif isinstance(value, str):
                match = re.fullmatch(r"alpha_(\d{1,3})", value.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("alpha 只支持整数编号或 alpha_NNN 名称。")
                number = int(match.group(1))
            else:
                raise ValueError("alpha 只支持整数编号或 alpha_NNN 名称。")
            if number < 1 or number > max_factor:
                raise ValueError(f"alpha 编号必须在 1 到 {max_factor} 之间。")
            numbers.append(number)
    return list(dict.fromkeys(f"alpha_{number:03d}" for number in numbers))


def _factor_values_for_alpha(date_value: str, securities, alpha_values, max_factor: int):
    if securities is None:
        raise ValueError("为避免公网请求无界扩张，必须显式提供 code/index 股票代码列表。")
    security_list = securities if isinstance(securities, list) else [securities]
    if not security_list or len(security_list) > 20:
        raise ValueError("一次 Alpha 请求最多支持 20 个代码。")

    factor_names = _normalize_alpha_factors(alpha_values, max_factor)
    combined = {factor_name: [] for factor_name in factor_names}
    for security in security_list:
        result = get_factor_values(
            securities=[security],
            factors=factor_names,
            start_date=date_value,
            end_date=date_value,
        )
        if _is_sdk_error(result):
            return result
        if not isinstance(result, dict):
            raise ValueError("get_factor_values 返回格式异常。")
        for factor_name in factor_names:
            values = result.get(factor_name, [])
            if isinstance(values, list):
                combined[factor_name].extend(values)
    return combined


def _normalize_query_root(root: str | None):
    if root is None:
        return None
    normalized = str(root).strip().lower()
    if normalized not in RUN_QUERY_ALLOWED_ROOTS:
        allow_text = ", ".join(sorted(RUN_QUERY_ALLOWED_ROOTS))
        raise ValueError(f"root 只允许使用: {allow_text}")
    return normalized


def _normalize_bk_category(category: int | str | None):
    if category is None or isinstance(category, int):
        return category
    if isinstance(category, str):
        key = category.strip()
        if key in BK_CATEGORY_ALIASES:
            return BK_CATEGORY_ALIASES[key]
        lowered = key.lower()
        if lowered in BK_CATEGORY_ALIASES:
            return BK_CATEGORY_ALIASES[lowered]
        return key
    return category


def _bk_get(x=None, category: int | str | None = None, fields: str | None = None):
    category = _normalize_bk_category(category)
    kwargs = {}
    if fields is not None:
        kwargs["fields"] = fields

    if x is None:
        if category is None:
            return bk.get(**kwargs)
        return bk.get(category=category, **kwargs)

    if category is None:
        return bk.get(x, **kwargs)
    return bk.get(x, category, **kwargs)


@mcp.tool()
def stockdb_get_industry(security: str | list, date: str = None, df: bool = False) -> str:
    """【工具】查询股票所属行业
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - security: 标的代码，支持单支股票代码字符串（如 '000001'）或代码列表（如 ['000001', '000002']） (默认: 必填)  - date: 查询日期，字符串格式如 'YYYY-MM-DD'，默认为 None 表示获取最新交易日的行业数据 (默认: None)
返回字段:
示例: get_industry('000001', date='2024-01-31')"""
    try:
        kwargs = {"security": security}
        if date is not None:
            kwargs["date"] = date
        result = get_industry(**kwargs)
        if df and isinstance(result, dict):
            rows = []
            for security_code, industry_map in result.items():
                if not isinstance(industry_map, dict):
                    rows.append({
                        "security": security_code,
                        "industry_type": None,
                        "industry_code": None,
                        "industry_name": industry_map,
                    })
                    continue
                for industry_type, industry_info in industry_map.items():
                    if isinstance(industry_info, dict):
                        rows.append({
                            "security": security_code,
                            "industry_type": industry_type,
                            "industry_code": industry_info.get("industry_code"),
                            "industry_name": industry_info.get("industry_name"),
                        })
                    else:
                        rows.append({
                            "security": security_code,
                            "industry_type": industry_type,
                            "industry_code": None,
                            "industry_name": industry_info,
                        })
            if rows:
                return _format_result(rows)
        return _format_result(result)
    except Exception as e:
        return f"调用get_industry失败: {str(e)}"

@mcp.tool()
def stockdb_get_data(
        code: Union[str, List[str]],
        start: Optional[str] = None,
        end: Optional[str] = None,
        frequency: str = '1d',
        fields: Optional[Union[str, List[str]]] = None,
        limit: Optional[int] = None,
        desc: bool = False,
        as_df: bool = False,
        fq: Optional[str] = 'qfq'
    ) -> Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], Any]:
    """
    #【行情】股票日/分钟数据优先使用。
    主要：所有行情查询应该优先使用此接口！速度快支持批量，参数说明：
    k = rd.get_data(
        code="600633",                   # 【必须】单股"600633" 或 批量列表["600633", "600422"]
        start="20260625",                # 【可选】默认None(查全量)。8位日期"YYYYMMDD" 或 14位日期(到秒)
        end="20260625",                  # 【可选】默认None(查全量)。8位日期"YYYYMMDD" 或 14位日期
        frequency="5m",                  # 【可选】默认'1d'。可选: 1d(日K), 1m/5m/15m/30m/60m(分钟), 1w(周), 1M(月)
        fields="date,code,volume,close", # 【可选】默认None(全字段dict)。可选: 字段逗号拼接串 或 列表
        limit=100,                       # 【可选】默认None(不限)。限制返回的最大记录条数
        desc=False,                      # 【可选】默认False(升序)。True(时间降序) / False(时间升序)
        as_df=False                      # 【可选】默认False(返回list)。True(返回 Pandas DataFrame) / False
        fq="qfq"                         # 【可选】默认qfq(返回前复权)。hfq(返回 后复权) / None返回 不复权
    )
    print(k)
    """
    try:
        result = rd.get_data(
        code=code,
        start=start,
        end=end,
        frequency=frequency,
        fields=fields,
        limit=limit and int(limit) or None,
        desc=desc,
        as_df=as_df,
        fq=fq
        )
        return str(result)
    except Exception as e:
        return f"调用get_data失败: {str(e)}"


@mcp.tool()
def stockdb_get_all_securities(types: list = [], date: str = None) -> str:
    """【工具】获取交易列表
时间范围: 上市至今更新频率: 8:00更新
参数说明:  - types: 过滤标的类型的列表。可选元素：'stock'(股票), 'fund'(基金), 'index'(指数), 'futures'(期货), 'etf'(ETF), 'lof'(LOF), 'fja'(分级A), 'fjb'(分级B), 'open_fund'(开放式基金), 'bond_fund'(债券基金), 'stock_fund'(股票型), 'QDII_fund'(QDII), 'money_market_fund'(货币型), 'mixture_fund'(混合型), 'options'(期权), 'conbond'(可转债)。默认为 [] 返回所有股票（不含基金/指数/期货） (默认: [])  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 datetime/date 对象，获取该日期处于上市状态的标的；默认为 None 表示获取所有上市/退市标的 (默认: None)
返回字段:  - display_name: 中文名称  - name: 缩写简称  - start_date: 上市日期  - end_date: 退市日期, 如果没有退市则为2200-01-01  - type: 类型: stock(股票), index(交易所指数), etf(ETF基金), fja(分级A), fjb(分级B), fjm(分级母基金), mmf(场内交易的货币基金), open_fund(开放式基金), bond_fund(债券基金), stock_fund(股票型基金), QDII_fund(QDII基金), money_market_fund(场外交易的货币基金), mixture_fund(混合型基金), options(期权), futures(期货), conbond(可转债)
示例: get_all_securities(['stock'], '2024-01-31')"""
    try:
        result = get_all_securities(types=types, date=date)
        return str(result)
    except Exception as e:
        return f"调用get_all_securities失败: {str(e)}"

@mcp.tool()
def stockdb_get_security_info(code: str, date: str = None) -> str:
    """【工具】获取单支标的信息
时间范围: 上市至今更新频率: 8:00更新
参数说明:  - code: 证券代码字符串，如 '000001' 或 '000001'，支持股票、基金、指数、期货、期权、可转债等单个标的 (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象（传入 datetime 时忽略日内时间）；默认为 None (默认: None)
返回字段:
示例: get_security_info('000001')"""
    try:
        raw_code = str(code).split('.')[0]
        result = get_security_info(code=code, date=date) or get_security_info(code=raw_code, date=date)
        if result and str(result).strip():
            if hasattr(result, 'code') or hasattr(result, 'display_name'):
                info_dict = {
                    'code': getattr(result, 'code', code),
                    'display_name': getattr(result, 'display_name', ''),
                    'name': getattr(result, 'name', ''),
                    'start_date': str(getattr(result, 'start_date', '')),
                    'end_date': str(getattr(result, 'end_date', '')),
                    'type': getattr(result, 'type', '')
                }
                return str(info_dict)
            return str(result)
        # 备用方案：通过 get_all_securities 精准查找
        all_sec = get_all_securities(date=date)
        if hasattr(all_sec, 'loc') and (raw_code in all_sec.index or code in all_sec.index):
            idx = raw_code if raw_code in all_sec.index else code
            row = all_sec.loc[idx]
            return str({
                'code': idx,
                'display_name': row.get('display_name', ''),
                'name': row.get('name', ''),
                'start_date': str(row.get('start_date', '')),
                'end_date': str(row.get('end_date', '')),
                'type': row.get('type', '')
            })
        return f"未查询到代码 {code} 的标的信息"
    except Exception as e:
        return f"调用get_security_info失败: {str(e)}"

@mcp.tool()
def stockdb_get_trade_days(start_date: str = None, end_date: str = None, count: int = None) -> str:
    """【工具】获取指定范围交易日
时间范围: 上市至今更新频率: 8:00更新
参数说明:  - start_date: 开始日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None（表示取至今天 today） (默认: None)  - count: 获取的交易日数量（整数 > 0）。当指定 count 时，只能传入 start_date 或 end_date 之一（配合 end_date 表示向前取 count 个交易日，配合 start_date 表示向后取 count 个交易日） (默认: None)
返回字段:
示例: get_trade_days(start_date='2024-01-01', end_date='2024-01-10')"""
    try:
        result = get_trade_days(start_date=start_date, end_date=end_date, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_trade_days失败: {str(e)}"

@mcp.tool()
def stockdb_get_money_flow(security_list: list, start_date: str = None, end_date: str = None, fields: list = None, count: int = None) -> str:
    """【行情】股票日/分钟资金流向
时间范围: 2010年至今（股票可用）；2015年至今（股票可用）更新频率: 盘后19:00更新（股票可用）；分钟级别在每日15:00更新、天行情每日19：00更新（股票可用）
参数说明:  - security_list: 单只股票代码字符串（如 '000001'）或股票代码列表（如 ['000001', '600000']） (默认: 必填)  - start_date: 开始日期/时间，与 count 二选一。支持 'YYYY-MM-DD'（日级）或 'YYYY-MM-DD HH:MM:SS'（分钟级）格式；默认为 None (默认: None)  - end_date: 结束日期/截止时间，支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 格式；默认为 None 表示获取至最新 (默认: None)  - fields: 筛选字段列表。可选包含：'inflow_xl'/'outflow_xl'/'netflow_xl'(超大单), 'inflow_l'/'outflow_l'/'netflow_l'(大单), 'inflow_m'/'outflow_m'/'netflow_m'(中单), 'inflow_s'/'outflow_s'/'netflow_s'(小单)；默认为 None (默认: None)  - count: 获取数据条数（整数 > 0），与 start_date 二选一。表示向前推算 count 个交易日/分钟数据 (默认: None)
返回字段:  - date: 交易日期；str (YYYY-MM-DD)  - sec_code: 股票代码；str  - change_pct: 涨跌幅(%)；float  - net_amount_main: 主力净额(万元)；float  - net_pct_main: 主力净额占比(%)；float  - net_amount_xl: 超大单净额(万元)；float  - net_pct_xl: 超大单净额占比(%)；float  - net_amount_l: 大单净额(万元)；float  - net_pct_l: 大单净额占比(%)；float  - net_amount_m: 中单净额(万元)；float  - net_pct_m: 中单净额占比(%)；float  - net_amount_s: 小单净额(万元)；float  - net_pct_s: 小单净额占比(%)；float
示例: get_money_flow(security_list=['000001'], start_date='2024-01-01', end_date='2024-01-05')"""
    try:
        result = get_money_flow(security_list=security_list, start_date=start_date, end_date=end_date, fields=fields, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_money_flow失败: {str(e)}"

@mcp.tool()
def stockdb_get_ticks(security: str, start_dt: str = None, end_dt: str = None, count: int = None, fields: list = None, skip: bool = True, df: bool = True) -> str:
    """【行情】通用 Tick 数据
时间范围: 2010年至今（股票、期货、基金可用）；上市至今（期权可用）；2017年至今（指数可用）；2019年至今（债券/可转债可用）；2010-01-01至今（股票、期货可用）；2010-01-01 至今（指数可用）；2019-01-01 至今（债券/可转债可用）更新频率: 盘后15:00更新，24:00校对完成入库（股票可用）；夜盘交易数据凌晨2点30后更新,日盘交易数据盘后15点更新；24点入库（期货、期权可用）；盘后15点更新，24点入库（基金、指数、债券/可转债可用）；盘后15:00更新，24:00入库（股票、期货、期权、指数、基金、债券/可转债可用）
参数说明:  - security: 标的代码字符串，仅限单只（如 '000001' 或 'IF2403'），支持股票、期货、期权、基金、指数、可转债 (默认: 必填)  - start_dt: 开始时间/日期，与 count 二选一。支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 格式字符串或 date/datetime 对象 (默认: None)  - end_dt: 结束时间/日期，支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 格式；默认为 None 表示获取至最新 (默认: None)  - count: 获取的 Tick 数量（整数 > 0），与 start_dt 二选一。表示向前推算取 count 个 Tick 数据 (默认: None)  - fields: 筛选字段列表。股票/基金包含五档盘口 ['time', 'current', 'high', 'low', 'volume', 'money', 'a1_v'..'a5_v', 'a1_p'..'a5_p', 'b1_v'..'b5_v', 'b1_p'..'b5_p']；期货增加 'position'(持仓量)；默认为 None 返回全部字段 (默认: None)  - skip: 是否过滤无成交量的 Tick 记录，默认为 True（自动跳过无成交的 Tick） (默认: True)  - df: 返回数据格式，默认为 True 返回 pandas.DataFrame；设为 False 返回 numpy.ndarray (默认: True)
返回字段:  - time: 时间  - current: 当前价  - high: 当日最高价  - low: 当日最低价  - volume: 累计成交量  - money: 累计成交额  - position: 持仓量（仅期货、期权可用）
示例: get_ticks('000001', count=3, end_dt='2024-01-31')"""
    try:
        kwargs = {
            "security": security,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "count": count,
            "skip": skip,
            "df": df,
        }
        if fields is not None:
            kwargs["fields"] = fields
        result = get_ticks(**kwargs)
        return str(result)
    except Exception as e:
        return f"调用get_ticks失败: {str(e)}"

@mcp.tool()
def stockdb_get_last_tick(security: str, count: int = 1, fields: list = None, skip: bool = True, df: bool = True) -> str:
    """【行情】最新 Tick 数据
时间范围: 上市至今（实时）更新频率: 盘中实时秒级更新
参数说明:  - security: 标的代码字符串，仅限单只（如 '000001' 或 'IF2403'），支持股票/基金/期权/期货/可转债/指数 (默认: 必填)  - count: 拉取的最新 Tick 条数（整数 > 0），默认为 1（获取最新单条 Tick 快照数据） (默认: 1)  - fields: 筛选字段列表（如 ['time', 'current', 'high', 'low', 'volume', 'money']）；默认为 None 返回全部可用字段 (默认: None)  - skip: 是否过滤无成交量的 Tick 记录，默认为 True（自动跳过无成交 Tick） (默认: True)  - df: 返回数据格式，默认为 True 返回 pandas.DataFrame；设为 False 返回 numpy.ndarray (默认: True)
返回字段:  - time: 时间  - current: 当前价  - high: 当日最高价  - low: 当日最低价  - volume: 累计成交量  - money: 累计成交额
示例: get_last_tick('000001')"""
    try:
        kwargs = {
            "security": security,
            "count": count,
            "skip": skip,
            "df": df,
        }
        if fields is not None:
            kwargs["fields"] = fields
        result = get_last_tick(**kwargs)
        return str(result)
    except Exception as e:
        return f"调用get_last_tick失败: {str(e)}"

@mcp.tool()
def stockdb_get_bars(security: list, count: int = 30, unit: str = '1d', fields: list = ('date', 'open', 'high', 'low', 'close'), include_now: bool = False, end_dt: str = None, fq_ref_date: str = None, df: bool = True, skip_paused: bool = True) -> str:
    """【行情】通用固定行情窗口,应该优先使用stockdb_get_data
时间范围: 2005年至今（股票、基金、指数可用）；2010年至今（指数可用）；2019年至今（债券/可转债可用）更新频率: 盘后15:00更新，24:00校对完成入库（股票可用）；盘后15点更新，24点入库（基金、指数、债券/可转债可用）；凌晨3:00更新（指数可用）
参数说明:  - security: 标的代码字符串（如 '000001'）或代码列表 List（如 ['000001', '000002']） (默认: 必填)  - count: 获取 bar 的个数（整数 > 0），表示获取指定时间范围内对应频率的 count 个 K线/Bar 数据 (默认: 30)  - unit: bar 的时间周期/频率，支持：'1m', '5m', '15m', '30m', '60m', '120m', '1d', '1w'(周), '1M'(月) (默认: '1d')  - fields: 获取字段列表/元组，可选：'date', 'open', 'close', 'high', 'low', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused' (默认: ('date', 'open', 'high', 'low', 'close'))  - include_now: 是否包含当前未完结的实时 bar（如 9:33 分调用 unit='5m' 时是否包含 9:30-9:33 临时数据），默认为 False (默认: False)  - end_dt: 查询截止时间，支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 字符串；默认为 None 表示取最新 (默认: None)  - fq_ref_date: 复权基准日期，格式如 'YYYY-MM-DD'；默认为 None 表示获取不复权数据 (默认: None)  - df: 返回数据格式，默认为 True（单标的返回 DataFrame，多标的返回 MultiIndex DataFrame）；设为 False（单标的返回 np.ndarray，多标的返回字典 key=code, value=np.ndarray） (默认: True)  - skip_paused: 是否跳过停牌日的 bar 数据，默认为 True（自动过滤停牌日） (默认: True)
返回字段:  - date: 日期  - open: 时间段开始时价格  - close: 时间段结束时价格  - low: 时间段中的最低价  - high: 时间段中的最高价  - volume: 时间段中的成交的标的数量  - money: 时间段中的成交的金额  - factor: 复权因子  - open_interest: 持仓量  - paused: 是否停牌  - high_limit: 当天涨停价  - low_limit: 当天跌停价  - avg: 当天均价  - pre_close: 前收价
示例: get_bars('000001', 3, unit='1d', fields=['date', 'open', 'close'], end_dt='2024-12-31')"""
    try:
        sec_param = security
        if isinstance(security, list):
            sec_param = security[0] if len(security) > 0 else '000001'
        # 不传底层不支持的 skip_paused 参数
        result = get_bars(security=sec_param, count=count, unit=unit, fields=fields, include_now=include_now, end_dt=end_dt, fq_ref_date=fq_ref_date, df=df)
        return str(result)
    except Exception as e:
        return f"调用get_bars失败: {str(e)}"

@mcp.tool()
def stockdb_get_price(security: list, start_date: str = None, end_date: str = None, frequency: str = 'daily', fields: list = None, skip_paused: bool = False, fq: str = 'pre', count: int = None, panel: bool = False, fill_paused: str = True, round: bool = True) -> str:
    """【行情】通用移动行情窗口，应该优先使用stockdb_get_data
时间范围: 2005年至今（股票、期货、基金、指数可用）；2010年至今（指数可用）；2019年至今（债券/可转债可用）更新频率: 盘后15:00更新，24:00校对完成入库（股票可用）；夜盘交易数据凌晨2点30后更新,日盘交易数据盘后15点更新；24点入库（期货可用）；盘后15点更新，24点入库（基金、指数、债券/可转债可用）；凌晨3:00更新（指数可用）
参数说明:  - security: 单支标的代码字符串（如 '000001'）或标的代码列表 List（如 ['000001', '600000']） (默认: 必填)  - start_date: 开始时间，与 count 二选一。支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 格式字符串；默认为 None (默认: None)  - end_date: 结束时间，支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 格式；默认为 None 表示获取至最新 (默认: None)  - frequency: 时间周期/频率，支持：'daily'/'1d'(日频), 'minute'/'1m'(1分钟), 或任意 'Xd'(X天), 'Xm'(X分钟)如 '5m','15m','60m','3d' (默认: 'daily')  - fields: 获取字段列表，为 None 时默认 ['open','close','high','low','volume','money']；可选字段包含 'factor','high_limit','low_limit','avg','pre_close','paused','open_interest' (默认: None)  - skip_paused: 是否跳过停牌/未上市日期，默认为 False（不跳过，停牌日默认按上一交易日收盘价填充；设为 True 则直接跳过/过滤） (默认: False)  - fq: 复权选项，可选：'pre'(前复权，默认), 'post'(后复权), None(不复权/真实价格) (默认: 'pre')  - count: 获取的周期数据条数（整数 > 0），与 start_date 二选一。表示取 end_date 之前的 count 个 frequency 数据 (默认: None)  - panel: 多标的时是否返回 Panel 对象，默认为 True（在新版 pandas 下会自动转为包含 code、time 列的 DataFrame） (默认: True)  - fill_paused: 停牌数据填充模式，默认为 True（用停牌前收盘价填充）；设为 False（使用 NaN 填充停牌数据） (默认: True)  - round: 复权价格是否四舍五入保留固定位数（股票保留2位小数，基金保留3位小数），默认为 True (默认: True)
返回字段:  - None: 表示['open', 'close', 'high', 'low', 'volume', 'money']这几个标准字段  - open: 时间段开始时价格  - close: 时间段结束时价格  - low: 时间段中的最低价  - high: 时间段中的最高价  - volume: 时间段中的成交的标的数量  - money: 时间段中的成交的金额  - factor: 复权因子  - high_limit: 指定交易日的当日涨停价  - low_limit: 指定交易日的当日跌停价  - avg: 时间段中的平均价  - pre_close: 前一个单位时间结束时的价格,按天则是前一天的收盘价  - paused: bool值,股票是否停牌;  - open_interest: 持仓量
示例: get_price('000001', count=3, end_date='2024-12-31', fields=['open', 'close', 'volume'])"""
    try:
        result = get_price(security=security, start_date=start_date, end_date=end_date, frequency=frequency, fields=fields, skip_paused=skip_paused, fq=fq, count=count, panel=panel, fill_paused=fill_paused, round=round)
        return str(result)
    except Exception as e:
        return f"调用get_price失败: {str(e)}"

@mcp.tool()
def stockdb_get_marginsec_stocks(date: str = None) -> str:
    """【行情】通用融券标的列表
时间范围: 2010年至今更新频率: 每天21:00更新下一交易日（股票可用）；下一个交易日9点之前更新（基金可用）
参数说明:  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取上交所、深交所最新披露的可融券股票及基金标的代码列表 (默认: None)
    返回字段:
示例: get_marginsec_stocks(date='2024-01-31')"""
    try:
        result = get_marginsec_stocks(**({"date": date} if date is not None else {}))
        return _format_result(result)
    except Exception as e:
        return f"调用get_marginsec_stocks失败: {str(e)}"

@mcp.tool()
def stockdb_get_margincash_stocks(date: str = None) -> str:
    """【行情】通用融资标的列表
时间范围: 2010年至今更新频率: 每天21:00更新下一交易日（股票可用）；下一个交易日9点之前更新（基金可用）
参数说明:  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取上交所、深交所最新披露的可融资股票及基金标的代码列表 (默认: None)
    返回字段:
示例: get_margincash_stocks(date='2024-01-31')"""
    try:
        result = get_margincash_stocks(**({"date": date} if date is not None else {}))
        return _format_result(result)
    except Exception as e:
        return f"调用get_margincash_stocks失败: {str(e)}"

@mcp.tool()
def stockdb_get_mtss(security_list: list, start_date: str = None, end_date: str = None, fields: list = None, count: int = None) -> str:
    """【行情】通用融资融券信息
时间范围: 2010年至今更新频率: 下一个交易日9点之前更新
参数说明:  - security_list: 单只股票/基金代码字符串（如 '000001'）或代码列表 List（如 ['000001', '510300']） (默认: 必填)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取至最新交易日 (默认: None)  - fields: 筛选字段列表。可选字段：'fin_value'(融资余额), 'fin_buy_value'(融资买入额), 'fin_refund_value'(融资偿还额), 'sec_value'(融券余量), 'sec_sell_value'(融券卖出量), 'sec_refund_value'(融券偿还量), 'fin_sec_value'(融资融券余额)；默认为 None 返回全部字段 (默认: None)  - count: 获取的交易日天数（整数 > 0），与 start_date 二选一。表示取 end_date 之前的 count 个交易日两融数据 (默认: None)
返回字段:  - date: None  - sec_code: None  - fin_value: None  - fin_buy_value: None  - fin_refund_value: None  - sec_value: None  - sec_sell_value: None  - sec_refund_value: None  - fin_sec_value: None
示例: get_mtss('000001', '2024-01-02', '2024-01-05')"""
    try:
        result = get_mtss(security_list=security_list, start_date=start_date, end_date=end_date, fields=fields, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_mtss失败: {str(e)}"

@mcp.tool()
def stockdb_get_extras(info: str, security_list: list, start_date: str = None, end_date: str = None, df: bool = True, count: int = None) -> str:
    """【行情】通用补充数据
时间范围: 2005年至今（股票、期货可用）；上市至今（基金可用）更新频率: 盘前9:15更新（股票可用）；盘后17:00 更新（期货可用）；盘后17点到下一交易日9点（基金可用）
参数说明:  - info: 额外数据类型，支持五种枚举字符串：'is_st'(股票ST状态), 'acc_net_value'(基金累计净值), 'unit_net_value'(基金单位净值), 'futures_sett_price'(期货结算价), 'futures_positions'(期货持仓量) (默认: 必填)  - security_list: 单只标的代码字符串（如 '000001'）或代码列表 List（如 ['000001', '600000']） (默认: 必填)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取至最新 (默认: None)  - df: 返回格式，默认为 True（返回以日期为索引、标的代码为列名的 DataFrame）；设为 False（返回字典 key=标的代码, value=np.ndarray） (默认: True)  - count: 获取的数据天数（整数 > 0），与 start_date 二选一。表示取 end_date 之前的 count 个交易日数据 (默认: None)
返回字段:  - acc_net_value: 基金累计净值（基金可用）  - unit_net_value: 基金单位净值（基金可用）  - adj_net_value: 场外基金的复权净值（基金可用）
示例: get_extras('is_st', security_list=['000001'], start_date='2024-01-01', end_date='2024-01-05')"""
    try:
        result = get_extras(info=info, security_list=security_list, start_date=start_date, end_date=end_date, df=df, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_extras失败: {str(e)}"

@mcp.tool()
def stockdb_get_call_auction(security: list, start_date: str, end_date: str, fields: list = None) -> str:
    """【行情】通用集合竞价数据
时间范围: 2010年至今（股票可用）；上市至今（期权可用）；2019年至今（基金、债券/可转债可用）；2017年至今（指数可用）更新频率: 盘后15:00更新，24:00校对完成入库（股票、债券/可转债可用）；盘后15点更新（期权可用）；盘后15点更新，24点入库（基金可用）；盘后15点更新，24点入库（指数可用）
参数说明:  - security: 单只标的代码字符串（如 '000001'）或代码列表 List（如 ['000001', '600000']） (默认: 必填)  - start_date: 开始日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: 必填)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: 必填)  - fields: 筛选字段列表。包含 ['time', 'current', 'volume', 'money', 'b1_v'..'b5_v', 'b1_p'..'b5_p', 'a1_v'..'a5_v', 'a1_p'..'a5_p']；默认为 None 返回全部竞价字段 (默认: None)
返回字段:  - time: 时间  - current: 当前价（不复权）（股票、期权、基金可用）；当前价（债券/可转债可用）  - volume: 累计成交量（股）  - money: 累计成交额（元）  - a1_v ... a5_v: 一档至五档卖量 (a1_v, a2_v, a3_v, a4_v, a5_v)；float  - a1_p ... a5_p: 一档至五档卖价 (a1_p, a2_p, a3_p, a4_p, a5_p)；float  - b1_v ... b5_v: 一档至五档买量 (b1_v, b2_v, b3_v, b4_v, b5_v)；float  - b1_p ... b5_p: 一档至五档买价 (b1_p, b2_p, b3_p, b4_p, b5_p)；float
示例: get_call_auction('000001', '2024-01-02', '2024-01-05')"""
    try:
        result = get_call_auction(security=security, start_date=start_date, end_date=end_date, fields=fields)
        return str(result)
    except Exception as e:
        return f"调用get_call_auction失败: {str(e)}"

def _legacy_stockdb_get_concept_stocks(concept_code: str, date: str = None) -> str:
    """【板块】概念成分股
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - concept_code: 概念板块编码字符串（如 'SC0001'），可先通过 get_concepts() 获取全部概念编码 (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为今天 today (默认: None)
返回字段:
示例: get_concept_stocks('SC0001', date='2024-01-31')"""
    try:
        result = _bk_get(x=concept_code, category=0)
        return _format_result(_extract_bk_symbols(result, category_prefix="概念"))
    except Exception as e:
        return f"调用get_concept_stocks失败: {str(e)}"

def _legacy_stockdb_get_concept(security: str | list | None = None, date: str = None) -> str:
    """【板块】股票所属概念板块
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - security: 单只标的代码字符串（如 '000001'）或标的代码列表 List (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象（忽略日内时间） (默认: None)
返回字段:
示例: get_concepts()"""
    try:
        if security is None:
            result = _bk_get(category=0, fields="name,code")
        else:
            result = _bk_get(x=security, category=0)
        return _format_result(result)
    except Exception as e:
        return f"调用get_concept失败: {str(e)}"

@mcp.tool()
def stockdb_get_billboard_list(stock_list: list = None, start_date: str = None, end_date: str = None, count: int = None) -> str:
    """【板块】股票龙虎榜数据
时间范围: 2005年至今更新频率: 盘后20:00和22:00更新
参数说明:  - stock_list: 单只股票代码字符串或股票代码列表 List（如 ['000001']）；为 None 时表示获取指定日期范围内的所有龙虎榜股票 (默认: None)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None (默认: None)  - count: 获取的交易日天数（整数 > 0），配合 end_date 表示获取 end_date 前 count 个交易日的龙虎榜数据 (默认: None)
返回字段:
示例: get_billboard_list(stock_list=['000001'], start_date='2024-01-01', end_date='2024-01-31')"""
    try:
        result = get_billboard_list(stock_list=stock_list, start_date=start_date, end_date=end_date, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_billboard_list失败: {str(e)}"

def _legacy_stockdb_get_industries(name: str = 'zjw', date: str = None) -> str:
    """【板块】行业列表
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - name: 行业分类标准名称，支持：'sw_l1'(申万一级), 'sw_l2'(申万二级), 'sw_l3'(申万三级), 'zjw'(证监会分类，默认) (默认: 'zjw')  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取最新行业代码分类 (默认: None)
返回字段:
示例: get_industries(name='zjw')"""
    try:
        key = str(name).strip()
        if key.lower() in {"zjw", "证监会", "csrc"}:
            return "bk.get 仅覆盖概念和申万行业板块，zjw 不在 bk.get 范围内；如需证监会行业，请继续使用 get_industry 在线行业接口。"
        result = _bk_get(category=key, fields="name,code")
        return _format_result(result)
    except Exception as e:
        return f"调用get_industries失败: {str(e)}"

def _legacy_stockdb_get_industry_stocks(industry_code: str, date: str = None) -> str:
    """【板块】行业成份股
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - industry_code: 行业分类编码字符串（如 '801780' 或 'HY007'），可先通过 get_industries() 获取行业编码 (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为今天 today (默认: None)
返回字段:
示例: get_industry_stocks('HY001', date='2024-01-31')"""
    try:
        result = _bk_get(x=industry_code)
        return _format_result(_extract_bk_symbols(result, category_prefix="申万"))
    except Exception as e:
        return f"调用get_industry_stocks失败: {str(e)}"

@mcp.tool()
def stockdb_bk_get(x: str | list | None = None, category: int | str | None = None, fields: str | None = None) -> str:
    """【板块】统一板块查询接口
对齐合并接口.js 的 bk.get() 用法。
示例:
- stockdb_bk_get(fields='name,code')
- stockdb_bk_get(category='概念', fields='name,code')
- stockdb_bk_get(x='600633', category=1)
- stockdb_bk_get(x=['600633','000007'], category=1, fields='name')
- stockdb_bk_get(x='5G', category=0, fields='symbols')
- stockdb_bk_get(x='交通运输', category=1, fields='symbols')
- stockdb_bk_get(x='801170.SL')
category 支持 0/1/2/3 或 概念/申万一级/申万二级/申万三级 / sw_l1 / sw_l2 / sw_l3。"""
    try:
        result = _bk_get(x=x, category=category, fields=fields)
        return _format_result(result)
    except Exception as e:
        return f"调用bk.get失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_valuation_legacy(
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】估值数据表
时间范围: 2005年至今更新频率: 交易日24:00更新
参数说明:  - filters: 结构化过滤条件列表，如 [{"field":"code","op":"==","value":"000001.XSHE"}] (默认: None)  - fields: 字段名列表 (默认: None)  - order_by: 排序条件列表 (默认: None)  - limit: 最大返回条数 (默认: None)  - date: 查询日期，与 statDate 二选一 (默认: None)  - statDate: 财报统计财期，如 '2024q4' (默认: None)
返回字段:  - code: 字段数据类型：VARCHAR(12)  - pcf_ratio: 字段数据类型：FLOAT  - pubDate: 字段数据类型：DATE  - pe_ratio: 字段数据类型：FLOAT  - day: 字段数据类型：DATE  - circulating_cap: 字段数据类型：FLOAT  - pe_ratio_lyr: 字段数据类型：FLOAT  - pb_ratio: 字段数据类型：FLOAT  - ps_ratio: 字段数据类型：FLOAT  - capitalization: 字段数据类型：FLOAT  - turnover_ratio: 字段数据类型：FLOAT  - id: 字段数据类型：INTEGER  - market_cap: 字段数据类型：FLOAT  - circulating_market_cap: 字段数据类型：FLOAT
示例: filters=[{"field":"code","op":"==","value":"000001.XSHE"}], date='2024-12-31'"""
    try:
        result = _call_fundamentals_from_spec(
            "valuation", filters, fields, order_by, limit, date, statDate
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_income_legacy(
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】利润表
时间范围: 2005年至今更新频率: 交易日24:00更新
参数说明:  - filters: 结构化过滤条件列表，如 [{"field":"code","op":"==","value":"000001.XSHE"}] (默认: None)  - fields: 字段名列表 (默认: None)  - order_by: 排序条件列表 (默认: None)  - limit: 最大返回条数 (默认: None)  - date: 查询日期，与 statDate 二选一 (默认: None)  - statDate: 财报统计财期，如 '2024q4' (默认: None)
返回字段:  - exchange_income: 字段数据类型：FLOAT  - operating_cost: 字段数据类型：FLOAT  - code: 字段数据类型：VARCHAR(12)  - administration_expense: 字段数据类型：FLOAT  - interest_income_fin: 字段数据类型：FLOAT  - discon_operate_net_profit: 字段数据类型：FLOAT  - ci_minority_owners: 字段数据类型：FLOAT  - rd_expenses: 字段数据类型：FLOAT  - operating_tax_surcharges: 字段数据类型：FLOAT  - invest_income_associates: 字段数据类型：FLOAT  - non_operating_revenue: 字段数据类型：FLOAT  - other_composite_income_mino_at: 字段数据类型：FLOAT  - id: 字段数据类型：INTEGER  - diluted_eps: 字段数据类型：FLOAT  - total_composite_income: 字段数据类型：FLOAT  - pubDate: 字段数据类型：DATE  - asset_deal_income: 字段数据类型：FLOAT  - commission_income: 字段数据类型：FLOAT  - total_operating_revenue: 字段数据类型：FLOAT  - net_pay_insurance_claims: 字段数据类型：FLOAT  - net_open_hedge_income: 字段数据类型：FLOAT  - credit_impairment_loss: 字段数据类型：FLOAT  - sust_operate_net_profit: 字段数据类型：FLOAT  - sale_expense: 字段数据类型：FLOAT  - interest_cost_fin: 字段数据类型：FLOAT  - disposal_loss_non_current_liability: 字段数据类型：FLOAT  - operating_revenue: 字段数据类型：FLOAT  - statDate: 字段数据类型：DATE  - policy_dividend_payout: 字段数据类型：FLOAT  - non_operating_expense: 字段数据类型：FLOAT  - other_composite_income: 字段数据类型：FLOAT  - net_profit: 字段数据类型：FLOAT  - premiums_earned: 字段数据类型：FLOAT  - operating_profit: 字段数据类型：FLOAT  - interest_expense: 字段数据类型：FLOAT  - interest_income: 字段数据类型：FLOAT  - income_tax_expense: 字段数据类型：FLOAT  - other_earnings: 字段数据类型：FLOAT  - np_parent_company_owners: 字段数据类型：FLOAT  - basic_eps: 字段数据类型：FLOAT  - day: 字段数据类型：DATE  - total_profit: 字段数据类型：FLOAT  - commission_expense: 字段数据类型：FLOAT  - investment_income: 字段数据类型：FLOAT  - refunded_premiums: 字段数据类型：FLOAT  - asset_impairment_loss: 字段数据类型：FLOAT  - minority_profit: 字段数据类型：FLOAT  - fair_value_variable_income: 字段数据类型：FLOAT  - withdraw_insurance_contract_reserve: 字段数据类型：FLOAT  - reinsurance_cost: 字段数据类型：FLOAT  - financial_expense: 字段数据类型：FLOAT  - ci_parent_company_owners: 字段数据类型：FLOAT  - total_operating_cost: 字段数据类型：FLOAT
示例: filters=[{"field":"code","op":"==","value":"000001.XSHE"}], statDate='2024q4'"""
    try:
        result = _call_fundamentals_from_spec(
            "income", filters, fields, order_by, limit, date, statDate
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_continuously(
    table: str,
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    end_date: str = None,
    count: int = 1,
    panel: bool = False,
) -> str:
    """【财务】安全版 get_fundamentals_continuously
table 仅支持 valuation / income / cash_flow / indicator / balance。
filters 示例: [{"field":"code","op":"==","value":"000001.XSHE"}]"""
    try:
        query_obj, _ = _build_query(
            table=table,
            fields=fields,
            filters=filters,
            order_by=order_by,
            limit=limit,
            allowed_roots=FUNDAMENTAL_ALLOWED_ROOTS,
        )
        kwargs = {"count": count, "panel": panel}
        if end_date is not None:
            kwargs["end_date"] = end_date
        result = get_fundamentals_continuously(query_obj, **kwargs)
        if _is_sdk_error(result) and count == 1:
            # The local SDK currently rejects this endpoint, but its one-period
            # result is equivalent to the already working fundamentals call.
            result = _call_fundamentals_from_spec(
                table,
                filters,
                fields,
                order_by,
                limit or 1,
                end_date,
                None,
            )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals_continuously失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_generic_legacy(
    table: str,
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】查询财务数据(附综合案例)
时间范围: 2005年至今更新频率: 每天24:00更新（"24:00"是指夜间持续更新, 一般最后一个更新时间为次日盘前 09:00）
参数说明:  - table: 财务表名，限 valuation/income/cash_flow/indicator/balance (必填)  - filters: 结构化过滤条件列表 (默认: None)  - fields: 字段名列表 (默认: None)  - order_by: 排序条件列表 (默认: None)  - limit: 最大返回条数 (默认: None)  - date: 查询日期，与 statDate 二选一 (默认: None)  - statDate: 财报统计财期 (默认: None)
返回字段:
示例: table='valuation', filters=[{"field":"code","op":"==","value":"000001.XSHE"}], date='2024-12-31'"""
    try:
        result = _call_fundamentals_from_spec(
            table, filters, fields, order_by, limit, date, statDate
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_cash_flow_legacy(
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】现金流量表
时间范围: 2005年至今更新频率: 交易日24:00更新
参数说明:  - filters: 结构化过滤条件列表，如 [{"field":"code","op":"==","value":"000001.XSHE"}] (默认: None)  - fields: 字段名列表 (默认: None)  - order_by: 排序条件列表 (默认: None)  - limit: 最大返回条数 (默认: None)  - date: 查询日期，与 statDate 二选一 (默认: None)  - statDate: 财报统计财期，如 '2024q4' (默认: None)
返回字段:  - borrowing_repayment: 字段数据类型：FLOAT  - code: 字段数据类型：VARCHAR(12)  - net_original_insurance_cash: 字段数据类型：FLOAT  - subtotal_finance_cash_inflow: 字段数据类型：FLOAT  - invest_proceeds: 字段数据类型：FLOAT  - net_invest_cash_flow: 字段数据类型：FLOAT  - subtotal_invest_cash_inflow: 字段数据类型：FLOAT  - subtotal_operate_cash_inflow: 字段数据类型：FLOAT  - other_cash_to_invest_act: 字段数据类型：FLOAT  - cash_from_mino_s_invest_sub: 字段数据类型：FLOAT  - subtotal_invest_cash_outflow: 字段数据类型：FLOAT  - impawned_loan_net_increase: 字段数据类型：FLOAT  - net_buyback: 字段数据类型：FLOAT  - net_increase_in_placements: 字段数据类型：FLOAT  - staff_behalf_paid: 字段数据类型：FLOAT  - goods_and_services_cash_paid: 字段数据类型：FLOAT  - id: 字段数据类型：INTEGER  - net_cash_from_sub_company: 字段数据类型：FLOAT  - net_deposit_in_cb_and_ib: 字段数据类型：FLOAT  - net_deposit_increase: 字段数据类型：FLOAT  - handling_charges_and_commission: 字段数据类型：FLOAT  - pubDate: 字段数据类型：DATE  - cash_and_equivalents_at_end: 字段数据类型：FLOAT  - fix_intan_other_asset_dispo_cash: 字段数据类型：FLOAT  - invest_withdrawal_cash: 字段数据类型：FLOAT  - policy_dividend_cash_paid: 字段数据类型：FLOAT  - other_finance_act_payment: 字段数据类型：FLOAT  - net_operate_cash_flow: 字段数据类型：FLOAT  - proceeds_from_sub_to_mino_s: 字段数据类型：FLOAT  - goods_sale_and_service_render_cash: 字段数据类型：FLOAT  - statDate: 字段数据类型：DATE  - tax_payments: 字段数据类型：FLOAT  - other_finance_act_cash: 字段数据类型：FLOAT  - net_cash_deal_subcompany: 字段数据类型：FLOAT  - net_insurer_deposit_investment: 字段数据类型：FLOAT  - cash_from_invest: 字段数据类型：FLOAT  - dividend_interest_payment: 字段数据类型：FLOAT  - net_borrowing_from_central_bank: 字段数据类型：FLOAT  - cash_from_bonds_issue: 字段数据类型：FLOAT  - net_cash_received_from_reinsurance_business: 字段数据类型：FLOAT  - other_cashin_related_operate: 字段数据类型：FLOAT  - subtotal_operate_cash_outflow: 字段数据类型：FLOAT  - net_loan_and_advance_increase: 字段数据类型：FLOAT  - other_cash_from_invest_act: 字段数据类型：FLOAT  - net_borrowing_from_finance_co: 字段数据类型：FLOAT  - net_deal_trading_assets: 字段数据类型：FLOAT  - day: 字段数据类型：DATE  - tax_levy_refund: 字段数据类型：FLOAT  - invest_cash_paid: 字段数据类型：FLOAT  - other_operate_cash_paid: 字段数据类型：FLOAT  - fix_intan_other_asset_acqui_cash: 字段数据类型：FLOAT  - original_compensation_paid: 字段数据类型：FLOAT  - cash_equivalent_increase: 字段数据类型：FLOAT  - net_finance_cash_flow: 字段数据类型：FLOAT  - cash_from_borrowing: 字段数据类型：FLOAT  - cash_equivalents_at_beginning: 字段数据类型：FLOAT  - exchange_rate_change_effect: 字段数据类型：FLOAT  - subtotal_finance_cash_outflow: 字段数据类型：FLOAT  - interest_and_commission_cashin: 字段数据类型：FLOAT
示例: filters=[{"field":"code","op":"==","value":"000001.XSHE"}], statDate='2024q4'"""
    try:
        result = _call_fundamentals_from_spec(
            "cash_flow", filters, fields, order_by, limit, date, statDate
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_history_fundamentals(security: str | list, fields: list, watch_date: str = None, stat_date: str = None, count: int = 1, interval: str = '1q', stat_by_year: bool = False) -> str:
    """【财务】获取多个季度/年度的财务数据
时间范围: 2005年至今更新频率: 每天24:00更新
参数说明:  - security: 单只股票代码字符串（如 '000001'）或代码列表 List (默认: 必填)  - fields: 财务报表字段列表，如 [balance.cash_equivalents, income.total_operating_revenue, indicator.eps] (默认: 必填)  - watch_date: 观察日期，与 stat_date 二选一。格式如 'YYYY-MM-DD'，获取该日期前（含）已发布的最新报表 (默认: None)  - stat_date: 统计财期，与 watch_date 二选一。格式如 '2024q1'/'2023q4'（季度）或 '2023'（年度） (默认: None)  - count: 获取的历史报告期数量（整数 > 0），默认为 1 (默认: 1)  - interval: 报告期间隔，可选：'1q'(逐季间隔，默认) 或 '1y'(逐年间隔) (默认: '1q')  - stat_by_year: 是否获取年报数据，默认为 False（返回季度数据）；设为 True 表示获取年报且 interval 必须为 '1y' (默认: False)
    返回字段:
示例: get_fundamentals(query(income).filter(income.code == '000001'), {'statDate': '2024q4'})"""
    try:
        if count != 1:
            return "当前 stock_sdk 未公开 get_history_fundamentals；MCP 仅支持用 get_fundamentals 兼容单个报告期。"

        field_paths = [_validate_path(item, "fields") for item in _ensure_list(fields, "fields")]
        parts = [item.split(".") for item in field_paths]
        roots = {item[0] for item in parts if len(item) == 2}
        if len(roots) != 1 or any(len(item) != 2 or item[0] not in FUNDAMENTAL_ALLOWED_ROOTS for item in parts):
            raise ValueError("单报告期兼容调用要求 fields 属于同一个 table，并使用 table.field 格式。")

        table = next(iter(roots))
        security_list = security if isinstance(security, list) else [security]
        filters = [{"field": "code", "op": "in", "value": security_list}]
        field_names = [item[1] for item in parts]
        result = _call_fundamentals_from_spec(
            table,
            filters=filters,
            fields=field_names,
            date=watch_date,
            stat_date=stat_date,
            limit=1,
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_history_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_valuation(security_list: list, start_date: str = None, end_date: str = None, fields: list = None, count: int = None) -> str:
    """【财务】获取多个标的在指定交易日范围内的市值表数据
时间范围: 2005年至今更新频率: 每天盘前(08:30)更新当日总股本及流通股本数据，便于用户盘中计算各类指标，其他字段置空； 每天盘后(16:30)更新全部指标
参数说明:  - security_list: 单只标的代码字符串（如 '000001'）或代码列表 List (默认: 必填)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串；默认为 None 表示获取至最新 (默认: None)  - fields: 市值表字段列表。可选包含：'pe_ratio'(PE), 'turnover_ratio'(换手率), 'pb_ratio'(PB), 'ps_ratio'(PS), 'pcf_ratio'(PCF), 'capitalization'(总股本), 'market_cap'(总市值), 'circulating_cap'(流通股本), 'circulating_market_cap'(流通市值) (默认: None)  - count: 查询交易日天数（整数 > 0），与 start_date 二选一。表示向前查询 count 个交易日的市值数据 (默认: None)
返回字段:
示例: get_fundamentals(query(valuation).filter(valuation.code == '000001'), {'date': '2024-12-31'})"""
    try:
        # 1. 底层校验约束: 必须指定 start_date 或 count 其中一个
        if start_date is None and count is None:
            count = 1

        # 2. 脏字段清理防护: 若未传 fields 或传了含有 pcf_ratio2 的默认列表，清洗为合规字段
        valid_default_fields = [
            'pe_ratio', 'pb_ratio', 'ps_ratio', 'pcf_ratio',
            'capitalization', 'market_cap', 'circulating_cap', 'circulating_market_cap'
        ]
        if fields is None:
            fields = valid_default_fields
        elif isinstance(fields, (list, tuple)):
            fields = [f for f in fields if f != 'pcf_ratio2']

        sec_list = security_list if isinstance(security_list, list) else [security_list]
        kwargs = {"security_list": sec_list, "fields": fields, "count": count}
        if start_date is not None:
            kwargs["start_date"] = start_date
        if end_date is not None:
            kwargs["end_date"] = end_date
        result = get_valuation(**kwargs)
        if _is_sdk_error(result) and count == 1 and start_date is None and end_date is not None:
            # Some SDK deployments reject get_valuation's one-day request even
            # though the same valuation row is available through fundamentals.
            result = _call_fundamentals_from_spec(
                "valuation",
                filters=[{"field": "code", "op": "in", "value": sec_list}],
                fields=fields,
                date=end_date,
                limit=1,
            )
        return _format_result(result)
    except Exception as e:
        return f"调用get_valuation失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals_indicator_legacy(
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】财务指标表
时间范围: 2005年至今更新频率: 交易日24:00更新
参数说明:  - filters: 结构化过滤条件列表，如 [{"field":"code","op":"==","value":"000001.XSHE"}] (默认: None)  - fields: 字段名列表 (默认: None)  - order_by: 排序条件列表 (默认: None)  - limit: 最大返回条数 (默认: None)  - date: 查询日期，与 statDate 二选一 (默认: None)  - statDate: 财报统计财期，如 '2024q4' (默认: None)
返回字段:  - operating_expense_to_total_revenue: 字段数据类型：FLOAT  - ga_expense_to_total_revenue: 字段数据类型：FLOAT  - code: 字段数据类型：VARCHAR(12)  - inc_net_profit_annual: 字段数据类型：FLOAT  - net_profit_margin: 字段数据类型：FLOAT  - inc_total_revenue_year_on_year: 字段数据类型：FLOAT  - roa: 字段数据类型：FLOAT  - inc_net_profit_to_shareholders_annual: 字段数据类型：FLOAT  - roe: 字段数据类型：FLOAT  - operating_profit_to_profit: 字段数据类型：FLOAT  - inc_return: 字段数据类型：FLOAT  - inc_net_profit_to_shareholders_year_on_year: 字段数据类型：FLOAT  - id: 字段数据类型：INTEGER  - net_profit_to_total_revenue: 字段数据类型：FLOAT  - ocf_to_operating_profit: 字段数据类型：FLOAT  - pubDate: 字段数据类型：DATE  - inc_revenue_year_on_year: 字段数据类型：FLOAT  - inc_net_profit_year_on_year: 字段数据类型：FLOAT  - gross_profit_margin: 字段数据类型：FLOAT  - expense_to_total_revenue: 字段数据类型：FLOAT  - inc_operation_profit_annual: 字段数据类型：FLOAT  - goods_sale_and_service_to_revenue: 字段数据类型：FLOAT  - inc_operation_profit_year_on_year: 字段数据类型：FLOAT  - statDate: 字段数据类型：DATE  - operating_profit: 字段数据类型：FLOAT  - operation_profit_to_total_revenue: 字段数据类型：FLOAT  - adjusted_profit_to_profit: 字段数据类型：FLOAT  - day: 字段数据类型：DATE  - ocf_to_revenue: 字段数据类型：FLOAT  - value_change_profit: 字段数据类型：FLOAT  - financing_expense_to_total_revenue: 字段数据类型：FLOAT  - inc_total_revenue_annual: 字段数据类型：FLOAT  - eps: 字段数据类型：FLOAT  - inc_revenue_annual: 字段数据类型：FLOAT  - invesment_profit_to_profit: 字段数据类型：FLOAT  - adjusted_profit: 字段数据类型：FLOAT
示例: filters=[{"field":"code","op":"==","value":"000001.XSHE"}], date='2024-12-31'"""
    try:
        result = _call_fundamentals_from_spec(
            "indicator", filters, fields, order_by, limit, date, statDate
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_fundamentals(
    table: str,
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = None,
    date: str = None,
    statDate: str = None,
) -> str:
    """【财务】安全版 get_fundamentals
table 仅支持 valuation / income / cash_flow / indicator / balance。
filters 示例: [{"field":"code","op":"==","value":"000001.XSHE"}]
    fields 示例: ["code","statDate","net_operate_cash_flow"]"""
    try:
        result = _call_fundamentals_from_spec(
            table,
            filters,
            fields,
            order_by,
            limit,
            date,
            statDate,
        )
        return _format_result(result)
    except Exception as e:
        return f"调用get_fundamentals失败: {str(e)}"

@mcp.tool()
def stockdb_get_locked_shares(stock_list: list = None, start_date: str = None, end_date: str = None, forward_count: int = None) -> str:
    """【财务】限售解禁股
时间范围: 2005年至今更新频率: 交易日24:00更新
参数说明:  - stock_list: 单只股票代码或股票代码列表 List（如 ['000001']） (默认: None)  - start_date: 开始日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: None)  - forward_count: 交易日数量（整数 > 0），配合 start_date 表示获取 start_date 到未来 forward_count 个交易日的数据 (默认: None)
返回字段:  - day: 解禁日期；str (YYYY-MM-DD)  - code: 股票代码；str  - num: 解禁股数；float  - rate1: 解禁股数/总股本比例；float  - rate2: 解禁股数/总流通股本比例；float
示例: get_locked_shares(stock_list=['000001'], start_date='2024-01-01', end_date='2024-12-31')"""
    try:
        result = get_locked_shares(stock_list=stock_list, start_date=start_date, end_date=end_date, forward_count=forward_count)
        return str(result)
    except Exception as e:
        return f"调用get_locked_shares失败: {str(e)}"

@mcp.tool()
def stockdb_get_future_contracts(underlying_symbol: str, date: str = None) -> str:
    """【期货】指定日期的期货列表数据
时间范围: 2005年至今更新频率: 8:00更新
参数说明:  - underlying_symbol: 期货品种大写代码字符串（如 'AG'(白银), 'AU'(黄金), 'IF'(沪深300股指), 'RB'(螺纹钢)） (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取最新交易日的可交易合约标的列表 (默认: None)
    返回字段:
示例: get_future_contracts('IF', date='2024-01-31')"""
    try:
        kwargs = {"underlying_symbol": underlying_symbol}
        if date is not None:
            kwargs["date"] = date
        result = get_future_contracts(**kwargs)
        if _is_sdk_error(result):
            result = _future_contract_names(underlying_symbol, date)
        return _format_result(result)
    except Exception as e:
        return f"调用get_future_contracts失败: {str(e)}"

@mcp.tool()
def stockdb_get_dominant_future(underlying_symbol: str = None, date: str = None, end_date: str = None) -> str:
    """【期货】期货主力合约
时间范围: 2005年至今更新频率: 19点更新下一交易日
参数说明:  - underlying_symbol: 期货品种大写代码字符串（如 'AG', 'IF', 'RB'）；默认为 None (默认: None)  - date: 指定日期/时间，支持 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS' 字符串（注：夜盘 19:00 之后记为下一个交易日）；默认为当前时间 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式；未指定时返回单个主力合约字符串，指定时返回指定日期区间的主力合约时间序列 Series (默认: None)
    返回字段:
示例: get_dominant_future('IF', date='2024-01-31')"""
    try:
        kwargs = {}
        if underlying_symbol is not None:
            kwargs["underlying_symbol"] = underlying_symbol
        if date is not None:
            kwargs["date"] = date
        if end_date is not None:
            kwargs["end_date"] = end_date
        result = get_dominant_future(**kwargs)
        if _is_sdk_error(result) and end_date is None and underlying_symbol is not None:
            query_date = date or datetime.date.today().strftime("%Y-%m-%d")
            contracts = _future_contract_names(underlying_symbol, query_date)[:12]
            ranked = []
            for contract in contracts:
                bars = get_bars(
                    contract,
                    count=1,
                    unit="1d",
                    fields=["open_interest"],
                    end_dt=query_date,
                    df=False,
                )
                if isinstance(bars, list) and bars and isinstance(bars[0], dict):
                    ranked.append((bars[0].get("open_interest") or 0, contract))
            if ranked:
                result = max(ranked)[1]
        return _format_result(result)
    except Exception as e:
        return f"调用get_dominant_future失败: {str(e)}"

@mcp.tool()
def stockdb_get_index_weights(index_id: str, date: str = None) -> str:
    """【指数】指数成分股权重(月度)
时间范围: 2005年至今更新频率: 每天8点检查更新；注意该数据是月度的，中证指数公司一般只在月末/月初披露
参数说明:  - index_id: 指数标准代码字符串（如 '000300' 或 '000001'） (默认: 必填)  - date: 查询权重的日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为 None 表示获取最新月度权重数据 (默认: None)
返回字段:
示例: get_index_weights('000300.XSHG', date='2024-01-31')"""
    try:
        idx = index_id
        if '.' not in idx and len(idx) == 6:
            if idx.startswith('000') or idx.startswith('93'):
                idx = idx + '.XSHG'
            elif idx.startswith('399'):
                idx = idx + '.XSHE'
        result = get_index_weights(index_id=idx, date=date)
        return str(result)
    except Exception as e:
        return f"调用get_index_weights失败: {str(e)}"

@mcp.tool()
def stockdb_get_index_stocks(index_symbol: str, date: str = None) -> str:
    """【指数】获取指数成分股
时间范围: 2005年至今（指数可用）；2010年至今（指数可用）更新频率: 每天8点检查更新（指数可用）；每日8:00更新（指数可用）
参数说明:  - index_symbol: 指数代码字符串（如 '000300' 或 '000001'） (默认: 必填)  - date: 查询日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为今天 today (默认: None)
返回字段:
示例: get_index_stocks('000300', date='2024-01-31')"""
    try:
        idx = index_symbol
        if '.' not in idx and len(idx) == 6:
            if idx.startswith('000') or idx.startswith('93'):
                idx = idx + '.XSHG'
            elif idx.startswith('399'):
                idx = idx + '.XSHE'
        if date is None:
            date = datetime.date.today().strftime('%Y-%m-%d')
        result = get_index_stocks(index_symbol=idx, date=date)
        return str(result)
    except Exception as e:
        return f"调用get_index_stocks失败: {str(e)}"

@mcp.tool()
def stockdb_get_all_alpha_101(date: str, code: list = None, alpha: list = None) -> str:
    """【因子】Alpha 101 因子
时间范围: 2005至今更新频率: 次日08:00更新，动态复权
参数说明:  - date: 计算日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: 必填)  - code: 标的代码字符串（如 '000001'）或代码列表 List；默认为 None 表示全市场股票 (默认: None)  - alpha: 指定计算的 Alpha 因子名称列表（如 ['alpha_001', 'alpha_002']）；默认为 None 表示计算并返回全部因子 DataFrame (默认: None)
    返回字段:
示例: alpha('2019-01-22', ['000001', '000002'], 'pre', [1, 2])"""
    try:
        result = _factor_values_for_alpha(date, code, alpha, 101)
        return _format_result(result)
    except Exception as e:
        return f"调用get_all_alpha_101失败: {str(e)}"

@mcp.tool()
def stockdb_get_all_alpha_191(date: str, code: list = None, alpha: list = None) -> str:
    """【因子】Alpha 191 因子
时间范围: 2005至今更新频率: 次日08:00更新，动态复权
参数说明:  - date: 计算日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: 必填)  - code: 标的代码字符串（如 '000001'）或代码列表 List；默认为 None 表示全市场股票 (默认: None)  - alpha: 指定计算的 Alpha 因子名称列表（如 ['alpha_001', 'alpha_002']）；默认为 None 表示计算并返回全部因子 DataFrame (默认: None)
    返回字段:
示例: alpha('2019-01-22', ['000001', '000002'], 'pre', [1, 2])"""
    try:
        result = _factor_values_for_alpha(date, code, alpha, 191)
        return _format_result(result)
    except Exception as e:
        return f"调用get_all_alpha_191失败: {str(e)}"

@mcp.tool()
def stockdb_alpha(date: str, index: str | list, fq: str = 'pre', alpha: list = None) -> str:
    """【因子】Alpha因子计算
时间范围: 上市至今更新频率: 盘后更新
参数说明:  - date: 计算日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象 (默认: 必填)  - index: 股票池/基准指数代码字符串，如 '000300'(沪深300), '000905'(中证500) (默认: 必填)  - fq: 复权选项，可选：'pre'(前复权，默认), 'post'(后复权), None(不复权) (默认: pre)  - alpha: 指定计算的 alpha 因子编号列表（如 ['alpha_001']）；默认为 [] 表示计算全套 (默认: [])
    返回字段:
示例: alpha('2019-01-22', ['000001', '000002'], 'pre', [1, 2])"""
    try:
        result = None
        try:
            result = sdk.alpha(date=date, index=index, fq=fq, alpha=alpha or [])
        except Exception:
            result = None
        if _is_sdk_error(result) or result is None or (
            isinstance(result, str) and '"error"' in result
        ):
            result = _factor_values_for_alpha(date, index, alpha, 191)
        return _format_result(result)
    except Exception as e:
        return f"调用alpha失败: {str(e)}"

@mcp.tool()
def stockdb_get_factor_kanban_values(
    universe: str = None,
    bt_cycle: str = None,
    category: str = None,
    model: str = 'long_only',
    kwargs: dict = None,
) -> str:
    """【因子】因子看板列表数据
时间范围: 2005至今更新频率: 9:00更新前一交易日
参数说明:  - universe: 股票池范围，可选：'hs300'(沪深300), 'zz500'(中证500), 'zz800'(中证800), 'all'(全A股) (默认: None)  - bt_cycle: 回测统计周期，可选：'month'(月频), 'week'(周频), 'day'(日频) (默认: None)  - category: 因子分类维度，可选：'quality'(质量), 'valuation'(估值), 'momentum'(动量), 'growth'(成长), 'risk'(风险) (默认: None)  - model: 统计分析模型/评估函数名；默认为 None (默认: 'long_only')  - kwargs: 可选 JSON 对象，仅支持 skip_paused 和 commision_slippage (默认: None)
返回字段:  - date: 数据的更新日期（实际数据可获取时间比date晚一个交易日）  - universe: 股票池范围，如 'hs300', 'zz500'  - bt_cycle: 回测/测试周期  - skip_paused: 是否过滤涨停及停牌股  - commision_slippage: 手续费及滑点设置  - category: 因子分类名称  - code: 因子代码  - compound_return_1q: 一分位数累积收益 (long_only)  - compound_return_5q: 五分位数累积收益 (long_only)  - annualized_return_1q: 一分位数年化收益率 (long_only)  - annualized_return_5q: 五分位数年化收益率 (long_only)  - max_drawdown_1q: 一分位数最大回撤 (long_only)  - max_drawdown_5q: 五分位数最大回撤 (long_only)  - sharpe_1q: 一分位数夏普比率 (long_only)  - sharpe_5q: 五分位数夏普比率 (long_only)  - turnover_ratio_1q: 一分位数换手率 (long_only)  - turnover_ratio_5q: 五分位数换手率 (long_only)  - compound_return_ls: 多空组合累积收益 (long_short)  - annualized_return_ls: 多空组合年化收益率 (long_short)  - max_drawdown_ls: 多空组合最大回撤 (long_short)  - sharpe_ls: 多空组合夏普比率 (long_short)  - turnover_ratio_ls: 多空组合换手率 (long_short)  - annual_return_bm: 基准指数年化收益率  - ic_mean: IC均值  - ir: IR值（信息比率）  - good_ic: IC绝对值大于0.02的比率
示例: get_factor_kanban_values(universe='hs300', bt_cycle='year_1', category='style', model='long_only')"""
    try:
        options = kwargs or {}
        if not isinstance(options, dict):
            raise ValueError("kwargs 必须是 JSON 对象。")
        unsupported = set(options) - {"skip_paused", "commision_slippage"}
        if unsupported:
            raise ValueError(f"kwargs 包含不支持的参数: {', '.join(sorted(unsupported))}")

        call_kwargs = {"model": model}
        if universe is not None:
            call_kwargs["universe"] = universe
        if bt_cycle is not None:
            call_kwargs["bt_cycle"] = bt_cycle
        if category is not None:
            call_kwargs["category"] = category
        call_kwargs.update(options)
        result = get_factor_kanban_values(**call_kwargs)
        return _format_result(result)
    except Exception as e:
        return f"调用get_factor_kanban_values失败: {str(e)}"

@mcp.tool()
def stockdb_MACD(security_list: list, check_date: str, SHORT: int = 12, LONG: int = 26, MID: int = 9, unit: str = '1d', include_now: bool = True, fq_ref_date: str = None) -> str:
    """【因子】技术指标因子
时间范围: 上市至今更新频率: 盘后更新
参数说明:  - result = 函数名称(): 技术指标计算函数名称字符串，如 'MACD', 'RSI', 'KDJ', 'BOLL', 'MA' (默认: 必填)  - security_list: 单只股票代码字符串（如 '000001'）或代码列表 List (默认: 必填)  - check_date: 计算/检查日期，支持 'YYYY-MM-DD' 格式字符串或 date/datetime 对象；默认为最新日期 (默认: 必填)  - SHORT: 快线/短周期参数（整数 > 0），如 MACD 默认为 12 (默认: 12)  - LONG: 慢线/长周期参数（整数 > 0），如 MACD 默认为 26 (默认: 26)  - MID: 信号线/平滑周期参数（整数 > 0），如 MACD 默认为 9 (默认: 9)  - unit: K线时间周期/频率，支持：'1m','5m','15m','30m','60m','1d','1w','1M' (默认: 1d)  - include_now: 是否包含当前未完结的实时 bar，默认为 False (默认: True)  - fq_ref_date: 复权基准日期，默认为 None 表示不复权 (默认: None)
返回字段:
示例: MACD(security_list=['000001'], check_date='2024-01-31')"""
    try:
        result = MACD(security_list=security_list, check_date=check_date, SHORT=SHORT, LONG=LONG, MID=MID, unit=unit, include_now=include_now, fq_ref_date=fq_ref_date)
        return str(result)
    except Exception as e:
        return f"调用MACD失败: {str(e)}"

@mcp.tool()
def stockdb_get_factor_values(securities: list, factors: list, start_date: str = None, end_date: str = None, count: int = None) -> str:
    """【因子】因子值
时间范围: 2005年至今更新频率: 下一自然日5:00、8:00更新
参数说明:  - securities: 单只股票/标的代码字符串（如 '000001'）或代码列表 List (默认: 必填)  - factors: 因子名称字符串或列表，如['size', 'beta', 'momentum', 'residual_volatility', 'non_linear_size', 'book_to_price_ratio', 'liquidity', 'earnings_yield', 'growth', 'leverage'](默认: 必填)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式；默认为 None 表示获取至最新 (默认: None)  - count: 获取的交易日天数（整数 > 0），与 start_date 二选一 (默认: None)
返回字段:
示例: get_factor_values(securities=['000001'], factors=['size', 'beta'], start_date='2024-01-01', end_date='2024-01-05')"""
    try:
        result = get_factor_values(securities=securities, factors=factors, start_date=start_date, end_date=end_date, count=count)
        return _format_result(result)
    except Exception as e:
        return f"调用get_factor_values失败: {str(e)}"

@mcp.tool()
def stockdb_get_factor_values_legacy(securities: list, factors: list, start_date: str = None, end_date: str = None, count: int = None) -> str:
    """【因子】获取因子值
时间范围: 上市至今更新频率: 每日盘后更新
参数说明:  - securities: 单只股票/标的代码字符串（如 '000001'）或代码列表 List (默认: 必填)  - factors: 因子名称字符串或列表，如 ['pe_ratio', 'market_cap'] (默认: 必填)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式；默认为 None 表示获取至最新 (默认: None)  - count: 获取的交易日天数（整数 > 0），与 start_date 二选一 (默认: None)
返回字段:
示例: get_factor_values(securities=['000001'], factors=['size', 'beta'], start_date='2024-01-01', end_date='2024-01-05')"""
    try:
        result = get_factor_values(securities=securities, factors=factors, start_date=start_date, end_date=end_date, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_factor_values失败: {str(e)}"

@mcp.tool()
def stockdb_get_index_style_exposure(index: str, factors: list = None, start_date: str = None, end_date: str = None, count: int = None) -> str:
    """【因子】获取重点宽基指数风格暴露（新）
时间范围: 2011-08-31至今更新频率: 9:00更新前一交易日
参数说明:  - index: 宽基指数代码字符串，如 '000300.XSHG'(沪深300) 或 '000905.XSHG'(中证500) (默认: 必填)  - factors: 风格因子名称列表，如 ['size'(市值), 'beta'(贝塔), 'momentum'(动量), 'volatility'(波动率), 'value'(价值)] (默认: None)  - start_date: 开始日期，与 count 二选一。支持 'YYYY-MM-DD' 格式字符串 (默认: None)  - end_date: 结束日期，支持 'YYYY-MM-DD' 格式；默认为 None 表示获取至最新 (默认: None)  - count: 获取交易日天数（整数 > 0），与 start_date 二选一 (默认: None)
返回字段:
示例: get_index_style_exposure('932000.CSI', ['size'], '2024-01-01', '2024-01-05')"""
    try:
        idx=index
        if '.' not in idx and len(idx) == 6:
            if idx.startswith('000') or idx.startswith('93'):
                idx = idx + '.XSHG'
            elif idx.startswith('399'):
                idx = idx + '.XSHE'
        result = get_index_style_exposure(index=idx, factors=factors, start_date=start_date, end_date=end_date, count=count)
        return str(result)
    except Exception as e:
        return f"调用get_index_style_exposure失败: {str(e)}"

@mcp.tool()
def stockdb_list_query_tables(root: str = None) -> str:
    """列出 stockdb_run_query 可用的数据表
root 可选: bond / finance / opt
"""
    try:
        normalized_root = _normalize_query_root(root)
        usage = {
            "tool": "stockdb_run_query",
            "operators": ["==", "!=", ">", ">=", "<", "<=", "in", "like", "between"],
            "table_param": "使用返回结果中的完整表名，例如 finance.STK_AH_PRICE_COMP",
            "order_by_example": [{"field": "day", "direction": "desc"}],
            "filter_example": [{"field": "code", "op": "==", "value": "000001"}],
        }

        if normalized_root is not None:
            tables = list(RUN_QUERY_TABLES[normalized_root])
            result = {
                "root": normalized_root,
                "count": len(tables),
                "tables": tables,
                "usage": usage,
            }
        else:
            roots = sorted(RUN_QUERY_ALLOWED_ROOTS)
            result = {
                "roots": roots,
                "count": sum(len(RUN_QUERY_TABLES[item]) for item in roots),
                "counts": {item: len(RUN_QUERY_TABLES[item]) for item in roots},
                "tables_by_root": {item: list(RUN_QUERY_TABLES[item]) for item in roots},
                "usage": usage,
            }

        return _format_result(result)
    except Exception as e:
        return f"调用stockdb_list_query_tables失败: {str(e)}"

@mcp.tool()
def stockdb_run_query(
    table: str,
    filters: list = None,
    fields: list = None,
    order_by: list = None,
    limit: int = 100,
    offset: int = None,
) -> str:
    """【数据库】安全版 run_query
table 仅支持 bond / finance / opt 前缀。
filters 示例: [{"field":"a_code","op":"==","value":"000002"}]
order_by 示例: [{"field":"day","direction":"desc"}]"""
    try:
        query_obj, root_name = _build_query(
            table=table,
            fields=fields,
            filters=filters,
            order_by=order_by,
            limit=limit,
            offset=offset,
            allowed_roots=RUN_QUERY_ALLOWED_ROOTS,
        )
        namespace = SAFE_ROOTS[root_name]
        if not hasattr(namespace, "run_query"):
            raise ValueError(f"{root_name} 不支持 run_query")
        result = namespace.run_query(query_obj)
        return _format_result(result)
    except Exception as e:
        return f"调用run_query失败: {str(e)}"



if __name__ == "__main__":
    asyncio.run(mcp.run_stdio_async())
