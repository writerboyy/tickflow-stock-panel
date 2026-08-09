"""Declarative contracts for the audited Tushare Proxy data lake import."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


DatasetKind = Literal[
    "daily",
    "financial",
    "extension",
    "pit_index",
    "pit_industry",
    "reference",
    "corporate_action",
]
QueryScope = Literal["symbol", "index", "trade_date", "global"]


@dataclass(frozen=True, slots=True)
class TushareField:
    name: str
    dtype: Literal["string", "int", "float", "bool"] = "string"
    label: str = ""
    unit: str | None = None
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TushareDatasetSpec:
    api_name: str
    label: str
    group: str
    kind: DatasetKind
    scope: QueryScope
    primary_key: tuple[str, ...]
    logical_date: str
    fields: tuple[TushareField, ...]
    canonical_target: str | None = None
    revisioned: bool = False
    max_rows: int = 5_000
    factor_input: bool = False
    overlap_fields: tuple[str, ...] = ()
    empty_is_valid: bool = False

    @property
    def table_id(self) -> str:
        return f"ext_tushare_{self.api_name}"

    @property
    def units(self) -> dict[str, str]:
        return {field.name: field.unit for field in self.fields if field.unit}

    @property
    def normalized_primary_key(self) -> tuple[str, ...]:
        if self.revisioned:
            return (*self.primary_key, "source_revision_hash")
        return self.primary_key


def _f(
    name: str,
    dtype: Literal["string", "int", "float", "bool"] = "string",
    label: str = "",
    unit: str | None = None,
    *aliases: str,
) -> TushareField:
    return TushareField(name, dtype, label or name, unit, tuple(aliases))


_SYMBOL = _f("symbol", "string", "标的代码", None, "ts_code", "con_code")
_TRADE_DATE = _f("trade_date", "string", "交易日期")
_ANN_DATE = _f("ann_date", "string", "公告日期")
_END_DATE = _f("end_date", "string", "报告期")


DATASET_SPECS: dict[str, TushareDatasetSpec] = {
    "stock_basic": TushareDatasetSpec(
        "stock_basic", "A股标的与上市状态", "reference", "reference", "global",
        ("symbol",), "list_date",
        (
            _SYMBOL, _f("name", "string", "证券简称"),
            _f("exchange", "string", "交易所"), _f("market", "string", "市场板块"),
            _f("list_status", "string", "上市状态"),
            _f("list_date", "string", "上市日期"),
            _f("delist_date", "string", "退市日期"),
        ),
        canonical_target="instruments/instruments.parquet",
        max_rows=10_000,
    ),
    "etf_basic": TushareDatasetSpec(
        "etf_basic", "ETF标的", "reference", "reference", "global",
        ("symbol",), "list_date",
        (
            _SYMBOL, _f("name", "string", "基金简称"),
            _f("management", "string", "管理人"), _f("custodian", "string", "托管人"),
            _f("fund_type", "string", "基金类型"),
            _f("list_date", "string", "上市日期"),
            _f("delist_date", "string", "退市日期"),
        ),
        canonical_target="instruments_etf/instruments_etf.parquet",
    ),
    "index_basic": TushareDatasetSpec(
        "index_basic", "指数标的", "reference", "reference", "global",
        ("symbol",), "list_date",
        (
            _SYMBOL, _f("name", "string", "指数简称"),
            _f("fullname", "string", "指数全称"), _f("market", "string", "市场"),
            _f("publisher", "string", "发布方"), _f("index_type", "string", "指数风格"),
            _f("category", "string", "指数类别"), _f("base_date", "string", "基期"),
            _f("base_point", "float", "基点"), _f("list_date", "string", "发布日期"),
        ),
        canonical_target="instruments_index/instruments_index.parquet",
    ),
    "trade_cal": TushareDatasetSpec(
        "trade_cal", "交易日历", "reference", "reference", "global",
        ("exchange", "cal_date"), "cal_date",
        (
            _f("exchange", "string", "交易所"),
            _f("cal_date", "string", "日历日期"),
            _f("is_open", "bool", "是否交易日"),
            _f("pretrade_date", "string", "上一交易日", None, "pretrade_date", "pre_date"),
        ),
        canonical_target="pit_reference/history/trade_calendar/part.parquet",
    ),
    "namechange": TushareDatasetSpec(
        "namechange", "证券名称与ST状态历史", "reference", "reference", "symbol",
        ("symbol", "start_date", "name"), "start_date",
        (
            _SYMBOL, _f("name", "string", "证券名称"),
            _f("start_date", "string", "开始日期"),
            _f("end_date", "string", "结束日期"),
            _f("ann_date", "string", "公告日期"),
            _f("change_reason", "string", "变更原因"),
        ),
        canonical_target="instrument_name_history/part.parquet",
    ),
    "daily": TushareDatasetSpec(
        "daily", "A股日线", "daily", "daily", "symbol", ("symbol", "date"), "date",
        (
            _SYMBOL,
            _f("date", "string", "交易日期", None, "trade_date"),
            _f("open", "float", "开盘价", "元"),
            _f("high", "float", "最高价", "元"),
            _f("low", "float", "最低价", "元"),
            _f("close", "float", "收盘价", "元"),
            _f("volume", "float", "成交量", "股", "vol"),
            _f("amount", "float", "成交额", "元", "amt"),
        ),
        canonical_target="kline_daily",
    ),
    "fund_daily": TushareDatasetSpec(
        "fund_daily", "ETF日线", "daily", "daily", "symbol", ("symbol", "date"), "date",
        (
            _SYMBOL,
            _f("date", "string", "交易日期", None, "trade_date"),
            _f("open", "float", "开盘价", "元"),
            _f("high", "float", "最高价", "元"),
            _f("low", "float", "最低价", "元"),
            _f("close", "float", "收盘价", "元"),
            _f("volume", "float", "成交量", "股", "vol"),
            _f("amount", "float", "成交额", "元", "amt"),
        ),
        canonical_target="kline_etf_daily",
    ),
    "index_daily": TushareDatasetSpec(
        "index_daily", "指数日线", "daily", "daily", "index", ("symbol", "date"), "date",
        (
            _SYMBOL,
            _f("date", "string", "交易日期", None, "trade_date"),
            _f("open", "float", "开盘点位"),
            _f("high", "float", "最高点位"),
            _f("low", "float", "最低点位"),
            _f("close", "float", "收盘点位"),
            _f("volume", "float", "成交量", "股", "vol"),
            _f("amount", "float", "成交额", "元", "amt"),
        ),
        canonical_target="kline_index_daily",
    ),
    "daily_basic": TushareDatasetSpec(
        "daily_basic", "每日估值与股本", "daily", "extension", "symbol",
        ("symbol", "trade_date"), "trade_date",
        (
            _SYMBOL, _TRADE_DATE,
            _f("close", "float", "收盘价", "元"),
            _f("turnover_rate", "float", "换手率", "%"),
            _f("turnover_rate_f", "float", "自由流通换手率", "%"),
            _f("volume_ratio", "float", "量比"),
            _f("pe", "float", "市盈率"), _f("pe_ttm", "float", "滚动市盈率"),
            _f("pb", "float", "市净率"), _f("ps", "float", "市销率"),
            _f("ps_ttm", "float", "滚动市销率"), _f("dv_ratio", "float", "股息率", "%"),
            _f("dv_ttm", "float", "滚动股息率", "%"),
            _f("total_share", "float", "总股本", "万股"),
            _f("float_share", "float", "流通股本", "万股"),
            _f("free_share", "float", "自由流通股本", "万股"),
            _f("total_mv", "float", "总市值", "万元"),
            _f("circ_mv", "float", "流通市值", "万元"),
        ),
        factor_input=True,
        overlap_fields=("close",),
    ),
    "income": TushareDatasetSpec(
        "income", "利润表", "financials", "financial", "symbol",
        ("symbol", "period_end", "announce_date"), "announce_date",
        (
            _SYMBOL,
            _f("period_end", "string", "报告期", None, "end_date"),
            _f("announce_date", "string", "公告日期", None, "f_ann_date", "ann_date"),
            _f("revenue", "float", "营业收入", "元", "revenue"),
            _f("operating_cost", "float", "营业成本", "元", "oper_cost"),
            _f("selling_expense", "float", "销售费用", "元", "sell_exp"),
            _f("admin_expense", "float", "管理费用", "元", "admin_exp"),
            _f("rd_expense", "float", "研发费用", "元", "rd_exp"),
            _f("financial_expense", "float", "财务费用", "元", "fin_exp"),
            _f("operating_profit", "float", "营业利润", "元", "operate_profit"),
            _f("non_operating_income", "float", "营业外收入", "元", "non_oper_income"),
            _f("non_operating_expense", "float", "营业外支出", "元", "non_oper_exp"),
            _f("total_profit", "float", "利润总额", "元"),
            _f("income_tax", "float", "所得税", "元"),
            _f("net_income", "float", "净利润", "元", "n_income"),
            _f("net_income_attributable", "float", "归母净利润", "元", "n_income_attr_p"),
            _f("net_income_deducted", "float", "扣非归母净利润", "元", "deduct_parent_netprofit"),
            _f("basic_eps", "float", "基本每股收益", "元"),
            _f("diluted_eps", "float", "稀释每股收益", "元"),
        ),
        canonical_target="financials/income/part.parquet", revisioned=True,
    ),
    "balancesheet": TushareDatasetSpec(
        "balancesheet", "资产负债表", "financials", "financial", "symbol",
        ("symbol", "period_end", "announce_date"), "announce_date",
        (
            _SYMBOL,
            _f("period_end", "string", "报告期", None, "end_date"),
            _f("announce_date", "string", "公告日期", None, "f_ann_date", "ann_date"),
            _f("total_assets", "float", "资产总计", "元"),
            _f("total_current_assets", "float", "流动资产合计", "元", "total_cur_assets"),
            _f("cash_and_equivalents", "float", "货币资金", "元", "money_cap"),
            _f("accounts_receivable", "float", "应收账款", "元", "accounts_receiv"),
            _f("inventory", "float", "存货", "元", "inventories"),
            _f("total_non_current_assets", "float", "非流动资产合计", "元", "total_nca"),
            _f("fixed_assets", "float", "固定资产", "元", "fix_assets"),
            _f("intangible_assets", "float", "无形资产", "元", "intan_assets"),
            _f("goodwill", "float", "商誉", "元"),
            _f("total_liabilities", "float", "负债合计", "元", "total_liab"),
            _f("total_current_liabilities", "float", "流动负债合计", "元", "total_cur_liab"),
            _f("short_term_borrowing", "float", "短期借款", "元", "st_borr"),
            _f("accounts_payable", "float", "应付账款", "元", "acct_payable"),
            _f("total_non_current_liabilities", "float", "非流动负债合计", "元", "total_ncl"),
            _f("long_term_borrowing", "float", "长期借款", "元", "lt_borr"),
            _f("total_equity", "float", "所有者权益合计", "元", "total_hldr_eqy_inc_min_int"),
            _f("equity_attributable", "float", "归母权益", "元", "total_hldr_eqy_exc_min_int"),
            _f("retained_earnings", "float", "未分配利润", "元", "undistr_porfit"),
            _f("minority_interest", "float", "少数股东权益", "元", "minority_int"),
        ),
        canonical_target="financials/balance_sheet/part.parquet", revisioned=True,
    ),
    "cashflow": TushareDatasetSpec(
        "cashflow", "现金流量表", "financials", "financial", "symbol",
        ("symbol", "period_end", "announce_date"), "announce_date",
        (
            _SYMBOL,
            _f("period_end", "string", "报告期", None, "end_date"),
            _f("announce_date", "string", "公告日期", None, "f_ann_date", "ann_date"),
            _f("net_operating_cash_flow", "float", "经营现金流净额", "元", "n_cashflow_act"),
            _f("net_investing_cash_flow", "float", "投资现金流净额", "元", "n_cashflow_inv_act"),
            _f("net_financing_cash_flow", "float", "筹资现金流净额", "元", "n_cash_flows_fnc_act"),
            _f("capex", "float", "资本开支", "元", "c_pay_acq_const_fiolta"),
            _f("net_cash_change", "float", "现金净增加额", "元", "n_incr_cash_cash_equ"),
        ),
        canonical_target="financials/cash_flow/part.parquet", revisioned=True,
    ),
    "fina_indicator": TushareDatasetSpec(
        "fina_indicator", "财务指标", "financials", "financial", "symbol",
        ("symbol", "period_end", "announce_date"), "announce_date",
        (
            _SYMBOL,
            _f("period_end", "string", "报告期", None, "end_date"),
            _f("announce_date", "string", "公告日期", None, "f_ann_date", "ann_date"),
            _f("eps_basic", "float", "基本每股收益", "元", "eps"),
            _f("eps_diluted", "float", "稀释每股收益", "元", "dt_eps"),
            _f("bps", "float", "每股净资产", "元"),
            _f("roe_diluted", "float", "摊薄ROE", "%", "roe_dt"),
            _f("roe", "float", "ROE", "%"),
            _f("gross_margin", "float", "毛利率", "%", "grossprofit_margin"),
            _f("net_margin", "float", "净利率", "%", "netprofit_margin"),
            _f("debt_to_asset_ratio", "float", "资产负债率", "%", "debt_to_assets"),
            _f("operating_cash_to_revenue", "float", "经营现金流收入比", "%", "ocf_to_or"),
            _f("revenue_yoy", "float", "营收同比", "%", "or_yoy"),
            _f("net_income_yoy", "float", "净利润同比", "%", "netprofit_yoy"),
            _f("inventory_turnover", "float", "存货周转率"),
            _f("ocfps", "float", "每股经营现金流", "元"),
        ),
        canonical_target="financials/metrics/part.parquet", revisioned=True,
    ),
    "dividend": TushareDatasetSpec(
        "dividend", "现金分红与除息事件", "financials", "corporate_action", "symbol",
        ("symbol", "ann_date", "end_date"), "ann_date",
        (
            _SYMBOL, _ANN_DATE, _END_DATE,
            _f("div_proc", "string", "实施进度"),
            _f("cash_div", "float", "税前每股现金分红", "元/股"),
            _f("cash_div_tax", "float", "税后每股现金分红", "元/股"),
            _f("record_date", "string", "股权登记日"),
            _f("ex_date", "string", "除权除息日"),
            _f("pay_date", "string", "派息日"),
            _f("base_share", "float", "分红基准股本", "万股"),
        ),
        canonical_target="corporate_actions/stock_dividends.parquet",
        revisioned=True,
    ),
}


def _extension(
    api_name: str,
    label: str,
    primary_key: tuple[str, ...],
    logical_date: str,
    fields: tuple[TushareField, ...],
    *,
    scope: QueryScope = "symbol",
    revisioned: bool = False,
    overlap_fields: tuple[str, ...] = (),
    empty_is_valid: bool = False,
) -> TushareDatasetSpec:
    return TushareDatasetSpec(
        api_name, label, "factors", "extension", scope, primary_key, logical_date,
        fields, revisioned=revisioned, factor_input=True, overlap_fields=overlap_fields,
        empty_is_valid=empty_is_valid,
    )


DATASET_SPECS.update({
    "index_weight": TushareDatasetSpec(
        "index_weight", "指数历史权重", "reference", "extension", "index",
        ("index_symbol", "member_symbol", "trade_date"), "trade_date",
        (
            _f("index_symbol", "string", "指数代码", None, "index_code"),
            _f("member_symbol", "string", "成分代码", None, "con_code"),
            _TRADE_DATE,
            _f("weight", "float", "指数权重", "%"),
        ),
        factor_input=True,
        max_rows=10_000,
    ),
    "suspend_d": TushareDatasetSpec(
        "suspend_d", "停复牌历史", "reference", "extension", "trade_date",
        ("symbol", "suspend_date"), "suspend_date",
        (
            _SYMBOL,
            _f("suspend_date", "string", "停牌日期"),
            _f("resume_date", "string", "复牌日期"),
            _f("ann_date", "string", "公告日期"),
            _f("suspend_timing", "string", "日内停牌时段"),
            _f("suspend_type", "string", "停牌类型"),
        ),
        factor_input=True,
    ),
    "moneyflow": _extension(
        "moneyflow", "个股资金流", ("symbol", "trade_date"), "trade_date",
        (_SYMBOL, _TRADE_DATE, *tuple(
            _f(name, "float", label, unit)
            for name, label, unit in (
                ("buy_sm_vol", "小单买入量", "手"), ("buy_sm_amount", "小单买入额", "万元"),
                ("sell_sm_vol", "小单卖出量", "手"), ("sell_sm_amount", "小单卖出额", "万元"),
                ("buy_md_vol", "中单买入量", "手"), ("buy_md_amount", "中单买入额", "万元"),
                ("sell_md_vol", "中单卖出量", "手"), ("sell_md_amount", "中单卖出额", "万元"),
                ("buy_lg_vol", "大单买入量", "手"), ("buy_lg_amount", "大单买入额", "万元"),
                ("sell_lg_vol", "大单卖出量", "手"), ("sell_lg_amount", "大单卖出额", "万元"),
                ("buy_elg_vol", "特大单买入量", "手"), ("buy_elg_amount", "特大单买入额", "万元"),
                ("sell_elg_vol", "特大单卖出量", "手"), ("sell_elg_amount", "特大单卖出额", "万元"),
                ("net_mf_vol", "净流入量", "手"), ("net_mf_amount", "净流入额", "万元"),
            )
        )),
    ),
    "margin": _extension(
        "margin", "交易所融资融券", ("exchange_id", "trade_date"), "trade_date",
        (_TRADE_DATE, _f("exchange_id", "string", "交易所"),
         _f("rzye", "float", "融资余额", "元"), _f("rzmre", "float", "融资买入额", "元"),
         _f("rqye", "float", "融券余额", "元"), _f("rqmcl", "float", "融券卖出量", "股"),
         _f("rzrqye", "float", "两融余额", "元"), _f("rqyl", "float", "融券余量", "股")),
        scope="trade_date",
    ),
    "margin_detail": _extension(
        "margin_detail", "个股融资融券", ("symbol", "trade_date"), "trade_date",
        (_SYMBOL, _TRADE_DATE,
         _f("rzye", "float", "融资余额", "元"), _f("rqye", "float", "融券余额", "元"),
         _f("rzmre", "float", "融资买入额", "元"), _f("rqyl", "float", "融券余量", "股"),
         _f("rzche", "float", "融资偿还额", "元"), _f("rqchl", "float", "融券偿还量", "股"),
         _f("rqmcl", "float", "融券卖出量", "股"), _f("rzrqye", "float", "两融余额", "元")),
    ),
    "top_list": _extension(
        "top_list", "龙虎榜", ("symbol", "trade_date", "reason"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("name", "string", "名称"),
         _f("close", "float", "收盘价", "元"), _f("pct_change", "float", "涨跌幅", "%"),
         _f("turnover_rate", "float", "换手率", "%"), _f("amount", "float", "成交额", "千元"),
         _f("l_sell", "float", "榜单卖出额", "万元"), _f("l_buy", "float", "榜单买入额", "万元"),
         _f("l_amount", "float", "榜单成交额", "万元"), _f("net_amount", "float", "榜单净买额", "万元"),
         _f("net_rate", "float", "榜单净买占比", "%"), _f("amount_rate", "float", "榜单成交占比", "%"),
         _f("float_values", "float", "流通市值", "万元"), _f("reason", "string", "上榜原因")),
        scope="trade_date", overlap_fields=("close", "pct_change", "turnover_rate", "amount", "float_values"),
    ),
    "limit_list_d": _extension(
        "limit_list_d", "涨跌停明细", ("symbol", "trade_date", "limit"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("industry", "string", "行业"), _f("name", "string", "名称"),
         _f("close", "float", "收盘价", "元"), _f("pct_chg", "float", "涨跌幅", "%"),
         _f("amount", "float", "成交额", "千元"), _f("limit_amount", "float", "板上成交额", "千元"),
         _f("float_mv", "float", "流通市值", "万元"), _f("total_mv", "float", "总市值", "万元"),
         _f("turnover_ratio", "float", "换手率", "%"), _f("fd_amount", "float", "封单金额", "千元"),
         _f("first_time", "string", "首次封板时间"), _f("last_time", "string", "最后封板时间"),
         _f("open_times", "int", "开板次数"), _f("up_stat", "string", "涨停统计"),
         _f("limit_times", "int", "连板数"), _f("limit", "string", "涨跌停方向")),
        scope="trade_date", overlap_fields=("close", "pct_chg", "amount", "float_mv", "total_mv"),
    ),
    "limit_list_ths": _extension(
        "limit_list_ths", "同花顺涨停榜", ("symbol", "trade_date"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("name", "string", "名称"), _f("reason", "string", "涨停原因"),
         _f("tag", "string", "标签"), _f("status", "string", "状态"),
         _f("first_time", "string", "首次封板时间"), _f("last_time", "string", "最后封板时间"),
         _f("open_num", "int", "开板次数"), _f("limit_order", "float", "封单额", "元"),
         _f("amount", "float", "成交额", "元"), _f("turnover_rate", "float", "换手率", "%"),
         _f("free_float", "float", "流通市值", "元")),
        scope="trade_date", overlap_fields=("amount", "turnover_rate", "free_float"),
    ),
    "forecast": _extension(
        "forecast", "业绩预告", ("symbol", "ann_date", "end_date"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("type", "string", "预告类型"),
         _f("p_change_min", "float", "同比下限", "%"), _f("p_change_max", "float", "同比上限", "%"),
         _f("net_profit_min", "float", "净利润下限", "万元"), _f("net_profit_max", "float", "净利润上限", "万元"),
         _f("last_parent_net", "float", "上年同期归母净利润", "万元"),
         _f("first_ann_date", "string", "首次公告日"), _f("summary", "string", "摘要"),
         _f("change_reason", "string", "变动原因")), revisioned=True,
    ),
    "express": _extension(
        "express", "业绩快报", ("symbol", "ann_date", "end_date"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE,
         _f("revenue", "float", "营业收入", "元"), _f("operate_profit", "float", "营业利润", "元"),
         _f("total_profit", "float", "利润总额", "元"), _f("n_income", "float", "净利润", "元"),
         _f("total_assets", "float", "总资产", "元"),
         _f("total_hldr_eqy_exc_min_int", "float", "归母权益", "元"),
         _f("diluted_eps", "float", "每股收益", "元"), _f("diluted_roe", "float", "净资产收益率", "%"),
         _f("yoy_net_profit", "float", "上年同期净利润", "元"), _f("bps", "float", "每股净资产", "元"),
         _f("yoy_sales", "float", "营收同比", "%"), _f("yoy_op", "float", "营业利润同比", "%"),
         _f("yoy_tp", "float", "利润总额同比", "%"), _f("yoy_dedu_np", "float", "扣非净利同比", "%"),
         _f("perf_summary", "string", "业绩摘要"), _f("is_audit", "int", "是否审计"),
         _f("remark", "string", "备注")), revisioned=True, empty_is_valid=True,
    ),
    "disclosure_date": _extension(
        "disclosure_date", "财报披露计划", ("symbol", "end_date", "ann_date"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("pre_date", "string", "预计披露日"),
         _f("actual_date", "string", "实际披露日"), _f("modify_date", "string", "修改日期")), revisioned=True,
    ),
    "stk_holdernumber": _extension(
        "stk_holdernumber", "股东户数", ("symbol", "ann_date", "end_date"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("holder_num", "float", "股东户数", "户")), revisioned=True,
    ),
    "top10_holders": _extension(
        "top10_holders", "十大股东", ("symbol", "ann_date", "end_date", "holder_name"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("holder_name", "string", "股东名称"),
         _f("hold_amount", "float", "持股数量", "股"), _f("hold_ratio", "float", "持股比例", "%"),
         _f("hold_float_ratio", "float", "流通持股比例", "%"), _f("hold_change", "float", "持股变动", "股"),
         _f("holder_type", "string", "股东类型")), revisioned=True,
    ),
    "top10_floatholders": _extension(
        "top10_floatholders", "十大流通股东", ("symbol", "ann_date", "end_date", "holder_name"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("holder_name", "string", "股东名称"),
         _f("hold_amount", "float", "持股数量", "股"), _f("hold_ratio", "float", "持股比例", "%"),
         _f("hold_float_ratio", "float", "流通持股比例", "%"), _f("hold_change", "float", "持股变动", "股"),
         _f("holder_type", "string", "股东类型")), revisioned=True,
    ),
    "stk_holdertrade": _extension(
        "stk_holdertrade", "股东增减持", ("symbol", "ann_date", "holder_name", "begin_date", "close_date"), "ann_date",
        (_SYMBOL, _ANN_DATE, _f("holder_name", "string", "股东名称"), _f("holder_type", "string", "股东类型"),
         _f("in_de", "string", "增减持方向"), _f("change_vol", "float", "变动数量", "股"),
         _f("change_ratio", "float", "变动比例", "%"), _f("after_share", "float", "变动后持股", "股"),
         _f("after_ratio", "float", "变动后比例", "%"), _f("avg_price", "float", "平均价格", "元"),
         _f("total_share", "float", "总股本", "股"), _f("begin_date", "string", "开始日期"),
         _f("close_date", "string", "结束日期")), revisioned=True,
    ),
    "block_trade": _extension(
        "block_trade", "大宗交易", ("symbol", "trade_date", "price", "vol", "buyer", "seller"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("price", "float", "成交价", "元"), _f("vol", "float", "成交量", "万股"),
         _f("amount", "float", "成交额", "万元"), _f("buyer", "string", "买方营业部"), _f("seller", "string", "卖方营业部")),
        scope="trade_date",
    ),
    "repurchase": _extension(
        "repurchase", "股票回购", ("symbol", "ann_date", "end_date", "proc"), "ann_date",
        (_SYMBOL, _ANN_DATE, _END_DATE, _f("proc", "string", "进度"), _f("exp_date", "string", "到期日期"),
         _f("vol", "float", "回购数量", "股"), _f("amount", "float", "回购金额", "万元"),
         _f("high_limit", "float", "价格上限", "元"), _f("low_limit", "float", "价格下限", "元")), revisioned=True,
    ),
    "share_float": _extension(
        "share_float", "限售股解禁", ("symbol", "ann_date", "float_date", "holder_name", "share_type"), "ann_date",
        (_SYMBOL, _ANN_DATE, _f("float_date", "string", "解禁日期"), _f("float_share", "float", "解禁股数", "股"),
         _f("float_ratio", "float", "解禁比例", "%"), _f("holder_name", "string", "股东名称"),
         _f("share_type", "string", "股份类型")), revisioned=True,
    ),
    "cyq_perf": _extension(
        "cyq_perf", "筹码胜率", ("symbol", "trade_date"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("his_low", "float", "历史最低价", "元"), _f("his_high", "float", "历史最高价", "元"),
         _f("cost_5pct", "float", "5%成本", "元"), _f("cost_15pct", "float", "15%成本", "元"),
         _f("cost_50pct", "float", "50%成本", "元"), _f("cost_85pct", "float", "85%成本", "元"),
         _f("cost_95pct", "float", "95%成本", "元"), _f("weight_avg", "float", "加权平均成本", "元"),
         _f("winner_rate", "float", "胜率", "%")),
    ),
    "cyq_chips": _extension(
        "cyq_chips", "筹码分布", ("symbol", "trade_date", "price"), "trade_date",
        (_SYMBOL, _TRADE_DATE, _f("price", "float", "价格", "元"), _f("percent", "float", "筹码占比", "%")),
    ),
})


DATASET_SPECS.update({
    "index_member_all": TushareDatasetSpec(
        "index_member_all", "申万行业历史成分", "reference", "pit_industry", "global",
        ("member_symbol", "industry_standard", "industry_level", "effective_from"),
        "effective_from",
        (_f("member_symbol", "string", "成分代码", None, "ts_code"),
         _f("member_name", "string", "成分名称", None, "name"),
         _f("industry_standard", "string", "行业标准"),
         _f("industry_standard_code", "string", "行业标准编码"),
         _f("industry_level", "int", "行业级别"),
         _f("industry_code", "string", "行业代码"),
         _f("industry_name", "string", "行业名称"),
         _f("effective_from", "string", "生效日期", None, "in_date"),
         _f("effective_to", "string", "失效日期", None, "out_date")),
        max_rows=10_000,
    ),
    "ci_index_member": TushareDatasetSpec(
        "ci_index_member", "中信行业历史成分", "reference", "pit_industry", "global",
        ("member_symbol", "industry_standard", "effective_from"), "effective_from",
        (_f("member_symbol", "string", "成分代码", None, "con_code", "ts_code"),
         _f("member_name", "string", "成分名称", None, "con_name", "name"),
         _f("industry_standard", "string", "行业标准"),
         _f("industry_code", "string", "行业代码", None, "l1_code", "index_code"),
         _f("industry_name", "string", "行业名称", None, "l1_name", "index_name"),
         _f("effective_from", "string", "生效日期", None, "in_date"),
         _f("effective_to", "string", "失效日期", None, "out_date")),
    ),
})


GROUPS: dict[str, tuple[str, ...]] = {
    "reference": (
        "stock_basic", "etf_basic", "index_basic", "trade_cal", "namechange",
        "suspend_d", "index_member_all", "index_weight", "ci_index_member",
    ),
    "daily": ("daily", "daily_basic", "fund_daily", "index_daily"),
    "financials": ("income", "balancesheet", "cashflow", "fina_indicator", "dividend"),
    "factors": tuple(
        name for name, spec in DATASET_SPECS.items() if spec.group == "factors"
    ),
}


def resolve_datasets(values: tuple[str, ...] | list[str]) -> tuple[TushareDatasetSpec, ...]:
    names: list[str] = []
    for value in values:
        name = str(value).strip()
        if not name:
            continue
        names.extend(GROUPS.get(name, (name,)))
    names = list(dict.fromkeys(names))
    unknown = [name for name in names if name not in DATASET_SPECS]
    if unknown:
        raise ValueError(f"unknown Tushare dataset(s): {', '.join(unknown)}")
    return tuple(DATASET_SPECS[name] for name in names)
