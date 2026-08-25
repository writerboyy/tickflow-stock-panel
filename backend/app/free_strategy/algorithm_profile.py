"""Human-readable algorithm profiles for paper strategies."""
from __future__ import annotations

import ast
from typing import Any


_PROFILES: dict[str, dict[str, Any]] = {
    "seven_stars": {
        "summary": "在大类 ETF 池中选择趋势最强且交易状态正常的单一标的，下午完成轮换，并在盘中持续执行盈利保护。",
        "inputs": [
            "固定 ETF 池的前复权日线，至少覆盖 45 根；评分使用最近 25 个交易日，短期确认使用最近 10 个交易日。",
            "当日闭合分钟行情，包括原始成交价、成交量、停牌/可交易状态和涨跌停价格。",
            "截至前一交易日已经披露的单位净值，用于计算 ETF 二级市场溢价，缺失时不猜测。",
        ],
        "steps": [
            {"title": "构造评分序列", "detail": "对每只 ETF 取最近 25 日收盘价并追加当前分钟价格。令 y=ln(price)，时间权重为 (1+i/25)^2，对 y 做加权线性回归。年化趋势为 exp(slope×250)-1，最终动量分数为 年化趋势×R²。"},
            {"title": "趋势和量价过滤", "detail": "仅保留 0<动量分数<100、10 日年化收益非负的标的；最近 3 个交易日任一日跌幅超过 3% 即剔除。若当日量/近 5 日均量>2 且年化趋势>100%，视为异常放量并剔除。"},
            {"title": "交易状态过滤", "detail": "停牌、不可交易、已从前一交易日高点回撤 5% 的 ETF 不进入候选；债券 ETF 511010、511220、511380 从轮动池排除。"},
            {"title": "净值与溢价过滤", "detail": "按披露日对齐最近可用单位净值，溢价率=(对应交易日价格-单位净值)/单位净值，要求不高于 20%。净值、历史价格或日期对齐缺失时，本次选股 fail-closed，保留原持仓。"},
            {"title": "排名和调仓", "detail": "候选按动量分数从高到低排序，只选择第一名。13:09 卖出不再是目标的持仓，13:10 使用实际可用现金买入目标；策略交易资金上限 100,000 元，单笔低于 5,000 元不下单。"},
            {"title": "盈利保护", "detail": "09:45 至 14:56 的 16 个检查点比较现价和前一交易日最高价。现价<=前高×95% 时卖出全部可卖数量，并把该 ETF 加入当日禁止回补名单。"},
        ],
        "parameters": [
            "趋势窗口=25 日；短期窗口=10 日；年化交易日=250。",
            "溢价上限=20%；单日急跌过滤=-3%；异常放量阈值=2 倍。",
            "盈利保护回撤=5%；资金上限=100,000 元；最小交易金额=5,000 元。",
            "策略内参考成交参数：滑点 0.5 bps、佣金 0.02%、最低佣金 5 元、ETF 最小价位 0.001 元；最终以账户公共成交引擎配置为准。",
        ],
        "pseudocode": [
            "for etf in pool - excluded_bond_etfs:",
            "    prices = last_25_daily_closes + current_price",
            "    score = annualized_weighted_log_slope(prices) * weighted_r_squared(prices)",
            "    reject if score not in (0, 100) or annualized_return_10d < 0",
            "    reject if any(last_3_daily_return < -3%) or abnormal_volume",
            "rank candidates by score descending",
            "target = first candidate whose disclosed_NAV_premium <= 20%",
            "13:09 sell holdings except target; 13:10 buy target with available cash",
            "intraday: if current_price <= previous_day_high * 0.95, sell and block re-entry today",
        ],
    },
    "small_cap_limitup": {
        "summary": "从全市场小市值股票中寻找具有历史涨停基因的候选，结合行业分散、周度调仓和盘中止损管理组合。",
        "inputs": [
            "股票维表、上市日期和历史名称；历史名称缺失时用最近 120 日涨跌停制度推断历史 ST 状态。",
            "前复权日线最多 750 日、总股本、流通股本、当日分钟行情及涨跌停价。",
            "截至前一交易日有效的申万二级行业 PIT 分类；行业数据缺失时禁止生成新组合。",
        ],
        "steps": [
            {"title": "小市值初选", "detail": "排除科创板、北交所、ST/*ST/退市、上市不足 375 天、停牌、涨跌停不可买和近 20 天止损黑名单标的。按 前收盘价×总股本 从小到大排序，最多保留前 1,000 只。"},
            {"title": "识别涨停基因", "detail": "读取最多 750 日历史，在最近 500 日内必须至少出现一次相邻两个交易日连续涨停。按最近一次涨停距今天数从近到远排序后，主动剔除最新鲜的前 10%，降低刚被集中交易的拥挤风险。"},
            {"title": "形态偏离排序", "detail": "对每只合格股票，从最近一次涨停向前寻找最近的阴线低点，以 当前前复权价格/该低点 排序，比例越小越靠前。"},
            {"title": "行业分散", "detail": "按前一交易日有效的申万二级行业逐只去重，每个行业只保留排名最靠前的一只，先形成最多 10 只候选，最终组合最多持有 6 只。行业 PIT 数据不完整时停止换仓。"},
            {"title": "周度调仓", "detail": "每周第二个已出现的交易日执行。先卖出落选且当时未封涨停的持仓，再按总资产/6 计算单只目标市值，以 100 股整手补齐；数据缺失时保持原持仓，不使用旧候选冒险下单。"},
            {"title": "止损和午后替换", "detail": "个股跌至成本的 91% 卖出，并加入 20 天亏损黑名单；002 股票上一交易日平均收盘/开盘<=93% 时触发市场止损。昨日涨停股开板或换手异常时卖出，并从候选中补位。"},
            {"title": "空仓月份", "detail": "1 月和 4 月停止新交易并清理持仓；完成一次清仓后保持空仓状态，直到进入正常交易月份。"},
        ],
        "parameters": [
            "持仓数=6；小市值初选=1,000；历史窗口=750 日；连板检测=最近 500 日。",
            "上市天数>=375；最新涨停拥挤剔除比例=10%；止损黑名单=20 个自然日。",
            "个股止损线=成本×91%；市场止损线=002 股票平均收盘/开盘×93%。",
            "策略资金参考上限=130,000 元；1 月、4 月为空仓月份。",
        ],
        "pseudocode": [
            "universe = filter(listed>=375d, non_ST, non_KCBJ, tradable, not_loss_blacklisted)",
            "small_caps = sort_by(previous_close * total_shares)[:1000]",
            "gene = keep(stock has consecutive_limit_up within last_500_of_750_days)",
            "gene = drop freshest 10% by days_since_last_limit_up",
            "rank by adjusted_current_price / pre_limit_bearish_low ascending",
            "targets = first stock of each SW_level2_industry, max 6",
            "weekly: sell dropped non-limit-up holdings, then equal-weight buy targets",
            "intraday: stop individual at -9%; market stop at ratio<=0.93; replace opened limit-ups",
            "January or April: close positions and remain flat",
        ],
    },
    "five_fortunes": {
        "summary": "在多品类 ETF 中按趋势质量、流动性和净值溢价进行轮动，并根据市场状态动态选择进攻或防御标的。",
        "inputs": [
            "全局 ETF 目录及前一交易日成交额，用于动态构造正常期和走弱期流动性池。",
            "至少 61 根 ETF 日线、当日分钟价格和成交量、已披露单位净值。",
            "沪深 300、中证 1000、创业板和红利等代理指数日线，用于市场状态投票。",
        ],
        "steps": [
            {"title": "动态流动性池", "detail": "用全市场 ETF 前一日平均成交额生成门槛：正常期门槛=市场平均成交额/20,000，走弱期门槛=市场平均成交额/3,000。仅分析成交额超过当前门槛且历史覆盖完整的 ETF。"},
            {"title": "市场状态", "detail": "09:40 检查 4 个代理指数：至少 4 个低于 MA20 判定走弱候选，至少 4 个高于 MA10 判定正常候选，其余为震荡；状态连续 2 天确认后切换，避免单日抖动。"},
            {"title": "趋势评分", "detail": "使用 25 日加权对数回归计算 score=年化趋势×R²，同时计算 21 日短趋势。要求 0<score<=5；正常期 R²>0.39、震荡期 R²>0.40，走弱期价格必须高于 MA10。"},
            {"title": "量价与净值过滤", "detail": "当日量/近 5 日均量必须<1.9，最近 3 日不能出现超过 3% 的单日下跌，短趋势要求 0<short_score<=6。溢价上限：正常期 30%、震荡期 10%、走弱期 8%。"},
            {"title": "目标和换仓保护", "detail": "非走弱期保留分数达到第一名 90% 的候选，走弱期只保留第一名。现持仓仍合格时优先保留；新旧标的相关性>=0.88 且原持仓动量<=8，或相关性>0.85 且动量<=7 时阻止无意义换仓。"},
            {"title": "交易和防御", "detail": "13:10 先卖出风险或非目标持仓，13:11 再按实际剩余现金买入。连续 4 次筛选无合格标的时允许使用货币 ETF 511880 防御。"},
            {"title": "组合风控", "detail": "10:31 检查净值回撤。正常/震荡期达到 10%减半、12%切防御、20%清仓；走弱期阈值收紧为 5%/8%/12%。单只 ETF 正常期跌至成本 91%、走弱期跌至成本 95% 时止损并冷却 2 日。"},
        ],
        "parameters": [
            "主趋势窗口=25 日；短趋势窗口=21 日；候选分数带=最高分的 90%。",
            "动量分数上限=5；成交量比上限=1.9；最近单日跌幅下限=-3%。",
            "防御 ETF=511880.SH；状态确认=2 日；止损后回补冷却=2 日。",
            "回撤阈值：正常/震荡 10%/12%/20%，走弱 5%/8%/12%。",
        ],
        "pseudocode": [
            "regime = confirm_2_days(vote(indexes vs MA10/MA20))",
            "liquid_pool = ETFs with previous_amount > regime_liquidity_threshold",
            "for etf in liquid_pool: compute weighted_momentum_25, R2, momentum_21, MA10, volume_ratio, NAV_premium",
            "filtered = apply regime-specific R2, trend, volume, loss and premium gates",
            "candidate_band = score >= top_score*0.9; weak_regime keeps top only",
            "target = keep eligible holding else best candidate, subject to correlation guard",
            "13:10 sell non-target; 13:11 buy target with actual cash",
            "10:31 apply portfolio drawdown ladder; intraday apply per-symbol stop and cooldown",
        ],
    },
    "five_fortunes_v2": {
        "summary": "五福 ETF 轮动的增强版本，在原有趋势、流动性和溢价框架上增加弱势识别、量价背离与盘中趋势确认。",
        "inputs": [
            "全市场 ETF 目录、前一日成交额、至少 61 根日线和已披露单位净值。",
            "4 个市场代理指数的日线，用于弱势、震荡和观察窗口自适应。",
            "候选 ETF 最近 30 根分钟收盘价，用于买入前趋势回归确认。",
        ],
        "steps": [
            {"title": "环境识别", "detail": "4 个代理指数中至少 3 个低于参考均线进入弱势候选，至少 3 个恢复到均线上方退出；连续确认后切换。另以 10 日涨跌绝对值投票识别震荡，3 个以上代理满足时标记震荡。"},
            {"title": "自适应观察窗口", "detail": "基础动量窗口在 23/25 日之间自适应：25 日窗口连续 2 天 R² 偏高时缩短到 23 日，23 日窗口连续 2 天 R² 偏低时恢复到 25 日。"},
            {"title": "候选过滤", "detail": "加权动量分数要求 0<=score<=5，成交量比<1.8，最近 3 日每日跌幅不低于 -3%，单位净值溢价<=30%，并检查均线、拟合度和量价背离。非走弱期保留达到最高分 90% 的候选。"},
            {"title": "相关性换仓保护", "detail": "持仓仍在候选范围时优先保留；新旧 ETF 相关性>=0.88 且持仓动量<=8，或相关性>0.85 且动量<=7 时拦截换仓，减少同类资产间无收益切换。"},
            {"title": "分钟趋势确认", "detail": "买入前对最近 30 根分钟收盘价做线性回归，要求每分钟相对斜率>0.1%且 R²>0.3。未通过的目标进入待买队列，在后续定时点重试；14:55 进行最后处理。"},
            {"title": "交易与风险", "detail": "13:10 卖出非目标并生成待买计划，卖出到账后按真实现金下单。单只 ETF 跌至成本 95% 止损；组合回撤阶梯与五福基础版一致，走弱期使用 5%/8%/12%，其他环境使用 10%/12%/20%。"},
        ],
        "parameters": [
            "动量窗口=23 或 25 日自适应；候选分数带=最高分的 90%。",
            "成交量比上限=1.8；单日跌幅下限=-3%；净值溢价上限=30%。",
            "分钟确认窗口=30 分钟；相对斜率>0.001；R²>0.3。",
            "单标的止损=成本×95%；防御 ETF=511880.SH。",
        ],
        "pseudocode": [
            "regime = confirmed_vote(4 market proxies); choppy = count(abs(return_10d) small)>=3",
            "lookback = adapt_between_23_and_25_using_R2_streak",
            "metrics = weighted_momentum + R2 + MA + volume_ratio + loss_filter + NAV_premium + divergence",
            "candidates = filter(metrics); keep score >= top_score*0.9",
            "target = apply holding preference and correlation guard",
            "13:10 sell old holdings and queue target",
            "buy only when 30m regression slope>0.001 and R2>0.3; retry, then final check at 14:55",
            "apply 5% symbol stop and regime-specific portfolio drawdown ladder",
        ],
    },
    "strong_momentum": {
        "summary": "使用前一交易日及更早数据生成强势股候选，在集合竞价和早盘关键秒点确认强度后入场，并通过实时报价管理退出。",
        "inputs": [
            "仅使用 D-1 及以前的日线、历史名称和强势股快照生成候选，避免把 D 日结果泄漏到盘前。",
            "D 日 WebSocket/秒级报价，包括原始开高低现价、成交量、涨跌停价和可交易状态。",
            "账户现金、持仓成本、T+1 可卖数量和每个持仓的入场交易日。",
        ],
        "steps": [
            {"title": "盘前候选", "detail": "读取 D-1 强势股快照及最多 30 日历史，保留候选元数据、前收盘、前日涨幅、换手率和放量特征。当天只做盘中确认，不重新用当天日线筛选。"},
            {"title": "开盘过滤", "detail": "要求开盘涨幅在 0%~8%，不能直接开在涨停或跌停；若前一日为高量涨停，开盘涨幅>=5%则剔除。开盘后现价相对开盘跌幅不能低于 -0.3%，当前相对前收涨幅不能超过 10%。"},
            {"title": "关键秒点入场", "detail": "09:30:16、09:31:00、09:32:00、09:37:00、10:29:00 依次检查。候选按 现价/开盘价-1 优先、当日涨幅次优先排序，填充剩余持仓槽位。"},
            {"title": "仓位分配", "detail": "总资产<10万最多 2 只，<30万 3只，<100万 4只，<500万 5只，否则 8只。每个剩余槽位分配剩余现金的等份，数量向下取整到 100 股。"},
            {"title": "开盘卖出", "detail": "持仓首次早盘检查时，若未涨停且开盘涨幅<5%，卖出全部 T+1 可卖数量。"},
            {"title": "实时退出", "detail": "每笔报价更新最高价。成本收益<=-4%止损；收益>=19%且当日未触及涨停时止盈；10:20 后触及涨停又从日内高点回撤>=1.5%时卖出；持有满 3 个交易日且未涨停时退出。"},
            {"title": "当日回补限制", "detail": "卖出后只有当现价低于卖出价 0.3% 以上才允许重新进入，避免在同一价格附近反复交易。所有 T+1、费用、滑点和涨跌停可成交性由公共引擎执行。"},
        ],
        "parameters": [
            "入场时点=09:30:16、09:31:00、09:32:00、09:37:00、10:29:00。",
            "开盘涨幅范围=0%~8%；开盘后允许回落=-0.3%；当日涨幅上限=10%。",
            "成本止损=-4%；止盈=+19%；炸板回撤=1.5%；最长持有=3 个交易日。",
            "股票整手=100 股；总资金暴露上限=100%。",
        ],
        "pseudocode": [
            "candidates = snapshot_using_data_through_D_minus_1",
            "on each entry_time:",
            "    gate by 0<=open_gain<=8%, not limit open, intraday_drop>=-0.3%, current_gain<=10%",
            "    rank by intraday_lift then current_gain descending",
            "    split remaining cash equally across remaining slots; round to 100 shares",
            "on every quote: update session high and hit-limit state",
            "sell if opening weak, profit<=-4%, profit>=19%, post-limit drawdown>=1.5%, or holding_days>=3",
            "after same-day sell, re-enter only below sold_price*0.997",
        ],
    },
    "performance_small_cap": {
        "summary": "从低价小市值股票中筛选盈利质量和分红表现较好的标的，按月调仓，并用小盘风格流动性和指数背离控制风险。",
        "inputs": [
            "截至前一交易日已披露的利润表、财务指标、资产负债表和公告日期。",
            "过去一年现金分红、历史总股本、PIT 市值、股票维表及历史名称。",
            "股票/指数日线与当日分钟行情，用于小盘指数、流动性和 MACD 风控。",
        ],
        "steps": [
            {"title": "股息率预筛", "detail": "统计前一交易日前一年内的现金分红总额，以 分红总额/PIT 市值 排序，只保留排名前 25% 的股票。没有可用分红、市值或历史股本时不伪造排名。"},
            {"title": "盈利质量过滤", "detail": "使用当时已公告的最近一期财务数据，要求归母净利润>0、净利润>0、营业收入>1 亿元、ROE>0、ROA>0。公告日期晚于选股日的数据不得使用。"},
            {"title": "低价小市值排序", "detail": "排除科创板、北交所、ST/*ST/退市和不可交易标的；非原持仓价格必须<6 元。其余按 PIT 市值从小到大排序，最多选择 5 只。"},
            {"title": "月度调仓", "detail": "首次运行或每月首个交易日 09:30 重新计算目标。先卖出落选持仓，再按总资产/5 等权补齐，数量向下取整到 100 股。选股依赖缺失时保留持仓。"},
            {"title": "昨日涨停保护", "detail": "昨日涨停的持仓在常规月调仓时暂不卖出，14:00 再检查是否开板；开板后卖出并从当前候选顺序补位。"},
            {"title": "小盘风格流动性", "detail": "构造市值最小的 400 只股票风格样本，并计算流动性分位。达到 97% 极端分位时关闭交易，恢复到 70% 以下后才重新允许交易，避免高拥挤阶段立即抄底。"},
            {"title": "指数与 MACD 风控", "detail": "合成小盘指数值超过 18.72 时触发风险关闭；同时监控 399303.SZ 的 MACD 顶背离/死叉和底背离/金叉。风险触发后至少等待 5 个交易日，并在恢复条件成立后才重新开放。"},
        ],
        "parameters": [
            "持仓数=5；非持仓最高价格<6 元；股息率排名保留前 25%。",
            "营业收入>1 亿元；归母净利润、净利润、ROE、ROA 均>0。",
            "小盘样本=市值最小 400 只；指数风险阈值=18.72；禁买期=5 个交易日。",
            "流动性风险进入分位=97%；恢复分位=70%；基准指数=399303.SZ。",
        ],
        "pseudocode": [
            "pit_date = previous_trading_day",
            "dividend_pool = top_25_percent(cash_dividend_1y / PIT_market_cap)",
            "quality = revenue>1e8 and attributable_profit>0 and net_profit>0 and ROE>0 and ROA>0",
            "candidates = non_ST, non_KCBJ, tradable, price<6; sort by PIT_market_cap",
            "targets = candidates[:5]",
            "first trading day of month: sell dropped holdings, equal-weight buy targets",
            "protect yesterday limit-ups until 14:00, then replace opened boards",
            "disable trading on liquidity/index/MACD risk; wait at least 5 sessions before recovery",
        ],
    },
    "four_mode": {
        "summary": "同时运行一进二、弱转强、趋势股和首板四套选股逻辑，按市场风险和模式优先级统一分配仓位。",
        "inputs": [
            "股票 PIT 快照、80 日普通历史、65 日趋势历史、历史名称和集合竞价 Tick。",
            "上证指数至少 120 日行情，用于趋势、动量、波动率和微观结构仓位系数。",
            "盘中分钟行情，包括原始价、成交量、成交额、VWAP、涨跌停和可交易状态。",
        ],
        "steps": [
            {"title": "四模式预选", "detail": "09:05 分别生成一进二(yje)、弱转强(rzq)、趋势股(qs)、首板(sb)静态候选。候选完全来自 PIT 快照和历史行情，不使用收盘后才知道的数据。"},
            {"title": "市场风险仓位", "detail": "09:24 对指数计算 MA3/10/20/60 多头结构、MA20 斜率、乖离、量价背离、RSI/MACD 动量、ATR14/ATR60 波动和放量微观结构，合成 0~100 分后得到 0~100% 仓位系数；连续放量下跌时最高压到 30%。"},
            {"title": "情绪模式优先级", "detail": "指数>2%且涨停>50 为主升，指数>0且涨停扩张为修复，指数<-1.5%且涨停<20 为冰点，指数>0但涨停<30 为背离，否则为退潮。不同阶段调整 yje/sb/rzq 的买入顺序。"},
            {"title": "竞价确认", "detail": "09:25:45 使用边界前最后一笔可见 Tick 更新四模式候选；快照不完整时不买入。09:27 标记开盘卖出，09:28 根据最终候选、风险系数和模式额度下单。"},
            {"title": "买入额度", "detail": "组合最多 10 只。一进二最多 4 只；首板最多 4 只且总额度不超过 30%；趋势股最多 3 只且总额度不超过 35%。涨停、停牌、一字板或不可交易标的不买。"},
            {"title": "首板盘中确认", "detail": "首板至少积累 5 根分钟数据。要求涨幅 2%~8%且综合分>=60，或涨幅 8%~9.5%且价格在 VWAP 上方；综合分由放量站上 VWAP、价格强度和最近 5 分钟量价攻击次数构成。"},
            {"title": "持仓退出", "detail": "趋势股使用 最高价-1.5×ATR14 跟踪止损，最多持有 10 个交易日。其他模式按开盘强弱设置 0%/-2%/-3%止损和 10:00 时间退出；通用盘中止损为 -3%，14:30 未涨停则卖出。"},
        ],
        "parameters": [
            "最大持仓=10；一进二上限=4；首板上限=4/30%；趋势股上限=3/35%。",
            "趋势股 ATR 周期=14，跟踪倍数=1.5，最长持有=10 个交易日。",
            "关键时点=09:05、09:24、09:25:45、09:27、09:28、10:00、11:30、14:30、15:01/02/05。",
            "首板确认：分钟样本>=5，常规涨幅 2%~8%且得分>=60，强势分支 8%~9.5%且站上 VWAP。",
        ],
        "pseudocode": [
            "09:05 candidates = snapshot(yje, rzq, qs, sb)",
            "09:24 risk_ratio = score(index trend + momentum + volatility + microstructure)",
            "priority = classify_sentiment(index_change, limit_up_count)",
            "09:25:45 confirm candidates with last visible auction tick; fail closed if incomplete",
            "09:27 apply opening exits; 09:28 allocate buys by mode caps and priority",
            "for each minute: update VWAP/volume, confirm first-board signals, monitor positions",
            "trend exit at highest-1.5*ATR14 or 10 sessions",
            "other modes exit on opening rule, -3% stop, or not-limit-up at 14:30",
        ],
    },
}


def _source_identity(source: str) -> tuple[str, str]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return "", ""
    doc = ast.get_docstring(tree, clean=True) or ""
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id == "STRATEGY_KIND":
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                break
            return str(value), doc
    return "", doc


def _profile_key(strategy: dict[str, Any], source: str) -> str:
    kind, doc = _source_identity(source)
    if kind in _PROFILES:
        return kind
    strategy_id = str(strategy.get("id") or "")
    name = str(strategy.get("name") or "")
    marker = f"{strategy_id} {name} {doc}".lower()
    if "七星" in marker:
        return "seven_stars"
    if "涨停基因" in marker or strategy_id == "small_cap_limitup":
        return "small_cap_limitup"
    if "五福 2.0" in marker or "五福2.0" in marker or strategy_id == "five_fortunes_v2":
        return "five_fortunes_v2"
    if "五福" in marker or "etf动量轮动" in marker:
        return "five_fortunes"
    return ""


def _mode_text(account: dict[str, Any]) -> str:
    mode = str(account.get("market_mode") or "")
    return {
        "bar_1m": "按闭合 1 分钟 K 线推进策略",
        "bar_1d": "按日 K 推进策略",
        "poll_3s": "按 3 秒实时报价快照推进策略",
        "websocket": "按 WebSocket 实时报价事件推进策略",
    }.get(mode, f"按 {mode or '当前账户'} 行情模式推进策略")


def build_algorithm_profile(
    strategy: dict[str, Any],
    account: dict[str, Any],
) -> dict[str, Any]:
    """Return strategy-specific behavior plus the account's real runtime contract."""
    source = str(strategy.get("source") or "")
    _kind, doc = _source_identity(source)
    profile = _PROFILES.get(_profile_key(strategy, source))
    if profile is None:
        callback = str(account.get("execution_mode") or "full_bar")
        callback_text = {
            "quote": "每次收到报价时执行 on_quote 回调并更新订单",
            "scheduled": "只在策略注册的定时点计算信号和订单",
            "full_bar": "每根闭合 K 线执行 on_bar 回调并更新订单",
        }.get(callback, "按策略注册的行情回调执行")
        profile = {
            "summary": doc.splitlines()[0] if doc else "该策略按照源码中注册的行情回调、定时任务和交易规则运行。",
            "inputs": [_mode_text(account)],
            "steps": [
                {"title": "行情输入", "detail": _mode_text(account)},
                {"title": "信号计算", "detail": callback_text},
                {"title": "订单执行", "detail": "策略生成目标仓位或买卖委托，公共引擎统一处理可成交性、费用、滑点和资金约束。"},
            ],
            "parameters": [],
            "pseudocode": [
                "receive market event or scheduled callback",
                "calculate signals using data visible at the callback time",
                "submit target positions or orders to the shared execution engine",
            ],
        }

    config = account.get("config") if isinstance(account.get("config"), dict) else {}
    risk = account.get("risk_config") if isinstance(account.get("risk_config"), dict) else {}
    schedules = [str(value) for value in account.get("scheduled_times") or []]
    settlement = {"t0": "T+0", "t1": "T+1"}.get(
        str(config.get("settlement") or "t1").lower(),
        str(config.get("settlement") or "t1").upper(),
    )
    fill_policy = "当前行情" if config.get("fill_policy") == "close" else "下一笔可成交行情"
    runtime = [
        _mode_text(account),
        f"成交使用{fill_policy}，结算规则为 {settlement}，滑点 {float(config.get('slippage_bps') or 0):g} bps",
    ]
    if schedules:
        runtime.append(f"定时触发点：{', '.join(schedules)}")
    if risk:
        runtime.append(
            "账户风控：单标的上限 "
            f"{float(risk.get('max_symbol_exposure_pct') or 0) * 100:g}%，"
            f"单日亏损锁定 {float(risk.get('daily_loss_pct') or 0) * 100:g}%，"
            f"最大回撤锁定 {float(risk.get('max_drawdown_pct') or 0) * 100:g}%"
        )
    return {
        "summary": str(profile["summary"]),
        "inputs": [str(item) for item in profile.get("inputs", [])],
        "steps": [dict(item) for item in profile["steps"]],
        "parameters": [str(item) for item in profile.get("parameters", [])],
        "pseudocode": [str(item) for item in profile.get("pseudocode", [])],
        "runtime": runtime,
    }
