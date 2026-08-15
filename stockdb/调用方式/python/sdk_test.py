#/pybao/stock_sdk.py
from stock_sdk import *

############
#【方式1】一秒启动方案（体验远程数据,去掉"""即可）
############

"""
init("8.138.149.215",7899)
set_init("8.138.149.215:12328")
print("start run online")
"""

#[+]注意：远程测试符合需求，你应该使用【方式2】本地数据，暴力拉取5分钟，将会被永久封禁设备(本地也无法使用) [+]
#[+]注意：远程测试符合需求，你应该使用【方式2】本地数据，暴力拉取5分钟，将会被永久封禁设备(本地也无法使用) [+]
#[+]注意：远程测试符合需求，你应该使用【方式2】本地数据，暴力拉取5分钟，将会被永久封禁设备(本地也无法使用) [+]

############
#【方式2】本地启动方案【核心】(本地数据)
# 更新 并启动 stockdb.exe 然后直接运行本文件，接口不变。
############


#1、原生rd
print("原生rd日:",rd.get("日k",'600633','20260727>20260729'))
print("原生分钟:",rd.get("分钟k",'600633','20260727093*'))


#2、板块映射
print("\n板块映射",bk.get('600633')[-10:])

#3、标的
d=rd.get("股票代码")
hs=[v for v in d['6'] if '60'==v[:2]]+d['0'] #3100只
print("\n标的",hs[:40])

#4、批量查询:
k = rd.get_data(
    hs[:3],
    start="20260713",
    end="20260715",
    frequency="1d",
    fq="qfq"
)
print("\n批量查询",k)

#5、批量指标
macd = zb.get(["macd","kdj"],
    hs[10:13],
    start="20260710",
    end="20260725",
    frequency="1d",
    fq="qfq"
)

print("\n批量指标",macd)

#6、财务api(在线)
df=get_fundamentals(query(cash_flow).filter(cash_flow.code == '000001.XSHE'), statDate='2024q4')
print("\n财务api(在线)",df)


#7、在线tick行情
t=get_last_tick('000001',count=10)
print("\n在线tick行情",t or 1)


#8、保存私有数据 ./mydb
t=rd.set('我的数据','600633','20270101',{"value支持":1,"dict":{'1':1},"list":[1,1],"int":1,"str":"1"})
print("\n保存私有数据",t or 1)


#9、读取私有数据 ./mydb
t=rd.get('我的数据','600633','20270101')
print("\n读取私有数据",t or {"value支持":1,"dict":{'1':1},"list":[1,1],"int":1,"str":"1"})

#[from stock_sdk import * 即可专注你的策略]

"""
#参数说明：
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


"""
##异步##get_data_async

k = await rd.get_data_async(.....)


##异步##原生支持rd.xxx同异步

k = await rd.vals("日k",'600633',"202605*")
"""