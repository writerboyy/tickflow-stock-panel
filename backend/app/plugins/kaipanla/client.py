"""固定端点的开盘啦 HTTP 客户端。"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from app.plugins.kaipanla.credentials import KaipanlaCredentials, load_credentials

_PATH = "/w1/api/index.php"
_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 14; V2178A Build/UP1A.231005.007)",
}

_ROUTES: dict[int | str, tuple[str, str, dict[str, str]]] = {
    13: (
        "POST",
        "apphq.longhuvip.com",
        {
            "Order": "0",
            "a": "GetStockDaDanTrendIncremental",
            "c": "StockL2Data",
            "Index": "0",
            "Money": "2",
            "IsBS": "0",
            "apiv": "w31",
        },
    ),
    14: (
        "POST",
        "apphq.longhuvip.com",
        {
            "a": "GetWeiTuo_W14",
            "c": "StockL2Data",
            "st": "3000",
            "Tur": "30",
            "Type": "2",
            "Vol": "500",
            "VType": "1",
            "VOrder": "0",
            "apiv": "w39",
        },
    ),
    115: (
        "POST",
        "apphwhq.longhuvip.com",
        {
            "Order": "1",
            "a": "MorningBiddingList",
            "c": "HomeDingPan",
            "PidType": "0",
            "Type": "4",
        },
    ),
    30: (
        "POST",
        "apphis.longhuvip.com",
        {
            "Order": "1",
            "a": "MorningBiddingList",
            "c": "HisHomeDingPan",
            "PidType": "0",
            "Type": "4",
        },
    ),
    31: ("POST", "apphwhq.longhuvip.com", {"a": "GetStockBid", "c": "StockL2Data"}),
    15: (
        "POST",
        "apphwshhq.longhuvip.com",
        {
            "a": "GetPlateInfo_w38",
            "c": "DailyLimitResumption",
        },
    ),
    "limit_up_expression": (
        "POST",
        "apphis.longhuvip.com",
        {
            "a": "ZhangTingExpression",
            "c": "HisHomeDingPan",
        },
    ),
    "rise_fall_analysis": (
        "GET",
        "apphwshhq.longhuvip.com",
        {
            "a": "RiseFallAnalysis",
            "c": "HomeDingPan",
            "apiv": "w43",
            "VerSion": "5.22.0.2",
        },
    ),
    "market_performance": (
        "GET",
        "apphwshhq.longhuvip.com",
        {
            "a": "GetPlate_Info_QJ",
            "c": "ZhiShuRanking",
            "apiv": "w42",
            "VerSion": "5.21.0.2",
            "Date": "",
        },
    ),
    "limit_up_ladder": (
        "POST",
        "apphwhq.longhuvip.com",
        {
            "a": "GetYTFP_SCTD",
            "c": "FuPanLa",
        },
    ),
    76: (
        "POST",
        "apphwhq.longhuvip.com",
        {
            "a": "GetZhangTingGene",
            "apiv": "w42",
            "c": "StockL2Data",
            "PhoneOSNew": "1",
            "VerSion": "5.21.0.0",
        },
    ),
    100: (
        "POST",
        "applhb.longhuvip.com",
        {
            "a": "GetStockList",
            "c": "LongHuBang",
            "Type": "2",
        },
    ),
    101: (
        "POST",
        "applhb.longhuvip.com",
        {
            "a": "GetNewOneStockInfo",
            "c": "Stock",
            "Type": "0",
        },
    ),
    108: (
        "POST",
        "apphwshhq.longhuvip.com",
        {
            "a": "GetYDTP_ZDJK_Today",
            "c": "StockBidYiDong",
        },
    ),
    109: (
        "POST",
        "apphwshhq.longhuvip.com",
        {
            "a": "GetPianLiZhi_Many",
            "c": "StockBidYiDong",
        },
    ),
    "fund_interval": (
        "POST",
        "apphis.longhuvip.com",
        {
            "a": "GetInterviewsByDateStock",
            "c": "StockLineData",
            "Type": "2",
            "FilterBJS": "1",
            "Order": "1",
        },
    ),
    "fund_capital_net": (
        "POST",
        "apphis.longhuvip.com",
        {
            "a": "GetMainMonitor_Trend_w30",
            "c": "StockL2History",
            "Money": "0",
            "IsBS": "0",
            "apiv": "w42",
        },
    ),
    "fund_large_order_statistics": (
        "POST",
        "apphis.longhuvip.com",
        {"a": "GetDaDanKLine2", "c": "StockLineData"},
    ),
    "northbound_sector_latest": (
        "GET",
        "apphis.longhuvip.com",
        {
            "a": "GGList_BXZJ",
            "c": "ZhuLiChiCang",
            "Order": "1",
            "st": "20",
            "Index": "0",
            "Date": "",
            "Type": "1",
            "apiv": "w44",
            "VerSion": "5.23.0.4",
        },
    ),
    "northbound_sector_history": (
        "GET",
        "apphis.longhuvip.com",
        {
            "a": "GGList_BXZJ",
            "c": "ZhuLiChiCang",
            "Order": "1",
            "st": "20",
            "Index": "0",
            "Type": "1",
            "apiv": "w44",
            "VerSion": "5.23.0.4",
        },
    ),
    "northbound_stocks_latest": (
        "GET",
        "apphis.longhuvip.com",
        {
            "a": "GGList_BXZJ_Stocks",
            "c": "ZhuLiChiCang",
            "Order": "1",
            "st": "20",
            "Index": "0",
            "Date": "",
            "Type": "1",
            "apiv": "w44",
            "VerSion": "5.23.0.4",
        },
    ),
    "northbound_stocks_history": (
        "GET",
        "apphis.longhuvip.com",
        {
            "a": "GGList_BXZJ_Stocks",
            "c": "ZhuLiChiCang",
            "Order": "1",
            "st": "20",
            "Index": "0",
            "Type": "1",
            "apiv": "w44",
            "VerSion": "5.23.0.4",
        },
    ),
    "shareholder_changes": (
        "POST",
        "apphis.longhuvip.com",
        {"a": "GetGuDongInfoTenByDate", "c": "YiDianCangWei"},
    ),
    "shareholder_count_changes": (
        "GET",
        "applhb.longhuvip.com",
        {
            "a": "GuDongRenShu",
            "c": "YiDianCangWei",
            "Order": "10",
            "ShowDate": "0",
            "IsNew": "0",
            "JiangFu": "1",
            "Tag": "0",
            "apiv": "w44",
            "VerSion": "5.23.0.4",
        },
    ),
    "dragon_tiger_movement": (
        "POST",
        "apphis.longhuvip.com",
        {"a": "GetYTFP_LHBDX", "c": "FuPanLa"},
    ),
    "dragon_tiger_details": (
        "POST",
        "applhb.longhuvip.com",
        {"a": "GetNewOneStockInfo", "c": "Stock", "Type": "0"},
    ),
    "sector_strength": (
        "POST",
        "apphq.longhuvip.com",
        {"a": "RealRankingInfo", "c": "ZhiShuRanking", "Type": "1", "Order": "1", "ZSType": "7"},
    ),
    "sector_constituents": (
        "POST",
        "apphis.longhuvip.com",
        {
            "a": "ZhiShuStockList_W8",
            "c": "ZhiShuRanking",
            "Order": "1",
            "st": "1000",
            "Index": "0",
            "old": "1",
            "Type": "6",
            "IsZZ": "0",
            "IsKZZType": "0",
            "TSZB": "0",
            "TSZB_Type": "0",
            "filterType": "0",
        },
    ),
}


class KaipanlaRequestError(RuntimeError):
    """不包含请求 URL 或凭据的上游请求错误。"""


def _redact_payload(value: Any, credentials: KaipanlaCredentials) -> Any:
    secret_names = {"token", "userid", "deviceid"}
    secret_values = (credentials.token, credentials.userid, credentials.deviceid)
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if str(key).casefold() in secret_names
            else _redact_payload(item, credentials)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_payload(item, credentials) for item in value]
    if isinstance(value, str):
        result = value
        for secret in secret_values:
            if secret and len(secret) >= 6:
                result = result.replace(secret, "[REDACTED]")
        return result
    return value


class KaipanlaClient:
    def __init__(
        self,
        credentials: KaipanlaCredentials | None = None,
        http_client: httpx.AsyncClient | None = None,
        attempts: int = 3,
    ) -> None:
        self.credentials = credentials or load_credentials()
        if self.credentials is None:
            raise KaipanlaRequestError("开盘啦凭据未配置")
        self._client = http_client or httpx.AsyncClient(timeout=15.0, follow_redirects=False)
        self._owns_client = http_client is None
        self._attempts = max(1, attempts)

    async def __aenter__(self) -> KaipanlaClient:
        return self

    async def __aexit__(self, *_args) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def request(self, endpoint: int | str, params: dict[str, object] | None = None) -> dict:
        route = _ROUTES.get(endpoint)
        if route is None:
            raise ValueError(f"不支持的开盘啦接口: /{endpoint}")
        method, host, base_params = route
        query = {**base_params, **{key: str(value) for key, value in (params or {}).items()}}
        url = f"https://{host}{_PATH}"

        for attempt in range(1, self._attempts + 1):
            try:
                if method == "GET":
                    response = await self._client.request(
                        method,
                        url,
                        params={**query, **self.credentials.as_form()},
                        headers=_HEADERS,
                    )
                else:
                    response = await self._client.request(
                        method,
                        url,
                        params=query,
                        data=self.credentials.as_form(),
                        headers=_HEADERS,
                    )
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt == self._attempts:
                    raise KaipanlaRequestError(f"开盘啦 /{endpoint} 请求失败") from None
                await asyncio.sleep(0.5 * attempt)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt < self._attempts:
                    await asyncio.sleep(0.5 * attempt)
                    continue
            if response.status_code != 200:
                raise KaipanlaRequestError(f"开盘啦 /{endpoint} 返回 HTTP {response.status_code}")
            try:
                payload = response.json()
            except ValueError:
                raise KaipanlaRequestError(f"开盘啦 /{endpoint} 返回非 JSON 数据") from None
            if not isinstance(payload, dict):
                raise KaipanlaRequestError(f"开盘啦 /{endpoint} 返回结构无效")
            payload = _redact_payload(payload, self.credentials)
            errcode = payload.get("errcode")
            if errcode not in (None, 0, "0"):
                raise KaipanlaRequestError(f"开盘啦 /{endpoint} 返回业务错误")
            return payload

        raise KaipanlaRequestError(f"开盘啦 /{endpoint} 请求失败")
