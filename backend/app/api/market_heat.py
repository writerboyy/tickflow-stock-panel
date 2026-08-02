"""市场热度与飙升雷达 API。"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.plugins.hithink.client import HiThinkApiError, HiThinkAuthError
from app.services.market_heat import build_market_heat_radar, build_market_heat_rank_trend

router = APIRouter(prefix="/api/market-heat", tags=["market-heat"])


@router.get("/radar")
def market_heat_radar(
    trend_days: int = Query(30, ge=7, le=90, description="热股榜代表股票排名趋势自然日窗口"),
):
    try:
        return build_market_heat_radar(trend_days=trend_days)
    except HiThinkAuthError as exc:
        raise HTTPException(
            status_code=503,
            detail="未配置同花顺/Fuyao API Key，无法获取热股与飙升榜。",
        ) from exc
    except HiThinkApiError as exc:
        status = 403 if exc.code in {2001, 2003} else 502
        request_id = f" request_id={exc.request_id}" if exc.request_id else ""
        raise HTTPException(
            status_code=status,
            detail=f"同花顺/Fuyao 特色数据请求失败：{exc.message or exc.code}{request_id}",
        ) from exc


@router.get("/trend")
def market_heat_rank_trend(
    thscode: str = Query(..., min_length=3, description="单只股票 thscode"),
    ticker: str | None = Query(None, description="股票代码，用于展示"),
    name: str | None = Query(None, description="股票名称，用于展示"),
    trend_days: int = Query(30, ge=7, le=90, description="热股排名趋势自然日窗口"),
):
    try:
        return build_market_heat_rank_trend(
            thscode=thscode,
            ticker=ticker or "",
            name=name or "",
            trend_days=trend_days,
        )
    except HiThinkAuthError as exc:
        raise HTTPException(
            status_code=503,
            detail="未配置同花顺/Fuyao API Key，无法获取热股排名趋势。",
        ) from exc
    except HiThinkApiError as exc:
        status = 403 if exc.code in {2001, 2003} else 502
        request_id = f" request_id={exc.request_id}" if exc.request_id else ""
        raise HTTPException(
            status_code=status,
            detail=f"同花顺/Fuyao 热股排名趋势请求失败：{exc.message or exc.code}{request_id}",
        ) from exc
