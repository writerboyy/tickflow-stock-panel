"""QMT ZMQ provider for stock Tick history."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from app.config import settings
from app.data_providers.base import AssetType, ProviderCapabilities
from app.data_providers.normalizer import normalize_tick
from app.services.qmt_trading import QmtZmqRpcClient

_CN_TZ = ZoneInfo("Asia/Shanghai")


class QmtProvider:
    name = "qmt"
    capabilities = ProviderCapabilities(tick=True)

    def __init__(self, client: Any | None = None) -> None:
        self.client = client or QmtZmqRpcClient(settings)

    def get_tick(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",
    ) -> pl.DataFrame:
        if asset_type != "stock":
            raise ValueError("QMT Tick 导入首版仅支持股票")
        if not symbols:
            return pl.DataFrame()
        if start_time is None or end_time is None:
            raise ValueError("QMT Tick 导入必须指定开始和结束时间")
        start_text = start_time.strftime("%Y%m%d")
        end_text = end_time.strftime("%Y%m%d")
        frames: list[pl.DataFrame] = []
        for symbol in symbols:
            self.client.call("download_history_data", {
                "stock_code": symbol,
                "period": "tick",
                "start_time": start_text,
                "end_time": end_text,
            })
            raw = self.client.call("get_market_data_ex", {
                "field_list": [],
                "stock_list": [symbol],
                "period": "tick",
                "start_time": start_text,
                "end_time": end_text,
                "count": -1,
                "fill_data": False,
            })
            frame = normalize_tick(raw, default_symbol=symbol, source=self.name)
            if not frame.is_empty():
                frames.append(frame)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        raw = self.client.call("get_trading_dates", {
            "market": "SH",
            "start_time": start.strftime("%Y%m%d"),
            "end_time": end.strftime("%Y%m%d"),
            "count": -1,
        })
        result: list[date] = []
        for value in raw or []:
            parsed: date | None = None
            if isinstance(value, datetime):
                parsed = value.date()
            elif isinstance(value, date):
                parsed = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                number = float(value)
                if number >= 1_000_000_000_000:
                    parsed = datetime.fromtimestamp(
                        number / 1000, timezone.utc,
                    ).astimezone(_CN_TZ).date()
                elif number >= 1_000_000_000:
                    parsed = datetime.fromtimestamp(
                        number, timezone.utc,
                    ).astimezone(_CN_TZ).date()
                else:
                    value = str(int(number))
            if parsed is None:
                text = str(value or "").strip()[:10].replace("-", "")
                try:
                    parsed = datetime.strptime(text, "%Y%m%d").date()
                except ValueError:
                    continue
            if start <= parsed <= end:
                result.append(parsed)
        return sorted(set(result))

    def get_instruments(self, asset_type: AssetType) -> pl.DataFrame:  # noqa: ARG002
        return pl.DataFrame()

    def get_daily(self, *args, **kwargs) -> pl.DataFrame:  # noqa: ANN002, ANN003, ARG002
        return pl.DataFrame()

    def get_adj_factors(self, *args, **kwargs) -> pl.DataFrame:  # noqa: ANN002, ANN003, ARG002
        return pl.DataFrame()

    def get_minute(self, *args, **kwargs) -> pl.DataFrame:  # noqa: ANN002, ANN003, ARG002
        return pl.DataFrame()

    def get_realtime(self, *args, **kwargs) -> pl.DataFrame:  # noqa: ANN002, ANN003, ARG002
        return pl.DataFrame()
