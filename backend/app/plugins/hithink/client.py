"""Minimal HiThink/Fuyao REST client for supplemental reference snapshots."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.config import settings


DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
DEFAULT_TIMEOUT_SECONDS = 30


class HiThinkAuthError(RuntimeError):
    """Raised when no HiThink API key is configured."""


class HiThinkApiError(RuntimeError):
    """Raised when the HiThink API returns a non-zero business code."""

    def __init__(self, code: int, message: str, request_id: str | None = None) -> None:
        super().__init__(f"[hithink code={code}] {message} (request_id={request_id})")
        self.code = code
        self.message = message
        self.request_id = request_id


@dataclass(frozen=True)
class HiThinkClient:
    api_key: str | None = None
    base_url: str | None = None
    timeout: int = DEFAULT_TIMEOUT_SECONDS

    def _api_key(self) -> str:
        value = (
            self.api_key
            or settings.hithink_finance_api_key
            or os.getenv("HITHINK_FINANCE_API_KEY")
            or os.getenv("FUYAO_TOKEN")
            or os.getenv("API_KEY")
            or ""
        ).strip()
        if not value:
            raise HiThinkAuthError(
                "HITHINK_FINANCE_API_KEY is required for HiThink supplemental snapshots"
            )
        return value

    def _base_url(self) -> str:
        return (self.base_url or settings.hithink_finance_base_url or DEFAULT_BASE_URL).rstrip("/")

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        clean_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None and value != ""
        }
        query = f"?{urlencode(clean_params)}" if clean_params else ""
        request = Request(
            f"{self._base_url()}{path}{query}",
            headers={"X-api-key": self._api_key()},
            method="GET",
        )
        with urlopen(request, timeout=self.timeout) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))
        code = int(payload.get("code", -1))
        if code != 0:
            raise HiThinkApiError(
                code=code,
                message=str(payload.get("message") or ""),
                request_id=payload.get("request_id"),
            )
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    def get_index_constituents(self, thscode: str) -> dict[str, Any]:
        return self.get(
            "/api/a-share-index/constituents/ths-stock-list",
            {"thscode": thscode},
        )

    def get_ths_index_list(self, tag: str) -> dict[str, Any]:
        return self.get("/api/a-share-index/catalog/ths-index-list", {"tag": tag})

    def list_tickers(
        self,
        *,
        exchange: str = "SH,SZ,BJ",
        asset_type: str = "a-share",
        limit: int = 10000,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            data = self.get(
                "/api/meta/tickers/list",
                {
                    "exchange": exchange,
                    "asset_type": asset_type,
                    "limit": limit,
                    "offset": offset,
                },
            )
            items = data.get("item") or []
            if not isinstance(items, list):
                raise ValueError("HiThink tickers list returned non-list item payload")
            rows.extend(item for item in items if isinstance(item, dict))
            if len(items) < limit:
                return rows
            offset += limit

