"""Minimal HiThink/Fuyao REST client for supplemental reference snapshots."""

from __future__ import annotations

import json
import os
import ssl
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.config import settings


DEFAULT_BASE_URL = "https://fuyao.aicubes.cn"
DEFAULT_TIMEOUT_SECONDS = 30
API_KEY_NAMES = ("HITHINK_FINANCE_API_KEY", "FUYAO_TOKEN", "API_KEY")


class HiThinkAuthError(RuntimeError):
    """Raised when no HiThink API key is configured."""


class HiThinkApiError(RuntimeError):
    """Raised when the HiThink API returns a non-zero business code."""

    def __init__(self, code: int, message: str, request_id: str | None = None) -> None:
        super().__init__(f"[hithink code={code}] {message} (request_id={request_id})")
        self.code = code
        self.message = message
        self.request_id = request_id


def _credentials_env_paths() -> tuple[Path, ...]:
    if sys.platform == "win32":
        appdata = os.getenv("APPDATA")
        return (Path(appdata) / "hithink-finance" / "credentials.env",) if appdata else ()
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "hithink-finance"
            / "credentials.env",
        )
    config_home = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
    return (config_home / "hithink-finance" / "credentials.env",)


def _read_credentials_env_api_key() -> str:
    for path in _credentials_env_paths():
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            continue
        except OSError:
            continue
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() in API_KEY_NAMES:
                return value.strip().strip('"').strip("'")
    return ""


def _ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:  # noqa: BLE001
        return ssl.create_default_context()


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
            or _read_credentials_env_api_key()
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
        with urlopen(request, timeout=self.timeout, context=_ssl_context()) as response:  # noqa: S310
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

    def get_hot_stock_list(self, period: str = "day") -> dict[str, Any]:
        return self.get("/api/a-share/special-data/hot-stock-list", {"period": period})

    def get_skyrocket_list(self, period: str = "day") -> dict[str, Any]:
        return self.get("/api/a-share/special-data/skyrocket-list", {"period": period})

    def get_hot_stock_rank_trend(
        self,
        thscode: str,
        *,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        return self.get(
            "/api/a-share/special-data/hot-stock-rank-trend",
            {"thscode": thscode, "start_date": start_date, "end_date": end_date},
        )

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
