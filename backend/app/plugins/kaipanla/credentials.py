"""开盘啦连接凭据解析与本地存储。"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

from app import secrets_store

ALLOWED_HOSTS = frozenset(
    {
        "apphwhq.longhuvip.com",
        "apphis.longhuvip.com",
        "apphwshhq.longhuvip.com",
        "applhb.longhuvip.com",
        "apphq.longhuvip.com",
    }
)
EXPECTED_PATH = "/w1/api/index.php"

_PARAMS = {
    "token": "Token",
    "userid": "UserID",
    "deviceid": "DeviceID",
    "phoneosnew": "PhoneOSNew",
    "version": "VerSion",
    "apiv": "apiv",
}
SECRET_KEYS = tuple(f"kaipanla_{name}" for name in _PARAMS)


@dataclass(frozen=True)
class KaipanlaCredentials:
    token: str
    userid: str
    deviceid: str
    phoneosnew: str
    version: str
    apiv: str

    def as_form(self) -> dict[str, str]:
        return {
            "Token": self.token,
            "UserID": self.userid,
            "DeviceID": self.deviceid,
            "PhoneOSNew": self.phoneosnew,
            "VerSion": self.version,
            "apiv": self.apiv,
        }

    def as_secret_updates(self) -> dict[str, str]:
        return {f"kaipanla_{name}": getattr(self, name) for name in _PARAMS}


def parse_authorized_url(source_url: str) -> KaipanlaCredentials:
    """只从已知开盘啦主机的标准接口 URL 中提取公共连接字段。"""
    value = source_url.strip()
    if not value or len(value) > 4096:
        raise ValueError("授权 URL 为空或过长")

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("授权 URL 格式无效") from exc

    if parsed.scheme != "https":
        raise ValueError("授权 URL 必须使用 HTTPS")
    if parsed.hostname not in ALLOWED_HOSTS or port not in (None, 443):
        raise ValueError("授权 URL 主机不在开盘啦白名单中")
    if parsed.username or parsed.password or parsed.path != EXPECTED_PATH or parsed.fragment:
        raise ValueError("授权 URL 路径或结构无效")

    query = {key.casefold(): value.strip() for key, value in parse_qsl(parsed.query)}
    missing = [public for key, public in _PARAMS.items() if not query.get(key)]
    if missing:
        raise ValueError(f"授权 URL 缺少连接字段: {', '.join(missing)}")

    return KaipanlaCredentials(**{name: query[name] for name in _PARAMS})


def load_credentials() -> KaipanlaCredentials | None:
    raw = secrets_store.load()
    values = {name: str(raw.get(f"kaipanla_{name}") or "").strip() for name in _PARAMS}
    if not all(values.values()):
        return None
    return KaipanlaCredentials(**values)


def save_authorized_url(source_url: str) -> KaipanlaCredentials:
    credentials = parse_authorized_url(source_url)
    secrets_store.save(credentials.as_secret_updates())
    return credentials


def clear_credentials() -> None:
    secrets_store.clear(*SECRET_KEYS)


def credential_status(credentials: KaipanlaCredentials | None = None) -> dict:
    current = credentials or load_credentials()
    if current is None:
        return {
            "configured": False,
            "token_masked": "",
            "user_id_masked": "",
            "device_id_masked": "",
        }
    return {
        "configured": True,
        "token_masked": secrets_store.mask(current.token),
        "user_id_masked": secrets_store.mask(current.userid, prefix=2, suffix=2),
        "device_id_masked": secrets_store.mask(current.deviceid),
    }
