"""HTTP CONNECT proxy support for BaoStock's raw TCP socket client."""

from __future__ import annotations

import os
import socket
from collections.abc import Iterable, Mapping
from urllib.parse import urlparse


_TRUTHY = {"1", "true", "yes", "y", "on"}


def force_proxy_enabled(env: Mapping[str, str] | None = None) -> bool:
    value = (env or os.environ).get("BAOSTOCK_FORCE_PROXY", "")
    return value.strip().casefold() in _TRUTHY


def iter_proxy_candidates(env: Mapping[str, str] | None = None) -> Iterable[str]:
    values = env or os.environ
    seen: set[str] = set()
    for key in ("BAOSTOCK_PROXY_URL", "ALL_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        value = values.get(key, "").strip()
        if value and value not in seen:
            seen.add(value)
            yield value

    icube_host = values.get("ICUBE_PROXY_HOST", "").strip()
    if icube_host:
        candidate = f"http://{icube_host}:7890"
        if candidate not in seen:
            yield candidate


def parse_http_proxy_url(proxy_url: str) -> tuple[str, int] | None:
    raw = proxy_url.strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = f"http://{raw}"
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "http").casefold()
    if scheme != "http" or not parsed.hostname:
        return None
    return parsed.hostname, parsed.port or 80


def _recv_until_header_end(sock: socket.socket, max_bytes: int = 8192) -> bytes:
    data = b""
    while b"\r\n\r\n" not in data and len(data) < max_bytes:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


def open_baostock_proxy_connection(
    target_host: str,
    target_port: int,
    *,
    timeout: float,
    proxy_candidates: Iterable[str] | None = None,
) -> socket.socket:
    last_error: Exception | None = None
    for proxy_url in proxy_candidates or iter_proxy_candidates():
        parsed = parse_http_proxy_url(proxy_url)
        if parsed is None:
            continue
        proxy_host, proxy_port = parsed
        sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
        try:
            request = (
                f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
                f"Host: {target_host}:{target_port}\r\n"
                "Proxy-Connection: Keep-Alive\r\n"
                "\r\n"
            )
            sock.sendall(request.encode("ascii"))
            response = _recv_until_header_end(sock)
            first_line = response.split(b"\r\n", 1)[0].decode(
                "latin1",
                errors="replace",
            )
            if " 200 " not in first_line:
                raise OSError(f"BaoStock proxy CONNECT failed: {first_line or 'empty response'}")
            sock.settimeout(timeout)
            return sock
        except Exception as exc:
            last_error = exc
            sock.close()
            continue
    if last_error is not None:
        raise last_error
    raise OSError("No HTTP proxy configured for BaoStock")
