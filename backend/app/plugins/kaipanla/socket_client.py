"""Read current Kaipanla board constituents from the vendor socket protocol."""
from __future__ import annotations

import socket
import ssl
import struct
from collections.abc import Iterable
from typing import Any


_HOST = "hwsockapp.longhuvip.com"
_PORT = 14000
_PAGE_SIZE = 30
_MAX_PAGES = 100


class KaipanlaSocketError(RuntimeError):
    """Socket requests fail without including login material in the error."""


def _finite(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _receive_packet(sock: ssl.SSLSocket) -> bytes | None:
    header = b""
    while len(header) < 3:
        chunk = sock.recv(3 - len(header))
        if not chunk:
            return None
        header += chunk
    body_length = struct.unpack(">H", header[1:3])[0]
    body = b""
    while len(body) < body_length:
        chunk = sock.recv(body_length - len(body))
        if not chunk:
            return None
        body += chunk
    return header + body


def _parse_packet(packet: bytes, plate_id: str) -> list[dict[str, Any]]:
    if len(packet) < 3 or packet[0] != 0x40:
        raise KaipanlaSocketError("开盘啦板块成分响应类型无效")
    body = packet[3:]
    start = body.find(b"\x0A\x06")
    if start < 0:
        return []
    values: list[dict[str, Any]] = []
    position = start
    while position < len(body):
        position = body.find(b"\x0A\x06", position)
        if position < 0:
            break
        index = position + 1
        if index >= len(body):
            break
        code_size = body[index]
        index += 1
        if index + code_size > len(body):
            break
        code = body[index:index + code_size].decode("utf-8", errors="ignore")
        index += code_size + 1
        if index >= len(body):
            break
        name_size = body[index]
        index += 1
        if index + name_size > len(body):
            break
        name = body[index:index + name_size].decode("utf-8", errors="ignore")
        next_position = body.find(b"\x0A\x06", position + 2)
        end = next_position if next_position >= 0 else len(body)
        fields: list[str] = []
        field_position = body.find(b"\xA2\x06", position + 2)
        while field_position >= 0 and field_position < end:
            field_index = field_position + 2
            if field_index >= len(body):
                break
            field_size = body[field_index]
            field_index += 1
            if field_index + field_size > len(body):
                break
            fields.append(body[field_index:field_index + field_size].decode("utf-8", errors="ignore"))
            field_position = body.find(b"\xA2\x06", field_index + field_size)
        if code and code != plate_id:
            price = _finite(fields[1] if len(fields) > 1 else None)
            change_pct = _finite(fields[2] if len(fields) > 2 else None)
            values.append({
                "plate_id": plate_id,
                "code": code,
                "symbol": code,
                "name": name,
                "tags": fields[0] if fields else None,
                "last_price": price,
                "change_pct": change_pct / 100 if change_pct is not None else None,
                "amount": _finite(fields[3] if len(fields) > 3 else None),
                "turnover_rate": (
                    _finite(fields[4]) / 100 if len(fields) > 4 and _finite(fields[4]) is not None else None
                ),
                "main_net": _finite(fields[9] if len(fields) > 9 else None),
                "limit_tag": fields[18] if len(fields) > 18 else None,
            })
        position = end
    return values


def _page_command(plate_id: str, page_index: int) -> bytes:
    code = plate_id.encode("ascii", errors="strict").hex().upper()
    parts = ["40", "00", "18", "00", "6A", "09", "C5", "00", "00", "03", "02", "09", "C5", "0A", "06", code, "10", "06", "18", "01"]
    if page_index:
        offset = 0x11 + (page_index - 1) * _PAGE_SIZE
        encoded: list[str] = []
        while True:
            value = offset & 0x7F
            offset >>= 7
            encoded.append(f"{value | 0x80:02X}" if offset else f"{value:02X}")
            if not offset:
                break
        parts[2] = f"{0x18 + 1 + len(encoded):02X}"
        parts.extend(["30", *encoded])
    parts.extend(["38", f"{_PAGE_SIZE:02X}"])
    return bytes.fromhex(" ".join(parts))


class KaipanlaSocketClient:
    def __init__(self, login_packet: bytes) -> None:
        self._login_packet = login_packet

    def fetch_blocks(self, plate_ids: Iterable[str]) -> dict[str, list[dict[str, Any]]]:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        raw_sock = socket.create_connection((_HOST, _PORT), timeout=15)
        sock = context.wrap_socket(raw_sock, server_hostname=_HOST)
        sock.settimeout(15)
        try:
            sock.sendall(self._login_packet)
            for _ in range(10):
                packet = _receive_packet(sock)
                if packet and packet[0] in (0x30, 0x60):
                    break
            else:
                raise KaipanlaSocketError("开盘啦 socket 登录失败")
            return {plate_id: self._fetch_block(sock, plate_id) for plate_id in plate_ids}
        except (OSError, ssl.SSLError) as exc:
            raise KaipanlaSocketError("开盘啦 socket 连接失败") from exc
        finally:
            sock.close()

    @staticmethod
    def _fetch_block(sock: ssl.SSLSocket, plate_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page_index in range(_MAX_PAGES):
            sock.sendall(_page_command(plate_id, page_index))
            packet = None
            for _ in range(10):
                candidate = _receive_packet(sock)
                if candidate and candidate[0] == 0x40:
                    packet = candidate
                    break
            if packet is None:
                raise KaipanlaSocketError("开盘啦板块成分响应超时")
            parsed = _parse_packet(packet, plate_id)
            fresh = [row for row in parsed if row["code"] not in seen]
            if not fresh:
                break
            seen.update(row["code"] for row in fresh)
            rows.extend(fresh)
            if len(parsed) < _PAGE_SIZE:
                break
        else:
            raise KaipanlaSocketError("开盘啦板块成分分页超过安全上限")
        return rows
