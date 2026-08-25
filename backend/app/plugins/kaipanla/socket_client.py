"""Read current Kaipanla board constituents from the vendor socket protocol."""
from __future__ import annotations

import socket
import ssl
import struct
from collections.abc import Iterable
import re
from typing import Any


_HOST = "hwsockapp.longhuvip.com"
_PORT = 14000
_PAGE_SIZE = 30
_MAX_PAGES = 100
_JIJIANG_READ_TIMEOUT = 0.35
_JIJIANG_MAX_PAGES = 20
_JIJIANG_MAX_PACKETS = 128
_JIJIANG_CMD_FIRST = bytes.fromhex(
    "40 00 12 00 25 08 37 08 00 03 02 08 37 10 04 18 01 28 1E 38 01"
)


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


def _receive_packet_with_timeout(
    sock: ssl.SSLSocket,
    timeout: float,
) -> bytes | None:
    sock.settimeout(timeout)
    try:
        return _receive_packet(sock)
    except socket.timeout:
        return None


def _encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint cannot encode negative value")
    encoded = bytearray()
    while True:
        current = value & 0x7F
        value >>= 7
        encoded.append(current | 0x80 if value else current)
        if not value:
            return bytes(encoded)


def _jijiang_command(offset: int) -> bytes:
    if offset <= 0:
        return _JIJIANG_CMD_FIRST
    body = _JIJIANG_CMD_FIRST[3:]
    marker = b"\x18\x01\x28\x1E"
    marker_index = body.find(marker)
    if marker_index < 0:
        return _JIJIANG_CMD_FIRST
    prefix = body[: marker_index + 2]
    suffix = body[marker_index + 2 :]
    new_body = prefix + b"\x20" + _encode_varint(offset) + suffix
    return b"\x40" + struct.pack(">H", len(new_body)) + new_body


def _parse_number(value: object) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text or text in {"-", "--", "—"}:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    if "亿" in text:
        number *= 100_000_000
    elif "万" in text:
        number *= 10_000
    return number


def _parse_percent_fraction(value: object) -> float | None:
    number = _parse_number(value)
    return number / 100 if number is not None else None


def _stock_symbol(code: str) -> str:
    normalized = code.strip().upper()
    if "." in normalized:
        return normalized
    if normalized.startswith(("4", "8")):
        return f"{normalized}.BJ"
    if normalized.startswith(("0", "2", "3")):
        return f"{normalized}.SZ"
    return f"{normalized}.SH"


def _parse_jijiang_stocks(body: bytes) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while offset < len(body):
        start = body.find(b"\x0A\x06", offset)
        if start < 0 or start + 2 > len(body):
            break
        code_length = body[start + 1]
        index = start + 2
        if index + code_length > len(body):
            break
        code = body[index : index + code_length].decode("utf-8", errors="replace").strip()
        index += code_length
        name = ""
        tags: list[str] = []
        while index < len(body) - 1:
            if body[index : index + 2] in (b"\xA2\x06", b"\x0A\x06"):
                break
            field = body[index]
            field_number = field >> 3
            wire_type = field & 0x7
            index += 1
            if wire_type == 2:
                if index >= len(body):
                    break
                length = body[index]
                index += 1
                if index + length > len(body):
                    break
                content = body[index : index + length].decode("utf-8", errors="replace").strip()
                index += length
                if field_number == 2:
                    name = content
                elif field_number == 3 and content:
                    tags.append(content)
            elif wire_type == 0:
                while index < len(body) and body[index] & 0x80:
                    index += 1
                index += 1
            elif wire_type == 1:
                index += 8
            elif wire_type == 5:
                index += 4
            else:
                break

        next_start = body.find(b"\x0A\x06", start + 2)
        end = next_start if next_start >= 0 else len(body)
        fields: list[str] = []
        field_index = index
        while field_index < end:
            marker_index = body.find(b"\xA2\x06", field_index, end)
            if marker_index < 0 or marker_index + 3 > end:
                break
            length = body[marker_index + 2]
            value_start = marker_index + 3
            value_end = value_start + length
            if value_end > end:
                break
            fields.append(body[value_start:value_end].decode("utf-8", errors="replace").strip())
            field_index = value_end

        if code:
            price = _parse_number(fields[5] if len(fields) > 5 else None)
            change_pct = _parse_percent_fraction(fields[6] if len(fields) > 6 else None)
            rise_speed_pct = _parse_percent_fraction(fields[11] if len(fields) > 11 else None)
            sector = fields[12] if len(fields) > 12 else ""
            if not sector or sector == "-":
                sector = next(
                    (
                        value
                        for value in fields
                        if value and any(mark in value for mark in ("、", "概念", "板块"))
                    ),
                    "",
                )
            rows.append({
                "thscode": _stock_symbol(code),
                "ticker": code,
                "name": name,
                "change_pct": change_pct,
                "last_price": price,
                "rise_speed_pct": rise_speed_pct,
                "sector": sector,
                "main_force": _parse_number(fields[14] if len(fields) > 14 else None),
                "turnover_amount": _parse_number(fields[7] if len(fields) > 7 else None),
                "tags": tags,
            })
        offset = end
    return rows


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

    def fetch_jijiang_realtime(self) -> list[dict[str, Any]]:
        """Fetch the vendor's paged stocks-near-limit-up realtime list."""
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        raw_sock = socket.create_connection((_HOST, _PORT), timeout=15)
        sock = context.wrap_socket(raw_sock, server_hostname=_HOST)
        sock.settimeout(15)
        try:
            sock.sendall(self._login_packet)
            logged_in = False
            for _ in range(10):
                packet = _receive_packet_with_timeout(sock, 1.0)
                if packet and packet[0] in (0x30, 0x60):
                    logged_in = True
                    break
            if not logged_in:
                raise KaipanlaSocketError("开盘啦 socket 登录失败")

            rows: list[dict[str, Any]] = []
            seen: set[str] = set()
            offset = 0
            for _ in range(_JIJIANG_MAX_PAGES):
                sock.sendall(_jijiang_command(offset))
                page_body = b""
                got_packet = False
                empty_tries = 0
                for _ in range(_JIJIANG_MAX_PACKETS):
                    packet = _receive_packet_with_timeout(sock, _JIJIANG_READ_TIMEOUT)
                    if not packet:
                        if got_packet:
                            break
                        empty_tries += 1
                        if empty_tries >= 2:
                            break
                        continue
                    got_packet = True
                    empty_tries = 0
                    if packet[0] == 0x40:
                        page_body += packet[3:]
                if not page_body:
                    break
                page_rows = _parse_jijiang_stocks(page_body)
                fresh = [row for row in page_rows if row["thscode"] not in seen]
                if not fresh:
                    break
                seen.update(row["thscode"] for row in fresh)
                rows.extend(fresh)
                if len(page_rows) < _PAGE_SIZE:
                    break
                offset = len(seen)
            return [{**row, "rank": index + 1} for index, row in enumerate(rows)]
        except (OSError, ssl.SSLError) as exc:
            raise KaipanlaSocketError("开盘啦即将涨停 socket 连接失败") from exc
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
