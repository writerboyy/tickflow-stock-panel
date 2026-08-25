#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
模块：获取板块 801001 成分股数据并导出 Excel（包含全部字段）
"""

import os
import re
import socket
import ssl
import struct
import time
import threading
import queue

# 字段名称定义
field_hz_names = [
    '板块',             # blocks
    '价格',             # price
    '涨幅',             # zdf
    '成交额',           # cje
    'data_4',
    '涨速',             # zs
    '实际流通',         # sjlt
    '主力买',           # zlmr
    '主力卖',           # zlmc
    '主力净额',         # zlje
    'data_10',
    'data_11',
    'data_12',
    'data_13',
    '卖流占比',         # mlzb
    '净流占比',         # zlzb
    '区间涨幅',         # qjzf
    '量比',             # lb
    '连扳高度',         # lbgd
    'data_19',
    'data_20',
    'data_21',
    'data_22',
    'data_23',
    '振幅',             # zhenfu
    'data_25',
    'data_26',
    'data_27',
    '总市值',           # zsz
    '流通市值',         # lzsz
    'data_30',
    'data_31',
    'data_32',
    '第一季度机构增仓', # dsjdjgzc（表头展示名）
    'data_jjlb',        # 原竞价量比，不导出 CSV
    'data_jjje',        # 原竞价金额
    'data_jjzf',        # 原竞价涨幅
    'data_36',
    'data_37',
    'data_38',
    'data_39',
    'data_40',
    'data_41',
    'data_42',
    'data_43',
    'data_44',
    'data_45',
    'data_46',
    'data_47',
    'data_48',
    'data_49',
    'data_50',
    'data_51',
    'data_52',
    'data_53',
    'data_lfzcje',      # 原两分钟成交额，不导出 CSV
    'data_55',
    'data_56',
    '人气值',           # rqz
    '人气变化',         # rqbh
    'data_59'
]

TARGET_BLOCK_ID = "801248"

# openpyxl 不允许写入 Excel 的控制字符
ILLEGAL_CHARACTERS_RE = re.compile(r'[\000-\010]|[\013\014]|[\016-\037]')


def sanitize_excel_value(value):
    """清理 Excel 不支持的控制字符。"""
    if value is None:
        return ''
    if isinstance(value, (int, float, bool)):
        return value
    return ILLEGAL_CHARACTERS_RE.sub('', str(value))


def parse_stock_list(stock_list_body):
    """解析股票列表"""
    stock_list = []
    stock_start = 0

    # 解析每个股票
    while stock_start < len(stock_list_body):
        # 寻找股票分隔符 0A 06
        stock_start = stock_list_body.find(b'\x0A\x06', stock_start)
        if stock_start == -1:
            break

        # 解析股票代码和名称
        index = stock_start + 1
        if index >= len(stock_list_body):
            break
        stock_code_length = stock_list_body[index]
        index += 1
        if index + stock_code_length > len(stock_list_body):
            break
        stock_code = stock_list_body[index:index+stock_code_length].decode('utf-8', errors='ignore')
        index += stock_code_length + 1
        if index >= len(stock_list_body):
            break
        stock_name_length = stock_list_body[index]
        index += 1
        if index + stock_name_length > len(stock_list_body):
            break
        stock_name = stock_list_body[index:index+stock_name_length].decode('utf-8', errors='ignore')

        # 解析股票数据
        endIndex = stock_list_body.find(b'\x0A\x06', stock_start + 2)
        stock_end = endIndex if endIndex != -1 else len(stock_list_body)
        stock_data = {}
        data_start = stock_list_body.find(b'\xA2\x06', stock_start + 2)
        fieldindex = 0

        while data_start < stock_end and data_start != -1:
            if not (stock_list_body[data_start] == 0xA2 and stock_list_body[data_start+1] == 0x06):
                break
            index = data_start + 2
            if index >= len(stock_list_body):
                break
            data_length = stock_list_body[index]
            index += 1
            if index + data_length > len(stock_list_body):
                break
            data_content = stock_list_body[index:index+data_length].decode('utf-8', errors='ignore')
            if fieldindex < len(field_hz_names):
                stock_data[field_hz_names[fieldindex]] = data_content
            else:
                stock_data[f'field_{fieldindex}'] = data_content
            data_start = index + data_length
            fieldindex += 1

        # 添加股票到列表
        stock_list.append({
            'code': stock_code,
            'name': stock_name,
            'data': stock_data
        })

        # 移动到下一个股票
        stock_start = stock_end

    return stock_list


def parse_packet_40(data):
    """解析40类型数据包"""
    try:
        if len(data) < 3:
            raise ValueError("数据太短，无法解析包头")

        # 检查包头
        packet_type = data[0]
        packet_body_length = struct.unpack('>H', data[1:3])[0]

        if packet_type != 0x40:
            raise ValueError(f"错误的包类型: 0x{packet_type:02X}")

        # 检查数据长度
        if len(data) < 3 + packet_body_length:
            raise ValueError("数据长度不足")

        # 解析包体
        message_body = data[3:]

        # 检查是否有错误信息
        if len(data) >= 6 and data[:3] == b'\x40\x00\x27':
            index = data.find(b'\x0F\x12', 3)
            if index != -1 and len(data) > index + 2:
                errmsg_len = data[index + 2]
                if len(data) > index + 3 + errmsg_len:
                    errmsg = data[index+3:index+3+errmsg_len].decode('utf-8')
                    print(f"服务器报错: {errmsg}")
                    return None

        # 查找股票数据开始位置
        stock_start = message_body.find(b'\x0A\x06')
        if stock_start == -1:
            print("未找到股票数据开始标记")
            return []

        # 解析股票列表
        stock_list_body = message_body[stock_start:]
        stock_list = parse_stock_list(stock_list_body)
        return stock_list

    except Exception as e:
        print(f"解析40类型数据包出错: {str(e)}")
        return None


def receive_packet(sock):
    """接收一个完整的数据包"""
    # 先读取3字节的包头
    header = b''
    while len(header) < 3:
        chunk = sock.recv(3 - len(header))
        if not chunk:
            return None
        header += chunk

    packet_type = header[0]
    packet_body_length = struct.unpack('>H', header[1:3])[0]
    total_length = 3 + packet_body_length

    # 读取包体
    body = b''
    while len(body) < packet_body_length:
        chunk = sock.recv(packet_body_length - len(body))
        if not chunk:
            return None
        body += chunk

    return header + body


LONGHU_SOCK_HOST = "hwsockapp.longhuvip.com"
LONGHU_SOCK_PORT = 14000

LOGIN_CMD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "login.txt")


def load_login_cmd(path=LOGIN_CMD_PATH):
    """从 login.txt 读取登录包十六进制（空格/换行均可），返回 bytes。"""
    with open(path, "r", encoding="utf-8") as f:
        hex_str = f.read()
    return bytes.fromhex(hex_str)


LOGIN_CMD = load_login_cmd()


def open_longhu_ssl_socket():
    """建立 TCP + SSL 连接（未登录）。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    ssl_sock = context.wrap_socket(sock, server_hostname=LONGHU_SOCK_HOST)
    ssl_sock.connect((LONGHU_SOCK_HOST, LONGHU_SOCK_PORT))
    return ssl_sock


def login_longhu_socket(ssl_sock):
    """在已连接的 ssl_sock 上发送登录包并等待成功包。失败时返回 False（由调用方关闭 socket）。"""
    print("发送登录命令...")
    ssl_sock.sendall(LOGIN_CMD)
    print("登录命令已发送，等待登录响应...")
    max_wait = 10
    for _ in range(max_wait):
        packet = receive_packet(ssl_sock)
        if packet:
            packet_type = packet[0]
            if packet_type in (0x30, 0x60):
                print("登录成功！")
                return True
            if packet_type in (0x40, 0x11):
                continue
    print("等待登录响应超时")
    return False


def fetch_block_stocks_via_socket(ssl_sock, block_id, max_stocks_per_page=30, post_connect_pause=0.0):
    """
    在已登录的 ssl_sock 上拉取指定板块成分股；不关闭 socket。
    post_connect_pause: 刚登录后短暂等待（秒），复用连接跨板块时传 0。
    """
    if post_connect_pause > 0:
        time.sleep(post_connect_pause)

    # 构造单页请求命令的函数
    def build_block_cmd(page_index):
        def encode_uleb128(value: int):
            encoded_bytes = []
            while True:
                byte = value & 0x7F
                value >>= 7
                if value:
                    encoded_bytes.append(f"{(byte | 0x80):02X}")
                else:
                    encoded_bytes.append(f"{byte:02X}")
                    break
            return encoded_bytes

        block_id_hex = "".join([f"{ord(c):02X}" for c in block_id])
        max_stocks_hex = f"{min(max_stocks_per_page, 255):02X}"
        count_byte = 0x6A
        base_parts = [
            "40", "00", "18", "00",
            f"{count_byte:02X}",
            "09", "C5", "00", "00", "03", "02", "09", "C5", "0A", "06",
            block_id_hex,
            "10", "06", "18", "01"
        ]
        pagination_parts = []
        if page_index >= 1:
            page_offset = 0x11 + (page_index - 1) * 0x1E
            encoded_offset = encode_uleb128(page_offset)
            pagination_parts = ["30"] + encoded_offset
            new_len = 0x18 + len(pagination_parts)
            base_parts[2] = f"{new_len:02X}"
        else:
            base_parts[2] = "18"
        tail_parts = ["38", max_stocks_hex]
        cmd_parts = base_parts + pagination_parts + tail_parts
        cmd = " ".join(cmd_parts)
        return cmd, bytes.fromhex(cmd)

    def fetch_one_page(page_index):
        cmd_str, cmd_bytes = build_block_cmd(page_index)
        print(f"发送成分股请求 (block_id={block_id}, 请求条数={max_stocks_per_page}, 页码={page_index})...")
        ssl_sock.sendall(cmd_bytes)
        print(f"成分股请求已发送 (命令: {cmd_str})")
        print("等待成分股数据响应...")
        max_wait = 10
        for _ in range(max_wait):
            packet = receive_packet(ssl_sock)
            if not packet:
                continue
            packet_type = packet[0]
            if packet_type == 0x40:
                print("接收到成分股数据包")
                stock_list = parse_packet_40(packet)
                return stock_list
            if packet_type == 0x11:
                continue
        print("等待成分股数据超时")
        return None

    all_stocks = []
    page_index = 0
    max_pages_guard = 500
    batch_size = 10

    def fetch_page_batch(start_page, result_queue):
        batch_results = []
        for i in range(start_page, min(start_page + batch_size, max_pages_guard)):
            stock_list = fetch_one_page(i)
            if stock_list is None:
                print(f"第 {i} 页解析失败或超时")
                break
            if len(stock_list) <= 1:
                print(f"第 {i} 页无数据，视为最后一页")
                break
            if i == 0:
                batch_results.extend(stock_list)
            else:
                batch_results.extend(stock_list[1:])
            print(f"第 {i} 页获取到 {len(stock_list)-1 if i>0 else len(stock_list)} 条成分股")

        result_queue.put(batch_results)

    while page_index < max_pages_guard:
        result_queue = queue.Queue()
        thread = threading.Thread(
            target=fetch_page_batch,
            args=(page_index, result_queue)
        )
        thread.start()
        thread.join()

        batch_results = result_queue.get()
        if not batch_results:
            break

        all_stocks.extend(batch_results)

        page_index += batch_size

        print(f"批次完成，当前已获取 {len(all_stocks)} 条成分股")

        time.sleep(0.5)

    if all_stocks:
        print("正在进行数据去重...")
        seen_codes = set()
        unique_stocks = []

        for stock in all_stocks:
            if stock['code'] not in seen_codes:
                seen_codes.add(stock['code'])
                unique_stocks.append(stock)

        print(f"去重前：{len(all_stocks)} 条")
        print(f"去重后：{len(unique_stocks)} 条")
        print(f"去除了 {len(all_stocks) - len(unique_stocks)} 条重复数据")

        all_stocks = unique_stocks

    print(f"板块 {block_id} 成分股获取完成，共 {len(all_stocks)} 条")

    return all_stocks


class BlockStocksSession:
    """
    复用同一 TCP/SSL 连接与登录态，连续拉取多个板块成分股；
    连接失效时再重新连接并登录。
    """

    def __init__(self):
        self._ssl_sock = None
        self._need_login_pause = False

    def close(self):
        if self._ssl_sock is not None:
            try:
                self._ssl_sock.close()
            except OSError:
                pass
            self._ssl_sock = None

    def _socket_alive(self):
        if self._ssl_sock is None:
            return False
        try:
            self._ssl_sock.getpeername()
        except OSError:
            return False
        return True

    def _connect_and_login(self):
        self.close()
        try:
            print(f"\n正在连接 {LONGHU_SOCK_HOST}:{LONGHU_SOCK_PORT} 并登录...")
            self._ssl_sock = open_longhu_ssl_socket()
            if not login_longhu_socket(self._ssl_sock):
                self.close()
                return False
            self._need_login_pause = True
            print("登录成功，后续板块将复用本连接（无需重复登录）。")
            return True
        except (socket.timeout, ssl.SSLError, OSError, ConnectionError) as e:
            print(f"连接或登录失败: {e}")
            self.close()
            return False
        except Exception as e:
            print(f"连接或登录失败: {e}")
            import traceback
            traceback.print_exc()
            self.close()
            return False

    def get_block_stocks(self, block_id, max_stocks_per_page=30):
        """
        使用复用连接获取单个板块成分股；失败时自动重连并重试一次。
        """
        last_error = None
        for attempt in range(2):
            try:
                if not self._socket_alive():
                    print("连接不可用，重新连接并登录...")
                    if not self._connect_and_login():
                        return None

                pause = 0.5 if self._need_login_pause else 0.0
                self._need_login_pause = False

                if pause > 0:
                    print(f"\n新连接已就绪，正在获取板块 {block_id} 的成分股...")
                else:
                    print(f"\n复用已登录连接，正在获取板块 {block_id} 的成分股...")

                return fetch_block_stocks_via_socket(
                    self._ssl_sock,
                    block_id,
                    max_stocks_per_page=max_stocks_per_page,
                    post_connect_pause=pause,
                )
            except (socket.timeout, ssl.SSLError, OSError, ConnectionError, BrokenPipeError) as e:
                last_error = e
                print(f"连接异常，将重新登录后重试: {e}")
                self.close()
                if attempt == 0:
                    continue
                return None
            except Exception as e:
                print(f"获取板块成分股出错: {e}")
                import traceback
                traceback.print_exc()
                self.close()
                return None

        if last_error:
            print(f"重试后仍失败: {last_error}")
        return None


def get_block_stocks(block_id, max_stocks_per_page=30):
    """
    单次使用：拉取一个板块后关闭连接（与旧行为一致，便于单独脚本调用）。
    """
    session = BlockStocksSession()
    try:
        return session.get_block_stocks(block_id, max_stocks_per_page=max_stocks_per_page)
    finally:
        session.close()


def export_stocks_to_excel(stocks, block_id, output_path=None):
    """将全部成分股字段导出到 Excel（含 data_* 等未命名中文字段）。"""
    try:
        from openpyxl import Workbook
    except ImportError as exc:
        raise ImportError("导出 Excel 需要 openpyxl，请先执行: pip install openpyxl") from exc

    if output_path is None:
        output_path = os.path.join(os.path.dirname(__file__), f"板块_{block_id}_成分股.xlsx")

    extra_fields = []
    known_fields = set(field_hz_names)
    for stock in stocks:
        for key in stock.get('data', {}):
            if key not in known_fields:
                extra_fields.append(key)
    extra_fields = sorted(set(extra_fields))
    all_fields = list(field_hz_names) + extra_fields

    wb = Workbook()
    ws = wb.active
    ws.title = "成分股"
    ws.append([sanitize_excel_value(v) for v in (['代码', '名称'] + all_fields)])

    for stock in stocks:
        data = stock.get('data', {})
        row = [stock.get('code', ''), stock.get('name', '')]
        row.extend(data.get(field, '') for field in all_fields)
        ws.append([sanitize_excel_value(v) for v in row])

    wb.save(output_path)
    return output_path, len(all_fields) + 2


if __name__ == "__main__":
    block_id = TARGET_BLOCK_ID
    print(f"正在获取板块 {block_id} 的成分股...")
    stocks = get_block_stocks(block_id)
    if stocks:
        print(f"\n板块 {block_id} 成分股列表（前5只）：")
        for i, stock in enumerate(stocks[:5]):
            print(f"{i+1}. {stock['code']} - {stock['name']}")
        print(f"\n共获取到 {len(stocks)} 只成分股")

        excel_path, column_count = export_stocks_to_excel(stocks, block_id)
        print(f"\n已导出 Excel: {excel_path}")
        print(f"共写入 {column_count} 列")
    else:
        print("获取成分股失败")