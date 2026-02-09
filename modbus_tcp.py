"""
Client helper: build request + parse response tối thiểu.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Optional, Tuple, List, Union


def hexdump(b: bytes) -> str:
    return b.hex(" ")


def frame_from_stream_buffer(buf: bytes) -> Tuple[Optional[bytes], bytes]:
    if len(buf) < 6:
        return None, buf
    tid, pid, length = struct.unpack(">HHH", buf[:6])
    total = 6 + length
    if len(buf) < total:
        return None, buf
    return buf[:total], buf[total:]


def build_mbap(transaction_id: int, unit_id: int, pdu: bytes) -> bytes:
    length = 1 + len(pdu)
    return struct.pack(">HHH", transaction_id, 0, length) + bytes([unit_id]) + pdu


def build_fc03_request(transaction_id: int, unit_id: int, address: int, count: int) -> bytes:
    pdu = bytes([3]) + struct.pack(">H", address) + struct.pack(">H", count)
    return build_mbap(transaction_id, unit_id, pdu)


def build_fc06_request(transaction_id: int, unit_id: int, address: int, value: int) -> bytes:
    pdu = bytes([6]) + struct.pack(">H", address) + struct.pack(">H", value & 0xFFFF)
    return build_mbap(transaction_id, unit_id, pdu)


@dataclass(frozen=True)
class ModbusResponse:
    transaction_id: int
    unit_id: int
    function_code: int
    data: Union[List[int], None, int]  # list for FC03, None for FC06, int for exception


def parse_response(frame: bytes) -> ModbusResponse:
    if len(frame) < 8:
        raise ValueError("frame too short")

    tid, pid, length = struct.unpack(">HHH", frame[:6])
    unit_id = frame[6]
    pdu = frame[7:]

    if pid != 0:
        raise ValueError("protocol id must be 0")

    fc = pdu[0]
    if fc & 0x80:
        return ModbusResponse(tid, unit_id, fc, pdu[1])

    if fc == 3:
        byte_count = pdu[1]
        data_bytes = pdu[2 : 2 + byte_count]
        regs = [struct.unpack(">H", data_bytes[i : i + 2])[0] for i in range(0, len(data_bytes), 2)]
        return ModbusResponse(tid, unit_id, fc, regs)

    if fc == 6:
        return ModbusResponse(tid, unit_id, fc, None)

    return ModbusResponse(tid, unit_id, fc, None)
