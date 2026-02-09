"""
BÀI TOÁN (Modbus TCP Client - demo "write rồi read lại"):
- Chạy ở Terminal #2.
- Connect tới 127.0.0.1:1502.
- Gửi:
  1) FC06 write addr=0 value=123
  2) FC03 read addr=0 count=1

CÁCH CHẠY:
  cd MODBUS_TCP_client
  python client.py
"""

import logging
import socket
from typing import Tuple

from modbus_tcp import (
    frame_from_stream_buffer,
    parse_response,
    build_fc03_request,
    build_fc06_request,
    hexdump,
)

HOST = "127.0.0.1"
PORT = 1502

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def recv_one_frame(conn: socket.socket, buf: bytes) -> Tuple[bytes, bytes]:
    while True:
        frame, buf = frame_from_stream_buffer(buf)
        if frame is not None:
            return frame, buf

        data = conn.recv(4096)
        if not data:
            raise ConnectionError("server closed")
        logging.info(f"[RECV] bytes={len(data)} hex={hexdump(data)}")
        buf += data


def main() -> None:
    tid = 1
    buf = b""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(5.0)
        s.connect((HOST, PORT))
        logging.info(f"Connected to {HOST}:{PORT}")

        # 1) FC06 write
        req1 = build_fc06_request(tid, unit_id=1, address=0, value=123)
        s.sendall(req1)
        logging.info(f"[SEND] TID={tid} hex={hexdump(req1)}")
        frame, buf = recv_one_frame(s, buf)
        logging.info(f"[RESP] {parse_response(frame)}")

        tid += 1

        # 2) FC03 read
        req2 = build_fc03_request(tid, unit_id=1, address=0, count=1)
        s.sendall(req2)
        logging.info(f"[SEND] TID={tid} hex={hexdump(req2)}")
        frame, buf = recv_one_frame(s, buf)
        logging.info(f"[RESP] {parse_response(frame)}")


if __name__ == "__main__":
    main()
