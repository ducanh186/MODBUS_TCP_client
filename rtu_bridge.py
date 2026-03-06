from __future__ import annotations

import argparse
import json
import logging
import struct
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

import serial

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | RTU-BRIDGE | %(levelname)s | %(message)s",
)
log = logging.getLogger("rtu_bridge")


def _u16_to_int16(x: int) -> int:
    return x - 0x10000 if x >= 0x8000 else x


def decode_power_kw(reg_u16: int) -> float:
    return _u16_to_int16(reg_u16) * 0.1


def _crc16(data: bytes) -> int:
    """Calculate Modbus CRC16."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def _rtu_read_input_registers(ser: serial.Serial, slave_id: int, start: int, count: int) -> int | None:
    """Send FC04 request and return register value (or None on error)."""
    # Build request: slave, FC04, start_hi, start_lo, count_hi, count_lo
    request = struct.pack(">BBHH", slave_id, 0x04, start, count)
    crc = _crc16(request)
    request += struct.pack("<H", crc)  # CRC is little-endian
    
    ser.reset_input_buffer()
    ser.write(request)
    
    # Expected response: slave, FC04, byte_count, data..., CRC (2 bytes)
    # For 1 register: 1+1+1+2+2 = 7 bytes
    expected_len = 3 + (count * 2) + 2
    response = ser.read(expected_len)
    
    if len(response) < expected_len:
        raise ValueError(f"Short response: got {len(response)} bytes, expected {expected_len}")
    
    # Verify CRC
    response_crc = struct.unpack("<H", response[-2:])[0]
    calculated_crc = _crc16(response[:-2])
    if response_crc != calculated_crc:
        raise ValueError(f"CRC mismatch: got 0x{response_crc:04X}, expected 0x{calculated_crc:04X}")
    
    # Check for exception response
    if response[1] & 0x80:
        raise ValueError(f"Modbus exception: code {response[2]}")
    
    # Parse data (big-endian)
    data_bytes = response[3:-2]
    values = struct.unpack(f">{count}H", data_bytes)
    return values[0] if count == 1 else values


_snapshot_lock = threading.Lock()
_snapshot: dict = {
    "active_power_kw": None,
    "raw": None,
    "comm": {"ok": False, "last_ok_ts": None, "last_error": "not polled yet"},
}


def _poller(
    com_port: str,
    slave_id: int,
    baudrate: int,
    interval_s: float,
    stop_event: threading.Event,
) -> None:
    global _snapshot
    log.info(f"RTU poller started: {com_port} slave={slave_id} baud={baudrate}")

    while not stop_event.is_set():
        try:
            # Use deferred open pattern for com0com compatibility
            ser = serial.Serial()
            ser.port = com_port
            ser.baudrate = baudrate
            ser.bytesize = 8
            ser.parity = "N"
            ser.stopbits = 1
            ser.timeout = 2
            ser.open()
            
            raw = _rtu_read_input_registers(ser, slave_id, start=0, count=1)
            ser.close()

            kw = round(decode_power_kw(raw), 1)
            ts = time.strftime("%Y-%m-%dT%H:%M:%S")
            with _snapshot_lock:
                _snapshot = {
                    "active_power_kw": kw,
                    "raw": raw,
                    "comm": {"ok": True, "last_ok_ts": ts, "last_error": None},
                }
            log.info(f"[OK] raw=0x{raw:04X} => {kw:+.1f} kW")
        except Exception as exc:
            with _snapshot_lock:
                _snapshot["comm"]["ok"] = False
                _snapshot["comm"]["last_error"] = str(exc)
            log.warning(f"RTU poll error: {exc}")

        stop_event.wait(interval_s)


class BridgeHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/api/multimeter":
            with _snapshot_lock:
                data = dict(_snapshot)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RTU Bridge — Multimeter RTU -> HTTP JSON",
    )
    parser.add_argument("--rtu-com", default="COM5")
    parser.add_argument("--rtu-slave", type=int, default=10)
    parser.add_argument("--rtu-baud", type=int, default=9600)
    parser.add_argument("--http-port", type=int, default=8081)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args()

    # Start RTU poller in background thread
    stop_event = threading.Event()
    poller = threading.Thread(
        target=_poller,
        args=(args.rtu_com, args.rtu_slave, args.rtu_baud,
              args.poll_interval, stop_event),
        daemon=True,
    )
    poller.start()

    # Start HTTP server (blocking)
    server = HTTPServer(("0.0.0.0", args.http_port), BridgeHandler)
    log.info(f"RTU Bridge HTTP: http://localhost:{args.http_port}/api/multimeter")
    log.info(f"RTU client: {args.rtu_com} slave={args.rtu_slave} baud={args.rtu_baud}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        stop_event.set()
        server.shutdown()


if __name__ == "__main__":
    main()
