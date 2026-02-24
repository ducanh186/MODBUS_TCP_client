from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

from pymodbus.client import ModbusSerialClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | RTU-BRIDGE | %(levelname)s | %(message)s",
)
log = logging.getLogger("rtu_bridge")


def _u16_to_int16(x: int) -> int:
    return x - 0x10000 if x >= 0x8000 else x


def decode_power_kw(reg_u16: int) -> float:
    return _u16_to_int16(reg_u16) * 0.1


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
            client = ModbusSerialClient(
                port=com_port,
                baudrate=baudrate,
                bytesize=8,
                parity="N",
                stopbits=1,
                timeout=2,
            )
            client.connect()
            rr = client.read_input_registers(0, count=1, device_id=slave_id)
            client.close()

            if not rr.isError():
                raw = rr.registers[0]
                kw = round(decode_power_kw(raw), 1)
                ts = time.strftime("%Y-%m-%dT%H:%M:%S")
                with _snapshot_lock:
                    _snapshot = {
                        "active_power_kw": kw,
                        "raw": raw,
                        "comm": {"ok": True, "last_ok_ts": ts, "last_error": None},
                    }
            else:
                with _snapshot_lock:
                    _snapshot["comm"]["ok"] = False
                    _snapshot["comm"]["last_error"] = f"Modbus error: {rr}"
                log.warning(f"RTU read error: {rr}")
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
