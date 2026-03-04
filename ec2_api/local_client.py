"""
Local Client — Bài tập 4
Runs on local PC alongside Modbus simulator.
Two loops (10s interval):
  1. Collect device data → POST /api/device
  2. Poll pending commands → GET /api/commands → execute → PATCH executed

Usage:
    python local_client.py --api-url http://<EC2_IP>:8000 \
                           --secret-key <SERVER_SECRET> \
                           --client-key local-agent-1 \
                           --modbus-host 127.0.0.1 --modbus-port 15020
"""

import argparse
import json
import logging
import threading
import time
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | LOCAL | %(levelname)s | %(message)s",
)
log = logging.getLogger("local_client")

# ---------------------------------------------------------------------------
# Token manager
# ---------------------------------------------------------------------------
class TokenManager:
    def __init__(self, api_url: str, secret_key: str, client_key: str):
        self.api_url = api_url.rstrip("/")
        self.secret_key = secret_key
        self.client_key = client_key
        self.token = None
        self.expires_at = 0  # epoch seconds
        self._lock = threading.Lock()

    def get_token(self) -> str:
        with self._lock:
            # Refresh if token expires in < 10s
            if self.token is None or time.time() > (self.expires_at - 10):
                self._refresh()
            return self.token

    def _refresh(self):
        log.info("Refreshing JWT token...")
        resp = requests.post(
            f"{self.api_url}/auth/token",
            json={"secret_key": self.secret_key, "client_key": self.client_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["token"]
        self.expires_at = time.time() + data.get("expires_in", 60)
        log.info("Token refreshed, TTL=%ds", data.get("expires_in", 60))

    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "X-Client-Key": self.client_key,
            "Content-Type": "application/json",
        }


# ---------------------------------------------------------------------------
# Device data collector (mock — no Modbus dependency needed)
# ---------------------------------------------------------------------------
def collect_device_data_mock() -> list[dict]:
    """Return mock device data for all 5 devices."""
    import random
    now = datetime.now(timezone.utc).isoformat()

    base_power = random.uniform(-50, 50)
    soc = random.uniform(30, 80)

    devices = [
        {
            "device_id": "PMS",
            "timestamp": now,
            "demand_control_power": round(base_power, 1),
            "total_active_power": round(base_power * 0.95, 1),
            "soc_avg": round(soc, 0),
            "soh_avg": 98,
            "capacity_total": 200.0,
        },
        {
            "device_id": "PCS1",
            "timestamp": now,
            "active_power": round(base_power / 2, 1),
        },
        {
            "device_id": "PCS2",
            "timestamp": now,
            "active_power": round(base_power / 2, 1),
        },
        {
            "device_id": "BMS1",
            "timestamp": now,
            "soc": round(soc, 0),
            "soh": 98,
            "capacity": 100.0,
        },
        {
            "device_id": "BMS2",
            "timestamp": now,
            "soc": round(soc + random.uniform(-2, 2), 0),
            "soh": 99,
            "capacity": 100.0,
        },
    ]
    return devices


def collect_device_data_modbus(host: str, port: int) -> list[dict]:
    """Read actual Modbus registers from simulator."""
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        log.warning("pymodbus not installed, falling back to mock data")
        return collect_device_data_mock()

    client = ModbusTcpClient(host, port=port)
    if not client.connect():
        log.error("Cannot connect to Modbus server %s:%d", host, port)
        return collect_device_data_mock()

    now = datetime.now(timezone.utc).isoformat()
    SCALE = 0.1
    devices = []

    def _s16(val):
        return val - 0x10000 if val >= 0x8000 else val

    try:
        # PMS (unit_id=1): HR0=demand, IR0=total_power, IR1=soc_avg, IR2=soh_avg, IR3=capacity
        hr = client.read_holding_registers(0, count=1, slave=1)
        ir = client.read_input_registers(0, count=4, slave=1)
        if not hr.isError() and not ir.isError():
            devices.append({
                "device_id": "PMS", "timestamp": now,
                "demand_control_power": round(_s16(hr.registers[0]) * SCALE, 1),
                "total_active_power": round(_s16(ir.registers[0]) * SCALE, 1),
                "soc_avg": ir.registers[1],
                "soh_avg": ir.registers[2],
                "capacity_total": round(ir.registers[3] * SCALE, 1),
            })

        # PCS1 (unit_id=2), PCS2 (unit_id=3): IR0=active_power
        for uid, name in [(2, "PCS1"), (3, "PCS2")]:
            ir = client.read_input_registers(0, count=1, slave=uid)
            if not ir.isError():
                devices.append({
                    "device_id": name, "timestamp": now,
                    "active_power": round(_s16(ir.registers[0]) * SCALE, 1),
                })

        # BMS1 (unit_id=4), BMS2 (unit_id=5): IR0=soc, IR1=soh, IR2=capacity
        for uid, name in [(4, "BMS1"), (5, "BMS2")]:
            ir = client.read_input_registers(0, count=3, slave=uid)
            if not ir.isError():
                devices.append({
                    "device_id": name, "timestamp": now,
                    "soc": ir.registers[0],
                    "soh": ir.registers[1],
                    "capacity": round(ir.registers[2] * SCALE, 1),
                })
    except Exception as e:
        log.error("Modbus read error: %s", e)
    finally:
        client.close()

    return devices if devices else collect_device_data_mock()


# ---------------------------------------------------------------------------
# Loop 1: Upload device data (every 10s)
# ---------------------------------------------------------------------------
def data_upload_loop(tm: TokenManager, api_url: str, collector_fn, interval: int = 10):
    """Collect device data and POST to API every `interval` seconds."""
    api_url = api_url.rstrip("/")
    while True:
        try:
            devices = collector_fn()
            for dev_data in devices:
                resp = requests.post(
                    f"{api_url}/api/device",
                    headers=tm.headers(),
                    json=dev_data,
                    timeout=10,
                )
                if resp.ok:
                    log.info("Uploaded %s → %s", dev_data["device_id"], resp.json().get("s3_key", "?"))
                else:
                    log.warning("Upload %s failed: %d %s", dev_data["device_id"], resp.status_code, resp.text[:100])
        except Exception as e:
            log.error("Data upload error: %s", e)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Loop 2: Poll & execute commands (every 10s)
# ---------------------------------------------------------------------------
def command_poll_loop(tm: TokenManager, api_url: str, interval: int = 10):
    """Poll pending commands and mark as executed."""
    api_url = api_url.rstrip("/")
    while True:
        try:
            resp = requests.get(
                f"{api_url}/api/commands?status=pending",
                headers=tm.headers(),
                timeout=10,
            )
            if resp.ok:
                cmds = resp.json()
                for cmd in cmds:
                    log.info(">>> Received command #%d: %s %.1f kW",
                             cmd["id"], cmd["command"], cmd.get("power_kw") or 0)

                    # TODO: Actually apply command to Modbus simulator
                    # e.g. write demand_control_power to PMS HR0

                    # Mark as executed
                    patch_resp = requests.patch(
                        f"{api_url}/api/commands/{cmd['id']}",
                        headers=tm.headers(),
                        json={"status": "executed"},
                        timeout=10,
                    )
                    if patch_resp.ok:
                        log.info("    Command #%d marked executed", cmd["id"])
                    else:
                        log.warning("    Failed to ack command #%d: %s", cmd["id"], patch_resp.text[:100])
            else:
                log.warning("Command poll failed: %d", resp.status_code)
        except Exception as e:
            log.error("Command poll error: %s", e)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Local client for BESS API")
    parser.add_argument("--api-url", required=True, help="EC2 API URL (http://<ip>:8000)")
    parser.add_argument("--secret-key", required=True, help="Server secret key")
    parser.add_argument("--client-key", default="local-agent-1", help="Client identifier")
    parser.add_argument("--modbus-host", default="127.0.0.1")
    parser.add_argument("--modbus-port", type=int, default=15020)
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of Modbus")
    parser.add_argument("--interval", type=int, default=10, help="Poll interval in seconds")
    args = parser.parse_args()

    tm = TokenManager(args.api_url, args.secret_key, args.client_key)

    # Test auth
    log.info("Testing authentication...")
    try:
        tm.get_token()
        log.info("Auth OK!")
    except Exception as e:
        log.error("Auth failed: %s", e)
        return

    # Choose data collector
    if args.mock:
        collector = collect_device_data_mock
        log.info("Using MOCK device data")
    else:
        collector = lambda: collect_device_data_modbus(args.modbus_host, args.modbus_port)
        log.info("Using Modbus data from %s:%d", args.modbus_host, args.modbus_port)

    # Start both loops
    t1 = threading.Thread(target=data_upload_loop, args=(tm, args.api_url, collector, args.interval), daemon=True)
    t2 = threading.Thread(target=command_poll_loop, args=(tm, args.api_url, args.interval), daemon=True)

    t1.start()
    t2.start()
    log.info("Local client running (interval=%ds). Press Ctrl+C to stop.", args.interval)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
