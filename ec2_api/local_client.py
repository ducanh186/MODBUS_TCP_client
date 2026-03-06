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


def collect_device_data_modbus(host: str, port: int, rtu_bridge_url: str = None) -> list[dict]:
    """
    Read actual Modbus registers from the multi-port plant simulator.

    Each device runs on its OWN TCP port with device_id=1:
      PMS  → port (base)     HR0=demand, IR0-3=total/soc/soh/cap
      PCS1 → port (base+1)   HR0=setpoint, IR0=active_power
      PCS2 → port (base+2)   HR0=setpoint, IR0=active_power
      BMS1 → port (base+4)   IR0=soc, IR1=soh, IR2=capacity
      BMS2 → port (base+5)   IR0=soc, IR1=soh, IR2=capacity
      Multimeter → via RTU bridge HTTP (optional)

    The `port` argument is the PMS base port (default 15020).
    """
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        log.warning("pymodbus not installed, falling back to mock data")
        return collect_device_data_mock()

    now = datetime.now(timezone.utc).isoformat()
    SCALE = 0.1
    devices = []

    def _s16(val):
        return val - 0x10000 if val >= 0x8000 else val

    def _read_device(device_port, read_fn):
        """Connect to a single device port, call read_fn, then close."""
        client = ModbusTcpClient(host, port=device_port)
        try:
            if not client.connect():
                log.warning("Cannot connect to %s:%d", host, device_port)
                return None
            return read_fn(client)
        except Exception as e:
            log.error("Modbus read error on port %d: %s", device_port, e)
            return None
        finally:
            client.close()

    # --- PMS (base port) ---
    def _read_pms(c):
        hr = c.read_holding_registers(0, count=1, device_id=1)
        ir = c.read_input_registers(0, count=4, device_id=1)
        if hr.isError() or ir.isError():
            return None
        return {
            "device_id": "PMS", "timestamp": now,
            "demand_control_power": round(_s16(hr.registers[0]) * SCALE, 1),
            "total_active_power": round(_s16(ir.registers[0]) * SCALE, 1),
            "soc_avg": ir.registers[1],
            "soh_avg": ir.registers[2],
            "capacity_total": round(ir.registers[3] * SCALE, 1),
        }
    res = _read_device(port, _read_pms)
    if res:
        devices.append(res)

    # --- PCS1 (base+1), PCS2 (base+2) ---
    for offset, name in [(1, "PCS1"), (2, "PCS2")]:
        def _read_pcs(c, _name=name):
            ir = c.read_input_registers(0, count=1, device_id=1)
            if ir.isError():
                return None
            return {
                "device_id": _name, "timestamp": now,
                "active_power": round(_s16(ir.registers[0]) * SCALE, 1),
            }
        res = _read_device(port + offset, _read_pcs)
        if res:
            devices.append(res)

    # --- BMS1 (base+4), BMS2 (base+5) ---
    for offset, name in [(4, "BMS1"), (5, "BMS2")]:
        def _read_bms(c, _name=name):
            ir = c.read_input_registers(0, count=3, device_id=1)
            if ir.isError():
                return None
            return {
                "device_id": _name, "timestamp": now,
                "soc": ir.registers[0],
                "soh": ir.registers[1],
                "capacity": round(ir.registers[2] * SCALE, 1),
            }
        res = _read_device(port + offset, _read_bms)
        if res:
            devices.append(res)

    # --- Multimeter (via RTU bridge HTTP) ---
    if rtu_bridge_url:
        try:
            resp = requests.get(rtu_bridge_url, timeout=2)
            if resp.ok:
                mm_data = resp.json()
                devices.append({
                    "device_id": "Multimeter",
                    "timestamp": now,
                    "active_power": mm_data.get("active_power_kw"),
                    "raw": mm_data.get("raw"),
                    "comm_ok": mm_data.get("comm", {}).get("ok", False),
                })
                log.debug("Multimeter: %s kW", mm_data.get("active_power_kw"))
            else:
                log.warning("RTU bridge returned %d", resp.status_code)
        except Exception as e:
            log.warning("RTU bridge poll failed: %s", e)

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
def _apply_command_to_modbus(modbus_host: str, modbus_port: int, cmd: dict) -> bool:
    """
    Write demand_control_power to PMS HR0 based on command type.

    charge    → negative kW (battery absorbs)
    discharge → positive kW (battery exports)
    standby   → 0 kW
    """
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        log.warning("pymodbus not installed — cannot write command to simulator")
        return False

    command = cmd.get("command", "")
    power_kw = abs(cmd.get("power_kw") or 0)

    if command == "charge":
        demand_kw = -power_kw      # negative = charging
    elif command == "discharge":
        demand_kw = power_kw       # positive = discharging
    else:  # standby
        demand_kw = 0.0

    # Encode to int16 (scale 0.1 kW per LSB) → uint16 two's complement
    raw = int(round(demand_kw / 0.1))
    if raw < 0:
        raw = raw + 0x10000        # two's complement for negative
    raw = raw & 0xFFFF

    try:
        client = ModbusTcpClient(modbus_host, port=modbus_port)
        if not client.connect():
            log.error("Cannot connect to PMS %s:%d to write command", modbus_host, modbus_port)
            return False
        wr = client.write_register(0, raw, device_id=1)  # HR0 = demand_control_power
        client.close()
        if wr.isError():
            log.error("PMS HR0 write failed: %s", wr)
            return False
        log.info("    Wrote PMS HR0 = %.1f kW (raw=0x%04X) [%s]", demand_kw, raw, command)
        return True
    except Exception as e:
        log.error("Modbus write error: %s", e)
        return False


def command_poll_loop(tm: TokenManager, api_url: str,
                      modbus_host: str = "127.0.0.1",
                      modbus_port: int = 15020,
                      interval: int = 10):
    """Poll pending commands, apply to Modbus simulator, and mark as executed."""
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

                    # Apply command to Modbus simulator (write PMS HR0)
                    ok = _apply_command_to_modbus(modbus_host, modbus_port, cmd)
                    status = "executed" if ok else "failed"

                    # Mark as executed/failed
                    patch_resp = requests.patch(
                        f"{api_url}/api/commands/{cmd['id']}",
                        headers=tm.headers(),
                        json={"status": status},
                        timeout=10,
                    )
                    if patch_resp.ok:
                        log.info("    Command #%d marked %s", cmd["id"], status)
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
    parser.add_argument("--rtu-bridge", default="http://localhost:8081/api/multimeter",
                        help="RTU bridge URL for multimeter (set to empty to disable)")
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

    # RTU bridge URL
    rtu_url = args.rtu_bridge if args.rtu_bridge else None

    # Choose data collector
    if args.mock:
        collector = collect_device_data_mock
        log.info("Using MOCK device data")
    else:
        collector = lambda: collect_device_data_modbus(args.modbus_host, args.modbus_port, rtu_url)
        log.info("Using Modbus data from %s:%d", args.modbus_host, args.modbus_port)
        if rtu_url:
            log.info("RTU bridge: %s", rtu_url)

    # Start both loops
    t1 = threading.Thread(target=data_upload_loop, args=(tm, args.api_url, collector, args.interval), daemon=True)
    t2 = threading.Thread(target=command_poll_loop,
                          args=(tm, args.api_url, args.modbus_host, args.modbus_port, args.interval),
                          daemon=True)

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
