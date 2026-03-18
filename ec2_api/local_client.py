"""
Local Client — Bài tập 4
Runs on local PC alongside Modbus simulator.
Five loops:
  1. Collect device data → POST /api/device (10s)
  2. Poll queued commands → GET /api/commands → execute (10s)
  3. Schedule execution with ramp + feedback (10s)
  4. Transducer polling — read frequency from Modbus (<= 0.1s)
  5. Frequency control loop — ichiji droop command (0.5s)

Usage:
    python local_client.py --api-url http://<EC2_IP>:8000 \
                           --secret-key <SERVER_SECRET> \
                           --client-key local-agent-1 \
                           --modbus-host 127.0.0.1 --modbus-port 15020
"""

from __future__ import annotations

import argparse
import collections
import json
import logging
import math
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | LOCAL | %(levelname)s | %(message)s",
)
log = logging.getLogger("local_client")

# Command lifecycle vocabulary (must match EC2 API contract)
STATUS_EXECUTING = "executing"
STATUS_EXECUTED = "executed"
STATUS_FAILED = "failed"
STATUS_BLOCKED = "blocked"
DEFAULT_DEVICE_ID = "bess-01"

# ---------------------------------------------------------------------------
# Schedule execution constants
# ---------------------------------------------------------------------------
MAX_POWER_KW = 1999.0                              # ±1999 kW clamp
CYCLE_INTERVAL_S = 10                               # 10s per cycle
RAMP_RATE_PCT_PER_SEC = 100 / 30                    # 3.333 %/s  (0→100% in 30s)
MAX_DELTA_PER_CYCLE = (MAX_POWER_KW
                       * (RAMP_RATE_PCT_PER_SEC / 100)
                       * CYCLE_INTERVAL_S)           # ≈ 666.33 kW/cycle
FEEDBACK_TOLERANCE_PCT = 1.0                        # 1 % dead-band
DEFAULT_MEASUREMENT_INTERVAL_S = 0.1
MAX_MEASUREMENT_INTERVAL_S = 0.1
MAX_DEADBAND_BY_BASE_FREQUENCY_HZ = {
    50.0: 0.01,
    60.0: 0.012,
}


def _get_deadband_limit_hz(base_frequency_hz: float) -> float:
    return MAX_DEADBAND_BY_BASE_FREQUENCY_HZ.get(
        float(base_frequency_hz),
        MAX_DEADBAND_BY_BASE_FREQUENCY_HZ[50.0],
    )


def _sanitize_measurement_interval_s(interval_s: float) -> float:
    if interval_s <= 0:
        return DEFAULT_MEASUREMENT_INTERVAL_S
    return min(interval_s, MAX_MEASUREMENT_INTERVAL_S)


def _measurement_interval_from_plan(plan: Optional[dict]) -> float:
    if not plan:
        return DEFAULT_MEASUREMENT_INTERVAL_S

    try:
        interval_ms = float(plan.get("measurement_interval_ms", 100))
    except (AttributeError, TypeError, ValueError):
        return DEFAULT_MEASUREMENT_INTERVAL_S

    return _sanitize_measurement_interval_s(interval_ms / 1000.0)


class FrequencyRuntimeConfig:
    def __init__(self, measurement_interval_s: float = DEFAULT_MEASUREMENT_INTERVAL_S):
        self._lock = threading.Lock()
        self._measurement_interval_s = _sanitize_measurement_interval_s(measurement_interval_s)

    def set_measurement_interval_s(self, interval_s: float) -> float:
        interval_s = _sanitize_measurement_interval_s(interval_s)
        with self._lock:
            self._measurement_interval_s = interval_s
        return interval_s

    def get_measurement_interval_s(self) -> float:
        with self._lock:
            return self._measurement_interval_s

# ---------------------------------------------------------------------------
# Alarm definitions  (Day 7)
# ---------------------------------------------------------------------------
ALARM_DEFS = {
    # block: "charge" = block negative kW, "discharge" = block positive kW
    "BMS0000": {"level": "major", "decision_time_s": 0,  "block": "charge",    "desc": "SOC >= 100% (overcharge)"},
    "BMS0001": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "SOC >= 90% (high warning)"},
    "BMS0002": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "SOC <= 10% (low warning)"},
    "BMS0003": {"level": "major", "decision_time_s": 0,  "block": "discharge", "desc": "SOC <= 0% (deep discharge)"},
    "PMS0000": {"level": "major", "decision_time_s": 0,  "block": "charge",    "desc": "BMS1 overcharge forwarded"},
    "PMS0001": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "BMS1 high SOC forwarded"},
    "PMS0002": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "BMS1 low SOC forwarded"},
    "PMS0003": {"level": "major", "decision_time_s": 0,  "block": "discharge", "desc": "BMS1 deep discharge forwarded"},
    "PMS0008": {"level": "major", "decision_time_s": 0,  "block": "charge",    "desc": "BMS2 overcharge forwarded"},
    "PMS0009": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "BMS2 high SOC forwarded"},
    "PMS0010": {"level": "minor", "decision_time_s": 30, "block": None,         "desc": "BMS2 low SOC forwarded"},
    "PMS0011": {"level": "major", "decision_time_s": 0,  "block": "discharge", "desc": "BMS2 deep discharge forwarded"},
}


def decode_bms_alarm(alarm_u16: int) -> list[str]:
    """Decode BMS alarm uint16 → list of alarm code strings."""
    codes = []
    for bit in range(4):
        if alarm_u16 & (1 << bit):
            codes.append(f"BMS{bit:04d}")
    return codes


def decode_pms_alarm(alarm_u16: int) -> list[str]:
    """Decode PMS alarm uint16 → list of alarm code strings."""
    codes = []
    for bit in (0, 1, 2, 3, 8, 9, 10, 11):
        if alarm_u16 & (1 << bit):
            codes.append(f"PMS{bit:04d}")
    return codes

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


def collect_device_data_modbus(host: str, port: int, rtu_bridge_url: str = None,
                               api_url: str = None) -> list[dict]:
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
        client = ModbusTcpClient(host, port=device_port, timeout=2)
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
        ir = c.read_input_registers(0, count=5, device_id=1)
        if hr.isError() or ir.isError():
            return None
        return {
            "device_id": "PMS", "timestamp": now,
            "demand_control_power": round(_s16(hr.registers[0]) * SCALE, 1),
            "total_active_power": round(_s16(ir.registers[0]) * SCALE, 1),
            "soc_avg": ir.registers[1],
            "soh_avg": ir.registers[2],
            "capacity_total": round(ir.registers[3] * SCALE, 1),
            "alarm": ir.registers[4],
        }
    res = _read_device(port, _read_pms)
    if res:
        alarm_val = res.get("alarm", 0)
        res["occurs_alarms"] = decode_pms_alarm(alarm_val)
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
            ir = c.read_input_registers(0, count=4, device_id=1)
            if ir.isError():
                return None
            return {
                "device_id": _name, "timestamp": now,
                "soc": ir.registers[0],
                "soh": ir.registers[1],
                "capacity": round(ir.registers[2] * SCALE, 1),
                "alarm": ir.registers[3],
            }
        res = _read_device(port + offset, _read_bms)
        if res:
            alarm_val = res.get("alarm", 0)
            res["occurs_alarms"] = decode_bms_alarm(alarm_val)
            devices.append(res)

    # --- Multimeter (via RTU bridge HTTP) ---
    if rtu_bridge_url:
        # Check API toggle (skip poll if disabled)
        mm_enabled = True
        if api_url:
            try:
                sr = requests.get(f"{api_url.rstrip('/')}/api/settings/multimeter", timeout=2)
                if sr.ok:
                    mm_enabled = sr.json().get("enabled", True)
            except Exception:
                pass  # default ON if API unreachable
        if not mm_enabled:
            log.debug("Multimeter disabled via dashboard toggle")
        else:
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

    # --- Transducer (base+6) ---
    def _read_transducer(c):
        ir = c.read_input_registers(0, count=1, device_id=1)
        if ir.isError():
            return None
        freq_raw = ir.registers[0]
        return {
            "device_id": "Transducer",
            "timestamp": now,
            "frequency_hz": round(freq_raw * 0.001, 3),
        }
    res = _read_device(port + 6, _read_transducer)
    if res:
        devices.append(res)

    return devices if devices else collect_device_data_mock()


# ---------------------------------------------------------------------------
# Loop 1: Upload device data (every 10s)
# ---------------------------------------------------------------------------
def data_upload_loop(tm: TokenManager, api_url: str, collector_fn,
                     alarm_tracker: AlarmTracker, interval: int = 10):
    """Collect device data and POST to API every `interval` seconds."""
    api_url = api_url.rstrip("/")
    while True:
        try:
            devices = collector_fn()
            for dev_data in devices:
                # Read status from alarm tracker (updated by schedule_execution_loop)
                dev_id = dev_data["device_id"]
                if dev_id in ("BMS1", "BMS2", "PMS"):
                    dev_data["status"] = alarm_tracker.get_status(dev_id)
                else:
                    dev_data.setdefault("status", "normal")
                    dev_data.setdefault("occurs_alarms", [])
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
        client = ModbusTcpClient(modbus_host, port=modbus_port, timeout=2)
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
                      interval: int = 10,
                      alarm_tracker: Optional["AlarmTracker"] = None,
                      device_id: str = DEFAULT_DEVICE_ID):
    """Poll queued commands, set executing, apply to Modbus, then set final status."""
    api_url = api_url.rstrip("/")
    while True:
        try:
            resp = requests.get(
                f"{api_url}/api/commands?status=queued&device_id={device_id}",
                headers=tm.headers(),
                timeout=10,
            )
            if resp.ok:
                cmds = resp.json()
                for cmd in cmds:
                    cmd_id = cmd.get("id")
                    cmd_ref = cmd.get("command_id") or f"id-{cmd_id}"
                    cmd_device = cmd.get("device_id") or device_id
                    cmd_kw = cmd.get("power_kw") or 0
                    cmd_type = cmd.get("command", "")
                    log.info(">>> Received %s [%s] %s %.1f kW",
                             cmd_ref, cmd_device, cmd_type, cmd_kw)

                    # Move lifecycle queued -> executing before attempting Modbus write.
                    begin_resp = requests.patch(
                        f"{api_url}/api/commands/{cmd_id}",
                        headers=tm.headers(),
                        json={"status": STATUS_EXECUTING, "command_id": cmd.get("command_id"), "device_id": cmd_device},
                        timeout=10,
                    )
                    if not begin_resp.ok:
                        log.warning("    Cannot mark %s as executing: %s", cmd_ref, begin_resp.text[:140])
                        continue

                    # Directional block check
                    blocked = alarm_tracker.get_blocked_directions() if alarm_tracker else set()
                    if cmd_type == "charge" and "charge" in blocked:
                        log.warning("    %s BLOCKED — charge not allowed (SOC full)", cmd_ref)
                        status = STATUS_BLOCKED
                    elif cmd_type == "discharge" and "discharge" in blocked:
                        log.warning("    %s BLOCKED — discharge not allowed (SOC empty)", cmd_ref)
                        status = STATUS_BLOCKED
                    else:
                        # Apply command to Modbus simulator (write PMS HR0)
                        ok = _apply_command_to_modbus(modbus_host, modbus_port, cmd)
                        status = STATUS_EXECUTED if ok else STATUS_FAILED

                    # Mark final lifecycle status
                    patch_resp = requests.patch(
                        f"{api_url}/api/commands/{cmd_id}",
                        headers=tm.headers(),
                        json={"status": status, "command_id": cmd.get("command_id"), "device_id": cmd_device},
                        timeout=10,
                    )
                    if patch_resp.ok:
                        log.info("    %s marked %s", cmd_ref, status)
                    else:
                        log.warning("    Failed to finalize %s: %s", cmd_ref, patch_resp.text[:140])
            else:
                log.warning("Command poll failed: %d", resp.status_code)
        except Exception as e:
            log.error("Command poll error: %s", e)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Helpers: write kW to PMS HR0
# ---------------------------------------------------------------------------
def _write_pms_hr0(modbus_host: str, modbus_port: int, demand_kw: float) -> bool:
    """Encode *demand_kw* to int16 (scale 0.1) and write PMS HR0."""
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        log.warning("pymodbus not installed — cannot write to simulator")
        return False

    raw = int(round(demand_kw / 0.1))
    if raw < 0:
        raw = raw + 0x10000
    raw = raw & 0xFFFF

    try:
        client = ModbusTcpClient(modbus_host, port=modbus_port, timeout=2)
        if not client.connect():
            log.error("Cannot connect to PMS %s:%d", modbus_host, modbus_port)
            return False
        wr = client.write_register(0, raw, device_id=1)
        client.close()
        if wr.isError():
            log.error("PMS HR0 write failed: %s", wr)
            return False
        return True
    except Exception as e:
        log.error("Modbus write error: %s", e)
        return False


def _read_multimeter_power(rtu_bridge_url: str) -> Optional[float]:
    """Read active_power_kw from RTU bridge HTTP endpoint."""
    if not rtu_bridge_url:
        return None
    try:
        resp = requests.get(rtu_bridge_url, timeout=2)
        if resp.ok:
            return resp.json().get("active_power_kw")
    except Exception as e:
        log.warning("Multimeter read failed: %s", e)
    return None


# ---------------------------------------------------------------------------
# Loop 3: Schedule-based execution (10s/cycle)
#   Day 3 — basic schedule fetch + write
#   Day 4 — ramp rate limiting
#   Day 5 — feedback loop
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Alarm tracker engine  (Day 8)
# ---------------------------------------------------------------------------
class AlarmTracker:
    """Track alarm persistence per device, determine confirmed status."""

    def __init__(self):
        self._lock = threading.Lock()
        # device_name → {code → first_seen_time}
        self._active: dict[str, dict[str, float]] = {}
        # device_name → "normal" | "minor" | "major"
        self._status: dict[str, str] = {}

    def update(self, device_name: str, occurs_alarms: list[str]) -> str:
        """Update tracker with current alarm codes, return new status."""
        with self._lock:
            return self._update_locked(device_name, occurs_alarms)

    def _update_locked(self, device_name: str, occurs_alarms: list[str]) -> str:
        now = time.monotonic()
        prev = self._active.setdefault(device_name, {})

        # Add new, keep existing
        current_codes = set(occurs_alarms)
        for code in current_codes:
            if code not in prev:
                prev[code] = now

        # Remove cleared alarms
        for code in list(prev):
            if code not in current_codes:
                del prev[code]

        # Determine confirmed status
        status = "normal"
        for code, first_seen in prev.items():
            defn = ALARM_DEFS.get(code)
            if defn is None:
                continue
            duration = now - first_seen
            if duration >= defn["decision_time_s"]:
                # Confirmed
                if defn["level"] == "major":
                    status = "major"
                    break           # major is highest, no need to continue
                elif defn["level"] == "minor" and status != "major":
                    status = "minor"

        self._status[device_name] = status
        return status

    def any_major(self) -> bool:
        """Return True if any device currently has confirmed major alarm."""
        with self._lock:
            return any(s == "major" for s in self._status.values())

    def get_blocked_directions(self) -> set[str]:
        """Return set of blocked directions: {'charge'}, {'discharge'}, or both.

        Only considers *confirmed* major alarms (past decision_time).
        """
        with self._lock:
            blocked: set[str] = set()
            for dev_alarms in self._active.values():
                now = time.monotonic()
                for code, first_seen in dev_alarms.items():
                    defn = ALARM_DEFS.get(code)
                    if defn is None or defn["level"] != "major":
                        continue
                    if now - first_seen < defn["decision_time_s"]:
                        continue
                    direction = defn.get("block")
                    if direction:
                        blocked.add(direction)
            return blocked

    def get_status(self, device_name: str) -> str:
        with self._lock:
            return self._status.get(device_name, "normal")


def schedule_execution_loop(
    tm: TokenManager,
    api_url: str,
    modbus_host: str = "127.0.0.1",
    modbus_port: int = 15020,
    rtu_bridge_url: Optional[str] = None,
    alarm_tracker: Optional["AlarmTracker"] = None,
    freq_mode_active: Optional[threading.Event] = None,
):
    """Fetch active schedules and execute them with ramp + feedback."""
    api_url = api_url.rstrip("/")

    # State persisted across cycles
    current_command_kw: float = 0.0
    is_ramping: bool = False
    if alarm_tracker is None:
        alarm_tracker = AlarmTracker()

    while True:
        try:
            # ── 1. Fetch active schedules ─────────────────────────────
            resp = requests.get(
                f"{api_url}/api/schedules/active",
                headers=tm.headers(),
                timeout=10,
            )
            if not resp.ok:
                log.warning("Schedule fetch failed: %d", resp.status_code)
                time.sleep(CYCLE_INTERVAL_S)
                continue

            schedules = resp.json()   # sorted by start_time ASC
            now = datetime.now(timezone.utc)

            # ── 2. Find current & next schedule ──────────────────────
            current_schedule = None
            next_schedule = None

            for s in schedules:
                st = datetime.fromisoformat(s["start_time"])
                et = datetime.fromisoformat(s["end_time"])
                if st <= now < et:
                    current_schedule = s
                    break

            if current_schedule is not None:
                cur_end = datetime.fromisoformat(current_schedule["end_time"])
                for s in schedules:
                    st = datetime.fromisoformat(s["start_time"])
                    if st == cur_end:
                        next_schedule = s
                        break

            # ── 3. Determine effective schedule (with pre-ramp) ──────
            recent_schedule = current_schedule   # default

            if next_schedule is not None and current_schedule is not None:
                next_power = next_schedule["control_power_kw"] or 0.0
                delta = abs(next_power - current_command_kw)
                if delta > 0:
                    ramp_cycles = math.ceil(delta / MAX_DELTA_PER_CYCLE)
                    ramp_duration_s = ramp_cycles * CYCLE_INTERVAL_S
                    next_start = datetime.fromisoformat(next_schedule["start_time"])
                    ramp_start = next_start - timedelta(seconds=ramp_duration_s)
                    if now >= ramp_start:
                        recent_schedule = next_schedule
                        log.info("Pre-ramping toward next schedule #%s (%.1f kW)",
                                 next_schedule["id"], next_power)

            # ── 4. Compute target ────────────────────────────────────
            if recent_schedule is None:
                target_kw = 0.0
            else:
                raw_target = recent_schedule["control_power_kw"] or 0.0
                target_kw = max(-MAX_POWER_KW, min(MAX_POWER_KW, raw_target))

            # ── 4b. Alarm check — read alarm registers (Day 8) ─────
            try:
                from pymodbus.client import ModbusTcpClient as _McpCli
                for _dev, _off, _decode in [
                    ("BMS1", 4, decode_bms_alarm),
                    ("BMS2", 5, decode_bms_alarm),
                    ("PMS",  0, decode_pms_alarm),
                ]:
                    _alarm_addr = 3 if _dev.startswith("BMS") else 4
                    _c = _McpCli(modbus_host, port=modbus_port + _off, timeout=2)
                    if _c.connect():
                        _rr = _c.read_input_registers(_alarm_addr, count=1, device_id=1)
                        _c.close()
                        if not _rr.isError():
                            _codes = _decode(_rr.registers[0])
                            _st = alarm_tracker.update(_dev, _codes)
                            if _codes:
                                log.info("Alarm %s: codes=%s status=%s", _dev, _codes, _st)
                    else:
                        alarm_tracker.update(_dev, [])
            except ImportError:
                pass
            except Exception as _e:
                log.warning("Alarm read error: %s", _e)

            # ── 4c. Directional block — block harmful direction only ──
            blocked = alarm_tracker.get_blocked_directions()
            if blocked:
                if target_kw < 0 and "charge" in blocked:
                    log.warning("MAJOR alarm — charge blocked (SOC full), forcing 0 kW")
                    target_kw = 0.0
                elif target_kw > 0 and "discharge" in blocked:
                    log.warning("MAJOR alarm — discharge blocked (SOC empty), forcing 0 kW")
                    target_kw = 0.0
                # If both directions blocked (edge: BMS1 full + BMS2 empty), force 0
                if "charge" in blocked and "discharge" in blocked:
                    log.warning("MAJOR alarm — both directions blocked, forcing 0 kW")
                    target_kw = 0.0

            # ── 5. Apply ramp rate limiting ──────────────────────────
            diff = target_kw - current_command_kw
            if abs(diff) <= MAX_DELTA_PER_CYCLE:
                new_command_kw = target_kw
                is_ramping = False
            else:
                sign = 1 if diff > 0 else -1
                new_command_kw = current_command_kw + sign * MAX_DELTA_PER_CYCLE
                is_ramping = True
                log.info("Ramping: %.1f → %.1f (target %.1f, delta/cycle %.1f)",
                         current_command_kw, new_command_kw, target_kw, MAX_DELTA_PER_CYCLE)

            # ── 6. Feedback loop (Day 5) ─────────────────────────────
            #   Conditions: ramp finished  AND  target != 0
            if (not is_ramping
                    and recent_schedule is not None
                    and target_kw != 0.0
                    and rtu_bridge_url):
                actual_kw = _read_multimeter_power(rtu_bridge_url)
                if actual_kw is not None:
                    error = target_kw - actual_kw
                    tolerance = abs(target_kw) * (FEEDBACK_TOLERANCE_PCT / 100)
                    if abs(error) > tolerance:
                        new_command_kw = new_command_kw + error
                        new_command_kw = max(-MAX_POWER_KW, min(MAX_POWER_KW, new_command_kw))
                        log.info("Feedback: target=%.1f actual=%.1f error=%.1f → cmd=%.1f",
                                 target_kw, actual_kw, error, new_command_kw)
                    else:
                        log.debug("Feedback OK: |error|=%.1f within %.1f%% tolerance",
                                  abs(error), FEEDBACK_TOLERANCE_PCT)

            # ── 7. Write to PMS HR0 (skip if frequency mode is active) ─
            if freq_mode_active and freq_mode_active.is_set():
                log.debug("Schedule exec: skipping PMS write — frequency mode active")
                current_command_kw = new_command_kw
                time.sleep(CYCLE_INTERVAL_S)
                continue

            ok = _write_pms_hr0(modbus_host, modbus_port, new_command_kw)
            if ok:
                log.info("Schedule exec: wrote %.1f kW to PMS HR0 (target=%.1f, sched=%s)",
                         new_command_kw, target_kw,
                         recent_schedule["id"] if recent_schedule else "none")
            else:
                log.warning("Schedule exec: PMS HR0 write FAILED")

            current_command_kw = new_command_kw

        except Exception as e:
            log.error("Schedule execution error: %s", e)

        time.sleep(CYCLE_INTERVAL_S)


# ---------------------------------------------------------------------------
# Loop 4: Transducer polling (<= 0.1s) — read frequency from Modbus
# ---------------------------------------------------------------------------
def transducer_poll_loop(
    modbus_host: str,
    transducer_port: int,
    freq_buffer: collections.deque,
    runtime_config: Optional[FrequencyRuntimeConfig] = None,
):
    """Read transducer IR0 at the configured measurement interval."""
    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        log.warning("pymodbus not installed — transducer polling disabled")
        return

    log_counter = 0

    while True:
        cycle_started = time.monotonic()
        try:
            client = ModbusTcpClient(modbus_host, port=transducer_port, timeout=2)
            if client.connect():
                rr = client.read_input_registers(0, count=1, device_id=1)
                client.close()
                if not rr.isError():
                    freq_hz = round(rr.registers[0] * 0.001, 3)
                    freq_buffer.append(freq_hz)
                    log_counter += 1
                    if log_counter % 50 == 0:  # INFO every 5s
                        log.info("transducer_sample: %.3f Hz (buffer=%d)", freq_hz, len(freq_buffer))
                    else:
                        log.debug("transducer_sample: %.3f Hz", freq_hz)
                else:
                    log.warning("Transducer read error: %s", rr)
            else:
                log.warning("Cannot connect to transducer %s:%d", modbus_host, transducer_port)
        except Exception as e:
            log.warning("Transducer poll error: %s", e)
        interval_s = (
            runtime_config.get_measurement_interval_s()
            if runtime_config is not None
            else DEFAULT_MEASUREMENT_INTERVAL_S
        )
        sleep_s = max(0.0, interval_s - (time.monotonic() - cycle_started))
        time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Loop 5: Frequency control (ichiji 一次調整力) — droop command every 0.5s
# ---------------------------------------------------------------------------
def compute_frequency_command(plan: dict, freq_samples: list, blocked_directions: set) -> dict:
    """Compute PMS command from frequency plan and recent samples.

    Returns dict with f_eff, delta_f, delta_p_kw, p_cmd_kw, abnormal_event.
    """
    n = min(len(freq_samples), 5)
    f_eff = sum(freq_samples[-5:]) / n

    base_f = plan["base_frequency_hz"]
    deadband_hz = min(
        max(float(plan.get("deadband_hz", 0.0)), 0.0),
        _get_deadband_limit_hz(base_f),
    )
    delta_f = base_f - f_eff

    if abs(delta_f) <= deadband_hz:
        delta_p_signed = 0.0
    else:
        delta_f_eff = abs(delta_f) - deadband_hz
        f_full = base_f * (plan["droop_percent"] / 100.0)
        ratio = min(delta_f_eff / f_full, 1.0) if f_full > 0 else 0.0
        delta_p_raw = ratio * plan["awarded_power_kw"]
        # delta_f > 0 means freq below base → discharge (+kW)
        delta_p_signed = delta_p_raw if delta_f > 0 else -delta_p_raw

    p_target = plan["baseline_power_kw"] + delta_p_signed

    # Directional block: if blocked, keep baseline
    if delta_p_signed > 0 and "discharge" in blocked_directions:
        p_target = plan["baseline_power_kw"]
        delta_p_signed = 0.0
    elif delta_p_signed < 0 and "charge" in blocked_directions:
        p_target = plan["baseline_power_kw"]
        delta_p_signed = 0.0

    p_cmd = max(-MAX_POWER_KW, min(MAX_POWER_KW, p_target))
    abnormal = f_eff <= plan.get("abnormal_frequency_hz", 49.8)

    return {
        "f_eff": round(f_eff, 3),
        "delta_f": round(delta_f, 4),
        "delta_p_kw": round(delta_p_signed, 1),
        "p_cmd_kw": round(p_cmd, 1),
        "abnormal_event": abnormal,
        "baseline_power_kw": plan["baseline_power_kw"],
        "deadband_hz": round(deadband_hz, 4),
        "plan_id": plan.get("id"),
    }


def frequency_control_loop(
    tm: TokenManager,
    api_url: str,
    modbus_host: str,
    modbus_port: int,
    freq_buffer: collections.deque,
    alarm_tracker: AlarmTracker,
    freq_mode_active: threading.Event,
    runtime_config: Optional[FrequencyRuntimeConfig] = None,
    device_id: str = DEFAULT_DEVICE_ID,
    interval: float = 0.5,
):
    """Fetch active frequency plan and compute droop command every 0.5s."""
    api_url = api_url.rstrip("/")
    cached_plan = None
    plan_fetch_time = 0.0

    while True:
        try:
            # Re-fetch plan every 10s
            now_mono = time.monotonic()
            if cached_plan is None or (now_mono - plan_fetch_time) > 10.0:
                try:
                    resp = requests.get(
                        f"{api_url}/api/plans/active?device_id={device_id}",
                        timeout=5,
                    )
                    if resp.ok:
                        cached_plan = resp.json()  # None if no active plan
                    else:
                        cached_plan = None
                except Exception as e:
                    log.warning("Frequency plan fetch error: %s", e)
                plan_fetch_time = now_mono

            if not cached_plan:
                freq_mode_active.clear()
                if runtime_config is not None:
                    runtime_config.set_measurement_interval_s(DEFAULT_MEASUREMENT_INTERVAL_S)
                time.sleep(interval)
                continue

            # Signal that frequency mode is active
            freq_mode_active.set()
            if runtime_config is not None:
                runtime_config.set_measurement_interval_s(
                    _measurement_interval_from_plan(cached_plan)
                )

            # Need at least 5 samples
            if len(freq_buffer) < 5:
                log.info("frequency_control: waiting for samples (%d/5)", len(freq_buffer))
                time.sleep(interval)
                continue

            # Compute command
            blocked = alarm_tracker.get_blocked_directions()
            result = compute_frequency_command(
                cached_plan,
                list(freq_buffer),
                blocked,
            )

            # Write PMS HR0
            ok = _write_pms_hr0(modbus_host, modbus_port, result["p_cmd_kw"])

            log.info(
                "frequency_compute: f_eff=%.3f Δf=%.4f ΔP=%.1f cmd=%.1f kW abnormal=%s write=%s",
                result["f_eff"], result["delta_f"], result["delta_p_kw"],
                result["p_cmd_kw"], result["abnormal_event"], "OK" if ok else "FAIL",
            )

            # Upload snapshot as device_id="frequency-control"
            try:
                snapshot_data = {
                    "device_id": "frequency-control",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **result,
                    "blocked_directions": list(blocked),
                }
                requests.post(
                    f"{api_url}/api/device",
                    headers=tm.headers(),
                    json=snapshot_data,
                    timeout=5,
                )
            except Exception as e:
                log.debug("Frequency snapshot upload error: %s", e)

        except Exception as e:
            log.error("Frequency control error: %s", e)

        time.sleep(interval)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Local client for BESS API")
    parser.add_argument("--api-url", required=True, help="EC2 API URL (http://<ip>:8000)")
    parser.add_argument("--secret-key", required=True, help="Server secret key")
    parser.add_argument("--client-key", default="local-agent-1", help="Client identifier")
    parser.add_argument("--device-id", default=DEFAULT_DEVICE_ID, help="Target device id for command polling")
    parser.add_argument("--modbus-host", default="127.0.0.1")
    parser.add_argument("--modbus-port", type=int, default=15020)
    parser.add_argument("--rtu-bridge", default="http://localhost:8081/api/multimeter",
                        help="RTU bridge URL for multimeter (set to empty to disable)")
    parser.add_argument("--mock", action="store_true", help="Use mock data instead of Modbus")
    parser.add_argument("--interval", type=int, default=10, help="Poll interval in seconds")
    parser.add_argument("--transducer-port", type=int, default=15026,
                        help="Transducer Modbus TCP port (default 15026)")
    parser.add_argument("--no-transducer", action="store_true",
                        help="Disable transducer polling and frequency control")
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
        collector = lambda: collect_device_data_modbus(args.modbus_host, args.modbus_port, rtu_url, args.api_url)
        log.info("Using Modbus data from %s:%d", args.modbus_host, args.modbus_port)
        if rtu_url:
            log.info("RTU bridge: %s", rtu_url)

    # Shared alarm tracker
    alarm_tracker = AlarmTracker()

    # Shared frequency state
    freq_buffer = collections.deque(maxlen=50)  # recent transducer samples
    freq_mode_active = threading.Event()
    frequency_runtime_config = FrequencyRuntimeConfig()

    # Start all loops
    t1 = threading.Thread(target=data_upload_loop,
                          args=(tm, args.api_url, collector, alarm_tracker, args.interval),
                          daemon=True)
    t2 = threading.Thread(target=command_poll_loop,
                          args=(tm, args.api_url, args.modbus_host, args.modbus_port,
                                args.interval, alarm_tracker, args.device_id),
                          daemon=True)
    t3 = threading.Thread(target=schedule_execution_loop,
                          args=(tm, args.api_url, args.modbus_host, args.modbus_port,
                                rtu_url, alarm_tracker, freq_mode_active),
                          daemon=True)

    t1.start()
    t2.start()
    t3.start()

    # Transducer + frequency control threads (Loop 4 & 5)
    if not args.no_transducer:
        t4 = threading.Thread(target=transducer_poll_loop,
                              args=(args.modbus_host, args.transducer_port,
                                    freq_buffer, frequency_runtime_config),
                              daemon=True)
        t5 = threading.Thread(target=frequency_control_loop,
                              args=(tm, args.api_url, args.modbus_host, args.modbus_port,
                                    freq_buffer, alarm_tracker, freq_mode_active,
                                    frequency_runtime_config, args.device_id, 0.5),
                              daemon=True)
        t4.start()
        t5.start()
        log.info("Transducer polling (port=%d) and frequency control started.",
                 args.transducer_port)
    else:
        log.info("Transducer/frequency control disabled (--no-transducer)")

    log.info("Local client running (device_id=%s, interval=%ds). Press Ctrl+C to stop.",
             args.device_id, args.interval)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
