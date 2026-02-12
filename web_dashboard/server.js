/**
 * server.js — Express HTTP server + Modbus poller bootstrap.
 *
 * Endpoints:
 *   GET  /api/snapshot          → current device snapshot (JSON)
 *   POST /api/pms/demand        → write demand_control_power_kw to PMS HR0 via FC06
 *
 * Static files served from ./public (web UI).
 */

const path = require("path");
const express = require("express");
const ModbusClient = require("./modbus_client");
const { getSnapshot, startPolling } = require("./poller");

// ---- Configuration ----
const HTTP_PORT = 3000;
const MODBUS_HOST = "127.0.0.1";
const MODBUS_PORT = 15020;
const POLL_INTERVAL_MS = 1000;

// PMS unit_id and HR0 address
const PMS_UNIT_ID = 1;
const PMS_HR0_ADDR = 0;

// Business-level power limits (kW) — int16 × 0.1 scale
const POWER_MIN_KW = -3276.8;
const POWER_MAX_KW = 3276.7;

// ---- Last command state (server-side, survives page refresh) ----
let lastCommand = { kw: null, raw: null, ts: null, error: null };

// ---- Express app ----
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ---- Modbus client (shared with poller) ----
const modbusClient = new ModbusClient(MODBUS_HOST, MODBUS_PORT);

// ---- API: GET /api/snapshot ----
app.get("/api/snapshot", (_req, res) => {
  const snap = getSnapshot();
  res.json({ ...snap, last_command: lastCommand, config: { host: MODBUS_HOST, port: MODBUS_PORT, power_min_kw: POWER_MIN_KW, power_max_kw: POWER_MAX_KW } });
});

// ---- API: POST /api/pms/demand ----
app.post("/api/pms/demand", async (req, res) => {
  try {
    const { demand_control_power_kw } = req.body;

    // Validate input
    if (demand_control_power_kw === undefined || demand_control_power_kw === null) {
      return res.status(400).json({ ok: false, error: "Missing demand_control_power_kw" });
    }

    const kw = parseFloat(demand_control_power_kw);
    if (isNaN(kw)) {
      return res.status(400).json({ ok: false, error: "demand_control_power_kw must be a number" });
    }
    if (kw < POWER_MIN_KW || kw > POWER_MAX_KW) {
      return res.status(400).json({
        ok: false,
        error: `demand_control_power_kw must be between ${POWER_MIN_KW} and ${POWER_MAX_KW}`,
      });
    }

    // Encode and write via FC06
    const rawU16 = ModbusClient.encodePowerKw(kw);
    await modbusClient.ensureConnection();
    await modbusClient.writeHR(PMS_UNIT_ID, PMS_HR0_ADDR, rawU16);

    const writtenKw = parseFloat(ModbusClient.decodePowerKw(rawU16).toFixed(1));
    console.log(`[api] Wrote PMS HR0 = ${writtenKw} kW (raw=${rawU16})`);

    lastCommand = { kw: writtenKw, raw: rawU16, ts: new Date().toISOString(), error: null };
    res.json({ ok: true, written_raw: rawU16, written_kw: writtenKw });
  } catch (err) {
    console.error(`[api] Write PMS demand error: ${err.message}`);
    lastCommand = { ...lastCommand, error: err.message, ts: new Date().toISOString() };
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ---- Start ----
async function main() {
  try {
    await modbusClient.connect();
  } catch (err) {
    console.warn(`[main] Initial Modbus connect failed: ${err.message} — poller will retry.`);
  }

  // Start polling loop (runs in background)
  const poller = startPolling(modbusClient, POLL_INTERVAL_MS);
  console.log(`[main] Poller started (interval=${POLL_INTERVAL_MS}ms)`);

  app.listen(HTTP_PORT, () => {
    console.log(`[main] Web dashboard: http://localhost:${HTTP_PORT}`);
    console.log(`[main] Modbus target: ${MODBUS_HOST}:${MODBUS_PORT}`);
  });
}

main();
