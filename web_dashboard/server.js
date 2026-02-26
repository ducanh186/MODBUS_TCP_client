/**
 * server.js — Express HTTP server + Modbus poller bootstrap + telemetry collector.
 *
 * Endpoints:
 *   GET  /api/snapshot          → current device snapshot (JSON)
 *   POST /api/pms/demand        → write demand_control_power_kw to PMS HR0 via FC06
 *
 * Collector: every COLLECT_INTERVAL_MS (default 10s), reads poller snapshot,
 * transforms to telemetry rows, and batch-inserts into PostgreSQL.
 *
 * Static files served from ./public (web UI).
 */

require("dotenv").config();

const path = require("path");
const express = require("express");
const ModbusClient = require("./modbus_client");
const { getSnapshot, startPolling, DEVICE_PORTS } = require("./poller");
const { pool, insertTelemetryBatch } = require("./db");
const { buildTelemetryRows } = require("./transform");

// ---- Configuration ----
const HTTP_PORT = 3000;
const MODBUS_HOST = "127.0.0.1";
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

// ---- One Modbus client per device port ----
const clients = {
  pms:  new ModbusClient(MODBUS_HOST, DEVICE_PORTS.pms.port),
  pcs1: new ModbusClient(MODBUS_HOST, DEVICE_PORTS.pcs1.port),
  pcs2: new ModbusClient(MODBUS_HOST, DEVICE_PORTS.pcs2.port),
  bms1: new ModbusClient(MODBUS_HOST, DEVICE_PORTS.bms1.port),
  bms2: new ModbusClient(MODBUS_HOST, DEVICE_PORTS.bms2.port),
};

// ---- API: GET /api/snapshot ----
app.get("/api/snapshot", (_req, res) => {
  const snap = getSnapshot();
  res.json({
    ...snap,
    last_command: lastCommand,
    config: {
      host: MODBUS_HOST,
      port: DEVICE_PORTS.pms.port,
      power_min_kw: POWER_MIN_KW,
      power_max_kw: POWER_MAX_KW,
    },
  });
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
    await clients.pms.ensureConnection();
    await clients.pms.writeHR(PMS_UNIT_ID, PMS_HR0_ADDR, rawU16);

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

// ---- API: GET /api/timeline ----
// Query params: device (default "pms"), metric (default "active_power_kw"),
//               minutes (default 60), limit (default 300, max 2000)
app.get("/api/timeline", async (req, res) => {
  try {
    const device  = String(req.query.device  ?? "pms");
    const metric  = String(req.query.metric  ?? "active_power_kw");
    const minutes = Number(req.query.minutes ?? 60);
    const limit   = Math.min(Number(req.query.limit ?? 300), 2000);

    const sql = `
      SELECT ts, device_id, metric, value, unit, comm_ok
      FROM telemetry
      WHERE device_id = $1
        AND metric    = $2
        AND ts >= now() - ($3::text || ' minutes')::interval
      ORDER BY ts ASC
      LIMIT $4
    `;
    const result = await pool.query(sql, [device, metric, minutes, limit]);
    res.json(result.rows);
  } catch (err) {
    console.error(`[api] /api/timeline error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ---- API: GET /api/timeline/metrics ----
// Returns list of distinct (device_id, metric) combinations available in DB
app.get("/api/timeline/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT device_id, metric
      FROM telemetry
      ORDER BY device_id, metric
    `);
    res.json(result.rows);
  } catch (err) {
    console.error(`[api] /api/timeline/metrics error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// ---- Telemetry collector (persist snapshot → PostgreSQL every N seconds) ----
const COLLECT_INTERVAL_MS = Number(process.env.COLLECT_INTERVAL_MS || 10_000);
let collectInFlight = false;

async function collectTick() {
  if (collectInFlight) {
    console.warn("[collector] tick skipped (previous still running)");
    return;
  }
  collectInFlight = true;
  const started = Date.now();
  try {
    const snap = getSnapshot();
    if (!snap || !snap.ts) return; // no data yet

    const ts = new Date(); // collector tick time
    const rows = buildTelemetryRows(snap, ts);

    if (rows.length) {
      await insertTelemetryBatch(rows);
      console.log(
        `[collector] inserted=${rows.length} ts=${ts.toISOString()} latency_ms=${Date.now() - started}`
      );
    }
  } catch (err) {
    console.error(`[collector] tick failed: ${err.message}`);
  } finally {
    collectInFlight = false;
  }
}

// ---- Start ----
async function main() {
  // Try initial connects (non-fatal — poller will retry)
  await Promise.allSettled(
    Object.entries(clients).map(async ([name, c]) => {
      try {
        await c.connect();
      } catch (err) {
        console.warn(`[main] Initial connect ${name} (port ${DEVICE_PORTS[name].port}) failed: ${err.message}`);
      }
    })
  );

  // Start polling loop (runs in background)
  const poller = startPolling(clients, POLL_INTERVAL_MS);
  console.log(`[main] Poller started (interval=${POLL_INTERVAL_MS}ms)`);

  // Start telemetry collector (persist to DB)
  if (process.env.DATABASE_URL) {
    setInterval(() => collectTick().catch(console.error), COLLECT_INTERVAL_MS);
    console.log(`[main] Collector started (interval=${COLLECT_INTERVAL_MS}ms, db=${process.env.DATABASE_URL.replace(/\/\/.*@/, "//***@")})`);
  } else {
    console.warn("[main] DATABASE_URL not set — collector disabled");
  }

  app.listen(HTTP_PORT, () => {
    console.log(`[main] Web dashboard: http://localhost:${HTTP_PORT}`);
    console.log(`[main] Polling devices on ports: ${Object.entries(DEVICE_PORTS).map(([k,v]) => `${k}=${v.port}`).join(", ")}`);
  });
}

// Graceful shutdown — close DB pool
process.on("SIGINT", async () => {
  console.log("\n[main] Shutting down...");
  try { await pool.end(); } catch {}
  process.exit(0);
});

main();

