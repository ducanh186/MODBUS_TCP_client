/**
 * poller.js — Polls ALL Modbus devices every 1 second.
 *
 * Maintains an in-memory snapshot with per-device comm status.
 * Each register value includes: decoded value, raw uint16, and meta (reg name + scale).
 * Anti-overlap guard: skips poll tick if previous cycle is still running.
 */

const ModbusClient = require("./modbus_client");

// ---- Device definitions matching server devices_spec.py ----
const DEVICE_DEFS = {
  pms:  { unit_id: 1, type: "PMS" },
  pcs1: { unit_id: 2, type: "PCS" },
  pcs2: { unit_id: 3, type: "PCS" },
  bms1: { unit_id: 4, type: "BMS" },
  bms2: { unit_id: 5, type: "BMS" },
};

// ---- Default comm status ----
function defaultComm(msg = "not polled yet") {
  return { ok: false, last_ok_ts: null, last_error: msg };
}

// ---- Snapshot held in memory ----
let snapshot = {
  ts: null,
  devices: {
    pms:  { unit_id: 1, demand_control_power_kw: null, total_active_power_kw: null, soc_avg_pct: null, soh_avg_pct: null, capacity_total_kwh: null,
            demand_control_power_raw: null, total_active_power_raw: null, soc_avg_raw: null, soh_avg_raw: null, capacity_total_raw: null,
            demand_control_power_meta: { reg: "HR0", scale: 0.1, unit: "kW", fc: "FC03" },
            total_active_power_meta:   { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
            soc_avg_meta:              { reg: "IR1", scale: 1,   unit: "%",  fc: "FC04" },
            soh_avg_meta:              { reg: "IR2", scale: 1,   unit: "%",  fc: "FC04" },
            capacity_total_meta:       { reg: "IR3", scale: 0.1, unit: "kWh",fc: "FC04" },
            comm: defaultComm() },
    pcs1: { unit_id: 2, active_power_kw: null, active_power_raw: null,
            active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
            comm: defaultComm() },
    pcs2: { unit_id: 3, active_power_kw: null, active_power_raw: null,
            active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
            comm: defaultComm() },
    bms1: { unit_id: 4, soc_pct: null, soh_pct: null, capacity_kwh: null,
            soc_raw: null, soh_raw: null, capacity_raw: null,
            soc_meta:      { reg: "IR0", scale: 1,   unit: "%",   fc: "FC04" },
            soh_meta:      { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
            capacity_meta: { reg: "IR2", scale: 0.1, unit: "kWh", fc: "FC04" },
            comm: defaultComm() },
    bms2: { unit_id: 5, soc_pct: null, soh_pct: null, capacity_kwh: null,
            soc_raw: null, soh_raw: null, capacity_raw: null,
            soc_meta:      { reg: "IR0", scale: 1,   unit: "%",   fc: "FC04" },
            soh_meta:      { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
            capacity_meta: { reg: "IR2", scale: 0.1, unit: "kWh", fc: "FC04" },
            comm: defaultComm() },
  },
};

/** Return current snapshot. */
function getSnapshot() {
  return snapshot;
}

// ---- Per-device poll functions ----

async function pollPMS(client) {
  const uid = DEVICE_DEFS.pms.unit_id;
  try {
    const hr = await client.readHR(uid, 0, 1);   // FC03: HR0
    const ir = await client.readIR(uid, 0, 4);   // FC04: IR0..IR3
    const now = new Date().toISOString();

    snapshot.devices.pms = {
      unit_id: uid,
      // Decoded values
      demand_control_power_kw: parseFloat(ModbusClient.decodePowerKw(hr[0]).toFixed(1)),
      total_active_power_kw:   parseFloat(ModbusClient.decodePowerKw(ir[0]).toFixed(1)),
      soc_avg_pct:             ModbusClient.decodeScaled(ir[1], 1),
      soh_avg_pct:             ModbusClient.decodeScaled(ir[2], 1),
      capacity_total_kwh:      parseFloat(ModbusClient.decodeScaled(ir[3], 0.1).toFixed(1)),
      // Raw uint16 values (for debug tooltips)
      demand_control_power_raw: hr[0],
      total_active_power_raw:   ir[0],
      soc_avg_raw:              ir[1],
      soh_avg_raw:              ir[2],
      capacity_total_raw:       ir[3],
      // Meta (static, but included per snapshot for frontend simplicity)
      demand_control_power_meta: { reg: "HR0", scale: 0.1, unit: "kW", fc: "FC03" },
      total_active_power_meta:   { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
      soc_avg_meta:              { reg: "IR1", scale: 1,   unit: "%",  fc: "FC04" },
      soh_avg_meta:              { reg: "IR2", scale: 1,   unit: "%",  fc: "FC04" },
      capacity_total_meta:       { reg: "IR3", scale: 0.1, unit: "kWh",fc: "FC04" },
      comm: { ok: true, last_ok_ts: now, last_error: null },
    };
  } catch (err) {
    snapshot.devices.pms.comm.ok = false;
    snapshot.devices.pms.comm.last_error = err.message;
    console.error(`[poller] PMS poll error: ${err.message}`);
  }
}

async function pollPCS(client, name, uid) {
  try {
    const ir = await client.readIR(uid, 0, 1);   // FC04: IR0
    const now = new Date().toISOString();

    snapshot.devices[name] = {
      unit_id: uid,
      active_power_kw:  parseFloat(ModbusClient.decodePowerKw(ir[0]).toFixed(1)),
      active_power_raw: ir[0],
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
      comm: { ok: true, last_ok_ts: now, last_error: null },
    };
  } catch (err) {
    snapshot.devices[name].comm.ok = false;
    snapshot.devices[name].comm.last_error = err.message;
    console.error(`[poller] ${name.toUpperCase()} poll error: ${err.message}`);
  }
}

async function pollBMS(client, name, uid) {
  try {
    const ir = await client.readIR(uid, 0, 3);   // FC04: IR0..IR2
    const now = new Date().toISOString();

    snapshot.devices[name] = {
      unit_id: uid,
      soc_pct:      ModbusClient.decodeScaled(ir[0], 1),
      soh_pct:      ModbusClient.decodeScaled(ir[1], 1),
      capacity_kwh: parseFloat(ModbusClient.decodeScaled(ir[2], 0.1).toFixed(1)),
      soc_raw:      ir[0],
      soh_raw:      ir[1],
      capacity_raw: ir[2],
      soc_meta:      { reg: "IR0", scale: 1,   unit: "%",   fc: "FC04" },
      soh_meta:      { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
      capacity_meta: { reg: "IR2", scale: 0.1, unit: "kWh", fc: "FC04" },
      comm: { ok: true, last_ok_ts: now, last_error: null },
    };
  } catch (err) {
    snapshot.devices[name].comm.ok = false;
    snapshot.devices[name].comm.last_error = err.message;
    console.error(`[poller] ${name.toUpperCase()} poll error: ${err.message}`);
  }
}

// ---- Main poll cycle ----

async function pollAll(client) {
  await pollPMS(client);
  await pollPCS(client, "pcs1", DEVICE_DEFS.pcs1.unit_id);
  await pollPCS(client, "pcs2", DEVICE_DEFS.pcs2.unit_id);
  await pollBMS(client, "bms1", DEVICE_DEFS.bms1.unit_id);
  await pollBMS(client, "bms2", DEVICE_DEFS.bms2.unit_id);
  snapshot.ts = new Date().toISOString();
}

/**
 * Start the polling loop with overlap guard.
 * @param {ModbusClient} client - connected ModbusClient instance
 * @param {number} intervalMs   - polling interval in ms (default 1000)
 * @returns {{ stop: Function }} - call stop() to halt polling
 */
function startPolling(client, intervalMs = 1000) {
  let running = true;
  let isPolling = false; // overlap guard

  async function loop() {
    while (running) {
      if (!isPolling) {
        isPolling = true;
        try {
          await client.ensureConnection();
          await pollAll(client);
        } catch (err) {
          console.error(`[poller] Poll cycle error: ${err.message}`);
        } finally {
          isPolling = false;
        }
      } else {
        console.warn("[poller] Skipping tick — previous poll still running");
      }
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }

  loop(); // fire-and-forget (async)

  return {
    stop() {
      running = false;
    },
  };
}

module.exports = { getSnapshot, startPolling, DEVICE_DEFS };
