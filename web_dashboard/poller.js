/**
 * poller.js — Polls PMS device only (external-client rule: PMS + Multimeter).
 *
 * Architecture: multi-port. PCS/BMS are on separate ports and must NOT be
 * accessed directly from the external dashboard.
 *
 * Maintains an in-memory snapshot with per-device comm status.
 * Anti-overlap guard: skips poll tick if previous cycle is still running.
 */

const ModbusClient = require("./modbus_client");

// ---- Device definitions (external client: PMS only) ----
const DEVICE_DEFS = {
  pms: { unit_id: 1, type: "PMS" },
  // PCS1/PCS2/BMS1/BMS2 are on separate TCP ports — NOT accessible from
  // external dashboard per system design (internal comms only).
};

// ---- Default comm status ----
function defaultComm(msg = "not polled yet") {
  return { ok: false, last_ok_ts: null, last_error: msg };
}

// ---- Snapshot held in memory ----
let snapshot = {
  ts: null,
  devices: {
    pms: {
      unit_id: 1,
      demand_control_power_kw: null, total_active_power_kw: null,
      soc_avg_pct: null, soh_avg_pct: null, capacity_total_kwh: null,
      demand_control_power_raw: null, total_active_power_raw: null,
      soc_avg_raw: null, soh_avg_raw: null, capacity_total_raw: null,
      demand_control_power_meta: { reg: "HR0", scale: 0.1, unit: "kW",  fc: "FC03" },
      total_active_power_meta:   { reg: "IR0", scale: 0.1, unit: "kW",  fc: "FC04" },
      soc_avg_meta:              { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
      soh_avg_meta:              { reg: "IR2", scale: 1,   unit: "%",   fc: "FC04" },
      capacity_total_meta:       { reg: "IR3", scale: 0.1, unit: "kWh", fc: "FC04" },
      comm: defaultComm(),
    },
  },
};

/** Return current snapshot. */
function getSnapshot() {
  return snapshot;
}

// ---- Poll function: PMS only ----

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

// ---- Main poll cycle (PMS only) ----

async function pollAll(client) {
  await pollPMS(client);
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
// NOTE: PCS/BMS are on separate ports (15021-15025) and must never be polled
// from the external dashboard. They are accessible only via internal controllers.
