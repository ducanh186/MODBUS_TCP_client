const http = require("http");
const ModbusClient = require("./modbus_client");

const RTU_BRIDGE_URL = process.env.RTU_BRIDGE_URL || "http://localhost:8081/api/multimeter";

// ---- Device port map (matches plant.yaml) ----
const DEVICE_PORTS = {
  pms:  { port: 15020, unit_id: 1, type: "PMS" },
  pcs1: { port: 15021, unit_id: 1, type: "PCS" },
  pcs2: { port: 15022, unit_id: 1, type: "PCS" },
  bms1: { port: 15024, unit_id: 1, type: "BMS" },
  bms2: { port: 15025, unit_id: 1, type: "BMS" },
};

// Export for use in server.js
const DEVICE_DEFS = {
  pms: { unit_id: 1, type: "PMS" },
};

function defaultComm(msg = "not polled yet") {
  return { ok: false, last_ok_ts: null, last_error: msg };
}

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
    pcs1: {
      unit_id: 1, port: 15021,
      active_power_kw: null, active_power_raw: null,
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
      comm: defaultComm(),
    },
    pcs2: {
      unit_id: 1, port: 15022,
      active_power_kw: null, active_power_raw: null,
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04" },
      comm: defaultComm(),
    },
    bms1: {
      unit_id: 1, port: 15024,
      soc_pct: null, soh_pct: null, capacity_kwh: null,
      soc_raw: null, soh_raw: null, capacity_raw: null,
      soc_meta:      { reg: "IR0", scale: 1,   unit: "%",   fc: "FC04" },
      soh_meta:      { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
      capacity_meta: { reg: "IR2", scale: 0.1, unit: "kWh", fc: "FC04" },
      comm: defaultComm(),
    },
    bms2: {
      unit_id: 1, port: 15025,
      soc_pct: null, soh_pct: null, capacity_kwh: null,
      soc_raw: null, soh_raw: null, capacity_raw: null,
      soc_meta:      { reg: "IR0", scale: 1,   unit: "%",   fc: "FC04" },
      soh_meta:      { reg: "IR1", scale: 1,   unit: "%",   fc: "FC04" },
      capacity_meta: { reg: "IR2", scale: 0.1, unit: "kWh", fc: "FC04" },
      comm: defaultComm(),
    },
    multimeter: {
      unit_id: 10,
      active_power_kw: null,
      active_power_raw: null,
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04 (RTU)" },
      comm: defaultComm("bridge not polled yet"),
    },
  },
};

/** Return current snapshot (deep copy to avoid mutation during async DB insert). */
function getSnapshot() {
  return JSON.parse(JSON.stringify(snapshot));
}

// ---- Poll helpers ----

async function pollPMS(client) {
  const uid = DEVICE_PORTS.pms.unit_id;
  try {
    await client.ensureConnection();
    const hr = await client.readHR(uid, 0, 1);
    const ir = await client.readIR(uid, 0, 4);
    const now = new Date().toISOString();

    snapshot.devices.pms = {
      unit_id: uid,
      demand_control_power_kw: parseFloat(ModbusClient.decodePowerKw(hr[0]).toFixed(1)),
      total_active_power_kw:   parseFloat(ModbusClient.decodePowerKw(ir[0]).toFixed(1)),
      soc_avg_pct:             ModbusClient.decodeScaled(ir[1], 1),
      soh_avg_pct:             ModbusClient.decodeScaled(ir[2], 1),
      capacity_total_kwh:      parseFloat(ModbusClient.decodeScaled(ir[3], 0.1).toFixed(1)),
      demand_control_power_raw: hr[0],
      total_active_power_raw:   ir[0],
      soc_avg_raw:              ir[1],
      soh_avg_raw:              ir[2],
      capacity_total_raw:       ir[3],
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

async function pollPCS(key, client) {
  const uid = DEVICE_PORTS[key].unit_id;
  const prev = snapshot.devices[key];
  try {
    await client.ensureConnection();
    const ir = await client.readIR(uid, 0, 1);
    const now = new Date().toISOString();
    snapshot.devices[key] = {
      ...prev,
      active_power_kw: parseFloat(ModbusClient.decodePowerKw(ir[0]).toFixed(1)),
      active_power_raw: ir[0],
      comm: { ok: true, last_ok_ts: now, last_error: null },
    };
  } catch (err) {
    snapshot.devices[key] = { ...prev, comm: { ok: false, last_ok_ts: (prev.comm || {}).last_ok_ts || null, last_error: err.message } };
    console.error(`[poller] ${key.toUpperCase()} poll error: ${err.message}`);
  }
}

async function pollBMS(key, client) {
  const uid = DEVICE_PORTS[key].unit_id;
  const prev = snapshot.devices[key];
  try {
    await client.ensureConnection();
    const ir = await client.readIR(uid, 0, 3);
    const now = new Date().toISOString();
    snapshot.devices[key] = {
      ...prev,
      soc_pct:      ModbusClient.decodeScaled(ir[0], 1),
      soh_pct:      ModbusClient.decodeScaled(ir[1], 1),
      capacity_kwh: parseFloat(ModbusClient.decodeScaled(ir[2], 0.1).toFixed(1)),
      soc_raw: ir[0], soh_raw: ir[1], capacity_raw: ir[2],
      comm: { ok: true, last_ok_ts: now, last_error: null },
    };
  } catch (err) {
    snapshot.devices[key] = { ...prev, comm: { ok: false, last_ok_ts: (prev.comm || {}).last_ok_ts || null, last_error: err.message } };
    console.error(`[poller] ${key.toUpperCase()} poll error: ${err.message}`);
  }
}

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = "";
      res.on("data", (chunk) => { body += chunk; });
      res.on("end", () => {
        try { resolve(JSON.parse(body)); }
        catch (e) { reject(e); }
      });
    }).on("error", reject);
  });
}

async function pollMultimeter() {
  try {
    const data = await fetchJSON(RTU_BRIDGE_URL);
    snapshot.devices.multimeter = {
      unit_id: 10,
      active_power_kw: data.active_power_kw,
      active_power_raw: data.raw,
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04 (RTU)" },
      comm: data.comm || { ok: false, last_ok_ts: null, last_error: "no data from bridge" },
    };
  } catch (err) {
    const prev = snapshot.devices.multimeter || {};
    snapshot.devices.multimeter = {
      ...prev,
      comm: {
        ok: false,
        last_ok_ts: (prev.comm || {}).last_ok_ts || null,
        last_error: `Bridge unreachable: ${err.message}`,
      },
    };
  }
}

// clients passed in from server.js: { pms, pcs1, pcs2, bms1, bms2 }
async function pollAll(clients) {
  await Promise.allSettled([
    pollPMS(clients.pms),
    pollPCS("pcs1", clients.pcs1),
    pollPCS("pcs2", clients.pcs2),
    pollBMS("bms1", clients.bms1),
    pollBMS("bms2", clients.bms2),
    pollMultimeter(),
  ]);
  snapshot.ts = new Date().toISOString();
}

function startPolling(clients, intervalMs = 1000) {
  let running = true;
  let isPolling = false;

  async function loop() {
    while (running) {
      if (!isPolling) {
        isPolling = true;
        try {
          await pollAll(clients);
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

  loop();

  return { stop() { running = false; } };
}

module.exports = { getSnapshot, startPolling, DEVICE_DEFS, DEVICE_PORTS };
