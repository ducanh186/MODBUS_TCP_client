const http = require("http");
const ModbusClient = require("./modbus_client");

const RTU_BRIDGE_URL = process.env.RTU_BRIDGE_URL || "http://localhost:8081/api/multimeter";

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
    multimeter: {
      unit_id: 10,
      active_power_kw: null,
      active_power_raw: null,
      active_power_meta: { reg: "IR0", scale: 0.1, unit: "kW", fc: "FC04 (RTU)" },
      comm: defaultComm("bridge not polled yet"),
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

async function pollAll(client) {
  await pollPMS(client);
  await pollMultimeter();
  snapshot.ts = new Date().toISOString();
}

function startPolling(client, intervalMs = 1000) {
  let running = true;
  let isPolling = false;

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
