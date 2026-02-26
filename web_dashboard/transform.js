/**
 * transform.js — Convert poller snapshot → telemetry DB rows.
 *
 * Only persists PMS + multimeter (Phase 2 scope).
 * Renames snapshot keys to match frozen DB convention.
 */

// Snapshot key → [db_metric, unit]
const PMS_METRIC_MAP = {
  demand_control_power_kw: ["demand_control_power_kw", "kW"],
  total_active_power_kw:   ["active_power_kw",         "kW"],
  soc_avg_pct:             ["soc_pct",                 "%"],
  soh_avg_pct:             ["soh_pct",                 "%"],
  capacity_total_kwh:      ["capacity_kwh",            "kWh"],
};

/**
 * Build telemetry rows from a poller snapshot.
 *
 * @param {object} snapshot  - From getSnapshot() (deep copy)
 * @param {Date}   ts        - Collector tick timestamp
 * @returns {Array<{ts, device_id, metric, value, unit, comm_ok, raw}>}
 */
function buildTelemetryRows(snapshot, ts) {
  const rows = [];
  if (!snapshot || !snapshot.devices) return rows;

  // ---- PMS ----
  const pms = snapshot.devices.pms;
  if (pms) {
    const commOk = pms.comm ? pms.comm.ok : true;

    if (!commOk) {
      // PMS comm failed — store one error marker row
      rows.push({
        ts,
        device_id: "pms",
        metric: "comm_error",
        value: 0,
        unit: null,
        comm_ok: false,
        raw: { error: pms.comm ? pms.comm.last_error : "unknown" },
      });
    } else {
      for (const [snapKey, [dbMetric, unit]] of Object.entries(PMS_METRIC_MAP)) {
        const v = pms[snapKey];
        if (typeof v === "number" && Number.isFinite(v)) {
          rows.push({
            ts,
            device_id: "pms",
            metric: dbMetric,
            value: v,
            unit,
            comm_ok: true,
            raw: null,
          });
        }
      }
    }
  }

  // ---- Multimeter ----
  const mm = snapshot.devices.multimeter;
  if (mm) {
    const commOk = mm.comm ? mm.comm.ok : true;
    const val = mm.active_power_kw;

    if (typeof val === "number" && Number.isFinite(val) && commOk) {
      rows.push({
        ts,
        device_id: "multimeter",
        metric: "active_power_kw",
        value: val,
        unit: "kW",
        comm_ok: true,
        raw: null,
      });
    } else {
      rows.push({
        ts,
        device_id: "multimeter",
        metric: "comm_error",
        value: 0,
        unit: null,
        comm_ok: false,
        raw: { error: mm.comm ? mm.comm.last_error : "no_data" },
      });
    }
  }

  return rows;
}

module.exports = { buildTelemetryRows };
