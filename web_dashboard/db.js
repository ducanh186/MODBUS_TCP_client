/**
 * db.js — PostgreSQL connection pool + batch insert for telemetry rows.
 *
 * Expects DATABASE_URL env var (set in .env or system environment).
 * Uses 'pg' module with a small pool (max 5 connections).
 */

const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 30_000,
});

/**
 * Batch insert telemetry rows.
 *
 * @param {Array<{ts: Date, device_id: string, metric: string, value: number,
 *                unit: string|null, comm_ok: boolean, raw: object|null}>} rows
 */
async function insertTelemetryBatch(rows) {
  if (!rows.length) return;

  // Build parameterised query: $1..$7 per row
  const values = [];
  const placeholders = [];
  let idx = 1;

  for (const r of rows) {
    placeholders.push(
      `($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`
    );
    values.push(
      r.ts,
      r.device_id,
      r.metric,
      r.value,
      r.unit ?? null,
      r.comm_ok ?? true,
      r.raw ? JSON.stringify(r.raw) : null
    );
  }

  const sql = `
    INSERT INTO telemetry (ts, device_id, metric, value, unit, comm_ok, raw)
    VALUES ${placeholders.join(", ")}
  `;

  await pool.query(sql, values);
}

module.exports = { pool, insertTelemetryBatch };
