-- 001_create_telemetry.sql
-- Purpose: minimal time-series table + indexes for telemetry storage

-- 0) track migrations (super minimal)
CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 1) main table
CREATE TABLE IF NOT EXISTS telemetry (
  id        BIGSERIAL PRIMARY KEY,
  ts        TIMESTAMPTZ NOT NULL,
  device_id TEXT NOT NULL,
  metric    TEXT NOT NULL,
  value     DOUBLE PRECISION NOT NULL,
  unit      TEXT NULL,
  comm_ok   BOOLEAN NOT NULL DEFAULT TRUE,
  raw       JSONB NULL
);

-- 2) indexes for timeline queries
CREATE INDEX IF NOT EXISTS idx_telemetry_dev_metric_ts
ON telemetry (device_id, metric, ts DESC);

CREATE INDEX IF NOT EXISTS idx_telemetry_ts
ON telemetry (ts DESC);

-- 3) mark migration applied
INSERT INTO schema_migrations (version)
VALUES ('001_create_telemetry')
ON CONFLICT DO NOTHING;
