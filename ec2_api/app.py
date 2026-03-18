"""
EC2 API Server — Bài tập 4
Flask API for device telemetry ingest (→ S3 pipeline) and command management.

Endpoints:
  POST /auth/token             — authenticate, return JWT (60s TTL)
  POST /api/device             — receive device JSON, save to S3
  GET  /api/device             — read device_data from RDS
  POST /api/commands           — frontend creates charge/discharge command
  GET  /api/commands           — local polls queued commands
  PATCH /api/commands/<id>     — local updates command lifecycle status
  POST /api/schedules          — create charge/discharge schedule
  GET  /api/schedules          — list schedules (optional ?status=)
  DELETE /api/schedules/<id>   — soft-delete (cancel) a schedule
  GET  /api/schedules/active   — active + future schedules
  GET  /                       — serve frontend
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional

import boto3
import jwt
import pg8000
from flask import Flask, jsonify, request, send_from_directory

# ---------------------------------------------------------------------------
# Logging — set up before anything so startup errors are visible
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | API | %(levelname)s | %(message)s",
)
_startup_log = logging.getLogger("ec2_api.startup")

# ---------------------------------------------------------------------------
# Non-secret config from environment (safe to have defaults / be in .env)
# ---------------------------------------------------------------------------
AWS_REGION  = os.environ.get("AWS_REGION", "ap-northeast-1")
S3_BUCKET   = os.environ.get("S3_BUCKET", "")
DB_HOST     = os.environ.get("DB_HOST", "")
DB_PORT     = int(os.environ.get("DB_PORT", "5432"))
DB_NAME     = os.environ.get("DB_NAME", "postgres")
DB_USER     = os.environ.get("DB_USER", "postgres")
JWT_TTL_SEC = int(os.environ.get("JWT_TTL_SEC", "60"))

# SSM prefix — override for non-prod environments
_SSM_PREFIX = os.environ.get("SSM_PREFIX", "/bess/app")


# ---------------------------------------------------------------------------
# Secret loader — env var first (local dev), SSM second (EC2 prod)
# ---------------------------------------------------------------------------
def _load_secret(env_var: str, ssm_path: str) -> str:
    """Load a secret value with this priority:

    1. Environment variable ``env_var``
       → set this in your local ``.env`` (never commit that file).
    2. AWS SSM Parameter Store at ``ssm_path`` (SecureString, decrypted).
       → used automatically on EC2 via instance IAM role.

    Exits the process with a clear message if neither source has the value.
    This is intentional: running with an empty secret is worse than not running.
    """
    value = os.environ.get(env_var, "").strip()
    if value:
        _startup_log.info("Secret '%s' loaded from environment variable.", env_var)
        return value

    _startup_log.info("'%s' not in env — fetching from SSM: %s", env_var, ssm_path)
    try:
        ssm = boto3.client("ssm", region_name=AWS_REGION)
        resp = ssm.get_parameter(Name=ssm_path, WithDecryption=True)
        _startup_log.info("Secret '%s' loaded from SSM OK.", ssm_path)
        return resp["Parameter"]["Value"]
    except Exception as exc:
        _startup_log.critical(
            "FATAL: Cannot load secret '%s'.\n"
            "  SSM path : %s\n"
            "  Error    : %s: %s\n"
            "  Fix      : set env var %s, or create SSM SecureString at %s "
            "and ensure the EC2 instance role has ssm:GetParameter permission.",
            env_var, ssm_path, type(exc).__name__, exc, env_var, ssm_path,
        )
        sys.exit(1)


# Load secrets at startup — process exits immediately if unavailable.
# No secret ever has a hardcoded fallback in source code.
SERVER_SECRET = _load_secret("SERVER_SECRET", f"{_SSM_PREFIX}/server_secret")
DB_PASS       = _load_secret("DB_PASS",       f"{_SSM_PREFIX}/db_pass")

# Validate non-secret required values
for _var, _val in [("S3_BUCKET", S3_BUCKET), ("DB_HOST", DB_HOST)]:
    if not _val:
        _startup_log.critical(
            "FATAL: Required env var %s is not set. Add it to your .env or EC2 environment.", _var
        )
        sys.exit(1)

# ---------------------------------------------------------------------------
# App init
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder="static", static_url_path="")
s3 = boto3.client("s3", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_db():
    """Return a pg8000 connection (SSL required for RDS)."""
    return pg8000.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASS,
        ssl_context=True,
    )


# ---------------------------------------------------------------------------
# Command contract helpers
# ---------------------------------------------------------------------------
COMMAND_STATUS_ORDER = (
    "accepted",
    "queued",
    "executing",
    "executed",
    "failed",
    "blocked",
    "timeout",
)

COMMAND_STATUS_ALIASES = {
    "pending": "queued",
    "expired": "timeout",
    "canceled": "timeout",
    "cancelled": "timeout",
}

TERMINAL_COMMAND_STATUSES = {"executed", "failed", "blocked", "timeout"}

ALLOWED_COMMAND_TRANSITIONS = {
    "accepted": {"queued", "executing", "executed", "failed", "blocked", "timeout"},
    "queued": {"executing", "executed", "failed", "blocked", "timeout"},
    "executing": {"executed", "failed", "blocked", "timeout"},
    "executed": set(),
    "failed": set(),
    "blocked": set(),
    "timeout": set(),
}

DEFAULT_DEVICE_ID = "bess-01"
_command_schema_ready = False


def _normalize_command_status(raw_status: Optional[str]) -> str:
    s = (raw_status or "").strip().lower()
    if not s:
        return ""
    return COMMAND_STATUS_ALIASES.get(s, s)


def _is_valid_transition(current_status: str, new_status: str) -> bool:
    if current_status == new_status:
        return True
    return new_status in ALLOWED_COMMAND_TRANSITIONS.get(current_status, set())


def _generate_command_id() -> str:
    now = datetime.now(timezone.utc)
    return f"cmd-{now:%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"


def _ensure_command_contract_schema(conn) -> None:
    """Best-effort schema migration so API/client can share one command vocabulary."""
    global _command_schema_ready
    if _command_schema_ready:
        return

    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT data_type, udt_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'commands' AND column_name = 'status'"
        )
        status_col = cur.fetchone()
        if not status_col:
            raise RuntimeError("commands.status column not found")

        # If status is enum, convert to TEXT so lifecycle values can evolve safely.
        if status_col[0] == "USER-DEFINED":
            cur.execute("ALTER TABLE commands ALTER COLUMN status TYPE TEXT USING status::text")

        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'commands' AND column_name = 'command_id'"
        )
        if not cur.fetchone():
            cur.execute("ALTER TABLE commands ADD COLUMN command_id TEXT")

        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'commands' AND column_name = 'device_id'"
        )
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE commands "
                "ADD COLUMN device_id TEXT NOT NULL DEFAULT %s",
                (DEFAULT_DEVICE_ID,),
            )

        cur.execute(
            "UPDATE commands "
            "SET command_id = CONCAT('cmd-', to_char(COALESCE(created_at, now()), 'YYYYMMDD-HH24MISS'), '-', LPAD(id::text, 6, '0')) "
            "WHERE command_id IS NULL OR command_id = ''"
        )
        cur.execute(
            "UPDATE commands SET device_id = %s "
            "WHERE device_id IS NULL OR device_id = ''",
            (DEFAULT_DEVICE_ID,),
        )

        cur.execute("ALTER TABLE commands ALTER COLUMN command_id SET NOT NULL")
        cur.execute("ALTER TABLE commands ALTER COLUMN device_id SET DEFAULT %s", (DEFAULT_DEVICE_ID,))
        cur.execute("ALTER TABLE commands ALTER COLUMN device_id SET NOT NULL")
        cur.execute("ALTER TABLE commands ALTER COLUMN status SET DEFAULT 'queued'")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_commands_command_id ON commands(command_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_commands_device_status_created ON commands(device_id, status, created_at DESC)")

        # Normalize legacy statuses to the unified lifecycle vocabulary.
        cur.execute(
            "UPDATE commands SET status = 'queued' "
            "WHERE status::text = 'pending'"
        )
        cur.execute(
            "UPDATE commands SET status = 'timeout', executed_at = COALESCE(executed_at, now()) "
            "WHERE status::text IN ('expired', 'canceled', 'cancelled')"
        )

        conn.commit()
        _command_schema_ready = True
    finally:
        cur.close()


def _to_command_dict(row) -> dict:
    return {
        "id": row[0],
        "command_id": row[1],
        "device_id": row[2],
        "command": row[3],
        "power_kw": row[4],
        "status": row[5],
        "created_at": row[6].isoformat() if row[6] else None,
        "executed_at": row[7].isoformat() if row[7] else None,
        "expires_at": row[8].isoformat() if row[8] else None,
    }


def _parse_expire_minutes(raw_value, default_value: int) -> int:
    try:
        minutes = int(raw_value)
    except (TypeError, ValueError):
        minutes = default_value
    return max(1, min(minutes, 24 * 60))


def _queue_command(conn, *, command: str, power_kw: float, created_by: str,
                   device_id: str, expire_minutes: int) -> dict:
    cur = conn.cursor()
    try:
        # "Latest wins": if a new command arrives while one is still queued/executing,
        # mark older active commands as timeout.
        cur.execute(
            "UPDATE commands SET status = 'timeout', executed_at = COALESCE(executed_at, now()) "
            "WHERE device_id = %s AND status IN ('queued', 'executing') AND expires_at > now()",
            (device_id,),
        )

        command_id = _generate_command_id()
        cur.execute(
            "INSERT INTO commands (command_id, device_id, command, power_kw, status, created_by, expires_at) "
            "VALUES (%s, %s, %s, %s, 'queued', %s, now() + make_interval(mins => %s)) "
            "RETURNING id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at",
            (command_id, device_id, command, power_kw, created_by, expire_minutes),
        )
        row = cur.fetchone()
        conn.commit()
        return _to_command_dict(row)
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------
def create_token(client_key: str) -> str:
    """Create JWT signed with HMAC(SERVER_SECRET + client_key)."""
    signing_key = SERVER_SECRET + ":" + client_key
    payload = {
        "sub": client_key,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(seconds=JWT_TTL_SEC),
    }
    return jwt.encode(payload, signing_key, algorithm="HS256")


def decode_token(token: str, client_key: str) -> dict:
    """Decode and verify JWT."""
    signing_key = SERVER_SECRET + ":" + client_key
    return jwt.decode(token, signing_key, algorithms=["HS256"])


def require_auth(f):
    """Decorator — require valid JWT in Authorization header.
    Expects: Authorization: Bearer <token>
             X-Client-Key: <client_key>
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        client_key = request.headers.get("X-Client-Key", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Missing Bearer token"}), 401
        token = auth_header.split(" ", 1)[1]
        if not client_key:
            return jsonify({"error": "Missing X-Client-Key header"}), 401
        try:
            claims = decode_token(token, client_key)
            request.jwt_claims = claims
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except jwt.InvalidTokenError as e:
            return jsonify({"error": f"Invalid token: {e}"}), 401
        return f(*args, **kwargs)
    return wrapper


# ---------------------------------------------------------------------------
# Routes: Auth
# ---------------------------------------------------------------------------
@app.route("/auth/token", methods=["POST"])
def auth_token():
    """Authenticate with secret_key + client_key → JWT."""
    data = request.get_json(force=True)
    secret_key = data.get("secret_key", "")
    client_key = data.get("client_key", "")

    if secret_key != SERVER_SECRET:
        return jsonify({"error": "Invalid secret_key"}), 401
    if not client_key:
        return jsonify({"error": "client_key required"}), 400

    token = create_token(client_key)
    return jsonify({
        "token": token,
        "expires_in": JWT_TTL_SEC,
        "client_key": client_key,
    })


# ---------------------------------------------------------------------------
# Routes: Device data
# ---------------------------------------------------------------------------
@app.route("/api/device", methods=["POST"])
@require_auth
def post_device_data():
    """Receive device JSON telemetry → save to S3 raw/ prefix + direct RDS write.
    Direct RDS write ensures snapshot is fresh immediately.
    S3 event → SQS → Lambda → RDS pipeline is kept as backup/archive.
    """
    data = request.get_json(force=True)

    # Validate minimal fields
    if "device_id" not in data:
        return jsonify({"error": "device_id required"}), 400

    device_id = data["device_id"]
    ts = data.get("timestamp", datetime.now(timezone.utc).isoformat())

    # S3 key: raw/<device_id>/<ts>_<uuid>.json
    uid = uuid.uuid4().hex[:8]
    ts_safe = ts.replace(":", "-").replace("+", "_")
    s3_key = f"raw/{device_id}/{ts_safe}_{uid}.json"

    # Add metadata
    data["_s3_key"] = s3_key
    data["_received_at"] = datetime.now(timezone.utc).isoformat()

    # Upload to S3 (archive + Lambda pipeline backup)
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json",
        )
    except Exception as exc:
        logging.warning("S3 upload failed (non-fatal): %s", exc)

    # Direct RDS write — ensures snapshot freshness without Lambda delay
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO device_data (device_id, ts, payload, s3_key) "
            "VALUES (%s, %s::timestamptz, %s, %s)",
            (device_id, ts, json.dumps(data), s3_key),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logging.warning("Direct RDS write failed (non-fatal): %s", exc)

    return jsonify({"status": "saved", "s3_key": s3_key}), 201


@app.route("/api/device", methods=["GET"])
@require_auth
def get_device_data():
    """Read latest device data from RDS."""
    device_id = request.args.get("device_id")
    limit = min(int(request.args.get("limit", "50")), 200)

    conn = get_db()
    cur = conn.cursor()
    try:
        if device_id:
            cur.execute(
                "SELECT id, device_id, ts, payload, s3_key, created_at "
                "FROM device_data WHERE device_id = %s "
                "ORDER BY ts DESC LIMIT %s",
                (device_id, limit),
            )
        else:
            cur.execute(
                "SELECT id, device_id, ts, payload, s3_key, created_at "
                "FROM device_data ORDER BY ts DESC LIMIT %s",
                (limit,),
            )
        rows = cur.fetchall()
        result = []
        for r in rows:
            payload = r[3] if isinstance(r[3], dict) else json.loads(r[3])
            result.append({
                "id": r[0], "device_id": r[1],
                "ts": r[2].isoformat() if r[2] else None,
                "payload": payload,
                "s3_key": r[4],
                "created_at": r[5].isoformat() if r[5] else None,
            })
        return jsonify(result)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Routes: Commands (charge / discharge / standby)
# ---------------------------------------------------------------------------
@app.route("/api/commands", methods=["POST"])
@require_auth
def post_command():
    """Frontend creates a command. Response status=accepted, persisted status=queued."""
    data = request.get_json(force=True)
    command = data.get("command", "").lower()
    try:
        power_kw = float(data.get("power_kw") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "power_kw must be a number"}), 400
    device_id = (data.get("device_id") or DEFAULT_DEVICE_ID).strip()
    expire_minutes = _parse_expire_minutes(data.get("expire_minutes", 5), 5)

    if command not in ("charge", "discharge", "standby"):
        return jsonify({"error": "command must be charge|discharge|standby"}), 400
    if not device_id:
        return jsonify({"error": "device_id required"}), 400
    if command == "standby":
        power_kw = 0.0
    else:
        power_kw = abs(power_kw)

    conn = get_db()
    try:
        _ensure_command_contract_schema(conn)
        queued = _queue_command(
            conn,
            command=command,
            power_kw=power_kw,
            created_by=request.jwt_claims.get("sub", "frontend"),
            device_id=device_id,
            expire_minutes=expire_minutes,
        )
        return jsonify({
            "status": "accepted",
            "command_id": queued["command_id"],
            "device_id": queued["device_id"],
            "command": queued["command"],
            "power_kw": queued["power_kw"],
            "queued_status": queued["status"],
            "id": queued["id"],
            "created_at": queued["created_at"],
            "expires_at": queued["expires_at"],
        }), 201
    finally:
        conn.close()


@app.route("/api/commands", methods=["GET"])
@require_auth
def get_commands():
    """Local polls queued/executing commands. Auto-timeout old active commands."""
    status_filter = _normalize_command_status(request.args.get("status", "queued"))
    if not status_filter:
        status_filter = "queued"

    if status_filter not in ("queued", "executing", "all"):
        return jsonify({"error": "status must be queued|executing|all"}), 400

    device_id = (request.args.get("device_id") or DEFAULT_DEVICE_ID).strip()
    if not device_id:
        return jsonify({"error": "device_id required"}), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        _ensure_command_contract_schema(conn)

        # Timeout stale active commands first.
        cur.execute(
            "UPDATE commands SET status = 'timeout', executed_at = COALESCE(executed_at, now()) "
            "WHERE device_id = %s AND status IN ('accepted', 'queued', 'executing') AND expires_at < now()",
            (device_id,),
        )
        conn.commit()

        if status_filter == "all":
            cur.execute(
                "SELECT id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at "
                "FROM commands WHERE device_id = %s AND status IN ('queued', 'executing') "
                "ORDER BY created_at ASC",
                (device_id,),
            )
        else:
            cur.execute(
                "SELECT id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at "
                "FROM commands WHERE device_id = %s AND status = %s "
                "ORDER BY created_at ASC",
                (device_id, status_filter),
            )
        rows = cur.fetchall()
        result = [_to_command_dict(r) for r in rows]
        return jsonify(result)
    finally:
        cur.close()
        conn.close()


@app.route("/api/commands/<int:cmd_id>", methods=["PATCH"])
@require_auth
def patch_command(cmd_id):
    """Local updates command status (queued -> executing -> terminal)."""
    global _last_command

    data = request.get_json(force=True)
    new_status = _normalize_command_status(data.get("status", ""))
    ack_command_id = (data.get("command_id") or "").strip()
    ack_device_id = (data.get("device_id") or "").strip()

    if new_status not in COMMAND_STATUS_ORDER:
        return jsonify({"error": "status must be accepted|queued|executing|executed|failed|blocked|timeout"}), 400

    conn = get_db()
    cur = conn.cursor()
    try:
        _ensure_command_contract_schema(conn)
        cur.execute(
            "SELECT id, command_id, device_id, status, command, power_kw "
            "FROM commands WHERE id = %s",
            (cmd_id,),
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Command not found"}), 404

        current_db_status = row[3]
        current_status = _normalize_command_status(row[3])
        if ack_command_id and ack_command_id != row[1]:
            return jsonify({"error": "command_id does not match this id"}), 409
        if ack_device_id and ack_device_id != row[2]:
            return jsonify({"error": "device_id does not match this command"}), 409
        if not _is_valid_transition(current_status, new_status):
            return jsonify({
                "error": f"invalid transition {current_status}->{new_status}",
                "current_status": current_status,
            }), 409

        if new_status in TERMINAL_COMMAND_STATUSES:
            cur.execute(
                "UPDATE commands SET status = %s, executed_at = now() "
                "WHERE id = %s AND status = %s "
                "RETURNING id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at",
                (new_status, cmd_id, current_db_status),
            )
        else:
            cur.execute(
                "UPDATE commands SET status = %s "
                "WHERE id = %s AND status = %s "
                "RETURNING id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at",
                (new_status, cmd_id, current_db_status),
            )
        updated = cur.fetchone()
        if not updated:
            return jsonify({"error": "Command status changed concurrently, please retry"}), 409
        payload = _to_command_dict(updated)

        cmd = payload["command"]
        mag = payload["power_kw"] or 0
        if cmd == "charge":
            signed_kw = -abs(mag)
        elif cmd == "discharge":
            signed_kw = abs(mag)
        else:
            signed_kw = 0.0

        _last_command = {
            "command_id": payload["command_id"],
            "device_id": payload["device_id"],
            "kw": signed_kw,
            "raw": _encode_power_kw(signed_kw),
            "status": payload["status"],
            "ts": datetime.now(timezone.utc).isoformat(),
            "error": None,
        }

        conn.commit()
        return jsonify({
            "ok": True,
            "id": payload["id"],
            "command_id": payload["command_id"],
            "device_id": payload["device_id"],
            "previous_status": current_status,
            "status": payload["status"],
        })
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Routes: Command history (for frontend dashboard)
# ---------------------------------------------------------------------------
@app.route("/api/commands/history", methods=["GET"])
@require_auth
def get_command_history():
    """Get recent command history for frontend display."""
    limit = min(int(request.args.get("limit", "20")), 100)
    device_id = (request.args.get("device_id") or "").strip()

    conn = get_db()
    cur = conn.cursor()
    try:
        _ensure_command_contract_schema(conn)
        if device_id:
            cur.execute(
                "SELECT id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at "
                "FROM commands WHERE device_id = %s ORDER BY created_at DESC LIMIT %s",
                (device_id, limit),
            )
        else:
            cur.execute(
                "SELECT id, command_id, device_id, command, power_kw, status, created_at, executed_at, expires_at "
                "FROM commands ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
        rows = cur.fetchall()
        result = [_to_command_dict(r) for r in rows]
        return jsonify(result)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Routes: Old UI compatibility — /api/snapshot (no auth, matches Node.js format)
# ---------------------------------------------------------------------------
# Server-side last_command state (survives page refresh, same as old Node server)
_last_command = {
    "command_id": None,
    "device_id": None,
    "kw": None,
    "raw": None,
    "status": None,
    "ts": None,
    "error": None,
}

# In-memory multimeter toggle (default ON, resets on restart)
_multimeter_enabled = True

# File-backed toggle for cross-worker consistency (gunicorn runs 2+ workers)
_MM_FLAG = "/tmp/bess_multimeter_enabled"


def _mm_is_enabled() -> bool:
    try:
        return open(_MM_FLAG).read().strip() == "1"
    except FileNotFoundError:
        return True  # default ON


def _mm_set_enabled(val: bool):
    global _multimeter_enabled
    _multimeter_enabled = val
    with open(_MM_FLAG, "w") as f:
        f.write("1" if val else "0")

# Power encoding helpers (int16 × 0.1 scale, same as ModbusClient)
POWER_SCALE = 0.1
POWER_MIN_KW = -3276.8
POWER_MAX_KW = 3276.7


def _encode_power_kw(kw):
    """kW → uint16 raw (two's complement for negatives)."""
    raw = int(round(kw / POWER_SCALE))
    if raw < 0:
        raw = raw + 0x10000
    return raw & 0xFFFF


def _s16(raw):
    """uint16 → signed int16."""
    return raw - 0x10000 if raw >= 0x8000 else raw


def _build_comm(ts_iso):
    """Build comm status dict from a timestamp ISO string."""
    if not ts_iso:
        return {"ok": False, "last_ok_ts": None, "last_error": "no data"}
    return {"ok": True, "last_ok_ts": ts_iso, "last_error": None}


def _build_snapshot_from_db():
    """Read latest device_data rows from RDS and transform to old snapshot format."""
    conn = get_db()
    cur = conn.cursor()
    try:
        # Get latest row per device_id
        cur.execute(
            "SELECT DISTINCT ON (device_id) device_id, ts, payload, created_at "
            "FROM device_data ORDER BY device_id, ts DESC"
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    # Parse rows into dict keyed by device_id
    latest = {}
    for r in rows:
        device_id = r[0]
        dt = r[1]
        if dt is None:
            ts = None
        else:
            # Ensure timezone-aware so browser parses as UTC (not local time)
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                ts = dt.isoformat()  # already has +00:00
            else:
                ts = dt.isoformat() + "+00:00"  # force UTC label
        payload = r[2] if isinstance(r[2], dict) else json.loads(r[2])
        latest[device_id] = {"ts": ts, "payload": payload}

    now = datetime.now(timezone.utc).isoformat()

    # Build PMS
    pms_data = latest.get("PMS")
    if pms_data:
        p = pms_data["payload"]
        demand_kw = p.get("demand_control_power")
        total_kw = p.get("total_active_power")
        soc_avg = p.get("soc_avg")
        soh_avg = p.get("soh_avg")
        cap_total = p.get("capacity_total")
        demand_raw = _encode_power_kw(demand_kw) if demand_kw is not None else None
        total_raw = _encode_power_kw(total_kw) if total_kw is not None else None
        pms = {
            "unit_id": 1,
            "demand_control_power_kw": demand_kw,
            "total_active_power_kw": total_kw,
            "soc_avg_pct": soc_avg,
            "soh_avg_pct": soh_avg,
            "capacity_total_kwh": cap_total,
            "demand_control_power_raw": demand_raw,
            "total_active_power_raw": total_raw,
            "soc_avg_raw": int(soc_avg) if soc_avg is not None else None,
            "soh_avg_raw": int(soh_avg) if soh_avg is not None else None,
            "capacity_total_raw": int(round(cap_total / POWER_SCALE)) if cap_total is not None else None,
            "demand_control_power_meta": {"reg": "HR0", "scale": 0.1, "unit": "kW", "fc": "FC03"},
            "total_active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04"},
            "soc_avg_meta": {"reg": "IR1", "scale": 1, "unit": "%", "fc": "FC04"},
            "soh_avg_meta": {"reg": "IR2", "scale": 1, "unit": "%", "fc": "FC04"},
            "capacity_total_meta": {"reg": "IR3", "scale": 0.1, "unit": "kWh", "fc": "FC04"},
            "occurs_alarms": p.get("occurs_alarms", []),
            "status": p.get("status", "normal"),
            "comm": _build_comm(pms_data["ts"]),
        }
    else:
        pms = {
            "unit_id": 1,
            "demand_control_power_kw": None, "total_active_power_kw": None,
            "soc_avg_pct": None, "soh_avg_pct": None, "capacity_total_kwh": None,
            "demand_control_power_raw": None, "total_active_power_raw": None,
            "soc_avg_raw": None, "soh_avg_raw": None, "capacity_total_raw": None,
            "demand_control_power_meta": {"reg": "HR0", "scale": 0.1, "unit": "kW", "fc": "FC03"},
            "total_active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04"},
            "soc_avg_meta": {"reg": "IR1", "scale": 1, "unit": "%", "fc": "FC04"},
            "soh_avg_meta": {"reg": "IR2", "scale": 1, "unit": "%", "fc": "FC04"},
            "capacity_total_meta": {"reg": "IR3", "scale": 0.1, "unit": "kWh", "fc": "FC04"},
            "occurs_alarms": [],
            "status": "normal",
            "comm": {"ok": False, "last_ok_ts": None, "last_error": "not polled yet"},
        }

    # Build PCS1, PCS2
    def build_pcs(did, port):
        d = latest.get(did)
        if d:
            p = d["payload"]
            ap = p.get("active_power")
            return {
                "unit_id": 1, "port": port,
                "active_power_kw": ap,
                "active_power_raw": _encode_power_kw(ap) if ap is not None else None,
                "active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04"},
                "comm": _build_comm(d["ts"]),
            }
        return {
            "unit_id": 1, "port": port,
            "active_power_kw": None, "active_power_raw": None,
            "active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04"},
            "comm": {"ok": False, "last_ok_ts": None, "last_error": "not polled yet"},
        }

    # Build BMS1, BMS2
    def build_bms(did, port):
        d = latest.get(did)
        if d:
            p = d["payload"]
            soc = p.get("soc")
            soh = p.get("soh")
            cap = p.get("capacity")
            return {
                "unit_id": 1, "port": port,
                "soc_pct": soc, "soh_pct": soh, "capacity_kwh": cap,
                "soc_raw": int(soc) if soc is not None else None,
                "soh_raw": int(soh) if soh is not None else None,
                "capacity_raw": int(round(cap / POWER_SCALE)) if cap is not None else None,
                "soc_meta": {"reg": "IR0", "scale": 1, "unit": "%", "fc": "FC04"},
                "soh_meta": {"reg": "IR1", "scale": 1, "unit": "%", "fc": "FC04"},
                "capacity_meta": {"reg": "IR2", "scale": 0.1, "unit": "kWh", "fc": "FC04"},
                "occurs_alarms": p.get("occurs_alarms", []),
                "status": p.get("status", "normal"),
                "comm": _build_comm(d["ts"]),
            }
        return {
            "unit_id": 1, "port": port,
            "soc_pct": None, "soh_pct": None, "capacity_kwh": None,
            "soc_raw": None, "soh_raw": None, "capacity_raw": None,
            "soc_meta": {"reg": "IR0", "scale": 1, "unit": "%", "fc": "FC04"},
            "soh_meta": {"reg": "IR1", "scale": 1, "unit": "%", "fc": "FC04"},
            "capacity_meta": {"reg": "IR2", "scale": 0.1, "unit": "kWh", "fc": "FC04"},
            "occurs_alarms": [],
            "status": "normal",
            "comm": {"ok": False, "last_ok_ts": None, "last_error": "not polled yet"},
        }

    # Build Multimeter
    def build_multimeter():
        if not _mm_is_enabled():
            return {
                "unit_id": 10, "disabled": True,
                "active_power_kw": None, "active_power_raw": None,
                "active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04 (RTU)"},
                "comm": {"ok": False, "last_ok_ts": None, "last_error": "Disabled by user"},
            }
        d = latest.get("Multimeter")
        if d:
            p = d["payload"]
            ap = p.get("active_power")
            raw = p.get("raw")
            comm_ok = p.get("comm_ok", False)
            return {
                "unit_id": 10,
                "active_power_kw": ap, "active_power_raw": raw,
                "active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04 (RTU)"},
                "comm": _build_comm(d["ts"]) if comm_ok else {"ok": False, "last_ok_ts": None, "last_error": "RTU comm error"},
            }
        return {
            "unit_id": 10,
            "active_power_kw": None, "active_power_raw": None,
            "active_power_meta": {"reg": "IR0", "scale": 0.1, "unit": "kW", "fc": "FC04 (RTU)"},
            "comm": {"ok": False, "last_ok_ts": None, "last_error": "No RTU data received"},
        }

    # Build Transducer
    def build_transducer():
        d = latest.get("Transducer")
        if d:
            p = d["payload"]
            freq = p.get("frequency_hz")
            return {
                "unit_id": 1, "port": 15026,
                "frequency_hz": freq,
                "frequency_raw": int(round(freq / 0.001)) if freq is not None else None,
                "frequency_meta": {"reg": "IR0", "scale": 0.001, "unit": "Hz", "fc": "FC04"},
                "comm": _build_comm(d["ts"]),
            }
        return {
            "unit_id": 1, "port": 15026,
            "frequency_hz": None, "frequency_raw": None,
            "frequency_meta": {"reg": "IR0", "scale": 0.001, "unit": "Hz", "fc": "FC04"},
            "comm": {"ok": False, "last_ok_ts": None, "last_error": "not polled yet"},
        }

    # Build frequency control snapshot
    def build_frequency_control():
        d = latest.get("frequency-control")
        if d:
            p = d["payload"]
            return {
                "f_eff": p.get("f_eff"),
                "delta_f": p.get("delta_f"),
                "delta_p_kw": p.get("delta_p_kw"),
                "p_cmd_kw": p.get("p_cmd_kw"),
                "abnormal_event": p.get("abnormal_event", False),
                "blocked_directions": p.get("blocked_directions", []),
                "plan_id": p.get("plan_id"),
                "ts": d["ts"],
            }
        return None

    return {
        "ts": now,
        "devices": {
            "pms": pms,
            "pcs1": build_pcs("PCS1", 15021),
            "pcs2": build_pcs("PCS2", 15022),
            "bms1": build_bms("BMS1", 15024),
            "bms2": build_bms("BMS2", 15025),
            "multimeter": build_multimeter(),
            "transducer": build_transducer(),
        },
        "frequency_control": build_frequency_control(),
        "last_command": _last_command,
        "config": {
            "host": "EC2 (AWS Pipeline)",
            "port": 8000,
            "power_min_kw": POWER_MIN_KW,
            "power_max_kw": POWER_MAX_KW,
        },
    }


@app.route("/api/snapshot", methods=["GET"])
def api_snapshot():
    """Old UI compatibility — returns snapshot in same format as Node.js server."""
    try:
        snap = _build_snapshot_from_db()
        return jsonify(snap)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Routes: Old UI compatibility — /api/pms/demand (creates command in DB)
# ---------------------------------------------------------------------------
@app.route("/api/pms/demand", methods=["POST"])
def api_pms_demand():
    """Old UI compatibility — write demand via commands table.
    Accepts: { demand_control_power_kw: number }
    Returns: { ok, written_raw, written_kw, command_id, status, queued_status }
    """
    global _last_command
    data = request.get_json(force=True)
    kw_input = data.get("demand_control_power_kw")
    device_id = (data.get("device_id") or DEFAULT_DEVICE_ID).strip()

    if kw_input is None:
        return jsonify({"ok": False, "error": "Missing demand_control_power_kw"}), 400
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400

    try:
        kw = float(kw_input)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "demand_control_power_kw must be a number"}), 400
    if kw < POWER_MIN_KW or kw > POWER_MAX_KW:
        return jsonify({"ok": False, "error": f"Range: {POWER_MIN_KW} to {POWER_MAX_KW}"}), 400

    raw_u16 = _encode_power_kw(kw)
    written_kw = round(_s16(raw_u16) * POWER_SCALE, 1)

    # Determine command type from sign
    if kw < 0:
        cmd_type = "charge"
    elif kw > 0:
        cmd_type = "discharge"
    else:
        cmd_type = "standby"

    try:
        conn = get_db()
        try:
            _ensure_command_contract_schema(conn)
            queued = _queue_command(
                conn,
                command=cmd_type,
                power_kw=abs(written_kw),
                created_by="dashboard",
                device_id=device_id,
                expire_minutes=5,
            )
        finally:
            conn.close()

        _last_command = {
            "command_id": queued["command_id"],
            "device_id": queued["device_id"],
            "kw": written_kw,
            "raw": raw_u16,
            "status": queued["status"],
            "ts": datetime.now(timezone.utc).isoformat(),
            "error": None,
        }
        return jsonify({
            "ok": True,
            "written_raw": raw_u16,
            "written_kw": written_kw,
            "command_id": queued["command_id"],
            "device_id": queued["device_id"],
            "status": "accepted",
            "queued_status": queued["status"],
        })

    except Exception as e:
        _last_command = {**_last_command, "error": str(e), "ts": datetime.now(timezone.utc).isoformat()}
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------------------------
# Routes: Multimeter toggle
# ---------------------------------------------------------------------------
@app.route("/api/settings/multimeter", methods=["GET"])
def api_settings_multimeter_get():
    return jsonify({"enabled": _mm_is_enabled()})


@app.route("/api/settings/multimeter", methods=["POST"])
def api_settings_multimeter_post():
    body = request.get_json(silent=True) or {}
    enabled = bool(body.get("enabled", True))
    _mm_set_enabled(enabled)
    _startup_log.info("Multimeter toggle → %s", "ON" if enabled else "OFF")
    return jsonify({"ok": True, "enabled": enabled})


# ---------------------------------------------------------------------------
# Routes: Old UI compatibility — /api/timeline (query device_data history)
# ---------------------------------------------------------------------------
@app.route("/api/timeline", methods=["GET"])
def api_timeline():
    """Old UI compatibility — return historical data in timeline table format.
    Query: device (pms|pcs1|...), metric, minutes, limit
    """
    device = request.args.get("device", "pms").lower()
    metric = request.args.get("metric", "active_power_kw")
    minutes = min(int(request.args.get("minutes", "60")), 1440)
    limit = min(int(request.args.get("limit", "300")), 2000)

    # Map device name to device_id in DB
    device_map = {"pms": "PMS", "pcs1": "PCS1", "pcs2": "PCS2", "bms1": "BMS1", "bms2": "BMS2", "multimeter": "Multimeter"}
    device_id = device_map.get(device, device.upper())

    # Map metric name to payload JSON key
    metric_key_map = {
        "active_power_kw": ("total_active_power", "kW"),
        "demand_control_power_kw": ("demand_control_power", "kW"),
        "soc_pct": ("soc_avg", "%"),
        "soh_pct": ("soh_avg", "%"),
        "capacity_kwh": ("capacity_total", "kWh"),
    }

    # For PCS devices, active_power_kw → active_power
    if device_id in ("PCS1", "PCS2"):
        metric_key_map["active_power_kw"] = ("active_power", "kW")
    # For BMS devices
    if device_id in ("BMS1", "BMS2"):
        metric_key_map["soc_pct"] = ("soc", "%")
        metric_key_map["soh_pct"] = ("soh", "%")
        metric_key_map["capacity_kwh"] = ("capacity", "kWh")
    # For Multimeter
    if device_id == "Multimeter":
        metric_key_map["active_power_kw"] = ("active_power", "kW")

    payload_key, unit = metric_key_map.get(metric, (metric, ""))

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts, device_id, payload "
            "FROM device_data "
            "WHERE device_id = %s AND ts >= now() - make_interval(mins => %s) "
            "ORDER BY ts ASC LIMIT %s",
            (device_id, minutes, limit),
        )
        rows = cur.fetchall()
        result = []
        for r in rows:
            payload = r[2] if isinstance(r[2], dict) else json.loads(r[2])
            value = payload.get(payload_key)
            if value is not None:
                result.append({
                    "ts": r[0].isoformat() if r[0] else None,
                    "device_id": device,
                    "metric": metric,
                    "value": value,
                    "unit": unit,
                    "comm_ok": True,
                })
        return jsonify(result)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Routes: Schedules (charge / discharge scheduling)
# ---------------------------------------------------------------------------
def _parse_iso(ts_str):
    """Parse ISO 8601 string → timezone-aware datetime, or None."""
    if not ts_str:
        return None
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def _schedule_row_to_dict(r):
    """Convert a schedule DB row tuple to a JSON-serialisable dict."""
    return {
        "id": r[0],
        "start_time": r[1].isoformat() if r[1] else None,
        "end_time": r[2].isoformat() if r[2] else None,
        "control_power_kw": float(r[3]) if r[3] is not None else None,
        "status": r[4],
        "created_at": r[5].isoformat() if r[5] else None,
        "created_by": r[6],
    }


@app.route("/api/schedules", methods=["POST"])
def post_schedule():
    """Create a charge/discharge schedule with overlap validation.
    Body: { start_time, end_time, control_power_kw, created_by? }
    - Positive control_power_kw = discharge, negative = charge
    - Returns 409 if overlapping an existing active schedule
    """
    data = request.get_json(force=True)

    # --- Validate required fields ---
    for field in ("start_time", "end_time", "control_power_kw"):
        if field not in data or data[field] is None:
            return jsonify({"error": f"{field} is required"}), 400

    try:
        start = _parse_iso(data["start_time"])
        end = _parse_iso(data["end_time"])
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid datetime format: {e}"}), 400

    if start is None or end is None:
        return jsonify({"error": "start_time and end_time must be valid ISO 8601"}), 400
    if end <= start:
        return jsonify({"error": "end_time must be after start_time"}), 400

    power_kw = float(data["control_power_kw"])
    created_by = data.get("created_by", "frontend")

    # --- Overlap check against active schedules ---
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, start_time, end_time, control_power_kw "
            "FROM schedules "
            "WHERE status = 'active' "
            "  AND start_time < %s AND end_time > %s",
            (end, start),
        )
        overlaps = cur.fetchall()
        if overlaps:
            conflicts = [{
                "id": o[0],
                "start_time": o[1].isoformat() if o[1] else None,
                "end_time": o[2].isoformat() if o[2] else None,
                "control_power_kw": float(o[3]) if o[3] is not None else None,
            } for o in overlaps]
            return jsonify({
                "error": "Schedule overlaps with existing active schedule(s)",
                "conflicts": conflicts,
            }), 409

        # --- Insert ---
        cur.execute(
            "INSERT INTO schedules (start_time, end_time, control_power_kw, created_by) "
            "VALUES (%s, %s, %s, %s) "
            "RETURNING id, start_time, end_time, control_power_kw, status, created_at, created_by",
            (start, end, power_kw, created_by),
        )
        row = cur.fetchone()
        conn.commit()
        return jsonify(_schedule_row_to_dict(row)), 201
    except Exception as exc:
        conn.rollback()
        logging.error("POST /api/schedules failed: %s", exc)
        return jsonify({"error": str(exc)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/api/schedules", methods=["GET"])
def get_schedules():
    """List schedules.  Optional query: ?status=active&limit=50"""
    status_filter = request.args.get("status")
    limit = min(int(request.args.get("limit", "50")), 200)

    conn = get_db()
    cur = conn.cursor()
    try:
        if status_filter:
            cur.execute(
                "SELECT id, start_time, end_time, control_power_kw, status, created_at, created_by "
                "FROM schedules WHERE status = %s "
                "ORDER BY start_time ASC LIMIT %s",
                (status_filter, limit),
            )
        else:
            cur.execute(
                "SELECT id, start_time, end_time, control_power_kw, status, created_at, created_by "
                "FROM schedules ORDER BY start_time ASC LIMIT %s",
                (limit,),
            )
        rows = cur.fetchall()
        return jsonify([_schedule_row_to_dict(r) for r in rows])
    finally:
        cur.close()
        conn.close()


@app.route("/api/schedules/active", methods=["GET"])
def get_active_schedules():
    """Active schedules whose end_time is still in the future."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, start_time, end_time, control_power_kw, status, created_at, created_by "
            "FROM schedules "
            "WHERE status = 'active' AND end_time > now() "
            "ORDER BY start_time ASC"
        )
        rows = cur.fetchall()
        return jsonify([_schedule_row_to_dict(r) for r in rows])
    finally:
        cur.close()
        conn.close()


@app.route("/api/schedules/<int:schedule_id>", methods=["DELETE"])
def delete_schedule(schedule_id):
    """Soft-delete: set status = 'canceled'."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE schedules SET status = 'canceled' "
            "WHERE id = %s AND status = 'active' "
            "RETURNING id",
            (schedule_id,),
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return jsonify({"error": "Schedule not found or already canceled"}), 404
        return jsonify({"ok": True, "id": schedule_id, "status": "canceled"})
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Frequency Plans (一次調整力 ichiji)
# ---------------------------------------------------------------------------
MAX_DEADBAND_BY_BASE_FREQUENCY_HZ = {
    50.0: 0.01,
    60.0: 0.012,
}
MAX_MEASUREMENT_INTERVAL_MS = 100
_freq_plans_schema_ready = False


def _get_deadband_limit_hz(base_frequency_hz: float) -> float:
    return MAX_DEADBAND_BY_BASE_FREQUENCY_HZ.get(
        float(base_frequency_hz),
        MAX_DEADBAND_BY_BASE_FREQUENCY_HZ[50.0],
    )


def _ensure_frequency_plans_schema(conn) -> None:
    """Auto-create frequency_plans table if missing."""
    global _freq_plans_schema_ready
    if _freq_plans_schema_ready:
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS frequency_plans (
                id SERIAL PRIMARY KEY,
                plan_type TEXT NOT NULL DEFAULT 'FREQUENCY',
                device_id TEXT NOT NULL DEFAULT 'bess-01',
                enabled BOOLEAN NOT NULL DEFAULT false,
                base_frequency_hz REAL NOT NULL DEFAULT 50.0,
                baseline_power_kw REAL NOT NULL DEFAULT 0.0,
                abnormal_frequency_hz REAL NOT NULL DEFAULT 49.8,
                deadband_hz REAL NOT NULL DEFAULT 0.01,
                droop_percent REAL NOT NULL DEFAULT 5.0,
                awarded_power_kw REAL NOT NULL DEFAULT 300.0,
                command_interval_ms INT NOT NULL DEFAULT 500,
                measurement_interval_ms INT NOT NULL DEFAULT 100,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        conn.commit()
        _freq_plans_schema_ready = True
        _startup_log.info("frequency_plans schema ready")
    except Exception as e:
        conn.rollback()
        _startup_log.warning("frequency_plans schema migration error: %s", e)
    finally:
        cur.close()


@app.route("/api/plans", methods=["POST"])
def post_plan():
    """Create or update a frequency plan."""
    data = request.get_json(force=True)

    base_freq = float(data.get("base_frequency_hz", 50.0))
    if base_freq not in (50.0, 60.0):
        return jsonify({"error": "base_frequency_hz must be 50 or 60"}), 400

    deadband = float(data.get("deadband_hz", 0.01))
    deadband_limit = _get_deadband_limit_hz(base_freq)
    if deadband <= 0 or deadband > deadband_limit:
        return jsonify({
            "error": (
                f"deadband_hz must be > 0 and <= {deadband_limit:g} "
                f"for {int(base_freq)}Hz"
            )
        }), 400

    droop = float(data.get("droop_percent", 5.0))
    if droop <= 0 or droop > 5:
        return jsonify({"error": "droop_percent must be > 0 and <= 5"}), 400

    awarded = float(data.get("awarded_power_kw", 300.0))
    if awarded <= 0:
        return jsonify({"error": "awarded_power_kw must be > 0"}), 400

    device_id = (data.get("device_id") or DEFAULT_DEVICE_ID).strip()
    enabled = bool(data.get("enabled", False))
    baseline = float(data.get("baseline_power_kw", 0.0))
    abnormal = float(data.get("abnormal_frequency_hz", 49.8))
    cmd_interval = int(data.get("command_interval_ms", 500))
    if cmd_interval <= 0:
        return jsonify({"error": "command_interval_ms must be > 0"}), 400
    meas_interval = int(data.get("measurement_interval_ms", 100))
    if meas_interval <= 0 or meas_interval > MAX_MEASUREMENT_INTERVAL_MS:
        return jsonify({
            "error": (
                f"measurement_interval_ms must be > 0 and <= "
                f"{MAX_MEASUREMENT_INTERVAL_MS}"
            )
        }), 400

    try:
        conn = get_db()
        try:
            _ensure_frequency_plans_schema(conn)
            cur = conn.cursor()

            # If enabling this plan, disable all others for same device_id
            if enabled:
                cur.execute(
                    "UPDATE frequency_plans SET enabled = false, updated_at = now() "
                    "WHERE device_id = %s AND enabled = true",
                    (device_id,),
                )

            cur.execute(
                "INSERT INTO frequency_plans "
                "(plan_type, device_id, enabled, base_frequency_hz, baseline_power_kw, "
                " abnormal_frequency_hz, deadband_hz, droop_percent, awarded_power_kw, "
                " command_interval_ms, measurement_interval_ms) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                ("FREQUENCY", device_id, enabled, base_freq, baseline,
                 abnormal, deadband, droop, awarded, cmd_interval, meas_interval),
            )
            plan_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
        finally:
            conn.close()

        return jsonify({"ok": True, "id": plan_id, "enabled": enabled})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/plans", methods=["GET"])
def get_plans():
    """List all frequency plans."""
    device_id = request.args.get("device_id", "")
    try:
        conn = get_db()
        try:
            _ensure_frequency_plans_schema(conn)
            cur = conn.cursor()
            if device_id:
                cur.execute(
                    "SELECT id, plan_type, device_id, enabled, base_frequency_hz, "
                    "baseline_power_kw, abnormal_frequency_hz, deadband_hz, droop_percent, "
                    "awarded_power_kw, command_interval_ms, measurement_interval_ms, "
                    "created_at, updated_at "
                    "FROM frequency_plans WHERE device_id = %s ORDER BY id DESC",
                    (device_id,),
                )
            else:
                cur.execute(
                    "SELECT id, plan_type, device_id, enabled, base_frequency_hz, "
                    "baseline_power_kw, abnormal_frequency_hz, deadband_hz, droop_percent, "
                    "awarded_power_kw, command_interval_ms, measurement_interval_ms, "
                    "created_at, updated_at "
                    "FROM frequency_plans ORDER BY id DESC"
                )
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        plans = []
        for r in rows:
            plans.append({
                "id": r[0], "plan_type": r[1], "device_id": r[2], "enabled": r[3],
                "base_frequency_hz": r[4], "baseline_power_kw": r[5],
                "abnormal_frequency_hz": r[6], "deadband_hz": r[7],
                "droop_percent": r[8], "awarded_power_kw": r[9],
                "command_interval_ms": r[10], "measurement_interval_ms": r[11],
                "created_at": r[12].isoformat() if r[12] else None,
                "updated_at": r[13].isoformat() if r[13] else None,
            })
        return jsonify(plans)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/plans/active", methods=["GET"])
def get_active_plan():
    """Get the active (enabled) frequency plan for a device."""
    device_id = request.args.get("device_id", DEFAULT_DEVICE_ID)
    try:
        conn = get_db()
        try:
            _ensure_frequency_plans_schema(conn)
            cur = conn.cursor()
            cur.execute(
                "SELECT id, plan_type, device_id, enabled, base_frequency_hz, "
                "baseline_power_kw, abnormal_frequency_hz, deadband_hz, droop_percent, "
                "awarded_power_kw, command_interval_ms, measurement_interval_ms, "
                "created_at, updated_at "
                "FROM frequency_plans WHERE device_id = %s AND enabled = true "
                "ORDER BY updated_at DESC LIMIT 1",
                (device_id,),
            )
            r = cur.fetchone()
            cur.close()
        finally:
            conn.close()

        if not r:
            return jsonify(None)

        return jsonify({
            "id": r[0], "plan_type": r[1], "device_id": r[2], "enabled": r[3],
            "base_frequency_hz": r[4], "baseline_power_kw": r[5],
            "abnormal_frequency_hz": r[6], "deadband_hz": r[7],
            "droop_percent": r[8], "awarded_power_kw": r[9],
            "command_interval_ms": r[10], "measurement_interval_ms": r[11],
            "created_at": r[12].isoformat() if r[12] else None,
            "updated_at": r[13].isoformat() if r[13] else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/plans/<int:plan_id>", methods=["DELETE"])
def delete_plan(plan_id):
    """Disable a frequency plan."""
    try:
        conn = get_db()
        try:
            _ensure_frequency_plans_schema(conn)
            cur = conn.cursor()
            cur.execute(
                "UPDATE frequency_plans SET enabled = false, updated_at = now() WHERE id = %s",
                (plan_id,),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return jsonify({"ok": True, "id": plan_id, "enabled": False})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Routes: Frontend
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("API_PORT", "8000"))
    print(f"Starting API server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
