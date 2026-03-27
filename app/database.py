import sqlite3
import time
import os
import datetime
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", "/data/monitor.db")
RETENTION_SECONDS = 86400  # 24 hours


def _ensure_dir():
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)


@contextmanager
def get_conn():
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bandwidth (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL    NOT NULL,
                interface TEXT    NOT NULL,
                bps_in    REAL    NOT NULL,
                bps_out   REAL    NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gateways (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL    NOT NULL,
                gateway   TEXT    NOT NULL,
                rtt_ms    REAL,
                rttd_ms   REAL,
                loss_pct  REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usage_summary (
                period_type TEXT NOT NULL,
                period_key  TEXT NOT NULL,
                bytes_in    REAL NOT NULL DEFAULT 0,
                bytes_out   REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (period_type, period_key)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bw_ts    ON bandwidth(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gw_ts    ON gateways(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bw_iface ON bandwidth(interface, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gw_name  ON gateways(gateway, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_us_type  ON usage_summary(period_type, period_key)")
        conn.commit()


def _prune(conn):
    cutoff = time.time() - RETENTION_SECONDS
    conn.execute("DELETE FROM bandwidth WHERE timestamp < ?", (cutoff,))
    conn.execute("DELETE FROM gateways  WHERE timestamp < ?", (cutoff,))


def insert_bandwidth(interface: str, bps_in: float, bps_out: float):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO bandwidth (timestamp, interface, bps_in, bps_out) VALUES (?,?,?,?)",
            (time.time(), interface, bps_in, bps_out),
        )
        _prune(conn)
        conn.commit()


def insert_gateway(gateway: str, rtt_ms: float | None, rttd_ms: float | None, loss_pct: float | None):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO gateways (timestamp, gateway, rtt_ms, rttd_ms, loss_pct) VALUES (?,?,?,?,?)",
            (time.time(), gateway, rtt_ms, rttd_ms, loss_pct),
        )
        _prune(conn)
        conn.commit()


def update_usage_summary(bytes_in: float, bytes_out: float):
    """
    Accumulate bytes_in / bytes_out into all four period buckets for right now.
    Retains: 24 hourly, 365 daily, 120 monthly (10 yrs), all yearly records.
    """
    now = datetime.datetime.now()
    periods = [
        ("hourly",  now.strftime("%Y-%m-%d %H")),
        ("daily",   now.strftime("%Y-%m-%d")),
        ("monthly", now.strftime("%Y-%m")),
        ("yearly",  now.strftime("%Y")),
    ]
    with get_conn() as conn:
        for pt, pk in periods:
            conn.execute("""
                INSERT INTO usage_summary (period_type, period_key, bytes_in, bytes_out)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(period_type, period_key) DO UPDATE SET
                    bytes_in  = bytes_in  + excluded.bytes_in,
                    bytes_out = bytes_out + excluded.bytes_out
            """, (pt, pk, bytes_in, bytes_out))

        # Prune old records (keep most-recent N per type)
        for pt, limit in (("hourly", 24), ("daily", 365), ("monthly", 120)):
            conn.execute(f"""
                DELETE FROM usage_summary
                WHERE period_type = ?
                  AND period_key < (
                      SELECT MIN(period_key) FROM (
                          SELECT period_key FROM usage_summary
                          WHERE period_type = ?
                          ORDER BY period_key DESC
                          LIMIT ?
                      )
                  )
            """, (pt, pt, limit))

        conn.commit()


def compute_hourly_from_bandwidth() -> list[dict]:
    """
    Compute per-hour totals by integrating bps rates stored in the bandwidth
    table (covers the last 24 h). Works immediately — no warm-up needed.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, bps_in, bps_out FROM bandwidth ORDER BY timestamp"
        ).fetchall()

    buckets: dict[str, dict] = {}
    for i in range(1, len(rows)):
        dt = rows[i]["timestamp"] - rows[i - 1]["timestamp"]
        if dt <= 0 or dt > 7200:   # skip gaps > 2 h (e.g. after restart)
            continue
        bi = rows[i]["bps_in"]  * dt / 8
        bo = rows[i]["bps_out"] * dt / 8
        key = datetime.datetime.fromtimestamp(rows[i]["timestamp"]).strftime("%Y-%m-%d %H")
        if key not in buckets:
            buckets[key] = {"period_key": key, "bytes_in": 0.0, "bytes_out": 0.0}
        buckets[key]["bytes_in"]  += bi
        buckets[key]["bytes_out"] += bo

    return sorted(buckets.values(), key=lambda x: x["period_key"], reverse=True)


def compute_daily_from_bandwidth() -> list[dict]:
    """
    Compute per-day totals from the bandwidth table for recent days,
    merged with the persistent usage_summary for days older than 24 h.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, bps_in, bps_out FROM bandwidth ORDER BY timestamp"
        ).fetchall()

    recent: dict[str, dict] = {}
    for i in range(1, len(rows)):
        dt = rows[i]["timestamp"] - rows[i - 1]["timestamp"]
        if dt <= 0 or dt > 7200:
            continue
        bi = rows[i]["bps_in"]  * dt / 8
        bo = rows[i]["bps_out"] * dt / 8
        key = datetime.datetime.fromtimestamp(rows[i]["timestamp"]).strftime("%Y-%m-%d")
        if key not in recent:
            recent[key] = {"period_key": key, "bytes_in": 0.0, "bytes_out": 0.0}
        recent[key]["bytes_in"]  += bi
        recent[key]["bytes_out"] += bo

    # Merge: stored rows for older days, freshly-computed for recent days
    merged: dict[str, dict] = {r["period_key"]: dict(r)
                                for r in get_usage_summary("daily")}
    merged.update(recent)   # recent always wins
    return sorted(merged.values(), key=lambda x: x["period_key"], reverse=True)


def get_usage_summary(period_type: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT period_key, bytes_in, bytes_out FROM usage_summary "
            "WHERE period_type=? ORDER BY period_key DESC",
            (period_type,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_bandwidth_history(interface: str, since: float | None = None) -> list[dict]:
    if since is None:
        since = time.time() - RETENTION_SECONDS
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, bps_in, bps_out FROM bandwidth "
            "WHERE interface=? AND timestamp>=? ORDER BY timestamp",
            (interface, since),
        ).fetchall()
    return [dict(r) for r in rows]


def get_gateway_history(gateway: str | None = None, since: float | None = None) -> list[dict]:
    if since is None:
        since = time.time() - RETENTION_SECONDS
    with get_conn() as conn:
        if gateway:
            rows = conn.execute(
                "SELECT timestamp, gateway, rtt_ms, rttd_ms, loss_pct FROM gateways "
                "WHERE gateway=? AND timestamp>=? ORDER BY timestamp",
                (gateway, since),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT timestamp, gateway, rtt_ms, rttd_ms, loss_pct FROM gateways "
                "WHERE timestamp>=? ORDER BY timestamp",
                (since,),
            ).fetchall()
    return [dict(r) for r in rows]


def get_daily_totals(interface: str) -> dict:
    """
    Return total bytes in/out since midnight (local time) today.
    Computed by integrating the stored bps rates over their intervals.
    """
    now = datetime.datetime.now()
    midnight = datetime.datetime(now.year, now.month, now.day).timestamp()

    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, bps_in, bps_out FROM bandwidth "
            "WHERE interface=? AND timestamp>=? ORDER BY timestamp",
            (interface, midnight),
        ).fetchall()

    if len(rows) < 2:
        return {"bytes_in": 0.0, "bytes_out": 0.0, "since": midnight}

    total_in = total_out = 0.0
    for i in range(1, len(rows)):
        dt = rows[i]["timestamp"] - rows[i - 1]["timestamp"]
        total_in  += rows[i]["bps_in"]  * dt / 8
        total_out += rows[i]["bps_out"] * dt / 8

    return {"bytes_in": total_in, "bytes_out": total_out, "since": midnight}


def get_stored_interfaces() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT interface FROM bandwidth").fetchall()
    return [r["interface"] for r in rows]


def get_stored_gateways() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT DISTINCT gateway FROM gateways").fetchall()
    return [r["gateway"] for r in rows]
