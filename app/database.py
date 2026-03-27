import sqlite3
import time
import os
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bw_ts  ON bandwidth(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gw_ts  ON gateways(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bw_iface ON bandwidth(interface, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gw_name  ON gateways(gateway, timestamp)")
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
    Resets automatically at 00:00 each day.
    """
    import datetime
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
        # bps_in represents the rate during the interval ending at rows[i]
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
