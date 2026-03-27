import asyncio
import json
import logging
import os
import socket
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from collector import OPNsenseCollector
from database import (
    compute_daily_from_bandwidth,
    compute_hourly_from_bandwidth,
    compute_monthly_from_daily,
    compute_yearly_from_monthly,
    get_bandwidth_history,
    get_daily_totals,
    get_gateway_history,
    get_stored_gateways,
    get_stored_interfaces,
    get_usage_summary,
    init_db,
    insert_bandwidth,
    insert_gateway,
    update_usage_summary,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/data/config.json")

_DEFAULT_CONFIG: dict = {
    "host":         os.environ.get("OPNSENSE_HOST",       "https://192.168.1.1"),
    "api_key":      os.environ.get("OPNSENSE_API_KEY",    ""),
    "api_secret":   os.environ.get("OPNSENSE_API_SECRET", ""),
    # Leave empty to auto-resolve from gateway's 'if' field
    "interface":    os.environ.get("OPNSENSE_INTERFACE",  ""),
    "gateway":      os.environ.get("OPNSENSE_GATEWAY",    "WAN_DHCP"),
    "verify_ssl":   os.environ.get("OPNSENSE_VERIFY_SSL", "false").lower() == "true",
    "poll_interval": int(os.environ.get("POLL_INTERVAL", "60")),
}


def _load_config() -> dict:
    p = Path(CONFIG_PATH)
    if p.exists():
        try:
            saved = json.loads(p.read_text())
            cfg = _DEFAULT_CONFIG.copy()
            cfg.update(saved)
            return cfg
        except Exception:
            pass
    return _DEFAULT_CONFIG.copy()


def _save_config(cfg: dict):
    p = Path(CONFIG_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cfg, indent=2))


_config: dict = _load_config()

# ---------------------------------------------------------------------------
# Background polling state
# ---------------------------------------------------------------------------
_collector: Optional[OPNsenseCollector] = None
_poll_task: Optional[asyncio.Task] = None
_status: dict = {"ok": None, "error": None, "last_poll": None}

# The actual BSD kernel interface name used for bandwidth (may be auto-resolved)
_active_interface: str = _config.get("interface", "")
# OPNsense logical interface name (e.g. "wan", "opt1") — needed for traffic/top API
_active_opn_interface: str = ""

# ── Top-talkers state ─────────────────────────────────────────────────────────
# DNS cache: ip -> (hostname, expiry_timestamp)
_dns_cache: dict[str, tuple[str, float]] = {}
DNS_TTL = 300.0  # 5 minutes

# Per-IP rate history: ip -> deque of (timestamp, combined_rate_bps)
_talker_history: dict[str, deque] = {}

# Last interface name that actually returned top-talker records (cached for speed)
_top_talker_working_iface: str = ""


def _make_collector() -> OPNsenseCollector:
    return OPNsenseCollector(
        host=_config["host"],
        api_key=_config["api_key"],
        api_secret=_config["api_secret"],
        verify_ssl=_config["verify_ssl"],
    )


def _resolve_interfaces_from_gateways(gateways: list[dict]) -> tuple[str, str]:
    """
    Given the live gateway list, return (bsd_if, opn_if) for bandwidth monitoring.

    bsd_if — BSD kernel name (e.g. 'ix1') used for interface statistics API.
    opn_if — OPNsense logical name (e.g. 'wan') used for traffic/top API.

    Priority:
      1. Explicit override in config['interface'] (bsd_if only; opn_if stays empty)
      2. Names from the configured gateway
      3. Names from the first gateway that has them
    """
    manual = _config.get("interface", "").strip()
    if manual:
        return manual, ""

    target = _config.get("gateway", "").strip()

    if target:
        for gw in gateways:
            if gw["name"] == target and gw.get("bsd_if"):
                return gw["bsd_if"], gw.get("opn_if", "")

    for gw in gateways:
        if gw.get("bsd_if"):
            return gw["bsd_if"], gw.get("opn_if", "")

    return "", ""


# Keep old name as alias so nothing else breaks
def _resolve_interface_from_gateways(gateways: list[dict]) -> str:
    return _resolve_interfaces_from_gateways(gateways)[0]


async def _poll_once(col: OPNsenseCollector):
    global _status, _active_interface, _active_opn_interface

    try:
        # ── 1. Gateway status (RTT / RTTd / loss + interface resolution) ──
        gateways = await col.get_gateway_status()

        target_gw = _config.get("gateway", "")
        for gw in gateways:
            if gw["rtt_ms"] is not None:
                if not target_gw or gw["name"] == target_gw:
                    insert_gateway(
                        gw["name"],
                        gw["rtt_ms"],
                        gw.get("rttd_ms"),
                        gw.get("loss_pct", 0.0),
                    )
                    logger.info(
                        "GW %s (%s): rtt=%.2f ms  rttd=%s ms  loss=%.1f%%",
                        gw["name"],
                        gw.get("address", ""),
                        gw["rtt_ms"],
                        f"{gw['rttd_ms']:.2f}" if gw.get("rttd_ms") is not None else "n/a",
                        gw.get("loss_pct") or 0,
                    )

        # ── 2. Resolve BSD + OPN interface names ────────────────────────────
        resolved, resolved_opn = _resolve_interfaces_from_gateways(gateways)
        if resolved and resolved != _active_interface:
            logger.info(
                "Active interface: %r → %r (bsd) / %r (opn)",
                _active_interface, resolved, resolved_opn,
            )
        _active_interface = resolved
        _active_opn_interface = resolved_opn

        # ── 3. Bandwidth ────────────────────────────────────────────────────
        if _active_interface:
            bw = await col.get_bandwidth(_active_interface)
            if bw is not None:
                bps_in, bps_out, bytes_in, bytes_out = bw
                insert_bandwidth(_active_interface, bps_in, bps_out)
                update_usage_summary(bytes_in, bytes_out)
                logger.info(
                    "BW %s: ↓ %.2f Mbps  ↑ %.2f Mbps",
                    _active_interface, bps_in / 1e6, bps_out / 1e6,
                )
        else:
            logger.warning(
                "No interface configured and could not auto-resolve from gateways. "
                "Set 'gateway' in Settings (e.g. Port2_WAN) or specify 'interface' explicitly."
            )

        _status = {"ok": True, "error": None, "last_poll": time.time()}

    except Exception as exc:
        logger.error("Poll failed: %s", exc)
        _status = {"ok": False, "error": str(exc), "last_poll": time.time()}


async def _poll_loop():
    global _collector
    _collector = _make_collector()
    while True:
        await _poll_once(_collector)
        await asyncio.sleep(_config.get("poll_interval", 60))


def _restart_poll():
    global _poll_task, _collector, _active_interface, _active_opn_interface, _top_talker_working_iface
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()
    _collector = None
    _active_interface = _config.get("interface", "")
    _active_opn_interface = ""
    _top_talker_working_iface = ""
    _poll_task = asyncio.create_task(_poll_loop())


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _restart_poll()
    yield
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()


app = FastAPI(title="OPNsense Monitor", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------
class ConfigUpdate(BaseModel):
    host:          Optional[str]  = None
    api_key:       Optional[str]  = None
    api_secret:    Optional[str]  = None
    interface:     Optional[str]  = None
    gateway:       Optional[str]  = None
    verify_ssl:    Optional[bool] = None
    poll_interval: Optional[int]  = None


def _safe_config() -> dict:
    c = _config.copy()
    if c.get("api_secret"):
        c["api_secret"] = "••••••••"
    return c


@app.get("/api/status")
async def api_status():
    return {
        "poll_status":        _status,
        "config":             _safe_config(),
        "active_interface":   _active_interface,
    }


@app.get("/api/config")
async def api_get_config():
    return _safe_config()


@app.post("/api/config")
async def api_set_config(update: ConfigUpdate):
    global _config
    patch = update.model_dump(exclude_none=True)
    if patch.get("api_secret") == "••••••••":
        patch.pop("api_secret")
    _config.update(patch)
    _save_config(_config)
    _restart_poll()
    return {"ok": True}


@app.get("/api/data")
async def api_data(hours: float = 24):
    since = time.time() - hours * 3600

    # Use the actively-resolved interface for bandwidth lookup
    iface = _active_interface or _config.get("interface", "")
    bandwidth = get_bandwidth_history(iface, since) if iface else []

    target_gw = _config.get("gateway", "")
    raw_gw = get_gateway_history(target_gw or None, since)

    gw_grouped: dict[str, list] = {}
    for row in raw_gw:
        name = row["gateway"]
        gw_grouped.setdefault(name, []).append(row)

    return {
        "bandwidth":    bandwidth,
        "gateways":     gw_grouped,
        "daily_totals": get_daily_totals(iface) if iface else {"bytes_in": 0.0, "bytes_out": 0.0},
        "meta": {
            "active_interface":  iface,
            "target_gateway":    target_gw,
            "stored_interfaces": get_stored_interfaces(),
            "stored_gateways":   get_stored_gateways(),
        },
    }


@app.get("/api/interfaces")
async def api_interfaces():
    if _collector is None:
        return {"interfaces": get_stored_interfaces()}
    return {"interfaces": await _collector.get_available_interfaces()}


@app.get("/api/gateways-live")
async def api_gateways_live():
    if _collector is None:
        raise HTTPException(503, "Collector not ready")
    gateways = await _collector.get_gateway_status()
    return {"gateways": gateways}


@app.get("/api/usage-summary")
async def api_usage_summary():
    import datetime
    now = datetime.datetime.now()
    return {
        # Hourly + daily: computed live from the bandwidth table — no warm-up needed
        "hourly":  compute_hourly_from_bandwidth(),
        "daily":   compute_daily_from_bandwidth(),
        # Monthly + yearly: derived from daily totals — always accurate
        "monthly": compute_monthly_from_daily(),
        "yearly":  compute_yearly_from_monthly(),
        "current_keys": {
            "hourly":  now.strftime("%Y-%m-%d %H"),
            "daily":   now.strftime("%Y-%m-%d"),
            "monthly": now.strftime("%Y-%m"),
            "yearly":  now.strftime("%Y"),
        },
    }


async def _dns_lookup(ip: str) -> str:
    """Reverse DNS with in-process cache (5-min TTL). Never raises."""
    now = time.time()
    cached = _dns_cache.get(ip)
    if cached and now < cached[1]:
        return cached[0]
    try:
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, socket.gethostbyaddr, ip),
            timeout=2.0,
        )
        name = result[0]
    except Exception:
        name = ip
    _dns_cache[ip] = (name, now + DNS_TTL)
    return name


@app.get("/api/top-talkers")
async def api_top_talkers():
    """
    Returns the current top bandwidth users on the active WAN interface.
    Includes per-host 1-minute rate history to identify sustained top users.
    """
    global _top_talker_working_iface

    if _collector is None or not _active_interface:
        return {"talkers": [], "top_ip": None, "second_ip": None}

    # The OPNsense traffic/top API endpoint accepts OPNsense logical interface
    # names (e.g. "wan", "opt1").  The BSD name (e.g. "ix1") may also work on
    # some builds and is kept as a fallback.  Once a name returns records we
    # cache it to avoid redundant probing on subsequent calls.
    candidates: list[str] = []
    if _top_talker_working_iface:
        candidates.append(_top_talker_working_iface)
    if _active_opn_interface and _active_opn_interface not in candidates:
        candidates.append(_active_opn_interface)
    if _active_interface not in candidates:
        candidates.append(_active_interface)

    records: list[dict] = []
    iface_used = candidates[0] if candidates else _active_interface
    for candidate in candidates:
        try:
            result = await _collector.get_top_talkers(candidate, limit=10)
            logger.debug(
                "Top talkers probe: iface=%r → %d records", candidate, len(result)
            )
            if result:
                records = result
                iface_used = candidate
                if _top_talker_working_iface != candidate:
                    logger.info("Top talkers: using interface %r", candidate)
                    _top_talker_working_iface = candidate
                break
        except Exception as exc:
            logger.warning("Top talkers fetch failed (iface=%r): %s", candidate, exc)

    if not records:
        logger.debug(
            "Top talkers: no records from any candidate %s (opn=%r bsd=%r)",
            candidates, _active_opn_interface, _active_interface,
        )

    now = time.time()

    # Update per-host 2-minute rate history
    for rec in records:
        ip = rec["address"]
        if ip not in _talker_history:
            _talker_history[ip] = deque()
        dq = _talker_history[ip]
        dq.append((now, rec["rate_bits_in"] + rec["rate_bits_out"]))
        while dq and dq[0][0] < now - 120:
            dq.popleft()

    # Score each host by average combined rate over the last 60 s
    scores: dict[str, float] = {}
    for ip, dq in _talker_history.items():
        window = [(ts, r) for ts, r in dq if ts >= now - 60]
        if window:
            scores[ip] = sum(r for _, r in window) / len(window)

    ranked = sorted(scores, key=lambda x: scores[x], reverse=True)
    top_ip    = ranked[0] if len(ranked) > 0 else None
    second_ip = ranked[1] if len(ranked) > 1 else None

    # Resolve hostnames for IPs that don't already have one from the API
    need_resolve = [r["address"] for r in records
                    if not r["hostname"] or r["hostname"] == r["address"]]
    if need_resolve:
        resolved = await asyncio.gather(*[_dns_lookup(ip) for ip in need_resolve])
        name_map = dict(zip(need_resolve, resolved))
        for rec in records:
            if rec["address"] in name_map:
                rec["hostname"] = name_map[rec["address"]]

    return {
        "talkers":   records[:5],
        "top_ip":    top_ip,
        "second_ip": second_ip,
    }


@app.get("/api/debug/iface-stats")
async def api_debug_iface_stats():
    """
    Returns the raw OPNsense interface statistics response.
    Useful for diagnosing bandwidth collection issues (field names, interface names).
    """
    if _collector is None:
        raise HTTPException(503, "Collector not ready")
    try:
        raw = await _collector.get_raw_interface_stats()
        return {"raw": raw, "active_interface": _active_interface}
    except Exception as exc:
        raise HTTPException(500, str(exc))


@app.get("/api/debug/top-talkers-raw")
async def api_debug_top_talkers_raw():
    """
    Returns the raw OPNsense response for the traffic/top endpoint for each
    candidate interface name.  Use this to diagnose why top talkers may not
    be showing: check which interface names return records and what fields
    the records contain.
    """
    if _collector is None:
        raise HTTPException(503, "Collector not ready")

    candidates = list(dict.fromkeys(filter(None, [
        _active_opn_interface,
        _active_interface,
    ])))
    if not candidates:
        return {"error": "no active interface resolved yet", "candidates": []}

    results = []
    for iface in candidates:
        try:
            raw = await _collector.get_top_talkers_raw(iface)
        except Exception as exc:
            raw = {"interface_used": iface, "error": str(exc)}
        results.append(raw)

    return {
        "active_bsd_interface": _active_interface,
        "active_opn_interface": _active_opn_interface,
        "working_cache":        _top_talker_working_iface,
        "probes":               results,
    }


# ---------------------------------------------------------------------------
# Static files (frontend) — must be last
# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
