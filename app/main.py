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
from pydantic import BaseModel, Field

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
    set_poll_interval,
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
    "interface":         os.environ.get("OPNSENSE_INTERFACE",        ""),
    "gateway":           os.environ.get("OPNSENSE_GATEWAY",          "WAN_DHCP"),
    # Logical OPNsense interface name for traffic/top (e.g. "lan", "opt1").
    # Leave empty for auto-discovery.
    "top_talker_interface": os.environ.get("OPNSENSE_TOP_IF", ""),
    "verify_ssl":   os.environ.get("OPNSENSE_VERIFY_SSL", "false").lower() == "true",
    "poll_interval": int(os.environ.get("POLL_INTERVAL", "60")),
    # Feature toggles — disable to reduce load on the firewall
    "enable_gateway_monitor": True,
    "enable_top_talkers":     True,
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

    errors: list[str] = []
    monitor_gw = _config.get("enable_gateway_monitor", True)

    # ── 1. Gateway status (RTT / RTTd / loss + interface resolution) ──
    # Skipped when the monitor is off and the interface is already known,
    # so disabling it actually spares the firewall this call.
    if monitor_gw or not _active_interface:
        try:
            gateways = await col.get_gateway_status()
        except Exception as exc:
            logger.error("Gateway status failed: %s", exc)
            errors.append(f"gateway status: {exc}")
            gateways = []

        if monitor_gw:
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
        # Keep the previous names if the gateway call failed.
        resolved, resolved_opn = _resolve_interfaces_from_gateways(gateways)
        if resolved:
            if resolved != _active_interface:
                logger.info(
                    "Active interface: %r → %r (bsd) / %r (opn)",
                    _active_interface, resolved, resolved_opn,
                )
            _active_interface = resolved
            _active_opn_interface = resolved_opn

    # ── 3. Bandwidth ────────────────────────────────────────────────────────
    if _active_interface:
        try:
            bw = await col.get_bandwidth(_active_interface)
            if bw is not None:
                bps_in, bps_out, bytes_in, bytes_out = bw
                insert_bandwidth(_active_interface, bps_in, bps_out)
                update_usage_summary(bytes_in, bytes_out)
                logger.info(
                    "BW %s: ↓ %.2f Mbps  ↑ %.2f Mbps",
                    _active_interface, bps_in / 1e6, bps_out / 1e6,
                )
        except Exception as exc:
            logger.error("Bandwidth poll failed: %s", exc)
            errors.append(f"bandwidth: {exc}")
    else:
        logger.warning(
            "No interface configured and could not auto-resolve from gateways. "
            "Set 'gateway' in Settings (e.g. Port2_WAN) or specify 'interface' explicitly."
        )

    _status = {
        "ok":        not errors,
        "error":     "; ".join(errors) or None,
        "last_poll": time.time(),
    }


async def _poll_loop():
    global _collector, _status
    _collector = _make_collector()
    while True:
        try:
            await _poll_once(_collector)
        except Exception as exc:
            logger.error("Poll failed: %s", exc)
            _status = {"ok": False, "error": str(exc), "last_poll": time.time()}
        await asyncio.sleep(_poll_interval())


def _poll_interval() -> int:
    """Configured poll interval, clamped so a bad config.json can't hammer the firewall."""
    try:
        return min(max(int(_config.get("poll_interval", 60)), 10), 3600)
    except (TypeError, ValueError):
        return 60


def _restart_poll():
    global _poll_task, _collector, _active_interface, _active_opn_interface, _top_talker_working_iface
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()
    _collector = None
    _active_interface = _config.get("interface", "")
    _active_opn_interface = ""
    _top_talker_working_iface = ""
    set_poll_interval(_poll_interval())
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
    host:                    Optional[str]  = None
    api_key:                 Optional[str]  = None
    api_secret:              Optional[str]  = None
    interface:               Optional[str]  = None
    gateway:                 Optional[str]  = None
    top_talker_interface:    Optional[str]  = None
    verify_ssl:              Optional[bool] = None
    poll_interval:           Optional[int]  = Field(None, ge=10, le=3600)
    enable_gateway_monitor:  Optional[bool] = None
    enable_top_talkers:      Optional[bool] = None


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
    iface = _active_interface or _config.get("interface", "")
    return {
        # Computed from the active interface's bandwidth rows, merged with
        # the persistent counter-based totals (see database._merge_with_stored)
        "hourly":  compute_hourly_from_bandwidth(iface),
        "daily":   compute_daily_from_bandwidth(iface),
        "monthly": compute_monthly_from_daily(iface),
        "yearly":  compute_yearly_from_monthly(iface),
        "current_keys": {
            "hourly":  now.strftime("%Y-%m-%d %H"),
            "daily":   now.strftime("%Y-%m-%d"),
            "monthly": now.strftime("%Y-%m"),
            "yearly":  now.strftime("%Y"),
        },
    }


def _is_private_ip(ip: str) -> bool:
    """Returns True if ip is an RFC1918 private address."""
    try:
        p = list(map(int, ip.split(".")))
        if len(p) != 4:
            return False
        return (
            p[0] == 10
            or (p[0] == 172 and 16 <= p[1] <= 31)
            or (p[0] == 192 and p[1] == 168)
        )
    except (ValueError, AttributeError):
        return False


async def _discover_top_talker_interface() -> str:
    """
    Discovers the correct OPNsense logical interface name for traffic/top by:
    1. Calling /api/diagnostics/traffic/interface for the list of logical keys
    2. Probing each key with traffic/top (up to 3 records each)
    3. Preferring the key whose records include private (RFC1918) IPs — these
       are the internal hosts whose bandwidth usage we actually want to show

    Caches the result in _top_talker_working_iface.
    """
    global _top_talker_working_iface

    if _collector is None:
        return ""

    try:
        keys = await _collector.get_traffic_interface_names()
    except Exception as exc:
        logger.warning("traffic/interface list failed: %s", exc)
        return ""

    if not keys:
        logger.warning("traffic/interface returned no interface keys")
        return ""

    logger.info("Top-talker interface discovery: probing keys %s", keys)

    best_key = ""
    best_score = -1  # score: 2=has private IPs, 1=has any records, 0=none

    for key in keys:
        try:
            records = await _collector.get_top_talkers(key, limit=3)
        except Exception as exc:
            logger.debug("Probe %r failed: %s", key, exc)
            continue

        if not records:
            logger.debug("Probe %r: no records", key)
            continue

        has_private = any(_is_private_ip(r["address"]) for r in records)
        score = 2 if has_private else 1
        logger.info("Probe %r: %d records, private_ips=%s", key, len(records), has_private)

        if score > best_score:
            best_score = score
            best_key = key
            if score == 2:
                break  # private-IP interface found — stop searching

    if best_key:
        logger.info("Top talkers: auto-discovered interface %r (score=%d)", best_key, best_score)
        _top_talker_working_iface = best_key
    else:
        logger.warning("Top talkers: discovery found no working interface key")

    return best_key


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

    if not _config.get("enable_top_talkers", True):
        return {"talkers": [], "top_ip": None, "second_ip": None, "disabled": True}

    if _collector is None or not _active_interface:
        return {"talkers": [], "top_ip": None, "second_ip": None}

    # Build ordered candidate list:
    #  1. Cached working interface (fast path)
    #  2. User-configured override
    #  3. OPN logical name from gateway status (often empty)
    #  4. BSD name (may work on some OPNsense builds)
    candidates: list[str] = []
    for name in [
        _top_talker_working_iface,
        _config.get("top_talker_interface", "").strip(),
        _active_opn_interface,
        _active_interface,
    ]:
        if name and name not in candidates:
            candidates.append(name)

    records: list[dict] = []
    iface_used = candidates[0] if candidates else _active_interface

    for candidate in candidates:
        try:
            result = await _collector.get_top_talkers(candidate, limit=10)
            if result:
                records = result
                iface_used = candidate
                if _top_talker_working_iface != candidate:
                    logger.info("Top talkers: using interface %r", candidate)
                    _top_talker_working_iface = candidate
                break
        except Exception as exc:
            logger.warning("Top talkers fetch failed (iface=%r): %s", candidate, exc)

    # Nothing worked with known candidates — run full discovery
    if not records:
        discovered = await _discover_top_talker_interface()
        if discovered:
            try:
                records = await _collector.get_top_talkers(discovered, limit=10)
                iface_used = discovered
            except Exception as exc:
                logger.warning("Top talkers discovery fetch failed: %s", exc)

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


@app.get("/api/debug/traffic-interfaces")
async def api_debug_traffic_interfaces():
    """
    Returns the raw /api/diagnostics/traffic/interface response from OPNsense.
    The keys in the 'interfaces' dict are the logical interface names you need
    to pass to the traffic/top endpoint (e.g. 'wan', 'opt1', 'lan').
    """
    if _collector is None:
        raise HTTPException(503, "Collector not ready")
    try:
        async with _collector._client() as c:
            r = await c.get(f"{_collector.base_url}/api/diagnostics/traffic/interface")
            data = r.json()
        return {
            "http_status":           r.status_code,
            "interface_keys":        list((data.get("interfaces") or {}).keys()),
            "raw":                   data,
            "active_bsd_interface":  _active_interface,
            "active_opn_interface":  _active_opn_interface,
            "working_cache":         _top_talker_working_iface,
            "configured_top_if":     _config.get("top_talker_interface", ""),
        }
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
