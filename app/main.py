import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from collector import OPNsenseCollector
from database import (
    get_bandwidth_history,
    get_daily_totals,
    get_gateway_history,
    get_stored_gateways,
    get_stored_interfaces,
    init_db,
    insert_bandwidth,
    insert_gateway,
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


def _make_collector() -> OPNsenseCollector:
    return OPNsenseCollector(
        host=_config["host"],
        api_key=_config["api_key"],
        api_secret=_config["api_secret"],
        verify_ssl=_config["verify_ssl"],
    )


def _resolve_interface_from_gateways(gateways: list[dict]) -> str:
    """
    Given the live gateway list, return the BSD interface name to use for
    bandwidth monitoring.

    Priority:
      1. Explicit override in config['interface']
      2. bsd_if extracted from the configured gateway's 'if' API field
      3. bsd_if from the first gateway that has one
    """
    # Explicit override always wins
    manual = _config.get("interface", "").strip()
    if manual:
        return manual

    target = _config.get("gateway", "").strip()

    # Try the configured gateway first
    if target:
        for gw in gateways:
            if gw["name"] == target and gw.get("bsd_if"):
                return gw["bsd_if"]

    # Fall back to any gateway that has a bsd_if
    for gw in gateways:
        if gw.get("bsd_if"):
            return gw["bsd_if"]

    return ""


async def _poll_once(col: OPNsenseCollector):
    global _status, _active_interface

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

        # ── 2. Resolve BSD interface for bandwidth ──────────────────────────
        resolved = _resolve_interface_from_gateways(gateways)
        if resolved and resolved != _active_interface:
            logger.info(
                "Active interface: %r → %r (auto-resolved from gateway)",
                _active_interface, resolved,
            )
        _active_interface = resolved

        # ── 3. Bandwidth ────────────────────────────────────────────────────
        if _active_interface:
            bw = await col.get_bandwidth(_active_interface)
            if bw is not None:
                insert_bandwidth(_active_interface, bw[0], bw[1])
                logger.info(
                    "BW %s: ↓ %.2f Mbps  ↑ %.2f Mbps",
                    _active_interface, bw[0] / 1e6, bw[1] / 1e6,
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
    global _poll_task, _collector, _active_interface
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()
    _collector = None
    # Reset resolved interface so it gets re-resolved after config change
    _active_interface = _config.get("interface", "")
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


# ---------------------------------------------------------------------------
# Static files (frontend) — must be last
# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
