import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from collector import OPNsenseCollector
from database import (
    get_bandwidth_history,
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
    "interface":    os.environ.get("OPNSENSE_INTERFACE",  "em0"),
    "gateway":      os.environ.get("OPNSENSE_GATEWAY",    ""),
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
# Background polling
# ---------------------------------------------------------------------------
_collector: Optional[OPNsenseCollector] = None
_poll_task: Optional[asyncio.Task] = None
_status: dict = {"ok": None, "error": None, "last_poll": None}


def _make_collector() -> OPNsenseCollector:
    return OPNsenseCollector(
        host=_config["host"],
        api_key=_config["api_key"],
        api_secret=_config["api_secret"],
        verify_ssl=_config["verify_ssl"],
    )


async def _poll_once(col: OPNsenseCollector):
    global _status
    try:
        # Bandwidth
        bw = await col.get_bandwidth(_config["interface"])
        if bw is not None:
            insert_bandwidth(_config["interface"], bw[0], bw[1])
            logger.info(
                "BW %s: ↓ %.2f Mbps  ↑ %.2f Mbps",
                _config["interface"], bw[0] / 1e6, bw[1] / 1e6,
            )

        # Gateways
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
                        "GW %s: rtt=%.2f ms  rttd=%s ms  loss=%.1f%%",
                        gw["name"], gw["rtt_ms"],
                        f"{gw['rttd_ms']:.2f}" if gw.get("rttd_ms") is not None else "n/a",
                        gw.get("loss_pct") or 0,
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
    global _poll_task, _collector
    if _poll_task and not _poll_task.done():
        _poll_task.cancel()
    _collector = None
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
    """Return config with secret masked."""
    c = _config.copy()
    if c.get("api_secret"):
        c["api_secret"] = "••••••••"
    return c


@app.get("/api/status")
async def api_status():
    return {"poll_status": _status, "config": _safe_config()}


@app.get("/api/config")
async def api_get_config():
    return _safe_config()


@app.post("/api/config")
async def api_set_config(update: ConfigUpdate):
    global _config
    patch = update.model_dump(exclude_none=True)
    # Never overwrite the stored secret with the mask placeholder
    if patch.get("api_secret") == "••••••••":
        patch.pop("api_secret")
    _config.update(patch)
    _save_config(_config)
    _restart_poll()
    return {"ok": True}


@app.get("/api/data")
async def api_data(hours: float = 24):
    since = time.time() - hours * 3600
    bandwidth = get_bandwidth_history(_config["interface"], since)

    target_gw = _config.get("gateway", "")
    raw_gw = get_gateway_history(target_gw or None, since)

    # Group by gateway name
    gw_grouped: dict[str, list] = {}
    for row in raw_gw:
        name = row["gateway"]
        gw_grouped.setdefault(name, []).append(row)

    return {
        "bandwidth": bandwidth,
        "gateways":  gw_grouped,
        "meta": {
            "interface":     _config["interface"],
            "target_gateway": target_gw,
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


# ---------------------------------------------------------------------------
# Static files (frontend) — must be last
# ---------------------------------------------------------------------------
app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")
