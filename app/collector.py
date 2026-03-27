import re
import time
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


def _parse_ms(value) -> Optional[float]:
    """Parse '1.23 ms', '1.23ms', or bare float to milliseconds."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "~", "-", "N/A", "n/a"):
        return None
    m = re.search(r"([\d.]+)", s)
    if m:
        return float(m.group(1))
    return None


def _parse_pct(value) -> Optional[float]:
    """Parse '0.0 %', '0.0%', or bare float to percentage."""
    if value is None:
        return None
    s = str(value).strip()
    if s in ("", "~", "-", "N/A", "n/a"):
        return None
    m = re.search(r"([\d.]+)", s)
    if m:
        return float(m.group(1))
    return None


class OPNsenseCollector:
    def __init__(self, host: str, api_key: str, api_secret: str, verify_ssl: bool = False):
        self.base_url = host.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret
        self.verify_ssl = verify_ssl
        # {interface: (timestamp, bytes_in, bytes_out)}
        self._prev: dict[str, tuple[float, float, float]] = {}

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            auth=(self.api_key, self.api_secret),
            verify=self.verify_ssl,
            timeout=15,
        )

    async def get_available_interfaces(self) -> list[str]:
        try:
            async with self._client() as c:
                r = await c.get(f"{self.base_url}/api/diagnostics/interface/getInterfaceStatistics")
                r.raise_for_status()
                data = r.json()
            stats = data.get("statistics") or {}
            return list(stats.keys())
        except Exception as exc:
            logger.warning("Could not fetch interfaces: %s", exc)
            return []

    async def get_bandwidth(self, interface: str) -> Optional[tuple[float, float]]:
        """
        Returns (bps_in, bps_out) calculated from cumulative byte counters,
        or None on the first sample (no previous reading to diff against).
        """
        try:
            async with self._client() as c:
                r = await c.get(f"{self.base_url}/api/diagnostics/interface/getInterfaceStatistics")
                r.raise_for_status()
                data = r.json()

            stats = data.get("statistics") or {}
            iface = stats.get(interface)
            if iface is None:
                available = list(stats.keys())
                logger.warning(
                    "Interface %r not found. Available: %s", interface, available
                )
                return None

            # OPNsense returns BSD-style counter names
            bytes_in = int(
                iface.get("ibytes")
                or iface.get("bytes received")
                or iface.get("bytes_received")
                or 0
            )
            bytes_out = int(
                iface.get("obytes")
                or iface.get("bytes transmitted")
                or iface.get("bytes_transmitted")
                or 0
            )

            now = time.time()
            prev = self._prev.get(interface)
            self._prev[interface] = (now, float(bytes_in), float(bytes_out))

            if prev is None:
                return None  # First sample — need a delta

            prev_ts, prev_in, prev_out = prev
            dt = now - prev_ts
            if dt <= 0:
                return None

            bps_in  = (bytes_in  - prev_in)  * 8 / dt
            bps_out = (bytes_out - prev_out) * 8 / dt

            # Ignore negative values (counter wrap or reset)
            if bps_in < 0 or bps_out < 0:
                return None

            return bps_in, bps_out

        except Exception as exc:
            logger.error("Bandwidth collection failed: %s", exc)
            return None

    async def get_gateway_status(self) -> list[dict]:
        """
        Returns list of:
          {name, address, status_text, rtt_ms, rttd_ms, loss_pct}
        """
        try:
            async with self._client() as c:
                r = await c.get(f"{self.base_url}/api/routes/gateway/status")
                r.raise_for_status()
                data = r.json()

            items = data.get("items") or []
            result = []
            for item in items:
                rtt  = _parse_ms(item.get("delay")  or item.get("rtt"))
                rttd = _parse_ms(item.get("stddev") or item.get("rttd") or item.get("delay_stddev"))
                loss = _parse_pct(item.get("loss"))
                result.append({
                    "name":        item.get("name", "unknown"),
                    "address":     item.get("address", ""),
                    "status_text": item.get("status", "unknown"),
                    "rtt_ms":      rtt,
                    "rttd_ms":     rttd,
                    "loss_pct":    loss,
                })
            return result

        except Exception as exc:
            logger.error("Gateway status collection failed: %s", exc)
            return []
