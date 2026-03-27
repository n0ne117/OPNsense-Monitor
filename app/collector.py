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

    async def get_gateway_status(self) -> list[dict]:
        """
        Returns list of gateway dicts including:
          name, address, status_text, rtt_ms, rttd_ms, loss_pct,
          bsd_if  (BSD kernel interface name, e.g. 'ix1')
          opn_if  (OPNsense logical name, e.g. 'opt1')
        """
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
            # OPNsense returns 'if' = BSD kernel interface (e.g. ix1, em0, vtnet0)
            # and 'interface' = OPNsense logical name (e.g. wan, opt1)
            bsd_if = (
                item.get("if")
                or item.get("if_name")
                or item.get("ifname")
            )
            opn_if = (
                item.get("interface")
                or item.get("interface_name")
            )
            result.append({
                "name":        item.get("name", "unknown"),
                "address":     item.get("address", ""),
                "status_text": item.get("status", "unknown"),
                "rtt_ms":      rtt,
                "rttd_ms":     rttd,
                "loss_pct":    loss,
                "bsd_if":      bsd_if,
                "opn_if":      opn_if,
            })
        return result

    async def get_raw_interface_stats(self) -> dict:
        """Return the raw /api/diagnostics/interface/getInterfaceStatistics response."""
        async with self._client() as c:
            r = await c.get(f"{self.base_url}/api/diagnostics/interface/getInterfaceStatistics")
            r.raise_for_status()
            return r.json()

    async def get_available_interfaces(self) -> list[str]:
        try:
            data = await self.get_raw_interface_stats()
            stats = data.get("statistics") or {}
            return list(stats.keys())
        except Exception as exc:
            logger.warning("Could not fetch interfaces: %s", exc)
            return []

    @staticmethod
    def _find_stats_key(stats: dict, interface: str) -> Optional[str]:
        """
        OPNsense returns composite stat keys such as:
          '[Port2_WAN] (ix1) / aa:bb:cc:dd:ee:ff'   ← link-layer entry (has byte counters)
          '[Port2_WAN] (ix1) / 203.0.113.10'         ← address entry   (bytes may be 0)
        Given a BSD interface name like 'ix1', return the best matching key.
        Prefers the link-layer (MAC) entry because that always carries the
        cumulative interface byte totals in BSD netstat output.
        """
        # Exact match (plain BSD name — older OPNsense versions)
        if interface in stats:
            return interface

        mac_re = re.compile(r'(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}')
        ip_re  = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')

        # Collect all keys that mention this interface name
        matches = [
            k for k in stats
            if f"({interface})" in k or f"[{interface}]" in k
        ]
        if not matches:
            matches = [k for k in stats if interface in k]

        if not matches:
            return None

        # Prefer link-layer (MAC) entry — it always has the real byte counters
        mac_matches = [k for k in matches if mac_re.search(k)]
        if mac_matches:
            return mac_matches[0]

        # Fall back to IP-address entry
        ip_matches = [k for k in matches if ip_re.search(k)]
        return ip_matches[0] if ip_matches else matches[0]

    async def get_bandwidth(self, interface: str) -> Optional[tuple[float, float]]:
        """
        Returns (bps_in, bps_out) calculated from cumulative byte counters,
        or None on the first sample (no previous reading to diff against).
        """
        try:
            data = await self.get_raw_interface_stats()
            stats = data.get("statistics") or {}

            key = self._find_stats_key(stats, interface)
            if key is None:
                available = list(stats.keys())
                logger.warning(
                    "Interface %r not found in statistics. Available: %s",
                    interface, available,
                )
                return None

            if key != interface:
                logger.debug("Interface %r matched stats key %r", interface, key)

            iface = stats[key]

            # Log field names on first call so users can debug mismatches
            if interface not in self._prev:
                logger.info(
                    "Interface %r (key=%r) fields: %s", interface, key, list(iface.keys())
                )

            # Try every known field-name variant for byte counters
            def _int(v) -> int:
                try:
                    return int(v) if v not in (None, "", "0") else 0
                except (ValueError, TypeError):
                    return 0

            bytes_in = (
                _int(iface.get("received-bytes"))
                or _int(iface.get("ibytes"))
                or _int(iface.get("bytes received"))
                or _int(iface.get("bytes_received"))
                or _int(iface.get("rx_bytes"))
                or 0
            )
            bytes_out = (
                _int(iface.get("sent-bytes"))
                or _int(iface.get("obytes"))
                or _int(iface.get("bytes transmitted"))
                or _int(iface.get("bytes_transmitted"))
                or _int(iface.get("tx_bytes"))
                or 0
            )

            now = time.time()
            prev = self._prev.get(interface)
            self._prev[interface] = (now, float(bytes_in), float(bytes_out))

            if prev is None:
                logger.info(
                    "Interface %r: first sample captured (in=%d, out=%d bytes). "
                    "Rate will be available next poll.",
                    interface, bytes_in, bytes_out,
                )
                return None  # Need a second sample to calculate rate

            prev_ts, prev_in, prev_out = prev
            dt = now - prev_ts
            if dt <= 0:
                return None

            bps_in  = (bytes_in  - prev_in)  * 8 / dt
            bps_out = (bytes_out - prev_out) * 8 / dt

            # Ignore negative values (counter wrap or reset — discard this sample)
            if bps_in < 0 or bps_out < 0:
                logger.warning(
                    "Interface %r: negative delta (counter reset?), discarding sample.",
                    interface,
                )
                return None

            return bps_in, bps_out

        except Exception as exc:
            logger.error("Bandwidth collection failed: %s", exc)
            return None
