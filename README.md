# OPNsense Monitor

A lightweight, self-hosted dashboard that polls the OPNsense REST API and displays real-time network metrics for the last 24 hours.

![Dark dashboard with bandwidth, RTT, and packet loss charts]

## Features

- **Bandwidth** — live download / upload graph, auto-scaled (bps → Kbps → Mbps → Gbps)
- **RTT & Jitter (RTTd)** — latency and standard deviation per gateway
- **Packet loss** — percentage and estimated packet count (dual-axis chart)
- **24-hour rolling storage** — SQLite, no external database required
- **Auto-detects interface** — specify a gateway name (e.g. `WAN_DHCP`) and the physical interface is resolved automatically from the OPNsense API
- **In-browser settings** — change host, credentials, gateway, and poll interval without restarting the container
- **Per-gateway tabs** — monitor multiple gateways side-by-side

---

## Requirements

- Docker and Docker Compose
- An OPNsense firewall with API access enabled
- A user/key with at least read access to:
  - `Routes: Gateway status`
  - `Diagnostics: Interface statistics`

---

## Quick start

### 1. Create an API key in OPNsense

1. Log in to OPNsense → **System → Access → Users**
2. Edit (or create) a user and scroll to **API keys**
3. Click **+** to generate a key/secret pair — save both, the secret is shown only once
4. Ensure the user has the privileges listed above

### 2. Clone and configure

```bash
git clone https://github.com/n0ne117/OPNsense-Monitor.git
cd OPNsense-Monitor
cp .env.example .env
```

Edit `.env`:

```env
OPNSENSE_HOST=https://192.168.1.1        # OPNsense address (include port if non-standard)
OPNSENSE_API_KEY=your_api_key_here
OPNSENSE_API_SECRET=your_api_secret_here
OPNSENSE_GATEWAY=WAN_DHCP                # Gateway name as shown in OPNsense
OPNSENSE_INTERFACE=                      # Leave empty — auto-resolved from gateway
OPNSENSE_VERIFY_SSL=false                # Set true only if OPNsense has a valid cert
POLL_INTERVAL=60                         # Seconds between API polls
```

> **Finding your gateway name:** In OPNsense go to **System → Gateways → Configuration** — use the value in the **Name** column exactly as shown (e.g. `WAN_DHCP`, `Port2_WAN`).

### 3. Start the container

```bash
docker compose up -d
```

Open **http://your-host:5051** in a browser.

The first data point appears after two poll cycles (the first poll seeds the byte counters, the second calculates the rate).

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `OPNSENSE_HOST` | `https://192.168.1.1` | OPNsense base URL, include custom port if needed |
| `OPNSENSE_API_KEY` | — | API key from OPNsense user management |
| `OPNSENSE_API_SECRET` | — | API secret (paired with the key above) |
| `OPNSENSE_GATEWAY` | `WAN_DHCP` | Gateway name to monitor; also used to auto-resolve the WAN interface |
| `OPNSENSE_INTERFACE` | *(empty)* | Override the BSD interface name (e.g. `ix1`). Leave empty for auto-detection |
| `OPNSENSE_VERIFY_SSL` | `false` | Verify the OPNsense TLS certificate (`true`/`false`) |
| `POLL_INTERVAL` | `60` | Polling interval in seconds (minimum 10) |

All of the above can also be changed at runtime via the **⚙ Settings** panel in the UI without rebuilding the container. Settings are persisted to `/data/config.json` inside the container.

---

## Data storage

Metrics are stored in a SQLite database at `./data/monitor.db` (mounted as a volume). Records older than 24 hours are pruned automatically on every write. No external database is needed.

---

## Troubleshooting

**Bandwidth shows 0 or no data**

Open the Settings panel and click **List Gateways** to confirm the gateway name and that a `bsd_if` is returned. If needed, copy the BSD interface name shown there into the **Interface Override** field.

You can also inspect the raw OPNsense statistics response at:
```
http://your-host:5051/api/debug/iface-stats
```

**RTT / loss not appearing**

Check that the gateway name in Settings matches exactly what OPNsense shows (case-sensitive). The **List Gateways** button shows all available names.

**SSL certificate errors**

If OPNsense uses a self-signed certificate (the default), keep `OPNSENSE_VERIFY_SSL=false`. Set it to `true` only when using a certificate signed by a trusted CA.

**Container can't reach OPNsense**

Ensure the container's network can reach the OPNsense host. If running on the same machine as OPNsense, use the LAN IP rather than `localhost`.

---

## Project structure

```
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
└── app/
    ├── main.py          # FastAPI app, REST API, background scheduler
    ├── collector.py     # OPNsense API client
    ├── database.py      # SQLite helpers (24 h rolling window)
    └── static/
        └── index.html   # Single-page frontend (Chart.js)
```
