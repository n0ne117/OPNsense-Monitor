# OPNsense Monitor

A lightweight, self-hosted dashboard that polls the OPNsense REST API and shows real-time WAN bandwidth, gateway quality, top talkers and long-term data usage.

## Features

**Live dashboard**
- **Bandwidth** — download / upload graph, auto-scaled (bps → Kbps → Mbps → Gbps)
- **RTT & Jitter (RTTd)** — latency and its standard deviation per gateway
- **Packet loss** — percentage and estimated lost packets (dual-axis chart)
- **Alert banner** — *warning* at RTT ≥ 65 ms or loss ≥ 1 %, *critical* at RTT ≥ 100 ms or loss ≥ 4 %; also warns when no fresh gateway data arrives (gateway down or polling failing)
- **Top talkers** — the 5 hosts currently using the most bandwidth, with reverse-DNS hostnames; 👑 marks the top user and ② the runner-up, ranked by their average over the last minute
- **Today's totals** — downloaded / uploaded bytes since midnight
- **Time ranges** — last 30 min (default), 1 h, 3 h, 6 h, 12 h or 24 h
- **Synced tooltips** — hovering one chart shows the values at the same time on all charts
- **Per-gateway tabs** — switch between gateways when more than one is monitored

**Usage summary**
- Hourly, daily, monthly and yearly download / upload / total tables
- Totals are accumulated from the interface byte counters and kept long-term (see [Data storage](#data-storage)); outages are not counted as traffic

**Setup & operation**
- **Auto-detects interfaces** — give it a gateway name (e.g. `WAN_DHCP`) and the WAN interface is resolved automatically; the top-talkers interface is discovered automatically as well
- **In-browser settings** — change host, credentials, gateway, interfaces and poll interval without restarting the container
- **Feature toggles** — disable the gateway monitor or top talkers to reduce load on the firewall
- **Multi-arch Docker image** — `linux/amd64` and `linux/arm64` (e.g. Raspberry Pi)

---

## Requirements

- Docker and Docker Compose
- An OPNsense firewall with API access enabled
- An API key/secret for a user with read access to:
  - `Routes: Gateway status` — RTT / loss and interface auto-detection
  - `Diagnostics: Interface statistics` — bandwidth
  - the traffic diagnostics API (`/api/diagnostics/traffic/*`) — top talkers (optional)

---

## Quick start

### 1. Create an API key in OPNsense

1. Log in to OPNsense → **System → Access → Users**
2. Edit (or create) a user and scroll to **API keys**
3. Click **+** to generate a key/secret pair — save both, the secret is shown only once
4. Ensure the user has the privileges listed above

### 2. Create a `docker-compose.yml`

The prebuilt image is published to the GitHub Container Registry:

```yaml
services:
  opnsense-monitor:
    image: ghcr.io/n0ne117/opnsense-monitor:latest
    container_name: opnsense-monitor
    restart: unless-stopped
    ports:
      - "5051:8080"
    volumes:
      - ./data:/data
    environment:
      - OPNSENSE_HOST=https://192.168.1.1
      - OPNSENSE_API_KEY=your_api_key_here
      - OPNSENSE_API_SECRET=your_api_secret_here
      - OPNSENSE_GATEWAY=WAN_DHCP
      - OPNSENSE_VERIFY_SSL=false
      - POLL_INTERVAL=60
```

Alternatively, clone the repo and use the included `docker-compose.yml`, which reads its values from a `.env` file:

```bash
git clone https://github.com/n0ne117/OPNsense-Monitor.git
cd OPNsense-Monitor
cp .env.example .env   # then edit .env
```

> **Finding your gateway name:** In OPNsense go to **System → Gateways → Configuration** — use the value in the **Name** column exactly as shown (e.g. `WAN_DHCP`, `Port2_WAN`).

### 3. Start the container

```bash
docker compose up -d
```

Open **http://your-host:5051** in a browser.

The first data point appears after two poll cycles (the first poll seeds the byte counters, the second calculates the rate).

### Updating

```bash
docker compose pull && docker compose up -d
```

To build the image yourself instead, replace `image:` with `build: .` in `docker-compose.yml` and run `docker compose up -d --build`.

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `OPNSENSE_HOST` | `https://192.168.1.1` | OPNsense base URL, include custom port if needed |
| `OPNSENSE_API_KEY` | — | API key from OPNsense user management |
| `OPNSENSE_API_SECRET` | — | API secret (paired with the key above) |
| `OPNSENSE_GATEWAY` | `WAN_DHCP` | Gateway to monitor; also used to auto-resolve the WAN interface. Empty = all gateways |
| `OPNSENSE_INTERFACE` | *(empty)* | Override the BSD interface name (e.g. `ix1`). Leave empty for auto-detection |
| `OPNSENSE_TOP_IF` | *(empty)* | Override the OPNsense interface used for top talkers (e.g. `lan`, `opt1`). Leave empty for auto-discovery |
| `OPNSENSE_VERIFY_SSL` | `false` | Verify the OPNsense TLS certificate (`true`/`false`) |
| `POLL_INTERVAL` | `60` | Polling interval in seconds (10–3600) |

All of the above, plus the **Gateway Monitor** and **Top Talkers** toggles, can be changed at runtime in the **⚙ Settings** panel without restarting the container. Settings saved there are persisted to `/data/config.json` and take precedence over the environment variables.

---

## Data storage

Everything lives in a SQLite database at `./data/monitor.db` (mounted volume) — no external database needed.

| Data | Retention |
|---|---|
| Bandwidth and gateway samples (charts) | 24 hours |
| Hourly usage totals | 24 hours |
| Daily usage totals | 365 days |
| Monthly usage totals | 10 years |
| Yearly usage totals | forever |

---

## Security

The dashboard and its API have **no authentication** — anyone who can reach the port can view the data and change the settings (including the OPNsense host the API key is sent to). Run it on a trusted LAN only and don't expose it to the internet.

---

## Troubleshooting

**Bandwidth shows 0 or no data**

Open the Settings panel and click **List Gateways** to confirm the gateway name and that a `bsd_if` is returned. If needed, copy the BSD interface name shown there into the **Interface Override** field. If you saved settings in the UI earlier, values in `/data/config.json` override the environment variables.

You can also inspect the raw OPNsense statistics response at:
```
http://your-host:5051/api/debug/iface-stats
```

**RTT / loss not appearing**

Check that the gateway name in Settings matches exactly what OPNsense shows (case-sensitive). The **List Gateways** button shows all available names.

**Top talkers not appearing**

The strip only shows when OPNsense returns traffic records. These debug endpoints show which interface names were tried and what OPNsense returned:
```
http://your-host:5051/api/debug/traffic-interfaces
http://your-host:5051/api/debug/top-talkers-raw
```
If auto-discovery picks the wrong interface, set **Top Talkers Interface** in Settings (e.g. `lan`).

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
├── .github/workflows/
│   └── docker-publish.yml   # Builds & publishes the image to GHCR
└── app/
    ├── main.py          # FastAPI app, REST API, background poller, top talkers
    ├── collector.py     # OPNsense API client
    ├── database.py      # SQLite storage and usage aggregation
    └── static/
        └── index.html   # Single-page frontend (Chart.js)
```
