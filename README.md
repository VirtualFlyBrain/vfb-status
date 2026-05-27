# vfb-status

A self-contained uptime tracker for [Virtual Fly Brain](https://virtualflybrain.org) public services. Probes every endpoint listed in `config/services.yml` hourly and on demand, and renders a single status page.

Built after a `data.virtualflybrain.org` outage to give us one URL to glance at when a user reports something is broken.

## What it checks

The full list lives in [`config/services.yml`](config/services.yml). It covers the main site, `v2`/`v3` viewers, the `data` and `nas0` file servers, PDB / KB Neo4j browsers, Owlery, the owl cache, SOLR, VFBquery, the VFB3 MCP server, IIP3D, R/NBLAST, and every hosted CATMAID instance (`fafb`, `l1em`, `vnc1`, `fanc`, `l3vnc`, `abd1-5`, `iav-tnt`, `iav-robo`, `igor`). Add or remove entries by editing the YAML and restarting the container.

Each check is a plain HTTPS GET. A service is considered up when the status code is in its `expect_status` list and (where configured) the response body contains the `expect` substring. Defaults follow the rancher-compose `health_check.request_line` entries in [VirtualFlyBrain/RancherServices](https://github.com/VirtualFlyBrain/RancherServices) where one exists.

## Run it

```bash
docker compose up -d
```

Then open `http://localhost:8000/`. Hit **Refresh now** to force an immediate probe of every endpoint; otherwise checks run automatically every hour.

The page auto-refreshes every 60 s. Endpoints:

- `GET /` — status page with per-service status strip and 24 h / 7 d / 30 d uptime %
- `GET /api/status` — JSON of the latest results
- `GET /api/uptime` — per-service uptime % over 24 h / 7 d / 30 d
- `GET /api/history?service=<name>&limit=200` — recent raw history rows for one service
- `GET /healthz` — liveness for Rancher / Docker
- `POST /refresh` — force an immediate re-probe of every service

## History storage

Every probe writes a row to a SQLite database (`HISTORY_DB`, default `/data/history.db`). The schema is one append-only `history` table indexed by `(service, ts)`. On a mounted volume this gives you long-term history across container restarts and image rebuilds — open it with any SQLite client for ad-hoc queries.

```sql
SELECT service, status, http_status, latency_ms, error, checked_at
FROM history
WHERE service = 'data.virtualflybrain.org (file server)'
  AND ts >= strftime('%s', 'now', '-7 days')
ORDER BY ts DESC;
```

Rows older than `HISTORY_RETENTION_DAYS` are pruned on startup and once a day. Set `HISTORY_RETENTION_DAYS=0` to keep forever.

The status strip on the page renders the most recent `HISTORY_BUCKETS` buckets (default 72 hours = 3 days), oldest left. Reduction rule per bucket: any `down` → bucket is red; otherwise any `up` → bucket is green; otherwise grey (no data).

## Configuration

Environment variables (set in `docker-compose.yml`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `CHECK_INTERVAL_SECONDS` | `3600` | Seconds between scheduled probe runs. |
| `CONFIG_PATH` | `config/services.yml` | Path to the service list inside the container. |
| `STATE_FILE` | `/data/state.json` | Optional. Persists last-known results across restarts. Unset = in-memory only. |
| `HISTORY_DB` | `/data/history.db` | SQLite database for long-term probe history. Mount the parent directory as a volume to retain history across container rebuilds. Empty = history disabled, strip hidden, uptime % shown as "—". |
| `HISTORY_RETENTION_DAYS` | `365` | Rows older than this are pruned on startup and once a day. `0` = keep forever. |
| `HISTORY_BUCKETS` | `72` | Number of buckets in the status strip on the page. |
| `HISTORY_BUCKET_SECONDS` | `3600` | One bucket = this many seconds. Default 1 h × 72 = 3 days of history visible inline. |
| `RANCHER_SERVERS` | _(empty)_ | Comma- or whitespace-separated list of short hostnames. Each `$NAME` is probed at `http://$NAME.$RANCHER_DOMAIN:$RANCHER_PORT`. Synthesised into a separate "Rancher servers" group on the page. Mirrors the existing VFB shell check that hits `:5050` on each node. |
| `RANCHER_DOMAIN` | `inf.ed.ac.uk` | Domain suffix appended to each `RANCHER_SERVERS` name. |
| `RANCHER_PORT` | `5050` | Port to probe on each rancher server. |
| `RANCHER_TIMEOUT` | `5` | Per-request timeout in seconds for rancher-server probes. |

**Rancher checks only work from inside the Edinburgh network.** Port 5050 on `inf.ed.ac.uk` is dropped at the Informatics firewall — externally only 80, 443 and 7687 are reachable. If you run this container off-campus, leave `RANCHER_SERVERS` empty.

The `config/` directory is mounted read-only into the container, so you can edit `services.yml` on the host and `docker compose restart vfb-status` to pick up changes.

## Service-list schema

```yaml
defaults:
  timeout: 10                    # seconds, per request
  method: GET
  expect_status: [200, 301, 302] # acceptable HTTP status codes

groups:
  - name: Core user-facing
    services:
      - name: Friendly label shown in the UI
        url: https://example.virtualflybrain.org/
        timeout: 20                # overrides default
        expect_status: [200]       # overrides default
        expect: "Some response body substring"   # optional content check
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Tests are deliberately not included — the value here is the live probes against production. To dry-run the config, run the app locally; the first hourly tick fires immediately on startup.

## Deploying

Same pattern as the rest of the VFB stack: build the image, push to Docker Hub (`virtualflybrain/vfb-status:latest`), and point a rancher-compose service at it. The container exposes port 8000 and `GET /healthz` for the LB.

## Licence

MIT. See [`LICENCE`](LICENCE).
