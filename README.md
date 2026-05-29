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
- `GET /api/cache` — latest snapshot of every cache service (hit/miss counts, connections, hit rate)
- `GET /api/cache/history?service=<name>&since_seconds=86400&max_points=200` — down-sampled cache time series
- `GET /api/app` — latest snapshot of every application service (`shape: vfbquery` etc.)
- `GET /api/app/history?service=<name>&since_seconds=86400&max_points=200` — down-sampled app time series
- `GET /api/neo4j` — latest snapshot per Neo4j database, including `db_status`, `node_count`, and any Neo4j start-up error
- `GET /api/neo4j/history?service=<name>&since_seconds=86400&max_points=200` — down-sampled Neo4j time series
- `GET /api/cluster` — Rancher cluster overview: hosts (with LB-coverage and DNS-ingress flags) and active services in the configured stacks
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

## Cache services

Caches running [virtualflybrain/owl_cache](https://github.com/VirtualFlyBrain/owl_cache) ≥1.1.22 expose `/status` with JSON describing the nginx cache hit/miss counters and connection breakdown. List them under `cache_services:` in `config/services.yml`:

```yaml
cache_services:
  - name: VFB3 cache (v3-cached.virtualflybrain.org)
    status_url: https://v3-cached.virtualflybrain.org/status
    fronts: vfbquery.virtualflybrain.org
  - name: IIP3D cache (iip3d.virtualflybrain.org)
    status_url: https://iip3d.virtualflybrain.org/status
    verify_tls: false
```

Each entry is probed on the same schedule as the regular endpoints; the parsed metrics go into the `cache_history` table. The page renders a card per cache with the latest snapshot plus two inline sparklines: active connections (load proxy) and Δ cache_total per check (request-rate proxy). Cache endpoints whose `/status` is unreachable still record an error row, which makes it easy to see when older `1.1.20` images get upgraded.

### Per-container probes via Rancher v1 API

A bare LB-fronted probe of `https://<host>/status` only ever shows one random backend's view, which is misleading when a service runs more than one container. Add an optional `rancher:` block to each `cache_services` entry to enumerate the service's running instances via the Rancher v1 API and probe each container's `/status` directly:

```yaml
cache_services:
  - name: VFB3 cache (v3-cached.virtualflybrain.org)
    status_url: https://v3-cached.virtualflybrain.org/status   # fallback if API fails
    rancher:
      service_url: https://herd.virtualflybrain.org/v2-beta/projects/1a5/services/1s337
      container_port: 80
      container_path: /status
```

Required env on the container: `RANCHER_API_KEY` and `RANCHER_API_SECRET` (environment-scoped read-only key from the Rancher UI). Per-container rows land in `cache_history` with the `container` column populated; the LB fallback rows have `container = NULL`. The page card aggregates across containers for the summary stats and sparklines, and shows a per-container breakdown table below.

**Networking requirement:** the deployed status container must be on the Rancher overlay network so that `10.42.x.x` `primaryIpAddress` values are reachable. If they're not, the probe logs a warning and falls back to the LB-fronted single probe automatically.

## Rancher cluster overview

When `RANCHER_API_KEY` / `RANCHER_API_SECRET` are set, the page renders a "Rancher cluster" section above the regular groups showing:

- **Host coverage:** N of M hosts active, how many run the load balancer, how many are pointed at by public DNS
- **Service health:** N of M `state=active` services healthy in the configured stacks. Inactive services are filtered out (they're meant to be stopped, so they're not signal). A table lists every service where `currentScale < scale` or `healthState != healthy`.

Each row in the existing Rancher servers group also gains three badges sourced from this overview:

- **`rancher: active`** / **`rancher: inactive`** — host's `state` from the Rancher API (orthogonal to the `:5050` HTTP check, which only tells you the host's HTTP listener is up).
- **`LB`** — the active `vfb-loadbalancer-main` is running on this host.
- **`DNS`** — the host's `agentIpAddress` is in the A-records for `RANCHER_DNS_HOSTNAME` (default `virtualflybrain.org`). Dynamic — re-evaluated every probe via a fresh `getaddrinfo` lookup, so DNS changes are picked up automatically.

Configure via env vars:

| Variable | Default | Purpose |
| --- | --- | --- |
| `RANCHER_PROJECT_URL` | `https://herd.virtualflybrain.org/v2-beta/projects/1a5` | Project endpoint on the Rancher v1 API. |
| `RANCHER_STACKS` | `vfb-services-live` | Comma- or whitespace-separated stack names to include in the service overview. Leave tight to avoid dev-stack noise. |
| `RANCHER_DNS_HOSTNAME` | `virtualflybrain.org` | Hostname whose A-records mark a host as DNS-ingress. |
| `RANCHER_DNS_HOSTS` | _(empty)_ | Static override of the DNS-ingress flag (comma-separated short host names). Leave empty for dynamic DNS detection. |

## Application services (`/status`)

Application services with their own JSON `/status` go under `app_services:`. Each entry declares its `shape:`, which selects the parser and renderer. Currently supported:

- **`shape: vfbquery`** — extracts `workers`, `max_concurrent`, `max_queue_depth`, `active`, `waiting`, `total_served`, `cache_size`, `cache_hits`, `coalesced_total`, `coalesced_in_flight`, `scanner_probes_blocked`, `solr_cache.enabled`.

```yaml
app_services:
  - name: VFBquery (vfbquery.virtualflybrain.org)
    status_url: https://vfbquery.virtualflybrain.org/status
    shape: vfbquery
```

Metrics persist in the `app_history` table. The page card shows the live counters plus inline sparklines for active requests, queued requests, and Δ `total_served` per check (request rate). Adding a new shape is a small change: define how to parse the fields you care about and how to render them.

## Neo4j databases (`neo4j_services:`)

The `/browser/` endpoint of a Neo4j server returns 200 even when its database is empty or refusing to start. The `neo4j_services:` block runs a two-stage Cypher probe to catch the silent-failure case:

1. `SHOW DATABASES` against `/db/system/tx/commit` — verifies `currentStatus == "online"` for the target database. If the database is offline, Neo4j's own start-up error is captured (e.g. `"Unable to start DatabaseId{...[neo4j]}."`) and shown on the page.
2. `MATCH (n) RETURN count(n)` against `/db/{db}/tx/commit` — verifies the node count is at least `min_nodes`.

Credentials come from environment variables — set `NEO4J_PASSWORD` on the container (or per-service `password_env:` if PDB and KB diverge). The YAML never contains the password.

```yaml
neo4j_services:
  - name: PDB Neo4j (pdb.virtualflybrain.org)
    base_url: https://pdb.virtualflybrain.org
    db: neo4j
    user: neo4j
    password_env: NEO4J_PASSWORD
    min_nodes: 1000000      # alert if the load truncated the graph
```

The page card shows the current node count, the configured `min_nodes`, Neo4j's reported `db_status`, and a sparkline of node count over time (so big-bang DB rebuilds are visible). Results persist in the `neo4j_history` table.

## Configuration

Environment variables (set in `docker-compose.yml`):

| Variable | Default | Purpose |
| --- | --- | --- |
| `CHECK_INTERVAL_SECONDS` | `3600` | Seconds between scheduled probe runs. |
| `CONFIG_PATH` | `config/services.yml` | Path to the service list inside the container. |
| `STATE_FILE` | `/data/state.json` | Optional. Persists last-known results across restarts. Unset = in-memory only. |
| `HISTORY_DB` | `/data/history.db` | SQLite database for long-term probe history. **On by default** — set to `""` to disable. Mount `/data` (or whatever path you choose) as a persistent volume so history survives container rebuilds. If the path isn't writable, history is disabled automatically and a clear error is logged. |
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
