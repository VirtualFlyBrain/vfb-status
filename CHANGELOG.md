# Changelog

All notable changes to vfb-status are recorded here. The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.0] — 2026-05-29

Per-container probing for `app_services` (specifically VFBquery, which now runs `scale: 4`). Same Rancher v1 API pattern we built for `cache_services` in v0.6.0.

### Added

- Optional `rancher:` block on `app_services` entries. When set, every probe enumerates the service's running containers via the Rancher v1 API and probes each one's `/status` directly at its `primaryIpAddress:container_port`. Falls back to the LB-fronted single probe if the API isn't configured.
- New `container` column on `app_history` (auto-migrated on existing DBs). NULL for LB rows, populated with the Rancher instance name for per-container rows.
- New `idx_app_service_container_ts` index, created in `_migrate()` (same forward-compatible pattern as cache_history).
- VFBquery entry in `config/services.yml` now has a `rancher:` block pointing at service id `1s345`, container port `8080`. Currently scale 4.

### Changed

- App card now shows: cluster-summed stats at the top (active, waiting, served, hit rate, etc.), a per-container breakdown table, and three cluster sparklines (active, waiting, Δ total_served).
- `/api/app` response now returns `summary` (cluster aggregate) and `containers[]` (per-container detail). Existing clients reading the old top-level fields will need to switch to `summary` or per-container rows.

## [0.7.3] — 2026-05-29

Fix the every-request 500 storm caused by `sqlite3.OperationalError: disk I/O error`. Root cause: Rancher's default "start-then-stop" rolling upgrade ran two vfb-status containers concurrently against the same SQLite file on the mounted volume, corrupting the WAL.

### Changed

- Default `journal_mode` is now **DELETE** instead of WAL. SQLite docs explicitly require DELETE on NFS, and Rancher persistent volumes are commonly NFS-backed. Set `HISTORY_JOURNAL_MODE=WAL` to opt back in on local-SSD deploys.
- Added `PRAGMA busy_timeout=2000` so transient contention waits 2 s instead of erroring immediately.

### Added

- Exclusive `fcntl.flock` on a sentinel file (`<HISTORY_DB>.lock`) at startup. If a second vfb-status instance starts against the same volume (botched upgrade, accidental scale > 1), it logs a clear error and refuses to write — better to lose history briefly than corrupt the DB. On filesystems that don't support `flock` the app warns and proceeds.

### Fixed

- Every History read method (`uptime_pct`, `buckets`, `cache_series`, `cache_latest`, `app_series`, `neo4j_series`, `recent`, `services`) now catches `sqlite3.Error` and returns safe defaults (`(None, 0)`, `[]`, `["unknown"] * n`, etc.) so a DB-layer issue degrades the page gracefully instead of producing HTTP 500.
- New `_safe_service_row()` wrapper around `_service_row()` in the index route catches any remaining exception, logs it, and renders an "unknown" placeholder with the error attached — the page renders even if the history layer is completely broken.

## [0.7.2] — 2026-05-29

### Changed

- Bake VFB's public read-only Neo4j credentials (`neo4j` / `vfb`) into `config/services.yml` so the deployed status container doesn't need `NEO4J_PASSWORD` set. The `password_env` field still wins when the env var is set, so password rotation and non-VFB deployments keep working unchanged. Documented inline that the embedded credential is the published read-only pair from the VFBconnect docs.
- Page distinguishes auth failure from DB-down: when the Neo4j probe error contains `401` / `Unauthorized` / `auth`, the card shows an amber `auth?` pill instead of red `down`. Hover tooltip points at the `password:` / `NEO4J_PASSWORD` config.

## [0.7.1] — 2026-05-29

### Fixed

- Startup crash on upgrade from v0.5.0 or earlier: `sqlite3.OperationalError: no such column: container`. The cache_history container-column index was declared in the main SCHEMA, which executescript runs *before* `_migrate()` ALTERs the column in. Moved the index creation into `_migrate()` so it runs after the ALTER. Fresh installs are unaffected.

## [0.7.0] — 2026-05-29

Rancher cluster overview, host augmentation, DNS-ingress flag.

### Added

- New "Rancher cluster" section on the page showing: hosts active vs total, LB coverage, public-DNS ingress count, and active services healthy vs total in the configured stacks. A table lists every service where `currentScale < scale` or `healthState != healthy`.
- New env vars:
  - `RANCHER_PROJECT_URL` (default `https://herd.virtualflybrain.org/v2-beta/projects/1a5`)
  - `RANCHER_STACKS` (default `vfb-services-live`) — comma- or whitespace-separated. Inactive services are filtered out automatically.
  - `RANCHER_DNS_HOSTNAME` (default `virtualflybrain.org`) — dynamic DNS ingress detection.
  - `RANCHER_DNS_HOSTS` — optional static override.
- Each row in the existing Rancher servers group gains three badges:
  - **`rancher: active/inactive`** — host's `state` from the Rancher API (orthogonal to the `:5050` HTTP check).
  - **`LB`** — the active `vfb-loadbalancer-main` is running on this host.
  - **`DNS`** — the host's `agentIpAddress` is in the public DNS A-records. Re-evaluated every probe.
- New tables: `rancher_host_history` and `rancher_service_history`. Same retention as the rest.
- New endpoint: `GET /api/cluster`.

### Notes

- Per-container restart counts (suggestion #3 in the v0.6.0 conversation) were dropped. Rancher v1 returns `null` for `restartCount` on every instance in this project, so building UI for it would show nothing useful.

## [0.6.0] — 2026-05-29

Per-container cache probes via the Rancher v1 API. The previous LB-fronted `/status` probe only ever saw one random backend's view, which was misleading for any service with `scale > 1` (VFB3-Cache and IIP3D-Cache both currently run 2 containers).

### Added

- New optional `rancher:` block on `cache_services` entries. When set, every probe enumerates the service's running container instances via the Rancher v1 API and probes each container's `/status` directly at its `primaryIpAddress`.
- New env vars `RANCHER_API_KEY` and `RANCHER_API_SECRET` for Basic-auth to the Rancher API. Environment-scoped read-only is enough. Never commit them.
- New `container` column on `cache_history` (auto-migrated on existing DBs). NULL for LB-level rows, populated with the container name for per-container rows.
- Per-container breakdown table on each cache card, plus cluster-summed totals at the top. Sparklines now aggregate across containers per timestamp.
- `/api/cache` now returns both a per-service `summary` (cluster sum, container count, ok count) and a `containers[]` list with per-container metrics.

### Fixed

- Mistakenly listed `query-cache-server` (1s322) as an owl_cache. It's a Solr cache and doesn't speak the owl_cache `/status` JSON shape. Removed.

### Notes

- The Rancher API path requires the deployed container to be on the Rancher overlay network so that `10.42.x.x` instance IPs are reachable. If they're not, the probe logs a warning and falls back transparently to the LB-fronted single probe.

## [0.5.0] — 2026-05-29

Catch the "load failed, DB empty, /browser/ still 200" failure mode on the VFB Neo4j endpoints.

### Added

- New `neo4j_services:` block in `config/services.yml`. Each entry triggers a two-stage probe:
  1. `SHOW DATABASES` against `/db/system/tx/commit` — verifies the target database's `currentStatus == "online"`. Surfaces Neo4j's own start-up error verbatim when offline.
  2. `MATCH (n) RETURN count(n)` against `/db/{db}/tx/commit` — verifies the node count is at least `min_nodes`.
- New `neo4j_history` SQLite table with one row per probe (`ok`, `db_status`, `db_error`, `node_count`, `latency_ms`).
- New "Neo4j databases — content checks" section on the page. Per-DB card with status pill, current node count, configured `min_nodes`, latency, surfaced errors, and a node-count sparkline so DB rebuilds are visible.
- New endpoints: `GET /api/neo4j` (latest snapshot per DB) and `GET /api/neo4j/history?service=<name>` (down-sampled time series).
- New env var: `NEO4J_PASSWORD` for the Cypher API. Per-service override via `password_env:`. The YAML never contains the password.

### Why

The existing `/browser/` HTTP checks for PDB and KB only proved the Neo4j HTTP listener was alive. When a database load failed, the listener still responded 200 on `/browser/` but the DB was empty or refused to start. The two-stage probe catches this — verified live against the currently-offline KB Neo4j, which reports `currentStatus: "offline"` with error `"Unable to start \`DatabaseId{...[neo4j]}\`."`.

## [0.4.1] — 2026-05-29 (folded into 0.5.0)

### Changed

- `HISTORY_DB` is now **on by default** at `/data/history.db`. Set `HISTORY_DB=""` (explicit empty string) to disable. This fixes the recurring footgun where the deployed status page silently ran without history because the env var wasn't propagated through the rancher service definition.

### Fixed

- A non-writable `HISTORY_DB` path no longer kills the app — history is disabled cleanly and a clear ERROR is logged explaining what to mount and where.

## [0.4.0] — 2026-05-27

Add application-service `/status` tracking (VFBquery shape).

### Added

- New `app_services:` block in `config/services.yml` for application services that expose their own JSON `/status`. Each entry declares a `shape:` so the parser knows what fields to extract.
- `shape: vfbquery` parses VFBquery's `/status`: `status`, `workers`, `max_concurrent`, `max_queue_depth`, `active`, `waiting`, `total_served`, `cache_size`, `cache_hits`, `coalesced_total`, `coalesced_in_flight`, `scanner_probes_blocked`, `solr_cache.enabled`.
- New `app_history` SQLite table — one row per probe, indexed by `(service, ts)`. Same retention rules as `history` and `cache_history`.
- New "Application services — /status" section on the page. Per-service card with twelve live counters (incl. utilisation % for `active` vs `max_concurrent` and `waiting` vs `max_queue_depth`) and three inline SVG sparklines: active requests (concurrency), queued requests (waiting), and Δ `total_served` per check (request rate).
- `GET /api/app` — latest snapshot per app service. `GET /api/app/history?service=<name>` — down-sampled time series.

### Fixed

- Cache services config: scheme corrected per LB routing. `owl.virtualflybrain.org` and `iip3d.virtualflybrain.org` are HTTP-only at the rancher LB; `v3-cached.virtualflybrain.org` is HTTPS-only. Earlier sandbox probe results that suggested `iip3d` `/status` was unroutable were a probe-side artefact — the live deployment confirms all three caches probe successfully.

## [0.3.0] — 2026-05-27

Track cache `/status` data and visualise load over time.

### Added

- New `cache_services:` block in `config/services.yml`. Each entry probes a caching service's `/status` JSON (the shape exposed by `virtualflybrain/owl_cache` ≥1.1.22) on the regular probe schedule.
- New `cache_history` SQLite table. Each probe persists `cache_total`, `cache_hit`, `cache_miss`, the connection breakdown (`active`/`reading`/`writing`/`waiting`), and the parsed `nginx`/`upstream` health flags.
- New "Caching services — load over time" section on the page. Per-cache card with the latest counters and two inline SVG sparklines: active-connections and Δ cache_total per check (request-rate proxy). Pure CSS + inline SVG, no chart-library dependency.
- Caches whose `/status` is unreachable (e.g. older owl_cache:1.1.20 images that predate the endpoint) still record an error row, which makes the upgrade trail visible in history.
- `GET /api/cache` — latest snapshot per cache service. `GET /api/cache/history?service=<name>` — down-sampled time series.

## [0.2.0] — 2026-05-27

Long-term uptime history + a visual status strip on the page.

### Added

- SQLite history storage. Every probe writes a row to `HISTORY_DB` (default `/data/history.db`). One append-only `history` table indexed by `(service, ts)`; survives container restarts when the data directory is mounted as a volume.
- Daily prune job, retention controlled by `HISTORY_RETENTION_DAYS` (default 365). Set to `0` to keep forever.
- Per-service status strip on the page — `HISTORY_BUCKETS` × `HISTORY_BUCKET_SECONDS` (default 72 × 1 h = 3 days visible inline). Bucket reduction rule: any `down` → red, else any `up` → green, else grey (no data).
- 24 h / 7 d / 30 d uptime percentages displayed alongside the strip. `unknown` rows are excluded from the denominator.
- `GET /api/uptime` — per-service uptime % over 24 h / 7 d / 30 d (and the underlying sample counts).
- `GET /api/history?service=<name>&limit=N` — recent raw history rows for a single service.

### Changed

- Status page layout switched from a wide table to a grid card per service. Mobile layout collapses the strip below the name and meta.

## [0.1.0] — 2026-05-27

Initial release. Self-contained Docker uptime tracker for public-facing Virtual Fly Brain services. Built after the 2026-05-27 `data.virtualflybrain.org` outage.

### Added

- FastAPI + APScheduler app that probes every endpoint in `config/services.yml` hourly and on demand.
- 32 public endpoints across six groups: Core user-facing, Data + file servers, APIs, Image + NBLAST, CATMAID hosted instances, Auxiliary. `data.virtualflybrain.org` is included explicitly.
- Per-service `verify_tls`, `expect_status`, `expect` (body substring), `timeout`, and `method` knobs.
- Rancher node checks driven by the `RANCHER_SERVERS` env var — comma- or whitespace-separated short hostnames, each probed at `http://$NAME.$RANCHER_DOMAIN:$RANCHER_PORT`. Defaults match the existing VFB shell health check (`inf.ed.ac.uk:5050`). Six VFB nodes wired in by default: `buttermilk parsley sourcream chive mayo dill`.
- Status page at `/` with a **Refresh now** button that forces a synchronous re-probe via `POST /refresh`. JSON at `/api/status`; liveness at `/healthz`.
- Optional state persistence to a mounted volume (`STATE_FILE`).
- Docker Hub publishing workflow (`.github/workflows/docker-publish.yml`) using `docker/metadata-action@v5`, the org-level `DOCKER_HUB_USER` / `DOCKER_HUB_PASSWORD` secrets, multi-arch builds (`linux/amd64`, `linux/arm64`), and the same tag scheme as `vfb3-mcp` and `owl_cache`.

### Known limitations

- Rancher node checks only resolve from inside the Edinburgh network — port 5050 on `inf.ed.ac.uk` is dropped by the Informatics firewall externally. Set `RANCHER_SERVERS=""` when deploying off-campus.
- Four subdomains (`nas0`, `iip3d`, `nblast`, `abd1-5.catmaid`) ship with `verify_tls: false` because the production cert SAN doesn't cover them. The servers are up; the cert provisioning is a separate problem.
- Kubernetes nodes are intentionally not handled here — separate checks planned for a later release.

[0.8.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.8.0
[0.7.3]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.7.3
[0.7.2]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.7.2
[0.7.1]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.7.1
[0.7.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.7.0
[0.6.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.6.0
[0.5.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.5.0
[0.4.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.4.0
[0.3.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.3.0
[0.2.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.2.0
[0.1.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.1.0
