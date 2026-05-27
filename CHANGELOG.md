# Changelog

All notable changes to vfb-status are recorded here. The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.3.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.3.0
[0.2.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.2.0
[0.1.0]: https://github.com/VirtualFlyBrain/vfb-status/releases/tag/v0.1.0
