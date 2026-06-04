"""VFB service uptime tracker.

Single-file FastAPI app. Loads endpoint definitions from config/services.yml,
probes each one on a schedule (hourly by default) and on /refresh, and renders
a status page with a colored history strip per service.

Design notes
------------
- One container, no external DB process. The history database is SQLite on a
  mounted volume — point HISTORY_DB at /data/history.db for long-term storage.
- Concurrent probing: per-request timeout * fan-out via asyncio.gather.
- Probing is a GET, follows redirects, and considers a service "up" when the
  status code is in `expect_status` AND (if `expect` is set) the response body
  contains the substring.
- Every probe writes a row to the `history` table. The status page summarises
  per-service uptime % over 24 h / 7 d / 30 d windows and renders a strip of
  fixed-width buckets (default 72 hours of history).
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vfb-status")

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config/services.yml"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "")) if os.environ.get("STATE_FILE") else None
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", str(60 * 60)))
USER_AGENT = "vfb-status/1.0 (+https://github.com/VirtualFlyBrain/vfb-status)"

# History storage on the mounted volume. On by default at /data/history.db
# so a bare `docker run` with a `-v` mount picks up history without extra
# configuration. Explicitly set HISTORY_DB="" to disable.
_HISTORY_DB_RAW = os.environ.get("HISTORY_DB", "/data/history.db")
HISTORY_DB = Path(_HISTORY_DB_RAW) if _HISTORY_DB_RAW else None
# Retention: trim rows older than this on startup and once a day. 0 = keep forever.
HISTORY_RETENTION_DAYS = int(os.environ.get("HISTORY_RETENTION_DAYS", "365"))
# Status-strip width on the page (one block per bucket, default 1 h per block).
HISTORY_BUCKETS = int(os.environ.get("HISTORY_BUCKETS", "72"))
HISTORY_BUCKET_SECONDS = int(os.environ.get("HISTORY_BUCKET_SECONDS", "3600"))

# Rancher server health endpoints — comma- or whitespace-separated list of
# short hostnames. Each name N is probed at http://N.inf.ed.ac.uk:5050.
# Only reachable from inside the Edinburgh network (port 5050 is dropped
# by the inf.ed.ac.uk firewall externally). Empty = none.
RANCHER_SERVERS = [
    s.strip()
    for s in os.environ.get("RANCHER_SERVERS", "").replace(",", " ").split()
    if s.strip()
]
RANCHER_DOMAIN = os.environ.get("RANCHER_DOMAIN", "inf.ed.ac.uk")
RANCHER_PORT = int(os.environ.get("RANCHER_PORT", "5050"))
RANCHER_TIMEOUT = float(os.environ.get("RANCHER_TIMEOUT", "20"))

# Rancher cluster overview. Empty RANCHER_PROJECT_URL disables the cluster
# section entirely. RANCHER_STACKS limits the service-overview to the listed
# stacks (default vfb-services-live — production). Public-DNS-ingress hosts
# can be overridden via RANCHER_DNS_HOSTS; otherwise we resolve
# RANCHER_DNS_HOSTNAME and match the IPs to host.agentIpAddress.
RANCHER_PROJECT_URL = os.environ.get(
    "RANCHER_PROJECT_URL",
    "https://herd.virtualflybrain.org/v2-beta/projects/1a5",
)
RANCHER_STACKS = [
    s.strip()
    for s in os.environ.get("RANCHER_STACKS", "vfb-services-live").replace(",", " ").split()
    if s.strip()
]
RANCHER_DNS_HOSTNAME = os.environ.get("RANCHER_DNS_HOSTNAME", "virtualflybrain.org")
RANCHER_DNS_HOSTS = [
    s.strip()
    for s in os.environ.get("RANCHER_DNS_HOSTS", "").replace(",", " ").split()
    if s.strip()
]
RANCHER_CLUSTER_TIMEOUT = float(os.environ.get("RANCHER_CLUSTER_TIMEOUT", "20"))


# ----- domain types -----------------------------------------------------------


@dataclass
class ServiceSpec:
    name: str
    url: str
    group: str
    method: str = "GET"
    timeout: float = 10.0
    expect_status: list[int] = field(default_factory=lambda: [200, 301, 302])
    expect: str | None = None
    verify_tls: bool = True  # set false where the cert legitimately doesn't cover the subdomain


@dataclass
class CheckResult:
    name: str
    group: str
    url: str
    status: str  # "up" | "down" | "unknown"
    http_status: int | None = None
    latency_ms: int | None = None
    error: str | None = None
    checked_at: str | None = None  # ISO timestamp


@dataclass
class RancherEnumeration:
    """Optional Rancher v1 API source that enumerates a service's running
    container instances. When set on a CacheServiceSpec, each container's
    /status is probed directly via its primaryIpAddress so we don't get a
    random single backend's view from the LB.

    auth comes from env vars (default RANCHER_API_KEY / RANCHER_API_SECRET).
    """

    service_url: str            # e.g. https://herd.virtualflybrain.org/v2-beta/projects/1a5/services/1s336
    container_port: int = 80    # port on the container that exposes /status
    container_path: str = "/status"
    container_scheme: str = "http"
    api_key_env: str = "RANCHER_API_KEY"
    api_secret_env: str = "RANCHER_API_SECRET"
    timeout: float = 8.0


@dataclass
class CacheServiceSpec:
    """A caching service whose /status endpoint reports cache counters + connections.

    Shape: virtualflybrain/owl_cache >=1.1.22 — JSON with health{}, upstream{},
    cache{total,hit,miss}, connections{active,reading,writing,waiting}.
    """

    name: str
    status_url: str
    fronts: str | None = None  # human-readable label of the upstream
    timeout: float = 8.0
    verify_tls: bool = True
    rancher: RancherEnumeration | None = None


@dataclass
class Neo4jServiceSpec:
    """A Neo4j endpoint we want to verify is online + has data.

    Probe sequence:
    1. SHOW DATABASES against {base_url}/db/system/tx/commit — verify the
       target database's currentStatus is 'online'.
    2. MATCH (n) RETURN count(n) AS n against {base_url}/db/{db}/tx/commit
       — verify count >= min_nodes.

    Credentials come from env vars to keep the YAML free of secrets.
    """

    name: str
    base_url: str
    db: str = "neo4j"
    user: str = "neo4j"
    password_env: str = "NEO4J_PASSWORD"
    password: str = ""           # used when password_env is empty / unset
    min_nodes: int = 1
    timeout: float = 15.0
    verify_tls: bool = True
    fronts: str | None = None
    rancher: "RancherEnumeration | None" = None


@dataclass
class Neo4jCheck:
    name: str
    base_url: str
    db: str
    ok: bool
    checked_at: str
    container: str | None = None  # set when probed per-container via Rancher API
    latency_ms: int | None = None
    db_status: str | None = None  # "online" | "offline" | other Neo4j states
    db_error: str | None = None   # from SHOW DATABASES
    node_count: int | None = None
    error: str | None = None


@dataclass
class RancherHost:
    id: str
    hostname: str          # full hostname, e.g. "buttermilk.inf.ed.ac.uk"
    short_name: str        # e.g. "buttermilk"
    state: str             # "active" | "inactive" | ...
    agent_state: str | None
    agent_ip: str | None
    is_dns_ingress: bool = False
    has_lb: bool = False


@dataclass
class RancherService:
    id: str
    name: str
    stack: str
    type: str              # e.g. "service", "loadBalancerService"
    state: str             # "active" | "inactive" | ...
    health_state: str | None
    scale: int | None
    current_scale: int | None

    @property
    def is_degraded(self) -> bool:
        if self.state != "active":
            return False  # explicitly stopped — not signal
        if self.health_state and self.health_state != "healthy":
            return True
        if (
            self.scale is not None
            and self.current_scale is not None
            and self.current_scale < self.scale
        ):
            return True
        return False


@dataclass
class RancherClusterResult:
    fetched_at: str
    ok: bool
    error: str | None = None
    hosts: list[RancherHost] = field(default_factory=list)
    services: list[RancherService] = field(default_factory=list)


@dataclass
class AppServiceSpec:
    """An application service exposing its own JSON /status.

    `shape` selects the parser/renderer. Currently supports:
    - "vfbquery": workers/active/waiting/total_served/cache_hits/coalesced/...
    """

    name: str
    status_url: str
    shape: str = "vfbquery"
    fronts: str | None = None
    timeout: float = 8.0
    verify_tls: bool = True
    rancher: "RancherEnumeration | None" = None


@dataclass
class AppCheck:
    """Generic app /status result. Stores everything that's relevant for the
    known shapes plus the raw JSON document.
    """

    name: str
    status_url: str
    shape: str
    ok: bool
    checked_at: str
    error: str | None = None
    container: str | None = None   # set when probed per-container via Rancher API

    # vfbquery shape
    q_status: str | None = None
    q_version: str | None = None
    q_workers: int | None = None
    q_max_concurrent: int | None = None
    q_max_queue_depth: int | None = None
    q_active: int | None = None
    q_waiting: int | None = None
    q_total_served: int | None = None
    q_cache_size: int | None = None
    q_cache_hits: int | None = None
    q_coalesced_total: int | None = None
    q_coalesced_in_flight: int | None = None
    q_scanner_blocked: int | None = None
    q_solr_cache_enabled: bool | None = None

    raw: str | None = None

    @property
    def queue_pct(self) -> float | None:
        if self.q_max_queue_depth and self.q_max_queue_depth > 0 and self.q_waiting is not None:
            return 100.0 * self.q_waiting / self.q_max_queue_depth
        return None

    @property
    def concurrency_pct(self) -> float | None:
        if self.q_max_concurrent and self.q_max_concurrent > 0 and self.q_active is not None:
            return 100.0 * self.q_active / self.q_max_concurrent
        return None


@dataclass
class CacheCheck:
    name: str
    status_url: str
    ok: bool
    checked_at: str
    error: str | None = None
    container: str | None = None   # set when probed via Rancher API per-container
    nginx_healthy: bool | None = None
    upstream_healthy: bool | None = None
    upstream_host: str | None = None
    upstream_port: int | None = None
    cache_total: int | None = None
    cache_hit: int | None = None
    cache_miss: int | None = None
    conn_active: int | None = None
    conn_reading: int | None = None
    conn_writing: int | None = None
    conn_waiting: int | None = None
    raw: str | None = None

    @property
    def hit_rate(self) -> float | None:
        if self.cache_total and self.cache_total > 0 and self.cache_hit is not None:
            return 100.0 * self.cache_hit / self.cache_total
        return None


# ----- config loading ---------------------------------------------------------


def load_services(path: Path) -> list[ServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    defaults = raw.get("defaults", {}) or {}
    services: list[ServiceSpec] = []
    for group in raw.get("groups", []):
        group_name = group["name"]
        for svc in group.get("services", []):
            services.append(
                ServiceSpec(
                    name=svc["name"],
                    url=svc["url"],
                    group=group_name,
                    method=svc.get("method", defaults.get("method", "GET")),
                    timeout=float(svc.get("timeout", defaults.get("timeout", 10))),
                    expect_status=list(
                        svc.get("expect_status", defaults.get("expect_status", [200, 301, 302]))
                    ),
                    expect=svc.get("expect"),
                    verify_tls=bool(svc.get("verify_tls", defaults.get("verify_tls", True))),
                )
            )
    log.info("loaded %d services from %s", len(services), path)
    return services


def load_neo4j_services(path: Path) -> list[Neo4jServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    out: list[Neo4jServiceSpec] = []
    for svc in raw.get("neo4j_services", []) or []:
        ranch = None
        rblk = svc.get("rancher")
        if rblk:
            ranch = RancherEnumeration(
                service_url=rblk["service_url"].rstrip("/"),
                container_port=int(rblk.get("container_port", 7474)),
                container_path=rblk.get("container_path", ""),  # unused for neo4j
                container_scheme=rblk.get("container_scheme", "http"),
                api_key_env=rblk.get("api_key_env", "RANCHER_API_KEY"),
                api_secret_env=rblk.get("api_secret_env", "RANCHER_API_SECRET"),
                timeout=float(rblk.get("timeout", 15.0)),
            )
        out.append(
            Neo4jServiceSpec(
                name=svc["name"],
                base_url=svc["base_url"].rstrip("/"),
                db=svc.get("db", "neo4j"),
                user=svc.get("user", "neo4j"),
                password_env=svc.get("password_env", "NEO4J_PASSWORD"),
                password=str(svc.get("password", "") or ""),
                min_nodes=int(svc.get("min_nodes", 1)),
                timeout=float(svc.get("timeout", 15.0)),
                verify_tls=bool(svc.get("verify_tls", True)),
                fronts=svc.get("fronts"),
                rancher=ranch,
            )
        )
    if out:
        log.info("loaded %d neo4j services from %s", len(out), path)
    return out


def load_app_services(path: Path) -> list[AppServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    out: list[AppServiceSpec] = []
    for svc in raw.get("app_services", []) or []:
        ranch = None
        rblk = svc.get("rancher")
        if rblk:
            ranch = RancherEnumeration(
                service_url=rblk["service_url"].rstrip("/"),
                container_port=int(rblk.get("container_port", 80)),
                container_path=rblk.get("container_path", "/status"),
                container_scheme=rblk.get("container_scheme", "http"),
                api_key_env=rblk.get("api_key_env", "RANCHER_API_KEY"),
                api_secret_env=rblk.get("api_secret_env", "RANCHER_API_SECRET"),
                timeout=float(rblk.get("timeout", 8.0)),
            )
        out.append(
            AppServiceSpec(
                name=svc["name"],
                status_url=svc["status_url"],
                shape=svc.get("shape", "vfbquery"),
                fronts=svc.get("fronts"),
                timeout=float(svc.get("timeout", 8.0)),
                verify_tls=bool(svc.get("verify_tls", True)),
                rancher=ranch,
            )
        )
    if out:
        log.info("loaded %d app services from %s", len(out), path)
    return out


def load_cache_services(path: Path) -> list[CacheServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    out: list[CacheServiceSpec] = []
    for svc in raw.get("cache_services", []) or []:
        ranch = None
        rblk = svc.get("rancher")
        if rblk:
            ranch = RancherEnumeration(
                service_url=rblk["service_url"].rstrip("/"),
                container_port=int(rblk.get("container_port", 80)),
                container_path=rblk.get("container_path", "/status"),
                container_scheme=rblk.get("container_scheme", "http"),
                api_key_env=rblk.get("api_key_env", "RANCHER_API_KEY"),
                api_secret_env=rblk.get("api_secret_env", "RANCHER_API_SECRET"),
                timeout=float(rblk.get("timeout", 8.0)),
            )
        out.append(
            CacheServiceSpec(
                name=svc["name"],
                status_url=svc["status_url"],
                fronts=svc.get("fronts"),
                timeout=float(svc.get("timeout", 8.0)),
                verify_tls=bool(svc.get("verify_tls", True)),
                rancher=ranch,
            )
        )
    if out:
        log.info("loaded %d cache services from %s", len(out), path)
    return out


def rancher_server_specs() -> list[ServiceSpec]:
    """Synthesise checks for each name in RANCHER_SERVERS.

    The endpoint convention matches the existing VFB shell health check:
    `curl http://$SERVER.inf.ed.ac.uk:5050` — 200 = up.
    """
    specs: list[ServiceSpec] = []
    for name in RANCHER_SERVERS:
        host = f"{name}.{RANCHER_DOMAIN}"
        specs.append(
            ServiceSpec(
                name=f"{host}:{RANCHER_PORT}",
                url=f"http://{host}:{RANCHER_PORT}",
                group="Rancher servers (inf.ed.ac.uk)",
                method="GET",
                timeout=RANCHER_TIMEOUT,
                expect_status=[200],
            )
        )
    if specs:
        log.info("synthesised %d rancher-server checks from RANCHER_SERVERS", len(specs))
    return specs


# ----- probing ----------------------------------------------------------------


async def probe(client_verify: httpx.AsyncClient, client_no_verify: httpx.AsyncClient, svc: ServiceSpec) -> CheckResult:
    """Probe a single service. Never raises — all errors land in CheckResult."""
    started = datetime.now(timezone.utc)
    client = client_verify if svc.verify_tls else client_no_verify
    try:
        resp = await client.request(
            svc.method,
            svc.url,
            timeout=svc.timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        )
        latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
        status_ok = resp.status_code in svc.expect_status
        body_ok = True
        if svc.expect and status_ok:
            body_ok = svc.expect in resp.text
        return CheckResult(
            name=svc.name,
            group=svc.group,
            url=svc.url,
            status="up" if status_ok and body_ok else "down",
            http_status=resp.status_code,
            latency_ms=latency_ms,
            error=None if status_ok and body_ok else _explain(resp, svc, body_ok),
            checked_at=started.isoformat(timespec="seconds"),
        )
    except httpx.TimeoutException:
        return CheckResult(
            name=svc.name,
            group=svc.group,
            url=svc.url,
            status="down",
            error=f"timeout after {svc.timeout}s",
            checked_at=started.isoformat(timespec="seconds"),
        )
    except Exception as exc:  # noqa: BLE001 — top-level safety
        return CheckResult(
            name=svc.name,
            group=svc.group,
            url=svc.url,
            status="down",
            error=f"{type(exc).__name__}: {exc}",
            checked_at=started.isoformat(timespec="seconds"),
        )


def _explain(resp: httpx.Response, svc: ServiceSpec, body_ok: bool) -> str:
    if resp.status_code not in svc.expect_status:
        return f"unexpected status {resp.status_code} (wanted {svc.expect_status})"
    if not body_ok:
        return f"body did not contain expected string {svc.expect!r}"
    return "unknown failure"


async def probe_all(services: Iterable[ServiceSpec]) -> list[CheckResult]:
    # Two clients so we can opt out of TLS verification per service (some VFB
    # subdomains are served by a cert with a non-covering SAN; the server is
    # still up — it's a known cert-provisioning issue).
    async with httpx.AsyncClient(verify=True) as c_verify, httpx.AsyncClient(verify=False) as c_no_verify:
        tasks = [probe(c_verify, c_no_verify, svc) for svc in services]
        return await asyncio.gather(*tasks)


def _parse_cache_json(
    name: str,
    url: str,
    body: dict[str, Any],
    started: str,
    container: str | None = None,
) -> CacheCheck:
    health = body.get("health") or {}
    upstream = body.get("upstream") or {}
    cache = body.get("cache") or {}
    conns = body.get("connections") or {}
    return CacheCheck(
        name=name,
        status_url=url,
        ok=True,
        checked_at=body.get("updated_at") or started,
        container=container,
        nginx_healthy=_to_bool(health.get("nginx")),
        upstream_healthy=_to_bool(health.get("upstream")),
        upstream_host=upstream.get("host"),
        upstream_port=_to_int(upstream.get("port")),
        cache_total=_to_int(cache.get("total")),
        cache_hit=_to_int(cache.get("hit")),
        cache_miss=_to_int(cache.get("miss")),
        conn_active=_to_int(conns.get("active")),
        conn_reading=_to_int(conns.get("reading")),
        conn_writing=_to_int(conns.get("writing")),
        conn_waiting=_to_int(conns.get("waiting")),
    )


async def _fetch_cache_status(
    client: httpx.AsyncClient, url: str, timeout: float
) -> tuple[bool, dict[str, Any] | None, str | None]:
    """Return (ok, parsed_json, error_message)."""
    try:
        resp = await client.get(
            url,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        if resp.status_code != 200:
            return False, None, f"HTTP {resp.status_code}"
        try:
            return True, resp.json(), None
        except Exception as exc:  # noqa: BLE001
            return False, None, f"invalid JSON: {exc}"
    except httpx.TimeoutException:
        return False, None, f"timeout after {timeout}s"
    except Exception as exc:  # noqa: BLE001
        return False, None, f"{type(exc).__name__}: {exc}"


async def _rancher_list_instances(
    client: httpx.AsyncClient, ranch: RancherEnumeration
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Return (instances, error). Each instance has at least 'name' and
    'primaryIpAddress' from the Rancher v1 API.
    """
    key = os.environ.get(ranch.api_key_env, "")
    secret = os.environ.get(ranch.api_secret_env, "")
    if not key or not secret:
        return None, f"missing {ranch.api_key_env} / {ranch.api_secret_env}"
    try:
        # The service URL gives us the service object — instances are at
        # `${service_url}/instances`.
        url = f"{ranch.service_url}/instances"
        resp = await client.get(
            url,
            timeout=ranch.timeout,
            auth=(key, secret),
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        if resp.status_code != 200:
            return None, f"Rancher API HTTP {resp.status_code}"
        body = resp.json()
        raw = body.get("data") or []
        instances = [
            {
                "id": i.get("id"),
                "name": i.get("name"),
                "primaryIpAddress": i.get("primaryIpAddress"),
                "state": i.get("state"),
                "healthState": i.get("healthState"),
                "hostId": i.get("hostId"),
            }
            for i in raw
            if i.get("state") == "running"
        ]
        return instances, None
    except httpx.TimeoutException:
        return None, f"Rancher API timeout after {ranch.timeout}s"
    except Exception as exc:  # noqa: BLE001
        return None, f"Rancher API {type(exc).__name__}: {exc}"


async def probe_cache(
    client_verify: httpx.AsyncClient,
    client_no_verify: httpx.AsyncClient,
    svc: CacheServiceSpec,
) -> list[CacheCheck]:
    """Probe a cache. Returns one CacheCheck per container when the cache has
    a Rancher enumeration block configured, otherwise a single CacheCheck for
    the LB-fronted /status. Never raises.
    """
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client = client_verify if svc.verify_tls else client_no_verify

    if svc.rancher is None:
        ok, body, err = await _fetch_cache_status(client, svc.status_url, svc.timeout)
        if not ok or body is None:
            return [
                CacheCheck(
                    name=svc.name, status_url=svc.status_url, ok=False,
                    checked_at=started, error=err,
                )
            ]
        return [_parse_cache_json(svc.name, svc.status_url, body, started)]

    # Rancher API enumeration path.
    ranch = svc.rancher
    # The Rancher API itself is HTTPS — always verify there.
    instances, err = await _rancher_list_instances(client_verify, ranch)
    if instances is None:
        # API call failed — log the reason and fall back to a single LB probe
        # so we don't lose data. Attach the rancher error only if the fallback
        # itself also failed (so successful cards don't show a fake error).
        log.warning("cache %s: rancher enumeration failed: %s", svc.name, err)
        ok, body, ferr = await _fetch_cache_status(client, svc.status_url, svc.timeout)
        if ok and body is not None:
            return [_parse_cache_json(svc.name, svc.status_url, body, started)]
        return [
            CacheCheck(
                name=svc.name, status_url=svc.status_url, ok=False,
                checked_at=started, error=f"{ferr} [rancher: {err}]",
            )
        ]

    if not instances:
        return [
            CacheCheck(
                name=svc.name, status_url=svc.status_url, ok=False,
                checked_at=started, error="Rancher API returned no running instances",
            )
        ]

    # Probe every container in parallel, by primaryIpAddress.
    async def _probe_one(inst: dict[str, Any]) -> CacheCheck:
        ip = inst.get("primaryIpAddress")
        cname = inst.get("name") or inst.get("id") or "unknown"
        if not ip:
            return CacheCheck(
                name=svc.name, status_url=svc.status_url, ok=False,
                checked_at=started, container=cname,
                error="instance has no primaryIpAddress",
            )
        url = f"{ranch.container_scheme}://{ip}:{ranch.container_port}{ranch.container_path}"
        ok, body, err = await _fetch_cache_status(client, url, ranch.timeout)
        if not ok or body is None:
            return CacheCheck(
                name=svc.name, status_url=url, ok=False,
                checked_at=started, container=cname, error=err,
            )
        return _parse_cache_json(svc.name, url, body, started, container=cname)

    return list(await asyncio.gather(*[_probe_one(i) for i in instances]))


def _to_bool(v: Any) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes", "y")
    return bool(v)


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


async def probe_all_caches(services: Iterable[CacheServiceSpec]) -> list[CacheCheck]:
    if not services:
        return []
    async with httpx.AsyncClient(verify=True) as cv, httpx.AsyncClient(verify=False) as cnv:
        nested = await asyncio.gather(*[probe_cache(cv, cnv, s) for s in services])
        flat: list[CacheCheck] = []
        for batch in nested:
            flat.extend(batch)
        return flat


def _parse_app_json(
    svc: AppServiceSpec, url: str, data: dict[str, Any], started: str,
    container: str | None = None, raw: str | None = None,
) -> AppCheck:
    if svc.shape == "vfbquery":
        solr_cache = data.get("solr_cache") or {}
        return AppCheck(
            name=svc.name, status_url=url, shape=svc.shape,
            ok=True, checked_at=started, container=container,
            q_status=data.get("status"),
            q_version=(str(data.get("version")) if data.get("version") is not None else None),
            q_workers=_to_int(data.get("workers")),
            q_max_concurrent=_to_int(data.get("max_concurrent")),
            q_max_queue_depth=_to_int(data.get("max_queue_depth")),
            q_active=_to_int(data.get("active")),
            q_waiting=_to_int(data.get("waiting")),
            q_total_served=_to_int(data.get("total_served")),
            q_cache_size=_to_int(data.get("cache_size")),
            q_cache_hits=_to_int(data.get("cache_hits")),
            q_coalesced_total=_to_int(data.get("coalesced_total")),
            q_coalesced_in_flight=_to_int(data.get("coalesced_in_flight")),
            q_scanner_blocked=_to_int(data.get("scanner_probes_blocked")),
            q_solr_cache_enabled=_to_bool(solr_cache.get("enabled")),
            raw=raw,
        )
    return AppCheck(
        name=svc.name, status_url=url, shape=svc.shape,
        ok=True, checked_at=started, container=container, raw=raw,
    )


async def _fetch_app_status(
    client: httpx.AsyncClient, url: str, timeout: float,
) -> tuple[bool, dict[str, Any] | None, str | None]:
    try:
        resp = await client.get(
            url, timeout=timeout, follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        if resp.status_code != 200:
            return False, None, f"HTTP {resp.status_code}"
        try:
            return True, resp.json(), None
        except Exception as exc:  # noqa: BLE001
            return False, None, f"invalid JSON: {exc}"
    except httpx.TimeoutException:
        return False, None, f"timeout after {timeout}s"
    except Exception as exc:  # noqa: BLE001
        return False, None, f"{type(exc).__name__}: {exc}"


async def probe_app(
    client_verify: httpx.AsyncClient,
    client_no_verify: httpx.AsyncClient,
    svc: AppServiceSpec,
) -> list[AppCheck]:
    """Probe an application service's /status JSON. Returns one AppCheck per
    container when a Rancher enumeration block is set, else a single
    LB-fronted AppCheck. Never raises.
    """
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client = client_verify if svc.verify_tls else client_no_verify

    if svc.rancher is None:
        ok, data, err = await _fetch_app_status(client, svc.status_url, svc.timeout)
        if not ok or data is None:
            return [
                AppCheck(
                    name=svc.name, status_url=svc.status_url, shape=svc.shape,
                    ok=False, checked_at=started, error=err,
                )
            ]
        return [_parse_app_json(svc, svc.status_url, data, started)]

    # Rancher API enumeration — same pattern as probe_cache.
    ranch = svc.rancher
    instances, rerr = await _rancher_list_instances(client_verify, ranch)
    if instances is None:
        log.warning("app %s: rancher enumeration failed: %s", svc.name, rerr)
        ok, data, err = await _fetch_app_status(client, svc.status_url, svc.timeout)
        if ok and data is not None:
            return [_parse_app_json(svc, svc.status_url, data, started)]
        return [
            AppCheck(
                name=svc.name, status_url=svc.status_url, shape=svc.shape,
                ok=False, checked_at=started, error=f"{err} [rancher: {rerr}]",
            )
        ]

    if not instances:
        return [
            AppCheck(
                name=svc.name, status_url=svc.status_url, shape=svc.shape,
                ok=False, checked_at=started,
                error="Rancher API returned no running instances",
            )
        ]

    async def _probe_one(inst: dict[str, Any]) -> AppCheck:
        ip = inst.get("primaryIpAddress")
        cname = inst.get("name") or inst.get("id") or "unknown"
        if not ip:
            return AppCheck(
                name=svc.name, status_url=svc.status_url, shape=svc.shape,
                ok=False, checked_at=started, container=cname,
                error="instance has no primaryIpAddress",
            )
        url = f"{ranch.container_scheme}://{ip}:{ranch.container_port}{ranch.container_path}"
        ok, data, err = await _fetch_app_status(client, url, ranch.timeout)
        if not ok or data is None:
            return AppCheck(
                name=svc.name, status_url=url, shape=svc.shape,
                ok=False, checked_at=started, container=cname, error=err,
            )
        return _parse_app_json(svc, url, data, started, container=cname)

    return list(await asyncio.gather(*[_probe_one(i) for i in instances]))


async def probe_all_apps(services: Iterable[AppServiceSpec]) -> list[AppCheck]:
    if not services:
        return []
    async with httpx.AsyncClient(verify=True) as cv, httpx.AsyncClient(verify=False) as cnv:
        nested = await asyncio.gather(*[probe_app(cv, cnv, s) for s in services])
        flat: list[AppCheck] = []
        for batch in nested:
            flat.extend(batch)
        return flat


async def _probe_neo4j_at(
    client: httpx.AsyncClient,
    svc: Neo4jServiceSpec,
    base_url: str,
    container: str | None = None,
) -> Neo4jCheck:
    """Run the two-stage Neo4j health check against a specific base_url.

    Never raises. "up" iff: HTTP succeeds, SHOW DATABASES reports
    currentStatus == 'online' for svc.db, count >= svc.min_nodes.
    """
    started = datetime.now(timezone.utc)
    iso_started = started.isoformat(timespec="seconds")
    # Env var wins (allows secret rotation without a redeploy); fall back to
    # the YAML-embedded password (used when the credentials are intentionally
    # public, e.g. VFB's read-only neo4j:vfb).
    password = os.environ.get(svc.password_env, "") or svc.password
    auth = (svc.user, password) if password else None

    async def _cypher(url: str, statement: str) -> tuple[int, Any, str | None]:
        try:
            resp = await client.post(
                url,
                timeout=svc.timeout,
                json={"statements": [{"statement": statement}]},
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                auth=auth,
            )
            try:
                return resp.status_code, resp.json(), None
            except Exception as exc:  # noqa: BLE001
                return resp.status_code, None, f"non-JSON response: {exc}"
        except httpx.TimeoutException:
            return 0, None, f"timeout after {svc.timeout}s"
        except Exception as exc:  # noqa: BLE001
            return 0, None, f"{type(exc).__name__}: {exc}"

    # 1) DB status from system db
    sys_url = f"{base_url}/db/system/tx/commit"
    code, body, err = await _cypher(sys_url, "SHOW DATABASES")
    if err:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container,
            error=f"SHOW DATABASES: {err}",
        )
    if code != 200 or not body:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container,
            error=f"SHOW DATABASES HTTP {code}",
        )
    db_status: str | None = None
    db_error: str | None = None
    for r in (body.get("results") or [{}])[0].get("data", []):
        row = r.get("row") or []
        # Columns: name, address, role, requestedStatus, currentStatus, error, default
        if len(row) >= 6 and row[0] == svc.db:
            db_status = row[4]
            db_error = row[5] or None
            break
    if db_status is None:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container, db_status=None,
            error=f"database '{svc.db}' not found in SHOW DATABASES",
        )
    if db_status != "online":
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container,
            db_status=db_status, db_error=db_error,
            error=f"db '{svc.db}' is {db_status}" + (f": {db_error}" if db_error else ""),
        )

    # 2) Node count
    data_url = f"{base_url}/db/{svc.db}/tx/commit"
    code, body, err = await _cypher(data_url, "MATCH (n) RETURN count(n) AS n")
    latency_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    if err:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container, latency_ms=latency_ms,
            db_status=db_status, db_error=db_error,
            error=f"count: {err}",
        )
    if code != 200 or not body:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container, latency_ms=latency_ms,
            db_status=db_status, db_error=db_error,
            error=f"count HTTP {code}",
        )
    count: int | None = None
    try:
        count = int(body["results"][0]["data"][0]["row"][0])
    except (KeyError, IndexError, ValueError, TypeError):
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container, latency_ms=latency_ms,
            db_status=db_status, db_error=db_error,
            error="could not parse count from Cypher response",
        )
    if count < svc.min_nodes:
        return Neo4jCheck(
            name=svc.name, base_url=base_url, db=svc.db, ok=False,
            checked_at=iso_started, container=container, latency_ms=latency_ms,
            db_status=db_status, db_error=db_error,
            node_count=count,
            error=f"count {count} < min_nodes {svc.min_nodes}",
        )
    return Neo4jCheck(
        name=svc.name, base_url=base_url, db=svc.db, ok=True,
        checked_at=iso_started, container=container, latency_ms=latency_ms,
        db_status=db_status, db_error=db_error,
        node_count=count,
    )


async def probe_neo4j(
    client_verify: httpx.AsyncClient,
    client_no_verify: httpx.AsyncClient,
    svc: Neo4jServiceSpec,
) -> list[Neo4jCheck]:
    """Probe a Neo4j service. Returns one Neo4jCheck per container when a
    Rancher enumeration block is set, otherwise a single LB-fronted check.
    Never raises.
    """
    started_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client = client_verify if svc.verify_tls else client_no_verify

    if svc.rancher is None:
        return [await _probe_neo4j_at(client, svc, svc.base_url)]

    ranch = svc.rancher
    instances, rerr = await _rancher_list_instances(client_verify, ranch)
    if instances is None:
        log.warning("neo4j %s: rancher enumeration failed: %s", svc.name, rerr)
        # Fall back to the LB probe so we don't lose all signal.
        fallback = await _probe_neo4j_at(client, svc, svc.base_url)
        if fallback.error:
            fallback.error = f"{fallback.error} [rancher: {rerr}]"
        else:
            fallback.error = f"[rancher: {rerr}]"
        return [fallback]

    if not instances:
        return [
            Neo4jCheck(
                name=svc.name, base_url=svc.base_url, db=svc.db, ok=False,
                checked_at=started_iso,
                error="Rancher API returned no running instances",
            )
        ]

    async def _probe_one(inst: dict[str, Any]) -> Neo4jCheck:
        ip = inst.get("primaryIpAddress")
        cname = inst.get("name") or inst.get("id") or "unknown"
        if not ip:
            return Neo4jCheck(
                name=svc.name, base_url=svc.base_url, db=svc.db, ok=False,
                checked_at=started_iso, container=cname,
                error="instance has no primaryIpAddress",
            )
        container_url = f"{ranch.container_scheme}://{ip}:{ranch.container_port}"
        return await _probe_neo4j_at(client, svc, container_url, container=cname)

    return list(await asyncio.gather(*[_probe_one(i) for i in instances]))


async def probe_all_neo4j(services: Iterable[Neo4jServiceSpec]) -> list[Neo4jCheck]:
    if not services:
        return []
    async with httpx.AsyncClient(verify=True) as cv, httpx.AsyncClient(verify=False) as cnv:
        nested = await asyncio.gather(*[probe_neo4j(cv, cnv, s) for s in services])
        flat: list[Neo4jCheck] = []
        for batch in nested:
            flat.extend(batch)
        return flat


# ----- Rancher cluster overview ----------------------------------------------


def _short_host_name(hostname: str | None) -> str:
    return (hostname or "").split(".", 1)[0]


async def _resolve_dns_ingress_ips(hostname: str) -> set[str]:
    """Return the set of A-record IPs for `hostname`. Empty on failure."""
    if not hostname:
        return set()
    try:
        infos = await asyncio.to_thread(
            __import__("socket").getaddrinfo,
            hostname,
            None,
            0,
            __import__("socket").SOCK_STREAM,
        )
        return {info[4][0] for info in infos}
    except Exception as exc:  # noqa: BLE001
        log.warning("DNS lookup for %s failed: %s", hostname, exc)
        return set()


async def probe_rancher_cluster() -> RancherClusterResult:
    """Build a cluster overview: every host, plus all `state=active` services
    within the configured stacks, plus the LB-coverage and DNS-ingress flags
    per host. Never raises.
    """
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if not RANCHER_PROJECT_URL:
        return RancherClusterResult(
            fetched_at=started, ok=False,
            error="RANCHER_PROJECT_URL is empty — cluster overview disabled",
        )
    key = os.environ.get("RANCHER_API_KEY", "")
    secret = os.environ.get("RANCHER_API_SECRET", "")
    if not key or not secret:
        return RancherClusterResult(
            fetched_at=started, ok=False,
            error="missing RANCHER_API_KEY / RANCHER_API_SECRET",
        )

    base = RANCHER_PROJECT_URL.rstrip("/")
    auth = (key, secret)
    timeout = RANCHER_CLUSTER_TIMEOUT
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    async with httpx.AsyncClient(verify=True, auth=auth, headers=headers) as client:
        async def _get(url: str) -> dict[str, Any] | None:
            try:
                resp = await client.get(url, timeout=timeout)
                if resp.status_code != 200:
                    log.warning("rancher GET %s -> HTTP %d", url, resp.status_code)
                    return None
                return resp.json()
            except Exception as exc:  # noqa: BLE001
                log.warning("rancher GET %s failed: %s", url, exc)
                return None

        hosts_body, _stacks_idx_body = await asyncio.gather(
            _get(f"{base}/hosts?limit=-1"),
            _get(f"{base}/stacks?limit=-1"),
        )

        if hosts_body is None or _stacks_idx_body is None:
            return RancherClusterResult(
                fetched_at=started, ok=False,
                error="Rancher API not reachable (hosts/stacks fetch failed)",
            )

        # Hosts
        hosts: list[RancherHost] = []
        for h in hosts_body.get("data", []) or []:
            hosts.append(
                RancherHost(
                    id=str(h.get("id") or ""),
                    hostname=str(h.get("hostname") or ""),
                    short_name=_short_host_name(h.get("hostname")),
                    state=str(h.get("state") or ""),
                    agent_state=h.get("agentState"),
                    agent_ip=h.get("agentIpAddress"),
                )
            )

        # DNS ingress
        if RANCHER_DNS_HOSTS:
            dns_set = {n.lower() for n in RANCHER_DNS_HOSTS}
            for h in hosts:
                h.is_dns_ingress = h.short_name.lower() in dns_set
        else:
            ips = await _resolve_dns_ingress_ips(RANCHER_DNS_HOSTNAME)
            for h in hosts:
                if h.agent_ip and h.agent_ip in ips:
                    h.is_dns_ingress = True

        # Services in the configured stacks
        stacks_by_name = {
            s.get("name"): s for s in (_stacks_idx_body.get("data") or [])
        }
        services: list[RancherService] = []
        for stack_name in RANCHER_STACKS:
            stack = stacks_by_name.get(stack_name)
            if stack is None:
                log.warning("RANCHER_STACKS: stack %r not found", stack_name)
                continue
            stack_id = stack.get("id")
            body = await _get(f"{base}/stacks/{stack_id}/services?limit=-1")
            if body is None:
                continue
            for s in body.get("data", []) or []:
                if s.get("state") != "active":
                    continue
                services.append(
                    RancherService(
                        id=str(s.get("id") or ""),
                        name=str(s.get("name") or ""),
                        stack=stack_name,
                        type=str(s.get("type") or "service"),
                        state=str(s.get("state") or ""),
                        health_state=s.get("healthState"),
                        scale=s.get("scale"),
                        current_scale=s.get("currentScale"),
                    )
                )

        # LB host coverage — find every active loadBalancerService and its
        # running instances; mark each touched host.
        lb_services = [
            s for s in services if s.type == "loadBalancerService"
        ]
        lb_host_ids: set[str] = set()
        for lb in lb_services:
            inst_body = await _get(f"{base}/services/{lb.id}/instances?limit=-1")
            if inst_body is None:
                continue
            for inst in inst_body.get("data", []) or []:
                if inst.get("state") != "running":
                    continue
                hid = inst.get("hostId")
                if hid:
                    lb_host_ids.add(str(hid))
        for h in hosts:
            if h.id in lb_host_ids:
                h.has_lb = True

    return RancherClusterResult(
        fetched_at=started, ok=True,
        hosts=hosts, services=services,
    )


# ----- history storage --------------------------------------------------------


class History:
    """SQLite-backed probe history. One row per check, indexed by (service, ts)."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service TEXT NOT NULL,
        checked_at TEXT NOT NULL,
        ts INTEGER NOT NULL,
        status TEXT NOT NULL,
        http_status INTEGER,
        latency_ms INTEGER,
        error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_service_ts ON history (service, ts);
    CREATE INDEX IF NOT EXISTS idx_ts ON history (ts);

    CREATE TABLE IF NOT EXISTS cache_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service TEXT NOT NULL,
        container TEXT,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        ok INTEGER NOT NULL,
        nginx_healthy INTEGER,
        upstream_healthy INTEGER,
        upstream_host TEXT,
        upstream_port INTEGER,
        cache_total INTEGER,
        cache_hit INTEGER,
        cache_miss INTEGER,
        conn_active INTEGER,
        conn_reading INTEGER,
        conn_writing INTEGER,
        conn_waiting INTEGER,
        error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_cache_service_ts ON cache_history (service, ts);
    CREATE INDEX IF NOT EXISTS idx_cache_ts ON cache_history (ts);
    -- NOTE: idx_cache_service_container_ts lives in _migrate() so the
    -- `container` column has been ALTER-ed in on upgrades from <0.6.0 before
    -- the index is created. SCHEMA must not reference columns that may not
    -- yet exist on legacy databases.

    CREATE TABLE IF NOT EXISTS app_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service TEXT NOT NULL,
        shape TEXT NOT NULL,
        container TEXT,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        ok INTEGER NOT NULL,
        error TEXT,
        q_status TEXT,
        q_version TEXT,
        q_workers INTEGER,
        q_max_concurrent INTEGER,
        q_max_queue_depth INTEGER,
        q_active INTEGER,
        q_waiting INTEGER,
        q_total_served INTEGER,
        q_cache_size INTEGER,
        q_cache_hits INTEGER,
        q_coalesced_total INTEGER,
        q_coalesced_in_flight INTEGER,
        q_scanner_blocked INTEGER,
        q_solr_cache_enabled INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_app_service_ts ON app_history (service, ts);
    CREATE INDEX IF NOT EXISTS idx_app_ts ON app_history (ts);
    -- NOTE: idx_app_service_container_ts is created in _migrate() for the
    -- same reason as cache_history's container index (column may be added
    -- in-place on upgrade from <0.8.0).

    CREATE TABLE IF NOT EXISTS neo4j_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service TEXT NOT NULL,
        container TEXT,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        ok INTEGER NOT NULL,
        db_status TEXT,
        db_error TEXT,
        node_count INTEGER,
        latency_ms INTEGER,
        error TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_neo4j_service_ts ON neo4j_history (service, ts);
    CREATE INDEX IF NOT EXISTS idx_neo4j_ts ON neo4j_history (ts);

    CREATE TABLE IF NOT EXISTS rancher_host_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        host_id TEXT NOT NULL,
        hostname TEXT NOT NULL,
        state TEXT,
        agent_state TEXT,
        agent_ip TEXT,
        is_dns_ingress INTEGER,
        has_lb INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_rh_host_ts ON rancher_host_history (host_id, ts);
    CREATE INDEX IF NOT EXISTS idx_rh_ts ON rancher_host_history (ts);

    CREATE TABLE IF NOT EXISTS rancher_service_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        stack TEXT,
        type TEXT,
        state TEXT,
        health_state TEXT,
        scale INTEGER,
        current_scale INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_rs_service_ts ON rancher_service_history (service_id, ts);
    CREATE INDEX IF NOT EXISTS idx_rs_ts ON rancher_service_history (ts);
    """

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None
        self._lock_fd: int | None = None
        if path is None:
            log.warning("history is disabled (HISTORY_DB explicitly empty)")
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive flock on a sentinel file so a second container running
            # against the same volume (e.g. during a botched Rancher rolling
            # upgrade) refuses to write rather than corrupting the SQLite DB.
            # Lock survives only as long as this process holds the fd open.
            lock_path = path.with_suffix(path.suffix + ".lock")
            try:
                self._lock_fd = os.open(
                    str(lock_path), os.O_CREAT | os.O_RDWR, 0o644
                )
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                log.error(
                    "history disabled: another vfb-status instance is already "
                    "writing to %s. Refusing to corrupt the database. "
                    "Set scale=1 and Rancher upgrade strategy to "
                    "Stop-then-Start.", path,
                )
                if self._lock_fd is not None:
                    os.close(self._lock_fd)
                    self._lock_fd = None
                return
            except OSError as exc:
                # Filesystem doesn't support flock (some NFS configs). Log
                # and continue without the guard — better to risk corruption
                # than crash.
                log.warning("history lock unavailable (%s); proceeding", exc)
            self._conn = sqlite3.connect(
                str(path),
                check_same_thread=False,
                isolation_level=None,  # autocommit; we handle transactions
            )
            # journal_mode is configurable so we can pick NFS-safe defaults.
            # WAL is faster but breaks on NFS (sqlite docs: "if a database is
            # accessed via NFS, then it must use journal_mode=DELETE"). The
            # default is DELETE — set HISTORY_JOURNAL_MODE=WAL to opt back in
            # when the volume is local SSD.
            journal = os.environ.get("HISTORY_JOURNAL_MODE", "DELETE").upper()
            try:
                self._conn.execute(f"PRAGMA journal_mode={journal}")
            except sqlite3.Error as exc:
                log.warning("failed to set journal_mode=%s: %s", journal, exc)
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=2000")
            self._conn.executescript(self.SCHEMA)
            self._migrate()
            log.info("history db at %s (writable, journal_mode=%s)", path, journal)
        except OSError as exc:
            # Most likely /data isn't mounted writable. Don't crash the app —
            # just disable history and surface the reason loudly on the page.
            self._conn = None
            log.error(
                "history disabled: cannot open %s (%s). "
                "Mount a persistent volume at %s and ensure it is writable.",
                path, exc, path.parent,
            )

    @property
    def enabled(self) -> bool:
        return self._conn is not None

    def _migrate(self) -> None:
        """Forward-compatible column adds for existing databases. SQLite's
        ALTER TABLE only supports ADD COLUMN, which is all we need here.

        Runs AFTER the main SCHEMA executescript, so any new tables already
        exist. Indexes that reference newly-added columns are created here
        rather than in SCHEMA — putting them in SCHEMA crashes on upgrade
        because executescript runs before this migration.
        """
        if self._conn is None:
            return
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(cache_history)")}
        if "container" not in cols:
            self._conn.execute("ALTER TABLE cache_history ADD COLUMN container TEXT")
            log.info("history: added container column to cache_history")
        # Always assert — safe IF NOT EXISTS, covers fresh installs too.
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cache_service_container_ts "
            "ON cache_history (service, container, ts)"
        )
        # Same pattern for app_history (added in v0.8.0)
        app_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(app_history)")}
        if app_cols and "container" not in app_cols:
            self._conn.execute("ALTER TABLE app_history ADD COLUMN container TEXT")
            log.info("history: added container column to app_history")
        if app_cols and "q_version" not in app_cols:
            self._conn.execute("ALTER TABLE app_history ADD COLUMN q_version TEXT")
            log.info("history: added q_version column to app_history")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_app_service_container_ts "
            "ON app_history (service, container, ts)"
        )
        # Same pattern for neo4j_history (added in v0.9.0)
        n4_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(neo4j_history)")}
        if n4_cols and "container" not in n4_cols:
            self._conn.execute("ALTER TABLE neo4j_history ADD COLUMN container TEXT")
            log.info("history: added container column to neo4j_history")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_neo4j_service_container_ts "
            "ON neo4j_history (service, container, ts)"
        )

    def record(self, results: Iterable[CheckResult]) -> None:
        if self._conn is None:
            return
        rows = [
            (
                r.name,
                r.checked_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
                int(time.time()),
                r.status,
                r.http_status,
                r.latency_ms,
                r.error,
            )
            for r in results
        ]
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.executemany(
                "INSERT INTO history (service, checked_at, ts, status, http_status, latency_ms, error)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def prune(self, retention_days: int) -> int:
        if self._conn is None or retention_days <= 0:
            return 0
        cutoff = int(time.time()) - retention_days * 86400
        cur = self._conn.cursor()
        cur.execute("DELETE FROM history WHERE ts < ?", (cutoff,))
        removed_h = cur.rowcount or 0
        cur.execute("DELETE FROM cache_history WHERE ts < ?", (cutoff,))
        removed_c = cur.rowcount or 0
        cur.execute("DELETE FROM app_history WHERE ts < ?", (cutoff,))
        removed_a = cur.rowcount or 0
        cur.execute("DELETE FROM neo4j_history WHERE ts < ?", (cutoff,))
        removed_n = cur.rowcount or 0
        cur.execute("DELETE FROM rancher_host_history WHERE ts < ?", (cutoff,))
        removed_rh = cur.rowcount or 0
        cur.execute("DELETE FROM rancher_service_history WHERE ts < ?", (cutoff,))
        removed_rs = cur.rowcount or 0
        total = removed_h + removed_c + removed_a + removed_n + removed_rh + removed_rs
        if total:
            log.info(
                "history: pruned %d svc + %d cache + %d app + %d neo4j + %d rh + %d rs older than %dd",
                removed_h, removed_c, removed_a, removed_n, removed_rh, removed_rs,
                retention_days,
            )
        return total

    def record_rancher_cluster(self, result: RancherClusterResult) -> None:
        if self._conn is None or not result.ok:
            return
        now = int(time.time())
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.executemany(
                "INSERT INTO rancher_host_history "
                "(ts, checked_at, host_id, hostname, state, agent_state, agent_ip, "
                " is_dns_ingress, has_lb) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        now, result.fetched_at, h.id, h.hostname, h.state,
                        h.agent_state, h.agent_ip,
                        1 if h.is_dns_ingress else 0,
                        1 if h.has_lb else 0,
                    )
                    for h in result.hosts
                ],
            )
            cur.executemany(
                "INSERT INTO rancher_service_history "
                "(ts, checked_at, service_id, name, stack, type, state, health_state, "
                " scale, current_scale) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        now, result.fetched_at, s.id, s.name, s.stack, s.type,
                        s.state, s.health_state, s.scale, s.current_scale,
                    )
                    for s in result.services
                ],
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def record_neo4j(self, results: Iterable[Neo4jCheck]) -> None:
        if self._conn is None:
            return
        rows = [
            (
                r.name,
                r.container,
                int(time.time()),
                r.checked_at,
                1 if r.ok else 0,
                r.db_status,
                r.db_error,
                r.node_count,
                r.latency_ms,
                r.error,
            )
            for r in results
        ]
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.executemany(
                "INSERT INTO neo4j_history "
                "(service, container, ts, checked_at, ok, db_status, db_error, node_count, latency_ms, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def neo4j_series(
        self, service: str, since_seconds: int, max_points: int = 120
    ) -> list[dict[str, Any]]:
        """Down-sampled, oldest-first. Aggregates across containers per ts:
        MAX(node_count) (cluster members should be in sync; MAX wins ties and
        catches a divergent member dragging the average down).
        """
        if self._conn is None:
            return []
        cutoff = int(time.time()) - since_seconds
        try:
            cur = self._conn.execute(
                "SELECT ts, "
                " MAX(ok) AS any_ok, "
                " MAX(node_count) AS node_count, "
                " AVG(latency_ms) AS latency_ms "
                "FROM neo4j_history WHERE service = ? AND ts >= ? "
                "GROUP BY ts ORDER BY ts ASC",
                (service, cutoff),
            )
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            log.warning("neo4j_series read failed for %s: %s", service, exc)
            return []
        if not rows:
            return []
        step = max(1, len(rows) // max_points)
        return [
            {"ts": ts, "ok": bool(ok) if ok is not None else False,
             "node_count": n, "latency_ms": (int(l) if l is not None else None)}
            for ts, ok, n, l in rows[::step]
        ]

    def record_cache(self, results: Iterable[CacheCheck]) -> None:
        if self._conn is None:
            return
        rows = [
            (
                r.name,
                r.container,
                int(time.time()),
                r.checked_at,
                1 if r.ok else 0,
                None if r.nginx_healthy is None else (1 if r.nginx_healthy else 0),
                None if r.upstream_healthy is None else (1 if r.upstream_healthy else 0),
                r.upstream_host,
                r.upstream_port,
                r.cache_total,
                r.cache_hit,
                r.cache_miss,
                r.conn_active,
                r.conn_reading,
                r.conn_writing,
                r.conn_waiting,
                r.error,
            )
            for r in results
        ]
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.executemany(
                "INSERT INTO cache_history "
                "(service, container, ts, checked_at, ok, nginx_healthy, upstream_healthy, "
                " upstream_host, upstream_port, cache_total, cache_hit, cache_miss, "
                " conn_active, conn_reading, conn_writing, conn_waiting, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def cache_series(
        self, service: str, since_seconds: int, max_points: int = 96
    ) -> list[dict[str, Any]]:
        """Return up to `max_points` rows summing across containers per ts.

        Down-sampled, oldest first. Sums null fields gracefully — if every
        container reports null for a metric the aggregate is null.
        """
        if self._conn is None:
            return []
        cutoff = int(time.time()) - since_seconds
        try:
            cur = self._conn.execute(
                "SELECT ts, "
                " MAX(ok) AS any_ok, "
                " SUM(cache_total) AS cache_total, "
                " SUM(cache_hit) AS cache_hit, "
                " SUM(cache_miss) AS cache_miss, "
                " SUM(conn_active) AS conn_active, "
                " SUM(conn_reading) AS conn_reading, "
                " SUM(conn_writing) AS conn_writing, "
                " SUM(conn_waiting) AS conn_waiting "
                "FROM cache_history WHERE service = ? AND ts >= ? "
                "GROUP BY ts ORDER BY ts ASC",
                (service, cutoff),
            )
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            log.warning("cache_series read failed for %s: %s", service, exc)
            return []
        if not rows:
            return []
        step = max(1, len(rows) // max_points)
        sampled = rows[::step]
        return [
            {
                "ts": ts,
                "ok": bool(any_ok) if any_ok is not None else False,
                "cache_total": ct,
                "cache_hit": ch,
                "cache_miss": cm,
                "conn_active": ca,
                "conn_reading": cr,
                "conn_writing": cw,
                "conn_waiting": cwt,
            }
            for ts, any_ok, ct, ch, cm, ca, cr, cw, cwt in sampled
        ]

    def cache_latest(self, service: str) -> dict[str, Any] | None:
        """Latest row for one cache service. With per-container rows now,
        this is "any one" — prefer the API-shaped /api/cache output for the
        aggregate.
        """
        if self._conn is None:
            return None
        try:
            cur = self._conn.execute(
                "SELECT ts, checked_at, ok, nginx_healthy, upstream_healthy, "
                "upstream_host, upstream_port, cache_total, cache_hit, cache_miss, "
                "conn_active, conn_reading, conn_writing, conn_waiting, error "
                "FROM cache_history WHERE service = ? ORDER BY ts DESC LIMIT 1",
                (service,),
            )
            row = cur.fetchone()
        except sqlite3.Error as exc:
            log.warning("cache_latest read failed for %s: %s", service, exc)
            return None
        if not row:
            return None
        (ts, checked_at, ok, nh, uh, uhost, uport, ct, ch, cm, ca, cr, cw, cwt, err) = row
        return {
            "ts": ts,
            "checked_at": checked_at,
            "ok": bool(ok),
            "nginx_healthy": (None if nh is None else bool(nh)),
            "upstream_healthy": (None if uh is None else bool(uh)),
            "upstream_host": uhost,
            "upstream_port": uport,
            "cache_total": ct,
            "cache_hit": ch,
            "cache_miss": cm,
            "conn_active": ca,
            "conn_reading": cr,
            "conn_writing": cw,
            "conn_waiting": cwt,
            "error": err,
            "hit_rate": (100.0 * ch / ct) if (ct and ch is not None) else None,
        }

    def uptime_pct(self, service: str, since_seconds: int) -> tuple[float | None, int]:
        """Return (uptime_pct, total_known_rows) over the last `since_seconds`.

        `unknown` rows are excluded from the denominator. Returns (None, 0) when
        we have no data in the window or when the DB read errors.
        """
        if self._conn is None:
            return None, 0
        cutoff = int(time.time()) - since_seconds
        try:
            cur = self._conn.execute(
                "SELECT status, COUNT(*) FROM history WHERE service = ? AND ts >= ? GROUP BY status",
                (service, cutoff),
            )
            counts = {row[0]: row[1] for row in cur.fetchall()}
        except sqlite3.Error as exc:
            log.warning("uptime_pct read failed for %s: %s", service, exc)
            return None, 0
        up = counts.get("up", 0)
        down = counts.get("down", 0)
        known = up + down
        if known == 0:
            return None, 0
        return (100.0 * up / known), known

    def buckets(
        self, service: str, n_buckets: int, bucket_seconds: int
    ) -> list[str]:
        """Return a list of `n_buckets` statuses for the given service, oldest first.

        Each bucket spans `bucket_seconds`. Status reduction rule per bucket:
        any `down` -> `down`, else any `up` -> `up`, else `unknown` (no data).
        """
        if self._conn is None:
            return ["unknown"] * n_buckets
        now = int(time.time())
        start = now - n_buckets * bucket_seconds
        try:
            cur = self._conn.execute(
                "SELECT ts, status FROM history "
                "WHERE service = ? AND ts >= ? ORDER BY ts ASC",
                (service, start),
            )
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            log.warning("buckets read failed for %s: %s", service, exc)
            return ["unknown"] * n_buckets
        out = ["unknown"] * n_buckets
        for ts, status in rows:
            idx = (ts - start) // bucket_seconds
            if idx < 0 or idx >= n_buckets:
                continue
            current = out[idx]
            # down beats up beats unknown
            if current == "down":
                continue
            if status == "down":
                out[idx] = "down"
            elif status == "up" and current != "down":
                out[idx] = "up"
        return out

    def record_app(self, results: Iterable[AppCheck]) -> None:
        if self._conn is None:
            return
        rows = [
            (
                r.name,
                r.shape,
                r.container,
                int(time.time()),
                r.checked_at,
                1 if r.ok else 0,
                r.error,
                r.q_status,
                r.q_version,
                r.q_workers,
                r.q_max_concurrent,
                r.q_max_queue_depth,
                r.q_active,
                r.q_waiting,
                r.q_total_served,
                r.q_cache_size,
                r.q_cache_hits,
                r.q_coalesced_total,
                r.q_coalesced_in_flight,
                r.q_scanner_blocked,
                None if r.q_solr_cache_enabled is None else (1 if r.q_solr_cache_enabled else 0),
            )
            for r in results
        ]
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.executemany(
                "INSERT INTO app_history "
                "(service, shape, container, ts, checked_at, ok, error, "
                " q_status, q_version, q_workers, q_max_concurrent, q_max_queue_depth, "
                " q_active, q_waiting, q_total_served, q_cache_size, q_cache_hits, "
                " q_coalesced_total, q_coalesced_in_flight, q_scanner_blocked, "
                " q_solr_cache_enabled) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def app_series(
        self, service: str, since_seconds: int, max_points: int = 120
    ) -> list[dict[str, Any]]:
        """Down-sampled, oldest-first. Sums across containers per ts so the
        cluster view of active / waiting / total_served is what's plotted.
        """
        if self._conn is None:
            return []
        cutoff = int(time.time()) - since_seconds
        try:
            cur = self._conn.execute(
                "SELECT ts, "
                " MAX(ok) AS any_ok, "
                " SUM(q_active) AS q_active, "
                " SUM(q_waiting) AS q_waiting, "
                " SUM(q_total_served) AS q_total_served, "
                " SUM(q_cache_hits) AS q_cache_hits, "
                " SUM(q_coalesced_in_flight) AS q_coalesced_in_flight, "
                " SUM(q_coalesced_total) AS q_coalesced_total "
                "FROM app_history WHERE service = ? AND ts >= ? "
                "GROUP BY ts ORDER BY ts ASC",
                (service, cutoff),
            )
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            log.warning("app_series read failed for %s: %s", service, exc)
            return []
        if not rows:
            return []
        step = max(1, len(rows) // max_points)
        return [
            {
                "ts": ts,
                "ok": bool(ok) if ok is not None else False,
                "active": qa,
                "waiting": qw,
                "total_served": ts_count,
                "cache_hits": ch,
                "coalesced_in_flight": cif,
                "coalesced_total": ct,
            }
            for ts, ok, qa, qw, ts_count, ch, cif, ct in rows[::step]
        ]

    def recent(self, service: str, limit: int = 200) -> list[dict[str, Any]]:
        if self._conn is None:
            return []
        try:
            cur = self._conn.execute(
                "SELECT checked_at, status, http_status, latency_ms, error "
                "FROM history WHERE service = ? ORDER BY ts DESC LIMIT ?",
                (service, limit),
            )
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            log.warning("recent read failed for %s: %s", service, exc)
            return []
        return [
            {
                "checked_at": ca,
                "status": st,
                "http_status": hs,
                "latency_ms": lm,
                "error": err,
            }
            for ca, st, hs, lm, err in rows
        ]

    def services(self) -> list[str]:
        if self._conn is None:
            return []
        try:
            cur = self._conn.execute("SELECT DISTINCT service FROM history ORDER BY service")
            return [row[0] for row in cur.fetchall()]
        except sqlite3.Error as exc:
            log.warning("services list read failed: %s", exc)
            return []


# ----- app state --------------------------------------------------------------


class State:
    def __init__(
        self,
        services: list[ServiceSpec],
        cache_services: list[CacheServiceSpec] | None = None,
        app_services: list[AppServiceSpec] | None = None,
        neo4j_services: list[Neo4jServiceSpec] | None = None,
        history: History | None = None,
    ):
        self.services = services
        self.cache_services = cache_services or []
        self.app_services = app_services or []
        self.neo4j_services = neo4j_services or []
        self.results: dict[str, CheckResult] = {}
        self.cache_results: dict[str, list[CacheCheck]] = {}
        self.app_results: dict[str, list[AppCheck]] = {}
        self.neo4j_results: dict[str, list[Neo4jCheck]] = {}
        self.cluster_result: RancherClusterResult | None = None
        self.last_run: str | None = None
        self.running = False
        self.history = history or History(None)
        self._lock = asyncio.Lock()

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_run": self.last_run,
            "results": [r.__dict__ for r in self.results.values()],
        }

    def load_from_disk(self) -> None:
        if not STATE_FILE or not STATE_FILE.exists():
            return
        try:
            data = json.loads(STATE_FILE.read_text())
            self.last_run = data.get("last_run")
            for r in data.get("results", []):
                self.results[r["name"]] = CheckResult(**r)
            log.info("restored %d results from %s", len(self.results), STATE_FILE)
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to load state from %s: %s", STATE_FILE, exc)

    def persist(self) -> None:
        if not STATE_FILE:
            return
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STATE_FILE.write_text(json.dumps(self.to_dict(), indent=2))
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to persist state to %s: %s", STATE_FILE, exc)

    async def run_checks(self) -> None:
        # Serialise check runs so a manual refresh during the scheduled run
        # doesn't double-probe everything.
        async with self._lock:
            self.running = True
            try:
                log.info(
                    "probing %d services + %d caches + %d apps",
                    len(self.services),
                    len(self.cache_services),
                    len(self.app_services),
                )
                results, cache_results, app_results, neo4j_results, cluster_result = await asyncio.gather(
                    probe_all(self.services),
                    probe_all_caches(self.cache_services),
                    probe_all_apps(self.app_services),
                    probe_all_neo4j(self.neo4j_services),
                    probe_rancher_cluster(),
                )
                for r in results:
                    self.results[r.name] = r
                self.cache_results = {}
                for cr in cache_results:
                    self.cache_results.setdefault(cr.name, []).append(cr)
                self.app_results = {}
                for ar in app_results:
                    self.app_results.setdefault(ar.name, []).append(ar)
                self.neo4j_results = {}
                for nr in neo4j_results:
                    self.neo4j_results.setdefault(nr.name, []).append(nr)
                self.cluster_result = cluster_result
                self.last_run = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self.persist()
                if self.history.enabled:
                    try:
                        self.history.record(results)
                        if cache_results:
                            self.history.record_cache(cache_results)
                        if app_results:
                            self.history.record_app(app_results)
                        if neo4j_results:
                            self.history.record_neo4j(neo4j_results)
                        if cluster_result is not None:
                            self.history.record_rancher_cluster(cluster_result)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("history write failed: %s", exc)
                up = sum(1 for r in results if r.status == "up")
                cache_ok = sum(1 for c in cache_results if c.ok)
                cache_total = len({c.name for c in cache_results}) or len(self.cache_services)
                app_ok = sum(1 for a in app_results if a.ok)
                neo_ok = sum(1 for n in neo4j_results if n.ok)
                cluster_msg = ""
                if cluster_result is not None:
                    if cluster_result.ok:
                        deg = sum(1 for s in cluster_result.services if s.is_degraded)
                        cluster_msg = (
                            f", cluster {len(cluster_result.hosts)} hosts / "
                            f"{len(cluster_result.services)} active services "
                            f"({deg} degraded)"
                        )
                    else:
                        cluster_msg = f", cluster: {cluster_result.error}"
                log.info(
                    "checks complete: %d/%d up, %d/%d cache rows (%d services), "
                    "%d/%d apps, %d/%d neo4j%s",
                    up, len(results), cache_ok, len(cache_results), cache_total,
                    app_ok, len(app_results), neo_ok, len(neo4j_results), cluster_msg,
                )
            finally:
                self.running = False


# ----- FastAPI wiring ---------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    services = load_services(CONFIG_PATH) + rancher_server_specs()
    cache_services = load_cache_services(CONFIG_PATH)
    app_services = load_app_services(CONFIG_PATH)
    neo4j_services = load_neo4j_services(CONFIG_PATH)
    history = History(HISTORY_DB)
    history.prune(HISTORY_RETENTION_DAYS)
    state = State(
        services,
        cache_services=cache_services,
        app_services=app_services,
        neo4j_services=neo4j_services,
        history=history,
    )
    state.load_from_disk()
    app.state.state = state

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        state.run_checks,
        trigger=IntervalTrigger(seconds=CHECK_INTERVAL_SECONDS),
        next_run_time=datetime.now(timezone.utc),  # also fires immediately at startup
        id="hourly-checks",
        max_instances=1,
        coalesce=True,
    )
    if history.enabled and HISTORY_RETENTION_DAYS > 0:
        scheduler.add_job(
            lambda: history.prune(HISTORY_RETENTION_DAYS),
            trigger=IntervalTrigger(seconds=86400),
            id="history-prune",
            max_instances=1,
            coalesce=True,
        )
    scheduler.start()
    app.state.scheduler = scheduler
    log.info("scheduler started; interval=%ds", CHECK_INTERVAL_SECONDS)
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="VFB status", lifespan=lifespan)
templates = Jinja2Templates(directory="app/templates")


def _service_row(state: State, svc: ServiceSpec) -> dict[str, Any]:
    result = state.results.get(svc.name) or CheckResult(
        name=svc.name, group=svc.group, url=svc.url, status="unknown"
    )
    u24, n24 = state.history.uptime_pct(svc.name, 24 * 3600)
    u7, n7 = state.history.uptime_pct(svc.name, 7 * 86400)
    u30, n30 = state.history.uptime_pct(svc.name, 30 * 86400)
    return {
        "result": result,
        "buckets": state.history.buckets(svc.name, HISTORY_BUCKETS, HISTORY_BUCKET_SECONDS),
        "uptime_24h": u24,
        "uptime_7d": u7,
        "uptime_30d": u30,
        "n_24h": n24,
        "n_7d": n7,
        "n_30d": n30,
    }


def _cache_chart_window_seconds() -> int:
    # Show roughly the same horizon as the status strip by default.
    return HISTORY_BUCKETS * HISTORY_BUCKET_SECONDS


def _cache_card(state: State, svc: CacheServiceSpec) -> dict[str, Any]:
    containers = state.cache_results.get(svc.name, [])  # list[CacheCheck]
    # Cluster-level totals from this probe's data.
    ok_containers = [c for c in containers if c.ok]
    def _sum(attr: str) -> int | None:
        vals = [getattr(c, attr) for c in ok_containers if getattr(c, attr) is not None]
        return sum(vals) if vals else None
    summary = {
        "total": _sum("cache_total"),
        "hit": _sum("cache_hit"),
        "miss": _sum("cache_miss"),
        "active": _sum("conn_active"),
        "reading": _sum("conn_reading"),
        "writing": _sum("conn_writing"),
        "waiting": _sum("conn_waiting"),
    }
    summary["hit_rate"] = (
        100.0 * summary["hit"] / summary["total"]
        if summary["total"] and summary["hit"] is not None
        else None
    )
    summary["container_count"] = len(containers)
    summary["ok_count"] = len(ok_containers)
    # Series: aggregate across all containers per ts (sum).
    series = state.history.cache_series(svc.name, _cache_chart_window_seconds(), max_points=120)
    active_pts = [(r["ts"], r["conn_active"]) for r in series if r["conn_active"] is not None]
    rate_pts: list[tuple[int, int]] = []
    if len(series) >= 2:
        prev_total = None
        for r in series:
            t = r["cache_total"]
            if prev_total is not None and t is not None:
                delta = max(0, t - prev_total)
                rate_pts.append((r["ts"], delta))
            prev_total = t
    return {
        "spec": svc,
        "containers": containers,
        "summary": summary,
        "series": series,
        "active_pts": active_pts,
        "rate_pts": rate_pts,
    }


def _neo4j_card(state: State, svc: Neo4jServiceSpec) -> dict[str, Any]:
    containers = state.neo4j_results.get(svc.name, [])
    ok_containers = [c for c in containers if c.ok]
    counts = [c.node_count for c in ok_containers if c.node_count is not None]
    statuses = sorted({c.db_status for c in containers if c.db_status})
    summary = {
        "container_count": len(containers),
        "ok_count": len(ok_containers),
        "node_count_max": max(counts) if counts else None,
        "node_count_min": min(counts) if counts else None,
        "node_count_mixed": len(set(counts)) > 1,
        "db_statuses": statuses,
        "db_status_display": statuses[0] if len(statuses) == 1 else (
            " / ".join(statuses) if statuses else None
        ),
        "any_db_error": next(
            (c.db_error for c in containers if c.db_error), None
        ),
        "avg_latency_ms": (
            int(sum(c.latency_ms for c in ok_containers if c.latency_ms is not None)
                / max(1, sum(1 for c in ok_containers if c.latency_ms is not None)))
            if any(c.latency_ms is not None for c in ok_containers) else None
        ),
    }
    series = state.history.neo4j_series(
        svc.name, _cache_chart_window_seconds(), max_points=120
    )
    count_pts = [(r["ts"], r["node_count"]) for r in series if r["node_count"] is not None]
    return {
        "spec": svc,
        "containers": containers,
        "summary": summary,
        "series": series,
        "count_pts": count_pts,
    }


def _app_card(state: State, svc: AppServiceSpec) -> dict[str, Any]:
    containers = state.app_results.get(svc.name, [])
    ok_containers = [c for c in containers if c.ok]

    def _sum(attr: str) -> int | None:
        vals = [getattr(c, attr) for c in ok_containers if getattr(c, attr) is not None]
        return sum(vals) if vals else None

    # For per-container max-config fields (workers / max_concurrent /
    # max_queue_depth) the sum across the cluster is the meaningful figure.
    versions = sorted({c.q_version for c in ok_containers if c.q_version})
    summary = {
        "container_count": len(containers),
        "ok_count": len(ok_containers),
        "versions": versions,
        "version_mixed": len(versions) > 1,
        "version_display": versions[0] if len(versions) == 1 else (
            " / ".join(versions) if versions else None
        ),
        "active": _sum("q_active"),
        "waiting": _sum("q_waiting"),
        "total_served": _sum("q_total_served"),
        "cache_hits": _sum("q_cache_hits"),
        "cache_size": _sum("q_cache_size"),
        "coalesced_total": _sum("q_coalesced_total"),
        "coalesced_in_flight": _sum("q_coalesced_in_flight"),
        "scanner_blocked": _sum("q_scanner_blocked"),
        "workers_total": _sum("q_workers"),
        "max_concurrent_total": _sum("q_max_concurrent"),
        "max_queue_depth_total": _sum("q_max_queue_depth"),
    }
    if summary["max_concurrent_total"] and summary["active"] is not None:
        summary["concurrency_pct"] = (
            100.0 * summary["active"] / summary["max_concurrent_total"]
        )
    else:
        summary["concurrency_pct"] = None
    if summary["max_queue_depth_total"] and summary["waiting"] is not None:
        summary["queue_pct"] = (
            100.0 * summary["waiting"] / summary["max_queue_depth_total"]
        )
    else:
        summary["queue_pct"] = None

    series = state.history.app_series(
        svc.name, _cache_chart_window_seconds(), max_points=120
    )
    active_pts = [(r["ts"], r["active"]) for r in series if r["active"] is not None]
    waiting_pts = [(r["ts"], r["waiting"]) for r in series if r["waiting"] is not None]
    rate_pts: list[tuple[int, int]] = []
    if len(series) >= 2:
        prev_served = None
        for r in series:
            ts_ = r["total_served"]
            if prev_served is not None and ts_ is not None:
                rate_pts.append((r["ts"], max(0, ts_ - prev_served)))
            prev_served = ts_
    return {
        "spec": svc,
        "containers": containers,
        "summary": summary,
        "series": series,
        "active_pts": active_pts,
        "waiting_pts": waiting_pts,
        "rate_pts": rate_pts,
    }


def _safe_service_row(state: State, svc: ServiceSpec) -> dict[str, Any]:
    """Wrapper that guarantees a renderable dict even if history reads throw.

    Lets the page render whatever state we do have when the SQLite layer is
    broken (e.g. NFS-backed DB with WAL corruption), instead of returning HTTP
    500 from every request.
    """
    try:
        return _service_row(state, svc)
    except Exception as exc:  # noqa: BLE001
        log.warning("_service_row failed for %s: %s", svc.name, exc)
        return {
            "result": state.results.get(svc.name) or CheckResult(
                name=svc.name, group=svc.group, url=svc.url, status="unknown",
                error=f"history read error: {exc}",
            ),
            "buckets": ["unknown"] * HISTORY_BUCKETS,
            "uptime_24h": None, "uptime_7d": None, "uptime_30d": None,
            "n_24h": 0, "n_7d": 0, "n_30d": 0,
        }


def _override_row_with_cluster(
    row: dict[str, Any], svc: ServiceSpec, cluster_hosts_by_short: dict[str, "RancherHost"]
) -> dict[str, Any]:
    """When a rancher_servers row has a matching host in the Rancher API
    cluster view, the API state is authoritative. The :5050 HTTP probe is
    kept for history + the meta column but no longer drives the pill — a
    slow or unreachable cowcheck endpoint shouldn't paint the host red when
    Rancher itself reports it active.
    """
    if not svc.group.startswith("Rancher servers"):
        return row
    short = svc.name.split(".", 1)[0].lower()
    host = cluster_hosts_by_short.get(short)
    if host is None:
        return row
    r = row["result"]
    if host.state == "active":
        # Override pill to up; preserve original error in a note for context.
        r.status = "up"
        if r.error:
            r.error = f"cowcheck :5050 — {r.error} (Rancher says host active)"
    else:
        r.status = "down"
        r.error = f"Rancher state: {host.state}"
    return row


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state: State = request.app.state.state
    cluster_hosts_by_short_for_override = (
        {h.short_name.lower(): h for h in state.cluster_result.hosts}
        if state.cluster_result and state.cluster_result.ok
        else {}
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for svc in state.services:
        row = _safe_service_row(state, svc)
        row = _override_row_with_cluster(row, svc, cluster_hosts_by_short_for_override)
        grouped.setdefault(svc.group, []).append(row)
    total = len(state.services)
    # Recompute from the (possibly cluster-overridden) rows so the headline
    # numbers match the pills shown in the rancher_servers section.
    all_rows = [r for rows in grouped.values() for r in rows]
    up = sum(1 for r in all_rows if r["result"].status == "up")
    down = sum(1 for r in all_rows if r["result"].status == "down")
    cache_cards = [_cache_card(state, s) for s in state.cache_services]
    app_cards = [_app_card(state, s) for s in state.app_services]
    neo4j_cards = [_neo4j_card(state, s) for s in state.neo4j_services]
    # Cluster overview helpers
    cluster = state.cluster_result
    cluster_hosts_by_short = (
        {h.short_name.lower(): h for h in cluster.hosts}
        if cluster and cluster.ok
        else {}
    )
    cluster_summary = None
    cluster_degraded = []
    cluster_active_lb_hosts = 0
    if cluster and cluster.ok:
        cluster_degraded = [s for s in cluster.services if s.is_degraded]
        cluster_active_lb_hosts = sum(1 for h in cluster.hosts if h.has_lb)
        active_hosts = sum(1 for h in cluster.hosts if h.state == "active")
        cluster_summary = {
            "hosts_total": len(cluster.hosts),
            "hosts_active": active_hosts,
            "lb_coverage": cluster_active_lb_hosts,
            "dns_ingress_count": sum(1 for h in cluster.hosts if h.is_dns_ingress),
            "services_total": len(cluster.services),
            "services_degraded": len(cluster_degraded),
            "stacks": list(RANCHER_STACKS),
        }
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "groups": grouped,
            "last_run": state.last_run,
            "running": state.running,
            "total": total,
            "up": up,
            "down": down,
            "interval_seconds": CHECK_INTERVAL_SECONDS,
            "history_enabled": state.history.enabled,
            "history_buckets": HISTORY_BUCKETS,
            "history_bucket_seconds": HISTORY_BUCKET_SECONDS,
            "cache_cards": cache_cards,
            "app_cards": app_cards,
            "neo4j_cards": neo4j_cards,
            "cluster": cluster,
            "cluster_summary": cluster_summary,
            "cluster_hosts_by_short": cluster_hosts_by_short,
            "cluster_degraded": cluster_degraded,
        },
    )


@app.get("/api/status")
async def api_status(request: Request) -> JSONResponse:
    state: State = request.app.state.state
    return JSONResponse(state.to_dict())


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/refresh")
async def refresh(request: Request) -> JSONResponse:
    """Force an immediate re-probe of every service."""
    state: State = request.app.state.state
    await state.run_checks()
    return JSONResponse({"last_run": state.last_run, "count": len(state.results)})


@app.get("/api/history")
async def api_history(request: Request, service: str, limit: int = 200) -> JSONResponse:
    """Recent history rows for a single service. Newest first."""
    state: State = request.app.state.state
    if not state.history.enabled:
        raise HTTPException(status_code=404, detail="history is disabled")
    if not any(s.name == service for s in state.services):
        raise HTTPException(status_code=404, detail=f"unknown service: {service}")
    return JSONResponse(
        {
            "service": service,
            "rows": state.history.recent(service, limit=max(1, min(limit, 5000))),
        }
    )


@app.get("/api/cache")
async def api_cache(request: Request) -> JSONResponse:
    """Latest snapshot per cache service. When a cache has multiple
    containers (Rancher enumeration), `containers` lists each one and
    `summary` is the cluster sum.
    """
    state: State = request.app.state.state
    out = []
    for svc in state.cache_services:
        containers = state.cache_results.get(svc.name, [])
        per_container = [
            {
                "container": c.container,
                "ok": c.ok,
                "checked_at": c.checked_at,
                "error": c.error,
                "status_url": c.status_url,
                "nginx_healthy": c.nginx_healthy,
                "upstream_healthy": c.upstream_healthy,
                "upstream_host": c.upstream_host,
                "upstream_port": c.upstream_port,
                "cache_total": c.cache_total,
                "cache_hit": c.cache_hit,
                "cache_miss": c.cache_miss,
                "hit_rate": c.hit_rate,
                "conn_active": c.conn_active,
                "conn_reading": c.conn_reading,
                "conn_writing": c.conn_writing,
                "conn_waiting": c.conn_waiting,
            }
            for c in containers
        ]
        ok = [c for c in containers if c.ok]
        def _s(attr: str) -> int | None:
            vals = [getattr(c, attr) for c in ok if getattr(c, attr) is not None]
            return sum(vals) if vals else None
        total = _s("cache_total")
        hit = _s("cache_hit")
        summary = {
            "container_count": len(containers),
            "ok_count": len(ok),
            "cache_total": total,
            "cache_hit": hit,
            "cache_miss": _s("cache_miss"),
            "hit_rate": (100.0 * hit / total) if (total and hit is not None) else None,
            "conn_active": _s("conn_active"),
            "conn_reading": _s("conn_reading"),
            "conn_writing": _s("conn_writing"),
            "conn_waiting": _s("conn_waiting"),
        }
        out.append(
            {
                "service": svc.name,
                "status_url": svc.status_url,
                "fronts": svc.fronts,
                "rancher_enabled": svc.rancher is not None,
                "summary": summary,
                "containers": per_container,
            }
        )
    return JSONResponse({"caches": out})


@app.get("/api/cluster")
async def api_cluster(request: Request) -> JSONResponse:
    """Rancher cluster overview — hosts (incl. LB coverage + DNS ingress flag)
    and active services in the configured stacks.
    """
    state: State = request.app.state.state
    r = state.cluster_result
    if r is None:
        return JSONResponse({"ok": False, "error": "no cluster probe has completed yet"})
    return JSONResponse(
        {
            "ok": r.ok,
            "error": r.error,
            "fetched_at": r.fetched_at,
            "stacks_watched": list(RANCHER_STACKS),
            "dns_hostname": RANCHER_DNS_HOSTNAME,
            "hosts": [
                {
                    "id": h.id,
                    "hostname": h.hostname,
                    "short_name": h.short_name,
                    "state": h.state,
                    "agent_state": h.agent_state,
                    "agent_ip": h.agent_ip,
                    "is_dns_ingress": h.is_dns_ingress,
                    "has_lb": h.has_lb,
                }
                for h in r.hosts
            ],
            "services": [
                {
                    "id": s.id,
                    "name": s.name,
                    "stack": s.stack,
                    "type": s.type,
                    "state": s.state,
                    "health_state": s.health_state,
                    "scale": s.scale,
                    "current_scale": s.current_scale,
                    "degraded": s.is_degraded,
                }
                for s in r.services
            ],
        }
    )


@app.get("/api/neo4j")
async def api_neo4j(request: Request) -> JSONResponse:
    """Latest snapshot per Neo4j service. When the service has multiple
    containers (Rancher enumeration), `containers` lists each one with its
    own probe outcome and `summary` is the cluster aggregate.
    """
    state: State = request.app.state.state
    out = []
    for svc in state.neo4j_services:
        containers = state.neo4j_results.get(svc.name, [])
        per_container = [
            {
                "container": c.container,
                "ok": c.ok,
                "checked_at": c.checked_at,
                "base_url": c.base_url,
                "latency_ms": c.latency_ms,
                "db_status": c.db_status,
                "db_error": c.db_error,
                "node_count": c.node_count,
                "error": c.error,
            }
            for c in containers
        ]
        ok = [c for c in containers if c.ok]
        counts = [c.node_count for c in ok if c.node_count is not None]
        statuses = sorted({c.db_status for c in containers if c.db_status})
        summary = {
            "container_count": len(containers),
            "ok_count": len(ok),
            "node_count_max": max(counts) if counts else None,
            "node_count_min": min(counts) if counts else None,
            "node_count_mixed": len(set(counts)) > 1,
            "db_statuses": statuses,
        }
        out.append(
            {
                "service": svc.name,
                "base_url": svc.base_url,
                "db": svc.db,
                "fronts": svc.fronts,
                "min_nodes": svc.min_nodes,
                "rancher_enabled": svc.rancher is not None,
                "summary": summary,
                "containers": per_container,
            }
        )
    return JSONResponse({"neo4j": out})


@app.get("/api/neo4j/history")
async def api_neo4j_history(
    request: Request, service: str, since_seconds: int = 86400, max_points: int = 200
) -> JSONResponse:
    state: State = request.app.state.state
    if not state.history.enabled:
        raise HTTPException(status_code=404, detail="history is disabled")
    if not any(n.name == service for n in state.neo4j_services):
        raise HTTPException(status_code=404, detail=f"unknown neo4j service: {service}")
    return JSONResponse(
        {
            "service": service,
            "since_seconds": since_seconds,
            "series": state.history.neo4j_series(
                service, max(60, since_seconds), max_points=max(2, min(max_points, 2000))
            ),
        }
    )


@app.get("/api/app")
async def api_app(request: Request) -> JSONResponse:
    """Latest snapshot per app service. When a service has multiple containers
    (Rancher enumeration), `containers` lists each one and `summary` is the
    cluster-wide aggregate.
    """
    state: State = request.app.state.state
    out = []
    for svc in state.app_services:
        containers = state.app_results.get(svc.name, [])
        per_container = [
            {
                "container": c.container,
                "ok": c.ok,
                "checked_at": c.checked_at,
                "error": c.error,
                "status_url": c.status_url,
                "q_status": c.q_status,
                "version": c.q_version,
                "workers": c.q_workers,
                "max_concurrent": c.q_max_concurrent,
                "max_queue_depth": c.q_max_queue_depth,
                "active": c.q_active,
                "waiting": c.q_waiting,
                "total_served": c.q_total_served,
                "cache_size": c.q_cache_size,
                "cache_hits": c.q_cache_hits,
                "coalesced_total": c.q_coalesced_total,
                "coalesced_in_flight": c.q_coalesced_in_flight,
                "scanner_probes_blocked": c.q_scanner_blocked,
                "solr_cache_enabled": c.q_solr_cache_enabled,
                "queue_pct": c.queue_pct,
                "concurrency_pct": c.concurrency_pct,
            }
            for c in containers
        ]
        ok = [c for c in containers if c.ok]
        def _s(attr: str) -> int | None:
            vals = [getattr(c, attr) for c in ok if getattr(c, attr) is not None]
            return sum(vals) if vals else None
        versions = sorted({c.q_version for c in ok if c.q_version})
        summary = {
            "container_count": len(containers),
            "ok_count": len(ok),
            "versions": versions,
            "version_mixed": len(versions) > 1,
            "version_display": versions[0] if len(versions) == 1 else (
                " / ".join(versions) if versions else None
            ),
            "active": _s("q_active"),
            "waiting": _s("q_waiting"),
            "total_served": _s("q_total_served"),
            "cache_hits": _s("q_cache_hits"),
            "cache_size": _s("q_cache_size"),
            "workers_total": _s("q_workers"),
            "max_concurrent_total": _s("q_max_concurrent"),
            "max_queue_depth_total": _s("q_max_queue_depth"),
        }
        out.append(
            {
                "service": svc.name,
                "shape": svc.shape,
                "status_url": svc.status_url,
                "fronts": svc.fronts,
                "rancher_enabled": svc.rancher is not None,
                "summary": summary,
                "containers": per_container,
            }
        )
    return JSONResponse({"apps": out})


@app.get("/api/app/history")
async def api_app_history(
    request: Request, service: str, since_seconds: int = 86400, max_points: int = 200
) -> JSONResponse:
    state: State = request.app.state.state
    if not state.history.enabled:
        raise HTTPException(status_code=404, detail="history is disabled")
    if not any(a.name == service for a in state.app_services):
        raise HTTPException(status_code=404, detail=f"unknown app service: {service}")
    return JSONResponse(
        {
            "service": service,
            "since_seconds": since_seconds,
            "series": state.history.app_series(
                service, max(60, since_seconds), max_points=max(2, min(max_points, 2000))
            ),
        }
    )


@app.get("/api/cache/history")
async def api_cache_history(
    request: Request, service: str, since_seconds: int = 86400, max_points: int = 200
) -> JSONResponse:
    """Down-sampled time series for one cache service. Oldest first."""
    state: State = request.app.state.state
    if not state.history.enabled:
        raise HTTPException(status_code=404, detail="history is disabled")
    if not any(c.name == service for c in state.cache_services):
        raise HTTPException(status_code=404, detail=f"unknown cache service: {service}")
    return JSONResponse(
        {
            "service": service,
            "since_seconds": since_seconds,
            "series": state.history.cache_series(
                service, max(60, since_seconds), max_points=max(2, min(max_points, 2000))
            ),
        }
    )


@app.get("/api/uptime")
async def api_uptime(request: Request) -> JSONResponse:
    """Per-service uptime % over 24h / 7d / 30d windows."""
    state: State = request.app.state.state
    out = []
    for svc in state.services:
        u24, n24 = state.history.uptime_pct(svc.name, 24 * 3600)
        u7, n7 = state.history.uptime_pct(svc.name, 7 * 86400)
        u30, n30 = state.history.uptime_pct(svc.name, 30 * 86400)
        out.append(
            {
                "service": svc.name,
                "group": svc.group,
                "uptime_24h": u24,
                "uptime_7d": u7,
                "uptime_30d": u30,
                "n_24h": n24,
                "n_7d": n7,
                "n_30d": n30,
            }
        )
    return JSONResponse({"history_enabled": state.history.enabled, "services": out})
