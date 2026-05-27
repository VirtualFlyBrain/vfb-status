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

# History storage on the mounted volume. Empty / unset disables history.
HISTORY_DB = Path(os.environ.get("HISTORY_DB", "")) if os.environ.get("HISTORY_DB") else None
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
RANCHER_TIMEOUT = float(os.environ.get("RANCHER_TIMEOUT", "5"))


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

    # vfbquery shape
    q_status: str | None = None
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


def load_app_services(path: Path) -> list[AppServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    out: list[AppServiceSpec] = []
    for svc in raw.get("app_services", []) or []:
        out.append(
            AppServiceSpec(
                name=svc["name"],
                status_url=svc["status_url"],
                shape=svc.get("shape", "vfbquery"),
                fronts=svc.get("fronts"),
                timeout=float(svc.get("timeout", 8.0)),
                verify_tls=bool(svc.get("verify_tls", True)),
            )
        )
    if out:
        log.info("loaded %d app services from %s", len(out), path)
    return out


def load_cache_services(path: Path) -> list[CacheServiceSpec]:
    raw = yaml.safe_load(path.read_text())
    out: list[CacheServiceSpec] = []
    for svc in raw.get("cache_services", []) or []:
        out.append(
            CacheServiceSpec(
                name=svc["name"],
                status_url=svc["status_url"],
                fronts=svc.get("fronts"),
                timeout=float(svc.get("timeout", 8.0)),
                verify_tls=bool(svc.get("verify_tls", True)),
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


async def probe_cache(
    client_verify: httpx.AsyncClient,
    client_no_verify: httpx.AsyncClient,
    svc: CacheServiceSpec,
) -> CacheCheck:
    """Fetch /status JSON. Return a CacheCheck — never raises."""
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client = client_verify if svc.verify_tls else client_no_verify
    try:
        resp = await client.get(
            svc.status_url,
            timeout=svc.timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        if resp.status_code != 200:
            return CacheCheck(
                name=svc.name,
                status_url=svc.status_url,
                ok=False,
                checked_at=started,
                error=f"HTTP {resp.status_code}",
            )
        try:
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            return CacheCheck(
                name=svc.name,
                status_url=svc.status_url,
                ok=False,
                checked_at=started,
                error=f"invalid JSON: {exc}",
            )
        health = data.get("health") or {}
        upstream = data.get("upstream") or {}
        cache = data.get("cache") or {}
        conns = data.get("connections") or {}
        return CacheCheck(
            name=svc.name,
            status_url=svc.status_url,
            ok=True,
            checked_at=data.get("updated_at") or started,
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
            raw=resp.text,
        )
    except httpx.TimeoutException:
        return CacheCheck(
            name=svc.name,
            status_url=svc.status_url,
            ok=False,
            checked_at=started,
            error=f"timeout after {svc.timeout}s",
        )
    except Exception as exc:  # noqa: BLE001
        return CacheCheck(
            name=svc.name,
            status_url=svc.status_url,
            ok=False,
            checked_at=started,
            error=f"{type(exc).__name__}: {exc}",
        )


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
        return await asyncio.gather(*[probe_cache(cv, cnv, s) for s in services])


async def probe_app(
    client_verify: httpx.AsyncClient,
    client_no_verify: httpx.AsyncClient,
    svc: AppServiceSpec,
) -> AppCheck:
    """Fetch an application service's /status JSON. Never raises."""
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    client = client_verify if svc.verify_tls else client_no_verify
    try:
        resp = await client.get(
            svc.status_url,
            timeout=svc.timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        if resp.status_code != 200:
            return AppCheck(
                name=svc.name, status_url=svc.status_url, shape=svc.shape,
                ok=False, checked_at=started, error=f"HTTP {resp.status_code}",
            )
        try:
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            return AppCheck(
                name=svc.name, status_url=svc.status_url, shape=svc.shape,
                ok=False, checked_at=started, error=f"invalid JSON: {exc}",
            )

        if svc.shape == "vfbquery":
            solr_cache = data.get("solr_cache") or {}
            return AppCheck(
                name=svc.name,
                status_url=svc.status_url,
                shape=svc.shape,
                ok=True,
                checked_at=started,
                q_status=data.get("status"),
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
                raw=resp.text,
            )

        # Unknown shape — record raw only.
        return AppCheck(
            name=svc.name, status_url=svc.status_url, shape=svc.shape,
            ok=True, checked_at=started, raw=resp.text,
        )
    except httpx.TimeoutException:
        return AppCheck(
            name=svc.name, status_url=svc.status_url, shape=svc.shape,
            ok=False, checked_at=started, error=f"timeout after {svc.timeout}s",
        )
    except Exception as exc:  # noqa: BLE001
        return AppCheck(
            name=svc.name, status_url=svc.status_url, shape=svc.shape,
            ok=False, checked_at=started, error=f"{type(exc).__name__}: {exc}",
        )


async def probe_all_apps(services: Iterable[AppServiceSpec]) -> list[AppCheck]:
    if not services:
        return []
    async with httpx.AsyncClient(verify=True) as cv, httpx.AsyncClient(verify=False) as cnv:
        return await asyncio.gather(*[probe_app(cv, cnv, s) for s in services])


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

    CREATE TABLE IF NOT EXISTS app_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service TEXT NOT NULL,
        shape TEXT NOT NULL,
        ts INTEGER NOT NULL,
        checked_at TEXT NOT NULL,
        ok INTEGER NOT NULL,
        error TEXT,
        q_status TEXT,
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
    """

    def __init__(self, path: Path | None) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(path),
                check_same_thread=False,
                isolation_level=None,  # autocommit; we handle transactions
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(self.SCHEMA)
            log.info("history db at %s", path)

    @property
    def enabled(self) -> bool:
        return self._conn is not None

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
        if removed_h or removed_c or removed_a:
            log.info(
                "history: pruned %d service + %d cache + %d app rows older than %dd",
                removed_h, removed_c, removed_a, retention_days,
            )
        return removed_h + removed_c + removed_a

    def record_cache(self, results: Iterable[CacheCheck]) -> None:
        if self._conn is None:
            return
        rows = [
            (
                r.name,
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
                "(service, ts, checked_at, ok, nginx_healthy, upstream_healthy, "
                " upstream_host, upstream_port, cache_total, cache_hit, cache_miss, "
                " conn_active, conn_reading, conn_writing, conn_waiting, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def cache_series(
        self, service: str, since_seconds: int, max_points: int = 96
    ) -> list[dict[str, Any]]:
        """Return up to `max_points` evenly-down-sampled rows for the given cache service.

        Newest first becomes inconvenient for line drawing, so we return oldest-first.
        """
        if self._conn is None:
            return []
        cutoff = int(time.time()) - since_seconds
        cur = self._conn.execute(
            "SELECT ts, ok, nginx_healthy, upstream_healthy, "
            "cache_total, cache_hit, cache_miss, "
            "conn_active, conn_reading, conn_writing, conn_waiting "
            "FROM cache_history WHERE service = ? AND ts >= ? ORDER BY ts ASC",
            (service, cutoff),
        )
        rows = cur.fetchall()
        if not rows:
            return []
        # Even down-sample for the chart — never more than max_points.
        step = max(1, len(rows) // max_points)
        sampled = rows[::step]
        return [
            {
                "ts": ts,
                "ok": bool(ok),
                "nginx_healthy": (None if nh is None else bool(nh)),
                "upstream_healthy": (None if uh is None else bool(uh)),
                "cache_total": ct,
                "cache_hit": ch,
                "cache_miss": cm,
                "conn_active": ca,
                "conn_reading": cr,
                "conn_writing": cw,
                "conn_waiting": cwt,
            }
            for ts, ok, nh, uh, ct, ch, cm, ca, cr, cw, cwt in sampled
        ]

    def cache_latest(self, service: str) -> dict[str, Any] | None:
        if self._conn is None:
            return None
        cur = self._conn.execute(
            "SELECT ts, checked_at, ok, nginx_healthy, upstream_healthy, "
            "upstream_host, upstream_port, cache_total, cache_hit, cache_miss, "
            "conn_active, conn_reading, conn_writing, conn_waiting, error "
            "FROM cache_history WHERE service = ? ORDER BY ts DESC LIMIT 1",
            (service,),
        )
        row = cur.fetchone()
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
        we have no data in the window.
        """
        if self._conn is None:
            return None, 0
        cutoff = int(time.time()) - since_seconds
        cur = self._conn.execute(
            "SELECT status, COUNT(*) FROM history WHERE service = ? AND ts >= ? GROUP BY status",
            (service, cutoff),
        )
        counts = {row[0]: row[1] for row in cur.fetchall()}
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
        cur = self._conn.execute(
            "SELECT ts, status FROM history "
            "WHERE service = ? AND ts >= ? ORDER BY ts ASC",
            (service, start),
        )
        out = ["unknown"] * n_buckets
        for ts, status in cur.fetchall():
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
                int(time.time()),
                r.checked_at,
                1 if r.ok else 0,
                r.error,
                r.q_status,
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
                "(service, shape, ts, checked_at, ok, error, "
                " q_status, q_workers, q_max_concurrent, q_max_queue_depth, "
                " q_active, q_waiting, q_total_served, q_cache_size, q_cache_hits, "
                " q_coalesced_total, q_coalesced_in_flight, q_scanner_blocked, "
                " q_solr_cache_enabled) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    def app_series(
        self, service: str, since_seconds: int, max_points: int = 120
    ) -> list[dict[str, Any]]:
        if self._conn is None:
            return []
        cutoff = int(time.time()) - since_seconds
        cur = self._conn.execute(
            "SELECT ts, ok, q_active, q_waiting, q_total_served, q_cache_hits, "
            "q_coalesced_in_flight, q_coalesced_total "
            "FROM app_history WHERE service = ? AND ts >= ? ORDER BY ts ASC",
            (service, cutoff),
        )
        rows = cur.fetchall()
        if not rows:
            return []
        step = max(1, len(rows) // max_points)
        return [
            {
                "ts": ts,
                "ok": bool(ok),
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
        cur = self._conn.execute(
            "SELECT checked_at, status, http_status, latency_ms, error "
            "FROM history WHERE service = ? ORDER BY ts DESC LIMIT ?",
            (service, limit),
        )
        return [
            {
                "checked_at": ca,
                "status": st,
                "http_status": hs,
                "latency_ms": lm,
                "error": err,
            }
            for ca, st, hs, lm, err in cur.fetchall()
        ]

    def services(self) -> list[str]:
        if self._conn is None:
            return []
        cur = self._conn.execute("SELECT DISTINCT service FROM history ORDER BY service")
        return [row[0] for row in cur.fetchall()]


# ----- app state --------------------------------------------------------------


class State:
    def __init__(
        self,
        services: list[ServiceSpec],
        cache_services: list[CacheServiceSpec] | None = None,
        app_services: list[AppServiceSpec] | None = None,
        history: History | None = None,
    ):
        self.services = services
        self.cache_services = cache_services or []
        self.app_services = app_services or []
        self.results: dict[str, CheckResult] = {}
        self.cache_results: dict[str, CacheCheck] = {}
        self.app_results: dict[str, AppCheck] = {}
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
                results, cache_results, app_results = await asyncio.gather(
                    probe_all(self.services),
                    probe_all_caches(self.cache_services),
                    probe_all_apps(self.app_services),
                )
                for r in results:
                    self.results[r.name] = r
                for cr in cache_results:
                    self.cache_results[cr.name] = cr
                for ar in app_results:
                    self.app_results[ar.name] = ar
                self.last_run = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self.persist()
                if self.history.enabled:
                    try:
                        self.history.record(results)
                        if cache_results:
                            self.history.record_cache(cache_results)
                        if app_results:
                            self.history.record_app(app_results)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("history write failed: %s", exc)
                up = sum(1 for r in results if r.status == "up")
                cache_ok = sum(1 for c in cache_results if c.ok)
                app_ok = sum(1 for a in app_results if a.ok)
                log.info(
                    "checks complete: %d/%d up, %d/%d caches ok, %d/%d apps ok",
                    up, len(results), cache_ok, len(cache_results), app_ok, len(app_results),
                )
            finally:
                self.running = False


# ----- FastAPI wiring ---------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    services = load_services(CONFIG_PATH) + rancher_server_specs()
    cache_services = load_cache_services(CONFIG_PATH)
    app_services = load_app_services(CONFIG_PATH)
    history = History(HISTORY_DB)
    history.prune(HISTORY_RETENTION_DAYS)
    state = State(
        services,
        cache_services=cache_services,
        app_services=app_services,
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
    latest = state.cache_results.get(svc.name)
    series = state.history.cache_series(svc.name, _cache_chart_window_seconds(), max_points=120)
    # Pre-compute SVG-ready normalised points for conn_active (the load indicator)
    # and cache_total *delta per step* (request-rate proxy).
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
        "latest": latest,
        "series": series,
        "active_pts": active_pts,
        "rate_pts": rate_pts,
    }


def _app_card(state: State, svc: AppServiceSpec) -> dict[str, Any]:
    latest = state.app_results.get(svc.name)
    series = state.history.app_series(
        svc.name, _cache_chart_window_seconds(), max_points=120
    )
    active_pts = [(r["ts"], r["active"]) for r in series if r["active"] is not None]
    waiting_pts = [(r["ts"], r["waiting"]) for r in series if r["waiting"] is not None]
    rate_pts: list[tuple[int, int]] = []
    cache_rate_pts: list[tuple[int, int]] = []
    if len(series) >= 2:
        prev_served = None
        prev_hits = None
        for r in series:
            ts_ = r["total_served"]
            ch = r["cache_hits"]
            if prev_served is not None and ts_ is not None:
                rate_pts.append((r["ts"], max(0, ts_ - prev_served)))
            if prev_hits is not None and ch is not None:
                cache_rate_pts.append((r["ts"], max(0, ch - prev_hits)))
            prev_served = ts_
            prev_hits = ch
    return {
        "spec": svc,
        "latest": latest,
        "series": series,
        "active_pts": active_pts,
        "waiting_pts": waiting_pts,
        "rate_pts": rate_pts,
        "cache_rate_pts": cache_rate_pts,
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state: State = request.app.state.state
    grouped: dict[str, list[dict[str, Any]]] = {}
    for svc in state.services:
        grouped.setdefault(svc.group, []).append(_service_row(state, svc))
    total = len(state.services)
    up = sum(1 for r in state.results.values() if r.status == "up")
    down = sum(1 for r in state.results.values() if r.status == "down")
    cache_cards = [_cache_card(state, s) for s in state.cache_services]
    app_cards = [_app_card(state, s) for s in state.app_services]
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
    """Latest snapshot per cache service."""
    state: State = request.app.state.state
    out = []
    for svc in state.cache_services:
        latest = state.cache_results.get(svc.name)
        out.append(
            {
                "service": svc.name,
                "status_url": svc.status_url,
                "fronts": svc.fronts,
                "ok": latest.ok if latest else None,
                "checked_at": latest.checked_at if latest else None,
                "error": latest.error if latest else None,
                "nginx_healthy": latest.nginx_healthy if latest else None,
                "upstream_healthy": latest.upstream_healthy if latest else None,
                "upstream_host": latest.upstream_host if latest else None,
                "upstream_port": latest.upstream_port if latest else None,
                "cache_total": latest.cache_total if latest else None,
                "cache_hit": latest.cache_hit if latest else None,
                "cache_miss": latest.cache_miss if latest else None,
                "hit_rate": latest.hit_rate if latest else None,
                "conn_active": latest.conn_active if latest else None,
                "conn_reading": latest.conn_reading if latest else None,
                "conn_writing": latest.conn_writing if latest else None,
                "conn_waiting": latest.conn_waiting if latest else None,
            }
        )
    return JSONResponse({"caches": out})


@app.get("/api/app")
async def api_app(request: Request) -> JSONResponse:
    """Latest snapshot per app service."""
    state: State = request.app.state.state
    out = []
    for svc in state.app_services:
        l = state.app_results.get(svc.name)
        out.append(
            {
                "service": svc.name,
                "shape": svc.shape,
                "status_url": svc.status_url,
                "fronts": svc.fronts,
                "ok": l.ok if l else None,
                "checked_at": l.checked_at if l else None,
                "error": l.error if l else None,
                "q_status": l.q_status if l else None,
                "workers": l.q_workers if l else None,
                "max_concurrent": l.q_max_concurrent if l else None,
                "max_queue_depth": l.q_max_queue_depth if l else None,
                "active": l.q_active if l else None,
                "waiting": l.q_waiting if l else None,
                "total_served": l.q_total_served if l else None,
                "cache_size": l.q_cache_size if l else None,
                "cache_hits": l.q_cache_hits if l else None,
                "coalesced_total": l.q_coalesced_total if l else None,
                "coalesced_in_flight": l.q_coalesced_in_flight if l else None,
                "scanner_probes_blocked": l.q_scanner_blocked if l else None,
                "solr_cache_enabled": l.q_solr_cache_enabled if l else None,
                "queue_pct": l.queue_pct if l else None,
                "concurrency_pct": l.concurrency_pct if l else None,
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
