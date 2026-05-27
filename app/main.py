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
        removed = cur.rowcount or 0
        if removed:
            log.info("history: pruned %d rows older than %dd", removed, retention_days)
        return removed

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
    def __init__(self, services: list[ServiceSpec], history: History | None = None):
        self.services = services
        self.results: dict[str, CheckResult] = {}
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
                log.info("probing %d services", len(self.services))
                results = await probe_all(self.services)
                for r in results:
                    self.results[r.name] = r
                self.last_run = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self.persist()
                if self.history.enabled:
                    try:
                        self.history.record(results)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("history write failed: %s", exc)
                up = sum(1 for r in results if r.status == "up")
                log.info("checks complete: %d/%d up", up, len(results))
            finally:
                self.running = False


# ----- FastAPI wiring ---------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    services = load_services(CONFIG_PATH) + rancher_server_specs()
    history = History(HISTORY_DB)
    history.prune(HISTORY_RETENTION_DAYS)
    state = State(services, history=history)
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


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state: State = request.app.state.state
    grouped: dict[str, list[dict[str, Any]]] = {}
    for svc in state.services:
        grouped.setdefault(svc.group, []).append(_service_row(state, svc))
    total = len(state.services)
    up = sum(1 for r in state.results.values() if r.status == "up")
    down = sum(1 for r in state.results.values() if r.status == "down")
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
