"""VFB service uptime tracker.

Single-file FastAPI app. Loads endpoint definitions from config/services.yml,
probes each one on a schedule (hourly by default) and on /refresh, and renders
a status page that the user can reload manually.

Design notes
------------
- One container, no external DB. State lives in memory; if you want persistence
  across restarts, mount a volume at /data and set STATE_FILE=/data/state.json.
- Concurrent probing: per-request timeout * fan-out via asyncio.gather.
- Probing is a GET, follows redirects, and considers a service "up" when the
  status code is in `expect_status` AND (if `expect` is set) the response body
  contains the substring.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
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
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vfb-status")

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config/services.yml"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "")) if os.environ.get("STATE_FILE") else None
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", str(60 * 60)))
USER_AGENT = "vfb-status/1.0 (+https://github.com/VirtualFlyBrain/vfb-status)"


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


# ----- app state --------------------------------------------------------------


class State:
    def __init__(self, services: list[ServiceSpec]):
        self.services = services
        self.results: dict[str, CheckResult] = {}
        self.last_run: str | None = None
        self.running = False
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
                up = sum(1 for r in results if r.status == "up")
                log.info("checks complete: %d/%d up", up, len(results))
            finally:
                self.running = False


# ----- FastAPI wiring ---------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    services = load_services(CONFIG_PATH)
    state = State(services)
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
    scheduler.start()
    app.state.scheduler = scheduler
    log.info("scheduler started; interval=%ds", CHECK_INTERVAL_SECONDS)
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="VFB status", lifespan=lifespan)
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    state: State = request.app.state.state
    grouped: dict[str, list[CheckResult]] = {}
    for svc in state.services:
        result = state.results.get(svc.name) or CheckResult(
            name=svc.name, group=svc.group, url=svc.url, status="unknown"
        )
        grouped.setdefault(svc.group, []).append(result)
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
