"""Cache status after a VFB release: what has been refreshed, what is running, and when it should finish.

A release is only "done" for users once the caches behind it have caught up: the pdb reload and rollout, the
Owlery restart, the vfb_json precompute and then the VFBquery / owl cache warm-ups. This module reads the
Kubernetes cluster (read-only, with the pod's service account) and builds one timeline per release.

Two parts, kept apart so the logic can be tested without a cluster:

* ``build_status(raw, now)`` is pure: raw Argo workflows / Jobs / Deployments / ReplicaSets in, a JSON-able
  dict out (current release, per-stage state, predicted times, past releases).
* ``collect_raw()`` / ``get_status()`` talk to the Kubernetes API and Solr with a short cache.

Predictions are honest estimates, not promises. A stage's duration is the median of that stage's earlier runs
in the cluster (Argo keeps them); a stage with no history uses a default from the environment and is marked
as such. Stages the cluster does not record (the owl / v3-cached nginx warm-ups are run by hand) are shown as
"not tracked" rather than guessed.
"""

from __future__ import annotations

import asyncio
import logging
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger("vfb-status.cache-status")

SA_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")
KUBE_TIMEOUT = float(os.environ.get("CACHE_STATUS_KUBE_TIMEOUT", "20"))
CACHE_TTL_SECONDS = float(os.environ.get("CACHE_STATUS_TTL", "60"))
SOLR_URL = os.environ.get("CACHE_STATUS_SOLR_URL", "https://solr.k8s.virtualflybrain.org/solr").rstrip("/")
PIPELINE_NS = os.environ.get("CACHE_STATUS_PIPELINE_NS", "pipeline")
HISTORY_RUNS = int(os.environ.get("CACHE_STATUS_HISTORY_RUNS", "5"))


@dataclass(frozen=True)
class Stage:
    key: str
    label: str
    kind: str  # workflow | job | deployment | manual
    match: str  # name prefix (workflow / job) or "namespace/name" (deployment)
    after: tuple[str, ...]  # stages that must finish first
    default_minutes: float | None  # used when there is no history
    note: str = ""


# Order is the display order. `after` is the dependency used for predictions: a stage cannot start before the
# stages it follows have finished. Owlery restarts alongside the pdb reload; the cache warm-ups need both the
# new vfb_json and the new Owlery.
STAGES: tuple[Stage, ...] = (
    Stage("release", "Release promoted (Solr alias, public OWL, archive)", "workflow", "vfb-release-", (), 8),
    Stage("pdb-load", "pdb data reload", "job", "pdb/pdb-loader-", ("release",), 30),
    Stage("pdb-rollout", "pdb + pdb-analytics restart", "deployment", "pdb/pdb", ("pdb-load",), 45,
          "The data volumes are cloned from the loaded copy, which dominates the time."),
    Stage("owlery", "Owlery restart", "deployment", "owlery/owlery", ("release",), 60,
          "Pods roll in small surges, about 15-20 min each."),
    Stage("vfb_json", "vfb_json precompute (in place)", "workflow", "vfb-json-precompute-",
          ("pdb-rollout",), None, "Must finish before the VFBquery cache is refreshed."),
    Stage("vfbquery_cache", "VFBquery cache refresh (in place)", "workflow", "vfbquery-force-refresh",
          ("vfb_json", "owlery"), None,
          "Entries are overwritten in place; the old ones keep being served meanwhile."),
    Stage("owl-caches", "owl / v3-cached nginx warm-ups", "manual", "", ("vfb_json", "owlery"), None,
          "Run by hand with owlery-cache-reload, so the cluster does not record it."),
)
STAGE_BY_KEY = {s.key: s for s in STAGES}


# ---------------------------------------------------------------------------------------------------------------
# helpers


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds") if dt else None


def _default_minutes(stage: Stage) -> float | None:
    env = os.environ.get("CACHE_STATUS_DEFAULT_MIN_" + stage.key.upper().replace("-", "_"))
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return stage.default_minutes


@dataclass
class Run:
    """One observed run of a stage (a workflow, a Job or a deployment rollout)."""

    name: str
    started: datetime | None
    finished: datetime | None
    phase: str  # running | succeeded | failed | pending

    @property
    def duration(self) -> timedelta | None:
        if self.started and self.finished:
            return self.finished - self.started
        return None


def _workflow_runs(workflows: list[dict[str, Any]], prefix: str) -> list[Run]:
    out: list[Run] = []
    for wf in workflows:
        name = wf.get("metadata", {}).get("name", "")
        if not name.startswith(prefix):
            continue
        st = wf.get("status", {})
        phase = (st.get("phase") or "Pending").lower()
        phase = {"succeeded": "succeeded", "running": "running", "failed": "failed", "error": "failed"}.get(
            phase, "pending")
        out.append(Run(name, parse_ts(st.get("startedAt")) or parse_ts(wf["metadata"].get("creationTimestamp")),
                       parse_ts(st.get("finishedAt")), phase))
    out.sort(key=lambda r: r.started or datetime.min.replace(tzinfo=timezone.utc))
    return out


def _job_runs(jobs: list[dict[str, Any]], ns_prefix: str) -> list[Run]:
    ns, _, prefix = ns_prefix.partition("/")
    out: list[Run] = []
    for job in jobs:
        meta = job.get("metadata", {})
        if meta.get("namespace") != ns or not meta.get("name", "").startswith(prefix):
            continue
        st = job.get("status", {})
        started = parse_ts(st.get("startTime")) or parse_ts(meta.get("creationTimestamp"))
        finished = parse_ts(st.get("completionTime"))
        if finished:
            phase = "succeeded"
        elif st.get("failed") and not st.get("active"):
            phase = "failed"
        else:
            phase = "running" if st.get("active") or started else "pending"
        out.append(Run(meta.get("name", ""), started, finished, phase))
    out.sort(key=lambda r: r.started or datetime.min.replace(tzinfo=timezone.utc))
    return out


def _deployment_runs(deployments: list[dict[str, Any]], replicasets: list[dict[str, Any]],
                     ns_name: str) -> list[Run]:
    """The latest rollout of a deployment: starts when its newest ReplicaSet was created, ends when the
    Progressing condition reports NewReplicaSetAvailable on a fully updated, fully ready deployment."""
    ns, _, name = ns_name.partition("/")
    dep = next((d for d in deployments if d["metadata"].get("namespace") == ns and d["metadata"]["name"] == name),
               None)
    if not dep:
        return []
    owned = [
        rs for rs in replicasets
        if rs["metadata"].get("namespace") == ns
        and any(o.get("kind") == "Deployment" and o.get("name") == name
                for o in rs["metadata"].get("ownerReferences", []))
    ]
    owned.sort(key=lambda rs: rs["metadata"].get("creationTimestamp", ""))
    started = parse_ts(owned[-1]["metadata"].get("creationTimestamp")) if owned else None
    st = dep.get("status", {})
    want = dep.get("spec", {}).get("replicas", 1)
    settled = (
        st.get("observedGeneration", 0) >= dep["metadata"].get("generation", 0)
        and st.get("updatedReplicas", 0) == want
        and st.get("readyReplicas", 0) == want
        and st.get("replicas", 0) == want
    )
    finished = None
    for cond in st.get("conditions", []):
        if cond.get("type") == "Progressing" and cond.get("reason") == "NewReplicaSetAvailable" and settled:
            finished = parse_ts(cond.get("lastUpdateTime"))
    failed = any(c.get("type") == "Progressing" and c.get("status") == "False" for c in st.get("conditions", []))
    phase = "succeeded" if finished else ("failed" if failed else "running")
    return [Run(name, started, finished, phase)]


def _runs_for(stage: Stage, raw: dict[str, Any]) -> list[Run]:
    if stage.kind == "workflow":
        return _workflow_runs(raw.get("workflows", []), stage.match)
    if stage.kind == "job":
        return _job_runs(raw.get("jobs", []), stage.match)
    if stage.kind == "deployment":
        return _deployment_runs(raw.get("deployments", []), raw.get("replicasets", []), stage.match)
    return []


def _median_minutes(runs: list[Run]) -> tuple[float | None, int]:
    done = [r.duration.total_seconds() / 60 for r in runs if r.phase == "succeeded" and r.duration]
    done = done[-HISTORY_RUNS:]
    if not done:
        return None, 0
    return statistics.median(done), len(done)


def _release_runs(raw: dict[str, Any]) -> list[Run]:
    return [r for r in _runs_for(STAGE_BY_KEY["release"], raw) if r.phase == "succeeded" and r.finished]


def _latest_in_window(runs: list[Run], t0: datetime, before: datetime | None) -> Run | None:
    """The newest run of a stage that started after release time t0 and before the next release. A failed
    run followed by a retry therefore shows the retry; a lone failed run shows as failed."""
    found = [r for r in runs if r.started and r.started >= t0 - timedelta(minutes=1)
             and (before is None or r.started < before)]
    return found[-1] if found else None


# ---------------------------------------------------------------------------------------------------------------
# the pure builder


def build_status(raw: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    releases = _release_runs(raw)
    if not releases:
        return {"ok": True, "now": iso(now), "release": None, "stages": [], "history": [],
                "message": "No completed release found in the cluster's workflow history."}

    per_stage_runs = {s.key: _runs_for(s, raw) for s in STAGES}
    medians = {s.key: _median_minutes(per_stage_runs[s.key]) for s in STAGES}

    def timeline(idx: int, live: bool) -> dict[str, Any]:
        rel = releases[idx]
        t0 = rel.finished
        nxt = releases[idx + 1].started if idx + 1 < len(releases) else None
        stages: dict[str, dict[str, Any]] = {}
        for s in STAGES:
            if s.key == "release":
                run: Run | None = rel
            elif s.kind == "manual":
                run = None
            elif s.kind == "deployment":
                # only the latest rollout is visible, so only the newest release can show it
                r = per_stage_runs[s.key][0] if per_stage_runs[s.key] else None
                run = r if (live and r and r.started and r.started >= t0 - timedelta(minutes=1)) else None
            else:
                run = _latest_in_window(per_stage_runs[s.key], t0, nxt)
            st: dict[str, Any] = {"key": s.key, "label": s.label, "note": s.note, "after": list(s.after),
                                  "kind": s.kind, "run": run.name if run else None}
            if run:
                st.update(state=run.phase, started=iso(run.started), finished=iso(run.finished),
                          duration_min=round(run.duration.total_seconds() / 60, 1) if run.duration else None)
            else:
                st.update(state="not-tracked" if s.kind == "manual" else "pending", started=None,
                          finished=None, duration_min=None)
            stages[s.key] = st

        if live:
            _predict(stages, medians, now)
        done_ends = [parse_ts(stages[k]["finished"]) for k in ("vfb_json", "vfbquery_cache", "owlery")
                     if stages[k]["state"] == "succeeded" and stages[k]["finished"]]
        return {
            "run": rel.name.removeprefix("vfb-release-"),
            "released_at": iso(t0),
            "stages": [stages[s.key] for s in STAGES],
            "vfb_json_done_after_min": (
                round((parse_ts(stages["vfb_json"]["finished"]) - t0).total_seconds() / 60)
                if stages["vfb_json"]["state"] == "succeeded" and stages["vfb_json"]["finished"] else None),
            "_done_ends": done_ends,
        }

    current = timeline(len(releases) - 1, live=True)
    history = [timeline(i, live=False) for i in range(len(releases) - 2, max(len(releases) - 2 - HISTORY_RUNS, -1), -1)]
    for h in history:
        h.pop("_done_ends", None)
    current.pop("_done_ends", None)

    # headline: when are the caches expected to be fully caught up?
    tracked = [s for s in current["stages"] if s["state"] != "not-tracked"]
    ends = [parse_ts(s.get("predicted_finish") or s.get("finished")) for s in tracked]
    unknown = any(e is None for e in ends)
    all_done = all(s["state"] == "succeeded" for s in tracked)
    known = [e for e in ends if e]
    headline = {
        "all_tracked_done": all_done,
        "predicted_all_done": None if (unknown or not ends) else iso(max(known)),
        # the latest finish among stages that do have an estimate (a lower bound when others have none)
        "predicted_known_done": iso(max(known)) if known else None,
        "unknown_stages": [s["key"] for s in tracked if not (s.get("predicted_finish") or s.get("finished"))],
    }
    return {
        "ok": True,
        "now": iso(now),
        "release": current,
        "headline": headline,
        "history": history,
        "durations": {k: {"median_min": round(m, 1) if m is not None else None, "runs": n}
                      for k, (m, n) in medians.items()},
        "solr": raw.get("solr", {}),
        "kube_error": raw.get("kube_error"),
    }


def _predict(stages: dict[str, dict[str, Any]], medians: dict[str, tuple[float | None, int]],
             now: datetime) -> None:
    """Fill predicted_start / predicted_finish / basis for every stage that has not finished."""
    finish: dict[str, datetime | None] = {}
    for s in STAGES:  # STAGES is in dependency order
        st = stages[s.key]
        if st["state"] == "succeeded":
            finish[s.key] = parse_ts(st["finished"])
            continue
        if st["state"] == "not-tracked":
            finish[s.key] = None
            continue
        med, n = medians[s.key]
        if med is not None:
            minutes, basis = med, f"median of {n} earlier run{'s' if n != 1 else ''}"
        else:
            minutes, basis = _default_minutes(s), "default estimate (no earlier runs)"
        st["basis"] = basis if minutes is not None else "no estimate (no earlier runs)"
        if st["state"] == "failed":
            finish[s.key] = None
            st["basis"] = "failed: no prediction"
            continue
        if st["state"] == "running" and st.get("started"):
            started = parse_ts(st["started"])
            if minutes is None:
                finish[s.key] = None
                continue
            predicted = started + timedelta(minutes=minutes)
            st["overrunning"] = predicted < now
            if st["overrunning"]:
                # past its usual time: still running, so finish is "soon", never in the past
                predicted = now
            st["predicted_finish"] = iso(predicted)
            finish[s.key] = predicted
            continue
        # pending: starts when everything it follows has finished
        dep_ends = [finish.get(a) for a in s.after]
        if any(d is None for d in dep_ends) or minutes is None:
            finish[s.key] = None
            continue
        start = max([now] + [d for d in dep_ends if d])
        st["predicted_start"] = iso(start)
        st["predicted_finish"] = iso(start + timedelta(minutes=minutes))
        finish[s.key] = start + timedelta(minutes=minutes)


# ---------------------------------------------------------------------------------------------------------------
# cluster access (in-cluster service account, read-only)


def _kube_client() -> tuple[httpx.AsyncClient, str] | None:
    token_file = SA_DIR / "token"
    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    if not token_file.exists() or not host:
        return None
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    client = httpx.AsyncClient(
        base_url=f"https://{host}:{port}",
        headers={"Authorization": f"Bearer {token_file.read_text().strip()}"},
        verify=str(SA_DIR / "ca.crt"),
        timeout=KUBE_TIMEOUT,
    )
    return client, host


async def _list(client: httpx.AsyncClient, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    resp = await client.get(path, params=params or {})
    resp.raise_for_status()
    return resp.json().get("items", [])


async def _solr_counts() -> dict[str, Any]:
    out: dict[str, Any] = {}
    async with httpx.AsyncClient(timeout=15) as client:
        for coll in ("vfb_json", "vfbquery_cache", "ontology"):
            try:
                r = await client.get(f"{SOLR_URL}/{coll}/select", params={"q": "*:*", "rows": 0, "wt": "json"})
                r.raise_for_status()
                out[coll] = r.json()["response"]["numFound"]
            except Exception as exc:  # noqa: BLE001
                out[coll] = None
                log.warning("solr count for %s failed: %s", coll, exc)
    return out


async def collect_raw() -> dict[str, Any]:
    raw: dict[str, Any] = {"workflows": [], "jobs": [], "deployments": [], "replicasets": []}
    kc = _kube_client()
    if kc is None:
        raw["kube_error"] = "not running inside the cluster (no service account): no workflow data"
    else:
        client, _ = kc
        try:
            async with client:
                wf, jobs, dep_pdb, dep_owl, rs_pdb, rs_owl = await asyncio.gather(
                    _list(client, f"/apis/argoproj.io/v1alpha1/namespaces/{PIPELINE_NS}/workflows"),
                    _list(client, "/apis/batch/v1/namespaces/pdb/jobs"),
                    _list(client, "/apis/apps/v1/namespaces/pdb/deployments"),
                    _list(client, "/apis/apps/v1/namespaces/owlery/deployments"),
                    _list(client, "/apis/apps/v1/namespaces/pdb/replicasets"),
                    _list(client, "/apis/apps/v1/namespaces/owlery/replicasets"),
                )
            raw["workflows"] = wf
            raw["jobs"] = jobs
            raw["deployments"] = dep_pdb + dep_owl
            raw["replicasets"] = rs_pdb + rs_owl
        except Exception as exc:  # noqa: BLE001
            log.warning("kubernetes read failed: %s", exc)
            raw["kube_error"] = f"kubernetes API read failed: {exc}"
    raw["solr"] = await _solr_counts()
    return raw


_cache: dict[str, Any] = {"at": 0.0, "value": None}
_lock = asyncio.Lock()


async def get_status(force: bool = False) -> dict[str, Any]:
    async with _lock:
        if not force and _cache["value"] is not None and time.monotonic() - _cache["at"] < CACHE_TTL_SECONDS:
            return _cache["value"]
        raw = await collect_raw()
        value = build_status(raw)
        value["kube_error"] = raw.get("kube_error")
        _cache.update(at=time.monotonic(), value=value)
        return value
