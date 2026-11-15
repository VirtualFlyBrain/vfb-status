"""Tests for the pure part of app/cache_status.py (no cluster needed). Run: python -m pytest tests -q"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app import cache_status as cs  # noqa: E402


def wf(name, start, end=None, phase="Succeeded"):
    st = {"phase": phase, "startedAt": start}
    if end:
        st["finishedAt"] = end
    return {"metadata": {"name": name}, "status": st}


def dep(ns, name, gen, ready, want, started_rs, done_at=None, restart=True):
    """A deployment and its newest ReplicaSet. restart=True: made by `kubectl rollout restart` (the annotation
    matches the creation time); restart=False: a manifest change, which keeps an older annotation."""
    conds = [{"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable" if done_at else "ReplicaSetUpdated",
              "lastUpdateTime": done_at or started_rs}]
    return ({"metadata": {"namespace": ns, "name": name, "generation": gen},
             "spec": {"replicas": want},
             "status": {"observedGeneration": gen, "updatedReplicas": ready, "readyReplicas": ready,
                        "replicas": want if ready == want else want + 1, "conditions": conds}},
            {"metadata": {"namespace": ns, "name": name + "-abc", "creationTimestamp": started_rs,
                          "ownerReferences": [{"kind": "Deployment", "name": name}]},
             "spec": {"template": {"metadata": {"annotations": {
                 "kubectl.kubernetes.io/restartedAt": started_rs if restart else "2026-10-01T00:00:00Z"}}}}})


def job(name, start, end=None):
    st = {"startTime": start}
    if end:
        st["completionTime"] = end
    else:
        st["active"] = 1
    return {"metadata": {"namespace": "pdb", "name": name, "creationTimestamp": start}, "status": st}


NOW = datetime(2026, 11, 9, 16, 0, tzinfo=timezone.utc)


def base_raw():
    return {
        "workflows": [
            wf("vfb-release-20261027", "2026-10-28T18:09:28Z", "2026-10-28T18:14:47Z"),
            wf("vfb-json-precompute-20261027-x", "2026-10-28T19:23:20Z", "2026-10-29T09:54:39Z"),
            wf("vfb-release-20261105", "2026-11-06T23:48:05Z", "2026-11-06T23:54:05Z"),
            wf("vfb-json-precompute-20261105", "2026-11-07T01:08:53Z", "2026-11-07T19:57:25Z"),
            wf("vfb-release-20261108", "2026-11-09T10:05:40Z", "2026-11-09T10:11:43Z"),
            wf("vfb-json-precompute-20261108", "2026-11-09T15:31:18Z", None, "Running"),
            wf("vfb-data-release-20261108", "2026-11-08T04:54:40Z", "2026-11-09T02:05:07Z"),
        ],
        "jobs": [job("pdb-loader-202611091020", "2026-11-09T10:20:00Z", "2026-11-09T10:47:00Z")],
        "deployments": [], "replicasets": [],
    }


def test_no_release():
    out = cs.build_status({"workflows": []}, NOW)
    assert out["release"] is None


def test_current_release_and_history():
    out = cs.build_status(base_raw(), NOW)
    assert out["release"]["run"] == "20261108"
    assert [h["run"] for h in out["history"]] == ["20261105", "20261027"]
    # release 20261105 finished 2026-11-06 23:54:05, its precompute 2026-11-07 19:57:25 -> 20 h 03 min
    assert out["history"][0]["vfb_json_done_after_min"] == 1203


def test_running_stage_prediction_uses_median():
    out = cs.build_status(base_raw(), NOW)
    stages = {s["key"]: s for s in out["release"]["stages"]}
    vj = stages["vfb_json"]
    assert vj["state"] == "running"
    # median of 14.5 h and 18.8 h earlier runs, started 15:31 -> still in the future at 16:00
    assert vj["basis"].startswith("median of 2")
    assert datetime.fromisoformat(vj["predicted_finish"]) > NOW
    assert stages["pdb-load"]["state"] == "succeeded"


def test_overrunning_stage_never_predicts_in_the_past():
    raw = base_raw()
    late = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)
    out = cs.build_status(raw, late)
    vj = {s["key"]: s for s in out["release"]["stages"]}["vfb_json"]
    assert vj["overrunning"] is True
    assert datetime.fromisoformat(vj["predicted_finish"]) >= late


def test_pending_stage_waits_for_dependencies_and_untracked_is_not_guessed():
    out = cs.build_status(base_raw(), NOW)
    stages = {s["key"]: s for s in out["release"]["stages"]}
    assert stages["owl-caches"]["state"] == "not-tracked"
    # vfbquery_cache has no history and no default: no prediction, and the headline says so
    assert stages["vfbquery_cache"].get("predicted_finish") is None
    assert "vfbquery_cache" in out["headline"]["unknown_stages"]
    assert out["headline"]["predicted_all_done"] is None


def test_deployment_rollout_states():
    d, rs = dep("owlery", "owlery", 3, 4, 4, "2026-11-09T10:50:00Z", "2026-11-09T12:30:00Z")
    raw = base_raw()
    raw["deployments"], raw["replicasets"] = [d], [rs]
    st = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["owlery"]
    assert st["state"] == "succeeded" and st["duration_min"] == 100.0

    d, rs = dep("owlery", "owlery", 3, 2, 4, "2026-11-09T10:50:00Z")
    raw["deployments"], raw["replicasets"] = [d], [rs]
    st = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["owlery"]
    assert st["state"] == "running"


def test_failed_run_is_reported_not_hidden():
    raw = base_raw()
    raw["workflows"][5] = wf("vfb-json-precompute-20261108", "2026-11-09T15:31:18Z", "2026-11-09T15:40:00Z", "Failed")
    st = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["vfb_json"]
    assert st["state"] == "failed"
    assert st["basis"] == "failed: no prediction"


def test_real_cluster_snapshot_2026_11_09():
    """Trimmed copy of the cluster's own objects during release 20261108 (see tests/fixtures)."""
    import json
    raw = json.loads((Path(__file__).parent / "fixtures" / "cluster_2026-11-09.json").read_text())
    out = cs.build_status(raw, datetime(2026, 11, 9, 18, 0, tzinfo=timezone.utc))
    stages = {s["key"]: s for s in out["release"]["stages"]}
    assert out["release"]["run"] == "20261108"
    assert stages["pdb-load"]["duration_min"] == 26.1
    assert stages["pdb-rollout"]["state"] == "succeeded" and stages["pdb-rollout"]["duration_min"] == 43.4
    assert stages["owlery"]["state"] == "succeeded"
    assert stages["vfb_json"]["state"] == "running"
    # the stopped 20261030 precompute must not count towards the median (only the 2 completed runs do)
    assert stages["vfb_json"]["basis"] == "median of 2 earlier runs"
    runs = {h["run"]: h for h in out["history"]}
    assert runs["20261030"]["vfb_json_done_after_min"] is None
    assert [s["state"] for s in runs["20261030"]["stages"] if s["key"] == "vfb_json"] == ["failed"]


def test_manifest_change_rollout_is_not_a_release_restart():
    """0.16.0 counted any rollout after the release: a config change on 13 Nov showed as release 20261109's
    pdb restart. Only a `rollout restart` ReplicaSet counts."""
    d, rs = dep("owlery", "owlery", 3, 4, 4, "2026-11-09T10:50:00Z", "2026-11-09T12:30:00Z", restart=False)
    raw = base_raw()
    raw["deployments"], raw["replicasets"] = [d], [rs]
    st = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["owlery"]
    assert st["state"] == "pending" and st["run"] is None


def test_superseded_restart_is_done_without_a_finish_time():
    d, rs_new = dep("owlery", "owlery", 4, 4, 4, "2026-11-09T14:00:00Z", "2026-11-09T14:20:00Z", restart=False)
    _, rs_old = dep("owlery", "owlery", 3, 4, 4, "2026-11-09T10:50:00Z")
    rs_old["metadata"]["name"] = "owlery-old"
    raw = base_raw()
    raw["deployments"], raw["replicasets"] = [d], [rs_old, rs_new]
    st = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["owlery"]
    assert st["state"] == "succeeded" and st["started"].startswith("2026-11-09T10:50") and st["finished"] is None


def test_pending_becomes_no_record_after_the_grace_period():
    raw = base_raw()
    raw["jobs"] = []  # the loader Job has been deleted (ttlSecondsAfterFinished)
    soon = {s["key"]: s for s in cs.build_status(raw, NOW)["release"]["stages"]}["pdb-load"]
    assert soon["state"] == "pending" and soon.get("predicted_finish")  # 6 h after the release: still plausible
    late_now = datetime(2026, 11, 15, 14, 0, tzinfo=timezone.utc)
    out = cs.build_status(raw, late_now)
    late = {s["key"]: s for s in out["release"]["stages"]}["pdb-load"]
    assert late["state"] == "no-record" and late.get("predicted_finish") is None
    assert "pdb-load" in out["headline"]["no_record_stages"]
    assert out["headline"]["predicted_all_done"] is None  # no estimate that slides forward for ever


def test_real_cluster_snapshot_2026_11_15():
    """Release 20261109 six days on. The loader Job is gone, pdb was restarted for the release on 9 Nov 23:56
    and rolled again for a config change on 13 Nov (must not count), and Owlery was last restarted before the
    release was promoted."""
    import json
    raw = json.loads((Path(__file__).parent / "fixtures" / "cluster_2026-11-15.json").read_text())
    out = cs.build_status(raw, datetime(2026, 11, 15, 14, 12, tzinfo=timezone.utc))
    stages = {s["key"]: s for s in out["release"]["stages"]}
    assert out["release"]["run"] == "20261109"
    assert stages["pdb-load"]["state"] == "no-record"
    assert stages["pdb-rollout"]["state"] == "succeeded"
    assert stages["pdb-rollout"]["started"].startswith("2026-11-09T23:56") and stages["pdb-rollout"]["finished"] is None
    assert stages["owlery"]["state"] == "no-record"
    assert stages["vfb_json"]["state"] == "succeeded" and stages["vfbquery_cache"]["state"] == "succeeded"
    assert out["headline"]["all_tracked_done"] is False
    assert out["headline"]["no_record_stages"] == ["pdb-load", "owlery"]
    assert out["headline"]["predicted_all_done"] is None
    hist = {h["run"]: {s["key"]: s for s in h["stages"]} for h in out["history"]}
    assert hist["20261108"]["owlery"]["started"].startswith("2026-11-09T15:29")
    assert all(s["state"] != "pending" for h in hist.values() for s in h.values())
