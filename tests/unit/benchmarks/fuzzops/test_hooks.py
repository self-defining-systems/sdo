"""SDO's fuzzops-bench hooks: baseline manifest, check, render comparison, idle verdict and install arguments."""

from __future__ import annotations

import json
import signal
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from benchmarks.fuzzops import hooks
from benchmarks.fuzzops.config import BENCH_PROFILE
from benchmarks.fuzzops.hooks import (
    Baseline,
    HookError,
    Kube,
    OperatorBlock,
    SourceBranch,
    idle_reason,
    idle_verdict,
    install_existing_launch,
    plain_docs,
    plain_pack,
    render_difference,
    source_branch_up,
    write_baseline,
)

COMMIT = "a" * 40


def _git(repo: Path, *args: str) -> str:
    base = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@invalid"]
    return subprocess.run([*base, *args], check=True, capture_output=True, text=True).stdout.strip()


def _bundle(tmp: Path, branch: str = "baseline") -> tuple[Path, str]:
    repo = tmp / "memory-repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", branch)
    (repo / "README.md").write_text("synthetic\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "synthetic memory")
    bundle = tmp / "baseline" / "memory.bundle"
    bundle.parent.mkdir(parents=True, exist_ok=True)
    _git(repo, "bundle", "create", str(bundle), branch)
    return bundle, _git(repo, "rev-parse", "HEAD")


RENDER = """\
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: ns1
spec:
  ports: [{port: 80}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  strategy:
    rollingUpdate: null
  template:
    spec:
      containers: [{name: web, image: "example/web:1"}]
---
apiVersion: batch/v1
kind: Job
metadata:
  name: migrate
---
"""


def _baseline_dir(tmp: Path, namespace: str = "ns1", **fields: Any) -> Path:
    bundle, commit = _bundle(tmp)
    root = bundle.parent
    (root / "render.yaml").write_text(RENDER)
    write_baseline(
        root,
        Baseline(
            namespace=namespace,
            bundle="memory.bundle",
            branch="baseline",
            commit=commit,
            render="render.yaml",
            objective="users are served",
            config={"response": {"timeout": "1800s"}},
            **fields,
        ),
    )
    return root


def _operator_file(tmp: Path, baseline: Path, **settings: Any) -> Path:
    path = tmp / "operator.json"
    path.write_text(
        json.dumps({"hooks": "sdo", "revision": "0123456789abcdef", "baseline": str(baseline), "settings": settings})
    )
    return path


# ---- baseline manifest ---------------------------------------------------------------------------------------


def test_baseline_manifest_round_trips_with_the_render_digest(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    loaded = Baseline.load(root)
    assert loaded.control == "ns1-sdo"
    assert loaded.app_name == "ns1"
    assert loaded.config == {"response": {"timeout": "1800s"}}
    assert len(loaded.render_sha256) == 64
    raw = json.loads((root / "baseline.json").read_text())
    assert raw["format"] == 2
    assert "controller_flags" not in raw


def _format_1(root: Path, flags: list[str]) -> None:
    raw = json.loads((root / "baseline.json").read_text())
    del raw["config"]
    raw.update(format=1, controller_flags=flags)
    (root / "baseline.json").write_text(json.dumps(raw))


def test_a_format_1_baseline_translates_its_controller_flags_to_config(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    _format_1(
        root,
        [
            "--response-timeout",
            "1800s",
            "--verification-timeout=300s",
            "--max-response-loops",
            "10",
            "--repair-policy",
            "recorded-actions",
            "--reflection-guidance",
            "baseline",
            "--healthy-baseline",
            "--responder-namespace",
            "a",
            "--responder-namespace",
            "b",
            "--allow-registry",
            "r1,r2",
            "--frozen-memory",
            "--self-heal-hold",
            "60s",
            "--closure-stability-window=15s",
            "--drift-batch-window",
            "20s",
        ],
    )
    loaded = Baseline.load(root)
    assert loaded.format == 2
    assert loaded.config == {
        "mode": "frozen",
        "detection": {"self_heal_hold": "60s", "stability_window": "15s", "drift_batch_window": "20s"},
        "response": {
            "timeout": "1800s",
            "verification_timeout": "300s",
            "max_loops": 10,
            "repair_policy": "recorded-actions",
            "permissive_namespaces": ["a", "b"],
        },
        "learning": {"reflection_guidance": "baseline", "healthy_baseline": True},
        "deployment": {"policy": {"allowed_registries": ["r1", "r2"]}},
    }
    assert json.loads(loaded.to_json())["format"] == 2


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--self-heal-window", "60s"], "--self-heal-window has no operator config key"),
        (["--model", "x"], "--model has no operator config key"),
        (["--kubeconfig=/k"], "set by the hook"),
        (["--watch-only"], "require --frozen-memory"),
        (["--max-response-loops"], "needs a value"),
    ],
)
def test_a_format_1_baseline_with_an_untranslatable_flag_is_refused(
    tmp_path: Path, flags: list[str], message: str
) -> None:
    root = _baseline_dir(tmp_path)
    _format_1(root, flags)
    with pytest.raises(ValueError, match=message):
        Baseline.load(root)


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"commit": "abc"}, "40 hex"),
        ({"bundle": "../memory.bundle"}, "inside the baseline"),
        ({"config": {"agent": {"model": "x"}}}, "agent.model, which the hook sets"),
        ({"config": {"evidence": {"run_id": "r"}}}, "evidence, which the hook sets"),
        ({"format": 1}, "format 1 is not 2"),
        ({"namespace": ""}, "namespace"),
        ({"control_namespace": "ns1"}, "differ"),
    ],
)
def test_baseline_manifest_rejects_bad_fields(fields: dict[str, Any], message: str) -> None:
    values: dict[str, Any] = {
        "namespace": "ns1",
        "bundle": "memory.bundle",
        "branch": "baseline",
        "commit": COMMIT,
        "render": "render.yaml",
    }
    values.update(fields)
    with pytest.raises(ValueError, match=message):
        Baseline(**values)


# ---- check ---------------------------------------------------------------------------------------------------


def _check(tmp: Path, baseline: Path, namespace: str = "ns1") -> tuple[int, str]:
    env = {"FUZZOPS_NAMESPACE": namespace, "BENCH_OPERATOR": str(_operator_file(tmp, baseline))}
    lines: list[str] = []
    rc = hooks.check(env, out=lines.append)
    return rc, "\n".join(lines)


def test_check_accepts_a_well_formed_baseline(tmp_path: Path) -> None:
    rc, out = _check(tmp_path, _baseline_dir(tmp_path))
    assert rc == 0, out


def test_check_refuses_another_namespace(tmp_path: Path) -> None:
    rc, out = _check(tmp_path, _baseline_dir(tmp_path), namespace="ns2")
    assert rc == 1
    assert "ns1" in out
    assert "ns2" in out
    assert len(out.splitlines()) == 1


def test_check_refuses_a_bundle_without_the_commit(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    raw = json.loads((root / "baseline.json").read_text())
    raw["commit"] = COMMIT
    (root / "baseline.json").write_text(json.dumps(raw))
    rc, out = _check(tmp_path, root)
    assert rc == 1
    assert "bundle" in out


def test_check_refuses_a_changed_render(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    (root / "render.yaml").write_text(RENDER + "# edited\n")
    rc, out = _check(tmp_path, root)
    assert (rc, "sha256" in out) == (1, True)


def test_check_refuses_a_missing_manifest(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    rc, out = _check(tmp_path, tmp_path / "empty")
    assert rc == 1
    assert "baseline.json" in out


# ---- operator block ------------------------------------------------------------------------------------------


def test_operator_block_defaults_the_image_tag_from_the_revision(tmp_path: Path) -> None:
    block = OperatorBlock.load(_operator_file(tmp_path, tmp_path, model="m"))
    assert block.image_tag == "fz-01234567"
    assert block.images() == {
        "controller": "sdo-controller:fz-01234567",
        "responder": "sdo-responder:fz-01234567",
        "script": "sdo-script-tools:fz-01234567",
    }
    assert block.provider == "codex"


def test_operator_block_requires_sdo_hooks(tmp_path: Path) -> None:
    path = tmp_path / "op.json"
    path.write_text(json.dumps({"hooks": "other", "revision": "r", "baseline": "/b", "settings": {}}))
    with pytest.raises(HookError, match="sdo"):
        OperatorBlock.load(path)


# ---- render --------------------------------------------------------------------------------------------------


def test_plain_docs_drop_jobs_and_empty_docs_and_strip_namespaces() -> None:
    docs = plain_docs(RENDER)
    assert [(d["kind"], d["metadata"]["name"]) for d in docs] == [("Service", "web"), ("Deployment", "web")]
    assert all("namespace" not in d["metadata"] for d in docs)


def test_render_difference_ignores_null_keys_and_order() -> None:
    live = list(reversed(plain_docs(RENDER)))
    live[0]["spec"]["strategy"] = {}  # rollingUpdate absent instead of null
    assert render_difference(live, plain_docs(RENDER)) == ""


def test_render_difference_names_the_changed_object() -> None:
    live = plain_docs(RENDER)
    live[1]["spec"]["template"]["spec"]["containers"][0]["image"] = "example/web:2"
    why = render_difference(live, plain_docs(RENDER))
    assert "Deployment/web" in why


def test_plain_pack_renders_beside_the_pack_files(tmp_path: Path) -> None:
    pack = tmp_path / "helm-pack"
    (pack / "faults").mkdir(parents=True)
    (pack / "app.yaml").write_text("name: app\nentry: {service: web, port: 80}\njourneys: journeys.yaml\n")
    (pack / "journeys.yaml").write_text("journeys: []\n")
    (pack / "faults" / "f.yaml").write_text("x: 1\n")
    out = plain_pack(pack, tmp_path / "plain", plain_docs(RENDER))
    assert sorted(p.name for p in out.root.iterdir()) == ["app.yaml", "faults", "journeys.yaml", "manifests"]
    (manifest,) = out.manifests()
    assert [d["kind"] for d in yaml.safe_load_all(manifest.read_text())] == ["Service", "Deployment"]


def test_plain_pack_keeps_a_pack_with_manifests(tmp_path: Path) -> None:
    pack = tmp_path / "plain-pack"
    (pack / "manifests").mkdir(parents=True)
    (pack / "app.yaml").write_text("name: app\nentry: {service: web, port: 80}\n")
    (pack / "manifests" / "a.yaml").write_text(RENDER)
    assert plain_pack(pack, tmp_path / "unused", None).root == pack


# ---- idle ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "why"),
    [
        (None, "no controller state yet"),
        ({"incident_open": True}, "incident open"),
        ({"pending_closure": {"request": {}}}, "closure pending"),
        # A permanently failed closure stays in the state for evidence and never blocks the next incident.
        ({"pending_closure": {"request": {}}, "closure_state": "CLOSURE_STATE_FAILED"}, ""),
        ({"pendingClosure": {"request": {}}, "closureState": "CLOSURE_STATE_FAILED"}, ""),
        ({"queued_reflection_incident_ids": ["i"]}, "reflection queued"),
        ({"queuedReflectionsUnknown": True}, "reflection queue unknown"),
        ({"version": "sdo.controller/v1"}, ""),
        # A new install's start-up period holds until SDO has seen it healthy or the bound passes.
        ({"startup": {"began_at": "t"}}, "startup baseline pending"),
        ({"startup": {"began_at": "t", "bound_at": "t"}}, "startup baseline pending (bound t)"),
        ({"startup": {"beganAt": "t", "boundAt": "t"}}, "startup baseline pending (bound t)"),
        # The controller names what holds the period, so a gate that fails on it names the cause.
        (
            {"startup": {"began_at": "t", "bound_at": "u", "holders": ["firing: user test checkout", "batched: x"]}},
            "startup baseline pending (bound u): firing: user test checkout; batched: x",
        ),
        (
            {"startup": {"began_at": "t", "holders": ["healthy since v; established after 2m0s"]}},
            "startup baseline pending: healthy since v; established after 2m0s",
        ),  # fmt: skip
        ({"startup": {"began_at": "t", "established_at": "t"}}, ""),
        ({"startup": {"began_at": "t", "bound_reached_at": "t"}}, ""),
        ({"startup": {"skipped_reason": "restored-state"}}, ""),
    ],
)
def test_idle_reason(state: dict[str, Any] | None, why: str) -> None:
    assert idle_reason(state) == why


NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _timeline(*rules: str) -> dict[str, Any]:
    stamp = (NOW - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "version": "v",
        "detector_timeline": [
            {"detector_id": "request-metrics", "rule_id": rule, "last_seen_at": stamp, "fingerprint": f"f{i}"}
            for i, rule in enumerate(rules)
        ],
    }


def test_idle_verdict_is_blind_while_request_metrics_cannot_watch_any_operation() -> None:
    state = _timeline("request-metrics.not-instrumented", "request-metrics.not-instrumented")
    assert idle_verdict(state, NOW, True, NOW).startswith("blind:")


def test_idle_verdict_is_idle_when_some_operation_is_watched_or_none_is_declared() -> None:
    mixed = _timeline("request-metrics.not-instrumented", "request-metrics.insufficient-baseline")
    assert idle_verdict(mixed, NOW, True, NOW) == ""
    assert idle_verdict(_timeline(), NOW, True, NOW) == ""


def test_idle_verdict_is_idle_for_a_ready_fresh_quiet_controller() -> None:
    assert idle_verdict({"version": "v"}, NOW - timedelta(seconds=5), True, NOW) == ""


def test_an_open_incident_is_an_alarm_with_its_id() -> None:
    state = {"incident_open": True, "incident_request": {"incident_id": "inc-1"}}
    assert idle_verdict(state, NOW, True, NOW) == "alarm: incident open inc-1"
    # Even when the controller is also down, an open incident is reported as an alarm.
    assert idle_verdict(state, NOW, False, NOW).startswith("alarm:")


@pytest.mark.parametrize(
    ("state", "age", "ready", "prefix"),
    [
        ({"version": "v"}, 5, False, "down: controller pod not Ready"),
        (None, 5, True, "down: no controller state"),
        ({"version": "v"}, 140, True, "stale: state not updated for 140s"),
        ({"pending_closure": {"request": {}}}, 5, True, "busy: closure pending"),
        ({"queued_reflection_incident_ids": ["i"]}, 5, True, "busy: reflection queued"),
        # Waiting for a first healthy window is ordinary busy, never an SDO alarm.
        ({"startup": {"began_at": "t"}}, 5, True, "busy: startup baseline pending"),
    ],
)
def test_idle_verdict_reasons(state: dict[str, Any] | None, age: int, ready: bool, prefix: str) -> None:
    assert idle_verdict(state, NOW - timedelta(seconds=age), ready, NOW).startswith(prefix)


def _fake_kubectl(responses: dict[str, dict[str, Any]], calls: list[list[str]]) -> Any:
    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        for key, value in responses.items():
            if key in cmd:
                return subprocess.CompletedProcess(cmd, 0, json.dumps(value), "")
        return subprocess.CompletedProcess(cmd, 1, "", "not found")

    return run


def test_idle_reads_the_pod_the_state_configmap_and_the_controller_log_once_each(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    stale = "2020-01-01T00:00:00Z"
    configmap = {
        "metadata": {"managedFields": [{"time": stale}, {"time": fresh}]},
        "data": {"runtime-state.json": json.dumps({"version": "sdo.controller/v1"})},
    }
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
    calls: list[list[str]] = []
    monkeypatch.setattr(hooks.subprocess, "run", _fake_kubectl({"configmap": configmap, "pods": pods}, calls))
    env = {"FUZZOPS_NAMESPACE": "ns1", "FUZZOPS_KUBE_CONTEXT": "kind-c", "KUBECONFIG": "/k"}
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 0, lines
    # the namespace and controller Job probes, then the pod, the state ConfigMap and the log
    assert len(calls) == 5
    assert sum("logs" in c for c in calls) == 1
    assert any("--show-managed-fields" in c for c in calls)
    assert all(c[c.index("-n") + 1] == "ns1-sdo" for c in calls if "-n" in c)


def test_idle_is_idle_once_the_log_shows_every_store_in_the_state_history_promoted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first verdict has no log; it must not hold on baselines it cannot read (the log read comes after it)."""
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    state = {
        "version": "sdo.controller/v1",
        "history": [{"detector_id": "store-state-redis", "evaluated_at": fresh, "status": "clear"}],
    }
    configmap = {
        "metadata": {"managedFields": [{"time": fresh}]},
        "data": {"runtime-state.json": json.dumps(state)},
    }
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
    logs = "INFO store_baseline_promoted component=redis"
    monkeypatch.setattr(
        hooks.subprocess, "run", _fake_kubectl({"configmap": configmap, "pods": pods, "logs": logs}, [])
    )
    env = {"FUZZOPS_NAMESPACE": "ns1", "FUZZOPS_KUBE_CONTEXT": "kind-c", "KUBECONFIG": "/k"}
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 0, lines


def test_idle_gives_up_waiting_for_store_baselines_after_the_bound_and_records_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    state = {
        "version": "sdo.controller/v1",
        "history": [{"detector_id": "store-state-redis", "evaluated_at": fresh, "status": "clear"}],
    }
    configmap = {
        "metadata": {"managedFields": [{"time": fresh}]},
        "data": {"runtime-state.json": json.dumps(state)},
    }
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
    monkeypatch.setattr(
        hooks.subprocess, "run", _fake_kubectl({"configmap": configmap, "pods": pods, "logs": "no promotions"}, [])
    )
    lane, out = tmp_path / "lane", tmp_path / "out"
    lane.mkdir()
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "KUBECONFIG": "/k",
        "BENCH_LANE_DIR": str(lane),
        "BENCH_OUT": str(out),
    }
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[-1].startswith("blind: no promoted baseline yet for redis")
    started = datetime.now(UTC) - timedelta(seconds=hooks.BASELINE_WAIT_MAX_S + 5)
    (lane / hooks.BASELINE_WAIT_RECORD).write_text(json.dumps({"since": started.strftime("%Y-%m-%dT%H:%M:%SZ")}))
    assert hooks.idle(env, out=lines.append) == 0
    assert lines[-1] == "idle"
    assert "redis" in json.loads((lane / hooks.BASELINE_MISSING_RECORD).read_text())["reason"]
    assert (out / hooks.BASELINE_MISSING_RECORD).is_file()


def _unpromoted_store_lane(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **extra: str) -> dict[str, str]:
    """An otherwise idle controller whose redis store baseline is not promoted (a fault reset it), on a lane."""
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    state = {
        "version": "sdo.controller/v1",
        "history": [{"detector_id": "store-state-redis", "evaluated_at": fresh, "status": "clear"}],
    }
    configmap = {"metadata": {"managedFields": [{"time": fresh}]}, "data": {"runtime-state.json": json.dumps(state)}}
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
    monkeypatch.setattr(
        hooks.subprocess, "run", _fake_kubectl({"configmap": configmap, "pods": pods, "logs": "no promotions"}, [])
    )
    lane = tmp_path / "lane"
    lane.mkdir(exist_ok=True)
    return {
        "FUZZOPS_NAMESPACE": "ns1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "KUBECONFIG": "/k",
        "BENCH_LANE_DIR": str(lane),
        **extra,
    }


def _expire_baseline_wait(env: dict[str, str]) -> None:
    started = datetime.now(UTC) - timedelta(seconds=hooks.BASELINE_WAIT_MAX_S + 5)
    record = Path(env["BENCH_LANE_DIR"]) / hooks.BASELINE_WAIT_RECORD
    record.write_text(json.dumps({"since": started.strftime("%Y-%m-%dT%H:%M:%SZ")}))


def test_an_expired_store_baseline_wait_answers_idle_on_every_poll_until_the_run_ends(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # the bench's waits need idle twice in a row (settle) or for a span (the inject gate): an expired wait that
    # answered idle once and then restarted itself would hold them for another full wait, again and again
    env = _unpromoted_store_lane(monkeypatch, tmp_path, BENCH_RUN="r1")
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 1
    _expire_baseline_wait(env)
    assert [hooks.idle(env, out=lines.append) for _ in range(3)] == [0, 0, 0], lines
    hooks.end_run(env)
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[-1].startswith(hooks.BASELINE_BLIND)


def test_a_teardowns_idle_never_waits_for_a_store_baseline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The release asks idle before it deletes the operator, after ``collect`` ended the run, so the call looks like
    a pre-inject gate; the bench says it is for a teardown and an unpromoted store baseline no longer holds it
    (a gitlab release waited 10 to 18 minutes for a baseline nothing would use)."""
    gate = _unpromoted_store_lane(monkeypatch, tmp_path)
    lines: list[str] = []
    assert hooks.idle(gate, out=lines.append) == 1
    assert lines[-1].startswith(hooks.BASELINE_BLIND)
    teardown = {**gate, hooks.IDLE_FOR_ENV: hooks.IDLE_FOR_TEARDOWN}
    assert hooks.idle(teardown, out=lines.append) == 0, lines
    assert lines[-1] == "idle"


def _lane_with_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, state: dict[str, Any], **extra: str
) -> dict[str, str]:
    """A lane whose ready controller reports ``state`` (fresh)."""
    fresh = (datetime.now(UTC) - timedelta(seconds=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    configmap = {"metadata": {"managedFields": [{"time": fresh}]}, "data": {"runtime-state.json": json.dumps(state)}}
    pods = {"items": [{"metadata": {"name": "p"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
    monkeypatch.setattr(hooks.subprocess, "run", _fake_kubectl({"configmap": configmap, "pods": pods, "logs": ""}, []))
    lane = tmp_path / "lane"
    lane.mkdir(exist_ok=True)
    return {
        "FUZZOPS_NAMESPACE": "ns1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "KUBECONFIG": "/k",
        "BENCH_LANE_DIR": str(lane),
        **extra,
    }


def _open_incident(dispatch: str) -> dict[str, Any]:
    return {
        "version": "sdo.controller/v1",
        "incident_open": True,
        "incident_request": {"incident_id": "fg1-1"},
        "dispatch_state": dispatch,
        "escalated_at": "2026-01-01T00:00:00Z",
    }


def test_a_teardowns_idle_does_not_wait_for_an_open_incident_no_agent_is_working_on(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An escalated incident stays open until its alerts clear or a human resets it; the run's settle already waited
    the cap for it. The release's idle asks only whether work has stopped, and a finished responder is not work (a
    commit-series release waited a second two hours on an incident the stub responder had escalated)."""
    lines: list[str] = []
    gate = _lane_with_state(monkeypatch, tmp_path, _open_incident("DISPATCH_STATE_COMPLETED"))
    assert hooks.idle(gate, out=lines.append) == 1
    assert lines[-1] == "alarm: incident open fg1-1"
    teardown = {**gate, hooks.IDLE_FOR_ENV: hooks.IDLE_FOR_TEARDOWN}
    assert hooks.idle(teardown, out=lines.append) == 0, lines
    assert lines[-1] == "idle"


def test_a_settle_the_workload_ended_its_own_incident_wait_for_waits_only_for_work_in_flight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A commit-series position waits for its own incident (bounded) and records the outcome before settle runs; an
    incident still open then is a result (SDO deploys nothing while one is open, so the next position records that).
    Settle waiting for it too ended every such position ``capped`` and broke the sequence (finding 9). The workload
    leaves a record in the lane directory; the run boundary (collect) removes it."""
    lines: list[str] = []
    gate = _lane_with_state(monkeypatch, tmp_path, _open_incident("DISPATCH_STATE_COMPLETED"))
    assert hooks.idle(gate, out=lines.append) == 1
    assert lines[-1] == "alarm: incident open fg1-1"
    (Path(gate["BENCH_LANE_DIR"]) / "settle-work-only.json").write_text("{}\n")
    assert hooks.idle(gate, out=lines.append) == 0, lines
    assert lines[-1] == "idle"
    hooks.end_run(gate)
    assert hooks.idle(gate, out=lines.append) == 1


def test_a_settle_the_workload_ended_its_own_incident_wait_for_still_waits_for_a_responder_working(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lines: list[str] = []
    env = _lane_with_state(monkeypatch, tmp_path, _open_incident("DISPATCH_STATE_RUNNING"))
    (Path(env["BENCH_LANE_DIR"]) / "settle-work-only.json").write_text("{}\n")
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[-1] == "busy: responder dispatch running"


def test_a_teardowns_idle_still_waits_for_a_responder_working_on_an_open_incident(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lines: list[str] = []
    env = _lane_with_state(
        monkeypatch,
        tmp_path,
        _open_incident("DISPATCH_STATE_RUNNING"),
        **{hooks.IDLE_FOR_ENV: hooks.IDLE_FOR_TEARDOWN},
    )
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[-1] == "busy: responder dispatch running"


@pytest.mark.parametrize(
    "settle_env",
    [
        # settle with the cell's own incident known
        {"BENCH_OWN_DETECTED_AT": "2026-01-01T00:00:00Z"},
        # settle without it (no BENCH_RUN), and the window end inside the run (its own BENCH_RUN)
        {},
        {"BENCH_RUN": "r1"},
    ],
)
def test_after_the_inject_an_unpromoted_store_baseline_never_holds_idle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, settle_env: dict[str, str]
) -> None:
    gate = _unpromoted_store_lane(monkeypatch, tmp_path, BENCH_RUN="r1")
    hooks.mark_injected(gate)  # ``watch`` started for r1: the inject happened
    env = _unpromoted_store_lane(monkeypatch, tmp_path, **settle_env)
    lines: list[str] = []
    assert [hooks.idle(env, out=lines.append) for _ in range(2)] == [0, 0], lines
    assert not (tmp_path / "lane" / hooks.BASELINE_WAIT_RECORD).exists()


def test_the_next_runs_inject_gate_still_waits_for_store_baselines(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # a run that ended without its boundary verb (the bench lost it) does not exempt the next run's gate
    hooks.mark_injected(_unpromoted_store_lane(monkeypatch, tmp_path, BENCH_RUN="r1"))
    env = _unpromoted_store_lane(monkeypatch, tmp_path, BENCH_RUN="r2")
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[-1].startswith(hooks.BASELINE_BLIND)


def test_idle_reports_a_missing_controller_as_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hooks.subprocess, "run", _fake_kubectl({}, []))
    env = {"FUZZOPS_NAMESPACE": "ns1", "FUZZOPS_KUBE_CONTEXT": "kind-c", "KUBECONFIG": "/k"}
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[0].startswith("down:")


# ---- install, teardown, watch --------------------------------------------------------------------------------


SINK_ENV = {"SDO_EVIDENCE_HOST_PATH": "/bench-evidence", "SDO_EVIDENCE_RUN_ID": "e-l-1"}


def test_install_existing_launch_passes_profile_baseline_and_experiment_config_and_the_settings(
    tmp_path: Path,
) -> None:
    root = _baseline_dir(tmp_path)
    block = OperatorBlock.load(
        _operator_file(
            tmp_path,
            root,
            model="gpt-x",
            effort="medium",
            image_tag="fz-t",
            config={"response": {"max_loops": 3}},
        )
    )
    args, env = install_existing_launch(
        tmp_path / "sdo-operator",
        tmp_path / "app",
        Baseline.load(root),
        block,
        tmp_path / "kubeconfig",
        SINK_ENV,
        tmp_path / "config",
    )
    assert args[1] == "install-existing"

    def flag(name: str) -> str:
        return args[args.index(name) + 1]

    assert (flag("--namespace"), flag("--control-namespace"), flag("--application")) == ("ns1", "ns1-sdo", "ns1")
    assert flag("--controller-image") == "sdo-controller:fz-t"
    assert flag("--script-image") == "sdo-script-tools:fz-t"
    assert flag("--kubeconfig") == str(tmp_path / "kubeconfig")
    files = [Path(args[i + 1]) for i, arg in enumerate(args) if arg == "--config"]
    assert files == [BENCH_PROFILE, tmp_path / "config" / "baseline.yaml", tmp_path / "config" / "experiment.yaml"]
    assert yaml.safe_load(files[1].read_text()) == {"response": {"timeout": "1800s"}}
    assert yaml.safe_load(files[2].read_text()) == {"response": {"max_loops": 3}}
    assert env == {
        "SDO_AGENT_PROVIDER": "codex",
        "SDO_AGENT_MODEL": "gpt-x",
        "SDO_AGENT_EFFORT": "medium",
        **SINK_ENV,
    }
    assert not any(arg.startswith(("--model", "--agent-", "--evidence-", "--response-")) for arg in args)


def test_a_source_branch_setting_becomes_the_remote_sdo_deploys_from(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    branch = {
        "ref": "sdo-source",
        "up": 'echo \'{"url": "git://r/x.git", "ref": "{ref}"}\' # {namespace}',
        "down": "true",
    }
    block = OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x", source_branch=branch))
    assert block.source_branch is not None
    values = {"kubeconfig": "k", "namespace": "ns1", "control": "ns1-sdo", "image": "i", "repository": "/r"}
    url, ref = source_branch_up(block.source_branch, values, tmp_path)
    assert (url, ref) == ("git://r/x.git", "sdo-source")
    assert json.loads((tmp_path / "source-branch.json").read_text())["url"] == "git://r/x.git"
    args, _ = install_existing_launch(
        tmp_path / "sdo-operator",
        tmp_path / "app",
        Baseline.load(root),
        block,
        tmp_path / "k",
        SINK_ENV,
        tmp_path / "c",
        source_branch=(url, ref),
    )
    assert args[args.index("--source-branch-url") + 1] == "git://r/x.git"
    assert args[args.index("--source-branch") + 1] == "sdo-source"
    plain = OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x"))
    assert plain.source_branch is None


def test_source_branch_commands_can_name_the_small_script_image(tmp_path: Path) -> None:
    values = hooks.source_branch_values(
        Path("/k"), "ns1", "ns1-sdo", {"controller": "ctl:t", "script": "scr:t"}, tmp_path
    )
    command = SourceBranch("sdo-source", "run --image {script_image} --big {image}", "true").command("up", values)
    assert command == "run --image scr:t --big ctl:t"


def test_a_source_branch_cannot_be_main_and_up_must_print_its_remote(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    with pytest.raises(HookError, match="main or master"):
        OperatorBlock.load(
            _operator_file(tmp_path, root, model="m", source_branch={"ref": "main", "up": "true", "down": "true"})
        )
    silent = SourceBranch("sdo-source", "echo nothing", "true")
    with pytest.raises(HookError, match="no JSON line"):
        source_branch_up(silent, {}, tmp_path)


def test_install_existing_launch_refuses_a_lane_without_an_evidence_sink(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    block = OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x"))
    with pytest.raises(HookError, match="evidence sink"):
        install_existing_launch(
            tmp_path / "b", tmp_path / "app", Baseline.load(root), block, tmp_path / "k", {}, tmp_path / "c"
        )


def test_the_experiment_config_cannot_set_what_the_hook_sets(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    with pytest.raises(HookError, match="agent.effort"):
        OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x", config={"agent": {"effort": "low"}}))


def test_the_benchmark_profile_differs_from_production_in_a_few_documented_fields() -> None:
    assert yaml.safe_load(BENCH_PROFILE.read_text()) == {
        "agent": {"recording": False},
        "response": {
            "repair_policy": "recorded-actions",
            "after_playbook": "wait",
            "after_playbook_wait": "5m",
            "investigators": 2,
        },
        "onboarding": {"journeys_required": False, "soak": "0s"},
        "detection": {"self_heal_hold": "60s"},
    }


@pytest.mark.parametrize("missing", ["BENCH_EVIDENCE_NODE", "BENCH_EVIDENCE_RUN"])
def test_prepare_fails_before_any_work_without_the_evidence_sink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    operator = _operator_file(tmp_path, _baseline_dir(tmp_path), model="gpt-x")
    calls: list[list[str]] = []
    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: calls.append(cmd))
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "BENCH_OPERATOR": str(operator),
        "BENCH_EVIDENCE_HOST": str(tmp_path / "evidence"),
        "BENCH_EVIDENCE_NODE": "/bench-evidence",
        "BENCH_EVIDENCE_RUN": "e-l-1",
    }
    del env[missing]
    with pytest.raises(HookError, match=missing):
        hooks.prepare(env, out=lambda _: None)
    assert calls == []


def test_teardown_without_an_operator_block_still_removes_the_operator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The platform tears down a lane's previous operator without an experiment's block (BENCH_OPERATOR unset)."""
    calls: list[list[str]] = []

    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(hooks.subprocess, "run", run)
    env = {"FUZZOPS_NAMESPACE": "ns1", "FUZZOPS_KUBE_CONTEXT": "kind-c", "KUBECONFIG": "/k"}
    assert hooks.teardown(env, out=lambda _: None) == 0
    assert any("delete" in c and "ns1-sdo" in c for c in calls)


def test_teardown_deletes_the_control_namespace_and_its_cluster_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _baseline_dir(tmp_path)
    calls: list[list[str]] = []

    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(hooks.subprocess, "run", run)
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "KUBECONFIG": "/k",
        "BENCH_OPERATOR": str(_operator_file(tmp_path, root)),
    }
    assert hooks.teardown(env, out=lambda _: None) == 0
    deletes = [c[c.index("delete") + 1 :] for c in calls if "delete" in c]
    first = next(c for c in calls if "delete" in c)
    assert first[first.index("delete") + 1 :][:2] == ["namespace", "ns1-sdo"]
    assert "--wait=true" in first
    assert ["clusterrolebinding", "sdo-admin-ns1-sdo"] in [d[:2] for d in deletes]
    assert ["clusterrole", "sdo-admin-ns1-sdo"] in [d[:2] for d in deletes]
    assert all("--ignore-not-found" in c for c in calls if "delete" in c)
    assert not any(c[c.index("delete") + 2] == "ns1" for c in calls if "delete" in c)


def test_watch_runs_the_observer_for_the_fault_without_pvc_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[list[str]] = []
    monkeypatch.setattr(hooks.observe, "main", lambda argv: seen.append(argv) or 0)
    (tmp_path / "bin").mkdir()
    (tmp_path / "bin" / "sdo-operator").write_text("")
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "BENCH_FAULT": "t@deploy/web",
        "BENCH_LANE_DIR": str(tmp_path),
        "BENCH_OPERATOR": str(_operator_file(tmp_path, _baseline_dir(tmp_path), image_tag="fz-t")),
    }
    assert hooks.watch(env) == 0
    argv = seen[0]
    assert argv[argv.index("--control-namespace") + 1] == "ns1-sdo"
    assert argv[argv.index("--fault") + 1] == "t@deploy/web"
    assert "--operator-binary" not in argv
    assert float(argv[argv.index("--fallback-page-after") + 1]) == 120
    env["BENCH_FAULT"] = "none"
    hooks.watch(env)
    assert "--fault" not in seen[1]
    assert "--fallback-page-after" not in seen[1]
    env["BENCH_FAULT"] = "t@deploy/web"
    env["BENCH_OPERATOR"] = str(_operator_file(tmp_path, tmp_path, fallback_page=False))
    hooks.watch(env)
    assert "--fallback-page-after" not in seen[2]
    env["BENCH_OPERATOR"] = str(_operator_file(tmp_path, tmp_path, fallback_page_seconds=90))
    hooks.watch(env)
    assert float(seen[3][seen[3].index("--fallback-page-after") + 1]) == 90


def test_collected_memory_paths_hold_what_a_seed_needs_to_apply_a_playbook() -> None:
    # the corpus cases are where Go derives a playbook's credited (symptom, component) pairs; a seed built from a
    # collected copy without them never applies the playbook on its own, and shared helpers (lib) are imported by it
    assert {".sdo/outcomes", ".sdo/playbooks", ".sdo/corpus", ".sdo/lib"} <= set(hooks.MEMORY_PATHS)


def test_kube_copy_memory_replaces_an_older_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src = tmp_path / "src"
    (src / ".sdo" / "outcomes").mkdir(parents=True)
    (src / ".sdo" / "outcomes" / "o.json").write_text("{}")
    tar = subprocess.run(["tar", "cf", "-", "-C", str(src), ".sdo"], check=True, capture_output=True).stdout
    real = subprocess.run

    def run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[0] == "kubectl" and "exec" in cmd:
            return subprocess.CompletedProcess(cmd, 0, tar, b"")
        return real(cmd, **kwargs)

    monkeypatch.setattr(hooks.subprocess, "run", run)
    monkeypatch.setattr(Kube, "controller_pod", lambda self, ns: "pod")
    dest = tmp_path / "memory"
    dest.mkdir()
    (dest / "stale").write_text("")
    assert Kube("kind-c", "/k").copy_memory("ns1-sdo", dest)
    assert (dest / ".sdo" / "outcomes" / "o.json").is_file()
    assert not (dest / "stale").exists()


# ---- baseline subcommand -------------------------------------------------------------------------------------


def test_rebranch_bundle_writes_the_named_branch(tmp_path: Path) -> None:
    raw, commit = _bundle(tmp_path, branch="main")
    out = tmp_path / "new-baseline" / "memory.bundle"
    out.parent.mkdir()
    assert hooks.rebranch_bundle(raw, out, "baseline", tmp_path / "work") == commit
    heads = subprocess.run(["git", "bundle", "list-heads", str(out)], check=True, capture_output=True, text=True)
    assert heads.stdout.split() == [commit, "refs/heads/baseline"]


def _detached_controller_repo(tmp: Path) -> tuple[Path, str]:
    """A controller-like repository: the accepted memory checked out at a detached HEAD, a stale branch behind it."""
    repo = tmp / "controller-repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / "README.md").write_text("synthetic\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "synthetic base")
    (repo / "learned.txt").write_text("synthetic learned\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "synthetic learned")
    head = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "--detach", head)
    _git(repo, "branch", "-f", "main", "HEAD~1")
    return repo, head


def test_archive_baseline_bundles_a_detached_head_as_the_named_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the controller keeps its accepted memory at a detached HEAD; the archive must bundle that commit, not a branch
    repo, head = _detached_controller_repo(tmp_path)
    monkeypatch.setattr(hooks, "APPLICATION_ROOT", str(repo))
    monkeypatch.setattr(hooks, "helm_render", lambda env, namespace: [{"kind": "ConfigMap", "metadata": {"name": "c"}}])
    monkeypatch.setattr(hooks, "checkout_revision", lambda: COMMIT)

    def exec_controller(self: Kube, control: str, script: str, timeout: float = 300) -> bytes:
        return subprocess.run(["sh", "-c", script], check=True, capture_output=True).stdout

    monkeypatch.setattr(Kube, "exec_controller", exec_controller)
    out = tmp_path / "archived"
    code = hooks.baseline_main(
        ["--kubeconfig", str(tmp_path / "kubeconfig"), "--context", "kind-c", "--namespace", "ns1", "--out", str(out)]
    )
    assert code == 0
    baseline = Baseline.load(out)
    assert baseline.commit == head
    assert hooks.bundle_problem(out / "memory.bundle", "baseline", head) == ""


def test_rebranch_bundle_refuses_a_bundle_whose_head_is_not_the_expected_commit(tmp_path: Path) -> None:
    raw, _ = _bundle(tmp_path, branch="main")
    out = tmp_path / "new-baseline" / "memory.bundle"
    out.parent.mkdir()
    with pytest.raises(HookError, match="not the controller"):
        hooks.rebranch_bundle(raw, out, "baseline", tmp_path / "work", expected=COMMIT)
    assert not out.exists()


# ---- evidence sink: transcripts and collect ------------------------------------------------------------------


def _sink_env(tmp: Path, **extra: str) -> dict[str, str]:
    return {
        "FUZZOPS_NAMESPACE": "ns1",
        "BENCH_EVIDENCE_HOST": str(tmp / "evidence"),
        "BENCH_EVIDENCE_NODE": "/bench-evidence",
        "BENCH_EVIDENCE_RUN": "e-l-1",
        **extra,
    }


def _sink_incident(run: Path, name: str, ledger: dict[str, Any], rollout: bool) -> None:
    incident = run / "incidents" / name
    incident.mkdir(parents=True)
    (incident / "ledger.json").write_text(json.dumps(ledger))
    if rollout:
        sessions = incident / "codex" / "sessions" / "2026" / "11" / "12"
        sessions.mkdir(parents=True)
        (sessions / "rollout-1.jsonl").write_text("{}\n")


RESPONDED = {
    "incident_id": "inc-a",
    "closure": {
        "detected_at": "2026-11-12T10:01:00Z",
        "result": {"responder_session_id": "s", "resolution_path": "responder"},
    },
    "outcome_document": {"incident_id": "inc-a", "classification": "success"},
    "acknowledged": True,
    "acknowledged_at": "2026-11-12T10:05:00Z",
}


def _sink_run(tmp: Path) -> Path:
    run = tmp / "evidence" / "e-l-1"
    run.mkdir(parents=True)
    (run / "layout.json").write_text(json.dumps({"schema": "sdo.evidence/v1", "run_id": "e-l-1", "created_at": "x"}))
    return run


def test_transcripts_fails_without_a_sink_on_the_host(tmp_path: Path) -> None:
    lines: list[str] = []
    assert hooks.transcripts(_sink_env(tmp_path), out=lines.append) == 1
    assert lines[0].startswith("no sink: ")
    lines.clear()
    assert hooks.transcripts({"FUZZOPS_NAMESPACE": "ns1"}, out=lines.append) == 1
    assert lines[0].startswith("no sink: ")


def test_transcripts_passes_on_a_fresh_sink_and_counts_missing_rollouts(tmp_path: Path) -> None:
    run = _sink_run(tmp_path)
    lines: list[str] = []
    assert hooks.transcripts(_sink_env(tmp_path), out=lines.append) == 0
    _sink_incident(run, "a" * 32, RESPONDED, rollout=False)
    lines.clear()
    assert hooks.transcripts(_sink_env(tmp_path), out=lines.append) == 1
    assert lines[0] == f"transcripts missing: 1 ({'a' * 8})"


def test_transcripts_verb_is_dispatched_by_main(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    _sink_run(tmp_path)
    for name, value in _sink_env(tmp_path).items():
        monkeypatch.setenv(name, value)
    assert hooks.main(["transcripts"]) == 0
    assert capsys.readouterr().out.startswith("transcripts: ok")


def test_collect_copies_the_interval_from_the_host_sink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = _sink_run(tmp_path)
    _sink_incident(run, "a" * 32, RESPONDED, rollout=True)
    old = {**RESPONDED, "incident_id": "inc-old"}
    old["closure"] = {**RESPONDED["closure"], "detected_at": "2026-11-11T09:00:00Z"}
    old["acknowledged_at"] = "2026-11-11T09:10:00Z"
    _sink_incident(run, "b" * 32, old, rollout=True)
    (run / "index.jsonl").write_text(
        json.dumps(
            {"kind": "incident", "id": "inc-a", "path": "incidents/" + "a" * 32, "sealed_at": "2026-11-12T10:06:00Z"}
        )
        + "\n"
        + json.dumps(
            {"kind": "incident", "id": "inc-old", "path": "incidents/" + "b" * 32, "sealed_at": "2026-11-11T09:11:00Z"}
        )
        + "\n"
    )
    calls: list[list[str]] = []

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, "", "no cluster")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    out = tmp_path / "out"
    env = _sink_env(
        tmp_path,
        BENCH_OUT=str(out),
        BENCH_INTERVAL_START="2026-11-12T10:00:00Z",
        BENCH_INTERVAL_END="2026-11-12T10:30:00Z",
    )
    hooks.collect(env, out=lambda _: None)
    assert not any("runtime-artifacts" in c for c in calls)
    evidence = out / "evidence"
    assert (evidence / "incidents" / ("a" * 32) / "ledger.json").is_file()
    assert not (evidence / "incidents" / ("b" * 32)).exists()
    assert [json.loads(line)["id"] for line in (evidence / "index.jsonl").read_text().splitlines()] == ["inc-a"]
    summary = json.loads((out / "transcripts.json").read_text())
    assert summary == {
        "incidents": [{"id": "inc-a", "outcome": True, "agents_ran": True, "rollouts": 1}],
        "rollouts": 1,
        "missing": [],
        "carry_over": [],
    }
    assert json.loads((out / "collect.json").read_text())["parts"]["evidence"].startswith("ok")
    assert json.loads((out / "collect.json").read_text())["parts"]["view"].startswith("ok")
    assert json.loads((out / "view.json").read_text())["schema"] == "fuzzops.view/v1"


def test_collect_records_the_watchers_fallback_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run = tmp_path / "run"
    watch_out = run / "exec" / "watch"
    watch_out.mkdir(parents=True)
    doc = {
        "inject_at": "2026-11-12T10:00:00.123Z",
        "sdo_page_at": "2026-11-12T10:02:01Z",
        "page_source": "fallback",
        "fallback_page_at": "2026-11-12T10:02:00.123Z",
        "fallback_id": "fallback-test",
    }
    (run / "record.json").write_text(json.dumps({"lifecycle": {"inject_at": doc["inject_at"]}}))
    (watch_out / "sdo-page.json").write_text(json.dumps(doc))
    calls: list[list[str]] = []

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, "", "no cluster")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    dest = run / "operator"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(dest)), out=lambda _: None)
    summary = json.loads((dest / "collect.json").read_text())
    assert {key: summary[key] for key in doc} == doc
    assert any("delete" in cmd and "sdo-external-incident" in cmd for cmd in calls)


def test_collect_removes_a_later_episodes_pending_page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """SDO paged itself first; the harness paged a later harm episode. The inbox still goes before the reset."""
    run = tmp_path / "run"
    watch_out = run / "exec" / "watch"
    watch_out.mkdir(parents=True)
    episode = {"at": "2026-11-12T10:05:00Z", "fallback_id": "fallback-later", "fallback_page_at": None}
    doc = {
        "inject_at": "2026-11-12T10:00:00.123Z",
        "sdo_page_at": "2026-11-12T10:00:20Z",
        "page_source": "own",
        "episodes": [{"at": "2026-11-12T10:00:05Z", "sdo_page_at": "2026-11-12T10:00:20Z"}, episode],
        "page_limit": 2,
    }
    (run / "record.json").write_text(json.dumps({"lifecycle": {"inject_at": doc["inject_at"]}}))
    (watch_out / "sdo-page.json").write_text(json.dumps(doc))
    calls: list[list[str]] = []

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 1, "", "no cluster")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    dest = run / "operator"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(dest)), out=lambda _: None)
    summary = json.loads((dest / "collect.json").read_text())
    assert summary["page_limit"] == 2
    assert summary["episodes"][1]["fallback_id"] == "fallback-later"
    assert any("delete" in cmd and "sdo-external-incident" in cmd for cmd in calls)


def test_collect_does_not_report_the_managers_run_json_as_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bench manager writes run.json after the step that ran collect; collect's manifest must not hold the lane
    for a file that cannot exist yet."""
    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, 1, "", "no"))
    out = tmp_path / "run" / "operator"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out)), out=lambda _: None)
    manifest = json.loads((out / "evidence-manifest.json").read_text())
    assert not (tmp_path / "run" / "run.json").exists()
    assert not any("run.json" in problem for problem in manifest["problems"])


def test_collect_records_a_view_failure_without_raising(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, 1, "", "no"))

    def broken(_: Path) -> dict[str, Any]:
        raise ValueError("boom")

    monkeypatch.setattr(hooks.view, "write", broken)
    out = tmp_path / "out"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out)), out=lambda _: None)
    assert json.loads((out / "collect.json").read_text())["parts"]["view"] == "error: boom"


def test_reset_incidents_requires_an_acknowledged_receipt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []
    answer = {"control_namespace": "fg1-sdo", "generation": 3, "acknowledged": True}

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, json.dumps(answer) + "\n", "")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    kc = tmp_path / "kc"
    assert hooks.reset_incidents(kc.parent / "sdo-operator", "fg1-sdo", kc)["generation"] == 3
    assert calls[0] == [
        str(kc.parent / "sdo-operator"),
        "incidents",
        "reset",
        "--control-namespace",
        "fg1-sdo",
        "--timeout",
        "2m",
        "--kubeconfig",
        str(kc),
    ]
    answer["acknowledged"] = False
    with pytest.raises(hooks.HookError, match="not acknowledged"):
        hooks.reset_incidents(kc.parent / "sdo-operator", "fg1-sdo", None)


def _collect_with_reset_spy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, complete: bool
) -> tuple[list[list[str]], Path]:
    lane = tmp_path / "lane"
    (lane / "bin").mkdir(parents=True)
    (lane / "bin" / "sdo-operator").write_text("")
    resets: list[list[str]] = []

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        if cmd[1:3] == ["incidents", "reset"]:
            resets.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, '{"generation": 7, "acknowledged": true}\n', "")
        return subprocess.CompletedProcess(cmd, 1, "", "no cluster")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    if complete:
        monkeypatch.setattr(hooks.cellevidence, "write_manifest", lambda _run, **_kw: hooks.cellevidence.CellEvidence())
    out = tmp_path / "run" / "operator"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out), BENCH_LANE_DIR=str(lane)), out=lambda _: None)
    return resets, out


def test_collect_ends_with_an_operator_reset_once_the_evidence_is_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resets, out = _collect_with_reset_spy(tmp_path, monkeypatch, complete=True)
    assert len(resets) == 1
    assert json.loads((out / "collect.json").read_text())["parts"]["reset"] == "ok (generation 7)"
    assert json.loads((out / "incidents-reset.json").read_text())["generation"] == 7


def test_collect_blocks_the_reset_while_the_evidence_is_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resets, out = _collect_with_reset_spy(tmp_path, monkeypatch, complete=False)
    parts = json.loads((out / "collect.json").read_text())["parts"]
    assert resets == []
    assert parts["evidence-manifest"].startswith("error: evidence incomplete: ")
    assert parts["reset"].startswith("error: blocked")
    assert not (out / "incidents-reset.json").exists()


class _Killed(BaseException):
    """The platform killed the hook's process group."""


def test_a_collect_that_is_killed_never_leaves_a_manifest_that_says_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A killed collect left no manifest, the platform read that as nothing wrong and reset the lane. An earlier
    attempt's complete manifest must not stand either while the evidence is collected again."""

    def killed(*_: Any, **__: Any) -> None:
        raise _Killed

    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, 1, "", "no"))
    monkeypatch.setattr(hooks.capture, "capture_all", killed)
    out = tmp_path / "run" / "operator"
    out.mkdir(parents=True)
    (out / "evidence-manifest.json").write_text('{"problems": []}')
    with pytest.raises(_Killed):
        hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out), BENCH_LANE_DIR=str(tmp_path)), out=lambda _: None)
    assert json.loads((out / "evidence-manifest.json").read_text())["problems"] == ["collect did not finish"]


def test_a_part_that_hangs_ends_by_its_deadline_and_the_manifest_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every part hangs in a command that would outlive the platform's limit (and survives its own failures): collect
    still ends, with its manifest, on its own."""
    monkeypatch.setattr(hooks, "COLLECT_PART_MIN_S", 0.02)

    def hang(*_: Any, **__: Any) -> subprocess.CompletedProcess[str]:
        while True:
            try:
                signal.pause()
            except Exception:
                continue

    monkeypatch.setattr(hooks.subprocess, "run", hang)
    out = tmp_path / "run" / "operator"
    env = _sink_env(tmp_path, BENCH_OUT=str(out), BENCH_VERB_LIMIT_SECONDS="0.1")
    assert hooks.collect(env, out=lambda _: None) == 1
    manifest = json.loads((out / "evidence-manifest.json").read_text())
    assert any("collect part logs incomplete (error: not done within" in p for p in manifest["problems"])
    assert any("collect part observability" in p for p in manifest["problems"])


def _verify(env: dict[str, str]) -> dict[str, Any]:
    lines: list[str] = []
    assert hooks.verify_evidence(env, out=lines.append) == 0
    return json.loads(lines[-1])


def _evidence(monkeypatch: pytest.MonkeyPatch, *answers: list[str]) -> list[Path]:
    """The manifest lists these problems, one list per time it is built."""
    built: list[Path] = []

    def write_manifest(run: Path, run_pending: bool = False) -> Any:
        assert run_pending, "run.json is the manager's"
        built.append(run)
        return hooks.cellevidence.CellEvidence(problems=answers[min(len(built), len(answers)) - 1])

    monkeypatch.setattr(hooks.cellevidence, "write_manifest", write_manifest)
    return built


@pytest.mark.parametrize("problems", [[], ["controller.log missing", "observability jaeger incomplete (lost)"]])
def test_verify_evidence_answers_the_manifests_problems(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problems: list[str]
) -> None:
    built = _evidence(monkeypatch, problems)
    monkeypatch.setattr(hooks, "collect", lambda *_, **__: pytest.fail("a first check only reads"))
    answer = _verify(_sink_env(tmp_path, BENCH_RUN_DIR=str(tmp_path / "run")))
    assert answer == {"verified": not problems, "reasons": problems}
    assert built == [tmp_path / "run"]


@pytest.mark.parametrize(("then", "verified"), [([], True), (["still missing"], False)])
def test_a_later_check_collects_again_before_it_answers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, then: list[str], verified: bool
) -> None:
    _evidence(monkeypatch, ["collect.json missing"], then)
    collected: list[str] = []
    monkeypatch.setattr(hooks, "collect", lambda env, out: collected.append(env["BENCH_OUT"]) or 1)
    env = _sink_env(tmp_path, BENCH_RUN_DIR=str(tmp_path / "run"), BENCH_EVIDENCE_RECHECK="1")
    assert _verify(env) == {"verified": verified, "reasons": then}
    assert collected == [str(tmp_path / "run" / "operator")]


def test_a_later_check_of_complete_or_waived_evidence_collects_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _evidence(monkeypatch, [])
    monkeypatch.setattr(hooks, "collect", lambda *_, **__: pytest.fail("nothing to collect"))
    env = _sink_env(tmp_path, BENCH_RUN_DIR=str(tmp_path / "run"), BENCH_EVIDENCE_RECHECK="1")
    assert _verify(env)["verified"] is True


def test_a_killed_collect_is_not_verified_by_an_earlier_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def killed(*_: Any, **__: Any) -> None:
        raise _Killed

    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, 1, "", "no"))
    monkeypatch.setattr(hooks.capture, "capture_all", killed)
    out = tmp_path / "run" / "operator"
    out.mkdir(parents=True)
    (out / "collect.json").write_text('{"parts": {}}')
    with pytest.raises(_Killed):
        hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out), BENCH_LANE_DIR=str(tmp_path)), out=lambda _: None)
    answer = _verify(_sink_env(tmp_path, BENCH_RUN_DIR=str(tmp_path / "run")))
    assert "collect.json missing (operator/collect.json)" in answer["reasons"]


def test_verify_evidence_is_a_verb() -> None:
    assert "verify-evidence" in hooks.VERBS


def test_every_part_of_collect_ends_before_the_platforms_limit() -> None:
    by = list(hooks.COLLECT_BY.values())
    assert by == sorted(by)
    assert by[-1] < 1
    assert hooks.COLLECT_BY["observability"] <= 0.8, "the manifest is written in what is left"


def test_the_captures_get_the_time_the_platform_leaves_them(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    given: list[float] = []

    def capture_all(*_: Any, seconds: float, **__: Any) -> dict[str, Any]:
        given.append(seconds)
        return {"window": ["", ""]} | {k: {"status": "ok"} for k in hooks.cellevidence.CAPTURES}

    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: subprocess.CompletedProcess(cmd, 1, "", "no"))
    monkeypatch.setattr(hooks.capture, "capture_all", capture_all)
    env = _sink_env(tmp_path, BENCH_OUT=str(tmp_path / "run" / "operator"), BENCH_LANE_DIR=str(tmp_path))
    env |= {"BENCH_VERB_LIMIT_SECONDS": "1000"}
    hooks.collect(env, out=lambda _: None, clock=lambda: 50.0)
    assert given == [pytest.approx(1000 * hooks.COLLECT_BY["observability"] * 0.95)]


def test_reset_after_install_waits_for_the_controller(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    answers = [(1, ""), (1, ""), (0, '{"generation": 1, "acknowledged": true}')]
    monkeypatch.setattr(
        hooks.subprocess,
        "run",
        lambda cmd, **_: subprocess.CompletedProcess(cmd, *answers.pop(0), "not running"),
    )
    now = [0.0]
    receipt = hooks.reset_when_running(
        tmp_path / "sdo-operator",
        "fg1-sdo",
        None,
        patience=100,
        sleep=lambda s: now.__setitem__(0, now[0] + s),
        clock=lambda: now[0],
    )
    assert receipt["generation"] == 1
    assert not answers
    answers[:] = [(1, "")] * 20
    with pytest.raises(hooks.HookError, match="not acknowledged"):
        hooks.reset_when_running(
            tmp_path / "sdo-operator",
            "fg1-sdo",
            None,
            patience=30,
            sleep=lambda s: now.__setitem__(0, now[0] + s),
            clock=lambda: now[0],
        )


class _StartingController:
    """A lane controller that writes its state ConfigMap only once its start-up (informers, traffic warm-up,
    first evaluation) is done, after ``starts_after`` polls."""

    def __init__(self, starts_after: int, ready: bool = True) -> None:
        self.polls = 0
        self.starts_after = starts_after
        self.ready = ready

    def state(self, control: str) -> dict[str, Any] | None:
        self.polls += 1
        return {"step_count": 1} if self.polls > self.starts_after else None

    def controller_ready(self, control: str) -> bool:
        return self.ready

    def controller_log_tail(self, control: str, lines: int = 3) -> str:
        return "INFO responder_standby_created application=fg1"


def test_reset_is_requested_only_after_the_controller_finished_starting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The controller acknowledges resets from its main loop, which it enters after a start-up that took 17 minutes
    on a loaded host: asking earlier only burns 2-minute acknowledgement waits."""
    controller = _StartingController(starts_after=40)
    asked: list[int] = []

    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        asked.append(controller.polls)
        return subprocess.CompletedProcess(cmd, 0, '{"generation": 5, "acknowledged": true}', "")

    monkeypatch.setattr(hooks.subprocess, "run", run)
    now = [0.0]
    receipt = hooks.reset_when_started(
        controller,  # type: ignore[arg-type]
        tmp_path / "sdo-operator",
        "fg1-sdo",
        None,
        sleep=lambda s: now.__setitem__(0, now[0] + s),
        clock=lambda: now[0],
    )
    assert receipt["generation"] == 5
    assert asked == [41]
    assert 400 <= now[0] < hooks.CONTROLLER_START_PATIENCE_S


def test_a_controller_that_never_finishes_starting_fails_with_where_it_is(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    controller = _StartingController(starts_after=10**9, ready=True)
    monkeypatch.setattr(hooks.subprocess, "run", lambda cmd, **_: pytest.fail("reset asked of a starting controller"))
    now = [0.0]
    with pytest.raises(hooks.HookError) as raised:
        hooks.reset_when_started(
            controller,  # type: ignore[arg-type]
            tmp_path / "sdo-operator",
            "fg1-sdo",
            None,
            start_patience=120,
            sleep=lambda s: now.__setitem__(0, now[0] + s),
            clock=lambda: now[0],
        )
    message = str(raised.value)
    assert "did not finish starting within 2m" in message
    assert "pod Ready" in message
    assert "responder_standby_created" in message
    assert "no state ConfigMap" in message


def _fake_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, registry_fails: bool = False) -> Path:
    """docker, kind and a host registry script that log their calls; returns the log."""
    log = tmp_path / "calls.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {
        "docker": 'echo "docker $*" >> "$CALLS"\n',
        "kind": (
            'echo "kind $*" >> "$CALLS"\n[ "$1 $2" = "get nodes" ] && echo c-control-plane && echo c-worker\nexit 0\n'
        ),
    }.items():
        (bin_dir / name).write_text(f"#!/bin/sh\n{body}")
        (bin_dir / name).chmod(0o755)
    script = tmp_path / "registry.sh"
    script.write_text(f'#!/bin/sh\necho "registry $*" >> "$CALLS"\n{"exit 1" if registry_fails else "exit 0"}\n')
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:/usr/bin:/bin")
    monkeypatch.setenv("CALLS", str(log))
    return log


def test_operator_images_go_through_the_host_registry_when_the_bench_offers_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_tools(tmp_path, monkeypatch)
    lines: list[str] = []
    hooks.load_images("c", ["img:a", "img:b"], lines.append, registry=tmp_path / "registry.sh")
    calls = log.read_text().splitlines()
    assert "registry push img:a img:b" in calls
    assert not any(call.startswith("kind load") for call in calls)


def test_operator_images_are_loaded_into_the_nodes_without_a_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_tools(tmp_path, monkeypatch)
    hooks.load_images("c", ["img:a"], lambda _line: None, registry=None)
    assert any(call.startswith("kind load docker-image") for call in log.read_text().splitlines())


def test_a_failing_registry_falls_back_to_loading_the_nodes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log = _fake_tools(tmp_path, monkeypatch, registry_fails=True)
    lines: list[str] = []
    hooks.load_images("c", ["img:a"], lines.append, registry=tmp_path / "registry.sh")
    assert any(call.startswith("kind load docker-image") for call in log.read_text().splitlines())
    assert any("registry" in line and "falling back" in line for line in lines)


# ---- harness-paged arm ---------------------------------------------------------------------------------------


def test_harness_page_setting_selects_external_mode_with_detectors_as_clues(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    block = OperatorBlock.load(
        _operator_file(tmp_path, root, model="m", harness_page=True, config={"response": {"max_loops": 3}})
    )
    assert block.harness_page is not None
    assert block.harness_page.after == 30
    args, _ = install_existing_launch(
        tmp_path / "sdo-operator",
        tmp_path / "app",
        Baseline.load(root),
        block,
        tmp_path / "kc",
        SINK_ENV,
        tmp_path / "c",
    )
    files = [Path(args[i + 1]) for i, arg in enumerate(args) if arg == "--config"]
    assert yaml.safe_load(files[-1].read_text()) == {
        "mode": "external",
        "response": {"max_loops": 3, "external_clues": True},
    }


def test_without_harness_page_the_operator_mode_is_untouched(tmp_path: Path) -> None:
    block = OperatorBlock.load(_operator_file(tmp_path, tmp_path, model="m"))
    assert block.harness_page is None
    assert block.effective_config() == {}


def test_harness_page_refuses_an_experiment_mode(tmp_path: Path) -> None:
    with pytest.raises(HookError, match="mode"):
        OperatorBlock.load(_operator_file(tmp_path, tmp_path, model="m", harness_page=True, config={"mode": "frozen"}))


# ---- provider vcluster: registry images and pulled evidence --------------------------------------------------


def test_lane_registry_images_push_and_name_images_as_every_node_pulls_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = _fake_tools(tmp_path, monkeypatch)
    script = tmp_path / "registry.sh"
    script.write_text(
        '#!/bin/sh\necho "registry $*" >> "$CALLS"\n[ "$1" = ref ] && echo "localhost:30500/library/$2"\nexit 0\n'
    )
    lines: list[str] = []
    refs = hooks.lane_registry_images(script, ["sdo-controller:t", "sdo-responder:t"], lines.append)
    assert refs == {
        "sdo-controller:t": "localhost:30500/library/sdo-controller:t",
        "sdo-responder:t": "localhost:30500/library/sdo-responder:t",
    }
    calls = log.read_text().splitlines()
    assert "registry up" in calls
    assert "registry push sdo-controller:t sdo-responder:t" in calls
    assert not any(call.startswith("kind") for call in calls)


def test_a_lane_without_nodes_cannot_fall_back_to_loading_images() -> None:
    with pytest.raises(HookError, match="registry script"):
        hooks.lane_registry_images(None, ["sdo-controller:t"], lambda _line: None)


def test_pull_mode_teardown_stops_the_operator_and_pulls_before_deleting_the_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _baseline_dir(tmp_path)
    events: list[str] = []

    def run(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        words = " ".join(cmd)
        if "delete deployments,jobs,statefulsets" in words:
            events.append("stop")
        elif "delete namespace" in words:
            events.append("delete namespace")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(hooks.subprocess, "run", run)
    monkeypatch.setattr(hooks.sink, "pull_run_dir", lambda *_: events.append("pull"))
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "KUBECONFIG": "/k",
        "BENCH_OPERATOR": str(_operator_file(tmp_path, root)),
        "BENCH_EVIDENCE_MODE": "pull",
        "BENCH_EVIDENCE_RUN": "e-l-1",
        "BENCH_EVIDENCE_HOST": str(tmp_path / "ev"),
    }
    assert hooks.teardown(env, out=lambda _: None) == 0
    assert events == ["stop", "pull", "delete namespace"]


def test_pull_mode_teardown_goes_on_when_the_pull_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _baseline_dir(tmp_path)
    deleted: list[str] = []
    monkeypatch.setattr(
        hooks.subprocess,
        "run",
        lambda cmd, **_: deleted.append(" ".join(cmd)) or subprocess.CompletedProcess(cmd, 0, "", ""),
    )

    def broken(*_: Any) -> None:
        raise hooks.sink.SinkError("no pod")

    monkeypatch.setattr(hooks.sink, "pull_run_dir", broken)
    lines: list[str] = []
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "BENCH_OPERATOR": str(_operator_file(tmp_path, root)),
        "BENCH_EVIDENCE_MODE": "pull",
        "BENCH_EVIDENCE_RUN": "e-l-1",
        "BENCH_EVIDENCE_HOST": str(tmp_path / "ev"),
    }
    assert hooks.teardown(env, out=lines.append) == 0
    assert any("delete namespace ns1-sdo" in d for d in deleted)
    assert any("evidence pull failed" in line for line in lines)


def test_dev_mount_in_pull_mode_projects_over_the_evidence_claim_not_a_node_path(tmp_path: Path) -> None:
    from benchmarks.fuzzops import hooks

    block = hooks.OperatorBlock.__new__(hooks.OperatorBlock)
    object.__setattr__(block, "dev_mount", True)
    env = {"BENCH_EVIDENCE_MODE": "pull", "BENCH_EVIDENCE_RUN": "e-l-1"}
    assert hooks.dev_mount_env(env, block, tmp_path / "bin", lambda _m: None) == {
        "SDO_DEV_MOUNT": "1",
        "SDO_DEV_MOUNT_PATH": ".dev",
        "SDO_DEV_MOUNT_CLAIM": "sdo-evidence",
    }


def test_wait_idle_stable_restarts_the_quiet_period_when_an_incident_opens(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    clock = {"t": 0.0}
    monkeypatch.setattr(hooks.time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(hooks.time, "sleep", lambda s: clock.__setitem__("t", clock["t"] + s))
    # idle until t=30, an incident from 30 to 60, idle again after
    monkeypatch.setattr(hooks, "wait_idle", lambda *a, **k: "alarm: incident open i" if 30 <= clock["t"] < 60 else "")
    kube = SimpleNamespace()
    assert hooks.wait_idle_stable(kube, "c", 1000, 90) == ""  # type: ignore[arg-type]
    assert clock["t"] >= 150  # 60 + 90 quiet seconds, not 90


def test_reset_incidents_verb_resets_and_writes_the_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lane = tmp_path / "lane"
    (lane / "bin").mkdir(parents=True)
    (lane / "bin" / "sdo-operator").write_text("")
    calls: list[list[str]] = []

    def run_cmd(cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, '{"generation": 9, "acknowledged": true}\n', "")

    monkeypatch.setattr(hooks.subprocess, "run", run_cmd)
    out = tmp_path / "scratch"
    env = _sink_env(tmp_path, BENCH_OUT=str(out), BENCH_LANE_DIR=str(lane))
    assert hooks.reset_incidents_verb(env, out=lambda _: None) == 0
    assert any(c[1:3] == ["incidents", "reset"] for c in calls)
    assert json.loads((out / "incidents-reset.json").read_text())["generation"] == 9
    assert "reset-incidents" in hooks.VERBS


def _store_history(*entries: tuple[str, str]) -> dict[str, Any]:
    stamp = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "version": "v",
        "history": [
            {
                "detector_id": detector,
                "evaluated_at": stamp,
                "status": "clear",
            }
            for detector, _ in entries
        ],
    }


def _log(*lines: str) -> str:
    return "\n".join(lines)


EVALUATED = '{"evaluated_detectors":["store-state-postgresql","store-state-redis"],"findings":[]}'


def test_idle_verdict_holds_while_a_data_store_has_no_promoted_baseline() -> None:
    state = _store_history(("store-state-postgresql", ""), ("store-state-redis", ""))
    log = _log(EVALUATED, "INFO store_baseline_promoted component=redis")

    verdict = idle_verdict(state, NOW, True, NOW, controller_log=log)

    assert verdict == "blind: no promoted baseline yet for postgresql"


def test_idle_verdict_releases_once_every_evaluated_store_is_promoted() -> None:
    state = _store_history(("store-state-postgresql", ""), ("store-state-redis", ""))
    log = _log(
        EVALUATED, "INFO store_baseline_promoted component=redis", "INFO store_baseline_promoted component=postgresql"
    )

    assert idle_verdict(state, NOW, True, NOW, controller_log=log) == ""


def test_idle_verdict_after_a_restart_needs_no_new_promotion_when_baselines_were_restored() -> None:
    """A seed import or reset restarts the controller over the same store Pods; the restarted controller adopts the
    baselines it persisted on its first evaluation and logs them as promoted, so the lane is idle at once. A store
    whose persisted baseline no longer describes it is discarded and still waits for a promotion."""
    state = _store_history(("store-state-postgresql", ""), ("store-state-redis", ""))
    restarted = _log(
        EVALUATED,
        "2026/11/19 21:49:45 INFO store_baseline_promoted component=postgresql restored=true",
        "2026/11/19 21:49:45 INFO store_baseline_promoted component=redis restored=true",
    )
    assert idle_verdict(state, NOW, True, NOW, controller_log=restarted) == ""

    replaced = _log(
        EVALUATED,
        "2026/11/19 21:49:45 INFO store_baseline_promoted component=postgresql restored=true",
        '2026/11/19 21:49:45 INFO store_baseline_discarded component=redis reason="query declaration changed"',
    )
    assert idle_verdict(state, NOW, True, NOW, controller_log=replaced) == "blind: no promoted baseline yet for redis"


def test_idle_verdict_needs_the_controller_log_to_judge_baselines() -> None:
    state = _store_history(("store-state-postgresql", ""))

    assert idle_verdict(state, NOW, True, NOW, controller_log=None).startswith("blind:")


def test_idle_verdict_without_baselines_ignores_unpromoted_stores() -> None:
    state = _store_history(("store-state-postgresql", ""))

    assert idle_verdict(state, NOW, True, NOW, baselines=False) == ""
    assert idle_verdict(state, NOW, True, NOW, controller_log=_log(EVALUATED)).startswith("blind:")


def test_wait_idle_before_the_seed_needs_no_promoted_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Kube:
        def state_configmap(self, control: str) -> tuple[dict[str, Any], datetime]:
            return _store_history(("store-state-postgresql", "")), datetime.now(UTC)

        def controller_ready(self, control: str) -> bool:
            return True

    assert hooks.wait_idle(_Kube(), "ns", 1.0) == ""  # type: ignore[arg-type]


def test_run_seed_runs_the_packs_seed_script_with_the_lane_environment(tmp_path: Path) -> None:
    root = tmp_path / "pack"
    (root / "manifests").mkdir(parents=True)
    (root / "manifests" / "a.yaml").write_text("kind: Service\n")
    (root / "app.yaml").write_text("name: s\nseed: ./seed.sh\nentry: {service: web, port: 80}\n")
    (root / "seed.sh").write_text('#!/bin/sh\necho "$NAMESPACE $KUBE_CONTEXT $KIND_CLUSTER" > "$PWD/seeded"\n')
    (root / "seed.sh").chmod(0o755)
    env = {
        "FUZZOPS_NAMESPACE": "ns1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
        "FUZZOPS_KIND_CLUSTER": "c",
        "KUBECONFIG": "/k",
    }
    assert hooks.run_seed(hooks.SourcePack(root), env) is True
    assert (root / "seeded").read_text().strip() == "ns1 kind-c c"
    (root / "app.yaml").write_text("name: s\nentry: {service: web, port: 80}\n")
    assert hooks.run_seed(hooks.SourcePack(root), env) is False
    (root / "app.yaml").write_text("name: s\nseed: ./seed.sh\nentry: {service: web, port: 80}\n")
    (root / "seed.sh").write_text("#!/bin/sh\nexit 3\n")
    with pytest.raises(HookError, match="seed"):
        hooks.run_seed(hooks.SourcePack(root), env)


def test_memory_render_is_applied_only_when_it_differs_from_the_install_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    applied: list[str] = []

    def fake_run(args: list[str], what: str, timeout: float = 600, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        applied.append(kwargs.get("input", ""))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(hooks, "_run", fake_run)
    waited: list[str] = []
    monkeypatch.setattr(hooks, "wait_rollouts", lambda kube, ns, timeout_s=600: waited.append(ns))
    install = [{"kind": "ConfigMap", "metadata": {"name": "nginx"}, "data": {"a": "1"}}]
    repo = tmp_path / "memory"
    (repo / "k8s").mkdir(parents=True)
    kube = Kube("kind-c", "/k")
    (repo / "k8s" / "render.yaml").write_text(yaml.safe_dump_all(install))
    assert hooks.apply_memory_render(kube, "ns1", repo, install, lambda line: None) is False
    assert calls == []
    assert waited == []
    instrumented = [
        {"kind": "ConfigMap", "metadata": {"name": "nginx"}, "data": {"a": "2"}},
        {"kind": "Job", "metadata": {"name": "migrations-1db9599"}},
    ]
    (repo / "k8s" / "render.yaml").write_text(yaml.safe_dump_all(instrumented))
    assert hooks.apply_memory_render(kube, "ns1", repo, install, lambda line: None) is True
    assert any("apply" in c for c in calls)
    # the Job of a series memory is not applied: one already in the namespace is immutable, and the deployer runs it
    assert [d["kind"] for d in yaml.safe_load_all(applied[-1])] == ["ConfigMap"]
    assert waited == ["ns1"]


def _describe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any, settings: dict[str, Any]) -> dict[str, Any]:
    block = tmp_path / "operator.json"
    block.write_text(json.dumps({"hooks": "sdo", "revision": "abc1234", "settings": settings}))
    monkeypatch.setenv("BENCH_OPERATOR", str(block))
    assert hooks.main(["describe"]) == 0
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_describe_tells_lanes_what_the_platform_needs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    answer = _describe(tmp_path, monkeypatch, capsys, {"model": "opus"})
    assert answer == {
        "max_startup": "30m",
        "needs_credentials": True,
        "inputs": {"seed": {"requires": ["baseline"]}},
        "verify_evidence": True,
    }


def test_describe_reads_the_startup_bound_and_the_stub_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    answer = _describe(tmp_path, monkeypatch, capsys, {"model": "stub", "startup_bound": "0s"})
    assert answer["max_startup"] == "0s"
    assert answer["needs_credentials"] is False
    nested = _describe(tmp_path, monkeypatch, capsys, {"config": {"response": {"startup_bound": "12m"}}})
    assert nested["max_startup"] == "12m"


def _check_input(
    capsys: Any, monkeypatch: pytest.MonkeyPatch, kind: str, path: Path, phase: str = "add", **extra: str
) -> dict[str, Any]:
    monkeypatch.setenv("BENCH_INPUT_KIND", kind)
    monkeypatch.setenv("BENCH_INPUT_PATH", str(path))
    monkeypatch.setenv("BENCH_INPUT_PHASE", phase)
    monkeypatch.setenv("FUZZOPS_NAMESPACE", "app1")
    for key, value in extra.items():
        monkeypatch.setenv(key, value)
    assert hooks.main(["check-input"]) == 0
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def test_check_input_notes_a_directory_that_is_no_memory_bundle_yet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    answer = _check_input(capsys, monkeypatch, "seed", tmp_path / "later")
    assert answer["ok"] is True
    assert "not a memory bundle yet" in answer["note"]


def test_check_input_judges_the_memory_check_like_the_platform_used_to(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    (tmp_path / "baseline.json").write_text("{}")
    commit = "a" * 40
    monkeypatch.setattr(hooks, "_load_input_baseline", lambda path: hooks.InputBaseline("app1", commit, "main"))
    monkeypatch.setattr(hooks, "_input_problem", lambda kind, path, namespace: "")
    runs: list[str] = []

    def migrate(path: Path, baseline_input: Any) -> tuple[str, int, list[str]]:
        runs.append(str(path))
        return commit, 1, ["telemetry.unbound[metrics-alerts]: not bound"]

    monkeypatch.setattr(hooks, "_memory_migrate", migrate)
    # add time: no cluster is known yet, so an unbound telemetry source is no failure
    answer = _check_input(capsys, monkeypatch, "baseline", tmp_path)
    assert answer["ok"] is True
    assert "1 allowed" in answer["detail"]
    # preflight does not allow it unless asked to
    refused = _check_input(capsys, monkeypatch, "baseline", tmp_path, phase="preflight")
    assert refused["ok"] is False
    assert "telemetry.unbound" in refused["detail"]
    allowed = _check_input(
        capsys, monkeypatch, "baseline", tmp_path, phase="preflight", BENCH_INPUT_ALLOW="telemetry.unbound"
    )
    assert allowed["ok"] is True
    # a bundle whose head is not the commit baseline.json names is refused
    monkeypatch.setattr(hooks, "_memory_migrate", lambda path, b: ("b" * 40, 0, []))
    wrong = _check_input(capsys, monkeypatch, "baseline", tmp_path)
    assert wrong["ok"] is False
    assert "baseline.json says" in wrong["detail"]


def test_check_input_says_when_every_install_of_a_baseline_runs_the_health_judge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    root = _baseline_dir(tmp_path)
    baseline = Baseline.load(root)
    monkeypatch.setattr(hooks, "_input_problem", lambda kind, path, namespace: "")
    monkeypatch.setattr(hooks, "_memory_migrate", lambda path, b: (b.commit, 0, []))
    answer = _check_input(capsys, monkeypatch, "baseline", root)
    assert answer["ok"] is True
    assert "no health judgment" in answer["detail"]
    # a seed is imported into a running lane: no install, no judge
    assert "no health judgment" not in _check_input(capsys, monkeypatch, "seed", root)["detail"]
    # the same memory with a validated initial judgment on top
    repo = tmp_path / "judged-repo"
    _git(tmp_path, "clone", "-q", "-b", baseline.branch, str(root / baseline.bundle), str(repo))
    (repo / "judge.md").write_text("judged\n")
    _git(repo, "add", "-A")
    trailers = "SDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed"
    _git(repo, "commit", "-q", "-m", f"validated initial\n\n{trailers}")
    _git(repo, "bundle", "create", str(root / baseline.bundle), baseline.branch)
    head = _git(repo, "rev-parse", "HEAD")
    monkeypatch.setattr(hooks, "_memory_migrate", lambda path, b: (head, 0, []))
    monkeypatch.setattr(
        hooks, "_load_input_baseline", lambda path: hooks.InputBaseline("ns1", head, baseline.branch, baseline.bundle)
    )
    assert "no health judgment" not in _check_input(capsys, monkeypatch, "baseline", root)["detail"]


def test_memory_diagnostics_skip_validator_progress_lines() -> None:
    """A memory with learned Go code prints INFO stage progress before its diagnostics (LB19); real output of a learned
    bundle (shared with the lanes fallback test), run through the platform's CHECK prefix."""
    fixture = (
        Path(__file__).resolve().parents[4] / "benchmarks/lanes/internal/bench/testdata/memory-migrate-learned.out"
    )
    output = "\n".join(line.removeprefix("CHECK ") for line in fixture.read_text().splitlines()[1:-1])
    diagnostics = hooks.memory_diagnostics(output)
    assert diagnostics
    assert all("telemetry.unbound" in d for d in diagnostics)
    assert hooks.memory_diagnostics("2026/11/20 17:35:01 ERROR vet failed\n") == [
        "2026/11/20 17:35:01 ERROR vet failed"
    ]
    assert hooks.memory_diagnostics('2026/11/20 17:34:51 INFO memory_check_stage_started stage="x"\n') == []


def test_check_input_refuses_a_baseline_for_another_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> None:
    (tmp_path / "baseline.json").write_text("{}")
    monkeypatch.setattr(hooks, "_load_input_baseline", lambda path: hooks.InputBaseline("other", "a" * 40, "main"))
    monkeypatch.setattr(
        hooks,
        "_input_problem",
        lambda kind, path, namespace: "baseline namespace other differs from FUZZOPS_NAMESPACE app1",
    )
    answer = _check_input(capsys, monkeypatch, "baseline", tmp_path)
    assert answer["ok"] is False
    assert "namespace" in answer["detail"]


@pytest.mark.parametrize("verb", ["preflight", "export-artifact", "verify-artifact", "reseed"])
def test_lanes_contract_verbs_not_implemented_answer_unsupported(verb: str, capsys: Any) -> None:
    assert hooks.main([verb]) == 64
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1]) == {"unsupported": True}


def test_phase_timer_prints_the_seconds_each_prepare_phase_took() -> None:
    now = [100.0]
    lines: list[str] = []
    timer = hooks.PhaseTimer(lines.append, clock=lambda: now[0])
    now[0] = 112.4
    timer.mark("images pinned")
    now[0] = 112.9
    timer.mark("namespaces")
    now[0] = 400.0
    timer.mark("controller started")
    assert lines == [
        "prepare phase: images pinned 12s (total 12s)",
        "prepare phase: namespaces 0s (total 13s)",
        "prepare phase: controller started 287s (total 300s)",
    ]


# ---- a lane with a source branch: reset rewinds it, verify-start proves the start ------------------------------------


def _branch_block(tmp_path: Path, **extra: str) -> dict[str, str]:
    block = tmp_path / "operator.json"
    branch = {
        "ref": "sdo-source",
        "up": "up {namespace}",
        "down": "down {namespace}",
        "rewind": "rewind {namespace} {ref} {head} {script_image}",
        **extra,
    }
    block.write_text(
        json.dumps(
            {
                "hooks": "sdo",
                "revision": "abc1234",
                "baseline": "/b",
                "settings": {"image_tag": "t1", "source_branch": branch},
            }
        )
    )
    lane = tmp_path / "lane"
    lane.mkdir(exist_ok=True)
    return {
        "BENCH_OPERATOR": str(block),
        "BENCH_LANE_DIR": str(lane),
        "KUBECONFIG": "/k",
        "FUZZOPS_NAMESPACE": "fg1",
        "FUZZOPS_KUBE_CONTEXT": "kind-c",
    }


def test_a_reset_rewinds_the_source_branch_to_the_head_and_drops_sdos_refused_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def run(args: list[str], what: str, timeout: float = 600, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(hooks, "_run", run)
    env = _branch_block(tmp_path)
    hooks.rewind_source_branch(env, Kube("kind-c", "/k"), "fg1-sdo", "9d992b3")
    assert calls[0][:2] == ["sh", "-c"]
    assert calls[0][2] == "rewind fg1 sdo-source 9d992b3 sdo-script-tools:t1"
    assert calls[1][-4:] == ["delete", "configmap", "sdo-head-hold", "--ignore-not-found"]


def test_a_lane_with_no_source_branch_has_nothing_to_rewind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hooks, "_run", lambda *a, **k: pytest.fail("nothing to run"))
    block = tmp_path / "operator.json"
    block.write_text(
        json.dumps({"hooks": "sdo", "revision": "abc1234", "baseline": "/b", "settings": {"image_tag": "t1"}})
    )
    env = {"BENCH_OPERATOR": str(block), "BENCH_LANE_DIR": str(tmp_path)}
    hooks.rewind_source_branch(env, Kube("kind-c", "/k"), "fg1-sdo", "9d992b3")


def test_a_branch_that_cannot_be_rewound_makes_the_lane_one_to_rebuild(tmp_path: Path) -> None:
    env = _branch_block(tmp_path, rewind="")
    with pytest.raises(HookError, match="no rewind command"):
        hooks.rewind_source_branch(env, Kube("kind-c", "/k"), "fg1-sdo", "9d992b3")


PREPARED = {"head": "h" * 40, "status": "", "outcomes": ""}
BASE_IMAGES = {"Deployment/web": ["web:v1"], "StatefulSet/db": ["db:v1", "init:v1"]}


def test_a_lane_that_equals_a_fresh_install_has_no_start_problems() -> None:
    assert hooks.start_problems(PREPARED, PREPARED, "", BASE_IMAGES, BASE_IMAGES) == []


def test_a_lane_left_on_a_later_release_with_an_open_incident_and_moved_memory_is_not_a_fresh_install() -> None:
    moved = {"head": "x" * 40, "status": "", "outcomes": "o1.json"}
    live = {"Deployment/web": ["web:v2"], "StatefulSet/db": ["db:v1", "init:v1"]}
    problems = hooks.start_problems(PREPARED, moved, "alarm: incident open fg1-1", BASE_IMAGES, live)
    assert any("accepted head" in p for p in problems)
    assert any("outcomes changed" in p for p in problems)
    assert any("incident open fg1-1" in p for p in problems)
    assert any("Deployment/web runs web:v2, the base release has web:v1" in p for p in problems)


def test_a_missing_workload_and_a_missing_prepared_state_are_start_problems() -> None:
    assert hooks.start_problems(None, PREPARED, "", {"Deployment/web": ["web:v1"]}, {}) == [
        "the lane has no recorded prepared state",
        "Deployment/web is not running",
    ]


def test_verify_start_is_a_hook_verb_that_fails_with_its_problems(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    monkeypatch.setattr(hooks, "verify_start", lambda env: ["the operator is not quiet: busy: closure pending"])
    assert hooks.verify_start_verb({}) == 1
    answer = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert answer == {"ok": False, "problems": ["the operator is not quiet: busy: closure pending"]}
    monkeypatch.setattr(hooks, "verify_start", lambda env: [])
    assert hooks.verify_start_verb({}) == 0
