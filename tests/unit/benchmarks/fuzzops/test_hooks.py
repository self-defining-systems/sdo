"""SDO's fuzzops-bench hooks: baseline manifest, check, render comparison, idle verdict and install arguments."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
import yaml

from benchmarks.fuzzops import hooks
from benchmarks.fuzzops.hooks import (
    Baseline,
    HookError,
    Kube,
    OperatorBlock,
    idle_reason,
    idle_verdict,
    install_existing_args,
    plain_docs,
    plain_pack,
    render_difference,
    write_baseline,
)

if TYPE_CHECKING:
    from pathlib import Path

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
            controller_flags=("--response-timeout", "1800s"),
            **fields,
        ),
    )
    return root


def _operator_file(tmp: Path, baseline: Path, **settings: str) -> Path:
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
    assert loaded.controller_flags == ("--response-timeout", "1800s")
    assert len(loaded.render_sha256) == 64
    raw = json.loads((root / "baseline.json").read_text())
    assert raw["format"] == 1


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"commit": "abc"}, "40 hex"),
        ({"bundle": "../memory.bundle"}, "inside the baseline"),
        ({"controller_flags": ("--model", "x")}, "set by the hook"),
        ({"controller_flags": ("--kubeconfig=/k",)}, "set by the hook"),
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
    ],
)
def test_idle_reason(state: dict[str, Any] | None, why: str) -> None:
    assert idle_reason(state) == why


NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


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


def test_idle_reads_the_pod_and_the_state_configmap_once_each(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert len(calls) == 2
    assert any("--show-managed-fields" in c for c in calls)
    assert all(c[c.index("-n") + 1] == "ns1-sdo" for c in calls)


def test_idle_reports_a_missing_controller_as_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hooks.subprocess, "run", _fake_kubectl({}, []))
    env = {"FUZZOPS_NAMESPACE": "ns1", "FUZZOPS_KUBE_CONTEXT": "kind-c", "KUBECONFIG": "/k"}
    lines: list[str] = []
    assert hooks.idle(env, out=lines.append) == 1
    assert lines[0].startswith("down:")


# ---- install, teardown, watch --------------------------------------------------------------------------------


def test_install_existing_args_take_the_baseline_flags_and_the_settings(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    block = OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x", effort="medium", image_tag="fz-t"))
    sink_flags = ["--evidence-sink-host-path", "/bench-evidence", "--evidence-run-id", "e-l-1"]
    args = install_existing_args(
        tmp_path / "sdo-operator", tmp_path / "app", Baseline.load(root), block, tmp_path / "kubeconfig", sink_flags
    )
    assert args[1] == "install-existing"

    def flag(name: str) -> str:
        return args[args.index(name) + 1]

    assert (flag("--namespace"), flag("--control-namespace"), flag("--application")) == ("ns1", "ns1-sdo", "ns1")
    assert (flag("--model"), flag("--agent-effort"), flag("--agent-provider")) == ("gpt-x", "medium", "codex")
    assert flag("--controller-image") == "sdo-controller:fz-t"
    assert flag("--script-image") == "sdo-script-tools:fz-t"
    assert flag("--response-timeout") == "1800s"
    assert flag("--kubeconfig") == str(tmp_path / "kubeconfig")
    assert (flag("--evidence-sink-host-path"), flag("--evidence-run-id")) == ("/bench-evidence", "e-l-1")


def test_install_existing_args_refuse_a_lane_without_an_evidence_sink(tmp_path: Path) -> None:
    root = _baseline_dir(tmp_path)
    block = OperatorBlock.load(_operator_file(tmp_path, root, model="gpt-x"))
    with pytest.raises(HookError, match="evidence sink"):
        install_existing_args(tmp_path / "b", tmp_path / "app", Baseline.load(root), block, tmp_path / "k", [])


@pytest.mark.parametrize("flag", ["--evidence-sink-host-path", "--evidence-run-id", "--evidence-sink-volume"])
def test_a_baseline_cannot_set_the_evidence_sink(flag: str) -> None:
    with pytest.raises(ValueError, match="set by the hook"):
        Baseline(
            namespace="ns1", bundle="b", branch="x", commit=COMMIT, render="r", controller_flags=(flag, "/elsewhere")
        )


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
    assert calls[0][calls[0].index("delete") + 1 :][:2] == ["namespace", "ns1-sdo"]
    assert "--wait=true" in calls[0]
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
    env["BENCH_FAULT"] = "none"
    hooks.watch(env)
    assert "--fault" not in seen[1]


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
    }
    assert json.loads((out / "collect.json").read_text())["parts"]["evidence"].startswith("ok")
    assert json.loads((out / "collect.json").read_text())["parts"]["view"].startswith("ok")
    assert json.loads((out / "view.json").read_text())["schema"] == "fuzzops.view/v1"


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


def test_collect_ends_with_an_operator_reset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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
    out = tmp_path / "out"
    hooks.collect(_sink_env(tmp_path, BENCH_OUT=str(out), BENCH_LANE_DIR=str(lane)), out=lambda _: None)
    assert len(resets) == 1
    assert json.loads((out / "collect.json").read_text())["parts"]["reset"] == "ok (generation 7)"
    assert json.loads((out / "incidents-reset.json").read_text())["generation"] == 7


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
