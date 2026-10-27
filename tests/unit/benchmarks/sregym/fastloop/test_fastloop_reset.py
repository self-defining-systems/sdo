from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.fastloop import cli, reset
from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.loop import AgentOutcome, InjectionWindow, LoopConfig, run_incidents
from benchmarks.sregym.fastloop.records import OracleVerdict
from benchmarks.sregym.fastloop.reset import KubectlResetCluster, reset_environment

if TYPE_CHECKING:
    from pathlib import Path


class FakeCluster:
    def __init__(self) -> None:
        self.objects = [
            {"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 0}}
        ]
        self.accepted_commit = "changed"
        self.sessions = ["old-session"]
        self.state = {"incident_open": True}
        self.jobs = ["sdo-responder-old"]

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        assert namespace == "hotel"
        return self.objects

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        assert namespace == "hotel"
        self.objects = [value]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.objects = [item for item in self.objects if item["metadata"]["name"] != name]

    def reset_controller(self, seed_commit: str, *, preserve_repository: bool = False) -> None:
        if not preserve_repository:
            self.accepted_commit = seed_commit
        self.sessions.clear()
        self.state.clear()
        self.jobs.clear()


class FakeWorker:
    calls: list[str]

    def __init__(self) -> None:
        self.calls = []

    def request(self, op: str, **_kwargs: object) -> dict[str, object]:
        self.calls.append(op)
        return {"healthy": True} if op == "health" else {"success": True}


def test_reset_restores_application_and_clears_controller_on_existing_environment(tmp_path: Path) -> None:
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    environment.save()
    baseline = ApplicationBaseline(
        "hotel", [{"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 1}}]
    )
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    cluster = FakeCluster()

    worker = FakeWorker()
    reset_environment(environment, cluster, worker)

    assert cluster.objects == baseline.objects
    assert cluster.accepted_commit == environment.seed_commit
    assert cluster.sessions == cluster.jobs == []
    assert cluster.state == {}
    assert worker.calls == ["clean_start", "health", "baseline_oracle"]


def test_reset_requires_a_captured_baseline_and_seed_commit(tmp_path: Path) -> None:
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    cluster = FakeCluster()

    with pytest.raises(ValueError, match="seed commit"):
        reset_environment(environment, cluster, FakeWorker())
    assert cluster.accepted_commit == "changed"


def test_reset_accepts_up_cluster_before_controller_install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_kubectl(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", 'Error from server (NotFound): namespaces "sdo" not found')

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", fake_kubectl)
    KubectlResetCluster(tmp_path / "kubeconfig", "sdo").reset_controller("a" * 40)

    assert calls == [["get", "namespace", "sdo"]]


def test_fail_soft_controller_reset_preserves_accepted_repository(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []
    payloads: list[dict[str, Any]] = []

    def fake_kubectl(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if "input_text" in kwargs:
            payloads.append(json.loads(kwargs["input_text"]))
        if args[:2] == ["get", "job"]:
            spec = {
                "containers": [
                    {
                        "name": "controller",
                        "image": "controller:test",
                        "volumeMounts": [{"name": "repository", "mountPath": "/workspace"}],
                    }
                ],
                "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "repository"}}],
            }
            return subprocess.CompletedProcess(args, 0, json.dumps({"spec": {"template": {"spec": spec}}}), "")
        if args[:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(
                args, 0, '{"items":[{"metadata":{"name":"controller"},"status":{"phase":"Running"}}]}', ""
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.kubectl", fake_kubectl)

    def fake_reset_control_state(control: str, **kwargs: Any) -> dict[str, Any]:
        calls.append(["reset-control-state", control, str(kwargs)])
        return {"control_namespace": control, "deleted": []}

    monkeypatch.setattr("benchmarks.sregym.fastloop.reset.reset_control_state", fake_reset_control_state)
    KubectlResetCluster(tmp_path / "kubeconfig", "sdo").reset_controller("a" * 40, preserve_repository=True)

    assert payloads, "warm reset must remove proposal scratch without deleting accepted history"
    assert "git -C" not in payloads[0]["spec"]["containers"][0]["command"][2]
    stopped = next(i for i, args in enumerate(calls) if args[:2] == ["delete", "jobs"])
    retired = next(i for i, args in enumerate(calls) if args[:3] == ["delete", "configmap", "sdo-controller-state"])
    assert stopped < retired
    # Worker ConfigMaps of killed attempts go after every writer stopped, and only they:
    # the Go owner keeps onboarding authority when workers_only is set.
    workers = next(i for i, args in enumerate(calls) if args[0] == "reset-control-state")
    assert stopped < workers
    assert "'workers_only': True" in calls[workers][2]
    assert not any(args[:2] == ["delete", "namespace"] for args in calls)


def test_failed_learning_keeps_program_for_third_incident(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import datetime, timezone

    workspace = tmp_path / "workspace"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)
    (workspace / "README.md").write_text("seed\n")
    subprocess.run(["git", "add", "README.md"], cwd=workspace, check=True)
    commit = ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm"]
    subprocess.run([*commit, "seed"], cwd=workspace, check=True)
    seed_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
    program = workspace / ".sdo/playbooks/service-targetport-mismatch/diagnose.go"
    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=workspace,
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit=seed_commit,
    )
    baseline = ApplicationBaseline(
        "hotel",
        [{"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 1}}],
    )
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    cluster = FakeCluster()
    monkeypatch.setattr(reset, "KubectlResetCluster", lambda *_args: cluster)
    # This callback exercises the full CLI restore shell around the fake app.
    # Expose its actual ready workload through the Kubernetes read boundary.
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import admission, baseline_recovery, clean_start

    document = json.loads(environment.baseline_path.read_text())
    document.update(mutating_webhook_configs=[], validating_webhook_configs=[])
    environment.baseline_path.write_text(json.dumps(document))

    def api(args, **_kwargs):
        objects = []
        if args[:2] in (["get", "deployments,replicasets"], ["get", "deployments,statefulsets,daemonsets,pods"]):
            for original in cluster.objects:
                value = json.loads(json.dumps(original))
                value["metadata"].update(uid="app-uid", generation=1)
                value["spec"]["selector"] = {"matchLabels": {"app": "app"}}
                value["status"] = {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "readyReplicas": 1}
                objects.append(value)
            objects.append(
                {
                    "kind": "Pod",
                    "metadata": {"name": "app-pod", "uid": "app-pod-uid", "labels": {"app": "app"}},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                }
            )
        return SimpleNamespace(stdout=json.dumps({"items": objects}), returncode=0, stderr="")

    monkeypatch.setattr(reset, "kubectl", api)
    monkeypatch.setattr(admission, "kubectl", api)
    monkeypatch.setattr(baseline_recovery, "kubectl", api)
    monkeypatch.setattr(clean_start, "kubectl", api)
    now = datetime(2026, 10, 21, tzinfo=timezone.utc)
    accepted_commit = ""

    class Agent:
        name = "sdo"
        model = "test"

        def resolve(self, index: int, _problem: str, inject: Any) -> AgentOutcome:
            window = inject()
            if index == 2:
                assert program.is_file()
                assert (
                    subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=workspace, text=True).strip()
                    == accepted_commit
                )
                assert cluster.accepted_commit == accepted_commit
            return AgentOutcome(injection=window, incident_id=f"incident-{index}")

        def learn(self, outcome: AgentOutcome) -> AgentOutcome:
            nonlocal accepted_commit
            if outcome.incident_id == "incident-0":
                program.parent.mkdir(parents=True)
                program.write_text("package targetport\n")
                subprocess.run(["git", "add", ".sdo"], cwd=workspace, check=True)
                subprocess.run([*commit, "accepted program"], cwd=workspace, check=True)
                accepted_commit = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=workspace, text=True
                ).strip()
                cluster.accepted_commit = accepted_commit
            if outcome.incident_id == "incident-1":
                raise RuntimeError("learning failed")
            return outcome

        def prepare_recovery(self, outcome: AgentOutcome | None) -> None:
            """This scripted agent has no background controller."""

        def close(self) -> None:
            pass

    class Driver:
        def inject(self, _problem: str) -> InjectionWindow:
            return InjectionWindow(started_at=now, finished_at=now)

        def oracle(self) -> OracleVerdict:
            return OracleVerdict(kind="sregym-mitigation-oracle", success=True)

        def recover(self) -> float:
            return 0.0

    config = LoopConfig(
        run_id="test",
        problems=("fault",),
        incidents=3,
        results_path=tmp_path / "incidents.jsonl",
        continue_on_error=True,
    )
    records = run_incidents(
        Agent(),
        Driver(),
        config,
        on_failed_incident=lambda: cli._reset_failed_incident(environment, FakeWorker()),
    )
    assert len(records) == 3
    assert "learning failed" in (records[1].error or "")
    assert records[2].error is None
    assert cluster.accepted_commit == accepted_commit


def test_reset_cancels_and_awaits_workers_before_deleting_captures(tmp_path: Path, monkeypatch) -> None:
    workers_running = True
    cleaned = False
    pod_spec = {
        "containers": [
            {
                "name": "controller",
                "image": "controller:test",
                "volumeMounts": [{"name": "repository", "mountPath": "/workspace"}],
            }
        ],
        "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "repository"}}],
    }

    def fake_kubectl(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal workers_running, cleaned
        if args[:2] == ["get", "pods"]:
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    {"items": [{"metadata": {"name": "controller"}, "status": {"phase": "Running"}, "spec": pod_spec}]}
                ),
                "",
            )
        if args[:2] == ["get", "job"]:
            return subprocess.CompletedProcess(args, 0, json.dumps({"spec": {"template": {"spec": pod_spec}}}), "")
        if args[:2] == ["delete", "jobs"]:
            assert "--wait=true" in args
            assert "--cascade=foreground" in args
            workers_running = False
        payload = kwargs.get("input_text", "")
        if args[:1] == ["exec"] or '"kind": "Pod"' in payload:
            assert not workers_running, "reset removed captures while a responder could still read them"
            cleaned = True
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(reset, "kubectl", fake_kubectl)
    monkeypatch.setattr(
        reset, "reset_control_state", lambda control, **_: {"control_namespace": control, "deleted": []}
    )
    KubectlResetCluster(tmp_path / "kubeconfig", "sdo").reset_controller("a" * 40)
    assert cleaned


@pytest.mark.parametrize(
    ("has_job", "orphan_phase"), [(True, None), (False, None), (False, "Running"), (False, "Pending")]
)
@pytest.mark.parametrize("preserve", [True, False])
@pytest.mark.parametrize("stop_fails_once", [False, True])
def test_pending_reset_retires_state_and_preserves_durable_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    has_job: bool,
    orphan_phase: str | None,
    preserve: bool,
    stop_fails_once: bool,
) -> None:
    """Execute the actual cleanup script on synthetic Git and ledger files."""
    import shlex

    workspace = tmp_path / "workspace"
    repository = workspace / "application"
    repository.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    commit = ["git", "-C", str(repository), "-c", "user.name=Test", "-c", "user.email=test@example.invalid"]
    (repository / "README.md").write_text("seed\n")
    subprocess.run([*commit, "add", "."], check=True)
    subprocess.run([*commit, "commit", "-qm", "seed"], check=True)
    seed = subprocess.check_output([*commit, "rev-parse", "HEAD"], text=True).strip()
    memory = repository / ".sdo/playbooks/repair/diagnose.go"
    memory.parent.mkdir(parents=True)
    memory.write_text("package repair\n")
    subprocess.run([*commit, "add", "."], check=True)
    subprocess.run([*commit, "commit", "-qm", "accepted memory"], check=True)
    accepted = subprocess.check_output([*commit, "rev-parse", "HEAD"], text=True).strip()
    ledger = workspace / "worktrees/native-ledger/prior.json"
    ledger.parent.mkdir(parents=True)
    ledger.write_text('{"incident_id":"prior","version":1}\n')
    ledger_bytes = ledger.read_bytes()
    session = workspace / ".sdo-runtime/sessions/prior/evidence.json"
    session.parent.mkdir(parents=True)
    session.write_text("{}\n")
    proposal = workspace / "worktrees/proposal-prior/proposal.txt"
    proposal.parent.mkdir(parents=True)
    proposal.write_text("scratch\n")
    unrelated = workspace / "credentials/opaque"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"synthetic-do-not-touch")
    historical = {"sdo-prior-request": "evidence", "sdo-prior-result": "evidence"}
    transient = {"sdo-controller-state", "sdo-controller-state-drift-verdicts"}
    configmaps = {**historical, **dict.fromkeys(transient, "pending")}
    state: dict[str, Any] = {"incident_open": True, "incident_request": {"incident_id": "prior"}}
    stopped = False
    fail_stop = stop_fails_once
    orphan_stopped: list[bool] = []
    spec = {
        "containers": [
            {
                "name": "controller",
                "image": "controller:test",
                "volumeMounts": [{"name": "repository", "mountPath": "/workspace"}],
            }
        ],
        "volumes": [{"name": "repository", "persistentVolumeClaim": {"claimName": "accepted"}}],
    }

    def fake_kubectl(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal stopped, fail_stop
        if args[:2] == ["get", "pods"]:
            items = (
                [{"metadata": {"name": "orphan-controller"}, "status": {"phase": orphan_phase}, "spec": spec}]
                if orphan_phase
                else []
            )
            return subprocess.CompletedProcess(args, 0, json.dumps({"items": items}), "")
        if args[:2] == ["get", "job"]:
            return subprocess.CompletedProcess(
                args,
                0 if has_job else 1,
                json.dumps({"spec": {"template": {"spec": spec}}}) if has_job else "",
                "NotFound" if not has_job else "",
            )
        if args[:2] == ["delete", "jobs"]:
            if fail_stop:
                fail_stop = False
                raise RuntimeError("synthetic writer stop failed")
            assert "--wait=true" in args
            assert "--cascade=foreground" in args
            stopped = True
        if args[:3] == ["delete", "pod", "orphan-controller"]:
            assert "--wait=true" in args
            orphan_stopped.append(True)
        if args[:3] == ["delete", "configmap", "sdo-controller-state"]:
            assert stopped, "reset retired state while a controller could recreate it"
            assert not orphan_phase or orphan_stopped, "orphan controller still running"
            for name in args[2:]:
                configmaps.pop(name, None)
            state.clear()
        if args[:1] == ["apply"]:
            assert stopped
            pod = json.loads(kwargs["input_text"])
            command = pod["spec"]["containers"][0]["command"]
            command[2] = command[2].replace("/workspace", shlex.quote(str(workspace)))
            subprocess.run(command, check=True, capture_output=True)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(reset, "kubectl", fake_kubectl)
    cluster = KubectlResetCluster(tmp_path / "kubeconfig", "sdo")
    if stop_fails_once:
        with pytest.raises(RuntimeError, match="synthetic writer stop failed"):
            cluster.reset_controller(seed, preserve_repository=preserve)
        assert state["incident_open"] is True
        assert set(configmaps) == set(historical) | transient
        assert ledger.read_bytes() == ledger_bytes
        assert proposal.exists()
        assert session.exists()
        assert memory.exists()
    cluster.reset_controller(seed, preserve_repository=preserve)
    head = subprocess.check_output([*commit, "rev-parse", "HEAD"], text=True).strip()
    if preserve or not (has_job or orphan_phase):
        assert head == accepted
        assert memory.is_file()
        assert ledger.read_bytes() == ledger_bytes, "warm reset destroyed durable broker history"
    else:
        assert head == seed
        assert not ledger.exists()
        assert not session.exists()
    assert state == {}, "next controller would resume an expired prior incident"
    assert configmaps == historical, "reset must retire transient drift cache and retain historical evidence"
    assert unrelated.read_bytes() == b"synthetic-do-not-touch"
    if has_job or orphan_phase:
        assert not proposal.exists(), "proposal checkout must not survive a completed reset"
        assert not session.exists(), "session scratch must not survive a completed reset"


def test_restore_finishes_recovery_mutations_before_ready_snapshot(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.baseline_recovery import save_application_baseline

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    environment.baseline_path.write_text("{}")
    save_application_baseline(environment.baseline_path, (ApplicationBaseline("hotel", FakeCluster().objects),))
    events = []
    oracle_calls = 0

    def request(op, **kwargs):
        nonlocal oracle_calls
        events.append(op)
        if op == "health":
            return {"healthy": True}
        if op == "baseline_oracle":
            oracle_calls += 1
            return {"success": oracle_calls > 1}
        return {"success": True}

    checks = SimpleNamespace(
        settle=lambda: None, ready=lambda: events.append("ready"), complete=lambda: events.append("complete")
    )
    reset.restore_application(environment, FakeCluster(), SimpleNamespace(request=request), checks=checks)
    assert events[-2:] == ["ready", "complete"]
    assert events.count("baseline_oracle") == 2


def test_restore_rejects_malformed_lock_before_any_application_mutation(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.baseline_recovery import save_application_baseline

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    environment.baseline_path.write_text("{}")
    save_application_baseline(environment.baseline_path, (ApplicationBaseline("hotel", FakeCluster().objects),))
    rendered = environment.workspace / ".sdo/rendered"
    rendered.mkdir(parents=True)
    (rendered / "lock.json").write_text(
        json.dumps({"objects": [{"ref": {"kind": "Deployment", "name": "app"}, "error": "unknown gap"}]})
    )
    worker = FakeWorker()
    with pytest.raises(ValueError, match="rendered.*gap"):
        reset.restore_application(environment, FakeCluster(), worker)
    assert worker.calls == []


@pytest.mark.parametrize("operation", ["reset", "restore"])
def test_recovery_never_mutates_while_native_reflection_is_pending(tmp_path: Path, operation: str) -> None:
    from datetime import datetime, timezone

    from benchmarks.sregym.adapter.persistent import ControllerRecord, PendingIncident, PersistentState

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="kind-test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="a" * 40,
    )
    environment.save()
    baseline = ApplicationBaseline(
        "hotel", [{"kind": "Deployment", "metadata": {"name": "app", "namespace": "hotel"}, "spec": {"replicas": 1}}]
    )
    environment.baseline_path.write_text(json.dumps({"fastloop_application": {"namespaces": [baseline.__dict__]}}))
    record = ControllerRecord(
        application="hotel",
        namespace="hotel",
        control_namespace="hotel-sdo",
        lifecycle_fingerprint="f",
        controller_pod_uid="uid",
        controller_pod_name="controller",
        installed_by_stage="test",
        installed_at=datetime.now(timezone.utc),
        runtime_config={},
        pending=PendingIncident(
            incident_id="i1",
            stage_label="test",
            receipt_dir=tmp_path,
            repository=environment.workspace,
            verified_at=datetime.now(timezone.utc),
            stage_receipt={},
        ),
    )
    state = PersistentState(controllers={"hotel": record})
    state.save(environment.state_path)
    cluster, worker = FakeCluster(), FakeWorker()
    invoke = reset_environment if operation == "reset" else reset.restore_application
    with pytest.raises(RuntimeError, match="durable terminal reflection"):
        invoke(environment, cluster, worker)
    assert cluster.accepted_commit == "changed"
    assert cluster.sessions == ["old-session"]
    assert worker.calls == []

    # The native learning edge clears pending only after its durable finalization.
    record.pending = None
    state.save(environment.state_path)
    invoke(environment, cluster, worker)
    assert worker.calls == ["clean_start", "health", "baseline_oracle"]


def test_restore_receipt_names_each_reapplied_object_and_warns_on_undeclared_recreation(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    import hashlib

    from benchmarks.sregym.fastloop.baseline_recovery import RESTORE_RECEIPTS, save_application_baseline

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    alias = {
        "kind": "Service",
        "metadata": {"name": "jaeger-agent", "namespace": "hotel"},
        "spec": {"type": "ExternalName", "externalName": "otel-collector.observe.svc.cluster.local"},
    }
    extra = {"kind": "ConfigMap", "metadata": {"name": "extra", "namespace": "hotel"}, "data": {"a": "b"}}
    app = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "app", "namespace": "hotel"},
        "spec": {"replicas": 1},
    }
    environment.baseline_path.write_text("{}")
    save_application_baseline(environment.baseline_path, (ApplicationBaseline("hotel", [app, alias, extra]),))
    lock = environment.workspace / ".sdo/rendered/lock.json"
    lock.parent.mkdir(parents=True)
    manifest = json.dumps(app).encode()
    (lock.parent / "app.yaml").write_bytes(manifest)
    reference = {"api_version": "apps/v1", "kind": "Deployment", "namespace": "hotel", "name": "app"}
    row = {"ref": reference, "file": "app.yaml", "sha256": hashlib.sha256(manifest).hexdigest()}
    lock.write_text(json.dumps({"objects": [row]}))
    cluster = FakeCluster()  # only Deployment/app (replicas 0): the alias and ConfigMap are absent

    reset.restore_application(environment, cluster, FakeWorker())

    receipts = [json.loads(line) for line in (tmp_path / RESTORE_RECEIPTS).read_text().splitlines()]
    assert receipts[0]["reapplied"] == [
        {"kind": "ConfigMap", "namespace": "hotel", "name": "extra", "recreated": True, "harness_owned": False},
        {"kind": "Deployment", "namespace": "hotel", "name": "app", "recreated": False, "harness_owned": False},
    ]
    assert receipts[0]["warnings"] == [
        {"type": "restore_recreated_undeclared_object", "object": "ConfigMap/extra"},
        {"type": "harness_redirect_left_absent", "object": "Service/jaeger-agent"},
    ]
    assert "restore_recreated_undeclared_object" in caplog.text
