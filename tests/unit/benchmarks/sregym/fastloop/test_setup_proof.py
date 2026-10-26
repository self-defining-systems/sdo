"""Setup reuse is exact-state-bound; live health is never cached."""

import json
import os
import time
from dataclasses import replace

import pytest

from benchmarks.sregym.fastloop import setup_proof as proof


@pytest.fixture
def fingerprint():
    return proof.SetupFingerprint(
        lane="fixture",
        code_revision="a" * 40,
        images={"controller": {"node": "sha256:" + "1" * 64}},
        accepted_memory_commit="b" * 40,
        installed_memory_commit="b" * 40,
        app_manifests="manifest-hash",
        live_objects={"app/Service/web": "uid-spec-hash"},
        tools={"dry_run.py": "tool-hash"},
    )


def save(tmp_path, fingerprint):
    report = {
        "passed": True,
        "fault_injected": False,
        "live_model_calls": False,
        "code_revision": fingerprint.code_revision,
        "lane": fingerprint.lane,
        "invariants": {name: {"passed": True} for name in proof.INVARIANTS},
    }
    return proof.save_receipt(tmp_path, fingerprint, report)


def test_same_fingerprint_reuses_original_passing_receipt(tmp_path, fingerprint):
    path = save(tmp_path, fingerprint)
    decision = proof.reuse_decision(tmp_path, fingerprint)
    assert decision == {"reused": True, "receipt": str(path), "changed": []}
    receipt = json.loads(path.read_text())
    assert receipt["fingerprint"]["images"] == fingerprint.images
    assert receipt["fingerprint"]["accepted_memory_commit"] == fingerprint.accepted_memory_commit


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("code_revision", "c" * 40),
        ("images", {"controller": {"node": "sha256:" + "2" * 64}}),
        ("accepted_memory_commit", "c" * 40),
        ("installed_memory_commit", "c" * 40),
        ("app_manifests", "changed-manifest"),
        ("live_objects", {"app/Service/web": "same-uid-drifted-port"}),
        ("tools", {"dry_run.py": "changed"}),
    ],
)
def test_changed_component_requires_full_proof_and_names_change(tmp_path, fingerprint, field, value):
    save(tmp_path, fingerprint)
    decision = proof.reuse_decision(tmp_path, replace(fingerprint, **{field: value}))
    assert decision["reused"] is False
    assert field in decision["changed"]


def test_reset_with_recreated_object_cannot_reuse(tmp_path, fingerprint):
    save(tmp_path, fingerprint)
    changed = replace(fingerprint, live_objects={"app/Service/web": "new-uid-same-spec"})
    assert not proof.reuse_decision(tmp_path, changed)["reused"]


def test_corrupt_or_failed_receipt_never_reuses(tmp_path, fingerprint):
    path = save(tmp_path, fingerprint)
    receipt = json.loads(path.read_text())
    receipt["report"]["passed"] = False
    path.write_text(json.dumps(receipt))
    assert not proof.reuse_decision(tmp_path, fingerprint)["reused"]


def test_uncommitted_code_cannot_be_certified(tmp_path, fingerprint):
    with pytest.raises(ValueError, match="committed"):
        save(tmp_path, replace(fingerprint, code_revision="a-dirty"))


def test_recorded_lane_fingerprint_is_fast(fingerprint):
    objects = [
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"namespace": "app", "name": f"web-{i}", "uid": str(i)},
            "spec": {"ports": [{"port": 80, "targetPort": 8080}]},
        }
        for i in range(1000)
    ]
    started = time.monotonic()
    identities = proof.object_identities(objects)
    assert len(identities) == 1000
    assert time.monotonic() - started < 15


def test_status_and_resource_version_churn_preserve_identity():
    obj = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"namespace": "app", "name": "web", "uid": "1"},
        "spec": {"ports": [{"port": 80}]},
    }
    before = proof.object_identities([obj])
    obj["status"] = {"loadBalancer": {}}
    obj["metadata"]["resourceVersion"] = "22"
    assert proof.object_identities([obj]) == before
    obj["spec"]["ports"][0]["port"] = 81
    assert proof.object_identities([obj]) != before


@pytest.mark.parametrize(
    ("component", "changed"),
    [
        ("code_revision", "c" * 40),
        ("images", {"controller": {"node": "sha256:" + "2" * 64}}),
        ("accepted_memory_commit", "c" * 40),
        ("installed_memory_commit", "c" * 40),
        ("app_manifests", "changed"),
        ("live_objects", {"app/Service/web": "drift"}),
        ("tools", {"dry_run.py": "changed"}),
    ],
)
def test_deploy_is_skipped_only_with_matching_proof(tmp_path, fingerprint, monkeypatch, component, changed):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import cli

    environment = SimpleNamespace(run_dir=tmp_path)
    calls = []
    worker = SimpleNamespace(request=lambda *args, **kwargs: calls.append(args[0]))
    save(tmp_path / proof.RECEIPTS, fingerprint)
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: fingerprint)
    decision = cli.deploy_with_setup_proof(environment, worker, tmp_path / "results")
    assert not calls
    assert decision["reused"] is True
    assert str(tmp_path / proof.RECEIPTS) in decision["receipt"]
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: replace(fingerprint, **{component: changed}))
    environment.deploy_problem = "fixture"
    environment.baseline_path = tmp_path / "baseline.json"
    decision = cli.deploy_with_setup_proof(environment, worker, tmp_path / "next")
    assert calls == ["deploy"]
    assert decision["changed"] == [component]
    assert json.loads((tmp_path / "next/setup-proof.json").read_text()) == decision


def test_dry_run_reuse_does_not_reset_or_redeploy_and_refreshes_health(tmp_path, fingerprint, monkeypatch):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import admission, cli, dry_run
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = SimpleNamespace(
        run_dir=tmp_path, cluster="fixture", namespace="app", kubeconfig=tmp_path / "kube", dev_mount=None
    )
    report = dry_run.DryRunReport("fixture", code_revision=fingerprint.code_revision)
    report.passed = True
    for row in report.invariants.values():
        row["passed"] = True
    from dataclasses import asdict

    original = proof.save_receipt(tmp_path / proof.RECEIPTS, fingerprint, asdict(report))
    monkeypatch.setattr(FastloopEnvironment, "load", lambda path: environment)
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: fingerprint)
    monkeypatch.setattr(cli, "_reset", lambda *args: pytest.fail("reset on reuse"))
    monkeypatch.setattr(cli, "_sdo_agent", lambda *args, **kwargs: pytest.fail("deploy on reuse"))
    monkeypatch.setattr(cli, "_worker", lambda *args: nullcontext(object()))
    calls = []
    monkeypatch.setenv("KUBECONFIG", "other-lane")
    monkeypatch.setenv("SDO_TIME_SCALE", "previous-scale")

    def live_admission(*args, **kwargs):
        assert os.environ["KUBECONFIG"] == str(environment.kubeconfig)
        calls.append("live-admission")

    monkeypatch.setattr(admission.LaneAdmission, "prove", live_admission)
    monkeypatch.setattr(adapter, "KubectlClusterOps", lambda: object())
    monkeypatch.setattr(dry_run, "assert_active_intake", lambda env: calls.append("intake"))
    assert dry_run.run_lane(tmp_path, 300) == 0
    result = json.loads((tmp_path / "eval-dry-run/report.json").read_text())
    assert result["setup_proof"]["receipt"] == str(original)
    assert result["setup_proof"]["reused"] is True
    assert result["fingerprint"]["live_objects"] == fingerprint.live_objects
    assert calls == ["intake", "live-admission"]


def test_dry_run_reuse_rejects_drift_during_live_checks(tmp_path, fingerprint, monkeypatch):
    # The proof is bound on both sides of the fresh checks, not only on entry.
    from contextlib import nullcontext
    from dataclasses import asdict
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import admission, cli, dry_run
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = SimpleNamespace(
        run_dir=tmp_path, cluster="fixture", namespace="app", kubeconfig=tmp_path / "kube", dev_mount=None
    )
    report = dry_run.DryRunReport("fixture", code_revision=fingerprint.code_revision)
    report.passed = True
    for row in report.invariants.values():
        row["passed"] = True
    proof.save_receipt(tmp_path / proof.RECEIPTS, fingerprint, asdict(report))
    monkeypatch.setenv("KUBECONFIG", "other-lane")
    monkeypatch.setenv("SDO_TIME_SCALE", "previous-scale")
    observations = iter([fingerprint, replace(fingerprint, live_objects={"app/Service/web": "drift"})])
    monkeypatch.setattr(FastloopEnvironment, "load", lambda path: environment)
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: next(observations))
    monkeypatch.setattr(cli, "_worker", lambda *args: nullcontext(object()))
    monkeypatch.setattr(admission.LaneAdmission, "prove", lambda *args, **kwargs: None)
    monkeypatch.setattr(adapter, "KubectlClusterOps", lambda: object())
    monkeypatch.setattr(dry_run, "assert_active_intake", lambda env: None)
    assert dry_run.run_lane(tmp_path, 300) == 1
    result = json.loads((tmp_path / "eval-dry-run/report.json").read_text())
    assert result["passed"] is False
    assert "changed during" in result["invariants"]["failure"]["reason"]


def test_collector_uses_bounded_gets_and_recorded_node_digests(tmp_path):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.environment import Images

    workspace = tmp_path / "repo"
    (workspace / "kubernetes").mkdir(parents=True)
    (workspace / "kubernetes/app.yaml").write_text("kind: Service\n")
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}")
    images = Images()
    node = {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": "node", "uid": "node-uid"},
        "status": {
            "images": [
                {"names": [f"docker.io/library/{name}", f"{name.split(':')[0]}@sha256:{'1' * 64}"]}
                for name in images.model_dump().values()
            ]
        },
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "web", "namespace": "app", "uid": "web-uid"},
        "spec": {"ports": [{"port": 80}]},
    }
    state = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "sdo-controller-state", "namespace": "app-sdo", "uid": "state-uid"},
        "data": {"runtime-state.json": json.dumps({"accepted_source_drift_revision": "inventory-content-hash"})},
    }
    controller = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "controller-1",
            "namespace": "app-sdo",
            "uid": "controller-uid",
            "ownerReferences": [{"kind": "Job", "name": "sdo-controller-run"}],
        },
        "spec": {"containers": [{"name": "controller", "args": ["controller", "--app", "/workspace/application"]}]},
    }
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[0] == "git":
            if "describe" in command:
                stdout = "a" * 40
            elif "rev-parse" in command:
                stdout = "b" * 40
            else:
                stdout = ""
        elif "exec" in command:
            assert command[command.index("--") + 1 :] == ["git", "-C", "/workspace/application", "rev-parse", "HEAD"]
            stdout = "b" * 40
        else:
            assert command[0] == "kubectl"
            assert "get" in command
            assert "secrets" not in " ".join(command)
            if proof.CLUSTER_RESOURCES in command:
                objects = [node]
            else:
                objects = [service] if command[command.index("-n") + 1] == "app" else [state, controller]
            stdout = json.dumps({"items": objects})
        return SimpleNamespace(stdout=stdout)

    environment = SimpleNamespace(
        cluster="fixture",
        namespace="app",
        workspace=workspace,
        kubeconfig=tmp_path / "kube",
        baseline_path=baseline,
        images=images,
        dev_mount=None,
        application="fixture",
        app="sregym",
        deploy_problem="fixture-problem",
        seed_commit="b" * 40,
        worker_env={},
        dev_mount_snapshot=False,
    )
    started = time.monotonic()
    measured = proof.collect_fingerprint(environment, run=run)
    assert time.monotonic() - started < 15
    assert measured.accepted_memory_commit == "b" * 40
    assert measured.installed_memory_commit == "b" * 40
    assert len(measured.images) == 3
    assert "app/Service/web" in measured.live_objects
    assert all(0 < kwargs["timeout"] <= 15 for _, kwargs in calls)
    assert sum(command[0] == "kubectl" for command, _ in calls) == 4
    assert measured.tools["lane-configuration"]
    environment.worker_env["SREGYM_PREINSTALL"] = "true"
    changed = proof.collect_fingerprint(environment, run=run)
    assert measured.tools["lane-configuration"] != changed.tools["lane-configuration"]


def test_live_intake_must_be_active_on_reuse(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop.dry_run import assert_active_intake

    monkeypatch.setattr(
        adapter, "kubectl", lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps({"data": {"state": "paused"}}))
    )
    with pytest.raises(ValueError, match="active SDO intake"):
        assert_active_intake(SimpleNamespace(namespace="app", kubeconfig=tmp_path / "kube", dev_mount=None))


def test_unknown_image_digest_never_grants_reuse():
    with pytest.raises(ValueError, match="digest"):
        proof.image_identity([{"metadata": {"name": "node"}, "status": {"images": []}}], {"controller": "mutable:tag"})


def test_missing_evidence_runs_normal_deployment(tmp_path, fingerprint, monkeypatch):
    from types import SimpleNamespace

    save(tmp_path / proof.RECEIPTS, fingerprint)

    def unavailable(_):
        raise TimeoutError("fixture deadline")

    monkeypatch.setattr(proof, "collect_fingerprint", unavailable)
    _, decision = proof.check_reuse(SimpleNamespace(run_dir=tmp_path))
    assert decision["reused"] is False
    assert "deadline" in decision["changed"][0]


@pytest.mark.parametrize("missing", proof.INVARIANTS)
def test_partial_report_cannot_certify_setup(tmp_path, fingerprint, missing):
    from dataclasses import asdict

    from benchmarks.sregym.fastloop.dry_run import DryRunReport

    report = DryRunReport(fingerprint.lane, code_revision=fingerprint.code_revision)
    report.passed = True
    for row in report.invariants.values():
        row["passed"] = True
    del report.invariants[missing]
    with pytest.raises(ValueError, match="invariants"):
        proof.save_receipt(tmp_path, fingerprint, asdict(report))


def test_deployment_reuse_requires_the_callers_live_admission(tmp_path, fingerprint, monkeypatch):
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import cli

    save(tmp_path / proof.RECEIPTS, fingerprint)
    monkeypatch.setattr(proof, "collect_fingerprint", lambda env: pytest.fail("non-SDO proof reuse"))
    calls = []
    environment = SimpleNamespace(run_dir=tmp_path, deploy_problem="fixture", baseline_path=tmp_path / "baseline")
    worker = SimpleNamespace(request=lambda *args, **kwargs: calls.append(args[0]))
    decision = cli.deploy_with_setup_proof(environment, worker, tmp_path / "result", allow_reuse=False)
    assert decision["reused"] is False
    assert calls == ["deploy"]
