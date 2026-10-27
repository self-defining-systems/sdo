"""Admission invariants apply to every app, including real harness manifests."""

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import clean_start


@pytest.mark.parametrize("namespace", ["hotel-reservation", "astronomy-shop", "social-network"])
def test_probe_placement_preserves_app_dns(namespace):
    value = {
        "kind": "Pod",
        "metadata": {"namespace": namespace, "labels": {"app.kubernetes.io/managed-by": "sregym-composite-probe"}},
        "spec": {"containers": [{"command": ["sh", "-c", f"wget http://frontend.{namespace}.svc.cluster.local"]}]},
    }
    original = deepcopy(value)
    isolated = clean_start.isolate_benchmark_probe(value)
    assert isolated["metadata"]["namespace"] == "sregym"
    assert isolated["spec"] == original["spec"]
    assert value == original


@pytest.mark.parametrize("kind", ["Pod", "Job", "ConfigMap"])
def test_admission_rejects_any_benchmark_labelled_object(monkeypatch, kind):
    monkeypatch.setattr(clean_start, "_injected_objects", lambda _: [])

    def api(args, **kwargs):
        if args[args.index("-o") + 1] == "name":
            return SimpleNamespace(stdout="")
        return SimpleNamespace(
            stdout='{"items":[{"kind":"'
            + kind
            + '","metadata":{"name":"foreign","labels":{"app.kubernetes.io/managed-by":"sregym-composite-probe"}}}]}'
        )

    monkeypatch.setattr(clean_start, "kubectl", api)
    with pytest.raises(RuntimeError, match="benchmark objects"):
        clean_start.assert_clean_baseline(Path("unused"), namespace="any-app")


def test_pending_traffic_with_reason_is_not_injection_proof():
    from benchmarks.sregym.adapter.eval_guards import controller_proof

    state = {
        "history": [
            {
                "detector_id": "traffic-health",
                "status": "pending",
                "reason": "unqualified scenarios: checkout",
                "evaluated_at": "2026-10-25T20:00:00Z",
            }
        ]
    }
    proof = controller_proof(state, {"traffic-health"}, "2026-10-25T19:59:00Z")
    assert not proof["passed"]
    assert "unqualified scenarios" in str(proof)


def test_old_clear_cannot_prove_health_after_resume():
    from benchmarks.sregym.adapter.eval_guards import controller_proof

    state = {"history": [{"detector_id": "traffic-health", "status": "clear", "evaluated_at": "2026-10-25T19:58:00Z"}]}
    assert not controller_proof(state, {"traffic-health"}, "2026-10-25T19:59:00Z")["passed"]


@pytest.mark.parametrize("path", [".sdo/outcomes/x.json", ".sdo/playbooks/repair/README.md", "kubernetes/app.yaml"])
def test_setup_health_ownership_cannot_admit_incident_learning(path):
    from benchmarks.sregym.adapter.eval_guards import validate_setup_commit

    with pytest.raises(ValueError, match="health-owned"):
        validate_setup_commit("health_judge", "initial", "passed", [path])


def test_real_setup_ownership_shapes_are_admitted():
    from benchmarks.sregym.adapter.eval_guards import validate_setup_commit

    validate_setup_commit(
        "health_judge",
        "initial",
        "passed",
        [
            ".sdo/diagnostics/detectors/health/traffic-health/detector.go",
            ".sdo/diagnostics/traffic/workloads/frontend.yaml",
            ".sdo/playbooks/health-objective/README.md",
        ],
    )
    validate_setup_commit("controller", "rendered", "passed", [".sdo/rendered/lock.json"])
    with pytest.raises(ValueError, match="unvalidated health setup"):
        validate_setup_commit("responder", "initial", "passed", [".sdo/rendered/lock.json"])


def test_stabilization_observes_until_deadline_then_fails_without_injecting():
    from benchmarks.sregym.adapter.eval_guards import StabilizationError, wait_for_proof

    class Clock:
        now = 0

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    clock = Clock()
    samples = []
    with pytest.raises(StabilizationError):
        wait_for_proof(
            lambda: {"passed": False, "reason": "pending"},
            samples.append,
            timeout_seconds=5,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    assert clock.now == 5
    assert samples[-1]["passed"] is False


def test_late_dependency_roll_fails_even_when_all_pods_ready():
    from benchmarks.sregym.adapter.eval_guards import assert_stable_workloads

    before = {"Deployment/registry": {"generation": 1, "pods": ["uid-a"]}}
    after = {"Deployment/registry": {"generation": 2, "pods": ["uid-b"]}}
    with pytest.raises(ValueError, match="rolled after Ready"):
        assert_stable_workloads(before, after)


def test_restore_uses_one_accepted_spec_instead_of_stale_capture(tmp_path):
    import hashlib

    import yaml

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, accepted_restore_baselines

    rendered = tmp_path / ".sdo/rendered"
    rendered.mkdir(parents=True)
    stale = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"namespace": "app", "name": "registry"},
        "spec": {"template": {"spec": {"containers": [{"name": "registry", "image": "old"}]}}},
    }
    accepted = deepcopy(stale)
    accepted["spec"]["template"]["spec"]["containers"][0]["image"] = "installed"
    payload = yaml.safe_dump(accepted).encode()
    (rendered / "registry.yaml").write_bytes(payload)
    (rendered / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {"kind": "Deployment", "namespace": "app", "name": "registry"},
                        "file": "registry.yaml",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ]
            }
        )
    )
    baselines = accepted_restore_baselines(tmp_path, (ApplicationBaseline("app", [stale]),))
    assert baselines[0].objects == [accepted]
    assert stale["spec"]["template"]["spec"]["containers"][0]["image"] == "old"


def test_rbac_equality_uses_real_installer_and_rejects_extra_grants():
    from benchmarks.sregym.fastloop.rbac import installer_rbac, require_rbac_equality

    expected = installer_rbac("fixture-app")
    listing = []
    for key in ("ClusterRoles", "ClusterBindings"):
        for original in expected[key]:
            row = deepcopy(original)
            row["kind"] = "ClusterRoleBinding" if key.endswith("Bindings") else "ClusterRole"
            listing.append(row)
    assert require_rbac_equality(expected, listing, "fixture-app")["passed"]
    for row in listing:
        if row["kind"] == "ClusterRole":
            row["rules"].append({"apiGroups": ["example.dev"], "resources": ["widgets"], "verbs": ["get"]})
            break
    with pytest.raises(ValueError, match="RBAC differs"):
        require_rbac_equality(expected, listing, "fixture-app")


def test_no_fault_dry_run_has_no_fault_selection_and_has_a_five_minute_cap(tmp_path):
    from benchmarks.sregym.fastloop.cli import build_parser

    args = build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path)])
    assert args.timeout == 300
    assert not hasattr(args, "problem")
    with pytest.raises(SystemExit):
        build_parser().parse_args(["eval-dry-run", "--run-dir", str(tmp_path), "--timeout", "301"])


def test_restore_retires_unlabelled_admission_objects_outside_captured_and_accepted_sets(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop.clean_start import restore_admission_baseline

    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps({"mutating_webhook_configs": ["installed"], "validating_webhook_configs": []}))
    calls = []

    def api(args, **kwargs):
        calls.append(args)
        items = [
            {"kind": "MutatingWebhookConfiguration", "metadata": {"name": name}}
            for name in ["installed", "stale-unlabelled"]
        ]
        return SimpleNamespace(stdout=json.dumps({"items": items}))

    monkeypatch.setattr(clean_start, "kubectl", api)
    restore_admission_baseline(Path("unused"), baseline, tmp_path)
    deletes = [call for call in calls if call[0] == "delete"]
    assert len(deletes) == 1
    assert deletes[0][1] == "MutatingWebhookConfiguration/stale-unlabelled"


def test_restore_retires_generated_failed_replicas_without_reapplying_app_specs():
    from benchmarks.sregym.fastloop.baseline_recovery import stalled_replica_sets

    deployment = {
        "kind": "Deployment",
        "metadata": {"name": "api", "uid": "deployment"},
        "spec": {"replicas": 1},
        "status": {"readyReplicas": 0},
    }
    replica = {
        "kind": "ReplicaSet",
        "metadata": {
            "name": "api-old",
            "ownerReferences": [{"kind": "Deployment", "uid": "deployment", "controller": True}],
        },
        "status": {"conditions": [{"type": "ReplicaFailure", "status": "True", "reason": "FailedCreate"}]},
    }
    assert stalled_replica_sets([deployment, replica]) == ["api-old"]
    deployment["status"]["readyReplicas"] = 1
    assert stalled_replica_sets([deployment, replica]) == []


def test_fresh_reset_removes_only_prior_responder_grants(monkeypatch):
    from benchmarks.sregym.fastloop import rbac

    calls = []

    def binding(name, subject_namespace):
        return {
            "kind": "RoleBinding",
            "metadata": {"name": name, "namespace": "app"},
            "subjects": [{"kind": "ServiceAccount", "name": "sdo-responder", "namespace": subject_namespace}],
        }

    def api(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(
            stdout=json.dumps({"items": [binding("prior-permissive", "app-sdo"), binding("unrelated", "peer-sdo")]})
        )

    monkeypatch.setattr(rbac, "kubectl", api)
    rbac.reset_responder_grants(Path("unused"), "app")
    assert [call[1] for call in calls if call[0] == "delete"] == ["RoleBinding/prior-permissive"]


@pytest.mark.parametrize("history", [None, []])
def test_absent_controller_observations_are_unproven(history):
    from benchmarks.sregym.adapter.eval_guards import controller_proof

    assert not controller_proof({"history": history}, {"traffic-health"}, "2026-10-25T20:00:00Z")["passed"]


@pytest.mark.parametrize("windows", [None, {}, {"health": {"scenarios": None}}])
def test_unstarted_prober_windows_are_unproven(windows):
    from benchmarks.sregym.adapter.eval_guards import qualified_scenarios

    assert not qualified_scenarios({"windows": windows}, {"frontend"})


def test_dependency_restart_requires_clients_newer_than_dependency_for_any_app():
    from benchmarks.sregym.fastloop.seed_recovery import StartupDependency, require_dependency_order

    dependency = StartupDependency("registry-x", ("api-y",))
    with pytest.raises(ValueError, match="register after"):
        require_dependency_order(
            dependency, {"registry-x": ["2026-10-25T20:00:02Z"], "api-y": ["2026-10-25T20:00:01Z"]}
        )
    require_dependency_order(dependency, {"registry-x": ["2026-10-25T20:00:02Z"], "api-y": ["2026-10-25T20:00:03Z"]})


def test_astronomy_fetcher_uses_harness_namespace_and_app_service_dns(monkeypatch):
    manager = SimpleNamespace(namespace="astronomy-shop", locust_url="load-generator:8089")
    created = []
    manager.start = lambda: created.append((manager.namespace, manager.locust_url))
    app = SimpleNamespace(namespace="astronomy-shop", frontend_service="frontend-proxy", wrk=manager)
    app.create_workload = lambda: None
    monkeypatch.setattr(clean_start, "kubectl", lambda *_args, **_kwargs: SimpleNamespace(stdout=""))
    assert clean_start.prepare_harness_traffic(app, Path("unused"))
    app.start_workload()
    assert created == [("sregym", "load-generator.astronomy-shop.svc.cluster.local:8089")]


@pytest.mark.parametrize(
    "status",
    [
        {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1, "readyReplicas": 1},
        {"observedGeneration": 2, "replicas": 2, "updatedReplicas": 1, "readyReplicas": 1},
        {"observedGeneration": 2, "replicas": 1, "updatedReplicas": 0, "readyReplicas": 1},
    ],
)
def test_old_ready_pods_do_not_prove_current_workload_rollout(monkeypatch, status):
    from benchmarks.sregym.fastloop import admission

    workload = {
        "kind": "Deployment",
        "metadata": {"name": "api", "generation": 2},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "api"}}},
        "status": status,
    }
    pod = {
        "kind": "Pod",
        "metadata": {"uid": "old", "labels": {"app": "api"}},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    monkeypatch.setattr(
        admission, "kubectl", lambda *a, **kw: SimpleNamespace(stdout=json.dumps({"items": [workload, pod]}))
    )
    with pytest.raises(ValueError, match="rollout not complete"):
        admission.workload_identity(Path("unused"), "any-app")


@pytest.mark.go_integration
def test_fresh_guard_accepts_actual_broker_health_bootstrap_and_rendered_commit(tmp_path):
    import shutil
    import subprocess

    from benchmarks.sregym.adapter.eval_guards import setup_lineage

    root = Path(__file__).resolve().parents[5]
    repository = tmp_path / "accepted"
    shutil.copytree(root / "apps/toy/seed", repository)
    (repository / "service.yaml").write_text(
        "apiVersion: v1\nkind: Service\nmetadata:\n  name: frontend\nspec:\n  ports:\n  - port: 80\n"
    )
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.com", "commit", "-qm", "installed seed"],
    ):
        subprocess.run(["git", "-C", str(repository), *args], check=True, capture_output=True)
    seed = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "HEAD"], text=True).strip()
    fixture = Path(__file__).with_name("fixtures") / "setup.go"
    actual = subprocess.run(
        ["go", "run", str(fixture), str(repository)],
        cwd=root / "sdo/operator",
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    published = json.loads(actual.stdout)
    assert ".sdo/playbooks/README.md" in published["health_paths"]
    lineage = setup_lineage(repository, seed)
    assert lineage["accepted_commit"] == published["head"]
    assert [row["actor"] for row in lineage["commits"]] == ["health_judge", "controller"]


def test_lane_resources_reject_before_any_health_wait_or_restore(monkeypatch, tmp_path):
    from benchmarks.sregym.fastloop import admission
    from benchmarks.sregym.fastloop.admission import LaneAdmission

    def reject(*args, **kwargs):
        raise ValueError("eval: resource admission rejected foreign app")

    monkeypatch.setattr(admission, "assert_lane_resources", reject)
    env = SimpleNamespace(kubeconfig=tmp_path / "kubeconfig", namespace="application")
    gate = LaneAdmission(env, object(), object(), tmp_path / "proof")
    with pytest.raises(ValueError, match="foreign app"):
        gate.prove("application-sdo")


def test_new_lane_restores_its_installed_source_instead_of_an_inherited_rendered_lock(tmp_path):
    from benchmarks.sregym.fastloop.baseline_recovery import (
        ApplicationBaseline,
        accepted_restore_baselines,
        load_application_baseline,
        save_application_baseline,
    )

    path = tmp_path / "baseline.json"
    path.write_text("{}")
    installed = {
        "kind": "Deployment",
        "metadata": {"name": "api", "namespace": "app"},
        "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "api", "image": "new-lane"}]}}},
    }
    save_application_baseline(path, (ApplicationBaseline("app", [installed]),), installed=True)
    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    (root / "lock.json").write_text('{"objects":[{"file":"missing-old-lane.yaml"}]}')
    baseline = load_application_baseline(path, "app")
    assert accepted_restore_baselines(tmp_path, baseline)[0].objects == [installed]
    value = json.loads(path.read_text())
    value["fastloop_application"]["namespaces"][0]["objects"][0]["spec"]["replicas"] = 0
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="installed baseline digest"):
        load_application_baseline(path, "app")


def test_reusing_a_lane_cannot_reseal_runtime_drift_as_installed_source(tmp_path):
    from benchmarks.sregym.fastloop.baseline_recovery import (
        ApplicationBaseline,
        save_application_baseline,
        seal_existing_install,
    )

    path = tmp_path / "baseline.json"
    path.write_text("{}")
    installed = {"kind": "Deployment", "metadata": {"name": "api", "namespace": "app"}, "spec": {"replicas": 1}}
    save_application_baseline(path, (ApplicationBaseline("app", [installed]),))
    original = path.read_bytes()
    changed = deepcopy(installed)
    changed["spec"]["replicas"] = 0
    cluster = SimpleNamespace(list_objects=lambda _: [changed])
    with pytest.raises(ValueError, match="original clean up capture"):
        seal_existing_install(path, cluster, "app")
    assert path.read_bytes() == original


def test_sealed_install_preserves_metadata_setup_but_yields_to_accepted_source_repair(tmp_path):
    import hashlib
    import subprocess

    import yaml

    from benchmarks.sregym.fastloop.baseline_recovery import (
        ApplicationBaseline,
        accepted_restore_baselines,
        load_application_baseline,
        save_application_baseline,
    )

    def commit(message):
        subprocess.run(["git", "-C", str(tmp_path), "add", "."], check=True, capture_output=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(tmp_path),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.com",
                "commit",
                "-qm",
                message,
            ],
            check=True,
            capture_output=True,
        )
        return subprocess.check_output(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], text=True).strip()

    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    (tmp_path / "application.txt").write_text("installed source")
    seed = commit("installed")
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text("{}")
    installed = {
        "kind": "Deployment",
        "metadata": {"namespace": "app", "name": "api"},
        "spec": {"template": {"spec": {"containers": [{"name": "api", "image": "installed"}]}}},
    }
    save_application_baseline(baseline_path, (ApplicationBaseline("app", [installed]),), installed=True)
    # The controller can publish metadata without changing installed source.
    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    repaired = deepcopy(installed)
    repaired["spec"]["template"]["spec"]["containers"][0]["image"] = "accepted-source-repair"
    payload = yaml.safe_dump(repaired).encode()
    (root / "api.yaml").write_bytes(payload)
    (root / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {"kind": "Deployment", "name": "api"},
                        "file": "api.yaml",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ]
            }
        )
    )
    # Keep benchmark capture files out of the application source diff.
    (tmp_path / "baseline.json").unlink()
    commit("controller metadata")
    baseline_path.write_text("{}")
    save_application_baseline(baseline_path, (ApplicationBaseline("app", [installed]),), installed=True)
    baselines = load_application_baseline(baseline_path, "app")
    assert accepted_restore_baselines(tmp_path, baselines, seed_commit=seed)[0].objects == [installed]
    baseline_path.unlink()
    (tmp_path / "application.txt").write_text("accepted repaired source")
    commit("accepted source repair")
    assert accepted_restore_baselines(tmp_path, baselines, seed_commit=seed)[0].objects == [repaired]


@pytest.mark.parametrize("case", ["error", "missing_error", "mixed_file"])
def test_restore_rejects_unbacked_or_malformed_rendered_rows(tmp_path, case):
    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, accepted_restore_baselines

    installed = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"namespace": "app", "name": "api"}}
    row = {
        "ref": {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"},
        "error": "source rendering failed",
    }
    if case == "missing_error":
        del row["error"]
    elif case == "mixed_file":
        row["file"] = "must-not-be-read.yaml"
    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    (root / "lock.json").write_text(json.dumps({"objects": [row]}))
    with pytest.raises(ValueError, match="rendered.*gap"):
        accepted_restore_baselines(tmp_path, (ApplicationBaseline("app", [installed]),))


def test_restore_rejects_sealed_manifest_for_another_object(tmp_path):
    import hashlib

    import yaml

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, accepted_restore_baselines

    value = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"namespace": "app", "name": "other"}}
    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    payload = yaml.safe_dump(value).encode()
    (root / "object.yaml").write_bytes(payload)
    (root / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {"api_version": "apps/v1", "kind": "Deployment", "namespace": "app", "name": "api"},
                        "file": "object.yaml",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                ]
            }
        )
    )
    with pytest.raises(ValueError, match="identity"):
        accepted_restore_baselines(tmp_path, (ApplicationBaseline("app", []),))


def test_stabilization_rejects_late_success_and_does_not_renew_budget():
    from benchmarks.sregym.adapter.eval_guards import StabilizationError, wait_for_proof

    now = [0.0]
    records = []

    def slow_sample():
        now[0] += 6
        return {"passed": True}

    with pytest.raises(StabilizationError):
        wait_for_proof(
            slow_sample,
            records.append,
            timeout_seconds=5,
            monotonic=lambda: now[0],
            sleep=lambda seconds: None,
        )
    assert not records[-1]["passed"]


def test_replaced_pod_under_unchanged_template_is_named_as_readiness_loss_not_rollout(monkeypatch):
    """Lane 52: a responder replaced the only pod; the ReplicaSet and template never changed."""
    from benchmarks.sregym.fastloop import admission

    workload = {
        "kind": "Deployment",
        "metadata": {"name": "currency", "generation": 1},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "currency"}}},
        "status": {"observedGeneration": 1, "replicas": 1, "updatedReplicas": 1},
    }
    pod = {
        "kind": "Pod",
        "metadata": {"uid": "new", "name": "currency-z4xmb", "labels": {"app": "currency"}},
        "status": {"phase": "Pending", "conditions": [{"type": "Ready", "status": "False"}]},
    }
    monkeypatch.setattr(
        admission, "kubectl", lambda *a, **kw: SimpleNamespace(stdout=json.dumps({"items": [workload, pod]}))
    )
    with pytest.raises(ValueError, match="workload not Ready: Deployment/currency") as refused:
        admission.workload_identity(Path("unused"), "any-app")
    message = str(refused.value)
    assert "rollout not complete" not in message
    assert "ready 0/1" in message
    assert "currency-z4xmb Pending" in message
