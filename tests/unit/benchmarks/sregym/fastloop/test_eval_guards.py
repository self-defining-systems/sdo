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


def test_stabilization_restores_once_then_fails_without_injecting():
    from benchmarks.sregym.adapter.eval_guards import StabilizationError, wait_for_proof

    class Clock:
        now = 0

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    clock = Clock()
    restores = []
    samples = []
    with pytest.raises(StabilizationError):
        wait_for_proof(
            lambda: {"passed": False, "reason": "pending"},
            lambda: restores.append("app-only"),
            samples.append,
            timeout_seconds=5,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )
    assert restores == ["app-only"]
    assert clock.now == 10
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
    for key in ("ControlRoles", "AppRoles", "ControlBindings", "AppBindings"):
        for original in expected[key]:
            row = deepcopy(original)
            row["kind"] = "RoleBinding" if key.endswith("Bindings") else "Role"
            listing.append(row)
    assert require_rbac_equality(expected, listing, "fixture-app")["passed"]
    for row in listing:
        if row["kind"] == "Role" and row["metadata"]["name"] == "sdo-responder":
            row["rules"].append({"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]})
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
