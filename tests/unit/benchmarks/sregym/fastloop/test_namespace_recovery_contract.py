"""Namespace recovery must retain prerequisites, independent of application names."""

from __future__ import annotations

from copy import deepcopy

import pytest

from benchmarks.sregym.fastloop.baseline_recovery import capture_application_baseline, reconcile_application
from tests.unit.benchmarks.sregym.fastloop.test_baseline_recovery import FakeCluster, obj


@pytest.mark.parametrize(
    ("namespace", "accounts"),
    [
        ("astronomy-shop", ["astronomy-shop", "grafana", "jaeger", "otel-collector"]),
        ("hotel-reservation", ["reservation-reader"]),
        ("social-network", ["social-reader"]),
    ],
)
def test_namespace_recovery_capture_contains_referenced_nondefault_service_accounts(namespace, accounts):
    # AST3's sealed-baseline reproducer: all four account names come from real
    # pod-template references, not recovery configuration.
    cluster = FakeCluster()
    for account in accounts:
        cluster.add(obj("Deployment", account, namespace, spec={"template": {"spec": {"serviceAccountName": account}}}))
        cluster.add(obj("ServiceAccount", account, namespace))
    role = obj("Role", "reader", namespace, rules=[{"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]}])
    binding = obj(
        "RoleBinding",
        "reader",
        namespace,
        roleRef={"kind": "Role", "name": "reader"},
        subjects=[{"kind": "ServiceAccount", "name": accounts[0], "namespace": namespace}],
    )
    cluster.add(role)
    cluster.add(binding)
    original = deepcopy(cluster.objects)
    baseline = capture_application_baseline(cluster, namespace)
    captured_accounts = {v["metadata"]["name"] for v in baseline.objects if v["kind"] == "ServiceAccount"}
    assert set(accounts) <= captured_accounts
    cluster.objects.clear()
    reconcile_application(cluster, baseline, namespace)
    assert cluster.objects == original
    order = [kind for _, kind, _ in cluster.touched]
    assert order.index("ServiceAccount") < order.index("Deployment")


def test_ast3_sealed_baseline_recovers_all_referenced_accounts_and_grants():
    from pathlib import Path

    from benchmarks.sregym.fastloop.baseline_recovery import load_application_baseline
    from benchmarks.sregym.fastloop.recovery_closure import complete_recovery_baselines, missing_prerequisites

    root = Path(__file__).parents[4] / "fixtures/benchmarks/namespace_recovery"
    baseline = load_application_baseline(root / "astronomy-baseline.json", "astronomy-shop")
    assert baseline
    recovered = complete_recovery_baselines(root, baseline)
    for captured in recovered:
        accounts = {v["metadata"]["name"] for v in captured.objects if v["kind"] == "ServiceAccount"}
        required = set()
        for value in captured.objects:
            from benchmarks.sregym.fastloop.recovery_closure import pod_spec

            pod = pod_spec(value)
            if pod and pod.get("serviceAccountName", "default") != "default":
                required.add(pod["serviceAccountName"])
        assert required <= accounts
    assert {v["kind"] for v in recovered[0].objects} >= {"Role", "RoleBinding"}
    # Grafana's Secret is a real second blocker, never synthesized or read.
    assert [r.label() for r in missing_prerequisites(recovered)] == ["astronomy-shop/Secret/grafana"]
    assert recovered[0].objects[: len(baseline[0].objects)] == baseline[0].objects


@pytest.mark.parametrize("namespace", ["hotel-reservation", "social-network", "astronomy-shop"])
def test_reference_closure_follows_env_volumes_rbac_and_stateful_claims(namespace):
    from benchmarks.sregym.fastloop.recovery_closure import prerequisites

    deployment = obj(
        "StatefulSet",
        "app",
        namespace,
        spec={
            "serviceName": "headless",
            "volumeClaimTemplates": [{"metadata": {"name": "data"}, "spec": {"storageClassName": "local-path"}}],
            "template": {
                "spec": {
                    "serviceAccountName": "reader",
                    "imagePullSecrets": [{"name": "registry"}],
                    "volumes": [
                        {"configMap": {"name": "mounted"}},
                        {"persistentVolumeClaim": {"claimName": "data"}},
                        {
                            "projected": {
                                "sources": [
                                    {"secret": {"name": "projected"}},
                                    {"configMap": {"name": "kube-root-ca.crt"}},
                                ]
                            }
                        },
                    ],
                    "initContainers": [{"envFrom": [{"configMapRef": {"name": "init"}}]}],
                    "containers": [
                        {
                            "env": [{"valueFrom": {"secretKeyRef": {"name": "password"}}}],
                            "envFrom": [{"configMapRef": {"name": "optional", "optional": True}}],
                        }
                    ],
                }
            },
        },
    )
    assert {(r.kind, r.name) for r in prerequisites(deployment, namespace)} == {
        ("Service", "headless"),
        ("ServiceAccount", "reader"),
        ("Secret", "registry"),
        ("ConfigMap", "mounted"),
        ("Secret", "projected"),
        ("ConfigMap", "init"),
        ("Secret", "password"),
        ("StorageClass", "local-path"),
    }
    binding = obj(
        "RoleBinding",
        "reader",
        namespace,
        roleRef={"kind": "ClusterRole", "name": "read"},
        subjects=[{"kind": "ServiceAccount", "name": "reader"}],
    )
    assert {r.label() for r in prerequisites(binding, namespace)} == {
        "ClusterRole/read",
        f"{namespace}/ServiceAccount/reader",
    }


def test_missing_prerequisites_rejects_before_controller_or_namespace_writes(tmp_path, monkeypatch):
    import subprocess
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop import reset
    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline, save_application_baseline
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel",
        application="hotel",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="synthetic",
    )
    env.baseline_path.write_text("{}")
    deployment = obj(
        "Deployment",
        "app",
        spec={
            "template": {
                "spec": {
                    "serviceAccountName": "absent",
                    "containers": [{"envFrom": [{"secretRef": {"name": "credential"}}]}],
                }
            }
        },
    )
    save_application_baseline(env.baseline_path, (ApplicationBaseline("hotel", [deployment]),))
    calls = []

    def command(args, **kwargs):
        calls.append(args)
        assert args[0] == "get", "rejection must precede every mutation"
        return subprocess.CompletedProcess(args, 1, "", "(NotFound)")

    monkeypatch.setattr(reset, "kubectl", command)
    cluster = SimpleNamespace(reset_controller=lambda *a, **k: pytest.fail("controller mutated"))
    with pytest.raises(ValueError, match="hotel/Secret/credential, hotel/ServiceAccount/absent"):
        reset.reset_environment(env, cluster, SimpleNamespace(), checks=reset.RestoreChecks(env))
    assert calls == [["get", "namespace", "hotel", "-o", "name"]]


def test_secret_manifest_payload_is_never_opened_during_closure(tmp_path):
    import json

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
    from benchmarks.sregym.fastloop.recovery_closure import complete_recovery_baselines, missing_prerequisites

    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    (root / "lock.json").write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "ref": {"kind": "Secret", "namespace": "hotel", "name": "credential"},
                        "file": "DO-NOT-OPEN",
                        "sha256": "unused",
                    }
                ]
            }
        )
    )
    value = obj("Pod", "app", spec={"containers": [{"envFrom": [{"secretRef": {"name": "credential"}}]}]})
    result = complete_recovery_baselines(tmp_path, (ApplicationBaseline("hotel", [value]),))
    assert [r.label() for r in missing_prerequisites(result)] == ["hotel/Secret/credential"]


@pytest.mark.parametrize("kind", ["Role", "RoleBinding", "ServiceAccount"])
def test_recovery_restores_top_level_identity_and_rbac_fields(kind, monkeypatch, tmp_path):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    fields = {
        "Role": {"rules": [{"verbs": ["get"], "resources": ["pods"]}]},
        "RoleBinding": {
            "roleRef": {"kind": "Role", "name": "reader"},
            "subjects": [{"kind": "ServiceAccount", "name": "reader"}],
        },
        "ServiceAccount": {"automountServiceAccountToken": False, "imagePullSecrets": [{"name": "registry"}]},
    }
    value = obj(kind, "reader", **fields[kind])
    live = deepcopy(value)
    live["metadata"]["resourceVersion"] = "17"
    field = {"Role": "rules", "RoleBinding": "subjects", "ServiceAccount": "imagePullSecrets"}[kind]
    live[field] = []

    def api(args, **kwargs):
        if args[0] == "get":
            return SimpleNamespace(stdout=json.dumps(live))
        assert args[0] == "patch"
        for operation in json.loads(__import__("pathlib").Path(args[4]).read_text()):
            if operation["op"] == "add":
                live[operation["path"].removeprefix("/")] = operation["value"]
        return SimpleNamespace(stdout="")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    KubectlApplicationCluster(tmp_path / "kubeconfig").apply("hotel", value)
    assert live[field] == value[field]


@pytest.mark.parametrize("damage", ["digest", "identity", "escape"])
def test_legacy_prerequisites_require_contained_identity_bound_manifest_bytes(tmp_path, damage):
    import hashlib
    import json

    from benchmarks.sregym.fastloop.baseline_recovery import ApplicationBaseline
    from benchmarks.sregym.fastloop.recovery_closure import complete_recovery_baselines

    root = tmp_path / ".sdo/rendered"
    root.mkdir(parents=True)
    manifest = json.dumps(obj("ServiceAccount", "wrong" if damage == "identity" else "reader"))
    (root / "account.yaml").write_text(manifest)
    row = {
        "ref": {"api_version": "apps/v1", "kind": "ServiceAccount", "namespace": "hotel", "name": "reader"},
        "file": "../account.yaml" if damage == "escape" else "account.yaml",
        "sha256": "invalid" if damage == "digest" else hashlib.sha256(manifest.encode()).hexdigest(),
    }
    (root / "lock.json").write_text(json.dumps({"objects": [row]}))
    workload = obj("Pod", "app", spec={"serviceAccountName": "reader"})
    with pytest.raises(
        ValueError, match={"digest": "digest differs", "identity": "identity differs", "escape": "escapes"}[damage]
    ):
        complete_recovery_baselines(tmp_path, (ApplicationBaseline("hotel", [workload]),))
