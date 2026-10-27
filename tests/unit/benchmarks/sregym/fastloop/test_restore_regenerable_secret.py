"""Recovery regenerates Secrets that a fault's own setup creates (lane 50, 2026-10-27).

The admission-webhook fault deletes its backend namespace on recovery. The
baseline captured that namespace (a leftover from an earlier fault) with the
webhook Deployment and Service, but Secrets are never archived, so the restored
webhook pod sat in ContainerCreating on a missing TLS Secret and reset refused.
Recovery now creates a fresh self-signed TLS Secret, as the fault's backend
setup does at injection, and never reads or archives Secret data.
"""

from __future__ import annotations

import subprocess
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509

from benchmarks.sregym.fastloop import reset
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    ApplicationNamespaceAbsent,
    reconcile_application,
    save_application_baseline,
)
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.regenerable import REGENERABLE_TLS_SECRETS

NAMESPACE = "policy-system"
SECRET = "pod-policy-webhook-tls"
SOURCES = Path(__file__).resolve().parents[5] / "third_party/sregym/sregym/conductor/problems"


def obj(kind: str, name: str, namespace: str, **fields: Any) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": namespace}, **fields}


def webhook_baseline(secret: str = SECRET) -> ApplicationBaseline:
    deployment = obj(
        "Deployment",
        "pod-policy-webhook",
        NAMESPACE,
        spec={
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [{"name": "server"}],
                    "volumes": [{"name": "certs", "secret": {"secretName": secret}}],
                }
            },
        },
    )
    service = obj("Service", "pod-policy-webhook", NAMESPACE, spec={"ports": [{"port": 443}]})
    return ApplicationBaseline(NAMESPACE, [obj("ServiceAccount", "default", NAMESPACE), service, deployment])


@dataclass
class ApiServer:
    """Namespaced writes need their namespace; Secrets are listed by nobody."""

    namespaces: set[str]
    objects: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    secrets: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    events: list[tuple[str, ...]] = field(default_factory=list)

    def list_objects(self, namespace: str) -> list[dict[str, Any]]:
        if namespace not in self.namespaces:
            raise ApplicationNamespaceAbsent(namespace)
        return [deepcopy(value) for key, value in self.objects.items() if key[0] == namespace]

    def delete(self, namespace: str, kind: str, name: str) -> None:
        self.objects.pop((namespace, kind, name), None)

    def apply(self, namespace: str, value: dict[str, Any]) -> None:
        self.events.append(("apply", namespace, value["kind"], value["metadata"]["name"]))
        stored = deepcopy(value)
        if stored["kind"] == "Deployment":
            replicas = stored["spec"]["replicas"]
            stored["status"] = {"updatedReplicas": replicas, "readyReplicas": replicas, "replicas": replicas}
        self.objects[(namespace, value["kind"], value["metadata"]["name"])] = stored

    def create_namespace(self, namespace: str) -> None:
        self.events.append(("create_namespace", namespace))
        self.namespaces.add(namespace)

    def ensure_secret(self, namespace: str, value: dict[str, Any]) -> bool:
        if namespace not in self.namespaces:
            raise RuntimeError(f'namespaces "{namespace}" not found')
        key = (namespace, value["metadata"]["name"])
        if key in self.secrets:
            return False
        self.events.append(("create_secret", namespace, value["metadata"]["name"]))
        self.secrets[key] = deepcopy(value)
        return True


def test_missing_webhook_tls_secret_is_regenerated_before_the_workload_is_restored() -> None:
    cluster = ApiServer(set())

    cleanup = reconcile_application(cluster, webhook_baseline(), NAMESPACE)

    events = cluster.events
    assert events.index(("create_secret", NAMESPACE, SECRET)) < events.index(
        ("apply", NAMESPACE, "Deployment", "pod-policy-webhook")
    )
    secret = cluster.secrets[(NAMESPACE, SECRET)]
    assert secret["type"] == "kubernetes.io/tls"
    assert set(secret["data"]) == {"tls.crt", "tls.key"}
    assert cleanup.regenerated == [f"{NAMESPACE}/Secret/{SECRET}"]


def test_regenerated_certificate_covers_the_namespace_service_names() -> None:
    import base64

    cluster = ApiServer({NAMESPACE})

    reconcile_application(cluster, webhook_baseline(), NAMESPACE)

    pem = base64.b64decode(cluster.secrets[(NAMESPACE, SECRET)]["data"]["tls.crt"])
    names = x509.load_pem_x509_certificate(pem).extensions.get_extension_for_class(x509.SubjectAlternativeName)
    assert set(names.value.get_values_for_type(x509.DNSName)) == {
        f"pod-policy-webhook.{NAMESPACE}.svc",
        f"pod-policy-webhook.{NAMESPACE}.svc.cluster.local",
    }


def test_existing_secret_is_never_replaced() -> None:
    cluster = ApiServer({NAMESPACE})
    cluster.secrets[(NAMESPACE, SECRET)] = {"metadata": {"name": SECRET}, "marker": "keep"}

    cleanup = reconcile_application(cluster, webhook_baseline(), NAMESPACE)

    assert cluster.secrets[(NAMESPACE, SECRET)]["marker"] == "keep"
    assert cleanup.regenerated == []


def test_unregistered_secret_is_not_invented() -> None:
    cluster = ApiServer({NAMESPACE})

    reconcile_application(cluster, webhook_baseline("someone-elses-credential"), NAMESPACE)

    assert cluster.secrets == {}


def test_fault_driver_recovery_regenerates_the_secret_after_the_fault_deleted_its_namespace() -> None:
    class Worker:
        def request(self, op: str, **_args: Any) -> dict[str, Any]:
            if op == "recover":
                cluster.namespaces.discard(NAMESPACE)
                cluster.secrets.clear()
                return {
                    "seconds": 1.0,
                    "protocol_version": 2,
                    "recovery_contract": "seed-compatible/v1",
                    "success": True,
                }
            if op == "refresh":
                return {"protocol_version": 2, "recovery_contract": "seed-compatible/v1", "success": True}
            if op == "health":
                return {"healthy": True, "unready": []}
            raise AssertionError(op)

    cluster = ApiServer({NAMESPACE})
    driver = SregymFaultDriver(
        Worker(),
        namespace="social-network",
        baseline=(webhook_baseline(),),
        cluster=cluster,
        health_timeout_seconds=2,
        health_poll_seconds=0.01,
    )

    driver.recover()

    assert (NAMESPACE, SECRET) in cluster.secrets
    assert driver.last_full_cleanup_verified


def _environment(tmp_path: Path, baseline: ApplicationBaseline) -> FastloopEnvironment:
    env = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="fake",
        kubeconfig=tmp_path / "kubeconfig",
        namespace=NAMESPACE,
        application="social-network",
        workspace=tmp_path / "workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        seed_commit="synthetic",
    )
    env.baseline_path.write_text("{}")
    save_application_baseline(env.baseline_path, (baseline,))
    return env


def _everything_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    def command(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert args[0] == "get", "validation must not mutate"
        return subprocess.CompletedProcess(args, 1, "", "(NotFound)")

    monkeypatch.setattr(reset, "kubectl", command)


def test_reset_validation_accepts_a_regenerable_missing_secret(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _everything_absent(monkeypatch)

    reset.RestoreChecks(_environment(tmp_path, webhook_baseline())).validate()


def test_reset_validation_still_refuses_an_unregistered_missing_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _everything_absent(monkeypatch)

    with pytest.raises(ValueError, match="policy-system/Secret/someone-elses-credential"):
        reset.RestoreChecks(_environment(tmp_path, webhook_baseline("someone-elses-credential"))).validate()


def test_reset_restore_regenerates_through_the_shared_reconcile(tmp_path: Path) -> None:
    """The reset path restores through ``reconcile_and_record``, which calls the same reconcile."""
    from benchmarks.sregym.fastloop.baseline_recovery import reconcile_and_record

    cluster = ApiServer(set())

    reconcile_and_record(cluster, (webhook_baseline(),), tmp_path / "workspace", tmp_path / "receipts.jsonl")

    assert (NAMESPACE, SECRET) in cluster.secrets


@pytest.mark.parametrize(
    "module",
    sorted(
        path.name
        for path in SOURCES.glob("*.py")
        if "kubernetes.io/tls" in path.read_text(encoding="utf-8") and path.name != "__init__.py"
    ),
)
def test_every_fault_that_creates_a_tls_secret_is_classified(module: str) -> None:
    """Ratchet: a new fault family that creates a TLS Secret must pick an owner here.

    Backend-namespace families are regenerated on restore (their Secret names are in the registry);
    app-namespace families create the Secret during injection and delete it in their own recovery,
    so no baseline workload ever references it.
    """
    backend_families = {
        "admission_webhook_tls_mismatch.py": {"pod-policy-webhook-tls"},
        "cumulative_admission_webhook_timeout_hotel_reservation.py": {"compliance-webhook-tls"},
        "mutating_webhook_resource_limits.py": {"platform-policy-controller-tls"},
    }
    fault_owned_in_app_namespace = {"expired_tls_hotel_reservation.py", "node_clock_drift.py"}
    assert module in backend_families or module in fault_owned_in_app_namespace, (
        f"{module} creates a kubernetes.io/tls Secret: add it to REGENERABLE_TLS_SECRETS or declare it fault-owned"
    )
    registered = {name for _, name in REGENERABLE_TLS_SECRETS}
    source = (SOURCES / module).read_text(encoding="utf-8")
    for name in backend_families.get(module, set()):
        assert name in registered
        # The registry names must still match the fault's own setup.
        assert name in source or name.removesuffix("-tls") in source


def test_kubectl_ensure_secret_creates_only_when_absent_and_never_reads_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from benchmarks.sregym.fastloop.baseline_recovery import KubectlApplicationCluster

    calls: list[list[str]] = []
    present = {"value": False}

    def api(args: list[str], **kwargs: Any) -> Any:
        calls.append(args)
        if args[0] == "get":
            assert args[1:4] == [f"secret/{SECRET}", "-o", "name"], "only the name may be inspected"
            return subprocess.CompletedProcess(args, 0, "secret/x\n" if present["value"] else "", "")
        assert args == ["create", "-f", "-"] and kwargs["namespace"] == NAMESPACE
        present["value"] = True
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr("benchmarks.sregym.fastloop.baseline_recovery.kubectl", api)
    cluster = KubectlApplicationCluster(tmp_path / "kubeconfig")
    manifest = {"kind": "Secret", "metadata": {"name": SECRET, "namespace": NAMESPACE}}

    assert cluster.ensure_secret(NAMESPACE, manifest) is True
    assert cluster.ensure_secret(NAMESPACE, manifest) is False
    assert [call[0] for call in calls] == ["get", "create", "get"]
