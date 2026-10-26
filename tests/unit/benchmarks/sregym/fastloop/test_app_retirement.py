"""App conversion owns exactly two namespaces, never the warm kind cluster."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop.app_retirement import RetirementRequest, retire_application
from benchmarks.sregym.fastloop.worker_bridge import attach_existing_cluster

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class Cluster:
    namespaces: set[str] = field(
        default_factory=lambda: {
            "hotel-reservation",
            "hotel-reservation-sdo",
            "kube-system",
            "default",
            "sregym",
            "unrelated",
        }
    )
    writes: list[str] = field(default_factory=list)
    owns_lease: bool = True
    waits: list[frozenset[str]] = field(default_factory=list)

    def check_lease(self):
        if not self.owns_lease:
            raise RuntimeError("lease denied")

    def namespace_names(self):
        return set(self.namespaces)

    def quiesce(self, namespace):
        assert namespace == "hotel-reservation-sdo"

    def delete_namespace(self, namespace):
        self.check_lease()
        self.writes.append(namespace)
        self.namespaces.discard(namespace)

    def wait_absent(self, namespaces):
        self.waits.append(frozenset(namespaces))
        assert not set(namespaces) & self.namespaces

    def verify_next(self, namespace):
        assert namespace == "social-network"
        assert namespace in self.namespaces
        assert not {"hotel-reservation", "hotel-reservation-sdo"} & self.namespaces
        return {"passed": True, "namespace": namespace}


def request():
    return RetirementRequest(
        cluster="luna-pilot-53", namespace="hotel-reservation", next_namespace="social-network", lease="responder-lanes"
    )


def test_retirement_waits_then_deploys_and_validates_next_app():
    cluster = Cluster()

    def deploy():
        assert cluster.waits == [frozenset({"hotel-reservation", "hotel-reservation-sdo"})]
        cluster.namespaces.add("social-network")

    result = retire_application(request(), cluster, deploy_next=deploy)
    assert set(cluster.writes) == {"hotel-reservation", "hotel-reservation-sdo"}
    assert result["lane_resources"]["passed"]
    assert result["cluster_recreated"] is False


def test_retirement_never_touches_any_namespace_outside_the_app_set():
    cluster = Cluster()
    protected = cluster.namespaces - {"hotel-reservation", "hotel-reservation-sdo"}
    retire_application(request(), cluster, deploy_next=lambda: cluster.namespaces.add("social-network"))
    assert protected <= cluster.namespaces
    assert not set(cluster.writes) & protected


def test_no_lease_means_no_namespace_read_or_mutation():
    cluster = Cluster(owns_lease=False)
    with pytest.raises(RuntimeError, match="lease denied"):
        retire_application(request(), cluster, deploy_next=lambda: pytest.fail("deployed without lease"))
    assert cluster.writes == []


def test_lease_loss_between_namespace_deletes_stops_immediately():
    cluster = Cluster()
    original = cluster.delete_namespace

    def delete(namespace):
        original(namespace)
        cluster.owns_lease = False

    cluster.delete_namespace = delete
    with pytest.raises(RuntimeError, match="lease denied"):
        retire_application(request(), cluster, deploy_next=lambda: pytest.fail("deployed after lease loss"))
    assert len(cluster.writes) == 1


@pytest.mark.parametrize("namespace", ["default", "kube-system", "sregym", "hotel-reservation-sdo", "unrelated"])
def test_arbitrary_namespace_cannot_become_a_retirement_target(namespace):
    with pytest.raises(ValueError, match="application namespace"):
        RetirementRequest(cluster="lane", namespace=namespace, next_namespace="social-network", lease="lease")


def test_partial_retirement_is_restartable_without_deleting_new_namespaces():
    cluster = Cluster()
    cluster.namespaces.discard("hotel-reservation")
    retire_application(request(), cluster, deploy_next=lambda: cluster.namespaces.add("social-network"))
    assert cluster.writes == ["hotel-reservation-sdo"]


def test_next_app_resource_failure_is_never_reported_as_a_success():
    cluster = Cluster()

    def verify(_namespace):
        raise ValueError("unmeasured next-app memory")

    cluster.verify_next = verify
    with pytest.raises(ValueError, match="unmeasured"):
        retire_application(request(), cluster, deploy_next=lambda: cluster.namespaces.add("social-network"))


def test_false_next_app_resource_verdict_is_rejected():
    cluster = Cluster()
    cluster.verify_next = lambda _namespace: {"passed": False}
    with pytest.raises(ValueError, match="lane_resources did not pass"):
        retire_application(request(), cluster, deploy_next=lambda: cluster.namespaces.add("social-network"))


def test_real_namespace_adapter_rejects_out_of_set_before_any_io(tmp_path, monkeypatch):
    from benchmarks.sregym.fastloop import app_retirement

    monkeypatch.setattr(app_retirement, "kubectl", lambda *_a, **_k: pytest.fail("touched protected namespace"))
    ops = app_retirement.KubectlRetirementOps(request(), tmp_path / "config")
    with pytest.raises(ValueError, match="outside the application set"):
        ops.delete_namespace("kube-system")


@pytest.mark.parametrize("pending", [True, False])
def test_native_quiesce_retains_pending_work_and_pauses_idle_controller(tmp_path, monkeypatch, pending):
    import json
    from types import SimpleNamespace

    from benchmarks.sregym import adapter
    from benchmarks.sregym.fastloop import app_retirement

    fake = SimpleNamespace(
        runtime_state=lambda _namespace: {"pending_closure": {"incident": "active"}} if pending else {}
    )
    pauses = []
    monkeypatch.setattr(adapter, "KubectlClusterOps", lambda: fake)
    monkeypatch.setattr(adapter, "pause_for_recovery", lambda ops, namespace: pauses.append((ops, namespace)))
    monkeypatch.setattr(
        app_retirement,
        "kubectl",
        lambda *_a, **_k: SimpleNamespace(
            stdout=json.dumps(
                {
                    "items": [{"metadata": {"name": "sdo-controller-run"}, "status": {"active": 1}}],
                }
            )
        ),
    )
    if pending:
        with pytest.raises(RuntimeError, match="unfinished native"):
            app_retirement.quiesce_native_controller(tmp_path / "config", "hotel-reservation-sdo")
        assert pauses == []
    else:
        app_retirement.quiesce_native_controller(tmp_path / "config", "hotel-reservation-sdo")
        assert pauses == [(fake, "hotel-reservation-sdo")]


def test_attach_only_never_falls_back_to_cluster_creation(tmp_path: Path, monkeypatch):
    from types import SimpleNamespace

    path = tmp_path / "kubeconfig"
    path.write_text("fixture")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-pilot-53")
    monkeypatch.setenv("KUBECONFIG", str(path))
    infra = SimpleNamespace(existing_cluster_is_reusable=lambda *_a: (False, "node topology differs"))
    with pytest.raises(ValueError, match="refusing.*recreat"):
        attach_existing_cluster(infra)


def test_attach_only_retains_the_fenced_kubeconfig(tmp_path: Path, monkeypatch):
    from types import SimpleNamespace

    path = tmp_path / "kubeconfig"
    path.write_text("fixture")
    monkeypatch.setenv("SREGYM_KIND_CLUSTER_NAME", "luna-pilot-53")
    monkeypatch.setenv("KUBECONFIG", str(path))
    checked = []
    infra = SimpleNamespace(
        existing_cluster_is_reusable=lambda *_a: (True, ""),
        preflight_cluster_requirements=lambda *args: checked.append(args),
    )
    assert attach_existing_cluster(infra) == {"cluster": "luna-pilot-53", "kubeconfig": str(path)}
    assert checked == [("luna-pilot-53", str(path))]
