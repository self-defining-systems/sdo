"""An aborted fault setup must not poison the next row of a reused lane (eval class C7)."""

from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import allowed_namespaces
from benchmarks.sregym.fastloop import admission
from benchmarks.sregym.fastloop.setup_ledger import (
    SetupLedger,
    SetupLedgerError,
    close_after_full_recovery,
    ledger_path,
    replay,
    run_in_window,
)

INGRESS = {
    "namespace/ingress-nginx",
    "ingressclass.networking.k8s.io/nginx",
    "clusterrole.rbac.authorization.k8s.io/ingress-nginx",
    "clusterrolebinding.rbac.authorization.k8s.io/ingress-nginx",
    "validatingwebhookconfiguration.admissionregistration.k8s.io/ingress-nginx-admission",
}
LANE = {"namespace/default", "namespace/kube-system", "namespace/hotel-reservation", "namespace/hotel-reservation-sdo"}


class FakeCluster:
    """Cluster-scoped objects of one lane; deletion takes a poll to land, like a terminating namespace."""

    def __init__(self, objects=LANE, *, stuck=frozenset(), lag=False):
        self.objects, self.stuck, self.deleted, self.lag = set(objects), set(stuck), [], lag
        self.pending: set[str] = set()

    def list_objects(self):
        return frozenset(self.objects)

    def delete(self, identity):
        self.deleted.append(identity)
        if identity in self.stuck:
            return
        if self.lag:
            self.pending.add(identity)
        else:
            self.objects.discard(identity)

    def tick(self, _seconds=0.0):
        self.objects -= self.pending
        self.pending.clear()


def replay_now(ledger, cluster, namespaces=("hotel-reservation",)):
    return replay(
        ledger,
        cluster,
        protected_namespaces=allowed_namespaces(set(namespaces)),
        timeout_seconds=5.0,
        poll_seconds=1.0,
        clock=iter(range(1000)).__next__,
        sleep=cluster.tick,
    )


@pytest.fixture(autouse=True)
def bridge_lane_guards():
    """Override the conftest stub: these tests run the real guards."""


@pytest.fixture
def ledger(tmp_path):
    return SetupLedger(ledger_path(tmp_path / "kubeconfig"))


def test_a_setup_cut_off_mid_injection_is_removed_by_the_next_replay(ledger):
    cluster = FakeCluster(lag=True)

    def cut_off():
        cluster.objects |= INGRESS
        raise TimeoutError("worker killed during injection")

    with pytest.raises(TimeoutError):
        run_in_window(ledger, cluster, cut_off)
    assert cluster.objects >= INGRESS
    # A new process (the next row) reads the same ledger.
    assert replay_now(SetupLedger(ledger.path), cluster) == INGRESS
    assert cluster.objects == LANE
    assert ledger.window() is None


def test_a_returned_setup_removes_exactly_what_it_created_and_never_what_a_responder_made(ledger):
    cluster = FakeCluster()

    def setup():
        cluster.objects |= INGRESS
        return {"valid": True}

    assert run_in_window(ledger, cluster, setup) == {"valid": True}
    cluster.objects.add("clusterrole.rbac.authorization.k8s.io/responder-made")
    assert replay_now(ledger, cluster) == INGRESS
    assert cluster.objects == LANE | {"clusterrole.rbac.authorization.k8s.io/responder-made"}


def test_lane_namespaces_are_never_removed_even_when_new_in_the_cut_off_window(ledger):
    cluster = FakeCluster()

    def cut_off():
        cluster.objects |= {"namespace/hotel-reservation-sdo", "namespace/kube-public", "namespace/ingress-nginx"}
        raise TimeoutError

    cluster.objects -= {"namespace/hotel-reservation-sdo"}
    with pytest.raises(TimeoutError):
        run_in_window(ledger, cluster, cut_off)
    assert replay_now(ledger, cluster) == {"namespace/ingress-nginx"}
    assert {"namespace/hotel-reservation-sdo", "namespace/kube-public"} <= cluster.objects


def test_full_recovery_closes_the_window_and_a_partial_recovery_does_not(ledger):
    cluster = FakeCluster()
    run_in_window(ledger, cluster, lambda: cluster.objects.update(INGRESS) or {})
    done = {"success": True, "pending": [], "errors": []}
    for request, reply in (
        ({"op": "recover", "fault": 0}, done),
        ({"op": "recover"}, {**done, "pending": [1]}),
        ({"op": "recover"}, {**done, "errors": [{"status": "error"}]}),
        ({"op": "recover"}, {**done, "success": False}),
        ({"op": "inject"}, done),
    ):
        assert not close_after_full_recovery(ledger, request, reply)
        assert ledger.window() is not None
    assert close_after_full_recovery(ledger, {"op": "recover"}, done)
    assert ledger.window() is None
    assert replay_now(ledger, cluster) == frozenset()
    assert cluster.objects >= INGRESS  # recovery, not the ledger, owns removal while the worker lives


def test_a_composition_keeps_the_first_setups_baseline(ledger):
    cluster = FakeCluster()
    run_in_window(ledger, cluster, lambda: cluster.objects.add("namespace/first") or {})
    run_in_window(ledger, cluster, lambda: cluster.objects.add("namespace/second") or {}, compose=True)
    assert replay_now(ledger, cluster) == {"namespace/first", "namespace/second"}


def test_a_new_plain_injection_replaces_a_stale_window(ledger):
    cluster = FakeCluster()
    run_in_window(ledger, cluster, lambda: cluster.objects.add("namespace/stale") or {})
    run_in_window(ledger, cluster, lambda: cluster.objects.add("namespace/fresh") or {})
    assert replay_now(ledger, cluster) == {"namespace/fresh"}


def test_an_object_that_will_not_go_fails_the_replay_and_keeps_the_window(ledger):
    cluster = FakeCluster(lag=True, stuck={"namespace/ingress-nginx"})
    with pytest.raises(TimeoutError):
        run_in_window(ledger, cluster, lambda: cluster.objects.update(INGRESS) or (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(SetupLedgerError, match="namespace/ingress-nginx"):
        replay_now(ledger, cluster)
    assert ledger.window() is not None


def test_deletion_order_removes_webhooks_before_namespaces(ledger):
    cluster = FakeCluster()
    run_in_window(ledger, cluster, lambda: cluster.objects.update(INGRESS) or {})
    replay_now(ledger, cluster)
    assert cluster.deleted[0].startswith("validatingwebhookconfiguration")
    assert cluster.deleted[-1] == "namespace/ingress-nginx"


def test_a_malformed_ledger_fails_closed(ledger):
    ledger.path.write_text("{not json")
    with pytest.raises(SetupLedgerError, match="unreadable"):
        ledger.window()


class Reached(Exception):
    """Raised once admission gets past its entry check."""


def entry_gate(tmp_path, monkeypatch, cluster):
    """The real admission entry over a fake lane whose resource check refuses a foreign namespace."""
    environment = SimpleNamespace(
        workspace=tmp_path, namespace="hotel-reservation", kubeconfig=tmp_path / "kubeconfig", deploy_problem="fixture"
    )

    def lane_resources(kubeconfig, *, app_namespaces):
        allowed = allowed_namespaces(app_namespaces)
        for identity in sorted(cluster.objects):
            name = identity.removeprefix("namespace/")
            if identity.startswith("namespace/") and name not in allowed:
                raise ValueError(f"eval: foreign app namespace {name} age=0.5h; expected {sorted(app_namespaces)}")
        return {"passed": True}

    def past_entry(self):
        raise Reached

    monkeypatch.setattr(admission, "assert_lane_resources", lane_resources)
    monkeypatch.setattr(admission, "assert_host_disk_headroom", lambda: {"passed": True})
    monkeypatch.setattr(admission.LaneAdmission, "_policy", past_entry)
    return admission.LaneAdmission(environment, SimpleNamespace(), tmp_path / "admission", lane_inventory=cluster)


def test_the_next_rows_entry_removes_an_aborted_setup(tmp_path, monkeypatch):
    cluster = FakeCluster()
    gate = entry_gate(tmp_path, monkeypatch, cluster)
    ledger = SetupLedger(ledger_path(gate.environment.kubeconfig))

    def aborted_setup():
        cluster.objects |= INGRESS
        raise TimeoutError("attempt aborted before injection completed")

    with pytest.raises(TimeoutError):
        run_in_window(ledger, cluster, aborted_setup)
    with pytest.raises(Reached):
        gate.prove("hotel-reservation-sdo")
    assert cluster.objects == LANE
    assert ledger.window() is None


def test_the_entry_check_still_refuses_a_foreign_namespace_nobody_recorded(tmp_path, monkeypatch):
    cluster = FakeCluster(LANE | {"namespace/ingress-nginx"})
    gate = entry_gate(tmp_path, monkeypatch, cluster)
    with pytest.raises(ValueError, match="foreign app namespace ingress-nginx"):
        gate.prove("hotel-reservation-sdo")
    assert "namespace/ingress-nginx" in cluster.objects


def test_the_worker_bridge_leaves_an_aborted_injection_for_the_next_entry_and_clears_after_full_recovery(
    tmp_path, monkeypatch
):
    from benchmarks.sregym.fastloop.worker_bridge import run_with_setup_ledger

    cluster = FakeCluster()
    gate = entry_gate(tmp_path, monkeypatch, cluster)
    kubeconfig = gate.environment.kubeconfig

    def install_then_abort():
        cluster.objects |= INGRESS
        raise TimeoutError("worker killed")

    with pytest.raises(TimeoutError):
        run_with_setup_ledger({"op": "inject"}, kubeconfig, install_then_abort, inventory=cluster)
    with pytest.raises(Reached):
        gate.prove("hotel-reservation-sdo")
    assert cluster.objects == LANE
    removed_by_replay = len(cluster.deleted)

    # A finished attempt: the worker's own recovery removed the setup, and the window closed with it.
    def install():
        cluster.objects |= INGRESS
        return {"fault": 0}

    run_with_setup_ledger({"op": "inject"}, kubeconfig, install, inventory=cluster)

    def recover():
        cluster.objects -= INGRESS
        return {"success": True, "pending": [], "errors": []}

    run_with_setup_ledger({"op": "recover"}, kubeconfig, recover, inventory=cluster)
    assert SetupLedger(ledger_path(kubeconfig)).window() is None
    with pytest.raises(Reached):
        gate.prove("hotel-reservation-sdo")
    assert len(cluster.deleted) == removed_by_replay  # the closed window left nothing to replay
