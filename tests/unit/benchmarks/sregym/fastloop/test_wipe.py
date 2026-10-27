"""Tier 1 wipes everything SDO and the harness own; tier 2 redeploys the app only when it must."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import allowed_namespaces
from benchmarks.sregym.fastloop.records import Injected, NotInjected
from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription
from benchmarks.sregym.faults.probe_catalog import CLASS_KINDS, DATA_LEVEL_CLASSES
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    KubectlLaneCluster,
    Tier2Reason,
    Tier2Trigger,
    WipeError,
    await_healthy_pods,
    credentials_hook,
    data_fault_rows,
    finalizer_holders,
    inventory_path,
    load_inventory,
    namespace_is_stuck,
    pod_problems,
    remove_until_gone,
    reseed_workspace,
    run_tier1,
    run_tier2,
    save_inventory,
    tier2_reason,
    unrecovered_rows,
)

if TYPE_CHECKING:
    from pathlib import Path

APP = "hotel-reservation"
CONTROL = "hotel-reservation-sdo"
KEEP = allowed_namespaces({APP}) - {CONTROL}
LANE = frozenset(
    {f"namespace/{name}" for name in KEEP | {CONTROL}} | {"clusterrole.rbac.authorization.k8s.io/cluster-admin"}
)


class FakeLane:
    """A lane whose namespaces hold objects; deleting a namespace removes its content after a short delay.

    Like the API server, a terminating namespace stays (with ``SomeFinalizersRemain``) while any object in it
    still carries a finalizer. ``finalizers`` lists those objects as ``(namespace, "kind/name")``.
    """

    def __init__(
        self,
        contents: dict[str, set[str]],
        cluster_objects: set[str],
        lag: int = 1,
        finalizers: set[tuple[str, str]] | None = None,
    ) -> None:
        self.contents = contents
        self.cluster_objects = cluster_objects
        self.terminating: dict[str, int] = {}
        self.lag = lag
        self.deleted: list[str] = []
        self.finalizers = set(finalizers or ())
        self.released: list[tuple[str, str]] = []

    def list_objects(self) -> frozenset[str]:
        for name in list(self.terminating):
            self.terminating[name] -= 1
            if self.terminating[name] <= 0 and not self._held(name):
                del self.terminating[name]
                self.contents.pop(name, None)
        return frozenset({f"namespace/{name}" for name in self.contents} | self.cluster_objects)

    def _held(self, namespace: str) -> list[str]:
        return sorted(item for owner, item in self.finalizers if owner == namespace)

    def blocking_objects(self, namespace: str) -> tuple[str, ...]:
        stuck = namespace in self.terminating and self.terminating[namespace] <= 0
        return tuple(self._held(namespace)) if stuck else ()

    def remove_finalizers(self, namespace: str, item: str) -> None:
        self.released.append((namespace, item))
        self.finalizers.discard((namespace, item))

    def delete(self, identity: str) -> None:
        self.deleted.append(identity)
        kind, _, name = identity.partition("/")
        if kind == "namespace":
            self.terminating.setdefault(name, self.lag)
        else:
            self.cluster_objects.discard(identity)

    def has(self, namespace: str, item: str) -> bool:
        return item in self.contents.get(namespace, set())


def tier1(lane: FakeLane, tmp_path: Path, **overrides):
    provisioned: list[str] = []
    steps: list[str] = []

    def provision(namespace: str) -> None:
        provisioned.append(namespace)
        lane.contents[namespace] = {"secret/sdo-agent-credentials"}

    arguments = {
        "inventory": tmp_path / "lane.inventory.json",
        "keep_namespaces": KEEP,
        "control_namespace": CONTROL,
        "provision_credentials": provision,
        "reseed": lambda: steps.append("reseed"),
        "forget_controller": lambda: steps.append("forget"),
        "timeout_seconds": 30,
        "poll_seconds": 0,
        "sleep": lambda _seconds: None,
    }
    arguments.update(overrides)
    receipt = run_tier1(lane, **arguments)
    return receipt, provisioned, steps


def lane_with(contents: dict[str, set[str]] | None = None, extra: set[str] = frozenset()) -> FakeLane:
    base = {name: set() for name in KEEP | {CONTROL}}
    base.update(contents or {})
    lane = FakeLane(base, {identity for identity in LANE | set(extra) if not identity.startswith("namespace/")})
    return lane


def recorded_baseline(tmp_path: Path) -> None:
    save_inventory(tmp_path / "lane.inventory.json", {i for i in LANE if not i.startswith("namespace/")})


def test_stale_worker_state_from_a_killed_attempt_does_not_survive_tier_1(tmp_path: Path) -> None:
    stale = {
        "configmap/sdo-health-refresh-4b570905c3-request",
        "configmap/sdo-health-refresh-4b570905c3-execution",
        "configmap/sdo-exec-late-1",
        "configmap/sdo-controller-state",
        "job/sdo-controller-run",
    }
    lane = lane_with({CONTROL: stale | {"configmap/kube-root-ca.crt"}})
    recorded_baseline(tmp_path)
    _, provisioned, _ = tier1(lane, tmp_path)
    assert provisioned == [CONTROL]
    assert lane.contents[CONTROL] == {"secret/sdo-agent-credentials"}
    assert not any(lane.has(CONTROL, item) for item in stale)


def test_leftover_fault_namespaces_and_cluster_objects_never_survive(tmp_path: Path) -> None:
    lane = lane_with(
        {"ingress-nginx": {"deployment/controller"}, "gatekeeper-system": set()},
        extra={
            "ingressclass.networking.k8s.io/nginx",
            "validatingwebhookconfiguration.admissionregistration.k8s.io/ingress-nginx-admission",
            "clusterrole.rbac.authorization.k8s.io/ingress-nginx",
        },
    )
    lane.contents[APP] = {"deployment/frontend"}
    recorded_baseline(tmp_path)
    receipt, _, _ = tier1(lane, tmp_path)
    assert set(lane.contents) == KEEP | {CONTROL}
    assert lane.contents[APP] == {"deployment/frontend"}
    assert lane.cluster_objects == {i for i in LANE if not i.startswith("namespace/")}
    assert "namespace/ingress-nginx" in receipt.removed
    assert "ingressclass.networking.k8s.io/nginx" in receipt.removed
    # webhooks and ingress classes go before the namespaces whose backends they route to
    assert lane.deleted.index("ingressclass.networking.k8s.io/nginx") < lane.deleted.index("namespace/ingress-nginx")


def test_infrastructure_app_and_baseline_objects_are_never_removed(tmp_path: Path) -> None:
    lane = lane_with()
    recorded_baseline(tmp_path)
    tier1(lane, tmp_path)
    assert lane.deleted == [f"namespace/{CONTROL}"]


def test_a_lane_without_inventory_adopts_its_cluster_objects_and_wipes_namespaces(tmp_path: Path) -> None:
    lane = lane_with({"ingress-nginx": set()}, extra={"clusterrole.rbac.authorization.k8s.io/harness"})
    receipt, _, _ = tier1(lane, tmp_path)
    assert receipt.inventory_bootstrapped
    assert "namespace/ingress-nginx" not in lane.list_objects()
    assert "clusterrole.rbac.authorization.k8s.io/harness" in (load_inventory(inventory_path_for(tmp_path)) or ())
    again, _, _ = tier1(lane, tmp_path)
    assert not again.inventory_bootstrapped


def inventory_path_for(tmp_path: Path) -> Path:
    return tmp_path / "lane.inventory.json"


def test_tier_1_fails_closed_when_an_object_cannot_be_removed(tmp_path: Path) -> None:
    lane = lane_with({"ingress-nginx": set()})
    lane.lag = 10**6
    recorded_baseline(tmp_path)
    ticks = iter(range(0, 1000, 10))
    with pytest.raises(WipeError, match="ingress-nginx"):
        tier1(lane, tmp_path, timeout_seconds=30, clock=lambda: float(next(ticks)))


def test_credentials_and_seed_run_only_after_the_control_namespace_is_gone(tmp_path: Path) -> None:
    lane = lane_with({CONTROL: {"configmap/sdo-controller-state"}})
    recorded_baseline(tmp_path)
    seen: list[bool] = []

    def provision(namespace: str) -> None:
        seen.append(f"namespace/{namespace}" in lane.list_objects())
        lane.contents[namespace] = {"secret/sdo-agent-credentials"}

    tier1(lane, tmp_path, provision_credentials=provision)
    assert seen == [False]


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), "-c", "user.name=T", "-c", "user.email=t@example.com", *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_memory_after_tier_1_is_exactly_the_planned_commit(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    git(repo, "init", "-q")
    heads = []
    for index in range(3):
        (repo / "memory.txt").write_text(f"commit {index}\n")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", f"accepted {index}")
        heads.append(git(repo, "rev-parse", "HEAD"))
    (repo / "stray.txt").write_text("written by a killed attempt\n")
    lane = lane_with()
    recorded_baseline(tmp_path)
    tier1(lane, tmp_path, reseed=lambda: reseed_workspace(repo, heads[1]))
    assert git(repo, "rev-parse", "HEAD") == heads[1]
    assert (repo / "memory.txt").read_text() == "commit 1\n"
    assert not (repo / "stray.txt").exists()


def test_a_planned_commit_that_does_not_exist_is_refused(tmp_path: Path) -> None:
    repo = tmp_path / "workspace"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "m").write_text("x")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "seed")
    with pytest.raises(WipeError, match="cannot seed"):
        reseed_workspace(repo, "0" * 40)


def test_the_credentials_hook_gets_the_namespace_and_lane_never_a_value(tmp_path: Path) -> None:
    hook = tmp_path / "hook.sh"
    out = tmp_path / "out.json"
    hook.write_text(f'#!/bin/sh\nprintf \'{{"args":"%s","kubeconfig":"%s"}}\' "$*" "$KUBECONFIG" > {out}\n')
    hook.chmod(0o755)
    credentials_hook(hook, tmp_path / "kubeconfig")(CONTROL)
    assert json.loads(out.read_text()) == {"args": CONTROL, "kubeconfig": str(tmp_path / "kubeconfig")}
    failing = tmp_path / "fail.sh"
    failing.write_text("#!/bin/sh\necho no lane lease >&2\nexit 3\n")
    failing.chmod(0o755)
    with pytest.raises(WipeError, match="no lane lease"):
        credentials_hook(failing, tmp_path / "kubeconfig")(CONTROL)


# Tier 2


def tier2(lane: FakeLane, reason, *, deploy=None, namespaces=(APP,), clock=None):
    """Run tier 2 against the fake lane; ``deploy`` stands in for the app deploy and sees the lane as left."""
    deployed: list[dict[str, set[str]]] = []

    def default_deploy() -> None:
        deployed.append({name: set(items) for name, items in lane.contents.items()})
        lane.contents[APP] = {"pvc/mongodb-geo", "pod/geo"}

    receipt = run_tier2(
        reason,
        _receipt(),
        cluster=lane,
        namespaces=namespaces,
        deploy=deploy or default_deploy,
        timeout_seconds=30,
        poll_seconds=0,
        sleep=lambda _seconds: None,
        **({"clock": clock} if clock else {}),
    )
    return receipt, deployed


def faulted_lane() -> FakeLane:
    """The hotel app whose geo database holds the injected user fault inside its PVC."""
    return lane_with({APP: {"pvc/mongodb-geo", "data/mongodb-geo/user-unregistered", "pod/geo"}})


def test_tier_2_keeps_the_warm_app_when_the_attempt_was_clean_and_the_app_is_healthy(tmp_path: Path) -> None:
    marker = AttemptMarker(tmp_path)
    marker.begin()
    marker.end([])
    assert tier2_reason(marker, lambda: None, list) is None
    lane = faulted_lane()
    receipt, deployed = tier2(lane, None)
    assert (deployed, receipt.tier2_ran, receipt.tier2_seconds, receipt.tier2_trigger) == ([], False, 0.0, None)
    assert lane.has(APP, "data/mongodb-geo/user-unregistered")
    assert lane.deleted == []


def test_tier_2_recreates_the_namespace_and_its_data_after_an_unrecovered_row(tmp_path: Path) -> None:
    marker = AttemptMarker(tmp_path)
    marker.begin()
    marker.end(["hr-c/001"])  # the row was injected and the harness recovery was not verified
    reason = tier2_reason(
        AttemptMarker(tmp_path),
        lambda: pytest.fail("a known-unrecovered app is not probed"),
        lambda: pytest.fail("a known-unrecovered app is not probed"),
    )
    assert reason is not None
    assert reason.trigger is Tier2Trigger.PREVIOUS_ROW_UNRECOVERED
    assert "hr-c/001" in reason.detail
    lane = faulted_lane()
    receipt, deployed = tier2(lane, reason)
    assert f"namespace/{APP}" in lane.deleted
    assert len(deployed) == 1
    assert APP not in deployed[0], "the deploy sees no app namespace: its PVC and the fault state are gone"
    assert CONTROL in deployed[0], "only the application is recreated"
    assert not lane.has(APP, "data/mongodb-geo/user-unregistered")
    assert lane.has(APP, "pvc/mongodb-geo")  # a fresh PVC from the deploy
    assert receipt.tier2_ran
    assert receipt.tier2_trigger == "previous_row_unrecovered"


def test_tier_2_recreates_the_namespace_when_a_pod_stays_unhealthy_after_the_restore(tmp_path: Path) -> None:
    reason = tier2_reason(
        AttemptMarker(tmp_path),
        lambda: None,
        lambda: ["hotel-reservation/geo-5d9f (CrashLoopBackOff)"],
    )
    assert reason is not None
    assert reason.trigger is Tier2Trigger.PODS_UNHEALTHY
    assert "geo-5d9f" in reason.detail
    lane = faulted_lane()
    receipt, _ = tier2(lane, reason)
    assert not lane.has(APP, "data/mongodb-geo/user-unregistered")
    assert receipt.tier2_trigger == "pods_unhealthy"


def test_tier_2_fails_closed_when_the_app_namespace_cannot_be_removed() -> None:
    lane = faulted_lane()
    lane.lag = 10**9  # stuck terminating
    ticks = iter(range(0, 1000, 20))
    reason = Tier2Reason(Tier2Trigger.PODS_UNHEALTHY, "x")
    with pytest.raises(WipeError, match=f"namespace/{APP}"):
        tier2(lane, reason, clock=lambda: next(ticks))


def test_a_pod_that_recovers_within_the_bound_does_not_trigger_tier_2() -> None:
    answers = iter([["a/x (Pending)"], ["a/x (Pending)"], []])
    clock = iter(range(100))
    assert (
        await_healthy_pods(
            lambda: next(answers),
            timeout_seconds=30,
            poll_seconds=0,
            clock=lambda: next(clock),
            sleep=lambda _seconds: None,
        )
        == []
    )


def test_a_pod_that_stays_unhealthy_is_reported_when_the_bound_ends() -> None:
    clock = iter(range(0, 1000, 10))
    stuck = await_healthy_pods(
        lambda: ["a/x (CrashLoopBackOff)"],
        timeout_seconds=30,
        poll_seconds=0,
        clock=lambda: next(clock),
        sleep=lambda _seconds: None,
    )
    assert stuck == ["a/x (CrashLoopBackOff)"]


def pod(name: str, phase: str, ready: bool, *, waiting: str | None = None, owner: str | None = None, deleting=False):
    status = {"ready": ready, "state": {"waiting": {"reason": waiting}} if waiting else {"running": {}}}
    metadata: dict = {"name": name, "namespace": APP}
    if owner:
        metadata["ownerReferences"] = [{"kind": owner}]
    if deleting:
        metadata["deletionTimestamp"] = "2026-10-27T00:00:00Z"
    return {
        "metadata": metadata,
        "status": {
            "phase": phase,
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "containerStatuses": [status],
        },
    }


def test_pod_problems_names_pods_that_are_not_ready_and_ignores_finished_jobs_and_terminating_pods() -> None:
    listing = {
        "items": [
            pod("frontend", "Running", True),
            pod("geo", "Running", False, waiting="CrashLoopBackOff"),
            pod("search", "Pending", False),
            pod("migrate", "Succeeded", False, owner="Job"),
            pod("old", "Running", False, deleting=True),
        ]
    }
    assert pod_problems(listing) == [f"{APP}/geo (CrashLoopBackOff)", f"{APP}/search (Pending)"]


def test_unrecovered_rows_name_injected_rows_without_verified_recovery() -> None:
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)

    def record(index: int, injection, **fields):
        return SimpleNamespace(index=index, problem_id=f"p{index}", injection=injection, **fields)

    injected = Injected(started_at=now, finished_at=now)
    recovered = {"recovery_error": None, "full_cleanup_verified": True}
    records = [
        record(0, injected, **recovered),
        record(1, injected, recovery_error=None, full_cleanup_verified=False),
        record(2, injected, recovery_error="geo crash", full_cleanup_verified=False),
        record(3, NotInjected(stage="injection"), recovery_error=None, full_cleanup_verified=False),
        record(4, NotInjected(stage="pre_injection"), recovery_error=None, full_cleanup_verified=False),
    ]
    assert unrecovered_rows(records) == ("p1", "p2", "p3")


def test_tier_2_end_to_end_marker_to_receipt_for_the_incident_that_left_database_state(tmp_path: Path) -> None:
    """The real marker (written by the run that ended) feeds the real decision, tier 2 and the receipt file."""
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    row = SimpleNamespace(
        index=1,
        problem_id="mongodb_user_unregistered",
        injection=Injected(started_at=now, finished_at=now),
        recovery_error="geo unhealthy",
        full_cleanup_verified=False,
    )
    ended = AttemptMarker(tmp_path / "lane")
    ended.begin()
    ended.end(unrecovered_rows([row]))
    reason = tier2_reason(AttemptMarker(tmp_path / "lane"), lambda: None, list)
    lane = faulted_lane()
    receipt, _ = tier2(lane, reason)
    document = json.loads(receipt.write(tmp_path / "results").read_text())
    assert document["tier2_trigger"] == "previous_row_unrecovered"
    assert "mongodb_user_unregistered" in document["tier2_reason"]
    assert not lane.has(APP, "data/mongodb-geo/user-unregistered")


HOTEL_DB_FAULTS = {"hr_mongo": ("storage_user_unregistered:mongodb-geo", "revoke_auth_mongodb:mongodb-geo")}


def injected_row(problem_id: str, *, recovered: bool = True, stage: str | None = None):
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    return SimpleNamespace(
        index=0,
        problem_id=problem_id,
        injection=NotInjected(stage=stage) if stage else Injected(started_at=now, finished_at=now),
        recovery_error=None if recovered else "geo unhealthy",
        full_cleanup_verified=recovered,
    )


def test_a_recovered_row_with_data_level_faults_still_recreates_the_app_and_its_data(tmp_path: Path) -> None:
    """hr-b/003: the first try recovered 5/5, yet its MongoDB user and role changes survived in the PVC."""
    row = injected_row("hr_mongo", recovered=True)
    assert unrecovered_rows([row]) == ()
    ended = AttemptMarker(tmp_path / "lane")
    ended.begin()
    ended.end(unrecovered_rows([row]), data_fault_rows([row], HOTEL_DB_FAULTS))
    reason = tier2_reason(
        AttemptMarker(tmp_path / "lane"),
        lambda: pytest.fail("a data-faulted app is not probed"),
        lambda: pytest.fail("a data-faulted app is not probed"),
    )
    assert reason is not None
    assert reason.trigger is Tier2Trigger.PREVIOUS_ROW_DATA_FAULTS
    lane = faulted_lane()
    receipt, deployed = tier2(lane, reason)
    assert not lane.has(APP, "data/mongodb-geo/user-unregistered")
    assert APP not in deployed[0]
    document = json.loads(receipt.write(tmp_path / "results").read_text())
    assert document["tier2_trigger"] == "previous_row_data_faults"
    assert document["previous_row_data_faults"] == [
        "hr_mongo: storage_user_unregistered:mongodb-geo",
        "hr_mongo: revoke_auth_mongodb:mongodb-geo",
    ]


def test_a_recovered_row_without_data_level_faults_keeps_the_warm_app(tmp_path: Path) -> None:
    row = injected_row("hr_selector")
    ended = AttemptMarker(tmp_path)
    ended.begin()
    ended.end(unrecovered_rows([row]), data_fault_rows([row], HOTEL_DB_FAULTS))
    assert tier2_reason(AttemptMarker(tmp_path), lambda: None, list) is None


def test_data_faults_count_only_rows_that_were_or_may_have_been_injected() -> None:
    rows = [
        injected_row("hr_mongo", stage="pre_injection"),
        injected_row("hr_mongo", stage="injection"),
        injected_row("other+hr_mongo"),
    ]
    assert data_fault_rows(rows, HOTEL_DB_FAULTS) == (
        "hr_mongo: storage_user_unregistered:mongodb-geo",
        "hr_mongo: revoke_auth_mongodb:mongodb-geo",
        "hr_mongo: storage_user_unregistered:mongodb-geo",
        "hr_mongo: revoke_auth_mongodb:mongodb-geo",
    )


def test_an_unrecovered_row_that_is_also_data_level_keeps_its_trigger_and_records_the_faults(tmp_path: Path) -> None:
    row = injected_row("hr_mongo", recovered=False)
    ended = AttemptMarker(tmp_path)
    ended.begin()
    ended.end(unrecovered_rows([row]), data_fault_rows([row], HOTEL_DB_FAULTS))
    reason = AttemptMarker(tmp_path).pending()
    assert reason is not None
    assert reason.trigger is Tier2Trigger.PREVIOUS_ROW_UNRECOVERED
    assert reason.data_faults == tuple(data_fault_rows([row], HOTEL_DB_FAULTS))


def test_the_fault_catalog_owns_which_faults_are_data_level() -> None:
    assert DATA_LEVEL_CLASSES <= set(CLASS_KINDS), "every data-level class is a cataloged fault class"

    def fault(class_name: str, component: str, index: int) -> FaultSpec:
        return FaultSpec(kind=CLASS_KINDS[class_name], component=component, class_name=class_name, declared_index=index)

    description = ProblemDescription(
        problem_id="mixed",
        composite=True,
        faults=(
            fault("MongoDBUserUnregistered", "mongodb-geo", 0),
            fault("WrongServiceSelector", "geo", 1),
            fault("MongoDBRevokeAuth", "mongodb-geo", 2),
        ),
        injection_order=(0, 1, 2),
    )
    assert description.data_level_faults() == ("storage_user_unregistered:mongodb-geo", "revoke_auth_mongodb:mongodb-geo")


def test_a_dead_attempt_leaves_the_marker_and_forces_tier_2(tmp_path: Path) -> None:
    AttemptMarker(tmp_path).begin()  # the attempt died: end never ran
    reason = tier2_reason(AttemptMarker(tmp_path), lambda: None, list)
    assert reason is not None
    assert reason.trigger is Tier2Trigger.UNCLEAN_ATTEMPT


def test_tier_2_redeploys_when_the_restore_invariant_fails(tmp_path: Path) -> None:
    def invariant() -> None:
        raise ValueError("restore invariant: workload not Ready: Deployment/frontend (ready 0/1)")

    reason = tier2_reason(AttemptMarker(tmp_path), invariant, lambda: pytest.fail("the invariant already failed"))
    assert reason is not None
    assert reason.trigger is Tier2Trigger.RESTORE_INVARIANT
    assert "workload not Ready" in reason.detail


def test_the_receipt_records_tier_seconds_and_trigger(tmp_path: Path) -> None:
    now = [0.0]

    def slow_deploy() -> None:
        now[0] = 7.5

    receipt, _ = tier2(
        faulted_lane(),
        Tier2Reason(Tier2Trigger.RESTORE_INVARIANT, "x"),
        deploy=slow_deploy,
        clock=lambda: now[0],
    )
    document = json.loads(receipt.write(tmp_path / "results").read_text())
    assert document["tier1_seconds"] == 12.0
    assert document["tier2_ran"] is True
    assert document["tier2_seconds"] == 7.5
    assert document["tier2_trigger"] == "restore_invariant"


def _receipt():
    from benchmarks.sregym.fastloop.wipe import Tier1Receipt

    return Tier1Receipt(12.0, (), False, ())


def test_inventory_path_sits_beside_the_kubeconfig(tmp_path: Path) -> None:
    assert inventory_path(tmp_path / "kubeconfig").name == "kubeconfig.lane-inventory.json"


def test_wipe_environment_resets_lane_seed_and_controller_registry_together(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment
    from benchmarks.sregym.fastloop.wipe import wipe_environment

    repo = tmp_path / "workspace"
    repo.mkdir()
    git(repo, "init", "-q")
    heads = []
    for index in range(2):
        (repo / "memory.txt").write_text(f"commit {index}\n")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", f"accepted {index}")
        heads.append(git(repo, "rev-parse", "HEAD"))
    environment = FastloopEnvironment(
        run_dir=tmp_path / "run",
        cluster="c",
        kubeconfig=tmp_path / "kubeconfig",
        namespace=APP,
        application=APP,
        workspace=repo,
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
    )
    environment.state_path.parent.mkdir(parents=True)
    environment.state_path.write_text('{"controllers": {"hotel-reservation": {}}}')
    lane = lane_with({CONTROL: {"configmap/sdo-health-refresh-x-request"}, "ingress-nginx": set()})
    save_inventory(inventory_path(environment.kubeconfig), {i for i in LANE if not i.startswith("namespace/")})
    provisioned: list[str] = []

    def provision(namespace: str) -> None:
        provisioned.append(namespace)
        lane.contents[namespace] = set()

    receipt = wipe_environment(
        environment,
        lane,
        applications=[APP],
        provision_credentials=provision,
        planned_commit=heads[0],
        timeout_seconds=30,
    )
    assert provisioned == [CONTROL]
    assert set(lane.contents) == KEEP | {CONTROL}
    assert lane.contents[CONTROL] == set()
    assert git(repo, "rev-parse", "HEAD") == heads[0]
    assert not environment.state_path.exists()
    assert receipt.removed == (f"namespace/{CONTROL}", "namespace/ingress-nginx")


# A fault's finalizer keeps an owned namespace Terminating (FinalizerDeadlock on hotel-reservation)

TOKEN = "configmap/reservation-cleanup-token"


def finalizer_stuck_lane(namespace: str = APP) -> FakeLane:
    lane = lane_with({namespace: {"pvc/mongodb-geo", TOKEN}})
    lane.finalizers = {(namespace, TOKEN)}
    return lane


def test_tier_2_force_finalizes_a_fault_object_that_holds_the_app_namespace_terminating() -> None:
    lane = finalizer_stuck_lane()
    receipt, deployed = tier2(lane, Tier2Reason(Tier2Trigger.PREVIOUS_ROW_UNRECOVERED, "hr-a/002"))
    assert lane.released == [(APP, TOKEN)]
    assert len(deployed) == 1
    assert APP not in deployed[0], "the namespace finished deleting before the deploy"
    assert receipt.force_finalized == (f"{APP}/{TOKEN}",)


def test_tier_2_fails_closed_naming_the_objects_when_force_finalizing_does_not_finish() -> None:
    lane = finalizer_stuck_lane()
    lane.remove_finalizers = lambda namespace, item: None  # the API keeps the finalizer
    clock = iter(range(0, 1000, 10)).__next__
    with pytest.raises(WipeError) as raised:
        tier2(lane, Tier2Reason(Tier2Trigger.PREVIOUS_ROW_UNRECOVERED, "hr-a/002"), clock=lambda: float(clock()))
    assert f"namespace/{APP}" in str(raised.value)
    assert TOKEN in str(raised.value)


def test_tier_1_force_finalizes_a_stuck_harness_namespace_and_records_it(tmp_path: Path) -> None:
    lane = lane_with({"chaos-fault": {"configmap/lock"}})
    lane.finalizers = {("chaos-fault", "configmap/lock")}
    recorded_baseline(tmp_path)
    receipt, _, _ = tier1(lane, tmp_path)
    assert "namespace/chaos-fault" not in lane.list_objects()
    assert receipt.force_finalized == ("chaos-fault/configmap/lock",)


def test_a_namespace_the_wipe_does_not_delete_is_never_force_finalized(tmp_path: Path) -> None:
    kept = next(iter(sorted(KEEP)))
    lane = lane_with({kept: {"configmap/pinned"}})
    lane.finalizers = {(kept, "configmap/pinned")}
    lane.terminating[kept] = 0  # terminating for some other reason: not the wipe's to release
    recorded_baseline(tmp_path)
    receipt, _, _ = tier1(lane, tmp_path)
    assert lane.released == []
    assert receipt.force_finalized == ()


def test_a_wipe_without_stuck_namespaces_force_finalizes_nothing(tmp_path: Path) -> None:
    lane = lane_with({"chaos-fault": {"configmap/ok"}})
    recorded_baseline(tmp_path)
    receipt, _, _ = tier1(lane, tmp_path)
    assert (receipt.force_finalized, lane.released) == ((), [])


def test_the_receipt_file_records_what_was_force_finalized(tmp_path: Path) -> None:
    lane = finalizer_stuck_lane()
    receipt, _ = tier2(lane, Tier2Reason(Tier2Trigger.PREVIOUS_ROW_UNRECOVERED, "hr-a/002"))
    written = json.loads(receipt.write(tmp_path).read_text(encoding="utf-8"))
    assert written["force_finalized"] == [f"{APP}/{TOKEN}"]


def test_only_a_terminating_namespace_reporting_remaining_content_is_stuck() -> None:
    def namespace(deleting: bool, *conditions: tuple[str, str]) -> dict:
        return {
            "metadata": {"name": APP, **({"deletionTimestamp": "2026-10-27T08:17:00Z"} if deleting else {})},
            "status": {"conditions": [{"type": kind, "status": status} for kind, status in conditions]},
        }

    assert namespace_is_stuck(namespace(True, ("NamespaceFinalizersRemaining", "True")))
    assert namespace_is_stuck(namespace(True, ("NamespaceContentRemaining", "True")))
    assert not namespace_is_stuck(namespace(True, ("NamespaceFinalizersRemaining", "False")))
    assert not namespace_is_stuck(namespace(True))
    assert not namespace_is_stuck(namespace(False, ("NamespaceFinalizersRemaining", "True")))


def test_finalizer_holders_name_the_objects_that_carry_finalizers() -> None:
    listing = {
        "items": [
            {
                "kind": "ConfigMap",
                "metadata": {
                    "name": "reservation-cleanup-token",
                    "finalizers": ["cleanup.reservations.io/pending-cleanup"],
                },
            },
            {"kind": "ConfigMap", "metadata": {"name": "plain"}},
            {"kind": "Pod", "metadata": {"name": "geo", "finalizers": []}},
        ]
    }
    assert finalizer_holders(listing) == (TOKEN,)


def test_kubectl_lane_cluster_releases_the_stuck_configmap_through_the_real_wipe_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The real ``KubectlLaneCluster`` into the real loop, against a kubectl that behaves like the API server."""
    state = {"finalizers": ["cleanup.reservations.io/pending-cleanup"], "gone": False}
    calls: list[list[str]] = []

    def fake_kubectl(args, *, namespace, kubeconfig=None, input_text=None, check=True):
        calls.append([*args, f"-n={namespace}"])
        out = ""
        if args[:2] == ["get", "namespace"]:
            out = json.dumps(
                {
                    "metadata": {"name": APP, "deletionTimestamp": "2026-10-27T08:17:00Z"},
                    "status": {"conditions": [{"type": "NamespaceFinalizersRemaining", "status": "True"}]},
                }
            )
        elif args[0] == "api-resources":
            out = "configmaps\npods\n"
        elif args[0] == "get" and args[1] == "configmaps,pods":
            item = {"kind": "ConfigMap", "metadata": {"name": "reservation-cleanup-token", **state_meta(state)}}
            out = json.dumps({"items": [item]})
        elif args[0] == "patch":
            state["finalizers"] = []
            state["gone"] = True
        return SimpleNamespace(stdout=out, returncode=0, stderr="")

    def state_meta(current: dict) -> dict:
        return {"finalizers": current["finalizers"]} if current["finalizers"] else {}

    monkeypatch.setattr("benchmarks.sregym.adapter.kubectl", fake_kubectl)
    # The suite-wide guard empties list_objects; here the namespace lives until its finalizer is gone.
    monkeypatch.setattr(
        KubectlLaneCluster,
        "list_objects",
        lambda self: frozenset() if state["gone"] else frozenset({f"namespace/{APP}"}),
    )
    cluster = KubectlLaneCluster(tmp_path.resolve() / "kubeconfig")
    released = remove_until_gone(
        cluster,
        [f"namespace/{APP}"],
        timeout_seconds=30,
        poll_seconds=0,
        clock=iter(range(1000)).__next__,
        sleep=lambda _seconds: None,
    )
    assert released == (f"{APP}/{TOKEN}",)
    patch = next(call for call in calls if call[0] == "patch")
    assert patch[1] == TOKEN
    assert f"-n={APP}" in patch
