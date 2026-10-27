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
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    Tier2Reason,
    Tier2Trigger,
    WipeError,
    await_healthy_pods,
    credentials_hook,
    inventory_path,
    load_inventory,
    pod_problems,
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
    """A lane whose namespaces hold objects; deleting a namespace removes its content after a short delay."""

    def __init__(self, contents: dict[str, set[str]], cluster_objects: set[str], lag: int = 1) -> None:
        self.contents = contents
        self.cluster_objects = cluster_objects
        self.terminating: dict[str, int] = {}
        self.lag = lag
        self.deleted: list[str] = []

    def list_objects(self) -> frozenset[str]:
        for name in list(self.terminating):
            self.terminating[name] -= 1
            if self.terminating[name] <= 0:
                del self.terminating[name]
                self.contents.pop(name, None)
        return frozenset({f"namespace/{name}" for name in self.contents} | self.cluster_objects)

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

    return Tier1Receipt(12.0, (), False)


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
