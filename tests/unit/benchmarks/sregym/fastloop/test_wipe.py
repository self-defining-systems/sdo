"""Tier 1 wipes everything SDO and the harness own; tier 2 redeploys the app only when it must."""

from __future__ import annotations

import json
import subprocess
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.adapter import allowed_namespaces
from benchmarks.sregym.fastloop.wipe import (
    AttemptMarker,
    WipeError,
    credentials_hook,
    inventory_path,
    load_inventory,
    reseed_workspace,
    run_tier1,
    run_tier2,
    save_inventory,
    tier2_reason,
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


def test_tier_2_keeps_the_warm_app_when_the_attempt_was_clean_and_the_invariant_holds(tmp_path: Path) -> None:
    marker = AttemptMarker(tmp_path)
    assert tier2_reason(marker, lambda: None) is None
    redeployed: list[str] = []
    receipt = run_tier2(None, lambda: redeployed.append("x"), _receipt())
    assert (redeployed, receipt.tier2_ran, receipt.tier2_seconds) == ([], False, 0.0)


def test_tier_2_redeploys_after_an_unclean_attempt(tmp_path: Path) -> None:
    marker = AttemptMarker(tmp_path)
    marker.begin()  # the attempt died: end_cleanly never ran
    assert AttemptMarker(tmp_path).unclean()
    reason = tier2_reason(AttemptMarker(tmp_path), lambda: pytest.fail("a known-unclean app is not probed"))
    assert reason == "previous attempt did not end cleanly"
    redeployed: list[str] = []
    receipt = run_tier2(reason, lambda: redeployed.append("x"), _receipt())
    assert redeployed == ["x"]
    assert receipt.tier2_ran
    assert receipt.tier2_reason == reason


def test_tier_2_redeploys_when_the_restore_invariant_fails(tmp_path: Path) -> None:
    def invariant() -> None:
        raise ValueError("restore invariant: workload not Ready: Deployment/frontend (ready 0/1)")

    reason = tier2_reason(AttemptMarker(tmp_path), invariant)
    assert reason is not None
    assert reason.startswith("restore invariant failed")


def test_a_clean_end_clears_the_marker(tmp_path: Path) -> None:
    marker = AttemptMarker(tmp_path)
    marker.begin()
    marker.end_cleanly()
    assert tier2_reason(marker, lambda: None) is None


def test_the_receipt_records_tier_seconds(tmp_path: Path) -> None:
    clock = iter([0.0, 7.5])
    receipt = run_tier2("restore invariant failed: x", lambda: None, _receipt(), clock=lambda: next(clock))
    path = receipt.write(tmp_path / "results")
    document = json.loads(path.read_text())
    assert document["tier1_seconds"] == 12.0
    assert document["tier2_ran"] is True
    assert document["tier2_seconds"] == 7.5


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
