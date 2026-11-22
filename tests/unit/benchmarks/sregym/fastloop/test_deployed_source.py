"""A lane keeps its application only while it is the deploy of the row's application revision.

``fastloop up`` deploys from its seed and a row's setup skipped the deploy while the app was healthy, so a lane
rebuilt from a seed served the seed's application to rows planning another accepted commit and SDO rejected
every install. The invariant: every harness deploy stamps the application baseline it captures with the source
it deployed (the revision fixed before the deploy, its identity, the cluster instance); a row keeps the app only
when that stamp names its application revision's source and the live app still matches the baseline, the state
every fault restore returns to. Anything unknown recreates.
"""

from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.adapter import kustomize_source
from benchmarks.sregym.fastloop import cli, deployed_application
from benchmarks.sregym.fastloop.baseline_recovery import (
    ApplicationBaseline,
    ApplicationNamespaceAbsent,
    save_application_baseline,
)
from benchmarks.sregym.fastloop.deployed_application import (
    application_source_identity,
    deployed_application_change,
    record_deployed_source,
)
from benchmarks.sregym.fastloop.wipe import AttemptMarker, Tier2Reason, Tier2Trigger, tier2_reason

if TYPE_CHECKING:
    from pathlib import Path

NAMESPACE = "app"


def _git(workspace: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(workspace), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(workspace: Path, files: dict[str, str]) -> str:
    for name, text in files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    _git(workspace, "add", "-A")
    _git(workspace, "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "c")
    return _git(workspace, "rev-parse", "HEAD")


def deployment(image: str = "app:latest", **annotations: str) -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "web", "namespace": NAMESPACE, "labels": {"service": "web"}},
        "spec": {
            "replicas": 1,
            "template": {"metadata": {"annotations": dict(annotations)}, "spec": {"containers": [{"image": image}]}},
        },
    }


def job(name: str, **extra: Any) -> dict[str, Any]:
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": NAMESPACE, **extra.pop("metadata", {})},
        "spec": {"template": {"spec": {"containers": [{"image": "tool"}]}}, **extra},
    }


class Cluster:
    """The live application and cluster instance a row observes."""

    def __init__(self) -> None:
        self.objects: list[dict[str, Any]] = [deployment()]
        self.instance = "cluster-1"
        self.absent = False

    def live(self, _kubeconfig: Path, _namespace: str) -> list[dict[str, Any]]:
        if self.absent:
            raise ApplicationNamespaceAbsent(NAMESPACE)
        return [json.loads(json.dumps(value)) for value in self.objects]


@pytest.fixture
def cluster(monkeypatch: pytest.MonkeyPatch) -> Cluster:
    live = Cluster()
    monkeypatch.setattr(deployed_application, "live_objects", live.live)
    monkeypatch.setattr(deployed_application, "cluster_instance", lambda _kubeconfig: live.instance)
    return live


@pytest.fixture
def lane(tmp_path: Path) -> SimpleNamespace:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _git(workspace, "init", "-q")
    _commit(workspace, {"services/reserve.go": "seed\n", ".sdo/goal.md": "goal\n"})
    baseline = tmp_path / "cluster_baseline_state.json"
    baseline.write_text("{}\n")
    return SimpleNamespace(
        kubeconfig=tmp_path / "worker_0.kubeconfig",
        namespace=NAMESPACE,
        workspace=workspace,
        app="hotel",
        problem_application="hotel-reservation",
        baseline_path=baseline,
        shared_accepted_id=None,
        run_dir=tmp_path,
        host_owned_kinds=(),
    )


def deploy(lane: SimpleNamespace, cluster: Cluster, revision: str | None = None) -> str:
    """What a harness deploy does: capture the baseline of the live app, then stamp it with the source."""
    revision = revision or _git(lane.workspace, "rev-parse", "HEAD")
    save_application_baseline(
        lane.baseline_path, (ApplicationBaseline(NAMESPACE, cluster.live(lane.kubeconfig, NAMESPACE)),), installed=True
    )
    record_deployed_source(lane, revision)
    return revision


def head(lane: SimpleNamespace) -> str:
    return _git(lane.workspace, "rev-parse", "HEAD")


def test_a_clean_lane_running_the_planned_deploy_keeps_its_app(lane, cluster) -> None:
    base = deploy(lane, cluster)
    assert deployed_application_change(lane, base) is None


def test_a_seed_deploy_is_not_the_planned_application(lane, cluster) -> None:
    seed = deploy(lane, cluster)
    planned = _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    assert deployed_application_change(lane, seed) is None
    assert deployed_application_change(lane, planned).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_memory_commits_keep_and_deployment_artifact_commits_recreate(lane, cluster) -> None:
    deploy(lane, cluster)
    learned = _commit(lane.workspace, {".sdo/playbooks/p.md": "learned\n", ".sdo/goal.md": "goal 2\n"})
    assert deployed_application_change(lane, learned) is None
    overlay = _commit(lane.workspace, {".sdo/deployment/plan.yaml": "telemetry overlay\n"})
    assert deployed_application_change(lane, overlay).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


SEED_PLAN = {
    "source": {
        "rendererId": "raw",
        "config": {"@type": "t/RawRenderConfig", "paths": [".sdo/deployment/manifests/app.yaml"]},
    },
    "images": [{"image": "registry/app:v1", "sourceId": "buildkit"}],
    "expectedWorkloadIds": ["Deployment/app/web"],
}


def _plan(source: dict[str, Any] | None = None, **fields: Any) -> str:
    return json.dumps({**SEED_PLAN, **({"source": source} if source else {}), **fields})


# What the harness deploys from, as the install records it (camelCase, as the Go side writes the plan).
_harness = kustomize_source("kubernetes")
HARNESS_SOURCE = {"rendererId": _harness["renderer_id"], "config": _harness["config"]}


def test_the_plan_an_install_records_for_the_source_the_app_was_deployed_from_keeps_the_app(lane, cluster) -> None:
    """A seeded memory's authored plan is replaced by the install with one naming the harness's own source.

    The lane's app was deployed from that source, so the recorded plan describes it: only the plan's pointer
    moved, which is what every learned (seeded) row's accepted commit carries after the first install.
    """
    _commit(lane.workspace, {".sdo/deployment/plan.yaml": _plan(), ".sdo/deployment/manifests/app.yaml": "authored\n"})
    deploy(lane, cluster)
    learned = _commit(
        lane.workspace, {".sdo/deployment/plan.yaml": _plan(HARNESS_SOURCE), ".sdo/playbooks/p/diagnose.go": "x\n"}
    )
    assert deployed_application_change(lane, learned) is None


def test_a_plan_naming_another_source_or_changing_more_than_the_source_recreates(lane, cluster) -> None:
    _commit(lane.workspace, {".sdo/deployment/plan.yaml": _plan(), ".sdo/deployment/manifests/app.yaml": "authored\n"})
    deploy(lane, cluster)
    other = {"rendererId": "kustomize", "config": {**HARNESS_SOURCE["config"], "path": "elsewhere"}}
    for files in (
        {".sdo/deployment/plan.yaml": _plan(other)},
        {".sdo/deployment/plan.yaml": _plan(HARNESS_SOURCE, images=[{"image": "registry/app:v2"}])},
        {".sdo/deployment/plan.yaml": _plan(HARNESS_SOURCE), ".sdo/deployment/manifests/app.yaml": "edited\n"},
        {".sdo/deployment/plan.yaml": _plan(HARNESS_SOURCE), ".sdo/deployment/patches/0001.patch": "patch\n"},
    ):
        row = _commit(lane.workspace, files)
        assert deployed_application_change(lane, row).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED, files


def test_a_row_that_changed_the_app_hands_the_next_row_a_recreate(lane, cluster) -> None:
    """A deployer patch, a verified source repair or an agent write is not what the deploy produced."""
    base = deploy(lane, cluster)
    cluster.objects = [deployment(image="app:repaired")]
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED


def test_restarts_and_job_churn_are_not_application_changes(lane, cluster) -> None:
    base = deploy(lane, cluster)
    cluster.objects = [
        deployment(**{"kubectl.kubernetes.io/restartedAt": "2026-11-19T21:35:48Z"}),
        job("nightly-1", metadata={"ownerReferences": [{"kind": "CronJob", "name": "nightly"}]}),
        job("once", ttlSecondsAfterFinished=60),
    ]
    assert deployed_application_change(lane, base) is None


def test_a_rebuilt_cluster_at_the_same_paths_does_not_inherit_the_stamp(lane, cluster) -> None:
    base = deploy(lane, cluster)
    cluster.instance = "cluster-2"
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED


def test_a_baseline_recaptured_without_a_deploy_is_no_longer_vouched_for(lane, cluster) -> None:
    base = deploy(lane, cluster)
    cluster.objects = [deployment(image="app:other")]
    save_application_baseline(
        lane.baseline_path, (ApplicationBaseline(NAMESPACE, cluster.live(lane.kubeconfig, NAMESPACE)),), installed=True
    )
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_unknown_facts_recreate_instead_of_aborting_the_row(lane, cluster, tmp_path, monkeypatch) -> None:
    base = deploy(lane, cluster)
    assert deployed_application_change(lane, "0" * 40).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED
    unreadable = SimpleNamespace(**{**vars(lane), "workspace": tmp_path / "not-a-repository"})
    assert deployed_application_change(unreadable, base).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED
    cluster.absent = True  # ApplicationNamespaceAbsent is a LookupError
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED
    cluster.absent = False
    monkeypatch.setattr(
        deployed_application, "cluster_instance", lambda _k: (_ for _ in ()).throw(RuntimeError("api down"))
    )
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED
    lane.baseline_path.unlink()
    assert deployed_application_change(lane, base).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_the_source_check_waits_for_restore_and_pod_health(tmp_path) -> None:
    changed = Tier2Reason(Tier2Trigger.ACCEPTED_SOURCE_CHANGED, "x")
    reason = tier2_reason(
        AttemptMarker(tmp_path), lambda: None, lambda: ["app/web (ContainerCreating)"], source_change=lambda: changed
    )
    assert reason.trigger is Tier2Trigger.PODS_UNHEALTHY
    assert tier2_reason(AttemptMarker(tmp_path), lambda: None, list, source_change=lambda: changed) is changed


class Worker:
    def __init__(self, deployed: bool = True, during: Any = None) -> None:
        self.requests: list[dict[str, Any]] = []
        self.deployed, self.during = deployed, during

    def request(self, op: str, **kwargs: Any) -> dict[str, Any]:
        assert op == "deploy"
        self.requests.append(kwargs)
        if self.during is not None:
            self.during()
        return {"deployed": self.deployed, "health": {"healthy": True}}


def recreation(lane, cluster, monkeypatch, run_tier2=None):
    def default(_reason, _tier1, *, deploy, **_kwargs):
        deploy()
        return SimpleNamespace(write=lambda _directory: None)

    monkeypatch.setattr(cli, "run_tier2", run_tier2 or default)
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: (NAMESPACE,))
    monkeypatch.setattr(cli, "HostLimits", SimpleNamespace(from_env=lambda _env: None))
    monkeypatch.setattr(cli, "LaneInjectionState", lambda *_args: None)
    monkeypatch.setattr(cli, "KubectlLaneCluster", lambda *_args: None)
    monkeypatch.setattr(
        cli, "KubectlApplicationCluster", lambda _k: SimpleNamespace(list_objects=lambda ns: cluster.live(_k, ns))
    )
    return SimpleNamespace(**vars(lane), deploy_problem="p", worker_env={})


def test_a_recreation_deploys_and_stamps_the_revision_fixed_before_it(lane, cluster, monkeypatch, tmp_path) -> None:
    """Another lane's broker may move HEAD during the deploy; the stamp names what was deployed."""
    planned = head(lane)
    worker = Worker(during=lambda: _commit(lane.workspace, {"services/reserve.go": "published meanwhile\n"}))
    environment = recreation(lane, cluster, monkeypatch)
    cli.recreate_application(environment, worker, object(), object(), tmp_path, revision=planned)
    assert worker.requests[0]["revision"] == planned
    assert deployed_application_change(lane, planned) is None
    assert deployed_application_change(lane, head(lane)) is not None


def test_a_failed_recreation_leaves_the_app_unknown(lane, cluster, monkeypatch, tmp_path) -> None:
    base = deploy(lane, cluster)

    def fail(*_args, **_kwargs):
        raise RuntimeError("rollout timed out")

    environment = recreation(lane, cluster, monkeypatch, run_tier2=fail)
    with pytest.raises(RuntimeError):
        cli.recreate_application(environment, Worker(), object(), object(), tmp_path, revision=base)
    assert deployed_application_change(lane, base) is not None


def test_a_setup_proof_deploy_that_deployed_stamps_its_baseline(lane, cluster, monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        cli, "KubectlApplicationCluster", lambda _k: SimpleNamespace(list_objects=lambda ns: cluster.live(_k, ns))
    )
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: (NAMESPACE,))
    base = head(lane)
    environment = SimpleNamespace(**vars(lane), deploy_problem="p")
    cli.deploy_with_setup_proof(environment, Worker(deployed=True), tmp_path, allow_reuse=False, revision=base)
    assert deployed_application_change(lane, base) is None
    other = _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    cli.deploy_with_setup_proof(environment, Worker(deployed=False), tmp_path, allow_reuse=False, revision=other)
    assert deployed_application_change(lane, other) is not None  # a skipped deploy vouches for nothing new


def setup(lane, cluster, monkeypatch, *, planned: str | None, agent: str = "claudecode"):
    calls: list[Any] = []

    def recreate(environment, _worker, reason, _tier1, _results, *, revision):
        calls.append(("recreate", reason.trigger, revision))
        deploy(environment, cluster, revision)

    monkeypatch.setattr(cli, "recreate_application", recreate)
    monkeypatch.setattr(
        cli, "deploy_with_setup_proof", lambda *_args, **kwargs: calls.append(("kept", kwargs["revision"]))
    )
    args = SimpleNamespace(agent=agent, planned_commit=planned)
    return calls, args


def test_a_vanilla_row_deploys_its_planned_source_without_moving_the_workspace(
    lane, cluster, monkeypatch, tmp_path
) -> None:
    """Only the deployed app follows the arm's source; the workspace (and everything an agent could read) stays."""
    base = head(lane)
    deploy(lane, cluster)  # the lane holds the base deploy
    later = _commit(lane.workspace, {"services/reserve.go": "a previous row's source\n", ".sdo/goal.md": "later\n"})
    before = {
        path: path.read_bytes() for path in lane.workspace.rglob("*") if path.is_file() and ".git" not in path.parts
    }
    calls, args = setup(lane, cluster, monkeypatch, planned=base)
    cli._setup_application(lane, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert calls == [("kept", base)]
    assert head(lane) == later
    assert before == {
        path: path.read_bytes() for path in lane.workspace.rglob("*") if path.is_file() and ".git" not in path.parts
    }
    ran = json.loads((tmp_path / "app-source.json").read_text())
    assert ran == {"revision": base, "source": application_source_identity(lane.workspace, base)}


def test_a_vanilla_row_on_a_seed_lane_recreates_its_planned_source_first(lane, cluster, monkeypatch, tmp_path) -> None:
    deploy(lane, cluster)  # seed
    base = _commit(lane.workspace, {"services/reserve.go": "accepted\n"})
    calls, args = setup(lane, cluster, monkeypatch, planned=base)
    cli._setup_application(lane, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert calls == [("recreate", Tier2Trigger.ACCEPTED_SOURCE_CHANGED, base)]
    assert json.loads((tmp_path / "app-source.json").read_text())["source"] == application_source_identity(
        lane.workspace, base
    )


def test_an_unverified_app_reports_no_source(lane, cluster, monkeypatch, tmp_path) -> None:
    deploy(lane, cluster)
    cluster.objects = [deployment(image="app:drifted")]
    monkeypatch.setattr(cli, "recreate_application", lambda *_args, **_kwargs: None)  # a recreate that changed nothing
    calls, args = setup(lane, cluster, monkeypatch, planned=None)
    monkeypatch.setattr(cli, "recreate_application", lambda *_args, **_kwargs: None)
    cli._setup_application(lane, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert json.loads((tmp_path / "app-source.json").read_text())["source"] is None


def test_a_warm_lane_running_other_application_source_falls_back_to_a_full_reset(lane, cluster) -> None:
    from benchmarks.sregym.fastloop.warm_environment import planned_application_unchanged

    base = deploy(lane, cluster)
    planned_application_unchanged(lane, base)
    planned = _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    with pytest.raises(ValueError, match="accepted_source_changed"):
        planned_application_unchanged(lane, planned)


def test_the_harness_deploy_renders_the_pinned_revision_not_head(lane) -> None:
    from benchmarks.sregym.fastloop.accepted_source import AcceptedSource

    base = head(lane)
    _commit(lane.workspace, {"services/reserve.go": "later\n"})
    assert AcceptedSource(lane.workspace, NAMESPACE, pinned_revision=base).revision == base
    assert AcceptedSource(lane.workspace, NAMESPACE).revision == head(lane)
