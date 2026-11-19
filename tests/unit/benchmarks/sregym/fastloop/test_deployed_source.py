"""A lane keeps its application only while the running app is what deploying the row's planned source produced.

``fastloop up`` deploys from its seed and a row's setup skips the deploy while the app is healthy, so a lane
rebuilt from a seed would otherwise serve the seed's application to rows planning another accepted commit,
and SDO would reject every install against it. The record binds a source identity to a fingerprint of the
live objects and running images on one cluster instance; any of the three changing recreates the app.
"""

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import cli, deployed_application
from benchmarks.sregym.fastloop.deployed_application import LiveApplication, application_fingerprint
from benchmarks.sregym.fastloop.wipe import AttemptMarker, Tier2Trigger, tier2_reason

LIVE = LiveApplication(cluster="cluster-1", objects="objects-1", images=("app@sha256:1",))


def _commit(workspace: Path, files: dict[str, str]) -> str:
    for name, text in files.items():
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    subprocess.run(["git", "-C", str(workspace), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(workspace), "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "c"],
        check=True,
    )
    return subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def live(monkeypatch: pytest.MonkeyPatch) -> dict[str, LiveApplication]:
    current = {"app": LIVE}
    monkeypatch.setattr(deployed_application, "observe", lambda _kubeconfig, _namespace: current["app"])
    return current


@pytest.fixture
def lane(tmp_path: Path) -> SimpleNamespace:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q", str(workspace)], check=True)
    _commit(workspace, {"services/reserve.go": "seed\n", ".sdo/goal.md": "goal\n"})
    return SimpleNamespace(
        kubeconfig=tmp_path / "worker_0.kubeconfig", namespace="app", workspace=workspace, app="hotel"
    )


def test_up_from_the_seed_then_a_row_planning_other_application_source_recreates_the_app(lane, live) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})  # the row's planned accepted commit
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_deployment_artifact_changes_recreate_the_app(lane, live) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {".sdo/deployment/plan.yaml": "telemetry overlay\n"})
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_memory_only_commits_keep_the_warm_app(lane, live) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {".sdo/playbooks/p.md": "learned\n", ".sdo/goal.md": "goal 2\n"})
    assert cli._deployed_source_change(lane) is None


@pytest.mark.parametrize(
    "mutated",
    [
        LiveApplication(cluster="cluster-1", objects="patched-by-a-row", images=LIVE.images),
        LiveApplication(cluster="cluster-1", objects=LIVE.objects, images=("app@sha256:repaired",)),
    ],
)
def test_an_app_changed_since_its_deploy_is_recreated(lane, live, mutated) -> None:
    """A deployer patch, verified source repair or agent write during a row is not what the record deployed."""
    cli._record_fresh_deploy(lane)
    live["app"] = mutated
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED


def test_a_rebuilt_cluster_at_the_same_kubeconfig_path_does_not_inherit_the_record(lane, live) -> None:
    cli._record_fresh_deploy(lane)
    live["app"] = LiveApplication(cluster="cluster-2", objects=LIVE.objects, images=LIVE.images)
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED


def test_a_lane_that_never_recorded_its_application_recreates_it(lane, live) -> None:
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_an_unreadable_workspace_is_unknown_and_recreates_instead_of_aborting(lane, live, tmp_path) -> None:
    cli._record_fresh_deploy(lane)
    unreadable = SimpleNamespace(**{**vars(lane), "workspace": tmp_path / "not-a-repository"})
    assert cli._deployed_source_change(unreadable).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_an_unobservable_app_is_unknown_and_recreates_instead_of_aborting(lane, monkeypatch) -> None:
    monkeypatch.setattr(deployed_application, "observe", lambda *_args: (_ for _ in ()).throw(RuntimeError("api")))
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.DEPLOYED_APPLICATION_CHANGED


def test_tier2_consults_the_deployed_source_before_keeping_a_healthy_app(lane, live, tmp_path: Path) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    reason = tier2_reason(
        AttemptMarker(tmp_path / "run"),
        lambda: None,
        list,
        source_change=lambda: cli._deployed_source_change(lane),
    )
    assert reason is not None
    assert reason.trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def _recreation_lane(lane, monkeypatch, run_tier2):
    monkeypatch.setattr(cli, "run_tier2", run_tier2)
    monkeypatch.setattr(cli, "_recapture_application_baseline", lambda _env: None)
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: (lane.namespace,))
    monkeypatch.setattr(cli, "HostLimits", SimpleNamespace(from_env=lambda _env: None))
    monkeypatch.setattr(cli, "LaneInjectionState", lambda *_args: None)
    monkeypatch.setattr(cli, "KubectlLaneCluster", lambda *_args: None)
    return SimpleNamespace(**vars(lane), deploy_problem="p", baseline_path=lane.workspace / "b.json", worker_env={})


class _Worker:
    def request(self, op, **_kwargs):
        assert op == "deploy"
        return {"deployed": True, "health": {"healthy": True}}


def test_a_recreation_records_the_app_only_after_it_completes(lane, live, monkeypatch, tmp_path) -> None:
    cli._record_fresh_deploy(lane)  # the seed
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    during = []

    def run_tier2(_reason, _tier1, *, deploy, **_kwargs):
        deploy()
        during.append(cli._deployed_source_change(lane))  # mid-transaction: nothing claims the new app yet
        return SimpleNamespace(write=lambda _directory: None)

    environment = _recreation_lane(lane, monkeypatch, run_tier2)
    cli.recreate_application(environment, _Worker(), object(), object(), tmp_path)
    assert during[0] is not None
    assert cli._deployed_source_change(lane) is None


def test_a_failed_recreation_leaves_the_app_unknown(lane, live, monkeypatch, tmp_path) -> None:
    cli._record_fresh_deploy(lane)

    def run_tier2(*_args, **_kwargs):
        raise RuntimeError("rollout timed out")

    environment = _recreation_lane(lane, monkeypatch, run_tier2)
    with pytest.raises(RuntimeError):
        cli.recreate_application(environment, _Worker(), object(), object(), tmp_path)
    assert cli._deployed_source_change(lane) is not None  # the old record no longer vouches for this app


def test_rows_that_do_not_wipe_at_start_still_recreate_a_stale_app(lane, live, monkeypatch, tmp_path) -> None:
    """Shared-accepted, parallel and vanilla rows have no Tier-1, but must not keep a seed app either."""
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    calls = []
    monkeypatch.setattr(cli, "recreate_application", lambda *args: calls.append(("recreate", args[2].trigger)))
    monkeypatch.setattr(cli, "deploy_with_setup_proof", lambda *_args, **_kwargs: calls.append(("deploy", None)))
    args = SimpleNamespace(agent="claudecode", planned_commit=None)
    cli._setup_application(lane, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert calls == [("recreate", Tier2Trigger.ACCEPTED_SOURCE_CHANGED)]
    calls.clear()
    cli._record_fresh_deploy(lane)
    cli._setup_application(lane, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert calls == [("deploy", None)]


def test_a_warm_lane_running_other_application_source_falls_back_to_a_full_reset(lane, live) -> None:
    from benchmarks.sregym.fastloop.warm_environment import planned_application_unchanged

    cli._record_fresh_deploy(lane)
    planned_application_unchanged(lane, None)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    with pytest.raises(ValueError, match="accepted_source_changed"):
        planned_application_unchanged(lane, None)


def test_the_fingerprint_ignores_rollout_restarts_but_not_spec_changes() -> None:
    def deployment(annotations, image="app:latest"):
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "web", "namespace": "app", "labels": {"service": "web"}},
            "spec": {
                "template": {"metadata": {"annotations": annotations}, "spec": {"containers": [{"image": image}]}}
            },
        }

    fresh = application_fingerprint([deployment({"p": "1"})], "app")
    assert application_fingerprint([deployment({"p": "1", "kubectl.kubernetes.io/restartedAt": "t"})], "app") == fresh
    assert application_fingerprint([deployment({"p": "2"})], "app") != fresh
    assert application_fingerprint([deployment({"p": "1"}, image="app:fixed")], "app") != fresh


def test_a_vanilla_row_reseeds_to_its_planned_source_and_records_what_it_ran(lane, live, monkeypatch, tmp_path) -> None:
    """A vanilla row on a lane holding other source recreates first; a matching lane keeps its warm app."""
    import json

    base = subprocess.run(
        ["git", "-C", str(lane.workspace), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    cli._record_fresh_deploy(lane)  # the lane holds the base deploy
    _commit(lane.workspace, {"services/reserve.go": "a previous row's source\n"})
    calls = []

    def recreate(environment, _worker, reason, _tier1, _results):
        calls.append(reason.trigger)
        cli._record_deployed_source(environment)

    monkeypatch.setattr(cli, "recreate_application", recreate)
    monkeypatch.setattr(cli, "deploy_with_setup_proof", lambda *_args, **_kwargs: calls.append("kept"))
    args = SimpleNamespace(agent="claudecode", planned_commit=base)
    vanilla = SimpleNamespace(**vars(lane), shared_accepted_id=None)
    cli._setup_application(vanilla, object(), tmp_path, args, tier1=None, redeploy_reason=None, warm=False)
    assert calls == ["kept"]  # reseeded to the base commit, which is what the lane runs
    ran = json.loads((tmp_path / "app-source.json").read_text())
    assert ran["revision"] == base
    assert ran["source"] == deployed_application.application_source_identity(lane.workspace, base)
