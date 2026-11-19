"""A warm lane keeps its application only while it runs the application content the row plans.

``fastloop up`` deploys from its seed and a row's setup skips the deploy while the app is healthy, so a
lane rebuilt from a seed would otherwise serve the seed's application to rows planning another accepted
commit, and SDO would reject every install against it.
"""

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop import cli
from benchmarks.sregym.fastloop.wipe import AttemptMarker, Tier2Trigger, tier2_reason


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
def lane(tmp_path: Path) -> SimpleNamespace:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q", str(workspace)], check=True)
    _commit(workspace, {"services/reserve.go": "seed\n", ".sdo/goal.md": "goal\n"})
    return SimpleNamespace(kubeconfig=tmp_path / "worker_0.kubeconfig", namespace="app", workspace=workspace)


def test_up_from_the_seed_then_a_row_planning_other_application_source_recreates_the_app(lane) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})  # the row's planned accepted commit
    reason = cli._deployed_source_change(lane)
    assert reason is not None
    assert reason.trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_deployment_artifact_changes_recreate_the_app(lane) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {".sdo/deployment/plan.yaml": "telemetry overlay\n"})
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_memory_only_commits_keep_the_warm_app(lane) -> None:
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {".sdo/playbooks/p.md": "learned\n", ".sdo/goal.md": "goal 2\n"})
    assert cli._deployed_source_change(lane) is None


def test_a_recreated_app_records_the_source_it_was_deployed_from(lane, monkeypatch) -> None:
    monkeypatch.setattr(cli, "application_namespaces", lambda *_args: ())
    monkeypatch.setattr(cli, "KubectlApplicationCluster", lambda *_args: None)
    monkeypatch.setattr(cli, "save_application_baseline", lambda *_args, **_kwargs: None)
    cli._record_fresh_deploy(lane)
    _commit(lane.workspace, {"services/reserve.go": "fixed\n"})
    cli._recapture_application_baseline(SimpleNamespace(**vars(lane), baseline_path=lane.workspace / "b.json"))
    assert cli._deployed_source_change(lane) is None


def test_a_lane_that_never_recorded_its_application_source_recreates_it(lane) -> None:
    assert cli._deployed_source_change(lane).trigger is Tier2Trigger.ACCEPTED_SOURCE_CHANGED


def test_tier2_consults_the_deployed_source_before_keeping_a_healthy_app(lane, tmp_path: Path) -> None:
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
