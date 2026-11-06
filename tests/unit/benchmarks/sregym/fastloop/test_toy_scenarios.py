"""Toy commit scenarios: the drivers and assertions against a deterministic fake world, plus the live-read parsers."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.fastloop import toy_scenarios as sc
from benchmarks.sregym.fastloop import toy_upstream as up
from benchmarks.sregym.fastloop.toy import prepare_workspace, provided_deployment_plan

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

PUSHED = "a" * 40
BASE = sc.Snapshot(
    head="h0",
    images={"backend": "toy:v1", "frontend": "toy:v1"},
    digests={"backend": "sha256:old", "frontend": "sha256:old"},
    agent_tasks=0,
    pod_failures=0,
    controller_relaunches=0,
)
TIMINGS = sc.ScenarioTimings(verdict=100.0, steady=20.0, poll=5.0)


@dataclass
class World:
    """A scripted cluster: ``events`` maps a time offset after the push to a mutation of the world."""

    snap: sc.Snapshot = BASE
    commits: list[sc.AcceptedCommit] = field(default_factory=list)
    hold_records: list[sc.HoldRecord] = field(default_factory=list)
    incident: bool = False
    now: float = 0.0
    pushed_at: float | None = None
    events: dict[float, Callable[[World], None]] = field(default_factory=dict)

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.pushed_at is not None:
            for at in sorted(self.events):
                if at + self.pushed_at <= self.now:
                    self.events.pop(at)(self)
                    break

    def push(self, name: str) -> str:
        self.pushed_at = self.now
        return PUSHED

    def snapshot(self) -> sc.Snapshot:
        return self.snap

    def accepted_commits(self) -> list[sc.AcceptedCommit]:
        return list(self.commits)

    def holds(self) -> list[sc.HoldRecord]:
        return list(self.hold_records)

    def incident_open(self) -> bool:
        return self.incident


def accept(world: World) -> None:
    world.commits.insert(0, sc.AcceptedCommit("h1", {"SDO-Actor": "upstream", "SDO-Upstream": PUSHED}))
    world.snap = replace(
        world.snap, head="h1", digests={"backend": "sha256:new", "frontend": "sha256:new"}, controller_relaunches=1
    )


def hold(reason: str) -> Callable[[World], None]:
    def apply(world: World) -> None:
        world.hold_records.append(sc.HoldRecord(PUSHED, reason))
        world.snap = replace(world.snap, images={"backend": "toy:v2", "frontend": "toy:v1"})

    return apply


def benign_world() -> World:
    return World(events={6: accept})


def broken_world() -> World:
    return World(events={6: hold("rollout: backend did not become ready")})


def test_benign_passes_on_a_clean_advance():
    result = sc.run_benign(benign_world(), TIMINGS)
    assert result.violations == []
    assert result.passed
    assert result.verdict == "accepted"
    assert result.pushed == PUSHED


def _after_accept(**changes: object) -> Callable[[World], None]:
    def apply(world: World) -> None:
        accept(world)
        world.snap = replace(world.snap, **changes)  # type: ignore[arg-type]

    return apply


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"agent_tasks": 1}, "agent tasks"),
        ({"pod_failures": 2}, "pod failures"),
        ({"controller_relaunches": 3}, "relaunched"),
        ({"digests": {"backend": "sha256:new", "frontend": "sha256:old"}}, "old image"),
    ],
    ids=["agent-task", "pod-failure", "relaunch-loop", "stale-workload"],
)
def test_benign_negative_controls(changes, expected):
    result = sc.run_benign(World(events={6: _after_accept(**changes)}), TIMINGS)
    assert any(expected in v for v in result.violations), result.violations


def test_benign_fails_on_an_incident_in_the_steady_window():
    world = World(events={6: accept, 12: lambda w: setattr(w, "incident", True)})
    result = sc.run_benign(world, TIMINGS)
    assert any("incident opened during the steady window" in v for v in result.violations)


def test_benign_fails_when_the_commit_is_held_or_never_moves():
    held = World(events={6: hold("rollout: stalled")})
    assert any("was held" in v for v in sc.run_benign(held, TIMINGS).violations)
    silent = sc.run_benign(World(), TIMINGS)
    assert any("neither accepted nor held" in v for v in silent.violations)


def test_benign_requires_the_upstream_actor():
    def accept_as_responder(world: World) -> None:
        accept(world)
        world.commits[0] = sc.AcceptedCommit("h1", {"SDO-Actor": "responder", "SDO-Upstream": PUSHED})

    result = sc.run_benign(World(events={6: accept_as_responder}), TIMINGS)
    assert any("SDO-Actor" in v for v in result.violations)


def test_broken_passes_on_a_typed_hold_with_the_cluster_left_alone():
    result = sc.run_broken(broken_world(), TIMINGS)
    assert result.violations == []
    assert result.verdict == "held"


def test_broken_passes_on_an_incident():
    result = sc.run_broken(World(events={6: lambda w: setattr(w, "incident", True)}), TIMINGS)
    assert result.violations == []
    assert result.verdict == "incident"


def test_broken_negative_controls():
    assert any("accepted as" in v for v in sc.run_broken(World(events={6: accept}), TIMINGS).violations)
    assert any("neither held" in v for v in sc.run_broken(World(), TIMINGS).violations)
    untyped = sc.run_broken(World(events={6: hold("it broke")}), TIMINGS)
    assert any("not typed" in v for v in untyped.violations)
    other = sc.run_broken(World(events={6: hold("merge_conflict: main.go")}), TIMINGS)
    assert any("not one of" in v for v in other.violations)


def test_broken_flags_a_rollback_without_an_incident():
    world = broken_world()

    def revert(w: World) -> None:
        w.snap = replace(w.snap, images=dict(BASE.images))

    world.events[12] = revert
    assert any("rolled back" in v for v in sc.run_broken(world, TIMINGS).violations)


def test_broken_allows_responder_changes_while_an_incident_is_open():
    world = broken_world()
    world.events[12] = lambda w: (
        setattr(w, "incident", True),
        setattr(w, "snap", replace(w.snap, images=dict(BASE.images))),
    )
    assert sc.run_broken(world, TIMINGS).violations == []


def test_broken_allows_memory_commits_to_move_the_head_under_a_hold():
    def hold_and_record_an_outcome(world: World) -> None:
        hold("rollout: x")(world)
        world.snap = replace(world.snap, head="h9")

    result = sc.run_broken(World(events={6: hold_and_record_an_outcome}), TIMINGS)
    assert result.violations == []
    assert any("accepted as" in v for v in sc.run_broken(World(events={6: accept}), TIMINGS).violations)


def test_timings_reject_non_positive_values():
    with pytest.raises(ValueError, match="poll"):
        sc.ScenarioTimings(verdict=1, steady=1, poll=0)
    assert sc.ScenarioTimings.standard().verdict > sc.ScenarioTimings.standard().poll


def test_hold_reason_kind():
    assert sc.HoldRecord("r", "rollout: stalled").reason_kind == "rollout"
    assert sc.HoldRecord("r", "stalled").reason_kind == ""


def git(path: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True, text=True).stdout


def test_accepted_log_parses_real_git_trailers(tmp_path: Path):
    git(tmp_path, "init", "-q", "-b", "main")
    ident = ["-c", "user.name=t", "-c", "user.email=t@invalid"]
    git(tmp_path, *ident, "commit", "-q", "--allow-empty", "-m", "seed")
    git(
        tmp_path,
        *ident,
        "commit",
        "-q",
        "--allow-empty",
        "-m",
        "sdo(advance): upstream\n\nSDO-Actor: upstream\nSDO-Phase: advance\nSDO-Upstream: " + PUSHED,
    )
    commits = sc.parse_accepted_log(git(tmp_path, *sc.ACCEPTED_LOG_ARGS))
    assert len(commits) == 2
    assert commits[0].trailers == {"SDO-Actor": "upstream", "SDO-Phase": "advance", "SDO-Upstream": PUSHED}
    assert commits[1].trailers == {}
    assert sc.upstream_commit(commits, PUSHED) == commits[0]
    assert sc.upstream_commit(commits, "b" * 40) is None


def test_parse_holds_reads_the_codec_and_rejects_foreign_keys():
    entry = {"path": ".", "fingerprint": "f", "revision": PUSHED, "reason": "rollout: x", "at": "2026-11-06T00:00:00Z"}
    configmap = {"data": {"hold-0123456789abcdef": json.dumps(entry)}}
    assert sc.parse_holds(configmap) == [sc.HoldRecord(PUSHED, "rollout: x")]
    assert sc.parse_holds({}) == []
    with pytest.raises(ValueError, match="unexpected key"):
        sc.parse_holds({"data": {"other": "{}"}})
    with pytest.raises(ValueError, match="lacks"):
        sc.parse_holds({"data": {"hold-0123456789abcdef": json.dumps({"revision": PUSHED})}})


def pod(app: str, phase: str, image_id: str, restarts: int = 0) -> dict:
    return {
        "metadata": {"labels": {"app": app}},
        "status": {"phase": phase, "containerStatuses": [{"imageID": image_id, "restartCount": restarts}]},
    }


def test_pod_parsers():
    pods = {
        "items": [
            pod("backend", "Running", "sha256:1", 2),
            pod("backend", "Pending", "sha256:2"),
            pod("web", "Failed", ""),
        ]
    }
    assert sc.workload_digests(pods) == {"backend": "sha256:1"}
    assert sc.pod_failure_count(pods) == 3


def test_helm_onboarding_provides_the_plan_of_the_assembled_source(tmp_path: Path):
    prepare_workspace(tmp_path, "unused:v1", fresh=True, variant="helm")
    plan = provided_deployment_plan("helm", "sdo-toy")
    assert plan is not None
    source = plan["source"]
    assert (tmp_path / source["config"]["chart_path"] / "Chart.yaml").is_file()
    assert (tmp_path / plan["images"][0]["config"]["dockerfile_path"]).is_file()
    assert plan["expected_workload_ids"] == [
        f"Deployment/sdo-toy/{name}" for name in ("frontend", "backend", "catalog")
    ]
    values = (tmp_path / "chart" / "values.yaml").read_text()
    image = plan["images"][0]["image"]
    repository, _, tag = image.rpartition(":")
    assert f"repository: {repository}\n" in values
    assert f"tag: {tag}\n" in values
    assert "pullPolicy: IfNotPresent" in values
    assert image.startswith("sdo-benchmark-registry.sdo-toy-sdo.svc.cluster.local:5000/")
    flags = sc.helm_onboarding_arguments(plan)
    assert flags[0] == "--deployment-plan"
    decoded = json.loads(flags[1])
    assert decoded["source"]["renderer_id"] == "helm"
    assert decoded["source"]["config"]["@type"].endswith(".HelmRenderConfig")
    assert decoded["images"][0]["source_id"] == "buildkit"


def test_only_helm_and_kustomize_provide_a_plan():
    assert [provided_deployment_plan(v, "ns") is not None for v in ("raw", "kustomize", "helm", "compose")] == [
        False,
        True,
        True,
        False,
    ]


def test_kustomize_overlay_deploys_the_plans_registry_image(tmp_path: Path):
    prepare_workspace(tmp_path, "unused:v1", fresh=True, variant="kustomize")
    plan = provided_deployment_plan("kustomize", "sdo-toy")
    assert plan is not None
    overlay = tmp_path / plan["source"]["config"]["path"] / "kustomization.yaml"
    image = plan["images"][0]["image"]
    repository, _, tag = image.rpartition(":")
    assert f"newName: {repository}\n" in overlay.read_text()
    assert f"newTag: {tag}\n" in overlay.read_text()
    assert "imagePullPolicy: Never" not in (tmp_path / "deploy/base/backend.yaml").read_text()


def test_down_deletes_the_upstream_namespace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls: list[list[str]] = []

    def run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(up.subprocess, "run", run)
    assert up.delete_upstream(tmp_path / "kc", up.UpstreamLayout("sdo-toy")) is None
    assert calls[0][-6:-2] == ["delete", "namespace", "sdo-toy-upstream", "--ignore-not-found=true"]


def test_delete_upstream_reports_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    def run(argv, **kwargs):
        raise subprocess.CalledProcessError(1, argv, stderr="forbidden")

    monkeypatch.setattr(up.subprocess, "run", run)
    error = up.delete_upstream(tmp_path / "kc", up.UpstreamLayout("sdo-toy"))
    assert error is not None
    assert "sdo-toy-upstream" in error
    assert "forbidden" in error


def test_fastloop_down_deletes_the_upstream_of_a_toy_lane(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    lane = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="toy-lane",
        kubeconfig=tmp_path / "config",
        namespace="sdo-toy",
        application="toy",
        workspace=tmp_path / "application_workspace",
        sregym_dir=tmp_path / "sregym",
        private_tmp=None,
        app="toy",
    )
    lane.workspace.mkdir()
    lane.save()
    calls: list[list[str]] = []
    monkeypatch.setattr(
        up.subprocess, "run", lambda argv, **kw: calls.append(argv) or subprocess.CompletedProcess(argv, 0, "", "")
    )
    assert cli.main(["down", "--run-dir", str(tmp_path)]) == 0
    assert [c[-6:-2] for c in calls] == [["delete", "namespace", "sdo-toy-upstream", "--ignore-not-found=true"]]
