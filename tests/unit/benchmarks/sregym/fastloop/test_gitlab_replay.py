"""GitLab release replay against a fake lane: outcomes, stop rules, records, and the real Git push of a release."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tarfile
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

import pytest

from apps.gitlab import assemble
from benchmarks.sregym.fastloop import gitlab_replay as gr
from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.gitlab_releases import Release, load_releases
from benchmarks.sregym.fastloop.toy_scenarios import AcceptedCommit, HoldRecord, Snapshot
from benchmarks.sregym.fastloop.toy_upstream import UpstreamError, UpstreamLayout
from tests.unit.apps.test_gitlab_workspace import FILES
from tests.unit.benchmarks.sregym.fastloop.operability_fixtures import green_read

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from benchmarks.sregym.fastloop.toy_operability import OperabilityRead

RELEASES = load_releases()
TIMINGS = gr.ReplayTimings(verdict=100.0, owner_action=30.0, resolution=60.0, settle=10.0, poll=5.0)
DIGEST = "sha256:" + "d" * 64
ROUTED = "cause={cause} owner={owner} items={items} {kind}: detail"
BASE = Snapshot("h0", {}, {"web": "sha256:old", "gitaly": "sha256:old"}, 0, 0, 0)


def sha_of(version: str) -> str:
    return hashlib.sha1(version.encode()).hexdigest()


@dataclass
class Lane:
    """A fake lane: pushing a release schedules the events its ``script`` names, offset from the push time."""

    script: dict[str, dict[float, Callable[[Lane, str], None]]] = field(default_factory=dict)
    chart: str = "10.4.2"
    snap: Snapshot = BASE
    commits: list[AcceptedCommit] = field(default_factory=lambda: [AcceptedCommit("h0", {})])
    hold_records: list[HoldRecord] = field(default_factory=list)
    paths: dict[str, list[str]] = field(default_factory=dict)
    incident: bool = False
    report: OperabilityRead | None = field(default_factory=lambda: green_read())
    now: float = 0.0
    pushes: list[str] = field(default_factory=list)
    pending: list[tuple[float, Callable[[Lane, str], None], str]] = field(default_factory=list)
    count: int = 0
    #: When the controller state ConfigMap appears (fake clock); reading it before that fails like kubectl does.
    state_at: float = 0.0

    def controller_state_present(self) -> bool:
        return self.now >= self.state_at

    def _fire(self) -> None:
        due = [e for e in self.pending if e[0] <= self.now]
        for event in sorted(due, key=lambda e: e[0]):
            self.pending.remove(event)
            event[1](self, event[2])

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        self._fire()

    def push(self, name: str) -> str:
        self.pushes.append(name)
        self.pending += [(self.now + at, fn, name) for at, fn in self.script.get(name, {}).items()]
        return sha_of(name)

    def snapshot(self) -> Snapshot:
        return replace(self.snap, agent_tasks=self.count)

    def agent_task_count(self) -> int:
        return self.count

    def accepted_commits(self) -> list[AcceptedCommit]:
        return list(self.commits)

    def holds(self) -> list[HoldRecord]:
        return list(self.hold_records)

    def incident_open(self) -> bool:
        assert self.controller_state_present(), "sdo-controller-state is not written yet"
        return self.incident

    def operability(self) -> OperabilityRead | None:
        return self.report

    def changed_paths(self, sha: str) -> list[str]:
        return self.paths.get(sha, [])

    def accepted_chart_version(self) -> str:
        return self.chart


def advance(version: str, *, new_image: bool = True, digest: str = DIGEST) -> Callable[[Lane, str], None]:
    def apply(lane: Lane, name: str) -> None:
        trailers = {"SDO-Actor": "upstream", "SDO-Upstream": sha_of(name), "SDO-Operability": f"advance {digest}"}
        lane.commits.insert(0, AcceptedCommit("c" + version, trailers))
        if new_image:
            lane.snap = replace(lane.snap, digests={"web": "sha256:" + version, "gitaly": "sha256:" + version})

    return apply


def hold(cause: str, owner: str, kind: str, items: str = "O1") -> Callable[[Lane, str], None]:
    def apply(lane: Lane, name: str) -> None:
        reason = ROUTED.format(cause=cause, owner=owner, items=items, kind=kind)
        lane.hold_records.append(HoldRecord(sha_of(name), reason))

    return apply


def agent_commit(actor: str, paths: list[str], *, adopt: str | None = None) -> Callable[[Lane, str], None]:
    def apply(lane: Lane, name: str) -> None:
        lane.count += 1
        sha = f"{actor}-{len(lane.commits)}"
        lane.commits.insert(0, AcceptedCommit(sha, {"SDO-Actor": actor}))
        lane.paths[sha] = paths
        if adopt:
            advance(adopt)(lane, name)

    return apply


def open_incident(lane: Lane, name: str) -> None:
    lane.incident = True


def close_incident(lane: Lane, name: str) -> None:
    lane.incident = False


def all_advance() -> dict[str, dict[float, Callable[[Lane, str], None]]]:
    return {r.version: {10.0: advance(r.version)} for r in RELEASES.releases[1:]}


def replay(lane: Lane, limit: int | None = None) -> gr.ReplaySummary:
    return gr.run_replay(lane, RELEASES, TIMINGS, lane="fastloop-w0", limit=limit)


def test_every_release_advances_with_zero_agent_turns_and_a_digest() -> None:
    lane = Lane(script=all_advance())
    summary = replay(lane)
    assert lane.pushes == ["10.5.0", "10.5.1", "10.5.3", "10.5.4"]
    assert [r.outcome for r in summary.results] == ["advanced"] * 4
    assert all(r.agent_tasks == 0 and r.human_edits == 0 for r in summary.results)
    assert {r.operability_digest for r in summary.results} == {DIGEST}
    assert all(r.accepted_commit == "c" + r.version for r in summary.results)
    assert all(r.wall_seconds == 10.0 for r in summary.results)
    assert summary.baseline == "10.4.2"
    assert summary.passed
    assert summary.human_edits == 0


def test_the_lane_resumes_after_the_release_it_already_holds() -> None:
    lane = Lane(script=all_advance(), chart="10.5.1")
    assert replay(lane).planned == ["10.5.3", "10.5.4"]
    assert lane.pushes == ["10.5.3", "10.5.4"]
    assert replay(Lane(script=all_advance()), limit=2).planned == ["10.5.0", "10.5.1"]


def test_a_lane_at_the_last_release_has_nothing_to_replay() -> None:
    with pytest.raises(ValueError, match="nothing to replay"):
        replay(Lane(chart="10.5.4"))


def test_an_image_release_that_changes_no_running_image_is_a_violation_but_a_chart_only_release_is_not() -> None:
    script = all_advance()
    script["10.5.1"] = {10.0: advance("10.5.1", new_image=False)}
    script["10.5.4"] = {10.0: advance("10.5.4", new_image=False)}
    summary = replay(Lane(script=script))
    by_version = {r.version: r for r in summary.results}
    assert any("no workload runs a new one" in v for v in by_version["10.5.1"].violations)
    assert by_version["10.5.4"].violations == []
    assert by_version["10.5.4"].expected["images_changed"] == 0
    assert not summary.passed


def test_an_advance_that_spent_agent_tasks_breaks_the_common_case() -> None:
    script = all_advance()

    def spend(lane: Lane, name: str) -> None:
        lane.count += 2
        advance("10.5.1")(lane, name)

    script["10.5.1"] = {10.0: spend}
    result = {r.version: r for r in replay(Lane(script=script)).results}["10.5.1"]
    assert result.agent_tasks == 2
    assert any("2 agent task(s)" in v for v in result.violations)


def test_an_advance_without_an_operability_digest_is_a_violation() -> None:
    script = all_advance()
    script["10.5.0"] = {10.0: advance("10.5.0", digest="")}
    first = replay(Lane(script=script), limit=1).results[0]
    assert any("SDO-Operability" in v for v in first.violations)


def test_a_hold_nobody_owns_ends_the_replay_before_the_next_push() -> None:
    script = all_advance()
    script["10.5.1"] = {10.0: hold("scope", "human", "scope", "O6")}
    lane = Lane(script=script)
    summary = replay(lane)
    assert lane.pushes == ["10.5.0", "10.5.1"]
    last = summary.results[-1]
    assert last.outcome == "held"
    assert (last.hold.cause, last.hold.owner, last.hold.items, last.hold.kind) == ("scope", "human", ("O6",), "scope")
    assert "has no owner action (owner 'human' is not an agent role" in summary.stopped
    assert last.wall_seconds == 10.0  # stopped at the verdict: waiting cannot help a human-owned hold
    assert summary.stopped.startswith("10.5.1:")
    assert not summary.passed


def test_a_hold_without_routing_is_not_actionable() -> None:
    lane = Lane(
        script={"10.5.0": {10.0: lambda lane, name: lane.hold_records.append(HoldRecord(sha_of(name), "render: x"))}}
    )
    summary = replay(lane)
    assert summary.results[0].hold.owner == ""
    assert "no routing prefix" in summary.stopped
    assert lane.pushes == ["10.5.0"]


def test_a_hold_whose_agent_owner_never_acts_stops_after_the_owner_window() -> None:
    lane = Lane(script={"10.5.0": {10.0: hold("patch", "deployer", "patch_conflict")}})
    summary = replay(lane)
    assert lane.pushes == ["10.5.0"]
    assert "no owner action within 30s (owner deployer, cause patch)" in summary.stopped
    assert summary.results[0].outcome == "held"
    assert summary.results[0].wall_seconds == 40.0


def test_a_hold_repaired_by_an_sdo_patch_is_recorded_as_repaired_and_the_replay_continues() -> None:
    script = all_advance()
    script["10.5.0"] = {
        10.0: hold("patch", "deployer", "patch_conflict"),
        20.0: agent_commit("deployer", [".sdo/deployment/patches/values.patch"], adopt="10.5.0"),
    }
    lane = Lane(script=script)
    summary = replay(lane)
    first = summary.results[0]
    assert first.outcome == "repaired"
    assert first.hold is not None
    assert first.hold.owner == "deployer"
    assert first.agent_tasks == 1
    assert [(c.actor, c.paths) for c in first.repair_commits] == [
        ("deployer", (".sdo/deployment/patches/values.patch",))
    ]
    assert first.violations == []
    assert lane.pushes[-1] == "10.5.4"
    assert summary.passed


def test_a_repair_that_edits_outside_sdo_is_a_violation() -> None:
    script = {
        "10.5.0": {
            10.0: hold("patch", "deployer", "patch_conflict"),
            20.0: agent_commit("deployer", ["chart/values-kind.yaml"], adopt="10.5.0"),
        }
    }
    result = replay(Lane(script=script), limit=1).results[0]
    assert any("outside .sdo/" in v for v in result.violations)


def test_human_commits_in_the_accepted_history_are_counted() -> None:
    script = {
        "10.5.0": {
            10.0: hold("patch", "deployer", "patch_conflict"),
            15.0: agent_commit("human", [".sdo/x"], adopt="10.5.0"),
        }
    }
    summary = replay(Lane(script=script), limit=1)
    assert summary.results[0].human_edits == 1
    assert summary.human_edits == 1
    assert any("human edit" in v for v in summary.results[0].violations)


def test_an_incident_that_closes_without_adopting_the_release_is_recorded_and_the_next_release_follows() -> None:
    script = all_advance()
    script["10.5.1"] = {
        10.0: open_incident,
        30.0: agent_commit("responder", [".sdo/playbooks/p.go"]),
        40.0: close_incident,
    }
    lane = Lane(script=script)
    summary = replay(lane)
    second = summary.results[1]
    assert second.outcome == "incident"
    assert second.incident_opened
    assert second.agent_tasks == 1
    assert lane.pushes == ["10.5.0", "10.5.1", "10.5.3", "10.5.4"]


def test_an_incident_that_never_closes_stops_the_replay() -> None:
    lane = Lane(script={"10.5.0": {10.0: open_incident}})
    summary = replay(lane)
    assert summary.results[0].outcome == "incident"
    assert "did not finish within 60s" in summary.stopped
    assert lane.pushes == ["10.5.0"]


def test_no_verdict_within_the_bound_stops_with_a_timeout() -> None:
    lane = Lane()
    summary = replay(lane)
    assert summary.results[0].outcome == "timeout"
    assert "neither accepted, held nor an incident within 100s" in summary.stopped


def test_an_incident_in_the_quiet_window_after_an_advance_stops_the_replay() -> None:
    script = all_advance()
    script["10.5.0"] = {10.0: advance("10.5.0"), 14.0: open_incident}
    lane = Lane(script=script)
    summary = replay(lane)
    assert lane.pushes == ["10.5.0"]
    assert "quiet window" in summary.stopped


def test_a_lane_that_is_not_steady_is_never_pushed_to() -> None:
    for lane in (Lane(report=None), Lane(report=green_read({"O4": "fail"})), Lane(incident=True)):
        summary = replay(lane)
        assert lane.pushes == []
        assert summary.preflight
        assert summary.stopped.startswith("the lane is not steady")
        assert summary.results == []


def test_records_are_written_after_every_release(tmp_path: Path) -> None:
    script = all_advance()
    script["10.5.1"] = {10.0: hold("scope", "human", "scope", "O6")}
    lane = Lane(script=script)
    seen: list[int] = []

    def write(summary: gr.ReplaySummary, record: gr.ReleaseRecord | None) -> None:
        gr.write_records(tmp_path, summary, record)
        seen.append(len(json.loads((tmp_path / gr.SUMMARY_FILE).read_text())["results"]))

    summary = gr.run_replay(lane, RELEASES, TIMINGS, lane="fastloop-w0", write=write)
    assert seen == [0, 1, 2]
    document = json.loads((tmp_path / gr.SUMMARY_FILE).read_text())
    assert document["passed"] is False
    assert document["stopped"] == summary.stopped
    assert (document["human_edits"], document["agent_tasks"]) == (0, 0)
    one = json.loads(gr.release_file(tmp_path, "10.5.1").read_text())
    assert one["outcome"] == "held"
    assert one["hold"]["owner"] == "human"
    assert one["expected"]["images_changed"] == 8
    assert json.loads(gr.release_file(tmp_path, "10.5.0").read_text())["operability_digest"] == DIGEST
    assert not gr.release_file(tmp_path, "10.5.3").exists()


def test_a_designer_follow_up_hold_on_an_accepted_advance_is_recorded_not_a_stop() -> None:
    def advance_then_owe(lane: Lane, name: str) -> None:
        advance("10.5.0")(lane, name)
        hold("coverage", "designer", "designer_refresh", "O3")(lane, name)

    summary = replay(Lane(script={**all_advance(), "10.5.0": {10.0: advance_then_owe}}), limit=1)
    result = summary.results[0]
    assert result.outcome == "advanced"
    assert result.hold is not None
    assert (result.hold.owner, result.hold.kind) == ("designer", "designer_refresh")
    assert summary.passed


# ---- pure helpers --------------------------------------------------------------------------------------------------


def test_chart_version_is_the_top_level_version_not_a_dependency() -> None:
    text = "apiVersion: v2\nname: gitlab\nversion: 10.5.1\ndependencies:\n  - name: x\n    version: 0.1.0\n"
    assert gr.parse_chart_version(text) == "10.5.1"
    with pytest.raises(ValueError, match="no version"):
        gr.parse_chart_version("name: x\n")


def test_operability_digest_reads_the_trailer() -> None:
    assert gr.operability_digest(AcceptedCommit("a", {"SDO-Operability": f"advance {DIGEST}"})) == DIGEST
    assert gr.operability_digest(AcceptedCommit("a", {})) == ""


def test_timings_must_be_positive() -> None:
    with pytest.raises(ValueError, match="poll"):
        gr.ReplayTimings(1, 1, 1, 1, 0)
    assert gr.ReplayTimings.standard().verdict > gr.ReplayTimings.standard().owner_action


# ---- the Git push of a release, on local repositories ----------------------------------------------------------------


def _archive(directory: Path, version: str, *, extra: dict[str, str] | None = None) -> Path:
    archive = directory / f"gitlab-{version}.tgz"
    files = {name: text.replace("version: 10.5.4", f"version: {version}") for name, text in FILES.items()}
    with tarfile.open(archive, "w:gz") as tar:
        for name, text in (files | (extra or {})).items():
            data = text.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return archive


def _release(archive: Path, version: str) -> Release:
    real = RELEASES.get(version)
    return replace(real, sha256=hashlib.sha256(archive.read_bytes()).hexdigest())


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def _seeded_remote(tmp_path: Path) -> tuple[str, UpstreamLayout]:
    old = _archive(tmp_path, "10.4.2", extra={"gitlab/templates/removed-upstream.yaml": "kind: ConfigMap\n"})
    pins = RELEASES.get("10.4.2").pins(assemble.load_pins())
    pins["gitlab_chart"]["sha256"] = hashlib.sha256(old.read_bytes()).hexdigest()
    workspace = tmp_path / "ws"
    from benchmarks.sregym.fastloop.gitlab_lane import prepare_workspace

    prepare_workspace(workspace, archive=old, pins=pins)
    remote = tmp_path / "remote.git"
    _git("init", "--bare", "-q", "-b", "main", str(remote))
    _git("-C", str(workspace), "push", "-q", str(remote), "HEAD:refs/heads/main")
    return str(remote), UpstreamLayout("gitlab")


def test_push_release_replaces_the_vendored_chart_and_bumps_the_dependency(tmp_path: Path) -> None:
    remote, layout = _seeded_remote(tmp_path)
    archive = _archive(tmp_path, "10.5.0")
    release = _release(archive, "10.5.0")
    sha = gr.push_release(release, archive, remote, layout)
    assert _git("--git-dir", remote, "rev-parse", "refs/heads/main").strip() == sha
    chart_yaml = _git("--git-dir", remote, "show", f"{sha}:chart/Chart.yaml")
    assert "name: gitlab\n    version: 10.5.0" in chart_yaml
    assert (
        gr.parse_chart_version(_git("--git-dir", remote, "show", f"{sha}:chart/charts/gitlab/Chart.yaml")) == "10.5.0"
    )
    tree = _git("--git-dir", remote, "ls-tree", "-r", "--name-only", sha).split()
    assert "chart/charts/gitlab/templates/removed-upstream.yaml" not in tree
    assert "chart/values-kind.yaml" in tree
    assert _git("--git-dir", remote, "log", "-1", "--format=%an %s", sha).strip() == (
        "gitlab-replay chore(deps): bump the gitlab chart to 10.5.0"
    )


def test_a_second_release_stacks_on_the_first(tmp_path: Path) -> None:
    remote, layout = _seeded_remote(tmp_path)
    for version in ("10.5.0", "10.5.1"):
        archive = _archive(tmp_path, version)
        gr.push_release(_release(archive, version), archive, remote, layout)
    assert _git("--git-dir", remote, "rev-list", "--count", "refs/heads/main").strip() == "3"
    assert "version: 10.5.1" in _git("--git-dir", remote, "show", "refs/heads/main:chart/Chart.yaml")


def test_push_release_refuses_a_remote_that_is_not_a_gitlab_lane_and_changes_nothing(tmp_path: Path) -> None:
    remote = tmp_path / "remote.git"
    seed = tmp_path / "seed"
    _git("init", "-q", "-b", "main", str(seed))
    (seed / "README").write_text("x")
    _git("-C", str(seed), "add", ".")
    _git("-C", str(seed), "-c", "user.name=a", "-c", "user.email=a@b", "commit", "-qm", "x")
    _git("init", "--bare", "-q", "-b", "main", str(remote))
    _git("-C", str(seed), "push", "-q", str(remote), "HEAD:refs/heads/main")
    before = _git("--git-dir", str(remote), "rev-parse", "main")
    archive = _archive(tmp_path, "10.5.0")
    with pytest.raises(UpstreamError, match="is this a gitlab lane"):
        gr.push_release(_release(archive, "10.5.0"), archive, str(remote), UpstreamLayout("gitlab"))
    assert _git("--git-dir", str(remote), "rev-parse", "main") == before


def test_a_tampered_archive_is_refused_and_a_verified_one_is_cached(tmp_path: Path) -> None:
    data = b"archive"
    release = replace(RELEASES.get("10.5.0"), sha256=hashlib.sha256(data).hexdigest())
    downloads: list[str] = []

    def opener(url: str, **_: object) -> io.BytesIO:
        downloads.append(url)
        return io.BytesIO(data)

    class Response(io.BytesIO):
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: object) -> None:
            return None

    cache = tmp_path / "cache"
    path = gr.fetch_archive(release, cache, lambda url, **k: (downloads.append(url), Response(data))[1])
    assert path.read_bytes() == data
    gr.fetch_archive(release, cache, opener)
    assert downloads == [release.url]
    bad = replace(release, sha256="0" * 64)
    with pytest.raises(ValueError, match="does not match"):
        gr.fetch_archive(bad, tmp_path / "other", lambda url, **k: Response(data))
    assert not list((tmp_path / "other").glob("*.tgz"))


def test_the_command_is_registered_under_gitlab() -> None:
    args = build_parser().parse_args(
        ["gitlab", "replay", "--run-dir", "/tmp/x", "--limit", "2", "--settle-seconds", "5"]
    )
    assert (args.limit, args.settle_seconds, args.handler) == (2, 5.0, gr.replay)


def test_the_replay_waits_for_the_controller_state_instead_of_crashing() -> None:
    lane = Lane(state_at=40.0, script={"10.5.0": {10.0: advance("10.5.0")}})
    summary = gr.run_replay(lane, RELEASES, TIMINGS, lane="lane", limit=1)
    assert summary.preflight == []
    assert summary.results[0].outcome == "advanced"
    assert lane.pushes == ["10.5.0"]


def test_a_controller_state_that_never_appears_is_a_preflight_violation_and_nothing_is_pushed() -> None:
    lane = Lane(state_at=10**9)
    timings = replace(TIMINGS, controller_state=30.0)
    summary = gr.run_replay(lane, RELEASES, timings, lane="lane", limit=1)
    assert any("sdo-controller-state" in v and "30s" in v for v in summary.preflight)
    assert lane.pushes == []


def test_pod_failures_are_never_negative_when_pods_are_replaced_during_the_advance() -> None:
    def replace_pods(lane: Lane, name: str) -> None:
        advance(name)(lane, name)
        lane.snap = replace(lane.snap, pod_failures=0)  # the restarted pods were deleted by the rollout

    lane = Lane(snap=replace(BASE, pod_failures=2), script={"10.5.0": {10.0: replace_pods}})
    assert replay(lane, 1).results[0].pod_failures == 0


def test_the_operability_block_never_passes_off_the_pre_advance_report_as_the_outcome() -> None:
    lane = Lane()  # no verdict: the report on the ConfigMap is still the one for the previous accepted commit
    record = replay(lane, 1).results[0]
    assert record.outcome == "timeout"
    assert record.operability == {"stale": True, "accepted_commit": "h0", "reason": "the release was not adopted"}


def test_an_advanced_release_records_the_report_only_when_it_is_for_the_adopted_commit() -> None:
    def advance_and_refresh(lane: Lane, name: str) -> None:
        advance(name)(lane, name)
        lane.report = green_read(commit="c10.5.0")

    lane = Lane(script={"10.5.0": {10.0: advance_and_refresh}})
    block = replay(lane, 1).results[0].operability
    assert block is not None
    assert "stale" not in block
    assert block["accepted_commit"] == "c10.5.0"
    lane = Lane(script={"10.5.0": {10.0: advance("10.5.0")}})  # ConfigMap still for h0
    block = replay(lane, 1).results[0].operability
    assert block is not None
    assert block["stale"] is True
    assert "items" not in block


def test_an_advance_report_is_current_when_its_digest_is_the_adopted_commits_trailer() -> None:
    """An advance report's subject is the accepted head the merge was made on (the adopted commit's parent), so the
    adopted commit is identified by the digest on its SDO-Operability trailer."""

    def advance_and_expose(lane: Lane, name: str) -> None:
        advance(name)(lane, name)
        lane.report = replace(green_read(commit="h0", stage="advance"), digest=DIGEST.removeprefix("sha256:"))

    lane = Lane(script={"10.5.0": {10.0: advance_and_expose}})
    block = replay(lane, 1).results[0].operability
    assert block is not None
    assert "stale" not in block
    assert block["stage"] == "advance"
    assert block["digest"] == DIGEST.removeprefix("sha256:")  # the ConfigMap form: bare hex; the trailer has sha256:
    other = Lane(
        script={
            "10.5.0": {
                10.0: lambda lane, name: (
                    advance(name)(lane, name),
                    setattr(lane, "report", replace(green_read(commit="h0", stage="advance"), digest="f" * 64)),
                )
            }
        }
    )
    stale = replay(other, 1).results[0].operability
    assert stale is not None
    assert stale["stale"] is True


def test_a_chart_only_release_that_rolls_pods_with_identical_images_lists_them_as_changed() -> None:
    """A chart version label sits in the pod template: the pods are replaced (a new template revision) while every
    image stays, and the report must say those workloads changed."""

    def label_only(lane: Lane, name: str) -> None:
        advance(name, new_image=False)(lane, name)
        lane.snap = replace(lane.snap, templates={"web": "rev-b", "gitaly": "rev-a"})

    lane = Lane(
        snap=replace(BASE, templates={"web": "rev-a", "gitaly": "rev-a"}), script={"10.5.0": {10.0: label_only}}
    )
    assert replay(lane, 1).results[0].changed_workloads == ["web"]
