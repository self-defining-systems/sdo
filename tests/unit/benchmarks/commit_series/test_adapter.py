"""The workload adapter: a series run prints the commit's record; every other call is the wrapped workload's."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.commit_series import adapter
from benchmarks.sregym.fastloop.toy_live import ClusterReadTimeout
from tests.unit.benchmarks.commit_series.test_commit_outcomes import Lane

if TYPE_CHECKING:
    from pathlib import Path

SPEC = {
    "name": "demo",
    "description": "three synthetic commits",
    "source": "synthetic",
    "upstream": {"repo": "/unused", "base": "b0", "commits": ["c1", "c2", "c3"]},
    "schedule": [0, 30, 90],
}


def instances(tmp_path: Path) -> Path:
    (tmp_path / "demo.json").write_text(json.dumps(SPEC))
    path = tmp_path / "instances.json"
    path.write_text(
        json.dumps({"ids": ["demo"], "series": {"demo": {"spec": "demo.json", "verdict_s": 60, "steady_s": 5}}})
    )
    return path


def env(tmp_path: Path, seed: int, instance: str = "demo", **more: str) -> dict[str, str]:
    args = [
        "--instance-file",
        str(instances(tmp_path)),
        "--instance",
        instance,
        "--seed",
        str(seed),
        "--operator",
        f"exec:sdo-x-r{seed}",
    ]
    return {"WORKLOAD_ARGS": json.dumps(args), "BENCH_LANE_DIR": str(tmp_path), **more}


def test_a_series_run_prints_the_commits_record_and_waits_for_its_schedule(tmp_path: Path) -> None:
    lane = Lane("accepted")
    out = io.StringIO()
    adapter.run(env(tmp_path, 1), out, ops=lane)  # type: ignore[arg-type]
    first = json.loads(out.getvalue().splitlines()[-1])
    assert (first["index"], first["outcome"], first["operator"]) == (1, "accepted", "exec:sdo-x-r1")
    out = io.StringIO()
    before = lane.t
    adapter.run(env(tmp_path, 2), out, ops=lane)  # type: ignore[arg-type]
    assert lane.t - before >= 29  # commit 2 is scheduled 30 s after the series started
    assert json.loads(out.getvalue().splitlines()[-1])["index"] == 2


def test_a_call_that_names_no_series_is_delegated_to_the_wrapped_workload(tmp_path: Path) -> None:
    marker = tmp_path / "ran"
    builtin = json.dumps(["sh", "-c", f"echo $0 > {marker}", "wrapped"])
    code = adapter.main(
        ["run"], {**env(tmp_path, 1, instance="some-fault"), "WORKLOAD_ACTION": "run", "WORKLOAD_BUILTIN": builtin}
    )
    assert code == 0
    assert marker.read_text().strip() == "wrapped"


def test_without_a_wrapped_workload_a_delegated_verb_is_unsupported(tmp_path: Path) -> None:
    out = io.StringIO()
    assert adapter.main(["gate"], {"WORKLOAD_ACTION": "check", "WORKLOAD_ARGS": "[]"}, out) == 64


def test_grade_writes_a_table_of_commits_for_series_records(tmp_path: Path) -> None:
    rows = [
        {"series": "demo", "index": i, "commit": f"c{i}" * 4, "outcome": o, "valid": True, "operator": f"exec:x-r{i}"}
        for i, o in ((2, "held"), (1, "accepted"))
    ]
    (tmp_path / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    assert adapter.grade({"WORKLOAD_RESULTS": str(tmp_path)}, io.StringIO()) == 0
    report = (tmp_path / "report.md").read_text().splitlines()
    assert report[2].startswith("| demo | 1 |")
    assert "held" in report[3]


def test_a_delegated_verb_goes_through_the_fuzzops_workload_so_a_lane_provider_gets_its_environment(
    tmp_path: Path,
) -> None:
    """The wrapped workload is fuzzops' python workload, not the bare fuzzops binary: on an EKS lane it names the
    entry address and puts the docker/kind shims on the path, which the pack's scripts need."""
    marker = tmp_path / "entry"
    builtin = json.dumps(["sh", "-c", f'echo "$FUZZOPS_ENTRY_HOST" > {marker}'])
    code = adapter.main(
        ["app-up"],
        {"WORKLOAD_ACTION": "up", "WORKLOAD_ARGS": "[]", "WORKLOAD_BUILTIN": builtin, "BENCH_ENTRY_HOST": "10.1.2.3"},
    )
    assert code == 0
    assert marker.read_text().strip() == "10.1.2.3"


def test_a_fault_after_a_commit_runs_through_the_fuzzops_workload_too(tmp_path: Path) -> None:
    marker = tmp_path / "entry"
    path = instances(tmp_path)
    path.write_text(json.dumps({**json.loads(path.read_text()), "fault_instances": "faults.json"}))
    entry = adapter.Entry.find(path, "demo")
    assert entry is not None
    builtin = ["sh", "-c", f'echo "$FUZZOPS_ENTRY_HOST" > {marker}; echo \'{{"valid": true}}\'', "wrapped"]
    env = {"WORKLOAD_ACTION": "run", "BENCH_ENTRY_HOST": "10.1.2.3", "WORKLOAD_BUILTIN": json.dumps(builtin)}
    record = adapter.inject_fault(builtin, entry, "fault-x", env)
    assert record == {"valid": True}
    assert marker.read_text().strip() == "10.1.2.3"


def test_a_series_whose_fault_file_is_missing_is_refused_before_anything_is_pushed(tmp_path: Path) -> None:
    """The lane platform stores the instances file alone (by content), so a fault file named relative to it is not
    there at run time. That used to surface only after the commit was pushed and deployed, as a fault run with no
    record that broke the rest of the series; the position must refuse at once, naming the file."""
    run_env = env(tmp_path, 1)
    (tmp_path / "demo.json").write_text(json.dumps({**SPEC, "faults": {"1": "fault-x"}}))
    path = tmp_path / "instances.json"
    path.write_text(json.dumps({**json.loads(path.read_text()), "fault_instances": "faults.json"}))
    lane = Lane("accepted")
    out = io.StringIO()
    adapter.run(run_env, out, ops=lane)  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert "faults.json" in record["invalid_reason"]
    assert lane.pushed == ""  # nothing was pushed


def test_a_series_fault_the_fault_file_does_not_list_is_refused_before_anything_is_pushed(tmp_path: Path) -> None:
    run_env = env(tmp_path, 1)
    (tmp_path / "demo.json").write_text(json.dumps({**SPEC, "faults": {"2": "fault-y"}}))
    (tmp_path / "faults.json").write_text(json.dumps({"ids": ["fault-x"], "instances": []}))
    path = tmp_path / "instances.json"
    path.write_text(json.dumps({**json.loads(path.read_text()), "fault_instances": str(tmp_path / "faults.json")}))
    lane = Lane("accepted")
    out = io.StringIO()
    adapter.run(run_env, out, ops=lane)  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert "fault-y" in record["invalid_reason"]
    assert lane.pushed == ""  # nothing was pushed


def test_the_safety_check_after_a_series_run_answers_inactive_for_the_lane_platforms_flags(tmp_path: Path) -> None:
    """The platform asks `run active` and `run undo` with `--file/--id`, not the run's `--instance-file/--instance`;
    a series has no fault to undo, and passing the call on made the lane go down with `fault still active`."""
    args = ["--file", str(instances(tmp_path)), "--id", "demo", "--pack", "/p", "--namespace", "fg1"]
    for action in ("active", "undo"):
        out = io.StringIO()
        code = adapter.main(["run"], {"WORKLOAD_ACTION": action, "WORKLOAD_ARGS": json.dumps(args)}, out)
        assert code == 0
        assert json.loads(out.getvalue()) == {"active": False}


def test_a_position_tells_settle_it_waited_for_its_own_incident(tmp_path: Path) -> None:
    """The record is printed after the position's own bounded incident wait: settle then waits only for work in flight
    (the subject hook's ``idle`` reads this file; the run boundary removes it)."""
    out = io.StringIO()
    adapter.run(env(tmp_path, 1), out, ops=Lane("incident"))  # type: ignore[arg-type]
    assert json.loads(out.getvalue().splitlines()[-1])["incident_opened"] is True
    assert (tmp_path / "settle-work-only.json").is_file()


class OwnedOps(Lane):
    """A lane the adapter would build for itself: it also answers the start checks."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("accepted")
        self.problems = problems
        self.checked = 0

    def start_problems(self, env: dict[str, str], lane_dir: Path) -> list[str]:
        self.checked += 1
        return self.problems


def test_the_first_position_proves_the_lane_equals_a_fresh_install_and_later_ones_do_not(tmp_path: Path) -> None:
    lane = OwnedOps([])
    out = io.StringIO()
    adapter.run(env(tmp_path, 1), out, ops=lane)  # type: ignore[arg-type]
    adapter.run(env(tmp_path, 2), out, ops=lane)  # type: ignore[arg-type]
    assert lane.checked == 0  # an injected lane is the test's own; see below for the one the adapter builds


def test_a_lane_that_is_not_at_the_series_start_is_an_invalid_run_not_a_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lane = OwnedOps(["Deployment/web runs web:v2, the base release has web:v1"])
    monkeypatch.setattr(adapter, "LaneOps", lambda *a, **k: lane)
    out = io.StringIO()
    assert adapter.run({**env(tmp_path, 1), "KUBECONFIG": "/k"}, out) == 0
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert "not at the series start" in record["invalid_reason"]
    assert "web:v2" in record["invalid_reason"]
    assert lane.pushed == "", "nothing was pushed to a lane that is not at the start"
    out = io.StringIO()
    adapter.run({**env(tmp_path, 2), "KUBECONFIG": "/k"}, out)
    assert lane.checked == 1  # position 2 does not ask: the series is under way


def test_position_one_starts_the_series_clock_again(tmp_path: Path) -> None:
    """A lane reset in place runs the series again: its schedule counts from the new first position."""
    adapter.wall_start(tmp_path, "demo", 100.0)
    assert adapter.wall_start(tmp_path, "demo", 500.0) == 100.0  # a later position
    assert adapter.wall_start(tmp_path, "demo", 900.0, first=True) == 900.0
    assert adapter.wall_start(tmp_path, "demo", 950.0) == 900.0


def test_a_cluster_read_that_times_out_is_a_harness_failure_not_a_lost_lane(tmp_path: Path) -> None:
    """On an overloaded manager host the first ``kubectl get`` timed out after 30 s, the uncaught
    ``TimeoutExpired`` killed the adapter and the lanes platform recorded 'lane lost' for position 1 and 'sequence
    broken' for the rest. The failure is the harness's: the position is invalid, says so, and carries the marker."""

    class Slow(Lane):
        def accepted_commits(self) -> list:  # type: ignore[type-arg]
            raise ClusterReadTimeout("kubectl get pods timed out 3 times after 30 s")

    out = io.StringIO()
    assert adapter.run(env(tmp_path, 1), out, ops=Slow("accepted")) == 0  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert record["invalid_reason"].startswith("harness failure: "), record
    assert record["harness_failure"] is True
    assert "timed out 3 times" in record["invalid_reason"]


def test_a_kubectl_that_times_out_is_retried_before_it_is_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from benchmarks.sregym.fastloop import toy_live

    attempts: list[int] = []

    def flaky(cmd: list[str], **kw: object) -> subprocess.CompletedProcess[str]:
        attempts.append(1)
        if len(attempts) < 3:
            raise subprocess.TimeoutExpired(cmd, 30)
        return subprocess.CompletedProcess(cmd, 0, "ok", "")

    monkeypatch.setattr(toy_live.subprocess, "run", flaky)
    ops = toy_live.KubectlScenarioOps.__new__(toy_live.KubectlScenarioOps)
    ops.environment = type("E", (), {"kubeconfig": "/k"})()  # type: ignore[assignment]
    ops.sleep = lambda seconds: None  # type: ignore[method-assign]
    assert ops._kubectl("ns", "get", "pods").stdout == "ok"
    assert len(attempts) == 3
    attempts.clear()
    monkeypatch.setattr(
        toy_live.subprocess, "run", lambda cmd, **kw: (_ for _ in ()).throw(subprocess.TimeoutExpired(cmd, 30))
    )
    with pytest.raises(toy_live.ClusterReadTimeout, match="timed out 3 times"):
        ops._kubectl("ns", "get", "pods")


def test_the_observation_clock_is_the_newest_detector_evaluation() -> None:
    """The quiet wait runs on the controller's newest evaluation in its persisted state (Go nanosecond times)."""
    from benchmarks.commit_series.adapter import newest_evaluation, rfc3339_seconds

    state = {
        "history": [
            {"detector_id": "a", "evaluated_at": "2026-11-23T06:24:12.037787019Z"},
            {"detector_id": "b", "evaluated_at": "2026-11-23T06:24:18.5Z"},
            {"detector_id": "c", "evaluated_at": "2026-11-23T06:24:01Z"},
        ]
    }
    assert newest_evaluation(state) == rfc3339_seconds("2026-11-23T06:24:18.5Z")
    assert rfc3339_seconds("2026-11-23T06:24:18.5Z") - rfc3339_seconds(
        "2026-11-23T06:24:12.037787019Z"
    ) == pytest.approx(6.462212981)
    assert newest_evaluation({}) is None
    with pytest.raises(ValueError, match="not an RFC 3339 time"):
        rfc3339_seconds("yesterday")


#: Commit 1, a fault, commit 2, commit 3: position 2 is the fault, position 3 pushes commit 2.
EVENTS = [{"commit": 1}, {"fault": "fault-x"}, {"commit": 2}, {"commit": 3}]
FAULT_RECORD = {
    "valid": True,
    "fault": "fault-x",
    "recovered_in_window": True,
    "window": {"BadSeconds": 12},
    "last_bad_second": 40,
    "lifecycle": {"inject_at": "2026-11-24T00:00:00Z"},
}


def fault_series(tmp_path: Path, events: list[dict[str, object]] = EVENTS) -> Path:
    """A series with fault events and an absolute fault file that lists ``fault-x``; returns the instances file."""
    path = instances(tmp_path)
    (tmp_path / "demo.json").write_text(json.dumps({**SPEC, "events": events}))
    (tmp_path / "faults.json").write_text(json.dumps({"ids": ["fault-x"], "instances": []}))
    path.write_text(json.dumps({**json.loads(path.read_text()), "fault_instances": str(tmp_path / "faults.json")}))
    return path


def wrapped(tmp_path: Path, output: str, *flags: str) -> str:
    """A wrapped workload command line that records its arguments and prints ``output``."""
    script = f"echo \"$@\" > {tmp_path / 'wrapped-args'}; echo '{output}'"
    return json.dumps(["sh", "-c", script, "wrapped", *flags])


def series_env(tmp_path: Path, position: int) -> dict[str, str]:
    """A run of ``position`` for the fault series, built after ``fault_series`` wrote its files."""
    run_env = env(tmp_path, position)
    builtin = wrapped(tmp_path, json.dumps(FAULT_RECORD), "--instance-file", "series.json", "--instance", "demo")
    return {**run_env, "WORKLOAD_ACTION": "run", "WORKLOAD_BUILTIN": builtin}


def test_a_fault_event_prints_the_fault_runs_record_as_the_positions_record(tmp_path: Path) -> None:
    """A fault event is its own position: the wrapped workload's run for that fault is the run, with no commit pushed,
    and settle is not told the position waited for its own incident (it waits for SDO to be idle, then collect
    resets the incidents, as after any fault run)."""
    run_env = series_env(tmp_path, 2)
    fault_series(tmp_path)
    lane = Lane("accepted")
    out = io.StringIO()
    assert adapter.run(run_env, out, ops=lane) == 0  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record == {
        **FAULT_RECORD,
        "series": "demo",
        "index": 2,
        "event": "fault",
        "fault_id": "fault-x",
        "after_commit": 1,
    }
    assert lane.pushed == ""
    assert not (tmp_path / "settle-work-only.json").exists()
    args = (tmp_path / "wrapped-args").read_text().split()
    assert args == ["--instance-file", str(tmp_path / "faults.json"), "--instance", "fault-x"]


def test_a_commit_event_after_a_fault_pushes_its_commit_and_records_both_numbers(tmp_path: Path) -> None:
    first_env, run_env = series_env(tmp_path, 1), series_env(tmp_path, 3)
    fault_series(tmp_path)
    lane = Lane("accepted")
    out = io.StringIO()
    adapter.run(first_env, io.StringIO(), ops=lane)  # type: ignore[arg-type]
    before = lane.t
    adapter.run(run_env, out, ops=lane)  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert (record["index"], record["event"], record["commit_index"], record["commit"]) == (3, "commit", 2, "c2")
    assert record["outcome"] == "accepted"
    assert "fault" not in record
    assert lane.pushed == "sha2"
    assert lane.t - before >= 29  # commit 2's schedule offset, not position 3's
    assert (tmp_path / "settle-work-only.json").is_file()


def test_a_plain_series_position_is_a_commit_event(tmp_path: Path) -> None:
    out = io.StringIO()
    adapter.run(env(tmp_path, 2), out, ops=Lane("accepted"))  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert (record["index"], record["event"], record["commit_index"]) == (2, "commit", 2)


def test_a_fault_event_the_fault_file_does_not_list_is_refused_before_anything_is_pushed(tmp_path: Path) -> None:
    run_env = env(tmp_path, 1)
    fault_series(tmp_path, [{"commit": 1}, {"fault": "fault-z"}, {"commit": 2}, {"commit": 3}])
    lane = Lane("accepted")
    out = io.StringIO()
    adapter.run(run_env, out, ops=lane)  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert "fault-z" in record["invalid_reason"]
    assert lane.pushed == ""


def test_a_position_past_the_last_event_is_an_invalid_run(tmp_path: Path) -> None:
    out = io.StringIO()
    adapter.run(env(tmp_path, 4), out, ops=Lane("accepted"))  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is False
    assert "position 4" in record["invalid_reason"]


def test_grade_writes_one_row_per_event(tmp_path: Path) -> None:
    rows = [
        {"series": "demo", "index": 1, "event": "commit", "commit_index": 1, "commit": "c1" * 6, "outcome": "accepted"},
        {**FAULT_RECORD, "series": "demo", "index": 2, "event": "fault", "fault_id": "fault-x", "after_commit": 1},
        {"series": "demo", "index": 3, "event": "commit", "commit_index": 2, "commit": "c2" * 6, "outcome": "held"},
    ]
    (tmp_path / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in reversed(rows)) + "\n")
    assert adapter.grade({"WORKLOAD_RESULTS": str(tmp_path)}, io.StringIO()) == 0
    report = (tmp_path / "report.md").read_text().splitlines()
    assert report[2].startswith("| demo | 1 | commit 1 | c1c1c1c1c1c1 | accepted |")
    assert report[3].startswith("| demo | 2 | fault fault-x |")
    assert "| True | True | 12 | 40 |" in report[3]
    assert report[4].startswith("| demo | 3 | commit 2 | c2c2c2c2c2c2 | held |")


def call_safety(tmp_path: Path, action: str, answer: str, capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    args = ["--file", str(tmp_path / "instances.json"), "--id", "demo", "--pack", "/p", "--namespace", "fg1"]
    builtin = wrapped(tmp_path, answer, "--file", "series.json", "--id", "demo", "--pack", "/p")
    out = io.StringIO()
    call_env = {
        "WORKLOAD_ACTION": action,
        "WORKLOAD_ARGS": json.dumps(args),
        "WORKLOAD_BUILTIN": builtin,
        "BENCH_LANE_DIR": str(tmp_path),
    }
    assert adapter.main(["run"], call_env, out) == 0
    lines = [x for x in (out.getvalue() + capsys.readouterr().out).splitlines() if x.strip()]
    return json.loads(lines[-1])


def test_the_safety_check_after_a_fault_position_asks_the_wrapped_workload_about_that_fault(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The platform does not pass the position to ``run active`` / ``run undo``: the run leaves its event in the lane
    directory, so a fault SDO did not fix is found and undone before the next push."""
    run_env = series_env(tmp_path, 2)
    fault_series(tmp_path)
    adapter.run(run_env, io.StringIO(), ops=Lane("accepted"))  # type: ignore[arg-type]
    for action in ("active", "undo"):
        (tmp_path / "wrapped-args").unlink(missing_ok=True)
        assert call_safety(tmp_path, action, "active=true", capsys) == {"active": True}
        args = (tmp_path / "wrapped-args").read_text().split()
        assert args[:4] == ["--file", str(tmp_path / "faults.json"), "--id", "fault-x"]


def test_the_safety_check_after_a_commit_position_answers_inactive_without_the_wrapped_workload(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fault_env, run_env = series_env(tmp_path, 2), series_env(tmp_path, 3)
    fault_series(tmp_path)
    adapter.run(fault_env, io.StringIO(), ops=Lane("accepted"))  # type: ignore[arg-type]
    adapter.run(run_env, io.StringIO(), ops=Lane("accepted"))  # type: ignore[arg-type]
    (tmp_path / "wrapped-args").unlink(missing_ok=True)
    assert call_safety(tmp_path, "active", "active=true", capsys) == {"active": False}
    assert not (tmp_path / "wrapped-args").exists()


def held_then(
    tmp_path: Path, position: int, spec: dict[str, object] | None = None, fate: str = "held"
) -> dict[str, object]:
    """Position 1's release ends ``fate``, then ``position`` runs; its record."""
    first_env, run_env = series_env(tmp_path, 1), series_env(tmp_path, position)
    path = fault_series(tmp_path)
    (tmp_path / "demo.json").write_text(json.dumps({**SPEC, "events": EVENTS, **(spec or {})}))
    assert path.exists()
    adapter.run(first_env, io.StringIO(), ops=Lane(fate))  # type: ignore[arg-type]
    (tmp_path / "wrapped-args").unlink(missing_ok=True)
    out = io.StringIO()
    adapter.run(run_env, out, ops=Lane("accepted"))  # type: ignore[arg-type]
    return json.loads(out.getvalue().splitlines()[-1])


def test_a_held_release_is_labelled_on_its_own_record(tmp_path: Path) -> None:
    first_env = series_env(tmp_path, 1)
    fault_series(tmp_path)
    out = io.StringIO()
    adapter.run(first_env, out, ops=Lane("held"))  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert (record["outcome"], record["release_held"]["commit_index"]) == ("held", 1)
    assert record["release_held"]["reason"]


def test_a_fault_is_not_run_after_a_held_release_and_the_series_stops_there(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The cluster then holds objects of a release SDO did not accept: a fault on it measures that state, not the
    release. The record is not valid, so the platform runs no later position of the series."""
    record = held_then(tmp_path, 2)
    assert (record["event"], record["fault_id"], record["valid"], record["comparable"]) == (
        "fault",
        "fault-x",
        False,
        False,
    )
    assert str(record["invalid_reason"]).startswith("release_held: commit 1")
    assert record["release_held"]["commit_index"] == 1  # type: ignore[index]
    assert not (tmp_path / "wrapped-args").exists(), "the fault was injected"
    assert call_safety(tmp_path, "active", "active=true", capsys) == {"active": False}, "there is no fault to undo"


def test_on_held_continue_runs_the_fault_and_labels_it(tmp_path: Path) -> None:
    record = held_then(tmp_path, 2, {"on_held": "continue"})
    assert (record["valid"], record["comparable"], record["release_held"]["commit_index"]) == (True, False, 1)  # type: ignore[index]
    assert record["window"] == FAULT_RECORD["window"]


def test_a_later_accepted_release_clears_the_hold(tmp_path: Path) -> None:
    fault_series(tmp_path, [{"commit": 1}, {"commit": 2}, {"fault": "fault-x"}, {"commit": 3}])
    events = [{"commit": 1}, {"commit": 2}, {"fault": "fault-x"}, {"commit": 3}]
    envs = [series_env(tmp_path, p) for p in (1, 2, 3)]
    fault_series(tmp_path, events)  # series_env rewrote the spec
    adapter.run(envs[0], io.StringIO(), ops=Lane("held"))  # type: ignore[arg-type]
    adapter.run(envs[1], io.StringIO(), ops=Lane("accepted"))  # type: ignore[arg-type]
    out = io.StringIO()
    adapter.run(envs[2], out, ops=Lane("accepted"))  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert record["valid"] is True
    assert "release_held" not in record


def test_a_new_series_on_the_lane_does_not_see_an_earlier_hold(tmp_path: Path) -> None:
    """A series starts at position 1: the hold a previous attempt left in the lane directory is not its release."""
    first_env, second_env = series_env(tmp_path, 1), series_env(tmp_path, 2)
    fault_series(tmp_path)
    adapter.run(first_env, io.StringIO(), ops=Lane("held"))  # type: ignore[arg-type]
    adapter.run(first_env, io.StringIO(), ops=Lane("accepted"))  # type: ignore[arg-type]
    out = io.StringIO()
    adapter.run(second_env, out, ops=Lane("accepted"))  # type: ignore[arg-type]
    assert json.loads(out.getvalue().splitlines()[-1])["valid"] is True


def test_grade_says_a_fault_row_after_a_held_release_is_not_comparable(tmp_path: Path) -> None:
    rows = [
        {
            **FAULT_RECORD,
            "series": "demo",
            "index": 2,
            "event": "fault",
            "fault_id": "fault-x",
            "after_commit": 1,
            "comparable": False,
            "release_held": {"commit_index": 1, "reason": "rollout"},
        },
    ]
    (tmp_path / "runs.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    adapter.grade({"WORKLOAD_RESULTS": str(tmp_path)}, io.StringIO())
    assert "not comparable: release 1 held" in (tmp_path / "report.md").read_text()


def test_a_release_that_was_not_accepted_does_not_clear_an_earlier_hold(tmp_path: Path) -> None:
    events = [{"commit": 1}, {"commit": 2}, {"fault": "fault-x"}, {"commit": 3}]
    envs = [series_env(tmp_path, p) for p in (1, 2, 3)]
    fault_series(tmp_path, events)  # series_env rewrote the spec
    adapter.run(envs[0], io.StringIO(), ops=Lane("held"))  # type: ignore[arg-type]
    adapter.run(envs[1], io.StringIO(), ops=Lane("accepted", status="conflict"))  # type: ignore[arg-type]
    out = io.StringIO()
    adapter.run(envs[2], out, ops=Lane("accepted"))  # type: ignore[arg-type]
    record = json.loads(out.getvalue().splitlines()[-1])
    assert (record["valid"], record["release_held"]["commit_index"]) == (False, 1)
