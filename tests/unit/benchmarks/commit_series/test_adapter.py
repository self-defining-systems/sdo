"""The workload adapter: a series run prints the commit's record; every other call is the wrapped workload's."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING

from benchmarks.commit_series import adapter
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
    record = adapter.inject_fault(builtin, {}, entry, "fault-x", io.StringIO(), env)
    assert record == {"valid": True}
    assert marker.read_text().strip() == "10.1.2.3"


def test_the_safety_check_after_a_series_run_answers_inactive_for_the_lane_platforms_flags(tmp_path: Path) -> None:
    """The platform asks `run active` and `run undo` with `--file/--id`, not the run's `--instance-file/--instance`;
    a series has no fault to undo, and passing the call on made the lane go down with `fault still active`."""
    args = ["--file", str(instances(tmp_path)), "--id", "demo", "--pack", "/p", "--namespace", "fg1"]
    for action in ("active", "undo"):
        out = io.StringIO()
        code = adapter.main(["run"], {"WORKLOAD_ACTION": action, "WORKLOAD_ARGS": json.dumps(args)}, out)
        assert code == 0
        assert json.loads(out.getvalue()) == {"active": False}
