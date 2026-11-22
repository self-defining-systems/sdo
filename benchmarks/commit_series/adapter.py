"""The commit-series workload adapter: `run` walks one commit of a series; other verbs are the wrapped workload's.

A lane experiment names this adapter as its ``workload`` command and lists a series in the instances file::

    {"ids": ["toy-series"], "series": {"toy-series": {"spec": "toy.json", "verdict_s": 600, "steady_s": 60}},
     "fault_instances": "faults.json"}

with ``sequences: [[1, 2, 3]]``: the seeds are the commit numbers, run in order on one lane that nothing resets between
them. Position *i* pushes commit *i* of the series to the branch SDO deploys from
(``$BENCH_LANE_DIR/source-branch.json``, written by the subject's prepare), waits for SDO's verdict and prints the
commit's record as the run record. A fault
listed at that point in the spec is injected afterwards through the wrapped workload (``fault_instances``).

The adapter knows nothing of lanes beyond the contract's environment, and SDO nothing of it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

from benchmarks.commit_series.scenario import Bounds, SeriesOps, run_commit
from benchmarks.common.replay.replay import ReplayError, ReplayRecord, replay, run_git
from benchmarks.common.replay.series import Series
from benchmarks.fuzzops import workload as fuzzops_workload
from benchmarks.sregym.fastloop.toy_live import KubectlScenarioOps
from benchmarks.sregym.fastloop.toy_scenarios import HoldRecord
from benchmarks.sregym.fastloop.toy_upstream import BASE_PATH, REPOSITORY, SERVICE, UpstreamLayout, forwarded_remote

SOURCE_BRANCH_FILE = "source-branch.json"
#: The control-namespace ConfigMap SDO keeps its one refused branch head in (``sdo/operator/runtime/head_poll.go``).
HEAD_HOLD_CONFIGMAP = "sdo-head-hold"
_GIT_ENV = {"GIT_TERMINAL_PROMPT": "0", "GIT_ALLOW_PROTOCOL": "file:git"}


class AdapterError(RuntimeError):
    """The run cannot start: a missing environment variable, instances file or source-branch record."""


def parse_flags(args: list[str]) -> dict[str, str]:
    """``--name value`` pairs of the run's options; a flag with no value is ``""``."""
    out: dict[str, str] = {}
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            name = args[i][2:]
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                out[name], i = args[i + 1], i + 2
                continue
            out[name] = ""
        i += 1
    return out


@dataclass(frozen=True)
class Entry:
    """The instances file's entry for one series: the spec and the scenario's bounds."""

    series: Series
    bounds: Bounds
    fault_instances: Path | None

    @classmethod
    def find(cls, instances_file: Path, instance: str) -> Entry | None:
        """The entry for ``instance``, or None when the instances file lists no series by that id."""
        data = json.loads(instances_file.read_text())
        raw = (data.get("series") or {}).get(instance)
        if raw is None:
            return None
        base = instances_file.parent
        series = Series.load(base / raw["spec"])
        options = {k: v for k, v in (("verdict", raw.get("verdict_s")), ("steady", raw.get("steady_s"))) if v}
        faults = data.get("fault_instances")
        return cls(series, Bounds(**options), base / faults if faults else None)


def wall_start(lane_dir: Path, name: str, now: float) -> float:
    """When position 1 of the series ran; later positions are scheduled from it."""
    path = lane_dir / f"series-{name}.json"
    if path.is_file():
        return float(json.loads(path.read_text())["started"])
    path.write_text(json.dumps({"started": now}))
    return now


class LaneOps(KubectlScenarioOps):
    """The lane's reads (kubectl) and the commit push (a port-forward to the lane's source-branch remote)."""

    def __init__(self, lane_dir: Path, kubeconfig: Path, series: Series) -> None:
        info = json.loads((lane_dir / SOURCE_BRANCH_FILE).read_text())

        class _Env:  # the two attributes the reads use
            namespace = info["namespace"]

        _Env.kubeconfig = kubeconfig  # type: ignore[attr-defined]
        layout = UpstreamLayout(info["namespace"], ref=info["ref"])
        super().__init__(_Env, layout)  # type: ignore[arg-type]
        self.control = info.get("control", self.control)
        self.series = series
        self.kubeconfig = kubeconfig
        self.pushed: list[str] = []  # the commits this run pushed: a hold is matched to them

    def now(self) -> float:
        return time.time()

    def holds(self) -> list[HoldRecord]:
        """SDO's hold on a head that carries a commit this run pushed, as that commit's hold.

        SDO publishes its own commit on the branch before it judges a push (a health refresh), so the held head is
        that commit and the pushed one is its ancestor; the follower's ``sdo-upstream-holds`` no longer exists."""
        result = self._kubectl(self.control, "get", "configmap", HEAD_HOLD_CONFIGMAP, "-o", "json", check=False)
        if result.returncode != 0:
            if "NotFound" in result.stderr:
                return []
            raise RuntimeError(f"reading {HEAD_HOLD_CONFIGMAP} failed: {result.stderr.strip()}")
        record = json.loads(json.loads(result.stdout)["data"]["hold"])
        reason = f"{record['reason']}: {record['detail']}"
        return [HoldRecord(sha, reason) for sha in self.pushed if self._is_ancestor(sha, record["commit"])]

    def _is_ancestor(self, ancestor: str, commit: str) -> bool:
        """Whether ``commit`` on the branch server carries ``ancestor`` (or is it)."""
        done = self._kubectl(
            self.layout.namespace,
            *("exec", f"deploy/{SERVICE}", "--", "git", f"--git-dir={BASE_PATH}/{REPOSITORY}"),
            *("merge-base", "--is-ancestor", ancestor, commit),
            check=False,
        )
        if done.returncode in (0, 1):
            return done.returncode == 0
        if done.returncode == 128 and "not a valid" in done.stderr.lower():
            return False  # the held head is not on the server: no push of ours is under it
        raise RuntimeError(f"asking the branch server about {commit[:12]} failed: {done.stderr.strip()}")

    def push_commit(self, index: int) -> ReplayRecord:
        with forwarded_remote(self.kubeconfig, self.layout) as remote:
            config = self.series.config(index, remote, self.layout.ref)
            result = replay(config, git=_git)
        if not result.records:
            raise ReplayError(f"commit {index} of {self.series.name} produced no record")
        if result.records[0].pushed_sha:
            self.pushed.append(result.records[0].pushed_sha)
        return result.records[0]


def _git(args: Any, cwd: Any = None, stdin: Any = None, env: Any = None) -> subprocess.CompletedProcess[bytes]:
    return run_git(args, cwd, stdin, {**_GIT_ENV, **(env or {})})


def inject_fault(
    builtin: list[str], flags: dict[str, str], entry: Entry, fault: str, stdout: IO[str], env: dict[str, str]
) -> dict[str, Any]:
    """Run the wrapped workload's ``run`` for ``fault`` (its own instances file); returns its record.

    The wrapped workload is fuzzops' python workload (``benchmarks.fuzzops.workload``), which adds a lane provider's
    environment to the bare fuzzops binary in ``builtin``; the fault goes through it like any other call."""
    if entry.fault_instances is None:
        raise AdapterError(
            f"the series puts fault {fault!r} after a commit but the instances file has no fault_instances"
        )
    command = list(builtin)
    for name, value in (("--instance-file", str(entry.fault_instances)), ("--instance", fault)):
        if name in command:
            command[command.index(name) + 1] = value
        else:
            command += [name, value]
    done = subprocess.run(
        [sys.executable, "-m", "benchmarks.fuzzops.workload", "run"],
        env={**env, "WORKLOAD_BUILTIN": json.dumps(command)},
        capture_output=True,
        text=True,
        check=False,
    )
    sys.stderr.write(done.stderr)
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    if not lines:
        return {"valid": False, "invalid_reason": f"fault run exited {done.returncode} with no record"}
    return json.loads(lines[-1])


def run(env: dict[str, str], stdout: IO[str], *, ops: SeriesOps | None = None) -> int:
    args = json.loads(env.get("WORKLOAD_ARGS", "[]"))
    flags = parse_flags(args)
    builtin = json.loads(env["WORKLOAD_BUILTIN"]) if env.get("WORKLOAD_BUILTIN") else []
    entry = (
        Entry.find(Path(flags["instance-file"]), flags["instance"])
        if "instance-file" in flags and "instance" in flags
        else None
    )
    if entry is None:
        return delegate("run", env)
    lane_dir = Path(env["BENCH_LANE_DIR"])
    index = int(flags["seed"])
    ops = ops or LaneOps(lane_dir, Path(env["KUBECONFIG"]), entry.series)
    start = wall_start(lane_dir, entry.series.name, ops.now())
    ahead = start + entry.series.offset(index) - ops.now()
    if ahead > 0:
        ops.sleep(ahead)
    record = run_commit(ops, entry.series, index, entry.bounds, flags.get("operator", ""))
    if fault := entry.series.faults.get(index):
        injected = inject_fault(builtin, flags, entry, fault, stdout, env)
        record["fault"] = injected
        record["valid"] = bool(record["valid"] and injected.get("valid", False))
        if "lifecycle" in injected:
            record["lifecycle"] = injected["lifecycle"]
    stdout.write(json.dumps(record, sort_keys=True) + "\n")
    return 0


def delegate(verb: str, env: dict[str, str]) -> int:
    """The call is not a series run: the wrapped workload (fuzzops' python workload, then the fuzzops binary named by
    ``WORKLOAD_BUILTIN``) answers it unchanged."""
    if not env.get("WORKLOAD_BUILTIN"):
        print(json.dumps({"unsupported": True}))
        return 64
    return fuzzops_workload.main([verb], env)


def main(argv: list[str], env: dict[str, str] | None = None, stdout: IO[str] | None = None) -> int:
    env = dict(os.environ) if env is None else env
    out: IO[str] = stdout if stdout is not None else sys.stdout
    verb, action = (argv[0] if argv else env.get("WORKLOAD_VERB", "")), env.get("WORKLOAD_ACTION", "")
    if verb == "run" and action == "run":
        return run(env, out)
    if verb == "run" and action in ("active", "undo") and _is_series(env):
        out.write(json.dumps({"active": False}) + "\n")
        return 0
    if verb == "grade":
        return grade(env, out)
    return delegate(verb, env)


def _series_entry(flags: dict[str, str]) -> Entry | None:
    """The series a call names. A run names it with ``--instance-file/--instance``; the platform's ``active`` and
    ``undo`` checks after a run use ``--file/--id``."""
    file = flags.get("instance-file") or flags.get("file")
    instance = flags.get("instance") or flags.get("id")
    return Entry.find(Path(file), instance) if file and instance else None


def _is_series(env: dict[str, str]) -> bool:
    return _series_entry(parse_flags(json.loads(env.get("WORKLOAD_ARGS", "[]")))) is not None


def grade(env: dict[str, str], stdout: IO[str]) -> int:
    """Series records are their own grade: a table of commits beside them. Anything else is the wrapped grader's."""
    directory = Path(env.get("WORKLOAD_RESULTS") or json.loads(env.get("WORKLOAD_ARGS", "[]"))[0])
    lines = [json.loads(x) for x in (directory / "runs.jsonl").read_text().splitlines() if x.strip()]
    if not lines or not all("series" in rec for rec in lines):
        return delegate("grade", env)
    rows = ["| series | # | commit | outcome | deploy s | incident | agent tasks |", "|---|---|---|---|---|---|---|"]
    ordered = sorted(lines, key=lambda r: (r["series"], r["index"]))
    rows.extend(
        f"| {r['series']} | {r['index']} | {r['commit'][:12]} | {r['outcome']} | {r.get('deploy_seconds', '')} "
        f"| {r.get('incident_opened', '')} | {r.get('agent_tasks', '')} |"
        for r in ordered
    )
    (directory / "report.md").write_text("\n".join(rows) + "\n")
    (directory / "grades.json").write_text(json.dumps(lines, indent=1, sort_keys=True) + "\n")
    return 0
