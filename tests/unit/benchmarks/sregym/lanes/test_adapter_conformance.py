"""The SREGym lanes adapter against the lanes step contract: every verb, the environment lanes gives it, no cluster.

Lanes (Go) builds one environment per adapter call (``benchmarks/lanes/internal/bench/step.go`` ``laneEnv``, the host's
``EnvList`` and ``eksEnv``). Each adapter verb is run here against a fake lane with exactly that environment and must

* exist in the contract (``benchmarks/lanes/docs/contract.md``) and in this table, so a new verb or action without a
  case fails (derived from the dispatch tables and the doc, not from a list kept here);
* read only variables the platform gives (or the host's own ``SREGYM_LANES_*`` settings) and name every one it needs
  in the case (``needs``), failing with ``<NAME> is not set`` and a JSON last line when one is missing: never a
  traceback, a wrong default or a silent read of the working directory;
* bound every command it runs: no subprocess without a timeout (static) and a long command that returns a named
  error when its deadline passes, with its whole process tree gone.

A bug of the class "the step had no ``SREGYM_LANES_ENV``", "a wait without a deadline" or "TERM left the child
running" fails here in seconds, in pytest, before it costs a lane.
"""

from __future__ import annotations

import ast
import json
import os
import re
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks.sregym.lanes import subject, workload
from benchmarks.sregym.lanes.workload import VERBS, dispatch

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

REPO = Path(__file__).resolve().parents[5]
BENCH = REPO / "benchmarks/lanes/internal/bench"
CONTRACT = REPO / "benchmarks/lanes/docs/contract.md"
PACK = REPO / "benchmarks/sregym/lanes/apps/hotel_reservation"
NOW = datetime(2026, 11, 22, 10, 0, tzinfo=UTC)
PROBLEM = "misconfig_app_hotel_res"
NAMESPACE = "hotel-reservation"

# Variables only the call itself carries (WorkloadEnv in workload.go) and the ones a step adds around a verb.
CALL_ENV = frozenset({"WORKLOAD_VERB", "WORKLOAD_ACTION", "WORKLOAD_ARGS", "WORKLOAD_RESULTS", "WORKLOAD_BUILTIN"})
RUN_ENV = frozenset({"FUZZOPS_EXEC_OUT", "BENCH_RUN", "BENCH_FAULT"})
#: The host's own settings of the SREGym workload (``workload_env`` of hosts.yaml), never given by the platform.
HOST_PREFIX = "SREGYM_LANES_"
#: ``SDO_OPUS_POOL_NFS_SERVER``: where an Opus pool lives when its lane has no node of its own (``hosts.yaml`` ``env``).
HOST_NAMES = frozenset({"BENCH_IMAGE_TAGS", "SDO_OPUS_POOL_NFS_SERVER"})


# ----- the platform's environment, read from the Go source -----


def _function(source: str, signature: str) -> str:
    start = source.index(signature)
    end = source.index("\n}\n", start)
    return source[start:end]


def go_lane_variables() -> set[str]:
    """Every variable lanes sets on a step's adapter calls: ``laneEnv``, the host ``EnvList`` and the EKS ``eksEnv``."""
    step = (BENCH / "step.go").read_text(encoding="utf-8")
    names = set(re.findall(r'"([A-Z][A-Z0-9_]+)=', _function(step, "func (r *stepRunner) laneEnv(")))
    inventory = (BENCH / "inventory.go").read_text(encoding="utf-8")
    names |= set(re.findall(r'all\["([A-Z][A-Z0-9_]+)"\]', _function(inventory, "func (h Host) EnvList(")))
    eks = (BENCH / "eks.go").read_text(encoding="utf-8")
    names |= set(re.findall(r'"([A-Z][A-Z0-9_]+)="', _function(eks, "func eksEnv(")))
    return names


def contract_values(root: Path) -> dict[str, str]:
    """A synthetic value for every variable the platform gives a step of an EKS vcluster lane (the widest case)."""
    values = {name: f"synthetic-{name.lower()}" for name in sorted(go_lane_variables())}
    values.update(
        {
            "KUBECONFIG": str(root / "kube" / "vc1"),
            "BENCH_LANE": "vc1",
            "BENCH_HOST": "aws-vc",
            "BENCH_LANE_DIR": str(root / "lane"),
            "BENCH_LANE_PROVIDER": "eks",
            "BENCH_LANE_VIRTUAL": "1",
            "BENCH_IMAGE_REGISTRY": "123456789012.dkr.ecr.us-west-2.amazonaws.com",
            "BENCH_EVIDENCE_MODE": "pull",
            "BENCH_EVIDENCE_HOST": str(root / "evidence"),
            "BENCH_ENTRY_HOST": "203.0.113.7",
            "BENCH_PACK": str(PACK),
            "FUZZOPS_NAMESPACE": NAMESPACE,
            "BENCH_EXPERIMENT": "conformance",
            # The host's workload_env, keyed by the spec's workload string (docs/contract.md).
            "SREGYM_LANES_ENV": str(root / "env"),
            "BENCH_OPERATOR": str(root / "operator.json"),
            "BENCH_OPERATOR_BLOCK": "1",
        }
    )
    return values


# ----- a fake lane and a recording environment -----


class RecordingEnv(dict[str, str]):
    """The environment of one call; remembers every name the adapter looked at."""

    def __init__(self, values: Mapping[str, str]) -> None:
        super().__init__(values)
        self.reads: set[str] = set()

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        self.reads.add(key)
        return super().get(key, default)

    def __getitem__(self, key: str) -> str:
        self.reads.add(key)
        return super().__getitem__(key)

    def __contains__(self, key: object) -> bool:
        self.reads.add(str(key))
        return super().__contains__(key)


class FakeRunner:
    """Answers commands by a word they contain; records them and the environment each got."""

    def __init__(self, answers: Mapping[str, str] | None = None) -> None:
        self.answers = dict(answers or {})
        self.calls: list[list[str]] = []

    def __call__(self, argv, *, env=None, cwd=None, tick=None) -> str:
        argv = list(argv)
        self.calls.append(argv)
        text = " ".join(argv)
        for key, answer in self.answers.items():
            if key in text:
                return answer
        return ""


@dataclass
class FakeLane:
    """The lane's files on this host: the fastloop environment, the kubeconfig and the accepted repository."""

    root: Path
    env_dir: Path = field(init=False)

    def __post_init__(self) -> None:
        self.env_dir = self.root / "env" / "vc1"
        self.env_dir.mkdir(parents=True)
        (self.root / "kube").mkdir()
        self.workspace = self.root / "workspace"
        (self.root / "kube" / "vc1").write_text("apiVersion: v1\n", encoding="utf-8")
        (self.root / "lane").mkdir()
        (self.root / "operator.json").write_text(json.dumps({"hooks": "sregym"}), encoding="utf-8")
        self.workspace.mkdir()
        self.write_environment()

    def write_environment(self) -> None:
        """What ``fastloop up`` leaves: the lane's environment file."""
        self.env_dir.mkdir(parents=True, exist_ok=True)
        (self.env_dir / "fastloop.json").write_text(
            json.dumps(
                {
                    "namespace": NAMESPACE,
                    "kubeconfig": str(self.root / "kube" / "vc1"),
                    "workspace": str(self.workspace),
                    "seed_commit": "",
                }
            ),
            encoding="utf-8",
        )


def deployments() -> str:
    spec = {"replicas": 1, "template": {"spec": {"containers": [{"image": "f:1"}]}}}
    return json.dumps({"items": [{"kind": "Deployment", "metadata": {"name": "frontend"}, "spec": spec}]})


def pods() -> str:
    ready = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
    return json.dumps({"items": [{"metadata": {"name": "frontend-0"}, "status": ready}]})


def fastloop_run(lane: FakeLane) -> Callable[..., str]:
    """A ``fastloop`` child that records one injected, cleaned-up incident (the accepted repository stays empty)."""

    def run(argv, *, env=None, cwd=None, tick=None, **limits) -> str:
        argv = list(argv)
        if "--run-id" in argv:
            results = lane.env_dir / "results" / argv[argv.index("--run-id") + 1]
            results.mkdir(parents=True)
            record = {
                "run_id": "r",
                "problem_id": PROBLEM,
                "injection": {"state": "injected", "started_at": "2026-11-22T10:01:00Z"},
                "oracle": {"kind": "mitigation", "success": True},
                "full_cleanup_verified": True,
            }
            (results / "incidents.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
        return ""

    return run


# ----- the cases: one per verb and action of the contract -----


def real_agent(lane: FakeLane, env: dict[str, str]) -> None:
    """The spec names a model: ``BENCH_OPERATOR`` is an operator file with provider, model, effort and a credential."""
    credential = lane.root / "credentials.json"
    credential.write_text("{}", encoding="utf-8")
    operator = lane.root / "real-operator.json"
    operator.write_text(
        json.dumps(
            {
                "hooks": "sregym",
                "settings": {
                    "provider": "claude",
                    "model": "synthetic-model",
                    "effort": "low",
                    "auth_file": str(credential),
                },
            }
        ),
        encoding="utf-8",
    )
    env["BENCH_OPERATOR"] = str(operator)


@dataclass(frozen=True)
class Case:
    verb: str
    action: str
    #: Variables the verb cannot work without: each, removed, must answer ``<NAME> is not set``.
    needs: frozenset[str] = frozenset()
    #: Variables the verb may read besides ``needs`` (a default stands in when they are absent).
    optional: frozenset[str] = frozenset()
    args: Callable[[Path], list[str]] = lambda root: []
    seams: Callable[[FakeLane, Mapping[str, str]], dict[str, Any]] = lambda lane, env: {}
    prepare: Callable[[FakeLane, dict[str, str]], None] = lambda lane, env: None
    #: The exit status the verb answers with the whole environment (64: it is unsupported).
    code: int = 0
    #: Names a second case of the same verb and action (the same call with another operator file).
    variant: str = ""

    @property
    def id(self) -> str:
        return f"{self.verb}/{self.action or '-'}" + (f"/{self.variant}" if self.variant else "")


def lane_files(lane: FakeLane, env: dict[str, str]) -> None:
    """The lane's `up` already happened: the fastloop environment is on disk."""


LANE = frozenset({"BENCH_LANE", "SREGYM_LANES_ENV"})
KUBE = frozenset({"KUBECONFIG"})

CASES: tuple[Case, ...] = (
    Case("identity", "", seams=lambda lane, env: {"runner": FakeRunner({"rev-parse HEAD": "abc\n"})}),
    Case("app-up", "check-pack", args=lambda root: ["--pack", str(PACK)]),
    Case("app-up", "down", args=lambda root: ["--pack", str(PACK)]),
    Case("app-up", "images", args=lambda root: ["--pack", str(PACK), "--cluster", "vc1"]),
    Case(
        # A fresh EKS lane: fastloop attaches through the provider and pushes images to its registry.
        "app-up",
        "up",
        needs=LANE | KUBE | {"BENCH_IMAGE_REGISTRY"},
        optional=frozenset(
            {
                "BENCH_LANE_PROVIDER",
                "BENCH_EVIDENCE_HOST",
                "BENCH_OPERATOR",
                "BENCH_OPERATOR_BLOCK",
                "BENCH_LANE_VIRTUAL",
            }
        ),
        args=lambda root: ["--pack", str(PACK), "--namespace", NAMESPACE],
        seams=lambda lane, env: {
            "runner": FakeRunner(),
            "long_runner": lambda argv, *, env=None, cwd=None, tick=None: lane.write_environment() or "",
            "eks_fence": lambda ctx, env_dir: False,
        },
    ),
    Case(
        "app-up",
        "up",
        variant="real-agent",
        needs=LANE | KUBE | {"BENCH_IMAGE_REGISTRY"},
        optional=frozenset(
            {
                "BENCH_LANE_PROVIDER",
                "BENCH_EVIDENCE_HOST",
                "BENCH_OPERATOR",
                "BENCH_OPERATOR_BLOCK",
                "BENCH_LANE_VIRTUAL",
            }
        ),
        args=lambda root: ["--pack", str(PACK), "--namespace", NAMESPACE],
        prepare=real_agent,
        seams=lambda lane, env: {
            "runner": FakeRunner(),
            "long_runner": lambda argv, *, env=None, cwd=None, tick=None: lane.write_environment() or "",
            "eks_fence": lambda ctx, env_dir: False,
        },
    ),
    Case("app-snapshot", "write", needs=KUBE, args=lambda root: ["--namespace", NAMESPACE, "--out", str(root / "fp")]),
    Case(
        "app-reset",
        "recover",
        args=lambda root: ["--namespace", NAMESPACE, "--pack", str(PACK)],
    ),
    Case("gate", "check", needs=KUBE, args=lambda root: ["--namespace", NAMESPACE]),
    Case(
        "run",
        "run",
        needs=LANE,
        optional=frozenset(
            {
                "BENCH_OPERATOR",
                "BENCH_OPERATOR_BLOCK",
                "BENCH_LANE_VIRTUAL",
                "BENCH_EXPERIMENT",
                "BENCH_RUN",
                "FUZZOPS_EXEC_OUT",
                "BENCH_CAP_SECONDS",
                "BENCH_READY_TIMEOUT_SECONDS",
            }
        ),
        args=lambda root: ["--operator", "exec:x", "--seed", "1", "--instance", PROBLEM],
        seams=lambda lane, env: {"runner": FakeRunner(), "long_runner": fastloop_run(lane)},
    ),
    Case(
        "run",
        "run",
        variant="real-agent",
        needs=LANE,
        optional=frozenset(
            {
                "BENCH_OPERATOR",
                "BENCH_OPERATOR_BLOCK",
                "BENCH_LANE_VIRTUAL",
                "BENCH_EXPERIMENT",
                "BENCH_RUN",
                "FUZZOPS_EXEC_OUT",
                "BENCH_CAP_SECONDS",
                "BENCH_READY_TIMEOUT_SECONDS",
            }
        ),
        args=lambda root: ["--operator", "exec:x", "--seed", "1", "--instance", PROBLEM],
        prepare=real_agent,
        seams=lambda lane, env: {"runner": FakeRunner(), "long_runner": fastloop_run(lane)},
    ),
    Case("run", "active", needs=LANE, args=lambda root: ["--file", "/i.json", "--id", PROBLEM]),
    Case("run", "undo", needs=LANE, args=lambda root: ["--file", "/i.json", "--id", PROBLEM]),
    Case(
        "grade",
        "report",
        needs=frozenset({"WORKLOAD_RESULTS"}),
        prepare=lambda lane, env: (Path(env["WORKLOAD_RESULTS"]).mkdir(parents=True, exist_ok=True)),
    ),
    Case("footprint", "", code=workload.UNSUPPORTED),
)
# `app-snapshot verify` needs the snapshot `write` made, so it runs through its own case below.
VERIFY = Case(
    "app-snapshot", "verify", needs=KUBE, args=lambda root: ["--namespace", NAMESPACE, "--against", str(root / "fp")]
)


def runner_for(case: Case, lane: FakeLane, env: Mapping[str, str]) -> dict[str, Any]:
    """The case's seams; a long command is recorded with the others, so "nothing ran" covers it too."""
    seams = dict(case.seams(lane, env))
    if "runner" not in seams and case.verb in ("app-snapshot", "gate"):
        seams["runner"] = FakeRunner({"get deployments,statefulsets": deployments(), "get pods": pods()})
    long_runner = seams.get("long_runner")
    runner = seams.get("runner")
    if long_runner is not None and isinstance(runner, FakeRunner):

        def recorded(argv, *, env=None, cwd=None, tick=None, **limits):
            runner.calls.append(list(argv))
            return long_runner(argv, env=env, cwd=cwd, tick=tick, **limits)

        seams["long_runner"] = recorded
    return seams


def run_case(
    case: Case, root: Path, *, without: str = "", extra: Mapping[str, str] | None = None
) -> tuple[int, dict[str, Any], RecordingEnv, list[list[str]]]:
    """One adapter call of ``case`` on a fresh fake lane, with the platform's environment minus ``without``."""
    lane = FakeLane(root)
    values = contract_values(root)
    values["WORKLOAD_RESULTS"] = str(root / "results")
    if case.verb == "run":
        values.update({"FUZZOPS_EXEC_OUT": str(root / "exec"), "BENCH_RUN": "r1", "BENCH_FAULT": PROBLEM})
    case.prepare(lane, values)
    if case.verb == "grade":
        (root / "results" / "runs.jsonl").write_text(
            json.dumps({"operator": "exec:x", "valid": True, "fault": PROBLEM}) + "\n", encoding="utf-8"
        )
    if (case.verb, case.action) == ("app-snapshot", "verify"):
        (root / "fp").write_text(
            json.dumps({"namespace": NAMESPACE, "workloads": ["Deployment/frontend replicas=1 images=f:1"]}),
            encoding="utf-8",
        )
    values.update(extra or {})
    values.pop(without, None)
    env = RecordingEnv({**values, "WORKLOAD_ACTION": case.action, "WORKLOAD_ARGS": json.dumps(case.args(root))})
    seams = runner_for(case, lane, values)
    code, result = dispatch(case.verb, env, now=lambda: NOW, **seams)
    runner = seams.get("runner")
    return code, result, env, (runner.calls if isinstance(runner, FakeRunner) else [])


ALL_CASES = (*CASES, VERIFY)


# ----- the contract tables, parsed from the doc and the Go source -----


def contract_workload_verbs() -> dict[str, set[str]]:
    """``verb -> operations`` of the workload verb table in docs/contract.md."""
    text = CONTRACT.read_text(encoding="utf-8")
    table = text[text.index("## Workload verbs") :]
    table = table[: table.index("\nThe built-in workload")]
    verbs: dict[str, set[str]] = {}
    for line in table.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) >= 2 and cells[0].startswith("`") and cells[0] != "`verb`":
            actions = set(re.findall(r"`([a-z-]+)`", cells[1])) if cells[1] != "none" else {""}
            verbs[cells[0].strip("`")] = actions
    return verbs


def go_subject_verbs() -> list[str]:
    source = (BENCH / "hooks.go").read_text(encoding="utf-8")
    block = source[source.index("var SubjectVerbs = []string{") :]
    return re.findall(r'"([a-z-]+)"', block[: block.index("}")])


def test_the_contract_table_and_the_dispatch_table_name_the_same_workload_verbs():
    assert set(contract_workload_verbs()) == set(VERBS) | {"footprint"}


def test_every_verb_and_action_of_the_contract_has_a_conformance_case():
    wanted = {(verb, action) for verb, actions in contract_workload_verbs().items() for action in actions}
    covered = {(case.verb, case.action) for case in ALL_CASES}
    assert wanted <= covered, f"add a Case for {sorted(wanted - covered)} (docs/contract.md lists it)"
    assert covered <= wanted, f"Case for {sorted(covered - wanted)} that docs/contract.md does not list"


def test_an_unknown_verb_or_action_is_unsupported_not_an_error(tmp_path):
    assert dispatch("teleport", {}) == (workload.UNSUPPORTED, {"unsupported": True})
    env = {**contract_values(tmp_path), "WORKLOAD_ACTION": "teleport", "WORKLOAD_ARGS": "[]"}
    for verb in VERBS:
        if verb not in ("identity", "grade"):
            runner = FakeRunner()
            assert dispatch(verb, env, runner=runner)[0] == workload.UNSUPPORTED, verb
            assert not runner.calls, f"{verb} ran {runner.calls} before it knew the action"


# ----- env: the platform's variables, per verb -----


def test_the_conformance_environment_has_every_variable_lanes_gives_a_step():
    names = go_lane_variables()
    assert {"KUBECONFIG", "BENCH_LANE", "BENCH_LANE_PROVIDER", "BENCH_IMAGE_REGISTRY"} <= names, "the Go parse broke"
    assert set(contract_values(Path("/x"))) >= names


@pytest.mark.parametrize("case", ALL_CASES, ids=lambda case: case.id)
def test_a_verb_answers_with_the_environment_lanes_gives_it(case, tmp_path):
    code, result, _, _ = run_case(case, tmp_path)
    assert code == case.code, result
    assert isinstance(result, dict)


@pytest.mark.parametrize("case", ALL_CASES, ids=lambda case: case.id)
def test_a_verb_reads_only_what_the_platform_gives_and_names_what_it_needs(case, tmp_path):
    _, _, env, _ = run_case(case, tmp_path)
    given = go_lane_variables() | CALL_ENV | RUN_ENV
    host = {name for name in env.reads if name.startswith(HOST_PREFIX) or name in HOST_NAMES}
    foreign = env.reads - given - host
    assert not foreign, f"{case.id} reads {sorted(foreign)}: lanes gives a step no such variable"
    undeclared = env.reads - given_declared(case) - CALL_ENV - host
    assert not undeclared, f"{case.id} reads {sorted(undeclared)}: list it in the case's needs or optional"
    assert case.needs <= env.reads | {"WORKLOAD_RESULTS"}, f"{case.id} declares needs it never reads"


def given_declared(case: Case) -> frozenset[str]:
    return case.needs | case.optional | (RUN_ENV if case.verb == "run" else frozenset())


NEEDS = [(case, name) for case in ALL_CASES for name in sorted(case.needs)]


@pytest.mark.parametrize(("case", "name"), NEEDS, ids=[f"{case.id}-{name}" for case, name in NEEDS])
def test_a_missing_variable_is_a_named_error_with_a_json_result(case, name, tmp_path):
    code, result, _, calls = run_case(case, tmp_path, without=name)
    assert code == 1, f"{case.id} without {name}: exit {code} {result}"
    assert f"{name} is not set" in result["error"], f"{case.id} without {name} failed unnamed: {result}"
    assert not calls, f"{case.id} ran {calls[0][:3]} before it checked {name}: fail in seconds, not after the work"


def test_the_adapter_reads_the_process_environment_only_in_main():
    for module in (workload, subject):
        source = Path(str(module.__file__)).read_text(encoding="utf-8")
        tree = ast.parse(source)
        for function in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name != "main"]:
            text = ast.get_source_segment(source, function) or ""
            assert "os.environ" not in text, f"{module.__name__}.{function.name}"
            assert "getenv" not in text, f"{module.__name__}.{function.name}"


# ----- subject verbs -----

SUBJECT_HANDLED = {
    "check",
    "teardown",
    "collect",
    "transcripts",
    "snapshot",
    "reset",
    "reset-incidents",
    "idle",
    "watch",
    "describe",
    "prepare",
    "swap",
}


def test_every_subject_verb_of_the_platform_is_handled_or_unsupported_here():
    verbs = set(go_subject_verbs())
    assert {"prepare", "swap", "idle", "describe", "preflight"} <= verbs, "the Go parse broke"
    unsupported = {
        verb for verb in verbs if subject.dispatch(verb, {}, version=lambda: "rev")[0] == subject.UNSUPPORTED
    }
    unknown = verbs - SUBJECT_HANDLED - unsupported
    assert not unknown
    # A verb the shim handles must be one the platform calls (or the contract defines).
    assert SUBJECT_HANDLED - {"reset-incidents"} <= verbs  # reset-incidents is the contract's optional extra


@pytest.mark.parametrize("verb", sorted(SUBJECT_HANDLED))
def test_a_subject_verb_answers_json_with_the_environment_lanes_gives_it(verb, tmp_path, capsys, monkeypatch):
    FakeLane(tmp_path)
    for name, value in contract_values(tmp_path).items():
        monkeypatch.setenv(name, value)
    code = subject.main([verb])
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert code == 0, last
    assert isinstance(json.loads(last), dict)


def test_a_subject_verb_that_cannot_work_names_why_instead_of_a_traceback(tmp_path, capsys, monkeypatch):
    FakeLane(tmp_path)
    for name, value in contract_values(tmp_path).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("BENCH_OPERATOR", str(tmp_path / "missing-operator.json"))
    code = subject.main(["prepare"])
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert code == 1
    assert "BENCH_OPERATOR" in json.loads(last)["error"]


# ----- bounds: no command without a deadline -----

ADAPTER_MODULES = sorted((REPO / "benchmarks/sregym/lanes").glob("*.py"))


def unbounded_calls(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        f"{path.name}:{node.lineno} subprocess.{node.func.attr}"
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "subprocess"
            and node.func.attr in {"run", "call", "check_call", "check_output"}
            and "timeout" not in {keyword.arg for keyword in node.keywords}
        )
    ]


def test_no_adapter_command_runs_without_a_timeout():
    found = [call for path in ADAPTER_MODULES for call in unbounded_calls(path)]
    assert not found, (
        "every command of the adapter carries a deadline (the platform's limit is a backstop):\n" + "\n".join(found)
    )


# ----- waits: a deadline ends the command in seconds, and no cancel leaves a child behind -----

TREE = "sleep 60 & echo $! > {pid}; wait"


def alive(pid: int) -> bool:
    """Whether the process runs (a zombie waiting for its parent to reap it does not)."""
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return False
    return state != "Z"


def gone_within(pid: int, seconds: float) -> bool:
    """Whether the process is gone within ``seconds`` (an event wait, not a sleep: it returns when the time is up)."""
    pause = threading.Event()
    waited = 0.0
    while alive(pid) and waited < seconds:
        pause.wait(0.02)
        waited += 0.02
    return not alive(pid)


def pid_from(path: Path) -> int:
    pause = threading.Event()
    for _ in range(500):
        if path.is_file() and path.read_text(encoding="utf-8").strip():
            return int(path.read_text(encoding="utf-8"))
        pause.wait(0.02)
    raise AssertionError(f"no pid in {path}")


def test_the_run_command_is_bound_to_the_rows_deadline(tmp_path):
    """The spec's cap reaches the workload as BENCH_CAP_SECONDS: the run must hand a deadline (and the wait it
    records) to the command it supervises, or a stuck row holds the lane to the platform's 6 h limit."""
    seen: dict[str, Any] = {}

    def supervised(argv, *, env=None, cwd=None, tick=None, **limits):
        seen.update(limits)
        return ""

    case = next(c for c in CASES if c.id == "run/run")
    bound = replace(case, seams=lambda lane, env: {"runner": FakeRunner(), "long_runner": supervised})
    run_case(bound, tmp_path, extra={"BENCH_CAP_SECONDS": "600", "BENCH_READY_TIMEOUT_SECONDS": "300"})
    assert seen.get("deadline_seconds", 0) > 900, seen
    assert "current_wait" in seen, seen


def test_a_command_past_its_deadline_is_a_named_error_and_leaves_no_process(tmp_path, monkeypatch):
    monkeypatch.setattr(workload, "KILL_GRACE_SECONDS", 1.0)
    pidfile = tmp_path / "pid"
    with pytest.raises(workload.RowDeadlineError, match="row deadline"):
        workload.run_logged(["sh", "-c", TREE.format(pid=pidfile)], deadline_seconds=0.5)
    assert gone_within(pid_from(pidfile), 5), "the command's child outlived the kill at the deadline"


def test_a_cancel_of_the_adapter_ends_the_whole_process_tree(tmp_path):
    """The platform cancels a step with TERM to the adapter's process group; the run is in a session of its own, so the
    adapter must pass the cancel on (an orphaned run keeps injecting into a lane the platform thinks is free)."""
    pidfile = tmp_path / "pid"
    code = (
        "from benchmarks.sregym.lanes import workload as w\n"
        "w.KILL_GRACE_SECONDS = 2\n"
        f"w.run_logged(['sh', '-c', {TREE.format(pid=pidfile)!r}])\n"
    )
    adapter = subprocess.Popen(
        [sys.executable, "-c", code], cwd=REPO, env={**os.environ, "PYTHONPATH": str(REPO)}, start_new_session=True
    )
    child = 0
    try:
        child = pid_from(pidfile)
        os.killpg(adapter.pid, signal.SIGTERM)
        assert adapter.wait(timeout=20) != 0
        assert gone_within(child, 5), "TERM of the adapter's process group left the run's child running"
    finally:
        if adapter.poll() is None:
            os.killpg(adapter.pid, signal.SIGKILL)
        if child and alive(child):
            os.kill(child, signal.SIGKILL)


# ----- the operator file reaches `app-up up`: a real agent's images come from it -----


def up_case() -> Case:
    return next(case for case in CASES if case.id == "app-up/up/real-agent")


def with_images(lane: FakeLane, env: dict[str, str]) -> None:
    real_agent(lane, env)
    path = Path(env["BENCH_OPERATOR"])
    document = json.loads(path.read_text(encoding="utf-8"))
    document["settings"].update({"controller_image": "sdo-controller:real", "responder_image": "sdo-responder:plain"})
    path.write_text(json.dumps(document), encoding="utf-8")


def test_a_real_agent_operator_file_gives_app_up_the_agents_images(tmp_path):
    case = replace(up_case(), prepare=with_images)
    code, result, _, calls = run_case(case, tmp_path)
    assert code == 0, result
    up = next(argv for argv in calls if "up" in argv and "--controller-image" in argv)
    assert up[up.index("--controller-image") + 1] == "sdo-controller:real"
    assert up[up.index("--responder-image") + 1] == "sdo-responder:plain"


def test_a_spec_with_an_operator_block_but_no_operator_file_is_a_named_error_not_the_host_defaults(tmp_path):
    case = up_case()
    code, result, _, calls = run_case(case, tmp_path, without="BENCH_OPERATOR")
    assert code == 1, result
    assert "real agent spec but BENCH_OPERATOR not set" in result["error"], result
    assert not calls, f"app-up up ran {calls[0][:3]} with the host's default images before it noticed"


def test_an_operator_file_that_cannot_be_read_is_a_named_error(tmp_path):
    code, result, _, calls = run_case(up_case(), tmp_path, extra={"BENCH_OPERATOR": str(tmp_path / "gone.json")})
    assert code == 1, result
    assert "BENCH_OPERATOR" in result["error"], result
    assert "gone.json" in result["error"], result
    assert not calls
