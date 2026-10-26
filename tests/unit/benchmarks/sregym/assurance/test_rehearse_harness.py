"""The harness rehearsal gate: scenario coverage, command shapes, budget, and each scenario's contract checks."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks.sregym.assurance import rehearse_harness as gate
from benchmarks.sregym.fastloop.pause_restart import PauseRestartError, RestartControllerInPauseOps

REPO_ROOT = Path(__file__).resolve().parents[5]


def test_scenarios_cover_the_known_plumbing_failures_with_real_streams() -> None:
    covered = {item for scenario in gate.SCENARIOS for item in scenario.covers}
    assert {"P51", "P52", "P53", "RH3"} <= covered
    assert len({scenario.name for scenario in gate.SCENARIOS}) == len(gate.SCENARIOS)
    for scenario in gate.SCENARIOS:
        if scenario.stream is not None:
            stream = json.loads((REPO_ROOT / gate.FIXTURES / scenario.stream).read_text(encoding="utf-8"))
            assert len(stream["incidents"]) == scenario.expected_incidents


def test_the_gate_fits_its_fifteen_minute_budget() -> None:
    assert gate.UP_BUDGET_SECONDS + sum(s.budget_seconds for s in gate.SCENARIOS) <= gate.BUDGET_SECONDS == 900


def test_unknown_check_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown checks"):
        gate.Scenario(name="x", covers=("P0",), stream=None, budget_seconds=1, checks=("nope",))


def test_commands_mount_the_freeze_and_pass_the_restart_evidence_only_to_the_restart_scenario(tmp_path: Path) -> None:
    code_root, run_dir = tmp_path / "freeze", tmp_path / "run"
    up = gate.up_arguments(code_root, run_dir, "7")
    assert up[up.index("--cluster-prefix") + 1] == "reh7-"
    assert up[up.index("--dev-mount") + 1] == str(code_root)
    assert up[up.index("--app") + 1] == "toy"

    by_name = {scenario.name: scenario for scenario in gate.SCENARIOS}
    restart = gate.run_arguments(code_root, run_dir, by_name["p51-restart-in-pause"], "rid")
    flag = restart.index("--restart-controller-in-injection-pause")
    assert restart[flag + 1] == str(run_dir / "results" / "rid" / "restart-in-pause.jsonl")
    plain = gate.run_arguments(code_root, run_dir, by_name["p52-source-repair-refresh"], "rid")
    assert "--restart-controller-in-injection-pause" not in plain
    assert plain[plain.index("--detection-timeout") + 1] == "75"


def test_preflight_refuses_a_busy_host_a_full_disk_and_a_non_checkout(tmp_path: Path) -> None:
    assert gate.preflight(REPO_ROOT, load=1.0, free_gib=500) == []
    assert any("load" in item for item in gate.preflight(REPO_ROOT, load=55.0, free_gib=500))
    assert any("GiB" in item for item in gate.preflight(REPO_ROOT, load=1.0, free_gib=10))
    assert any("not a harness checkout" in item for item in gate.preflight(tmp_path, load=1.0, free_gib=500))


# --- the controller restarts inside the injection pause ---------------------------------------------------


class _Inner:
    def __init__(self, events: list[str], pods: list[Any]) -> None:
        self.events, self.pods = events, pods

    def inject_after_resume(self, control: str, generation: str, inject: Any) -> dict[str, float]:
        self.events.append("pause")
        inject()
        self.events.append("resume")
        return {}

    def controller_pod(self, control: str) -> Any:
        return self.pods.pop(0) if len(self.pods) > 1 else self.pods[0]


def _kubectl(events: list[str], returncode: int = 0) -> Any:
    def run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        events.append(args[0])
        return subprocess.CompletedProcess(args, returncode, "", "boom" if returncode else "")

    return run


def test_restart_happens_after_the_pause_begins_and_before_the_fault(tmp_path: Path) -> None:
    events: list[str] = []
    pods = [SimpleNamespace(name="c-1", uid="u1"), SimpleNamespace(name="c-2", uid="u2")]
    evidence = tmp_path / "out" / "restart.jsonl"
    ops = RestartControllerInPauseOps(
        _Inner(events, pods),
        evidence,
        kubectl_runner=_kubectl(events),
        sleep=lambda _s: None,  # type: ignore[arg-type]
    )

    ops.inject_after_resume("ctl", "g1", lambda: events.append("inject"))

    assert events == ["pause", "delete", "wait", "inject", "resume"]
    row = json.loads(evidence.read_text(encoding="utf-8"))
    assert (row["old_uid"], row["new_uid"], row["generation"]) == ("u1", "u2", "g1")


def test_restart_fails_loudly_without_a_pod_or_a_replacement(tmp_path: Path) -> None:
    events: list[str] = []
    missing = RestartControllerInPauseOps(
        _Inner(events, [None]),
        tmp_path / "e.jsonl",
        kubectl_runner=_kubectl(events),
        sleep=lambda _s: None,  # type: ignore[arg-type]
    )
    with pytest.raises(PauseRestartError, match="no controller Pod"):
        missing.inject_after_resume("ctl", "g", lambda: events.append("inject"))
    assert "inject" not in events

    ticks = iter(range(0, 1000, 100))
    stuck = RestartControllerInPauseOps(
        _Inner(events, [SimpleNamespace(name="c-1", uid="u1")]),
        tmp_path / "e.jsonl",
        timeout_seconds=150,
        kubectl_runner=_kubectl(events),
        sleep=lambda _s: None,
        monotonic=lambda: float(next(ticks)),  # type: ignore[arg-type]
    )
    with pytest.raises(PauseRestartError, match="was not replaced"):
        stuck.inject_after_resume("ctl", "g", lambda: events.append("inject"))
    assert not (tmp_path / "e.jsonl").exists()


# --- scenario checks ---------------------------------------------------------------------------------------


def _ctx(tmp_path: Path, scenario: str, **extra: Any) -> gate.Context:
    chosen = next(s for s in gate.SCENARIOS if s.name == scenario)
    results = tmp_path / "results"
    results.mkdir(exist_ok=True)
    values: dict[str, Any] = {
        "run_dir": tmp_path,
        "results": results,
        "workspace": tmp_path / "ws",
        "scenario": chosen,
        "command": lambda argv: (1, "unused"),
    }
    values.update(extra)
    return gate.Context(**values)


def test_restart_check_fails_when_no_restart_was_recorded_or_the_resume_hung(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path, "p51-restart-in-pause")
    assert not gate.check_restart_in_pause(ctx)[0].ok

    (ctx.results / "restart-in-pause.jsonl").write_text(
        json.dumps({"old_uid": "a", "new_uid": "b"}) + "\n", encoding="utf-8"
    )
    assert all(check.ok for check in gate.check_restart_in_pause(ctx))

    logs = ctx.results / "incident" / "controller_logs"
    logs.mkdir(parents=True)
    (logs / "pod.log").write_text(
        "resume observation of namespace sdo-toy: context deadline exceeded\n", encoding="utf-8"
    )
    failed = [check for check in gate.check_restart_in_pause(ctx) if not check.ok]
    assert [check.name for check in failed] == ["no-blind-resume"]


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo_with_refresh(tmp_path: Path, *, rewrite_detector: bool) -> tuple[Path, str, str]:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    _git(workspace, "init", "-q")
    (workspace / "kubernetes").mkdir()
    (workspace / "kubernetes/policy.yaml").write_text("a\n", encoding="utf-8")
    _git(workspace, "add", "-A")
    _git(workspace, "commit", "-qm", "proposal")
    proposal = _git(workspace, "rev-parse", "HEAD")
    (workspace / ".sdo").mkdir()
    if rewrite_detector:
        detector = workspace / ".sdo/diagnostics/detectors/health/health-objective"
        detector.mkdir(parents=True)
        (detector / "detector.go").write_text("package x\n", encoding="utf-8")
    (workspace / ".sdo/outcomes.json").write_text("{}", encoding="utf-8")
    _git(workspace, "add", "-A")
    _git(workspace, "commit", "-qm", "outcome")
    return workspace, proposal, _git(workspace, "rev-parse", "HEAD")


@pytest.mark.parametrize(("rewrite", "ok"), [(True, True), (False, False)])
def test_source_repair_check_needs_a_health_detector_rewrite_between_proposal_and_outcome(
    tmp_path: Path, rewrite: bool, ok: bool
) -> None:
    workspace, proposal, outcome = _repo_with_refresh(tmp_path, rewrite_detector=rewrite)
    ctx = _ctx(tmp_path, "p52-source-repair-refresh", workspace=workspace)
    receipt = {"incident_id": "i1", "proposal_commit": proposal, "outcome_commit": outcome}
    (ctx.results / gate.STRICT_RECEIPT).write_text(json.dumps(receipt), encoding="utf-8")

    checks = gate.check_source_repair_refresh(ctx)

    assert all(check.ok for check in checks) is ok
    assert any("rewrote a health detector" in check.name for check in checks)


def test_source_repair_check_fails_without_a_source_proposal(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path, "p52-source-repair-refresh")
    (ctx.results / gate.STRICT_RECEIPT).write_text(
        json.dumps({"incident_id": "i1", "outcome_commit": "abc"}), encoding="utf-8"
    )
    assert not all(check.ok for check in gate.check_source_repair_refresh(ctx))


def test_rbac_check_denies_what_the_controller_cannot_delete(tmp_path: Path) -> None:
    seen: list[list[str]] = []

    def answer(argv: Any) -> tuple[int, str]:
        seen.append(list(argv))
        return (0, "no\n") if "replicasets.apps" in argv else (0, "yes\n")

    ctx = _ctx(tmp_path, "p53-program-write-rbac", command=answer, namespace="app", kubeconfig="kc")
    checks = gate.check_program_write_rbac(ctx)

    assert {check.name: check.ok for check in checks} == {
        "controller can delete replicasets.apps": False,
        "controller can patch replicasets.apps": False,
        "controller can delete pods": True,
    }
    assert all("system:serviceaccount:app-sdo:sdo-controller" in argv for argv in seen)


# --- the gate end to end, with fake commands ---------------------------------------------------------------


def _fake_gate(tmp_path: Path, commands: list[list[str]], *, fail: str | None = None, clock: Any = None) -> gate.Gate:
    def execute(argv: Any, **kwargs: Any) -> tuple[int, str]:
        commands.append(list(argv))
        return (1, "failed") if fail and fail in argv else (0, "yes")

    environment = SimpleNamespace(workspace=tmp_path / "ws", namespace="app", kubeconfig=Path("kc"))
    scenarios = (next(s for s in gate.SCENARIOS if s.name == "p53-program-write-rbac"),)
    return gate.Gate(
        tmp_path / "freeze",
        "3",
        tmp_path / "run",
        execute=execute,
        load_environment=lambda _run_dir: environment,
        scenarios=scenarios,
        **({"clock": clock} if clock else {}),
    )


def test_gate_brings_the_lane_up_once_resets_each_scenario_and_writes_the_summary(tmp_path: Path) -> None:
    commands: list[list[str]] = []
    runner = _fake_gate(tmp_path, commands)
    runner.run_dir.mkdir()

    summary = runner.run()

    verbs = [
        argv[0] if argv[0] == "kubectl" else argv[argv.index("benchmarks.sregym.fastloop") + 1] for argv in commands
    ]
    assert verbs[:2] == ["up", "reset"]
    assert verbs.count("up") == 1
    assert summary["passed"]
    assert summary["scenarios"][0]["covers"] == ("P53",)
    assert json.loads((runner.run_dir / gate.SUMMARY_FILENAME).read_text(encoding="utf-8"))["passed"] is True

    (runner.run_dir / "fastloop.json").write_text("{}", encoding="utf-8")
    commands.clear()
    runner.run()
    assert not any("up" in argv[argv.index("benchmarks.sregym.fastloop") + 1 :] for argv in commands if argv[0] == "uv")


def test_gate_fails_when_the_lane_cannot_come_up_or_a_scenario_check_fails(tmp_path: Path) -> None:
    commands: list[list[str]] = []
    down = _fake_gate(tmp_path, commands, fail="up")
    down.run_dir.mkdir()
    summary = down.run()
    assert not summary["passed"]
    assert summary["scenarios"][0]["status"] == "error"
    assert len(commands) == 1  # no scenario runs on a lane that did not come up

    denied = _fake_gate(tmp_path / "second", commands, fail="can-i")
    denied.run_dir.mkdir(parents=True)
    (denied.run_dir / "fastloop.json").write_text("{}", encoding="utf-8")
    result = denied.run()
    assert not result["passed"]
    assert result["scenarios"][0]["status"] == "fail"


def test_gate_fails_when_it_exceeds_its_budget(tmp_path: Path) -> None:
    ticks = iter(float(n * 400) for n in range(100))
    runner = _fake_gate(tmp_path, [], clock=lambda: next(ticks))
    runner.run_dir.mkdir()
    (runner.run_dir / "fastloop.json").write_text("{}", encoding="utf-8")

    summary = runner.run()

    assert not summary["within_budget"]
    assert not summary["passed"]


def test_dry_run_lists_every_command_without_touching_a_cluster(tmp_path: Path) -> None:
    text = gate.dry_run(tmp_path / "freeze", tmp_path / "run", "3")
    assert text.count(" reset ") == len(gate.SCENARIOS)
    assert "--restart-controller-in-injection-pause" in text
    assert "covers P53" in text


def test_fastloop_run_wraps_the_cluster_only_when_asked_to_restart_in_the_pause(tmp_path: Path) -> None:
    from benchmarks.sregym.fastloop.cli import _cluster_ops, build_parser

    parser = build_parser()
    base = ["run", "--run-dir", "x", "--agent", "sdo", "--healthy-baseline"]
    off = parser.parse_args(base)
    on = parser.parse_args([*base, "--restart-controller-in-injection-pause", str(tmp_path / "e.jsonl")])

    assert off.restart_controller_in_injection_pause is None
    assert type(_cluster_ops(off, "app")).__name__ == "HealthyBaselineCaptureOps"
    wrapped = _cluster_ops(on, "app")
    assert type(wrapped).__name__ == "RestartControllerInPauseOps"
    assert type(wrapped._inner).__name__ == "HealthyBaselineCaptureOps"  # type: ignore[attr-defined]


def test_up_names_the_freeze_images_and_preflight_names_missing_ones(tmp_path: Path) -> None:
    up = gate.up_arguments(tmp_path / "freeze", tmp_path / "run", "3", "le-h96")
    assert up[up.index("--controller-image") + 1] == "sdo-controller:le-h96"
    assert up[up.index("--responder-image") + 1] == "sdo-sregym-responder:le-h96"
    assert up[up.index("--validator-image") + 1] == "sdo-detector-validator:le-h96"
    problems = gate.preflight(REPO_ROOT, load=1.0, free_gib=500, absent_images=["sdo-controller:le-h96"])
    assert any("missing images" in item and "SDO_SCRIPTED_WORKER=1" in item for item in problems)
