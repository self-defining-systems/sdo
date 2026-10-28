from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.skilleval.metrics import (
    Usage,
    check_replay_notes,
    disallowed_paths,
    hand_repairs,
    memory_check_passed,
    programs_written,
    replay_failures,
)
from benchmarks.sregym.skilleval.runner import docker_command, table, toolchain_image, wait_for_host_load
from benchmarks.sregym.skilleval.scenario import Scenario, ScenarioSource

if TYPE_CHECKING:
    from pathlib import Path


def _scenario(session_mode: str = "resume") -> Scenario:
    return Scenario(
        scenario_id="synthetic",
        description="",
        expectation="",
        image="sdo-sregym-responder:le-test",
        incident_id="synthetic-incident",
        session_digest="0" * 32,
        thread_id="thread" if session_mode == "resume" else "",
        checkout_path="/workspace/proposal/checkout",
        outcome_commit="a" * 40,
        source=ScenarioSource(host="", row_dir="/row", incident="000", problem_id="", encounter="first"),
        session_mode=session_mode,
    )


def _environment(command: list[str]) -> dict[str, str]:
    pairs = [command[index + 1] for index, item in enumerate(command) if item == "-e"]
    return dict(pair.split("=", 1) for pair in pairs)


def _mounts(command: list[str]) -> dict[str, str]:
    specs = [command[index + 1] for index, item in enumerate(command) if item == "-v"]
    return {spec.split(":")[1]: spec.split(":")[0] for spec in specs}


def test_docker_command_mounts_the_scenario_where_the_live_job_had_it(tmp_path: Path) -> None:
    command = docker_command(
        scenario=_scenario(),
        run_dir=tmp_path / "run",
        skills=tmp_path / "skills",
        toolchain=tmp_path / "toolchain",
        auth=tmp_path / "auth.json",
        model="gpt-6-luna",
        name="skilleval-test",
    )

    mounts = _mounts(command)
    environment = _environment(command)
    assert mounts["/workspace/proposal/checkout"] == str(tmp_path / "run" / "checkout")
    assert mounts["/sdo/session"] == str(tmp_path / "run" / "session")
    assert mounts["/opt/sdo/sdo/skills"] == str(tmp_path / "skills")
    assert mounts["/sdo/credentials/auth.json"] == str(tmp_path / "auth.json")
    assert environment["CODEX_HOME"] == "/sdo/session/codex"
    assert environment["SDO_REFLECTION_MODEL"] == "gpt-6-luna"
    assert environment["SDO_BASE_COMMIT"] == "a" * 40
    assert environment["SDO_PHASE"] == "reflection"
    assert command[command.index("-w") + 1] == "/workspace/proposal/checkout"
    assert command[-5:] == ["sdo-sregym-responder:le-test", "-n", "10", "python3", "/skilleval/entry.py"]
    assert command[command.index("--entrypoint") + 1] == "nice"
    assert environment["GOMAXPROCS"] == "4"


def test_fresh_reflection_uses_a_private_codex_home(tmp_path: Path) -> None:
    command = docker_command(
        scenario=_scenario("fresh"),
        run_dir=tmp_path,
        skills=tmp_path,
        toolchain=tmp_path,
        auth=tmp_path / "auth.json",
        model="gpt-6-luna",
        name="skilleval-test",
    )

    assert _environment(command)["CODEX_HOME"] == "/tmp/codex"


def test_toolchain_image_is_the_controller_of_the_same_build() -> None:
    assert toolchain_image("sdo-sregym-responder:le-p4-h919-abc") == "sdo-controller:le-p4-h919-abc"


def test_usage_sums_every_logged_reflection_turn() -> None:
    usage = Usage.from_lines(
        [
            {
                "model_requests": 3,
                "duration_seconds": 10.5,
                "shell_commands": 2,
                "shell_command_lines": ["ls", "cat"],
                "usage": {"input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 7},
            },
            {"model_requests": 1, "duration_seconds": 1.5, "usage": {"input_tokens": 10, "output_tokens": 1}},
        ]
    )

    assert usage == Usage(
        model_calls=4,
        input_tokens=110,
        cached_input_tokens=80,
        output_tokens=8,
        seconds=12.0,
        shell_commands=2,
        shell_command_lines=("ls", "cat"),
    )


def test_diff_checks_follow_the_reflection_ownership_classes() -> None:
    changed = [
        ".sdo/playbooks/drift/mitigate.go",
        ".sdo/playbooks/drift/README.md",
        ".sdo/playbooks/health-objective/README.md",
        ".sdo/corpus/drift-case/open.json",
        ".sdo/diagnostics/detectors/incidents/drift/detector.go",
        ".sdo/diagnostics/detectors/health/workload/detector.go",
        ".sdo/outcomes/abc.json",
        "kubernetes/deployment.yaml",
    ]

    assert disallowed_paths(changed) == [
        ".sdo/diagnostics/detectors/health/workload/detector.go",
        ".sdo/outcomes/abc.json",
        ".sdo/playbooks/health-objective/README.md",
        "kubernetes/deployment.yaml",
    ]
    assert programs_written(changed) == ["drift"]


def test_hand_repairs_exclude_program_plans_and_failed_actions() -> None:
    outcome = {
        "repair_actions": [
            {"kind": "program-plan", "success": True, "target": "Deployment/a"},
            {"kind": "kubectl-patch", "success": True, "target": "Deployment/b"},
            {"kind": "kubectl-patch", "success": True, "target": "Deployment/b"},
            {"kind": "kubectl-apply", "success": False, "target": "Deployment/c"},
        ]
    }

    assert hand_repairs(outcome) == ["Deployment/b"]


def test_memory_check_pass_requires_exit_zero_within_time() -> None:
    assert memory_check_passed(None) is None
    assert memory_check_passed({"exit_code": 0, "timed_out": False}) is True
    assert memory_check_passed({"exit_code": 1, "timed_out": False}) is False


def test_table_renders_one_row_per_run() -> None:
    row = {
        "scenario": "synthetic",
        "label": "baseline",
        "status": "completed",
        "usage": {"model_calls": 5, "input_tokens": 2_000_000, "cached_input_tokens": 1_500_000, "output_tokens": 4000},
        "wall_seconds": 90.0,
        "memory_check_pass": True,
        "programs_written": ["drift"],
        "hand_repairs": ["Deployment/b", "Deployment/c"],
        "replay_cases": ["drift-case"],
        "replay": {"cases": 1, "passed": 1, "failures": []},
        "check_open_match": {"cases": 2, "match": 1},
        "disallowed_paths": [],
    }

    lines = table([row]).splitlines()

    assert (
        lines[-1] == "| synthetic | baseline | completed | 5 | 2.0 (1.5) | 4.0 | 1.5 | pass | 1/2 | 1/1 | 1/2 | yes |"
    )


def test_replay_judges_each_case_by_its_own_root_cause_programs() -> None:
    rows = [
        {"program_id": "drift", "case_id": "a", "expected_match": True, "faulted": {"result": "Match"},
         "healthy": {"result": "NoMatch"}},
        {"program_id": "webhook", "case_id": "a", "expected_match": False, "faulted": {"result": "Unknown"},
         "healthy": {"result": "Unknown"}},
        {"program_id": "detector:health", "case_id": "a", "expected_match": True, "faulted": {"result": ""}},
        {"program_id": "drift", "case_id": "b", "expected_match": True, "faulted": {"result": "Unknown"},
         "healthy": {"result": "NoMatch"}},
    ]  # fmt: skip

    assert replay_failures(rows) == (2, ["b: drift faulted=Unknown"])


def test_check_replay_notes_keep_this_incidents_cases() -> None:
    stdout = json.dumps(
        {
            "mitigation_replay_notes": [
                {
                    "program_id": "drift",
                    "case_id": "a",
                    "status": "STATUS_REPLAYED",
                    "effects": ["replay status: replayed", "open diagnosis: Match", "post diagnosis: NoMatch"],
                },
                {"program_id": "drift", "case_id": "older", "status": "STATUS_REPLAYED", "effects": []},
            ]
        }
    )

    assert check_replay_notes({"stdout": stdout}, ["a"]) == [
        {"program_id": "drift", "case_id": "a", "status": "STATUS_REPLAYED", "open": "Match", "post": "NoMatch"}
    ]


def test_runs_start_only_below_the_load_limit() -> None:
    loads = iter([82.0, 41.0, 39.5])
    slept: list[float] = []

    wait_for_host_load(40.0, load=lambda: next(loads), sleep=slept.append, poll_seconds=30.0)

    assert slept == [30.0, 30.0]
