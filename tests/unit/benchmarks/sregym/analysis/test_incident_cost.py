from __future__ import annotations

import csv
import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from benchmarks.sregym.analysis.incident_cost import (
    WARM_PROMPT_MARKER,
    IncidentCostError,
    TokenUsage,
    _reflection_usage,
    build_report,
    first_mutation_at,
    last_mutation_done_at,
    load_codex_runs,
    load_sdo_pipeline,
    main,
    read_verdict,
)


def test_native_reflection_usage_is_read_from_ledger_without_rollouts() -> None:
    usage = _reflection_usage({"native_ledger": {"reflection_decision": {"usage": _usage(42, 7)}}}, [])
    assert usage.input_tokens == 42
    assert usage.output_tokens == 7


PROBLEM_A = "missing_configmap_hotel_reservation"
PROBLEM_B = "missing_configmap_mongodb_rate_hotel_reservation"


def _usage(input_tokens: int, output_tokens: int, cached: int = 0) -> dict[str, int]:
    return {"input_tokens": input_tokens, "cached_input_tokens": cached, "output_tokens": output_tokens}


def _results_dir(experiment: Path, sequence: int, problem: str, agent: str) -> Path:
    results = experiment / "runs" / f"{sequence:06d}_{problem}" / "worker_0" / "results"
    (results / agent / problem / "run_1").mkdir(parents=True)
    return results


def _write_csv(results: Path, agent: str, *, diagnosis: bool, mitigation: bool, injected: float, submitted: float):
    with (results / f"{agent}_ALL_results.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["Diagnosis.success", "Mitigation.success", "fault_injected_at", "mitigation_submitted_at"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Diagnosis.success": str(diagnosis),
                "Mitigation.success": str(mitigation),
                "fault_injected_at": injected,
                "mitigation_submitted_at": submitted,
            }
        )


def _rollout(path: Path, prompt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {"type": "session_meta", "payload": {"id": "s"}},
        {
            "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": prompt}]},
        },
    ]
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


def _sdo_stage(
    pipeline: Path,
    index: int,
    name: str,
    problem: str,
    *,
    responder: dict[str, int],
    reflection: dict[str, int],
    warm: bool,
    incident_detectors: int,
    passed: bool = True,
    lifecycle: dict[str, int] | None = None,
) -> Path:
    stage = pipeline / f"stage_{index}_{name}"
    results = _results_dir(stage, 0, problem, "sdo_codex")
    _write_csv(
        results, "sdo_codex", diagnosis=True, mitigation=passed, injected=1000.0, submitted=1000.0 + 50 * (index + 1)
    )
    run = results / "sdo_codex" / problem / "run_1"
    session = f"session-{index}"
    receipt = {
        "incident_resolution_seconds": 100.0 + index,
        "usage": responder,
        "reflection_usage": reflection,
        "reflection_attempts": 0 if warm else 1,
        "reflection_skipped_reason": "repeated exact-match success" if warm else None,
        "phase_timings_seconds": {"post_recovery_learning_and_receipt": 5.0 if warm else 300.0},
        "memory_reuse": {"warm_path": warm, "match_reasons": ["exact-fingerprint"] if warm else []},
        "responder_session_id": session,
    }
    (run / "sdo_production_receipt_strict.json").write_text(json.dumps(receipt), encoding="utf-8")
    prompt = f"You are the SDO incident responder. {WARM_PROMPT_MARKER} Playbook..." if warm else "Cold path."
    _rollout(run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27" / f"rollout-x-{session}.jsonl", prompt)
    if not warm:
        usage_dir = run / "sdo_runtime" / "usage"
        usage_dir.mkdir(parents=True)
        (usage_dir / "controller-turns.jsonl").write_text(
            json.dumps({"duration_seconds": 240.0, "usage": reflection}) + "\n", encoding="utf-8"
        )
    if lifecycle is not None:
        (run / "sdo_turn_usage.jsonl").write_text(
            json.dumps({"duration_seconds": 900.0, "usage": lifecycle}) + "\n", encoding="utf-8"
        )
    sdo = stage / "application_workspace" / ".sdo"
    detectors = [{"id": "health-objective", "class": "health"}] + [
        {"id": f"incident-{n}", "class": "incident"} for n in range(incident_detectors)
    ]
    for detector in detectors:
        directory = sdo / "diagnostics" / "detectors" / ("health" if detector["class"] == "health" else "incidents")
        (directory / detector["id"]).mkdir(parents=True)
        (directory / detector["id"] / "manifest.yaml").write_text(yaml.safe_dump(detector), encoding="utf-8")
    for playbook in ["health-objective"] + [f"incident-{n}" for n in range(incident_detectors)]:
        (sdo / "playbooks" / playbook).mkdir(parents=True)
        (sdo / "playbooks" / playbook / "README.md").write_text("# playbook\n", encoding="utf-8")
    (sdo / "playbooks" / "README.md").write_text("# index\n", encoding="utf-8")
    return stage


def _codex_experiment(root: Path, runs: list[tuple[str, bool, int]]) -> Path:
    for sequence, (problem, passed, tokens) in enumerate(runs):
        results = _results_dir(root, sequence, problem, "codex")
        _write_csv(results, "codex", diagnosis=True, mitigation=passed, injected=0.0, submitted=200.0)
        (results / "codex" / problem / "run_1" / f"codex_results_{problem}_1.json").write_text(
            json.dumps({"usage_metrics": _usage(tokens, 0, cached=tokens // 2)}), encoding="utf-8"
        )
    return root


@pytest.fixture
def pipeline(tmp_path: Path) -> Path:
    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-variants"
    _sdo_stage(
        root,
        0,
        "first",
        PROBLEM_A,
        responder=_usage(600, 100),
        reflection=_usage(1_500, 100),
        warm=False,
        incident_detectors=1,
        lifecycle=_usage(10_000, 0),
    )
    _sdo_stage(root, 1, "repeat", PROBLEM_A, responder=_usage(200, 0), reflection={}, warm=True, incident_detectors=1)
    _sdo_stage(root, 2, "variant", PROBLEM_B, responder=_usage(900, 0), reflection={}, warm=False, incident_detectors=2)
    aborted = root / "stage_1_repeat.20260927_141722"
    aborted.mkdir()
    state = {
        "stages": [
            {"index": index, "name": name, "experiment_dir": f"/elsewhere/{root.name}/stage_{index}_{name}"}
            for index, name in enumerate(["first", "repeat", "variant"])
        ]
    }
    (root / "pipeline_state.json").write_text(json.dumps(state), encoding="utf-8")
    return root


@pytest.fixture
def codex_dirs(tmp_path: Path) -> list[Path]:
    return [
        _codex_experiment(tmp_path / "codex_a1", [(PROBLEM_A, True, 800), (PROBLEM_B, True, 1_000)]),
        _codex_experiment(tmp_path / "codex_a2", [(PROBLEM_A, False, 1_200)]),
    ]


def test_loads_stage_metrics_from_receipts_rollouts_and_memory(pipeline: Path) -> None:
    stages = load_sdo_pipeline(pipeline)

    assert [(stage.index, stage.problem_id) for stage in stages] == [(0, PROBLEM_A), (1, PROBLEM_A), (2, PROBLEM_B)]
    first, repeat, variant = stages
    assert first.verdict.raw_incl_judge_seconds == 50.0
    assert first.verdict.passed
    assert first.incident_resolution_seconds == 100.0
    assert first.responder.total() == 700
    assert first.reflection.total() == 1_600
    assert first.reflection_turn_seconds == 240.0
    assert first.learning_seconds == 300.0
    assert first.warm_path is False
    assert first.warm_prompt is False
    assert first.lifecycle.total() == 10_000
    assert repeat.warm_path is True
    assert repeat.warm_prompt is True
    assert repeat.reflection_skipped_reason is not None
    assert repeat.reflection.total() == 0
    assert variant.memory is not None
    assert (variant.memory.detectors, variant.memory.incident_detectors, variant.memory.playbooks) == (3, 2, 3)


def test_cumulative_totals_and_break_even_against_codex(pipeline: Path, codex_dirs: list[Path]) -> None:
    codex_runs = [run for directory in codex_dirs for run in load_codex_runs(directory)]
    report = build_report(load_sdo_pipeline(pipeline), codex_runs)

    cost_a = report.codex[PROBLEM_A]
    assert (cost_a.runs, cost_a.passed, cost_a.mean_tokens, cost_a.mean_raw_incl_judge_seconds) == (2, 1, 1_000, 200.0)
    assert [row.sdo_incident_tokens for row in report.cumulative] == [700, 900, 1_800]
    assert [row.sdo_tokens_with_learning for row in report.cumulative] == [2_300, 2_500, 3_400]
    assert [row.codex_tokens for row in report.cumulative] == [1_000, 2_000, 3_000]
    # Without stage timestamps the judge-free TTM is unknown, so no cumulative time is claimed.
    assert [row.sdo_ttm_seconds for row in report.cumulative] == [None, None, None]
    assert report.lifecycle_tokens == 10_000

    by_key = {(item.measure, item.includes_lifecycle): item for item in report.break_even}
    assert by_key[("incident tokens (responder)", False)].stage == 0
    learning = by_key[("total tokens incl. learning", False)]
    assert learning.stage is None
    assert learning.final_gap == 400
    # The only repeat stage saved 1,000 - 200 = 800 tokens, so one more repeat closes the gap.
    assert learning.projected_extra_repeats == 1
    with_lifecycle = by_key[("incident tokens (responder)", True)]
    assert with_lifecycle.stage is None
    assert with_lifecycle.final_gap == 10_000 + 1_800 - 3_000


def test_uncached_measure_subtracts_cached_input(pipeline: Path, codex_dirs: list[Path]) -> None:
    codex_runs = [run for directory in codex_dirs for run in load_codex_runs(directory)]

    report = build_report(load_sdo_pipeline(pipeline), codex_runs, uncached=True)

    assert report.codex[PROBLEM_A].mean_tokens == 500


def test_lifecycle_override_replaces_discovered_lifecycle(pipeline: Path) -> None:
    report = build_report(load_sdo_pipeline(pipeline), [], lifecycle_override=TokenUsage(input_tokens=5))

    assert report.lifecycle_tokens == 5
    assert all(row.codex_tokens is None for row in report.cumulative)


def test_cli_prints_tables_and_writes_json(pipeline: Path, codex_dirs: list[Path], tmp_path: Path, capsys) -> None:
    output = tmp_path / "report.json"

    assert main([str(pipeline), "--codex", *map(str, codex_dirs), "--json", str(output), "--include-invalid"]) == 0

    printed = capsys.readouterr().out
    assert "SDO stages" in printed
    assert "Break-even" in printed
    assert "PASS D+M+" in printed
    assert json.loads(output.read_text(encoding="utf-8"))["lifecycle_tokens"] == 10_000


def test_rejects_directory_without_stages(tmp_path: Path) -> None:
    with pytest.raises(IncidentCostError):
        load_sdo_pipeline(tmp_path)


def test_token_usage_validates_counts() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        TokenUsage(input_tokens=-1)
    with pytest.raises(TypeError):
        TokenUsage(output_tokens=1.5)  # type: ignore[arg-type]


def test_reflection_turn_seconds_count_only_the_stage_incident_in_a_shared_usage_log(tmp_path: Path) -> None:
    """A persistent controller's usage log accumulates every incident's reflection turns."""

    from benchmarks.sregym.analysis.incident_cost import incident_worktree_dirname

    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-persistent"
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=_usage(1, 0), reflection=_usage(1, 0), warm=False, incident_detectors=1
    )
    run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
    receipt_path = run / "sdo_production_receipt_strict.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["incident_id"] = "incident-2"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    records = [
        {"cwd": f"/workspace/wt/{incident_worktree_dirname('incident-1')}", "duration_seconds": 240.0, "usage": {}},
        {"cwd": f"/workspace/wt/{incident_worktree_dirname('incident-2')}", "duration_seconds": 30.0, "usage": {}},
        {"cwd": f"/workspace/wt/{incident_worktree_dirname('incident-2')}", "duration_seconds": 12.0, "usage": {}},
    ]
    (run / "sdo_runtime" / "usage" / "controller-turns.jsonl").write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )

    (only,) = load_sdo_pipeline(root)

    assert only.reflection_turn_seconds == 42.0


def _timing_csv(results: Path, **row: object) -> None:
    results.mkdir(parents=True, exist_ok=True)
    with (results / "codex_ALL_results.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def test_judge_excluded_time_subtracts_the_diagnosis_grading_wait(tmp_path: Path) -> None:
    # Diagnosis POSTed 20 s after injection; the conductor finished grading it and opened the
    # mitigation stage at TTL = 37 s; the mitigation POST came at 96 s.
    _timing_csv(
        tmp_path,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1000.0,
            "diagnosis_submitted_at": 1020.0,
            "TTL": 37.0,
            "mitigation_submitted_at": 1096.0,
        },
    )

    verdict = read_verdict(tmp_path)

    assert verdict is not None
    assert verdict.raw_incl_judge_seconds == pytest.approx(96.0)
    assert verdict.diagnosis_seconds == pytest.approx(20.0)
    assert verdict.grading_wait_seconds == pytest.approx(17.0)
    assert verdict.judge_excluded_seconds == pytest.approx(79.0)


@pytest.mark.parametrize("ttl", [15.0, None])
def test_deferred_diagnosis_grading_has_no_grading_wait(tmp_path: Path, ttl: float | None) -> None:
    # With defer_diagnosis_grading the mitigation stage opens at the diagnosis POST, so the
    # mitigation POST never sits behind the judge, whatever the TTL column says.
    row: dict[str, object] = {
        "Diagnosis.success": "True",
        "Mitigation.success": "True",
        "fault_injected_at": 1000.0,
        "diagnosis_submitted_at": 1020.0,
        "mitigation_submitted_at": 1096.0,
        "diagnosis_grading_deferred": "True",
    }
    if ttl is not None:
        row["TTL"] = ttl
    _timing_csv(tmp_path, **row)

    verdict = read_verdict(tmp_path)

    assert verdict is not None
    assert verdict.grading_wait_seconds == 0.0
    assert verdict.judge_excluded_seconds == pytest.approx(96.0)


def test_judge_excluded_time_is_unknown_without_stage_timestamps(tmp_path: Path) -> None:
    _timing_csv(
        tmp_path,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1000.0,
            "mitigation_submitted_at": 1096.0,
        },
    )

    verdict = read_verdict(tmp_path)

    assert verdict is not None
    assert verdict.grading_wait_seconds is None
    assert verdict.judge_excluded_seconds is None


def _tool_rollout(path: Path, calls: list[tuple[str, dict[str, object]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps({"timestamp": ts, "type": "response_item", "payload": payload}) + "\n" for ts, payload in calls
        ),
        encoding="utf-8",
    )


def _code_mode(cmd: str) -> dict[str, object]:
    return {
        "type": "custom_tool_call",
        "name": "exec",
        "input": f"const r = await tools.exec_command({{cmd:{json.dumps(cmd)}}});",
    }


def test_first_mutation_is_the_first_state_changing_command_after_injection(tmp_path: Path) -> None:
    rollout = tmp_path / "rollout-a.jsonl"
    _tool_rollout(
        rollout,
        [
            (
                "2026-09-27T12:00:05.000Z",
                _code_mode("kubectl -n hotel-reservation delete pod stale"),
            ),  # before injection
            ("2026-09-27T12:00:20.000Z", _code_mode("kubectl -n hotel-reservation get cm && kubectl logs deploy/geo")),
            (
                "2026-09-27T12:00:30.000Z",
                _code_mode("bash -n .sdo/playbooks/x/scripts/repair.sh; cat scripts/repair.sh"),
            ),
            ("2026-09-27T12:00:40.000Z", _code_mode("python3 -m benchmarks.sregym.adapter.submission diagnosis 'x'")),
            ("2026-09-27T12:00:51.500Z", _code_mode("kubectl create configmap mongo-geo-script -n hotel-reservation")),
            ("2026-09-27T12:01:10.000Z", _code_mode("kubectl -n hotel-reservation rollout restart deploy/geo")),
        ],
    )
    injected = 1790510410.0  # 2026-09-27T12:00:10Z

    assert first_mutation_at([rollout], after=injected) == pytest.approx(injected + 41.5)


@pytest.mark.parametrize(
    "call",
    [
        {
            "type": "function_call",
            "name": "exec_command",
            "arguments": json.dumps({"cmd": "bash .sdo/playbooks/m/scripts/repair.sh hotel-reservation geo"}),
        },
        _code_mode("cd /app && .sdo/playbooks/m/scripts/repair.sh hotel-reservation"),
        _code_mode("kubectl --namespace=hotel-reservation patch deployment geo -p '{}'"),
        _code_mode("cat <<'EOF' | kubectl apply -f -\nkind: ConfigMap\nEOF"),
    ],
)
def test_first_mutation_recognizes_playbook_repairs_and_kubectl_writes(tmp_path: Path, call: dict[str, object]) -> None:
    rollout = tmp_path / "rollout-b.jsonl"
    _tool_rollout(rollout, [("2026-09-27T12:00:30.000Z", call)])

    assert first_mutation_at([rollout], after=1790510410.0) == pytest.approx(1790510430.0)


def test_codex_runs_report_when_the_mitigation_was_applied(tmp_path: Path) -> None:
    results = tmp_path / "exp" / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results"
    _timing_csv(
        results,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1790510410.0,
            "diagnosis_submitted_at": 1790510440.0,
            "TTL": 45.0,
            "mitigation_submitted_at": 1790510500.0,
        },
    )
    _tool_rollout(
        results / "codex" / PROBLEM_A / "run_1" / "sessions" / "rollout-c.jsonl",
        [("2026-09-27T12:01:00.000Z", _code_mode("kubectl apply -f cm.yaml"))],
    )

    (run,) = load_codex_runs(tmp_path / "exp")

    assert run.verdict.mitigation_applied_seconds == pytest.approx(50.0)
    assert run.verdict.judge_excluded_seconds == pytest.approx(90.0 - 15.0)


def test_json_report_carries_judge_excluded_stage_time(pipeline: Path, tmp_path: Path) -> None:
    out = tmp_path / "report.json"
    main([str(pipeline), "--json", str(out), "--include-invalid"])

    for stage in json.loads(out.read_text(encoding="utf-8"))["stages"]:
        assert "judge_excluded_seconds" in stage["verdict"]
        assert "mitigation_applied_seconds" in stage["verdict"]


def test_headline_ttm_is_the_judge_excluded_time_when_nothing_mitigating_ran_during_grading(tmp_path: Path) -> None:
    results = tmp_path / "exp" / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results"
    _timing_csv(
        results,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1790510410.0,
            "diagnosis_submitted_at": 1790510440.0,
            "TTL": 45.0,
            "mitigation_submitted_at": 1790510500.0,
        },
    )
    rollout = results / "codex" / PROBLEM_A / "run_1" / "sessions" / "rollout-c.jsonl"
    _tool_rollout(
        rollout,
        [
            ("2026-09-27T12:00:20.000Z", {**_code_mode("kubectl apply -f cm.yaml"), "call_id": "a"}),
            ("2026-09-27T12:00:25.000Z", {"type": "custom_tool_call_output", "call_id": "a", "output": "ok"}),
        ],
    )

    (run,) = load_codex_runs(tmp_path / "exp")

    assert run.verdict.diagnosis_seconds == pytest.approx(30.0)
    assert run.verdict.last_mitigation_seconds == pytest.approx(15.0)
    assert run.verdict.ttm_seconds == pytest.approx(90.0 - 15.0)


def test_headline_ttm_keeps_mitigation_work_finished_during_diagnosis_grading(tmp_path: Path) -> None:
    """Subtracting the whole grading wait would undercount an agent that kept repairing meanwhile."""

    results = tmp_path / "exp" / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results"
    _timing_csv(
        results,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1790510410.0,  # 12:00:10Z
            "diagnosis_submitted_at": 1790510420.0,  # grading until 12:01:00Z (TTL 50)
            "TTL": 50.0,
            "mitigation_submitted_at": 1790510470.0,
        },
    )
    rollout = results / "codex" / PROBLEM_A / "run_1" / "sessions" / "rollout-c.jsonl"
    _tool_rollout(
        rollout,
        [
            ("2026-09-27T12:00:22.000Z", {**_code_mode("kubectl create cm x"), "call_id": "a"}),
            ("2026-09-27T12:00:23.000Z", {"type": "custom_tool_call_output", "call_id": "a", "output": ""}),
            ("2026-09-27T12:00:30.000Z", {**_code_mode("kubectl rollout restart deploy/geo"), "call_id": "b"}),
            ("2026-09-27T12:00:48.000Z", {"type": "custom_tool_call_output", "call_id": "b", "output": ""}),
        ],
    )

    (run,) = load_codex_runs(tmp_path / "exp")

    assert run.verdict.raw_incl_judge_seconds == pytest.approx(60.0)
    assert run.verdict.judge_excluded_seconds == pytest.approx(20.0)
    assert run.verdict.last_mitigation_seconds == pytest.approx(38.0)
    assert run.verdict.ttm_seconds == pytest.approx(38.0)


def test_last_mutation_ignores_reads_and_work_after_the_mitigation_submission(tmp_path: Path) -> None:
    rollout = tmp_path / "rollout-d.jsonl"
    _tool_rollout(
        rollout,
        [
            ("2026-09-27T12:00:05.000Z", {**_code_mode("kubectl delete pod stale"), "call_id": "pre"}),
            ("2026-09-27T12:00:06.000Z", {"type": "custom_tool_call_output", "call_id": "pre", "output": ""}),
            ("2026-09-27T12:00:20.000Z", {**_code_mode("kubectl patch deploy geo -p '{}'"), "call_id": "fix"}),
            ("2026-09-27T12:00:24.000Z", {"type": "custom_tool_call_output", "call_id": "fix", "output": ""}),
            ("2026-09-27T12:00:30.000Z", {**_code_mode("kubectl get pods"), "call_id": "read"}),
            ("2026-09-27T12:00:31.000Z", {"type": "custom_tool_call_output", "call_id": "read", "output": ""}),
            ("2026-09-27T12:01:30.000Z", {**_code_mode("kubectl delete pod later"), "call_id": "post"}),
            ("2026-09-27T12:01:31.000Z", {"type": "custom_tool_call_output", "call_id": "post", "output": ""}),
        ],
    )
    injected = 1790510410.0  # 12:00:10Z

    assert last_mutation_done_at([rollout], after=injected, before=injected + 60) == pytest.approx(injected + 14.0)
    # A mutating call still running at the submission is counted up to the submission.
    assert last_mutation_done_at([rollout], after=injected, before=injected + 12) == pytest.approx(injected + 12.0)


def test_headline_ttm_is_unknown_without_the_grading_wait(tmp_path: Path) -> None:
    _timing_csv(
        tmp_path,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 0.0,
            "mitigation_submitted_at": 9.0,
        },
    )

    verdict = read_verdict(tmp_path)

    assert verdict is not None
    assert verdict.ttm_seconds is None


def test_cumulative_time_and_tables_use_the_judge_free_ttm(tmp_path: Path, capsys) -> None:
    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-persistent"
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=_usage(1, 0), reflection=_usage(1, 0), warm=False, incident_detectors=1
    )
    results = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results"
    (results / "sdo_codex_ALL_results.csv").unlink()
    _timing_csv(
        results,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 1000.0,
            "diagnosis_submitted_at": 1010.0,
            "TTL": 40.0,
            "mitigation_submitted_at": 1070.0,
        },
    )
    codex = tmp_path / "codex"
    codex_results = _results_dir(codex, 0, PROBLEM_A, "codex")
    _timing_csv(
        codex_results,
        **{
            "Diagnosis.success": "True",
            "Mitigation.success": "True",
            "fault_injected_at": 0.0,
            "diagnosis_submitted_at": 20.0,
            "TTL": 50.0,
            "mitigation_submitted_at": 100.0,
        },
    )

    report = build_report(load_sdo_pipeline(root), load_codex_runs(codex))

    assert report.stages[0].verdict.diagnosis_seconds == pytest.approx(10.0)
    assert [row.sdo_ttm_seconds for row in report.cumulative] == [pytest.approx(40.0)]
    assert [row.codex_ttm_seconds for row in report.cumulative] == [pytest.approx(70.0)]
    assert report.codex[PROBLEM_A].mean_ttd_seconds == pytest.approx(20.0)
    assert report.codex[PROBLEM_A].mean_ttm_seconds == pytest.approx(70.0)

    main([str(root), "--codex", str(codex), "--include-invalid"])
    printed = capsys.readouterr().out
    for column in ("ttd_s", "ttm_s", "raw_incl_judge_s", "last_mut_s", "mean_ttd_s", "mean_ttm_s", "sdo_ttm_s"):
        assert column in printed
    assert "primary" not in printed


@pytest.mark.parametrize(
    "command",
    [
        "kubectl -n hotel-reservation rollout status deployment/mongodb-geo --timeout=30s",
        "kubectl create configmap x --from-file=a.sh --dry-run=client -o yaml > cm.yaml",
        "kubectl apply --dry-run=server -f cm.yaml",
        "kubectl rollout history deployment/geo",
    ],
)
def test_read_only_kubectl_forms_are_not_mutations(tmp_path: Path, command: str) -> None:
    rollout = tmp_path / "rollout-e.jsonl"
    _tool_rollout(rollout, [("2026-09-27T12:00:30.000Z", _code_mode(command))])

    assert first_mutation_at([rollout], after=1790510410.0) is None


def _token_turns(path: Path, turns: list[list[dict[str, int]]]) -> None:
    """Append Codex turns (``task_started`` then one ``token_count`` per model response) to a rollout."""

    total = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0}
    with path.open("a", encoding="utf-8") as stream:
        for requests in turns:
            stream.write(json.dumps({"type": "event_msg", "payload": {"type": "task_started"}}) + "\n")
            for last in requests:
                total = {key: total[key] + last[key] for key in total}
                info = {"total_token_usage": dict(total), "last_token_usage": last}
                stream.write(json.dumps({"type": "event_msg", "payload": {"type": "token_count", "info": info}}) + "\n")


def test_resumed_reflection_counts_only_its_own_turn_not_the_responder_before_it(tmp_path: Path) -> None:
    """Receipts recorded ``codex exec resume``'s session-cumulative usage as the reflection's."""

    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-persistent"
    responder, reflection = _usage(500, 10, cached=400), _usage(300, 20, cached=250)
    session_total = _usage(800, 30, cached=650)
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=responder, reflection=session_total, warm=False, incident_detectors=1
    )
    run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
    receipt_path = run / "sdo_production_receipt_strict.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["reflection_session_mode"] = "resume"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    rollout = run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27" / "rollout-x-session-0.jsonl"
    _token_turns(rollout, [[_usage(200, 4, cached=150), _usage(300, 6, cached=250)], [reflection]])

    (only,) = load_sdo_pipeline(root)

    # The rollout also counts each turn's model requests.
    assert only.responder == replace(TokenUsage.from_mapping(responder), requests=2)
    assert only.reflection == replace(TokenUsage.from_mapping(reflection), requests=1)


def test_fresh_session_reflection_keeps_the_receipt_usage(tmp_path: Path) -> None:
    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-persistent-fresh"
    reflection = _usage(300, 20, cached=250)
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=_usage(500, 10), reflection=reflection, warm=False, incident_detectors=1
    )
    run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
    receipt_path = run / "sdo_production_receipt_strict.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["reflection_session_mode"] = "fresh"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    (only,) = load_sdo_pipeline(root)

    assert only.reflection == TokenUsage.from_mapping(reflection)


# --------------------------------------------------------------------------- cache breakdown and pricing

LUNA = {"cache_read": 0.1, "cache_write": 1.25, "output": 5.0}  # gpt-6-luna in agentshim's table


def _token_count_line(last: dict[str, int], total: dict[str, int]) -> str:
    info = {"total_token_usage": total, "last_token_usage": last}
    return json.dumps({"type": "event_msg", "payload": {"type": "token_count", "info": info}}) + "\n"


def _write_rollout_turns(path: Path, turns: list[list[dict[str, int]]]) -> None:
    """A Codex rollout whose turns each make the given model requests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    total: dict[str, int] = {}
    lines = [json.dumps({"type": "session_meta", "payload": {"id": "s"}}) + "\n"]
    for requests in turns:
        lines.append(json.dumps({"type": "event_msg", "payload": {"type": "task_started"}}) + "\n")
        for last in requests:
            total = {key: total.get(key, 0) + last.get(key, 0) for key in {*total, *last}}
            lines.append(_token_count_line(last, dict(total)))
    path.write_text("".join(lines), encoding="utf-8")


def _request(input_tokens: int, cached: int, output: int, reasoning: int = 0) -> dict[str, int]:
    return {
        "input_tokens": input_tokens,
        "cached_input_tokens": cached,
        "cache_write_input_tokens": 0,
        "output_tokens": output,
        "reasoning_output_tokens": reasoning,
    }


def _config(experiment: Path, agent: str, model: str, provider: str | None = None) -> None:
    lines = ["[runner]", f'agent = "{agent}"', f'model = "{model}"']
    if provider is not None:
        lines += ["", f"[agent.{agent}]", f'provider = "{provider}"', f'model = "{model}"']
    experiment.mkdir(parents=True, exist_ok=True)
    (experiment / "experiment_config.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")


class TestTokenBreakdown:
    def test_a_pre_split_record_had_cache_writes_inside_cached(self) -> None:
        usage = TokenUsage.from_mapping(
            {"input_tokens": 150, "cached_input_tokens": 50, "cache_write_input_tokens": 30, "output_tokens": 40}
        )
        assert (usage.cached_input_tokens, usage.cache_write_input_tokens, usage.uncached_input_tokens) == (20, 30, 100)

    def test_a_split_record_reads_every_class_and_the_request_count(self) -> None:
        usage = TokenUsage.from_mapping(
            {
                "input_tokens": 1_000,
                "cached_input_tokens": 600,
                "cache_read_input_tokens": 600,
                "cache_write_input_tokens": 300,
                "cache_write_1h_input_tokens": 100,
                "uncached_input_tokens": 100,
                "output_tokens": 80,
                "reasoning_output_tokens": 20,
                "model_requests": 7,
            }
        )
        assert usage.cache_write_1h_input_tokens == 100
        assert usage.reasoning_output_tokens == 20
        assert usage.requests == 7
        assert usage.total() == 1_080

    def test_impossible_counts_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="exceed"):
            TokenUsage(input_tokens=10, cached_input_tokens=8, cache_write_input_tokens=5)

    def test_the_responder_breakdown_and_requests_come_from_its_rollout(self, tmp_path: Path) -> None:
        root = tmp_path / "pipeline"
        stage = _sdo_stage(
            root, 0, "first", PROBLEM_A, responder=_usage(300, 30, cached=200), reflection={}, warm=True,
            incident_detectors=0,
        )  # fmt: skip
        run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
        rollout = run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27" / "rollout-x-session-0.jsonl"
        _write_rollout_turns(rollout, [[_request(100, 50, 10, 4), _request(200, 150, 20, 6)]])

        (only,) = load_sdo_pipeline(root)

        assert only.responder == TokenUsage(
            input_tokens=300, cached_input_tokens=200, output_tokens=30, reasoning_output_tokens=10, requests=2
        )

    def test_codex_runs_take_reasoning_and_requests_from_their_rollout(self, tmp_path: Path) -> None:
        root = _codex_experiment(tmp_path / "codex", [(PROBLEM_A, True, 300)])
        run = root / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "codex" / PROBLEM_A / "run_1"
        _write_rollout_turns(
            run / "sessions" / "2026" / "09" / "27" / "rollout-x-y.jsonl",
            [[_request(100, 50, 10, 4), _request(200, 100, 20, 6)]],
        )

        (codex_run,) = load_codex_runs(root)

        assert codex_run.tokens == TokenUsage(
            input_tokens=300, cached_input_tokens=150, output_tokens=30, reasoning_output_tokens=10, requests=2
        )


class TestPricing:
    def _report(self, tmp_path: Path, *argv: str) -> tuple[str, dict]:
        root = tmp_path / "pipeline"
        stage = _sdo_stage(
            root, 0, "first", PROBLEM_A, responder=_usage(1_000, 100, cached=900), reflection={}, warm=True,
            incident_detectors=0,
        )  # fmt: skip
        _config(stage, "sdo_codex", "gpt-6-luna", provider="codex")
        codex = _codex_experiment(tmp_path / "codex", [(PROBLEM_A, True, 2_000)])
        _config(codex, "codex", "gpt-6-luna")
        out = tmp_path / "report.json"
        assert main([str(root), "--codex", str(codex), "--json", str(out), "--include-invalid", *argv]) == 0
        return out.name, json.loads(out.read_text(encoding="utf-8"))

    def test_weights_and_usd_come_from_agentshims_table(self, tmp_path: Path, capsys) -> None:
        _, document = self._report(tmp_path)
        printed = capsys.readouterr().out

        stage = document["stages"][0]["responder_summary"]
        # 100 uncached x 1 + 900 cache reads x 0.1 + 100 output x 5
        assert stage["weighted_tokens"] == pytest.approx(100 + 90 + 500)
        assert stage["usd"] == pytest.approx((100 * 0.10 + 900 * 0.01 + 100 * 0.50) / 1e6)
        codex = document["codex"][PROBLEM_A]["mean_summary"]
        # 1,000 cached of 2,000 input, no output
        assert codex["weighted_tokens"] == pytest.approx(1_000 + 1_000 * 0.1)
        assert document["pricing"]["table_version"]
        assert "prices: agentshim pricing table" in printed
        assert "last updated" in printed
        assert "gpt-6-luna" in printed

    def test_cli_weights_override_the_table(self, tmp_path: Path, capsys) -> None:
        _, document = self._report(tmp_path, "--weight", "output=8", "--weight", "cache_read=0.25")
        printed = capsys.readouterr().out

        stage = document["stages"][0]["responder_summary"]
        assert stage["weighted_tokens"] == pytest.approx(100 + 900 * 0.25 + 100 * 8)
        # USD always uses the table's prices.
        assert stage["usd"] == pytest.approx((100 * 0.10 + 900 * 0.01 + 100 * 0.50) / 1e6)
        assert "override" in printed

    def test_an_unpriced_model_reports_raw_tokens_only(self, tmp_path: Path, capsys) -> None:
        _, document = self._report(tmp_path, "--sdo-model", "codex:gpt-99-imaginary", "--codex-model", "codex:x")
        printed = capsys.readouterr().out

        stage = document["stages"][0]["responder_summary"]
        assert stage["weighted_tokens"] is None
        assert stage["usd"] is None
        assert stage["raw_tokens"] == 1_100
        assert "no price for codex/gpt-99-imaginary" in printed

    def test_complete_cli_weights_price_an_unknown_model(self, tmp_path: Path) -> None:
        _, document = self._report(
            tmp_path,
            "--sdo-model",
            "codex:gpt-99-imaginary",
            "--weight",
            "cache_read=0.1",
            "--weight",
            "cache_write=1",
            "--weight",
            "output=8",
        )
        stage = document["stages"][0]["responder_summary"]
        assert stage["weighted_tokens"] == pytest.approx(100 + 90 + 800)
        assert stage["usd"] is None

    def test_a_custom_pricing_table_is_used_and_named(self, tmp_path: Path, capsys) -> None:
        table = {
            "version": "pinned-1",
            "last_updated": "2026-09-01",
            "entries": [
                {
                    "vendor": "openai",
                    "model": "gpt-6-luna",
                    "input_usd": 1.0,
                    "cache_read_usd": 0.5,
                    "cache_write_usd": 1.0,
                    "output_usd": 2.0,
                    "source": "https://example.com/pricing",
                    "checked": "2026-09-01",
                }
            ],
        }
        path = tmp_path / "table.json"
        path.write_text(json.dumps(table), encoding="utf-8")
        _, document = self._report(tmp_path, "--pricing-table", str(path))
        printed = capsys.readouterr().out

        assert document["stages"][0]["responder_summary"]["weighted_tokens"] == pytest.approx(100 + 450 + 200)
        assert "pinned-1, last updated 2026-09-01" in printed

    def test_requests_per_incident_are_reported(self, tmp_path: Path) -> None:
        _, document = self._report(tmp_path)
        assert "requests" in document["stages"][0]["responder_summary"]


def test_a_claude_baseline_is_read_from_its_stream(tmp_path: Path) -> None:
    root = tmp_path / "claude"
    results = _results_dir(root, 0, PROBLEM_A, "claudecode")
    _write_csv(results, "claudecode", diagnosis=True, mitigation=True, injected=0.0, submitted=100.0)
    run = results / "claudecode" / PROBLEM_A / "run_1"
    # SREGym's own metrics drop cache writes and exclude cache reads from input.
    (run / f"claudecode_results_{PROBLEM_A}_1.json").write_text(
        json.dumps({"usage_metrics": {"input_tokens": 9, "cached_input_tokens": 400, "output_tokens": 50}}),
        encoding="utf-8",
    )
    result = {
        "type": "result",
        "subtype": "success",
        "num_turns": 6,
        "result": "done",
        "usage": {
            "input_tokens": 9,
            "cache_creation_input_tokens": 300,
            "cache_read_input_tokens": 400,
            "output_tokens": 50,
            "cache_creation": {"ephemeral_5m_input_tokens": 100, "ephemeral_1h_input_tokens": 200},
        },
    }
    (run / "claudecode.txt").write_text(json.dumps(result) + "\n", encoding="utf-8")
    _config(root, "claudecode", "claude-haiku-4-5")

    (claude_run,) = load_codex_runs(root)

    assert claude_run.tokens == TokenUsage(
        input_tokens=709,
        cached_input_tokens=400,
        cache_write_input_tokens=300,
        cache_write_1h_input_tokens=200,
        output_tokens=50,
        requests=6,
    )
    assert (claude_run.provider, claude_run.model) == ("claude", "claude-haiku-4-5")


def test_a_fresh_reflection_takes_its_breakdown_from_its_own_rollout(tmp_path: Path) -> None:
    """A fresh reflection runs in its own session; its rollout has the reasoning and requests receipts lacked."""
    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-persistent-fresh"
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=_usage(500, 10), reflection=_usage(300, 30, cached=200), warm=False,
        incident_detectors=1,
    )  # fmt: skip
    run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
    receipt_path = run / "sdo_production_receipt_strict.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["reflection_session_mode"] = "fresh"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    turns = run / "sdo_runtime" / "usage" / "controller-turns.jsonl"
    turns.write_text(
        json.dumps({"duration_seconds": 240.0, "session_id": "refl-1", "usage": _usage(300, 30, cached=200)}) + "\n",
        encoding="utf-8",
    )
    sessions = run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27"
    _write_rollout_turns(sessions / "rollout-y-refl-1.jsonl", [[_request(100, 50, 10, 3), _request(200, 150, 20, 5)]])

    (only,) = load_sdo_pipeline(root)

    assert only.reflection == TokenUsage(
        input_tokens=300, cached_input_tokens=200, output_tokens=30, reasoning_output_tokens=8, requests=2
    )


def test_a_stage_stopped_before_its_drain_keeps_responder_tokens_from_its_stage_end_evidence(tmp_path: Path) -> None:
    root = tmp_path / "20260927_000000_pipeline_sdo-codex-luna-sequence"
    responder = _usage(500, 10, cached=400)
    stage = _sdo_stage(
        root, 0, "first", PROBLEM_A, responder=responder, reflection=_usage(0, 0), warm=False, incident_detectors=1
    )
    run = stage / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results" / "sdo_codex" / PROBLEM_A / "run_1"
    # The pipeline stopped before the next stage drained this incident: no strict receipt.
    (run / "sdo_production_receipt_strict.json").unlink()
    resolution = {"incident_id": "incident-0", "responder_session_id": "session-0", "incident_resolution_seconds": 42.0}
    (run / "sdo_incident_resolution.json").write_text(json.dumps(resolution), encoding="utf-8")
    rollout = run / "sdo_runtime" / "codex" / "sessions" / "2026" / "09" / "27" / "rollout-x-session-0.jsonl"
    _token_turns(rollout, [[responder]])

    (only,) = load_sdo_pipeline(root)

    assert only.responder == replace(TokenUsage.from_mapping(responder), requests=1)
    assert only.incident_resolution_seconds == 42.0


def test_native_receipt_responder_usage_comes_from_the_ledger_closure_result() -> None:
    from benchmarks.sregym.analysis.incident_cost import receipt_responder_usage

    receipt = {
        "native_ledger": {
            "closure": {"result": {"usage": {"input_tokens": 1000, "cached_input_tokens": 800, "output_tokens": 50}}}
        }
    }
    usage = receipt_responder_usage(receipt)
    assert (usage.input_tokens, usage.cached_input_tokens, usage.output_tokens) == (1000, 800, 50)
    assert receipt_responder_usage({}).input_tokens == 0


def test_stage_reports_go_no_repair_counts_and_actual_declines(pipeline: Path) -> None:
    receipt = next(
        pipeline.glob("stage_0_*/runs/*/worker_0/results/sdo_codex/*/run_1/sdo_production_receipt_strict.json")
    )
    document = json.loads(receipt.read_text())
    document["native_ledger"] = {
        "reflection_no_repair_count": 2,
        "reflection_decision": {"decline_reason": "no_recorded_repair"},
    }
    receipt.write_text(json.dumps(document))
    stages = load_sdo_pipeline(pipeline)
    assert stages[0].reflection_no_repair_count == 2
    assert stages[0].no_repair_declined is True
    assert stages[1].reflection_no_repair_count is None
    assert stages[1].no_repair_declined is None


def test_command_execution_events_supply_the_mutation_floor(tmp_path: Path) -> None:
    """Synthetic completed-item events use execution times, not the later export timestamp."""
    rollout = Path(__file__).parent / "fixtures" / "command_execution.jsonl"
    injected = 1790510410.0
    assert first_mutation_at([rollout], after=injected) == pytest.approx(injected + 20, rel=0, abs=0.001)
    assert last_mutation_done_at([rollout], after=injected, before=injected + 60) == pytest.approx(
        injected + 38, rel=0, abs=0.001
    )
    # The completed record can be exported after submission; cap the command at the POST.
    assert last_mutation_done_at([rollout], after=injected, before=injected + 25) == pytest.approx(
        injected + 25, rel=0, abs=0.001
    )
    results = tmp_path / "exp" / "runs" / f"000000_{PROBLEM_A}" / "worker_0" / "results"
    _timing_csv(
        results,
        fault_injected_at=injected,
        diagnosis_submitted_at=injected + 10,
        TTL=50,
        mitigation_submitted_at=injected + 60,
    )
    target = results / "codex" / PROBLEM_A / "run_1" / "sessions" / "rollout-events.jsonl"
    target.parent.mkdir(parents=True)
    target.write_text(rollout.read_text(encoding="utf-8"), encoding="utf-8")
    (run,) = load_codex_runs(tmp_path / "exp")
    assert run.verdict.judge_excluded_seconds == pytest.approx(20)
    assert run.verdict.ttm_seconds == pytest.approx(38)


@pytest.mark.parametrize("invalid", ["missing-start", "reversed-clock", "incomplete", "not-command"])
def test_mutation_floor_ignores_incomplete_or_malformed_command_items(tmp_path: Path, invalid: str) -> None:
    fixture = Path(__file__).parent / "fixtures" / "command_execution.jsonl"
    record = json.loads(fixture.read_text(encoding="utf-8").splitlines()[2])
    if invalid == "missing-start":
        record["payload"].pop("started_at_ms")
    elif invalid == "reversed-clock":
        record["payload"]["completed_at_ms"] = record["payload"]["started_at_ms"] - 1
    elif invalid == "incomplete":
        record["payload"]["type"] = "item_started"
    else:
        record["payload"]["item"]["type"] = "AgentMessage"
    rollout = tmp_path / "rollout-malformed.jsonl"
    rollout.write_text(json.dumps(record) + "\n", encoding="utf-8")
    assert first_mutation_at([rollout], after=1790510410.0) is None
    assert last_mutation_done_at([rollout], after=1790510410.0, before=1790510470.0) is None


def test_stage_reflection_total_includes_every_logged_turn_of_its_incident(tmp_path: Path) -> None:
    import hashlib

    incident = "incident-9"
    run = tmp_path / "run"
    log = (
        run / "sdo_runtime" / "sessions" / hashlib.sha256(incident.encode()).hexdigest()[:32] / "reflection-usage.jsonl"
    )
    log.parent.mkdir(parents=True)
    turns = [_usage(100, 10), _usage(200, 20), _usage(50, 5)]
    log.write_text("".join(json.dumps({"session_id": "s", "usage": turn}) + "\n" for turn in turns), encoding="utf-8")
    from benchmarks.sregym.analysis.incident_cost import logged_reflection_cost

    receipt = {"reflection_usage": turns[-1]}
    usage = _reflection_usage(receipt, [], None, logged_reflection_cost(run, incident))
    assert (usage.input_tokens, usage.output_tokens) == (350, 35)
    # A receipt that already totals the turns is kept.
    assert _reflection_usage({"reflection_usage": _usage(350, 35)}, [], None, None).input_tokens == 350
