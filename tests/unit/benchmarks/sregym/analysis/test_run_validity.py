"""Every run is classified valid, invalid_infra (with a reason) or agent_failure from its own evidence.

Synthetic run directories follow SREGym's parallel layout
(``runs/<seq>_<problem>/worker_<n>/``); each test breaks one piece of evidence.
The last tests classify the real run directories under
``third_party/sregym/logs`` when they are present (``SDO_SREGYM_LOGS``
overrides the location).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from sregym_run_builder import GUARD, PROBLEM, Run, usage

from benchmarks.sregym.analysis.run_validity import ValidityPolicy, classify_path, main

STRICT = ValidityPolicy()
LEGACY = ValidityPolicy(legacy=True)


def _one(path: Path, policy: ValidityPolicy = STRICT):
    (result,) = classify_path(path, policy=policy)
    return result


def test_a_complete_codex_run_is_valid(tmp_path: Path) -> None:
    result = _one(Run().write(tmp_path / "20260928_100000_codex"))

    assert result.classification == "valid", result.reasons
    assert result.reasons == ()


def test_a_failed_oracle_is_an_agent_failure_not_infra(tmp_path: Path) -> None:
    result = _one(Run(mitigation="False").write(tmp_path / "exp"))

    assert result.classification == "agent_failure"
    assert any("Mitigation" in reason for reason in result.reasons)


def test_a_missing_manifest_is_invalid_unless_legacy(tmp_path: Path) -> None:
    exp = Run(manifest=None).write(tmp_path / "exp")

    assert _one(exp).classification == "invalid_infra"
    assert "manifest" in _one(exp).reasons[0]
    assert _one(exp, LEGACY).classification == "valid"


def test_a_waived_preflight_is_invalid_even_in_legacy_mode(tmp_path: Path) -> None:
    manifest = {"preflight": {"ok": False, "waived": True, "checks": [{"name": "disk", "status": "fail"}]}}
    result = _one(Run(manifest=manifest).write(tmp_path / "exp"), LEGACY)

    assert result.classification == "invalid_infra"
    assert any("preflight waived" in reason and "disk" in reason for reason in result.reasons)


def test_a_failed_cli_install_is_invalid_infra(tmp_path: Path) -> None:
    result = _one(Run(install_rc="1").write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("install" in reason and "404" in reason for reason in result.reasons)


def test_an_exhausted_codex_quota_is_invalid_infra(tmp_path: Path) -> None:
    events = '{"type":"error","message":"You\'ve hit your usage limit. Try again later."}\n'
    result = _one(Run(codex_events=events, mitigation="False").write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("usage limit" in reason for reason in result.reasons)


def test_tokens_that_do_not_reconcile_are_invalid_infra(tmp_path: Path) -> None:
    result = _one(Run(usage_metrics=usage(999, 800, 50)).write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("token" in reason and "999" in reason for reason in result.reasons)


def test_a_run_without_any_token_evidence_is_invalid_infra(tmp_path: Path) -> None:
    result = _one(Run(usage_metrics=None, rollout_turns=None).write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("no token evidence" in reason for reason in result.reasons)


def test_a_passed_run_without_judge_free_ttm_is_invalid_infra(tmp_path: Path) -> None:
    result = _one(Run(ttl="").write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("TTM" in reason and "TTL" in reason for reason in result.reasons)


def test_a_ttm_before_the_ttd_means_judge_time_leaked_and_is_invalid(tmp_path: Path) -> None:
    # TTL says grading ended 200 s after injection, but mitigation was submitted at +60 s.
    result = _one(Run(ttl="200").write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("judge" in reason for reason in result.reasons)


def test_an_isolation_guard_mismatch_is_invalid_infra(tmp_path: Path) -> None:
    log = GUARD + "AgentKubeconfigMismatch: agent kubeconfig does not reach cluster luna-w0 only\n"
    result = _one(Run(worker_log=log).write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("isolation guard" in reason for reason in result.reasons)


def test_a_missing_isolation_guard_is_invalid_in_strict_mode(tmp_path: Path) -> None:
    result = _one(Run(worker_log="== Fault Injection ==\n").write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("isolation guard" in reason for reason in result.reasons)


def test_agent_output_from_another_lane_is_invalid_infra(tmp_path: Path) -> None:
    output = "hotel pods on luna-w0-worker2 ... namespace listing from luna-w1-worker3"
    result = _one(Run(tool_output=output).write(tmp_path / "exp"), LEGACY)

    assert result.classification == "invalid_infra"
    assert any("luna-w1" in reason for reason in result.reasons)


def test_a_node_name_cut_by_tool_output_truncation_is_not_another_lane(tmp_path: Path) -> None:
    output = "hotel pods on luna-w0-worker2 ...     …20266 tokens truncated…na-w0-worker3   <none>"
    result = _one(Run(tool_output=output).write(tmp_path / "exp"), LEGACY)

    assert not any("foreign" in reason or "other lanes" in reason for reason in result.reasons), result.reasons


def test_legacy_runs_without_the_guard_are_invalid_only_when_they_overlapped_another_run(tmp_path: Path) -> None:
    logs = tmp_path / "logs"
    alone = Run(worker_log="", manifest=None).write(logs / "20260927_100000_codex")
    first = Run(worker_log="", manifest=None).write(logs / "20260927_190000_codex")
    second = Run(worker_log="", manifest=None).write(logs / "invalid_20260927_190100_codex")
    _window(alone, 1000, 2000)
    _window(first, 5000, 6000)
    _window(second, 5500, 6500)

    assert _one(alone, LEGACY).classification == "valid"
    overlapping = _one(first, LEGACY)
    assert overlapping.classification == "invalid_infra"
    assert any("invalid_20260927_190100_codex" in reason for reason in overlapping.reasons)
    assert _one(second, LEGACY).manual_label == "invalid"


def _window(experiment: Path, start: float, end: float) -> None:
    os.utime(experiment / "experiment_config.toml", (start, start))
    for path in experiment.rglob("*"):
        if path.is_file() and path.name != "experiment_config.toml":
            os.utime(path, (end, end))


def _receipt() -> dict[str, object]:
    return {"responder_session_id": "resp1", "usage": usage(1000, 800, 50), "cleaned": True}


def test_an_sdo_stage_without_a_valid_strict_receipt_is_an_agent_failure(tmp_path: Path) -> None:
    result = _one(Run(agent="sdo_codex", receipt=None).write(tmp_path / "exp"))

    assert result.classification == "agent_failure"
    assert any("receipt" in reason for reason in result.reasons)


def test_an_sdo_receipt_failing_production_validation_is_an_agent_failure(tmp_path: Path) -> None:
    result = _one(Run(agent="sdo_codex", receipt=_receipt()).write(tmp_path / "exp"))

    assert result.classification == "agent_failure"
    assert any("production validation" in reason for reason in result.reasons)


def test_leftover_responder_helpers_are_an_agent_failure(tmp_path: Path) -> None:
    log = (
        "2026-09-28T04:30:00Z controller error: clean up responder helpers: [delete helper pod hotel/curl: forbidden]\n"
    )
    run = Run(agent="sdo_codex", receipt=_receipt(), controller_log=log)

    result = _one(run.write(tmp_path / "exp"))

    assert any("sdo.dev/responder-helper" in reason for reason in result.reasons)


def _configured(provider: str, model: str) -> dict[str, object]:
    roles = {
        role: {"provider": provider, "model": model, "effort": "high", "source": "agent.sdo_codex"}
        for role in ("responder", "reflection", "lifecycle_deployer", "health_judge")
    }
    return {"schema_version": 1, "preflight": {"ok": True, "waived": False, "checks": []}, "models": [roles]}


def _provider_check(run: Run, tmp_path: Path):
    result = _one(run.write(tmp_path / "exp"))
    (check,) = (check for check in result.checks if check.name == "agent-provider")
    return result, check


def test_workers_that_resolved_the_configured_provider_and_model_pass(tmp_path: Path) -> None:
    resolved = [{"role": "responder", "provider": "claude", "model": "claude-haiku-5-5"}]
    _, check = _provider_check(
        Run(agent="sdo_codex", manifest=_configured("claude", "claude-haiku-5-5"), resolved_agents=resolved), tmp_path
    )

    assert check.status == "pass"


def test_a_worker_that_ran_a_different_provider_than_configured_is_invalid_infra(tmp_path: Path) -> None:
    resolved = [{"role": "responder", "provider": "codex", "model": None}]
    result, check = _provider_check(
        Run(agent="sdo_codex", manifest=_configured("claude", "claude-haiku-5-5"), resolved_agents=resolved), tmp_path
    )

    assert check.status == "fail"
    assert result.classification == "invalid_infra"
    assert any("claude/claude-haiku-5-5" in reason and "codex" in reason for reason in result.reasons)


def test_a_worker_on_another_model_than_configured_is_invalid_infra(tmp_path: Path) -> None:
    resolved = [
        {"role": "responder", "provider": "claude", "model": "claude-haiku-5-5"},
        {"role": "reflection", "provider": "claude", "model": "claude-opus-5-5"},
    ]
    _, check = _provider_check(
        Run(agent="sdo_codex", manifest=_configured("claude", "claude-haiku-5-5"), resolved_agents=resolved), tmp_path
    )

    assert check.status == "fail"


def test_runs_without_worker_resolution_records_are_not_recorded(tmp_path: Path) -> None:
    _, check = _provider_check(Run(agent="sdo_codex", manifest=_configured("claude", "m")), tmp_path)

    assert check.status == "not_recorded"


def test_the_cli_prints_every_run_and_exits_nonzero_when_any_is_invalid(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    good = Run().write(tmp_path / "good")
    bad = Run(install_rc="1").write(tmp_path / "bad")

    assert main([str(good)]) == 0
    assert main([str(good), str(bad), "--json", str(tmp_path / "out.json")]) == 1

    printed = capsys.readouterr().out
    assert "invalid_infra" in printed
    assert "valid" in printed
    document = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
    assert [run["classification"] for run in document] == ["valid", "invalid_infra"]


# --------------------------------------------------------------------------- real runs

LOGS = Path(os.environ.get("SDO_SREGYM_LOGS", Path(__file__).resolve().parents[5] / "third_party" / "sregym" / "logs"))
real = pytest.mark.skipif(not LOGS.is_dir(), reason=f"no SREGym run directories under {LOGS}")


@real
@pytest.mark.parametrize(
    "name",
    [
        "invalid_20260927_190901_codex",
        "invalid_20260927_190922_pipeline_sdo-codex-luna-persistent",
    ],
)
def test_real_runs_renamed_invalid_by_hand_are_classified_invalid_infra(name: str) -> None:
    if not (LOGS / name).is_dir():
        pytest.skip(f"{name} is not present")

    results = classify_path(LOGS / name, policy=LEGACY)

    assert results
    assert all(result.classification == "invalid_infra" for result in results), [r.reasons for r in results]


@real
def test_real_guarded_codex_baseline_is_valid_in_legacy_mode_and_invalid_without_a_manifest() -> None:
    name = "20260927_205946_codex"
    if not (LOGS / name).is_dir():
        pytest.skip(f"{name} is not present")

    assert {result.classification for result in classify_path(LOGS / name, policy=LEGACY)} == {"valid"}
    assert {result.classification for result in classify_path(LOGS / name, policy=STRICT)} == {"invalid_infra"}


@real
def test_the_real_run_whose_mitigation_the_harness_dropped_is_invalid_infra() -> None:
    name = "superseded_prefix_20260927_195049_codex"
    if not (LOGS / name).is_dir():
        pytest.skip(f"{name} is not present")

    by_run = {
        Path(result.results_dir).parent.parent.name: result for result in classify_path(LOGS / name, policy=LEGACY)
    }

    dropped = by_run[f"000003_{PROBLEM}"]
    assert dropped.classification == "invalid_infra"
    assert "harness dropped a submission" in dropped.reasons[0]


def test_an_acknowledged_mitigation_without_a_verdict_is_a_dropped_submission(tmp_path: Path) -> None:
    submit = (
        '{"type":"item.completed","item":{"type":"command_execution","command":"curl -X POST http://h:8000/submit",'
        '"aggregated_output":"{\\"status\\":\\"200\\",\\"message\\":\\"Submission received\\"}"}}\n'
    )
    run = Run(mitigation="", codex_events=submit * 2)

    result = _one(run.write(tmp_path / "exp"))

    assert result.classification == "invalid_infra"
    assert any("harness dropped a submission" in reason for reason in result.reasons)
    assert _one(Run(mitigation="").write(tmp_path / "quiet")).classification == "agent_failure"


def _pipeline(root: Path, stages: list[dict[str, str]]) -> Path:
    root.mkdir(parents=True)
    (root / "pipeline_config.toml").write_text('[pipeline]\nname = "p"\n', encoding="utf-8")
    for index, stage in enumerate(stages):
        if stage["status"] != "pending":
            stage["experiment_dir"] = str(root / f"stage_{index}_{stage['name']}")
    records = [{"index": index, "error": "", "experiment_dir": "", **stage} for index, stage in enumerate(stages)]
    (root / "pipeline_state.json").write_text(json.dumps({"stages": records}), encoding="utf-8")
    return root


def test_a_recorded_agent_failure_stage_ends_its_pipeline_normally(tmp_path: Path) -> None:
    """A pipeline that continued past an agent failure ran to its end; no receipt is still pending."""

    pipeline = _pipeline(
        tmp_path / "p",
        [{"name": "one", "status": "agent_failure"}, {"name": "two", "status": "completed"}],
    )
    stage = Run(agent="sdo_codex", receipt=None).write(pipeline / "stage_0_one")
    resolution = next(stage.rglob("run_1")) / "sdo_incident_resolution.json"
    resolution.write_text(json.dumps({"incident_id": "i", "responder_session_id": "resp1"}), encoding="utf-8")

    result = classify_path(pipeline / "stage_0_one", policy=LEGACY)[0]

    assert not any("deferred strict receipt" in reason for reason in result.reasons)


def test_a_stage_waiting_for_its_deferred_receipt_reports_pending(tmp_path: Path) -> None:
    pipeline = _pipeline(
        tmp_path / "p",
        [
            {"name": "one", "status": "completed"},
            {"name": "two", "status": "running"},
            {"name": "three", "status": "pending"},
        ],
    )
    stage = Run(agent="sdo_codex", receipt=None).write(pipeline / "stage_0_one")
    (pipeline / "stage_1_two").mkdir()
    resolution = next(stage.rglob("run_1")) / "sdo_incident_resolution.json"
    resolution.write_text(json.dumps({"incident_id": "i", "responder_session_id": "resp1"}), encoding="utf-8")

    results = classify_path(pipeline, policy=LEGACY)

    assert [Path(result.experiment_dir).name for result in results] == ["stage_0_one", "stage_1_two"]
    assert results[0].classification == "invalid_infra"
    assert "deferred strict receipt" in results[0].reasons[0]
    assert "pending" in results[0].reasons[0]
    assert "pipeline stopped" not in results[0].reasons[0]
    assert results[1].reasons == ("no problem run was recorded",)


@pytest.mark.parametrize(
    ("code", "expected"),
    [("CODE_CHECKER_UNAVAILABLE", "invalid_infra"), ("CODE_REJECTED_EVIDENCE", "agent_failure")],
)
def test_typed_receipt_failure_distinguishes_checker_infrastructure(tmp_path: Path, code: str, expected: str) -> None:
    stage = Run(agent="sdo_codex", receipt=None).write(tmp_path / "exp")
    rejected = next(stage.rglob("run_1")) / "sdo_rejected_production_receipt.json"
    rejected.write_text(
        json.dumps(
            {
                "validation_error": "receipt check failed",
                "failure": {"incident_id": "one", "code": code, "message": "receipt check failed"},
            }
        ),
        encoding="utf-8",
    )
    assert _one(stage).classification == expected
