"""Current native ProtoJSON receipts and per-session exports remain analyzable."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pytest
from sregym_run_builder import INJECTED, Run

from benchmarks.sregym.analysis.incident_cost import TokenUsage, load_sdo_pipeline, main
from benchmarks.sregym.analysis.run_validity import classify_path

if TYPE_CHECKING:
    from pathlib import Path


def _usage(inputs: int, cached: int, outputs: int, requests: int) -> dict[str, str]:
    return {
        "input_tokens": str(inputs),
        "cache_read_input_tokens": str(cached),
        "cached_input_tokens": str(cached),
        "output_tokens": str(outputs),
        "reasoning_output_tokens": "2",
        "model_requests": str(requests),
    }


def _receipt() -> dict[str, Any]:
    incident = "incident-1"
    commit = "a" * 40
    return {
        "schema_version": "sdo.production-receipt/v2",
        "broker_mode": "native",
        "worker_mount_mode": "proposal-only",
        "incident_id": incident,
        "namespace": "demo",
        "control_namespace": "demo-sdo",
        "controller_image": "controller:go",
        "responder_image": "responder:python",
        "validator_image": "validator:go",
        "controller_pod_uid": "pod-1",
        "outcome_commit": commit,
        "resolution": "sdo_mitigated",
        "native_ledger": {
            "incident_id": incident,
            "acknowledged": True,
            "outcome_commit": commit,
            "closure": {
                "request": {"incident_id": incident, "namespace": "demo", "repair_policy": "recorded-actions"},
                "result": {
                    "incident_id": incident,
                    "status": "completed",
                    "confirmed_root_causes": [{"summary": "wrong selector"}],
                    "repair_actions": [{"success": True}],
                    "responder_session_id": "resp1",
                    "usage": _usage(1000, 800, 50, 2),
                },
                "final_detector_states": [{"status": "clear"}],
                "verified_at": "2026-10-22T00:00:00Z",
            },
            "outcome_document": {
                "schema_version": 1,
                "incident_id": incident,
                "classification": "success",
                "diagnosis_verification": [{"verdict": "confirmed"}],
                "usage": _usage(1000, 800, 50, 2),
            },
            "receipt": {"incident_id": incident, "outcome_commit": commit, "ack_token": "ack"},
            "reflection_decision": {"learning_decision": "no_change", "usage": _usage(2000, 1500, 100, 3)},
        },
        "reflection_usage": _usage(2000, 1500, 100, 3),
        "responder_job_evidence": [{"incident_id": incident, "succeeded": True, "job_uid": "j1", "job_name": "j1"}],
    }


def _pipeline(tmp_path: Path, *, rollout: bool) -> tuple[Path, Path]:
    root = tmp_path / "pipeline"
    stage = Run(agent="sdo_codex", receipt=_receipt(), rollout_turns=None).write(root / "stage_0_one")
    (root / "pipeline_state.json").write_text(
        json.dumps({"stages": [{"index": 0, "name": "one", "experiment_dir": str(stage)}]})
    )
    run = next(stage.rglob("run_1"))
    if rollout:
        path = run / "sdo_runtime/sessions/incident-hash/codex/sessions/2026/10/22/rollout-x-resp1.jsonl"
        path.parent.mkdir(parents=True)
        records = [{"type": "event_msg", "payload": {"type": "task_started"}}]
        records.extend(
            {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "last_token_usage": {
                            "input_tokens": 500,
                            "cached_input_tokens": 400,
                            "output_tokens": 25,
                            "reasoning_output_tokens": 1,
                        }
                    },
                },
            }
            for _ in range(2)
        )
        for seconds, payload in [
            (
                45,
                {
                    "type": "function_call",
                    "call_id": "repair",
                    "name": "exec_command",
                    "arguments": json.dumps({"cmd": "kubectl patch deploy/geo -p '{}'"}),
                },
            ),
            (55, {"type": "function_call_output", "call_id": "repair", "output": "patched"}),
        ]:
            records.append(
                {
                    "timestamp": datetime.fromtimestamp(INJECTED + seconds, timezone.utc).isoformat(),
                    "type": "response_item",
                    "payload": payload,
                }
            )
        path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return root, run


@pytest.mark.parametrize("rollout", [False, True])
def test_native_receipt_is_valid_with_string_counts_and_nested_session(tmp_path: Path, rollout: bool) -> None:
    root, _ = _pipeline(tmp_path, rollout=rollout)
    (result,) = classify_path(root)
    assert result.classification == "valid", result.reasons
    assert (
        next(check for check in result.checks if check.name == "tokens").detail
        == f"{2 if rollout else 1} source(s) agree"
    )


@pytest.mark.parametrize("rollout", [False, True])
@pytest.mark.parametrize("flat_reflection", [False, True])
def test_cost_reports_native_usage_requests_and_mutation_floor(
    tmp_path: Path, rollout: bool, flat_reflection: bool
) -> None:
    root, run = _pipeline(tmp_path, rollout=rollout)
    if not flat_reflection:
        path = run / "sdo_production_receipt_strict.json"
        receipt = json.loads(path.read_text())
        del receipt["reflection_usage"]
        path.write_text(json.dumps(receipt))
    out = tmp_path / "report.json"
    assert main([str(root), "--json", str(out)]) == 0
    (stage,) = json.loads(out.read_text())["stages"]
    assert stage["responder_summary"]["raw_tokens"] == 1050
    assert stage["responder_summary"]["weighted_tokens"] == pytest.approx(200 + 80 + 250)
    assert stage["responder_summary"]["requests"] == 2
    assert stage["reflection_summary"]["raw_tokens"] == 2100
    assert stage["reflection_summary"]["weighted_tokens"] == pytest.approx(500 + 150 + 500)
    assert stage["reflection_summary"]["requests"] == 3
    assert stage["verdict"]["last_mitigation_seconds"] == (55 if rollout else None)
    assert stage["verdict"]["ttm_seconds"] == (55 if rollout else 50)
    (loaded,) = load_sdo_pipeline(root)
    assert loaded.responder.input_tokens == 1000
    assert loaded.reflection.input_tokens == 2000


def test_native_receipt_reconciles_against_nested_rollout(tmp_path: Path) -> None:
    root, run = _pipeline(tmp_path, rollout=True)
    path = run / "sdo_production_receipt_strict.json"
    receipt = json.loads(path.read_text())
    receipt["native_ledger"]["closure"]["result"]["usage"]["input_tokens"] = "1001"
    path.write_text(json.dumps(receipt))
    (result,) = classify_path(root)
    assert result.classification == "invalid_infra"
    assert any("token accounting mismatch" in reason for reason in result.reasons)


def test_other_retained_sessions_do_not_set_the_responder_mutation_floor(tmp_path: Path) -> None:
    root, run = _pipeline(tmp_path, rollout=True)
    rollout = next(run.rglob("rollout-*.jsonl"))
    other = rollout.with_name("rollout-x-other-incident.jsonl")
    records = [json.loads(line) for line in rollout.read_text().splitlines()]
    records[-1]["timestamp"] = datetime.fromtimestamp(INJECTED + 59, timezone.utc).isoformat()
    other.write_text("".join(json.dumps(record) + "\n" for record in records))

    (stage,) = load_sdo_pipeline(root)
    assert stage.verdict.ttm_seconds == 55
    assert stage.responder.requests == 2


def test_protojson_usage_preserves_cache_write_and_reasoning_breakdown() -> None:
    usage = TokenUsage.from_mapping(
        {**_usage(100, 60, 10, 4), "cache_write_input_tokens": "20", "cache_write_1h_input_tokens": "5"}
    )
    assert usage == TokenUsage(100, 60, 10, 20, 5, 2, 4)


@pytest.mark.parametrize(("requests", "expected"), [(None, None), ("0", 0), (0, 0)])
def test_optional_request_count_distinguishes_unknown_from_zero(requests: object, expected: int | None) -> None:
    assert TokenUsage.from_mapping({"model_requests": requests}).requests == expected
