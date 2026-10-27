import json
from pathlib import Path

import pytest

from sdo.agents.responder.continuation import ContinuationFeedback, continuation_prompt


def test_continuation_has_only_current_findings_and_receipts() -> None:
    payload = {
        "attempt": 1,
        "healthy": False,
        "summary": "one remains",
        "still_broken": [
            {
                "detector_id": "third",
                "rule_id": "synthetic",
                "fingerprint": "third/api",
                "summary": "third broken",
                "evidence": "fresh",
                "primary_resource": {"kind": "Service", "namespace": "app", "name": "third"},
            }
        ],
        "already_done": [
            {
                "action_id": "repair-a",
                "kind": "recorded-action",
                "summary": "first repaired",
                "receipt": {"action_id": "repair-a", "success": True},
            }
        ],
    }
    prompt = continuation_prompt(json.dumps(payload))
    assert "still_broken:" in prompt
    assert "already_done:" in prompt
    assert "third/api" in prompt
    assert "repair-a" in prompt
    assert "Continue the same incident" in prompt
    assert "".join(part.text for part in prompt.parts) == prompt
    assert {part.name for part in prompt.parts} == {"continuation.instructions", "continuation.incident_data"}
    template = Path("sdo/agents/responder/prompts/continuation.md").read_text()
    assert len(template) <= 1500
    assert ContinuationFeedback.model_validate(payload).still_broken[0].fingerprint == "third/api"


def test_continuation_rejects_untyped_lists() -> None:
    with pytest.raises(ValueError, match="still_broken"):
        continuation_prompt('{"attempt":1,"healthy":false,"summary":"x","still_broken":["opaque"],"already_done":[]}')


def test_ten_receipt_continuation_fits_complete_prompt_budget() -> None:
    from tests.unit.sdo.agents.responder.continuation_cases import action_id, ten_receipt_continuation

    payload = ten_receipt_continuation()
    prompt = continuation_prompt(json.dumps(payload))
    assert len(prompt) <= 1500
    assert "".join(part.text for part in prompt.parts) == prompt
    broken, done = prompt.split("already_done:", 1)
    for index in (8, 9):
        assert action_id(index) in broken
        assert action_id(index) not in done
    for index in range(8):
        assert action_id(index) in done
    assert "Forbidden" in broken
    assert "synthetic typed mutation" not in prompt
    evidence = Path(prompt.split("Full JSON: ", 1)[1].splitlines()[0])
    assert json.loads(evidence.read_text()) == payload
    assert evidence.stat().st_mode & 0o777 == 0o600


def test_latest_receipt_matches_exact_resource_and_excludes_unresolved_success() -> None:
    from tests.unit.sdo.agents.responder.continuation_cases import action_id, ten_receipt_continuation

    payload = ten_receipt_continuation()
    old = payload["already_done"][8]
    new = {
        **old,
        "action_id": "retry-account",
        "receipt": {
            **old["receipt"],
            "action_id": "retry-account",
            "success": True,
            "completed_at": "2026-01-01T00:02:00Z",
        },
        "error": "",
    }
    payload["already_done"].insert(0, new)  # time, not wire list order, defines latest
    prompt = continuation_prompt(json.dumps(payload))
    broken, done = prompt.split("already_done:", 1)
    assert "retry-account" in broken
    assert action_id(8) not in broken
    assert "retry-account" not in done
    lines = broken.splitlines()
    account = next(line for line in lines if "cleanup-0" in line)
    binding = next(line for line in lines if "cleanup-2" in line)
    assert "retry-account" in account
    assert "last=none" in binding


def test_continuation_large_fields_are_bounded_and_full_evidence_survives() -> None:
    from tests.unit.sdo.agents.responder.continuation_cases import action_id, ten_receipt_continuation

    payload = ten_receipt_continuation()
    payload["still_broken"][0]["summary"] = "巨大\n" * 4000
    payload["error"] = "verification unavailable\n" * 1000
    payload["already_done"][0]["summary"] = "receipt\n" * 4000
    payload["already_done"][1]["rolled_back"] = True
    prompt = continuation_prompt(json.dumps(payload))
    assert len(prompt) <= 1500
    assert "verification unavailable" in prompt
    assert action_id(1) not in prompt.split("already_done:", 1)[1]
    evidence = Path(prompt.split("Full JSON: ", 1)[1].splitlines()[0])
    assert json.loads(evidence.read_text()) == payload


def test_typed_failure_reason_and_same_name_other_namespace_are_not_success() -> None:
    from tests.unit.sdo.agents.responder.continuation_cases import action_id, ten_receipt_continuation

    payload = ten_receipt_continuation()
    failed = payload["already_done"][8]
    failed["error"] = "mutation failed after an independently observed authorization rejection"
    failed["receipt"]["failure_reason"] = "authorization_denied"
    other = payload["already_done"][0]
    other["receipt"]["resources"] = [{**payload["still_broken"][0]["primary_resource"], "namespace": "peer"}]
    prompt = continuation_prompt(json.dumps(payload))
    broken, done = prompt.split("already_done:", 1)
    assert "authorization_denied" in broken
    assert action_id(8) not in done
    assert action_id(0) in done


def test_large_inventory_explicitly_counts_omitted_rows_without_losing_evidence() -> None:
    from tests.unit.sdo.agents.responder.continuation_cases import ten_receipt_continuation

    payload = ten_receipt_continuation()
    payload["still_broken"] *= 30
    prompt = continuation_prompt(json.dumps(payload))
    assert len(prompt) <= 1500
    assert "still_broken: (90)" in prompt
    assert "Omitted " in prompt
    evidence = Path(prompt.split("Full JSON: ", 1)[1].splitlines()[0])
    assert len(json.loads(evidence.read_text())["still_broken"]) == 90


def test_source_differences_are_shown_as_information_not_as_broken_findings() -> None:
    difference = {
        "detector_id": "drift.unexplained",
        "rule_id": "unexplained",
        "fingerprint": "drift/config",
        "summary": "ConfigMap differs from accepted source",
        "evidence": "synthetic",
        "primary_resource": {"kind": "ConfigMap", "namespace": "app", "name": "policy"},
    }
    payload = {
        "attempt": 1,
        "healthy": False,
        "summary": "one remains",
        "still_broken": [],
        "source_differences": [difference],
        "already_done": [],
    }
    prompt = continuation_prompt(json.dumps(payload))
    assert "still_broken: (0)" in prompt
    assert "source_differences (information, not blocking): (1)" in prompt
    assert ContinuationFeedback.model_validate(payload).source_differences[0].fingerprint == "drift/config"
