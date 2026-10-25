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
    assert '"still_broken"' in prompt
    assert '"already_done"' in prompt
    assert "third/api" in prompt
    assert "repair-a" in prompt
    assert "Continue the same incident" in prompt
    template = Path("sdo/agents/responder/prompts/continuation.md").read_text()
    assert len(template) <= 1500
    assert ContinuationFeedback.model_validate(payload).still_broken[0].fingerprint == "third/api"


def test_continuation_rejects_untyped_lists() -> None:
    with pytest.raises(ValueError, match="still_broken"):
        continuation_prompt('{"attempt":1,"healthy":false,"summary":"x","still_broken":["opaque"],"already_done":[]}')
