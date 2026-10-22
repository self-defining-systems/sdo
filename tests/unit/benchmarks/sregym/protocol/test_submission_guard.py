from __future__ import annotations

from types import SimpleNamespace

import pytest

from benchmarks.sregym.protocol import submission_guard as guard
from sdo.contracts import SubmissionGuardFailure


def test_guard_uses_generated_decision_and_preserves_raw_status(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(guard, "_guard_binary", lambda: "/guard")

    def run(args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            returncode=0, stdout='{"failure":{"code":"CODE_INVALID_WIRE","message":"invalid stage"}}'
        )

    monkeypatch.setattr(guard, "run", run)
    result = guard.evaluate_submission("diagnosis", {"stage": ["mitigation"]}, timeout=2)
    assert result.failure.code == SubmissionGuardFailure.CODE_INVALID_WIRE
    assert calls[0][0] == ["/guard"]
    assert '"stage": ["mitigation"]' in calls[0][1]["input"]
    assert calls[0][1]["timeout"] == 2


@pytest.mark.parametrize("payload", ["not json", "{}", '{"failure":{"message":"untyped"}}'])
def test_guard_rejects_invalid_decision_contract(payload: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "_guard_binary", lambda: "/guard")
    monkeypatch.setattr(guard, "run", lambda *_, **__: SimpleNamespace(returncode=0, stdout=payload))
    with pytest.raises(guard.SubmissionGuardError):
        guard.evaluate_submission("diagnosis", {"stage": "mitigation"}, timeout=2)
