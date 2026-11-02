from __future__ import annotations

import json

import pytest

from benchmarks.sregym.adapter import submission
from benchmarks.sregym.adapter.submission import SubmissionBridgeError, submit_solution


class Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


def test_submission_bridge_returns_on_diagnosis_acknowledgement_without_waiting_for_grading() -> None:
    requests = []

    def opener(request, timeout: int):
        assert timeout == 300
        requests.append(request)
        if request.full_url.endswith("/status"):
            return Response({"stage": "diagnosis"})
        assert request.full_url.endswith("/submit"), "diagnosis must not block on benchmark grading"
        assert json.loads(request.data) == {"solution": "payment pods are fully isolated"}
        return Response({"status": "200", "message": "Submission received"})

    result = submit_solution(
        "payment pods are fully isolated",
        phase="diagnosis",
        api_base="http://conductor:8123",
        opener=opener,
    )

    assert result == {"status": "200", "message": "Submission received"}
    assert [request.full_url for request in requests] == [
        "http://conductor:8123/status",
        "http://conductor:8123/submit",
    ]


@pytest.mark.parametrize("stage", ["mitigation", "done", "awaiting_cleanup"])
def test_diagnosis_after_the_diagnosis_stage_is_never_posted_as_another_phase(stage: str) -> None:
    """The conductor's /submit is untyped, so a late diagnosis would be graded as the mitigation."""

    requests = []

    def opener(request, timeout: int):
        requests.append(request.full_url.rsplit("/", 1)[-1])
        assert request.full_url.endswith("/status"), "a wrong-phase diagnosis must not reach /submit"
        return Response({"stage": stage})

    result = submit_solution("also restore frontend", phase="diagnosis", api_base="http://c:8123", opener=opener)

    assert requests == ["status"]
    assert result["diagnosis"]["status"] == "already_submitted"
    assert result["diagnosis"]["stage"] == stage
    message = result["diagnosis"]["message"]
    assert "not submitted" in message
    assert ("mitigation" if stage == "mitigation" else "ended") in message


def test_diagnosis_waits_for_the_conductor_to_open_the_diagnosis_stage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("benchmarks.sregym.adapter.submission.time.sleep", lambda _seconds: None)
    stages = iter(["setup", "diagnosis"])
    requests = []

    def opener(request, timeout: int):
        requests.append(request.full_url.rsplit("/", 1)[-1])
        if request.full_url.endswith("/submit"):
            return Response({"status": "200", "message": "Submission received"})
        return Response({"stage": next(stages)})

    submit_solution("selector typo", phase="diagnosis", api_base="http://conductor:8123", opener=opener)

    assert requests == ["status", "status", "submit"]


@pytest.mark.parametrize("terminal_stage", ["done", "awaiting_cleanup"])
def test_submission_bridge_records_mitigation_then_completes_autonomous_run(terminal_stage: str) -> None:
    requests = []

    def opener(request, timeout: int):
        assert timeout == 300
        requests.append(request)
        if request.full_url.endswith("/submit"):
            assert json.loads(request.data) == {"solution": "policy deleted"}
            return Response({"status": "200", "message": "Submission received"})
        assert request.full_url.endswith("/status")
        assert request.data is None
        submitted = any(item.full_url.endswith("/submit") for item in requests)
        return Response({"stage": terminal_stage if submitted else "mitigation"})

    result = submit_solution(
        "policy deleted",
        phase="mitigation",
        api_base="http://conductor:8123",
        opener=opener,
    )

    assert result == {
        "mitigation": {"status": "200", "message": "Submission received"},
        "done": {"status": terminal_stage},
    }
    assert [request.full_url for request in requests] == [
        "http://conductor:8123/status",
        "http://conductor:8123/submit",
        "http://conductor:8123/status",
    ]


def test_mitigation_waits_for_diagnosis_grading_before_submitting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The conductor drops a submit that arrives while diagnosis is still being graded."""

    monkeypatch.setattr("benchmarks.sregym.adapter.submission.time.sleep", lambda _seconds: None)
    stages = iter(["diagnosis", "diagnosis", "mitigation", "done"])
    requests = []

    def opener(request, timeout: int):
        requests.append(request.full_url.rsplit("/", 1)[-1])
        if request.full_url.endswith("/submit"):
            return Response({"status": "200", "message": "Submission received"})
        return Response({"stage": next(stages)})

    result = submit_solution("configmap restored", phase="mitigation", api_base="http://conductor:8123", opener=opener)

    assert requests == ["status", "status", "status", "submit", "status"]
    assert result["done"] == {"status": "done"}


def test_submission_bridge_rejects_unacknowledged_autonomous_diagnosis() -> None:
    def opener(request, timeout: int):
        assert timeout == 300
        if request.full_url.endswith("/status"):
            return Response({"stage": "diagnosis"})
        return Response({"status": "error"})

    with pytest.raises(SubmissionBridgeError, match="not acknowledged"):
        submit_solution("answer", phase="diagnosis", api_base="http://conductor:8123", opener=opener)


@pytest.mark.parametrize("terminal_stage", ["done", "awaiting_cleanup"])
def test_repeated_mitigation_after_the_problem_ended_returns_at_once_without_resubmitting(terminal_stage: str) -> None:
    """A second mitigation call must not block for the submission timeout waiting for a stage that never reopens."""

    requests = []

    def opener(request, timeout: int):
        requests.append(request.full_url.rsplit("/", 1)[-1])
        assert request.full_url.endswith("/status"), "a finished problem must not receive another submit"
        return Response({"stage": terminal_stage})

    result = submit_solution("configmap restored again", phase="mitigation", api_base="http://c:8123", opener=opener)

    assert requests == ["status"]
    assert result == {"mitigation": {"status": "already_submitted"}, "done": {"status": terminal_stage}}


@pytest.mark.parametrize("phase", ["diagnosis", "mitigation"])
def test_main_submits_each_phase_without_a_status_gate(phase: str, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []
    monkeypatch.setenv("SDO_SREGYM_API_BASE", "http://bridge:18000")
    monkeypatch.setattr(
        submission,
        "submit_solution",
        lambda solution, *, phase, **_: calls.append((phase, solution)) or {"done": {"status": "done"}},
    )

    assert submission.main([phase, "removed the stray selector label"]) == 0
    assert calls == [(phase, "removed the stray selector label")]


@pytest.mark.parametrize("stage", [42, ["mitigation"], {"bad": "stage"}])
def test_malformed_stage_is_rejected_without_posting_or_poisoning_next_call(stage: object) -> None:
    posted = []
    current = [stage]

    def opener(request, timeout):
        if request.full_url.endswith("/status"):
            return Response({"stage": current[0]})
        posted.append(request.data)
        return Response({"status": "200"})

    with pytest.raises(SubmissionBridgeError, match="ProtoJSON") as rejected:
        submit_solution("first", phase="diagnosis", api_base="http://c", opener=opener)
    from sdo.contracts import SubmissionGuardFailure

    assert rejected.value.failure.code == SubmissionGuardFailure.CODE_INVALID_WIRE
    assert posted == []
    current[0] = "diagnosis"
    assert submit_solution("second", phase="diagnosis", api_base="http://c", opener=opener) == {"status": "200"}
    assert len(posted) == 1
