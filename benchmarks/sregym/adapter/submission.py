"""Narrow SREGym submission transport for an in-cluster responder Job.

This module forwards responder-authored answers to the conductor. It neither
launches the responder nor converts benchmark grading into health state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any, Literal

from google.protobuf import json_format

from benchmarks.sregym.protocol.submission_guard import SubmissionGuardError, evaluate_submission
from sdo.contracts import SUBMISSION_PHASES, ConductorSolution, SubmissionGuardDecision, SubmissionGuardFailure


class SubmissionBridgeError(RuntimeError):
    """Keep the Go rejection attached to this one submission attempt."""

    def __init__(self, message: str, failure: SubmissionGuardFailure | None = None) -> None:
        super().__init__(message)
        self.failure = failure


SUBMISSION_TIMEOUT_SECONDS = 300
STAGE_POLL_INTERVAL_SECONDS = 1.0


Opener = Callable[..., Any]


def _guard(
    phase: str, payload: object, *, timeout: float, acknowledgement: bool = False, submission_sent: bool = False
) -> SubmissionGuardDecision:
    try:
        decision = evaluate_submission(
            phase, payload, acknowledgement=acknowledgement, submission_sent=submission_sent, timeout=timeout
        )
    except SubmissionGuardError as exc:
        raise SubmissionBridgeError(str(exc)) from exc
    if decision.HasField("failure"):
        raise SubmissionBridgeError(decision.failure.message, decision.failure)
    return decision


def _wait_for_decision(
    api_base: str, phase: str, *, opener: Opener, submission_sent: bool = False
) -> SubmissionGuardDecision:
    deadline = time.monotonic() + SUBMISSION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        request = urllib.request.Request(f"{api_base.rstrip('/')}/status", method="GET")
        with opener(request, timeout=SUBMISSION_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read())
        remaining = max(0.001, deadline - time.monotonic())
        decision = _guard(phase, payload, timeout=min(30.0, remaining), submission_sent=submission_sent)
        if decision.action != SubmissionGuardDecision.ACTION_WAIT:
            return decision
        time.sleep(STAGE_POLL_INTERVAL_SECONDS)
    raise SubmissionBridgeError(f"SREGym did not reach the required stage for {phase}")


def submit_solution(
    solution: str,
    *,
    phase: Literal["diagnosis", "mitigation"],
    api_base: str,
    opener: Opener = urllib.request.urlopen,
) -> dict[str, Any]:
    base = api_base.rstrip("/")
    decision = _wait_for_decision(base, phase, opener=opener)
    if decision.action == SubmissionGuardDecision.ACTION_RETURN:
        return json_format.MessageToDict(decision.response, preserving_proto_field_name=True)
    payload = json_format.MessageToJson(
        ConductorSolution(solution=solution),
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    ).encode()
    phase_request = urllib.request.Request(
        f"{base}/submit",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener(phase_request, timeout=SUBMISSION_TIMEOUT_SECONDS) as response:
        result = json.loads(response.read())
    decision = _guard(phase, result, timeout=30.0, acknowledgement=True)
    if decision.action == SubmissionGuardDecision.ACTION_RETURN:
        return result
    completed = _wait_for_decision(base, phase, opener=opener, submission_sent=True)
    done = json_format.MessageToDict(completed.response.done, preserving_proto_field_name=True)
    return {"mitigation": result, "done": done}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=SUBMISSION_PHASES)
    parser.add_argument("solution")
    args = parser.parse_args(argv)
    api_base = os.getenv("SDO_SREGYM_API_BASE", "").strip()
    if not api_base:
        print("SDO_SREGYM_API_BASE is required", file=sys.stderr)
        return 2
    try:
        result = submit_solution(args.solution, phase=args.phase, api_base=api_base)
    except (OSError, ValueError, SubmissionBridgeError, urllib.error.URLError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
