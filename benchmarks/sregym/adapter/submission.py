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

from benchmarks.sregym.protocol.schema import TERMINAL_STAGES
from sdo.agents.responder import IncidentStatusReport, IncidentStatusState, live_incident_status


class SubmissionBridgeError(RuntimeError):
    """Raised when the benchmark is not ready to receive a submission."""


SUBMISSION_TIMEOUT_SECONDS = 300
STAGE_POLL_INTERVAL_SECONDS = 1.0


Opener = Callable[..., Any]


def _wait_for_stage(
    api_base: str,
    expected: set[str],
    *,
    opener: Opener,
) -> str:
    deadline = time.monotonic() + SUBMISSION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        request = urllib.request.Request(f"{api_base.rstrip('/')}/status", method="GET")
        with opener(request, timeout=SUBMISSION_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read())
        stage = payload.get("stage") if isinstance(payload, dict) else None
        if stage in expected:
            return str(stage)
        time.sleep(STAGE_POLL_INTERVAL_SECONDS)
    raise SubmissionBridgeError(f"SREGym did not reach one of {sorted(expected)}")


#: Stages after a phase's own stage, where the harness no longer awaits that phase.
_LATER_STAGES: dict[str, frozenset[str]] = {"diagnosis": frozenset({"mitigation"}), "mitigation": frozenset()}


def _diagnosis_not_submitted(stage: str, reason: str) -> dict[str, Any]:
    return {
        "diagnosis": {
            "status": "already_submitted",
            "stage": stage,
            "message": f"diagnosis not submitted: harness stage is {stage!r}; {reason}",
        }
    }


def submit_solution(
    solution: str,
    *,
    phase: Literal["diagnosis", "mitigation"],
    api_base: str,
    opener: Opener = urllib.request.urlopen,
) -> dict[str, Any]:
    base = api_base.rstrip("/")
    # /submit is untyped: the conductor grades whatever arrives as the phase it
    # is in. So the bridge, not the caller, must ensure the harness is awaiting
    # this phase before posting. The conductor also drops (while acknowledging)
    # any submit that arrives while diagnosis is still being graded, so a
    # mitigation waits for its stage to open.
    stage = _wait_for_stage(base, {phase, *_LATER_STAGES[phase], *TERMINAL_STAGES}, opener=opener)
    if stage in TERMINAL_STAGES:
        # A problem that already ended accepted its submissions; a repeated call
        # returns at once instead of waiting for a stage that never reopens.
        if phase == "mitigation":
            return {"mitigation": {"status": "already_submitted"}, "done": {"status": stage}}
        return _diagnosis_not_submitted(stage, "the problem has already ended; nothing more can be submitted.")
    if stage != phase:
        return _diagnosis_not_submitted(
            stage,
            "the diagnosis was already submitted and the harness is awaiting mitigation. Keep repairing and "
            "verifying; run the mitigation submission once when done. Never resubmit a diagnosis: it would be "
            "graded as your mitigation.",
        )
    payload = json.dumps({"solution": solution}).encode()
    phase_request = urllib.request.Request(
        f"{base}/submit",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener(phase_request, timeout=SUBMISSION_TIMEOUT_SECONDS) as response:
        result = json.loads(response.read())
    if not isinstance(result, dict):
        raise SubmissionBridgeError("SREGym submission response is not a JSON object")
    status = result.get("status")
    if status == "done":
        return result
    if status not in {"200", "ok", "acknowledged"}:
        raise SubmissionBridgeError(f"SREGym {phase} submission was not acknowledged")
    if phase == "diagnosis":
        # Like any SREGym agent, proceed on acknowledgement; grading runs
        # asynchronously while the responder repairs.
        return result
    terminal_stage = _wait_for_stage(base, set(TERMINAL_STAGES), opener=opener)
    done = {"status": terminal_stage}
    return {"mitigation": result, "done": done}


#: Exit status when mitigation is withheld because verification failed.
EXIT_VERIFICATION_FAILED = 4


def main(argv: list[str] | None = None, *, status_check: Callable[[], IncidentStatusReport] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("diagnosis", "mitigation"))
    parser.add_argument("solution")
    args = parser.parse_args(argv)
    api_base = os.getenv("SDO_SREGYM_API_BASE", "").strip()
    if not api_base:
        print("SDO_SREGYM_API_BASE is required", file=sys.stderr)
        return 2
    if args.phase == "mitigation":
        # Verify before submit: mitigation ends the problem, so it waits for
        # the same synthetic traffic the controller's closure gate requires.
        status = (status_check or live_incident_status)()
        if status.state == IncidentStatusState.UNHEALTHY:
            print(
                "mitigation not submitted: `python3 -m sdo.agents.responder.status_cli` "
                "reports the application unhealthy.\n" + status.render(),
                file=sys.stderr,
            )
            return EXIT_VERIFICATION_FAILED
    try:
        result = submit_solution(args.solution, phase=args.phase, api_base=api_base)
    except (OSError, ValueError, SubmissionBridgeError, urllib.error.URLError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
