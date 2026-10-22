"""Byte transport to the benchmark-only Go submission guard."""

from __future__ import annotations

import atexit
import json
import os
import shutil
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path
from subprocess import run

from google.protobuf import json_format

from sdo.contracts import SubmissionGuardDecision, SubmissionGuardRequest, validate


class SubmissionGuardError(RuntimeError):
    """The Go guard transport did not return a valid decision."""


@lru_cache(maxsize=1)
def _guard_binary() -> str:
    specified = os.environ.get("SDO_SUBMISSION_GUARD_BIN", "").strip()
    if specified:
        if not Path(specified).is_absolute():
            raise SubmissionGuardError("SDO_SUBMISSION_GUARD_BIN must be an absolute path")
        return specified
    installed = shutil.which("sdo-submission-guard")
    if installed:
        return installed
    module = Path(__file__).with_name("submission_go")
    if not (module / "go.mod").is_file():
        raise SubmissionGuardError("Go submission guard source is unavailable; set SDO_SUBMISSION_GUARD_BIN")
    directory = Path(tempfile.mkdtemp(prefix="sdo-submission-guard-"))
    atexit.register(shutil.rmtree, directory, ignore_errors=True)
    binary = directory / "sdo-submission-guard"
    try:
        built = run(
            ["go", "build", "-o", str(binary), "./cmd/sdo-submission-guard"],
            cwd=module,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SubmissionGuardError(f"Go submission guard could not build: {exc}") from exc
    if built.returncode:
        raise SubmissionGuardError(f"Go submission guard build failed: {built.stderr[-4096:]}")
    return str(binary)


def evaluate_submission(
    phase: str,
    payload: object,
    *,
    acknowledgement: bool = False,
    submission_sent: bool = False,
    timeout: float,
) -> SubmissionGuardDecision:
    request = SubmissionGuardRequest(phase=phase, submission_sent=submission_sent)
    document = json_format.MessageToDict(request, preserving_proto_field_name=True)
    # The external response is opaque until Go decodes its generated type.
    document["acknowledgement" if acknowledgement else "status"] = payload
    try:
        completed = run(
            [_guard_binary()], input=json.dumps(document), capture_output=True, text=True, timeout=timeout, check=False
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SubmissionGuardError(f"Go submission guard could not run: {exc}") from exc
    if completed.returncode:
        raise SubmissionGuardError(f"Go submission guard failed: {completed.stderr[-4096:]}")
    try:
        result = json_format.Parse(completed.stdout, SubmissionGuardDecision())
        if result.HasField("failure"):
            validate(result.failure)
        elif result.action not in (
            SubmissionGuardDecision.ACTION_WAIT,
            SubmissionGuardDecision.ACTION_POST,
            SubmissionGuardDecision.ACTION_RETURN,
        ):
            raise ValueError("missing guard action")
    except (json_format.ParseError, ValueError) as exc:
        raise SubmissionGuardError(f"Go submission guard returned an invalid decision: {exc}") from exc
    return result
