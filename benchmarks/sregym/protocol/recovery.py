"""Credential-free recovery classifications shared by the worker consumer and row writer."""

from __future__ import annotations

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

RecoveryStep = Literal["recover", "refresh", "health", "restore"]
RecoveryCode = Literal["timeout", "execution_failed", "exec_failed", "rollout_failed", "protocol_invalid", "unknown"]


class RecoveryFailureReason(BaseModel):
    """Only closed vocabulary and numeric identities may cross the redaction boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    incident_index: int | None = Field(default=None, ge=0, strict=True)
    component_index: int | None = Field(default=None, ge=0, strict=True)
    step: RecoveryStep
    code: RecoveryCode


class RecoveryError(RuntimeError):
    """Recovery failure with classifications that runners can use without parsing text."""

    def __init__(self, message: str, *, recovery_failures: tuple[RecoveryFailureReason, ...] = ()) -> None:
        super().__init__(message)
        self.recovery_failures = recovery_failures


def recovery_failure_reasons(reply: dict[str, Any], step: RecoveryStep) -> tuple[RecoveryFailureReason, ...]:
    """Project known worker lifecycle facts, never exception text or untrusted strings.

    The harness currently exposes ``error`` or ``timeout``; ``error`` cannot tell us
    whether an exec or rollout failed, so it truthfully maps to execution_failed.
    """
    reasons: list[RecoveryFailureReason] = []
    raw_errors = reply.get("errors")
    errors = cast("list[Any]", raw_errors) if isinstance(raw_errors, list) else []
    for raw_error in errors:
        if not isinstance(raw_error, dict):
            continue
        error = cast("dict[str, Any]", raw_error)
        component = error.get("component_index")
        incident = error.get("incident")
        reasons.append(
            RecoveryFailureReason(
                incident_index=incident if type(incident) is int and incident >= 0 else None,
                component_index=component if type(component) is int and component >= 0 else None,
                step=step,
                code="timeout"
                if error.get("status") == "timeout"
                else ("execution_failed" if error.get("status") == "error" else "unknown"),
            )
        )
    return tuple(reasons) or (RecoveryFailureReason(step=step, code="unknown"),)
