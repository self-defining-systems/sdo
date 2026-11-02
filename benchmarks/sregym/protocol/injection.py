"""SREGym v2 injection validity at the benchmark edge, before any incident credit."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from datetime import datetime


class InjectionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    valid: bool = Field(strict=True)
    infra_error: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class InvalidInjectionError(RuntimeError):
    """An ungradeable injection; benchmark evidence must stay outside production tasks."""

    infra_error = "injection_invalid"

    def __init__(self, details: dict[str, Any], *, reply: dict[str, Any] | None = None) -> None:
        super().__init__("injection_invalid: SREGym did not install a valid fault")
        self.details = details
        self.reply = reply or {}
        self.cleanup_error: str | None = None


class PreInjectionIncidentError(InvalidInjectionError):
    """An incident that this injection did not cause: the row is retried, never graded."""

    def __init__(self, message: str, details: dict[str, Any]) -> None:
        super().__init__(details)
        RuntimeError.__init__(self, message)


class IncidentOpenBeforeInjectionError(PreInjectionIncidentError):
    """SDO incident work was still open when the fault was about to be applied, so the lane was not admitted."""

    infra_error = "incident_open_before_injection"


class IncidentPredatesInjectionError(PreInjectionIncidentError):
    """The incident a row would be graded on was detected before the fault was injected."""

    infra_error = "incident_predates_injection"


def ensure_incident_follows_injection(
    incident_id: str | None, detected_at: datetime | None, injection_started_at: datetime | None
) -> None:
    """Reject an incident detected before the injection that this row is bound to."""
    if detected_at is None or injection_started_at is None or detected_at >= injection_started_at:
        return
    raise IncidentPredatesInjectionError(
        f"incident {incident_id or 'unidentified'} was detected at {detected_at.isoformat()}, before the injection "
        f"started at {injection_started_at.isoformat()}",
        {
            "incident_id": incident_id,
            "detected_at": detected_at.isoformat(),
            "injection_started_at": injection_started_at.isoformat(),
        },
    )


def check_injection(reply: dict[str, Any]) -> None:
    # Legacy synthetic workers have no version. A v2 reply must always carry validity.
    # The active SREGym subprocess is pinned to v2; no stage spelling implies validity.
    if reply.get("protocol_version") == 2:
        result = InjectionResult.model_validate(reply.get("injection"))
        if not result.valid:
            raise InvalidInjectionError(result.details, reply=reply)


class FaultComponent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    fault: int = Field(ge=0)
    incident: int = Field(ge=0)
    problem_id: str
    component_index: int | None = None
    injection_order: int = Field(ge=0)
    identity: dict[str, Any] = Field(default_factory=dict)
