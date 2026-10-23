"""SREGym v2 injection validity at the benchmark edge, before any incident credit."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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
