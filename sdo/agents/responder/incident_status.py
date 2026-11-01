"""Render incident verification returned by the controller's existing tool transport."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.responder import tools_cli


class IncidentStatusState(str, Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNAVAILABLE = "unavailable"


class VerifyScenarioVerdict(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    scenario: str
    healthy: bool
    qualified: bool = True
    recent_failures: list[str] = Field(default_factory=list, alias="recentFailures")


class VerifyBurstResult(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    workload: str
    healthy: bool
    verdicts: list[VerifyScenarioVerdict] = Field(default_factory=list)


@dataclass(frozen=True)
class IncidentStatusReport:
    state: IncidentStatusState
    burst: VerifyBurstResult | None
    detail: str

    def render(self) -> str:
        lines = [f"{self.state.value}: {self.detail}"]
        if self.burst:
            for verdict in self.burst.verdicts:
                lines.extend(verdict.recent_failures)
        return "\n".join(lines)


def live_incident_status() -> IncidentStatusReport:
    """Benchmark submission adapters use the same incident tool as `sdo verify`."""
    try:
        response = tools_cli.verify_incident()
    except (OSError, ValueError, TimeoutError) as exc:
        return IncidentStatusReport(IncidentStatusState.UNAVAILABLE, None, str(exc))
    raw_burst = response.get("burst")
    burst = VerifyBurstResult.model_validate(raw_burst) if raw_burst else None
    if response.get("error"):
        state = IncidentStatusState.UNAVAILABLE
    else:
        state = IncidentStatusState.HEALTHY if response.get("healthy") else IncidentStatusState.UNHEALTHY
    return IncidentStatusReport(state, burst, str(response.get("summary", "")))
