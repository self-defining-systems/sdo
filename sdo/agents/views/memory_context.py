"""Read-only incident closure and outcome context supplied to agents.

These DTOs describe logical evidence. The Go memory edge owns its storage.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic resolves this at runtime.

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.views.models import (
    DetectorEvaluation,  # noqa: TC001 - Pydantic resolves these field types at runtime.
    DetectorTimelineEntry,  # noqa: TC001
    IncidentRequest,  # noqa: TC001
    IncidentResult,  # noqa: TC001
    StateChanges,  # noqa: TC001
)


class BrokerClosure(BaseModel):
    """Controller verified closure sent as an agent task, without broker API ownership."""

    model_config = ConfigDict(extra="allow")
    request: IncidentRequest
    result: IncidentResult | None = None
    dispatch_error: str | None = None
    final_detector_states: list[DetectorEvaluation] = Field(default_factory=list)
    incident_detector_states: list[DetectorEvaluation] = Field(default_factory=list)
    final_state_changes: StateChanges | None = None
    detector_timeline: list[DetectorTimelineEntry] = Field(default_factory=list)
    incident_detector_fired_before_dispatch: bool = False
    incident_detector_fired_after_dispatch: bool = False
    no_incident_detector_fired: bool = False
    detected_at: datetime
    dispatched_at: datetime
    responder_completed_at: datetime
    verified_at: datetime
