"""Read-only incident closure and outcome context supplied to agents.

These DTOs describe logical evidence. The Go memory edge owns its storage.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 - Pydantic resolves this at runtime.
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.views.models import (
    ConfirmedRootCause,
    DetectorEvaluation,
    DetectorTimelineEntry,
    Finding,
    IncidentRequest,
    IncidentResult,
    RepairActionReceipt,
    StateChanges,
    UsageMetrics,
)


class OutcomeClassification(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    EXTERNAL_RECOVERY = "external_recovery"
    CLEARED_WITHOUT_ACTION = "cleared_without_action"
    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EvidenceCheck(BaseModel):
    model_config = ConfigDict(extra="allow")
    kind: str = ""
    source: str = ""
    verified: bool | None = None
    reason: str = ""


class DetectorFlip(BaseModel):
    model_config = ConfigDict(extra="allow")
    detector_id: str = ""
    flipped: bool = False


class RepairAttribution(BaseModel):
    model_config = ConfigDict(extra="allow")
    reason: str = ""


class DiagnosisVerification(BaseModel):
    model_config = ConfigDict(extra="allow")
    summary: str = ""
    verdict: str = ""
    evidence: list[EvidenceCheck] = Field(default_factory=list)
    detectors: list[DetectorFlip] = Field(default_factory=list)
    repair: RepairAttribution | None = None


class OutcomeRecord(BaseModel):
    """Stable logical outcome shape; unknown historical evidence is preserved."""

    model_config = ConfigDict(extra="allow")
    schema_version: int = 1
    incident_id: str = Field(min_length=1)
    source_commit: str = ""
    deployed_commit: str = ""
    detector_history: list[DetectorEvaluation] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    surfaced_playbooks: list[str] = Field(default_factory=list)
    inspected_playbooks: list[str] = Field(default_factory=list)
    confirmed_playbooks: list[str] = Field(default_factory=list)
    rejected_playbooks: list[str] = Field(default_factory=list)
    applied_playbooks: list[str] = Field(default_factory=list)
    confirmed_root_causes: list[ConfirmedRootCause] = Field(default_factory=list)
    final_health_detector_state: list[DetectorEvaluation] = Field(default_factory=list)
    classification: OutcomeClassification
    repair_commit: str | None = None
    repair_actions: list[RepairActionReceipt] = Field(default_factory=list)
    memory_commit: str | None = None
    responder_backend: str = ""
    responder_model: str = ""
    usage: UsageMetrics = Field(default_factory=lambda: UsageMetrics(llm_calls=0, input_tokens=0, output_tokens=0))
    diagnosis_verification: list[DiagnosisVerification] = Field(default_factory=list)


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
