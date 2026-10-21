"""Generated proto contract messages and the canonical protojson convention.

This is the Python edge of the proto single-source-of-truth track. The ``.proto`` files under
``sdo/contracts/proto/`` are the one source; ``buf generate`` emits the messages into ``sdo/contracts/_gen``
and this facade re-exports them under a stable import path so callers never
reach into the generated tree directly.

The generated modules import each other by their proto package path
(``sdodev.contracts.v1alpha1.*``) and reference the vendored ``buf.validate``
options, so the generation root ``sdo/contracts/_gen`` must be importable. We
put it on ``sys.path`` here. The proto package is ``sdodev`` rather than ``sdo``
precisely so that root never shadows this real ``sdo`` package.

Contracts are serialized as **protojson, never binary**, and share one wire
convention with the Go controller (``sdo/operator/runtime`` ``protoContractJSON``):
proto (snake_case) field names, zero-valued fields omitted. ``to_canonical_json``
additionally sorts keys and indents so a payload is byte-comparable across the
Go and Python encoders — the identity the round-trip contract tests assert.
"""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from pathlib import Path
from datetime import datetime, timezone
from typing import TYPE_CHECKING

_GEN_ROOT = Path(__file__).resolve().parent / "_gen"
if str(_GEN_ROOT) not in sys.path:
    sys.path.insert(0, str(_GEN_ROOT))

import protovalidate
from buf.validate import validate_pb2
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from sdodev.contracts.v1alpha1 import (
    common_pb2,
    controller_config_pb2,
    controller_state_pb2,
    late_findings_pb2,
    lifecycle_pb2,
    memory_pb2,
    messages_pb2,
    outcome_pb2,
    telemetry_pb2,
    traffic_pb2,
    worker_pb2,
)

if TYPE_CHECKING:
    from google.protobuf.message import Message

ObjectRef = common_pb2.ObjectRef

ControllerConfig = controller_config_pb2.ControllerConfig

Finding = messages_pb2.Finding
DetectorEvaluation = messages_pb2.DetectorEvaluation
SurfacedPlaybook = messages_pb2.SurfacedPlaybook
PriorOutcomeEvidence = messages_pb2.PriorOutcomeEvidence
StateFieldChange = messages_pb2.StateFieldChange
StateChange = messages_pb2.StateChange
StateChanges = messages_pb2.StateChanges
ObservedStateChange = messages_pb2.ObservedStateChange
FollowUpContext = messages_pb2.FollowUpContext
IncidentRequest = messages_pb2.IncidentRequest
RootCauseEvidence = messages_pb2.RootCauseEvidence
ConfirmedRootCause = messages_pb2.ConfirmedRootCause
AppliedPlaybook = messages_pb2.AppliedPlaybook
VerificationEvidence = messages_pb2.VerificationEvidence
UsageMetrics = messages_pb2.UsageMetrics
TimingMetrics = messages_pb2.TimingMetrics
RepairActionReceipt = messages_pb2.RepairActionReceipt
IncidentResult = messages_pb2.IncidentResult
LateFindingsPull = messages_pb2.LateFindingsPull
IncidentView = messages_pb2.IncidentView
DetectorTimelineEntry = messages_pb2.DetectorTimelineEntry
IncidentClosure = messages_pb2.IncidentClosure
MemoryWatch = messages_pb2.MemoryWatch
MemoryScript = messages_pb2.MemoryScript
MemoryDetector = messages_pb2.MemoryDetector
MemoryPlaybook = messages_pb2.MemoryPlaybook
OperationalMemorySnapshot = messages_pb2.OperationalMemorySnapshot
OutcomeRecord = outcome_pb2.OutcomeRecord
OutcomeTimestamps = outcome_pb2.OutcomeTimestamps
DiagnosisVerification = outcome_pb2.DiagnosisVerification

MemoryDiagnostic = memory_pb2.MemoryDiagnostic
MemoryCheckReport = memory_pb2.MemoryCheckReport
MemoryArtifact = memory_pb2.MemoryArtifact
MemoryListing = memory_pb2.MemoryListing
MemoryOutcomes = memory_pb2.MemoryOutcomes

FiringRecord = telemetry_pb2.FiringRecord
LifecycleEvent = telemetry_pb2.LifecycleEvent

LateFinding = late_findings_pb2.LateFinding
LateFindingsReport = late_findings_pb2.LateFindingsReport
LateFindingsSnapshot = late_findings_pb2.LateFindingsSnapshot
LateFindingsHistory = late_findings_pb2.LateFindingsHistory
LateFindingsReceipt = late_findings_pb2.LateFindingsReceipt

ControllerState = controller_state_pb2.ControllerState
FindingState = controller_state_pb2.FindingState
BatcherState = controller_state_pb2.BatcherState
ClosureReceipt = controller_state_pb2.ClosureReceipt
ClosureFailure = controller_state_pb2.ClosureFailure
BrokerLedger = controller_state_pb2.BrokerLedger
ReflectionDecision = controller_state_pb2.ReflectionDecision
ControllerRolloutExpectation = controller_state_pb2.ControllerRolloutExpectation
ControllerRolloutReceipt = controller_state_pb2.ControllerRolloutReceipt

TrafficSLO = traffic_pb2.TrafficSLO
TrafficWorkloadScenario = traffic_pb2.TrafficWorkloadScenario
TrafficLink = traffic_pb2.TrafficLink
TrafficWorkload = traffic_pb2.TrafficWorkload

AgentRegistration = worker_pb2.AgentRegistration
AgentCapability = worker_pb2.AgentCapability
KubernetesJobLaunch = worker_pb2.KubernetesJobLaunch
LocalProcessLaunch = worker_pb2.LocalProcessLaunch
ProposalWorkspace = worker_pb2.ProposalWorkspace
AgentTask = worker_pb2.AgentTask
AgentResult = worker_pb2.AgentResult
ReflectionRequest = worker_pb2.ReflectionRequest
ReflectionResult = worker_pb2.ReflectionResult
SourceDeploymentRequest = lifecycle_pb2.SourceDeploymentRequest
SourceDeploymentResult = lifecycle_pb2.SourceDeploymentResult
DeploymentPlan = lifecycle_pb2.DeploymentPlan
RawManifestStrategy = lifecycle_pb2.RawManifestStrategy
ImageBuild = lifecycle_pb2.ImageBuild
ArchitectureRequest = lifecycle_pb2.ArchitectureRequest
ArchitectureResult = lifecycle_pb2.ArchitectureResult
HealthJudgeRequest = lifecycle_pb2.HealthJudgeRequest
HealthJudgeResult = lifecycle_pb2.HealthJudgeResult
SourceResource = lifecycle_pb2.SourceResource

__all__ = [
    "AgentCapability",
    "AgentRegistration",
    "AgentResult",
    "AgentTask",
    "AppliedPlaybook",
    "ConfirmedRootCause",
    "ControllerConfig",
    "DetectorEvaluation",
    "DetectorTimelineEntry",
    "Finding",
    "FiringRecord",
    "LateFinding",
    "LateFindingsHistory",
    "LateFindingsReceipt",
    "LateFindingsReport",
    "LateFindingsSnapshot",
    "LifecycleEvent",
    "OUTCOME_CLASSIFICATIONS",
    "OutcomeClassification",
    "ROOT_CAUSE_EVIDENCE_KINDS",
    "SCHEMA_VERSION",
    "validate",
    "FollowUpContext",
    "IncidentClosure",
    "IncidentRequest",
    "IncidentResult",
    "LateFindingsPull",
    "MemoryDetector",
    "MemoryPlaybook",
    "MemoryScript",
    "MemoryWatch",
    "OperationalMemorySnapshot",
    "BatcherState",
    "BrokerLedger",
    "ClosureFailure",
    "ClosureReceipt",
    "ControllerRolloutExpectation",
    "ControllerRolloutReceipt",
    "ControllerState",
    "FindingState",
    "ReflectionDecision",
    "datetime_timestamp",
    "timestamp_datetime",
    "timestamp_rfc3339",
    "IncidentView",
    "KubernetesJobLaunch",
    "LocalProcessLaunch",
    "ObjectRef",
    "MemoryArtifact",
    "MemoryCheckReport",
    "MemoryDiagnostic",
    "MemoryListing",
    "MemoryOutcomes",
    "OutcomeRecord",
    "OutcomeTimestamps",
    "DiagnosisVerification",
    "ObservedStateChange",
    "PriorOutcomeEvidence",
    "ProposalWorkspace",
    "ReflectionRequest",
    "ReflectionResult",
    "SourceDeploymentRequest",
    "SourceDeploymentResult",
    "DeploymentPlan",
    "RawManifestStrategy",
    "ImageBuild",
    "ArchitectureRequest",
    "ArchitectureResult",
    "HealthJudgeRequest",
    "HealthJudgeResult",
    "SourceResource",
    "RepairActionReceipt",
    "RootCauseEvidence",
    "StateChange",
    "StateChanges",
    "StateFieldChange",
    "SurfacedPlaybook",
    "TimingMetrics",
    "TrafficLink",
    "TrafficSLO",
    "TrafficWorkload",
    "TrafficWorkloadScenario",
    "UsageMetrics",
    "VerificationEvidence",
    "parse_json",
    "to_canonical_json",
]


def to_canonical_json(message: Message) -> str:
    """Serialize a proto message to the canonical, byte-stable protojson form."""
    wire = json_format.MessageToJson(message, preserving_proto_field_name=True)
    return json.dumps(json.loads(wire), indent=2, sort_keys=True) + "\n"


def parse_json(text: str, message: Message) -> Message:
    """Parse protojson ``text`` into ``message`` (populated in place and returned)."""
    return json_format.Parse(text, message)


def timestamp_datetime(value: Timestamp) -> datetime:
    """The timezone-aware UTC ``datetime`` of a proto ``Timestamp``."""
    return value.ToDatetime(tzinfo=timezone.utc)


def datetime_timestamp(value: datetime) -> Timestamp:
    """A proto ``Timestamp`` for ``value``; a naive ``datetime`` is taken as UTC."""
    stamp = Timestamp()
    stamp.FromDatetime(value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc))
    return stamp


def timestamp_rfc3339(value: Timestamp) -> str:
    """The RFC 3339 UTC text of a proto ``Timestamp``, the protojson form."""
    return value.ToJsonString()


def _string_rule(message: type[Message], field: str):
    options = message.DESCRIPTOR.fields_by_name[field].GetOptions()  # type: ignore[attr-defined]
    return options.Extensions[validate_pb2.field].string


# Values the proto states and Python reads from it rather than restating.
SCHEMA_VERSION: str = _string_rule(AgentTask, "schema_version").const
OUTCOME_CLASSIFICATIONS: tuple[str, ...] = tuple(getattr(_string_rule(OutcomeRecord, "classification"), "in"))
ROOT_CAUSE_EVIDENCE_KINDS: tuple[str, ...] = tuple(getattr(_string_rule(RootCauseEvidence, "kind"), "in"))
# Attribute access by upper-cased name (``OutcomeClassification.SUCCESS``); the
# members are the proto's ``in`` list, so there is no second list to drift.
OutcomeClassification = SimpleNamespace(**{value.upper(): value for value in OUTCOME_CLASSIFICATIONS})


def validate(message: Message) -> None:
    """Enforce the proto's protovalidate rules, raising ``ValueError`` on violation."""
    try:
        protovalidate.validate(message)
    except protovalidate.ValidationError as exc:
        details = "; ".join(_describe(violation) for violation in exc.violations)
        raise ValueError(f"invalid {message.DESCRIPTOR.name}: {details}") from exc


def _describe(violation: protovalidate.Violation) -> str:
    field = violation.proto.field
    path = ".".join(element.field_name for element in field.elements) if field is not None else ""
    return f"{path}: {violation.proto.message}"
