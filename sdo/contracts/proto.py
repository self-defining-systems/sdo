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
from typing import TYPE_CHECKING, TypeVar

_GEN_ROOT = Path(__file__).resolve().parent / "_gen"
if str(_GEN_ROOT) not in sys.path:
    sys.path.insert(0, str(_GEN_ROOT))

import protovalidate
from buf.validate import validate_pb2
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp

from sdodev.contracts.v1alpha1 import (
    benchmark_receipt_pb2,
    benchmark_submission_pb2,
    common_pb2,
    controller_config_pb2,
    controller_observation_pb2,
    controller_state_pb2,
    late_findings_pb2,
    lifecycle_pb2,
    memory_pb2,
    memory_diagnostic_pb2,
    messages_pb2,
    mitigation_menu_pb2,
    outcome_pb2,
    program_pb2,
    telemetry_pb2,
    traffic_pb2,
    worker_pb2,
)

if TYPE_CHECKING:
    from google.protobuf.message import Message

_MessageT = TypeVar("_MessageT", bound="Message")

ObjectRef = common_pb2.ObjectRef
ResponderJobEvidence = benchmark_receipt_pb2.ResponderJobEvidence
NativeProductionReceipt = benchmark_receipt_pb2.NativeProductionReceipt
ReceiptFailure = benchmark_receipt_pb2.ReceiptFailure
NativeReceiptResult = benchmark_receipt_pb2.NativeReceiptResult
ReceiptDrainResult = benchmark_receipt_pb2.ReceiptDrainResult
RejectedProductionReceipt = benchmark_receipt_pb2.RejectedProductionReceipt
ConductorStatus = benchmark_submission_pb2.ConductorStatus
ConductorAcknowledgement = benchmark_submission_pb2.ConductorAcknowledgement
ConductorSolution = benchmark_submission_pb2.ConductorSolution
SubmissionGuardRequest = benchmark_submission_pb2.SubmissionGuardRequest
SubmissionPhaseStatus = benchmark_submission_pb2.SubmissionPhaseStatus
SubmissionBridgeResponse = benchmark_submission_pb2.SubmissionBridgeResponse
SubmissionGuardFailure = benchmark_submission_pb2.SubmissionGuardFailure
SubmissionGuardDecision = benchmark_submission_pb2.SubmissionGuardDecision
ControllerObservationRequest = controller_observation_pb2.ControllerObservationRequest
ControllerObservationFailure = controller_observation_pb2.ControllerObservationFailure
ControllerObservation = controller_observation_pb2.ControllerObservation


PlaybookCoverage = program_pb2.PlaybookCoverage
PlaybookMatch = program_pb2.PlaybookMatch
ProgramSpec = program_pb2.ProgramSpec
ParamSpec = program_pb2.ParamSpec
Binding = program_pb2.Binding
Verdict = program_pb2.Verdict
Evidence = program_pb2.Evidence
Plan = program_pb2.Plan
Action = program_pb2.Action
PatchObject = program_pb2.PatchObject
ListEdit = program_pb2.ListEdit
ApplyObject = program_pb2.ApplyObject
KubernetesRequest = program_pb2.KubernetesRequest
WaitFor = program_pb2.WaitFor
Command = program_pb2.Command
HttpRequest = program_pb2.HttpRequest
EnsureExec = program_pb2.EnsureExec
EnsureHttp = program_pb2.EnsureHttp
RunJob = program_pb2.RunJob
CreateFromSource = program_pb2.CreateFromSource
DeleteObject = program_pb2.DeleteObject
PlanObjectEffect = program_pb2.PlanObjectEffect
ReconcileFromSource = program_pb2.ReconcileFromSource
Rollback = program_pb2.Rollback
SetField = program_pb2.SetField
PlanValue = program_pb2.PlanValue
ValueField = program_pb2.ValueField
ValueProvenance = program_pb2.ValueProvenance
ActionReport = program_pb2.ActionReport
DeletePod = program_pb2.DeletePod
ScaleWorkload = program_pb2.ScaleWorkload
Exec = program_pb2.Exec
RestartRollout = program_pb2.RestartRollout
ProgramRun = program_pb2.ProgramRun
PlanApplication = program_pb2.PlanApplication
MitigationMenu = mitigation_menu_pb2.MitigationMenu
MitigationOption = mitigation_menu_pb2.MitigationOption
MitigationSelectionSet = mitigation_menu_pb2.MitigationSelectionSet
MitigationOptionSelection = mitigation_menu_pb2.MitigationOptionSelection
MitigationSelection = mitigation_menu_pb2.MitigationSelection
MitigationReportQuery = mitigation_menu_pb2.MitigationReportQuery
MitigationReport = mitigation_menu_pb2.MitigationReport
MitigationRoundReport = mitigation_menu_pb2.MitigationRoundReport
MitigationActionReceipt = mitigation_menu_pb2.MitigationActionReceipt
MitigationBarrier = mitigation_menu_pb2.MitigationBarrier
MitigationObservation = mitigation_menu_pb2.MitigationObservation
MitigationSession = mitigation_menu_pb2.MitigationSession
MitigationSessionCheckpoint = mitigation_menu_pb2.MitigationSessionCheckpoint

ControllerConfig = controller_config_pb2.ControllerConfig

Finding = messages_pb2.Finding
DetectorEvaluation = messages_pb2.DetectorEvaluation
SurfacedPlaybook = messages_pb2.SurfacedPlaybook
PriorOutcomeEvidence = messages_pb2.PriorOutcomeEvidence
StateFieldChange = messages_pb2.StateFieldChange
StateChange = messages_pb2.StateChange
StateChanges = messages_pb2.StateChanges
NamespaceOnboarding = messages_pb2.NamespaceOnboarding
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

MitigationReplayNote = memory_pb2.MitigationReplayNote
MemoryDiagnostic = memory_diagnostic_pb2.MemoryDiagnostic
MemoryConflict = memory_diagnostic_pb2.MemoryConflict
AcceptedMemoryChange = memory_diagnostic_pb2.AcceptedMemoryChange
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
ReflectionEligibility = program_pb2.ReflectionEligibility
ReflectionCauseWork = program_pb2.ReflectionCauseWork
ReflectionWorkItem = program_pb2.ReflectionWorkItem
ReflectionRequest = worker_pb2.ReflectionRequest
ReflectionResult = worker_pb2.ReflectionResult
DriftEvent = messages_pb2.DriftEvent
DriftEvidence = messages_pb2.DriftEvidence
SdkGap = outcome_pb2.SdkGap
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
    "PatchObject",
    "ListEdit",
    "ApplyObject",
    "KubernetesRequest",
    "WaitFor",
    "Command",
    "HttpRequest",
    "EnsureExec",
    "EnsureHttp",
    "RunJob",
    "MitigationSession",
    "MitigationSessionCheckpoint",
    "MitigationMenu",
    "MitigationOption",
    "MitigationSelection",
    "MitigationReportQuery",
    "MitigationReport",
    "MitigationRoundReport",
    "MitigationActionReceipt",
    "MitigationBarrier",
    "MitigationObservation",
    "NATIVE_RECEIPT_SCHEMA_VERSION",
    "ResponderJobEvidence",
    "NativeProductionReceipt",
    "ReceiptFailure",
    "NativeReceiptResult",
    "ReceiptDrainResult",
    "RejectedProductionReceipt",
    "SUBMISSION_PHASES",
    "CONDUCTOR_TERMINAL_STAGES",
    "ConductorStatus",
    "ConductorAcknowledgement",
    "ConductorSolution",
    "SubmissionGuardRequest",
    "SubmissionPhaseStatus",
    "SubmissionBridgeResponse",
    "SubmissionGuardFailure",
    "SubmissionGuardDecision",
    "ControllerObservationRequest",
    "ControllerObservationFailure",
    "ControllerObservation",
    "Action",
    "AgentCapability",
    "AgentRegistration",
    "AgentResult",
    "AgentTask",
    "AppliedPlaybook",
    "Binding",
    "ConfirmedRootCause",
    "ControllerConfig",
    "DetectorEvaluation",
    "DetectorTimelineEntry",
    "Evidence",
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
    "IncidentClosure",
    "IncidentRequest",
    "NamespaceOnboarding",
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
    "ParamSpec",
    "Plan",
    "PlanApplication",
    "ProgramRun",
    "PlaybookCoverage",
    "PlaybookMatch",
    "ProgramSpec",
    "MemoryArtifact",
    "MemoryCheckReport",
    "MemoryDiagnostic",
    "MemoryConflict",
    "AcceptedMemoryChange",
    "MitigationReplayNote",
    "MemoryListing",
    "MemoryOutcomes",
    "OutcomeRecord",
    "OutcomeTimestamps",
    "PriorOutcomeEvidence",
    "ProposalWorkspace",
    "ReflectionEligibility",
    "ReflectionCauseWork",
    "ReflectionWorkItem",
    "ReflectionRequest",
    "ReflectionResult",
    "DriftEvent",
    "DriftEvidence",
    "SdkGap",
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
    "CreateFromSource",
    "DeleteObject",
    "PlanObjectEffect",
    "ReconcileFromSource",
    "RestartRollout",
    "Rollback",
    "RootCauseEvidence",
    "StateChange",
    "StateChanges",
    "StateFieldChange",
    "SetField",
    "PlanValue",
    "ValueField",
    "ValueProvenance",
    "ActionReport",
    "DeletePod",
    "ScaleWorkload",
    "Exec",
    "SurfacedPlaybook",
    "TimingMetrics",
    "TrafficLink",
    "TrafficSLO",
    "TrafficWorkload",
    "TrafficWorkloadScenario",
    "UsageMetrics",
    "Verdict",
    "VerificationEvidence",
    "parse_json",
    "to_canonical_json",
]


def to_canonical_json(message: Message) -> str:
    """Serialize a proto message to the canonical, byte-stable protojson form."""
    wire = json_format.MessageToJson(message, preserving_proto_field_name=True)
    return json.dumps(json.loads(wire), indent=2, sort_keys=True) + "\n"


def parse_json(text: str, message: _MessageT) -> _MessageT:
    """Parse protojson ``text`` into ``message`` (populated in place and returned)."""
    json_format.Parse(text, message)
    return message


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
NATIVE_RECEIPT_SCHEMA_VERSION: str = _string_rule(NativeProductionReceipt, "schema_version").const
SUBMISSION_PHASES: tuple[str, ...] = tuple(getattr(_string_rule(SubmissionGuardRequest, "phase"), "in"))
CONDUCTOR_TERMINAL_STAGES: tuple[str, ...] = tuple(
    name.removeprefix("CONDUCTOR_TERMINAL_STAGE_").lower()
    for name, value in benchmark_submission_pb2.ConductorTerminalStage.items()
    if value
)
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
