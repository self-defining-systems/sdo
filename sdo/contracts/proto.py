"""Generated proto contract messages and the canonical protojson convention.

This is the Python edge of the proto single-source-of-truth track. The ``.proto`` files under
``proto/`` are the one source; ``buf generate`` emits the messages into ``sdo/contracts/_gen``
and this facade re-exports them under a stable import path so callers never
reach into the generated tree directly.

The generated modules import each other by their proto package path
(``sdodev.contracts.v1alpha1.*``) and reference the vendored ``buf.validate``
options, so the generation root ``sdo/contracts/_gen`` must be importable. We
put it on ``sys.path`` here. The proto package is ``sdodev`` rather than ``sdo``
precisely so that root never shadows this real ``sdo`` package.

Contracts are serialized as **protojson, never binary**, and share one wire
convention with the Go controller (``controller/runtime`` ``protoContractJSON``):
proto (snake_case) field names, zero-valued fields omitted. ``to_canonical_json``
additionally sorts keys and indents so a payload is byte-comparable across the
Go and Python encoders — the identity the round-trip contract tests assert.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

_GEN_ROOT = Path(__file__).resolve().parent / "_gen"
if str(_GEN_ROOT) not in sys.path:
    sys.path.insert(0, str(_GEN_ROOT))

from google.protobuf import json_format

from sdodev.contracts.v1alpha1 import (
    common_pb2,
    controller_config_pb2,
    lifecycle_pb2,
    messages_pb2,
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
ProtoLateFindingsPull = messages_pb2.LateFindingsPull
IncidentView = messages_pb2.IncidentView
DetectorTimelineEntry = messages_pb2.DetectorTimelineEntry
IncidentClosure = messages_pb2.IncidentClosure
OperationalArtifactProposal = messages_pb2.OperationalArtifactProposal
ProposedDetector = messages_pb2.ProposedDetector
ProposedGoSource = messages_pb2.ProposedGoSource
ProposedPlaybook = messages_pb2.ProposedPlaybook
ProposedTrafficGenerator = messages_pb2.ProposedTrafficGenerator
ProtoMemoryWatch = messages_pb2.MemoryWatch
ProtoMemoryScript = messages_pb2.MemoryScript

FiringRecord = telemetry_pb2.FiringRecord

TrafficSLO = traffic_pb2.TrafficSLO
TrafficWorkloadScenario = traffic_pb2.TrafficWorkloadScenario
TrafficLink = traffic_pb2.TrafficLink
TrafficWorkload = traffic_pb2.TrafficWorkload
ProtoTrafficWorkload = traffic_pb2.TrafficWorkload

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
    "FollowUpContext",
    "IncidentClosure",
    "IncidentRequest",
    "IncidentResult",
    "ProtoLateFindingsPull",
    "IncidentView",
    "KubernetesJobLaunch",
    "LocalProcessLaunch",
    "ObjectRef",
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
