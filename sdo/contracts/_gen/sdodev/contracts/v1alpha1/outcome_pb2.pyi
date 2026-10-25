import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from sdodev.contracts.v1alpha1 import program_pb2 as _program_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OutcomeRecord(_message.Message):
    __slots__ = ("schema_version", "incident_id", "source_commit", "deployed_commit", "detector_history", "findings", "surfaced_playbooks", "inspected_playbooks", "confirmed_playbooks", "rejected_playbooks", "applied_playbooks", "confirmed_root_causes", "final_health_detector_state", "classification", "repair_commit", "repair_actions", "memory_commit", "responder_backend", "responder_model", "usage", "timestamps", "program_tool_calls", "program_runs", "plan_applications", "program_failure_signals", "response_loops_used", "response_loop_exhausted", "drift_events", "resolution_path", "fast_path_programs", "fast_path_skipped_reason", "fast_path_relation_covered_findings", "reflection_eligibility", "reflection_skip_reason")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    DEPLOYED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_HISTORY_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    SURFACED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    INSPECTED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    REJECTED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    APPLIED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_ROOT_CAUSES_FIELD_NUMBER: _ClassVar[int]
    FINAL_HEALTH_DETECTOR_STATE_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    REPAIR_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REPAIR_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    MEMORY_COMMIT_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_MODEL_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_RUNS_FIELD_NUMBER: _ClassVar[int]
    PLAN_APPLICATIONS_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_FAILURE_SIGNALS_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_LOOPS_USED_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_LOOP_EXHAUSTED_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_PATH_FIELD_NUMBER: _ClassVar[int]
    FAST_PATH_PROGRAMS_FIELD_NUMBER: _ClassVar[int]
    FAST_PATH_SKIPPED_REASON_FIELD_NUMBER: _ClassVar[int]
    FAST_PATH_RELATION_COVERED_FINDINGS_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_ELIGIBILITY_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_SKIP_REASON_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    incident_id: str
    source_commit: str
    deployed_commit: str
    detector_history: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DetectorEvaluation]
    findings: _containers.RepeatedCompositeFieldContainer[_messages_pb2.Finding]
    surfaced_playbooks: _containers.RepeatedScalarFieldContainer[str]
    inspected_playbooks: _containers.RepeatedScalarFieldContainer[str]
    confirmed_playbooks: _containers.RepeatedScalarFieldContainer[str]
    rejected_playbooks: _containers.RepeatedScalarFieldContainer[str]
    applied_playbooks: _containers.RepeatedScalarFieldContainer[str]
    confirmed_root_causes: _containers.RepeatedCompositeFieldContainer[_messages_pb2.ConfirmedRootCause]
    final_health_detector_state: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DetectorEvaluation]
    classification: str
    repair_commit: str
    repair_actions: _containers.RepeatedCompositeFieldContainer[_messages_pb2.RepairActionReceipt]
    memory_commit: str
    responder_backend: str
    responder_model: str
    usage: _messages_pb2.UsageMetrics
    timestamps: OutcomeTimestamps
    program_tool_calls: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramToolCall]
    program_runs: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramRun]
    plan_applications: _containers.RepeatedCompositeFieldContainer[_program_pb2.PlanApplication]
    program_failure_signals: _containers.RepeatedScalarFieldContainer[str]
    response_loops_used: int
    response_loop_exhausted: bool
    drift_events: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DriftEvent]
    resolution_path: str
    fast_path_programs: _containers.RepeatedScalarFieldContainer[str]
    fast_path_skipped_reason: str
    fast_path_relation_covered_findings: _containers.RepeatedCompositeFieldContainer[_program_pb2.RelationCoveredFinding]
    reflection_eligibility: _program_pb2.ReflectionEligibility
    reflection_skip_reason: str
    def __init__(self, schema_version: _Optional[int] = ..., incident_id: _Optional[str] = ..., source_commit: _Optional[str] = ..., deployed_commit: _Optional[str] = ..., detector_history: _Optional[_Iterable[_Union[_messages_pb2.DetectorEvaluation, _Mapping]]] = ..., findings: _Optional[_Iterable[_Union[_messages_pb2.Finding, _Mapping]]] = ..., surfaced_playbooks: _Optional[_Iterable[str]] = ..., inspected_playbooks: _Optional[_Iterable[str]] = ..., confirmed_playbooks: _Optional[_Iterable[str]] = ..., rejected_playbooks: _Optional[_Iterable[str]] = ..., applied_playbooks: _Optional[_Iterable[str]] = ..., confirmed_root_causes: _Optional[_Iterable[_Union[_messages_pb2.ConfirmedRootCause, _Mapping]]] = ..., final_health_detector_state: _Optional[_Iterable[_Union[_messages_pb2.DetectorEvaluation, _Mapping]]] = ..., classification: _Optional[str] = ..., repair_commit: _Optional[str] = ..., repair_actions: _Optional[_Iterable[_Union[_messages_pb2.RepairActionReceipt, _Mapping]]] = ..., memory_commit: _Optional[str] = ..., responder_backend: _Optional[str] = ..., responder_model: _Optional[str] = ..., usage: _Optional[_Union[_messages_pb2.UsageMetrics, _Mapping]] = ..., timestamps: _Optional[_Union[OutcomeTimestamps, _Mapping]] = ..., program_tool_calls: _Optional[_Iterable[_Union[_program_pb2.ProgramToolCall, _Mapping]]] = ..., program_runs: _Optional[_Iterable[_Union[_program_pb2.ProgramRun, _Mapping]]] = ..., plan_applications: _Optional[_Iterable[_Union[_program_pb2.PlanApplication, _Mapping]]] = ..., program_failure_signals: _Optional[_Iterable[str]] = ..., response_loops_used: _Optional[int] = ..., response_loop_exhausted: _Optional[bool] = ..., drift_events: _Optional[_Iterable[_Union[_messages_pb2.DriftEvent, _Mapping]]] = ..., resolution_path: _Optional[str] = ..., fast_path_programs: _Optional[_Iterable[str]] = ..., fast_path_skipped_reason: _Optional[str] = ..., fast_path_relation_covered_findings: _Optional[_Iterable[_Union[_program_pb2.RelationCoveredFinding, _Mapping]]] = ..., reflection_eligibility: _Optional[_Union[_program_pb2.ReflectionEligibility, _Mapping]] = ..., reflection_skip_reason: _Optional[str] = ...) -> None: ...

class OutcomeTimestamps(_message.Message):
    __slots__ = ("detected_at", "dispatched_at", "mitigated_at", "verified_at", "completed_at")
    DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    DISPATCHED_AT_FIELD_NUMBER: _ClassVar[int]
    MITIGATED_AT_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    detected_at: _timestamp_pb2.Timestamp
    dispatched_at: _timestamp_pb2.Timestamp
    mitigated_at: _timestamp_pb2.Timestamp
    verified_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., dispatched_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., mitigated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
