import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProgramSpec(_message.Message):
    __slots__ = ("id", "params")
    ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    id: str
    params: _containers.RepeatedCompositeFieldContainer[ParamSpec]
    def __init__(self, id: _Optional[str] = ..., params: _Optional[_Iterable[_Union[ParamSpec, _Mapping]]] = ...) -> None: ...

class ParamSpec(_message.Message):
    __slots__ = ("name", "kind", "required")
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    name: str
    kind: str
    required: bool
    def __init__(self, name: _Optional[str] = ..., kind: _Optional[str] = ..., required: _Optional[bool] = ...) -> None: ...

class Binding(_message.Message):
    __slots__ = ("name", "object")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    name: str
    object: _common_pb2.ObjectRef
    def __init__(self, name: _Optional[str] = ..., object: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...

class RelationCoveredFinding(_message.Message):
    __slots__ = ("finding_fingerprint", "program_id", "bindings")
    FINDING_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    finding_fingerprint: str
    program_id: str
    bindings: _containers.RepeatedCompositeFieldContainer[Binding]
    def __init__(self, finding_fingerprint: _Optional[str] = ..., program_id: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[Binding, _Mapping]]] = ...) -> None: ...

class Verdict(_message.Message):
    __slots__ = ("result", "evidence", "question")
    class Result(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RESULT_UNSPECIFIED: _ClassVar[Verdict.Result]
        RESULT_MATCH: _ClassVar[Verdict.Result]
        RESULT_NO_MATCH: _ClassVar[Verdict.Result]
        RESULT_UNKNOWN: _ClassVar[Verdict.Result]
        RESULT_NEEDS_JUDGEMENT: _ClassVar[Verdict.Result]
    RESULT_UNSPECIFIED: Verdict.Result
    RESULT_MATCH: Verdict.Result
    RESULT_NO_MATCH: Verdict.Result
    RESULT_UNKNOWN: Verdict.Result
    RESULT_NEEDS_JUDGEMENT: Verdict.Result
    RESULT_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    result: Verdict.Result
    evidence: _containers.RepeatedCompositeFieldContainer[Evidence]
    question: str
    def __init__(self, result: _Optional[_Union[Verdict.Result, str]] = ..., evidence: _Optional[_Iterable[_Union[Evidence, _Mapping]]] = ..., question: _Optional[str] = ...) -> None: ...

class Evidence(_message.Message):
    __slots__ = ("resource", "fact", "observed", "expected")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    FACT_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    resource: _common_pb2.ObjectRef
    fact: str
    observed: str
    expected: str
    def __init__(self, resource: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., fact: _Optional[str] = ..., observed: _Optional[str] = ..., expected: _Optional[str] = ...) -> None: ...

class Plan(_message.Message):
    __slots__ = ("actions", "rationale")
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    RATIONALE_FIELD_NUMBER: _ClassVar[int]
    actions: _containers.RepeatedCompositeFieldContainer[Action]
    rationale: str
    def __init__(self, actions: _Optional[_Iterable[_Union[Action, _Mapping]]] = ..., rationale: _Optional[str] = ...) -> None: ...

class Action(_message.Message):
    __slots__ = ("reconcile_from_source", "rollback", "set_field", "restart_rollout")
    RECONCILE_FROM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_FIELD_NUMBER: _ClassVar[int]
    RESTART_ROLLOUT_FIELD_NUMBER: _ClassVar[int]
    reconcile_from_source: ReconcileFromSource
    rollback: Rollback
    set_field: SetField
    restart_rollout: RestartRollout
    def __init__(self, reconcile_from_source: _Optional[_Union[ReconcileFromSource, _Mapping]] = ..., rollback: _Optional[_Union[Rollback, _Mapping]] = ..., set_field: _Optional[_Union[SetField, _Mapping]] = ..., restart_rollout: _Optional[_Union[RestartRollout, _Mapping]] = ...) -> None: ...

class ReconcileFromSource(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...

class Rollback(_message.Message):
    __slots__ = ("ref", "to_revision")
    REF_FIELD_NUMBER: _ClassVar[int]
    TO_REVISION_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    to_revision: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., to_revision: _Optional[str] = ...) -> None: ...

class SetField(_message.Message):
    __slots__ = ("ref", "path", "value", "reason")
    REF_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    path: str
    value: _struct_pb2.Value
    reason: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., path: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class RestartRollout(_message.Message):
    __slots__ = ("ref", "reason")
    REF_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    reason: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class ProgramRun(_message.Message):
    __slots__ = ("program_id", "bindings", "verdict", "duration", "error", "finding_fingerprint", "program_revision", "binding_source", "can_mitigate")
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    VERDICT_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    FINDING_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    BINDING_SOURCE_FIELD_NUMBER: _ClassVar[int]
    CAN_MITIGATE_FIELD_NUMBER: _ClassVar[int]
    program_id: str
    bindings: _containers.RepeatedCompositeFieldContainer[Binding]
    verdict: Verdict
    duration: _duration_pb2.Duration
    error: str
    finding_fingerprint: str
    program_revision: str
    binding_source: str
    can_mitigate: bool
    def __init__(self, program_id: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[Binding, _Mapping]]] = ..., verdict: _Optional[_Union[Verdict, _Mapping]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., error: _Optional[str] = ..., finding_fingerprint: _Optional[str] = ..., program_revision: _Optional[str] = ..., binding_source: _Optional[str] = ..., can_mitigate: _Optional[bool] = ...) -> None: ...

class PlanApplication(_message.Message):
    __slots__ = ("plan", "applied", "dry_run", "verification", "application_id", "program_ids", "error", "duration", "rolled_back", "rollback_error", "actions_applied", "started_at", "completed_at", "verification_completed", "verification_passed", "final_equivalence_verified")
    PLAN_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_IDS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ROLLED_BACK_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_ERROR_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_APPLIED_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_COMPLETED_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_PASSED_FIELD_NUMBER: _ClassVar[int]
    FINAL_EQUIVALENCE_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    plan: Plan
    applied: bool
    dry_run: bool
    verification: Verdict
    application_id: str
    program_ids: _containers.RepeatedScalarFieldContainer[str]
    error: str
    duration: _duration_pb2.Duration
    rolled_back: bool
    rollback_error: str
    actions_applied: _containers.RepeatedCompositeFieldContainer[Action]
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    verification_completed: bool
    verification_passed: bool
    final_equivalence_verified: bool
    def __init__(self, plan: _Optional[_Union[Plan, _Mapping]] = ..., applied: _Optional[bool] = ..., dry_run: _Optional[bool] = ..., verification: _Optional[_Union[Verdict, _Mapping]] = ..., application_id: _Optional[str] = ..., program_ids: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., rolled_back: _Optional[bool] = ..., rollback_error: _Optional[str] = ..., actions_applied: _Optional[_Iterable[_Union[Action, _Mapping]]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., verification_completed: _Optional[bool] = ..., verification_passed: _Optional[bool] = ..., final_equivalence_verified: _Optional[bool] = ...) -> None: ...

class ProgramToolCall(_message.Message):
    __slots__ = ("command", "at", "request_json", "response_json", "error", "summary")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    AT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_JSON_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_JSON_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    command: str
    at: _timestamp_pb2.Timestamp
    request_json: str
    response_json: str
    error: str
    summary: str
    def __init__(self, command: _Optional[str] = ..., at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., request_json: _Optional[str] = ..., response_json: _Optional[str] = ..., error: _Optional[str] = ..., summary: _Optional[str] = ...) -> None: ...

class PlanPreImage(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PlanPatch(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReflectionCauseWork(_message.Message):
    __slots__ = ("cause_index", "resources", "items")
    CAUSE_INDEX_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    cause_index: int
    resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    items: _containers.RepeatedCompositeFieldContainer[ReflectionWorkItem]
    def __init__(self, cause_index: _Optional[int] = ..., resources: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., items: _Optional[_Iterable[_Union[ReflectionWorkItem, _Mapping]]] = ...) -> None: ...

class ReflectionWorkItem(_message.Message):
    __slots__ = ("program_id", "bindings", "disposition", "question", "reason", "resources", "evidence_phase", "program_revision", "capture_open_phase", "evaluation")
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_PHASE_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_OPEN_PHASE_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_FIELD_NUMBER: _ClassVar[int]
    program_id: str
    bindings: _containers.RepeatedCompositeFieldContainer[Binding]
    disposition: str
    question: str
    reason: str
    resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    evidence_phase: str
    program_revision: str
    capture_open_phase: str
    evaluation: ProgramRun
    def __init__(self, program_id: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[Binding, _Mapping]]] = ..., disposition: _Optional[str] = ..., question: _Optional[str] = ..., reason: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., evidence_phase: _Optional[str] = ..., program_revision: _Optional[str] = ..., capture_open_phase: _Optional[str] = ..., evaluation: _Optional[_Union[ProgramRun, _Mapping]] = ...) -> None: ...

class ReflectionEligibility(_message.Message):
    __slots__ = ("version", "causes", "unassigned", "evidence_gap_count")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CAUSES_FIELD_NUMBER: _ClassVar[int]
    UNASSIGNED_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_GAP_COUNT_FIELD_NUMBER: _ClassVar[int]
    version: int
    causes: _containers.RepeatedCompositeFieldContainer[ReflectionCauseWork]
    unassigned: _containers.RepeatedCompositeFieldContainer[ReflectionWorkItem]
    evidence_gap_count: int
    def __init__(self, version: _Optional[int] = ..., causes: _Optional[_Iterable[_Union[ReflectionCauseWork, _Mapping]]] = ..., unassigned: _Optional[_Iterable[_Union[ReflectionWorkItem, _Mapping]]] = ..., evidence_gap_count: _Optional[int] = ...) -> None: ...
