import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
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
    __slots__ = ("program_id", "bindings", "verdict", "duration", "error")
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    VERDICT_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    program_id: str
    bindings: _containers.RepeatedCompositeFieldContainer[Binding]
    verdict: Verdict
    duration: _duration_pb2.Duration
    error: str
    def __init__(self, program_id: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[Binding, _Mapping]]] = ..., verdict: _Optional[_Union[Verdict, _Mapping]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class PlanApplication(_message.Message):
    __slots__ = ("plan", "applied", "dry_run", "verification")
    PLAN_FIELD_NUMBER: _ClassVar[int]
    APPLIED_FIELD_NUMBER: _ClassVar[int]
    DRY_RUN_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_FIELD_NUMBER: _ClassVar[int]
    plan: Plan
    applied: bool
    dry_run: bool
    verification: Verdict
    def __init__(self, plan: _Optional[_Union[Plan, _Mapping]] = ..., applied: _Optional[bool] = ..., dry_run: _Optional[bool] = ..., verification: _Optional[_Union[Verdict, _Mapping]] = ...) -> None: ...
