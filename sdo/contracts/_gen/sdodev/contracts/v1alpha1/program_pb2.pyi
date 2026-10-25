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
    __slots__ = ("name", "kind", "required", "value_kind", "default")
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_FIELD_NUMBER: _ClassVar[int]
    VALUE_KIND_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_FIELD_NUMBER: _ClassVar[int]
    name: str
    kind: str
    required: bool
    value_kind: str
    default: _struct_pb2.Value
    def __init__(self, name: _Optional[str] = ..., kind: _Optional[str] = ..., required: _Optional[bool] = ..., value_kind: _Optional[str] = ..., default: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...

class Binding(_message.Message):
    __slots__ = ("name", "object", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    object: _common_pb2.ObjectRef
    value: _struct_pb2.Value
    def __init__(self, name: _Optional[str] = ..., object: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...

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
    __slots__ = ("reconcile_from_source", "rollback", "set_field", "restart_rollout", "create_from_source", "delete_object", "delete_pod", "scale_workload", "exec", "remove_service_port", "ensure_database_user")
    RECONCILE_FROM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_FIELD_NUMBER: _ClassVar[int]
    SET_FIELD_FIELD_NUMBER: _ClassVar[int]
    RESTART_ROLLOUT_FIELD_NUMBER: _ClassVar[int]
    CREATE_FROM_SOURCE_FIELD_NUMBER: _ClassVar[int]
    DELETE_OBJECT_FIELD_NUMBER: _ClassVar[int]
    DELETE_POD_FIELD_NUMBER: _ClassVar[int]
    SCALE_WORKLOAD_FIELD_NUMBER: _ClassVar[int]
    EXEC_FIELD_NUMBER: _ClassVar[int]
    REMOVE_SERVICE_PORT_FIELD_NUMBER: _ClassVar[int]
    ENSURE_DATABASE_USER_FIELD_NUMBER: _ClassVar[int]
    reconcile_from_source: ReconcileFromSource
    rollback: Rollback
    set_field: SetField
    restart_rollout: RestartRollout
    create_from_source: CreateFromSource
    delete_object: DeleteObject
    delete_pod: DeletePod
    scale_workload: ScaleWorkload
    exec: Exec
    remove_service_port: RemoveServicePort
    ensure_database_user: EnsureDatabaseUser
    def __init__(self, reconcile_from_source: _Optional[_Union[ReconcileFromSource, _Mapping]] = ..., rollback: _Optional[_Union[Rollback, _Mapping]] = ..., set_field: _Optional[_Union[SetField, _Mapping]] = ..., restart_rollout: _Optional[_Union[RestartRollout, _Mapping]] = ..., create_from_source: _Optional[_Union[CreateFromSource, _Mapping]] = ..., delete_object: _Optional[_Union[DeleteObject, _Mapping]] = ..., delete_pod: _Optional[_Union[DeletePod, _Mapping]] = ..., scale_workload: _Optional[_Union[ScaleWorkload, _Mapping]] = ..., exec: _Optional[_Union[Exec, _Mapping]] = ..., remove_service_port: _Optional[_Union[RemoveServicePort, _Mapping]] = ..., ensure_database_user: _Optional[_Union[EnsureDatabaseUser, _Mapping]] = ...) -> None: ...

class CreateFromSource(_message.Message):
    __slots__ = ("ref", "reason")
    REF_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    reason: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class DeleteObject(_message.Message):
    __slots__ = ("ref", "reason")
    REF_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    reason: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class PlanObjectEffect(_message.Message):
    __slots__ = ("operation", "ref", "uid", "state", "confirmed", "irreversible", "deleted_pre_image")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    IRREVERSIBLE_FIELD_NUMBER: _ClassVar[int]
    DELETED_PRE_IMAGE_FIELD_NUMBER: _ClassVar[int]
    operation: str
    ref: _common_pb2.ObjectRef
    uid: str
    state: str
    confirmed: bool
    irreversible: bool
    deleted_pre_image: _struct_pb2.Struct
    def __init__(self, operation: _Optional[str] = ..., ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., uid: _Optional[str] = ..., state: _Optional[str] = ..., confirmed: _Optional[bool] = ..., irreversible: _Optional[bool] = ..., deleted_pre_image: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

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
    __slots__ = ("ref", "path", "value", "reason", "document_format", "document_path")
    REF_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FORMAT_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_PATH_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    path: str
    value: PlanValue
    reason: str
    document_format: str
    document_path: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., path: _Optional[str] = ..., value: _Optional[_Union[PlanValue, _Mapping]] = ..., reason: _Optional[str] = ..., document_format: _Optional[str] = ..., document_path: _Optional[_Iterable[str]] = ...) -> None: ...

class PlanValue(_message.Message):
    __slots__ = ("source", "observed", "parameter")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    source: ValueField
    observed: ValueField
    parameter: str
    def __init__(self, source: _Optional[_Union[ValueField, _Mapping]] = ..., observed: _Optional[_Union[ValueField, _Mapping]] = ..., parameter: _Optional[str] = ...) -> None: ...

class ValueField(_message.Message):
    __slots__ = ("ref", "path")
    REF_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    path: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., path: _Optional[str] = ...) -> None: ...

class ValueProvenance(_message.Message):
    __slots__ = ("origin", "ref", "path", "source_commit", "resource_version", "parameter")
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_FIELD_NUMBER: _ClassVar[int]
    origin: str
    ref: _common_pb2.ObjectRef
    path: str
    source_commit: str
    resource_version: str
    parameter: str
    def __init__(self, origin: _Optional[str] = ..., ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., path: _Optional[str] = ..., source_commit: _Optional[str] = ..., resource_version: _Optional[str] = ..., parameter: _Optional[str] = ...) -> None: ...

class ActionReport(_message.Message):
    __slots__ = ("action_index", "status", "values", "exit_code")
    ACTION_INDEX_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    EXIT_CODE_FIELD_NUMBER: _ClassVar[int]
    action_index: int
    status: str
    values: _containers.RepeatedCompositeFieldContainer[ValueProvenance]
    exit_code: int
    def __init__(self, action_index: _Optional[int] = ..., status: _Optional[str] = ..., values: _Optional[_Iterable[_Union[ValueProvenance, _Mapping]]] = ..., exit_code: _Optional[int] = ...) -> None: ...

class DeletePod(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...

class ScaleWorkload(_message.Message):
    __slots__ = ("ref", "replicas")
    REF_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    replicas: PlanValue
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., replicas: _Optional[_Union[PlanValue, _Mapping]] = ...) -> None: ...

class Exec(_message.Message):
    __slots__ = ("pod", "container", "command")
    POD_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    pod: _common_pb2.ObjectRef
    container: str
    command: _containers.RepeatedCompositeFieldContainer[PlanValue]
    def __init__(self, pod: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., container: _Optional[str] = ..., command: _Optional[_Iterable[_Union[PlanValue, _Mapping]]] = ...) -> None: ...

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

class RolloutPending(_message.Message):
    __slots__ = ("reason", "blockers")
    REASON_FIELD_NUMBER: _ClassVar[int]
    BLOCKERS_FIELD_NUMBER: _ClassVar[int]
    reason: str
    blockers: _containers.RepeatedCompositeFieldContainer[RolloutBlocker]
    def __init__(self, reason: _Optional[str] = ..., blockers: _Optional[_Iterable[_Union[RolloutBlocker, _Mapping]]] = ...) -> None: ...

class RolloutBlocker(_message.Message):
    __slots__ = ("ref", "reason", "warning_events")
    REF_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    WARNING_EVENTS_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    reason: str
    warning_events: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., reason: _Optional[str] = ..., warning_events: _Optional[_Iterable[str]] = ...) -> None: ...

class PlanApplication(_message.Message):
    __slots__ = ("plan", "applied", "dry_run", "verification", "application_id", "program_ids", "error", "duration", "rolled_back", "rollback_error", "actions_applied", "started_at", "completed_at", "verification_completed", "verification_passed", "final_equivalence_verified", "object_effects", "action_reports", "rollout_pending")
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
    OBJECT_EFFECTS_FIELD_NUMBER: _ClassVar[int]
    ACTION_REPORTS_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_PENDING_FIELD_NUMBER: _ClassVar[int]
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
    object_effects: _containers.RepeatedCompositeFieldContainer[PlanObjectEffect]
    action_reports: _containers.RepeatedCompositeFieldContainer[ActionReport]
    rollout_pending: RolloutPending
    def __init__(self, plan: _Optional[_Union[Plan, _Mapping]] = ..., applied: _Optional[bool] = ..., dry_run: _Optional[bool] = ..., verification: _Optional[_Union[Verdict, _Mapping]] = ..., application_id: _Optional[str] = ..., program_ids: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., rolled_back: _Optional[bool] = ..., rollback_error: _Optional[str] = ..., actions_applied: _Optional[_Iterable[_Union[Action, _Mapping]]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., verification_completed: _Optional[bool] = ..., verification_passed: _Optional[bool] = ..., final_equivalence_verified: _Optional[bool] = ..., object_effects: _Optional[_Iterable[_Union[PlanObjectEffect, _Mapping]]] = ..., action_reports: _Optional[_Iterable[_Union[ActionReport, _Mapping]]] = ..., rollout_pending: _Optional[_Union[RolloutPending, _Mapping]] = ...) -> None: ...

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

class RemoveServicePort(_message.Message):
    __slots__ = ("ref", "name", "port", "protocol", "allow_empty", "reason")
    REF_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    ALLOW_EMPTY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    name: str
    port: int
    protocol: str
    allow_empty: bool
    reason: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., name: _Optional[str] = ..., port: _Optional[int] = ..., protocol: _Optional[str] = ..., allow_empty: _Optional[bool] = ..., reason: _Optional[str] = ...) -> None: ...

class CredentialRef(_message.Message):
    __slots__ = ("env",)
    ENV_FIELD_NUMBER: _ClassVar[int]
    env: str
    def __init__(self, env: _Optional[str] = ...) -> None: ...

class DatabaseAuth(_message.Message):
    __slots__ = ("database", "username", "password")
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    database: str
    username: CredentialRef
    password: CredentialRef
    def __init__(self, database: _Optional[str] = ..., username: _Optional[_Union[CredentialRef, _Mapping]] = ..., password: _Optional[_Union[CredentialRef, _Mapping]] = ...) -> None: ...

class DatabaseTarget(_message.Message):
    __slots__ = ("engine", "pod", "container", "database", "auth", "shell")
    ENGINE_FIELD_NUMBER: _ClassVar[int]
    POD_FIELD_NUMBER: _ClassVar[int]
    CONTAINER_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    AUTH_FIELD_NUMBER: _ClassVar[int]
    SHELL_FIELD_NUMBER: _ClassVar[int]
    engine: str
    pod: _common_pb2.ObjectRef
    container: str
    database: str
    auth: DatabaseAuth
    shell: str
    def __init__(self, engine: _Optional[str] = ..., pod: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., container: _Optional[str] = ..., database: _Optional[str] = ..., auth: _Optional[_Union[DatabaseAuth, _Mapping]] = ..., shell: _Optional[str] = ...) -> None: ...

class DatabaseRole(_message.Message):
    __slots__ = ("database", "name")
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    database: str
    name: str
    def __init__(self, database: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class EnsureDatabaseUser(_message.Message):
    __slots__ = ("target", "user", "roles", "password", "reason")
    TARGET_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    target: DatabaseTarget
    user: str
    roles: _containers.RepeatedCompositeFieldContainer[DatabaseRole]
    password: CredentialRef
    reason: str
    def __init__(self, target: _Optional[_Union[DatabaseTarget, _Mapping]] = ..., user: _Optional[str] = ..., roles: _Optional[_Iterable[_Union[DatabaseRole, _Mapping]]] = ..., password: _Optional[_Union[CredentialRef, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class PlaybookCoverage(_message.Message):
    __slots__ = ("matches", "unexplained_finding_fingerprints")
    MATCHES_FIELD_NUMBER: _ClassVar[int]
    UNEXPLAINED_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    matches: _containers.RepeatedCompositeFieldContainer[PlaybookMatch]
    unexplained_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, matches: _Optional[_Iterable[_Union[PlaybookMatch, _Mapping]]] = ..., unexplained_finding_fingerprints: _Optional[_Iterable[str]] = ...) -> None: ...

class PlaybookMatch(_message.Message):
    __slots__ = ("playbook_id", "program_revision", "bindings", "explained_finding_fingerprints", "can_mitigate", "mitigation_selection", "verified_successes", "verified_failures")
    PLAYBOOK_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    EXPLAINED_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    CAN_MITIGATE_FIELD_NUMBER: _ClassVar[int]
    MITIGATION_SELECTION_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_SUCCESSES_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_FAILURES_FIELD_NUMBER: _ClassVar[int]
    playbook_id: str
    program_revision: str
    bindings: _containers.RepeatedCompositeFieldContainer[Binding]
    explained_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    can_mitigate: bool
    mitigation_selection: str
    verified_successes: int
    verified_failures: int
    def __init__(self, playbook_id: _Optional[str] = ..., program_revision: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[Binding, _Mapping]]] = ..., explained_finding_fingerprints: _Optional[_Iterable[str]] = ..., can_mitigate: _Optional[bool] = ..., mitigation_selection: _Optional[str] = ..., verified_successes: _Optional[int] = ..., verified_failures: _Optional[int] = ...) -> None: ...
