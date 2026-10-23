import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import common_pb2 as _common_pb2
from sdodev.contracts.v1alpha1 import program_pb2 as _program_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MitigationMenu(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "dispatch_epoch", "menu_id", "source_commit", "scope_digest", "policy_digest", "bindings", "options", "round_limit", "original_deadline", "remaining_batches")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    MENU_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    POLICY_DIGEST_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ROUND_LIMIT_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_BATCHES_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    dispatch_epoch: int
    menu_id: str
    source_commit: str
    scope_digest: str
    policy_digest: str
    bindings: _containers.RepeatedCompositeFieldContainer[_program_pb2.Binding]
    options: _containers.RepeatedCompositeFieldContainer[MitigationOption]
    round_limit: int
    original_deadline: _timestamp_pb2.Timestamp
    remaining_batches: int
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., dispatch_epoch: _Optional[int] = ..., menu_id: _Optional[str] = ..., source_commit: _Optional[str] = ..., scope_digest: _Optional[str] = ..., policy_digest: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[_program_pb2.Binding, _Mapping]]] = ..., options: _Optional[_Iterable[_Union[MitigationOption, _Mapping]]] = ..., round_limit: _Optional[int] = ..., original_deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., remaining_batches: _Optional[int] = ...) -> None: ...

class MitigationOption(_message.Message):
    __slots__ = ("option_id", "program_id", "program_revision", "dependency_revision", "effects", "disabled_reason", "reversible", "max_actions_per_round")
    class Effect(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        EFFECT_UNSPECIFIED: _ClassVar[MitigationOption.Effect]
        EFFECT_SCOPED_ROUTING_SET_FIELD: _ClassVar[MitigationOption.Effect]
        EFFECT_DEPLOYMENT_RESTART: _ClassVar[MitigationOption.Effect]
    EFFECT_UNSPECIFIED: MitigationOption.Effect
    EFFECT_SCOPED_ROUTING_SET_FIELD: MitigationOption.Effect
    EFFECT_DEPLOYMENT_RESTART: MitigationOption.Effect
    OPTION_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCY_REVISION_FIELD_NUMBER: _ClassVar[int]
    EFFECTS_FIELD_NUMBER: _ClassVar[int]
    DISABLED_REASON_FIELD_NUMBER: _ClassVar[int]
    REVERSIBLE_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIONS_PER_ROUND_FIELD_NUMBER: _ClassVar[int]
    option_id: str
    program_id: str
    program_revision: str
    dependency_revision: str
    effects: _containers.RepeatedScalarFieldContainer[MitigationOption.Effect]
    disabled_reason: str
    reversible: bool
    max_actions_per_round: int
    def __init__(self, option_id: _Optional[str] = ..., program_id: _Optional[str] = ..., program_revision: _Optional[str] = ..., dependency_revision: _Optional[str] = ..., effects: _Optional[_Iterable[_Union[MitigationOption.Effect, str]]] = ..., disabled_reason: _Optional[str] = ..., reversible: _Optional[bool] = ..., max_actions_per_round: _Optional[int] = ...) -> None: ...

class MitigationSelection(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "dispatch_epoch", "request_id", "menu_id", "option_id", "program_revision", "dependency_revision", "source_commit", "scope_digest", "policy_digest", "bindings", "round_limit")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MENU_ID_FIELD_NUMBER: _ClassVar[int]
    OPTION_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCY_REVISION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    POLICY_DIGEST_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    ROUND_LIMIT_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    dispatch_epoch: int
    request_id: str
    menu_id: str
    option_id: str
    program_revision: str
    dependency_revision: str
    source_commit: str
    scope_digest: str
    policy_digest: str
    bindings: _containers.RepeatedCompositeFieldContainer[_program_pb2.Binding]
    round_limit: int
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., dispatch_epoch: _Optional[int] = ..., request_id: _Optional[str] = ..., menu_id: _Optional[str] = ..., option_id: _Optional[str] = ..., program_revision: _Optional[str] = ..., dependency_revision: _Optional[str] = ..., source_commit: _Optional[str] = ..., scope_digest: _Optional[str] = ..., policy_digest: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[_program_pb2.Binding, _Mapping]]] = ..., round_limit: _Optional[int] = ...) -> None: ...

class MitigationReportQuery(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "dispatch_epoch", "attempt_id")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    dispatch_epoch: int
    attempt_id: str
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., dispatch_epoch: _Optional[int] = ..., attempt_id: _Optional[str] = ...) -> None: ...

class MitigationReport(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "request_id", "attempt_id", "acceptance_epoch", "status", "failed_phase", "rounds", "barriers", "remaining_batches", "residuals", "reason_code", "writes_frozen")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[MitigationReport.Status]
        STATUS_SUCCESS: _ClassVar[MitigationReport.Status]
        STATUS_REFUSED: _ClassVar[MitigationReport.Status]
        STATUS_EXECUTION_ERROR: _ClassVar[MitigationReport.Status]
        STATUS_UNRESOLVED: _ClassVar[MitigationReport.Status]
        STATUS_DIFFERENT_CAUSE: _ClassVar[MitigationReport.Status]
        STATUS_SUPERSEDED: _ClassVar[MitigationReport.Status]
        STATUS_INTERRUPTED: _ClassVar[MitigationReport.Status]
        STATUS_TIMEOUT: _ClassVar[MitigationReport.Status]
    STATUS_UNSPECIFIED: MitigationReport.Status
    STATUS_SUCCESS: MitigationReport.Status
    STATUS_REFUSED: MitigationReport.Status
    STATUS_EXECUTION_ERROR: MitigationReport.Status
    STATUS_UNRESOLVED: MitigationReport.Status
    STATUS_DIFFERENT_CAUSE: MitigationReport.Status
    STATUS_SUPERSEDED: MitigationReport.Status
    STATUS_INTERRUPTED: MitigationReport.Status
    STATUS_TIMEOUT: MitigationReport.Status
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FAILED_PHASE_FIELD_NUMBER: _ClassVar[int]
    ROUNDS_FIELD_NUMBER: _ClassVar[int]
    BARRIERS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_BATCHES_FIELD_NUMBER: _ClassVar[int]
    RESIDUALS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    WRITES_FROZEN_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    request_id: str
    attempt_id: str
    acceptance_epoch: int
    status: MitigationReport.Status
    failed_phase: str
    rounds: _containers.RepeatedCompositeFieldContainer[MitigationRoundReport]
    barriers: _containers.RepeatedCompositeFieldContainer[MitigationBarrier]
    remaining_batches: int
    residuals: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    reason_code: str
    writes_frozen: bool
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., request_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., acceptance_epoch: _Optional[int] = ..., status: _Optional[_Union[MitigationReport.Status, str]] = ..., failed_phase: _Optional[str] = ..., rounds: _Optional[_Iterable[_Union[MitigationRoundReport, _Mapping]]] = ..., barriers: _Optional[_Iterable[_Union[MitigationBarrier, _Mapping]]] = ..., remaining_batches: _Optional[int] = ..., residuals: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., reason_code: _Optional[str] = ..., writes_frozen: _Optional[bool] = ...) -> None: ...

class MitigationRoundReport(_message.Message):
    __slots__ = ("round_index", "round_id", "actions", "diagnosis", "verification", "barrier_ids", "independently_healthy", "original_findings_clear")
    ROUND_INDEX_FIELD_NUMBER: _ClassVar[int]
    ROUND_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSIS_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_FIELD_NUMBER: _ClassVar[int]
    BARRIER_IDS_FIELD_NUMBER: _ClassVar[int]
    INDEPENDENTLY_HEALTHY_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_FINDINGS_CLEAR_FIELD_NUMBER: _ClassVar[int]
    round_index: int
    round_id: str
    actions: _containers.RepeatedCompositeFieldContainer[MitigationActionReceipt]
    diagnosis: _program_pb2.Verdict
    verification: _program_pb2.Verdict
    barrier_ids: _containers.RepeatedScalarFieldContainer[str]
    independently_healthy: bool
    original_findings_clear: bool
    def __init__(self, round_index: _Optional[int] = ..., round_id: _Optional[str] = ..., actions: _Optional[_Iterable[_Union[MitigationActionReceipt, _Mapping]]] = ..., diagnosis: _Optional[_Union[_program_pb2.Verdict, _Mapping]] = ..., verification: _Optional[_Union[_program_pb2.Verdict, _Mapping]] = ..., barrier_ids: _Optional[_Iterable[str]] = ..., independently_healthy: _Optional[bool] = ..., original_findings_clear: _Optional[bool] = ...) -> None: ...

class MitigationActionReceipt(_message.Message):
    __slots__ = ("action_id", "resource", "effect", "state", "uid", "resource_version", "compensation_state", "rollback_conflict")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATE_UNSPECIFIED: _ClassVar[MitigationActionReceipt.State]
        STATE_NOT_APPLIED: _ClassVar[MitigationActionReceipt.State]
        STATE_APPLIED: _ClassVar[MitigationActionReceipt.State]
        STATE_UNKNOWN: _ClassVar[MitigationActionReceipt.State]
    STATE_UNSPECIFIED: MitigationActionReceipt.State
    STATE_NOT_APPLIED: MitigationActionReceipt.State
    STATE_APPLIED: MitigationActionReceipt.State
    STATE_UNKNOWN: MitigationActionReceipt.State
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    EFFECT_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    COMPENSATION_STATE_FIELD_NUMBER: _ClassVar[int]
    ROLLBACK_CONFLICT_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    resource: _common_pb2.ObjectRef
    effect: MitigationOption.Effect
    state: MitigationActionReceipt.State
    uid: str
    resource_version: str
    compensation_state: MitigationActionReceipt.State
    rollback_conflict: bool
    def __init__(self, action_id: _Optional[str] = ..., resource: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., effect: _Optional[_Union[MitigationOption.Effect, str]] = ..., state: _Optional[_Union[MitigationActionReceipt.State, str]] = ..., uid: _Optional[str] = ..., resource_version: _Optional[str] = ..., compensation_state: _Optional[_Union[MitigationActionReceipt.State, str]] = ..., rollback_conflict: _Optional[bool] = ...) -> None: ...

class MitigationBarrier(_message.Message):
    __slots__ = ("capture_id", "attempt_id", "round_index", "phase", "occurrence", "observations", "observed_at", "complete", "collections")
    CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    ROUND_INDEX_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    OCCURRENCE_FIELD_NUMBER: _ClassVar[int]
    OBSERVATIONS_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    capture_id: str
    attempt_id: str
    round_index: int
    phase: str
    occurrence: int
    observations: _containers.RepeatedCompositeFieldContainer[MitigationObservation]
    observed_at: _timestamp_pb2.Timestamp
    complete: bool
    collections: _containers.RepeatedCompositeFieldContainer[MitigationCollectionObservation]
    def __init__(self, capture_id: _Optional[str] = ..., attempt_id: _Optional[str] = ..., round_index: _Optional[int] = ..., phase: _Optional[str] = ..., occurrence: _Optional[int] = ..., observations: _Optional[_Iterable[_Union[MitigationObservation, _Mapping]]] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., complete: _Optional[bool] = ..., collections: _Optional[_Iterable[_Union[MitigationCollectionObservation, _Mapping]]] = ...) -> None: ...

class MitigationObservation(_message.Message):
    __slots__ = ("resource", "uid", "resource_version", "confirmed_absent", "effect_confirmed")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_ABSENT_FIELD_NUMBER: _ClassVar[int]
    EFFECT_CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    resource: _common_pb2.ObjectRef
    uid: str
    resource_version: str
    confirmed_absent: bool
    effect_confirmed: bool
    def __init__(self, resource: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., uid: _Optional[str] = ..., resource_version: _Optional[str] = ..., confirmed_absent: _Optional[bool] = ..., effect_confirmed: _Optional[bool] = ...) -> None: ...

class SelectionLedger(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "original_deadline", "dispatch_epoch", "source_history", "total_batches", "used_batches", "reserved_batches", "provider", "provider_session_id", "checkpoint_digest", "attempts", "writes_frozen", "owner_quiescent", "closed")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_HISTORY_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BATCHES_FIELD_NUMBER: _ClassVar[int]
    USED_BATCHES_FIELD_NUMBER: _ClassVar[int]
    RESERVED_BATCHES_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    WRITES_FROZEN_FIELD_NUMBER: _ClassVar[int]
    OWNER_QUIESCENT_FIELD_NUMBER: _ClassVar[int]
    CLOSED_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    original_deadline: _timestamp_pb2.Timestamp
    dispatch_epoch: int
    source_history: _containers.RepeatedScalarFieldContainer[str]
    total_batches: int
    used_batches: int
    reserved_batches: int
    provider: str
    provider_session_id: str
    checkpoint_digest: str
    attempts: _containers.RepeatedCompositeFieldContainer[SelectionAttempt]
    writes_frozen: bool
    owner_quiescent: bool
    closed: bool
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., original_deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., dispatch_epoch: _Optional[int] = ..., source_history: _Optional[_Iterable[str]] = ..., total_batches: _Optional[int] = ..., used_batches: _Optional[int] = ..., reserved_batches: _Optional[int] = ..., provider: _Optional[str] = ..., provider_session_id: _Optional[str] = ..., checkpoint_digest: _Optional[str] = ..., attempts: _Optional[_Iterable[_Union[SelectionAttempt, _Mapping]]] = ..., writes_frozen: _Optional[bool] = ..., owner_quiescent: _Optional[bool] = ..., closed: _Optional[bool] = ...) -> None: ...

class SelectionAttempt(_message.Message):
    __slots__ = ("request_id", "payload_hash", "attempt_id", "acceptance_epoch", "reserved_batches", "charged_batches", "actions", "rounds", "barriers", "report", "delivery_acknowledged", "closure_started", "closure_acknowledged")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_HASH_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    RESERVED_BATCHES_FIELD_NUMBER: _ClassVar[int]
    CHARGED_BATCHES_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    ROUNDS_FIELD_NUMBER: _ClassVar[int]
    BARRIERS_FIELD_NUMBER: _ClassVar[int]
    REPORT_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_STARTED_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    payload_hash: str
    attempt_id: str
    acceptance_epoch: int
    reserved_batches: int
    charged_batches: int
    actions: _containers.RepeatedCompositeFieldContainer[SelectionAction]
    rounds: _containers.RepeatedCompositeFieldContainer[MitigationRoundReport]
    barriers: _containers.RepeatedCompositeFieldContainer[MitigationBarrier]
    report: MitigationReport
    delivery_acknowledged: bool
    closure_started: bool
    closure_acknowledged: bool
    def __init__(self, request_id: _Optional[str] = ..., payload_hash: _Optional[str] = ..., attempt_id: _Optional[str] = ..., acceptance_epoch: _Optional[int] = ..., reserved_batches: _Optional[int] = ..., charged_batches: _Optional[int] = ..., actions: _Optional[_Iterable[_Union[SelectionAction, _Mapping]]] = ..., rounds: _Optional[_Iterable[_Union[MitigationRoundReport, _Mapping]]] = ..., barriers: _Optional[_Iterable[_Union[MitigationBarrier, _Mapping]]] = ..., report: _Optional[_Union[MitigationReport, _Mapping]] = ..., delivery_acknowledged: _Optional[bool] = ..., closure_started: _Optional[bool] = ..., closure_acknowledged: _Optional[bool] = ...) -> None: ...

class SelectionAction(_message.Message):
    __slots__ = ("round_index", "phase", "receipt", "complete")
    ROUND_INDEX_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    round_index: int
    phase: str
    receipt: MitigationActionReceipt
    complete: bool
    def __init__(self, round_index: _Optional[int] = ..., phase: _Optional[str] = ..., receipt: _Optional[_Union[MitigationActionReceipt, _Mapping]] = ..., complete: _Optional[bool] = ...) -> None: ...

class MitigationSessionCheckpoint(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "dispatch_epoch", "provider", "provider_session_id", "checkpoint_digest", "acknowledged_attempt_ids")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_ATTEMPT_IDS_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    dispatch_epoch: int
    provider: str
    provider_session_id: str
    checkpoint_digest: str
    acknowledged_attempt_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., dispatch_epoch: _Optional[int] = ..., provider: _Optional[str] = ..., provider_session_id: _Optional[str] = ..., checkpoint_digest: _Optional[str] = ..., acknowledged_attempt_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class MitigationSession(_message.Message):
    __slots__ = ("version", "incident_id", "session_id", "dispatch_epoch", "original_deadline", "provider", "provider_session_id", "checkpoint_digest", "pending_reports", "writes_frozen")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CHECKPOINT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    PENDING_REPORTS_FIELD_NUMBER: _ClassVar[int]
    WRITES_FROZEN_FIELD_NUMBER: _ClassVar[int]
    version: int
    incident_id: str
    session_id: str
    dispatch_epoch: int
    original_deadline: _timestamp_pb2.Timestamp
    provider: str
    provider_session_id: str
    checkpoint_digest: str
    pending_reports: _containers.RepeatedCompositeFieldContainer[MitigationReport]
    writes_frozen: bool
    def __init__(self, version: _Optional[int] = ..., incident_id: _Optional[str] = ..., session_id: _Optional[str] = ..., dispatch_epoch: _Optional[int] = ..., original_deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., provider: _Optional[str] = ..., provider_session_id: _Optional[str] = ..., checkpoint_digest: _Optional[str] = ..., pending_reports: _Optional[_Iterable[_Union[MitigationReport, _Mapping]]] = ..., writes_frozen: _Optional[bool] = ...) -> None: ...

class MitigationCollectionObservation(_message.Message):
    __slots__ = ("api_version", "kind", "namespace", "resource_version", "item_count")
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ITEM_COUNT_FIELD_NUMBER: _ClassVar[int]
    api_version: str
    kind: str
    namespace: str
    resource_version: str
    item_count: int
    def __init__(self, api_version: _Optional[str] = ..., kind: _Optional[str] = ..., namespace: _Optional[str] = ..., resource_version: _Optional[str] = ..., item_count: _Optional[int] = ...) -> None: ...
