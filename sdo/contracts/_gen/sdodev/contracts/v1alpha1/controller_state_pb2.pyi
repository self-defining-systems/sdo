import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import memory_diagnostic_pb2 as _memory_diagnostic_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from sdodev.contracts.v1alpha1 import outcome_pb2 as _outcome_pb2
from sdodev.contracts.v1alpha1 import worker_pb2 as _worker_pb2
from sdodev.contracts.v1alpha1 import mitigation_menu_pb2 as _mitigation_menu_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DispatchState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISPATCH_STATE_UNSPECIFIED: _ClassVar[DispatchState]
    DISPATCH_STATE_IDLE: _ClassVar[DispatchState]
    DISPATCH_STATE_WORKSPACE_PENDING: _ClassVar[DispatchState]
    DISPATCH_STATE_WORKSPACE_RUNNING: _ClassVar[DispatchState]
    DISPATCH_STATE_PENDING: _ClassVar[DispatchState]
    DISPATCH_STATE_RUNNING: _ClassVar[DispatchState]
    DISPATCH_STATE_COMPLETED: _ClassVar[DispatchState]

class ClosureState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLOSURE_STATE_UNSPECIFIED: _ClassVar[ClosureState]
    CLOSURE_STATE_PENDING: _ClassVar[ClosureState]
    CLOSURE_STATE_PROCESSING: _ClassVar[ClosureState]
    CLOSURE_STATE_COMMITTED: _ClassVar[ClosureState]
    CLOSURE_STATE_ACKNOWLEDGING: _ClassVar[ClosureState]
    CLOSURE_STATE_FAILED: _ClassVar[ClosureState]

class ClosureFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLOSURE_FAILURE_REASON_UNSPECIFIED: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_BROKER_ERROR: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_INVALID_RECEIPT: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_RETRY_EXHAUSTED: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_AGENT_TIMEOUT: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_OBSERVER_DENIED: _ClassVar[ClosureFailureReason]
    CLOSURE_FAILURE_REASON_OWNER_LOST: _ClassVar[ClosureFailureReason]

class ReflectionPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REFLECTION_PHASE_UNSPECIFIED: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_INSPECT: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_DISPATCH: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_REVIEW: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_PUBLISH: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_RESET: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_FAILED_CLEANUP: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_DONE: _ClassVar[ReflectionPhase]
    REFLECTION_PHASE_INSPECT_RESULT: _ClassVar[ReflectionPhase]
DISPATCH_STATE_UNSPECIFIED: DispatchState
DISPATCH_STATE_IDLE: DispatchState
DISPATCH_STATE_WORKSPACE_PENDING: DispatchState
DISPATCH_STATE_WORKSPACE_RUNNING: DispatchState
DISPATCH_STATE_PENDING: DispatchState
DISPATCH_STATE_RUNNING: DispatchState
DISPATCH_STATE_COMPLETED: DispatchState
CLOSURE_STATE_UNSPECIFIED: ClosureState
CLOSURE_STATE_PENDING: ClosureState
CLOSURE_STATE_PROCESSING: ClosureState
CLOSURE_STATE_COMMITTED: ClosureState
CLOSURE_STATE_ACKNOWLEDGING: ClosureState
CLOSURE_STATE_FAILED: ClosureState
CLOSURE_FAILURE_REASON_UNSPECIFIED: ClosureFailureReason
CLOSURE_FAILURE_REASON_BROKER_ERROR: ClosureFailureReason
CLOSURE_FAILURE_REASON_INVALID_RECEIPT: ClosureFailureReason
CLOSURE_FAILURE_REASON_RETRY_EXHAUSTED: ClosureFailureReason
CLOSURE_FAILURE_REASON_AGENT_TIMEOUT: ClosureFailureReason
CLOSURE_FAILURE_REASON_OBSERVER_DENIED: ClosureFailureReason
CLOSURE_FAILURE_REASON_OWNER_LOST: ClosureFailureReason
REFLECTION_PHASE_UNSPECIFIED: ReflectionPhase
REFLECTION_PHASE_INSPECT: ReflectionPhase
REFLECTION_PHASE_DISPATCH: ReflectionPhase
REFLECTION_PHASE_REVIEW: ReflectionPhase
REFLECTION_PHASE_PUBLISH: ReflectionPhase
REFLECTION_PHASE_RESET: ReflectionPhase
REFLECTION_PHASE_FAILED_CLEANUP: ReflectionPhase
REFLECTION_PHASE_DONE: ReflectionPhase
REFLECTION_PHASE_INSPECT_RESULT: ReflectionPhase

class FindingState(_message.Message):
    __slots__ = ("detector_id", "finding", "firing_count", "clear_count", "active", "first_seen_at", "activations", "clears", "not_persisted", "first_clear_at", "last_observed_at")
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    FINDING_FIELD_NUMBER: _ClassVar[int]
    FIRING_COUNT_FIELD_NUMBER: _ClassVar[int]
    CLEAR_COUNT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    FIRST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVATIONS_FIELD_NUMBER: _ClassVar[int]
    CLEARS_FIELD_NUMBER: _ClassVar[int]
    NOT_PERSISTED_FIELD_NUMBER: _ClassVar[int]
    FIRST_CLEAR_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    finding: _messages_pb2.Finding
    firing_count: int
    clear_count: int
    active: bool
    first_seen_at: _timestamp_pb2.Timestamp
    activations: int
    clears: int
    not_persisted: int
    first_clear_at: _timestamp_pb2.Timestamp
    last_observed_at: _timestamp_pb2.Timestamp
    def __init__(self, detector_id: _Optional[str] = ..., finding: _Optional[_Union[_messages_pb2.Finding, _Mapping]] = ..., firing_count: _Optional[int] = ..., clear_count: _Optional[int] = ..., active: _Optional[bool] = ..., first_seen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., activations: _Optional[int] = ..., clears: _Optional[int] = ..., not_persisted: _Optional[int] = ..., first_clear_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class BatcherState(_message.Message):
    __slots__ = ("findings", "deadlines", "deadline")
    class DeadlinesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _timestamp_pb2.Timestamp
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    DEADLINES_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    findings: _containers.RepeatedCompositeFieldContainer[_messages_pb2.Finding]
    deadlines: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    deadline: _timestamp_pb2.Timestamp
    def __init__(self, findings: _Optional[_Iterable[_Union[_messages_pb2.Finding, _Mapping]]] = ..., deadlines: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ClosureReceipt(_message.Message):
    __slots__ = ("incident_id", "worktree", "base_commit", "proposal_commit", "outcome_commit", "reflection_commit", "ack_token")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKTREE_FIELD_NUMBER: _ClassVar[int]
    BASE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_COMMIT_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ACK_TOKEN_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    worktree: str
    base_commit: str
    proposal_commit: str
    outcome_commit: str
    reflection_commit: str
    ack_token: str
    def __init__(self, incident_id: _Optional[str] = ..., worktree: _Optional[str] = ..., base_commit: _Optional[str] = ..., proposal_commit: _Optional[str] = ..., outcome_commit: _Optional[str] = ..., reflection_commit: _Optional[str] = ..., ack_token: _Optional[str] = ...) -> None: ...

class FailedIncidentClosure(_message.Message):
    __slots__ = ("closure", "failure")
    CLOSURE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    closure: _messages_pb2.IncidentClosure
    failure: ClosureFailure
    def __init__(self, closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., failure: _Optional[_Union[ClosureFailure, _Mapping]] = ...) -> None: ...

class ClosureFailure(_message.Message):
    __slots__ = ("incident_id", "attempts", "max_attempts", "last_error", "first_failed_at", "last_failed_at", "next_attempt_at", "permanent", "action", "reason")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    FIRST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    PERMANENT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    attempts: int
    max_attempts: int
    last_error: str
    first_failed_at: _timestamp_pb2.Timestamp
    last_failed_at: _timestamp_pb2.Timestamp
    next_attempt_at: _timestamp_pb2.Timestamp
    permanent: bool
    action: str
    reason: ClosureFailureReason
    def __init__(self, incident_id: _Optional[str] = ..., attempts: _Optional[int] = ..., max_attempts: _Optional[int] = ..., last_error: _Optional[str] = ..., first_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., permanent: _Optional[bool] = ..., action: _Optional[str] = ..., reason: _Optional[_Union[ClosureFailureReason, str]] = ...) -> None: ...

class ControllerState(_message.Message):
    __slots__ = ("version", "scheduler_deadlines", "finding_states", "pending_batch", "history", "incident_open", "responder_done", "incident_request", "incident_result", "dispatch_error", "incident_detected_at", "incident_dispatched_at", "responder_completed_at", "detector_clear_since", "dispatch_state", "incident_finding_keys", "pending_closure", "closure_state", "closure_receipt", "closure_failure", "last_acknowledged_incident_id", "evaluation_iteration", "detector_timeline", "incident_view", "drift_events", "ownership_adoption", "responder_tool_state_json", "failed_closures", "selection_ledger", "accepted_source_drift", "accepted_source_drift_revision", "namespace_onboarding", "queued_reflection_incident_ids", "queued_reflections_unknown", "busy_until", "step_count")
    class SchedulerDeadlinesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _timestamp_pb2.Timestamp
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    class FindingStatesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: FindingState
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[FindingState, _Mapping]] = ...) -> None: ...
    class DetectorClearSinceEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _timestamp_pb2.Timestamp
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SCHEDULER_DEADLINES_FIELD_NUMBER: _ClassVar[int]
    FINDING_STATES_FIELD_NUMBER: _ClassVar[int]
    PENDING_BATCH_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_OPEN_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_DONE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_REQUEST_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_RESULT_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_ERROR_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_DISPATCHED_AT_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLEAR_SINCE_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_STATE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_FINDING_KEYS_FIELD_NUMBER: _ClassVar[int]
    PENDING_CLOSURE_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_STATE_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_FAILURE_FIELD_NUMBER: _ClassVar[int]
    LAST_ACKNOWLEDGED_INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_ITERATION_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_TIMELINE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_VIEW_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_FIELD_NUMBER: _ClassVar[int]
    OWNERSHIP_ADOPTION_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_TOOL_STATE_JSON_FIELD_NUMBER: _ClassVar[int]
    FAILED_CLOSURES_FIELD_NUMBER: _ClassVar[int]
    SELECTION_LEDGER_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_SOURCE_DRIFT_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_SOURCE_DRIFT_REVISION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_ONBOARDING_FIELD_NUMBER: _ClassVar[int]
    QUEUED_REFLECTION_INCIDENT_IDS_FIELD_NUMBER: _ClassVar[int]
    QUEUED_REFLECTIONS_UNKNOWN_FIELD_NUMBER: _ClassVar[int]
    BUSY_UNTIL_FIELD_NUMBER: _ClassVar[int]
    STEP_COUNT_FIELD_NUMBER: _ClassVar[int]
    version: str
    scheduler_deadlines: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    finding_states: _containers.MessageMap[str, FindingState]
    pending_batch: BatcherState
    history: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DetectorEvaluation]
    incident_open: bool
    responder_done: bool
    incident_request: _messages_pb2.IncidentRequest
    incident_result: _messages_pb2.IncidentResult
    dispatch_error: str
    incident_detected_at: _timestamp_pb2.Timestamp
    incident_dispatched_at: _timestamp_pb2.Timestamp
    responder_completed_at: _timestamp_pb2.Timestamp
    detector_clear_since: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    dispatch_state: DispatchState
    incident_finding_keys: _containers.RepeatedScalarFieldContainer[str]
    pending_closure: _messages_pb2.IncidentClosure
    closure_state: ClosureState
    closure_receipt: ClosureReceipt
    closure_failure: ClosureFailure
    last_acknowledged_incident_id: str
    evaluation_iteration: int
    detector_timeline: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DetectorTimelineEntry]
    incident_view: _messages_pb2.IncidentView
    drift_events: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DriftEvent]
    ownership_adoption: OwnershipAdoptionStatus
    responder_tool_state_json: bytes
    failed_closures: _containers.RepeatedCompositeFieldContainer[FailedIncidentClosure]
    selection_ledger: _mitigation_menu_pb2.SelectionLedger
    accepted_source_drift: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DriftEvent]
    accepted_source_drift_revision: str
    namespace_onboarding: _messages_pb2.NamespaceOnboarding
    queued_reflection_incident_ids: _containers.RepeatedScalarFieldContainer[str]
    queued_reflections_unknown: bool
    busy_until: _timestamp_pb2.Timestamp
    step_count: int
    def __init__(self, version: _Optional[str] = ..., scheduler_deadlines: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., finding_states: _Optional[_Mapping[str, FindingState]] = ..., pending_batch: _Optional[_Union[BatcherState, _Mapping]] = ..., history: _Optional[_Iterable[_Union[_messages_pb2.DetectorEvaluation, _Mapping]]] = ..., incident_open: _Optional[bool] = ..., responder_done: _Optional[bool] = ..., incident_request: _Optional[_Union[_messages_pb2.IncidentRequest, _Mapping]] = ..., incident_result: _Optional[_Union[_messages_pb2.IncidentResult, _Mapping]] = ..., dispatch_error: _Optional[str] = ..., incident_detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., incident_dispatched_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., responder_completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detector_clear_since: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., dispatch_state: _Optional[_Union[DispatchState, str]] = ..., incident_finding_keys: _Optional[_Iterable[str]] = ..., pending_closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., closure_state: _Optional[_Union[ClosureState, str]] = ..., closure_receipt: _Optional[_Union[ClosureReceipt, _Mapping]] = ..., closure_failure: _Optional[_Union[ClosureFailure, _Mapping]] = ..., last_acknowledged_incident_id: _Optional[str] = ..., evaluation_iteration: _Optional[int] = ..., detector_timeline: _Optional[_Iterable[_Union[_messages_pb2.DetectorTimelineEntry, _Mapping]]] = ..., incident_view: _Optional[_Union[_messages_pb2.IncidentView, _Mapping]] = ..., drift_events: _Optional[_Iterable[_Union[_messages_pb2.DriftEvent, _Mapping]]] = ..., ownership_adoption: _Optional[_Union[OwnershipAdoptionStatus, _Mapping]] = ..., responder_tool_state_json: _Optional[bytes] = ..., failed_closures: _Optional[_Iterable[_Union[FailedIncidentClosure, _Mapping]]] = ..., selection_ledger: _Optional[_Union[_mitigation_menu_pb2.SelectionLedger, _Mapping]] = ..., accepted_source_drift: _Optional[_Iterable[_Union[_messages_pb2.DriftEvent, _Mapping]]] = ..., accepted_source_drift_revision: _Optional[str] = ..., namespace_onboarding: _Optional[_Union[_messages_pb2.NamespaceOnboarding, _Mapping]] = ..., queued_reflection_incident_ids: _Optional[_Iterable[str]] = ..., queued_reflections_unknown: _Optional[bool] = ..., busy_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., step_count: _Optional[int] = ...) -> None: ...

class ReflectionDecision(_message.Message):
    __slots__ = ("learning_decision", "no_change_reason", "playbook_ids", "error", "sdk_gaps", "usage", "decline_reason", "failure_reason")
    LEARNING_DECISION_FIELD_NUMBER: _ClassVar[int]
    NO_CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SDK_GAPS_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    DECLINE_REASON_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    learning_decision: str
    no_change_reason: str
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    error: str
    sdk_gaps: _containers.RepeatedCompositeFieldContainer[_outcome_pb2.SdkGap]
    usage: _messages_pb2.UsageMetrics
    decline_reason: str
    failure_reason: str
    def __init__(self, learning_decision: _Optional[str] = ..., no_change_reason: _Optional[str] = ..., playbook_ids: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., sdk_gaps: _Optional[_Iterable[_Union[_outcome_pb2.SdkGap, _Mapping]]] = ..., usage: _Optional[_Union[_messages_pb2.UsageMetrics, _Mapping]] = ..., decline_reason: _Optional[str] = ..., failure_reason: _Optional[str] = ...) -> None: ...

class ProposalRejectionLog(_message.Message):
    __slots__ = ("proposal_id", "rejections", "terminal")
    PROPOSAL_ID_FIELD_NUMBER: _ClassVar[int]
    REJECTIONS_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_FIELD_NUMBER: _ClassVar[int]
    proposal_id: str
    rejections: _containers.RepeatedCompositeFieldContainer[_memory_diagnostic_pb2.ProposalRejection]
    terminal: bool
    def __init__(self, proposal_id: _Optional[str] = ..., rejections: _Optional[_Iterable[_Union[_memory_diagnostic_pb2.ProposalRejection, _Mapping]]] = ..., terminal: _Optional[bool] = ...) -> None: ...

class ControllerRolloutExpectation(_message.Message):
    __slots__ = ("incident_id", "reflection_commit", "before_detector_fingerprint", "after_detector_fingerprint")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_COMMIT_FIELD_NUMBER: _ClassVar[int]
    BEFORE_DETECTOR_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    AFTER_DETECTOR_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    reflection_commit: str
    before_detector_fingerprint: str
    after_detector_fingerprint: str
    def __init__(self, incident_id: _Optional[str] = ..., reflection_commit: _Optional[str] = ..., before_detector_fingerprint: _Optional[str] = ..., after_detector_fingerprint: _Optional[str] = ...) -> None: ...

class ControllerRolloutReceipt(_message.Message):
    __slots__ = ("expectation", "build_artifact", "controller_revision", "observed_detector_fingerprint", "success", "verified_at")
    EXPECTATION_FIELD_NUMBER: _ClassVar[int]
    BUILD_ARTIFACT_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_REVISION_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_DETECTOR_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    expectation: ControllerRolloutExpectation
    build_artifact: str
    controller_revision: str
    observed_detector_fingerprint: str
    success: bool
    verified_at: _timestamp_pb2.Timestamp
    def __init__(self, expectation: _Optional[_Union[ControllerRolloutExpectation, _Mapping]] = ..., build_artifact: _Optional[str] = ..., controller_revision: _Optional[str] = ..., observed_detector_fingerprint: _Optional[str] = ..., success: _Optional[bool] = ..., verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class BrokerLedger(_message.Message):
    __slots__ = ("incident_id", "closure", "proposal_commit", "outcome_document", "requires_reflection", "outcome_commit", "reflection_processed", "reflection_decision", "reflection_commit", "rollout_expectation", "rollout_receipts", "receipt", "acknowledged", "acknowledged_at", "reflection_rejections", "reflection_rejected", "reflection_skipped_reason", "closure_phase_timestamps", "reflection_phase", "reflection_workspace", "reflection_paths", "reflection_results", "reflection_max_retries", "reflection_dispatch_at", "reflection_no_repair_count", "reflection_queued", "reflection_queued_at", "reflection_coverage_gaps")
    class ClosurePhaseTimestampsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _timestamp_pb2.Timestamp
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_COMMIT_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_REFLECTION_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_DECISION_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_EXPECTATION_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_RECEIPTS_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_AT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_REJECTIONS_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_REJECTED_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_SKIPPED_REASON_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_PHASE_TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_PHASE_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_PATHS_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_RESULTS_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_DISPATCH_AT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_NO_REPAIR_COUNT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_QUEUED_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_QUEUED_AT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_COVERAGE_GAPS_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    closure: _messages_pb2.IncidentClosure
    proposal_commit: str
    outcome_document: _outcome_pb2.OutcomeRecord
    requires_reflection: bool
    outcome_commit: str
    reflection_processed: bool
    reflection_decision: ReflectionDecision
    reflection_commit: str
    rollout_expectation: ControllerRolloutExpectation
    rollout_receipts: _containers.RepeatedCompositeFieldContainer[ControllerRolloutReceipt]
    receipt: ClosureReceipt
    acknowledged: bool
    acknowledged_at: _timestamp_pb2.Timestamp
    reflection_rejections: _containers.RepeatedCompositeFieldContainer[_memory_diagnostic_pb2.ProposalRejection]
    reflection_rejected: bool
    reflection_skipped_reason: str
    closure_phase_timestamps: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    reflection_phase: ReflectionPhase
    reflection_workspace: _worker_pb2.ProposalWorkspace
    reflection_paths: _containers.RepeatedScalarFieldContainer[str]
    reflection_results: _containers.RepeatedCompositeFieldContainer[ReflectionDecision]
    reflection_max_retries: int
    reflection_dispatch_at: _timestamp_pb2.Timestamp
    reflection_no_repair_count: int
    reflection_queued: bool
    reflection_queued_at: _timestamp_pb2.Timestamp
    reflection_coverage_gaps: _containers.RepeatedCompositeFieldContainer[_memory_diagnostic_pb2.MemoryDiagnostic]
    def __init__(self, incident_id: _Optional[str] = ..., closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., proposal_commit: _Optional[str] = ..., outcome_document: _Optional[_Union[_outcome_pb2.OutcomeRecord, _Mapping]] = ..., requires_reflection: _Optional[bool] = ..., outcome_commit: _Optional[str] = ..., reflection_processed: _Optional[bool] = ..., reflection_decision: _Optional[_Union[ReflectionDecision, _Mapping]] = ..., reflection_commit: _Optional[str] = ..., rollout_expectation: _Optional[_Union[ControllerRolloutExpectation, _Mapping]] = ..., rollout_receipts: _Optional[_Iterable[_Union[ControllerRolloutReceipt, _Mapping]]] = ..., receipt: _Optional[_Union[ClosureReceipt, _Mapping]] = ..., acknowledged: _Optional[bool] = ..., acknowledged_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reflection_rejections: _Optional[_Iterable[_Union[_memory_diagnostic_pb2.ProposalRejection, _Mapping]]] = ..., reflection_rejected: _Optional[bool] = ..., reflection_skipped_reason: _Optional[str] = ..., closure_phase_timestamps: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., reflection_phase: _Optional[_Union[ReflectionPhase, str]] = ..., reflection_workspace: _Optional[_Union[_worker_pb2.ProposalWorkspace, _Mapping]] = ..., reflection_paths: _Optional[_Iterable[str]] = ..., reflection_results: _Optional[_Iterable[_Union[ReflectionDecision, _Mapping]]] = ..., reflection_max_retries: _Optional[int] = ..., reflection_dispatch_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reflection_no_repair_count: _Optional[int] = ..., reflection_queued: _Optional[bool] = ..., reflection_queued_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reflection_coverage_gaps: _Optional[_Iterable[_Union[_memory_diagnostic_pb2.MemoryDiagnostic, _Mapping]]] = ...) -> None: ...

class OwnershipAdoptionStatus(_message.Message):
    __slots__ = ("source_commit", "adopted_objects", "conflicts_taken_over", "from_managers", "last_error", "skipped_reason", "retry_at", "failures", "accepted_objects", "unadopted_objects")
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ADOPTED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
    CONFLICTS_TAKEN_OVER_FIELD_NUMBER: _ClassVar[int]
    FROM_MANAGERS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_REASON_FIELD_NUMBER: _ClassVar[int]
    RETRY_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURES_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
    UNADOPTED_OBJECTS_FIELD_NUMBER: _ClassVar[int]
    source_commit: str
    adopted_objects: int
    conflicts_taken_over: int
    from_managers: _containers.RepeatedScalarFieldContainer[str]
    last_error: str
    skipped_reason: str
    retry_at: _timestamp_pb2.Timestamp
    failures: int
    accepted_objects: _containers.RepeatedScalarFieldContainer[str]
    unadopted_objects: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, source_commit: _Optional[str] = ..., adopted_objects: _Optional[int] = ..., conflicts_taken_over: _Optional[int] = ..., from_managers: _Optional[_Iterable[str]] = ..., last_error: _Optional[str] = ..., skipped_reason: _Optional[str] = ..., retry_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failures: _Optional[int] = ..., accepted_objects: _Optional[_Iterable[str]] = ..., unadopted_objects: _Optional[_Iterable[str]] = ...) -> None: ...
