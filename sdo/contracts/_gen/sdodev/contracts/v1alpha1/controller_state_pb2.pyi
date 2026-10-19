import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from sdodev.contracts.v1alpha1 import outcome_pb2 as _outcome_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FindingState(_message.Message):
    __slots__ = ("detector_id", "finding", "firing_count", "clear_count", "active", "first_seen_at", "activations", "clears", "not_persisted")
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    FINDING_FIELD_NUMBER: _ClassVar[int]
    FIRING_COUNT_FIELD_NUMBER: _ClassVar[int]
    CLEAR_COUNT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    FIRST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVATIONS_FIELD_NUMBER: _ClassVar[int]
    CLEARS_FIELD_NUMBER: _ClassVar[int]
    NOT_PERSISTED_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    finding: _messages_pb2.Finding
    firing_count: int
    clear_count: int
    active: bool
    first_seen_at: _timestamp_pb2.Timestamp
    activations: int
    clears: int
    not_persisted: int
    def __init__(self, detector_id: _Optional[str] = ..., finding: _Optional[_Union[_messages_pb2.Finding, _Mapping]] = ..., firing_count: _Optional[int] = ..., clear_count: _Optional[int] = ..., active: _Optional[bool] = ..., first_seen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., activations: _Optional[int] = ..., clears: _Optional[int] = ..., not_persisted: _Optional[int] = ...) -> None: ...

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

class ClosureFailure(_message.Message):
    __slots__ = ("incident_id", "attempts", "max_attempts", "last_error", "first_failed_at", "last_failed_at", "next_attempt_at", "permanent", "action")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    FIRST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    PERMANENT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    attempts: int
    max_attempts: int
    last_error: str
    first_failed_at: _timestamp_pb2.Timestamp
    last_failed_at: _timestamp_pb2.Timestamp
    next_attempt_at: _timestamp_pb2.Timestamp
    permanent: bool
    action: str
    def __init__(self, incident_id: _Optional[str] = ..., attempts: _Optional[int] = ..., max_attempts: _Optional[int] = ..., last_error: _Optional[str] = ..., first_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., permanent: _Optional[bool] = ..., action: _Optional[str] = ...) -> None: ...

class ControllerState(_message.Message):
    __slots__ = ("version", "scheduler_deadlines", "finding_states", "pending_batch", "history", "incident_open", "responder_done", "incident_request", "incident_result", "dispatch_error", "incident_detected_at", "incident_dispatched_at", "responder_completed_at", "detector_review_required", "detector_review_required_at", "detector_review_reason", "detector_clear_since", "incident_observed_changes", "dispatch_state", "incident_finding_keys", "pending_closure", "closure_state", "closure_receipt", "closure_failure", "last_acknowledged_incident_id", "pending_releases", "evaluation_iteration", "detector_timeline", "incident_view")
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
    DETECTOR_REVIEW_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_REVIEW_REQUIRED_AT_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_REVIEW_REASON_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLEAR_SINCE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_OBSERVED_CHANGES_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_STATE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_FINDING_KEYS_FIELD_NUMBER: _ClassVar[int]
    PENDING_CLOSURE_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_STATE_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_FAILURE_FIELD_NUMBER: _ClassVar[int]
    LAST_ACKNOWLEDGED_INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    PENDING_RELEASES_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_ITERATION_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_TIMELINE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_VIEW_FIELD_NUMBER: _ClassVar[int]
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
    detector_review_required: bool
    detector_review_required_at: _timestamp_pb2.Timestamp
    detector_review_reason: str
    detector_clear_since: _containers.MessageMap[str, _timestamp_pb2.Timestamp]
    incident_observed_changes: _containers.RepeatedCompositeFieldContainer[_messages_pb2.ObservedStateChange]
    dispatch_state: str
    incident_finding_keys: _containers.RepeatedScalarFieldContainer[str]
    pending_closure: _messages_pb2.IncidentClosure
    closure_state: str
    closure_receipt: ClosureReceipt
    closure_failure: ClosureFailure
    last_acknowledged_incident_id: str
    pending_releases: _containers.RepeatedScalarFieldContainer[str]
    evaluation_iteration: int
    detector_timeline: _containers.RepeatedCompositeFieldContainer[_messages_pb2.DetectorTimelineEntry]
    incident_view: _messages_pb2.IncidentView
    def __init__(self, version: _Optional[str] = ..., scheduler_deadlines: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., finding_states: _Optional[_Mapping[str, FindingState]] = ..., pending_batch: _Optional[_Union[BatcherState, _Mapping]] = ..., history: _Optional[_Iterable[_Union[_messages_pb2.DetectorEvaluation, _Mapping]]] = ..., incident_open: _Optional[bool] = ..., responder_done: _Optional[bool] = ..., incident_request: _Optional[_Union[_messages_pb2.IncidentRequest, _Mapping]] = ..., incident_result: _Optional[_Union[_messages_pb2.IncidentResult, _Mapping]] = ..., dispatch_error: _Optional[str] = ..., incident_detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., incident_dispatched_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., responder_completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detector_review_required: _Optional[bool] = ..., detector_review_required_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detector_review_reason: _Optional[str] = ..., detector_clear_since: _Optional[_Mapping[str, _timestamp_pb2.Timestamp]] = ..., incident_observed_changes: _Optional[_Iterable[_Union[_messages_pb2.ObservedStateChange, _Mapping]]] = ..., dispatch_state: _Optional[str] = ..., incident_finding_keys: _Optional[_Iterable[str]] = ..., pending_closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., closure_state: _Optional[str] = ..., closure_receipt: _Optional[_Union[ClosureReceipt, _Mapping]] = ..., closure_failure: _Optional[_Union[ClosureFailure, _Mapping]] = ..., last_acknowledged_incident_id: _Optional[str] = ..., pending_releases: _Optional[_Iterable[str]] = ..., evaluation_iteration: _Optional[int] = ..., detector_timeline: _Optional[_Iterable[_Union[_messages_pb2.DetectorTimelineEntry, _Mapping]]] = ..., incident_view: _Optional[_Union[_messages_pb2.IncidentView, _Mapping]] = ...) -> None: ...

class ReflectionDecision(_message.Message):
    __slots__ = ("learning_decision", "no_change_reason")
    LEARNING_DECISION_FIELD_NUMBER: _ClassVar[int]
    NO_CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    learning_decision: str
    no_change_reason: str
    def __init__(self, learning_decision: _Optional[str] = ..., no_change_reason: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("incident_id", "closure", "proposal_commit", "outcome_document", "requires_reflection", "outcome_commit", "reflection_processed", "reflection_decision", "reflection_commit", "rollout_expectation", "rollout_receipts", "receipt", "acknowledged", "acknowledged_at")
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
    def __init__(self, incident_id: _Optional[str] = ..., closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., proposal_commit: _Optional[str] = ..., outcome_document: _Optional[_Union[_outcome_pb2.OutcomeRecord, _Mapping]] = ..., requires_reflection: _Optional[bool] = ..., outcome_commit: _Optional[str] = ..., reflection_processed: _Optional[bool] = ..., reflection_decision: _Optional[_Union[ReflectionDecision, _Mapping]] = ..., reflection_commit: _Optional[str] = ..., rollout_expectation: _Optional[_Union[ControllerRolloutExpectation, _Mapping]] = ..., rollout_receipts: _Optional[_Iterable[_Union[ControllerRolloutReceipt, _Mapping]]] = ..., receipt: _Optional[_Union[ClosureReceipt, _Mapping]] = ..., acknowledged: _Optional[bool] = ..., acknowledged_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
