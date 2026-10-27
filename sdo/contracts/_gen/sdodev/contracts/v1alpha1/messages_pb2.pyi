import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import controller_config_pb2 as _controller_config_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from sdodev.contracts.v1alpha1 import common_pb2 as _common_pb2
from sdodev.contracts.v1alpha1 import program_pb2 as _program_pb2
from sdodev.contracts.v1alpha1 import traffic_pb2 as _traffic_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ClosureUnconfirmedReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED: _ClassVar[ClosureUnconfirmedReason]
    CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE: _ClassVar[ClosureUnconfirmedReason]
    CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE: _ClassVar[ClosureUnconfirmedReason]
CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED: ClosureUnconfirmedReason
CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE: ClosureUnconfirmedReason
CLOSURE_UNCONFIRMED_REASON_OBSERVER_UNAVAILABLE: ClosureUnconfirmedReason

class Finding(_message.Message):
    __slots__ = ("detector_id", "rule_id", "status", "severity", "summary", "evidence", "primary_resource", "related_resources", "playbook_ids", "parameter_bindings", "fingerprint", "metadata")
    class ParameterBindingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _common_pb2.ObjectRef
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_RESOURCE_FIELD_NUMBER: _ClassVar[int]
    RELATED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    rule_id: str
    status: str
    severity: str
    summary: str
    evidence: str
    primary_resource: _common_pb2.ObjectRef
    related_resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    parameter_bindings: _containers.MessageMap[str, _common_pb2.ObjectRef]
    fingerprint: str
    metadata: _struct_pb2.Struct
    def __init__(self, detector_id: _Optional[str] = ..., rule_id: _Optional[str] = ..., status: _Optional[str] = ..., severity: _Optional[str] = ..., summary: _Optional[str] = ..., evidence: _Optional[str] = ..., primary_resource: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., related_resources: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., playbook_ids: _Optional[_Iterable[str]] = ..., parameter_bindings: _Optional[_Mapping[str, _common_pb2.ObjectRef]] = ..., fingerprint: _Optional[str] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class DriftField(_message.Message):
    __slots__ = ("path", "old", "new", "source")
    PATH_FIELD_NUMBER: _ClassVar[int]
    OLD_FIELD_NUMBER: _ClassVar[int]
    NEW_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    path: str
    old: str
    new: str
    source: str
    def __init__(self, path: _Optional[str] = ..., old: _Optional[str] = ..., new: _Optional[str] = ..., source: _Optional[str] = ...) -> None: ...

class DriftEvent(_message.Message):
    __slots__ = ("ref", "fields", "source", "manager", "operation", "observed_at", "classification", "program_id", "manager_at", "investigation_status", "investigation_verdict", "investigation_reason", "investigation_evidence", "investigation_capture_id")
    REF_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    MANAGER_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    MANAGER_AT_FIELD_NUMBER: _ClassVar[int]
    INVESTIGATION_STATUS_FIELD_NUMBER: _ClassVar[int]
    INVESTIGATION_VERDICT_FIELD_NUMBER: _ClassVar[int]
    INVESTIGATION_REASON_FIELD_NUMBER: _ClassVar[int]
    INVESTIGATION_EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    INVESTIGATION_CAPTURE_ID_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    fields: _containers.RepeatedCompositeFieldContainer[DriftField]
    source: str
    manager: str
    operation: str
    observed_at: _timestamp_pb2.Timestamp
    classification: str
    program_id: str
    manager_at: _timestamp_pb2.Timestamp
    investigation_status: str
    investigation_verdict: str
    investigation_reason: str
    investigation_evidence: _containers.RepeatedCompositeFieldContainer[DriftEvidence]
    investigation_capture_id: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., fields: _Optional[_Iterable[_Union[DriftField, _Mapping]]] = ..., source: _Optional[str] = ..., manager: _Optional[str] = ..., operation: _Optional[str] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., classification: _Optional[str] = ..., program_id: _Optional[str] = ..., manager_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., investigation_status: _Optional[str] = ..., investigation_verdict: _Optional[str] = ..., investigation_reason: _Optional[str] = ..., investigation_evidence: _Optional[_Iterable[_Union[DriftEvidence, _Mapping]]] = ..., investigation_capture_id: _Optional[str] = ...) -> None: ...

class DriftEvidence(_message.Message):
    __slots__ = ("kind", "observation")
    KIND_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    kind: str
    observation: str
    def __init__(self, kind: _Optional[str] = ..., observation: _Optional[str] = ...) -> None: ...

class DetectorEvaluation(_message.Message):
    __slots__ = ("detector_id", "evaluated_at", "status", "fingerprints", "error", "reason")
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    EVALUATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    evaluated_at: _timestamp_pb2.Timestamp
    status: str
    fingerprints: _containers.RepeatedScalarFieldContainer[str]
    error: str
    reason: str
    def __init__(self, detector_id: _Optional[str] = ..., evaluated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[str] = ..., fingerprints: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class SurfacedPlaybook(_message.Message):
    __slots__ = ("playbook_id", "parameter_bindings")
    class ParameterBindingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _common_pb2.ObjectRef
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...
    PLAYBOOK_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    playbook_id: str
    parameter_bindings: _containers.MessageMap[str, _common_pb2.ObjectRef]
    def __init__(self, playbook_id: _Optional[str] = ..., parameter_bindings: _Optional[_Mapping[str, _common_pb2.ObjectRef]] = ...) -> None: ...

class PriorOutcomeEvidence(_message.Message):
    __slots__ = ("incident_id", "match_reason", "root_cause_summaries", "repair_action_summaries", "applied_playbook_ids", "source_commit", "exact_source_match")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_REASON_FIELD_NUMBER: _ClassVar[int]
    ROOT_CAUSE_SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    REPAIR_ACTION_SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    APPLIED_PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    EXACT_SOURCE_MATCH_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    match_reason: str
    root_cause_summaries: _containers.RepeatedScalarFieldContainer[str]
    repair_action_summaries: _containers.RepeatedScalarFieldContainer[str]
    applied_playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    source_commit: str
    exact_source_match: bool
    def __init__(self, incident_id: _Optional[str] = ..., match_reason: _Optional[str] = ..., root_cause_summaries: _Optional[_Iterable[str]] = ..., repair_action_summaries: _Optional[_Iterable[str]] = ..., applied_playbook_ids: _Optional[_Iterable[str]] = ..., source_commit: _Optional[str] = ..., exact_source_match: _Optional[bool] = ...) -> None: ...

class StateFieldChange(_message.Message):
    __slots__ = ("field", "before", "after")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    BEFORE_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    field: str
    before: str
    after: str
    def __init__(self, field: _Optional[str] = ..., before: _Optional[str] = ..., after: _Optional[str] = ...) -> None: ...

class StateChange(_message.Message):
    __slots__ = ("kind", "name", "change", "fields", "api_version", "namespace")
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    kind: str
    name: str
    change: str
    fields: _containers.RepeatedCompositeFieldContainer[StateFieldChange]
    api_version: str
    namespace: str
    def __init__(self, kind: _Optional[str] = ..., name: _Optional[str] = ..., change: _Optional[str] = ..., fields: _Optional[_Iterable[_Union[StateFieldChange, _Mapping]]] = ..., api_version: _Optional[str] = ..., namespace: _Optional[str] = ...) -> None: ...

class StateChanges(_message.Message):
    __slots__ = ("baseline_at", "observed_at", "changes", "omitted", "unobserved_kinds")
    BASELINE_AT_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    OMITTED_FIELD_NUMBER: _ClassVar[int]
    UNOBSERVED_KINDS_FIELD_NUMBER: _ClassVar[int]
    baseline_at: _timestamp_pb2.Timestamp
    observed_at: _timestamp_pb2.Timestamp
    changes: _containers.RepeatedCompositeFieldContainer[StateChange]
    omitted: int
    unobserved_kinds: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, baseline_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., changes: _Optional[_Iterable[_Union[StateChange, _Mapping]]] = ..., omitted: _Optional[int] = ..., unobserved_kinds: _Optional[_Iterable[str]] = ...) -> None: ...

class MemoryWatch(_message.Message):
    __slots__ = ("api_version", "kind")
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    api_version: str
    kind: str
    def __init__(self, api_version: _Optional[str] = ..., kind: _Optional[str] = ...) -> None: ...

class MemoryDetector(_message.Message):
    __slots__ = ("id", "detector_class", "owner", "watches", "firing", "possible_playbook_ids", "originating_incident", "description", "predicate_excerpt", "reads_events")
    ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    WATCHES_FIELD_NUMBER: _ClassVar[int]
    FIRING_FIELD_NUMBER: _ClassVar[int]
    POSSIBLE_PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_INCIDENT_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PREDICATE_EXCERPT_FIELD_NUMBER: _ClassVar[int]
    READS_EVENTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    detector_class: str
    owner: str
    watches: _containers.RepeatedCompositeFieldContainer[MemoryWatch]
    firing: int
    possible_playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    originating_incident: str
    description: str
    predicate_excerpt: str
    reads_events: bool
    def __init__(self, id: _Optional[str] = ..., detector_class: _Optional[str] = ..., owner: _Optional[str] = ..., watches: _Optional[_Iterable[_Union[MemoryWatch, _Mapping]]] = ..., firing: _Optional[int] = ..., possible_playbook_ids: _Optional[_Iterable[str]] = ..., originating_incident: _Optional[str] = ..., description: _Optional[str] = ..., predicate_excerpt: _Optional[str] = ..., reads_events: _Optional[bool] = ...) -> None: ...

class MemoryScript(_message.Message):
    __slots__ = ("name", "content")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    name: str
    content: str
    def __init__(self, name: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class MemoryPlaybook(_message.Message):
    __slots__ = ("id", "markdown", "scripts")
    ID_FIELD_NUMBER: _ClassVar[int]
    MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    SCRIPTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    markdown: str
    scripts: _containers.RepeatedCompositeFieldContainer[MemoryScript]
    def __init__(self, id: _Optional[str] = ..., markdown: _Optional[str] = ..., scripts: _Optional[_Iterable[_Union[MemoryScript, _Mapping]]] = ...) -> None: ...

class OperationalMemorySnapshot(_message.Message):
    __slots__ = ("code_path", "accepted_commit", "goal_markdown", "architecture_markdown", "playbook_index_markdown", "detectors", "playbooks")
    CODE_PATH_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    GOAL_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    ARCHITECTURE_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_INDEX_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    DETECTORS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    code_path: _controller_config_pb2.CodePathConfig
    accepted_commit: str
    goal_markdown: str
    architecture_markdown: str
    playbook_index_markdown: str
    detectors: _containers.RepeatedCompositeFieldContainer[MemoryDetector]
    playbooks: _containers.RepeatedCompositeFieldContainer[MemoryPlaybook]
    def __init__(self, code_path: _Optional[_Union[_controller_config_pb2.CodePathConfig, _Mapping]] = ..., accepted_commit: _Optional[str] = ..., goal_markdown: _Optional[str] = ..., architecture_markdown: _Optional[str] = ..., playbook_index_markdown: _Optional[str] = ..., detectors: _Optional[_Iterable[_Union[MemoryDetector, _Mapping]]] = ..., playbooks: _Optional[_Iterable[_Union[MemoryPlaybook, _Mapping]]] = ...) -> None: ...

class ProposedGoSource(_message.Message):
    __slots__ = ("name", "content")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    name: str
    content: str
    def __init__(self, name: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class ProposedDetector(_message.Message):
    __slots__ = ("id", "detector_class", "owner", "constructor", "watches", "interval", "firing", "clearing", "severity", "debounce", "possible_playbook_ids", "originating_commit", "originating_incident", "sources")
    ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    CONSTRUCTOR_FIELD_NUMBER: _ClassVar[int]
    WATCHES_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    FIRING_FIELD_NUMBER: _ClassVar[int]
    CLEARING_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    DEBOUNCE_FIELD_NUMBER: _ClassVar[int]
    POSSIBLE_PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_INCIDENT_FIELD_NUMBER: _ClassVar[int]
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    id: str
    detector_class: str
    owner: str
    constructor: str
    watches: _containers.RepeatedCompositeFieldContainer[MemoryWatch]
    interval: str
    firing: int
    clearing: int
    severity: str
    debounce: str
    possible_playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    originating_commit: str
    originating_incident: str
    sources: _containers.RepeatedCompositeFieldContainer[ProposedGoSource]
    def __init__(self, id: _Optional[str] = ..., detector_class: _Optional[str] = ..., owner: _Optional[str] = ..., constructor: _Optional[str] = ..., watches: _Optional[_Iterable[_Union[MemoryWatch, _Mapping]]] = ..., interval: _Optional[str] = ..., firing: _Optional[int] = ..., clearing: _Optional[int] = ..., severity: _Optional[str] = ..., debounce: _Optional[str] = ..., possible_playbook_ids: _Optional[_Iterable[str]] = ..., originating_commit: _Optional[str] = ..., originating_incident: _Optional[str] = ..., sources: _Optional[_Iterable[_Union[ProposedGoSource, _Mapping]]] = ...) -> None: ...

class ProposedPlaybook(_message.Message):
    __slots__ = ("id", "owner", "fault_class", "originating_incident", "markdown", "scripts")
    ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    FAULT_CLASS_FIELD_NUMBER: _ClassVar[int]
    ORIGINATING_INCIDENT_FIELD_NUMBER: _ClassVar[int]
    MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    SCRIPTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    owner: str
    fault_class: str
    originating_incident: str
    markdown: str
    scripts: _containers.RepeatedCompositeFieldContainer[MemoryScript]
    def __init__(self, id: _Optional[str] = ..., owner: _Optional[str] = ..., fault_class: _Optional[str] = ..., originating_incident: _Optional[str] = ..., markdown: _Optional[str] = ..., scripts: _Optional[_Iterable[_Union[MemoryScript, _Mapping]]] = ...) -> None: ...

class ProposedTrafficGenerator(_message.Message):
    __slots__ = ("id", "sources")
    ID_FIELD_NUMBER: _ClassVar[int]
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    id: str
    sources: _containers.RepeatedCompositeFieldContainer[ProposedGoSource]
    def __init__(self, id: _Optional[str] = ..., sources: _Optional[_Iterable[_Union[ProposedGoSource, _Mapping]]] = ...) -> None: ...

class OperationalArtifactProposal(_message.Message):
    __slots__ = ("detectors", "playbooks", "traffic_generators", "traffic_workloads", "delete_detector_ids", "delete_playbook_ids")
    DETECTORS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_GENERATORS_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_WORKLOADS_FIELD_NUMBER: _ClassVar[int]
    DELETE_DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    DELETE_PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    detectors: _containers.RepeatedCompositeFieldContainer[ProposedDetector]
    playbooks: _containers.RepeatedCompositeFieldContainer[ProposedPlaybook]
    traffic_generators: _containers.RepeatedCompositeFieldContainer[ProposedTrafficGenerator]
    traffic_workloads: _containers.RepeatedCompositeFieldContainer[_traffic_pb2.TrafficWorkload]
    delete_detector_ids: _containers.RepeatedScalarFieldContainer[str]
    delete_playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, detectors: _Optional[_Iterable[_Union[ProposedDetector, _Mapping]]] = ..., playbooks: _Optional[_Iterable[_Union[ProposedPlaybook, _Mapping]]] = ..., traffic_generators: _Optional[_Iterable[_Union[ProposedTrafficGenerator, _Mapping]]] = ..., traffic_workloads: _Optional[_Iterable[_Union[_traffic_pb2.TrafficWorkload, _Mapping]]] = ..., delete_detector_ids: _Optional[_Iterable[str]] = ..., delete_playbook_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class DisabledSetupDetector(_message.Message):
    __slots__ = ("detector_id", "accepted_revision", "fingerprints", "disabled_at", "reason")
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    DISABLED_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    accepted_revision: str
    fingerprints: _containers.RepeatedScalarFieldContainer[str]
    disabled_at: _timestamp_pb2.Timestamp
    reason: str
    def __init__(self, detector_id: _Optional[str] = ..., accepted_revision: _Optional[str] = ..., fingerprints: _Optional[_Iterable[str]] = ..., disabled_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class NamespaceOnboarding(_message.Message):
    __slots__ = ("disabled_setup_detectors", "namespace", "accepted_commit", "established_at", "objects", "environment_objects")
    DISABLED_SETUP_DETECTORS_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ESTABLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_OBJECTS_FIELD_NUMBER: _ClassVar[int]
    disabled_setup_detectors: _containers.RepeatedCompositeFieldContainer[DisabledSetupDetector]
    namespace: str
    accepted_commit: str
    established_at: _timestamp_pb2.Timestamp
    objects: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    environment_objects: _containers.RepeatedCompositeFieldContainer[InstallEnvironmentObject]
    def __init__(self, disabled_setup_detectors: _Optional[_Iterable[_Union[DisabledSetupDetector, _Mapping]]] = ..., namespace: _Optional[str] = ..., accepted_commit: _Optional[str] = ..., established_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., objects: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., environment_objects: _Optional[_Iterable[_Union[InstallEnvironmentObject, _Mapping]]] = ...) -> None: ...

class InstallEnvironmentObject(_message.Message):
    __slots__ = ("ref", "digest")
    REF_FIELD_NUMBER: _ClassVar[int]
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    digest: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., digest: _Optional[str] = ...) -> None: ...

class IncidentRequest(_message.Message):
    __slots__ = ("schema_version", "application", "namespace", "incident_id", "findings", "detector_history", "surfaced_playbooks", "relevant_outcomes", "source_commit", "deployed_commit", "repository_worktree", "repository_base_commit", "response_deadline", "cancellation_token", "repair_policy", "state_changes", "memory_snapshot", "initial_program_runs", "max_response_loops", "drift_events", "prepared_agent_task", "observation_generation", "playbook_coverage", "namespace_onboarding", "prior_stages", "compaction", "prior_response_progress", "disabled_setup_detectors")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_HISTORY_FIELD_NUMBER: _ClassVar[int]
    SURFACED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    RELEVANT_OUTCOMES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    DEPLOYED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_WORKTREE_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_BASE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REPAIR_POLICY_FIELD_NUMBER: _ClassVar[int]
    STATE_CHANGES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    INITIAL_PROGRAM_RUNS_FIELD_NUMBER: _ClassVar[int]
    MAX_RESPONSE_LOOPS_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_FIELD_NUMBER: _ClassVar[int]
    PREPARED_AGENT_TASK_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_GENERATION_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_ONBOARDING_FIELD_NUMBER: _ClassVar[int]
    PRIOR_STAGES_FIELD_NUMBER: _ClassVar[int]
    COMPACTION_FIELD_NUMBER: _ClassVar[int]
    PRIOR_RESPONSE_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    DISABLED_SETUP_DETECTORS_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    application: str
    namespace: str
    incident_id: str
    findings: _containers.RepeatedCompositeFieldContainer[Finding]
    detector_history: _containers.RepeatedCompositeFieldContainer[DetectorEvaluation]
    surfaced_playbooks: _containers.RepeatedCompositeFieldContainer[SurfacedPlaybook]
    relevant_outcomes: _containers.RepeatedCompositeFieldContainer[PriorOutcomeEvidence]
    source_commit: str
    deployed_commit: str
    repository_worktree: str
    repository_base_commit: str
    response_deadline: _timestamp_pb2.Timestamp
    cancellation_token: str
    repair_policy: str
    state_changes: StateChanges
    memory_snapshot: OperationalMemorySnapshot
    initial_program_runs: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramRun]
    max_response_loops: int
    drift_events: _containers.RepeatedCompositeFieldContainer[DriftEvent]
    prepared_agent_task: str
    observation_generation: str
    playbook_coverage: _program_pb2.PlaybookCoverage
    namespace_onboarding: NamespaceOnboarding
    prior_stages: _containers.RepeatedCompositeFieldContainer[StageReport]
    compaction: _containers.RepeatedScalarFieldContainer[str]
    prior_response_progress: _containers.RepeatedCompositeFieldContainer[ResponseProgress]
    disabled_setup_detectors: _containers.RepeatedCompositeFieldContainer[DisabledSetupDetector]
    def __init__(self, schema_version: _Optional[str] = ..., application: _Optional[str] = ..., namespace: _Optional[str] = ..., incident_id: _Optional[str] = ..., findings: _Optional[_Iterable[_Union[Finding, _Mapping]]] = ..., detector_history: _Optional[_Iterable[_Union[DetectorEvaluation, _Mapping]]] = ..., surfaced_playbooks: _Optional[_Iterable[_Union[SurfacedPlaybook, _Mapping]]] = ..., relevant_outcomes: _Optional[_Iterable[_Union[PriorOutcomeEvidence, _Mapping]]] = ..., source_commit: _Optional[str] = ..., deployed_commit: _Optional[str] = ..., repository_worktree: _Optional[str] = ..., repository_base_commit: _Optional[str] = ..., response_deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cancellation_token: _Optional[str] = ..., repair_policy: _Optional[str] = ..., state_changes: _Optional[_Union[StateChanges, _Mapping]] = ..., memory_snapshot: _Optional[_Union[OperationalMemorySnapshot, _Mapping]] = ..., initial_program_runs: _Optional[_Iterable[_Union[_program_pb2.ProgramRun, _Mapping]]] = ..., max_response_loops: _Optional[int] = ..., drift_events: _Optional[_Iterable[_Union[DriftEvent, _Mapping]]] = ..., prepared_agent_task: _Optional[str] = ..., observation_generation: _Optional[str] = ..., playbook_coverage: _Optional[_Union[_program_pb2.PlaybookCoverage, _Mapping]] = ..., namespace_onboarding: _Optional[_Union[NamespaceOnboarding, _Mapping]] = ..., prior_stages: _Optional[_Iterable[_Union[StageReport, _Mapping]]] = ..., compaction: _Optional[_Iterable[str]] = ..., prior_response_progress: _Optional[_Iterable[_Union[ResponseProgress, _Mapping]]] = ..., disabled_setup_detectors: _Optional[_Iterable[_Union[DisabledSetupDetector, _Mapping]]] = ...) -> None: ...

class ResponseProgress(_message.Message):
    __slots__ = ("incident_id", "successful_actions", "blockers", "error", "truncated", "omitted_episodes")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    SUCCESSFUL_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    BLOCKERS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    OMITTED_EPISODES_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    successful_actions: _containers.RepeatedCompositeFieldContainer[RepairActionReceipt]
    blockers: _containers.RepeatedCompositeFieldContainer[_program_pb2.RolloutPending]
    error: str
    truncated: bool
    omitted_episodes: int
    def __init__(self, incident_id: _Optional[str] = ..., successful_actions: _Optional[_Iterable[_Union[RepairActionReceipt, _Mapping]]] = ..., blockers: _Optional[_Iterable[_Union[_program_pb2.RolloutPending, _Mapping]]] = ..., error: _Optional[str] = ..., truncated: _Optional[bool] = ..., omitted_episodes: _Optional[int] = ...) -> None: ...

class RootCauseEvidence(_message.Message):
    __slots__ = ("kind", "source", "observation")
    KIND_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    kind: str
    source: str
    observation: str
    def __init__(self, kind: _Optional[str] = ..., source: _Optional[str] = ..., observation: _Optional[str] = ...) -> None: ...

class ConfirmedRootCause(_message.Message):
    __slots__ = ("summary", "resources", "evidence", "explained_detectors", "static_context")
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    EXPLAINED_DETECTORS_FIELD_NUMBER: _ClassVar[int]
    STATIC_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    summary: str
    resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    evidence: _containers.RepeatedCompositeFieldContainer[RootCauseEvidence]
    explained_detectors: _containers.RepeatedScalarFieldContainer[str]
    static_context: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, summary: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., evidence: _Optional[_Iterable[_Union[RootCauseEvidence, _Mapping]]] = ..., explained_detectors: _Optional[_Iterable[str]] = ..., static_context: _Optional[_Iterable[str]] = ...) -> None: ...

class AppliedPlaybook(_message.Message):
    __slots__ = ("playbook_id", "parameter_bindings", "script_names")
    class ParameterBindingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _common_pb2.ObjectRef
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...
    PLAYBOOK_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_NAMES_FIELD_NUMBER: _ClassVar[int]
    playbook_id: str
    parameter_bindings: _containers.MessageMap[str, _common_pb2.ObjectRef]
    script_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, playbook_id: _Optional[str] = ..., parameter_bindings: _Optional[_Mapping[str, _common_pb2.ObjectRef]] = ..., script_names: _Optional[_Iterable[str]] = ...) -> None: ...

class VerificationEvidence(_message.Message):
    __slots__ = ("name", "passed", "details", "observed_at")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    name: str
    passed: bool
    details: str
    observed_at: _timestamp_pb2.Timestamp
    def __init__(self, name: _Optional[str] = ..., passed: _Optional[bool] = ..., details: _Optional[str] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UsageMetrics(_message.Message):
    __slots__ = ("llm_calls", "input_tokens", "output_tokens", "cached_input_tokens", "cache_read_input_tokens", "cache_write_input_tokens", "cache_write_1h_input_tokens", "uncached_input_tokens", "reasoning_output_tokens", "model_requests", "total_cost_usd", "partial")
    LLM_CALLS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHED_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_READ_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_WRITE_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CACHE_WRITE_1H_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    UNCACHED_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    REASONING_OUTPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    MODEL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COST_USD_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FIELD_NUMBER: _ClassVar[int]
    llm_calls: int
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    cache_read_input_tokens: int
    cache_write_input_tokens: int
    cache_write_1h_input_tokens: int
    uncached_input_tokens: int
    reasoning_output_tokens: int
    model_requests: int
    total_cost_usd: float
    partial: bool
    def __init__(self, llm_calls: _Optional[int] = ..., input_tokens: _Optional[int] = ..., output_tokens: _Optional[int] = ..., cached_input_tokens: _Optional[int] = ..., cache_read_input_tokens: _Optional[int] = ..., cache_write_input_tokens: _Optional[int] = ..., cache_write_1h_input_tokens: _Optional[int] = ..., uncached_input_tokens: _Optional[int] = ..., reasoning_output_tokens: _Optional[int] = ..., model_requests: _Optional[int] = ..., total_cost_usd: _Optional[float] = ..., partial: _Optional[bool] = ...) -> None: ...

class TimingMetrics(_message.Message):
    __slots__ = ("started_at", "completed_at")
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RepairActionReceipt(_message.Message):
    __slots__ = ("action_id", "kind", "target", "resources", "summary", "details", "started_at", "completed_at", "success", "reversible", "failure_reason", "owners")
    ACTION_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    RESOURCES_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REVERSIBLE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    OWNERS_FIELD_NUMBER: _ClassVar[int]
    action_id: str
    kind: str
    target: str
    resources: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    summary: str
    details: str
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    success: bool
    reversible: bool
    failure_reason: str
    owners: _containers.RepeatedCompositeFieldContainer[ResourceOwners]
    def __init__(self, action_id: _Optional[str] = ..., kind: _Optional[str] = ..., target: _Optional[str] = ..., resources: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., summary: _Optional[str] = ..., details: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., success: _Optional[bool] = ..., reversible: _Optional[bool] = ..., failure_reason: _Optional[str] = ..., owners: _Optional[_Iterable[_Union[ResourceOwners, _Mapping]]] = ...) -> None: ...

class ResourceOwners(_message.Message):
    __slots__ = ("resource", "owner_chain")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    OWNER_CHAIN_FIELD_NUMBER: _ClassVar[int]
    resource: _common_pb2.ObjectRef
    owner_chain: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    def __init__(self, resource: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., owner_chain: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ...) -> None: ...

class IncidentResult(_message.Message):
    __slots__ = ("schema_version", "incident_id", "status", "confirmed_root_causes", "applied_playbooks", "repair_changes", "repair_actions", "final_detector_states", "proposed_artifact_ids", "verification_evidence", "usage", "timing", "responder_session_id", "error", "late_findings_pull", "program_tool_calls", "program_runs", "plan_applications", "response_loops_used", "response_loop_exhausted", "program_failure_signals", "resolution_path", "drift_events", "unresolved_findings", "drift_reprompt")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_ROOT_CAUSES_FIELD_NUMBER: _ClassVar[int]
    APPLIED_PLAYBOOKS_FIELD_NUMBER: _ClassVar[int]
    REPAIR_CHANGES_FIELD_NUMBER: _ClassVar[int]
    REPAIR_ACTIONS_FIELD_NUMBER: _ClassVar[int]
    FINAL_DETECTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    PROPOSED_ARTIFACT_IDS_FIELD_NUMBER: _ClassVar[int]
    VERIFICATION_EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    TIMING_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    LATE_FINDINGS_PULL_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_TOOL_CALLS_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_RUNS_FIELD_NUMBER: _ClassVar[int]
    PLAN_APPLICATIONS_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_LOOPS_USED_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_LOOP_EXHAUSTED_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_FAILURE_SIGNALS_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_PATH_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_FIELD_NUMBER: _ClassVar[int]
    UNRESOLVED_FINDINGS_FIELD_NUMBER: _ClassVar[int]
    DRIFT_REPROMPT_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    incident_id: str
    status: str
    confirmed_root_causes: _containers.RepeatedCompositeFieldContainer[ConfirmedRootCause]
    applied_playbooks: _containers.RepeatedCompositeFieldContainer[AppliedPlaybook]
    repair_changes: _containers.RepeatedScalarFieldContainer[str]
    repair_actions: _containers.RepeatedCompositeFieldContainer[RepairActionReceipt]
    final_detector_states: _containers.RepeatedCompositeFieldContainer[DetectorEvaluation]
    proposed_artifact_ids: _containers.RepeatedScalarFieldContainer[str]
    verification_evidence: _containers.RepeatedCompositeFieldContainer[VerificationEvidence]
    usage: UsageMetrics
    timing: TimingMetrics
    responder_session_id: str
    error: str
    late_findings_pull: LateFindingsPull
    program_tool_calls: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramToolCall]
    program_runs: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramRun]
    plan_applications: _containers.RepeatedCompositeFieldContainer[_program_pb2.PlanApplication]
    response_loops_used: int
    response_loop_exhausted: bool
    program_failure_signals: _containers.RepeatedScalarFieldContainer[str]
    resolution_path: str
    drift_events: _containers.RepeatedCompositeFieldContainer[DriftEvent]
    unresolved_findings: _containers.RepeatedCompositeFieldContainer[Finding]
    drift_reprompt: DriftReprompt
    def __init__(self, schema_version: _Optional[str] = ..., incident_id: _Optional[str] = ..., status: _Optional[str] = ..., confirmed_root_causes: _Optional[_Iterable[_Union[ConfirmedRootCause, _Mapping]]] = ..., applied_playbooks: _Optional[_Iterable[_Union[AppliedPlaybook, _Mapping]]] = ..., repair_changes: _Optional[_Iterable[str]] = ..., repair_actions: _Optional[_Iterable[_Union[RepairActionReceipt, _Mapping]]] = ..., final_detector_states: _Optional[_Iterable[_Union[DetectorEvaluation, _Mapping]]] = ..., proposed_artifact_ids: _Optional[_Iterable[str]] = ..., verification_evidence: _Optional[_Iterable[_Union[VerificationEvidence, _Mapping]]] = ..., usage: _Optional[_Union[UsageMetrics, _Mapping]] = ..., timing: _Optional[_Union[TimingMetrics, _Mapping]] = ..., responder_session_id: _Optional[str] = ..., error: _Optional[str] = ..., late_findings_pull: _Optional[_Union[LateFindingsPull, _Mapping]] = ..., program_tool_calls: _Optional[_Iterable[_Union[_program_pb2.ProgramToolCall, _Mapping]]] = ..., program_runs: _Optional[_Iterable[_Union[_program_pb2.ProgramRun, _Mapping]]] = ..., plan_applications: _Optional[_Iterable[_Union[_program_pb2.PlanApplication, _Mapping]]] = ..., response_loops_used: _Optional[int] = ..., response_loop_exhausted: _Optional[bool] = ..., program_failure_signals: _Optional[_Iterable[str]] = ..., resolution_path: _Optional[str] = ..., drift_events: _Optional[_Iterable[_Union[DriftEvent, _Mapping]]] = ..., unresolved_findings: _Optional[_Iterable[_Union[Finding, _Mapping]]] = ..., drift_reprompt: _Optional[_Union[DriftReprompt, _Mapping]] = ...) -> None: ...

class LateFindingsPull(_message.Message):
    __slots__ = ("incident_id", "pull_count", "detector_ids", "playbook_ids", "report_digest")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    PULL_COUNT_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    REPORT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    pull_count: int
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    report_digest: str
    def __init__(self, incident_id: _Optional[str] = ..., pull_count: _Optional[int] = ..., detector_ids: _Optional[_Iterable[str]] = ..., playbook_ids: _Optional[_Iterable[str]] = ..., report_digest: _Optional[str] = ...) -> None: ...

class IncidentView(_message.Message):
    __slots__ = ("incident_id", "observed_at", "blocking_detectors", "blocking_findings", "clearing_findings", "state_changes", "drift_events")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    BLOCKING_DETECTORS_FIELD_NUMBER: _ClassVar[int]
    BLOCKING_FINDINGS_FIELD_NUMBER: _ClassVar[int]
    CLEARING_FINDINGS_FIELD_NUMBER: _ClassVar[int]
    STATE_CHANGES_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    observed_at: _timestamp_pb2.Timestamp
    blocking_detectors: _containers.RepeatedScalarFieldContainer[str]
    blocking_findings: _containers.RepeatedCompositeFieldContainer[Finding]
    clearing_findings: _containers.RepeatedCompositeFieldContainer[Finding]
    state_changes: StateChanges
    drift_events: _containers.RepeatedCompositeFieldContainer[DriftEvent]
    def __init__(self, incident_id: _Optional[str] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., blocking_detectors: _Optional[_Iterable[str]] = ..., blocking_findings: _Optional[_Iterable[_Union[Finding, _Mapping]]] = ..., clearing_findings: _Optional[_Iterable[_Union[Finding, _Mapping]]] = ..., state_changes: _Optional[_Union[StateChanges, _Mapping]] = ..., drift_events: _Optional[_Iterable[_Union[DriftEvent, _Mapping]]] = ...) -> None: ...

class DetectorTimelineEntry(_message.Message):
    __slots__ = ("detector_id", "detector_class", "owner", "rule_id", "fingerprint", "parameter_bindings", "surfaced_playbook_ids", "first_activated_at", "last_seen_at", "cleared_at", "relation", "activations")
    class ParameterBindingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _common_pb2.ObjectRef
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    SURFACED_PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    FIRST_ACTIVATED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_AT_FIELD_NUMBER: _ClassVar[int]
    CLEARED_AT_FIELD_NUMBER: _ClassVar[int]
    RELATION_FIELD_NUMBER: _ClassVar[int]
    ACTIVATIONS_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    detector_class: str
    owner: str
    rule_id: str
    fingerprint: str
    parameter_bindings: _containers.MessageMap[str, _common_pb2.ObjectRef]
    surfaced_playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    first_activated_at: _timestamp_pb2.Timestamp
    last_seen_at: _timestamp_pb2.Timestamp
    cleared_at: _timestamp_pb2.Timestamp
    relation: str
    activations: int
    def __init__(self, detector_id: _Optional[str] = ..., detector_class: _Optional[str] = ..., owner: _Optional[str] = ..., rule_id: _Optional[str] = ..., fingerprint: _Optional[str] = ..., parameter_bindings: _Optional[_Mapping[str, _common_pb2.ObjectRef]] = ..., surfaced_playbook_ids: _Optional[_Iterable[str]] = ..., first_activated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_seen_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cleared_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., relation: _Optional[str] = ..., activations: _Optional[int] = ...) -> None: ...

class IncidentClosure(_message.Message):
    __slots__ = ("request", "result", "dispatch_error", "final_detector_states", "incident_detector_states", "detector_timeline", "incident_detector_fired_before_dispatch", "incident_detector_fired_after_dispatch", "no_incident_detector_fired", "final_state_changes", "detected_at", "dispatched_at", "responder_completed_at", "verified_at", "health_cleared_at", "cleaned_helpers", "observation_generation", "partial_detector_states", "verified_while_responder_running", "closure_stability_window", "unconfirmed_reason", "unconfirmed_detail", "source_check_notes", "state_persistence_failure")
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    DISPATCH_ERROR_FIELD_NUMBER: _ClassVar[int]
    FINAL_DETECTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_DETECTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_TIMELINE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_DETECTOR_FIRED_BEFORE_DISPATCH_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_DETECTOR_FIRED_AFTER_DISPATCH_FIELD_NUMBER: _ClassVar[int]
    NO_INCIDENT_DETECTOR_FIRED_FIELD_NUMBER: _ClassVar[int]
    FINAL_STATE_CHANGES_FIELD_NUMBER: _ClassVar[int]
    DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    DISPATCHED_AT_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    HEALTH_CLEARED_AT_FIELD_NUMBER: _ClassVar[int]
    CLEANED_HELPERS_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_GENERATION_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_DETECTOR_STATES_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_WHILE_RESPONDER_RUNNING_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_STABILITY_WINDOW_FIELD_NUMBER: _ClassVar[int]
    UNCONFIRMED_REASON_FIELD_NUMBER: _ClassVar[int]
    UNCONFIRMED_DETAIL_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CHECK_NOTES_FIELD_NUMBER: _ClassVar[int]
    STATE_PERSISTENCE_FAILURE_FIELD_NUMBER: _ClassVar[int]
    request: IncidentRequest
    result: IncidentResult
    dispatch_error: str
    final_detector_states: _containers.RepeatedCompositeFieldContainer[DetectorEvaluation]
    incident_detector_states: _containers.RepeatedCompositeFieldContainer[DetectorEvaluation]
    detector_timeline: _containers.RepeatedCompositeFieldContainer[DetectorTimelineEntry]
    incident_detector_fired_before_dispatch: bool
    incident_detector_fired_after_dispatch: bool
    no_incident_detector_fired: bool
    final_state_changes: StateChanges
    detected_at: _timestamp_pb2.Timestamp
    dispatched_at: _timestamp_pb2.Timestamp
    responder_completed_at: _timestamp_pb2.Timestamp
    verified_at: _timestamp_pb2.Timestamp
    health_cleared_at: _timestamp_pb2.Timestamp
    cleaned_helpers: _containers.RepeatedScalarFieldContainer[str]
    observation_generation: str
    partial_detector_states: _containers.RepeatedCompositeFieldContainer[DetectorEvaluation]
    verified_while_responder_running: bool
    closure_stability_window: _duration_pb2.Duration
    unconfirmed_reason: ClosureUnconfirmedReason
    unconfirmed_detail: str
    source_check_notes: _containers.RepeatedScalarFieldContainer[str]
    state_persistence_failure: StatePersistenceFailure
    def __init__(self, request: _Optional[_Union[IncidentRequest, _Mapping]] = ..., result: _Optional[_Union[IncidentResult, _Mapping]] = ..., dispatch_error: _Optional[str] = ..., final_detector_states: _Optional[_Iterable[_Union[DetectorEvaluation, _Mapping]]] = ..., incident_detector_states: _Optional[_Iterable[_Union[DetectorEvaluation, _Mapping]]] = ..., detector_timeline: _Optional[_Iterable[_Union[DetectorTimelineEntry, _Mapping]]] = ..., incident_detector_fired_before_dispatch: _Optional[bool] = ..., incident_detector_fired_after_dispatch: _Optional[bool] = ..., no_incident_detector_fired: _Optional[bool] = ..., final_state_changes: _Optional[_Union[StateChanges, _Mapping]] = ..., detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., dispatched_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., responder_completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., verified_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., health_cleared_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cleaned_helpers: _Optional[_Iterable[str]] = ..., observation_generation: _Optional[str] = ..., partial_detector_states: _Optional[_Iterable[_Union[DetectorEvaluation, _Mapping]]] = ..., verified_while_responder_running: _Optional[bool] = ..., closure_stability_window: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., unconfirmed_reason: _Optional[_Union[ClosureUnconfirmedReason, str]] = ..., unconfirmed_detail: _Optional[str] = ..., source_check_notes: _Optional[_Iterable[str]] = ..., state_persistence_failure: _Optional[_Union[StatePersistenceFailure, _Mapping]] = ...) -> None: ...

class StageReport(_message.Message):
    __slots__ = ("stage", "mitigations", "remaining_finding_fingerprints", "error", "cleared_finding_fingerprints")
    STAGE_FIELD_NUMBER: _ClassVar[int]
    MITIGATIONS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    CLEARED_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    stage: str
    mitigations: _containers.RepeatedCompositeFieldContainer[StageMitigation]
    remaining_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    error: str
    cleared_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, stage: _Optional[str] = ..., mitigations: _Optional[_Iterable[_Union[StageMitigation, _Mapping]]] = ..., remaining_finding_fingerprints: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., cleared_finding_fingerprints: _Optional[_Iterable[str]] = ...) -> None: ...

class StageMitigation(_message.Message):
    __slots__ = ("playbook_id", "program_revision", "bindings", "plan_id", "apply_receipt", "result", "skip_reason", "post_apply_diagnose", "cleared_finding_fingerprints", "remaining_finding_fingerprints", "error", "plan_action_start", "plan_action_count")
    PLAYBOOK_ID_FIELD_NUMBER: _ClassVar[int]
    PROGRAM_REVISION_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    APPLY_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    SKIP_REASON_FIELD_NUMBER: _ClassVar[int]
    POST_APPLY_DIAGNOSE_FIELD_NUMBER: _ClassVar[int]
    CLEARED_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FINDING_FINGERPRINTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PLAN_ACTION_START_FIELD_NUMBER: _ClassVar[int]
    PLAN_ACTION_COUNT_FIELD_NUMBER: _ClassVar[int]
    playbook_id: str
    program_revision: str
    bindings: _containers.RepeatedCompositeFieldContainer[_program_pb2.Binding]
    plan_id: str
    apply_receipt: _program_pb2.PlanApplication
    result: str
    skip_reason: str
    post_apply_diagnose: _program_pb2.ProgramRun
    cleared_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    remaining_finding_fingerprints: _containers.RepeatedScalarFieldContainer[str]
    error: str
    plan_action_start: int
    plan_action_count: int
    def __init__(self, playbook_id: _Optional[str] = ..., program_revision: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[_program_pb2.Binding, _Mapping]]] = ..., plan_id: _Optional[str] = ..., apply_receipt: _Optional[_Union[_program_pb2.PlanApplication, _Mapping]] = ..., result: _Optional[str] = ..., skip_reason: _Optional[str] = ..., post_apply_diagnose: _Optional[_Union[_program_pb2.ProgramRun, _Mapping]] = ..., cleared_finding_fingerprints: _Optional[_Iterable[str]] = ..., remaining_finding_fingerprints: _Optional[_Iterable[str]] = ..., error: _Optional[str] = ..., plan_action_start: _Optional[int] = ..., plan_action_count: _Optional[int] = ...) -> None: ...

class DriftReprompt(_message.Message):
    __slots__ = ("findings_before", "findings_after")
    FINDINGS_BEFORE_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_AFTER_FIELD_NUMBER: _ClassVar[int]
    findings_before: int
    findings_after: int
    def __init__(self, findings_before: _Optional[int] = ..., findings_after: _Optional[int] = ...) -> None: ...

class StatePersistenceFailure(_message.Message):
    __slots__ = ("reason", "detail")
    REASON_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    reason: str
    detail: str
    def __init__(self, reason: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...
