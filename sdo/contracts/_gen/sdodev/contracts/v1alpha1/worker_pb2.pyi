import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import memory_pb2 as _memory_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from sdodev.contracts.v1alpha1 import outcome_pb2 as _outcome_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AgentRegistration(_message.Message):
    __slots__ = ("schema_version", "agent_id", "capabilities", "permissions", "kubernetes_job", "local_process")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    KUBERNETES_JOB_FIELD_NUMBER: _ClassVar[int]
    LOCAL_PROCESS_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    agent_id: str
    capabilities: _containers.RepeatedCompositeFieldContainer[AgentCapability]
    permissions: _containers.RepeatedScalarFieldContainer[str]
    kubernetes_job: KubernetesJobLaunch
    local_process: LocalProcessLaunch
    def __init__(self, schema_version: _Optional[str] = ..., agent_id: _Optional[str] = ..., capabilities: _Optional[_Iterable[_Union[AgentCapability, _Mapping]]] = ..., permissions: _Optional[_Iterable[str]] = ..., kubernetes_job: _Optional[_Union[KubernetesJobLaunch, _Mapping]] = ..., local_process: _Optional[_Union[LocalProcessLaunch, _Mapping]] = ...) -> None: ...

class AgentCapability(_message.Message):
    __slots__ = ("task_type", "request_type_url", "result_type_url")
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_TYPE_URL_FIELD_NUMBER: _ClassVar[int]
    RESULT_TYPE_URL_FIELD_NUMBER: _ClassVar[int]
    task_type: str
    request_type_url: str
    result_type_url: str
    def __init__(self, task_type: _Optional[str] = ..., request_type_url: _Optional[str] = ..., result_type_url: _Optional[str] = ...) -> None: ...

class KubernetesJobLaunch(_message.Message):
    __slots__ = ("image", "command", "environment_names")
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_NAMES_FIELD_NUMBER: _ClassVar[int]
    image: str
    command: _containers.RepeatedScalarFieldContainer[str]
    environment_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, image: _Optional[str] = ..., command: _Optional[_Iterable[str]] = ..., environment_names: _Optional[_Iterable[str]] = ...) -> None: ...

class LocalProcessLaunch(_message.Message):
    __slots__ = ("command", "environment_names")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    ENVIRONMENT_NAMES_FIELD_NUMBER: _ClassVar[int]
    command: _containers.RepeatedScalarFieldContainer[str]
    environment_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, command: _Optional[_Iterable[str]] = ..., environment_names: _Optional[_Iterable[str]] = ...) -> None: ...

class ProposalWorkspace(_message.Message):
    __slots__ = ("proposal_id", "checkout_path", "base_commit")
    PROPOSAL_ID_FIELD_NUMBER: _ClassVar[int]
    CHECKOUT_PATH_FIELD_NUMBER: _ClassVar[int]
    BASE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    proposal_id: str
    checkout_path: str
    base_commit: str
    def __init__(self, proposal_id: _Optional[str] = ..., checkout_path: _Optional[str] = ..., base_commit: _Optional[str] = ...) -> None: ...

class AgentTask(_message.Message):
    __slots__ = ("schema_version", "task_id", "agent_id", "task_type", "request", "workspace", "deadline")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    WORKSPACE_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    task_id: str
    agent_id: str
    task_type: str
    request: _any_pb2.Any
    workspace: ProposalWorkspace
    deadline: _timestamp_pb2.Timestamp
    def __init__(self, schema_version: _Optional[str] = ..., task_id: _Optional[str] = ..., agent_id: _Optional[str] = ..., task_type: _Optional[str] = ..., request: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., workspace: _Optional[_Union[ProposalWorkspace, _Mapping]] = ..., deadline: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class AgentResult(_message.Message):
    __slots__ = ("schema_version", "task_id", "status", "response", "proposal_id", "error_summary")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    task_id: str
    status: str
    response: _any_pb2.Any
    proposal_id: str
    error_summary: str
    def __init__(self, schema_version: _Optional[str] = ..., task_id: _Optional[str] = ..., status: _Optional[str] = ..., response: _Optional[_Union[_any_pb2.Any, _Mapping]] = ..., proposal_id: _Optional[str] = ..., error_summary: _Optional[str] = ...) -> None: ...

class ReflectionRequest(_message.Message):
    __slots__ = ("closure", "proposal_commit", "outcome_commit", "memory_snapshot", "session_mode", "guidance", "outcome", "prior_outcomes", "validation_attempt", "previous_diagnostics")
    CLOSURE_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_COMMIT_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_COMMIT_FIELD_NUMBER: _ClassVar[int]
    MEMORY_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    SESSION_MODE_FIELD_NUMBER: _ClassVar[int]
    GUIDANCE_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    PRIOR_OUTCOMES_FIELD_NUMBER: _ClassVar[int]
    VALIDATION_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    closure: _messages_pb2.IncidentClosure
    proposal_commit: str
    outcome_commit: str
    memory_snapshot: _messages_pb2.OperationalMemorySnapshot
    session_mode: str
    guidance: str
    outcome: _outcome_pb2.OutcomeRecord
    prior_outcomes: _containers.RepeatedCompositeFieldContainer[_outcome_pb2.OutcomeRecord]
    validation_attempt: int
    previous_diagnostics: _containers.RepeatedCompositeFieldContainer[_memory_pb2.MemoryDiagnostic]
    def __init__(self, closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., proposal_commit: _Optional[str] = ..., outcome_commit: _Optional[str] = ..., memory_snapshot: _Optional[_Union[_messages_pb2.OperationalMemorySnapshot, _Mapping]] = ..., session_mode: _Optional[str] = ..., guidance: _Optional[str] = ..., outcome: _Optional[_Union[_outcome_pb2.OutcomeRecord, _Mapping]] = ..., prior_outcomes: _Optional[_Iterable[_Union[_outcome_pb2.OutcomeRecord, _Mapping]]] = ..., validation_attempt: _Optional[int] = ..., previous_diagnostics: _Optional[_Iterable[_Union[_memory_pb2.MemoryDiagnostic, _Mapping]]] = ...) -> None: ...

class ReflectionResult(_message.Message):
    __slots__ = ("summary", "learning_decision", "no_change_reason", "proposed_artifact_ids", "artifact_proposal", "sdk_gaps")
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    LEARNING_DECISION_FIELD_NUMBER: _ClassVar[int]
    NO_CHANGE_REASON_FIELD_NUMBER: _ClassVar[int]
    PROPOSED_ARTIFACT_IDS_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PROPOSAL_FIELD_NUMBER: _ClassVar[int]
    SDK_GAPS_FIELD_NUMBER: _ClassVar[int]
    summary: str
    learning_decision: str
    no_change_reason: str
    proposed_artifact_ids: _containers.RepeatedScalarFieldContainer[str]
    artifact_proposal: _messages_pb2.OperationalArtifactProposal
    sdk_gaps: _containers.RepeatedCompositeFieldContainer[SdkGap]
    def __init__(self, summary: _Optional[str] = ..., learning_decision: _Optional[str] = ..., no_change_reason: _Optional[str] = ..., proposed_artifact_ids: _Optional[_Iterable[str]] = ..., artifact_proposal: _Optional[_Union[_messages_pb2.OperationalArtifactProposal, _Mapping]] = ..., sdk_gaps: _Optional[_Iterable[_Union[SdkGap, _Mapping]]] = ...) -> None: ...

class SdkGap(_message.Message):
    __slots__ = ("fault_class", "primitive", "reason", "scratch_script")
    FAULT_CLASS_FIELD_NUMBER: _ClassVar[int]
    PRIMITIVE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SCRATCH_SCRIPT_FIELD_NUMBER: _ClassVar[int]
    fault_class: str
    primitive: str
    reason: str
    scratch_script: str
    def __init__(self, fault_class: _Optional[str] = ..., primitive: _Optional[str] = ..., reason: _Optional[str] = ..., scratch_script: _Optional[str] = ...) -> None: ...
