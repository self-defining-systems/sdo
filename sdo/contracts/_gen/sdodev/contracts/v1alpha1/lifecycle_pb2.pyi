from google.protobuf import any_pb2 as _any_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SourceDeploymentRequest(_message.Message):
    __slots__ = ("application", "namespace", "health_objective", "correction_feedback", "attempt", "registry_host")
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_OBJECTIVE_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_HOST_FIELD_NUMBER: _ClassVar[int]
    application: str
    namespace: str
    health_objective: str
    correction_feedback: str
    attempt: int
    registry_host: str
    def __init__(self, application: _Optional[str] = ..., namespace: _Optional[str] = ..., health_objective: _Optional[str] = ..., correction_feedback: _Optional[str] = ..., attempt: _Optional[int] = ..., registry_host: _Optional[str] = ...) -> None: ...

class SourceDeploymentResult(_message.Message):
    __slots__ = ("proposed", "summary", "agent_session_id", "plan")
    PROPOSED_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PLAN_FIELD_NUMBER: _ClassVar[int]
    proposed: bool
    summary: str
    agent_session_id: str
    plan: DeploymentPlan
    def __init__(self, proposed: _Optional[bool] = ..., summary: _Optional[str] = ..., agent_session_id: _Optional[str] = ..., plan: _Optional[_Union[DeploymentPlan, _Mapping]] = ...) -> None: ...

class DeploymentPlan(_message.Message):
    __slots__ = ("strategy_id", "strategy_config")
    STRATEGY_ID_FIELD_NUMBER: _ClassVar[int]
    STRATEGY_CONFIG_FIELD_NUMBER: _ClassVar[int]
    strategy_id: str
    strategy_config: _any_pb2.Any
    def __init__(self, strategy_id: _Optional[str] = ..., strategy_config: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class RawManifestStrategy(_message.Message):
    __slots__ = ("image_builds", "manifest_paths", "expected_workload_ids")
    IMAGE_BUILDS_FIELD_NUMBER: _ClassVar[int]
    MANIFEST_PATHS_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_WORKLOAD_IDS_FIELD_NUMBER: _ClassVar[int]
    image_builds: _containers.RepeatedCompositeFieldContainer[ImageBuild]
    manifest_paths: _containers.RepeatedScalarFieldContainer[str]
    expected_workload_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, image_builds: _Optional[_Iterable[_Union[ImageBuild, _Mapping]]] = ..., manifest_paths: _Optional[_Iterable[str]] = ..., expected_workload_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class ImageBuild(_message.Message):
    __slots__ = ("image", "context_path", "dockerfile_path")
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_PATH_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_PATH_FIELD_NUMBER: _ClassVar[int]
    image: str
    context_path: str
    dockerfile_path: str
    def __init__(self, image: _Optional[str] = ..., context_path: _Optional[str] = ..., dockerfile_path: _Optional[str] = ...) -> None: ...

class ArchitectureRequest(_message.Message):
    __slots__ = ("application", "accepted_source_commit", "correction_feedback", "attempt", "source_resources", "source_topology_fingerprint")
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TOPOLOGY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    application: str
    accepted_source_commit: str
    correction_feedback: str
    attempt: int
    source_resources: _containers.RepeatedCompositeFieldContainer[SourceResource]
    source_topology_fingerprint: str
    def __init__(self, application: _Optional[str] = ..., accepted_source_commit: _Optional[str] = ..., correction_feedback: _Optional[str] = ..., attempt: _Optional[int] = ..., source_resources: _Optional[_Iterable[_Union[SourceResource, _Mapping]]] = ..., source_topology_fingerprint: _Optional[str] = ...) -> None: ...

class ArchitectureResult(_message.Message):
    __slots__ = ("summary_markdown", "topology_fingerprint", "source_commit", "agent_session_id")
    SUMMARY_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    summary_markdown: str
    topology_fingerprint: str
    source_commit: str
    agent_session_id: str
    def __init__(self, summary_markdown: _Optional[str] = ..., topology_fingerprint: _Optional[str] = ..., source_commit: _Optional[str] = ..., agent_session_id: _Optional[str] = ...) -> None: ...

class HealthJudgeRequest(_message.Message):
    __slots__ = ("application", "namespace", "health_objective", "accepted_source_commit", "correction_feedback", "attempt", "architecture_summary_markdown", "source_resources", "source_topology_fingerprint")
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_OBJECTIVE_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ARCHITECTURE_SUMMARY_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TOPOLOGY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    application: str
    namespace: str
    health_objective: str
    accepted_source_commit: str
    correction_feedback: str
    attempt: int
    architecture_summary_markdown: str
    source_resources: _containers.RepeatedCompositeFieldContainer[SourceResource]
    source_topology_fingerprint: str
    def __init__(self, application: _Optional[str] = ..., namespace: _Optional[str] = ..., health_objective: _Optional[str] = ..., accepted_source_commit: _Optional[str] = ..., correction_feedback: _Optional[str] = ..., attempt: _Optional[int] = ..., architecture_summary_markdown: _Optional[str] = ..., source_resources: _Optional[_Iterable[_Union[SourceResource, _Mapping]]] = ..., source_topology_fingerprint: _Optional[str] = ...) -> None: ...

class SourceResource(_message.Message):
    __slots__ = ("kind", "name", "namespace", "source", "dependencies")
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DEPENDENCIES_FIELD_NUMBER: _ClassVar[int]
    kind: str
    name: str
    namespace: str
    source: str
    dependencies: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, kind: _Optional[str] = ..., name: _Optional[str] = ..., namespace: _Optional[str] = ..., source: _Optional[str] = ..., dependencies: _Optional[_Iterable[str]] = ...) -> None: ...

class HealthJudgeResult(_message.Message):
    __slots__ = ("authored", "summary", "agent_session_id", "objective_digest", "detector_ids", "artifact_proposal")
    AUTHORED_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECTIVE_DIGEST_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PROPOSAL_FIELD_NUMBER: _ClassVar[int]
    authored: bool
    summary: str
    agent_session_id: str
    objective_digest: str
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    artifact_proposal: _messages_pb2.OperationalArtifactProposal
    def __init__(self, authored: _Optional[bool] = ..., summary: _Optional[str] = ..., agent_session_id: _Optional[str] = ..., objective_digest: _Optional[str] = ..., detector_ids: _Optional[_Iterable[str]] = ..., artifact_proposal: _Optional[_Union[_messages_pb2.OperationalArtifactProposal, _Mapping]] = ...) -> None: ...
