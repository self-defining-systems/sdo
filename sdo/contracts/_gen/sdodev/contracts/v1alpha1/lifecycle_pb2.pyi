from buf.validate import validate_pb2 as _validate_pb2
from sdodev.contracts.v1alpha1 import deployment_source_pb2 as _deployment_source_pb2
from sdodev.contracts.v1alpha1 import memory_diagnostic_pb2 as _memory_diagnostic_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SourceDeploymentRequest(_message.Message):
    __slots__ = ("application", "namespace", "health_objective", "correction_feedback", "attempt", "registry_host", "renderers", "image_sources")
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_OBJECTIVE_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_HOST_FIELD_NUMBER: _ClassVar[int]
    RENDERERS_FIELD_NUMBER: _ClassVar[int]
    IMAGE_SOURCES_FIELD_NUMBER: _ClassVar[int]
    application: str
    namespace: str
    health_objective: str
    correction_feedback: str
    attempt: int
    registry_host: str
    renderers: _containers.RepeatedCompositeFieldContainer[_deployment_source_pb2.PluginDescriptor]
    image_sources: _containers.RepeatedCompositeFieldContainer[_deployment_source_pb2.PluginDescriptor]
    def __init__(self, application: _Optional[str] = ..., namespace: _Optional[str] = ..., health_objective: _Optional[str] = ..., correction_feedback: _Optional[str] = ..., attempt: _Optional[int] = ..., registry_host: _Optional[str] = ..., renderers: _Optional[_Iterable[_Union[_deployment_source_pb2.PluginDescriptor, _Mapping]]] = ..., image_sources: _Optional[_Iterable[_Union[_deployment_source_pb2.PluginDescriptor, _Mapping]]] = ...) -> None: ...

class SourceDeploymentResult(_message.Message):
    __slots__ = ("proposed", "summary", "agent_session_id")
    PROPOSED_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    proposed: bool
    summary: str
    agent_session_id: str
    def __init__(self, proposed: _Optional[bool] = ..., summary: _Optional[str] = ..., agent_session_id: _Optional[str] = ...) -> None: ...

class DeploymentPlan(_message.Message):
    __slots__ = ("source", "images")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    IMAGES_FIELD_NUMBER: _ClassVar[int]
    source: _deployment_source_pb2.DeploymentSource
    images: _containers.RepeatedCompositeFieldContainer[_deployment_source_pb2.ImageSource]
    def __init__(self, source: _Optional[_Union[_deployment_source_pb2.DeploymentSource, _Mapping]] = ..., images: _Optional[_Iterable[_Union[_deployment_source_pb2.ImageSource, _Mapping]]] = ...) -> None: ...

class ImageBuild(_message.Message):
    __slots__ = ("image", "context_path", "dockerfile_path")
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_PATH_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_PATH_FIELD_NUMBER: _ClassVar[int]
    image: str
    context_path: str
    dockerfile_path: str
    def __init__(self, image: _Optional[str] = ..., context_path: _Optional[str] = ..., dockerfile_path: _Optional[str] = ...) -> None: ...

class HealthJudgeRequest(_message.Message):
    __slots__ = ("telemetry", "application", "namespace", "health_objective", "accepted_source_commit", "correction_feedback", "attempt", "architecture_summary_markdown", "source_resources", "validation_attempt", "previous_diagnostics")
    TELEMETRY_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    HEALTH_OBJECTIVE_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    CORRECTION_FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ARCHITECTURE_SUMMARY_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    VALIDATION_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    telemetry: _messages_pb2.TelemetryContext
    application: str
    namespace: str
    health_objective: str
    accepted_source_commit: str
    correction_feedback: str
    attempt: int
    architecture_summary_markdown: str
    source_resources: _containers.RepeatedCompositeFieldContainer[SourceResource]
    validation_attempt: int
    previous_diagnostics: _containers.RepeatedCompositeFieldContainer[_memory_diagnostic_pb2.MemoryDiagnostic]
    def __init__(self, telemetry: _Optional[_Union[_messages_pb2.TelemetryContext, _Mapping]] = ..., application: _Optional[str] = ..., namespace: _Optional[str] = ..., health_objective: _Optional[str] = ..., accepted_source_commit: _Optional[str] = ..., correction_feedback: _Optional[str] = ..., attempt: _Optional[int] = ..., architecture_summary_markdown: _Optional[str] = ..., source_resources: _Optional[_Iterable[_Union[SourceResource, _Mapping]]] = ..., validation_attempt: _Optional[int] = ..., previous_diagnostics: _Optional[_Iterable[_Union[_memory_diagnostic_pb2.MemoryDiagnostic, _Mapping]]] = ...) -> None: ...

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
    __slots__ = ("authored", "summary", "agent_session_id", "detector_ids", "artifact_proposal")
    AUTHORED_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    AGENT_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PROPOSAL_FIELD_NUMBER: _ClassVar[int]
    authored: bool
    summary: str
    agent_session_id: str
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    artifact_proposal: _messages_pb2.OperationalArtifactProposal
    def __init__(self, authored: _Optional[bool] = ..., summary: _Optional[str] = ..., agent_session_id: _Optional[str] = ..., detector_ids: _Optional[_Iterable[str]] = ..., artifact_proposal: _Optional[_Union[_messages_pb2.OperationalArtifactProposal, _Mapping]] = ...) -> None: ...
