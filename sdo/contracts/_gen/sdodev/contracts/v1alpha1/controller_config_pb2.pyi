import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from sdodev.contracts.v1alpha1 import deployment_source_pb2 as _deployment_source_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ControllerConfig(_message.Message):
    __slots__ = ("memory_host_binary", "memory_host_digest", "namespace", "control_namespace", "app_root", "application", "source_commit", "deployed_commit", "responder_image", "repository_pvc", "repository_mount_path", "repository_pvc_subpath", "responder_credentials_secret", "broker_worktree_root", "firing_telemetry_path", "duration", "identity", "exit_after_closure", "maintenance_configmap", "agent_registration_directory", "responder_agent_id", "diagnostics_fingerprint", "controller_artifact_digest", "controller_revision", "broker_mode", "worker_mount_mode", "drift_batch_window", "toolchain_image", "closure_stability_window", "self_heal_hold", "script_image", "memory_upstream", "code_path", "render_environment", "deployment_policy", "deploy_identity", "telemetry_environment_file", "settings_path")
    MEMORY_HOST_BINARY_FIELD_NUMBER: _ClassVar[int]
    MEMORY_HOST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    CONTROL_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    APP_ROOT_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    DEPLOYED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_IMAGE_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_PVC_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_MOUNT_PATH_FIELD_NUMBER: _ClassVar[int]
    REPOSITORY_PVC_SUBPATH_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_CREDENTIALS_SECRET_FIELD_NUMBER: _ClassVar[int]
    BROKER_WORKTREE_ROOT_FIELD_NUMBER: _ClassVar[int]
    FIRING_TELEMETRY_PATH_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    EXIT_AFTER_CLOSURE_FIELD_NUMBER: _ClassVar[int]
    MAINTENANCE_CONFIGMAP_FIELD_NUMBER: _ClassVar[int]
    AGENT_REGISTRATION_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_AGENT_ID_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTICS_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_ARTIFACT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_REVISION_FIELD_NUMBER: _ClassVar[int]
    BROKER_MODE_FIELD_NUMBER: _ClassVar[int]
    WORKER_MOUNT_MODE_FIELD_NUMBER: _ClassVar[int]
    DRIFT_BATCH_WINDOW_FIELD_NUMBER: _ClassVar[int]
    TOOLCHAIN_IMAGE_FIELD_NUMBER: _ClassVar[int]
    CLOSURE_STABILITY_WINDOW_FIELD_NUMBER: _ClassVar[int]
    SELF_HEAL_HOLD_FIELD_NUMBER: _ClassVar[int]
    SCRIPT_IMAGE_FIELD_NUMBER: _ClassVar[int]
    MEMORY_UPSTREAM_FIELD_NUMBER: _ClassVar[int]
    CODE_PATH_FIELD_NUMBER: _ClassVar[int]
    RENDER_ENVIRONMENT_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_POLICY_FIELD_NUMBER: _ClassVar[int]
    DEPLOY_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    TELEMETRY_ENVIRONMENT_FILE_FIELD_NUMBER: _ClassVar[int]
    SETTINGS_PATH_FIELD_NUMBER: _ClassVar[int]
    memory_host_binary: str
    memory_host_digest: str
    namespace: str
    control_namespace: str
    app_root: str
    application: str
    source_commit: str
    deployed_commit: str
    responder_image: str
    repository_pvc: str
    repository_mount_path: str
    repository_pvc_subpath: str
    responder_credentials_secret: str
    broker_worktree_root: str
    firing_telemetry_path: str
    duration: _duration_pb2.Duration
    identity: str
    exit_after_closure: bool
    maintenance_configmap: str
    agent_registration_directory: str
    responder_agent_id: str
    diagnostics_fingerprint: str
    controller_artifact_digest: str
    controller_revision: str
    broker_mode: str
    worker_mount_mode: str
    drift_batch_window: _duration_pb2.Duration
    toolchain_image: str
    closure_stability_window: _duration_pb2.Duration
    self_heal_hold: _duration_pb2.Duration
    script_image: str
    memory_upstream: MemoryUpstream
    code_path: CodePathConfig
    render_environment: _deployment_source_pb2.RenderEnvironment
    deployment_policy: _deployment_source_pb2.DeploymentPolicy
    deploy_identity: DeployIdentity
    telemetry_environment_file: str
    settings_path: str
    def __init__(self, memory_host_binary: _Optional[str] = ..., memory_host_digest: _Optional[str] = ..., namespace: _Optional[str] = ..., control_namespace: _Optional[str] = ..., app_root: _Optional[str] = ..., application: _Optional[str] = ..., source_commit: _Optional[str] = ..., deployed_commit: _Optional[str] = ..., responder_image: _Optional[str] = ..., repository_pvc: _Optional[str] = ..., repository_mount_path: _Optional[str] = ..., repository_pvc_subpath: _Optional[str] = ..., responder_credentials_secret: _Optional[str] = ..., broker_worktree_root: _Optional[str] = ..., firing_telemetry_path: _Optional[str] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., identity: _Optional[str] = ..., exit_after_closure: _Optional[bool] = ..., maintenance_configmap: _Optional[str] = ..., agent_registration_directory: _Optional[str] = ..., responder_agent_id: _Optional[str] = ..., diagnostics_fingerprint: _Optional[str] = ..., controller_artifact_digest: _Optional[str] = ..., controller_revision: _Optional[str] = ..., broker_mode: _Optional[str] = ..., worker_mount_mode: _Optional[str] = ..., drift_batch_window: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., toolchain_image: _Optional[str] = ..., closure_stability_window: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., self_heal_hold: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., script_image: _Optional[str] = ..., memory_upstream: _Optional[_Union[MemoryUpstream, _Mapping]] = ..., code_path: _Optional[_Union[CodePathConfig, _Mapping]] = ..., render_environment: _Optional[_Union[_deployment_source_pb2.RenderEnvironment, _Mapping]] = ..., deployment_policy: _Optional[_Union[_deployment_source_pb2.DeploymentPolicy, _Mapping]] = ..., deploy_identity: _Optional[_Union[DeployIdentity, _Mapping]] = ..., telemetry_environment_file: _Optional[str] = ..., settings_path: _Optional[str] = ...) -> None: ...

class DeployIdentity(_message.Message):
    __slots__ = ("buildkit_image", "registry_push_secret", "registry_host", "registry_insecure", "build_timeout", "build_budget", "rollout_timeout")
    BUILDKIT_IMAGE_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_PUSH_SECRET_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_HOST_FIELD_NUMBER: _ClassVar[int]
    REGISTRY_INSECURE_FIELD_NUMBER: _ClassVar[int]
    BUILD_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    BUILD_BUDGET_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    buildkit_image: str
    registry_push_secret: str
    registry_host: str
    registry_insecure: bool
    build_timeout: _duration_pb2.Duration
    build_budget: _duration_pb2.Duration
    rollout_timeout: _duration_pb2.Duration
    def __init__(self, buildkit_image: _Optional[str] = ..., registry_push_secret: _Optional[str] = ..., registry_host: _Optional[str] = ..., registry_insecure: _Optional[bool] = ..., build_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., build_budget: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., rollout_timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class MemoryUpstream(_message.Message):
    __slots__ = ("url", "branch", "ssh_dir")
    URL_FIELD_NUMBER: _ClassVar[int]
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    SSH_DIR_FIELD_NUMBER: _ClassVar[int]
    url: str
    branch: str
    ssh_dir: str
    def __init__(self, url: _Optional[str] = ..., branch: _Optional[str] = ..., ssh_dir: _Optional[str] = ...) -> None: ...

class CodePathConfig(_message.Message):
    __slots__ = ("name", "overrides")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    name: str
    overrides: CodePathOverrides
    def __init__(self, name: _Optional[str] = ..., overrides: _Optional[_Union[CodePathOverrides, _Mapping]] = ...) -> None: ...

class CodePathOverrides(_message.Message):
    __slots__ = ("detector_source", "coverage", "response_stages", "memory_validation", "activation", "agent_guidance")
    DETECTOR_SOURCE_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_STAGES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_VALIDATION_FIELD_NUMBER: _ClassVar[int]
    ACTIVATION_FIELD_NUMBER: _ClassVar[int]
    AGENT_GUIDANCE_FIELD_NUMBER: _ClassVar[int]
    detector_source: str
    coverage: str
    response_stages: _containers.RepeatedScalarFieldContainer[str]
    memory_validation: str
    activation: str
    agent_guidance: str
    def __init__(self, detector_source: _Optional[str] = ..., coverage: _Optional[str] = ..., response_stages: _Optional[_Iterable[str]] = ..., memory_validation: _Optional[str] = ..., activation: _Optional[str] = ..., agent_guidance: _Optional[str] = ...) -> None: ...
