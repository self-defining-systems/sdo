import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeploymentSource(_message.Message):
    __slots__ = ("renderer_id", "config")
    RENDERER_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    renderer_id: str
    config: _any_pb2.Any
    def __init__(self, renderer_id: _Optional[str] = ..., config: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class ImageSource(_message.Message):
    __slots__ = ("image", "source_id", "config")
    IMAGE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    image: str
    source_id: str
    config: _any_pb2.Any
    def __init__(self, image: _Optional[str] = ..., source_id: _Optional[str] = ..., config: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class RawRenderConfig(_message.Message):
    __slots__ = ("paths",)
    PATHS_FIELD_NUMBER: _ClassVar[int]
    paths: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, paths: _Optional[_Iterable[str]] = ...) -> None: ...

class KustomizeRenderConfig(_message.Message):
    __slots__ = ("path",)
    PATH_FIELD_NUMBER: _ClassVar[int]
    path: str
    def __init__(self, path: _Optional[str] = ...) -> None: ...

class BuildKitImageConfig(_message.Message):
    __slots__ = ("context_path", "dockerfile_path")
    CONTEXT_PATH_FIELD_NUMBER: _ClassVar[int]
    DOCKERFILE_PATH_FIELD_NUMBER: _ClassVar[int]
    context_path: str
    dockerfile_path: str
    def __init__(self, context_path: _Optional[str] = ..., dockerfile_path: _Optional[str] = ...) -> None: ...

class PrebuiltImageConfig(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HelmRenderConfig(_message.Message):
    __slots__ = ("chart_path", "value_files", "values")
    CHART_PATH_FIELD_NUMBER: _ClassVar[int]
    VALUE_FILES_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    chart_path: str
    value_files: _containers.RepeatedScalarFieldContainer[str]
    values: _struct_pb2.Struct
    def __init__(self, chart_path: _Optional[str] = ..., value_files: _Optional[_Iterable[str]] = ..., values: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class GitPollConfig(_message.Message):
    __slots__ = ("repository", "ref", "poll_interval")
    REPOSITORY_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    POLL_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    repository: str
    ref: str
    poll_interval: _duration_pb2.Duration
    def __init__(self, repository: _Optional[str] = ..., ref: _Optional[str] = ..., poll_interval: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class PluginDescriptor(_message.Message):
    __slots__ = ("id", "config_type", "config_schema_json", "doc")
    ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONFIG_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    DOC_FIELD_NUMBER: _ClassVar[int]
    id: str
    config_type: str
    config_schema_json: str
    doc: str
    def __init__(self, id: _Optional[str] = ..., config_type: _Optional[str] = ..., config_schema_json: _Optional[str] = ..., doc: _Optional[str] = ...) -> None: ...

class SourcesFile(_message.Message):
    __slots__ = ("sources",)
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    sources: _containers.RepeatedCompositeFieldContainer[UpstreamSource]
    def __init__(self, sources: _Optional[_Iterable[_Union[UpstreamSource, _Mapping]]] = ...) -> None: ...

class UpstreamSource(_message.Message):
    __slots__ = ("path", "kind", "config")
    PATH_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    path: str
    kind: str
    config: _struct_pb2.Struct
    def __init__(self, path: _Optional[str] = ..., kind: _Optional[str] = ..., config: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
