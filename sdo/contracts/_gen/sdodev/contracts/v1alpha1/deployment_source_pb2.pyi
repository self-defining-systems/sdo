from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import any_pb2 as _any_pb2
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
