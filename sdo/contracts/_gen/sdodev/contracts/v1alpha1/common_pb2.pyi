from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor
CONTROLLER_OWNED_FIELD_NUMBER: _ClassVar[int]
controller_owned: _descriptor.FieldDescriptor

class ObjectRef(_message.Message):
    __slots__ = ("api_version", "kind", "namespace", "name")
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_version: str
    kind: str
    namespace: str
    name: str
    def __init__(self, api_version: _Optional[str] = ..., kind: _Optional[str] = ..., namespace: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...
