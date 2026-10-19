from buf.validate import validate_pb2 as _validate_pb2
from sdodev.contracts.v1alpha1 import outcome_pb2 as _outcome_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MemoryDiagnostic(_message.Message):
    __slots__ = ("rule", "path", "line", "message", "hint")
    RULE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    HINT_FIELD_NUMBER: _ClassVar[int]
    rule: str
    path: str
    line: int
    message: str
    hint: str
    def __init__(self, rule: _Optional[str] = ..., path: _Optional[str] = ..., line: _Optional[int] = ..., message: _Optional[str] = ..., hint: _Optional[str] = ...) -> None: ...

class MemoryCheckReport(_message.Message):
    __slots__ = ("diagnostics", "root", "actor", "phase", "base")
    DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    ROOT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    BASE_FIELD_NUMBER: _ClassVar[int]
    diagnostics: _containers.RepeatedCompositeFieldContainer[MemoryDiagnostic]
    root: str
    actor: str
    phase: str
    base: str
    def __init__(self, diagnostics: _Optional[_Iterable[_Union[MemoryDiagnostic, _Mapping]]] = ..., root: _Optional[str] = ..., actor: _Optional[str] = ..., phase: _Optional[str] = ..., base: _Optional[str] = ...) -> None: ...

class MemoryArtifact(_message.Message):
    __slots__ = ("kind", "id", "path", "detector_class")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    kind: str
    id: str
    path: str
    detector_class: str
    def __init__(self, kind: _Optional[str] = ..., id: _Optional[str] = ..., path: _Optional[str] = ..., detector_class: _Optional[str] = ...) -> None: ...

class MemoryListing(_message.Message):
    __slots__ = ("artifacts",)
    ARTIFACTS_FIELD_NUMBER: _ClassVar[int]
    artifacts: _containers.RepeatedCompositeFieldContainer[MemoryArtifact]
    def __init__(self, artifacts: _Optional[_Iterable[_Union[MemoryArtifact, _Mapping]]] = ...) -> None: ...

class MemoryOutcomes(_message.Message):
    __slots__ = ("outcomes",)
    OUTCOMES_FIELD_NUMBER: _ClassVar[int]
    outcomes: _containers.RepeatedCompositeFieldContainer[_outcome_pb2.OutcomeRecord]
    def __init__(self, outcomes: _Optional[_Iterable[_Union[_outcome_pb2.OutcomeRecord, _Mapping]]] = ...) -> None: ...
