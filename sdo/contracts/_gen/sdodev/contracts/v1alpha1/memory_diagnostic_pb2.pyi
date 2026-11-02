import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MemoryDiagnostic(_message.Message):
    __slots__ = ("rule", "path", "line", "message", "hint", "memory_conflict")
    RULE_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    LINE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    HINT_FIELD_NUMBER: _ClassVar[int]
    MEMORY_CONFLICT_FIELD_NUMBER: _ClassVar[int]
    rule: str
    path: str
    line: int
    message: str
    hint: str
    memory_conflict: MemoryConflict
    def __init__(self, rule: _Optional[str] = ..., path: _Optional[str] = ..., line: _Optional[int] = ..., message: _Optional[str] = ..., hint: _Optional[str] = ..., memory_conflict: _Optional[_Union[MemoryConflict, _Mapping]] = ...) -> None: ...

class MemoryConflict(_message.Message):
    __slots__ = ("base_commit", "accepted_commit", "conflicting_paths", "accepted_changes", "validator_errors")
    BASE_COMMIT_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_COMMIT_FIELD_NUMBER: _ClassVar[int]
    CONFLICTING_PATHS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_CHANGES_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_ERRORS_FIELD_NUMBER: _ClassVar[int]
    base_commit: str
    accepted_commit: str
    conflicting_paths: _containers.RepeatedScalarFieldContainer[str]
    accepted_changes: _containers.RepeatedCompositeFieldContainer[AcceptedMemoryChange]
    validator_errors: _containers.RepeatedCompositeFieldContainer[MemoryDiagnostic]
    def __init__(self, base_commit: _Optional[str] = ..., accepted_commit: _Optional[str] = ..., conflicting_paths: _Optional[_Iterable[str]] = ..., accepted_changes: _Optional[_Iterable[_Union[AcceptedMemoryChange, _Mapping]]] = ..., validator_errors: _Optional[_Iterable[_Union[MemoryDiagnostic, _Mapping]]] = ...) -> None: ...

class AcceptedMemoryChange(_message.Message):
    __slots__ = ("commit", "incident_id", "paths")
    COMMIT_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    PATHS_FIELD_NUMBER: _ClassVar[int]
    commit: str
    incident_id: str
    paths: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, commit: _Optional[str] = ..., incident_id: _Optional[str] = ..., paths: _Optional[_Iterable[str]] = ...) -> None: ...

class ProposalRejection(_message.Message):
    __slots__ = ("attempt", "diagnostics", "rejected_at")
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    REJECTED_AT_FIELD_NUMBER: _ClassVar[int]
    attempt: int
    diagnostics: _containers.RepeatedCompositeFieldContainer[MemoryDiagnostic]
    rejected_at: _timestamp_pb2.Timestamp
    def __init__(self, attempt: _Optional[int] = ..., diagnostics: _Optional[_Iterable[_Union[MemoryDiagnostic, _Mapping]]] = ..., rejected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
