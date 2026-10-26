from buf.validate import validate_pb2 as _validate_pb2
from sdodev.contracts.v1alpha1 import outcome_pb2 as _outcome_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
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

class MitigationReplayNote(_message.Message):
    __slots__ = ("program_id", "case_id", "status", "effects")
    class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_UNSPECIFIED: _ClassVar[MitigationReplayNote.Status]
        STATUS_REPLAYED: _ClassVar[MitigationReplayNote.Status]
        STATUS_SKIPPED_UNREPLAYABLE_EFFECT: _ClassVar[MitigationReplayNote.Status]
        STATUS_OBSERVED_TRANSITION: _ClassVar[MitigationReplayNote.Status]
    STATUS_UNSPECIFIED: MitigationReplayNote.Status
    STATUS_REPLAYED: MitigationReplayNote.Status
    STATUS_SKIPPED_UNREPLAYABLE_EFFECT: MitigationReplayNote.Status
    STATUS_OBSERVED_TRANSITION: MitigationReplayNote.Status
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EFFECTS_FIELD_NUMBER: _ClassVar[int]
    program_id: str
    case_id: str
    status: MitigationReplayNote.Status
    effects: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, program_id: _Optional[str] = ..., case_id: _Optional[str] = ..., status: _Optional[_Union[MitigationReplayNote.Status, str]] = ..., effects: _Optional[_Iterable[str]] = ...) -> None: ...

class MemoryCheckReport(_message.Message):
    __slots__ = ("diagnostics", "root", "actor", "phase", "base", "mitigation_replay_notes")
    DIAGNOSTICS_FIELD_NUMBER: _ClassVar[int]
    ROOT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    BASE_FIELD_NUMBER: _ClassVar[int]
    MITIGATION_REPLAY_NOTES_FIELD_NUMBER: _ClassVar[int]
    diagnostics: _containers.RepeatedCompositeFieldContainer[MemoryDiagnostic]
    root: str
    actor: str
    phase: str
    base: str
    mitigation_replay_notes: _containers.RepeatedCompositeFieldContainer[MitigationReplayNote]
    def __init__(self, diagnostics: _Optional[_Iterable[_Union[MemoryDiagnostic, _Mapping]]] = ..., root: _Optional[str] = ..., actor: _Optional[str] = ..., phase: _Optional[str] = ..., base: _Optional[str] = ..., mitigation_replay_notes: _Optional[_Iterable[_Union[MitigationReplayNote, _Mapping]]] = ...) -> None: ...

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
