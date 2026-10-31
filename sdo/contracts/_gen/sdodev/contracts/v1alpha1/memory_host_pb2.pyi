import datetime

from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import common_pb2 as _common_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from sdodev.contracts.v1alpha1 import program_pb2 as _program_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReadyRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReadyResponse(_message.Message):
    __slots__ = ("memory_commit", "binary_digest", "mirror_sequence", "mirror_complete", "detectors", "programs", "mitigator_ids")
    MEMORY_COMMIT_FIELD_NUMBER: _ClassVar[int]
    BINARY_DIGEST_FIELD_NUMBER: _ClassVar[int]
    MIRROR_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    MIRROR_COMPLETE_FIELD_NUMBER: _ClassVar[int]
    DETECTORS_FIELD_NUMBER: _ClassVar[int]
    PROGRAMS_FIELD_NUMBER: _ClassVar[int]
    MITIGATOR_IDS_FIELD_NUMBER: _ClassVar[int]
    memory_commit: str
    binary_digest: str
    mirror_sequence: int
    mirror_complete: bool
    detectors: _containers.RepeatedCompositeFieldContainer[MemoryDetectorRegistration]
    programs: _containers.RepeatedCompositeFieldContainer[_program_pb2.ProgramSpec]
    mitigator_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, memory_commit: _Optional[str] = ..., binary_digest: _Optional[str] = ..., mirror_sequence: _Optional[int] = ..., mirror_complete: _Optional[bool] = ..., detectors: _Optional[_Iterable[_Union[MemoryDetectorRegistration, _Mapping]]] = ..., programs: _Optional[_Iterable[_Union[_program_pb2.ProgramSpec, _Mapping]]] = ..., mitigator_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class MemoryDetectorRegistration(_message.Message):
    __slots__ = ("id", "specification")
    ID_FIELD_NUMBER: _ClassVar[int]
    SPECIFICATION_FIELD_NUMBER: _ClassVar[int]
    id: str
    specification: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., specification: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class MirrorRequest(_message.Message):
    __slots__ = ("sequence", "reset_mirror", "namespace", "upserts", "deletes", "complete")
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    RESET_MIRROR_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    UPSERTS_FIELD_NUMBER: _ClassVar[int]
    DELETES_FIELD_NUMBER: _ClassVar[int]
    COMPLETE_FIELD_NUMBER: _ClassVar[int]
    sequence: int
    reset_mirror: bool
    namespace: str
    upserts: _containers.RepeatedCompositeFieldContainer[MemoryObject]
    deletes: _containers.RepeatedCompositeFieldContainer[_common_pb2.ObjectRef]
    complete: bool
    def __init__(self, sequence: _Optional[int] = ..., reset_mirror: _Optional[bool] = ..., namespace: _Optional[str] = ..., upserts: _Optional[_Iterable[_Union[MemoryObject, _Mapping]]] = ..., deletes: _Optional[_Iterable[_Union[_common_pb2.ObjectRef, _Mapping]]] = ..., complete: _Optional[bool] = ...) -> None: ...

class MemoryObject(_message.Message):
    __slots__ = ("ref", "object")
    REF_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ObjectRef
    object: _struct_pb2.Struct
    def __init__(self, ref: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ..., object: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class MirrorResponse(_message.Message):
    __slots__ = ("sequence",)
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    sequence: int
    def __init__(self, sequence: _Optional[int] = ...) -> None: ...

class MemoryObservation(_message.Message):
    __slots__ = ("snapshot", "observed_at", "traffic", "reachability", "metrics", "alerts", "usage", "logs")
    class TrafficEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Struct
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
    class MetricsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Struct
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
    class LogsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Struct
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_FIELD_NUMBER: _ClassVar[int]
    REACHABILITY_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    ALERTS_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    LOGS_FIELD_NUMBER: _ClassVar[int]
    snapshot: _struct_pb2.Struct
    observed_at: _timestamp_pb2.Timestamp
    traffic: _containers.MessageMap[str, _struct_pb2.Struct]
    reachability: _struct_pb2.Struct
    metrics: _containers.MessageMap[str, _struct_pb2.Struct]
    alerts: _struct_pb2.Struct
    usage: _struct_pb2.Struct
    logs: _containers.MessageMap[str, _struct_pb2.Struct]
    def __init__(self, snapshot: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., traffic: _Optional[_Mapping[str, _struct_pb2.Struct]] = ..., reachability: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., metrics: _Optional[_Mapping[str, _struct_pb2.Struct]] = ..., alerts: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., usage: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., logs: _Optional[_Mapping[str, _struct_pb2.Struct]] = ...) -> None: ...

class EvaluateRequest(_message.Message):
    __slots__ = ("detector_ids", "mirror_sequence", "observation")
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    MIRROR_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    mirror_sequence: int
    observation: MemoryObservation
    def __init__(self, detector_ids: _Optional[_Iterable[str]] = ..., mirror_sequence: _Optional[int] = ..., observation: _Optional[_Union[MemoryObservation, _Mapping]] = ...) -> None: ...

class MemoryDetectorResult(_message.Message):
    __slots__ = ("detector_id", "findings", "error")
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    FINDINGS_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    findings: _containers.RepeatedCompositeFieldContainer[_messages_pb2.Finding]
    error: str
    def __init__(self, detector_id: _Optional[str] = ..., findings: _Optional[_Iterable[_Union[_messages_pb2.Finding, _Mapping]]] = ..., error: _Optional[str] = ...) -> None: ...

class EvaluateResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[MemoryDetectorResult]
    def __init__(self, results: _Optional[_Iterable[_Union[MemoryDetectorResult, _Mapping]]] = ...) -> None: ...

class MemoryReadReceipt(_message.Message):
    __slots__ = ("operation", "arguments", "result", "error")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    ARGUMENTS_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    operation: str
    arguments: _struct_pb2.Value
    result: _struct_pb2.Value
    error: str
    def __init__(self, operation: _Optional[str] = ..., arguments: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ..., result: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class MemoryProgramInput(_message.Message):
    __slots__ = ("program_id", "bindings", "mirror_sequence", "observation", "reads")
    PROGRAM_ID_FIELD_NUMBER: _ClassVar[int]
    BINDINGS_FIELD_NUMBER: _ClassVar[int]
    MIRROR_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    OBSERVATION_FIELD_NUMBER: _ClassVar[int]
    READS_FIELD_NUMBER: _ClassVar[int]
    program_id: str
    bindings: _containers.RepeatedCompositeFieldContainer[_program_pb2.Binding]
    mirror_sequence: int
    observation: MemoryObservation
    reads: _containers.RepeatedCompositeFieldContainer[MemoryReadReceipt]
    def __init__(self, program_id: _Optional[str] = ..., bindings: _Optional[_Iterable[_Union[_program_pb2.Binding, _Mapping]]] = ..., mirror_sequence: _Optional[int] = ..., observation: _Optional[_Union[MemoryObservation, _Mapping]] = ..., reads: _Optional[_Iterable[_Union[MemoryReadReceipt, _Mapping]]] = ...) -> None: ...

class DiagnoseRequest(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: MemoryProgramInput
    def __init__(self, input: _Optional[_Union[MemoryProgramInput, _Mapping]] = ...) -> None: ...

class DiagnoseResponse(_message.Message):
    __slots__ = ("verdict", "error", "reads")
    VERDICT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    READS_FIELD_NUMBER: _ClassVar[int]
    verdict: _program_pb2.Verdict
    error: str
    reads: _containers.RepeatedCompositeFieldContainer[MemoryReadReceipt]
    def __init__(self, verdict: _Optional[_Union[_program_pb2.Verdict, _Mapping]] = ..., error: _Optional[str] = ..., reads: _Optional[_Iterable[_Union[MemoryReadReceipt, _Mapping]]] = ...) -> None: ...

class MitigateRequest(_message.Message):
    __slots__ = ("input",)
    INPUT_FIELD_NUMBER: _ClassVar[int]
    input: MemoryProgramInput
    def __init__(self, input: _Optional[_Union[MemoryProgramInput, _Mapping]] = ...) -> None: ...

class MitigateResponse(_message.Message):
    __slots__ = ("plan", "error", "reads")
    PLAN_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    READS_FIELD_NUMBER: _ClassVar[int]
    plan: _program_pb2.Plan
    error: str
    reads: _containers.RepeatedCompositeFieldContainer[MemoryReadReceipt]
    def __init__(self, plan: _Optional[_Union[_program_pb2.Plan, _Mapping]] = ..., error: _Optional[str] = ..., reads: _Optional[_Iterable[_Union[MemoryReadReceipt, _Mapping]]] = ...) -> None: ...
