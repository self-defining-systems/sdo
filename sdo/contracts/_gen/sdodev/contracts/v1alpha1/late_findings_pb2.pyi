import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LateFinding(_message.Message):
    __slots__ = ("detector_id", "detector_class", "rule_id", "fingerprint", "severity", "parameter_bindings", "playbook_ids", "activated_at", "still_active")
    class ParameterBindingsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _common_pb2.ObjectRef
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_common_pb2.ObjectRef, _Mapping]] = ...) -> None: ...
    DETECTOR_ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    PARAMETER_BINDINGS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    ACTIVATED_AT_FIELD_NUMBER: _ClassVar[int]
    STILL_ACTIVE_FIELD_NUMBER: _ClassVar[int]
    detector_id: str
    detector_class: str
    rule_id: str
    fingerprint: str
    severity: str
    parameter_bindings: _containers.MessageMap[str, _common_pb2.ObjectRef]
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    activated_at: _timestamp_pb2.Timestamp
    still_active: bool
    def __init__(self, detector_id: _Optional[str] = ..., detector_class: _Optional[str] = ..., rule_id: _Optional[str] = ..., fingerprint: _Optional[str] = ..., severity: _Optional[str] = ..., parameter_bindings: _Optional[_Mapping[str, _common_pb2.ObjectRef]] = ..., playbook_ids: _Optional[_Iterable[str]] = ..., activated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., still_active: _Optional[bool] = ...) -> None: ...

class LateFindingsReport(_message.Message):
    __slots__ = ("schema_version", "incident_id", "telemetry_available", "late_findings")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    TELEMETRY_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    LATE_FINDINGS_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    incident_id: str
    telemetry_available: bool
    late_findings: _containers.RepeatedCompositeFieldContainer[LateFinding]
    def __init__(self, schema_version: _Optional[str] = ..., incident_id: _Optional[str] = ..., telemetry_available: _Optional[bool] = ..., late_findings: _Optional[_Iterable[_Union[LateFinding, _Mapping]]] = ...) -> None: ...

class LateFindingsSnapshot(_message.Message):
    __slots__ = ("digest", "detector_ids", "playbook_ids")
    DIGEST_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    digest: str
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, digest: _Optional[str] = ..., detector_ids: _Optional[_Iterable[str]] = ..., playbook_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class LateFindingsHistory(_message.Message):
    __slots__ = ("snapshots",)
    SNAPSHOTS_FIELD_NUMBER: _ClassVar[int]
    snapshots: _containers.RepeatedCompositeFieldContainer[LateFindingsSnapshot]
    def __init__(self, snapshots: _Optional[_Iterable[_Union[LateFindingsSnapshot, _Mapping]]] = ...) -> None: ...

class LateFindingsReceipt(_message.Message):
    __slots__ = ("incident_id", "detector_ids", "playbook_ids", "report_digest")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    DETECTOR_IDS_FIELD_NUMBER: _ClassVar[int]
    PLAYBOOK_IDS_FIELD_NUMBER: _ClassVar[int]
    REPORT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    detector_ids: _containers.RepeatedScalarFieldContainer[str]
    playbook_ids: _containers.RepeatedScalarFieldContainer[str]
    report_digest: str
    def __init__(self, incident_id: _Optional[str] = ..., detector_ids: _Optional[_Iterable[str]] = ..., playbook_ids: _Optional[_Iterable[str]] = ..., report_digest: _Optional[str] = ...) -> None: ...
