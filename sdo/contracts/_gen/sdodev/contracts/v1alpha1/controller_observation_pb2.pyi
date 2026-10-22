from buf.validate import validate_pb2 as _validate_pb2
from sdodev.contracts.v1alpha1 import controller_state_pb2 as _controller_state_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ControllerObservationRequest(_message.Message):
    __slots__ = ("state", "known_incident_ids", "incident_id", "stop_on_review")
    STATE_FIELD_NUMBER: _ClassVar[int]
    KNOWN_INCIDENT_IDS_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    STOP_ON_REVIEW_FIELD_NUMBER: _ClassVar[int]
    state: _controller_state_pb2.ControllerState
    known_incident_ids: _containers.RepeatedScalarFieldContainer[str]
    incident_id: str
    stop_on_review: bool
    def __init__(self, state: _Optional[_Union[_controller_state_pb2.ControllerState, _Mapping]] = ..., known_incident_ids: _Optional[_Iterable[str]] = ..., incident_id: _Optional[str] = ..., stop_on_review: _Optional[bool] = ...) -> None: ...

class ControllerObservationFailure(_message.Message):
    __slots__ = ("code", "incident_id", "message")
    class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CODE_UNSPECIFIED: _ClassVar[ControllerObservationFailure.Code]
        CODE_INVALID_SNAPSHOT: _ClassVar[ControllerObservationFailure.Code]
        CODE_CLOSURE_FAILED: _ClassVar[ControllerObservationFailure.Code]
        CODE_DETECTOR_REVIEW_REQUIRED: _ClassVar[ControllerObservationFailure.Code]
    CODE_UNSPECIFIED: ControllerObservationFailure.Code
    CODE_INVALID_SNAPSHOT: ControllerObservationFailure.Code
    CODE_CLOSURE_FAILED: ControllerObservationFailure.Code
    CODE_DETECTOR_REVIEW_REQUIRED: ControllerObservationFailure.Code
    CODE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: ControllerObservationFailure.Code
    incident_id: str
    message: str
    def __init__(self, code: _Optional[_Union[ControllerObservationFailure.Code, str]] = ..., incident_id: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ControllerObservation(_message.Message):
    __slots__ = ("known_incident_ids", "new_incident_opened", "verified_incident_id", "verified_closure", "acknowledged", "open_incident_summary", "failure")
    KNOWN_INCIDENT_IDS_FIELD_NUMBER: _ClassVar[int]
    NEW_INCIDENT_OPENED_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_CLOSURE_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    OPEN_INCIDENT_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    known_incident_ids: _containers.RepeatedScalarFieldContainer[str]
    new_incident_opened: bool
    verified_incident_id: str
    verified_closure: _messages_pb2.IncidentClosure
    acknowledged: bool
    open_incident_summary: str
    failure: ControllerObservationFailure
    def __init__(self, known_incident_ids: _Optional[_Iterable[str]] = ..., new_incident_opened: _Optional[bool] = ..., verified_incident_id: _Optional[str] = ..., verified_closure: _Optional[_Union[_messages_pb2.IncidentClosure, _Mapping]] = ..., acknowledged: _Optional[bool] = ..., open_incident_summary: _Optional[str] = ..., failure: _Optional[_Union[ControllerObservationFailure, _Mapping]] = ...) -> None: ...
