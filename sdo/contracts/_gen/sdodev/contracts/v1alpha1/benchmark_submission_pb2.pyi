from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ConductorTerminalStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONDUCTOR_TERMINAL_STAGE_UNSPECIFIED: _ClassVar[ConductorTerminalStage]
    CONDUCTOR_TERMINAL_STAGE_DONE: _ClassVar[ConductorTerminalStage]
    CONDUCTOR_TERMINAL_STAGE_COMPLETED: _ClassVar[ConductorTerminalStage]
    CONDUCTOR_TERMINAL_STAGE_FINISHED: _ClassVar[ConductorTerminalStage]
    CONDUCTOR_TERMINAL_STAGE_AWAITING_CLEANUP: _ClassVar[ConductorTerminalStage]
CONDUCTOR_TERMINAL_STAGE_UNSPECIFIED: ConductorTerminalStage
CONDUCTOR_TERMINAL_STAGE_DONE: ConductorTerminalStage
CONDUCTOR_TERMINAL_STAGE_COMPLETED: ConductorTerminalStage
CONDUCTOR_TERMINAL_STAGE_FINISHED: ConductorTerminalStage
CONDUCTOR_TERMINAL_STAGE_AWAITING_CLEANUP: ConductorTerminalStage

class ConductorStatus(_message.Message):
    __slots__ = ("stage",)
    STAGE_FIELD_NUMBER: _ClassVar[int]
    stage: str
    def __init__(self, stage: _Optional[str] = ...) -> None: ...

class ConductorAcknowledgement(_message.Message):
    __slots__ = ("status", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    message: str
    def __init__(self, status: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ConductorSolution(_message.Message):
    __slots__ = ("solution",)
    SOLUTION_FIELD_NUMBER: _ClassVar[int]
    solution: str
    def __init__(self, solution: _Optional[str] = ...) -> None: ...

class SubmissionGuardRequest(_message.Message):
    __slots__ = ("phase", "status", "acknowledgement", "submission_sent")
    PHASE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGEMENT_FIELD_NUMBER: _ClassVar[int]
    SUBMISSION_SENT_FIELD_NUMBER: _ClassVar[int]
    phase: str
    status: ConductorStatus
    acknowledgement: ConductorAcknowledgement
    submission_sent: bool
    def __init__(self, phase: _Optional[str] = ..., status: _Optional[_Union[ConductorStatus, _Mapping]] = ..., acknowledgement: _Optional[_Union[ConductorAcknowledgement, _Mapping]] = ..., submission_sent: _Optional[bool] = ...) -> None: ...

class SubmissionPhaseStatus(_message.Message):
    __slots__ = ("status", "stage", "message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: str
    stage: str
    message: str
    def __init__(self, status: _Optional[str] = ..., stage: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class SubmissionBridgeResponse(_message.Message):
    __slots__ = ("diagnosis", "mitigation", "done")
    DIAGNOSIS_FIELD_NUMBER: _ClassVar[int]
    MITIGATION_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    diagnosis: SubmissionPhaseStatus
    mitigation: SubmissionPhaseStatus
    done: SubmissionPhaseStatus
    def __init__(self, diagnosis: _Optional[_Union[SubmissionPhaseStatus, _Mapping]] = ..., mitigation: _Optional[_Union[SubmissionPhaseStatus, _Mapping]] = ..., done: _Optional[_Union[SubmissionPhaseStatus, _Mapping]] = ...) -> None: ...

class SubmissionGuardFailure(_message.Message):
    __slots__ = ("code", "message")
    class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CODE_UNSPECIFIED: _ClassVar[SubmissionGuardFailure.Code]
        CODE_INVALID_WIRE: _ClassVar[SubmissionGuardFailure.Code]
        CODE_INVALID_PHASE: _ClassVar[SubmissionGuardFailure.Code]
        CODE_NOT_ACKNOWLEDGED: _ClassVar[SubmissionGuardFailure.Code]
    CODE_UNSPECIFIED: SubmissionGuardFailure.Code
    CODE_INVALID_WIRE: SubmissionGuardFailure.Code
    CODE_INVALID_PHASE: SubmissionGuardFailure.Code
    CODE_NOT_ACKNOWLEDGED: SubmissionGuardFailure.Code
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: SubmissionGuardFailure.Code
    message: str
    def __init__(self, code: _Optional[_Union[SubmissionGuardFailure.Code, str]] = ..., message: _Optional[str] = ...) -> None: ...

class SubmissionGuardDecision(_message.Message):
    __slots__ = ("action", "response", "failure")
    class Action(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ACTION_UNSPECIFIED: _ClassVar[SubmissionGuardDecision.Action]
        ACTION_WAIT: _ClassVar[SubmissionGuardDecision.Action]
        ACTION_POST: _ClassVar[SubmissionGuardDecision.Action]
        ACTION_RETURN: _ClassVar[SubmissionGuardDecision.Action]
    ACTION_UNSPECIFIED: SubmissionGuardDecision.Action
    ACTION_WAIT: SubmissionGuardDecision.Action
    ACTION_POST: SubmissionGuardDecision.Action
    ACTION_RETURN: SubmissionGuardDecision.Action
    ACTION_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    action: SubmissionGuardDecision.Action
    response: SubmissionBridgeResponse
    failure: SubmissionGuardFailure
    def __init__(self, action: _Optional[_Union[SubmissionGuardDecision.Action, str]] = ..., response: _Optional[_Union[SubmissionBridgeResponse, _Mapping]] = ..., failure: _Optional[_Union[SubmissionGuardFailure, _Mapping]] = ...) -> None: ...
