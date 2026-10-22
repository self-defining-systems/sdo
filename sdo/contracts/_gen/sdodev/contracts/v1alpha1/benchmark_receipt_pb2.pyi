import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sdodev.contracts.v1alpha1 import controller_state_pb2 as _controller_state_pb2
from sdodev.contracts.v1alpha1 import messages_pb2 as _messages_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ResponderJobEvidence(_message.Message):
    __slots__ = ("incident_id", "job_name", "job_uid", "succeeded")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_NAME_FIELD_NUMBER: _ClassVar[int]
    JOB_UID_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    job_name: str
    job_uid: str
    succeeded: bool
    def __init__(self, incident_id: _Optional[str] = ..., job_name: _Optional[str] = ..., job_uid: _Optional[str] = ..., succeeded: _Optional[bool] = ...) -> None: ...

class NativeProductionReceipt(_message.Message):
    __slots__ = ("schema_version", "recorded_at", "broker_mode", "worker_mount_mode", "incident_id", "namespace", "control_namespace", "controller_workload", "controller_image", "responder_image", "validator_image", "controller_pod_uid", "native_ledger", "responder_job_evidence", "proposal_commit", "outcome_commit", "reflection_commit", "reflection_usage", "reflection_skipped_reason", "outcome_classification", "resolution")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    RECORDED_AT_FIELD_NUMBER: _ClassVar[int]
    BROKER_MODE_FIELD_NUMBER: _ClassVar[int]
    WORKER_MOUNT_MODE_FIELD_NUMBER: _ClassVar[int]
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    CONTROL_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_WORKLOAD_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_IMAGE_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_IMAGE_FIELD_NUMBER: _ClassVar[int]
    VALIDATOR_IMAGE_FIELD_NUMBER: _ClassVar[int]
    CONTROLLER_POD_UID_FIELD_NUMBER: _ClassVar[int]
    NATIVE_LEDGER_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_JOB_EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    PROPOSAL_COMMIT_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_COMMIT_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_USAGE_FIELD_NUMBER: _ClassVar[int]
    REFLECTION_SKIPPED_REASON_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    recorded_at: _timestamp_pb2.Timestamp
    broker_mode: str
    worker_mount_mode: str
    incident_id: str
    namespace: str
    control_namespace: str
    controller_workload: str
    controller_image: str
    responder_image: str
    validator_image: str
    controller_pod_uid: str
    native_ledger: _controller_state_pb2.BrokerLedger
    responder_job_evidence: _containers.RepeatedCompositeFieldContainer[ResponderJobEvidence]
    proposal_commit: str
    outcome_commit: str
    reflection_commit: str
    reflection_usage: _messages_pb2.UsageMetrics
    reflection_skipped_reason: str
    outcome_classification: str
    resolution: str
    def __init__(self, schema_version: _Optional[str] = ..., recorded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., broker_mode: _Optional[str] = ..., worker_mount_mode: _Optional[str] = ..., incident_id: _Optional[str] = ..., namespace: _Optional[str] = ..., control_namespace: _Optional[str] = ..., controller_workload: _Optional[str] = ..., controller_image: _Optional[str] = ..., responder_image: _Optional[str] = ..., validator_image: _Optional[str] = ..., controller_pod_uid: _Optional[str] = ..., native_ledger: _Optional[_Union[_controller_state_pb2.BrokerLedger, _Mapping]] = ..., responder_job_evidence: _Optional[_Iterable[_Union[ResponderJobEvidence, _Mapping]]] = ..., proposal_commit: _Optional[str] = ..., outcome_commit: _Optional[str] = ..., reflection_commit: _Optional[str] = ..., reflection_usage: _Optional[_Union[_messages_pb2.UsageMetrics, _Mapping]] = ..., reflection_skipped_reason: _Optional[str] = ..., outcome_classification: _Optional[str] = ..., resolution: _Optional[str] = ...) -> None: ...

class ReceiptFailure(_message.Message):
    __slots__ = ("incident_id", "code", "message")
    class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CODE_UNSPECIFIED: _ClassVar[ReceiptFailure.Code]
        CODE_INVALID_WIRE: _ClassVar[ReceiptFailure.Code]
        CODE_REJECTED_EVIDENCE: _ClassVar[ReceiptFailure.Code]
        CODE_CHECKER_UNAVAILABLE: _ClassVar[ReceiptFailure.Code]
    CODE_UNSPECIFIED: ReceiptFailure.Code
    CODE_INVALID_WIRE: ReceiptFailure.Code
    CODE_REJECTED_EVIDENCE: ReceiptFailure.Code
    CODE_CHECKER_UNAVAILABLE: ReceiptFailure.Code
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    code: ReceiptFailure.Code
    message: str
    def __init__(self, incident_id: _Optional[str] = ..., code: _Optional[_Union[ReceiptFailure.Code, str]] = ..., message: _Optional[str] = ...) -> None: ...

class NativeReceiptResult(_message.Message):
    __slots__ = ("receipt", "failure")
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    receipt: NativeProductionReceipt
    failure: ReceiptFailure
    def __init__(self, receipt: _Optional[_Union[NativeProductionReceipt, _Mapping]] = ..., failure: _Optional[_Union[ReceiptFailure, _Mapping]] = ...) -> None: ...

class ReceiptDrainResult(_message.Message):
    __slots__ = ("incident_id", "waited_seconds", "failure")
    INCIDENT_ID_FIELD_NUMBER: _ClassVar[int]
    WAITED_SECONDS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    incident_id: str
    waited_seconds: float
    failure: ReceiptFailure
    def __init__(self, incident_id: _Optional[str] = ..., waited_seconds: _Optional[float] = ..., failure: _Optional[_Union[ReceiptFailure, _Mapping]] = ...) -> None: ...

class RejectedProductionReceipt(_message.Message):
    __slots__ = ("validation_error", "receipt", "failure", "receipt_json")
    VALIDATION_ERROR_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    validation_error: str
    receipt: _struct_pb2.Struct
    failure: ReceiptFailure
    receipt_json: str
    def __init__(self, validation_error: _Optional[str] = ..., receipt: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., failure: _Optional[_Union[ReceiptFailure, _Mapping]] = ..., receipt_json: _Optional[str] = ...) -> None: ...
