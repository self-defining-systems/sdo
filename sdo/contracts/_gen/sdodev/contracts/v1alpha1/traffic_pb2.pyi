from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TrafficSLO(_message.Message):
    __slots__ = ("window", "min_samples", "max_age", "max_error_rate", "max_timeout_rate", "latency_percentile", "max_latency")
    WINDOW_FIELD_NUMBER: _ClassVar[int]
    MIN_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    MAX_AGE_FIELD_NUMBER: _ClassVar[int]
    MAX_ERROR_RATE_FIELD_NUMBER: _ClassVar[int]
    MAX_TIMEOUT_RATE_FIELD_NUMBER: _ClassVar[int]
    LATENCY_PERCENTILE_FIELD_NUMBER: _ClassVar[int]
    MAX_LATENCY_FIELD_NUMBER: _ClassVar[int]
    window: int
    min_samples: int
    max_age: str
    max_error_rate: float
    max_timeout_rate: float
    latency_percentile: int
    max_latency: str
    def __init__(self, window: _Optional[int] = ..., min_samples: _Optional[int] = ..., max_age: _Optional[str] = ..., max_error_rate: _Optional[float] = ..., max_timeout_rate: _Optional[float] = ..., latency_percentile: _Optional[int] = ..., max_latency: _Optional[str] = ...) -> None: ...

class TrafficWorkloadScenario(_message.Message):
    __slots__ = ("id", "weight", "slo")
    ID_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_FIELD_NUMBER: _ClassVar[int]
    SLO_FIELD_NUMBER: _ClassVar[int]
    id: str
    weight: int
    slo: TrafficSLO
    def __init__(self, id: _Optional[str] = ..., weight: _Optional[int] = ..., slo: _Optional[_Union[TrafficSLO, _Mapping]] = ...) -> None: ...

class TrafficLink(_message.Message):
    __slots__ = ("to", "port", "protocol", "http", "grpc")
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    PROTOCOL_FIELD_NUMBER: _ClassVar[int]
    HTTP_FIELD_NUMBER: _ClassVar[int]
    GRPC_FIELD_NUMBER: _ClassVar[int]
    to: str
    port: int
    protocol: str
    http: TrafficLinkHTTPOptions
    grpc: TrafficLinkGRPCOptions
    def __init__(self, to: _Optional[str] = ..., port: _Optional[int] = ..., protocol: _Optional[str] = ..., http: _Optional[_Union[TrafficLinkHTTPOptions, _Mapping]] = ..., grpc: _Optional[_Union[TrafficLinkGRPCOptions, _Mapping]] = ..., **kwargs) -> None: ...

class TrafficLinkHTTPOptions(_message.Message):
    __slots__ = ("path", "status_min", "status_max")
    PATH_FIELD_NUMBER: _ClassVar[int]
    STATUS_MIN_FIELD_NUMBER: _ClassVar[int]
    STATUS_MAX_FIELD_NUMBER: _ClassVar[int]
    path: str
    status_min: int
    status_max: int
    def __init__(self, path: _Optional[str] = ..., status_min: _Optional[int] = ..., status_max: _Optional[int] = ...) -> None: ...

class TrafficLinkGRPCOptions(_message.Message):
    __slots__ = ("tls",)
    TLS_FIELD_NUMBER: _ClassVar[int]
    tls: bool
    def __init__(self, tls: _Optional[bool] = ...) -> None: ...

class TrafficWorkload(_message.Message):
    __slots__ = ("api_version", "kind", "name", "description", "purpose", "arrival", "rate_per_second", "duration", "timeout", "iteration_timeout", "seed", "slo", "scenarios", "links", "interval", "failures")
    API_VERSION_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    ARRIVAL_FIELD_NUMBER: _ClassVar[int]
    RATE_PER_SECOND_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    ITERATION_TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SLO_FIELD_NUMBER: _ClassVar[int]
    SCENARIOS_FIELD_NUMBER: _ClassVar[int]
    LINKS_FIELD_NUMBER: _ClassVar[int]
    INTERVAL_FIELD_NUMBER: _ClassVar[int]
    FAILURES_FIELD_NUMBER: _ClassVar[int]
    api_version: str
    kind: str
    name: str
    description: str
    purpose: str
    arrival: str
    rate_per_second: float
    duration: str
    timeout: str
    iteration_timeout: str
    seed: int
    slo: TrafficSLO
    scenarios: _containers.RepeatedCompositeFieldContainer[TrafficWorkloadScenario]
    links: _containers.RepeatedCompositeFieldContainer[TrafficLink]
    interval: str
    failures: int
    def __init__(self, api_version: _Optional[str] = ..., kind: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., purpose: _Optional[str] = ..., arrival: _Optional[str] = ..., rate_per_second: _Optional[float] = ..., duration: _Optional[str] = ..., timeout: _Optional[str] = ..., iteration_timeout: _Optional[str] = ..., seed: _Optional[int] = ..., slo: _Optional[_Union[TrafficSLO, _Mapping]] = ..., scenarios: _Optional[_Iterable[_Union[TrafficWorkloadScenario, _Mapping]]] = ..., links: _Optional[_Iterable[_Union[TrafficLink, _Mapping]]] = ..., interval: _Optional[str] = ..., failures: _Optional[int] = ...) -> None: ...
