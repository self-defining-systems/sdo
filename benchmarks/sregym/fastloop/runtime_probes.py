"""Pure runtime evidence decisions over the shared read-only observation edge."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.object_probes import (
    ObjectReader,
    _selector_matches,
    deployment_available,
    flag_disabled,
)
from benchmarks.sregym.fastloop.runtime_observation import ObservationUnavailable, Operation, ReadOnlyObservation

if TYPE_CHECKING:
    from benchmarks.sregym.faults.description import FaultSpec

Observer = Callable[[ReadOnlyObservation], str]


def _targets(read: ObjectReader, component: str) -> tuple[tuple[str, str], ...]:
    obj = read("deployment", component)
    listing = read("pods", "")
    if not obj or listing is None or not deployment_available(lambda *_: obj, component):
        return ()
    selector = (obj.get("spec") or {}).get("selector") or {}
    targets = []
    for pod in listing.get("items") or []:
        metadata = pod.get("metadata") or {}
        if metadata.get("deletionTimestamp") or not _selector_matches(metadata.get("labels") or {}, selector):
            continue
        status = pod.get("status") or {}
        if status.get("phase") != "Running" or not any(
            c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions") or []
        ):
            return ()
        containers = (pod.get("spec") or {}).get("containers") or []
        selected = [c.get("name") for c in containers if c.get("name") == component]
        if not selected and len(containers) == 1:
            selected = [containers[0].get("name")]
        if len(selected) != 1 or not selected[0] or not metadata.get("name"):
            return ()
        targets.append((metadata["name"], selected[0]))
    desired = int((obj.get("spec") or {}).get("replicas", 1))
    return tuple(targets) if len(targets) >= desired else ()


def _exec_values(
    read: ObjectReader, observe: Observer, component: str, operation: Operation, *, hostname: str = ""
) -> tuple[str, ...]:
    targets = _targets(read, component)
    return tuple(observe(ReadOnlyObservation(operation, pod, container, hostname)) for pod, container in targets)


def _port_range(raw: str) -> tuple[int, int]:
    values = raw.split()
    if len(values) != 2:
        raise ObservationUnavailable("invalid ephemeral port observation")
    try:
        low, high = map(int, values)
    except ValueError:
        raise ObservationUnavailable("invalid ephemeral port observation") from None
    if not 0 < low <= high <= 65535:
        raise ObservationUnavailable("invalid ephemeral port observation")
    return low, high


@dataclass(frozen=True)
class KafkaPartition:
    group: str
    topic: str
    partition: int
    current: int
    end: int
    lag: int

    @property
    def identity(self) -> tuple[str, str, int]:
        return self.group, self.topic, self.partition


def _kafka_rows(raw: str) -> tuple[KafkaPartition, ...]:
    rows = []
    for line in raw.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[:6] == ["GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG"]:
            continue
        if line.strip().startswith("Consumer group '") and line.strip().endswith("has no active members."):
            continue
        if len(parts) < 6:
            raise ObservationUnavailable("incomplete Kafka offset observation")
        try:
            row = KafkaPartition(parts[0], parts[1], *map(int, parts[2:6]))
        except ValueError:
            raise ObservationUnavailable("incomplete Kafka offset observation") from None
        if (
            min(row.partition, row.current, row.end, row.lag) < 0
            or row.end < row.current
            or row.lag != row.end - row.current
        ):
            raise ObservationUnavailable("invalid Kafka offset observation")
        rows.append(row)
    if not rows or len({row.identity for row in rows}) != len(rows):
        raise ObservationUnavailable("missing or ambiguous Kafka offset observation")
    return tuple(rows)


def _memory_headroom(raw: str) -> bool:
    if raw.count("__SDO_LIMIT__") != 1 or raw.count("__SDO_USAGE__") != 1:
        return False
    info, memory = raw.split("__SDO_LIMIT__", 1)
    limit, usage = memory.split("__SDO_USAGE__", 1)
    fields = dict(line.split(":", 1) for line in info.splitlines() if ":" in line and not line.startswith("#"))
    try:
        used, maximum = int(fields["used_memory"]), int(fields["maxmemory"])
        capacity, current = int(limit.strip()), int(usage.strip())
    except (KeyError, ValueError):
        return False
    return (
        used > 0
        and capacity > 0
        and current > 0
        and max(used, current) * 5 < capacity * 4
        and (maximum == 0 or used < maximum)
    )


def _no_memory_flood(read: ObjectReader) -> bool:
    jobs, pods = read("jobs", ""), read("pods", "")
    if jobs is None or pods is None:
        return False
    if any((job.get("metadata") or {}).get("name") == "valkey-memory-flood" for job in jobs.get("items") or []):
        return False
    return not any(
        any(
            owner.get("kind") == "Job" and owner.get("name") == "valkey-memory-flood"
            for owner in (pod.get("metadata") or {}).get("ownerReferences") or []
        )
        and (pod.get("status") or {}).get("phase") not in {"Succeeded", "Failed"}
        for pod in pods.get("items") or []
    )


def _application_read(observe: Observer, operation: Operation) -> bool:
    try:
        body = json.loads(observe(ReadOnlyObservation(operation)))
    except json.JSONDecodeError:
        return False
    if operation == "cart_read":
        return isinstance(body, dict) and isinstance(body.get("items"), list) and isinstance(body.get("userId"), str)
    return (
        isinstance(body, list)
        and bool(body)
        and all(isinstance(item, dict) and isinstance(item.get("id"), str) and item["id"] for item in body)
    )


def _certificate_valid(raw: str, hostname: str, now: datetime) -> bool:
    fields = dict(line.split("=", 1) for line in raw.splitlines() if line.startswith(("notBefore=", "notAfter=")))
    try:
        start, end = (
            datetime.strptime(fields[key], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=timezone.utc)
            for key in ("notBefore", "notAfter")
        )
    except (KeyError, ValueError):
        return False
    return start <= now < end and f"Hostname {hostname} does match certificate" in raw.splitlines()


def runtime_probe(
    fault: FaultSpec,
    read: ObjectReader,
    observe: Observer,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> Callable[[], bool]:
    component = fault.component
    kind = fault.kind
    if kind == "ephemeral_port_range_hotel_reservation":
        baseline = tuple(_port_range(value) for value in _exec_values(read, observe, component, "ephemeral_ports"))
        if not baseline or len(set(baseline)) != 1 or baseline[0][1] - baseline[0][0] < 1024:
            raise ValueError("ephemeral probe requires a healthy pre-injection port range")

        def ports_restored() -> bool:
            values = _exec_values(read, observe, component, "ephemeral_ports")
            return bool(values) and all(_port_range(value) == baseline[0] for value in values)

        return ports_restored
    if kind in {"valkey_auth_disruption", "secret_rotation_stale_env_credentials_astronomy_shop"}:
        operation = "cart_read" if kind == "valkey_auth_disruption" else "products_read"
        return lambda: _application_read(observe, operation)
    if kind in {"storage_user_unregistered", "revoke_auth_mongodb"}:
        if component not in {"mongodb-geo", "mongodb-rate"}:
            raise ValueError("Mongo auth probe needs a source-grounded geo/rate target")
        mongo_operation: Operation = "mongo_geo_read" if component == "mongodb-geo" else "mongo_rate_read"

        def mongo_readable() -> bool:
            values = _exec_values(read, observe, component, mongo_operation)
            return bool(values) and all(value.strip() == "__SDO_MONGO_READ_OK__" for value in values)

        return mongo_readable
    if kind == "valkey_memory_disruption":

        def memory_ready() -> bool:
            values = _exec_values(read, observe, component, "valkey_memory")
            return bool(values) and _no_memory_flood(read) and all(_memory_headroom(value) for value in values)

        return memory_ready
    if kind == "expired_tls_hotel_reservation":

        def tls_valid() -> bool:
            ingress = read("ingress", fault.parameters["ingress_name"])
            if not ingress:
                return False
            hosts = {host for tls in (ingress.get("spec") or {}).get("tls") or [] for host in tls.get("hosts") or []}
            hosts &= {rule.get("host") for rule in (ingress.get("spec") or {}).get("rules") or []}
            if len(hosts) != 1:
                return False
            hostname = hosts.pop()
            values = _exec_values(read, observe, component, "tls_certificate", hostname=hostname)
            return bool(values) and all(_certificate_valid(value, hostname, now()) for value in values)

        return tls_valid
    if kind == "kafka_queue_problems":
        values = _exec_values(read, observe, component, "kafka_offsets")
        if len(values) != 1:
            raise ObservationUnavailable("Kafka probe requires one healthy broker observation")
        baseline = {row.identity: row for row in _kafka_rows(values[0])}

        def queue_drained() -> bool:
            if not flag_disabled(read, "kafkaQueueProblems"):
                return False
            current_values = _exec_values(read, observe, component, "kafka_offsets")
            if len(current_values) != 1:
                return False
            rows = {row.identity: row for row in _kafka_rows(current_values[0])}
            return baseline.keys() <= rows.keys() and all(
                row.lag <= baseline[key].lag and row.current >= baseline[key].current and row.end >= baseline[key].end
                if key in baseline
                else row.lag == 0
                for key, row in rows.items()
            )

        return queue_drained
    if kind == "kafka_poison_pill_hol_block":
        first: tuple[int, int] | None = None

        def processing_progress() -> bool:
            nonlocal first
            values = _exec_values(read, observe, "kafka", "kafka_progress")
            if len(values) != 1 or "__SDO_OUTPUT__" not in values[0]:
                return False
            raw_rows, raw_output = values[0].split("__SDO_OUTPUT__", 1)
            rows = [
                row
                for row in _kafka_rows(raw_rows)
                if row.group == "orders-validator" and row.topic == "orders-fulfillment"
            ]
            if len(rows) != 1:
                return False
            output_lines = [line.split(":") for line in raw_output.splitlines() if line.startswith("orders-processed:")]
            if len(output_lines) != 1 or len(output_lines[0]) != 3:
                return False
            try:
                output = int(output_lines[0][2])
            except ValueError:
                return False
            row = rows[0]
            if output < 0 or row.partition != 0 or output_lines[0][1] != "0":
                return False
            if first is None:
                first = (row.current, output)
                return False
            return row.current > first[0] and output > first[1] and row.lag == 0

        return processing_progress
    raise ValueError("no read-only runtime probe for this fault")
