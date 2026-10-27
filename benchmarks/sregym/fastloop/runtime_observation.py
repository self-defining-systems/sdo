"""One bounded, read-only observation edge for runtime fault probes.

Only fixed read operations are admitted. No credential-store lookup, Secret endpoint,
mutation, package installation or arbitrary command is part of this interface.
Raw command errors are never copied into the tracker: applications can include
connection strings in their error bodies.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar
from urllib.parse import quote

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from collections.abc import Callable

Operation = Literal[
    "ephemeral_ports",
    "valkey_memory",
    "kafka_offsets",
    "kafka_progress",
    "cart_read",
    "products_read",
    "tls_certificate",
    "mongo_geo_read",
    "mongo_rate_read",
]

_EXEC_SCRIPTS = {
    "ephemeral_ports": "cat /proc/sys/net/ipv4/ip_local_port_range",
    "valkey_memory": (
        "valkey-cli --raw INFO memory || exit 1; "
        "printf '\\n__SDO_LIMIT__\\n'; "
        "if [ -r /sys/fs/cgroup/memory.max ]; then "
        "sdo_mem_limit_path=/sys/fs/cgroup/memory.max; sdo_mem_usage_path=/sys/fs/cgroup/memory.current; "
        "else sdo_mem_limit_path=/sys/fs/cgroup/memory/memory.limit_in_bytes; "
        "sdo_mem_usage_path=/sys/fs/cgroup/memory/memory.usage_in_bytes; fi; "
        'cat "$sdo_mem_limit_path" || exit 1; '
        "printf '\\n__SDO_USAGE__\\n'; "
        'cat "$sdo_mem_usage_path"'
    ),
    "kafka_offsets": (
        "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "
        "--describe --all-groups --timeout 3000"
    ),
    "kafka_progress": (
        "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 "
        "--describe --group orders-validator --timeout 3000 || exit 1; "
        "printf '\\n__SDO_OUTPUT__\\n'; "
        "/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 "
        "--topic orders-processed --time -1"
    ),
    # Public certificate only; self-signed development certificates are allowed.
    # Dates and hostname are checked independently by the deterministic probe.
    "tls_certificate": (
        "openssl s_client -connect ingress-nginx-controller.ingress-nginx.svc.cluster.local:443 "
        '-servername "$1" -showcerts </dev/null 2>/dev/null '
        '| openssl x509 -noout -dates -checkhost "$1"'
    ),
}
_MONGO_TARGETS = {
    "mongo_geo_read": ("mongodb-geo", "geo", "geo-db", "geo"),
    "mongo_rate_read": ("mongodb-rate", "rate", "rate-db", "inventory"),
}
_EXEC_SCRIPTS.update(dict.fromkeys(_MONGO_TARGETS, "exec mongo --quiet --norc"))

_HTTP_PATHS = {
    "cart_read": "/api/cart?sessionId=sdo-read-only-fault-observation&currencyCode=USD",
    "products_read": "/api/products?currencyCode=USD",
}
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?\Z")


class ObservationUnavailable(ValueError):
    """A private observation could not establish a result (no upstream payload)."""


_T = TypeVar("_T")


@dataclass(frozen=True)
class _ObservationBudget:
    deadline: float
    monotonic: Callable[[], float]


_BASELINE_BUDGET: ContextVar[_ObservationBudget | None] = ContextVar("baseline_observation_budget", default=None)


def observation_timeout(default: float) -> float:
    """Clamp each subprocess to the complete baseline capture's remaining time."""
    budget = _BASELINE_BUDGET.get()
    if budget is None:
        return default
    remaining = budget.deadline - budget.monotonic()
    if remaining <= 0:
        raise ObservationUnavailable("baseline observation deadline exhausted")
    return min(default, remaining)


def baseline_observation(
    capture: Callable[[], _T],
    *,
    timeout_seconds: float = 90,
    poll_seconds: float = 3,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> _T:
    """Retry the complete capture, discarding partial pre-injection references."""
    if not 0 < timeout_seconds <= 120 or poll_seconds <= 0:
        raise ValueError("baseline observation requires a positive budget <=120s and poll interval")
    deadline = monotonic() + timeout_seconds
    token = _BASELINE_BUDGET.set(_ObservationBudget(deadline, monotonic))
    try:
        while True:
            try:
                result = capture()
                if monotonic() > deadline:
                    raise ObservationUnavailable("baseline observation deadline exhausted")
                return result
            except (ObservationUnavailable, subprocess.TimeoutExpired):
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise ObservationUnavailable("baseline observation unavailable; never_injected_retry") from None
                sleep(min(poll_seconds, remaining))
    finally:
        _BASELINE_BUDGET.reset(token)


def _mongo_program(source: str, database: str, collection: str) -> str:
    """Admit only the pinned app's literal-login initialization shape.

    Values stay local and are JSON-quoted into private stdin, never argv or an
    observation/result object. Unsupported source expressions fail closed.
    """
    values = []
    for variable in ("adminUsername", "adminPassword"):
        matches = re.findall(r"(?m)^\s*" + variable + r'\s*:=\s*("(?:[^"\\]|\\.)*")\s*$', source)
        value = None
        if len(matches) == 1:
            try:
                value = json.loads(matches[0])
            except ValueError:
                pass
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise ObservationUnavailable("pinned application login source is unsupported")
        values.append(json.dumps(value))
    connection = 'url = "mongodb://" + adminUsername + ":" + adminPassword + "@" + url'
    if connection not in source or "func initializeDatabase(url string) *mgo.Session" not in source:
        raise ObservationUnavailable("pinned application login source is unsupported")
    return (
        '(function() { var authdb = db.getSiblingDB("admin"); '
        f"if (authdb.auth({values[0]}, {values[1]}) !== 1) {{ quit(3); }} "
        f'var r = authdb.getSiblingDB("{database}").runCommand('
        '{find: "' + collection + '", filter: {}, limit: 1, singleBatch: true, maxTimeMS: 3000}); '
        "if (r.ok !== 1 || !r.cursor || !Array.isArray(r.cursor.firstBatch)) { quit(4); } "
        'print("__SDO_MONGO_READ_OK__"); quit(0); })();\n'
    )


@dataclass(frozen=True)
class ReadOnlyObservation:
    operation: Operation
    pod: str = ""
    container: str = ""
    hostname: str = ""

    def __post_init__(self) -> None:
        if self.operation not in _EXEC_SCRIPTS and self.operation not in _HTTP_PATHS:
            raise ValueError("unknown read-only observation operation")
        if self.operation in _EXEC_SCRIPTS:
            if not _NAME.fullmatch(self.pod) or not _NAME.fullmatch(self.container):
                raise ValueError("exec observations require exact Pod and container names")
        elif self.pod or self.container:
            raise ValueError("service observations cannot carry exec targets")
        if self.operation == "tls_certificate":
            if not _NAME.fullmatch(self.hostname):
                raise ValueError("TLS observation requires a public DNS hostname")
        elif self.hostname:
            raise ValueError("only TLS observations accept a hostname")


class KubectlObservationTransport:
    def __init__(
        self,
        *,
        namespace: str,
        kubeconfig: Path | str | None = None,
        run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        read_mongo_source: Callable[[str], str] | None = None,
        source_root: Path | None = None,
    ) -> None:
        if not _NAME.fullmatch(namespace):
            raise ValueError("observation namespace must be an exact namespace name")
        self.namespace = namespace
        self.kubeconfig = str(kubeconfig) if kubeconfig else None
        self.run = run or subprocess.run
        self._source_root = (source_root or Path(__file__).resolve().parents[3]).resolve()
        self._read_mongo_source = read_mongo_source or self._pinned_mongo_source

    def _read(self, command: list[str], *, stdin: str | None = None) -> str:
        result = None
        try:
            result = self.run(
                command,
                input=stdin,
                capture_output=True,
                text=True,
                check=False,
                timeout=observation_timeout(wait_for(WaitKind.PROBE)),
            )
        except Exception:
            # Raise outside the handler: even __context__ must not retain a
            # private exception, subprocess output, or credential-bearing stdin.
            pass
        if result is None:
            raise ObservationUnavailable("runtime observation transport unavailable")
        if result.returncode != 0:
            raise ObservationUnavailable("runtime observation command failed")
        if not isinstance(result.stdout, str) or len(result.stdout.encode("utf-8")) > 65536:
            raise ObservationUnavailable("runtime observation response is missing or oversized")
        return result.stdout

    def _pinned_mongo_source(self, component: str) -> str:
        root = self._source_root
        harness = root / "third_party/sregym"
        apps = harness / "SREGym-applications"
        for directory in (harness, apps):
            actual = self._read(["git", "-C", str(directory), "rev-parse", "--show-toplevel"])
            if Path(actual.strip()).resolve() != directory.resolve():
                raise ObservationUnavailable("pinned application source is not initialized")

        def gitlink(directory: Path, revision: str, path: str) -> str:
            raw = self._read(["git", "-C", str(directory), "ls-tree", revision, "--", path])
            fields = raw.strip().split()
            if (
                len(fields) != 4
                or fields[:2] != ["160000", "commit"]
                or fields[3] != path
                or re.fullmatch(r"[0-9a-f]{40}", fields[2]) is None
            ):
                raise ObservationUnavailable("pinned application source identity is unavailable")
            return fields[2]

        harness_pin = gitlink(root, "HEAD", "third_party/sregym")
        app_pin = gitlink(harness, harness_pin, "SREGym-applications")
        service = {"mongodb-geo": "geo", "mongodb-rate": "rate"}[component]
        return self._read(["git", "-C", str(apps), "show", f"{app_pin}:hotelReservation/cmd/{service}/db.go"])

    def __call__(self, request: ReadOnlyObservation) -> str:
        stdin = None
        if request.operation in _MONGO_TARGETS:
            component, _, database, collection = _MONGO_TARGETS[request.operation]
            source = None
            try:
                source = self._read_mongo_source(component)
            except Exception:
                pass
            if not isinstance(source, str) or len(source) > 65536:
                raise ObservationUnavailable("pinned application source is unavailable")
            stdin = _mongo_program(source, database, collection)
        command = ["kubectl"]
        if self.kubeconfig:
            command += ["--kubeconfig", self.kubeconfig]
        if request.operation in _HTTP_PATHS:
            # GET service proxy reaches the dependent application's real read
            # handler. It does not read database credentials or synthesize replies.
            path = (
                f"/api/v1/namespaces/{quote(self.namespace, safe='')}/services/"
                f"http:frontend:8080/proxy{_HTTP_PATHS[request.operation]}"
            )
            command += ["get", "--raw", path, "--request-timeout=8s"]
        else:
            command += [
                "-n",
                self.namespace,
                "exec",
                *(["-i"] if stdin is not None else []),
                request.pod,
                "-c",
                request.container,
                "--",
                "timeout",
                "8s",
                "sh",
                "-c",
                _EXEC_SCRIPTS[request.operation],
                "sdo-read-only-observation",
            ]
            if request.hostname:
                command.append(request.hostname)
        raw = self._read(command, stdin=stdin)
        if stdin is not None:
            if "__SDO_MONGO_READ_OK__" not in raw.splitlines():
                raise ObservationUnavailable("authenticated application query did not complete")
            return "__SDO_MONGO_READ_OK__"
        return raw
