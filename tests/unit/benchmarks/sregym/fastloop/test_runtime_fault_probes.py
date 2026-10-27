"""Independent runtime observations; API fixtures never contain credentials."""

from __future__ import annotations

import pytest

from benchmarks.sregym.fastloop.fault_tracker import probes_for
from benchmarks.sregym.faults.worker import describe_problem


def deployment(name: str):
    return {
        "metadata": {"generation": 2},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": name}},
            "template": {"spec": {"containers": [{"name": name}]}},
        },
        "status": {"observedGeneration": 2, "replicas": 1, "readyReplicas": 1, "updatedReplicas": 1},
    }


class Cluster:
    def __init__(self, name="frontend"):
        self.objects = {
            ("deployment", name): deployment(name),
            ("pods", ""): {
                "items": [
                    {
                        "metadata": {"name": name + "-pod", "labels": {"app": name}},
                        "spec": {"containers": [{"name": name}]},
                        "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
                    }
                ]
            },
            ("jobs", ""): {"items": []},
            ("configmap", "flagd-config"): {
                "data": {"demo.flagd.json": '{"flags":{"kafkaQueueProblems":{"defaultVariant":"off"}}}'}
            },
        }
        self.values = {}
        self.calls = []

    def __call__(self, kind, name):
        return self.objects.get((kind, name))

    def observe(self, request):
        self.calls.append(request)
        return self.values[request.operation]


def fault(class_name, component, **attributes):
    return describe_problem("fixture", type(class_name, (), {"faulty_service": component, **attributes})()).faults[0]


def test_ephemeral_range_is_observed_inside_every_current_pod():
    f = fault("EphemeralPortRangeHotelReservation", "frontend", sysctl_scope="frontend_only")
    c = Cluster()
    c.values["ephemeral_ports"] = "32768 60999\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    c.values["ephemeral_ports"] = "32000 32003\n"
    assert probe() is False
    c.values["ephemeral_ports"] = "32768 60999\n"
    assert probe() is True
    c.objects[("pods", "")]["items"] = []
    assert probe() is False


@pytest.mark.parametrize(
    ("class_name", "operation", "bad", "good"),
    [
        ("ValkeyAuthDisruption", "cart_read", '{"error":"FailedPrecondition"}', '{"userId":"","items":[]}'),
        (
            "SecretRotationStaleEnvCredentialsAstronomyShop",
            "products_read",
            '{"error":"database unavailable"}',
            '[{"id":"item1","name":"Public product"}]',
        ),
    ],
)
def test_auth_uses_the_dependent_application_read_result(class_name, operation, bad, good):
    component = "valkey-cart" if class_name == "ValkeyAuthDisruption" else "product-catalog"
    f = fault(class_name, component)
    c = Cluster(component)
    c.values[operation] = bad
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False
    c.values[operation] = good
    assert probe() is True
    c.values[operation] = '{"status":"healthy"}'
    assert probe() is False


def test_valkey_memory_requires_headroom_and_no_active_flood():
    f = fault("ValkeyMemoryDisruption", "valkey-cart")
    c = Cluster("valkey-cart")
    c.values["valkey_memory"] = (
        "used_memory:990000\nmaxmemory:0\nmaxmemory_policy:noeviction\n__SDO_LIMIT__\n1000000\n__SDO_USAGE__\n990000\n"
    )
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False
    c.values["valkey_memory"] = (
        "used_memory:10000\nmaxmemory:0\nmaxmemory_policy:noeviction\n__SDO_LIMIT__\n1000000\n__SDO_USAGE__\n50000\n"
    )
    c.objects[("jobs", "")]["items"] = [{"metadata": {"name": "valkey-memory-flood"}}]
    assert probe() is False
    c.objects[("jobs", "")]["items"] = []
    assert probe() is True
    c.values["valkey_memory"] = "NOAUTH Authentication required."
    assert probe() is False


def test_kafka_queue_requires_drained_lag_not_just_disabled_flag():
    f = fault("KafkaQueueProblems", "kafka")
    c = Cluster("kafka")
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
    c.values["kafka_offsets"] = header + "accounting orders 0 100 100 0 client host client\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    c.values["kafka_offsets"] = header + "accounting orders 0 100 200 100 client host client\n"
    assert probe() is False
    c.values["kafka_offsets"] = header + "accounting orders 0 200 200 0 client host client\n"
    assert probe() is True
    c.values["kafka_offsets"] = header
    from benchmarks.sregym.fastloop.fault_tracker import FaultTracker

    tracker = FaultTracker({f.name: probe}, stable_seconds=0)
    sample = tracker.poll()
    assert sample.states == {f.name: False}
    assert "missing" in sample.errors[f.name]


def test_poison_pill_requires_consumer_and_processed_topic_progress_without_hidden_offset():
    f = fault(
        "KafkaPoisonPillHOLBlock", "orders-validator", TOPIC="orders-fulfillment", CONSUMER_GROUP="orders-validator"
    )
    c = Cluster("kafka")
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\n"
    c.values["kafka_progress"] = (
        header
        + "orders-validator orders-fulfillment 0 20 30 10 client host client\n__SDO_OUTPUT__\norders-processed:0:20\n"
    )
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False
    c.values["kafka_progress"] = (
        header
        + "orders-validator orders-fulfillment 0 30 30 0 client host client\n__SDO_OUTPUT__\norders-processed:0:20\n"
    )
    assert probe() is False  # Skipping every record is not processing progress.
    c.values["kafka_progress"] = (
        header
        + "orders-validator orders-fulfillment 0 31 31 0 client host client\n__SDO_OUTPUT__\norders-processed:0:30\n"
    )
    assert probe() is True


def test_expired_tls_observes_the_public_certificate_not_secret_data():
    f = fault("ExpiredTlsHotelReservation", "frontend", ingress_name="frontend-ingress")
    c = Cluster()
    c.objects[("ingress", "frontend-ingress")] = {
        "spec": {"tls": [{"hosts": ["hotel-reservation.local"]}], "rules": [{"host": "hotel-reservation.local"}]}
    }
    c.values["tls_certificate"] = (
        "notBefore=Jan  1 00:00:00 2020 GMT\n"
        "notAfter=Jan  1 00:00:00 2021 GMT\n"
        "Hostname hotel-reservation.local does match certificate\n"
    )
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False
    c.values["tls_certificate"] = (
        "notBefore=Jan  1 00:00:00 2020 GMT\n"
        "notAfter=Jan  1 00:00:00 2099 GMT\n"
        "Hostname hotel-reservation.local does match certificate\n"
    )
    assert probe() is True
    c.values["tls_certificate"] = c.values["tls_certificate"].replace("does match", "does NOT match")
    assert probe() is False


@pytest.mark.parametrize("class_name", ["MongoDBUserUnregistered", "MongoDBRevokeAuth"])
@pytest.mark.parametrize(
    ("component", "operation"), [("mongodb-geo", "mongo_geo_read"), ("mongodb-rate", "mongo_rate_read")]
)
def test_mongo_requires_a_fresh_authenticated_application_collection_read(class_name, component, operation):
    f = fault(class_name, component)
    c = Cluster(component)
    c.values[operation] = "authentication failed"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False
    c.values[operation] = "unauthorized"
    assert probe() is False
    c.values[operation] = "__SDO_MONGO_READ_OK__\n"
    assert probe() is True
    assert c.calls[-1].operation == operation
    assert c.calls[-1].pod == component + "-pod"
    c.values[operation] = '{"features":[{"id":"cached-hotel"}]}'
    assert probe() is False


@pytest.mark.parametrize("class_name", ["MongoDBUserUnregistered", "MongoDBRevokeAuth"])
def test_mongo_tracking_rejects_targets_without_a_source_grounded_login(class_name):
    f = fault(class_name, "mongodb-user")
    assert f.tracking_supported is False
    c = Cluster("mongodb-user")
    with pytest.raises(ValueError, match="target"):
        probes_for((f,), c, observe=c.observe)
    assert c.calls == []


@pytest.mark.parametrize(
    "operation",
    [
        "ephemeral_ports",
        "valkey_memory",
        "kafka_offsets",
        "kafka_progress",
        "tls_certificate",
        "cart_read",
        "products_read",
    ],
)
def test_shared_transport_is_bounded_and_returns_only_the_selected_read(operation):
    import subprocess

    from benchmarks.sregym.fastloop.runtime_observation import KubectlObservationTransport, ReadOnlyObservation

    request = ReadOnlyObservation(
        operation,
        **({"pod": "app-pod", "container": "app"} if operation not in {"cart_read", "products_read"} else {}),
        **({"hostname": "hotel-reservation.local"} if operation == "tls_certificate" else {}),
    )
    calls = []

    def run(command, **kwargs):
        assert kwargs["timeout"] == 10
        assert kwargs["check"] is False
        assert kwargs["capture_output"] is True
        assert command[:3] == ["kubectl", "--kubeconfig", "private-kubeconfig"]
        assert "secret" not in command
        assert "apply" not in command
        assert "delete" not in command
        if operation in {"cart_read", "products_read"}:
            assert command[3:5] == ["get", "--raw"]
            assert "/services/http:frontend:8080/proxy/api/" in command[5]
        else:
            assert command[3:10] == ["-n", "app", "exec", "app-pod", "-c", "app", "--"]
            assert command[10:14] == ["timeout", "8s", "sh", "-c"]
            assert all(
                word not in command[14]
                for word in ["CONFIG SET", "redis-cli AUTH", "valkey-cli AUTH", "get secret", "os.environ"]
            )
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "read-result")

    assert (
        KubectlObservationTransport(namespace="app", kubeconfig="private-kubeconfig", run=run)(request) == "read-result"
    )
    assert len(calls) == 1


@pytest.mark.parametrize("response", ["command_failed", "timeout", "oversized"])
def test_observation_failures_never_copy_credential_bearing_payloads(response):
    import subprocess

    from benchmarks.sregym.fastloop.runtime_observation import (
        KubectlObservationTransport,
        ObservationUnavailable,
        ReadOnlyObservation,
    )

    canary = "PRIVATE_CREDENTIAL_CANARY"

    def run(command, **kwargs):
        if response == "timeout":
            raise subprocess.TimeoutExpired(command, 10, output=canary, stderr=canary)
        return subprocess.CompletedProcess(
            command,
            1 if response == "command_failed" else 0,
            canary * 10000 if response == "oversized" else canary,
            canary,
        )

    with pytest.raises(ObservationUnavailable) as error:
        KubectlObservationTransport(namespace="app", run=run)(ReadOnlyObservation("cart_read"))
    assert canary not in str(error.value)


@pytest.mark.parametrize("operation", ["get_secret", "shell", "write", "mongo_credentials"])
def test_transport_has_no_arbitrary_command_or_secret_operation(operation):
    from benchmarks.sregym.fastloop.runtime_observation import ReadOnlyObservation

    with pytest.raises(ValueError, match="unknown"):
        ReadOnlyObservation(operation)


@pytest.mark.parametrize("hostname", ["hotel.local;cat /secret", "$(env)", "localhost/path", ""])
def test_tls_metadata_cannot_inject_a_command(hostname):
    from benchmarks.sregym.fastloop.runtime_observation import ReadOnlyObservation

    with pytest.raises(ValueError, match="hostname"):
        ReadOnlyObservation("tls_certificate", "app-pod", "app", hostname)


def test_one_unrepaired_pod_prevents_runtime_resolution():
    f = fault("EphemeralPortRangeHotelReservation", "frontend", sysctl_scope="frontend_only")
    c = Cluster()
    second = {
        **c.objects[("pods", "")]["items"][0],
        "metadata": {"name": "frontend-second", "labels": {"app": "frontend"}},
    }
    c.objects[("pods", "")]["items"].append(second)
    c.objects[("deployment", "frontend")]["spec"]["replicas"] = 2
    c.objects[("deployment", "frontend")]["status"].update(replicas=2, readyReplicas=2, updatedReplicas=2)
    bad = False

    def observe(request):
        return "32000 32003" if bad and request.pod == "frontend-second" else "32768 60999"

    probe = probes_for((f,), c, observe=observe)[f.name]
    bad = True
    assert probe() is False
    bad = False
    assert probe() is True


def test_kafka_offset_disappearance_or_rewind_cannot_count_as_repair():
    f = fault("KafkaQueueProblems", "kafka")
    c = Cluster("kafka")
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\n"
    c.values["kafka_offsets"] = header + "accounting orders 0 100 100 0\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    c.values["kafka_offsets"] = header + "accounting orders 0 0 0 0\n"
    assert probe() is False


def test_missing_runtime_transport_fails_before_injection():
    f = fault("ValkeyAuthDisruption", "valkey-cart")
    c = Cluster("valkey-cart")
    with pytest.raises(ValueError, match="transport"):
        probes_for((f,), lambda kind, name: c(kind, name))


def test_transport_decode_failure_does_not_escape_with_a_private_reason():
    from benchmarks.sregym.fastloop.runtime_observation import (
        KubectlObservationTransport,
        ObservationUnavailable,
        ReadOnlyObservation,
    )

    def run(*args, **kwargs):
        raise UnicodeDecodeError("utf-8", b"private-bytes", 0, 1, "PRIVATE_CREDENTIAL_CANARY")

    with pytest.raises(ObservationUnavailable) as error:
        KubectlObservationTransport(namespace="app", run=run)(ReadOnlyObservation("products_read"))
    assert "PRIVATE_CREDENTIAL_CANARY" not in str(error.value)


def test_new_kafka_backlog_is_not_hidden_by_healthy_preexisting_groups():
    f = fault("KafkaQueueProblems", "kafka")
    c = Cluster("kafka")
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\n"
    c.values["kafka_offsets"] = header + "accounting orders 0 100 100 0\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    c.values["kafka_offsets"] += "new-accounting orders 1 0 100 100\n"
    assert probe() is False


@pytest.mark.parametrize("suffix", ["0 101 101 - - -", "0 - 101 101 - - -"])
def test_incomplete_kafka_rows_are_unavailable_even_beside_a_complete_partition(suffix):
    from benchmarks.sregym.fastloop.fault_tracker import FaultTracker

    f = fault("KafkaQueueProblems", "kafka")
    c = Cluster("kafka")
    header = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\n"
    c.values["kafka_offsets"] = header + "accounting orders 0 100 100 0\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    c.values["kafka_offsets"] += "new-accounting new-orders " + suffix + "\n"
    sample = FaultTracker({f.name: probe}).poll()
    assert sample.states[f.name] is False
    assert sample.errors


def test_private_transport_errors_are_sanitized_for_every_operation():
    import subprocess

    from hypothesis import given, settings
    from hypothesis import strategies as st

    from benchmarks.sregym.fastloop.fault_tracker import FaultTracker
    from benchmarks.sregym.fastloop.runtime_observation import KubectlObservationTransport, ReadOnlyObservation

    @settings(max_examples=48, deadline=1000)
    @given(st.text(min_size=1, max_size=64))
    def check_canary(fragment):
        canary = "PRIVATE_CREDENTIAL_" + fragment + "_CANARY"
        for operation in (
            "ephemeral_ports",
            "valkey_memory",
            "kafka_offsets",
            "kafka_progress",
            "tls_certificate",
            "cart_read",
            "products_read",
            "mongo_geo_read",
            "mongo_rate_read",
        ):
            request = ReadOnlyObservation(
                operation,
                **({"pod": "app-pod", "container": "app"} if operation not in {"cart_read", "products_read"} else {}),
                **({"hostname": "hotel.local"} if operation == "tls_certificate" else {}),
            )
            for failure in ("exit", "os", "timeout", "decode"):

                def run(command, *, failure=failure, **kwargs):
                    if failure == "os":
                        raise OSError(canary)
                    if failure == "timeout":
                        raise subprocess.TimeoutExpired(command, 10, output=canary, stderr=canary)
                    if failure == "decode":
                        raise UnicodeDecodeError("utf-8", b"invalid", 0, 1, canary)
                    return subprocess.CompletedProcess(command, 1, canary, canary)

                transport = KubectlObservationTransport(namespace="app", run=run)
                tracker = FaultTracker(
                    {"fault": lambda transport=transport, request=request: bool(transport(request))}, clock=lambda: 0
                )
                sample = tracker.poll()
                assert sample.states == {"fault": False}
                assert "ObservationUnavailable" in sample.errors["fault"]
                assert canary not in str(tracker.timeline())

    check_canary()


def test_valkey_allocator_headroom_cannot_hide_an_almost_full_cgroup():
    f = fault("ValkeyMemoryDisruption", "valkey-cart")
    c = Cluster("valkey-cart")
    c.values["valkey_memory"] = "used_memory:10000\nmaxmemory:0\n__SDO_LIMIT__\n1000000\n"
    probe = probes_for((f,), c, observe=c.observe)[f.name]
    assert probe() is False  # Missing actual cgroup usage is unknown headroom.
    c.values["valkey_memory"] += "__SDO_USAGE__\n990000\n"
    assert probe() is False
    c.values["valkey_memory"] = c.values["valkey_memory"].replace("990000", "50000")
    assert probe() is True


def mongo_source(username, password):
    import json

    return (
        "package main\nfunc initializeDatabase(url string) *mgo.Session {\n"
        f"adminUsername := {json.dumps(username)}\nadminPassword := {json.dumps(password)}\n"
        'url = "mongodb://" + adminUsername + ":" + adminPassword + "@" + url\n}\n'
    )


@pytest.mark.parametrize(
    ("component", "operation"), [("mongodb-geo", "mongo_geo_read"), ("mongodb-rate", "mongo_rate_read")]
)
def test_mongo_credentials_are_private_stdin_and_never_results_or_reports(component, operation, tmp_path):
    import json
    import subprocess

    from benchmarks.sregym.fastloop.composite import write_composite_report
    from benchmarks.sregym.fastloop.fault_tracker import FaultTracker
    from benchmarks.sregym.fastloop.runtime_observation import KubectlObservationTransport

    username = "GENERATED_USER_CANARY"
    password = 'GENERATED_PASSWORD_CANARY_"_$()'
    requests = []

    def source(target):
        requests.append(target)
        return mongo_source(username, password)

    def run(command, **kwargs):
        assert username not in repr(command)
        assert password not in repr(command)
        assert json.dumps(username) in kwargs["input"]
        assert json.dumps(password) in kwargs["input"]
        assert "-i" in command
        assert "db.auth" in kwargs["input"]
        assert "runCommand" in kwargs["input"]
        assert kwargs["input"].index("db.auth") < kwargs["input"].index("runCommand")
        assert "find:" in kwargs["input"]
        assert "maxTimeMS: 3000" in kwargs["input"]
        return subprocess.CompletedProcess(command, 0, username + "\n__SDO_MONGO_READ_OK__\n", password)

    c = Cluster(component)
    f = fault("MongoDBRevokeAuth", component)
    transport = KubectlObservationTransport(namespace="app", run=run, read_mongo_source=source)
    tracker = FaultTracker(probes_for((f,), c, observe=transport), stable_seconds=0, clock=lambda: 0)
    assert tracker.poll().states == {f.name: True}
    assert requests == [component]
    report = {"timeline": tracker.timeline(), "resolved_s": tracker.resolution_times()}
    path = tmp_path / "report.json"
    write_composite_report(path, report)
    for value in (repr(report), path.read_text()):
        assert username not in value
        assert password not in value


@pytest.mark.parametrize("failure", ["exit", "os", "timeout", "decode", "unexpected"])
def test_mongo_errors_detach_private_exception_chains_and_reports(failure, tmp_path):
    import subprocess
    import traceback

    from benchmarks.sregym.fastloop.composite import write_composite_report
    from benchmarks.sregym.fastloop.fault_tracker import FaultTracker
    from benchmarks.sregym.fastloop.runtime_observation import (
        KubectlObservationTransport,
        ObservationUnavailable,
        ReadOnlyObservation,
    )

    canary = "GENERATED_PRIVATE_CANARY"

    def run(command, **kwargs):
        assert canary not in repr(command)
        assert canary in kwargs["input"]
        if failure == "os":
            raise OSError(canary)
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 10, output=canary, stderr=canary)
        if failure == "decode":
            raise UnicodeDecodeError("utf-8", b"invalid", 0, 1, canary)
        if failure == "unexpected":
            raise RuntimeError(canary)
        return subprocess.CompletedProcess(command, 1, canary, canary)

    transport = KubectlObservationTransport(
        namespace="app",
        run=run,
        read_mongo_source=lambda _: mongo_source(canary, canary),
    )
    request = ReadOnlyObservation("mongo_geo_read", "app-pod", "app")
    with pytest.raises(ObservationUnavailable) as caught:
        transport(request)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert canary not in "".join(traceback.format_exception(caught.value))
    tracker = FaultTracker({"fault": lambda: bool(transport(request))}, clock=lambda: 0)
    assert tracker.poll().states == {"fault": False}
    path = tmp_path / "report.json"
    write_composite_report(path, {"timeline": tracker.timeline()})
    assert canary not in path.read_text()


def test_source_read_failure_is_private_and_never_retains_a_cause():
    from benchmarks.sregym.fastloop.runtime_observation import (
        KubectlObservationTransport,
        ObservationUnavailable,
        ReadOnlyObservation,
    )

    def source(_):
        raise ValueError("GENERATED_PRIVATE_SOURCE_CANARY")

    transport = KubectlObservationTransport(namespace="app", read_mongo_source=source)
    with pytest.raises(ObservationUnavailable) as caught:
        transport(ReadOnlyObservation("mongo_geo_read", "app-pod", "app"))
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert "GENERATED_PRIVATE_SOURCE_CANARY" not in str(caught.value)


def test_mongo_source_is_bound_to_both_gitlinks_not_app_head_or_worktree(tmp_path):
    import subprocess

    from benchmarks.sregym.fastloop.runtime_observation import KubectlObservationTransport, ReadOnlyObservation

    root = tmp_path / "repo"
    harness = root / "third_party/sregym"
    apps = harness / "SREGym-applications"

    def git(directory, *args):
        return subprocess.run(
            ["git", "-C", str(directory), *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()

    def initialize(directory):
        directory.mkdir(parents=True, exist_ok=True)
        git(directory, "init", "--quiet")

    def commit(directory):
        git(
            directory,
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--quiet",
            "-m",
            "synthetic source fixture",
        )
        return git(directory, "rev-parse", "HEAD")

    initialize(apps)
    source = apps / "hotelReservation/cmd/geo/db.go"
    source.parent.mkdir(parents=True)
    source.write_text(mongo_source("PINNED_USER_CANARY", "PINNED_PASSWORD_CANARY"))
    git(apps, "add", "--", "hotelReservation/cmd/geo/db.go")
    app_pin = commit(apps)
    source.write_text(mongo_source("FLOATING_USER_CANARY", "FLOATING_PASSWORD_CANARY"))
    git(apps, "add", "--", "hotelReservation/cmd/geo/db.go")
    commit(apps)
    source.write_text(mongo_source("DIRTY_USER_CANARY", "DIRTY_PASSWORD_CANARY"))
    initialize(harness)
    git(harness, "update-index", "--add", "--cacheinfo", f"160000,{app_pin},SREGym-applications")
    harness_pin = commit(harness)
    initialize(root)
    git(root, "update-index", "--add", "--cacheinfo", f"160000,{harness_pin},third_party/sregym")
    commit(root)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        if command[0] == "git":
            return subprocess.run(command, **kwargs)
        assert "PINNED_USER_CANARY" in kwargs["input"]
        assert "PINNED_PASSWORD_CANARY" in kwargs["input"]
        assert "FLOATING_" not in kwargs["input"]
        assert "DIRTY_" not in kwargs["input"]
        assert "CANARY" not in repr(command)
        return subprocess.CompletedProcess(command, 0, "__SDO_MONGO_READ_OK__\n")

    transport = KubectlObservationTransport(namespace="app", run=run, source_root=root)
    assert transport(ReadOnlyObservation("mongo_geo_read", "app-pod", "app")) == "__SDO_MONGO_READ_OK__"
    assert ["git", "-C", str(harness), "ls-tree", harness_pin, "--", "SREGym-applications"] in calls
    assert ["git", "-C", str(apps), "show", f"{app_pin}:hotelReservation/cmd/geo/db.go"] in calls
    # Uninitialized submodules can silently resolve the parent's Git root.
    import shutil

    shutil.rmtree(apps / ".git")
    from benchmarks.sregym.fastloop.runtime_observation import ObservationUnavailable

    calls.clear()
    with pytest.raises(ObservationUnavailable):
        transport(ReadOnlyObservation("mongo_geo_read", "app-pod", "app"))
    assert all(command[0] == "git" for command in calls)


@pytest.mark.parametrize("variant", ["missing", "duplicate", "expression", "unrelated"])
def test_unsupported_source_never_runs_a_mongo_query(variant):
    from benchmarks.sregym.fastloop.runtime_observation import (
        KubectlObservationTransport,
        ObservationUnavailable,
        ReadOnlyObservation,
    )

    source = mongo_source("SOURCE_USER_CANARY", "SOURCE_PASSWORD_CANARY")
    if variant == "missing":
        source = "package main"
    elif variant == "duplicate":
        source += '\nadminPassword := "ANOTHER_CANARY"\n'
    elif variant == "expression":
        source = source.replace('"SOURCE_PASSWORD_CANARY"', 'os.Getenv("PRIVATE_VARIABLE")')
    else:
        source = source.replace('"mongodb://"', '"different-protocol://"')
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        raise AssertionError("unsupported source must fail before exec")

    transport = KubectlObservationTransport(namespace="app", run=run, read_mongo_source=lambda _: source)
    with pytest.raises(ObservationUnavailable) as caught:
        transport(ReadOnlyObservation("mongo_geo_read", "app-pod", "app"))
    assert caught.value.__context__ is None
    assert "CANARY" not in str(caught.value)
    assert calls == []


def test_tracker_baseline_retries_complete_kafka_capture(monkeypatch):
    from functools import partial

    from benchmarks.sregym.fastloop import runtime_observation
    from benchmarks.sregym.fastloop.composite import CompositeSettings, make_tracker

    f = fault("KafkaQueueProblems", "kafka")
    c = Cluster("kafka")
    c.values["kafka_offsets"] = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\ngroup topic 0 10 10 0\n"
    observe = c.observe
    attempts = []

    def flaky(request):
        attempts.append(request)
        if len(attempts) < 3:
            raise runtime_observation.ObservationUnavailable("transient")
        return observe(request)

    c.observe = flaky
    clock = [0.0]
    monkeypatch.setattr(
        runtime_observation,
        "baseline_observation",
        partial(
            runtime_observation.baseline_observation,
            monotonic=lambda: clock[0],
            sleep=lambda t: clock.__setitem__(0, clock[0] + t),
        ),
    )
    tracker = make_tracker("synthetic", c, CompositeSettings(), faults=(f,))
    assert tracker is not None
    assert tracker.poll().states[f.name] is True
    assert len(attempts) == 4
