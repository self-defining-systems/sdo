"""Hand-written SG2 regressions; no cluster or recorded experiment outputs."""

from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop.fault_spec_worker import describe_problem
from benchmarks.sregym.faults.description import ProblemDescription, validate_suite


def part(class_name, target="geo"):
    return type(
        class_name,
        (),
        {
            "faulty_service": target,
            "namespace": "hotel-reservation",
            "inject_fault": lambda self: None,
            "recover_fault": lambda self: None,
            "mitigation_oracle": object(),
        },
    )()


def test_colocated_description_preserves_declared_indices_and_injection_order():
    problem = SimpleNamespace(
        problems=[part("MongoDBUserUnregistered"), part("IncorrectImage", "search")],
        draws=[
            SimpleNamespace(key="storage_user_unregistered", param_dict=lambda: {"mode": "bad"}, footprints=()),
            None,
        ],
        injection_order=[1, 0],
    )
    described = describe_problem("generated", problem)
    assert described.schema_version == "sdo.sregym-fault-description/v1"
    assert [fault.declared_index for fault in described.faults] == [0, 1]
    assert described.injection_order == (1, 0)
    assert described.faults[0].parameters == {"mode": "bad"}
    assert not described.tracking_supported
    assert described.faults[0].class_name == "MongoDBUserUnregistered"
    assert ProblemDescription.model_validate_json(described.model_dump_json()) == described
    validate_suite({"generated": described})  # aggregate oracle remains available
    with pytest.raises(ValueError, match="tracking.*MongoDBUserUnregistered"):
        validate_suite({"generated": described}, require_tracking=True)


def test_old_composite_excludes_decoys_without_renumbering_faults():
    problem = SimpleNamespace(
        components=[part("IncorrectImage"), part("Decoy"), part("WrongServiceSelector")], fault_indices=(0, 2)
    )
    described = describe_problem("old", problem)
    assert [fault.declared_index for fault in described.faults] == [0, 2]
    assert described.injection_order == (0, 2)
    assert described.tracking_supported


@pytest.mark.parametrize(
    ("class_name", "resource"),
    [
        ("PSARestrictedBlocksRecreation", "Namespace"),
        ("NamespaceMemoryLimit", "ResourceQuota"),
        ("AdmissionWebhookTLSMismatch", "Webhook"),
    ],
)
def test_suite_rejects_unobservable_unauthorized_repair_scope(class_name, resource):
    described = describe_problem("unsupported", part(class_name))
    with pytest.raises(ValueError, match=resource):
        validate_suite({"unsupported": described})


def test_unknown_class_fails_closed_for_capabilities_but_can_be_described():
    described = describe_problem("new", part("UnknownFutureFault"))
    assert not described.tracking_supported
    with pytest.raises(ValueError, match="unreviewed"):
        validate_suite({"new": described})


MIXED10_COMPOSITES = (
    "mf__hotel_reservation__n2__s2596534647__v_",
    "mf__hotel_reservation__n2__s3346820597__v_",
    "mf__hotel_reservation__n2__s2834477618__v_",
    "mf__hotel_reservation__n2__s1773744812__v_",
    "mf__hotel_reservation__n2__s3830920265__v_",
    "mf__hotel_reservation__n2__s3801549391__v_",
    "mf__hotel_reservation__n2__s1085951852__v_",
)


def test_real_sdo3_discovery_accepts_all_seven_sg2_composites():
    from pathlib import Path

    from benchmarks.sregym.fastloop.fault_tracker import describe_sregym_problems

    root = Path(__file__).resolve().parents[5]
    catalog = describe_sregym_problems(MIXED10_COMPOSITES, sregym_dir=root / "third_party/sregym")
    assert set(catalog) == set(MIXED10_COMPOSITES)
    assert all(len(item.faults) == 2 and item.composite for item in catalog.values())
    for item in catalog.values():
        if item.tracking_supported:
            validate_suite({item.problem_id: item}, require_tracking=True, capability_mode="permissive")
        else:
            with pytest.raises(ValueError, match="tracking unsupported"):
                validate_suite({item.problem_id: item}, require_tracking=True, capability_mode="permissive")


def test_adding_fault_indices_to_colocated_does_not_lose_draws():
    problem = SimpleNamespace(problems=[part("WrongBinUsage")], draws=[None], injection_order=[0], fault_indices=(0,))
    assert describe_problem("generated", problem).faults[0].class_name == "WrongBinUsage"


def test_proposed_twelve_incident_suite_repeats_are_inside_current_sdo_scope():
    from pathlib import Path

    from benchmarks.sregym.fastloop.fault_tracker import describe_sregym_problems
    from benchmarks.sregym.fastloop.stream import load_stream

    root = Path(__file__).resolve().parents[5]
    stream = load_stream(root / "tests/fixtures/sregym/fastloop/sdo3_initial.json")
    catalog = describe_sregym_problems(tuple(ids[0] for ids in stream), sregym_dir=root / "third_party/sregym")
    validate_suite(catalog)
    assert len(stream) == 12
    assert stream[-2:] == (("missing_configmap_hotel_reservation",), ("wrong_service_selector_hotel_reservation",))
    assert len(set(stream[:10])) == 10
    for position in (10, 11):
        assert position - stream.index(stream[position]) >= 3
        validate_suite({stream[position][0]: catalog[stream[position][0]]})
    assert sum(item.composite for item in catalog.values()) == 5


@pytest.mark.parametrize("stream_mode", [False, True])
def test_unsupported_requested_tracking_fails_before_worker_deployment(tmp_path, monkeypatch, stream_mode):
    from benchmarks.sregym.fastloop import cli
    from benchmarks.sregym.fastloop.environment import FastloopEnvironment

    environment = FastloopEnvironment(
        run_dir=tmp_path,
        cluster="test",
        kubeconfig=tmp_path / "kubeconfig",
        namespace="hotel-reservation",
        application="hotel-reservation",
        workspace=tmp_path,
        sregym_dir=tmp_path,
        private_tmp=None,
    )
    monkeypatch.setattr(FastloopEnvironment, "load", lambda _: environment)
    monkeypatch.setattr(cli, "kubectl", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    problem_id = "mf__synthetic"
    monkeypatch.setattr(
        "benchmarks.sregym.fastloop.fault_tracker.describe_sregym_problems",
        lambda *args, **kwargs: {problem_id: describe_problem(problem_id, part("MongoDBUserUnregistered"))},
    )
    monkeypatch.setattr(cli, "_worker", lambda _: pytest.fail("preflight must finish before deployment"))
    arguments = ["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--require-fault-tracking"]
    if stream_mode:
        stream = tmp_path / "stream.json"
        stream.write_text('["mf__synthetic"]')
        arguments += ["--stream", str(stream)]
    else:
        arguments += ["--problems", problem_id]
    with pytest.raises(ValueError, match="tracking unsupported"):
        cli.main(arguments)


def test_real_astronomy_discovery_cannot_run_commands_or_call_the_cluster():
    import os
    import subprocess
    from pathlib import Path

    root = Path(__file__).resolve().parents[5]
    script = """
import json, socket, subprocess
from pathlib import Path
from unittest.mock import patch
from benchmarks.sregym.faults.description import ProblemDescription, validate_suite
from benchmarks.sregym.faults.worker import describe
from sregym.service.kubectl import KubeCtl
from kubernetes.client.rest import RESTClientObject

def forbidden(*args, **kwargs):
    raise AssertionError("offline discovery attempted a command or network/API call")

with (
    patch.object(subprocess, "Popen", forbidden),
    patch.object(socket.socket, "connect", forbidden),
    patch.object(KubeCtl, "exec_command", forbidden),
    patch.object(KubeCtl, "exec_command_checked", forbidden),
    patch.object(RESTClientObject, "request", forbidden),
):
    result = describe(["mf__astronomy_shop__n3__s1416840650__reliable__v_"])
    faults = next(iter(result.values()))["faults"]
    assert [f["class_name"] for f in faults] == [
        "LoadGeneratorFloodHomepage", "ResourceRequestTooLarge", "AdmissionWebhookTLSMismatch",
    ]
    assert [f["declared_index"] for f in faults] == [0, 1, 2]
    ids = json.loads(Path("../../tests/fixtures/sregym/fastloop/sdo3_permissive.json").read_text())["incidents"]
    catalog = {key: ProblemDescription.model_validate(value) for key, value in describe(ids).items()}
    evidence = validate_suite(catalog, capability_mode="permissive")
    assert len(catalog) == 10 and len(evidence.faults) == 31
    strict_failures = 0
    for key, item in catalog.items():
        try:
            validate_suite({key: item})
        except ValueError:
            strict_failures += 1
    assert strict_failures == 9
"""
    reply = subprocess.run(
        ["uv", "run", "--directory", str(root / "third_party/sregym"), "python", "-c", script],
        env={**os.environ, "PYTHONPATH": str(root), "LITELLM_LOCAL_MODEL_COST_MAP": "True"},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert reply.returncode == 0, (reply.stdout + reply.stderr)[-4000:]
