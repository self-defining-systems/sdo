"""The latent stream resolves against a frozen SREGym catalog without a cluster."""

import json
from collections import Counter, defaultdict
from pathlib import Path

from benchmarks.sregym.fastloop.fault_tracker import FaultSpec, probes_for, stream_faults
from benchmarks.sregym.fastloop.stream import load_stream_config

ROOT = Path(__file__).resolve().parents[5]
STREAM = ROOT / "tests/fixtures/sregym/fastloop/pbprog_latent.json"
CATALOG = ROOT / "tests/fixtures/sregym/latent_fault_catalog.json"


def test_latent_stream_resolves_known_specs_and_valid_hotel_targets() -> None:
    config = load_stream_config(STREAM)
    fixture = json.loads(CATALOG.read_text(encoding="utf-8"))
    catalog = {problem_id: (FaultSpec(**entry["fault"]),) for problem_id, entry in fixture["problems"].items()}
    targets = fixture["targets"]

    assert config.diagnosis_grading == "deferred"
    assert config.judge_model_id == "codex-gpt-6-luna"
    assert 9 <= len(config.incidents) <= 12
    counts: Counter[str] = Counter()
    repeats: dict[str, set[str]] = defaultdict(set)
    for ids in config.incidents:
        assert len(ids) == 1  # A health-visible companion would mask a detection miss.
        (fault,) = stream_faults(ids, catalog=catalog)
        entry = fixture["problems"][ids[0]]
        assert entry["namespace"] == "hotel-reservation"
        assert entry["mitigation_oracle"]
        target = targets[fault.component]
        assert target["service_ports"]
        assert target["container_ports"]
        assert all(target["pod_labels"].get(key) == value for key, value in target["selector"].items())
        assert set(probes_for((fault,), lambda _kind, _name: None)) == {fault.name}
        counts[fault.kind] += 1
        if fault.kind == "target_port":
            repeats[fault.kind].add(fault.component)
            assert entry["parameters"]["bad_port"] not in target["container_ports"]

    assert counts["incorrect_image"] == counts["readiness"] == 1
    assert set(counts) == {"target_port", "incorrect_image", "readiness"}
    assert all(counts[kind] == len(components) >= 3 for kind, components in repeats.items())
