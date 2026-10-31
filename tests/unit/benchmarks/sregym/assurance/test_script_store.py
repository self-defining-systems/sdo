from pathlib import Path

from benchmarks.sregym.assurance.scenarios import SCENARIOS
from benchmarks.sregym.assurance.script_store import DirectoryStore, ScriptBundle
from benchmarks.sregym.assurance.usage import KIND_OFFSETS, request_usage


def test_a_binding_is_never_replaced_by_a_later_script(tmp_path: Path) -> None:
    store = DirectoryStore(tmp_path)
    first = ScriptBundle("first", "warm", 1)
    later = ScriptBundle("later", "warm", 2)

    store.bind("inc-1", first)
    kept = store.bind("inc-1", later)

    assert kept == first
    assert store.binding("inc-1") is not None


def test_a_one_shot_marker_is_claimed_once(tmp_path: Path) -> None:
    store = DirectoryStore(tmp_path)
    assert store.claim("inc-1:crash") is True
    assert store.claim("inc-1:crash") is False


def test_synthetic_usage_is_deterministic_and_distinct_per_turn_kind() -> None:
    assert request_usage(4, "responder", 2) == request_usage(4, "responder", 2)
    kinds = {request_usage(4, kind, 0).input_tokens for kind in KIND_OFFSETS}
    assert len(kinds) == len(KIND_OFFSETS)
    usage = request_usage(4, "reflection-resume", 3)
    assert usage.cached_input_tokens <= usage.input_tokens
    assert usage.reasoning_output_tokens <= usage.output_tokens


def test_every_scenario_incident_names_a_known_fault_and_target() -> None:
    for specs in SCENARIOS.values():
        for spec in specs:
            assert spec.fault in {"network_policy_block", "missing_configmap"}
            assert spec.target
