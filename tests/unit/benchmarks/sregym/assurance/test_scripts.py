"""The assurance catalog compiles to the public agent-script contract."""

import pytest

from benchmarks.sregym.assurance.scripts import assurance_script


@pytest.mark.parametrize(
    ("fault", "target"), [("network_policy_block", "recommendation"), ("missing_configmap", "mongodb-geo")]
)
@pytest.mark.parametrize(
    "mitigation", ["correct", "wrong_then_correct", "wrong_only_honest", "wrong_only_claimed", "healed_noop"]
)
@pytest.mark.parametrize("reflection", ["learn", "no_change", "crash_once", "crash_always", "invalid_then_learn"])
def test_all_modes_compile(fault: str, target: str, mitigation: str, reflection: str) -> None:
    script = assurance_script(fault=fault, target=target, mitigation=mitigation, reflection=reflection)
    assert script.select("responder", "incident", 0)
    assert script.select("reflector", "incident", 0)


def test_chaos_windows_and_step_times_survive_compilation() -> None:
    script = assurance_script(
        fault="network_policy_block",
        target="recommendation",
        pause_before_repair_seconds=45,
        pause_before_result_seconds=20,
    )
    turn = script.select("responder", "incident", 0)
    assert [(s.kind, getattr(s, "seconds", None)) for s in turn.steps if s.kind == "wait"] == [
        ("wait", 45),
        ("wait", 20),
    ]
    action = turn.result["repair_actions"][0]
    assert action["started_at"].startswith("{{steps.")
    assert action["completed_at"].startswith("{{steps.")


def test_warm_scripts_run_the_published_playbook() -> None:
    for fault, target in [("network_policy_block", "recommendation"), ("missing_configmap", "mongodb-rate")]:
        turn = assurance_script(fault=fault, target=target, warm=True).select("responder", "i", 0)
        assert any("/scripts/repair.sh" in getattr(step, "command", "") for step in turn.steps)
        assert turn.result["applied_playbooks"]


def test_healed_script_has_no_mutation_or_cause() -> None:
    turn = assurance_script(fault="network_policy_block", target="recommendation", mitigation="healed_noop").select(
        "responder", "i", 0
    )
    assert turn.result["repair_actions"] == []
    assert turn.result["confirmed_root_causes"] == []
    assert len(turn.steps) == 1
