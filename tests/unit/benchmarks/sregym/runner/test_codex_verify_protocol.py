"""The Codex baseline's verify protocol: config, prompt text and its route to SREGym.

User direction (2026-09-28): the default baseline verifies its work,
concisely. ``verify_protocol`` is ``"concise"`` by default, with ``"full"``
(the original, longer step-by-step protocol, used to isolate the verify loop
from memory) and ``"none"`` (the unmodified stock instruction) as opt-ins.
The legacy booleans (``true``/``false``) still work, for old TOMLs and
snapshots: ``true`` maps to ``"full"``, ``false`` to ``"none"``.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest

from benchmarks.sregym.runner.codex_baseline import (
    ALLOW_EXEC_ENV,
    CONCISE_VERIFY_PROMPT,
    EXEC_DISCLOSURE_PROMPT,
    FULL_VERIFY_PROMPT,
    PROMPT_APPENDIX_ENV,
    VERIFY_PROTOCOL_PROMPT,
    CodexBaselineConfig,
)
from benchmarks.sregym.runner.experiment import (
    ExperimentConfig,
    _serialize_config,
    config_to_env,
    load_experiment_config,
)

if TYPE_CHECKING:
    from pathlib import Path


def _codex(agent_config: dict | None = None) -> ExperimentConfig:
    return ExperimentConfig(agent="codex", model="gpt-6-luna", agent_config=agent_config or {})


def test_the_protocol_defaults_to_concise() -> None:
    assert CodexBaselineConfig().verify_protocol == "concise"
    assert CodexBaselineConfig().prompt_appendix() == CONCISE_VERIFY_PROMPT


def test_full_verify_protocol_prompt_is_the_legacy_prompt_text() -> None:
    assert VERIFY_PROTOCOL_PROMPT == FULL_VERIFY_PROMPT
    assert CodexBaselineConfig(verify_protocol="full").prompt_appendix() == FULL_VERIFY_PROMPT


def test_none_mode_gives_the_stock_prompt() -> None:
    assert CodexBaselineConfig(verify_protocol="none").prompt_appendix() == ""


@pytest.mark.parametrize(("legacy", "mode"), [(True, "full"), (False, "none")])
def test_the_legacy_booleans_map_to_full_and_none(legacy: bool, mode: str) -> None:
    assert CodexBaselineConfig(verify_protocol=legacy).verify_protocol == mode
    assert CodexBaselineConfig.from_agent_config({"verify_protocol": legacy}).verify_protocol == mode


def test_the_protocol_config_rejects_unknown_keys() -> None:
    with pytest.raises(ValueError, match="verify_protocl"):
        CodexBaselineConfig.from_agent_config({"verify_protocl": True})


def test_the_protocol_config_rejects_an_unknown_mode() -> None:
    with pytest.raises(ValueError, match="verify_protocol"):
        CodexBaselineConfig.from_agent_config({"verify_protocol": "yes"})


def test_the_protocol_config_rejects_a_non_string_non_boolean_mode() -> None:
    with pytest.raises(TypeError, match="verify_protocol"):
        CodexBaselineConfig.from_agent_config({"verify_protocol": 1})


def test_an_invalid_codex_agent_config_fails_at_load(tmp_path: Path) -> None:
    source = tmp_path / "exp.toml"
    source.write_text('[runner]\nagent = "codex"\n\n[agent.codex]\nverify_protocl = true\n', encoding="utf-8")

    with pytest.raises(ValueError, match="verify_protocl"):
        load_experiment_config(source)


@pytest.mark.parametrize(
    "requirement",
    [
        "confirm the user-facing symptom is actually gone",
        "end-to-end check",
        "not just pod status",
        "delete any helper pods or Jobs you created",
    ],
)
def test_the_concise_prompt_states_each_requirement(requirement: str) -> None:
    assert requirement in CONCISE_VERIFY_PROMPT


@pytest.mark.parametrize(
    "requirement",
    [
        # (a) reproduce a user-facing symptom first and record it
        "reproduce a concrete user-facing symptom",
        "end-to-end request",
        "component that is unreachable",
        "exact command you ran and its output",
        # (b) evidence tying the root cause to the symptom
        "evidence that ties the root cause to that symptom",
        # (c) re-run the same check after the fix until it passes, then submit
        "re-run the same symptom check",
        "until it passes",
        "Only then submit the mitigation",
        # (d) delete helper pods and Jobs before submitting
        "delete every helper pod, Job",
    ],
)
def test_the_full_protocol_prompt_states_each_requirement(requirement: str) -> None:
    assert requirement in FULL_VERIFY_PROMPT


@pytest.mark.parametrize(
    "hint", ["configmap", "failure-admin", "mongo", "hotel", "decoy", "red herring", "geo", "rate", "selector"]
)
def test_neither_protocol_prompt_names_a_fault_or_decoy(hint: str) -> None:
    assert hint not in CONCISE_VERIFY_PROMPT.lower()
    assert hint not in FULL_VERIFY_PROMPT.lower()


def test_the_concise_prompt_does_not_prescribe_how_to_reproduce_the_symptom() -> None:
    """The full protocol's step 1 anchored the agent on a healthy-endpoint check; concise must not."""
    assert "reproduce" not in CONCISE_VERIFY_PROMPT.lower()
    assert "kubectl get pods" not in CONCISE_VERIFY_PROMPT.lower()


def test_verify_protocol_reaches_sregym_as_the_prompt_appendix(tmp_path: Path) -> None:
    env = config_to_env(_codex(), tmp_path)
    assert env[PROMPT_APPENDIX_ENV] == CONCISE_VERIFY_PROMPT

    env = config_to_env(_codex({"codex": {"verify_protocol": "full"}}), tmp_path)
    assert env[PROMPT_APPENDIX_ENV] == FULL_VERIFY_PROMPT

    env = config_to_env(_codex({"codex": {"verify_protocol": True}}), tmp_path)
    assert env[PROMPT_APPENDIX_ENV] == FULL_VERIFY_PROMPT


def test_the_stock_baseline_never_inherits_a_stray_appendix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PROMPT_APPENDIX_ENV, "leaked from the shell")

    env = config_to_env(_codex({"codex": {"verify_protocol": "none"}}), tmp_path)
    assert PROMPT_APPENDIX_ENV not in env

    env = config_to_env(_codex({"codex": {"verify_protocol": False}}), tmp_path)
    assert PROMPT_APPENDIX_ENV not in env

    env = config_to_env(ExperimentConfig(agent="sdo_codex"), tmp_path)
    assert PROMPT_APPENDIX_ENV not in env


def test_verify_protocol_survives_the_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "exp.toml"
    source.write_text(
        textwrap.dedent(
            """
            [runner]
            agent = "codex"

            [agent.codex]
            verify_protocol = "full"
            """
        ),
        encoding="utf-8",
    )
    snapshot = tmp_path / "snapshot.toml"
    snapshot.write_text(_serialize_config(load_experiment_config(source)), encoding="utf-8")

    assert load_experiment_config(snapshot).agent_config["codex"] == {"verify_protocol": "full"}


# --------------------------------------------------------------------------- exec disclosure


def test_the_exec_disclosure_is_off_unless_asked_for() -> None:
    assert CodexBaselineConfig().exec_disclosure is False
    assert CodexBaselineConfig().prompt_appendix() == CONCISE_VERIFY_PROMPT


def test_the_exec_disclosure_is_its_own_block_after_the_verify_text() -> None:
    concise = CodexBaselineConfig(exec_disclosure=True).prompt_appendix()
    assert concise == CONCISE_VERIFY_PROMPT + "\n" + EXEC_DISCLOSURE_PROMPT

    stock = CodexBaselineConfig(verify_protocol="none", exec_disclosure=True).prompt_appendix()
    assert stock == EXEC_DISCLOSURE_PROMPT


@pytest.mark.parametrize("verb", ["kubectl exec", "attach", "port-forward", "cp"])
def test_the_exec_disclosure_names_every_unavailable_verb(verb: str) -> None:
    assert verb in EXEC_DISCLOSURE_PROMPT


@pytest.mark.parametrize("alternative", ["Kubernetes API", "logs", "HTTP requests from a helper pod"])
def test_the_exec_disclosure_names_the_neutral_alternatives(alternative: str) -> None:
    assert alternative in EXEC_DISCLOSURE_PROMPT


def test_the_exec_disclosure_rejects_a_non_boolean() -> None:
    with pytest.raises(TypeError, match="exec_disclosure"):
        CodexBaselineConfig(exec_disclosure="yes")  # type: ignore[arg-type]


def test_the_exec_disclosure_reaches_sregym_in_the_appendix(tmp_path: Path) -> None:
    env = config_to_env(_codex({"codex": {"verify_protocol": "concise", "exec_disclosure": True}}), tmp_path)
    assert env[PROMPT_APPENDIX_ENV].endswith(EXEC_DISCLOSURE_PROMPT)


# --------------------------------------------------------------------------- exec parity


def test_exec_through_the_agent_proxy_is_off_unless_asked_for(tmp_path: Path) -> None:
    assert CodexBaselineConfig().allow_exec is False
    env = config_to_env(_codex({"codex": {"verify_protocol": "concise"}}), tmp_path)
    assert ALLOW_EXEC_ENV not in env


def test_allow_exec_reaches_sregym_as_the_proxy_switch(tmp_path: Path) -> None:
    env = config_to_env(_codex({"codex": {"verify_protocol": "concise", "allow_exec": True}}), tmp_path)
    assert env[ALLOW_EXEC_ENV] == "1"
    assert ALLOW_EXEC_ENV == "SREGYM_AGENT_PROXY_ALLOW_EXEC"


def test_allow_exec_does_not_change_the_prompt() -> None:
    assert CodexBaselineConfig(allow_exec=True).prompt_appendix() == CONCISE_VERIFY_PROMPT


def test_a_switch_left_in_the_shell_never_enables_exec_for_an_arm_that_did_not_ask(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(ALLOW_EXEC_ENV, "1")
    assert ALLOW_EXEC_ENV not in config_to_env(_codex({"codex": {"verify_protocol": "concise"}}), tmp_path)
    assert ALLOW_EXEC_ENV not in config_to_env(ExperimentConfig(agent="sdo_codex", model="m"), tmp_path)


def test_allow_exec_rejects_a_non_boolean() -> None:
    with pytest.raises(TypeError, match="allow_exec"):
        CodexBaselineConfig(allow_exec="yes")  # type: ignore[arg-type]


def test_no_codex_baseline_prompt_carries_a_benchmark_tailored_token() -> None:
    import re

    from tests.unit.test_benchmark_neutrality import TAILORED_TOKENS

    for text in (CONCISE_VERIFY_PROMPT, FULL_VERIFY_PROMPT, EXEC_DISCLOSURE_PROMPT):
        for pattern, why in TAILORED_TOKENS:
            assert not re.search(pattern, text, re.IGNORECASE), why
