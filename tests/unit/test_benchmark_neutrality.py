"""SDO's prompts, templates, and benchmark goal text carry no benchmark-tailored knowledge.

SDO must discover fault-relevant checks from the application and its objective. These
tokens were chosen because they name, or were written against, the SREGym phase-1
faults (missing ConfigMap, wrong Service selector, deny-all NetworkPolicy, and the
composites K1/K2), their decoy ConfigMaps, or the hotel-reservation application. Each
one appeared in an SDO prompt or template before it was removed. A generic operational mechanism is allowed; a hint that
only makes sense once you know the benchmark's problems is not.

Keep the list focused: add a token only when it identifies a specific benchmark fault,
decoy, or application, and document why next to it.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import pytest

from benchmarks.sregym.adapter.driver import _deployed_lifecycle_context

ROOT = Path(__file__).resolve().parents[2]

#: Source trees whose text reaches an SDO agent prompt, a lifecycle template, the
#: controller's detectors, or the benchmark adapter's goal and responder text.
SCANNED_ROOTS = (
    "sdo",
    "sdo/skills",
    "sdo/operator/sdk",
    "sdo/operator/core",
    "sdo/operator/runtime",
    "sdo/operator/builder",
    "benchmarks/sregym/adapter",
)
SCANNED_SUFFIXES = {".py", ".go", ".yaml", ".yml", ".md", ".txt", ".tmpl"}

#: (pattern, why it is tailored). Patterns are case-insensitive regular expressions.
TAILORED_TOKENS: tuple[tuple[str, str], ...] = (
    (r"network-policy-total-isolation", "rule id matching SREGym's injected deny-all NetworkPolicy exactly"),
    (r"deny-all", "name of SREGym's injected NetworkPolicy (network_policy_block, K1)"),
    (r"missing-configmap finding", "health-judge instruction naming the missing_configmap fault"),
    (r"failure-admin", "SREGym's decoy ConfigMaps"),
    (r"mongo-geo-script|mongo-rate-script", "the ConfigMaps SREGym deletes in S1 and K1"),
    (
        r"missing_configmap|wrong_service_selector|network_policy_block|readiness_probe_misconfiguration"
        r"|composite_policy_and_rate|composite_frontend_selector",
        "SREGym problem ids",
    ),
    (r"however suspicious", "state-diff steer written against the decoy ConfigMaps"),
    (r"kubelet mount backoff", "repair tip for the missing_configmap fault"),
    (r"delete networkpolicies", "permission text shaped around the network_policy_block repair"),
    (r"/hotels\b|\binDate\b|\boutDate\b|/recommendations\b", "hotel-reservation paths in generic prompts"),
)


def _scanned_files() -> list[Path]:
    files: list[Path] = []
    for relative in SCANNED_ROOTS:
        base = ROOT / relative
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.suffix not in SCANNED_SUFFIXES:
                continue
            # Tests may name benchmark faults; they never reach a prompt or a detector.
            # Skill examples are the exception: agents read them, tests included.
            in_skills = "skills" in path.relative_to(ROOT).parts[:2]
            if (
                (path.name.endswith("_test.go") and not in_skills)
                or "testdata" in path.parts
                or "__pycache__" in path.parts
            ):
                continue
            files.append(path)
    return files


def test_scan_covers_the_prompt_builders_and_templates() -> None:
    scanned = {path.relative_to(ROOT).as_posix() for path in _scanned_files()}

    for required in (
        "sdo/agents/lifecycle/agents.py",
        "sdo/agents/health_judge/worker.py",
        "sdo/agents/responder/codex.py",
        "sdo/agents/responder/reflection.py",
        "sdo/skills/sdo-memory/SKILL.md",
        "sdo/skills/sdo-detector-authoring/SKILL.md",
        "sdo/skills/sdo-detector-authoring/references/sdk.md",
        "sdo/skills/sdo-detector-authoring/references/rules.md",
        "sdo/skills/sdo-playbook-authoring/SKILL.md",
        "sdo/skills/sdo-playbook-authoring/references/rules.md",
        "sdo/skills/sdo-playbook-authoring/examples/restart-loop/README.md",
        "sdo/skills/sdo-detector-authoring/examples/restart-loop/detector_test.go",
        "sdo/operator/runtime/deploy/rbac.yaml",
        "benchmarks/sregym/adapter/driver.py",
        "benchmarks/sregym/adapter/runtime.py",
    ):
        assert required in scanned


@pytest.mark.parametrize(("pattern", "reason"), TAILORED_TOKENS, ids=[token for token, _ in TAILORED_TOKENS])
def test_sdo_sources_carry_no_benchmark_tailored_token(pattern: str, reason: str) -> None:
    expression = re.compile(pattern, re.IGNORECASE)
    hits = [
        f"{path.relative_to(ROOT)}:{number}: {line.strip()}"
        for path in _scanned_files()
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1)
        if expression.search(line)
    ]

    assert not hits, f"benchmark-tailored token ({reason}) reappeared:\n" + "\n".join(hits)


def test_adapter_goal_names_only_observed_topology_and_user_outcomes() -> None:
    payload = {
        "items": [
            {"kind": "Deployment", "metadata": {"name": "frontend"}, "spec": {"template": {"spec": {}}}},
            {"kind": "Service", "metadata": {"name": "frontend"}, "spec": {}},
            {"kind": "ConfigMap", "metadata": {"name": "frontend-config"}},
        ]
    }

    def fake_runner(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")

    objective = _deployed_lifecycle_context("demo", command_runner=fake_runner).health_objective

    # The objective states outcomes; it does not enumerate fault classes to check.
    assert "ConfigMap" not in objective
    assert "NetworkPolicy" not in objective
    assert "selector" not in objective.lower()


def test_adapter_responder_instructions_ask_for_user_outcomes_not_a_fault_specific_check() -> None:
    from benchmarks.sregym.adapter.runtime import _responder_instructions

    text = _responder_instructions()

    assert "ready endpoint" not in text
    assert "verify representative requests" in text


@pytest.mark.parametrize("mode", ["concise", "full"])
def test_codex_baseline_verify_prompts_carry_no_benchmark_tailored_token(mode: str) -> None:
    """The baseline arm's verify text must be as neutral as SDO's prompts, or the comparison is unfair."""
    from benchmarks.sregym.runner.codex_baseline import CONCISE_VERIFY_PROMPT, FULL_VERIFY_PROMPT

    text = {"concise": CONCISE_VERIFY_PROMPT, "full": FULL_VERIFY_PROMPT}[mode]
    hits = [pattern for pattern, _ in TAILORED_TOKENS if re.search(pattern, text, re.IGNORECASE)]

    assert not hits, f"benchmark-tailored tokens in the {mode} verify prompt: {hits}"
    for fault_class in ("configmap", "networkpolicy", "selector", "readiness"):
        assert fault_class not in text.lower()
