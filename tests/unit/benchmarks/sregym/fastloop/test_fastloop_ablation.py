"""Ablation policy selects fixed memory before the controller starts."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.fastloop.cli import build_parser


@pytest.mark.parametrize("arm", ["full", "detectors-only"])
def test_arm_selects_read_only_variant_and_forces_repeat_policy(arm):
    from benchmarks.sregym.fastloop.ablation import configure_arm

    args = build_parser().parse_args(["run", "--run-dir", "/tmp/run", "--agent", "sdo", "--ablation-arm", arm])
    env = SimpleNamespace(
        app="sregym", problem_application="hotel_reservation", shared_accepted_id="", workspace=Path("/workspace")
    )
    calls = []
    revision = "a" * 40

    def git(*arguments):
        calls.append(arguments)
        return revision if arguments[0] == "rev-parse" else ""

    selected = configure_arm(args, env, git=git)
    assert selected.branch == f"ablate/20261103-{arm}"
    assert selected.repository == "git@github.com:vibesys-playground/hotel-reservation-sdo.git"
    assert selected.commit == revision
    assert args.frozen_memory
    assert args.replay == "off"
    assert args.planned_commit == revision
    assert calls[0] == ("fetch", "--no-tags", selected.repository, selected.branch)
    assert not any("push" in call for call in calls)


def test_ablation_rejects_baseline_agent_before_fetch():
    from benchmarks.sregym.fastloop.ablation import configure_arm

    args = build_parser().parse_args(["run", "--run-dir", "/tmp/run", "--agent", "codex", "--ablation-arm", "full"])
    with pytest.raises(ValueError, match="SDO"):
        configure_arm(args, SimpleNamespace(), git=lambda *args: pytest.fail("must not fetch"))


@pytest.mark.parametrize(
    ("application", "repository"),
    [
        ("social-network", "social-network-sdo.git"),
        ("opentelemetry-demo-astronomy-shop", "astronomy-shop-sdo.git"),
    ],
)
def test_arm_repository_uses_problem_application(application, repository):
    from benchmarks.sregym.fastloop.ablation import configure_arm

    args = build_parser().parse_args(["run", "--run-dir", "/tmp/run", "--agent", "sdo", "--ablation-arm", "full"])
    env = SimpleNamespace(
        app="sregym", problem_application=application, shared_accepted_id="", workspace=Path("/workspace")
    )
    result = configure_arm(args, env, git=lambda *args: "a" * 40)
    assert result.repository.endswith(repository)
