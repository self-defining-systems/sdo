"""Fixed-memory repeat policy; all remote selection stays benchmark-owned."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable

    from benchmarks.sregym.fastloop.environment import FastloopEnvironment


@dataclass(frozen=True)
class ArmMemory:
    arm: str
    repository: str
    branch: str
    commit: str


def configure_arm(
    args: argparse.Namespace,
    environment: FastloopEnvironment,
    *,
    git: Callable[..., str] | None = None,
) -> ArmMemory:
    """Fetch the chosen variant and pin the ordinary row-start reset to its SHA."""
    if args.agent != "sdo":
        raise ValueError("ablation arms require the SDO agent")
    if environment.shared_accepted_id:
        raise ValueError("ablation arms require private memory, without shared writers")
    arm = args.ablation_arm
    if arm not in ("full", "detectors-only", "playbooks-only"):
        raise ValueError(f"unsupported ablation arm: {arm}")
    app = environment.problem_application.replace("_", "-")
    if app == "opentelemetry-demo-astronomy-shop":
        app = "astronomy-shop"
    repository = getattr(args, "ablation_repository", None)
    if repository is None:
        if app not in ("hotel-reservation", "social-network", "astronomy-shop"):
            raise ValueError("this app requires an explicit --ablation-repository")
        repository = f"git@github.com:vibesys-playground/{app}-sdo.git"
    branch = getattr(args, "ablation_branch", None) or f"ablate/20261103-{arm}"
    if git is None:

        def run_git(*arguments: str) -> str:
            return subprocess.run(
                host_git_command(environment.workspace, *arguments),
                check=True,
                capture_output=True,
                text=True,
                timeout=wait_for(WaitKind.SLOW_COMMAND),
            ).stdout.strip()

        git = run_git

    git("fetch", "--no-tags", repository, branch)
    commit = git("rev-parse", "FETCH_HEAD^{commit}")
    if args.planned_commit and args.planned_commit != commit:
        raise ValueError("--planned-commit conflicts with selected ablation variant")
    # The variant is the row's input, not a controller-authored setup commit.
    # Restore and lineage admission must use the same revision as the row-start wipe.
    environment.seed_commit = commit
    args.planned_commit = commit
    args.external_incidents = arm == "playbooks-only"
    args.frozen_memory = True
    args.replay = "off"
    return ArmMemory(arm, repository, branch, commit)
