"""Judge replayed snapshots with one kit's validators, offline, and compare kits.

Each snapshot is checked out into a disposable clone and judged with ``onboarding journeys --plan``
(probes built and scheduled, nothing sent) and, when that kit's journeys does not already include it,
``memory check``. Both run in the network-less sandbox because ``--plan`` executes the agent's probe
code. Results are cached per kit and tree, so a recorded kit is judged once and a new kit only pays
for its own builds.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from benchmarks.onboarding_gate.classify import Rejection, journeys_rejections, memory_check_rejections
from benchmarks.onboarding_gate.replay import Replayed, git
from benchmarks.onboarding_gate.sandbox import Sandbox, go_binds, go_environment, host_path


@dataclass(frozen=True)
class Kit:
    """An ``sdo-operator`` binary and the operator source tree it builds probes against."""

    label: str
    binary: Path
    operator_root: Path

    def __post_init__(self) -> None:
        if not self.binary.is_file():
            raise ValueError(f"kit {self.label}: {self.binary} is not a file")
        if not (self.operator_root / "sdk").is_dir():
            raise ValueError(f"kit {self.label}: {self.operator_root} is not an operator source tree")

    @classmethod
    def from_directory(cls, directory: Path) -> Kit:
        """A session kit: ``bin/sdo-operator-real`` (or ``bin/sdo-operator``) and ``src/sdo/operator``."""
        binary = directory / "bin" / "sdo-operator-real"
        if not binary.is_file():
            binary = directory / "bin" / "sdo-operator"
        return cls(
            directory.name if directory.name != "kit" else directory.parent.name,
            binary,
            directory / "src" / "sdo" / "operator",
        )

    @classmethod
    def build(cls, repo: Path, cache: Path) -> Kit:
        """Build ``sdo-operator`` from a checkout's working tree (committed or not)."""
        head = git(repo, "rev-parse", "HEAD")
        dirty = subprocess.run(
            ["git", "-C", str(repo), "diff", "HEAD", "--", "sdo/operator"], capture_output=True, check=True
        ).stdout
        label = head[:12] + (f"+{hashlib.sha256(dirty).hexdigest()[:8]}" if dirty else "")
        binary = cache / "kits" / label / "sdo-operator"
        if not binary.is_file():
            binary.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["go", "build", "-o", str(binary), "./cmd/sdo-operator"],
                cwd=repo / "sdo" / "operator",
                env={**os.environ, "CGO_ENABLED": "0"},
                check=True,
            )
        return cls(label, binary, repo / "sdo" / "operator")

    def identity(self) -> str:
        digest = hashlib.sha256(self.binary.read_bytes()).hexdigest()[:16]
        return f"{self.label}-{digest}"


@dataclass
class Judgement:
    """A kit's raw validator output for one snapshot tree; rejections are classified on read."""

    tree: str
    plan: str = ""
    memory: str = ""
    error: str = ""
    # False for kits whose journeys has no --plan: only memory check judged the snapshot.
    plan_supported: bool = True

    @property
    def rejections(self) -> set[Rejection]:
        return journeys_rejections(self.plan) | memory_check_rejections(self.memory)

    def to_json(self) -> dict[str, object]:
        return {
            "tree": self.tree,
            "plan": self.plan,
            "memory": self.memory,
            "error": self.error,
            "plan_supported": self.plan_supported,
        }

    @classmethod
    def from_json(cls, data: dict[str, object]) -> Judgement:
        return cls(
            str(data["tree"]),
            str(data.get("plan", "")),
            str(data.get("memory", "")),
            str(data.get("error", "")),
            bool(data.get("plan_supported", True)),
        )


def judge(kit: Kit, repo: Path, commit: str, base: str, timeout: float = 300.0) -> Judgement:
    """Judge one snapshot commit of a replayed checkout with kit."""
    tree = git(repo, "rev-parse", f"{commit}^{{tree}}")
    with tempfile.TemporaryDirectory(prefix="sdo-gate-") as temp:
        root = Path(temp)
        snapshot, tmp = root / "repo", root / "tmp"
        tmp.mkdir()
        subprocess.run(["git", "clone", "-q", "--shared", "--no-checkout", str(repo), str(snapshot)], check=True)
        git(snapshot, "checkout", "-q", "--detach", commit)
        env = go_environment() | {
            "PATH": f"/gate-kit:{host_path()}",
            "HOME": "/tmp",
            "TMPDIR": "/tmp",
            "SDO_ACTOR": "health_judge",
            "SDO_PHASE": "initial",
            "SDO_BASE_COMMIT": base,
            "SDO_OPERATOR_ROOT": "/gate-operator",
            "GIT_CONFIG_GLOBAL": "/dev/null",
        }
        sandbox = Sandbox(
            binds={snapshot: "/repo", tmp: "/tmp", **go_binds(env)},
            env=env,
            read_only={kit.binary.parent: "/gate-kit", kit.operator_root: "/gate-operator"},
            chdir="/repo",
        )
        binary = f"/gate-kit/{kit.binary.name}"
        plan = sandbox.run(
            [
                binary,
                "onboarding",
                "journeys",
                "--repo",
                "/repo",
                "--namespace",
                "replay",
                "--plan",
                "--operator-root",
                "/gate-operator",
            ],
            timeout,
        )
        plan_supported = not (plan.returncode == 2 and "usage:" in plan.stdout + plan.stderr)
        if plan.returncode not in (0, 1) and plan_supported:
            return Judgement(
                tree, error=f"journeys --plan exit {plan.returncode}: {(plan.stderr or plan.stdout)[-400:]}"
            )
        memory = ""
        planned = plan.stdout if plan_supported else ""
        if not any(line.startswith(("memory check", "red memory-check")) for line in planned.splitlines()):
            # A kit whose journeys does not run memory check: run it as the agent had to.
            git(snapshot, "checkout", "-q", "--force", commit)
            git(snapshot, "clean", "-fdq")
            checked = sandbox.run([binary, "memory", "check", "--root", "/repo", "--replay", "off"], timeout)
            if checked.returncode not in (0, 1):
                return Judgement(tree, error=f"memory check exit {checked.returncode}: {checked.stdout[-400:]}")
            memory = checked.stdout
        return Judgement(tree, planned, memory, plan_supported=plan_supported)


def judge_all(
    kit: Kit, sessions: list[Replayed], cache: Path, workers: int = 8, trees: set[str] | None = None
) -> dict[str, Judgement]:
    """Judge every distinct snapshot tree of sessions (or only trees) with kit, reusing cached judgements."""
    store = cache / "judged-v2" / kit.identity()
    store.mkdir(parents=True, exist_ok=True)
    jobs: dict[str, tuple[Path, str, str]] = {}
    for session in sessions:
        for snapshot in session.snapshots:
            if trees is None or snapshot.tree in trees:
                jobs.setdefault(snapshot.tree, (Path(session.repo), snapshot.commit, session.base))
    results: dict[str, Judgement] = {}

    def run(tree: str) -> None:
        path = store / f"{tree}.json"
        if path.is_file():
            results[tree] = Judgement.from_json(json.loads(path.read_text()))
            return
        repo, commit, base = jobs[tree]
        judged = judge(kit, repo, commit, base)
        if not judged.error:
            path.write_text(json.dumps(judged.to_json()) + "\n")
        results[tree] = judged

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(run, jobs))
    return results
