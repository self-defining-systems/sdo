"""``fastloop gitlab rehearse``: replay the GitLab chart releases through SDO's real upstream-follow path offline.

The live replay (``gitlab_replay``) needs a warm kind lane and real containers, and stops at the first blocker. The
rehearsal runs the same releases through SDO's production onboarding, advancer, deployer and broker against a real
API server (envtest) with a fake kubelet, scripted agents and an injected clock, and reports every blocker of every
release in one run. It needs no cluster, no model and no lease.

This module only prepares the input, the way the lane vendors each release (``assemble.assemble`` at that release's
pins is the tree ``gitlab_replay.push_release`` commits), and runs the generic Go harness
(``TestReplayRehearsalFromInput`` in ``sdo/operator/operatorcli``) on it. SDO production code never reads it.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from apps.gitlab import assemble

if TYPE_CHECKING:
    from benchmarks.sregym.fastloop.gitlab_releases import Release, ReleaseList

REPO_ROOT = Path(__file__).resolve().parents[3]
OPERATOR_ROOT = REPO_ROOT / "sdo" / "operator"
INPUT_FILE = "rehearsal-input.json"
GO_TEST = "^TestReplayRehearsalFromInput$"


@dataclass(frozen=True)
class Behavior:
    """A fake-kubelet behavior for matching workloads (the Go harness's ``rehearsal.Behavior``)."""

    name: str
    kind: str = ""
    ready_after: str = ""
    job_failures: int = 0
    exit_code: int = 0
    from_release: int = 0
    until_release: int = 0

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a behavior names a workload (or a prefix ending in '*')")
        if self.from_release < 0 or self.until_release < 0:
            raise ValueError("release indexes are not negative")

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name}
        for key in ("kind", "ready_after", "job_failures", "exit_code", "from_release", "until_release"):
            value = getattr(self, key)
            if value:
                out[key] = value
        return out


@dataclass(frozen=True)
class RehearsalInput:
    """What the Go harness reads: the lane's deployment source and one vendored tree per release (baseline first)."""

    namespace: str
    deployment_source: dict[str, Any]
    releases: tuple[tuple[str, Path], ...]
    behaviors: tuple[Behavior, ...] = field(default_factory=tuple)
    warm_up: str = ""
    name: str = "gitlab"

    def __post_init__(self) -> None:
        if len(self.releases) < 2:
            raise ValueError("a rehearsal needs the baseline and at least one release")

    def as_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "namespace": self.namespace,
            "deployment_source": self.deployment_source,
            "releases": [{"name": name, "dir": str(path)} for name, path in self.releases],
            "behaviors": [b.as_json() for b in self.behaviors],
        }
        if self.warm_up:
            out["warm_up"] = self.warm_up
        return out


def plan_rehearsal(releases: ReleaseList, start: str, limit: int | None = None) -> tuple[Release, ...]:
    """The baseline ``start`` and the releases after it (at most ``limit`` of them)."""
    after = releases.after(start)
    if limit is not None:
        after = after[:limit]
    if not after:
        raise ValueError(f"no release follows {start}")
    return (releases.get(start), *after)


def vendor_release(release: Release, archive: Path, dest: Path) -> Path:
    """The followed tree at ``release``: the umbrella chart with the verified upstream archive vendored."""
    return assemble.assemble(dest, archive=archive, pins=release.pins(assemble.load_pins()))


def prepare(
    out: Path,
    plan: tuple[Release, ...],
    archives: dict[str, Path],
    *,
    behaviors: tuple[Behavior, ...] = (),
    warm_up: str = "",
) -> Path:
    """Vendor every release under ``out/releases`` and write the harness input; return its path."""
    from benchmarks.sregym.fastloop.gitlab_lane import NAMESPACE, deployment_source

    trees = []
    for release in plan:
        tree = out / "releases" / release.version
        if not tree.exists():
            vendor_release(release, archives[release.version], tree)
        trees.append((release.version, tree.resolve()))
    document = RehearsalInput(NAMESPACE, deployment_source(), tuple(trees), behaviors, warm_up)
    path = out / INPUT_FILE
    path.write_text(json.dumps(document.as_json(), indent=2) + "\n", encoding="utf-8")
    return path


def run_harness(input_path: Path, out: Path) -> int:
    """Run the Go harness on ``input_path``; it writes ``rehearsal.json`` and ``rehearsal.txt`` into ``out``.

    ``go test`` has no deadline of its own here: the harness bounds itself (each wait is on its injected clock).
    """
    env = {**os.environ, "SDO_REHEARSAL_INPUT": str(input_path.resolve()), "SDO_REHEARSAL_OUT": str(out.resolve())}
    command = ["go", "test", "-tags", "integration", "-count=1", "-timeout=0", "-run", GO_TEST]
    command += ["./operatorcli/"]
    return subprocess.run(command, cwd=OPERATOR_ROOT, env=env, check=False).returncode


def parse_behaviors(text: str) -> tuple[Behavior, ...]:
    """Behaviors from a JSON list (``--behaviors``)."""
    return tuple(Behavior(**entry) for entry in json.loads(text)) if text else ()


def rehearse(args: Any) -> int:
    """``fastloop gitlab rehearse``: vendor the releases, run the harness, print the summary."""
    import tempfile

    from benchmarks.sregym.fastloop.gitlab_releases import load_releases
    from benchmarks.sregym.fastloop.gitlab_replay import fetch_archive

    releases = load_releases()
    plan = plan_rehearsal(releases, args.start or releases.releases[0].version, args.limit)
    cache = args.archive_cache or Path(tempfile.gettempdir()) / "gitlab-chart-archives"
    archives = {release.version: fetch_archive(release, cache) for release in plan}
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    path = prepare(out, plan, archives, behaviors=parse_behaviors(args.behaviors or ""), warm_up=args.warm_up or "")
    code = run_harness(path, out)
    summary = out / "rehearsal.txt"
    if summary.is_file():
        print(summary.read_text(encoding="utf-8"))
    return code


def add_gitlab_rehearse_parser(commands: Any) -> None:
    """Register ``fastloop gitlab rehearse`` on the ``gitlab`` subcommand's subparsers."""
    parser = commands.add_parser(
        "rehearse",
        help="replay chart releases through SDO's upstream-follow path offline (envtest, no cluster, no model)",
    )
    parser.add_argument("--out", type=Path, required=True, help="where releases, input and the report are written")
    parser.add_argument("--start", help="baseline release (default: the list's baseline)")
    parser.add_argument("--limit", type=int, help="rehearse at most this many releases after the baseline")
    parser.add_argument("--archive-cache", type=Path, help="where chart archives are downloaded (verified)")
    parser.add_argument(
        "--behaviors", help='fake-kubelet behaviors as JSON, e.g. [{"kind":"Job","name":"x","job_failures":-1}]'
    )
    parser.add_argument("--warm-up", help="scripted probe warm-up after a pod becomes ready, e.g. 2m (default none)")
    parser.set_defaults(handler=rehearse)
