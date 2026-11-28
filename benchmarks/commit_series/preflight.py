"""Offline preflight of a series against the baseline it runs on, before any live attempt.

``python -m benchmarks.commit_series.preflight --series series.json --baseline DIR [--kubeconfig K]`` checks, for every
commit of the series, what a live run would otherwise find one attempt at a time:

``plan``     every workload (pod-bearing object: Deployment, StatefulSet, DaemonSet, Job, CronJob, ...) the release
             renders is named by the baseline's deployment plan and every plan entry names one (the rule of
             ``sdo/operator/rendered/workloads.go``: a generated Job name is ``Job/<ns>/<prefix>-*``).
``migrate``  a release that changes the chart renders its migration Job (a pod waits in an init container for it).
``replay``   the real replayer pushes the commits, in order, onto the baseline's memory branch (a local bare remote).
``images``   every image reference of every release resolves in its registry (``docker manifest inspect``).
``dry-run``  with a lane's kubeconfig: a server-side dry-run apply of every release's render succeeds. An immutable
             field on an object that exists (a Job's pod template) is a note: SDO's deployer recreates such objects.

Exit status 1 when a check that would stop a release fails. Nothing here talks to SDO.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from benchmarks.common.replay.replay import replay, run_git
from benchmarks.common.replay.series import Series

RENDER_FILE = "k8s/render.yaml"
#: Pod-bearing kinds and where their pod spec is (sdo/operator/rendered/podtemplate.go).
POD_SPEC = {
    "Deployment": ("spec", "template", "spec"),
    "StatefulSet": ("spec", "template", "spec"),
    "DaemonSet": ("spec", "template", "spec"),
    "ReplicaSet": ("spec", "template", "spec"),
    "ReplicationController": ("spec", "template", "spec"),
    "Job": ("spec", "template", "spec"),
    "CronJob": ("spec", "jobTemplate", "spec", "template", "spec"),
    "Pod": ("spec",),
}
_GENERATED = re.compile(r"^[a-z0-9]{5,10}$")
_GIT_ENV = {"GIT_TERMINAL_PROMPT": "0", "GIT_ALLOW_PROTOCOL": "file:git"}


@dataclass
class Report:
    """One line per (release, check): ``ok`` or the problems; ``notes`` never fail a run."""

    rows: list[tuple[str, str, list[str]]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add(self, release: str, check: str, problems: list[str]) -> None:
        self.rows.append((release, check, problems))

    @property
    def failed(self) -> bool:
        return any(problems for _, _, problems in self.rows)

    def render(self) -> str:
        lines = []
        for release, check, problems in self.rows:
            lines.append(f"{release:>10}  {check:<8}  " + ("ok" if not problems else "FAIL"))
            lines.extend(f"{'':>22}  - {p}" for p in problems)
        lines.extend(f"note: {n}" for n in self.notes)
        return "\n".join(lines)


def workloads(docs: list[dict[str, Any]], namespace: str) -> list[str]:
    """``Kind/namespace/name`` of every pod-bearing object."""
    return [f"{d['kind']}/{namespace}/{d['metadata']['name']}" for d in docs if d.get("kind") in POD_SPEC]


def _matches(entry: str, ident: str) -> bool:
    if entry == ident:
        return True
    prefix, star = entry.removesuffix("-*"), entry.endswith("-*")
    token = ident.removeprefix(prefix + "-") if star and ident.startswith(prefix + "-") else None
    return token is not None and bool(_GENERATED.match(token))


def migration_problems(docs: list[dict[str, Any]]) -> list[str]:
    jobs = [d["metadata"]["name"] for d in docs if d.get("kind") == "Job"]
    return [] if any("migrations" in name for name in jobs) else ["the render has no migration Job"]


def images(docs: list[dict[str, Any]]) -> set[str]:
    found: set[str] = set()
    for doc in docs:
        path = POD_SPEC.get(doc.get("kind", ""))
        if path is None:
            continue
        node: Any = doc
        for key in path:
            node = (node or {}).get(key)
        for field_name in ("initContainers", "containers"):
            found.update(str(c["image"]) for c in (node or {}).get(field_name, []) if c.get("image"))
    return found


def missing_images(refs: set[str], inspect: Any = None) -> list[str]:
    """The references ``docker manifest inspect`` cannot resolve (checked in parallel)."""
    inspect = inspect or _inspect

    def check(ref: str) -> str:
        return "" if inspect(ref) else ref

    with ThreadPoolExecutor(max_workers=8) as pool:
        return sorted(r for r in pool.map(check, sorted(refs)) if r)


def pullable(ref: str) -> str:
    """``name:tag@sha256:...`` as the registry is asked: the digest alone (the CLI refuses the pair)."""
    return re.sub(r":[^/@:]+@", "@", ref)


def _inspect(ref: str) -> bool:
    done = subprocess.run(
        ["docker", "manifest", "inspect", pullable(ref)], capture_output=True, text=True, check=False, timeout=120
    )
    return done.returncode == 0


def dry_run(kubeconfig: Path, namespace: str, render: str) -> tuple[list[str], list[str]]:
    """(problems, notes) of a server-side dry-run apply of ``render``: an immutable-field error is a note."""
    done = subprocess.run(
        ["kubectl", "--kubeconfig", str(kubeconfig), "-n", namespace, "apply", "--server-side", "--force-conflicts"]
        + ["--dry-run=server", "-f", "-"],
        input=render,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    if done.returncode == 0:
        return [], []
    problems, notes = [], []
    for line in done.stderr.splitlines():
        if not line.strip() or line.startswith("Error from server (Conflict)"):
            continue
        (notes if "immutable" in line else problems).append(line.strip()[:300])
    return problems, notes


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return done.stdout.strip()


def replay_problems(series: Series, baseline: Path, scratch: Path) -> dict[int, list[str]]:
    """Push the series' commits, one by one, with the real replayer onto the baseline's memory branch (a bare remote
    in ``scratch``); the problems per 1-based commit index."""
    remote = scratch / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "sdo-source", str(remote)], check=True)
    info = json.loads((baseline / "baseline.json").read_text())
    clone = scratch / "memory"
    subprocess.run(["git", "clone", "-q", "-b", info["branch"], str(baseline / info["bundle"]), str(clone)], check=True)
    _git(clone, "push", "-q", str(remote), "HEAD:refs/heads/sdo-source")
    problems: dict[int, list[str]] = {}
    for index in range(1, len(series.commits) + 1):
        record = replay(series.config(index, str(remote), "sdo-source"), git=_replay_git).records[0]
        problems[index] = [] if record.status == "pushed" else [f"{record.status}: {', '.join(record.paths)}"]
    return problems


def _replay_git(args: Any, cwd: Any = None, stdin: Any = None, env: Any = None) -> subprocess.CompletedProcess[bytes]:
    return run_git(args, cwd, stdin, {**_GIT_ENV, **(env or {})})


def release_renders(series: Series) -> list[tuple[str, str]]:
    """(label, render text) of the base and every commit of the series."""
    repo = Path(series.repo)
    out = []
    for label, rev in (("base", series.base), *((f"#{i}", sha) for i, sha in enumerate(series.commits, 1))):
        out.append((label, _git(repo, "show", f"{rev}:{RENDER_FILE}") + "\n"))
    return out


def run(
    series: Series,
    baseline: Path,
    *,
    namespace: str = "fg1",
    kubeconfig: Path | None = None,
    check_images: bool = True,
    inspect: Any = None,
) -> Report:
    report = Report()
    with tempfile.TemporaryDirectory(prefix="series-preflight-") as tmp:
        scratch = Path(tmp)
        seen_images: set[str] = set()
        for label, render in release_renders(series):
            docs = [d for d in yaml.safe_load_all(render) if isinstance(d, dict)]
            if label != "base":
                report.add(label, "migrate", migration_problems(docs))
            seen_images |= images(docs)
            if kubeconfig is not None:
                problems, notes = dry_run(kubeconfig, namespace, render)
                report.add(label, "dry-run", problems)
                report.notes += [f"{label}: {n}" for n in notes]
        for index, problems in replay_problems(series, baseline, scratch).items():
            report.add(f"#{index}", "replay", problems)
    if check_images:
        report.add("series", "images", [f"{r} does not resolve" for r in missing_images(seen_images, inspect)])
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="commit_series.preflight", description=__doc__)
    parser.add_argument("--series", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--namespace", default="fg1")
    parser.add_argument("--kubeconfig", type=Path, help="a lane's kubeconfig, for the server-side dry-run")
    parser.add_argument("--skip-images", action="store_true")
    args = parser.parse_args(argv)
    report = run(
        Series.load(args.series),
        args.baseline,
        namespace=args.namespace,
        kubeconfig=args.kubeconfig,
        check_images=not args.skip_images,
    )
    print(report.render())
    return 1 if report.failed else 0


if __name__ == "__main__":
    sys.exit(main())
