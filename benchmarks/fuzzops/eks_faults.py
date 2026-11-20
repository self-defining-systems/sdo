"""Which fuzzops faults an EKS lane can run.

fuzzops refuses a fault whose steps include a ``NodeLevel`` action on a lane whose nodes cannot be reached
(``env.Env.HasNodes``): those actions write cgroup files, fill ``/dev/shm`` or read ``/proc`` inside a node, or build a
patched image and load it into a kind node. An EKS lane has no node the host can reach, so the same faults are not for
it. Everything else a fault does goes through the Kubernetes API, ``kubectl exec`` or the app's own protocols.

The verdict is read from a fuzzops checkout, never listed by hand: a fault template is a YAML file
(``examples/*/faults`` or ``templates``) whose steps name actions, and an action is a Go ``Spec`` with ``Name`` and
optionally ``NodeLevel``.

``python -m benchmarks.fuzzops.eks_faults matrix --fuzzops DIR --points points.tsv [--out F]`` writes the matrix;
``... filter --fuzzops DIR ID...`` prints the instance ids an EKS lane can run, in order. Instance ids look like
``template@target?params``.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

SCHEMA = "fuzzops.eks-faults/v1"
_SPEC_NAME = re.compile(r'\bName:\s*"([a-z0-9-]+)"')
_NODE_LEVEL = re.compile(r"\bNodeLevel:\s*true\b")
_IMAGE_MIRROR = re.compile(r"\bImageMirror:\s*true\b")
_ACTION_REF = re.compile(r"^\s*(?:-\s*)?action:\s*([a-z0-9-]+)\s*$", re.MULTILINE)
#: Why a known node-level action rules a fault out (an action not listed gets the generic reason).
REASONS = {
    "cgroup-throttle": "writes a container's cgroup (cpu.max, io.max) from the node",
    "build-and-deploy-patched-image": "builds a patched image on the host and loads it into the kind node",
    "shm-fill": "fills the pod's /dev/shm from the node",
    "connection-hold": "runs a client inside the dependency pod's network namespace from the node",
    "link-shape": "shapes the pod's egress with tc netem from the node",
    "fd-limit": "lowers a process's open-files limit with prlimit from the node",
    "retag-image": "loads the retagged host image into the kind node",
    "http-call": "sends the request from a node",
    "admission-mutator": "runs its webhook backend on the host and reaches it from the node",
}


class FaultSpecError(RuntimeError):
    """A fault template the checkout does not have, or a checkout that is not a fuzzops tree."""


@dataclass(frozen=True)
class Verdict:
    template: str
    ok: bool
    actions: tuple[str, ...] = ()
    reason: str = ""


def node_level_actions(fuzzops: Path) -> set[str]:
    """The names of the actions whose ``Spec`` says ``NodeLevel: true``, except those that also say
    ``ImageMirror: true`` (their only node access is delivering a built image, which an EKS lane does through its
    registry mirror; a fuzzops revision before that field lists them)."""
    found: set[str] = set()
    sources = sorted((fuzzops / "internal" / "actions").glob("*/*.go"))
    if not sources:
        raise FaultSpecError(f"{fuzzops} has no internal/actions: not a fuzzops checkout")
    for source in sources:
        if source.name.endswith("_test.go"):
            continue
        text = source.read_text()
        if _NODE_LEVEL.search(text) and not _IMAGE_MIRROR.search(text):
            found.update(_SPEC_NAME.findall(text)[:1])
    return found


def _template_file(fuzzops: Path, template: str) -> Path:
    for pattern in (f"examples/*/faults/{template}.yaml", f"templates/{template}.yaml"):
        for path in sorted(fuzzops.glob(pattern)):
            return path
    raise FaultSpecError(f"no fault template {template!r} under {fuzzops}")


def template_of(instance_id: str) -> str:
    return instance_id.split("@", 1)[0]


def classify(fuzzops: Path, instance_ids: Iterable[str]) -> dict[str, Verdict]:
    """Verdict per template of the given instance ids (a template is judged once)."""
    node = node_level_actions(fuzzops)
    out: dict[str, Verdict] = {}
    for instance_id in instance_ids:
        template = template_of(instance_id)
        if template in out:
            continue
        used = tuple(dict.fromkeys(_ACTION_REF.findall(_template_file(fuzzops, template).read_text())))
        blocked = tuple(a for a in used if a in node)
        reason = "; ".join(f"{a} {REASONS.get(a, 'needs access to the cluster nodes')}" for a in blocked)
        out[template] = Verdict(template, not blocked, blocked, f"node-level step: {reason}" if blocked else "")
    return out


def keep_eks_ok(fuzzops: Path, instance_ids: Sequence[str]) -> list[str]:
    verdicts = classify(fuzzops, instance_ids)
    return [i for i in instance_ids if verdicts[template_of(i)].ok]


def read_points(path: Path) -> list[tuple[str, str, str]]:
    """``(app, family template, short)`` rows of a points file (tab separated; ``#`` lines are comments)."""
    rows = []
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            app, family, short, *_ = line.split("\t")
            rows.append((app, family, short))
    return rows


def matrix(fuzzops: Path, points: Sequence[tuple[str, str, str]], *, revision: str) -> dict[str, Any]:
    verdicts = classify(fuzzops, [family for _, family, _ in points])
    families: dict[str, dict[str, Any]] = {}
    for app, family, short in points:
        v = verdicts[family]
        entry: dict[str, Any] = {"app": app, "template": family, "eks": v.ok, "reason": v.reason}
        if not v.ok:
            entry["actions"] = list(v.actions)
        families[short] = entry
    return {
        "schema": SCHEMA,
        "fuzzops_rev": revision,
        "node_level_actions": sorted(node_level_actions(fuzzops)),
        "families": families,
    }


def _revision(fuzzops: Path) -> str:
    done = subprocess.run(
        ["git", "-C", str(fuzzops), "rev-parse", "--short=7", "HEAD"], capture_output=True, text=True, check=False
    )
    return done.stdout.strip() or "unknown"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    m = sub.add_parser("matrix", help="the EKS verdict of each family of a points file")
    m.add_argument("--fuzzops", type=Path, required=True)
    m.add_argument("--points", type=Path, required=True)
    m.add_argument("--out", type=Path)
    f = sub.add_parser("filter", help="print the instance ids an EKS lane can run")
    f.add_argument("--fuzzops", type=Path, required=True)
    f.add_argument("ids", nargs="+")
    args = parser.parse_args(argv)
    try:
        if args.command == "matrix":
            text = json.dumps(
                matrix(args.fuzzops, read_points(args.points), revision=_revision(args.fuzzops)),
                indent=1,
                sort_keys=True,
            )
            if args.out:
                args.out.write_text(text + "\n")
            else:
                print(text)
        else:
            print("\n".join(keep_eks_ok(args.fuzzops, args.ids)))
    except FaultSpecError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
