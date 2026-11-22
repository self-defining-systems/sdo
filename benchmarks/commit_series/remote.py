"""The branch SDO deploys from, as a Git remote in the lane's cluster: ``up`` creates and seeds it, ``down`` removes it.

A subject's prepare runs ``up`` before it installs SDO and gives the printed URL and branch to SDO as its source branch
(``--source-branch-url``, ``--source-branch``); its teardown runs ``down``. ``up`` prints one JSON line, which the
subject keeps as ``$BENCH_LANE_DIR/source-branch.json`` for the workload adapter. ``rewind`` puts the branch back at a
commit the server already holds (a lane reset: the pushes of a run are history the next run must not inherit). Reuses
the toy upstream's server.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from benchmarks.sregym.fastloop.toy_upstream import (
    BASE_PATH,
    REPOSITORY,
    SERVICE,
    UpstreamLayout,
    delete_upstream,
    serve_upstream,
)


def rewind(kubeconfig: Path, layout: UpstreamLayout, head: str) -> int:
    """Point the branch at ``head``, which the server must hold; anything pushed after it stays unreachable."""
    git = ["kubectl", "--kubeconfig", str(kubeconfig), "-n", layout.namespace, "exec", f"deploy/{SERVICE}", "--"]
    git += ["git", f"--git-dir={BASE_PATH}/{REPOSITORY}"]
    for args in (["cat-file", "-e", f"{head}^{{commit}}"], ["update-ref", f"refs/heads/{layout.ref}", head]):
        done = subprocess.run([*git, *args], capture_output=True, text=True, check=False)
        if done.returncode != 0:
            print(f"git {' '.join(args)} on the branch server failed: {done.stderr.strip()}", file=sys.stderr)
            return 1
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="commit_series remote")
    parser.add_argument("action", choices=["up", "down", "rewind"])
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument(
        "--namespace", required=True, help="the app namespace; the server lives in <namespace>-upstream"
    )
    parser.add_argument("--ref", default="sdo-source")
    parser.add_argument("--control", default="", help="SDO's control namespace when it is not <namespace>-sdo")
    parser.add_argument("--image", help="image with bash and git (up)")
    parser.add_argument("--repository", type=Path, help="the accepted repository whose HEAD seeds the branch (up)")
    parser.add_argument("--head", help="the commit the branch goes back to (rewind)")
    args = parser.parse_args(argv)
    layout = UpstreamLayout(args.namespace, ref=args.ref)
    if args.action == "rewind":
        if not args.head:
            parser.error("rewind needs --head")
        return rewind(args.kubeconfig, layout, args.head)
    if args.action == "down":
        if (problem := delete_upstream(args.kubeconfig, layout)) is not None:
            print(problem, file=sys.stderr)
            return 1
        return 0
    if not args.image or not args.repository:
        parser.error("up needs --image and --repository")
    try:
        head = serve_upstream(args.kubeconfig, args.repository, layout, args.image)
    except subprocess.CalledProcessError as exc:
        # A server that never rolled out: say why (image pull, scheduling) instead of only the exit status.
        shown = subprocess.run(
            ["kubectl", "--kubeconfig", str(args.kubeconfig), "-n", layout.namespace, "describe", "pods"],
            capture_output=True,
            text=True,
            check=False,
        )
        print(f"{exc}\n{shown.stdout[-3000:]}{shown.stderr[-500:]}", file=sys.stderr)
        return 1
    info = {"url": layout.cluster_url, "ref": layout.ref, "namespace": args.namespace, "seeded": head}
    if args.control:
        info["control"] = args.control
    print(json.dumps(info, sort_keys=True))
    return 0
