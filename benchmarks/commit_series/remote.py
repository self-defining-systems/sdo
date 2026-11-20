"""The branch SDO deploys from, as a Git remote in the lane's cluster: ``up`` creates and seeds it, ``down`` removes it.

A subject's prepare runs ``up`` before it installs SDO and gives the printed URL and branch to SDO as its source branch
(``--source-branch-url``, ``--source-branch``); its teardown runs ``down``. ``up`` prints one JSON line, which the
subject keeps as ``$BENCH_LANE_DIR/source-branch.json`` for the workload adapter. Reuses the toy upstream's server.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from benchmarks.sregym.fastloop.toy_upstream import UpstreamLayout, delete_upstream, serve_upstream


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="commit_series remote")
    parser.add_argument("action", choices=["up", "down"])
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument(
        "--namespace", required=True, help="the app namespace; the server lives in <namespace>-upstream"
    )
    parser.add_argument("--ref", default="sdo-source")
    parser.add_argument("--control", default="", help="SDO's control namespace when it is not <namespace>-sdo")
    parser.add_argument("--image", help="image with bash and git (up)")
    parser.add_argument("--repository", type=Path, help="the accepted repository whose HEAD seeds the branch (up)")
    args = parser.parse_args(argv)
    layout = UpstreamLayout(args.namespace, ref=args.ref)
    if args.action == "down":
        if (problem := delete_upstream(args.kubeconfig, layout)) is not None:
            print(problem, file=sys.stderr)
            return 1
        return 0
    if not args.image or not args.repository:
        parser.error("up needs --image and --repository")
    head = serve_upstream(args.kubeconfig, args.repository, layout, args.image)
    info = {"url": layout.cluster_url, "ref": layout.ref, "namespace": args.namespace, "seeded": head}
    if args.control:
        info["control"] = args.control
    print(json.dumps(info, sort_keys=True))
    return 0
