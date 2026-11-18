"""Command line of the compiled swap, for a benchmark runner that is not the fuzzops hooks.

Two commands, both printing one JSON line on stdout and failing with ``error: <reason>`` (exit 1)::

    python -m benchmarks.devswap record --checkout DIR --kubeconfig K --context C \\
        --control-namespace NS --lane lane.json
    python -m benchmarks.devswap swap   --sha SHA --repo REPO --kubeconfig K --context C --control-namespace NS \\
        --lane lane.json --dev-dir DIR [--install-args FILE --install-env FILE]

``record`` runs once, right after the lane's operator install and before anything touches the accepted memory:
it keeps what the lane was built from (compiled-path tree ids, contract descriptors) and the accepted
repository's state.
``swap`` puts the target revision's operator on that lane (module docstring of ``swap``). ``--dev-dir`` is the host
directory the lane's pods mount as their dev tree (the node path under the evidence mount). ``--install-args`` and
``--install-env`` are JSON files holding the ``install-existing`` argv (a list) and environment (an object) the lane was
installed with; with them the swap regenerates the memory's Go-generated files with the new binary.
``--sha`` with ``--repo`` makes (or reuses) a detached checkout of the revision under ``--checkouts``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from benchmarks.devswap import devtree
from benchmarks.devswap.controller import repository_state, set_controller_suspended
from benchmarks.devswap.kube import Kubectl, SwapError, run
from benchmarks.devswap.swap import SwapPlan, SwapState, swap_compiled
from benchmarks.devswap.tree import prepare_tree_from_install

IDLE_WAIT_S = 300.0


def resolve_checkout(repo: Path, sha: str, root: Path) -> Path:
    """A detached checkout of ``sha`` of ``repo`` under ``root`` (made once, reused for the same revision)."""
    full = run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{sha}^{{commit}}"], "resolve the revision"
    ).stdout.strip()
    target = root / full[:12]
    if (target / ".git").exists():
        head = subprocess.run(
            ["git", "-C", str(target), "rev-parse", "HEAD"], capture_output=True, text=True, check=False
        ).stdout.strip()
        if head == full:
            return target
        raise SwapError(f"{target} is at {head[:12]}, not {full[:12]}")
    root.mkdir(parents=True, exist_ok=True)
    run(["git", "-C", str(repo), "worktree", "add", "--detach", str(target), full], "check out the revision")
    return target


def _ready(kube: Kubectl, control: str, timeout: float = 0.0) -> str:
    """Why the controller is not ready ("" when it is), waiting up to ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while True:
        if kube.controller_ready(control):
            return ""
        if time.monotonic() >= deadline:
            return "the controller pod is not ready"
        time.sleep(3)


def _kube(args: argparse.Namespace) -> Kubectl:
    return Kubectl(args.context, str(Path(args.kubeconfig).resolve()) if args.kubeconfig else "")


def record(args: argparse.Namespace) -> dict[str, object]:
    checkout = Path(args.checkout).resolve()
    kube = _kube(args)
    lane = {
        "revision": run(["git", "-C", str(checkout), "rev-parse", "HEAD"], "git rev-parse").stdout.strip(),
        "compiled": devtree.fingerprint(checkout),
        "contracts": devtree.contracts_snapshot(checkout),
        "state": repository_state(kube, args.control_namespace),
    }
    Path(args.lane).write_text(json.dumps(lane) + "\n")
    return {"lane": str(Path(args.lane).resolve()), "revision": lane["revision"], "head": lane["state"]["head"]}


def swap(args: argparse.Namespace) -> dict[str, object]:
    started = time.monotonic()
    lane_file = Path(args.lane)
    lane = json.loads(lane_file.read_text())
    if args.sha:
        if not args.repo:
            raise SwapError("--sha needs --repo")
        checkout = resolve_checkout(Path(args.repo), args.sha, Path(args.checkouts).resolve())
    else:
        checkout = Path(args.checkout).resolve()
    kube, control = _kube(args), args.control_namespace
    dev_dir = Path(args.dev_dir).resolve()
    binary = Path(args.binary).resolve() if args.binary else None
    prepare = None
    if args.install_args:
        # prepare-tree runs the binary the swap placed (None: the lane's own, when the swap placed none)
        prepare = prepare_tree_from_install(
            json.loads(Path(args.install_args).read_text()),
            json.loads(Path(args.install_env).read_text()) if args.install_env else {},
            binary,
        )
    work = lane_file.resolve().parent

    def persist(state: SwapState) -> None:
        """The lane record says what the lane runs and where its memory is, at each change."""
        lane["compiled"], lane["contracts"] = dict(state.compiled), state.contracts
        lane["state"] = {**lane["state"], "head": state.head}
        lane_file.write_text(json.dumps(lane) + "\n")

    result = swap_compiled(
        SwapPlan(
            kube=kube,
            control=control,
            checkout=checkout,
            lane_compiled=lane.get("compiled") or {},
            lane_contracts=lane.get("contracts", ""),
            prepared=lane["state"],
            place=lambda new: devtree.sync(checkout, dev_dir, new),
            idle=lambda: _ready(kube, control),
            wait_idle=lambda: _ready(kube, control, IDLE_WAIT_S),
            work_root=work,
            current_binary=binary or dev_dir / ".fastloop" / "sdo-operator",
            prepare_tree=prepare,
            on_binary=lambda built: _copy_binary(built, binary),
            persist=persist,
            allow_dirty=args.allow_dirty,
            warm_go_cache=not args.no_warm_go_cache,
        ),
        lambda message: print(message, file=sys.stderr),
    )
    lane["revision"] = run(["git", "-C", str(checkout), "rev-parse", "HEAD"], "git rev-parse").stdout.strip()
    lane_file.write_text(json.dumps(lane) + "\n")
    return {
        "revision": lane["revision"],
        "head": result.head,
        "binary": result.binary,
        "restarted": result.restarted,
        "swap_s": round(time.monotonic() - started, 1),
        "phases": result.phases.seconds,
    }


def _copy_binary(built: Path, target: Path | None) -> None:
    """Keep the runner's own copy of the operator (its CLI calls) at the new build."""
    if target is not None and built != target:
        staged = target.with_name(target.name + ".new")
        staged.write_bytes(built.read_bytes())
        staged.chmod(0o755)
        staged.replace(target)


def resume_controller(args: argparse.Namespace) -> None:
    """After a failed swap: the controller Job must not stay suspended (a stopped controller answers nothing, and a lane
    reset would fail on it). Resuming a running one changes nothing."""
    try:
        set_controller_suspended(_kube(args), args.control_namespace, False)
    except (SwapError, OSError, subprocess.SubprocessError) as exc:
        print(f"could not resume the controller: {exc}", file=sys.stderr)


def parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m benchmarks.devswap", description=(__doc__ or "").split("\n\n")[0])
    sub = ap.add_subparsers(dest="verb", required=True)
    for name in ("record", "swap"):
        p = sub.add_parser(name)
        p.add_argument("--kubeconfig", default="")
        p.add_argument("--context", default="")
        p.add_argument("--control-namespace", required=True)
        p.add_argument("--lane", required=True, help="the lane record (JSON) this command writes or updates")
    sub.choices["record"].add_argument("--checkout", default=".")
    s = sub.choices["swap"]
    s.add_argument("--checkout", default=".", help="a checkout of the target revision (or use --sha and --repo)")
    s.add_argument("--sha", default="")
    s.add_argument("--repo", default="")
    s.add_argument("--checkouts", default=str(Path.home() / ".cache" / "sdo-swap-checkouts"))
    s.add_argument("--dev-dir", required=True)
    s.add_argument("--binary", default="", help="the runner's own copy of sdo-operator, kept at the new build")
    s.add_argument("--allow-dirty", action="store_true", help="build from uncommitted changes under sdo/operator")
    s.add_argument(
        "--no-warm-go-cache",
        action="store_true",
        help="do not compile the new tree into the lane's Go build cache before the controller restarts",
    )
    s.add_argument("--install-args", default="")
    s.add_argument("--install-env", default="")
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        receipt = record(args) if args.verb == "record" else swap(args)
    except (SwapError, OSError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        if args.verb == "swap":
            resume_controller(args)
        return 1
    print(json.dumps(receipt))
    return 0
