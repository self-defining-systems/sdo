"""Toy upstream for the e2e lane: an in-cluster Git server and a replayer that pushes scripted commits.

SDO follows a Git ref (``--source-kind git``). The benchmark owns the remote: a ``git daemon`` Deployment in
``<lane>-upstream`` (the controller image carries git), seeded with the workspace's source commit, and a replayer
that applies ``apps/toy/commits/<name>.patch`` on a host clone and pushes it to the followed ref. Nothing here
imports SDO production code; the only coupling is the Git ref and the ``--source-*`` flag values.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

COMMITS_DIR = Path(__file__).resolve().parents[3] / "apps" / "toy" / "commits"
GIT_PORT = 9418
SERVICE = "toy-upstream"
REPOSITORY = "toy.git"
BASE_PATH = "/srv/git"
_NAME = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")
_IDENTITY = ("-c", "user.name=toy-upstream", "-c", "user.email=toy-upstream@invalid")
#: Transports the replayer and seeder may use, and no credential prompts.
_GIT_ENV = {"GIT_TERMINAL_PROMPT": "0", "GIT_ALLOW_PROTOCOL": "file:git"}


class UpstreamError(RuntimeError):
    """The toy upstream could not seed, replay or serve a commit."""


@dataclass(frozen=True)
class UpstreamLayout:
    """Where the toy's Git remote lives for one lane."""

    lane_namespace: str
    ref: str = "main"
    poll_interval: str = "5s"

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.lane_namespace) or len(self.lane_namespace) > 55:
            raise ValueError(f"lane namespace {self.lane_namespace!r} is not a DNS label (max 55 characters)")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", self.ref) or ".." in self.ref:
            raise ValueError(f"upstream ref {self.ref!r} is not a plain branch name")
        if not re.fullmatch(r"[1-9][0-9]*(ms|s|m|h)", self.poll_interval):
            raise ValueError(f"poll interval {self.poll_interval!r} must look like 5s or 1m")

    @property
    def namespace(self) -> str:
        return f"{self.lane_namespace}-upstream"

    @property
    def cluster_url(self) -> str:
        """The URL SDO's controller (inside the cluster) follows."""
        return f"git://{SERVICE}.{self.namespace}.svc.cluster.local:{GIT_PORT}/{REPOSITORY}"


def daemon_script(base_path: str = BASE_PATH, *, port: int = GIT_PORT, ref: str = "main") -> str:
    """Shell run by the server container: create the bare repo once, then serve it with push enabled."""
    repo = f"{base_path}/{REPOSITORY}"
    return (
        "set -eu\n"
        f"[ -d {repo} ] || git init --bare -q -b {ref} {repo}\n"
        f"exec git daemon --reuseaddr --export-all --enable=receive-pack --base-path={base_path} "
        f"--listen=0.0.0.0 --port={port} {base_path}\n"
    )


def upstream_manifests(layout: UpstreamLayout, image: str) -> list[dict[str, Any]]:
    """Namespace, Deployment and Service for the in-cluster remote (``image`` must contain bash and git)."""
    if not image:
        raise ValueError("upstream server image is required")
    labels = {"app": SERVICE}
    ns = layout.namespace
    return [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}},
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": SERVICE, "namespace": ns, "labels": labels},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "containers": [
                            {
                                "name": "git",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["bash", "-c", daemon_script(ref=layout.ref)],
                                "ports": [{"containerPort": GIT_PORT, "name": "git"}],
                                "readinessProbe": {"tcpSocket": {"port": GIT_PORT}, "periodSeconds": 1},
                                "volumeMounts": [{"name": "repos", "mountPath": BASE_PATH}],
                            }
                        ],
                        "volumes": [{"name": "repos", "emptyDir": {}}],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": SERVICE, "namespace": ns},
            "spec": {"selector": labels, "ports": [{"name": "git", "port": GIT_PORT, "targetPort": GIT_PORT}]},
        },
    ]


def source_config(layout: UpstreamLayout) -> dict[str, Any]:
    """The ``GitPollConfig`` (proto JSON) the controller follows."""
    return {"repository": layout.cluster_url, "ref": layout.ref, "poll_interval": layout.poll_interval}


def source_args(layout: UpstreamLayout) -> list[str]:
    """Controller install flags that make SDO follow this upstream."""
    return ["--source-kind", "git", "--source-config", json.dumps(source_config(layout), sort_keys=True)]


def available_commits(directory: Path = COMMITS_DIR) -> tuple[str, ...]:
    return tuple(sorted(path.stem for path in directory.glob("*.patch")))


def patch_path(name: str, directory: Path = COMMITS_DIR) -> Path:
    path = directory / f"{name}.patch"
    if not re.fullmatch(r"[a-z][a-z0-9-]*", name) or not path.is_file():
        raise UpstreamError(f"unknown scripted commit {name!r}; available: {', '.join(available_commits(directory))}")
    return path


def _git(repository: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(host_git_command(repository, *args), capture_output=True, text=True, env=_environment())
    if check and result.returncode != 0:
        raise UpstreamError(f"git {' '.join(args[:2])} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result


def seed_upstream(workspace: Path, remote: str, layout: UpstreamLayout) -> str:
    """Push the workspace's source commit to the remote's followed ref (so SDO's accepted repo and the upstream
    start from the same SHA). Refuses a remote whose ref already holds a different commit."""
    head = _git(workspace, "rev-parse", "HEAD").stdout.strip()
    existing = _git(workspace, "ls-remote", remote, f"refs/heads/{layout.ref}").stdout.split()
    if existing:
        if existing[0] == head:
            return head
        raise UpstreamError(f"upstream {remote} already holds {existing[0]} on {layout.ref}; refusing to reseed")
    _git(workspace, "push", "--", remote, f"{head}:refs/heads/{layout.ref}")
    return head


def push_change(
    remote: str,
    layout: UpstreamLayout,
    message: str,
    change: Callable[[Path], None],
    *,
    identity: Sequence[str] = _IDENTITY,
) -> str:
    """Clone the followed ref, let ``change`` edit its work tree, commit everything and push (fast-forward only).

    Returns the pushed commit SHA. A change that raises, or a push the remote rejects, raises ``UpstreamError`` (or
    the change's own error) and changes nothing upstream. Shared by every replayer: what differs is the change.
    """
    with tempfile.TemporaryDirectory(prefix="upstream-replay-") as scratch:
        clone = Path(scratch) / "clone"
        cloned = subprocess.run(
            ["git", "clone", "-q", "--no-tags", "--branch", layout.ref, "--", remote, str(clone)],
            capture_output=True,
            text=True,
            env=_environment(),
        )
        if cloned.returncode != 0:
            raise UpstreamError(f"clone {remote} {layout.ref} failed: {cloned.stderr.strip()}")
        change(clone)
        _git(clone, "add", "--all")
        _git(clone, *identity, "commit", "-q", "-m", message)
        sha = _git(clone, "rev-parse", "HEAD").stdout.strip()
        _git(clone, "push", "--", remote, f"HEAD:refs/heads/{layout.ref}")
    return sha


def replay_commit(name: str, remote: str, layout: UpstreamLayout, *, directory: Path = COMMITS_DIR) -> str:
    """Apply ``<name>.patch`` on a fresh clone of the followed ref and push it (fast-forward only).

    Returns the pushed commit SHA. A patch that does not apply, or a push the remote rejects, raises
    ``UpstreamError`` and changes nothing upstream.
    """
    patch = patch_path(name, directory)

    def apply(clone: Path) -> None:
        _git(clone, "apply", "--index", str(patch.resolve()))

    return push_change(remote, layout, f"toy upstream: {name}", apply)


def _environment() -> dict[str, str]:
    return {**os.environ, **_GIT_ENV}


def port_forward_command(kubeconfig: Path, layout: UpstreamLayout, local_port: int) -> list[str]:
    return [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "-n",
        layout.namespace,
        "port-forward",
        f"service/{SERVICE}",
        f"{local_port}:{GIT_PORT}",
    ]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def forwarded_remote(
    kubeconfig: Path, layout: UpstreamLayout, *, timeout: float = wait_for(WaitKind.COMMAND)
) -> Iterator[str]:
    """Yield a host-reachable ``git://`` URL for the in-cluster remote; stops the port-forward on exit."""
    if shutil.which("kubectl") is None:
        raise UpstreamError("kubectl is not on PATH")
    port = _free_port()
    process = subprocess.Popen(
        port_forward_command(kubeconfig, layout, port), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True
    )
    try:
        deadline = time.monotonic() + timeout
        while True:
            if process.poll() is not None:
                raise UpstreamError(f"port-forward exited: {(process.stderr.read() if process.stderr else '').strip()}")
            try:
                socket.create_connection(("127.0.0.1", port), timeout=wait_for(WaitKind.POLL_FAST)).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise UpstreamError(f"port-forward to {layout.namespace} not ready within {timeout}s") from None
                time.sleep(wait_for(WaitKind.POLL_FAST) / 5)
        yield f"git://127.0.0.1:{port}/{REPOSITORY}"
    finally:
        process.terminate()
        try:
            process.wait(timeout=wait_for(WaitKind.POLL))
        except subprocess.TimeoutExpired:
            process.kill()


def apply_manifests(kubeconfig: Path, manifests: Sequence[dict[str, Any]]) -> None:
    """``kubectl apply`` the upstream server manifests and wait for the Deployment."""
    payload = json.dumps({"apiVersion": "v1", "kind": "List", "items": list(manifests)})
    base = ["kubectl", "--kubeconfig", str(kubeconfig)]
    subprocess.run([*base, "apply", "-f", "-"], input=payload, text=True, check=True)
    namespace = next(m["metadata"]["name"] for m in manifests if m["kind"] == "Namespace")
    subprocess.run([*base, "-n", namespace, "rollout", "status", f"deployment/{SERVICE}", "--timeout=120s"], check=True)


def delete_command(kubeconfig: Path, layout: UpstreamLayout) -> list[str]:
    return [
        "kubectl",
        "--kubeconfig",
        str(kubeconfig),
        "delete",
        "namespace",
        layout.namespace,
        "--ignore-not-found=true",
        "--wait=true",
        "--timeout=180s",
    ]


def delete_upstream(kubeconfig: Path, layout: UpstreamLayout) -> str | None:
    """Delete the lane's upstream namespace (absent is fine). Returns an error message, or None when it is gone."""
    try:
        subprocess.run(
            delete_command(kubeconfig, layout),
            check=True,
            capture_output=True,
            text=True,
            timeout=wait_for(WaitKind.SLOW_COMMAND, 2),
        )
    except subprocess.CalledProcessError as exc:
        return f"delete namespace {layout.namespace} failed: {(exc.stderr or '').strip()}"
    except subprocess.TimeoutExpired:
        return f"delete namespace {layout.namespace} timed out"
    return None


def add_toy_parser(commands: Any) -> None:
    """Register ``fastloop toy {serve,push,source-args}`` on the CLI's subparsers."""
    toy = commands.add_parser("toy", help="toy upstream: in-cluster Git remote and scripted commit replayer")
    actions = toy.add_subparsers(dest="toy_command", required=True)

    def shared(parser: Any, *, remote: bool) -> None:
        parser.add_argument("--run-dir", type=Path, help="fastloop lane directory (lane namespace and kubeconfig)")
        parser.add_argument("--lane-namespace", help="lane namespace when no --run-dir is given")
        parser.add_argument("--ref", default="main", help="followed branch")
        parser.add_argument("--poll-interval", default="5s")
        if remote:
            parser.add_argument("--remote", help="push to this Git URL instead of port-forwarding the lane's server")

    serve = actions.add_parser("serve", help="deploy the in-cluster Git server and seed it with the workspace commit")
    shared(serve, remote=False)
    serve.add_argument("--image", required=True, help="image with bash and git (the SDO controller image)")
    serve.set_defaults(handler=_serve)

    push = actions.add_parser("push", help="apply a scripted commit patch and push it to the followed ref")
    shared(push, remote=True)
    push.add_argument("--commit", required=True, choices=available_commits())
    push.set_defaults(handler=_push)

    scenario = actions.add_parser("scenario", help="push a scripted commit to a live lane and assert SDO's handling")
    shared(scenario, remote=False)
    scenario.add_argument("--commit", required=True, choices=available_commits())
    scenario.add_argument("--verdict-seconds", type=float, help="how long SDO has to accept or hold the commit")
    scenario.add_argument("--steady-seconds", type=float, help="quiet window after the verdict")
    scenario.set_defaults(handler=_scenario)

    args = actions.add_parser("source-args", help="print the controller flags that follow this upstream")
    shared(args, remote=False)
    args.set_defaults(handler=_source_args)


def _lane(args: Any) -> tuple[UpstreamLayout, Any]:
    environment = None
    namespace = args.lane_namespace
    if args.run_dir is not None:
        from benchmarks.sregym.fastloop.environment import FastloopEnvironment

        environment = FastloopEnvironment.load(args.run_dir.resolve())
        namespace = environment.namespace
    if namespace is None:
        raise SystemExit("give --run-dir or --lane-namespace")
    return UpstreamLayout(namespace, ref=args.ref, poll_interval=args.poll_interval), environment


def serve_upstream(kubeconfig: Path, workspace: Path, layout: UpstreamLayout, image: str) -> str:
    """Deploy the in-cluster Git server and seed it with the workspace commit; returns the seeded SHA."""
    apply_manifests(kubeconfig, upstream_manifests(layout, image))
    with forwarded_remote(kubeconfig, layout) as remote:
        return seed_upstream(workspace, remote, layout)


def _serve(args: Any) -> int:
    layout, environment = _lane(args)
    if environment is None:
        raise SystemExit("serve needs --run-dir (kubeconfig and workspace)")
    head = serve_upstream(environment.kubeconfig, environment.workspace, layout, args.image)
    print(json.dumps({"seeded": head, "repository": layout.cluster_url, "ref": layout.ref}, sort_keys=True))
    return 0


def _push(args: Any) -> int:
    layout, environment = _lane(args)
    if args.remote:
        sha = replay_commit(args.commit, args.remote, layout)
    elif environment is not None:
        with forwarded_remote(environment.kubeconfig, layout) as remote:
            sha = replay_commit(args.commit, remote, layout)
    else:
        raise SystemExit("push needs --remote or --run-dir")
    print(json.dumps({"commit": args.commit, "pushed": sha, "ref": layout.ref}, sort_keys=True))
    return 0


def _source_args(args: Any) -> int:
    layout, _ = _lane(args)
    print(json.dumps(source_args(layout)))
    return 0


def _scenario(args: Any) -> int:
    from benchmarks.sregym.fastloop.toy_live import run_scenario

    layout, environment = _lane(args)
    if environment is None:
        raise SystemExit("scenario needs --run-dir")
    return run_scenario(
        environment, layout, args.commit, verdict_seconds=args.verdict_seconds, steady_seconds=args.steady_seconds
    )
