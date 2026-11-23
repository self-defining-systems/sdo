"""The SREGym workload adapter of the lane platform: one verb per call, environment in, last stdout line JSON out.

``python -m benchmarks.sregym.lanes.workload <verb>`` with ``WORKLOAD_ACTION`` and ``WORKLOAD_ARGS`` (a JSON array of
``--flag value`` options) and the lane variables in the environment (``benchmarks/lanes/docs/contract.md``).

The app pack is a directory with ``app.json`` (``AppPack``). The lane's fastloop environment lives at
``$SREGYM_LANES_ENV/<lane>`` (a host variable of the lanes inventory); ``SREGYM_LANES_UP_ARGS`` adds host-specific
``fastloop up`` flags (images, CPU and memory caps), ``SREGYM_LANES_RUN_ARGS`` ``fastloop run`` flags.

Verbs:

``identity``      the workload id, the checkout's revision and whether tracked files are modified.
``app-up``        (``BENCH_LANE_PROVIDER=eks``: the provider owns the cluster; ``up`` reuses the lane when its
                  environment's recorded cluster passes the provider's fence, else ``fastloop up --platform eks``;
                  never kind.)
                  ``check-pack``: the pack loads; ``up``: ``fastloop up`` unless the lane's environment and cluster
                  are already there, then the lane kubeconfig; ``down``, ``images``: nothing (fastloop's row-start
                  reset restores the app, ``up`` delivers images).
``app-snapshot``  ``write``/``verify``: the namespace's workloads and their images (a fingerprint of the deploy).
``app-reset``     ``recover``: nothing to undo (``fastloop run`` recovers and cleans up its own fault).
``gate``          ``check``: every pod of the app namespace is Ready.
``run``           ``run``: one ``fastloop run`` of the problem (``--instance``) with the spec's agent (the scripted
                  registrations, or a real SDO agent when ``operator.settings`` names a model: ``agent.py``), from
                  the planned memory commit: the operator file's seed (a memory directory a plan edge exported,
                  which must descend from its baseline), else its baseline, else the lane's onboarding commit.
                  The record (last line) has
                  ``operator``, ``valid``, ``lifecycle.inject_at`` and the SREGym summary; the accepted memory after the
                  run is exported to ``$FUZZOPS_EXEC_OUT/memory`` (``memory.bundle`` and ``memory.json``).
                  ``active``/``undo``: the last run's verdict: clear only when it never injected, or with no
                  recovery error fastloop verified the full cleanup or the oracle saw the fault mitigated; in flight,
                  killed or unknown is active.
                  On a host with ``SREGYM_LANES_RESEED=1`` a row on a lane whose last row left an SDO install of the
                  same identity (``lane-install.json``: application, namespace, images, agent and configuration) runs
                  ``fastloop run --warm-lane``; the record's ``reseed`` says whether the lane was reseeded, fell back
                  to the fresh install (and why) or did not try (and why).
``grade``         ``report``: ``$WORKLOAD_RESULTS/runs.jsonl`` (the run records) rewritten as one grade per run.
``footprint``     unsupported.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol

from benchmarks.sregym.adapter import CURRENT_WAIT_ENV, WaitKind, control_namespace_for, wait_for
from benchmarks.sregym.fastloop.host_limits import HostLimits
from benchmarks.sregym.lanes.agent import REQUESTED_IMAGES_FILE, requested_images, settings_for

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

UNSUPPORTED = 64
EXIT_INVALID = 3
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTED_REGISTRATIONS = "benchmarks/sregym/fastloop/agent_registrations"
MEMORY_DIR = "memory"
BUNDLE = "memory.bundle"
MEMORY_FILE = "memory.json"
DEFAULT_RUN_TIMEOUT = 1500
#: Past the spec's ready_timeout (the gate) and cap (inject to idle), the margin a row's close-out and recovery get
#: before the adapter kills the run: no wait of the run path outlives its row.
ROW_CLOSEOUT_SECONDS = 600
#: After TERM, how long a run that is killed at its row deadline gets before its whole process tree is killed.
KILL_GRACE_SECONDS = 30.0


class AdapterError(RuntimeError):
    """A verb that cannot do its work; the message is the reason the platform records."""


class RowDeadlineError(AdapterError):
    """The run outlived its row deadline (the spec's ready_timeout + cap + a close-out margin) and was killed; the
    message names the wait it was in."""


class Runner(Protocol):
    def __call__(self, argv: Sequence[str], *, env: Mapping[str, str] | None = None, cwd: Path | None = None) -> str:
        """Run argv to completion; return its stdout; raise ``AdapterError`` on a non-zero exit."""
        ...


class LongRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: Path | None = None,
        tick: Callable[[], None] | None = None,
        deadline_seconds: float | None = None,
        current_wait: Path | None = None,
    ) -> str:
        """Run a long command to completion, calling ``tick`` every few seconds; raise ``AdapterError`` on failure,
        and ``RowDeadlineError`` (after killing its process tree) when it runs past ``deadline_seconds``.
        ``current_wait`` is the file the command records the wait it is in."""
        ...


def run_process(argv: Sequence[str], *, env: Mapping[str, str] | None = None, cwd: Path | None = None) -> str:
    """The real runner: stderr passes through to the platform's log. A command that outlives ``wait_for(OPERATION)``
    is killed and named (a ``kubectl`` or ``git`` that never answers must not hold the verb to the platform's limit)."""
    limit = wait_for(WaitKind.OPERATION)
    try:
        result = subprocess.run(
            list(argv),
            env=dict(env) if env is not None else None,
            cwd=cwd,
            stdout=subprocess.PIPE,
            text=True,
            check=False,
            timeout=limit,
        )
    except subprocess.TimeoutExpired as error:
        raise AdapterError(f"{shlex.join(argv)[:300]}: no answer in {limit:.0f}s, killed") from error
    if result.returncode:
        raise AdapterError(f"{shlex.join(argv)[:300]}: exit {result.returncode}")
    return result.stdout


TICK_SECONDS = 5.0


def _named_wait(current_wait: Path | None) -> str:
    """The wait the killed run last recorded in ``current_wait`` ("" when it recorded none)."""
    if current_wait is None:
        return ""
    try:
        named = json.loads(current_wait.read_text(encoding="utf-8"))
        return f"{named['wait']} (since {named.get('since', '?')})"
    except (OSError, ValueError, KeyError, TypeError):
        return ""


def _kill_tree(process: subprocess.Popen[bytes]) -> None:
    """TERM the run's process group, then KILL what is left after the grace (a child that ignores TERM included)."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=KILL_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def _terminate_with(process: subprocess.Popen[bytes]) -> Callable[[], None]:
    """Make a TERM or INT of this adapter end the run's whole process tree, and return the call that undoes it.

    The run is in a session of its own (so a deadline can kill its tree), which the platform's kill of the adapter's
    process group no longer reaches: without this the cancelled run would live on, orphaned, holding the lane. The tree
    gets TERM and ``KILL_GRACE_SECONDS`` (a cancelled run undoes its fault), then KILL; the adapter then exits."""
    if threading.current_thread() is not threading.main_thread():
        return lambda: None

    def handler(signum: int, _frame: object) -> None:
        _kill_tree(process)
        raise SystemExit(128 + signum)

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGTERM, signal.SIGINT)}

    def restore() -> None:
        for sig, old in previous.items():
            signal.signal(sig, old)

    return restore


def run_logged(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    tick: Callable[[], None] | None = None,
    deadline_seconds: float | None = None,
    current_wait: Path | None = None,
) -> str:
    """A long command whose stdout is progress: both streams go to the platform's log (stderr). ``tick`` is called
    every few seconds while it runs (the fallback page watches the run from there).

    The command runs in a process group of its own. Past ``deadline_seconds`` the group is killed (TERM, then KILL
    after ``KILL_GRACE_SECONDS``) and ``RowDeadlineError`` names the wait ``current_wait`` says it was in."""
    process = subprocess.Popen(
        list(argv),
        env=dict(env) if env is not None else None,
        cwd=cwd,
        stdout=sys.stderr,
        start_new_session=True,
    )
    expires = None if deadline_seconds is None else time.monotonic() + deadline_seconds
    restore = _terminate_with(process)
    try:
        while True:
            wait = TICK_SECONDS if expires is None else max(0.0, min(TICK_SECONDS, expires - time.monotonic()))
            try:
                code = process.wait(timeout=wait)
                break
            except subprocess.TimeoutExpired:
                if expires is not None and time.monotonic() >= expires:
                    named = _named_wait(current_wait)
                    _kill_tree(process)
                    raise RowDeadlineError(
                        f"row deadline {deadline_seconds:.0f}s exceeded (ready_timeout + cap + closeout); "
                        f"the run was killed while waiting for: {named or 'no wait recorded'}"
                    ) from None
                if tick is not None:
                    tick()
    finally:
        restore()
    if code:
        raise AdapterError(f"{shlex.join(argv)[:300]}: exit {code}")
    return ""


def parse_args(raw: str) -> dict[str, str]:
    """``WORKLOAD_ARGS`` (a JSON array of ``--flag value`` or ``--flag=value``) as a mapping.

    A bare flag is ``"true"``; a leading positional argument is ``"_"``."""
    items = json.loads(raw or "[]")
    if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
        raise AdapterError(f"WORKLOAD_ARGS must be a JSON array of strings, not {raw!r}")
    out: dict[str, str] = {}
    i = 0
    while i < len(items):
        item = items[i]
        if not item.startswith("--"):
            out.setdefault("_", item)
            i += 1
            continue
        key, eq, value = item[2:].partition("=")
        if not eq:
            if i + 1 < len(items) and not items[i + 1].startswith("--"):
                value = items[i + 1]
                i += 1
            else:
                value = "true"
        out[key] = value
        i += 1
    return out


@dataclass(frozen=True)
class AppPack:
    """``app.json`` of a pack: the SREGym application, its namespace and the problem ``fastloop up`` deploys."""

    application: str
    namespace: str
    deploy_problem: str
    up_args: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("application", "namespace", "deploy_problem"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"app.json: {name} must be a non-empty string")

    @classmethod
    def load(cls, pack: Path) -> AppPack:
        path = pack / "app.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise AdapterError(f"app pack {pack}: {error}") from error
        try:
            return cls(
                application=raw.get("application", ""),
                namespace=raw.get("namespace", ""),
                deploy_problem=raw.get("deploy_problem", ""),
                up_args=tuple(raw.get("up_args", ())),
            )
        except ValueError as error:
            raise AdapterError(f"app pack {pack}: {error}") from error


_LANE_RE = re.compile(r"^(?P<prefix>[a-z][a-z0-9-]*?)(?P<worker>[0-9]+)$")


def cluster_parts(lane: str) -> tuple[str, int]:
    """fastloop names a cluster ``<prefix><worker id>``: a lane name must end in its worker id."""
    match = _LANE_RE.match(lane)
    if not match:
        raise AdapterError(f"lane {lane!r}: an SREGym lane name is a prefix and a worker number, such as sgl1")
    return match["prefix"], int(match["worker"])


@dataclass
class Context:
    """One call: its verb, operation, options and environment, and the seams the verbs use."""

    verb: str
    action: str
    args: dict[str, str]
    env: Mapping[str, str]
    runner: Runner = run_process
    long_runner: LongRunner = run_logged
    root: Path = REPO_ROOT
    python: str = sys.executable
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    #: Whether the lane's recorded EKS cluster is the one KUBECONFIG reaches (``eks_fence``); a seam for tests.
    eks_fence: Callable[[Context, Path], bool] = field(default=lambda ctx, env_dir: eks_fence(ctx, env_dir))
    #: The fallback page's view of the lane (``eval.page.PageCluster``); None: the lane's controller over kubectl.
    page_cluster: Any = None

    def need(self, name: str) -> str:
        value = self.env.get(name, "")
        if not value:
            raise AdapterError(f"{name} is not set")
        return value

    def cap_seconds(self) -> float | None:
        """The spec's cap (injection to idle) from ``BENCH_CAP_SECONDS``; None without a positive one."""
        try:
            cap = float(self.env.get("BENCH_CAP_SECONDS", ""))
        except ValueError:
            return None
        return cap if cap > 0 else None

    def row_deadline_seconds(self) -> float | None:
        """The row's deadline: the spec's ready_timeout (the gate) plus its cap (inject to idle) plus a close-out
        margin, from the step's ``BENCH_READY_TIMEOUT_SECONDS`` and ``BENCH_CAP_SECONDS``; None without a cap."""
        try:
            cap = float(self.env.get("BENCH_CAP_SECONDS", ""))
        except ValueError:
            return None
        if cap <= 0:
            return None
        try:
            ready = max(0.0, float(self.env.get("BENCH_READY_TIMEOUT_SECONDS", "0")))
        except ValueError:
            ready = 0.0
        return ready + cap + ROW_CLOSEOUT_SECONDS

    def lane_env_dir(self) -> Path:
        return Path(self.need("SREGYM_LANES_ENV")) / self.need("BENCH_LANE")

    def pack(self) -> AppPack:
        return AppPack.load(Path(self.args.get("pack") or self.need("BENCH_PACK")))

    def kubectl(self, *args: str) -> str:
        # The server's own bound: a lane API server that stops answering ends the call, not the verb's limit.
        request = f"--request-timeout={wait_for(WaitKind.SLOW_COMMAND):.0f}s"
        return self.runner(["kubectl", "--kubeconfig", self.need("KUBECONFIG"), request, *args], env=self.env)


# ----- verbs -----


def identity(ctx: Context) -> tuple[int, dict[str, Any]]:
    revision = ctx.runner(["git", "-C", str(ctx.root), "rev-parse", "HEAD"]).strip()
    dirty = ctx.runner(["git", "-C", str(ctx.root), "status", "--porcelain", "--untracked-files=no"]).strip()
    return 0, {"workload": "sregym", "revision": revision, "modified": bool(dirty)}


def eks_up_args(ctx: Context) -> tuple[str, ...]:
    """The ``fastloop up`` flags of an EKS lane: attach to the provider's cluster and push images to its registry.

    ``BENCH_IMAGE_TAGS`` (``k=v,...``) tags the repositories the lane creates."""
    args = ["--platform", "eks", "--no-dev-mount", "--ecr-registry", ctx.need("BENCH_IMAGE_REGISTRY")]
    for tag in filter(None, (item.strip() for item in ctx.env.get("BENCH_IMAGE_TAGS", "").split(","))):
        args += ["--ecr-tag", tag]
    return tuple(args)


def eks_fence(ctx: Context, env_dir: Path) -> bool:
    """Whether the lane's recorded EKS cluster is the one KUBECONFIG reaches (the provider's fence)."""
    from benchmarks.lanes.eks import EksLane, EksLaneError, assert_lane_cluster

    recorded = json.loads((env_dir / "fastloop.json").read_text(encoding="utf-8")).get("eks")
    if not recorded:
        return False
    try:
        assert_lane_cluster(ctx.need("KUBECONFIG"), EksLane.model_validate(recorded))
    except EksLaneError:
        return False
    return True


def app_up(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action == "check-pack":
        try:
            pack = ctx.pack()
        except AdapterError as error:
            print(str(error), file=sys.stderr)
            return EXIT_INVALID, {"error": str(error)}
        try:
            HostLimits.from_env(ctx.env).check_capacity()
        except ValueError as error:
            print(str(error), file=sys.stderr)
            return EXIT_INVALID, {"error": str(error)}
        return 0, {"application": pack.application, "namespace": pack.namespace}
    if ctx.action in ("down", "images"):
        # fastloop's row-start reset restores the app; `up` delivers the images.
        return 0, {"skipped": ctx.action}
    if ctx.action != "up":
        return UNSUPPORTED, {"unsupported": True}
    pack = ctx.pack()
    lane = ctx.need("BENCH_LANE")
    env_dir = ctx.lane_env_dir()
    ctx.need("KUBECONFIG")  # the lane's kubeconfig is copied after `fastloop up`: a missing one fails now, not then
    agent = settings_for(ctx.env)  # a model this lane cannot run is refused before the lane is built
    eks = ctx.env.get("BENCH_LANE_PROVIDER") == "eks"
    clusters: list[str] = []
    if eks:
        # An EKS lane's cluster is the provider's (it exists before any step runs): no kind, no cluster.sh.
        reused = (env_dir / "fastloop.json").is_file() and ctx.eks_fence(ctx, env_dir)
    else:
        clusters = ctx.runner(["kind", "get", "clusters"], env=ctx.env).split()
        reused = (env_dir / "fastloop.json").is_file() and lane in clusters
        if not reused and ctx.env.get(CLUSTER_UP_ENV) != "1":
            # Only the host's cluster_up (cluster.sh) makes a lane: it starts the kind nodes under the lane's cgroup,
            # cores and memory. A build that finds no lane fails, and the platform makes the lane anew.
            raise AdapterError(
                f"lane {lane} has no fastloop environment or kind cluster: the host's cluster_up makes it"
            )
    if reused and agent is not None:
        # `up` does not run again on a reused lane: its controller and responder stay the images it was built with. A
        # spec naming others fails this build, which the platform answers by making the cluster anew.
        asked = env_dir / REQUESTED_IMAGES_FILE
        built = json.loads(asked.read_text(encoding="utf-8")) if asked.is_file() else {}
        if mismatch := agent.image_mismatch(built):
            raise AdapterError(f"{mismatch}: the lane's cluster must be made anew")
    if not reused:
        prefix, worker = cluster_parts(lane)
        if eks:
            if env_dir.exists():
                # An environment of another cluster or a failed `up`: this lane's own directory, nothing else.
                shutil.rmtree(env_dir, ignore_errors=True)
        elif lane in clusters or env_dir.exists():
            # What a failed `up` of this lane left (fastloop refuses an existing cluster name for a new environment):
            # the lane's own cluster and environment, nothing else.
            ctx.runner(["kind", "delete", "cluster", "--name", lane], env=ctx.env)
            shutil.rmtree(env_dir, ignore_errors=True)
        command = [
            ctx.python,
            "-m",
            "benchmarks.sregym.fastloop",
            "up",
            "--run-dir",
            str(env_dir),
            "--problem",
            pack.deploy_problem,
            "--cluster-prefix",
            prefix,
            "--worker-id",
            str(worker),
            *(eks_up_args(ctx) if eks else ()),
            *pack.up_args,
            *shlex.split(ctx.env.get("SREGYM_LANES_UP_ARGS", "")),
            # A real agent's operator images (settings.responder_image/controller_image) override the host's.
            *(agent.up_flags() if agent else ()),
        ]
        if seed := ctx.env.get(f"SREGYM_LANES_SEED_{pack.application}"):
            # The host's onboarded memory for this application (a git repository), per application: one host
            # serves lanes of several apps.
            command += ["--seed", seed]
        if evidence := ctx.env.get("BENCH_EVIDENCE_HOST"):
            # Where the evidence lands on this host; fastloop decides how it gets there (a node mount on kind, a pull
            # of the controller's claim on EKS, BENCH_EVIDENCE_MODE=pull).
            command += ["--evidence-root", evidence]
        ctx.long_runner(command, env=ctx.env, cwd=ctx.root)
        # What this lane was asked to run: `fastloop.json` keeps what `up` delivered (an EKS lane: ECR digests).
        (env_dir / REQUESTED_IMAGES_FILE).write_text(json.dumps(requested_images(command)) + "\n", encoding="utf-8")
    environment = json.loads((env_dir / "fastloop.json").read_text(encoding="utf-8"))
    if environment.get("namespace") != (ctx.args.get("namespace") or pack.namespace):
        raise AdapterError(
            f"lane app namespace {environment.get('namespace')!r}, spec namespace {ctx.args.get('namespace')!r}"
        )
    kubeconfig = Path(ctx.need("KUBECONFIG"))
    source = Path(environment["kubeconfig"])
    if source.resolve() != kubeconfig.resolve():
        kubeconfig.parent.mkdir(parents=True, exist_ok=True)
        kubeconfig.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return 0, {"reused": reused, "environment": str(env_dir)}


def _fingerprint(ctx: Context, namespace: str) -> list[str]:
    raw = json.loads(ctx.kubectl("-n", namespace, "get", "deployments,statefulsets", "-o", "json"))
    rows = []
    for item in raw.get("items", []):
        spec = item.get("spec", {})
        images = sorted(c.get("image", "") for c in spec.get("template", {}).get("spec", {}).get("containers", []))
        rows.append(
            f"{item.get('kind')}/{item['metadata']['name']} replicas={spec.get('replicas')} images={','.join(images)}"
        )
    return sorted(rows)


def app_snapshot(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action not in ("write", "verify"):
        return UNSUPPORTED, {"unsupported": True}
    namespace = ctx.args.get("namespace") or ctx.need("FUZZOPS_NAMESPACE")
    rows = _fingerprint(ctx, namespace)
    if ctx.action == "write":
        out = Path(ctx.args["out"])
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"namespace": namespace, "workloads": rows}, indent=1) + "\n", encoding="utf-8")
        return 0, {"workloads": len(rows)}
    if ctx.action == "verify":
        want = json.loads(Path(ctx.args["against"]).read_text(encoding="utf-8")).get("workloads", [])
        differ = sorted(set(want) ^ set(rows))
        return (0 if not differ else 1), {"differ": differ}
    return UNSUPPORTED, {"unsupported": True}


def operator_owned_objects(ctx: Context, namespace: str) -> list[str]:
    """The lane's cluster-scoped objects that SDO's install for ``namespace`` makes (``kubectl get -o name`` names).

    ``fastloop run`` installs SDO at every row start after wiping the last row's install (tier 1: every cluster object
    not in the lane's inventory goes), so the install's namespace, ClusterRoles and bindings are there at the end of
    every row and are never fault residue. They are found by who they bind: a ClusterRoleBinding with a ServiceAccount
    of the control namespace, and the ClusterRole it grants; a fault's own objects bind none of them.
    """
    from benchmarks.sregym.fastloop.wipe import install_owned_objects

    control = control_namespace_for(namespace)
    bindings = json.loads(ctx.kubectl("get", "clusterrolebindings", "-o", "json")).get("items", [])
    return sorted(install_owned_objects(control, bindings))


def app_reset(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action == "owned":
        namespace = ctx.args.get("namespace") or ctx.need("FUZZOPS_NAMESPACE")
        return 0, {"objects": operator_owned_objects(ctx, namespace)}
    if ctx.action != "recover":
        return UNSUPPORTED, {"unsupported": True}
    # fastloop run recovers and cleans up its own fault before it returns, and resets the lane at row start.
    return 0, {"recovered": "nothing left by fastloop run"}


def gate(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action != "check":
        return UNSUPPORTED, {"unsupported": True}
    namespace = ctx.args.get("namespace") or ctx.need("FUZZOPS_NAMESPACE")
    pods = json.loads(ctx.kubectl("-n", namespace, "get", "pods", "-o", "json")).get("items", [])
    not_ready = []
    for pod in pods:
        if pod.get("status", {}).get("phase") == "Succeeded":
            continue
        conditions = {c.get("type"): c.get("status") for c in pod.get("status", {}).get("conditions", [])}
        if conditions.get("Ready") != "True":
            not_ready.append(pod["metadata"]["name"])
    if not pods:
        return 1, {"clean": False, "why": f"no pods in {namespace}"}
    return (0 if not not_ready else 1), {"clean": not not_ready, "not_ready": not_ready}


@dataclass(frozen=True)
class MemorySeed:
    """A memory directory a plan edge exported: the accepted repository's bundle and the commit to run from."""

    directory: Path
    commit: str

    @classmethod
    def load(cls, directory: Path) -> MemorySeed:
        try:
            raw = json.loads((directory / MEMORY_FILE).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise AdapterError(f"memory seed {directory}: {error}") from error
        commit = raw.get("commit", "")
        if not re.fullmatch(r"[0-9a-f]{40}", commit) or not (directory / BUNDLE).is_file():
            raise AdapterError(f"memory seed {directory}: needs {MEMORY_FILE} with a 40-hex commit and {BUNDLE}")
        return cls(directory, commit)


def operator_input(env: Mapping[str, str], key: str) -> Path | None:
    """A directory of the operator file (``BENCH_OPERATOR``: ``baseline`` or ``seed``), staged on this host."""
    path = env.get("BENCH_OPERATOR")
    if not path:
        return None
    value = json.loads(Path(path).read_text(encoding="utf-8")).get(key)
    return Path(value) if value else None


def operator_seed(env: Mapping[str, str]) -> Path | None:
    return operator_input(env, "seed")


def _is_ancestor(ctx: Context, workspace: Path, ancestor: str, commit: str) -> bool:
    try:
        _git(ctx, workspace, "merge-base", "--is-ancestor", ancestor, commit)
    except AdapterError:
        return False
    return True


def _git(ctx: Context, workspace: Path, *args: str) -> str:
    path = str(workspace.resolve())
    return ctx.runner(["git", "-c", f"safe.directory={path}", "-C", path, *args], env=ctx.env)


def stream_settings(problem: str) -> dict[str, Any]:
    """One problem with the production capability review and responder (as the real-app rehearsal runs it)."""
    return {
        "incidents": [problem],
        "diagnosis_grading": "off",
        "capability_mode": "permissive",
        "permissive_responder": True,
    }


def token_usd(counts: Mapping[str, Any] | None, provider: str | None, model: str | None) -> float | None:
    """The USD of one recorded token usage at agentshim's price for the row's model: ``0.0`` when no model was called,
    ``None`` (unknown, never zero) when the usage cannot be priced."""
    from agentshim import TokenUsage, cost_usd, default_pricing, price_for

    counts = counts or {}
    if not any(counts.get(name) for name in ("input_tokens", "output_tokens", "model_requests")):
        return 0.0
    entry = price_for(provider or "", model, default_pricing()) if provider and model else None
    if entry is None:
        return None
    usage = TokenUsage(
        input_tokens=int(counts.get("input_tokens") or 0),
        output_tokens=int(counts.get("output_tokens") or 0),
        cache_read_input_tokens=int(counts.get("cached_input_tokens") or 0),
        cache_write_input_tokens=int(counts.get("cache_write_input_tokens") or 0),
        cache_write_1h_input_tokens=int(counts.get("cache_write_1h_input_tokens") or 0),
        reasoning_output_tokens=int(counts.get("reasoning_output_tokens") or 0),
    )
    return round(cost_usd(usage, entry), 6)


def summarize(record: Mapping[str, Any]) -> dict[str, Any]:
    """The SREGym part of a run record, from a fastloop incident record."""
    injection = record.get("injection") or {}
    oracle = record.get("oracle") or {}
    keys = (
        "run_id",
        "problem_id",
        "detected_at",
        "dispatched_at",
        "responder_started_at",
        "responder_finished_at",
        "mitigation_applied_at",
        "resolved_at",
        "time_to_fix_seconds",
        "resolution_path",
        "sdo_resolution",
        "sdo_outcome_classification",
        "warm_path",
        "match_reasons",
        "reflection_started_at",
        "reflection_finished_at",
        "reflection_attempts",
        "reflection_skipped_reason",
        "memory_before_commit",
        "memory_after_commit",
        "full_cleanup_verified",
        "fault_recovery_deferred",
        "infra_error",
        "recovery_error",
        "error",
        "incident_id",
        "evidence_incomplete",
        "provider",
        "model",
        "reasoning_effort",
    )
    out = {key: record.get(key) for key in keys}
    out["injected_at"] = injection.get("started_at")
    out["oracle_kind"] = oracle.get("kind")
    out["success"] = bool(oracle.get("success"))
    # The responder's and the closing turn's own usage, from the row's accounting (the responder session's tokens
    # and the reflection turns'), priced at the row's model: what the final report's cost columns read.
    provider, model = record.get("provider"), record.get("model")
    for role in ("responder", "reflection"):
        counts = record.get(f"{role}_tokens") or {}
        out[f"{role}_tokens"] = counts
        out[f"{role}_requests"] = int(counts.get("model_requests") or 0)
        out[f"{role}_usd"] = token_usd(counts, provider, model)
    return out


def _last_record(results: Path) -> dict[str, Any] | None:
    path = results / "incidents.jsonl"
    if not path.is_file():
        return None
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else None


def export_memory(ctx: Context, workspace: Path, out: Path, base: str) -> dict[str, Any]:
    """The accepted repository's HEAD as a bundle and its commit, for the next arm's seed."""
    head = _git(ctx, workspace, "rev-parse", "--verify", "HEAD^{commit}").strip()
    out.mkdir(parents=True, exist_ok=True)
    _git(ctx, workspace, "bundle", "create", str((out / BUNDLE).resolve()), "HEAD")
    changed = _git(ctx, workspace, "diff", "--name-only", base, head, "--", ".sdo/playbooks").split() if base else []
    meta = {"commit": head, "base": base, "playbook_files_changed": changed}
    (out / MEMORY_FILE).write_text(json.dumps(meta, indent=1) + "\n", encoding="utf-8")
    return meta


#: The eval runner's defaults (``EvalConfig.fallback_page_seconds``, ``detection_timeout_seconds``), which the
#: campaign's SDO arm runs with.
FALLBACK_PAGE_SECONDS = 120.0
DETECTION_TIMEOUT = 300


class KubectlLaneCluster:
    """``eval.page.PageCluster`` over the lane's own kubeconfig (``lane.info``)."""

    def own_incident(self, lane: Any, known: frozenset[str]) -> bool:
        from benchmarks.sregym.adapter import control_namespace_for, read_runtime_state
        from benchmarks.sregym.eval.page import has_own_incident

        control = control_namespace_for(lane.info["namespace"])
        return has_own_incident(read_runtime_state(control, kubeconfig=Path(lane.info["kubeconfig"])), known)

    def send(self, lane: Any, incident_id: str, message: str) -> None:
        from benchmarks.sregym.adapter import apply_external_incident, control_namespace_for

        namespace = lane.info["namespace"]
        apply_external_incident(
            control_namespace_for(namespace), namespace, incident_id, message, kubeconfig=Path(lane.info["kubeconfig"])
        )


@dataclass
class FallbackPager:
    """The eval runner's fallback page for one run (``eval.page.FallbackPageHook``, as ``fastloop_row`` drives it): once
    fastloop's injection marker exists and ``seconds`` have passed with no incident of SDO's own, SDO is paged through
    the external-incident inbox with the vanilla arm's incident text. ``out`` (page.json) records the decision; a failed
    page is recorded and tried again at the next tick."""

    results: Path
    out: Path
    lane: Any
    run_id: str
    seconds: float
    cluster: Any
    now: Callable[[], float]
    source: str | None = None
    #: The instant SDO was paged (epoch seconds), when the fallback page is the clock.
    paged_at: float | None = None
    hook: Any = None

    def __post_init__(self) -> None:
        from benchmarks.sregym.eval.page import FallbackPageHook

        self.hook = FallbackPageHook(self.seconds, self.cluster or KubectlLaneCluster(), self.now)

    def tick(self) -> None:
        if self.source is not None:
            return
        from benchmarks.sregym.fastloop.injection_marker import read_injection_marker

        marker = read_injection_marker(self.results)
        if marker is None:
            return
        cell = SimpleNamespace(arm="sdo", id=self.run_id)
        try:
            paged_at = self.hook.page(self.lane, cell, marker.started_at, marker.known_incident_ids)
        except Exception as exc:  # recorded, retried at the next tick, never stops the run
            self._write({"injected_at": marker.started_at, "error": f"{type(exc).__name__}: {exc}"})
            return
        own_at = self.hook.own_incident_at(cell)
        if paged_at is not None:
            self.source = "fallback"
            self.paged_at = paged_at
            self._write({"injected_at": marker.started_at, "paged_at": paged_at, "source": "fallback"})
        elif own_at is not None:
            self.source = "own"
            self._write({"injected_at": marker.started_at, "decided_at": own_at, "source": "own"})

    def _write(self, document: dict[str, Any]) -> None:
        self.out.parent.mkdir(parents=True, exist_ok=True)
        self.out.write_text(json.dumps(document) + "\n", encoding="utf-8")


FAULT_STATE = "lanes-fault.json"
#: Where the run records the wait it is in (beside the run's other exec output).
CURRENT_WAIT_FILE = "current-wait.json"
#: Set by cluster.sh (the host's cluster_up): only then may ``app-up up`` make the lane's cluster.
CLUSTER_UP_ENV = "SREGYM_LANES_CLUSTER_UP"


def write_fault_state(env_dir: Path, problem: str, state: str) -> None:
    """The lane's fault verdict for `run active`: ``unknown`` while a run is in flight, then ``clear`` (never injected,
    or fastloop verified the full cleanup with no recovery error) or ``active``."""
    (env_dir / FAULT_STATE).write_text(json.dumps({"problem": problem, "state": state}) + "\n", encoding="utf-8")


def fault_active(env_dir: Path, problem: str) -> bool:
    """Whether the lane may still hold the fault: anything but a ``clear`` verdict for this problem is active."""
    try:
        verdict = json.loads((env_dir / FAULT_STATE).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True
    return not (verdict.get("state") == "clear" and verdict.get("problem") == problem)


def without_flag(args: Sequence[str], flag: str) -> list[str]:
    """``args`` with every ``--flag VALUE`` / ``--flag=VALUE`` of ``flag`` removed."""
    out: list[str] = []
    skip = False
    for item in args:
        if skip:
            skip = False
        elif item == flag:
            skip = True
        elif not item.startswith(f"{flag}="):
            out.append(item)
    return out


#: Written next to ``fastloop.json`` once a row has run on the lane: what the SDO install on it was made with.
LANE_INSTALL_FILE = "lane-install.json"
#: The host's switch for running rows on an installed lane through fastloop's warm transaction (reseed) instead of
#: the row-start wipe and reinstall; while the transaction is proven on a host this is the orchestrator's to set.
RESEED_ENV = "SREGYM_LANES_RESEED"
#: ``fastloop run --warm-lane`` writes it into the row's results: whether the lane was reused, else why not.
WARM_RECEIPT = "warm-transaction.json"


#: Operator settings (``settings.config`` paths) that are a row's, not its install's: every row's controller is
#: installed from the row's own config overlay (fresh, or by the warm transaction's reseed), and these change only what
#: follows the row's closure. ``learning.closing_turn`` differs between the arms of one item that share a lane (only
#: the arm whose memory seeds another learns), so it must not make the next arm's lane look like another install.
PER_ROW_CONFIG = (("learning", "closing_turn"),)


def row_learns(real: Any) -> bool:
    """Whether the row's closing turn runs (``learning.closing_turn``, default on); the scripted agent's rows do."""
    if real is None:
        return True
    learning = (getattr(real, "config", None) or {}).get("learning") or {}
    return learning.get("closing_turn", True) is not False


def _install_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """The config overlay without the per-row settings (``PER_ROW_CONFIG``); a section they leave empty goes too."""
    kept = json.loads(json.dumps(dict(config)))
    for section, key in PER_ROW_CONFIG:
        part = kept.get(section)
        if isinstance(part, dict):
            part.pop(key, None)
            if not part:
                del kept[section]
    return kept


def install_identity(environment: Mapping[str, Any], built: Mapping[str, str], real: Any) -> dict[str, Any]:
    """What a row needs the lane's SDO install to be: the application, the images, the agent and its configuration.

    The controller's own settings are checked by fastloop against the installed controller
    (``WarmEnvironment.inspect``); this is the workload's cheap early decision, and what names the reason a row did
    not try."""
    images = {role: built.get(role, "") for role in ("controller", "responder")}
    agent: dict[str, str] | None = None
    if real is not None:
        images.update(
            {
                role: wanted
                for role, wanted in (("controller", real.controller_image), ("responder", real.responder_image))
                if wanted
            }
        )
        agent = {
            "provider": real.provider,
            "model": real.model,
            "effort": real.effort,
            "config_digest": hashlib.sha256(
                json.dumps(_install_config(real.config), sort_keys=True).encode()
            ).hexdigest(),
        }
    return {
        "application": str(environment.get("application", "")),
        "namespace": str(environment.get("namespace", "")),
        "images": images,
        "agent": agent,
    }


def _flatten(prefix: str, value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            key: item
            for name, part in value.items()
            for key, item in _flatten(f"{prefix}.{name}" if prefix else name, part).items()
        }
    return {prefix: value}


def reseed_decision(ctx: Context, env_dir: Path, identity: Mapping[str, Any]) -> str:
    """Why this row does not try the warm transaction ("" when it does)."""
    if ctx.env.get(RESEED_ENV) != "1":
        return f"reseed is not enabled for this host ({RESEED_ENV})"
    try:
        recorded = json.loads((env_dir / LANE_INSTALL_FILE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return "no earlier install on this lane"
    except (OSError, ValueError):
        return f"{LANE_INSTALL_FILE} is unreadable"
    had, wants = _flatten("", recorded), _flatten("", dict(identity))
    differ = sorted(name for name in {*had, *wants} if had.get(name) != wants.get(name))
    return "install identity differs: " + ", ".join(differ) if differ else ""


def keep_install_identity(env_dir: Path, identity: Mapping[str, Any], *, installed: bool) -> None:
    """Record what the lane's install is after a row that ran on it; a row that left no record leaves it unknown."""
    path = env_dir / LANE_INSTALL_FILE
    if installed:
        path.write_text(json.dumps(identity, indent=1) + "\n", encoding="utf-8")
    else:
        path.unlink(missing_ok=True)


def _fallback_reason(receipt: Mapping[str, Any]) -> str:
    """fastloop's ``<phase>:<error type>``, then the lane's own refusal text when it gave one."""
    reason = str(receipt.get("fallback_reason") or "unknown")
    detail = str(receipt.get("detail") or "")
    return f"{reason}: {detail}" if detail else reason


def reseed_record(results: Path, *, attempted: bool, reason: str) -> dict[str, Any]:
    """The row's ``reseed`` field: tried or not, reused or fell back, and why (from fastloop's receipt)."""
    record: dict[str, Any] = {
        "attempted": attempted,
        "reused": False,
        "reason": reason,
        "violations": [],
        "phase_seconds": {},
    }
    if not attempted:
        return record
    try:
        receipt = json.loads((results / WARM_RECEIPT).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {**record, "reason": "no warm-transaction receipt"}
    reused = receipt.get("reused") is True
    return {
        **record,
        "reused": reused,
        "reason": "" if reused else _fallback_reason(receipt),
        "violations": [str(item) for item in receipt.get("violations") or []],
        "phase_seconds": dict(receipt.get("phase_seconds") or {}),
    }


def run(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action in ("active", "undo"):
        # The adapter cannot undo a fault itself: both answer from the last run's verdict, and an unknown one is
        # active (fail safe: the platform then takes the lane down instead of running the next item on a live fault).
        active = fault_active(ctx.lane_env_dir(), ctx.args.get("id", ""))
        return 0, {"active": active, "state": f"active={str(active).lower()}"}
    if ctx.action != "run":
        return UNSUPPORTED, {"unsupported": True}
    problem = ctx.args.get("instance") or ""
    label = ctx.args.get("operator", "")
    started = ctx.now()
    record: dict[str, Any] = {
        "operator": label,
        "fault": problem,
        "seed": int(ctx.args.get("seed", "1")),
        "started": started.isoformat(),
    }
    if not problem:
        # A healthy (no-fault) run is not an SREGym problem.
        return 1, {
            **record,
            "valid": False,
            "invalid_reason": "unsupported",
            "error": "an SREGym run needs --instance PROBLEM",
        }
    env_dir = ctx.lane_env_dir()
    # Nothing is injected until fastloop runs: a row refused before that (a lane built for another spec, a missing
    # credential file, an unreadable seed) leaves the lane clean, and an unknown verdict would be taken for an active
    # fault and take the lane Down for a human reset. The verdict turns unknown just before fastloop starts.
    write_fault_state(env_dir, problem, "clear")
    real = settings_for(ctx.env)  # a model this lane cannot run is refused here, before anything of the row starts
    environment = json.loads((env_dir / "fastloop.json").read_text(encoding="utf-8"))
    workspace = Path(environment["workspace"])
    seed_dir = operator_seed(ctx.env)
    baseline_dir = operator_input(ctx.env, "baseline")
    planned = environment.get("seed_commit") or ""
    if baseline_dir is not None:
        # The base memory every arm of the item starts from (the platform requires one beside a seed): a run with no
        # seed starts from it, and a seed must be memory learned on top of it.
        baseline = MemorySeed.load(baseline_dir)
        _git(ctx, workspace, "fetch", "--no-tags", str((baseline.directory / BUNDLE).resolve()), baseline.commit)
        planned = baseline.commit
    if seed_dir is not None:
        seed = MemorySeed.load(seed_dir)
        _git(ctx, workspace, "fetch", "--no-tags", str((seed.directory / BUNDLE).resolve()), seed.commit)
        if baseline_dir is not None and not _is_ancestor(ctx, workspace, planned, seed.commit):
            raise AdapterError(f"memory seed {seed.commit} was not learned on the baseline {planned}")
        planned = seed.commit
    exec_out = Path(ctx.env.get("FUZZOPS_EXEC_OUT") or env_dir / "exec")
    exec_out.mkdir(parents=True, exist_ok=True)
    run_id = re.sub(
        r"[^A-Za-z0-9_.-]",
        "-",
        f"{ctx.env.get('BENCH_EXPERIMENT', 'lanes')}-{ctx.env.get('BENCH_RUN', started.strftime('%Y%m%dT%H%M%S'))}",
    )
    stream = exec_out / "stream.json"
    stream.write_text(json.dumps(stream_settings(problem), indent=1) + "\n", encoding="utf-8")
    host_args = shlex.split(ctx.env.get("SREGYM_LANES_RUN_ARGS", ""))
    run_env: Mapping[str, str] = ctx.env
    asked = env_dir / REQUESTED_IMAGES_FILE
    built = json.loads(asked.read_text(encoding="utf-8")) if asked.is_file() else environment.get("images") or {}
    if real is None:
        # The scripted agent (model-free): the scripted registrations; the host's run flags (its placeholder
        # credentials hook) apply as they are.
        agent_flags = ["--agent-registrations", str(ctx.root / SCRIPTED_REGISTRATIONS)]
    else:
        if not real.auth_file.is_file():
            raise AdapterError(f"agent credential file {real.auth_file} (operator.settings.auth_file) is missing")
        if mismatch := real.image_mismatch(built):
            raise AdapterError(f"{mismatch}: rebuild the lane for this spec")
        # A real agent: SDO's own responder and closing turn, the spec's config overlay, and the credentials hook that
        # puts the credential file into the lane's Secret. The host's placeholder hook must not replace it.
        host_args = without_flag(host_args, "--credentials-hook")
        agent_flags = real.run_flags(exec_out / "operator-config.yaml")
        run_env = {**ctx.env, **real.hook_env()}
    command = [
        ctx.python,
        "-m",
        "benchmarks.sregym.fastloop",
        "run",
        "--run-dir",
        str(env_dir),
        "--run-id",
        run_id,
        "--agent",
        "sdo",
        *agent_flags,
        "--stream",
        str(stream),
        # The eval runner's SDO row flags (benchmarks/sregym/eval/fastloop_row.py row_command).
        "--detection-timeout",
        str(DETECTION_TIMEOUT),
        # The healthy snapshots serve only the closing turn's learned-detector validation (learning.healthy_baseline):
        # a row whose closing turn is off never proposes a detector, so it does not capture them (~12 s before
        # injection).
        *(["--healthy-baseline"] if row_learns(real) else []),
        "--inject-before-resume",
        "--require-fault-tracking",
        "--require-full-cleanup",
        "--continue-on-error",
        "--timeout",
        ctx.env.get("SREGYM_LANES_RUN_TIMEOUT", str(DEFAULT_RUN_TIMEOUT)),
        *host_args,
    ]
    if planned:
        command += ["--planned-commit", planned]
    identity = install_identity(environment, built, real)
    not_reseeded = reseed_decision(ctx, env_dir, identity)
    if not not_reseeded:
        # The lane holds an install of this row's identity: fastloop reseeds it (or falls back to the wipe and
        # reinstall, and says why). The workload copies the results out when fastloop returns, so the evidence is
        # delivered first.
        command += ["--warm-lane", "--deliver-evidence-before-exit"]
    if ctx.env.get(RESEED_ENV) == "1":
        # With the switch on every row may be followed by a warm one, the first row of a lane included (it installs
        # fresh): the next row neither wipes the lane nor recreates the app, so this row recovers and verifies in the
        # row instead of leaving its recovery to a Tier-2 that never comes (an unverified row refuses the warm start).
        # The proof of the restored baseline is the next start's (the warm reset grades the app), not proved twice.
        command.append("--end-of-row-recovery")
    if (cap := ctx.cap_seconds()) is not None:
        # An incident still open at the spec's cap never closes: the row ends there, named, with its evidence.
        command += ["--closure-bound", str(cap)]
    failure = ""
    results = env_dir / "results" / run_id
    pager = FallbackPager(
        results=results,
        out=exec_out / "page.json",
        lane=SimpleNamespace(
            id=ctx.need("BENCH_LANE"),
            info={
                "namespace": environment["namespace"],
                "application": environment.get("application", ""),
                "description": environment.get("description", ""),
                "kubeconfig": environment["kubeconfig"],
            },
        ),
        run_id=run_id,
        seconds=float(ctx.env.get("SREGYM_LANES_FALLBACK_PAGE_SECONDS", str(FALLBACK_PAGE_SECONDS))),
        cluster=ctx.page_cluster,
        now=lambda: ctx.now().timestamp(),
    )
    # Until the run's record says otherwise, its fault may be in place (a run killed mid-way leaves it).
    write_fault_state(env_dir, problem, "unknown")
    current_wait = exec_out / CURRENT_WAIT_FILE
    current_wait.unlink(missing_ok=True)
    limits: dict[str, Any] = {}
    row_deadline = ctx.row_deadline_seconds()
    if row_deadline is not None:
        limits = {"deadline_seconds": row_deadline, "current_wait": current_wait}
    deadline_failure = ""
    try:
        ctx.long_runner(
            command, env={**run_env, CURRENT_WAIT_ENV: str(current_wait)}, cwd=ctx.root, tick=pager.tick, **limits
        )
    except RowDeadlineError as error:
        failure = deadline_failure = str(error)
    except AdapterError as error:
        failure = str(error)
    if results.is_dir():
        # The run's evidence travels with the run (the lane's environment is rebuilt with the lane).
        shutil.copytree(results, exec_out / "fastloop-results", dirs_exist_ok=True)
    incident = None if deadline_failure else _last_record(results)
    keep_install_identity(env_dir, identity, installed=incident is not None)
    record["reseed"] = reseed_record(results, attempted=not not_reseeded, reason=not_reseeded)
    if deadline_failure:
        # The fault may still be in place: its verdict stays unknown (active), so the platform recovers the lane.
        return 1, {**record, "valid": False, "invalid_reason": "row deadline exceeded", "error": deadline_failure}
    if incident is None:
        return 1, {
            **record,
            "valid": False,
            "invalid_reason": "no incident record",
            "error": failure or "fastloop run wrote no record",
        }
    summary = summarize(incident)
    summary["page_source"] = pager.source
    summary["paged_at"] = (
        datetime.fromtimestamp(pager.paged_at, UTC).isoformat().replace("+00:00", "Z")
        if pager.paged_at is not None
        else None
    )
    record["sregym"] = summary
    record["memory"] = export_memory(ctx, workspace, exec_out / MEMORY_DIR, planned)
    record["memory"]["seeded"] = seed_dir is not None
    if summary["injected_at"]:
        record["lifecycle"] = {"inject_at": summary["injected_at"]}
    # A row fastloop's evidence gate refused to grade (`NOT GRADED`: its transcripts were not copied out) is no
    # valid row: the injection happened, but nothing can grade or learn from it.
    why = (
        summary["infra_error"]
        or ("not injected" if not summary["injected_at"] else "")
        or (f"evidence incomplete: {summary['evidence_incomplete']}" if summary["evidence_incomplete"] else "")
    )
    record["valid"] = not why
    record["invalid_reason"] = why or ""
    record["error"] = summary["error"] or failure or ""
    # Clear: never injected, or no recovery error and either fastloop verified the full cleanup or the problem's oracle
    # saw the fault mitigated (fastloop then leaves the full cleanup to the next row's start: row_start_tier2, or on a
    # warm lane the next start's restore for a fault it reverts, fault_recovery_deferred; the attempt marker owes it).
    cleared = not summary["injected_at"] or (
        not summary["recovery_error"] and (bool(summary["full_cleanup_verified"]) or summary["success"])
    )
    write_fault_state(env_dir, problem, "clear" if cleared else "active")
    return (0 if record["valid"] else 1), record


def _seconds_between(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()


def page_to_healthy_seconds(summary: Mapping[str, Any]) -> float | None:
    """From the page to recovery, with the eval runner's clock: SDO's own detection (``own``), or the instant the
    harness paged it (``fallback``); ``None`` when SDO was never paged or never recovered."""
    source = summary.get("page_source")
    start = summary.get("detected_at") if source == "own" else summary.get("paged_at") if source == "fallback" else None
    return _seconds_between(start, summary.get("resolved_at"))


def agent_dispatched(summary: Mapping[str, Any]) -> bool:
    """Whether the agent ran. ``dispatched_at`` and ``responder_started_at`` are the incident's dispatch, a learned
    playbook's automatic stage (``resolution_path`` deterministic) included: the agent ran when it made model requests,
    else when the incident went to a responder."""
    if summary.get("responder_requests"):
        return True
    return summary.get("resolution_path") != "deterministic" and bool(summary.get("responder_started_at"))


def grade_line(record: Mapping[str, Any]) -> dict[str, Any]:
    """One run's grade: the platform reads ``operator`` and the boolean ``valid``; the rest are columns.

    The columns carry what a report per problem needs: whether a learned playbook was applied
    (``resolution_path``, ``warm_path``, ``match_reasons``), whether the agent was dispatched, the page-to-healthy time
    with the page's source, the responder's cost, and whether the closing turn finished (and learned)."""
    summary = record.get("sregym") or {}
    memory = record.get("memory") or {}
    reseed = record.get("reseed") or {}
    learned = len(memory.get("playbook_files_changed") or [])
    return {
        "operator": record.get("operator", ""),
        "valid": bool(record.get("valid")),
        "problem": record.get("fault"),
        "success": bool(summary.get("success")),
        "ttm_seconds": summary.get("time_to_fix_seconds"),
        "resolution_path": summary.get("resolution_path"),
        "page_source": summary.get("page_source"),
        "warm_path": summary.get("warm_path"),
        "match_reasons": summary.get("match_reasons") or [],
        "agent_dispatched": agent_dispatched(summary),
        "dispatched_at": summary.get("dispatched_at"),
        "injected_at": summary.get("injected_at"),
        "detected_at": summary.get("detected_at"),
        "paged_at": summary.get("paged_at"),
        "resolved_at": summary.get("resolved_at"),
        "page_to_healthy_seconds": page_to_healthy_seconds(summary),
        "responder_usd": summary.get("responder_usd"),
        "responder_requests": summary.get("responder_requests"),
        "reflection_usd": summary.get("reflection_usd"),
        "closing_finished": bool(summary.get("reflection_finished_at") or summary.get("reflection_skipped_reason")),
        "seeded": bool(memory.get("seeded")),
        "memory_before": summary.get("memory_before_commit"),
        "memory_after": memory.get("commit"),
        "playbooks_learned": learned,
        "learned": learned > 0,
        "invalid_reason": record.get("invalid_reason", ""),
        # How the row started: `reseeded` (installed lane, warm transaction), `fallback` (tried, fell back to a fresh
        # install) or `fresh` (did not try); the reason is the boundary that fell back or why it did not try.
        "reseed": "fresh" if not reseed.get("attempted") else "reseeded" if reseed.get("reused") else "fallback",
        "reseed_reason": reseed.get("reason", ""),
    }


def grade(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action != "report":
        return UNSUPPORTED, {"unsupported": True}
    results = Path(ctx.env.get("WORKLOAD_RESULTS") or ctx.args.get("_") or ctx.need("WORKLOAD_RESULTS"))
    path = results / "runs.jsonl"
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    grades = [grade_line(line) for line in lines]
    path.write_text("".join(json.dumps(g) + "\n" for g in grades), encoding="utf-8")
    (results / "grades.json").write_text(json.dumps(grades, indent=1) + "\n", encoding="utf-8")
    return 0, {"graded": len(grades)}


VERBS: dict[str, Callable[[Context], tuple[int, dict[str, Any]]]] = {
    "identity": identity,
    "app-up": app_up,
    "app-snapshot": app_snapshot,
    "app-reset": app_reset,
    "gate": gate,
    "run": run,
    "grade": grade,
}


def dispatch(verb: str, env: Mapping[str, str], **seams: Any) -> tuple[int, dict[str, Any]]:
    """Run one verb; an adapter error is exit 1 with its reason on stderr and in the result."""
    handler = VERBS.get(verb)
    if handler is None:
        return UNSUPPORTED, {"unsupported": True}
    try:
        ctx = Context(
            verb=verb,
            action=env.get("WORKLOAD_ACTION", ""),
            args=parse_args(env.get("WORKLOAD_ARGS", "[]")),
            env=env,
            **seams,
        )
        return handler(ctx)
    except (AdapterError, OSError, KeyError, json.JSONDecodeError) as error:
        print(f"sregym workload {verb}: {error}", file=sys.stderr)
        return 1, {"error": str(error)}


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    verb = args[0] if args else os.environ.get("WORKLOAD_VERB", "")
    code, result = dispatch(verb, os.environ)
    print(json.dumps(result), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
