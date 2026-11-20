"""The SREGym workload adapter of the lane platform: one verb per call, environment in, last stdout line JSON out.

``python -m benchmarks.sregym.lanes.workload <verb>`` with ``WORKLOAD_ACTION`` and ``WORKLOAD_ARGS`` (a JSON array of
``--flag value`` options) and the lane variables in the environment (``benchmarks/lanes/docs/contract.md``).

The app pack is a directory with ``app.json`` (``AppPack``). The lane's fastloop environment lives at
``$SREGYM_LANES_ENV/<lane>`` (a host variable of the lanes inventory); ``SREGYM_LANES_UP_ARGS`` adds host-specific
``fastloop up`` flags (images, CPU and memory caps), ``SREGYM_LANES_RUN_ARGS`` ``fastloop run`` flags.

Verbs:

``identity``      the checkout's revision and whether tracked files are modified.
``app-up``        ``check-pack``: the pack loads; ``up``: ``fastloop up`` unless the lane's environment and cluster
                  are already there, then the lane kubeconfig; ``down``, ``images``: nothing (fastloop's row-start
                  reset restores the app, ``up`` delivers images).
``app-snapshot``  ``write``/``verify``: the namespace's workloads and their images (a fingerprint of the deploy).
``app-reset``     ``recover``: nothing to undo (``fastloop run`` recovers and cleans up its own fault).
``gate``          ``check``: every pod of the app namespace is Ready.
``run``           ``run``: one ``fastloop run`` of the problem (``--instance``) with the subject's agent registrations,
                  from the planned memory commit: the operator file's seed (a memory directory a plan edge exported,
                  which must descend from its baseline), else its baseline, else the lane's onboarding commit.
                  The record (last line) has
                  ``operator``, ``valid``, ``lifecycle.inject_at`` and the SREGym summary; the accepted memory after the
                  run is exported to ``$FUZZOPS_EXEC_OUT/memory`` (``memory.bundle`` and ``memory.json``).
                  ``active``/``undo``: the last run's verdict: clear only when it never injected or fastloop
                  verified the full cleanup with no recovery error; in flight, killed or unknown is active.
``grade``         ``report``: ``$WORKLOAD_RESULTS/runs.jsonl`` (the run records) rewritten as one grade per run.
``footprint``     unsupported.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol

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


class AdapterError(RuntimeError):
    """A verb that cannot do its work; the message is the reason the platform records."""


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
    ) -> str:
        """Run a long command to completion, calling ``tick`` every few seconds; raise ``AdapterError`` on failure."""
        ...


def run_process(argv: Sequence[str], *, env: Mapping[str, str] | None = None, cwd: Path | None = None) -> str:
    """The real runner: stderr passes through to the platform's log."""
    result = subprocess.run(
        list(argv), env=dict(env) if env is not None else None, cwd=cwd, stdout=subprocess.PIPE, text=True, check=False
    )
    if result.returncode:
        raise AdapterError(f"{shlex.join(argv)[:300]}: exit {result.returncode}")
    return result.stdout


TICK_SECONDS = 5.0


def run_logged(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    tick: Callable[[], None] | None = None,
) -> str:
    """A long command whose stdout is progress: both streams go to the platform's log (stderr). ``tick`` is called
    every few seconds while it runs (the fallback page watches the run from there)."""
    process = subprocess.Popen(list(argv), env=dict(env) if env is not None else None, cwd=cwd, stdout=sys.stderr)
    while True:
        try:
            code = process.wait(timeout=TICK_SECONDS)
            break
        except subprocess.TimeoutExpired:
            if tick is not None:
                tick()
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
    #: The fallback page's view of the lane (``eval.page.PageCluster``); None: the lane's controller over kubectl.
    page_cluster: Any = None

    def need(self, name: str) -> str:
        value = self.env.get(name, "")
        if not value:
            raise AdapterError(f"{name} is not set")
        return value

    def lane_env_dir(self) -> Path:
        return Path(self.need("SREGYM_LANES_ENV")) / self.need("BENCH_LANE")

    def pack(self) -> AppPack:
        return AppPack.load(Path(self.args.get("pack") or self.need("BENCH_PACK")))

    def kubectl(self, *args: str) -> str:
        return self.runner(["kubectl", "--kubeconfig", self.need("KUBECONFIG"), *args], env=self.env)


# ----- verbs -----


def identity(ctx: Context) -> tuple[int, dict[str, Any]]:
    revision = ctx.runner(["git", "-C", str(ctx.root), "rev-parse", "HEAD"]).strip()
    dirty = ctx.runner(["git", "-C", str(ctx.root), "status", "--porcelain", "--untracked-files=no"]).strip()
    return 0, {"revision": revision, "modified": bool(dirty)}


def app_up(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action == "check-pack":
        try:
            pack = ctx.pack()
        except AdapterError as error:
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
    clusters = ctx.runner(["kind", "get", "clusters"], env=ctx.env).split()
    reused = (env_dir / "fastloop.json").is_file() and lane in clusters
    if not reused:
        prefix, worker = cluster_parts(lane)
        if lane in clusters or env_dir.exists():
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
            *pack.up_args,
            *shlex.split(ctx.env.get("SREGYM_LANES_UP_ARGS", "")),
        ]
        if evidence := ctx.env.get("BENCH_EVIDENCE_HOST"):
            command += ["--evidence-root", evidence]
        ctx.long_runner(command, env=ctx.env, cwd=ctx.root)
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


def app_reset(ctx: Context) -> tuple[int, dict[str, Any]]:
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


def summarize(record: Mapping[str, Any]) -> dict[str, Any]:
    """The SREGym part of a run record, from a fastloop incident record."""
    injection = record.get("injection") or {}
    oracle = record.get("oracle") or {}
    keys = (
        "run_id",
        "problem_id",
        "detected_at",
        "dispatched_at",
        "mitigation_applied_at",
        "resolved_at",
        "time_to_fix_seconds",
        "resolution_path",
        "sdo_resolution",
        "sdo_outcome_classification",
        "warm_path",
        "match_reasons",
        "memory_before_commit",
        "memory_after_commit",
        "full_cleanup_verified",
        "infra_error",
        "recovery_error",
        "error",
        "incident_id",
    )
    out = {key: record.get(key) for key in keys}
    out["injected_at"] = injection.get("started_at")
    out["oracle_kind"] = oracle.get("kind")
    out["success"] = bool(oracle.get("success"))
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
            self._write({"injected_at": marker.started_at, "paged_at": paged_at, "source": "fallback"})
        elif own_at is not None:
            self.source = "own"
            self._write({"injected_at": marker.started_at, "decided_at": own_at, "source": "own"})

    def _write(self, document: dict[str, Any]) -> None:
        self.out.parent.mkdir(parents=True, exist_ok=True)
        self.out.write_text(json.dumps(document) + "\n", encoding="utf-8")


FAULT_STATE = "lanes-fault.json"


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
        "--agent-registrations",
        str(ctx.root / SCRIPTED_REGISTRATIONS),
        "--stream",
        str(stream),
        # The eval runner's SDO row flags (benchmarks/sregym/eval/fastloop_row.py row_command).
        "--detection-timeout",
        str(DETECTION_TIMEOUT),
        "--healthy-baseline",
        "--inject-before-resume",
        "--require-fault-tracking",
        "--require-full-cleanup",
        "--continue-on-error",
        "--timeout",
        ctx.env.get("SREGYM_LANES_RUN_TIMEOUT", str(DEFAULT_RUN_TIMEOUT)),
        *shlex.split(ctx.env.get("SREGYM_LANES_RUN_ARGS", "")),
    ]
    if planned:
        command += ["--planned-commit", planned]
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
    try:
        ctx.long_runner(command, env=ctx.env, cwd=ctx.root, tick=pager.tick)
    except AdapterError as error:
        failure = str(error)
    if results.is_dir():
        # The run's evidence travels with the run (the lane's environment is rebuilt with the lane).
        shutil.copytree(results, exec_out / "fastloop-results", dirs_exist_ok=True)
    incident = _last_record(results)
    if incident is None:
        return 1, {
            **record,
            "valid": False,
            "invalid_reason": "no incident record",
            "error": failure or "fastloop run wrote no record",
        }
    summary = summarize(incident)
    summary["page_source"] = pager.source
    record["sregym"] = summary
    record["memory"] = export_memory(ctx, workspace, exec_out / MEMORY_DIR, planned)
    record["memory"]["seeded"] = seed_dir is not None
    if summary["injected_at"]:
        record["lifecycle"] = {"inject_at": summary["injected_at"]}
    why = summary["infra_error"] or ("not injected" if not summary["injected_at"] else "")
    record["valid"] = not why
    record["invalid_reason"] = why or ""
    record["error"] = summary["error"] or failure or ""
    cleared = not summary["injected_at"] or (summary["full_cleanup_verified"] and not summary["recovery_error"])
    write_fault_state(env_dir, problem, "clear" if cleared else "active")
    return (0 if record["valid"] else 1), record


def grade_line(record: Mapping[str, Any]) -> dict[str, Any]:
    """One run's grade: the platform reads ``operator`` and the boolean ``valid``; the rest are columns."""
    summary = record.get("sregym") or {}
    memory = record.get("memory") or {}
    return {
        "operator": record.get("operator", ""),
        "valid": bool(record.get("valid")),
        "problem": record.get("fault"),
        "success": bool(summary.get("success")),
        "ttm_seconds": summary.get("time_to_fix_seconds"),
        "resolution_path": summary.get("resolution_path"),
        "page_source": summary.get("page_source"),
        "warm_path": summary.get("warm_path"),
        "seeded": bool(memory.get("seeded")),
        "memory_before": summary.get("memory_before_commit"),
        "memory_after": memory.get("commit"),
        "playbooks_learned": len(memory.get("playbook_files_changed") or []),
        "invalid_reason": record.get("invalid_reason", ""),
    }


def grade(ctx: Context) -> tuple[int, dict[str, Any]]:
    if ctx.action != "report":
        return UNSUPPORTED, {"unsupported": True}
    results = Path(ctx.env.get("WORKLOAD_RESULTS") or ctx.args.get("_", ""))
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
