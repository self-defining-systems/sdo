"""Lanes subject for a plain Claude Code agent responding to failing pack journeys.

The agent is paged at the first harm onset, and paged again (a new session: same scoped identity, same prompt, nothing
carried over) when users are harmed anew after a quiet period (:mod:`benchmarks.fuzzops.repage`). Every page is a line
of ``pages.jsonl``; ``done.json``, ``usage.json``, the transcript and the session directory cover all of them.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from benchmarks.fuzzops import eks, repage
from benchmarks.fuzzops.cellevidence import atomic_write, atomic_write_json, cell_problems, read_json_object
from benchmarks.fuzzops.hooks import HookError, fuzzops_app, prove_golden
from benchmarks.fuzzops.view import redact_json_line

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

MODEL = "claude-opus-5-5"
EFFORT = "medium"
AGENT_SECONDS = 25 * 60
IDENTITY = "vanilla-agent"
MODULE = "benchmarks.fuzzops.vanilla"
VERTEX_ENV = ("CLAUDE_CODE_USE_VERTEX", "CLOUD_ML_REGION", "ANTHROPIC_VERTEX_PROJECT_ID")
ONSET_EVIDENCE = "onset-probes.jsonl"
PAGES = "pages.jsonl"
#: In the lane state while the supervisor only watches for a new onset; in the run directory to end that watch.
STANDBY = "standby"
STOP = "stop"
#: How long collect waits for a watching supervisor to leave.
RELEASE_SECONDS = 30.0
#: What a full EKS lane carries in kube-system (benchmarks/lanes/deploy/eks-lane.sh): the lane admin identity, the node
#: agent DaemonSet with its pods, and the node agent's pre-pull list. They belong to the lane, not to SDO.
LANE_PLATFORM = re.compile(
    r"(serviceaccount|clusterrolebinding\.rbac\.authorization\.k8s\.io)/sdo-lane-admin"
    r"|daemonset\.apps/sdo-lane-node-agent|pod/sdo-lane-node-agent-[a-z0-9]{5}|configmap/sdo-lane-prepull"
)
LANE_VERBS = ("prepare", "idle", "snapshot", "reset", "swap", "teardown")
EVIDENCE_FILES = (
    "start.json",
    "onset.json",
    ONSET_EVIDENCE,
    "done.json",
    PAGES,
    "usage.json",
    "transcript.jsonl",
    "stderr.log",
)


def page_text(namespace: str) -> str:
    """What every paged baseline is told."""
    return f"Users are reporting problems with the application in namespace {namespace}; investigate and fix it."


def timestamp(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).isoformat()


def binary(env: Mapping[str, str], name: str) -> str:
    path = shutil.which(name, path=env.get("PATH"))
    if not path:
        raise HookError(f"{name} is missing from PATH")
    return str(Path(path).resolve())


def state(env: Mapping[str, str]) -> Path:
    path = Path(env["BENCH_LANE_DIR"]) / "vanilla"
    path.mkdir(mode=0o700, exist_ok=True)
    return path.resolve()


def run_dir(env: Mapping[str, str]) -> Path:
    key = env["BENCH_RUN"]
    if not key or key in (".", "..") or Path(key).name != key:
        raise HookError("invalid BENCH_RUN")
    return state(env) / key


@contextmanager
def lane_lock(env: Mapping[str, str]) -> Iterator[BinaryIO]:
    # The detached supervisor inherits this open file description. Its exit
    # releases the lock even if fuzzops kills the watch process or it crashes.
    with (state(env) / "lock").open("ab") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HookError("busy: vanilla response in progress") from exc
        yield lock


def kubectl(env: Mapping[str, str], config: Path | None, *args: str, stdin: str | None = None) -> str:
    argv = [binary(env, "kubectl"), "--kubeconfig", str(config or Path(env["KUBECONFIG"]))]
    if config is None and env.get("FUZZOPS_KUBE_CONTEXT"):
        argv += ["--context", env["FUZZOPS_KUBE_CONTEXT"]]
    argv += ["-n", env["FUZZOPS_NAMESPACE"], *args]
    proc = subprocess.run(argv, env=env, input=stdin, capture_output=True, text=True, timeout=35, check=False)
    if proc.returncode and not (args[:2] == ("auth", "can-i") and proc.returncode == 1 and proc.stdout.strip() == "no"):
        raise HookError(f"kubectl {' '.join(args[:2])} failed (exit {proc.returncode})")
    return proc.stdout.strip()


def prove_no_sdo(env: Mapping[str, str]) -> dict[str, str]:
    cluster = kubectl(env, None, "get", "ns,clusterroles,clusterrolebindings,crd", "-o", "name")
    workloads = kubectl(env, None, "get", "deploy,sts,ds,pod,job,cronjob,cm,sa,role,rolebinding", "-A", "-o", "name")
    names = (cluster + "\n" + workloads).splitlines()
    found = [name for name in names if "sdo" in name.lower() and not LANE_PLATFORM.fullmatch(name)]
    if found:
        raise HookError(f"SDO objects found on the vanilla lane: {', '.join(found[:5])}")
    return {"checked_at": timestamp(time.time()), "result": "SDO absent"}


def role_objects(namespace: str) -> dict[str, Any]:
    meta = {"name": IDENTITY, "namespace": namespace}
    actions = ["create", "update", "patch", "delete"]
    reads = ["get", "list", "watch"]
    rules = [
        {
            "apiGroups": [""],
            "resources": [
                "pods",
                "pods/log",
                "pods/exec",
                "pods/portforward",
                "services",
                "endpoints",
                "configmaps",
                "persistentvolumeclaims",
                "events",
                "serviceaccounts",
                "replicationcontrollers",
                "resourcequotas",
                "limitranges",
            ],
            "verbs": reads + actions,
        },
        {"apiGroups": [""], "resources": ["secrets"], "verbs": actions},
        {
            "apiGroups": ["apps", "batch", "networking.k8s.io", "policy", "autoscaling"],
            "resources": ["*"],
            "verbs": reads + actions,
        },
    ]
    group = "rbac.authorization.k8s.io"
    return {
        "apiVersion": "v1",
        "kind": "List",
        "items": [
            {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": meta},
            {"apiVersion": f"{group}/v1", "kind": "Role", "metadata": meta, "rules": rules},
            {
                "apiVersion": f"{group}/v1",
                "kind": "RoleBinding",
                "metadata": meta,
                "subjects": [{"kind": "ServiceAccount", "name": IDENTITY, "namespace": namespace}],
                "roleRef": {"apiGroup": group, "kind": "Role", "name": IDENTITY},
            },
        ],
    }


def prepare(env: Mapping[str, str], version: str = f"{MODEL}/{EFFORT}") -> None:
    ns = env["FUZZOPS_NAMESPACE"]
    proof = prove_no_sdo(env)
    cluster = json.loads(kubectl(env, None, "config", "view", "--minify", "--flatten", "--raw", "-o", "json"))
    server = cluster["clusters"][0]["cluster"]
    # Copy only the endpoint and trust roots from the admin kubeconfig.
    trust = {k: server[k] for k in ("server", "certificate-authority-data", "tls-server-name") if k in server}
    if not trust.get("certificate-authority-data") or not trust["server"].startswith("https://"):
        raise HookError("vanilla requires a TLS-verified lane API endpoint")
    kubectl(env, None, "apply", "-f", "-", stdin=json.dumps(role_objects(ns)))
    token = kubectl(env, None, "create", "token", IDENTITY, "--duration=2h")
    if not token:
        raise HookError("no scoped service-account token returned")
    config = state(env) / "kubeconfig"
    atomic_write_json(
        config,
        {
            "apiVersion": "v1",
            "kind": "Config",
            "current-context": IDENTITY,
            "clusters": [{"name": IDENTITY, "cluster": trust}],
            "users": [{"name": IDENTITY, "user": {"token": token}}],
            "contexts": [{"name": IDENTITY, "context": {"cluster": IDENTITY, "user": IDENTITY, "namespace": ns}}],
        },
    )
    checks = [
        ("yes", "get", "pods", "-n", ns),
        *(("no", verb, "secrets", "-n", ns) for verb in ("get", "list", "watch")),
        ("no", "get", "pods", "-n", "kube-system"),
        ("no", "list", "pods", "--all-namespaces"),
        ("no", "list", "nodes"),
        ("no", "create", "clusterrolebindings"),
    ]
    try:
        for expected, *args in checks:
            if kubectl(env, config, "auth", "can-i", *args) != expected:
                raise HookError(f"scoped authorization check failed: {' '.join(args)} expected {expected}")
    except BaseException:
        config.unlink(missing_ok=True)
        raise
    atomic_write_json(state(env) / "isolation.json", proof)
    print(json.dumps({"pack": env["BENCH_PACK"], "operator_version": version, "memory": "none"}))


def refresh_token(env: Mapping[str, str]) -> None:
    """Mint a fresh scoped token into the state kubeconfig and prove it works (``prepare``'s token lives 2 h, a long
    stream outlives it: an applier with an expired token runs every plan command to "must be logged in")."""
    config = state(env) / "kubeconfig"
    data = json.loads(config.read_text())
    token = kubectl(env, None, "create", "token", IDENTITY, "--duration=2h")
    if not token:
        raise HookError("no scoped service-account token returned")
    data["users"][0]["user"]["token"] = token
    atomic_write_json(config, data)
    if kubectl(env, config, "auth", "can-i", "get", "pods", "-n", env["FUZZOPS_NAMESPACE"]) != "yes":
        raise HookError("the refreshed scoped token is not authorized")


def credentials_work(env: Mapping[str, str]) -> bool:
    """A no-op ``auth can-i`` with the scoped kubeconfig: false when the token expired or was revoked."""
    try:
        return (
            kubectl(env, state(env) / "kubeconfig", "auth", "can-i", "get", "pods", "-n", env["FUZZOPS_NAMESPACE"])
            == "yes"
        )
    except HookError:
        return False


def operator_settings(env: Mapping[str, str]) -> dict[str, Any]:
    settings = json.loads(Path(env["BENCH_OPERATOR"]).read_text())["settings"]
    window = settings.get("window_seconds")
    if not isinstance(window, int) or isinstance(window, bool) or window <= 0:
        raise HookError("vanilla requires a positive settings.window_seconds matching run.window")
    return settings


def vertex(env: Mapping[str, str]) -> tuple[Path, dict[str, str]]:
    settings = operator_settings(env)
    auth = Path(settings["auth_file"]).expanduser().resolve()
    extra = dict(item.split("=", 1) for item in settings["config"]["agent"]["extra_env"])
    if extra.get("CLAUDE_CODE_USE_VERTEX") != "1" or any(not extra.get(key) for key in VERTEX_ENV):
        raise HookError("operator settings require Claude Code Vertex environment")
    if not auth.is_file() or not os.access(auth, os.R_OK):
        raise HookError("Vertex auth_file is unreadable")
    return auth, {key: extra[key] for key in VERTEX_ENV}


SECRET_FIELDS = ("private_key", "refresh_token", "client_secret")


def auth_secrets(auth: Path) -> list[str]:
    """Every secret value of a Google credential file, at any depth (an impersonated account nests its source)."""

    def collect(value: Any) -> Iterator[str]:
        if isinstance(value, dict):
            for key, item in value.items():
                if key in SECRET_FIELDS and isinstance(item, str):
                    # A key may be logged whole, with escaped newlines, or line by line.
                    yield from (line.strip() for line in item.splitlines())
                else:
                    yield from collect(item)
        elif isinstance(value, list):
            for item in value:
                yield from collect(item)

    return [secret for secret in dict.fromkeys(collect(json.loads(auth.read_text()))) if secret]


def isolated(
    env: Mapping[str, str], box: Path, mounts: Sequence[tuple[str, str]], settings: Mapping[str, str]
) -> list[str]:
    """The bwrap argv, up to the command: the host's tools read-only, no host environment, writable state under box."""
    args = [
        binary(env, "bwrap"),
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--die-with-parent",
        "--new-session",
        "--clearenv",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
    ]
    for path in ("/usr", "/bin", "/lib", "/lib64", "/etc/ssl", "/etc/hosts", "/etc/resolv.conf", "/etc/nsswitch.conf"):
        if Path(path).exists():
            args += ["--ro-bind", path, path]
    for directory in ("home", "config", "work"):
        (box / directory).mkdir(exist_ok=True)
        args += ["--bind", str(box / directory), f"/{directory}"]
    for source, target in mounts:
        args += ["--ro-bind", source, target]
    for key, value in settings.items():
        args += ["--setenv", key, value]
    return [*args, "--chdir", "/work", "--"]


def sandbox(env: Mapping[str, str], box: Path, auth: Path, config: Path, extra: Mapping[str, str]) -> list[str]:
    mounts = (
        (binary(env, "claude"), "/tools/claude"),
        (binary(env, "kubectl"), "/tools/kubectl"),
        (str(auth), "/vertex.json"),
        (str(config), "/kubeconfig"),
    )
    settings = {
        **extra,
        "HOME": "/home",
        "CLAUDE_CONFIG_DIR": "/config",
        "KUBECONFIG": "/kubeconfig",
        "GOOGLE_APPLICATION_CREDENTIALS": "/vertex.json",
        "PATH": "/tools:/usr/local/bin:/usr/bin:/bin",
        "DISABLE_AUTOUPDATER": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    }
    return [*isolated(env, box, mounts, settings), "/tools/claude"]


def copy_log(source: Path, dest: Path, identities: Sequence[str], mode: str = "w") -> dict[str, Any]:
    # Keep the CLI JSONL shape (including IDs) for the evidence validator.
    result: dict[str, Any] = {}
    with source.open(errors="replace") as raw, dest.open(mode) as out:
        for line in raw:
            safe = redact_json_line(line.rstrip("\n"), identities)
            out.write(safe + "\n")
            try:
                event = json.loads(safe)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict) and event.get("type") == "result":
                result = event
    return result


def session_id(stream: Path) -> str | None:
    """A capped session has no result event; its first records name it too."""
    with stream.open(errors="replace") as lines:
        for line in lines:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict) and isinstance(event.get("session_id"), str):
                return event["session_id"]
    return None


def capture_agent(env: Mapping[str, str], target: Path, cutoff: float) -> dict[str, Any]:
    """One page: a new Claude Code session, stopped at ``cutoff``. Its logs add to the run's; returns its record."""
    auth, extra = vertex(env)
    ns = env["FUZZOPS_NAMESPACE"]
    with tempfile.TemporaryDirectory(prefix="vanilla-claude-") as temp:
        box = Path(temp)
        config = state(env) / "kubeconfig"
        argv = sandbox(env, box, auth, config, extra)
        argv += [
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            "--model",
            MODEL,
            "--effort",
            EFFORT,
        ]
        token = json.loads(config.read_text())["users"][0]["user"]["token"]
        secrets = [token, *auth_secrets(auth)]
        prompt = page_text(ns)
        started = time.time()
        capped = False
        with (box / "stream").open("w") as stream, (box / "stderr").open("w") as stderr:
            try:
                exit_code = subprocess.run(
                    argv,
                    input=prompt,
                    text=True,
                    stdout=stream,
                    stderr=stderr,
                    check=False,
                    timeout=max(0.01, cutoff - started),
                ).returncode
            except subprocess.TimeoutExpired:
                # Killing bwrap's PID namespace supervisor also kills Claude's descendants.
                capped, exit_code = True, None
        ended = time.time()
        result = copy_log(box / "stream", target / "transcript.jsonl", secrets, "a")
        copy_log(box / "stderr", target / "stderr.log", secrets, "a")
        projects = box / "config" / "projects"
        if projects.is_dir() and not projects.is_symlink():
            for path in projects.rglob("*.jsonl"):
                if path.is_symlink() or not path.is_file():
                    continue
                dest = target / "session-dir" / path.relative_to(projects)
                dest.parent.mkdir(parents=True, exist_ok=True)
                copy_log(path, dest, secrets)
        summary = result.get("result")
        return {
            "started_at": timestamp(started),
            "ended_at": timestamp(ended),
            "seconds": round(ended - started, 3),
            "session_id": session_id(box / "stream"),
            "capped": capped,
            "exit": exit_code,
            "result_event": bool(result),
            "is_error": result.get("is_error"),
            "usage": result.get("usage") or {},
            "total_cost_usd": result.get("total_cost_usd"),
            "summary": summary[:600] if isinstance(summary, str) else None,
        }


def add_usage(total: Any, more: Any) -> Any:
    """Sum two ``result.usage`` values field by field."""
    if isinstance(total, dict) and isinstance(more, dict):
        return {key: add_usage(total.get(key), more.get(key)) for key in {**total, **more}}
    if isinstance(total, list) and isinstance(more, list):
        return total + more
    if type(total) in (int, float) and type(more) in (int, float):
        return total + more
    return more if total is None else total


def write_pages(env: Mapping[str, str], target: Path, pages: Sequence[Mapping[str, Any]], stopped: str | None) -> None:
    """The run's evidence over every page so far (``stopped``: why no further page will come, once known)."""
    atomic_write(target / PAGES, "".join(json.dumps(page) + "\n" for page in pages))
    # A session without a result event has unknown tokens and cost: the run's totals are then unknown, never partial.
    usage: dict[str, Any] = {}
    cost: float | None = 0.0
    if all(page["result_event"] for page in pages):
        for page in pages:
            usage = add_usage(usage, page["usage"])
    for page in pages:
        known = page["result_event"] and page["total_cost_usd"] is not None
        cost = cost + page["total_cost_usd"] if known and cost is not None else None
    atomic_write_json(target / "usage.json", {**usage, "total_cost_usd": cost})
    summaries = [page["summary"] for page in pages if page["summary"] is not None]
    if summaries and env.get("FUZZOPS_REPORT"):
        atomic_write_json(Path(env["FUZZOPS_REPORT"]), {"summary": " | ".join(summaries)})
    exits = [page["exit"] for page in pages]
    errors = [page["is_error"] for page in pages]
    atomic_write_json(
        target / "done.json",
        {
            "started_at": pages[0]["started_at"],
            "ended_at": pages[-1]["ended_at"],
            "agent_seconds": round(sum(page["seconds"] for page in pages), 3),
            "pages": len(pages),
            "capped": any(page["capped"] for page in pages),
            "exit": next((code for code in exits if code not in (None, 0)), None if None in exits else 0),
            "model": MODEL,
            "effort": EFFORT,
            "result_event": all(page["result_event"] for page in pages),
            "is_error": True if True in errors else errors[-1],
            "repage": {
                "page_limit": repage.page_limit(env.get("BENCH_FAULT", "")),
                "quiet_seconds": repage.QUIET_SECONDS,
                "stopped": stopped,
            },
        },
    )


@contextmanager
def standing_by(env: Mapping[str, str]) -> Iterator[None]:
    """Say that nothing is working: to lanes' ``idle`` (the marker) and to FuzzOps' early stop (its standby file)."""
    markers = [state(env) / STANDBY]
    if env.get("FUZZOPS_STANDBY"):
        markers.append(Path(env["FUZZOPS_STANDBY"]))
    try:
        for marker in markers:
            marker.touch()
        yield
    finally:
        for marker in markers:
            marker.unlink(missing_ok=True)


def respond(
    env: Mapping[str, str],
    target: Path,
    injected: float,
    session: Callable[[Mapping[str, str], Path, float], dict[str, Any]] = capture_agent,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Page the agent at the first onset, then again at each later one while the window lasts."""
    limit = repage.page_limit(env.get("BENCH_FAULT", ""))
    deadline = datetime.fromisoformat(env["FUZZOPS_DEADLINE"]).timestamp()
    traffic = repage.Traffic(Path(env["FUZZOPS_TRAFFIC"]))
    harm = repage.Harm()

    def observe() -> None:
        for sample in traffic.drain():
            harm.observe(sample)

    observe()
    harm.page()
    pages: list[dict[str, Any]] = []
    paged_at, cutoff = clock(), injected + AGENT_SECONDS
    while True:
        onset = timestamp(harm.paged.timestamp()) if harm.paged else None
        record = session(env, target, cutoff)
        pages.append({"page": len(pages) + 1, "paged_at": timestamp(paged_at), "onset": onset, **record})
        write_pages(env, target, pages, None)
        if len(pages) >= limit:
            stopped = repage.PAGE_LIMIT
            observe()
        else:
            with standing_by(env):
                stopped = repage.await_onset_again(
                    traffic, harm, deadline, (target / STOP).exists, clock=clock, sleep=sleep
                )
        # Whether users had a quiet period after this page: if not, it is never followed by another.
        pages[-1]["users_recovered"] = None if harm.unsupported else harm.recovered
        if stopped != repage.ONSET:
            break
        harm.page()
        paged_at = clock()
        cutoff = paged_at + AGENT_SECONDS
        print(f"vanilla: users harmed again, page {len(pages) + 1}", file=sys.stderr, flush=True)
    write_pages(env, target, pages, stopped)


def await_onset(env: Mapping[str, str], target: Path, deadline: float) -> dict[str, Any]:
    if not env.get("FUZZOPS_TRAFFIC"):
        raise HookError("watch requires a FuzzOps pin that publishes FUZZOPS_TRAFFIC")
    with Path(env["FUZZOPS_TRAFFIC"]).open("rb") as source, (target / ONSET_EVIDENCE).open("wb") as evidence:
        sample = None
        while True:
            offset = source.tell()
            line = source.readline()
            if line.endswith(b"\n"):
                evidence.write(line)
                evidence.flush()
                sample = json.loads(line)
                if sample["first_bad"] is not None:
                    return sample
                continue
            remaining = deadline - time.time()
            if remaining <= 0:
                if sample is None or not sample["requests"]:
                    raise HookError("FuzzOps onset stream did not observe any requests")
                return sample
            source.seek(offset)
            time.sleep(min(0.25, remaining))


def supervise(env: Mapping[str, str], injected: float, lock_fd: int) -> None:
    os.set_inheritable(lock_fd, False)
    (state(env) / STANDBY).unlink(missing_ok=True)
    respond(env, run_dir(env), injected)


def watch(env: Mapping[str, str], module: str = MODULE, agent: str = "Claude Code") -> int:
    watch_at = time.time()
    target = run_dir(env)
    target.mkdir(mode=0o700)
    atomic_write_json(target / "start.json", {"watch_at": timestamp(watch_at)})
    if env.get("BENCH_FAULT") == "none":
        atomic_write_json(target / "done.json", {"skipped": "healthy control"})
        return 0
    print("vanilla: waiting for harm onset", file=sys.stderr, flush=True)
    try:
        deadline = datetime.fromisoformat(env["FUZZOPS_DEADLINE"]).timestamp()
        sample = await_onset(env, target, deadline)
        injected = datetime.fromisoformat(sample["inject_at"]).timestamp()
    except (HookError, OSError, ValueError, KeyError, TypeError) as exc:
        why = str(exc) if isinstance(exc, HookError) else f"invalid FuzzOps onset stream ({type(exc).__name__})"
        atomic_write_json(target / "done.json", {"error": why})
        raise HookError(why) from exc
    harmed = sample["first_bad"] is not None
    atomic_write_json(target / "start.json", {"watch_at": timestamp(watch_at), "inject_at": timestamp(injected)})
    atomic_write_json(target / "onset.json", {**sample, "at": timestamp(time.time()) if harmed else None})
    if not harmed:
        atomic_write_json(target / "done.json", {"skipped": "no user harm observed"})
        return 0
    with lane_lock(env) as lock:
        proc = subprocess.Popen(
            [sys.executable, "-m", module, "supervise", str(injected), str(lock.fileno())],
            env=env,
            start_new_session=True,
            pass_fds=(lock.fileno(),),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    print(f"vanilla: launched {agent}", file=sys.stderr, flush=True)
    return proc.wait()


def problems_of(dest: Path, done: Mapping[str, Any]) -> list[str]:
    problems = cell_problems(dest, "vanilla")
    paging = done.get("repage") or {}
    # A single fault's row stays usable on an older pin; with several faults the arm would be one-shot.
    if paging.get("stopped") == repage.UNSUPPORTED and paging.get("page_limit", 0) > 2:
        problems.append("the agent could not be paged again: this FuzzOps pin's traffic stream has no last_bad")
    if done.get("error") and done.get("result_event"):
        problems.append(f"vanilla supervisor failed: {done['error']}")
    if not done.get("result_event"):
        problems.append("Claude Code result event missing" + (f" ({done['error']})" if done.get("error") else ""))
    elif done.get("is_error") or done.get("exit") not in (None, 0):
        problems.append("Claude Code exited with an error")
    return problems


def publish(
    env: Mapping[str, str],
    arm: str,
    files: Sequence[str],
    check: Callable[[Path, Mapping[str, Any]], list[str]],
) -> int:
    """Copy the run's evidence out and write its manifest. The caller holds the lane lock."""
    target, dest = run_dir(env), Path(env["BENCH_OUT"])
    dest.mkdir(parents=True, exist_ok=True)
    manifest = dest / "evidence-manifest.json"
    if not target.exists() and manifest.is_file():
        return int(bool(json.loads(manifest.read_text())["problems"]))
    for name in files:
        if (target / name).is_file():
            shutil.copyfile(target / name, dest / name)
    if (target / "session-dir").is_dir():
        shutil.copytree(target / "session-dir", dest / "session-dir", dirs_exist_ok=True)
    proof = state(env) / "isolation.json"
    if proof.is_file():
        shutil.copyfile(proof, dest / proof.name)
    done = json.loads((target / "done.json").read_text()) if (target / "done.json").exists() else {}
    problems = [] if done.get("skipped") else check(dest, done)
    atomic_write_json(manifest, {"arm": arm, "inject_at": env.get("BENCH_INTERVAL_START"), "problems": problems})
    if target.is_dir():
        shutil.rmtree(target)
    return int(bool(problems))


def release_standby(env: Mapping[str, str]) -> None:
    """Lanes collects once the window is over: end a supervisor that is only watching, so it frees the lane."""
    if not (state(env) / STANDBY).exists() or not run_dir(env).is_dir():
        return
    (run_dir(env) / STOP).touch()
    until = time.monotonic() + RELEASE_SECONDS
    while time.monotonic() < until:
        try:
            with lane_lock(env):
                return
        except HookError:
            time.sleep(0.1)


def collect(env: Mapping[str, str]) -> int:
    release_standby(env)
    with lane_lock(env):
        return publish(env, "vanilla", EVIDENCE_FILES, problems_of)


def preflight(env: Mapping[str, str]) -> None:
    # Lanes' host preflight supplies no BENCH_OPERATOR. Probe the launcher
    # with inert inputs. The per-spec describe/check validates Vertex settings.
    with tempfile.TemporaryDirectory(prefix="vanilla-preflight-") as temp:
        box = Path(temp)
        dummy = box / "dummy.json"
        dummy.write_text("{}")
        proc = subprocess.run(
            [*sandbox(env, box, dummy, dummy, {}), "--version"],
            capture_output=True,
            timeout=20,
            check=False,
        )
    ok = proc.returncode == 0
    detail = "Claude Code starts in the namespace sandbox" if ok else "sandboxed Claude Code --version failed"
    print(json.dumps({"checks": [{"check": "vanilla-sandbox", "ok": ok, "detail": detail}]}))


def lifecycle(env: Mapping[str, str], verb: str, prepare_lane: Callable[[Mapping[str, str]], None]) -> None:
    """The lane verbs that need no agent, under the lane lock."""
    with lane_lock(env):
        if verb == "prepare":
            prepare_lane(env)
        elif verb == "idle":
            print("idle")
        elif verb == "swap":
            prove_no_sdo(env)
            prove_golden(lambda: fuzzops_app(env, "verify"), lambda: fuzzops_app(env, "reset"))
            prepare_lane(env)
        elif verb in ("snapshot", "reset"):
            print(json.dumps(fuzzops_app(env, verb)))
        else:
            kubectl(
                env,
                None,
                "delete",
                f"rolebinding/{IDENTITY}",
                f"role/{IDENTITY}",
                f"serviceaccount/{IDENTITY}",
                "--ignore-not-found",
            )
            (state(env) / "kubeconfig").unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    environ = dict(os.environ if env is None else env)
    eks.export_entry_host(environ)
    verb = args[0] if args else ""
    try:
        if verb == "describe":
            vertex(environ)
            print(json.dumps({"max_startup": "0s", "needs_credentials": True}))
        elif verb == "check":
            vertex(environ)
            for tool in ("bwrap", "kubectl", "claude"):
                binary(environ, tool)
        elif verb == "preflight":
            preflight(environ)
        elif verb == "watch":
            return watch(environ)
        elif verb == "supervise":
            supervise(environ, float(args[1]), int(args[2]))
        elif verb == "collect":
            return collect(environ)
        elif verb == "idle" and (state(environ) / STANDBY).exists():
            # As for SDO between incidents: nothing is working, so an operator-signalled window end may apply.
            print("idle")
        elif verb == "transcripts":
            # Lanes asks on pool build and teardown, outside a row. Collect validates each row's transcript.
            print("row transcripts are verified by collect")
        elif verb in LANE_VERBS:
            lifecycle(environ, verb, prepare)
        else:
            return 64
    except (HookError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        # Avoid echoing malformed credential contents or process arguments in hook logs.
        why = str(exc) if isinstance(exc, HookError) else f"{verb} failed ({type(exc).__name__})"
        if verb == "supervise":
            # Keep what earlier pages recorded.
            done = read_json_object(run_dir(environ) / "done.json")
            atomic_write_json(run_dir(environ) / "done.json", {**done, "error": why})
        if verb == "preflight":
            print(json.dumps({"checks": [{"check": "vanilla-sandbox", "ok": False, "detail": why}]}))
            return 0
        print(why, file=sys.stderr if verb in ("watch", "supervise") else sys.stdout)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
