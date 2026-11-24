"""Lanes subject for a plain Claude Code agent responding to failing pack journeys."""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, BinaryIO

from benchmarks.fuzzops import eks, page
from benchmarks.fuzzops.cellevidence import atomic_write_json, cell_problems
from benchmarks.fuzzops.hooks import HookError, fuzzops_app, prove_golden
from benchmarks.fuzzops.view import redact

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

MODEL = "claude-opus-5-5"
EFFORT = "medium"
ONSET_SECONDS = 150
AGENT_SECONDS = 25 * 60
IDENTITY = "vanilla-agent"
MODULE = "benchmarks.fuzzops.vanilla"
VERTEX_ENV = ("CLAUDE_CODE_USE_VERTEX", "CLOUD_ML_REGION", "ANTHROPIC_VERTEX_PROJECT_ID")
EVIDENCE_FILES = ("start.json", "onset.json", "done.json", "usage.json", "transcript.jsonl", "stderr.log")


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
    if any("sdo" in name.lower() for name in (cluster + "\n" + workloads).splitlines()):
        raise HookError("SDO objects found on the vanilla lane")
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


def prepare(env: Mapping[str, str]) -> None:
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
    print(json.dumps({"pack": env["BENCH_PACK"], "operator_version": f"{MODEL}/{EFFORT}", "memory": "none"}))


def vertex(env: Mapping[str, str]) -> tuple[Path, dict[str, str]]:
    settings = json.loads(Path(env["BENCH_OPERATOR"]).read_text())["settings"]
    auth = Path(settings["auth_file"]).expanduser().resolve()
    extra = dict(item.split("=", 1) for item in settings["config"]["agent"]["extra_env"])
    if extra.get("CLAUDE_CODE_USE_VERTEX") != "1" or any(not extra.get(key) for key in VERTEX_ENV):
        raise HookError("operator settings require Claude Code Vertex environment")
    if not auth.is_file() or not os.access(auth, os.R_OK):
        raise HookError("Vertex auth_file is unreadable")
    return auth, {key: extra[key] for key in VERTEX_ENV}


def sandbox(env: Mapping[str, str], box: Path, auth: Path, config: Path, extra: Mapping[str, str]) -> list[str]:
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
        (box / directory).mkdir()
        args += ["--bind", str(box / directory), f"/{directory}"]
    for source, target in (
        (binary(env, "claude"), "/tools/claude"),
        (binary(env, "kubectl"), "/tools/kubectl"),
        (str(auth), "/vertex.json"),
        (str(config), "/kubeconfig"),
    ):
        args += ["--ro-bind", source, target]
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
    for key, value in settings.items():
        args += ["--setenv", key, value]
    return [*args, "--chdir", "/work", "--", "/tools/claude"]


def copy_log(source: Path, dest: Path, identities: Sequence[str]) -> dict[str, Any]:
    # Keep the CLI JSONL shape (including IDs) for the evidence validator.
    result: dict[str, Any] = {}
    with source.open(errors="replace") as raw, dest.open("w") as out:
        for line in raw:
            safe = redact(line.rstrip("\n"), identities)
            out.write(safe + "\n")
            try:
                event = json.loads(safe)
            except json.JSONDecodeError:
                continue
            if isinstance(event, dict) and event.get("type") == "result":
                result = event
    return result


def capture_agent(env: Mapping[str, str], target: Path, injected: float) -> None:
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
        key = json.loads(auth.read_text()).get("private_key", "")
        secrets = [token, *key.splitlines()]
        prompt = f"Users are reporting problems with the application in namespace {ns}; investigate and fix it."
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
                    timeout=max(0.01, injected + AGENT_SECONDS - started),
                ).returncode
            except subprocess.TimeoutExpired:
                # Killing bwrap's PID namespace supervisor also kills Claude's descendants.
                capped, exit_code = True, None
        ended = time.time()
        result = copy_log(box / "stream", target / "transcript.jsonl", secrets)
        copy_log(box / "stderr", target / "stderr.log", secrets)
        projects = box / "config" / "projects"
        if projects.is_dir() and not projects.is_symlink():
            for path in projects.rglob("*.jsonl"):
                if path.is_symlink() or not path.is_file():
                    continue
                dest = target / "session-dir" / path.relative_to(projects)
                dest.parent.mkdir(parents=True, exist_ok=True)
                copy_log(path, dest, secrets)
        atomic_write_json(
            target / "usage.json",
            {
                **(result.get("usage") or {}),
                "total_cost_usd": result.get("total_cost_usd"),
            },
        )
        summary = result.get("result")
        if isinstance(summary, str) and env.get("FUZZOPS_REPORT"):
            atomic_write_json(Path(env["FUZZOPS_REPORT"]), {"summary": summary[:600]})
        atomic_write_json(
            target / "done.json",
            {
                "started_at": timestamp(started),
                "ended_at": timestamp(ended),
                "capped": capped,
                "exit": exit_code,
                "model": MODEL,
                "effort": EFFORT,
                "result_event": bool(result),
                "is_error": result.get("is_error"),
            },
        )


def await_onset(env: Mapping[str, str], injected: float) -> tuple[float | None, list[str]]:
    pack = Path(env["BENCH_PACK"])
    names = [journey["name"] for journey in page.journeys(pack)["journeys"]]
    fuzzops = Path(env["FUZZOPS_BIN"]).resolve()
    deadline = injected + ONSET_SECONDS
    args = [
        str(fuzzops),
        "check",
        "--pack",
        str(pack),
        "--namespace",
        env["FUZZOPS_NAMESPACE"],
        "--duration",
        "4s",
        "--rate",
        "6",
        "--trace",
        "--lines",
    ]
    while (remaining := deadline - time.time()) > 0:
        try:
            check = subprocess.run(
                args,
                cwd=fuzzops.parent.parent,
                env=env,
                timeout=min(20, remaining),
                capture_output=True,
                text=True,
                check=False,
            )
        except subprocess.TimeoutExpired:
            continue
        if check.returncode not in (0, 1):
            raise HookError(f"onset check failed (exit {check.returncode})")
        failing = page.parse_trace(check.stdout, check.stderr, names)
        if failing:
            return time.time(), sorted(failing)
        time.sleep(min(0.25, max(0, deadline - time.time())))
    return None, []


def supervise(env: Mapping[str, str], injected: float, lock_fd: int) -> None:
    os.set_inheritable(lock_fd, False)
    target = run_dir(env)
    atomic_write_json(target / "start.json", {"watch_at": timestamp(injected)})
    if env.get("BENCH_FAULT") == "none":
        atomic_write_json(target / "done.json", {"skipped": "healthy control"})
        return
    onset, failing = await_onset(env, injected)
    atomic_write_json(target / "onset.json", {"at": timestamp(onset) if onset else None, "failing": failing})
    capture_agent(env, target, injected)


def watch(env: Mapping[str, str]) -> int:
    injected = time.time()
    with lane_lock(env) as lock:
        target = run_dir(env)
        target.mkdir(mode=0o700)
        proc = subprocess.Popen(
            [sys.executable, "-m", MODULE, "supervise", str(injected), str(lock.fileno())],
            env=env,
            start_new_session=True,
            pass_fds=(lock.fileno(),),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    print("vanilla: waiting for harm onset", flush=True)
    return proc.wait()


def collect(env: Mapping[str, str]) -> int:
    target, dest = run_dir(env), Path(env["BENCH_OUT"])
    dest.mkdir(parents=True, exist_ok=True)
    with lane_lock(env):
        manifest = dest / "evidence-manifest.json"
        if not target.exists() and manifest.is_file():
            return int(bool(json.loads(manifest.read_text())["problems"]))
        for name in EVIDENCE_FILES:
            if (target / name).is_file():
                shutil.copyfile(target / name, dest / name)
        if (target / "session-dir").is_dir():
            shutil.copytree(target / "session-dir", dest / "session-dir", dirs_exist_ok=True)
        proof = state(env) / "isolation.json"
        if proof.is_file():
            shutil.copyfile(proof, dest / proof.name)
        done = json.loads((target / "done.json").read_text()) if (target / "done.json").exists() else {}
        problems = [] if done.get("skipped") == "healthy control" else cell_problems(dest, "vanilla")
        if not done.get("skipped") and not done.get("result_event"):
            problems.append("Claude Code result event missing" + (f" ({done['error']})" if done.get("error") else ""))
        elif done.get("is_error") or done.get("exit") not in (None, 0):
            problems.append("Claude Code exited with an error")
        atomic_write_json(
            manifest,
            {
                "arm": "vanilla",
                "inject_at": env.get("BENCH_INTERVAL_START"),
                "problems": problems,
            },
        )
        if target.is_dir():
            shutil.rmtree(target)
    return int(bool(problems))


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
        elif verb == "transcripts":
            # Lanes asks on pool build and teardown, outside a row. Collect validates each row's transcript.
            print("row transcripts are verified by collect")
        elif verb in ("prepare", "idle", "snapshot", "reset", "swap", "teardown"):
            with lane_lock(environ):
                if verb == "prepare":
                    prepare(environ)
                elif verb == "idle":
                    print("idle")
                elif verb == "swap":
                    prove_no_sdo(environ)
                    prove_golden(lambda: fuzzops_app(environ, "verify"), lambda: fuzzops_app(environ, "reset"))
                    prepare(environ)
                elif verb in ("snapshot", "reset"):
                    print(json.dumps(fuzzops_app(environ, verb)))
                else:
                    kubectl(
                        environ,
                        None,
                        "delete",
                        f"rolebinding/{IDENTITY}",
                        f"role/{IDENTITY}",
                        f"serviceaccount/{IDENTITY}",
                        "--ignore-not-found",
                    )
                    (state(environ) / "kubeconfig").unlink(missing_ok=True)
        else:
            return 64
    except (HookError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
        # Avoid echoing malformed credential contents or process arguments in hook logs.
        why = str(exc) if isinstance(exc, HookError) else f"{verb} failed ({type(exc).__name__})"
        if verb == "supervise":
            atomic_write_json(run_dir(environ) / "done.json", {"error": why})
        if verb == "preflight":
            print(json.dumps({"checks": [{"check": "vanilla-sandbox", "ok": False, "detail": why}]}))
            return 0
        print(why)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
