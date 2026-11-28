"""A small, cheap onboarding run: SDO's onboarding session against a synthetic notes app on kind.

The app (``synthetic/app``) is a standard-library HTTP service with token sign-in, seeded reads
and create/read/delete, in one Deployment on a local kind cluster (``sdo-fastgate``, created once
and reused; nothing shared). Each run gets a fresh namespace and a fresh start repository: the app
source, a goal with three journeys, and SDO's Go-owned bootstrap (``bootstrap/``). The candidate
kit is built from an SDO checkout's working tree. The session is the production launcher
(``sdo/agents/onboarding/run_session.py``) in an isolated bubblewrap root, as in a live onboarding
run, with a cheap model and a dollar cap. ``--scripted`` replaces the model with a fixed script
(``synthetic/scripted_agent.sh``) that writes known-good probes, for plumbing checks without a model.

After the session the exported commit is judged once with a full live ``onboarding journeys --alter``,
the checks SDO runs on the installed export after a live session.
Metrics: model requests, weighted tokens (input + 1.25 cache write 5m + 2 cache write 1h
+ 0.05 cache read + 5 output), cost, validator rejections the agent saw, journeys runs, export
rejections, wall time, and the journeys that passed.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from benchmarks.onboarding_gate.classify import journeys_rejections, memory_check_rejections
from benchmarks.onboarding_gate.sandbox import Sandbox, go_environment
from benchmarks.onboarding_gate.transcript import read_tool_calls

HERE = Path(__file__).resolve().parent
APP = HERE / "synthetic" / "app"
APPLICATION = "synthetic-notes"
ENTRY = "notes:8080"
DISALLOWED = (
    "ScheduleWakeup,CronCreate,CronDelete,CronList,RemoteTrigger,AskUserQuestion,EnterPlanMode,"
    "ExitPlanMode,PushNotification,EnterWorktree,ExitWorktree"
)
WEIGHTS = {
    "inputTokens": 1.0,
    "cacheCreationEphemeral5mInputTokens": 1.25,
    "cacheCreationEphemeral1hInputTokens": 2.0,
    "cacheReadInputTokens": 0.05,
    "outputTokens": 5.0,
}


class GateError(RuntimeError):
    """The gate could not set up or run (cluster, build, sandbox); not a finding about the change."""


@dataclass(frozen=True)
class E2EConfig:
    repo: Path  # the SDO checkout under test (its working tree)
    out: Path  # this run's directory (outside the repository)
    model: str = "claude-sonnet-5-5"
    effort: str = "medium"
    budget_usd: float = 1.0
    timeout_s: int = 420
    cluster: str = "sdo-fastgate"
    namespace: str = "fastgate-notes"
    scripted: bool = False
    claude_config: Path = Path.home() / ".claude"

    def __post_init__(self) -> None:
        if not (self.repo / "sdo" / "operator" / "go.mod").is_file():
            raise ValueError(f"{self.repo} is not an SDO checkout")
        if self.budget_usd <= 0 or self.timeout_s <= 0:
            raise ValueError("budget and timeout must be positive")


@dataclass
class E2EResult:
    mode: str
    model: str
    validated: str = ""
    exit_code: int = 0
    wall_s: float = 0.0
    session_s: float = 0.0
    requests: int = 0
    weighted_tokens: float = 0.0
    cost_usd: float = 0.0
    journeys_runs: int = 0
    validator_calls: int = 0
    rejections: dict[str, int] = field(default_factory=dict)
    export_rejections: int = 0
    journeys_passed: str = ""
    final_report_tail: str = ""

    def text(self) -> str:
        rejected = ", ".join(f"{k} {v}" for k, v in sorted(self.rejections.items())) or "none"
        return (
            f"{self.mode} ({self.model}): {'validated ' + self.validated[:12] if self.validated else 'NOT validated'}"
            f" (exit {self.exit_code})\n"
            f"  wall {self.wall_s:.0f}s (session {self.session_s:.0f}s); requests {self.requests}; "
            f"weighted tokens {self.weighted_tokens / 1e3:.0f}k; cost ${self.cost_usd:.2f}\n"
            f"  journeys runs {self.journeys_runs}; validator calls {self.validator_calls}; "
            f"export rejections {self.export_rejections}\n"
            f"  rejections seen: {rejected}\n"
            f"  final journeys check: {self.journeys_passed or 'not run'}\n"
        )


def _run(argv: list[str], **kwargs: object) -> str:
    done = subprocess.run(argv, capture_output=True, text=True, check=False, **kwargs)  # type: ignore[call-overload]
    if done.returncode != 0:
        raise GateError(f"{' '.join(argv[:4])} failed ({done.returncode}): {(done.stderr or done.stdout)[-800:]}")
    return done.stdout


def ensure_cluster(name: str, out: Path) -> Path:
    """A local kind cluster with the app image loaded; returns a kubeconfig for it."""
    if name not in _run(["kind", "get", "clusters"]).split():
        _run(["kind", "create", "cluster", "--name", name, "--wait", "120s", *_node_image()])
        _run(["docker", "pull", "-q", "python:3.12-alpine"]) if not _image_present() else None
        _run(["kind", "load", "docker-image", "python:3.12-alpine", "--name", name])
    kubeconfig = out / "kubeconfig"
    kubeconfig.write_text(_run(["kind", "get", "kubeconfig", "--name", name]))
    kubeconfig.chmod(0o600)
    return kubeconfig


def _node_image() -> list[str]:
    """A kind node image already on the host, newest first, so creation pulls nothing."""
    tags = _run(["docker", "images", "kindest/node", "--format", "{{.Tag}}"]).split()
    tags = sorted((tag for tag in tags if tag.startswith("v")), key=lambda t: [int(p) for p in re.findall(r"\d+", t)])
    return ["--image", f"kindest/node:{tags[-1]}"] if tags else []


def _image_present() -> bool:
    return (
        subprocess.run(
            ["docker", "image", "inspect", "python:3.12-alpine"], capture_output=True, check=False
        ).returncode
        == 0
    )


def render_manifest(namespace: str) -> str:
    code = "".join(f"    {line}\n" if line else "\n" for line in (APP / "server.py").read_text().splitlines())
    return (APP / "app.yaml").read_text().replace("NAMESPACE", namespace).replace("SERVER_PY\n", code)


def deploy_app(kubeconfig: Path, namespace: str) -> None:
    """A fresh namespace with the app running and ready."""
    kubectl = ["kubectl", "--kubeconfig", str(kubeconfig)]
    subprocess.run(
        [*kubectl, "delete", "namespace", namespace, "--ignore-not-found", "--wait=true"],
        capture_output=True,
        check=False,
        timeout=180,
    )
    _run([*kubectl, "create", "namespace", namespace])
    _run([*kubectl, "apply", "-f", "-"], input=render_manifest(namespace))
    _run([*kubectl, "-n", namespace, "rollout", "status", "deployment/notes", "--timeout=120s"])


def goal_document(namespace: str) -> str:
    journeys = [line.split("\t", 1) for line in (APP / "journeys.txt").read_text().splitlines() if line]
    front = ["---", "schema_version: 1", "owner: human", f"application: {json.dumps(APPLICATION)}", "journeys:"]
    for key, what in journeys:
        front += [f"  - id: {key}", f"    what: {json.dumps(what)}"]
    body = [
        "Set up monitoring for this application using the user journeys below. Alert us when users can no "
        "longer complete a journey, and leave the monitoring running after your session ends.",
        *(what for _, what in journeys),
    ]
    return "\n".join([*front, "---", *body]) + "\n"


def prompt(namespace: str) -> str:
    return (
        f"You are SDO's onboarding agent for {APPLICATION}, deployed and healthy in its cluster.\n"
        "The user's requirements and journeys are in .sdo/goal.md. Read and preserve that file verbatim.\n"
        f"Application namespace: {namespace}. Private entry service: {ENTRY}. Use isolated synthetic inputs "
        "for user-facing probes.\n"
        "Start with /kit/skills/sdo-onboarding/SKILL.md. Work alone and unattended, in one session of at most "
        "10 minutes; commit working states as you go.\n"
    )


def _git(repo: Path, *args: str) -> str:
    return _run(["git", "-C", str(repo), *args]).strip()


def start_repository(config: E2EConfig, work: Path, bootstrap: Path) -> str:
    """The session's checkout: app source, goal and Go bootstrap, committed; returns the base commit."""
    work.mkdir(parents=True)
    shutil.copy(APP / "server.py", work / "server.py")
    shutil.copy(APP / "README.md", work / "README.md")
    (work / "deploy").mkdir()
    manifest = render_manifest(config.namespace)
    (work / "deploy" / "app.yaml").write_text(manifest)
    _run(["git", "init", "-q", "-b", "main", str(work)])
    _git(work, "config", "user.name", "fastgate")
    _git(work, "config", "user.email", "fastgate@invalid")
    _git(work, "add", "-A")
    _git(work, "commit", "-qm", "Import synthetic application source")
    source = _git(work, "rev-parse", "HEAD")
    (work / ".sdo").mkdir()
    (work / ".sdo" / "goal.md").write_text(goal_document(config.namespace))
    rendered = config.out / "rendered.yaml"
    rendered.write_text(manifest)
    _run([str(bootstrap), str(work), APPLICATION, source, str(rendered)])
    _git(work, "add", "-f", ".sdo")
    _git(work, "commit", "-qm", "Record goal and onboarding bootstrap")
    return _git(work, "rev-parse", "HEAD")


def build_tools(config: E2EConfig) -> tuple[Path, Path]:
    """The candidate's sdo-operator binary and the bootstrap tool, built from the checkout."""
    tools = config.out / "tools"
    tools.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "CGO_ENABLED": "0"}
    operator, bootstrap = tools / "sdo-operator-real", tools / "bootstrap"
    _run(["go", "build", "-o", str(operator), "./cmd/sdo-operator"], cwd=config.repo / "sdo" / "operator", env=env)
    _run(["go", "build", "-o", str(bootstrap), "."], cwd=HERE / "bootstrap", env=env)
    return operator, bootstrap


def kit_directory(config: E2EConfig, operator: Path) -> Path:
    """/kit as the live harness lays it out; skills and operator source are bound from the checkout."""
    kit = config.out / "kit"
    (kit / "bin").mkdir(parents=True)
    (kit / "skills").mkdir()
    (kit / "src" / "sdo" / "operator").mkdir(parents=True)
    shutil.copy(operator, kit / "bin" / "sdo-operator-real")
    wrapper = kit / "bin" / "sdo-operator"
    wrapper.write_text(
        "#!/usr/bin/env bash\nexport SDO_OPERATOR_ROOT=/kit/src/sdo/operator CGO_ENABLED=0\n"
        "if [[ ${1:-} == memory ]]; then\n    export SDO_ACTOR=health_judge SDO_PHASE=initial\nfi\n"
        'exec /kit/bin/sdo-operator-real "$@"\n'
    )
    wrapper.chmod(0o755)
    shutil.copy(config.repo / "sdo" / "agents" / "onboarding" / "run_session.py", kit / "run_session.py")
    if config.scripted:
        for name in ("scripted_claude.py", "scripted_agent.sh"):
            shutil.copy(HERE / "synthetic" / name, kit / name)
        fixture = kit / "fixture"
        shutil.copytree(HERE / "synthetic" / "fixture", fixture)
    return kit


def session_sandbox(
    config: E2EConfig, kit: Path, work: Path, scratch: Path, sink: Path, kubeconfig: Path, base: str
) -> Sandbox:
    go_env = go_environment()
    goroot = Path(_run(["go", "env", "GOROOT"]).strip())
    claude = Path(shutil.which("claude") or "claude").resolve()
    tools = scratch.parent / "toolbin"
    tools.mkdir(exist_ok=True)
    read_only = {
        kit: "/kit",
        config.repo / "sdo" / "skills": "/kit/skills",
        config.repo / "sdo" / "operator": "/kit/src/sdo/operator",
        goroot: "/tool/go",
        kubeconfig: "/kubeconfig",
        tools: "/tool/bin",
    }
    if config.scripted:
        (tools / "claude").write_text('#!/bin/sh\nexec python3 /kit/scripted_claude.py "$@"\n')
        (tools / "claude").chmod(0o755)
    else:
        shutil.copy(claude, tools / "claude")
        credentials = config.claude_config / ".credentials.json"
        if credentials.is_file():
            shutil.copy(credentials, sink / ".credentials.json")
    kubectl = shutil.which("kubectl")
    if kubectl:
        shutil.copy(Path(kubectl).resolve(), tools / "kubectl")
    gocache, gomod = Path(go_env["GOCACHE"]), Path(go_env["GOMODCACHE"])
    env = {
        "PATH": "/kit/bin:/tool/bin:/tool/go/bin:/usr/local/bin:/usr/bin:/bin",
        "HOME": "/home/agent",
        "TMPDIR": "/tmp",
        "TERM": "dumb",
        "LANG": "C.UTF-8",
        "GOPATH": "/scratch/gopath",
        "GOCACHE": "/gocache",
        "GOMODCACHE": "/gomod",
        "GOPROXY": "off",
        "GOFLAGS": "-mod=mod",
        "GOTOOLCHAIN": "local",
        "CLAUDE_CONFIG_DIR": "/sink",
        "KUBECONFIG": "/kubeconfig",
        "SDO_OPERATOR_ROOT": "/kit/src/sdo/operator",
        "SDO_BASE_COMMIT": base,
        "OB": "/kit",
        "SDO_REPO": "/work",
        **({"GATE_NAMESPACE": config.namespace} if config.scripted else {}),
    }
    binds = {
        work: "/work",
        scratch: "/scratch",
        sink: "/sink",
        gocache: "/gocache",
        gomod: "/gomod",
        scratch.parent / "home": "/home/agent",
    }
    (scratch.parent / "home").mkdir(exist_ok=True)
    return Sandbox(binds=binds, env=env, read_only=read_only, chdir="/work", isolated=True, network=True)


def run(config: E2EConfig) -> E2EResult:
    started = time.monotonic()
    out = config.out
    if out.exists():
        raise GateError(f"{out} exists; give each run its own directory")
    out.mkdir(parents=True)
    result = E2EResult("scripted" if config.scripted else "agent", "scripted" if config.scripted else config.model)
    kubeconfig = ensure_cluster(config.cluster, out)
    operator, bootstrap = build_tools(config)
    deploy_app(kubeconfig, config.namespace)
    work, scratch, sink = out / "work", out / "scratch", out / "sink"
    base = start_repository(config, work, bootstrap)
    scratch.mkdir()
    sink.mkdir()
    (scratch / "prompt.txt").write_text(prompt(config.namespace))
    kit = kit_directory(config, operator)
    sandbox = session_sandbox(config, kit, work, scratch, sink, kubeconfig, base)
    claude_args = [
        "--model",
        config.model,
        "--effort",
        config.effort,
        "--max-budget-usd",
        str(config.budget_usd),
        "--disallowedTools",
        DISALLOWED,
    ]
    command = [
        "python3",
        "/kit/run_session.py",
        "--base",
        base,
        "--operator",
        "/kit/bin/sdo-operator-real",
        "/scratch/prompt.txt",
        "/scratch/transcript.jsonl",
        "/work",
        "--",
        *claude_args,
    ]
    session_start = time.monotonic()
    with (out / "driver.out").open("w") as stdout, (out / "driver.err").open("w") as stderr:
        try:
            done = subprocess.run(
                sandbox.argv(command),
                stdout=stdout,
                stderr=stderr,
                stdin=subprocess.DEVNULL,
                timeout=config.timeout_s,
                check=False,
            )
            result.exit_code = done.returncode
        except subprocess.TimeoutExpired:
            result.exit_code = 124
    result.session_s = time.monotonic() - session_start
    result.wall_s = time.monotonic() - started
    (out / "result.json").write_text(json.dumps(asdict(result), indent=1) + "\n")  # kept if evaluate fails
    evaluate(config, sandbox, result)
    result.wall_s = time.monotonic() - started
    (out / "result.json").write_text(json.dumps(asdict(result), indent=1) + "\n")
    return result


def evaluate(config: E2EConfig, sandbox: Sandbox, result: E2EResult) -> None:
    """Read a finished session's outcome and metrics, then judge its export once with a live journeys run."""
    out, scratch = config.out, config.out / "scratch"
    driver = (out / "driver.out").read_text()
    match = re.search(r"^Onboarding export validated: ([0-9a-f]{40})$", driver, re.MULTILINE)
    result.validated = match.group(1) if match else ""
    result.export_rejections = (out / "driver.err").read_text().count("Onboarding export rejected")
    measure(scratch / "transcript.jsonl", result)
    if result.validated:
        checked = sandbox.run(
            [
                "/kit/bin/sdo-operator",
                "onboarding",
                "journeys",
                "--repo",
                "/work",
                "--namespace",
                config.namespace,
                "--alter",
            ],
            timeout=300,
        )
        tail = [line for line in checked.stdout.splitlines() if line.strip()][-3:]
        result.final_report_tail = "\n".join(tail)
        summary = re.search(r"(\d+) of (\d+) passed", checked.stdout)
        result.journeys_passed = (
            f"{summary.group(1)} of {summary.group(2)} passed"
            if summary
            else (f"no summary (exit {checked.returncode})")
        )


def reevaluate(config: E2EConfig) -> E2EResult:
    """evaluate() again for an existing run directory (after a metrics fix), without a new session."""
    saved = json.loads((config.out / "result.json").read_text()) if (config.out / "result.json").is_file() else {}
    result = E2EResult(saved.get("mode", "agent"), saved.get("model", config.model))
    result.exit_code, result.wall_s, result.session_s = (
        saved.get("exit_code", 0),
        saved.get("wall_s", 0.0),
        saved.get("session_s", 0.0),
    )
    work = config.out / "work"
    base = _git(work, "rev-list", "--max-parents=0", "HEAD").splitlines()[0]
    base = _git(work, "rev-list", "-1", "--grep=Record goal and onboarding bootstrap", "HEAD") or base
    kubeconfig = ensure_cluster(config.cluster, config.out)
    sandbox = session_sandbox(
        config, config.out / "kit", work, config.out / "scratch", config.out / "sink", kubeconfig, base
    )
    evaluate(config, sandbox, result)
    (config.out / "result.json").write_text(json.dumps(asdict(result), indent=1) + "\n")
    return result


def measure(transcript: Path, result: E2EResult) -> None:
    """Requests, weighted tokens, cost and validator feedback from a stream-json transcript."""
    if not transcript.is_file():
        return
    messages: set[str] = set()
    usage = dict.fromkeys(WEIGHTS, 0.0)
    for line in transcript.read_text().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("type") == "assistant" and not event.get("parent_tool_use_id"):
            messages.add(str((event.get("message") or {}).get("id")))
        if event.get("type") == "result":
            # Per-result usage, as shared/weighted_tokens.py reads it: since the last result when the
            # CLI says so, else the result's own model usage.
            result.cost_usd = max(result.cost_usd, float(event.get("total_cost_usd") or 0))
            per_model = event.get("model_usage_since_last_result") or event.get("modelUsage") or {}
            for model_usage in per_model.values():
                for key in WEIGHTS:
                    usage[key] += float(model_usage.get(key) or 0)
                if not (
                    model_usage.get("cacheCreationEphemeral5mInputTokens")
                    or model_usage.get("cacheCreationEphemeral1hInputTokens")
                ):
                    usage["cacheCreationEphemeral5mInputTokens"] += float(
                        model_usage.get("cacheCreationInputTokens") or 0
                    )
    result.requests = len(messages)
    result.weighted_tokens = sum(usage[key] * weight for key, weight in WEIGHTS.items())
    rejections: dict[str, int] = {}
    for call in read_tool_calls(transcript):
        if not call.validates():
            continue
        result.validator_calls += 1
        if "onboarding journeys" in call.command:
            result.journeys_runs += 1
        for rejection in journeys_rejections(call.result) | memory_check_rejections(call.result):
            rejections[rejection.kind] = rejections.get(rejection.kind, 0) + 1
    result.rejections = rejections
