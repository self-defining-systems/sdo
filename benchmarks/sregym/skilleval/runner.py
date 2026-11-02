"""Run reflection scenarios with a chosen skills directory and tabulate the results.

Example::

    uv run python -m benchmarks.sregym.skilleval.runner \\
        --scenarios /mnt/data/shli/sdo-exp/mixed10/skilleval/scenarios \\
        --skills sdo/skills --label baseline --repeat 2 --parallel 3 \\
        --out /mnt/data/shli/sdo-exp/mixed10/skilleval/runs

Each run copies the scenario, mounts it into the scenario's recorded responder
image exactly where the live reflection Job had it, mounts ``--skills`` over the
image's skills root, resumes the responder's Codex or Claude Code session with
the recorded reflection request and the scenario's provider/model/effort, and
records cost, the independent memory check, the diff and replay verdicts.

The host is shared with experiment lanes: at most three runs at once, each
container at nice 10 with GOMAXPROCS=4, and a run starts only while the 1-minute
load average is below 40.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchmarks.sregym.skilleval.metrics import (
    RunMetrics,
    Usage,
    check_replay_notes,
    claude_usage,
    decision_kind,
    disallowed_paths,
    hand_repairs,
    memory_check_passed,
    programs_written,
    replay_verdicts,
)
from benchmarks.sregym.skilleval.scenario import Scenario
from libs.agent_cli.provider_capacity import ProviderCapacity

if TYPE_CHECKING:
    from collections.abc import Callable

ENTRY = Path(__file__).with_name("container_entry.py")
# Shared-host budget (user, 2026-10-28): runs yield to experiment lanes.
MAX_PARALLEL = 3
START_LOAD_LIMIT = 40.0
CONTAINER_NICE = "10"
CONTAINER_GOMAXPROCS = "4"
START_SETTLE_SECONDS = 20.0
DEFAULT_MODEL = "gpt-6-luna"
DEFAULT_BUDGET_SECONDS = 40 * 60
CONTAINER_UID = os.getuid()
CONTAINER_GID = os.getgid()


def toolchain_image(responder_image: str) -> str:
    """The controller image of the same build; its Go toolchain is what the live Job mounted."""

    name, _, tag = responder_image.partition(":")
    if not tag or not name.endswith("responder"):
        raise ValueError(f"cannot derive the toolchain image from {responder_image!r}")
    return f"sdo-controller:{tag}"


def prepare_toolchain(image: str, cache: Path, controller_image: str = "") -> Path:
    """Extract the Go toolchain and module cache once per build, and build ``sdo-replay`` with it."""

    tag = image.partition(":")[2]
    root = cache / tag
    if (root / "ready").is_file():
        return root
    staging = cache / f".{tag}.{os.getpid()}"
    _remove_tree(staging)
    staging.mkdir(parents=True)
    # The module cache is read-only; tar applies directory modes after their contents, docker cp does not.
    for source, target in (("usr/local/go", "go"), ("go", "cache")):
        (staging / target).mkdir()
        producer = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "tar",
                controller_image or toolchain_image(image),
                "-C",
                f"/{source}",
                "-cf",
                "-",
                ".",
            ],
            stdout=subprocess.PIPE,
        )
        subprocess.run(["tar", "-xf", "-", "-C", str(staging / target)], stdin=producer.stdout, check=True)
        if producer.wait():
            raise subprocess.CalledProcessError(producer.returncode, producer.args)
    (staging / "bin").mkdir()
    subprocess.run(
        [
            "docker", "run", "--rm", "--user", f"{CONTAINER_UID}:{CONTAINER_GID}",
            "-v", f"{staging / 'go'}:/usr/local/go:ro", "-v", f"{staging / 'cache'}:/go:ro",
            "-v", f"{staging / 'bin'}:/out", "-e", "HOME=/tmp", "-e", "GOCACHE=/tmp/go-cache",
            "-e", f"GOMAXPROCS={CONTAINER_GOMAXPROCS}",
            "-w", "/opt/sdo/sdo/operator", "--entrypoint", "nice", image,
            "-n", CONTAINER_NICE, "go", "build", "-o", "/out/sdo-replay", "./cmd/sdo-replay",
        ],
        check=True,
    )  # fmt: skip
    (staging / "ready").write_text(image + "\n", encoding="utf-8")
    try:
        staging.rename(root)
    except OSError:
        _remove_tree(staging)
    return root


def _remove_tree(path: Path) -> None:
    if path.exists():
        subprocess.run(["chmod", "-R", "u+w", str(path)], check=False)
        shutil.rmtree(path, ignore_errors=True)


def docker_command(
    *,
    scenario: Scenario,
    run_dir: Path,
    skills: Path,
    toolchain: Path,
    auth: Path,
    model: str,
    name: str,
    extra_skills: tuple[str, ...] = (),
    post_only: bool = False,
    replay: str = "on",
    code_root: Path | None = None,
    operator_binary: Path | None = None,
) -> list[str]:
    """The reflection Job's container, as one ``docker run``: same image, paths, user-visible env."""

    if replay not in {"on", "off"}:
        raise ValueError("replay must be on or off")
    if (code_root is None) != (operator_binary is None):
        raise ValueError("source overlay requires code_root and operator_binary together")
    session = "/sdo/session"
    mounts = {
        f"{run_dir / 'session'}": session,
        f"{run_dir / 'session' / 'corpus'}": f"{session}/corpus:ro",
        f"{run_dir / 'checkout'}": scenario.checkout_path,
        f"{run_dir / 'task.json'}": "/sdo/request/agent-task.json:ro",
        f"{run_dir / 'tmp'}": "/tmp",
        f"{run_dir / 'codex-homes'}": "/sdo/codex-homes",
        f"{run_dir / 'out'}": "/skilleval/out",
        f"{auth}": f"/sdo/credentials/{'.credentials.json' if scenario.provider == 'claude' else 'auth.json'}:ro",
        f"{skills}": "/opt/sdo/sdo/skills:ro",
        f"{toolchain / 'go'}": "/usr/local/go:ro",
        f"{toolchain / 'cache'}": "/go:ro",
        f"{toolchain / 'bin'}": "/skilleval/bin:ro",
        f"{ENTRY}": "/skilleval/entry.py:ro",
    }
    if code_root is not None and operator_binary is not None:
        for part in ("sdo", "libs", "benchmarks"):
            mounts[str(code_root / part)] = f"/opt/sdo/{part}:ro"
        mounts[str(operator_binary)] = "/usr/local/bin/sdo-operator:ro"
    if (run_dir / "session" / "api-history").is_dir():
        mounts[f"{run_dir / 'session' / 'api-history'}"] = f"{session}/api-history:ro"
    model = model or scenario.model
    environment = {
        "HOME": "/tmp/home",
        # A resumed reflection uses the responder's homes; a fresh one gets private homes, as live.
        "CODEX_HOME": f"{session}/codex" if scenario.session_mode == "resume" else "/tmp/codex",
        "CLAUDE_CONFIG_DIR": f"{session}/claude" if scenario.session_mode == "resume" else "/tmp/claude",
        "SDO_TURN_USAGE_LOG": f"{session}/reflection-usage.jsonl",
        "SDO_SESSION_DIR": session,
        "SDO_CODEX_HOME_ROOT": "/sdo/codex-homes",
        "SDO_ACTOR": "responder",
        "SDO_PHASE": "reflection",
        "SDO_BASE_COMMIT": scenario.outcome_commit,
        "SDO_HEALTHY_BASELINE": f"{session}/corpus/healthy",
        "SDO_CAPTURED_CORPUS_DIR": f"{session}/corpus",
        "SDO_AGENT_PROVIDER": scenario.provider,
        "SDO_REFLECTION_EFFORT": scenario.effort,
        "SDO_RESPONDER_EFFORT": scenario.effort,
        "SDO_RESPONDER_MODEL": model,
        "SDO_REFLECTION_MODEL": model,
        "SDO_LIFECYCLE_MODEL": model,
        "SDO_DEPLOYMENT_MODEL": model,
        "SDO_CODE_PATH": "go",
        "SDO_AGENT_GUIDANCE": "go",
        "SDO_MEMORY_VALIDATION": "go",
        "SDO_REPLAY": replay,
        "SKILLEVAL_EXTRA_SKILLS": ",".join(extra_skills),
        "GOMAXPROCS": CONTAINER_GOMAXPROCS,
        "SKILLEVAL_POST_ONLY": "1" if post_only else "0",
    }
    if code_root is not None and os.environ.get("SDO_OPUS_POOL_DIR"):
        mounts[os.environ["SDO_OPUS_POOL_DIR"]] = "/sdo/opus-pool"
        environment["SDO_OPUS_POOL_DIR"] = "/sdo/opus-pool"
        for key in ("SDO_OPUS_POOL_MODE", "SDO_OPUS_POOL_HOST_ID", "SDO_OPUS_MAX_SESSIONS"):
            if value := os.environ.get(key):
                environment[key] = value
    if (run_dir / "session" / "api-history").is_dir():
        environment["SDO_API_HISTORY_DIR"] = f"{session}/api-history"
    if (run_dir / "session" / "tools").is_dir():
        environment["SDO_TOOLS_DIR"] = f"{session}/tools"
    command = [
        "docker", "run", "--rm", "--name", name,
        "--user", f"{CONTAINER_UID}:{CONTAINER_GID}",
        "--security-opt", "seccomp=unconfined", "--read-only",
        "--cpus", "2", "--memory", "6g",
        "-w", scenario.checkout_path,
    ]  # fmt: skip
    for source, target in mounts.items():
        command += ["-v", f"{source}:{target}"]
    for key, value in sorted(environment.items()):
        command += ["-e", f"{key}={value}"]
    # nice inside the container: the docker CLI's priority does not reach the container's processes.
    return [*command, "--entrypoint", "nice", scenario.image, "-n", CONTAINER_NICE, "python3", "/skilleval/entry.py"]


def wait_for_host_load(
    limit: float = START_LOAD_LIMIT,
    *,
    load: Callable[[], float] = lambda: os.getloadavg()[0],
    sleep: Callable[[float], None] = time.sleep,
    poll_seconds: float = 30.0,
) -> None:
    """Block until the 1-minute load average is below ``limit``."""

    while (current := load()) >= limit:
        print(f"host load {current:.1f} >= {limit:g}; waiting to start the next run", file=sys.stderr, flush=True)
        sleep(poll_seconds)


def _fresh_task(scenario_dir: Path, destination: Path, budget_seconds: int) -> None:
    task = json.loads((scenario_dir / "task.json").read_text(encoding="utf-8"))
    deadline = datetime.now(UTC) + timedelta(seconds=budget_seconds)
    task["deadline"] = deadline.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    destination.write_text(json.dumps(task), encoding="utf-8")


def changed_paths(checkout: Path, base: str) -> list[str]:
    tracked = subprocess.run(
        ["git", "-C", str(checkout), "diff", "--name-only", base, "--"], capture_output=True, text=True, check=True
    ).stdout.split()
    untracked = subprocess.run(
        ["git", "-C", str(checkout), "ls-files", "--others", "--exclude-standard"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    return sorted(set(tracked) | set(untracked))


def run_one(
    scenario_dir: Path,
    *,
    skills: Path,
    out: Path,
    label: str,
    repeat_index: int,
    toolchain_cache: Path,
    auth: Path,
    model: str,
    budget_seconds: int,
    extra_skills: tuple[str, ...] = (),
    replay: str = "on",
    code_root: Path | None = None,
    operator_binary: Path | None = None,
) -> RunMetrics:
    scenario = Scenario.load(scenario_dir)
    toolchain = prepare_toolchain(scenario.image, toolchain_cache, scenario.controller_image)
    run_dir = out / scenario.scenario_id / f"{label}-r{repeat_index}"
    if run_dir.exists():
        raise FileExistsError(f"run directory exists: {run_dir}")
    run_dir.mkdir(parents=True)
    for part in ("session", "checkout"):
        subprocess.run(["cp", "-a", str(scenario_dir / part), str(run_dir / part)], check=True)
    for part in ("tmp", "codex-homes", "out"):
        (run_dir / part).mkdir()
    (run_dir / "tmp" / "home").mkdir()
    _fresh_task(scenario_dir, run_dir / "task.json", budget_seconds)
    name = f"skilleval-{scenario.scenario_id}-{label}-r{repeat_index}-{os.getpid()}"
    command = docker_command(
        scenario=scenario,
        run_dir=run_dir,
        skills=skills.resolve(),
        toolchain=toolchain,
        auth=auth,
        model=model,
        name=name,
        extra_skills=extra_skills,
        replay=replay,
        code_root=code_root,
        operator_binary=operator_binary,
    )
    (run_dir / "docker-command.json").write_text(json.dumps(command, indent=1), encoding="utf-8")
    (run_dir / "run-config.json").write_text(
        json.dumps(
            {
                "provider": scenario.provider,
                "model": model or scenario.model,
                "effort": scenario.effort,
                "replay": replay,
                "code_root": str(code_root) if code_root else None,
                "operator_binary": str(operator_binary) if operator_binary else None,
                "image": scenario.image,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    started = time.monotonic()
    capacity = ProviderCapacity(
        environment={
            **os.environ,
            "SDO_SESSION_DIR": str(run_dir / "session"),
            "SDO_PHASE": "reflection",
            "SDO_INCIDENT_ID": scenario.incident_id,
        }
    )
    admission = (
        capacity.opus_session(model or scenario.model)
        if code_root is None and scenario.provider == "claude" and os.environ.get("SDO_OPUS_POOL_DIR")
        else nullcontext()
    )
    try:
        with admission, (run_dir / "container.log").open("w", encoding="utf-8") as log:
            try:
                subprocess.run(
                    command, stdout=log, stderr=subprocess.STDOUT, timeout=budget_seconds + 1200, check=False
                )
            except subprocess.TimeoutExpired:
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
    finally:
        for credential in (run_dir / "session").rglob("*"):
            if credential.name in {"auth.json", ".credentials.json"} and credential.is_file():
                credential.unlink()
    wall = time.monotonic() - started
    metrics = collect(scenario_dir, run_dir, label=label, wall_seconds=wall)
    (run_dir / "metrics.json").write_text(json.dumps(metrics.to_json(), indent=2) + "\n", encoding="utf-8")
    shutil.rmtree(run_dir / "tmp", ignore_errors=True)
    shutil.rmtree(run_dir / "codex-homes", ignore_errors=True)
    return metrics


def remeasure(scenario_dir: Path, run_dir: Path, *, skills: Path, toolchain_cache: Path, auth: Path) -> RunMetrics:
    configuration = (
        json.loads((run_dir / "run-config.json").read_text()) if (run_dir / "run-config.json").is_file() else {}
    )
    """Rerun the memory check and replay of a finished run, without a model turn."""

    scenario = Scenario.load(scenario_dir)
    previous = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    for part in ("tmp", "codex-homes"):
        (run_dir / part).mkdir(exist_ok=True)
    (run_dir / "tmp" / "home").mkdir(exist_ok=True)
    command = docker_command(
        scenario=scenario,
        run_dir=run_dir,
        skills=skills.resolve(),
        toolchain=prepare_toolchain(scenario.image, toolchain_cache, scenario.controller_image),
        auth=auth,
        model=DEFAULT_MODEL,
        name=f"skilleval-post-{scenario.scenario_id}-{run_dir.name}-{os.getpid()}",
        post_only=True,
        replay=configuration.get("replay", "on"),
        code_root=Path(configuration["code_root"]) if configuration.get("code_root") else None,
        operator_binary=Path(configuration["operator_binary"]) if configuration.get("operator_binary") else None,
    )
    with (run_dir / "container-post.log").open("w", encoding="utf-8") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=1800, check=False)
    metrics = collect(scenario_dir, run_dir, label=previous["label"], wall_seconds=previous["wall_seconds"])
    (run_dir / "metrics.json").write_text(json.dumps(metrics.to_json(), indent=2) + "\n", encoding="utf-8")
    shutil.rmtree(run_dir / "tmp", ignore_errors=True)
    shutil.rmtree(run_dir / "codex-homes", ignore_errors=True)
    return metrics


def collect(scenario_dir: Path, run_dir: Path, *, label: str, wall_seconds: float) -> RunMetrics:
    scenario = Scenario.load(scenario_dir)
    task = json.loads((scenario_dir / "task.json").read_text(encoding="utf-8"))
    usage_path = run_dir / "session" / "reflection-usage.jsonl"
    records = (
        [json.loads(line) for line in usage_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if usage_path.is_file()
        else []
    )
    result_path = run_dir / "out" / "agent-result.json"
    result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.is_file() else {}
    summary_path = run_dir / "out" / "entry-summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else {}
    changed = changed_paths(run_dir / "checkout", scenario.outcome_commit)
    replay = summary.get("replay") or {}
    configuration = run_dir / "run-config.json"
    configured = json.loads(configuration.read_text()) if configuration.is_file() else {}
    model = configured.get("model", scenario.model)
    return RunMetrics(
        scenario=scenario.scenario_id,
        label=label,
        status=str(result.get("status") or "no_result"),
        failure_reason=str(result.get("failure_reason") or ""),
        wall_seconds=round(wall_seconds, 1),
        usage=(
            claude_usage(
                scenario_dir / "session" / "claude" / "projects",
                run_dir / "session" / "claude" / "projects",
                model=model,
                logged=Usage.from_lines(records),
            )
            if scenario.provider == "claude"
            else Usage.from_lines(records)
        ),
        memory_check_pass=memory_check_passed(summary.get("memory_check")),
        changed_paths=changed,
        disallowed_paths=disallowed_paths(changed),
        programs_written=programs_written(changed),
        hand_repairs=hand_repairs(task["request"]["outcome"]),
        decision=decision_kind(result),
        run_id=run_dir.name,
        reflection_seconds=summary.get("reflection_seconds"),
        replay_mode=configured.get("replay", "on"),
        replay_cases=list(summary.get("replay_cases") or []),
        replay_exit_code=replay.get("exit_code"),
        replay_rows=replay_verdicts(str(replay.get("stdout") or "")),
        check_replay=check_replay_notes(
            summary.get("memory_check"), list(summary.get("incident_cases") or summary.get("replay_cases") or [])
        ),
    )


def table(rows: list[dict[str, Any]]) -> str:
    header = (
        "| scenario | run | status | calls | input M (cached M) | output k | min | check "
        "| programs/hand repairs | replay cases passed | check: fault Match | only allowed files "
        "| raw M | weighted M | USD |"
    )
    lines = [header, "|" + "---|" * 15]
    for row in rows:
        usage = row["usage"]
        replay = f"{row['replay']['passed']}/{row['replay']['cases']}" if row["replay"]["cases"] else "-"
        check = row["check_open_match"]
        check_replay = f"{check['match']}/{check['cases']}" if check["cases"] else "-"
        weighted = format(usage["weighted_tokens"] / 1e6, ".3f") if usage.get("weighted_tokens") is not None else "-"
        lines.append(
            f"| {row['scenario']} | {row.get('run_id') or row['label']} | {row['status']} | {usage['model_calls']} | "
            f"{usage['input_tokens'] / 1e6:.1f} ({usage['cached_input_tokens'] / 1e6:.1f}) | "
            f"{usage['output_tokens'] / 1e3:.1f} | {(row.get('reflection_seconds') or row['wall_seconds']) / 60:.1f} | "
            f"{'-' if row['memory_check_pass'] is None else ('pass' if row['memory_check_pass'] else 'FAIL')} | "
            f"{len(row['programs_written'])}/{len(row['hand_repairs'])} | {replay} | {check_replay} | "
            f"{'yes' if not row['disallowed_paths'] else 'NO'} | "
            f"{(usage['input_tokens'] + usage['output_tokens']) / 1e6:.3f} | "
            f"{weighted} | "
            f"{format(usage['usd'], '.4f') if usage.get('usd') is not None else '-'} |"
        )
    return "\n".join(lines)


def credential_path(scenario: Scenario, override: Path | None) -> Path:
    """Select exactly one provider's existing credential file."""
    relative = ".claude/.credentials.json" if scenario.provider == "claude" else ".codex/auth.json"
    path = override or Path.home() / relative
    if not path.is_file():
        raise ValueError(f"provider credential file does not exist: {path}")
    return path.resolve()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--scenarios", type=Path, required=True, help="a scenario dir or a dir of scenario dirs")
    parser.add_argument("--only", action="append", default=[], help="scenario id to run (repeatable)")
    parser.add_argument("--skills", type=Path, required=True, help="skills root mounted over /opt/sdo/sdo/skills")
    parser.add_argument("--extra-skill", action="append", default=[], help="install this extra skill too")
    parser.add_argument("--label", required=True)
    parser.add_argument("--replay", choices=("on", "off"), default="on")
    parser.add_argument("--code-root", type=Path, help="frozen source overlay (sdo, libs, benchmarks)")
    parser.add_argument("--operator-binary", type=Path, help="operator built from --code-root")
    parser.add_argument(
        "--remeasure", action="store_true", help="rerun only the check and replay of finished runs under --out"
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--table-only", action="store_true", help="only rebuild the label's table under --out")
    parser.add_argument("--parallel", type=int, default=MAX_PARALLEL, help=f"at most {MAX_PARALLEL}")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--model", default="", help="override the scenario model")
    parser.add_argument("--budget-seconds", type=int, default=DEFAULT_BUDGET_SECONDS)
    parser.add_argument("--auth", type=Path, help="provider credential file; defaults to its CLI home")
    parser.add_argument(
        "--toolchain-cache", type=Path, default=Path("/mnt/data/shli/cache/skilleval-toolchain"), help="extracted Go"
    )
    args = parser.parse_args(argv)
    if (args.code_root is None) != (args.operator_binary is None):
        parser.error("--code-root and --operator-binary must be supplied together")
    if args.code_root is not None:
        args.code_root = args.code_root.resolve()
        args.operator_binary = args.operator_binary.resolve()
        if not args.operator_binary.is_file() or not all(
            (args.code_root / part).is_dir() for part in ("sdo", "libs", "benchmarks")
        ):
            parser.error("source overlay or operator binary is missing")
    if not 1 <= args.parallel <= MAX_PARALLEL:
        parser.error(f"--parallel must be between 1 and {MAX_PARALLEL} on this shared host")
    if not (args.skills / "sdo-memory" / "SKILL.md").is_file():
        parser.error(f"{args.skills} is not a skills root (no sdo-memory/SKILL.md)")
    if (args.scenarios / "scenario.json").is_file():
        scenarios = [args.scenarios]
    else:
        scenarios = sorted(path.parent for path in args.scenarios.glob("*/scenario.json"))
    if args.only:
        scenarios = [path for path in scenarios if path.name in set(args.only)]
    if not scenarios:
        parser.error("no scenarios selected")
    if not args.table_only:
        for path in scenarios:
            credential_path(Scenario.load(path), args.auth)
    for image in sorted({Scenario.load(path).image for path in scenarios} if not args.table_only else ()):
        scenario = next(Scenario.load(path) for path in scenarios if Scenario.load(path).image == image)
        prepare_toolchain(image, args.toolchain_cache, scenario.controller_image)
    jobs = [(path, index) for index in range(1, args.repeat + 1) for path in scenarios]
    if args.table_only:
        jobs = []
    if args.remeasure:
        jobs = [
            (path, int(run.name.rsplit("-r", 1)[1]))
            for path in scenarios
            for run in sorted((args.out / path.name).glob(f"{args.label}-r*"))
            if (run / "metrics.json").is_file()
        ]

    start_gate = threading.Lock()
    last_start = [0.0]

    def launch(job: tuple[Path, int]) -> dict[str, Any]:
        path, index = job
        with start_gate:
            # One start at a time: let the load average see the previous start, then require load1 < limit.
            time.sleep(max(0.0, last_start[0] + START_SETTLE_SECONDS - time.monotonic()))
            wait_for_host_load()
            last_start[0] = time.monotonic()
        try:
            if args.remeasure:
                metrics = remeasure(
                    path,
                    args.out / path.name / f"{args.label}-r{index}",
                    skills=args.skills,
                    toolchain_cache=args.toolchain_cache,
                    auth=credential_path(Scenario.load(path), args.auth),
                )
                row = metrics.to_json()
                print(json.dumps({key: row[key] for key in ("scenario", "label", "status")}), flush=True)
                return row
            metrics = run_one(
                path,
                skills=args.skills,
                out=args.out,
                label=args.label,
                repeat_index=index,
                toolchain_cache=args.toolchain_cache,
                auth=credential_path(Scenario.load(path), args.auth),
                model=args.model,
                budget_seconds=args.budget_seconds,
                extra_skills=tuple(args.extra_skill),
                replay=args.replay,
                code_root=args.code_root,
                operator_binary=args.operator_binary,
            )
        except (OSError, ValueError, subprocess.CalledProcessError) as exc:
            print(f"{path.name} r{index}: {exc}", file=sys.stderr)
            return {}
        row = metrics.to_json()
        print(json.dumps({key: row[key] for key in ("scenario", "label", "status", "wall_seconds")}), flush=True)
        return row

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        finished = [row for row in pool.map(launch, jobs) if row]
    args.out.mkdir(parents=True, exist_ok=True)
    # Tabulate every run of this label under --out, so a subset run extends the table.
    rows = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(args.out.glob(f"*/{args.label}-r*/metrics.json"))
    ]
    print(f"{len(finished)} of {len(jobs)} runs finished in this invocation", file=sys.stderr)
    report = args.out / f"{args.label}-summary.json"
    report.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    rendered = table(sorted(rows, key=lambda row: (row["scenario"], row["label"])))
    (args.out / f"{args.label}-table.md").write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
