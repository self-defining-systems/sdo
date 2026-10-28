"""Run reflection scenarios with a chosen skills directory and tabulate the results.

Example::

    uv run python -m benchmarks.sregym.skilleval.runner \\
        --scenarios /mnt/data/shli/sdo-exp/mixed10/skilleval/scenarios \\
        --skills sdo/skills --label baseline --repeat 2 --parallel 6 \\
        --out /mnt/data/shli/sdo-exp/mixed10/skilleval/runs

Each run copies the scenario, mounts it into the scenario's recorded responder
image exactly where the live reflection Job had it, mounts ``--skills`` over the
image's skills root, resumes the responder's Codex session with the recorded
reflection request (codex, model from the scenario's recorded configuration) and
records cost, the independent memory check, the diff and replay verdicts.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from benchmarks.sregym.skilleval.metrics import (
    RunMetrics,
    Usage,
    check_replay_notes,
    decision_kind,
    disallowed_paths,
    hand_repairs,
    memory_check_passed,
    programs_written,
    replay_verdicts,
)
from benchmarks.sregym.skilleval.scenario import Scenario

ENTRY = Path(__file__).with_name("container_entry.py")
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


def prepare_toolchain(image: str, cache: Path) -> Path:
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
                toolchain_image(image),
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
            "-w", "/opt/sdo/sdo/operator", "--entrypoint", "go", image,
            "build", "-o", "/out/sdo-replay", "./cmd/sdo-replay",
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
) -> list[str]:
    """The reflection Job's container, as one ``docker run``: same image, paths, user-visible env."""

    session = "/sdo/session"
    mounts = {
        f"{run_dir / 'session'}": session,
        f"{run_dir / 'session' / 'corpus'}": f"{session}/corpus:ro",
        f"{run_dir / 'checkout'}": scenario.checkout_path,
        f"{run_dir / 'task.json'}": "/sdo/request/agent-task.json:ro",
        f"{run_dir / 'tmp'}": "/tmp",
        f"{run_dir / 'codex-homes'}": "/sdo/codex-homes",
        f"{run_dir / 'out'}": "/skilleval/out",
        f"{auth}": "/sdo/credentials/auth.json:ro",
        f"{skills}": "/opt/sdo/sdo/skills:ro",
        f"{toolchain / 'go'}": "/usr/local/go:ro",
        f"{toolchain / 'cache'}": "/go:ro",
        f"{toolchain / 'bin'}": "/skilleval/bin:ro",
        f"{ENTRY}": "/skilleval/entry.py:ro",
    }
    if (run_dir / "session" / "api-history").is_dir():
        mounts[f"{run_dir / 'session' / 'api-history'}"] = f"{session}/api-history:ro"
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
        "SDO_AGENT_PROVIDER": "codex",
        "SDO_RESPONDER_MODEL": model,
        "SDO_REFLECTION_MODEL": model,
        "SDO_LIFECYCLE_MODEL": model,
        "SDO_DEPLOYMENT_MODEL": model,
        "SDO_CODE_PATH": "go",
        "SDO_AGENT_GUIDANCE": "go",
        "SDO_MEMORY_VALIDATION": "go",
        "SKILLEVAL_EXTRA_SKILLS": ",".join(extra_skills),
        "SKILLEVAL_POST_ONLY": "1" if post_only else "0",
    }
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
    return [*command, "--entrypoint", "python3", scenario.image, "/skilleval/entry.py"]


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
) -> RunMetrics:
    scenario = Scenario.load(scenario_dir)
    toolchain = prepare_toolchain(scenario.image, toolchain_cache)
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
    )
    (run_dir / "docker-command.json").write_text(json.dumps(command, indent=1), encoding="utf-8")
    started = time.monotonic()
    with (run_dir / "container.log").open("w", encoding="utf-8") as log:
        try:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=budget_seconds + 1200, check=False)
        except subprocess.TimeoutExpired:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
    wall = time.monotonic() - started
    metrics = collect(scenario_dir, run_dir, label=label, wall_seconds=wall)
    (run_dir / "metrics.json").write_text(json.dumps(metrics.to_json(), indent=2) + "\n", encoding="utf-8")
    shutil.rmtree(run_dir / "tmp", ignore_errors=True)
    shutil.rmtree(run_dir / "codex-homes", ignore_errors=True)
    return metrics


def remeasure(scenario_dir: Path, run_dir: Path, *, skills: Path, toolchain_cache: Path, auth: Path) -> RunMetrics:
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
        toolchain=prepare_toolchain(scenario.image, toolchain_cache),
        auth=auth,
        model=DEFAULT_MODEL,
        name=f"skilleval-post-{scenario.scenario_id}-{run_dir.name}-{os.getpid()}",
        post_only=True,
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
    return RunMetrics(
        scenario=scenario.scenario_id,
        label=label,
        status=str(result.get("status") or "no_result"),
        failure_reason=str(result.get("failure_reason") or ""),
        wall_seconds=round(wall_seconds, 1),
        usage=Usage.from_lines(records),
        memory_check_pass=memory_check_passed(summary.get("memory_check")),
        changed_paths=changed,
        disallowed_paths=disallowed_paths(changed),
        programs_written=programs_written(changed),
        hand_repairs=hand_repairs(task["request"]["outcome"]),
        decision=decision_kind(result),
        replay_cases=list(summary.get("replay_cases") or []),
        replay_exit_code=replay.get("exit_code"),
        replay_rows=replay_verdicts(str(replay.get("stdout") or "")),
        check_replay=check_replay_notes(
            summary.get("memory_check"), list(summary.get("incident_cases") or summary.get("replay_cases") or [])
        ),
    )


def table(rows: list[dict[str, Any]]) -> str:
    header = (
        "| scenario | label | status | calls | input M (cached M) | output k | min | check "
        "| programs/hand repairs | replay cases passed | check: fault Match | only allowed files |"
    )
    lines = [header, "|" + "---|" * 12]
    for row in rows:
        usage = row["usage"]
        replay = f"{row['replay']['passed']}/{row['replay']['cases']}" if row["replay"]["cases"] else "-"
        check = row["check_open_match"]
        check_replay = f"{check['match']}/{check['cases']}" if check["cases"] else "-"
        lines.append(
            f"| {row['scenario']} | {row['label']} | {row['status']} | {usage['model_calls']} | "
            f"{usage['input_tokens'] / 1e6:.1f} ({usage['cached_input_tokens'] / 1e6:.1f}) | "
            f"{usage['output_tokens'] / 1e3:.1f} | {row['wall_seconds'] / 60:.1f} | "
            f"{'-' if row['memory_check_pass'] is None else ('pass' if row['memory_check_pass'] else 'FAIL')} | "
            f"{len(row['programs_written'])}/{len(row['hand_repairs'])} | {replay} | {check_replay} | "
            f"{'yes' if not row['disallowed_paths'] else 'NO'} |"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--scenarios", type=Path, required=True, help="a scenario dir or a dir of scenario dirs")
    parser.add_argument("--only", action="append", default=[], help="scenario id to run (repeatable)")
    parser.add_argument("--skills", type=Path, required=True, help="skills root mounted over /opt/sdo/sdo/skills")
    parser.add_argument("--extra-skill", action="append", default=[], help="install this extra skill too")
    parser.add_argument("--label", required=True)
    parser.add_argument(
        "--remeasure", action="store_true", help="rerun only the check and replay of finished runs under --out"
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--table-only", action="store_true", help="only rebuild the label's table under --out")
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--budget-seconds", type=int, default=DEFAULT_BUDGET_SECONDS)
    parser.add_argument("--auth", type=Path, default=Path.home() / ".codex" / "auth.json")
    parser.add_argument(
        "--toolchain-cache", type=Path, default=Path("/mnt/data/shli/cache/skilleval-toolchain"), help="extracted Go"
    )
    args = parser.parse_args(argv)
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
    for image in sorted({Scenario.load(path).image for path in scenarios} if not args.table_only else ()):
        prepare_toolchain(image, args.toolchain_cache)
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

    def launch(job: tuple[Path, int]) -> dict[str, Any]:
        path, index = job
        try:
            if args.remeasure:
                metrics = remeasure(
                    path,
                    args.out / path.name / f"{args.label}-r{index}",
                    skills=args.skills,
                    toolchain_cache=args.toolchain_cache,
                    auth=args.auth,
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
                auth=args.auth,
                model=args.model,
                budget_seconds=args.budget_seconds,
                extra_skills=tuple(args.extra_skill),
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
