"""Assurance CLI: ``uv run python -m benchmarks.sregym.assurance <command>``.

    scenarios  list the scripted scenarios
    run        run scenarios against a fast-loop environment (``fastloop up``) with the scripted images
    report     summarize assurance.jsonl files

Example::

    bash benchmarks/sregym/assurance/build_images.sh v0.1.0 assure
    uv run python -m benchmarks.sregym.fastloop up --run-dir ../assure-runs/c0 --seed <lifecycle seed> \\
        --cluster-prefix assure-c --worker-id 0
    uv run python -m benchmarks.sregym.assurance run --run-dir ../assure-runs/c0 --scenario first-repeat
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from benchmarks.sregym.assurance.harness import (
    ASSURANCE_FILENAME,
    AssuranceError,
    ScriptedAgent,
    prepare_offline_environment,
)
from benchmarks.sregym.assurance.scenarios import SCENARIOS, IncidentSpec
from benchmarks.sregym.fastloop.environment import FastloopEnvironment, Images

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

SCRIPTED_IMAGE_LABEL = "sdo.dev/scripted-codex"


def scripted_images(tag: str) -> Images:
    return Images(
        controller=f"sdo-controller:{tag}",
        responder=f"sdo-sregym-responder:{tag}",
        validator=f"sdo-detector-validator:{tag}",
    )


def require_scripted(images: Images) -> None:
    """Refuse to run unless the controller and responder images carry the scripted CLI instead of a model CLI."""

    for image in (images.controller, images.responder):
        completed = subprocess.run(
            ["docker", "image", "inspect", image, "--format", f'{{{{index .Config.Labels "{SCRIPTED_IMAGE_LABEL}"}}}}'],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0 or completed.stdout.strip() != "true":
            raise AssuranceError(f"{image} is not a scripted-codex image; build it with build_images.sh")


def load_images(cluster: str, images: Images) -> None:
    for image in (images.controller, images.responder, images.validator):
        subprocess.run(["kind", "load", "docker-image", "--name", cluster, image], check=True, capture_output=True)


def reset_to_seed(environment: FastloopEnvironment) -> str:
    """Stop the persistent controller and return the workspace to its seed commit (``origin/HEAD``).

    Scenario expectations (first encounter learns, repeat is warm) assume the
    lifecycle-only seed memory, so ``--fresh`` starts from it. The broker's
    ledgers of earlier incidents are removed with the controller they belonged to.
    """

    import shutil

    from benchmarks.sregym.adapter import KubectlClusterOps, teardown

    errors = teardown(environment.state_path, ops=KubectlClusterOps()) if environment.state_path.exists() else []
    if errors:
        raise AssuranceError(f"persistent controller teardown failed: {errors}")
    workspace = str(environment.workspace)
    subprocess.run(["git", "-C", workspace, "reset", "--quiet", "--hard", "origin/HEAD"], check=True)
    subprocess.run(["git", "-C", workspace, "clean", "--quiet", "-fdx", "--", ".sdo"], check=True)
    shutil.rmtree(environment.workspace / ".git" / "sdo-broker", ignore_errors=True)
    return subprocess.run(
        ["git", "-C", workspace, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def expand(names: list[str], repeat: int) -> tuple[IncidentSpec, ...]:
    specs: list[IncidentSpec] = []
    for _ in range(repeat):
        for name in names:
            if name not in SCENARIOS:
                raise SystemExit(f"unknown scenario {name!r}; known: {', '.join(sorted(SCENARIOS))}")
            specs.extend(SCENARIOS[name])
    return tuple(specs)


def _run(args: argparse.Namespace) -> int:
    import os

    from benchmarks.sregym.adapter import (
        KubectlClusterOps,
        RuntimeConfig,
        control_namespace_for,
        deployed_lifecycle,
        inspect_existing,
        native_install_config,
    )
    from benchmarks.sregym.fastloop.cli import DEPLOY_OP_TIMEOUT_SECONDS
    from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
    from benchmarks.sregym.fastloop.loop import LoopConfig, run_incidents
    from benchmarks.sregym.fastloop.sdo_agent import SdoAgentSettings, SdoPersistentAgent
    from benchmarks.sregym.fastloop.worker_client import SregymWorker, worker_argv

    environment = FastloopEnvironment.load(args.run_dir.resolve())
    images = scripted_images(args.tag)
    require_scripted(images)
    if not args.skip_image_load:
        load_images(environment.cluster, images)
    os.environ["KUBECONFIG"] = str(environment.kubeconfig)
    if args.fresh:
        logger.info("fresh start: workspace reset to seed %s", reset_to_seed(environment))
    original = prepare_offline_environment(environment.run_dir)
    specs = expand(args.scenario, args.repeat)
    run_id = args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{'+'.join(args.scenario)}"
    results_dir = environment.run_dir / "results" / run_id

    def reuse_only(context: object) -> bool:
        del context
        inspect_existing(native_install_config(runtime_config))
        return True

    runtime_config = RuntimeConfig(
        repository=environment.workspace,
        namespace=environment.namespace,
        application=environment.application,
        controller_image=images.controller,
        responder_image=images.responder,
        validator_image=images.validator,
        repository_pvc="sdo-application-repository",
        credentials_secret="sdo-agent-credentials",
        model="gpt-6-luna",
        timeout_seconds=args.timeout,
        repair_policy="recorded-actions",
        agent_provider="codex",
        reflection_session=args.reflection_session,
        controller_namespace=control_namespace_for(environment.namespace),
    )
    inner = SdoPersistentAgent(
        SdoAgentSettings(
            repository=environment.workspace,
            namespace=environment.namespace,
            application=environment.application,
            runtime_config=runtime_config,
            state_path=environment.state_path,
            results_dir=results_dir,
            kubeconfig=str(environment.kubeconfig),
            verification_timeout_seconds=float(args.verification_timeout),
        ),
        ops=KubectlClusterOps(),
        lifecycle_inputs=lambda: deployed_lifecycle(environment.namespace),
        run_lifecycle=reuse_only,
    )
    agent = ScriptedAgent(
        inner,
        specs,
        namespace=environment.namespace,
        cluster=environment.cluster,
        workspace=environment.workspace,
        run_dir=environment.run_dir,
        results_dir=results_dir,
    )
    config = LoopConfig(
        run_id=run_id,
        problems=tuple(spec.problem_id for spec in specs),
        incidents=len(specs),
        results_path=results_dir / "incidents.jsonl",
    )
    worker_env = {**environment.worker_process_env(), **original}
    with SregymWorker(
        worker_argv(environment.sregym_dir, private_tmp=environment.private_tmp),
        env=worker_env,
        log_path=environment.run_dir / "worker.log",
    ) as worker:
        worker.request(
            "deploy",
            problem_id=environment.deploy_problem,
            baseline_path=str(environment.baseline_path),
            timeout=DEPLOY_OP_TIMEOUT_SECONDS,
        )
        driver = SregymFaultDriver(worker, namespace=environment.namespace, health_timeout_seconds=300.0)
        run_incidents(agent, driver, config)
    records = agent.records
    print(format_report(records))
    print(f"results: {results_dir}")
    return 0 if records and all(record.passed for record in records) else 1


def format_report(records: Sequence[object]) -> str:
    lines = []
    for record in records:
        checks = getattr(record, "checks", [])
        failed = [check.name for check in checks if not check.ok]
        lines.append(
            f"{getattr(record, 'index', '?'):>3} {getattr(record, 'scenario', '?'):<28} "
            f"{'PASS' if not failed else 'FAIL'} checks={len(checks)} failed={failed} "
            f"chaos={getattr(record, 'chaos', None)} error={(getattr(record, 'resolve_error', None) or '')[:80]}"
        )
    return "\n".join(lines)


def _report(args: argparse.Namespace) -> int:
    rows = []
    for path in args.paths:
        files = [path] if path.is_file() else sorted(path.rglob(ASSURANCE_FILENAME))
        for file in files:
            rows.extend(json.loads(line) for line in file.read_text(encoding="utf-8").splitlines() if line.strip())
    for row in rows:
        failed = [check["name"] for check in row["checks"] if not check["ok"]]
        print(f"{row['index']:>3} {row['scenario']:<28} {'PASS' if row['passed'] else 'FAIL'} failed={failed}")
    return 0 if rows and all(row["passed"] for row in rows) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="assurance", description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("scenarios", help="list scripted scenarios")
    listing.set_defaults(handler=lambda _args: print("\n".join(sorted(SCENARIOS))) or 0)
    run = commands.add_parser("run", help="run scripted scenarios against a fast-loop environment")
    run.add_argument("--run-dir", type=Path, required=True)
    run.add_argument("--scenario", action="append", required=True, help="scenario name; repeat to chain several")
    run.add_argument("--repeat", type=int, default=1, help="run the scenario list this many times")
    run.add_argument("--tag", default="assure", help="scripted image tag (build_images.sh)")
    run.add_argument("--timeout", type=int, default=900, help="responder timeout in seconds")
    run.add_argument("--verification-timeout", type=int, default=1200, help="seconds to wait for a verified incident")
    run.add_argument("--reflection-session", choices=("resume", "fresh"), default="fresh")
    run.add_argument("--skip-image-load", action="store_true")
    run.add_argument(
        "--fresh", action="store_true", help="tear down the controller and reset the workspace to the seed first"
    )
    run.add_argument("--run-id", default=None)
    run.set_defaults(handler=_run)
    report = commands.add_parser("report", help="summarize assurance.jsonl files")
    report.add_argument("paths", type=Path, nargs="+")
    report.set_defaults(handler=_report)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    if getattr(args, "repeat", 1) < 1:
        raise SystemExit("--repeat must be positive")
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
