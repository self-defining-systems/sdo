"""Prototype: run each cell as one guarded ``fastloop run`` row on a warm lane (SDO and the vanilla arm).

A fastloop row already owns the lane work for one incident: row-start reset to the planned memory commit, deploy,
inject, the agent, the one-shot oracle, learning and fenced recovery. This adapter therefore implements the
``LaneBackend`` and ``ArmExecutor`` interfaces on top of it instead of re-implementing those steps:

- ``claim`` loads the lane's fastloop environment and, for an SDO cell, fetches the snapshot commit from the app's
  snapshot repository into the lane's accepted-memory workspace (the row-start wipe then reseeds at that commit);
- ``execute`` runs the row subprocess and maps its ``IncidentRecord`` to an ``ArmResult`` (injection time, page,
  agent end, recovery, tokens inputs, phase seconds from the row prelude and driver timings);
- ``grade`` returns the row's own final oracle verdict (read once, at the end of the agent's work);
- ``collect`` copies the row's results directory into the cell directory.

UNVERIFIED LIVE. Command flags mirror ``fastloop.sequence.execute_row``; the arm-specific flags (vanilla agent name,
provider, model) are settings. Page time, injection time and the phase mapping come from record fields that this module
reads but that no test here ran against a real row.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.eval.cell import Accounting, ArmResult, Injection, Lane, Verdict
from benchmarks.sregym.eval.health import ProbeResult, http_probe
from benchmarks.sregym.eval.results import TokenRow
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.records import IncidentRecord, TokenCounts, load_records
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import datetime

    from benchmarks.sregym.eval.cell import ArmContext
    from benchmarks.sregym.eval.plan import Cell

#: A page counts as SDO's own only when its incident opened this soon after injection (the fallback page is not built).
OWN_PAGE_WINDOW_SECONDS = 120.0


class RowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    code_root: Path
    #: Lane id -> its ``fastloop up`` run directory (holds ``fastloop.json``).
    lane_environments: dict[str, Path]
    #: App -> repository holding the memory snapshot commits (base, merged, own deltas).
    snapshot_repos: dict[str, Path] = Field(default_factory=dict)
    #: App -> user-journey URLs for the independent prober. Without URLs the prober only checks pods are Ready.
    probe_urls: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    agent_names: dict[str, str] = {"sdo": "sdo", "vanilla": "claudecode"}
    provider: str = "claude"
    model: str = "claude-sonnet-5-5"
    timeout_seconds: int = Field(default=1800, gt=0)
    detection_timeout_seconds: int = Field(default=300, gt=0)
    #: Extra ``fastloop run`` flags per arm, after the defaults.
    extra_args: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    #: ``coord`` binary and lease for ``lane_guard``; ``None`` runs the row without the lease guard.
    coord: Path | None = None
    lease: str = "le-hu"
    python: str = sys.executable


def run_id(stage: str, cell: Cell, attempt: int) -> str:
    base = re.sub(r"[^A-Za-z0-9_.-]", "-", f"{stage}-{cell.app}-{cell.pass_}-{cell.problem}")
    return f"{base}-a{attempt}"


def row_command(
    settings: RowSettings, environment: FastloopEnvironment, cell: Cell, row_id: str, stream: Path, planned: str | None
) -> list[str]:
    run = [
        "benchmarks.sregym.fastloop",
        "run",
        "--run-dir",
        str(environment.run_dir),
        "--run-id",
        row_id,
        "--stream",
        str(stream),
        "--agent",
        settings.agent_names[cell.arm],
        "--provider",
        settings.provider,
        "--model",
        settings.model,
        "--timeout",
        str(settings.timeout_seconds),
        "--detection-timeout",
        str(settings.detection_timeout_seconds),
        "--healthy-baseline",
        "--inject-before-resume",
        "--require-fault-tracking",
        "--require-full-cleanup",
        "--continue-on-error",
    ]
    if planned:
        run += ["--planned-commit", planned]
    run += list(settings.extra_args.get(cell.arm, ()))
    if settings.coord is None:
        return [settings.python, "-m", *run]
    return [
        settings.python,
        "-m",
        "benchmarks.sregym.fastloop.lane_guard",
        "--cluster",
        environment.cluster,
        "--kubeconfig",
        str(environment.kubeconfig),
        "--lease",
        settings.lease,
        "--coord",
        str(settings.coord),
        "--",
        *run,
    ]


def _epoch(value: datetime | None) -> float | None:
    return None if value is None else value.timestamp()


def map_record(record: IncidentRecord, cell: Cell, memory_head: str | None) -> ArmResult:
    """The arm-neutral view of a fastloop incident record."""
    injected = _epoch(record.injection_started_at)
    agent_end = (
        _epoch(record.responder_finished_at) or _epoch(record.resolved_at) or _epoch(record.mitigation_applied_at)
    )
    sdo = cell.arm == "sdo"
    page: float | None
    source: str
    if sdo:
        detected = _epoch(record.detected_at)
        if detected is not None and injected is not None and detected - injected <= OWN_PAGE_WINDOW_SECONDS:
            page, source = detected, "own"
        else:
            page, source = None, "none"
    else:
        page = _epoch(record.responder_started_at) or _epoch(record.dispatched_at) or injected
        source = "instruction"
    prelude = record.run_prelude_phase_seconds
    driver = record.driver_phase_timings_seconds
    phases = {
        "claim_reset": prelude.get("worker_start", 0.0) + prelude.get("baseline_and_driver_setup", 0.0),
        "deploy": prelude.get("worker_deploy", 0.0),
    }
    if "inject" in driver:
        phases["inject"] = driver["inject"]
    error = record.infra_error or record.error
    return ArmResult(
        paged_at=page,
        page_source=source,
        agent_ended_at=agent_end if agent_end is not None else (injected or 0.0),
        status="error" if record.infra_error else "completed",
        recovered_at=_epoch(record.resolved_at) if sdo else None,
        closed_at=_epoch(record.reflection_started_at) or _epoch(record.resolved_at) if sdo else None,
        resolution_path=record.resolution_path or ("vanilla" if not sdo else None),
        binding_sources=tuple(record.binding_sources),
        injected_at=injected,
        phase_seconds=phases,
        memory_commit_after=record.memory_after_commit or memory_head,
        error=error,
    )


def _tokens(counts: TokenCounts) -> TokenRow:
    return TokenRow(
        uncached_input=counts.input_tokens - counts.cached_input_tokens - counts.cache_write_input_tokens,
        cache_read=counts.cached_input_tokens,
        cache_write=counts.cache_write_input_tokens,
        output=counts.output_tokens,
    )


class RecordAccountant:
    """STOPGAP accounting from the incident record (responder online, reflection offline). Replace with the ledger
    accountant; ``accounting.json`` marks these numbers ``source: incident-record``."""

    def __init__(self, rows: Mapping[str, IncidentRecord]) -> None:
        self._rows = rows

    def account(self, cell: Cell, directory: Path, result: ArmResult | None) -> Accounting:
        record = self._rows.get(cell.id)
        if record is None:
            return Accounting(TokenRow(), TokenRow(), {"source": "none"})
        return Accounting(
            _tokens(record.responder_tokens),
            _tokens(record.reflection_tokens),
            {"source": "incident-record", "drift_tokens": record.drift_tokens.model_dump()},
        )


class TranscriptPresenceGate:
    """STOPGAP gate: refuses to grade a cell whose collected directory holds no agent transcript file at all."""

    def __init__(self, patterns: Sequence[str] = ("**/*.jsonl",)) -> None:
        self._patterns = patterns

    def check(self, cell: Cell, directory: Path) -> Any:
        from benchmarks.sregym.eval.cell import GateVerdict

        found = [p for pattern in self._patterns for p in directory.glob(pattern) if p.name not in {"probes.jsonl"}]
        has_transcript = any(p.name != "incidents.jsonl" for p in found)
        if has_transcript:
            return GateVerdict(True)
        return GateVerdict(False, ("no agent transcript collected (stopgap presence check)",))


class FastloopRowRunner:
    """``LaneBackend`` and ``ArmExecutor`` over fastloop rows."""

    def __init__(
        self,
        settings: RowSettings,
        stage: str,
        *,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        now: Callable[[], float] | None = None,
        import_memory: Callable[[str, str, Path, str], None] | None = None,
        head: Callable[[Path], str] | None = None,
    ) -> None:
        import time

        self.settings = settings
        self.stage = stage
        self._run = run
        #: ``(app, problem, workspace, commit)``: make a first try's accepted memory fetchable from the snapshot store.
        self._import_memory = import_memory
        self._head = head or _workspace_head
        self._now = now or time.time
        self._lock = threading.Lock()
        self._state: dict[str, dict[str, Any]] = {}
        self.records: dict[str, IncidentRecord] = {}

    # LaneBackend -------------------------------------------------------------------------------------------------

    def claim(self, cell: Cell, lane_id: str, snapshot_commit: str | None) -> Lane:
        directory = self.settings.lane_environments[lane_id]
        environment = FastloopEnvironment.load(directory)
        if cell.arm == "sdo" and snapshot_commit:
            self._fetch(cell.app, environment, snapshot_commit)
        with self._lock:
            self._state[lane_id] = {
                "environment": environment,
                "planned": snapshot_commit if cell.arm == "sdo" else None,
            }
        return Lane(
            id=lane_id,
            app=cell.app,
            info={
                "environment": str(directory),
                "cluster": environment.cluster,
                "namespace": environment.namespace,
                "kubeconfig": str(environment.kubeconfig),
            },
        )

    def _fetch(self, app: str, environment: FastloopEnvironment, commit: str) -> None:
        repo = self.settings.snapshot_repos.get(app)
        if repo is None:
            raise ValueError(f"eval: no snapshot repository configured for app {app}")
        upload = f"git -c safe.directory={repo.resolve()} -c safe.directory={repo.resolve() / '.git'} upload-pack"
        subprocess.run(
            [
                *host_git_command(environment.workspace, "fetch", "--no-tags", f"--upload-pack={upload}"),
                str(repo.resolve()),
                commit,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def deploy(self, lane: Lane) -> None:
        """A fastloop row deploys (or reuses) the app itself."""

    def probe(self, lane: Lane) -> Callable[[], ProbeResult]:
        urls = self.settings.probe_urls.get(lane.app)
        kubeconfig, namespace = Path(lane.info["kubeconfig"]), lane.info["namespace"]
        if urls and all(u.startswith("kube:") for u in urls):
            from benchmarks.sregym.eval.kubeprobe import kube_probe

            return kube_probe(tuple(urls), kubeconfig)
        if urls:
            return http_probe(urls)

        def pods_ready() -> ProbeResult:
            listing = json.loads(
                kubectl(["get", "pods", "-o", "json"], namespace=namespace, kubeconfig=kubeconfig).stdout
            )
            pods = [p for p in listing["items"] if not p["metadata"].get("deletionTimestamp")]
            unready = [
                p["metadata"]["name"]
                for p in pods
                if p["status"].get("phase") != "Succeeded"
                and not any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
            ]
            return ProbeResult(bool(pods) and not unready, f"unready: {','.join(unready[:5])}" if unready else "")

        return pods_ready

    def inject(self, lane: Lane, cell: Cell) -> Injection:
        """The row injects; this is a placeholder that ``execute`` replaces with the recorded injection time."""
        return Injection(at=self._now())

    def grade(self, lane: Lane, cell: Cell) -> Verdict:
        record = self.records.get(cell.id)
        if record is None or record.oracle is None:
            raise RuntimeError("eval: the row produced no oracle verdict")
        harm = (
            json.dumps([r.model_dump(mode="json") for r in record.fault_reversions], default=str)
            if record.fault_reversions
            else None
        )
        return Verdict(success=record.oracle.success, detail=record.oracle.model_dump_json(), harm=harm)

    def collect(self, lane: Lane, cell: Cell, directory: Path) -> None:
        state = self._state.get(lane.id, {})
        results = state.get("results")
        if results is not None and Path(results).is_dir():
            shutil.copytree(results, directory / "fastloop", dirs_exist_ok=True)
        record = self.records.get(cell.id)
        if record is not None and record.artifacts_dir and Path(record.artifacts_dir).is_dir():
            shutil.copytree(record.artifacts_dir, directory / "artifacts", dirs_exist_ok=True)

    def release(self, lane: Lane, *, reusable: bool) -> None:
        with self._lock:
            self._state.pop(lane.id, None)

    # ArmExecutor -------------------------------------------------------------------------------------------------

    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        with self._lock:
            state = self._state[lane.id]
        environment: FastloopEnvironment = state["environment"]
        attempt = len(list((environment.run_dir / "results").glob(f"{run_id(self.stage, cell, 0)[:-3]}-a*")))
        row_id = run_id(self.stage, cell, attempt)
        stream = directory / "stream.json"
        stream.write_text(
            json.dumps(
                {
                    "incidents": [cell.problem],
                    "diagnosis_grading": "off",
                    "capability_mode": "permissive",
                    "permissive_responder": True,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        command = row_command(self.settings, environment, cell, row_id, stream, state["planned"])
        (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n", encoding="utf-8")
        started = self._now()
        with (directory / "row.stdout.log").open("a") as out, (directory / "row.stderr.log").open("a") as err:
            completed = self._run(
                command,
                cwd=self.settings.code_root,
                stdout=out,
                stderr=err,
                check=False,
                timeout=context.config.cell_cap_seconds + 600,
            )
        results = environment.run_dir / "results" / row_id
        state["results"] = results
        (directory / "row-exit.json").write_text(
            json.dumps({"returncode": completed.returncode, "wall_seconds": self._now() - started}) + "\n",
            encoding="utf-8",
        )
        path = results / "incidents.jsonl"
        if not path.is_file():
            return ArmResult(
                paged_at=None,
                page_source="none",
                agent_ended_at=self._now(),
                status="error",
                error=f"fastloop row exited {completed.returncode} without results",
            )
        records = load_records(path)
        if len(records) != 1:
            raise RuntimeError(f"eval: expected one incident record, found {len(records)}")
        record = records[0]
        self.records[cell.id] = record
        head = self._head(environment.workspace) if cell.arm == "sdo" else None
        if head and self._import_memory is not None:
            self._import_memory(cell.app, cell.problem, environment.workspace, head)
        return map_record(record, cell, memory_head=head)


def _workspace_head(workspace: Path) -> str:
    from benchmarks.sregym.fastloop.parallel_memory import memory_head

    return memory_head(workspace)
