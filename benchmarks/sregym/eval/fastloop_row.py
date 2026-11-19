"""Prototype: run each cell as one guarded ``fastloop run`` row on a warm lane (SDO and the vanilla arm).

A fastloop row already owns the lane work for one incident: row-start reset to the planned memory commit, deploy,
inject, the agent, the one-shot oracle, learning and fenced recovery. This adapter therefore implements the
``LaneBackend`` and ``ArmExecutor`` interfaces on top of it instead of re-implementing those steps:

- ``claim`` loads the lane's fastloop environment and, for an SDO cell, fetches the snapshot commit from the app's
  snapshot repository into the lane's accepted-memory workspace (the row-start wipe then reseeds at that commit);
- ``execute`` starts the row subprocess and returns at the row's online end (the grade is frozen and, for SDO, the
  outcome is committed: the ``incidents.online`` checkpoint), mapping that partial ``IncidentRecord`` to an
  ``ArmResult`` (injection time, page, agent end, recovery, tokens inputs, phase seconds);
- ``grade`` returns the row's own oracle verdict (read once, at the end of the agent's work);
- ``collect`` copies the row's results directory into the cell directory (called at the row's end and again after
  the drain, so the closing turn's result is in the evidence);
- ``drain`` waits on the held lane for the same row process to finish its closing turn, end-of-row fault recovery
  and exit, or stops it at ``drain_timeout_seconds``, and reports the closing (``ClosingResult``).

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
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.sregym.adapter import kubectl
from benchmarks.sregym.eval.cell import Accounting, ArmResult, Injection, Lane, Verdict
from benchmarks.sregym.eval.closing import ClosingResult
from benchmarks.sregym.eval.health import ProbeResult, http_probe
from benchmarks.sregym.eval.results import TokenRow
from benchmarks.sregym.fastloop.environment import FastloopEnvironment
from benchmarks.sregym.fastloop.host_limits import apply_lane_cpu
from benchmarks.sregym.fastloop.injection_marker import read_injection_marker
from benchmarks.sregym.fastloop.lane_cgroup import DEFAULT_LANE_CPUS, resolve_cgroup_parent
from benchmarks.sregym.fastloop.records import IncidentRecord, TokenCounts, load_records, online_checkpoint_path
from benchmarks.sregym.repository_git import host_git_command

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import datetime

    from benchmarks.sregym.eval.cell import ArmContext
    from benchmarks.sregym.eval.plan import Cell

#: A page counts as SDO's own only when its incident opened this soon after injection; later, the fallback page
#: (``page.FallbackPageHook``, at ``EvalConfig.fallback_page_seconds``) is the clock.
OWN_PAGE_WINDOW_SECONDS = 120.0


#: Attempts after the first that a row gets when it is rejected before the fault is injected.
ADMISSION_RETRIES = 5
#: Pre-injection failures that say the host was slow or the lane not yet settled (not the application): SDO's install
#: window rejecting an incomplete observation, a no-fault oracle rehearsal that ran while a recreated app still
#: registered, a namespace stuck Terminating. Both arms retry them identically.
PRE_INJECTION_RETRY_MARKERS = ("SDO rejected onboarding", "no-fault rehearsal has unhealthy", "wipe could not remove")


#: Lane time the pre-injection retries of one cell may use, about one normal row: a failed attempt reinstalls SDO
#: from scratch (5-6 min), and six of them held a lane for 30-66 min while every later row waited.
ADMISSION_RETRY_BUDGET_SECONDS = 900


def _rejected_before_injection(results: Path) -> str | None:
    """The attempt's pre-injection rejection cause, when it is one of the retryable kinds; ``None`` otherwise.

    The cause is normalized (numbers dropped), so two rejections that differ only in their timings compare equal.
    """
    path = results / "incidents.jsonl"
    if not path.is_file():
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    except (OSError, ValueError, IndexError):
        return None
    injection = record.get("injection") or {}
    text = f"{record.get('error') or ''} {record.get('infra_error') or ''}"
    if (
        injection.get("state") == "not_injected"
        and injection.get("stage") == "pre_injection"
        and any(marker in text for marker in PRE_INJECTION_RETRY_MARKERS)
    ):
        return re.sub(r"\d+(\.\d+)?", "N", text.strip())
    return None


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
    #: How long a row may take after its online end (closing turn, its memory check, end-of-row recovery) before the
    #: drain stops it and reports ``not_drained``: ``closing_turn_timeout`` + ``memory_check_timeout`` + 120 s.
    drain_timeout_seconds: int = Field(default=1500, gt=0)
    poll_seconds: float = Field(default=1.0, gt=0)
    #: Both arms pay the harness fault recovery and cleanup at their row's end (an SDO row would otherwise defer it to
    #: the next claim). Turn it off only to reproduce a run that deferred it.
    end_of_row_recovery: bool = True
    #: Extra ``fastloop run`` flags per arm, after the defaults.
    extra_args: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    #: ``coord`` binary and lease for ``lane_guard``; ``None`` runs the row without the lease guard.
    coord: Path | None = None
    lease: str = "le-hu"
    python: str = sys.executable
    #: CPU cap of one lane (all its kind nodes together), re-applied with ``docker update`` at every claim so lanes
    #: created before the cap existed get it too. Memory and PIDs stay as ``fastloop up`` set them.
    lane_cpus: float = Field(default=float(DEFAULT_LANE_CPUS), gt=0)
    #: systemd slice the lane's node containers should live under: ``auto`` (``sdo-eval.slice`` when the host has it),
    #: ``none`` or a slice name. It applies when a lane is created; a claim only reports where the node is.
    lane_cgroup_parent: str = "auto"


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
    if settings.end_of_row_recovery:
        run.append("--end-of-row-recovery")
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


def map_record(
    record: IncidentRecord, cell: Cell, memory_head: str | None, fallback_page_at: float | None = None
) -> ArmResult:
    """The arm-neutral view of a fastloop incident record.

    ``fallback_page_at`` is when the harness paged SDO because it had no incident of its own; it is then the clock.
    """
    injected = _epoch(record.injection_started_at)
    agent_end = (
        _epoch(record.responder_finished_at) or _epoch(record.resolved_at) or _epoch(record.mitigation_applied_at)
    )
    sdo = cell.arm == "sdo"
    page: float | None
    source: str
    if sdo and fallback_page_at is not None:
        page, source = fallback_page_at, "fallback"
    elif sdo:
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


def _tokens(counts: TokenCounts, *, requests: bool = False) -> TokenRow:
    return TokenRow(
        uncached_input=counts.input_tokens - counts.cached_input_tokens - counts.cache_write_input_tokens,
        cache_read=counts.cached_input_tokens,
        cache_write=counts.cache_write_input_tokens,
        output=counts.output_tokens,
        requests=counts.model_requests if requests else None,
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
        start: Callable[..., Any] = subprocess.Popen,
        apply_cpu: Callable[..., None] | None = None,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        import_memory: Callable[[str, str, Path, str], None] | None = None,
        head: Callable[[Path], str] | None = None,
    ) -> None:
        self.settings = settings
        self.stage = stage
        self._start = start
        self._apply_cpu = apply_cpu or apply_lane_cpu
        self._sleep = sleep
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
        self._apply_cpu(
            environment.cluster,
            f"{self.settings.lane_cpus:g}",
            parent=resolve_cgroup_parent(self.settings.lane_cgroup_parent),
        )
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
                "application": environment.application,
                "description": environment.description,
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
        if urls:
            return http_probe(urls)
        kubeconfig, namespace = Path(lane.info["kubeconfig"]), lane.info["namespace"]

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

    def _record_attempt(
        self, directory: Path, row_id: str, started: float, cause: str | None, *, retried: bool
    ) -> None:
        """One line per attempt, so lane accounting and parking see in-cell rejections, not only the cell's end."""
        line = {"run_id": row_id, "started": started, "ended": self._now(), "cause": cause, "retried": retried}
        with (directory / "attempts.jsonl").open("a", encoding="utf-8") as attempts:
            attempts.write(json.dumps(line) + "\n")

    def execute(self, lane: Lane, cell: Cell, injection: Injection, directory: Path, context: ArmContext) -> ArmResult:
        """Start the row and return at its online end; the same process keeps running until ``drain``."""
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
        results = environment.run_dir / "results"
        completed: int | None = None
        first_started: float | None = None
        previous_cause: str | None = None
        for admission_try in range(1 + ADMISSION_RETRIES):
            attempt = len(list((environment.run_dir / "results").glob(f"{run_id(self.stage, cell, 0)[:-3]}-a*")))
            row_id = run_id(self.stage, cell, attempt)
            command = row_command(self.settings, environment, cell, row_id, stream, state["planned"])
            (directory / "command.json").write_text(json.dumps(command, indent=2) + "\n", encoding="utf-8")
            started = self._now()
            first_started = started if first_started is None else first_started
            out = (directory / "row.stdout.log").open("a")
            err = (directory / "row.stderr.log").open("a")
            process = self._start(command, cwd=self.settings.code_root, stdout=out, stderr=err)
            results = environment.run_dir / "results" / row_id
            state.update(
                {
                    "results": results,
                    "process": process,
                    "handles": (out, err),
                    "started": started,
                    "online_end": None,
                    "fallback_page_at": None,
                }
            )
            deadline = started + context.config.cell_cap_seconds + 600
            online = online_checkpoint_path(results / "incidents.jsonl", 0)
            while not online.is_file() and process.poll() is None:
                self._fallback_page(lane, cell, directory, state, context)
                if self._now() > deadline:
                    self._stop(process)
                    break
                self._sleep(self.settings.poll_seconds)
            if online.is_file():
                record = IncidentRecord.model_validate_json(online.read_text(encoding="utf-8"))
                self.records[cell.id] = record
                state["online_end"] = self._now()
                self._record_attempt(directory, row_id, started, None, retried=False)
                return map_record(record, cell, None, state["fallback_page_at"])
            completed = self._finish_process(directory, state)
            cause = _rejected_before_injection(results)
            retry = (
                admission_try < ADMISSION_RETRIES
                and cause is not None
                and cause != previous_cause
                and self._now() - first_started < ADMISSION_RETRY_BUDGET_SECONDS
            )
            self._record_attempt(directory, row_id, started, cause, retried=retry)
            if retry:
                # The fault was never injected: the install window or the clean-app rehearsal failed on a slow or
                # loaded host, not on the application. Both arms start the next attempt from a clean wipe, but only
                # while the cause is new and the cell has not used about one row of lane time: a repeated cause is
                # deterministic, and the row ends with it instead of holding the lane for another install.
                previous_cause = cause
                continue
            break
        # The row ended without an online end: an error before the grade, or a row that finished in one step.
        path = results / "incidents.jsonl"
        if not path.is_file():
            return ArmResult(
                paged_at=None,
                page_source="none",
                agent_ended_at=self._now(),
                status="error",
                error=f"fastloop row exited {completed} without results",
            )
        records = load_records(path)
        if len(records) != 1:
            raise RuntimeError(f"eval: expected one incident record, found {len(records)}")
        record = records[0]
        self.records[cell.id] = record
        head = self._head(environment.workspace) if cell.arm == "sdo" else None
        if head and self._import_memory is not None:
            self._import_memory(cell.app, cell.problem, environment.workspace, head)
        state["finished"] = True
        return map_record(record, cell, head, state["fallback_page_at"])

    def _fallback_page(
        self, lane: Lane, cell: Cell, directory: Path, state: dict[str, Any], context: ArmContext
    ) -> None:
        """Offer the page hook the injection time once the row has injected; record the page in ``page.json``.

        A failing page is recorded and retried at the next poll; it never stops the row.
        """
        if state["fallback_page_at"] is not None:
            return
        marker = read_injection_marker(state["results"])
        if marker is None or state.get("own_incident_at") is not None:
            return
        injected = marker.started_at
        try:
            paged_at = context.page_hook.page(lane, cell, injected, marker.known_incident_ids)
        except Exception as exc:
            (directory / "page.json").write_text(
                json.dumps({"injected_at": injected, "error": f"{type(exc).__name__}: {exc}"}) + "\n",
                encoding="utf-8",
            )
            return
        own_at = context.page_hook.own_incident_at(cell)
        if paged_at is None and own_at is not None:
            # SDO opened an incident of its own since injection: no fallback; the record's detection is the page.
            state["own_incident_at"] = own_at
            (directory / "page.json").write_text(
                json.dumps({"injected_at": injected, "decided_at": own_at, "source": "own"}) + "\n",
                encoding="utf-8",
            )
        if paged_at is not None:
            state["fallback_page_at"] = paged_at
            (directory / "page.json").write_text(
                json.dumps({"injected_at": injected, "paged_at": paged_at, "source": "fallback"}) + "\n",
                encoding="utf-8",
            )

    def _stop(self, process: Any) -> None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()

    def _finish_process(self, directory: Path, state: dict[str, Any]) -> int | None:
        """Record the row process's exit (waiting for it) and close its log handles."""
        process = state["process"]
        code = process.poll()
        if code is None:
            code = process.wait(timeout=30)
        for handle in state.get("handles", ()):
            handle.close()
        (directory / "row-exit.json").write_text(
            json.dumps({"returncode": code, "wall_seconds": self._now() - state["started"]}) + "\n", encoding="utf-8"
        )
        return code  # type: ignore[no-any-return]

    # ClosingDrain ------------------------------------------------------------------------------------------------

    def drain(self, lane: Lane, cell: Cell, directory: Path, context: ArmContext) -> ClosingResult:
        with self._lock:
            state = self._state.get(lane.id)
        if not state or "process" not in state:
            return ClosingResult(status="not_applicable")
        environment: FastloopEnvironment = state["environment"]
        process = state["process"]
        deadline = self._now() + self.settings.drain_timeout_seconds
        stopped = False
        while process.poll() is None:
            if self._now() >= deadline:
                self._stop(process)
                stopped = True
                break
            self._sleep(self.settings.poll_seconds)
        if state.get("finished"):
            code: int | None = json.loads((directory / "row-exit.json").read_text(encoding="utf-8"))["returncode"]
        else:
            code = self._finish_process(directory, state)
        record = self._final_record(state)
        if record is not None:
            self.records[cell.id] = record
        else:
            record = self.records.get(cell.id)
        incident_id = record.incident_id if record else None
        if stopped:
            return ClosingResult(
                status="not_drained",
                incident_id=incident_id,
                decision="not_drained",
                error=f"the row did not finish within {self.settings.drain_timeout_seconds}s of its online end",
            )
        if code != 0 or state.get("final_missing"):
            return ClosingResult(
                status="error", incident_id=incident_id, error=f"fastloop row exited {code} after its online end"
            )
        sdo = cell.arm == "sdo"
        head = self._head(environment.workspace) if sdo else None
        if head and self._import_memory is not None:
            self._import_memory(cell.app, cell.problem, environment.workspace, head)
        ledger = _ledger(record)
        decision = ledger.get("reflection_decision")
        decision = decision if isinstance(decision, dict) else {}
        planned = state.get("planned")
        return ClosingResult(
            status="drained",
            incident_id=incident_id,
            closing_s=record.reflection_seconds if record and sdo else None,
            recovery_s=record.fault_recovery_seconds if record else None,
            memory_commit_after=head,
            decision=decision.get("learning_decision") if sdo else None,
            failure_reason=decision.get("failure_reason") if sdo else None,
            learning=learning_delta(environment.workspace, planned, head) if sdo and head and planned else {},
            source_repair=_text(ledger.get("source_repair")),
            source_repair_uncaptured=_text(ledger.get("source_repair_uncaptured")),
            cleanup_turns_used=ledger.get("cleanup_turns_used")
            if isinstance(ledger.get("cleanup_turns_used"), int)
            else None,
            phases={k: str(v) for k, v in (ledger.get("closure_phase_timestamps") or {}).items()},
            recovery=_recovery(record),
            offline_fallback=_tokens(record.reflection_tokens, requests=True) if record and sdo else None,
        )

    def _final_record(self, state: dict[str, Any]) -> IncidentRecord | None:
        path = state["results"] / "incidents.jsonl"
        if not path.is_file():
            state["final_missing"] = True
            return None
        records = load_records(path)
        if len(records) != 1:
            state["final_missing"] = True
            return None
        return records[0]


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _ledger(record: IncidentRecord | None) -> dict[str, Any]:
    """The native ledger of the incident's receipt (strict, else SDO's own rejected one), ``{}`` when absent."""
    if record is None or not record.artifacts_dir:
        return {}
    directory = Path(record.artifacts_dir)
    for name, unwrap in (("sdo_production_receipt_strict.json", False), ("sdo_rejected_production_receipt.json", True)):
        path = directory / name
        if not path.is_file():
            continue
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if unwrap and isinstance(document, dict):
            document = document.get("receipt")
        ledger = document.get("native_ledger") if isinstance(document, dict) else None
        if isinstance(ledger, dict):
            return ledger
    return {}


def _recovery(record: IncidentRecord | None) -> dict[str, Any]:
    """What the end-of-row recovery did, the same fields for both arms."""
    if record is None:
        return {}
    return {
        "fault_recovery_seconds": record.fault_recovery_seconds,
        "full_cleanup_verified": record.full_cleanup_verified,
        "cleanup_passes_verified": record.cleanup_passes_verified,
        "cleanup_skipped_reason": record.cleanup_skipped_reason,
        "recovery_error": record.recovery_error,
    }


#: Where each kind of learning lives under the accepted memory, and what names a unit of it.
_LEARNING_ROOTS = {
    "playbooks": ".sdo/playbooks/",
    "detectors": ".sdo/diagnostics/detectors/learned/",
    "lib": ".sdo/lib/",
    "corpus": ".sdo/corpus/",
}


def learning_delta(workspace: Path, base: str, head: str) -> dict[str, Any]:
    """What the closing turn added to accepted memory between ``base`` and ``head``, by unit, plus the commit."""
    listing = subprocess.run(
        host_git_command(
            workspace, "diff", "--name-status", "--no-renames", base, head, "--", *_LEARNING_ROOTS.values()
        ),
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    delta: dict[str, Any] = {kind: {"added": set(), "modified": set(), "deleted": set()} for kind in _LEARNING_ROOTS}
    names = {"A": "added", "M": "modified", "D": "deleted"}
    for line in listing.splitlines():
        status, _, path = line.partition("\t")
        for kind, root in _LEARNING_ROOTS.items():
            if path.startswith(root) and status[:1] in names:
                unit = path[len(root) :].split("/", 1)[0]
                delta[kind][names[status[:1]]].add(unit)
    for kind in delta:
        delta[kind] = {change: sorted(units) for change, units in delta[kind].items()}
    return {**delta, "base": base, "commit": head}


def _workspace_head(workspace: Path) -> str:
    from benchmarks.sregym.fastloop.parallel_memory import memory_head

    return memory_head(workspace)
