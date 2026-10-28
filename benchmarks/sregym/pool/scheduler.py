"""The single run owner: places a problem stream's first+repeat pairs on registry lanes across hosts.

One process owns the pair journal (an exclusive flock for its lifetime). Lane
state lives only in the registry. Each tick finishes ended deploys and pairs,
then places waiting pairs. A pair runs as one fastloop parallel plan on its
lane's host; its native rows decide the next step with fastloop's own classes:
graded rows continue, ``never_injected_retry`` rows get a fresh attempt,
configured known classes continue without the repeat, anything else holds the
lane and the pair for inspection.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from collections import Counter
from pathlib import Path  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, model_validator

from benchmarks.sregym.fastloop.records import IncidentRecord
from benchmarks.sregym.fastloop.sequence import never_injected
from benchmarks.sregym.pool.endpoints import MemoryEndpoint, load_memory_endpoints
from benchmarks.sregym.pool.hosts import HostConfig  # noqa: TC001 - Pydantic resolves this at runtime.
from benchmarks.sregym.pool.placement import DEFAULT_LANE_CPUS, Deploy, HostRoom, Pair, Run, place
from benchmarks.sregym.pool.registry import Lane, LaneRegistry, RegistryError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import TracebackType

    from benchmarks.sregym.pool.hosts import Host

WORKER = ["{python}", "-m", "benchmarks.sregym.pool"]


class SchedulerBusy(RuntimeError):
    """Another process already owns this run's pair journal."""


class StreamItem(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    id: str = Field(min_length=1)
    app: str = Field(min_length=1)


def load_stream(path: Path) -> list[StreamItem]:
    """A stream directory with ``slot_map.json`` (the mixed10 streams) or a JSON list of ``{id, app}``."""
    source = path / "slot_map.json" if path.is_dir() else path
    items = [StreamItem.model_validate(item) for item in json.loads(source.read_text())]
    if not items:
        raise ValueError(f"pool: stream {path} is empty")
    if len({item.id for item in items}) != len(items):
        raise ValueError(f"pool: stream {path} repeats a problem; pairs already run each problem twice")
    return items


class PoolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,40}$")
    registry: Path
    state_dir: Path
    stream: Path
    #: Required: each stream app's memory repo and run branch (no default).
    memory_endpoints: Path
    #: Harness build label; lanes on another build are redeployed before use.
    build: str = Field(min_length=1)
    hosts: dict[str, HostConfig] = Field(min_length=1)
    lane_cpus: float = Field(default=DEFAULT_LANE_CPUS, gt=0)
    provider: Literal["codex", "claude"] = "codex"
    model: str = Field(default="gpt-6-luna", min_length=1)
    timeout_seconds: int = Field(default=2400, gt=0)
    diagnosis_grading: Literal["off", "deferred", "inline"] = "off"
    capability_mode: Literal["strict", "permissive"] = "permissive"
    #: Host commands that deploy ``{app}`` (problem ``{problem}``) into ``{run_dir}`` for lane ``{lane}``.
    prepare_steps: list[list[str]] = Field(min_length=1)
    #: Native ``infra_error`` values that end a pair without its repeat and keep the lane.
    known_continue: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=3, ge=1)
    status_seconds: float = Field(default=600, ge=0)
    _items: list[StreamItem] = PrivateAttr(default_factory=list)
    _endpoints: dict[str, MemoryEndpoint] = PrivateAttr(default_factory=dict)

    @model_validator(mode="after")
    def stream_and_memory(self) -> Self:
        if len({host.prefix for host in self.hosts.values()}) != len(self.hosts):
            raise ValueError("pool: host lane prefixes must be distinct")
        self._items = load_stream(self.stream)
        self._endpoints = load_memory_endpoints(self.memory_endpoints, {item.app for item in self._items})
        return self

    @property
    def items(self) -> list[StreamItem]:
        return self._items

    @property
    def endpoints(self) -> dict[str, MemoryEndpoint]:
        return self._endpoints


class PairSpec(BaseModel):
    """What one pair process runs on its lane's host."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: str
    lane: str
    problem: str
    app: str
    encounters: list[Literal["first", "repeat"]] = Field(min_length=1)
    run_dir: Path
    output: Path
    provider: Literal["codex", "claude"]
    model: str
    timeout_seconds: int
    diagnosis_grading: Literal["off", "deferred", "inline"]
    capability_mode: Literal["strict", "permissive"]
    memory: MemoryEndpoint


class DeploySpec(BaseModel):
    """What one deploy process runs on its lane's host; it must leave a ready record."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    lane: str
    app: str
    problem: str
    build: str
    run_dir: Path
    ready: Path
    steps: list[list[str]] = Field(min_length=1)
    memory: MemoryEndpoint


class Ready(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    app: str
    source_digest: str = Field(min_length=1)
    build: str
    kubeconfig: Path


class Attempt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt: int
    lane: str
    unit: str
    encounters: list[str]
    disposition: str
    detail: str = ""


class PairRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str
    slot: int
    problem: str
    app: str
    status: Literal["pending", "running", "done", "held"] = "pending"
    encounters: list[Literal["first", "repeat"]] = Field(default_factory=lambda: ["first", "repeat"])
    attempt: int = 0
    lane: str = ""
    unit: str = ""
    history: list[Attempt] = Field(default_factory=list)


class Journal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: str
    pairs: list[PairRecord]
    lanes_created: dict[str, int] = Field(default_factory=dict)
    deploys: int = 0


class Disposition(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["done", "retry", "known", "held"]
    remaining: list[Literal["first", "repeat"]] = Field(default_factory=list)
    detail: str = ""


def pair_disposition(
    text: str | None, problem: str, encounters: list[Literal["first", "repeat"]], known: list[str]
) -> Disposition:
    """Decide from native rows only; missing or uncertain evidence holds (never reinject blindly)."""
    try:
        rows = [IncidentRecord.model_validate_json(line) for line in (text or "").splitlines() if line.strip()]
    except ValidationError as error:
        return Disposition(kind="held", detail=f"unreadable native rows: {error}"[:500])
    for index, encounter in enumerate(encounters):
        if index >= len(rows):
            return Disposition(kind="held", detail=f"no native record for the {encounter} encounter")
        row = rows[index]
        if row.problem_id != problem:
            return Disposition(kind="held", detail=f"native row names {row.problem_id}, expected {problem}")
        if never_injected(row):
            return Disposition(kind="retry", remaining=encounters[index:], detail="never_injected_retry")
        if row.infra_error in known:
            return Disposition(kind="known", detail=str(row.infra_error))
        if row.infra_error or row.recovery_error:
            return Disposition(kind="held", detail=f"{encounter}: {row.infra_error or row.recovery_error}"[:500])
    return Disposition(kind="done")


class Scheduler:
    def __init__(
        self, config: PoolConfig, hosts: Mapping[str, Host], registry: LaneRegistry, post: Callable[[str], None]
    ) -> None:
        if set(hosts) != set(config.hosts):
            raise ValueError("pool: host connections must match the configured hosts")
        self.config, self.hosts, self.registry, self.post = config, hosts, registry, post
        self._lock: Any = None
        self._journal: Journal | None = None
        self._last_status = 0.0

    # -- ownership and journal -------------------------------------------------
    def __enter__(self) -> Self:
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        lock = (self.config.state_dir / "owner.lock").open("a")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.close()
            raise SchedulerBusy(f"pool: {self.config.state_dir} already has a running owner") from None
        self._lock = lock
        path = self.config.state_dir / "pairs.json"
        if path.exists():
            journal = Journal.model_validate_json(path.read_text())
            if journal.run_id != self.config.run_id or [p.problem for p in journal.pairs] != [
                item.id for item in self.config.items
            ]:
                raise ValueError("pool: the journal belongs to another run or stream; use a new state directory")
        else:
            journal = Journal(
                run_id=self.config.run_id,
                pairs=[
                    PairRecord(id=f"s{slot:03d}", slot=slot, problem=item.id, app=item.app)
                    for slot, item in enumerate(self.config.items)
                ],
            )
        self._journal = journal
        self._save()
        return self

    def __exit__(
        self, kind: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    @property
    def journal(self) -> Journal:
        if self._journal is None:
            raise RuntimeError("pool: use the scheduler as a context manager")
        return self._journal

    def _save(self) -> None:
        path = self.config.state_dir / "pairs.json"
        temporary = path.with_suffix(".tmp")
        with temporary.open("w") as handle:
            handle.write(self.journal.model_dump_json(indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)

    # -- paths on a lane's host ------------------------------------------------
    def _root(self, host: str) -> Path:
        return self.config.hosts[host].work_root / self.config.run_id

    def _spec(self, host: str, unit: str) -> Path:
        return self._root(host) / "specs" / f"{unit}.json"

    def _log(self, host: str, unit: str) -> Path:
        return self._root(host) / "logs" / f"{unit}.log"

    def _output(self, host: str, unit: str) -> Path:
        return self._root(host) / "pairs" / unit

    def _ready(self, host: str, unit: str) -> Path:
        return self._root(host) / "ready" / f"{unit}.json"

    def _argv(self, host: str, verb: str, unit: str) -> list[str]:
        config = self.config.hosts[host]
        values = {"python": str(config.python), "code_root": str(config.code_root)}
        return [part.format(**values) for part in WORKER] + [verb, "--spec", str(self._spec(host, unit))]

    # -- the loop --------------------------------------------------------------
    def run(self, *, sleep: Callable[[float], None] = time.sleep, max_ticks: int | None = None) -> dict[str, int]:
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            ticks += 1
            self.tick()
            if not any(pair.status in {"pending", "running"} for pair in self.journal.pairs):
                break
            sleep(5)
        self.status(force=True)
        return self.summary()

    def summary(self) -> dict[str, int]:
        counts = Counter(pair.status for pair in self.journal.pairs)
        return {status: counts.get(status, 0) for status in ("done", "held", "pending", "running")}

    def tick(self) -> None:
        for lane in self.registry.read().lanes:
            if lane.state == "preparing":
                self._finish_deploy(lane)
            elif lane.state == "busy":
                self._finish_pair(lane)
        lanes = self.registry.read().lanes
        rooms = [
            HostRoom(name, host.lease_cores(), host.load1(), self.config.hosts[name].max_load)
            for name, host in self.hosts.items()
        ]
        pending = [
            Pair(pair.id, pair.problem, pair.app, tuple(pair.encounters))
            for pair in self.journal.pairs
            if pair.status == "pending"
        ]
        for decision in place(pending, lanes, rooms, build=self.config.build, lane_cpus=self.config.lane_cpus):
            if isinstance(decision, Run):
                self._start_pair(decision)
            else:
                self._start_deploy(decision)
        self._save()
        self.status()

    def status(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_status < self.config.status_seconds:
            return
        self._last_status = now
        lanes = self.registry.read().lanes
        per_host = []
        for name in self.config.hosts:
            states = Counter(lane.state for lane in lanes if lane.host == name)
            apps = Counter(lane.app for lane in lanes if lane.host == name and lane.app)
            per_host.append(
                f"{name}=ready:{states['ready']},busy:{states['busy']},preparing:{states['preparing']},"
                f"held:{states['held']} apps={dict(sorted(apps.items()))}"
            )
        counts = self.summary()
        retries = sum(
            1
            for pair in self.journal.pairs
            for attempt in pair.history
            if attempt.disposition == "never_injected_retry"
        )
        held = [f"{lane.id}({lane.reason[:80]})" for lane in lanes if lane.state == "held"]
        self.post(
            f"STATUS run={self.config.run_id} build={self.config.build} pairs done={counts['done']}/"
            f"{len(self.journal.pairs)} running={counts['running']} pending={counts['pending']} held={counts['held']} "
            f"retries={retries} lanes {' '.join(per_host)} lane_holds={held}"
        )

    # -- deploys ---------------------------------------------------------------
    def _start_deploy(self, decision: Deploy) -> None:
        host = self.hosts[decision.host]
        config = self.config.hosts[decision.host]
        if not host.gate():
            return
        self.journal.deploys += 1
        if decision.lane is None:
            number = self.journal.lanes_created.get(decision.host, 0)
            self.journal.lanes_created[decision.host] = number + 1
            lane_id = f"{config.prefix}-l{number}"
            run_dir = self._root(decision.host) / "lanes" / lane_id
        else:
            lane_id = decision.lane
            run_dir = self.registry.read().lane(lane_id).run_dir
        unit = f"pool-{self.config.run_id}-{lane_id}-d{self.journal.deploys}"
        self._save()
        memory = self.config.endpoints[decision.pair.app]
        values = {
            "python": str(config.python),
            "code_root": str(config.code_root),
            "lane": lane_id,
            "app": decision.pair.app,
            "problem": decision.pair.problem,
            "run_dir": str(run_dir),
            "build": self.config.build,
            "lease": config.lease,
            "coord": str(config.coord),
            "repo": memory.repo,
            "branch": memory.branch,
        }
        spec = DeploySpec(
            lane=lane_id,
            app=decision.pair.app,
            problem=decision.pair.problem,
            build=self.config.build,
            run_dir=run_dir,
            ready=self._ready(decision.host, unit),
            steps=[[part.format(**values) for part in step] for step in self.config.prepare_steps],
            memory=memory,
        )
        host.write_text(self._spec(decision.host, unit), spec.model_dump_json(indent=2) + "\n")
        if decision.lane is None:
            self.registry.add(
                Lane(
                    id=lane_id,
                    host=decision.host,
                    run_dir=run_dir,
                    cpus=self.config.lane_cpus,
                    state="preparing",
                    work=unit,
                    app=decision.pair.app,
                    build=self.config.build,
                )
            )
        else:
            self.registry.transition(
                lane_id,
                expect={"ready"},
                state="preparing",
                work=unit,
                app=decision.pair.app,
                source_digest="",
                build=self.config.build,
            )
        host.spawn(unit, self._argv(decision.host, "prepare", unit), self._log(decision.host, unit))
        verb = "CREATE" if decision.lane is None else "REDEPLOY"
        self.post(
            f"{verb} {lane_id} on {decision.host} app={decision.pair.app} for {decision.pair.problem} unit={unit}"
        )

    def _finish_deploy(self, lane: Lane) -> None:
        host = self.hosts[lane.host]
        if host.active(lane.work):
            return
        text = host.read_text(self._ready(lane.host, lane.work))
        try:
            ready = Ready.model_validate_json(text or "")
            if (ready.app, ready.build) != (lane.app, lane.build):
                raise ValueError(f"ready record says {ready.app}/{ready.build}, expected {lane.app}/{lane.build}")
        except ValueError as error:
            reason = f"deploy {lane.work} ended without a matching ready record: {error}"[:400]
            self.registry.transition(lane.id, expect={"preparing"}, state="held", work="", reason=reason)
            self.post(f"CASE {lane.id} on {lane.host} held: {reason}; log {self._log(lane.host, lane.work)}")
            return
        self.registry.transition(
            lane.id,
            expect={"preparing"},
            state="ready",
            work="",
            source_digest=ready.source_digest,
            kubeconfig=ready.kubeconfig,
        )
        self.post(f"READY {lane.id} on {lane.host} app={ready.app} source={ready.source_digest} build={ready.build}")

    # -- pairs -----------------------------------------------------------------
    def _pair(self, pair_id: str) -> PairRecord:
        return next(pair for pair in self.journal.pairs if pair.id == pair_id)

    def _start_pair(self, decision: Run) -> None:
        lane = self.registry.read().lane(decision.lane)
        host = self.hosts[lane.host]
        if not host.gate():
            return
        record = self._pair(decision.pair.id)
        unit = f"pool-{self.config.run_id}-{record.id}-a{record.attempt}"
        spec = PairSpec(
            run_id=f"{self.config.run_id}-{record.id}-a{record.attempt}",
            lane=lane.id,
            problem=record.problem,
            app=record.app,
            encounters=record.encounters,
            run_dir=lane.run_dir,
            output=self._output(lane.host, unit),
            provider=self.config.provider,
            model=self.config.model,
            timeout_seconds=self.config.timeout_seconds,
            diagnosis_grading=self.config.diagnosis_grading,
            capability_mode=self.config.capability_mode,
            memory=self.config.endpoints[record.app],
        )
        host.write_text(self._spec(lane.host, unit), spec.model_dump_json(indent=2) + "\n")
        # Journal and registry first: a crash before spawn leaves no native rows, which holds (never reinjects).
        record.status, record.lane, record.unit = "running", lane.id, unit
        self._save()
        self.registry.transition(lane.id, expect={"ready"}, state="busy", work=unit)
        host.spawn(unit, self._argv(lane.host, "pair", unit), self._log(lane.host, unit))
        self.post(
            f"START {record.id} {record.problem} {'+'.join(record.encounters)} on {lane.id}@{lane.host} "
            f"attempt={record.attempt} memory={spec.memory.repo}@{spec.memory.branch}"
        )

    def _finish_pair(self, lane: Lane) -> None:
        host = self.hosts[lane.host]
        if host.active(lane.work):
            return
        record = next((pair for pair in self.journal.pairs if pair.unit == lane.work), None)
        if record is None:
            reason = f"busy with {lane.work}, which no pair of this run owns"
            self.registry.transition(lane.id, expect={"busy"}, state="held", work="", reason=reason)
            self.post(f"CASE {lane.id} held: {reason}")
            return
        text = host.read_text(self._output(lane.host, lane.work) / "incidents.jsonl")
        result = pair_disposition(text, record.problem, record.encounters, self.config.known_continue)
        if result.kind == "retry" and record.attempt + 1 >= self.config.max_attempts:
            result = Disposition(kind="held", detail=f"never injected after {record.attempt + 1} attempts")
        label = {"known": f"known:{result.detail}", "retry": "never_injected_retry"}.get(result.kind, result.kind)
        record.history.append(
            Attempt(
                attempt=record.attempt,
                lane=lane.id,
                unit=lane.work,
                encounters=list(record.encounters),
                disposition=label,
                detail=result.detail,
            )
        )
        where = f"{lane.id}@{lane.host} log={self._log(lane.host, lane.work)}"
        if result.kind == "held":
            record.status = "held"
            self._save()
            self.registry.transition(lane.id, expect={"busy"}, state="held", work="", reason=result.detail)
            self.post(f"CASE {record.id} {record.problem} held on {where}: {result.detail}")
            return
        if result.kind == "retry":
            record.status, record.encounters, record.attempt = "pending", result.remaining, record.attempt + 1
            self.post(f"RETRY {record.id} {record.problem} {'+'.join(result.remaining)} never injected; fresh attempt")
        elif result.kind == "known":
            record.status = "done"
            self.post(f"KNOWN CONTINUE {record.id} {record.problem} class={result.detail} on {where}; repeat skipped")
        else:
            record.status = "done"
            self.post(f"FINISHED {record.id} {record.problem} {'+'.join(record.encounters)} on {where}")
        record.unit = ""
        self._save()
        self.registry.transition(lane.id, expect={"busy"}, state="ready", work="")


def release_hold(registry: LaneRegistry, lane_id: str, note: str) -> Lane:
    """An operator's fix-and-continue: the lane was inspected and is usable again."""
    if not note:
        raise RegistryError("pool: releasing a hold requires a note naming the fix")
    return registry.transition(lane_id, expect={"held"}, state="ready", work="", reason="")
