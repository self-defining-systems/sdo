"""Reconstruct encounter phases offline; overlapping rows must never be summed.

    uv run python -m benchmarks.sregym.analysis.encounter_timeline ENCOUNTER \\
        --incident-id INCIDENT --runtime EXPORT/sdo_runtime --workers ARM/workers

Reads only named evidence roots. Missing endpoints remain null, never zero.
Optional --receipt accepts the strict production receipt; --rollout accepts a
scoped provider transcript. No cluster, model, or operational-memory writes.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import shlex
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.analysis.incident_cost import _command_execution, _timestamp


@dataclass(frozen=True)
class Phase:
    name: str
    start: float | None
    end: float | None
    sources: tuple[str, ...] = ()
    note: str = ""
    elapsed_seconds: float | None = None

    @property
    def status(self) -> str:
        if self.elapsed_seconds is not None:
            return "complete" if math.isfinite(self.elapsed_seconds) and self.elapsed_seconds >= 0 else "invalid"
        if self.start is None and self.end is None:
            return "unknown"
        if self.start is None or self.end is None:
            return "incomplete"
        return "complete" if self.end >= self.start else "invalid"

    @property
    def seconds(self) -> float | None:
        if self.elapsed_seconds is not None:
            return self.elapsed_seconds if self.status == "complete" else None
        return round(self.end - self.start, 6) if self.status == "complete" else None

    def document(self) -> dict[str, Any]:
        return {**asdict(self), "status": self.status, "seconds": self.seconds}


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _lines(path: Path, warnings: list[str]) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    records = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            warnings.append(f"{path}:{number}: malformed JSONL record omitted")
            continue
        if not isinstance(record, dict):
            raise ValueError(f"{path}:{number}: expected a JSON object")
        records.append(record)
    return records


def load_evidence(
    encounter: Path,
    incident_id: str,
    *,
    runtime: Path | None = None,
    workers: Path | None = None,
    receipt: Path | None = None,
    rollouts: list[Path] | None = None,
    rows: Path | None = None,
    controller_log: Path | None = None,
) -> dict[str, Any]:
    """Load OP encounter exports without scanning application/vendor trees."""
    warnings: list[str] = []
    sources = [str(encounter / "audit.jsonl")]
    data: dict[str, Any] = {
        "incident_id": incident_id,
        "warnings": warnings,
        "sources": sources,
        "audit": _lines(encounter / "audit.jsonl", warnings),
        "lifecycle": [],
        "pods": [],
        "executions": [],
        "stage_timings": [],
    }
    if rows:
        sources.append(str(rows))
        matching = [row for row in _lines(rows, warnings) if row.get("incident_id") == incident_id]
        if matching:
            data["row"] = matching[-1]
    if controller_log:
        sources.append(str(controller_log))
        for line in controller_log.read_text(encoding="utf-8").splitlines():
            if "msg=validator_stage " not in line and " INFO validator_stage " not in line:
                continue
            try:
                fields = dict(part.split("=", 1) for part in shlex.split(line) if "=" in part)
                millis = int(fields["duration_ms"])
                data["stage_timings"].append(
                    {
                        "stage": fields["stage"],
                        "seconds": millis / 1000,
                        "job": fields["job"],
                        "recorded_at": line.split()[0],
                        "source": str(controller_log),
                    }
                )
            except (KeyError, ValueError):
                warnings.append(f"{controller_log}: malformed validator_stage omitted")
    if receipt:
        sources.append(str(receipt))
        data["receipt"] = _json(receipt)
        data["ledger"] = data["receipt"].get("native_ledger", {})
    ledger_path = encounter / "latest-native-ledger.json"
    if ledger_path.is_file() and not data.get("ledger"):
        sources.append(str(ledger_path))
        saved = _json(ledger_path)
        data["ledger"] = saved.get("ledger", saved)
        data["responder_job_name"] = saved.get("responder_job_name", "")
    if runtime:
        telemetry = runtime / "telemetry" if (runtime / "telemetry").is_dir() else runtime
        seen = set()
        for path in [telemetry / "lifecycle-events.jsonl.1", telemetry / "lifecycle-events.jsonl"]:
            sources.append(str(path))
            for event in _lines(path, warnings):
                identity = event.get("event_id") or (
                    event.get("incident_id"),
                    event.get("event"),
                    event.get("recorded_at"),
                )
                if identity not in seen:
                    data["lifecycle"].append(event)
                    seen.add(identity)
    if workers:
        for path in sorted(workers.glob("*.status.json")):
            sources.append(str(path))
            data["pods"].append(_json(path))
        for path in sorted(workers.glob("*.events.json")):
            sources.append(str(path))
            for event in _json(path).get("items", []):
                if event.get("reason") in {"Pulling", "Pulled"}:
                    data.setdefault("pod_events", []).append(event)
    # Stage output is trusted runner telemetry, not inferred from free text.
    for path in sorted((encounter / "fresh-start").glob("*validator*stdout.log")):
        sources.append(str(path))
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.fullmatch(r"SDO_VALIDATOR_STAGE:(.+)\t(\d+)", line)
            if match:
                data["stage_timings"].append({"stage": match[1], "seconds": int(match[2]) / 1000, "source": str(path)})
    for path in rollouts or []:
        sources.append(str(path))
        for record in _lines(path, warnings):
            payload = record.get("payload", {})
            item = payload.get("item", {})
            if (
                payload.get("type") == "item_completed"
                and item.get("type") == "McpToolCall"
                and item.get("server") == "sdo_reflection"
                and item.get("tool") == "check_memory"
            ):
                start, end = payload.get("started_at_ms"), payload.get("completed_at_ms")
                if all(isinstance(value, (int, float)) and math.isfinite(value) for value in (start, end)):
                    data["executions"].append({"start": start / 1000, "end": end / 1000, "source": str(path)})
            execution = _command_execution(record)
            if execution:
                command, start, end = execution
                # Require an invocation, not a read of documentation mentioning it.
                if re.search(r"(?:^|[;&|\n])\s*(?:\S*/)?sdo(?:-operator)?\s+memory\s+check(?:\s|$)", command):
                    data["executions"].append({"start": start, "end": end, "source": str(path)})
    grade = encounter / "sdo_incident_grade.json"
    if grade.is_file():
        data["grade"] = _json(grade)
        sources.append(str(grade))
    return data


def build_timeline(data: dict[str, Any]) -> list[Phase]:
    """Pure phase projection. Every duration requires two correlated clocks."""
    incident_id = data["incident_id"]
    ledger = data.get("ledger", {})
    if ledger and ledger.get("incident_id") != incident_id:
        raise ValueError("ledger incident does not match requested incident")
    timestamps = ledger.get("closure_phase_timestamps", {})
    sources = tuple(data.get("sources", []))
    lifecycle = {
        event["event"]: event for event in data.get("lifecycle", []) if event.get("incident_id") == incident_id
    }
    audit = {event["event"]: event for event in data.get("audit", [])}
    closed = lifecycle.get("incident_closed", {})
    closure = ledger.get("closure", {})

    def stamp(value: Any) -> float | None:
        return _timestamp(value) if value else None

    def event(name: str) -> float | None:
        return stamp(lifecycle.get(name, {}).get("recorded_at"))

    def clock(name: str) -> float | None:
        return stamp(timestamps.get(name))

    def phase(name: str, start: float | None, end: float | None, note: str = "") -> Phase:
        return Phase(name, start, end, sources, note)

    opened = event("incident_opened") or stamp(closure.get("detected_at"))
    completed = event("responder_completed") or stamp(closure.get("responder_completed_at"))
    health = stamp(closed.get("health_cleared_at")) or stamp(closure.get("health_cleared_at"))
    verified = event("incident_closed") or stamp(closure.get("verified_at"))
    dispatch = clock("reflection_dispatch") or stamp(ledger.get("reflection_dispatch_at"))
    done = clock("worker_result_written")
    injection = audit.get("injection_window", {})
    admission = audit.get("fresh_native_admission_verified", {})
    baseline = audit.get("pre_injection_baseline", {})
    responder_start = None
    pods = data.get("pods", [])
    job_name = data.get("responder_job_name")
    for pod in pods:
        metadata = pod.get("metadata", {})
        labels = metadata.get("labels", {})
        if job_name and job_name in {labels.get("job-name"), labels.get("batch.kubernetes.io/job-name")}:
            for container in pod.get("status", {}).get("containerStatuses", []):
                if container.get("name") == "agent":
                    state = container.get("state", {})
                    responder_start = stamp((state.get("terminated") or state.get("running") or {}).get("startedAt"))

    def stage_in_scope(stage: dict[str, Any]) -> bool:
        if not stage.get("recorded_at"):
            return True
        at = stamp(stage["recorded_at"])
        if at is None:
            return False
        setup_start = stamp(admission.get("install_invoked_at"))
        if setup_start is not None and opened is not None and setup_start <= at <= opened:
            return True
        ack = clock("closure_ack")
        return dispatch is not None and at >= dispatch and (ack is None or at <= ack)

    adapter_row = data.get("row", {})
    rows = [
        phase(
            "setup",
            stamp(admission.get("install_invoked_at")),
            stamp(baseline.get("finished_at")),
            "install to full pre-injection baseline; excludes prior preparation",
        ),
        phase("injection", stamp(injection.get("started_at")), stamp(injection.get("finished_at"))),
        phase(
            "injection_to_open",
            stamp(injection.get("finished_at")),
            opened,
            "injection END is the incident clock origin",
        ),
        phase(
            "open_to_responder_start",
            opened,
            responder_start,
            "actual agent container start; dispatch event can be backdated",
        ),
        phase("responder", responder_start, completed, "includes worker tools and model time; can overlap closure"),
        phase("closure_verify", health, verified, "health-clear to independent stable closure"),
        phase("outcome_commit", clock("closure_start"), clock("broker_commit")),
        phase(
            "reflection_worker", dispatch, done, "dispatch to result written; includes startup, models and local checks"
        ),
        phase("reflection_pickup", done, clock("worker_result_picked_up")),
        phase("validator_canaries", clock("worker_result_picked_up"), clock("network_canaries_passed")),
        phase(
            "validator_job",
            clock("validator_job_created"),
            clock("validator_job_finished"),
            "Job active is not proof the container started",
        ),
        phase("broker_commit", clock("validator_job_finished"), clock("reflection_commit")),
        phase(
            "controller_rebuild",
            None,
            None,
            "ledger relaunch/registry marks are both recorded AFTER successful startup; they cannot time the build",
        ),
        phase("rollout_ready", None, None, "requires a captured rollout start and readiness observation"),
    ]
    rows.append(
        phase(
            "closure_to_grade",
            verified,
            stamp(adapter_row.get("graded_at") or adapter_row.get("oracle_finished_at")),
            "includes waiting and oracle execution; not pure grading duration",
        )
    )
    for name in ["grade", "recovery", "reset"]:
        record = data.get(name, audit.get(name, {}))
        start, end = stamp(record.get("started_at")), stamp(record.get("finished_at"))
        elapsed = None
        if name == "recovery" and adapter_row.get("recovery_finished_at"):
            end = stamp(adapter_row["recovery_finished_at"])
            elapsed = adapter_row.get("fault_recovery_seconds")
        rows.append(Phase(name, start, end, sources, elapsed_seconds=elapsed))
    for group in ["driver_phase_timings_seconds", "fault_gate_timings_seconds", "run_prelude_phase_seconds"]:
        for name, duration in adapter_row.get(group, {}).items():
            rows.append(Phase(f"driver/{name}", None, None, sources, f"{group}; elapsed-only, may overlap", duration))
    if not data.get("executions"):
        rows.append(phase("reflection_memory_check", None, None, "no completed scoped command clocks captured"))
    memory_checks: set[tuple[float, float]] = set()
    for number, execution in enumerate(data.get("executions", []), 1):
        start, end = execution["start"], execution["end"]
        if (
            dispatch is not None
            and dispatch <= start <= end
            and (done is None or end <= done)
            and (start, end) not in memory_checks
        ):
            memory_checks.add((start, end))
            rows.append(
                Phase(
                    f"reflection_memory_check/{number}",
                    start,
                    end,
                    (execution["source"],),
                    "whole command envelope; nested in reflection",
                )
            )
    if data.get("executions") and not memory_checks:
        rows.append(phase("reflection_memory_check", None, None, "no completed command inside this reflection"))
    for pod in pods:
        metadata, status = pod.get("metadata", {}), pod.get("status", {})
        name = metadata.get("name", "")
        if not name.startswith("sdo-validator-"):
            continue
        created = stamp(metadata.get("creationTimestamp"))
        setup_start = stamp(admission.get("install_invoked_at"))
        if created is not None and setup_start is not None and created < setup_start:
            continue
        scheduled = next(
            (
                stamp(c.get("lastTransitionTime"))
                for c in status.get("conditions", [])
                if c.get("type") == "PodScheduled" and c.get("status") == "True"
            ),
            None,
        )
        rows.append(phase(f"validator_schedule/{name}", created, scheduled))
        for container in status.get("containerStatuses", []):
            state = container.get("state", {})
            running = state.get("terminated") or state.get("running") or {}
            rows.append(
                phase(
                    f"validator_container_start/{name}",
                    scheduled,
                    stamp(running.get("startedAt")),
                    "includes mounts, image pull and container creation",
                )
            )
        pull_events = [
            e
            for e in data.get("pod_events", [])
            if e.get("involvedObject", {}).get("uid") == metadata.get("uid") and metadata.get("uid")
        ]
        pulling = next(
            (stamp(e.get("firstTimestamp") or e.get("eventTime")) for e in pull_events if e["reason"] == "Pulling"),
            None,
        )
        pulled = next(
            (stamp(e.get("firstTimestamp") or e.get("eventTime")) for e in pull_events if e["reason"] == "Pulled"), None
        )
        rows.append(
            phase(f"validator_image_pull/{name}", pulling, pulled, "absent pull events do not imply a zero-cost pull")
        )
    rows.extend(
        Phase(
            f"validator_stage/{stage.get('job', 'runner')}/{stage['stage']}",
            None,
            None,
            (stage["source"],),
            "elapsed-only clock, not a wall-clock position; tests include replay",
            elapsed_seconds=stage["seconds"],
        )
        for stage in data.get("stage_timings", [])
        if stage_in_scope(stage)
    )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("encounter", type=Path)
    parser.add_argument("--incident-id", required=True)
    parser.add_argument("--runtime", type=Path)
    parser.add_argument("--workers", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--rows", type=Path, help="fastloop incidents.jsonl")
    parser.add_argument("--controller-log", type=Path, help="timestamped controller log with validator_stage records")
    parser.add_argument("--rollout", type=Path, action="append", default=[])
    args = parser.parse_args(argv)
    data = load_evidence(
        args.encounter,
        args.incident_id,
        runtime=args.runtime,
        workers=args.workers,
        receipt=args.receipt,
        rollouts=args.rollout,
        rows=args.rows,
        controller_log=args.controller_log,
    )
    print(
        json.dumps(
            {
                "schema_version": "sdo.encounter-timeline/v1",
                "incident_id": args.incident_id,
                "warnings": data["warnings"],
                "phases": [row.document() for row in build_timeline(data)],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
