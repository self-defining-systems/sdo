"""Per-incident records of the fast inner loop and their summary.

One JSON line per incident. Latencies are measured from the moment the loop
started injecting the fault, so they never go negative the way the conductor's
``fault_injected_at`` (stamped after injection returns) can. Reflection is a
separate column: it runs after independently verified recovery and is never
part of resolution time.
"""

from __future__ import annotations

import csv
import json
import os
import statistics
import tempfile
from datetime import datetime  # noqa: TC003 - Pydantic resolves this annotation at runtime.
from typing import TYPE_CHECKING, Any, Literal, get_args

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, computed_field, model_validator

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA_VERSION = "sdo.fastloop-incident/v1"
AgentName = Literal["sdo", "codex"]
#: ``composition`` grades a composite fault: it passes only when every fault's own oracle passes.
OracleKind = Literal["sregym-mitigation-oracle", "health-check", "composition"]


class _PersistedModel(BaseModel):
    """Serializes derived values for other tools, and ignores them when read back."""

    @model_validator(mode="before")
    @classmethod
    def _drop_computed_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {key: value for key, value in data.items() if key not in cls.model_computed_fields}
        return data


class TokenCounts(_PersistedModel):
    """Provider-reported token usage; ``input_tokens`` already includes cached input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    input_tokens: int = Field(default=0, ge=0)
    cached_input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @classmethod
    def from_usage(cls, usage: object) -> TokenCounts:
        if not isinstance(usage, dict):
            return cls()
        return cls(
            input_tokens=int(usage.get("input_tokens") or usage.get("inputTokens") or 0),
            cached_input_tokens=int(usage.get("cached_input_tokens") or usage.get("cachedInputTokens") or 0),
            output_tokens=int(usage.get("output_tokens") or usage.get("outputTokens") or 0),
        )


class OracleVerdict(BaseModel):
    """Deterministic post-incident check: the problem's own oracle, or a health check when it has none."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: OracleKind
    success: bool
    details: dict[str, Any] = Field(default_factory=dict)


class DiagnosisGrade(BaseModel):
    model_config = ConfigDict(extra="forbid")

    success: bool | None = None
    judge_model_id: str | None = None
    accuracy: float | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    judge_tokens: TokenCounts = Field(default_factory=TokenCounts)
    latency_seconds: float = Field(default=0.0, ge=0)
    error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


class IncidentRecord(_PersistedModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sdo.fastloop-incident/v1"] = SCHEMA_VERSION
    run_id: str = Field(min_length=1)
    index: int = Field(ge=0)
    agent: AgentName
    problem_id: str = Field(min_length=1)
    model: str
    injection_started_at: AwareDatetime
    injection_finished_at: AwareDatetime
    #: First detector firing (SDO only; the raw baseline has no detector).
    detected_at: AwareDatetime | None = None
    #: Last successful repair action (SDO) or agent exit (baseline).
    mitigation_applied_at: AwareDatetime | None = None
    #: Independently verified health (SDO) or agent exit (baseline).
    resolved_at: AwareDatetime | None = None
    responder_started_at: AwareDatetime | None = None
    responder_finished_at: AwareDatetime | None = None
    oracle_finished_at: AwareDatetime | None = None
    reflection_started_at: AwareDatetime | None = None
    reflection_finished_at: AwareDatetime | None = None
    rebuild_done_at: AwareDatetime | None = None
    recovery_finished_at: AwareDatetime | None = None
    oracle: OracleVerdict | None = None
    recovery_oracle: OracleVerdict | None = None
    diagnosis_grade: DiagnosisGrade | None = None
    diagnosis: str = ""
    mitigation: str = ""
    responder_tokens: TokenCounts = Field(default_factory=TokenCounts)
    drift_tokens: TokenCounts = Field(default_factory=TokenCounts)
    reflection_tokens: TokenCounts = Field(default_factory=TokenCounts)
    warm_path: bool | None = None
    match_reasons: list[str] = Field(default_factory=list)
    reflection_attempts: int | None = None
    reflection_skipped_reason: str | None = None
    resolution_path: str | None = None
    fast_path_programs: list[str] = Field(default_factory=list)
    closure_phase_seconds: dict[str, float] = Field(default_factory=dict)
    driver_phase_timings_seconds: dict[str, float] = Field(default_factory=dict)
    fault_gate_timings_seconds: dict[str, float] = Field(default_factory=dict)
    run_prelude_phase_seconds: dict[str, float] = Field(default_factory=dict)
    #: ``sdo_mitigated``, ``external_recovery`` or ``cleared_without_sdo_action`` from the validated strict
    #: receipt; ``None`` for the baseline or when no receipt validated.
    sdo_resolution: str | None = None
    #: Wall time from verified recovery until reflection and detector rollout were live.
    reflection_seconds: float | None = None
    baseline_gate_seconds: float | None = None
    #: One-time setup paid inside this incident (lifecycle and controller install), excluded from resolution.
    setup_seconds: float | None = None
    controller_installed: bool | None = None
    lifecycle_reused: bool | None = None
    #: ``validation-cache``, ``validator`` or ``workspace-attestation`` when the opt-in cache is on.
    lifecycle_validation_source: str | None = None
    #: Waiting for the previous incident's reflection before this one could start (part of the wall time).
    previous_reflection_drain_seconds: float | None = None
    fault_recovery_seconds: float | None = None
    cleanup_deleted_counts: dict[str, int] = Field(default_factory=dict)
    cleanup_restored: list[str] = Field(default_factory=list)
    cleanup_skipped_reason: str | None = None
    #: Wall time from the start of this incident to the loop being ready for the next one.
    incident_wall_seconds: float | None = None
    incident_id: str | None = None
    artifacts_dir: str | None = None
    program_diagnoses: int | None = None
    program_verdicts: dict[str, int] = Field(default_factory=dict)
    program_tool_calls: int | None = None
    tool_commands: dict[str, int] = Field(default_factory=dict)
    response_loops_used: int | None = None
    error: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def injection_to_detection_seconds(self) -> float | None:
        return _seconds(self.injection_started_at, self.detected_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def detection_to_mitigation_seconds(self) -> float | None:
        return _seconds(self.detected_at, self.mitigation_applied_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def injection_to_mitigation_seconds(self) -> float | None:
        return _seconds(self.injection_started_at, self.mitigation_applied_at)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def injection_to_resolution_seconds(self) -> float | None:
        return _seconds(self.injection_started_at, self.resolved_at)


def _csv_row(record: IncidentRecord) -> dict[str, Any]:
    row = record.model_dump(mode="json")
    for key, value in row.items():
        if isinstance(value, (dict, list)):
            row[key] = json.dumps(value, sort_keys=True)
    return row


def append_record(path: Path, record: IncidentRecord) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record.model_dump_json() + "\n")
    csv_path = path.with_suffix(".csv")
    row = _csv_row(record)
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        if handle.tell() == 0:
            writer.writeheader()
        writer.writerow(row)


def write_records(path: Path, records: list[IncidentRecord]) -> None:
    """Replace both result files after deferred grade updates are complete."""

    path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = path.with_suffix(".csv")
    for target in (path, csv_path):
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
        ) as handle:
            temporary = handle.name
            try:
                if target == path:
                    for record in records:
                        handle.write(record.model_dump_json() + "\n")
                elif records:
                    rows = [_csv_row(record) for record in records]
                    writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
            except BaseException:
                os.unlink(temporary)
                raise
        os.replace(temporary, target)


def load_records(path: Path) -> list[IncidentRecord]:
    records: list[IncidentRecord] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        records.append(IncidentRecord.model_validate_json(line))
    return records


class AgentSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    agent: AgentName
    incidents: int
    oracle_passed: int
    errors: int
    warm_path_fired: int
    median_incident_wall_seconds: float | None
    median_injection_to_detection_seconds: float | None
    median_detection_to_mitigation_seconds: float | None
    median_injection_to_mitigation_seconds: float | None
    median_injection_to_resolution_seconds: float | None
    median_reflection_seconds: float | None
    median_responder_tokens: float | None
    median_reflection_tokens: float | None


def _median(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def summarize(records: list[IncidentRecord]) -> list[AgentSummary]:
    rows: list[AgentSummary] = []
    present = {record.agent for record in records}
    agents: list[AgentName] = [agent for agent in get_args(AgentName) if agent in present]
    for agent in sorted(agents):
        selected = [record for record in records if record.agent == agent]
        rows.append(
            AgentSummary(
                agent=agent,
                incidents=len(selected),
                oracle_passed=sum(1 for record in selected if record.oracle is not None and record.oracle.success),
                errors=sum(1 for record in selected if record.error),
                warm_path_fired=sum(1 for record in selected if record.warm_path),
                median_incident_wall_seconds=_median([record.incident_wall_seconds for record in selected]),
                median_injection_to_detection_seconds=_median(
                    [record.injection_to_detection_seconds for record in selected]
                ),
                median_detection_to_mitigation_seconds=_median(
                    [record.detection_to_mitigation_seconds for record in selected]
                ),
                median_injection_to_mitigation_seconds=_median(
                    [record.injection_to_mitigation_seconds for record in selected]
                ),
                median_injection_to_resolution_seconds=_median(
                    [record.injection_to_resolution_seconds for record in selected]
                ),
                median_reflection_seconds=_median([record.reflection_seconds for record in selected]),
                median_responder_tokens=_median([float(record.responder_tokens.total_tokens) for record in selected]),
                median_reflection_tokens=_median(
                    [float(record.reflection_tokens.total_tokens) for record in selected if record.agent == "sdo"]
                ),
            )
        )
    return rows
