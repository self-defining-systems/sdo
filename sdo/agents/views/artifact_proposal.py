"""Logical agent-authored operational artifacts, independent of repository layout."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sdo.agents.views.models import ObjectRef  # noqa: TC001 - Pydantic resolves this annotation at runtime.


class ArtifactDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GoSourceDraft(ArtifactDraft):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*\.go$")
    content: str = Field(min_length=1)


class WatchDraft(ArtifactDraft):
    api_version: str = Field(min_length=1)
    kind: str = Field(min_length=1)


class TrafficSLODraft(ArtifactDraft):
    window: int | None = None
    min_samples: int | None = None
    max_age: str | None = None
    max_error_rate: float | None = None
    max_timeout_rate: float | None = None
    latency_percentile: int | None = None
    max_latency: str | None = None


class TrafficScenarioDraft(ArtifactDraft):
    id: str
    weight: int = 0
    slo: TrafficSLODraft | None = None


class TrafficLinkDraft(ArtifactDraft):
    from_: str = Field(alias="from")
    to: str
    port: int
    protocol: str = ""


class TrafficWorkloadDraft(ArtifactDraft):
    api_version: str
    kind: str
    name: str
    description: str = ""
    purpose: str
    arrival: str = ""
    rate_per_second: float = 0
    duration: str | None = None
    timeout: str = ""
    iteration_timeout: str = ""
    seed: int = 0
    slo: TrafficSLODraft | None = None
    scenarios: list[TrafficScenarioDraft] = Field(default_factory=list)
    links: list[TrafficLinkDraft] = Field(default_factory=list)
    interval: str | None = None
    failures: int | None = None


class ScriptDraft(ArtifactDraft):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*\.sh$")
    content: str = Field(min_length=1)


class TrafficGeneratorDraft(ArtifactDraft):
    id: str
    sources: list[GoSourceDraft] = Field(default_factory=list)


class CorpusBindingDraft(ArtifactDraft):
    name: str = Field(min_length=1)
    object: ObjectRef


class CorpusCaptureReferenceDraft(ArtifactDraft):
    incident_id: str = Field(min_length=1)
    capture_label: str = Field(pattern=r"^open(@(detect|settle|pre-apply))?$")
    open_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fixed_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    recordings_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    bindings: list[CorpusBindingDraft] = Field(default_factory=list)
    peer_bindings: list[CorpusBindingDraft] = Field(default_factory=list)
    root_cause_program_ids: list[str] = Field(default_factory=list)
    no_match_program_ids: list[str] = Field(default_factory=list)


class CorpusCaseDraft(ArtifactDraft):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    open_json: str = ""
    fixed_json: str = ""
    healthy_json: str = ""
    recordings_json: str = ""
    labels_json: str = ""
    reference: CorpusCaptureReferenceDraft | None = None

    @model_validator(mode="after")
    def require_one_source(self) -> CorpusCaseDraft:
        inline = all((self.open_json, self.fixed_json, self.healthy_json, self.recordings_json, self.labels_json))
        if (self.reference is not None) == inline or (
            self.reference is not None
            and any((self.open_json, self.fixed_json, self.healthy_json, self.recordings_json, self.labels_json))
        ):
            raise ValueError("corpus case requires exactly one inline body or capture reference")
        return self


class ProgramDraft(ArtifactDraft):
    sources: list[GoSourceDraft] = Field(min_length=1)
    corpus_cases: list[CorpusCaseDraft] = Field(min_length=1)


class LibraryDraft(ArtifactDraft):
    package: str = Field(min_length=1)
    sources: list[GoSourceDraft] = Field(min_length=1)


class DetectorDraft(ArtifactDraft):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]*$")
    detector_class: str = "incident"
    owner: str = "responder"
    constructor: str = "New"
    watches: list[WatchDraft] = Field(default_factory=list)
    interval: str = "30s"
    firing: int = 1
    clearing: int = 2
    severity: str = "warn"
    debounce: str = "500ms"
    possible_playbook_ids: list[str] = Field(default_factory=list)
    originating_commit: str = ""
    originating_incident: str = ""
    sources: list[GoSourceDraft] = Field(default_factory=list)


class PlaybookDraft(ArtifactDraft):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    owner: str = "responder"
    fault_class: str = Field(min_length=1)
    originating_incident: str = ""
    markdown: str = Field(min_length=1)
    scripts: list[ScriptDraft] = Field(default_factory=list)
    program: ProgramDraft | None = None


class OperationalArtifactDraft(ArtifactDraft):
    detectors: list[DetectorDraft] = Field(default_factory=list)
    playbooks: list[PlaybookDraft] = Field(default_factory=list)
    traffic_generators: list[TrafficGeneratorDraft] = Field(default_factory=list)
    traffic_workloads: list[TrafficWorkloadDraft] = Field(default_factory=list)
    libraries: list[LibraryDraft] = Field(default_factory=list)
    delete_detector_ids: list[str] = Field(default_factory=list)
    delete_playbook_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_mutation(self) -> OperationalArtifactDraft:
        if not (
            self.detectors
            or self.playbooks
            or self.traffic_generators
            or self.traffic_workloads
            or self.libraries
            or self.delete_detector_ids
            or self.delete_playbook_ids
        ):
            raise ValueError("artifact proposal must contain at least one logical mutation")
        return self

    def artifact_ids(self) -> list[str]:
        return [
            *(item.id for item in self.detectors),
            *(item.id for item in self.playbooks),
            *(item.id for item in self.traffic_generators),
            *(item.name for item in self.traffic_workloads),
            *self.delete_detector_ids,
            *self.delete_playbook_ids,
        ]
