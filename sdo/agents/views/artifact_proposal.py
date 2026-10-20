"""Logical agent-authored operational artifacts, independent of repository layout."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ArtifactDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GoSourceDraft(ArtifactDraft):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*\.go$")
    content: str = Field(min_length=1)


class WatchDraft(ArtifactDraft):
    api_version: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    namespace: str = ""


class ScriptDraft(ArtifactDraft):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*\.sh$")
    content: str = Field(min_length=1)


class CorpusCaseDraft(ArtifactDraft):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    open_json: str = Field(min_length=1)
    fixed_json: str = Field(min_length=1)
    healthy_json: str = Field(min_length=1)
    recordings_json: str = Field(min_length=1)
    labels_json: str = Field(min_length=1)


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
    originating_commit: str = Field(min_length=1)
    originating_incident: str = Field(min_length=1)
    sources: list[GoSourceDraft] = Field(default_factory=list)


class PlaybookDraft(ArtifactDraft):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    owner: str = "responder"
    fault_class: str = Field(min_length=1)
    originating_incident: str = Field(min_length=1)
    markdown: str = Field(min_length=1)
    scripts: list[ScriptDraft] = Field(default_factory=list)
    program: ProgramDraft | None = None


class OperationalArtifactDraft(ArtifactDraft):
    detectors: list[DetectorDraft] = Field(default_factory=list)
    playbooks: list[PlaybookDraft] = Field(default_factory=list)
    libraries: list[LibraryDraft] = Field(default_factory=list)
    delete_detector_ids: list[str] = Field(default_factory=list)
    delete_playbook_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_mutation(self) -> OperationalArtifactDraft:
        if not (self.detectors or self.playbooks or self.delete_detector_ids or self.delete_playbook_ids):
            raise ValueError("artifact proposal must contain at least one logical mutation")
        return self

    def artifact_ids(self) -> list[str]:
        return [
            *(item.id for item in self.detectors),
            *(item.id for item in self.playbooks),
            *self.delete_detector_ids,
            *self.delete_playbook_ids,
        ]
