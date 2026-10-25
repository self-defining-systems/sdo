"""Typed controller feedback rendered as the resumed responder's user turn."""

from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sdo.agents.prompt_templates import RenderedPrompt, named_prompt, render_prompt


class FindingResource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    api_version: str = ""
    kind: str = ""
    namespace: str = ""
    name: str = ""


class BrokenFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    primary_resource: FindingResource
    fingerprint: str
    detector_id: str
    rule_id: str
    summary: str
    evidence: str


class RepairReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action_id: str
    kind: str = ""
    target: str = ""
    resources: list[FindingResource] | None = None
    summary: str = ""
    details: str = ""
    started_at: str = ""
    completed_at: str = ""
    success: bool
    reversible: bool = False


class CompletedRepair(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["program-application", "recorded-action"]
    action_id: str
    application_id: str = ""
    program_ids: list[str] = Field(default_factory=list)
    summary: str
    rolled_back: bool = False
    error: str = ""
    receipt: RepairReceipt


class ContinuationFeedback(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt: int = Field(ge=1)
    healthy: bool
    summary: str
    error: str = ""
    still_broken: list[BrokenFinding]
    already_done: list[CompletedRepair]


def continuation_prompt(payload: str) -> RenderedPrompt:
    """Validate the transport boundary before rendering the factual lists."""
    ContinuationFeedback.model_validate_json(payload)
    return render_prompt(
        "responder",
        "continuation",
        part="continuation.instructions",
        feedback=named_prompt(
            "continuation.incident_data", json.dumps(json.loads(payload), ensure_ascii=False, indent=2)
        ),
    )
