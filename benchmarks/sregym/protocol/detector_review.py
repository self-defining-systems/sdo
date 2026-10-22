"""The record of an incident the controller stopped for detector review.

A stage whose controller never verified its incident healthy has no strict
receipt. This record stands in for it, so the stage's cost and the reason it
stopped stay with the published run.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

DETECTOR_REVIEW_FILENAME = "sdo_detector_review.json"
DETECTOR_REVIEW_SCHEMA = "sdo.sregym-detector-review/v1"


class DetectorReviewRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sdo.sregym-detector-review/v1"] = DETECTOR_REVIEW_SCHEMA
    status: Literal["detector_review_required"] = "detector_review_required"
    incident_id: str = Field(min_length=1)
    reason: str
    #: Detectors with findings still active when the controller stopped.
    blocking_detectors: list[str] = Field(default_factory=list)
    #: The responder's token usage, as the controller reported it for the incident.
    usage: dict[str, Any] | None = None
