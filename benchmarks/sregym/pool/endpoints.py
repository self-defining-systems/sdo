"""Shared app memory endpoints written by the memory service owner; the pool only reads them."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path


class MemoryEndpoint(BaseModel):
    """One app's accepted memory: a private repository and this run's branch. Never a default."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    repo: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    branch: str = Field(min_length=1)

    @field_validator("branch")
    @classmethod
    def not_main(cls, value: str) -> str:
        if value in {"main", "master"} or value.startswith("refs/heads/main"):
            raise ValueError("shared memory never uses the main branch; set a run branch")
        return value


def endpoint_key(app: str) -> str:
    """Stream app ids use underscores (hotel_reservation); endpoint keys use hyphens."""
    return app.replace("_", "-")


def load_memory_endpoints(path: Path, apps: Iterable[str]) -> dict[str, MemoryEndpoint]:
    """Every app the run can deploy must name its repo and branch, or the run refuses to start."""
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise ValueError(f"pool: cannot read memory endpoints {path}: {error}") from error
    if not isinstance(document, dict):
        raise ValueError(f"pool: memory endpoints {path} must map app -> {{repo, branch}}")
    endpoints: dict[str, MemoryEndpoint] = {}
    for app in sorted(set(apps)):
        entry = document.get(endpoint_key(app), document.get(app))
        if entry is None:
            raise ValueError(f"pool: no memory endpoint for app {app} in {path}; set repo and branch")
        try:
            endpoints[app] = MemoryEndpoint.model_validate(entry)
        except ValidationError as error:
            raise ValueError(f"pool: memory endpoint for app {app} in {path} is invalid: {error}") from error
    return endpoints
