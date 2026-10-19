"""Benchmark-side client of ``sdo-operator memory ls|outcomes``.

Benchmarks inspect finished operational repositories. They must not recompute where the Go memory
layout stores artifacts (outcome file names, detector directories); they ask the CLI, whose output
is ProtoJSON of the generated ``MemoryListing`` and ``MemoryOutcomes`` messages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import execute_operator
from sdo.contracts import MemoryListing, MemoryOutcomes, OutcomeRecord, parse_json

if TYPE_CHECKING:
    from pathlib import Path

CLI_TIMEOUT_SECONDS = 120


def outcomes_arguments(root: Path | str) -> list[str]:
    """Arguments of ``sdo-operator`` that print every stored outcome of the repository at ``root``."""

    return ["memory", "outcomes", "--root", str(root), "--json"]


def listing_arguments(root: Path | str) -> list[str]:
    """Arguments of ``sdo-operator`` that list the artifacts of the repository at ``root``."""

    return ["memory", "ls", "--root", str(root), "--json"]


def parse_outcomes(text: str) -> list[OutcomeRecord]:
    """Outcome records from ``memory outcomes --json`` output."""

    parsed = parse_json(text, MemoryOutcomes())
    assert isinstance(parsed, MemoryOutcomes)
    return list(parsed.outcomes)


def parse_listing(text: str) -> MemoryListing:
    """The artifact listing from ``memory ls --json`` output."""

    parsed = parse_json(text, MemoryListing())
    assert isinstance(parsed, MemoryListing)
    return parsed


def read_outcomes(root: Path) -> list[OutcomeRecord]:
    """Every outcome record stored in the checkout at ``root``."""

    command, *arguments = outcomes_arguments(root)
    return parse_outcomes(execute_operator(command, arguments, timeout=CLI_TIMEOUT_SECONDS))


def read_listing(root: Path) -> MemoryListing:
    """Every artifact of the checkout at ``root``."""

    command, *arguments = listing_arguments(root)
    return parse_listing(execute_operator(command, arguments, timeout=CLI_TIMEOUT_SECONDS))


def artifact_ids(listing: MemoryListing, kind: str, *, detector_class: str | None = None) -> list[str]:
    """Logical IDs of one artifact kind, in listing order; detectors may be narrowed by class."""

    return [
        artifact.id
        for artifact in listing.artifacts
        if artifact.kind == kind and (detector_class is None or artifact.detector_class == detector_class)
    ]
