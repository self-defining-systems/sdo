"""Bind retained cluster configuration to an independently passing setup receipt."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.setup_proof import SCHEMA, SetupFingerprint, digest, validate_report

if TYPE_CHECKING:
    from pathlib import Path


def check_cluster_scope(directory: Path, current: SetupFingerprint) -> None:
    """App restoration cannot prove that an existing cluster-wide object is clean.

    Compare every cluster-scoped configuration identity against the sealed setup
    producer's inventory. Memory and application objects may advance; cluster
    additions, removals and edits all invalidate warm reuse. Never create a
    passing baseline from the current, potentially post-fault cluster.
    """
    try:
        name = json.loads((directory / "latest.json").read_text())["receipt"]
        if not isinstance(name, str) or "/" in name or "\\" in name:
            raise ValueError("invalid setup receipt path")
        path = directory / name
        document = json.loads(path.read_text())
        if path.stem != digest(document) or document["schema_version"] != SCHEMA:
            raise ValueError("invalid setup receipt digest")
        baseline = SetupFingerprint(**document["fingerprint"])
        validate_report(document["report"], baseline)
        if baseline.lane != current.lane:
            raise ValueError("setup receipt belongs to another lane")
        expected = {key: value for key, value in baseline.live_objects.items() if key.startswith("<cluster>/")}
        observed = {key: value for key, value in current.live_objects.items() if key.startswith("<cluster>/")}
        if not expected or expected != observed:
            raise ValueError("retained cluster configuration differs from its passing setup")
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise ValueError(
            "eval: warm lane requires unchanged cluster configuration and a sealed passing setup"
        ) from error
