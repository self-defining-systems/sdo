"""Bind retained cluster configuration to an independently passing setup receipt."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from benchmarks.sregym.fastloop.setup_proof import SCHEMA, SetupFingerprint, digest, validate_report

if TYPE_CHECKING:
    from pathlib import Path


def check_cluster_scope(directory: Path, current: SetupFingerprint, application_namespace: str) -> None:
    """App restoration cannot prove that an existing cluster-wide object is clean.

    Compare cluster and retained infrastructure configuration against the sealed
    setup producer's inventory. Application and its controller objects may
    advance; additions, removals and edits in every other scope invalidate reuse. Never create a
    passing baseline from the current, potentially post-fault cluster.
    """
    if not application_namespace or "/" in application_namespace:
        raise ValueError("eval: warm scope requires the application namespace")
    mutable = {application_namespace, application_namespace + "-sdo"}

    def retained(snapshot: SetupFingerprint) -> dict[str, str]:
        return {key: value for key, value in snapshot.live_objects.items() if key.partition("/")[0] not in mutable}

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
        if any(getattr(baseline, key) != getattr(current, key) for key in ("code_revision", "images", "tools")):
            raise ValueError("retained runtime differs from its passing setup")
        expected, observed = retained(baseline), retained(current)
        if not any(key.startswith("<cluster>/") for key in expected) or expected != observed:
            raise ValueError("retained cluster configuration differs from its passing setup")
    except (OSError, ValueError, TypeError, KeyError) as error:
        raise ValueError(
            "eval: warm lane requires unchanged cluster configuration and a sealed passing setup"
        ) from error
