"""Conservative retention of our built images inside a delivered kind node.

History lives on the node, independent of run directories. A repository's first
observed generation keeps all pre-existing identities as a rollback baseline.
No operation here mutates the host Docker image store or build cache.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from typing import Any

_REPOSITORIES = frozenset(
    {"sdo-controller", "sdo-detector-validator", "sdo-responder", "sdo-sregym-responder", "sdo-script-tools"}
)
_STATE = "/var/lib/sdo-image-retention.json"


class ImageRetentionError(RuntimeError):
    """A cleanup failure retaining receipts for removals already completed."""

    def __init__(self, message: str, evidence: dict[str, Any]) -> None:
        super().__init__(message)
        self.evidence = evidence


def owned_repository(reference: str) -> str | None:
    """Exact build repositories only; a matching basename is insufficient."""
    name = reference.split("@", 1)[0].rsplit("/", 1)
    name[-1] = name[-1].split(":", 1)[0]
    repository = "/".join(name)
    for prefix in ("docker.io/library/", "library/"):
        if repository.startswith(prefix):
            repository = repository[len(prefix) :]
            break
    return repository if repository in _REPOSITORIES else None


def _alias_repository(reference: str) -> str | None:
    if repository := owned_repository(reference):
        return repository
    # Registry delivery uses a stable repository hash and a loopback endpoint.
    # Only the hashes of our exact build repositories grant deletion authority.
    parts = reference.split("@", 1)[0].rsplit("/", 1)
    parts[-1] = parts[-1].split(":", 1)[0]
    repository = "/".join(parts)
    for name in _REPOSITORIES:
        key = hashlib.sha256(("docker.io/library/" + name).encode()).hexdigest()[:16]
        if repository.startswith("127.0.0.1:") and repository.split("/", 1)[-1] == "sdo/" + key:
            return name
    return None


@dataclass(frozen=True)
class NodeImage:
    identity: str
    references: tuple[str, ...]
    size_bytes: int

    @classmethod
    def parse(cls, row: dict[str, Any]) -> NodeImage:
        identity = row["id"]
        references = tuple(row.get("repoTags", []) or []) + tuple(row.get("repoDigests", []) or [])
        size = int(row["size"])
        if not isinstance(identity, str) or not identity or size < 0 or not all(isinstance(r, str) for r in references):
            raise ValueError("eval: image retention inventory malformed")
        return cls(identity, references, size)


def _canonical_reference(reference: str) -> str:
    return reference.removeprefix("docker.io/").removeprefix("library/")


def _used(images: list[NodeImage], containers: list[dict[str, Any]]) -> set[str]:
    references: set[str] = set()
    for container in containers:
        reference = container["imageRef"]
        if not isinstance(reference, str) or not reference:
            raise ValueError("eval: image retention container identity unavailable")
        references.add(_canonical_reference(reference))
        name = container.get("image", {}).get("image")
        if name:
            references.add(_canonical_reference(name))
    return {
        image.identity
        for image in images
        if image.identity in references
        or references.intersection(_canonical_reference(ref) for ref in image.references)
        or references.intersection(ref.split("@", 1)[-1] for ref in image.references if "@" in ref)
    }


def _run(node: str, command: list[str], **kwargs: Any) -> str:
    return subprocess.run(
        ["docker", "exec", *(["-i"] if "input" in kwargs else []), node, *command],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
        **kwargs,
    ).stdout


def _free(node: str) -> int:
    return int(_run(node, ["df", "-B1", "--output=avail", "/var/lib/containerd"]).splitlines()[-1].strip())


def retain_node_images(node: str, delivered: list[str]) -> dict[str, Any]:
    """Commit delivered identities, then remove old unused exclusively owned IDs.

    A failed inventory or history read aborts before deletion. History is
    published before removals so an interrupted cleanup can safely be retried.
    """
    repositories = {repo for ref in delivered if (repo := owned_repository(ref))}
    if not repositories:
        return {"removed": [], "freed_bytes": 0}
    images = [NodeImage.parse(row) for row in json.loads(_run(node, ["crictl", "images", "-o", "json"]))["images"]]
    if len({image.identity for image in images}) != len(images):
        raise ValueError("eval: image retention inventory has duplicate image identity")
    try:
        history = json.loads(_run(node, ["cat", _STATE]))
    except subprocess.CalledProcessError as exc:
        if "No such file or directory" not in (exc.stderr or ""):
            raise
        history = {}
    if not isinstance(history, dict) or any(
        repo not in _REPOSITORIES
        or not isinstance(ids, list)
        or not ids
        or any(not isinstance(identity, str) or not identity for identity in ids)
        for repo, ids in history.items()
    ):
        raise ValueError("eval: image retention history malformed")
    used = _used(images, json.loads(_run(node, ["crictl", "ps", "-a", "-o", "json"]))["containers"])
    for repository in sorted(repositories):
        refs = {ref for ref in delivered if owned_repository(ref) == repository}
        matches = {
            image.identity
            for image in images
            if any(
                _canonical_reference(ref) == _canonical_reference(actual) for ref in refs for actual in image.references
            )
        }
        if len(matches) != 1:
            raise ValueError(f"eval: delivered image identity unavailable or ambiguous for {repository} on {node}")
        current = matches.pop()
        prior = history.get(repository)
        if prior is None:
            baseline = sorted(
                {
                    image.identity
                    for image in images
                    if any(_alias_repository(ref) == repository for ref in image.references)
                }
                - {current}
            )
            history[repository] = [current, *baseline]
        elif prior[0] != current:
            history[repository] = [current, prior[0]]
    protected = used | {identity for identities in history.values() for identity in identities}
    candidates = [
        image
        for image in images
        if image.identity not in protected
        and image.references
        and all(_alias_repository(ref) in history for ref in image.references)
    ]
    before = _free(node)
    _run(node, ["sh", "-c", f"cat > {_STATE}.tmp && mv {_STATE}.tmp {_STATE}"], input=json.dumps(history))
    evidence: dict[str, Any] = {"removed": [], "freed_bytes": 0, "free_before_bytes": before}
    try:
        for image in candidates:
            # Re-read immediately before removal; exited containers also retain images.
            if image.identity in _used(
                images, json.loads(_run(node, ["crictl", "ps", "-a", "-o", "json"]))["containers"]
            ):
                continue
            _run(node, ["crictl", "rmi", image.identity])
            evidence["removed"].append(
                {"id": image.identity, "references": list(image.references), "size_bytes": image.size_bytes}
            )
        evidence["free_after_bytes"] = _free(node)
        evidence["freed_bytes"] = max(0, evidence["free_after_bytes"] - before)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as exc:
        evidence["error"] = str(exc)
        try:
            evidence["free_after_bytes"] = _free(node)
            evidence["freed_bytes"] = max(0, evidence["free_after_bytes"] - before)
        except (OSError, subprocess.SubprocessError, ValueError, IndexError):
            evidence["freed_bytes"] = None
        raise ImageRetentionError(f"eval: image retention failed on {node}: {exc}", evidence) from exc
    return evidence


def retain_lane_images(cluster: str, delivered: list[str]) -> dict[str, Any]:
    """Collect evidence from each node discovered in this kind lane only."""
    if not any(owned_repository(ref) for ref in delivered):
        return {"nodes": {}, "freed_bytes": 0}
    nodes = subprocess.run(
        ["kind", "get", "nodes", "--name", cluster], check=True, capture_output=True, text=True, timeout=30
    ).stdout.splitlines()
    if not nodes:
        raise ValueError(f"eval: image retention found no kind nodes for {cluster}")
    evidence: dict[str, Any] = {"nodes": {}, "freed_bytes": 0}
    for node in nodes:
        try:
            evidence["nodes"][node] = retain_node_images(node, delivered)
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError, ImageRetentionError) as exc:
            evidence["nodes"][node] = exc.evidence if isinstance(exc, ImageRetentionError) else {"error": str(exc)}
            evidence["freed_bytes"] = sum(row.get("freed_bytes") or 0 for row in evidence["nodes"].values())
            raise ImageRetentionError(f"eval: image retention failed for lane {cluster}: {exc}", evidence) from exc
        evidence["freed_bytes"] += evidence["nodes"][node]["freed_bytes"]
    return evidence
