"""Apply one replay slot's accepted-source edits to a proposal checkout.

Hermetic test helper: it applies the accepted-source edit of each recorded restore/credentials
step to a proposal checkout, without a cluster or the ``sdo`` tool.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

import yaml

from tests.unit.benchmarks.sregym.adapter.replay_slots import SCRIPTED_REPLAY_SLOTS


def _edit_source(root: Path, source: dict[str, Any], *, credentials: bool) -> None:
    """Record the restored source object in the isolated proposal, never accepted Git.

    An explicit annotation preserves the restoration even when the accepted spec
    was already correct. Credential repairs also persist the Secret references,
    rather than publishing the rotated credential or reverting to the old value.
    """
    metadata = source["metadata"]
    for path in sorted((root / "kubernetes").rglob("*.y*ml")):
        if "templates" in path.parts or path.is_symlink():
            continue
        text = path.read_text(encoding="utf-8")
        chunks = re.split(r"(?m)^---[ \t]*\n", text)
        for index, chunk in enumerate(chunks):
            # Do not deserialize Secret payloads in a multi-document manifest.
            if re.search(r"(?m)^kind:\s*Secret\s*(?:#.*)?$", chunk) or re.search(r'"kind"\s*:\s*"Secret"', chunk):
                continue
            try:
                document = yaml.safe_load(chunk)
            except yaml.YAMLError:
                continue
            if not isinstance(document, dict) or document.get("kind") != source["kind"]:
                continue
            if document.get("metadata", {}).get("name") != metadata["name"]:
                continue
            annotations = document["metadata"].setdefault("annotations", {})
            annotations["sdo.dev/source-restoration"] = "accepted-source"
            if credentials:
                _credential_env(document)
            chunks[index] = yaml.safe_dump(document, sort_keys=False)
            path.write_text("---\n".join(chunks), encoding="utf-8")
            return
    raise ValueError(f"proposal has no editable source for {source['kind']}/{metadata['name']}")


def _credential_env(source: dict[str, Any]) -> None:
    for container in source["spec"]["template"]["spec"]["containers"]:
        # Only the owning application container consumes this DB connection.
        if container["name"] != source["metadata"]["name"]:
            continue
        env = container.setdefault("env", [])
        env[:] = [item for item in env if item["name"] != "DB_CONNECTION_STRING"]
        env.append(
            {
                "name": "DB_CONNECTION_STRING",
                "valueFrom": {"secretKeyRef": {"name": "product-catalog-db-conn", "key": "DB_CONNECTION_STRING"}},
            }
        )


def apply_slot_source_edits(slot: int, root: Path) -> list[str]:
    if type(slot) is not int or not 0 <= slot < len(SCRIPTED_REPLAY_SLOTS):
        raise ValueError("slot must be an integer from 0 through 9")
    edited = []
    for step in SCRIPTED_REPLAY_SLOTS[slot].steps:
        if step.operation not in {"restore", "credentials"}:
            continue
        _edit_source(
            root, {"kind": step.kind, "metadata": {"name": step.name}}, credentials=step.operation == "credentials"
        )
        edited.append(f"{step.kind}/{step.name}")
    return edited


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", required=True, type=int)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args()
    print(",".join(apply_slot_source_edits(args.slot, args.root)))


if __name__ == "__main__":
    main()
