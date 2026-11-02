#!/usr/bin/env python3
"""List typed SDK gaps from recorded outcomes and historical broker ledgers.

Writes one deduplicated JSON object per line. Exit 2 means unreadable or malformed
input; --fail-on-gap exits 1 when any gap exists. No cluster or model is used.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass
class RecordedGap:
    incident_id: str
    fault_class: str
    primitive: str
    reason: str
    scratch_script: str
    resources: list[dict[str, Any]] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)


def gaps_in(value: Any, incident: str = "") -> Iterator[RecordedGap]:
    if isinstance(value, dict):
        incident = str(value.get("incident_id", value.get("incidentId", incident)))
        gaps = value.get("sdk_gaps", value.get("sdkGaps", []))
        if not isinstance(gaps, list):
            raise ValueError("sdk_gaps must be a list")
        for gap in gaps:
            if not isinstance(gap, dict):
                raise ValueError("SDK gap must be an object")
            fields = {}
            for snake, camel in (
                ("fault_class", "faultClass"),
                ("primitive", "primitive"),
                ("reason", "reason"),
                ("scratch_script", "scratchScript"),
            ):
                text = gap.get(snake, gap.get(camel))
                if not isinstance(text, str) or not text.strip():
                    raise ValueError(f"SDK gap requires {snake}")
                fields[snake] = text
            resources = gap.get("resources", [])
            if not isinstance(resources, list) or any(not isinstance(ref, dict) for ref in resources):
                raise ValueError("SDK gap resources must be object references")
            yield RecordedGap(incident_id=incident, resources=resources, **fields)
        for key, item in value.items():
            if key not in ("sdk_gaps", "sdkGaps"):
                yield from gaps_in(item, incident)
    elif isinstance(value, list):
        for item in value:
            yield from gaps_in(item, incident)


def scan(paths: list[Path]) -> list[RecordedGap]:
    files: set[Path] = set()
    for path in paths:
        if path.is_dir():
            files.update(path.rglob("*.json"))
            files.update(path.rglob("*.jsonl"))
        elif path.is_file():
            files.add(path)
        else:
            raise ValueError(f"input does not exist: {path}")
    found: dict[str, RecordedGap] = {}
    for path in sorted(files):
        try:
            text = path.read_text()
            values = (
                [json.loads(line) for line in text.splitlines() if line.strip()]
                if path.suffix == ".jsonl"
                else [json.loads(text)]
            )
            for value in values:
                for gap in gaps_in(value):
                    key = json.dumps(asdict(gap), sort_keys=True, separators=(",", ":"))
                    row = found.setdefault(key, gap)
                    source = str(path)
                    if source not in row.sources:
                        row.sources.append(source)
        except (OSError, ValueError) as exc:
            raise ValueError(f"{path}: {exc}") from exc
    return sorted(
        found.values(),
        key=lambda gap: (
            gap.incident_id,
            gap.primitive,
            gap.fault_class,
            gap.reason,
            gap.scratch_script,
            json.dumps(gap.resources, sort_keys=True),
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths", type=Path, nargs="+", help="Outcome/ledger files or directories (including legacy JSONL)."
    )
    parser.add_argument("--fail-on-gap", action="store_true", help="Exit 1 when any recorded SDK gap exists.")
    args = parser.parse_args()
    try:
        rows = scan(args.paths)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    for row in rows:
        print(json.dumps(asdict(row), sort_keys=True, separators=(",", ":")))
    return int(args.fail_on_gap and bool(rows))


if __name__ == "__main__":
    raise SystemExit(main())
