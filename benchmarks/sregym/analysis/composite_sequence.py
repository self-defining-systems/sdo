"""Per-composite learning-curve table for a persistent-controller sequence.

For every run directory (one composite each) it reports, per fault, whether a learned (incident-class)
detector activated for the fault's component before the first dispatch, and how much operational
memory (incident detectors, playbooks) existed in the application workspace after each memory commit.

    uv run python -m benchmarks.sregym.analysis.composite_sequence <results-dir>...
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from benchmarks.sregym.fastloop.records import Injected, InjectionState, NotInjected
from benchmarks.sregym.faults.description import ProblemDescription

NETWORK_POLICY_PREFIX = "deny-all-"


def _targets(firing: dict[str, Any]) -> set[str]:
    names = {part for part in str(firing.get("fingerprint", "")).split("/") if part}
    for binding in (firing.get("parameter_bindings") or {}).values():
        if isinstance(binding, dict) and binding.get("name"):
            names.add(str(binding["name"]))
    return {name.removeprefix(NETWORK_POLICY_PREFIX) for name in names}


def first_activations(firings: list[dict[str, Any]], component: str) -> dict[str, dict[str, Any]]:
    """The first activation of a health and of a learned (non-health) detector for ``component``."""

    result: dict[str, dict[str, Any]] = {}
    for firing in firings:
        if firing.get("event") not in {"activated", "batched"} or component not in _targets(firing):
            continue
        kind = "health" if firing.get("detector_class") == "health" else "learned"
        result.setdefault(kind, firing)
    return result


def learned_before_dispatch(firings: list[dict[str, Any]], component: str) -> bool:
    learned = first_activations(firings, component).get("learned")
    return learned is not None and learned.get("dispatch_relation") in {"before_dispatch", "no_incident"}


def memory_counts(workspace: Path, revision: str = "HEAD") -> dict[str, int]:
    listing = subprocess.run(
        ["git", "-C", str(workspace), "ls-tree", "-r", "--name-only", revision, ".sdo"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    incident_detectors = {p.split("/")[4] for p in listing if p.startswith(".sdo/diagnostics/detectors/incidents/")}
    playbooks = {p.split("/")[2] for p in listing if p.startswith(".sdo/playbooks/") and p.count("/") >= 3}
    return {"incident_detectors": len(incident_detectors), "playbooks": len(playbooks)}


def load_firings(directory: Path, since: str | None = None) -> list[dict[str, Any]]:
    """Firings of one composite. The controller log is cumulative across a persistent sequence, so
    pass ``since`` (the composite's injection start) to drop earlier composites' firings."""

    out: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*/detector_firings.jsonl")):
        out += [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if since is not None:
        cutoff = _instant(since)
        out = [f for f in out if _instant(str(f.get("recorded_at", "1970-01-01T00:00:00Z"))) >= cutoff]
    return out


def _instant(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _injection_state(directory: Path) -> Injected | NotInjected | None:
    path = directory / "incidents.jsonl"
    if not path.is_file():
        return None
    row = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    event = row.get("injection")
    # Legacy timestamp columns are not proof of injection. Reconcile those rows
    # against the worker audit before using them in a learning curve.
    return TypeAdapter(InjectionState).validate_python(event) if event is not None else None


def main(argv: list[str]) -> int:
    for arg in argv:
        directory = Path(arg)
        reports = sorted(directory.glob("composite_*.json"))
        if not reports:
            continue
        problem = json.loads(reports[0].read_text(encoding="utf-8"))["problem_id"]
        injection = _injection_state(directory)
        if not isinstance(injection, Injected):
            status = (
                f"not_injected stage={injection.stage}"
                if isinstance(injection, NotInjected)
                else "injection_unverified"
            )
            print(f"{directory.name}\t{problem}\t{status}")
            continue
        firings = load_firings(directory, since=injection.started_at.isoformat())
        parts = []
        catalog_path = directory / "fault_descriptions.json"
        if not catalog_path.is_file():
            raise ValueError(f"{directory}: missing recorded fault_descriptions.json; cannot infer components")
        catalog = {
            key: ProblemDescription.model_validate(item)
            for key, item in json.loads(catalog_path.read_text(encoding="utf-8")).items()
        }
        for fault in (fault for key in problem.split("+") for fault in catalog[key].ordered_faults()):
            found = first_activations(firings, fault.component)
            learned = found.get("learned")
            if learned is None:
                state = "no"
            else:
                state = "before_dispatch" if learned_before_dispatch(firings, fault.component) else "after_dispatch"
            parts.append(f"{fault.name}: learned={state} health={'yes' if 'health' in found else 'no'}")
        print(f"{directory.name}\t{problem}\t" + "; ".join(parts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
