#!/usr/bin/env python3
"""One cost owner, two fixed PR contract jobs; all remaining work is nightly."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("go_cost_plan", Path(__file__).with_name("go_cost_plan.py"))
assert SPEC is not None
assert SPEC.loader is not None
costs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(costs)


def build(go: dict, python: dict, policy: dict) -> dict:
    items: dict[str, dict] = {}
    for scope, data in go.items():
        for name, packages in data["packages"].items():
            key = f"go:{scope}:{name}"
            items[key] = {
                "kind": "go",
                "scope": scope,
                "name": name,
                "packages": packages,
                "seconds": data["costs"][name],
            }
    for name, seconds in python["costs"].items():
        items["python:" + name] = {"kind": "python", "name": name, "seconds": seconds}
    seconds = {key: item["seconds"] for key, item in items.items()}
    # A test with no recorded cost must run (and so be measured) in the PR, and
    # so must the guards of memory validation, publication and compilation:
    # cheap filler never displaces them. Only measured cost sends work nightly.
    unmeasured = {f"go:{scope}:{name}" for scope, data in go.items() for name in data.get("unmeasured", [])}
    unmeasured |= {"python:" + name for name in python.get("unmeasured", [])}
    protected = [re.compile(pattern) for pattern in policy.get("protected_patterns", [])]
    priority = {
        key
        for key, item in items.items()
        if any(pattern.search(" ".join([key, *item.get("packages", [])])) for pattern in protected)
    }
    # Timing/restart contracts must run even when cheap filler fills the bins.
    # Reserve families, so the next regression needs no hand-maintained name.
    required_patterns = [re.compile(pattern) for pattern in policy.get("required_patterns", [])]
    mandatory = {key for key in items if any(pattern.search(key) for pattern in required_patterns)}
    for key in items:
        items[key]["priority"] = key in priority or key in unmeasured or key in mandatory
    bins, nightly = costs.pack(
        list(items),
        seconds,
        budget=policy["contract_work_seconds"],
        max_bins=policy["contract_jobs"],
        required=frozenset(unmeasured),
        mandatory=frozenset(mandatory),
        priority=frozenset(priority),
        priority_seconds=policy.get("protected_share", 0.0) * policy["contract_jobs"] * policy["contract_work_seconds"],
    )
    deferred_required = mandatory & set(nightly)
    if deferred_required:
        raise ValueError(f"required PR contracts exceed fixed budget: {sorted(deferred_required)}")
    assigned = [name for members in bins for name in members] + nightly
    if sorted(assigned) != sorted(items) or len(assigned) != len(set(assigned)):
        raise ValueError("every test must appear exactly once in PR or nightly")
    return {"bins": bins, "nightly": sorted(nightly), "items": items, "policy": policy, "go": go}


def nightly_bins(data: dict) -> list[list[str]]:
    """Replay every runtime contract nightly, plus deferred non-runtime work.

    A green PR subset cannot establish that the runtime package is green. Use
    the same compiled inventory and cost owner to cover it without adding PR
    jobs or maintaining another list of test names.
    """
    selected = sorted(
        set(data["nightly"])
        | {key for key, item in data["items"].items() if item["kind"] == "go" and item["scope"] == "runtime"}
    )
    seconds = {key: data["items"][key]["seconds"] for key in selected}
    groups, oversized = costs.pack(selected, seconds)
    return groups + [[key] for key in oversized] or [[]]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--nightly-manifest", type=Path)
    parser.add_argument("--nightly", action="store_true")
    args = parser.parse_args()
    data = build(
        json.loads(args.go.read_text()),
        json.loads(args.python.read_text()),
        json.loads((ROOT / "scripts/ci_policy.json").read_text()),
    )
    if args.nightly:
        data["bins"] = nightly_bins(data)
        print(
            "nightly_matrix="
            + json.dumps(
                {
                    "include": [
                        {
                            "bin": str(i),
                            "needs_uv": any(
                                data["items"][key]["kind"] == "python" or data["items"][key].get("scope") == "runtime"
                                for key in members
                            ),
                        }
                        for i, members in enumerate(data["bins"])
                    ]
                },
                separators=(",", ":"),
            )
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, sort_keys=True) + "\n")
    for index, members in enumerate(data["bins"]):
        print(f"contract-{index}: {len(members)} tests, {sum(data['items'][key]['seconds'] for key in members):.3f}s")
    print(f"Nightly: {len(data['nightly'])} tests; fixed PR contract jobs: {data['policy']['contract_jobs']}")
    if args.nightly_manifest:
        lines = [
            "# Deferred (nightly) test inventory",
            "",
            "Generated from compiled/collected inventories and measured costs; never a placement input.",
            "",
            "| Test | Estimated seconds | Reason |",
            "|---|---:|---|",
        ]
        for key in data["nightly"]:
            item = data["items"][key]
            reason = (
                "single test exceeds PR work budget"
                if item["seconds"] > data["policy"]["contract_work_seconds"]
                else "fixed PR work capacity; cheaper regression coverage takes priority"
            )
            if item.get("priority"):
                reason += " (protected guard beyond the protected share)"
            lines.append(f"| `{key}` | {item['seconds']:.3f} | {reason} |")
        args.nightly_manifest.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
