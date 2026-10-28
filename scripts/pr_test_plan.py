#!/usr/bin/env python3
"""One cost owner, two fixed PR contract jobs; all remaining work is nightly."""

from __future__ import annotations

import argparse
import importlib.util
import json
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
    bins, nightly = costs.pack(
        list(items), seconds, budget=policy["contract_work_seconds"], max_bins=policy["contract_jobs"]
    )
    assigned = [name for members in bins for name in members] + nightly
    if sorted(assigned) != sorted(items) or len(assigned) != len(set(assigned)):
        raise ValueError("every test must appear exactly once in PR or nightly")
    return {"bins": bins, "nightly": sorted(nightly), "items": items, "policy": policy, "go": go}


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
        selected = data["nightly"]
        seconds = {key: data["items"][key]["seconds"] for key in selected}
        groups, oversized = costs.pack(selected, seconds)
        data["bins"] = groups + [[key] for key in oversized]
        if not data["bins"]:
            data["bins"] = [[]]
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
            "# CIROOT1 nightly test inventory",
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
            lines.append(f"| `{key}` | {item['seconds']:.3f} | {reason} |")
        args.nightly_manifest.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
