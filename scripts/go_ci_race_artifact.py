#!/usr/bin/env python3
"""Transfer the current race test graph rather than a runner's compiler history."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Keep the consumer transfer comfortably inside its one-minute setup allocation.
MAX_ARCHIVE_BYTES = 320 * 1024 * 1024


def pack(modules: list[Path], cache: Path, output: Path, packages: dict[Path, list[str]] | None = None) -> None:
    cache = cache.resolve()
    exports: set[Path] = set()
    actions: dict[str, set[str]] = defaultdict(set)
    for module in modules:
        listed = subprocess.check_output(
            [
                "go",
                "list",
                "-race",
                "-test",
                "-deps",
                "-export",
                "-f",
                "{{if .Export}}{{.BuildID}} {{.Export}}{{end}}",
                *(packages[module] if packages is not None else ["./..."]),
            ],
            cwd=module,
            text=True,
        )
        for line in listed.splitlines():
            if not line:
                continue
            build_id, filename = line.split(" ", 1)
            exported = Path(filename).resolve()
            if not exported.is_file() or not exported.is_relative_to(cache):
                raise ValueError(f"race export outside selected compiler cache: {exported}")
            exports.add(exported)
            # Go's build ID stores the first 15 bytes of the action hash in
            # base64. Use it to locate the full action record, then verify its
            # output hash against the compiler-selected export. Fail closed if
            # Go changes that cache encoding instead of shipping a partial graph.
            encoded = build_id.split("/", 1)[0]
            prefix = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).hex()
            if len(prefix) != 30:
                raise ValueError(f"unsupported Go action build ID: {build_id}")
            actions[prefix].add(exported.name.removesuffix("-d"))
    if not exports:
        raise ValueError("race graph has no exported archives")
    selected = set(exports)
    found: set[str] = set()
    for bucket in sorted({prefix[:2] for prefix in actions}):
        for entry in (cache / bucket).iterdir():
            prefix = entry.name[:30]
            if not entry.name.endswith("-a") or prefix not in actions:
                continue
            fields = entry.read_text().split()
            if (
                len(fields) != 5
                or fields[0] != "v1"
                or fields[1] != entry.name.removesuffix("-a")
                or fields[2] not in actions[prefix]
            ):
                raise ValueError(f"unexpected Go action record: {entry}")
            selected.add(entry)
            found.add(prefix)
    if found != set(actions):
        raise ValueError(f"missing Go export action records: {sorted(set(actions) - found)}")
    names = sorted(str(path.relative_to(cache)) for path in selected)
    kept = sum(path.stat().st_size for path in selected)
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["tar", "-I", "zstd -T2 -9", "-cf", str(output.resolve()), "-C", str(cache), "--null", "-T", "-"],
        input=b"".join(os.fsencode(name) + b"\0" for name in names),
        check=True,
    )
    size = output.stat().st_size
    print(f"race graph: {len(exports)} exports; {kept} cache bytes; {size} transfer bytes", flush=True)
    if size > MAX_ARCHIVE_BYTES:
        output.unlink()
        raise ValueError(f"race artifact {size} exceeds {MAX_ARCHIVE_BYTES}-byte transfer budget; split its graph")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--module", type=Path, action="append")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", type=Path)
    args = parser.parse_args()
    cache = Path(os.environ["GOCACHE"])
    if args.plan is None:
        pack(args.module or [ROOT / "sdo/operator", ROOT / "sdo/operator/runtime"], cache, args.output)
        return
    plan = json.loads(args.plan.read_text())
    for index, packages in enumerate(package_graphs(plan)):
        pack(list(packages), cache, args.output / str(index) / "compiler.tar.zst", packages)
        shutil.copyfile(args.plan, args.output / str(index) / "plan.json")


def package_graphs(plan: dict, root: Path = ROOT) -> list[dict[Path, list[str]]]:
    graphs = []
    for members in plan["bins"]:
        packages = {}
        for scope in ("operator", "runtime"):
            selected = sorted(
                {
                    package
                    for key in members
                    if plan["items"][key]["kind"] == "go" and plan["items"][key]["scope"] == scope
                    for package in plan["items"][key]["packages"]
                }
            )
            if selected:
                module = root / "sdo/operator" / ("runtime" if scope == "runtime" else "")
                packages[module] = selected
        # Python compiler contracts also need the standard race library even in
        # a bin without Go tests. Its fixtures compile their own synthetic code.
        if not packages:
            packages[root / "sdo/operator"] = ["testing"]
        graphs.append(packages)
    return graphs


if __name__ == "__main__":
    main()
