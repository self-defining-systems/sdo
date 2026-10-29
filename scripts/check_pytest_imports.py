"""Reject pytest prepend-mode module collisions before collection starts."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path


def duplicate_import_names(root: Path) -> dict[str, list[Path]]:
    modules: dict[str, list[Path]] = defaultdict(list)
    for path in root.rglob("test_*.py"):
        name = path.stem
        package = path.parent
        while (package / "__init__.py").is_file():
            name = package.name + "." + name
            package = package.parent
        modules[name].append(path)
    return {name: paths for name, paths in modules.items() if len(paths) > 1}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1] / "tests")
    args = parser.parse_args()
    if not args.root.is_dir():
        parser.error(f"test root does not exist: {args.root}")
    collisions = duplicate_import_names(args.root)
    for name, paths in sorted(collisions.items()):
        print(f"pytest import collision {name}: " + ", ".join(str(path) for path in paths))
    return int(bool(collisions))


if __name__ == "__main__":
    raise SystemExit(main())
