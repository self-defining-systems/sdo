"""The dev mount exposes only an allowlist of benchmark modules: every in-repo module they import must be on it."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
GO = ROOT / "sdo/operator/devmount/devmount.go"


def _allowlist() -> set[str]:
    block = re.search(r"var benchmarkFiles = \[\]string\{(.*?)\n\}", GO.read_text(encoding="utf-8"), re.S)
    assert block is not None
    return set(re.findall(r'"(benchmarks/[^"]+\.py)"', block.group(1)))


def _imports(path: Path) -> set[str]:
    found: set[str] = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ImportFrom) and node.level == 0 and (node.module or "").startswith("benchmarks"):
            found.add(node.module or "")
            found.update(f"{node.module}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("benchmarks"))
    return found


def test_allowlisted_modules_import_only_allowlisted_modules() -> None:
    allow = _allowlist()
    missing: dict[str, list[str]] = {}
    for rel in sorted(allow):
        for name in _imports(ROOT / rel):
            module = ROOT / (name.replace(".", "/") + ".py")
            package = ROOT / name.replace(".", "/") / "__init__.py"
            for candidate in (module, package):
                if candidate.exists() and str(candidate.relative_to(ROOT)) not in allow:
                    missing.setdefault(rel, []).append(str(candidate.relative_to(ROOT)))
    assert not missing, f"imported by allowlisted modules but not mounted: {missing}"
