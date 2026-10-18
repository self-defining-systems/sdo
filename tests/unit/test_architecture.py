"""Static ownership and dependency checks for production SDO and benchmarks."""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Importable project namespaces whose private modules and explicit public APIs
# are governed here. Namespace-package parents such as ``benchmarks`` do not
# need an ``__init__.py`` to participate.
_PROJECT_PACKAGES: dict[str, Path] = {
    "sdo": _REPO_ROOT / "sdo",
    "benchmarks": _REPO_ROOT / "benchmarks",
    "libs": _REPO_ROOT / "libs",
}

# Each entry is an independently owned package with a public __init__.py
# facade. Imports from outside an owner must use that facade, not an internal
# implementation module. The more-specific agent-runtime owners intentionally
# precede their parent.
_OWNERS: tuple[str, ...] = (
    "sdo.agents.lifecycle",
    "sdo.agents.responder",
    "sdo.contracts",
    "benchmarks.sregym.adapter",
)

_PUBLIC_MODULES: tuple[str, ...] = ()


def _iter_py_files(*roots: Path) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.exists():
            files.extend(sorted(root.rglob("*.py")))
    return files


def _module_for_file(filepath: Path) -> str | None:
    for top_level, root in _PROJECT_PACKAGES.items():
        try:
            relative = filepath.relative_to(root)
        except ValueError:
            continue
        parts = list(relative.with_suffix("").parts)
        if parts and parts[-1] == "__init__":
            parts.pop()
        return ".".join((top_level, *parts))
    return None


def _owner(module: str) -> str | None:
    return next((owner for owner in _OWNERS if module == owner or module.startswith(f"{owner}.")), None)


def _imports(filepath: Path) -> list[tuple[str, int]]:
    tree = ast.parse(filepath.read_text(encoding="utf-8"))
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imports.append((node.module, node.lineno))
        elif isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
    return imports


def _fmt(filepath: Path, lineno: int, module: str, reason: str) -> str:
    return f"  {filepath.relative_to(_REPO_ROOT)}:{lineno}  '{module}'  — {reason}"


def _private_owner_dir(module: str) -> Path | None:
    parts = module.split(".")
    root = _PROJECT_PACKAGES.get(parts[0])
    if root is None:
        return None
    for index, part in enumerate(parts[1:], 1):
        if part.startswith("_"):
            return root.joinpath(*parts[1:index]) if index > 1 else root
    return None


def test_cross_package_imports_go_through_facades() -> None:
    violations: list[str] = []
    for filepath in _iter_py_files(*_PROJECT_PACKAGES.values()):
        home = _owner(_module_for_file(filepath) or "")
        for module, lineno in _imports(filepath):
            target = _owner(module)
            if target is None or target == home or module == target or module in _PUBLIC_MODULES:
                continue
            violations.append(_fmt(filepath, lineno, module, f"bypasses the '{target}' facade; import from '{target}'"))
    assert not violations, f"{len(violations)} facade violation(s) found:\n" + "\n".join(violations)


def test_no_private_submodule_imports_across_packages() -> None:
    violations: list[str] = []
    for filepath in _iter_py_files(*_PROJECT_PACKAGES.values()):
        for module, lineno in _imports(filepath):
            owner_dir = _private_owner_dir(module)
            if owner_dir is None:
                continue
            try:
                filepath.relative_to(owner_dir)
            except ValueError:
                private_part = next(part for part in module.split(".")[1:] if part.startswith("_"))
                violations.append(
                    _fmt(
                        filepath,
                        lineno,
                        module,
                        f"'{private_part}' is private to '{owner_dir.relative_to(_REPO_ROOT)}'",
                    )
                )
    assert not violations, f"{len(violations)} private-module violation(s) found:\n" + "\n".join(violations)


def test_public_packages_declare_all() -> None:
    missing: list[str] = []
    for package in _OWNERS:
        top_level, *parts = package.split(".")
        init = _PROJECT_PACKAGES[top_level].joinpath(*parts, "__init__.py")
        if not init.exists():
            missing.append(f"  {init.relative_to(_REPO_ROOT)} (missing facade)")
            continue
        source = init.read_text(encoding="utf-8").strip()
        if not source:
            continue
        tree = ast.parse(source)
        if not any(
            isinstance(node, (ast.Assign, ast.AnnAssign))
            and (
                (
                    isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
                )
                or (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and node.target.id == "__all__"
                )
            )
            for node in ast.walk(tree)
        ):
            missing.append(f"  {init.relative_to(_REPO_ROOT)}")
    assert not missing, "__all__ missing from non-empty package facade(s):\n" + "\n".join(missing)


def test_all_does_not_export_private_names() -> None:
    violations: list[str] = []
    for filepath in _iter_py_files(*_PROJECT_PACKAGES.values()):
        if filepath.name != "__init__.py":
            continue
        tree = ast.parse(filepath.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign) or not any(
                isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
            ):
                continue
            if not isinstance(node.value, (ast.List, ast.Tuple)):
                continue
            violations.extend(
                f"  {filepath.relative_to(_REPO_ROOT)}:{element.lineno}  '{element.value}' — private export"
                for element in node.value.elts
                if isinstance(element, ast.Constant)
                and isinstance(element.value, str)
                and element.value.startswith("_")
            )
    assert not violations, f"{len(violations)} private export(s) found:\n" + "\n".join(violations)


def test_production_sdo_does_not_import_benchmark_code() -> None:
    forbidden = ("benchmarks",)
    violations: list[str] = []
    sdo_root = _PROJECT_PACKAGES["sdo"]
    for filepath in _iter_py_files(sdo_root):
        for module, lineno in _imports(filepath):
            if any(module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden):
                violations.append(_fmt(filepath, lineno, module, "production SDO cannot depend on benchmark code"))
    assert not violations, f"{len(violations)} production-to-benchmark import(s) found:\n" + "\n".join(violations)


def test_operational_memory_does_not_import_agent_runtime() -> None:
    """Operational memory is a lower layer than executable agent backends."""

    violations: list[str] = []
    root = _PROJECT_PACKAGES["sdo"] / "operational_memory"
    for filepath in _iter_py_files(root):
        for module, lineno in _imports(filepath):
            if module == "sdo.agents" or module.startswith("sdo.agents."):
                violations.append(
                    _fmt(
                        filepath,
                        lineno,
                        module,
                        "use a structural reflector contract instead of importing an agent runtime",
                    )
                )
    assert not violations, f"{len(violations)} upward operational-memory import(s) found:\n" + "\n".join(violations)
