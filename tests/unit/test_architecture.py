"""Static ownership and dependency checks for production SDO and benchmarks."""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest

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


def _adapter_import_errors(files: list[Path]) -> list[str]:
    """Resolve actual import statements, including Python's submodule fallback."""
    errors = []
    for filepath in files:
        tree = ast.parse(filepath.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module != "benchmarks.sregym.adapter":
                continue
            statement = ast.Module(body=[node], type_ignores=[])
            try:
                exec(compile(statement, str(filepath), "exec"), {})
            except (ImportError, AttributeError) as error:
                errors.append(f"{filepath}:{node.lineno}: {error}; export the name from the adapter facade")
    return errors


def test_repository_adapter_facade_imports_resolve() -> None:
    # Tracked paths cover the whole repository without entering submodules or venvs.
    paths = subprocess.run(
        ["git", "ls-files", "--", "*.py"],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    errors = _adapter_import_errors([_REPO_ROOT / path for path in paths])
    assert not errors, "Unresolved adapter facade imports:\n" + "\n".join(errors)


def test_adapter_import_check_detects_missing_export(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from benchmarks.sregym import adapter

    source = tmp_path / "consumer.py"
    source.write_text("from benchmarks.sregym.adapter import ImageRetentionError\n")
    assert _adapter_import_errors([source]) == []
    monkeypatch.delitem(vars(adapter), "ImageRetentionError", raising=False)
    monkeypatch.delitem(adapter._EXPORTS, "ImageRetentionError")

    errors = _adapter_import_errors([source])

    assert len(errors) == 1
    assert "consumer.py:1" in errors[0]
    assert "ImageRetentionError" in errors[0]
