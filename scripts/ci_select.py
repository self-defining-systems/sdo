#!/usr/bin/env python3
"""Select the tests a pull request can affect; every other event runs the full suite.

``plan`` maps the files changed by ``git diff BASE HEAD`` to the Go packages and Python
test files they can affect. The rules are in ``.github/ci_selection.json``; anything the
mapping cannot attribute selects the full suite. ``misses`` is the other half of the
contract: after a failed merge-group or main run it says which failing tests the pull
request run would have skipped, so each miss becomes a regression test for the mapping.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ".github/ci_selection.json"
PATH_TOKEN = re.compile(r"[A-Za-z0-9_.\-]+(?:/[A-Za-z0-9_.\-]+)+/?")
DOTTED_TOKEN = re.compile(r"\b[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+\b")
FILE_TOKEN = re.compile(r"[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)*\.[A-Za-z0-9]{1,6}\b")
GO_RELATIVE = re.compile(r'"((?:\.\./)+[^"\s]*)"')


class SelectionError(RuntimeError):
    """The selector could not build a plan; callers fall back to the full suite."""


def glob_regex(pattern: str) -> re.Pattern[str]:
    out = []
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def matches(path: str, patterns: list[str]) -> bool:
    return any(glob_regex(pattern).match(path) for pattern in patterns)


@dataclass(frozen=True)
class Config:
    full_suite: list[str]
    documentation: list[str]
    python_always: list[str]
    go_workspace_modules: list[str]
    go_binaries: list[str]
    go_consumer_modules: list[str]
    python_go_consumers: list[str]
    python_shards: dict[str, list[str]]
    go_cli_shard: list[str]
    go_validate_gate_tests: list[str]

    def __post_init__(self) -> None:
        for name in ("full_suite", "go_workspace_modules", "go_binaries", "python_go_consumers"):
            if not getattr(self, name):
                raise ValueError(f"{CONFIG_PATH}: {name} must not be empty")

    @classmethod
    def load(cls, root: Path = ROOT) -> Config:
        data = json.loads((root / CONFIG_PATH).read_text())
        return cls(**{key: data[key] for key in cls.__dataclass_fields__})


@dataclass
class Plan:
    full: bool = False
    reasons: list[str] = field(default_factory=list)
    go_packages: list[str] = field(default_factory=list)  # directories relative to the repo root
    python_tests: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, object]:
        return {
            "mode": "full" if self.full else "selected",
            "reasons": self.reasons,
            "go_packages": self.go_packages,
            "python_tests": self.python_tests,
            "changed": self.changed,
        }


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--", ".", ":!third_party", ":!apps"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return sorted(p for p in out.split("\0") if p and (root / p).is_file())


def changed_files(root: Path, base: str, head: str) -> tuple[list[str], list[str]]:
    """Return (existing, deleted) paths changed between base and head."""
    out = subprocess.run(
        ["git", "diff", "--name-status", "--no-renames", "-z", base, head],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split("\0")
    existing: list[str] = []
    deleted: list[str] = []
    for status, path in zip(out[0::2], out[1::2], strict=False):
        (deleted if status.startswith("D") else existing).append(path)
    return existing, deleted


def module_name(path: str) -> str | None:
    pure = PurePosixPath(path)
    if pure.suffix != ".py":
        return None
    parts = list(pure.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts) if parts else None


class PythonGraph:
    """Import and mention edges between the repository's Python files and other files."""

    def __init__(self, root: Path, files: list[str]) -> None:
        self.root = root
        self.files = set(files)
        self.dirs = {str(parent) for f in files for parent in PurePosixPath(f).parents if str(parent) != "."}
        self.modules: dict[str, str] = {}
        for path in files:
            name = module_name(path)
            if name:
                self.modules[name] = path
        self.importers: dict[str, set[str]] = defaultdict(set)
        self.file_mentioners: dict[str, set[str]] = defaultdict(set)
        self.dir_mentioners: dict[str, set[str]] = defaultdict(set)
        self.name_mentioners: dict[str, set[str]] = defaultdict(set)
        self.basenames = Counter(PurePosixPath(f).name for f in files)
        for path in files:
            if path.endswith(".py"):
                self._scan(path)

    def _resolve(self, dotted: str) -> list[str]:
        """The files that importing ``dotted`` executes: it and every parent package."""
        parts = dotted.split(".")
        found = [
            self.modules[".".join(parts[:i])] for i in range(1, len(parts) + 1) if ".".join(parts[:i]) in self.modules
        ]
        return found

    def _imports(self, path: str, tree: ast.AST) -> set[str]:
        package = (module_name(path) or "").split(".")
        if not path.endswith("__init__.py"):
            package = package[:-1]
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    anchor = package[: len(package) - node.level + 1]
                    base = ".".join([*anchor, base] if base else anchor)
                names.add(base)
                names.update(f"{base}.{alias.name}" for alias in node.names)
        return {n for n in names if n}

    def _scan(self, path: str) -> None:
        text = (self.root / path).read_text(errors="replace")
        try:
            tree = ast.parse(text)
        except SyntaxError as exc:
            raise SelectionError(f"{path} does not parse: {exc}") from exc
        targets: set[str] = set()
        for name in self._imports(path, tree):
            targets.update(self._resolve(name))
        for token in DOTTED_TOKEN.findall(text):
            if token.split(".")[0] in {"self", "os", "sys"}:
                continue
            resolved = self._resolve(token)
            if resolved and "." in ".".join(token.split(".")[:2]):
                targets.update(resolved)
                # A mentioned package is run as ``-m package``: its entry point executes too.
                main = self.modules.get(token + ".__main__")
                if main:
                    targets.add(main)
        for target in targets - {path}:
            self.importers[target].add(path)
        for token in PATH_TOKEN.findall(text):
            token = token.removeprefix("./").rstrip("/")
            if token in self.files:
                self.file_mentioners[token].add(path)
            elif token in self.dirs:
                self.dir_mentioners[token].add(path)
        for token in set(FILE_TOKEN.findall(text)):
            self.name_mentioners[token].add(path)

    def dependents(self, path: str) -> set[str]:
        out = set(self.importers.get(path, ())) | self.file_mentioners.get(path, set())
        for parent in PurePosixPath(path).parents:
            out |= self.dir_mentioners.get(str(parent), set())
        name = PurePosixPath(path).name
        if not path.endswith(".py") and self.basenames[name] == 1:  # a shared name says nothing
            out |= self.name_mentioners.get(name, set())
        out.discard(path)
        return out

    def closure(self, seeds: set[str]) -> set[str]:
        seen = set(seeds)
        queue = list(seeds)
        while queue:
            for dep in self.dependents(queue.pop()):
                if dep not in seen:
                    seen.add(dep)
                    queue.append(dep)
        return seen


@dataclass(frozen=True)
class GoPackage:
    import_path: str
    directory: str  # relative to the repo root
    imports: frozenset[str]
    test_imports: frozenset[str]


def go_packages(root: Path, modules: list[str]) -> dict[str, GoPackage]:
    """List every package of the CI workspace modules; no dependency download is needed."""
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp) / "go.work"
        work.write_text("go 1.26.8\nuse (\n" + "".join(f"    {root / m}\n" for m in modules) + ")\n")
        env = {**os.environ, "GOWORK": str(work), "GOFLAGS": "-trimpath"}
        patterns = [f"./{m}/..." for m in modules]
        out = subprocess.run(
            [
                "go",
                "list",
                "-e",
                "-tags=integration",
                "-json=ImportPath,Dir,Imports,TestImports,XTestImports",
                *patterns,
            ],
            cwd=root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    decoder = json.JSONDecoder()
    packages: dict[str, GoPackage] = {}
    pos = 0
    while pos < len(out):
        while pos < len(out) and out[pos].isspace():
            pos += 1
        if pos >= len(out):
            break
        obj, pos = decoder.raw_decode(out, pos)
        directory = os.path.relpath(obj["Dir"], root)
        packages[obj["ImportPath"]] = GoPackage(
            obj["ImportPath"],
            directory,
            frozenset(obj.get("Imports", ())),
            frozenset(obj.get("TestImports", ())) | frozenset(obj.get("XTestImports", ())),
        )
    return packages


def go_package_of(path: str, by_dir: dict[str, GoPackage]) -> GoPackage | None:
    for parent in PurePosixPath(path).parents:
        if str(parent) in by_dir:
            return by_dir[str(parent)]
    return None


def go_file_mentions(root: Path, packages: dict[str, GoPackage], files: set[str]) -> dict[str, set[str]]:
    """Map a repo path to the Go packages whose sources name it (``../`` literals, root paths)."""
    mentions: dict[str, set[str]] = defaultdict(set)
    dirs = {str(p) for f in files for p in PurePosixPath(f).parents}
    for pkg in packages.values():
        for go in (root / pkg.directory).glob("*.go"):
            text = go.read_text(errors="replace")
            for literal in GO_RELATIVE.findall(text):
                resolved = os.path.normpath(os.path.join(pkg.directory, literal))
                if "*" in resolved:  # a glob names the files directly inside its directory
                    mentions[os.path.dirname(resolved) + "/*"].add(pkg.import_path)
                else:
                    mentions[resolved.rstrip("/")].add(pkg.import_path)
            for token in PATH_TOKEN.findall(text):
                token = token.rstrip("/")
                if token in files or token in dirs:
                    mentions[token].add(pkg.import_path)
    return mentions


def go_mentions_of(path: str, mentions: dict[str, set[str]], by_dir: dict[str, GoPackage]) -> set[str]:
    """Go packages that read ``path``: by its name, a glob over its directory, or a directory above it.

    A directory that holds Go packages is never a mention target by itself: tests name such
    directories only to build paths below them.
    """
    out = set(mentions.get(path, ()))
    parent = str(PurePosixPath(path).parent)
    out |= mentions.get(parent + "/*", set())
    for ancestor in PurePosixPath(path).parents:
        name = str(ancestor)
        if not any(d.startswith(name + "/") for d in by_dir):
            out |= mentions.get(name, set())
    return out


def plan_for(root: Path, config: Config, changed: list[str], deleted: list[str]) -> Plan:
    plan = Plan(changed=sorted([*changed, *deleted]))
    if not plan.changed:
        return plan
    for path in plan.changed:
        if matches(path, config.full_suite):
            plan.full = True
            plan.reasons.append(f"shared input changed: {path}")
    for path in deleted:
        if not path.endswith("_test.go") and not re.search(r"(^|/)test_[^/]*\.py$", path):
            plan.full = True
            plan.reasons.append(f"deleted non-test file cannot be attributed: {path}")
    if plan.full:
        return plan

    files = tracked_files(root)
    fileset = set(files)
    graph = PythonGraph(root, files)
    packages = go_packages(root, config.go_workspace_modules)
    by_dir = {p.directory: p for p in packages.values()}
    mentions = go_file_mentions(root, packages, fileset)

    # Go: packages whose code changed, then the packages that build with them.
    code_changed: set[str] = set()  # import paths whose non-test code changed
    test_changed: set[str] = set()  # import paths whose tests changed
    unattributed: list[str] = []
    python_seeds: set[str] = set()
    go_binary_input = False
    for path in changed:
        if matches(path, config.documentation):  # no test reads these; the repository-wide guards still run
            continue
        owner = go_package_of(path, by_dir)
        attributed = False
        if owner is not None:
            attributed = True
            (test_changed if path.endswith("_test.go") else code_changed).add(owner.import_path)
        for name in go_mentions_of(path, mentions, by_dir):
            test_changed.add(name)
            attributed = True
        if not path.endswith(".go"):
            python_seeds.add(path)
            if path.endswith(".py") or graph.dependents(path):
                attributed = True
        if matches(path, [m + "/**" for m in config.go_consumer_modules]) and not path.endswith("_test.go"):
            go_binary_input = True
        if not attributed:
            unattributed.append(path)
    if unattributed:
        plan.full = True
        plan.reasons.extend(f"cannot attribute: {p}" for p in unattributed)
        return plan

    built = set(code_changed)
    changed_reverse = True
    while changed_reverse:
        changed_reverse = False
        for pkg in packages.values():
            if pkg.import_path not in built and pkg.imports & built:
                built.add(pkg.import_path)
                changed_reverse = True
    selected = set(code_changed) | test_changed
    selected |= {p.import_path for p in packages.values() if (p.imports | p.test_imports) & built}
    plan.go_packages = sorted(packages[p].directory for p in selected)

    # Python tests that run the Go executables rerun when an executable's inputs changed.
    closure: set[str] = set()
    for binary in config.go_binaries:
        pkg = by_dir.get(binary)
        if pkg is None:
            raise SelectionError(f"configured Go binary package {binary} does not exist")
        stack = [pkg.import_path]
        while stack:
            current = stack.pop()
            if current in closure or current not in packages:
                continue
            closure.add(current)
            stack.extend(packages[current].imports)
    if built & closure or go_binary_input:
        plan.reasons.append("Go executable inputs changed: Python tests that run them are selected")
        for path in files:
            if path.endswith(".py") and matches(path, config.python_go_consumers):
                python_seeds.add(path)

    affected = graph.closure(python_seeds)
    tests = {
        p
        for p in affected
        if p.startswith("tests/") and PurePosixPath(p).name.startswith("test_") and p.endswith(".py")
    }
    if any(not p.endswith("_test.go") for p in changed):
        tests |= {p for p in files if p.endswith(".py") and matches(p, config.python_always)}
    plan.python_tests = sorted(tests)
    return plan


def shard_of(test: str, config: Config) -> str:
    for shard, patterns in config.python_shards.items():
        if matches(test, patterns):
            return shard
    return "core"


VALIDATE_DIR = "sdo/operator/memory/validate"


def load_plan(path: Path) -> Plan:
    data = json.loads(path.read_text())
    return Plan(
        full=data["mode"] == "full",
        reasons=data["reasons"],
        go_packages=data["go_packages"],
        python_tests=data["python_tests"],
        changed=data["changed"],
    )


def go_shard_of(directory: str, config: Config) -> str:
    """The Go shard that owns a package directory (relative to the repo root)."""
    if directory == VALIDATE_DIR:
        return "validate"
    for owned in config.go_cli_shard:
        if directory == owned or directory.startswith(owned + "/"):
            return "cli"
    return "rest"


# The ``gate`` shard runs the memory validator package too, restricted to go_validate_gate_tests;
# the ``validate`` shard runs the rest of that package. A package cannot be split any other way.
SHARD_PACKAGE_OWNER = {"gate": "validate"}


def shard_go_packages(plan: Plan, config: Config, shard: str) -> list[str]:
    """The ``go test`` patterns one Go shard runs; ``validate`` and ``gate`` share the memory validator
    package and ``cli`` is the directories (and subdirectories) listed in ``go_cli_shard``."""
    owner = SHARD_PACKAGE_OWNER.get(shard, shard)
    return [f"./{d}" for d in plan.go_packages if go_shard_of(d, config) == owner]


def go_test_filter(config: Config, shard: str) -> tuple[str, str] | None:
    """The ``go test`` flag (``-run`` or ``-skip``) and regexp that split the validator package."""
    if shard not in ("validate", "gate") or not config.go_validate_gate_tests:
        return None
    names = "|".join(re.escape(name) for name in config.go_validate_gate_tests)
    return ("-run", f"^({names})$") if shard == "gate" else ("-skip", f"^({names})$")


def partition_go_packages(listing: str, root: Path, config: Config, shard: str) -> list[str]:
    """Import paths of one shard from ``go list -f '{{.ImportPath}} {{.Dir}}'`` output."""
    chosen = []
    for line in listing.splitlines():
        if not line.strip():
            continue
        import_path, directory = line.split(" ", 1)
        if go_shard_of(Path(directory).relative_to(root).as_posix(), config) == shard:
            chosen.append(import_path)
    return chosen


def shard_python_tests(plan: Plan, config: Config, shard: str) -> list[str]:
    return [t for t in plan.python_tests if shard_of(t, config) == shard]


def failed_go_packages(log: Path) -> set[str]:
    failed: set[str] = set()
    for line in log.read_text(errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("Action") == "fail" and event.get("Package") and event.get("Test"):
            failed.add(event["Package"])
    return failed


def failed_python_tests(log: Path) -> set[str]:
    return set(re.findall(r"^FAILED (tests/[^\s:]+)", log.read_text(errors="replace"), re.MULTILINE))


def summary(plan: Plan) -> str:
    if plan.full:
        return "Full suite. " + "; ".join(plan.reasons[:5])
    return f"Selected {len(plan.go_packages)} Go packages and {len(plan.python_tests)} Python test files."


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan_cmd = sub.add_parser("plan")
    plan_cmd.add_argument("--event", required=True)
    plan_cmd.add_argument("--base", default="HEAD^1")
    plan_cmd.add_argument("--head", default="HEAD")
    plan_cmd.add_argument("--out", type=Path)
    for name in ("mode", "go-patterns", "python-paths", "skip"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--plan", type=Path, required=True)
        if name != "mode":
            cmd.add_argument("--shard", required=True)
        if name == "skip":
            cmd.add_argument("--kind", choices=["go", "python"], required=True)
    filter_cmd = sub.add_parser("go-test-filter")
    filter_cmd.add_argument("--shard", required=True)
    partition_cmd = sub.add_parser("partition-go")
    partition_cmd.add_argument("--shard", required=True)
    partition_cmd.add_argument("--root", type=Path, default=ROOT)
    miss_cmd = sub.add_parser("misses")
    miss_cmd.add_argument("--base", default="HEAD^1")
    miss_cmd.add_argument("--head", default="HEAD")
    miss_cmd.add_argument("--go-log", type=Path)
    miss_cmd.add_argument("--pytest-log", type=Path)
    args = parser.parse_args(argv)
    config = Config.load()

    if args.command == "go-test-filter":
        chosen_filter = go_test_filter(config, args.shard)
        if chosen_filter:
            print(*chosen_filter, sep="\n")
        return 0

    if args.command == "partition-go":
        print("\n".join(partition_go_packages(sys.stdin.read(), args.root, config, args.shard)))
        return 0

    if args.command in {"mode", "go-patterns", "python-paths", "skip"}:
        plan = load_plan(args.plan)
        if args.command == "mode":
            print("full" if plan.full else "selected")
        elif args.command == "go-patterns":
            print("\n".join(shard_go_packages(plan, config, args.shard)))
        elif args.command == "python-paths":
            print("\n".join(shard_python_tests(plan, config, args.shard)))
        else:  # environment lines for $GITHUB_ENV
            chosen = (
                shard_go_packages(plan, config, args.shard)
                if args.kind == "go"
                else shard_python_tests(plan, config, args.shard)
            )
            print(f"SDO_CI_SKIP={'true' if not plan.full and not chosen else 'false'}")
            print(f"SDO_CI_MODE={'full' if plan.full else 'selected'}")
        return 0

    if args.command == "plan" and args.event != "pull_request":
        plan = Plan(full=True, reasons=[f"{args.event} runs the full suite"])
    else:
        try:
            existing, deleted = changed_files(ROOT, args.base, args.head)
            plan = plan_for(ROOT, config, existing, deleted)
        except (SelectionError, subprocess.CalledProcessError, OSError) as exc:
            plan = Plan(full=True, reasons=[f"selection failed, running everything: {exc}"])
        if args.command == "plan" and not plan.changed:
            plan.reasons.append("no changed files")

    if args.command == "misses":
        skipped: list[str] = []
        if plan.full:
            print(f"The pull request run would have run the full suite: {summary(plan)}")
            return 0
        packages = {str(pkg_dir) for pkg_dir in plan.go_packages}
        if args.go_log and args.go_log.exists():
            by_dir = {p.import_path: p.directory for p in go_packages(ROOT, config.go_workspace_modules).values()}
            skipped += [
                f"Go package {pkg}"
                for pkg in sorted(failed_go_packages(args.go_log))
                if by_dir.get(pkg) not in packages
            ]
        if args.pytest_log and args.pytest_log.exists():
            skipped += [
                f"Python test {t}" for t in sorted(failed_python_tests(args.pytest_log)) if t not in plan.python_tests
            ]
        for item in skipped:
            print(f"SELECTION MISS: {item} failed here but the pull request run skipped it.")
        if skipped:
            print("Add a regression case to tests/unit/sdo/operator/test_ci_selection.py for each miss.")
        return 0

    text = json.dumps(plan.to_json(), indent=2, sort_keys=True)
    if args.out:
        args.out.write_text(text + "\n")
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a") as handle:
            handle.write(f"### Test selection\n{summary(plan)}\n")
    print(summary(plan), file=sys.stderr)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
