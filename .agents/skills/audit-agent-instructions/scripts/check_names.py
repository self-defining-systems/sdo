#!/usr/bin/env python3
"""Check executable instruction names against the CLI, protobufs and shipped skills.

Checks code spans/shell lines for sdo commands, request.field and RequestType.field
references, contextual `request field` names, and concrete skills/... file paths.
Unqualified prose and dynamic placeholders need the rollout audit's manual pass.
"""

from __future__ import annotations

import argparse
import importlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

COMMAND = re.compile(r"\bsdo[ \t]+([a-z][a-z0-9-]*)")
FIELD = re.compile(
    r"\b(request|[A-Z][A-Za-z]+Request)\.([a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)*)|\brequest field\s+`([a-z][a-z0-9_]*)`"
)
SKILL = re.compile(r"(?:sdo/)?skills/(sdo-[a-z][a-z0-9-]*)(/[a-zA-Z0-9_./-]+)?")
SKILL_FILE = re.compile(r"\b(sdo-[a-z][a-z0-9-]*)(/(?:SKILL\.md|references/[\w./-]+|assets/[\w./-]+))")


@dataclass(frozen=True)
class Problem:
    kind: str
    name: str
    line: int
    source: str = ""


@dataclass
class Catalogs:
    commands: set[str]
    fields: dict[str, Any]
    root: Path


def catalogs(root: Path) -> Catalogs:
    import argparse as ap

    from sdo.agents.responder.tools_cli import _parser

    commands = {
        command
        for action in _parser()._actions
        if isinstance(action, ap._SubParsersAction)
        for command in action.choices
    }
    # Import the repository's generated descriptors, including nested messages;
    # no copied field inventory can silently go stale.

    importlib.import_module("sdo.contracts.proto")
    fields: dict[str, Any] = {}

    def collect(message: Any) -> None:
        fields[message.name] = message
        for nested in message.nested_types:
            collect(nested)

    directory = root / "sdo/contracts/_gen/sdodev/contracts/v1alpha1"
    for path in sorted(directory.glob("*_pb2.py")):
        module = importlib.import_module(f"sdodev.contracts.v1alpha1.{path.stem}")
        for message in module.DESCRIPTOR.message_types_by_name.values():
            collect(message)
    return Catalogs(commands, fields, root)


def field_exists(role: str, path: str, available: dict[str, Any]) -> bool:
    if role != "request" and role not in available:
        return False
    messages = (
        [available[role]] if role in available else [m for name, m in available.items() if name.endswith("Request")]
    )
    for message in messages:
        valid = True
        for part in path.split("."):
            field = message.fields_by_name.get(part) if message else None
            if field is None:
                valid = False
                break
            message = field.message_type
        if valid:
            return True
    return False


def check_text(text: str, available: Catalogs, *, source: str = "") -> list[Problem]:
    problems: list[Problem] = []

    def report(kind: str, name: str, offset: int) -> None:
        problems.append(Problem(kind, name, text.count("\n", 0, offset) + 1, source))

    for match in COMMAND.finditer(text):
        # "the local sdo interface exposes ..." names the interface, not a command.
        if match[1] == "interface" and text[match.end() :].startswith(" exposes"):
            continue
        if match[1] not in available.commands:
            report("command", match[1], match.start())
    for match in FIELD.finditer(text):
        role, path = (match[1], match[2]) if match[1] else ("request", match[3])
        if not field_exists(role, path, available.fields):
            report("field", f"{role}.{path}", match.start())
    for match in [*SKILL.finditer(text), *SKILL_FILE.finditer(text)]:
        name, suffix = match[1], (match[2] or "/SKILL.md").rstrip("./")
        if not (available.root / "sdo/skills" / name / suffix.lstrip("/")).exists():
            report("skill", match.group(), match.start())
    return list(dict.fromkeys(problems))


def instruction_files(root: Path) -> list[Path]:
    files = set((root / "sdo/skills").rglob("*.md"))
    for directory in (root / "sdo/agents", root / "sdo/operator"):
        files.update(directory.glob("**/prompts/*.md"))
    # Transitional C5 sources: retain these while any inline prompt is present.
    for relative in (
        "sdo/agents/lifecycle/agents.py",
        "sdo/agents/deployer/worker.py",
        "sdo/agents/responder/codex.py",
        "sdo/agents/responder/reflection.py",
        "sdo/agents/responder/reflection_authoring.py",
        "sdo/agents/responder/reflection_brief.py",
    ):
        path = root / relative
        if path.exists():
            files.add(path)
    return sorted(files)


def check_repository(root: Path) -> list[Problem]:
    available = catalogs(root)
    return [
        problem
        for path in instruction_files(root)
        for problem in check_text(path.read_text(encoding="utf-8"), available, source=str(path.relative_to(root)))
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[4])
    args = parser.parse_args()
    problems = check_repository(args.root.resolve())
    for problem in problems:
        print(f"{problem.source}:{problem.line}: unknown {problem.kind}: {problem.name}")
    raise SystemExit(bool(problems))


if __name__ == "__main__":
    main()
