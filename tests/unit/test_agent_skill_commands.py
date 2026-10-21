"""Keep the fastloop skills aligned with commands and result contracts."""

import argparse
import re
from pathlib import Path

from benchmarks.sregym.fastloop.cli import build_parser
from benchmarks.sregym.fastloop.records import IncidentRecord
from sdo.contracts import OutcomeRecord

ROOT = Path(__file__).resolve().parents[2]
SKILLS = ("iterate-sdo", "run-fastloop-experiment")
CODE = re.compile(r"(?<!`)`([^`\n]+)`(?!`)")
REPO_PATH = re.compile(r"(?<!\S)(?:docs|benchmarks|sdo|scripts|tests)/[\w./-]+")


def test_fastloop_skills_reference_existing_commands_paths_and_fields() -> None:
    parser = build_parser()
    subparsers = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
    report = ROOT / ".agents/skills/run-fastloop-experiment/references/report.md"
    files = [
        file
        for skill in SKILLS
        for file in (ROOT / ".agents/skills" / skill).rglob("*.md")
        if file.name == "SKILL.md" or file.parent.name == "references"
    ]
    assert files

    for file in files:
        body = file.read_text(encoding="utf-8")
        for script in re.findall(r"\bscripts/[\w.-]+", body):
            assert (ROOT / script).is_file(), f"{file}: missing {script}"
        for link in re.findall(r"\[[^]]+\]\(([^)#]+)(?:#[^)]+)?\)", body):
            assert (file.parent / link).exists(), f"{file}: broken link {link}"
        for code in CODE.findall(body):
            for path in REPO_PATH.findall(code):
                assert (ROOT / path).exists(), f"{file}: missing {path}"
        for line in body.splitlines():
            for command in (
                match for code in CODE.findall(line) for match in re.finditer(r"\bfastloop\s+([\w-]+)", code)
            ):
                name = command.group(1)
                assert name in subparsers.choices, f"{file}: unknown fastloop command {name}"
                accepted = {option for action in subparsers.choices[name]._actions for option in action.option_strings}
                for flag in re.findall(r"(?<![\w-])--[\w-]+", line):
                    assert flag in accepted, f"{file}: {name} does not accept {flag}"
                if name == "replay":
                    stage = re.search(r"--stage\s+([\w\\|]+)", line)
                    if stage:
                        stage_action = next(
                            action for action in subparsers.choices[name]._actions if "--stage" in action.option_strings
                        )
                        for value in stage.group(1).replace("\\", "").split("|"):
                            assert value in stage_action.choices, f"{file}: invalid replay stage {value}"

    row_fields = set(IncidentRecord.model_fields) | set(IncidentRecord.model_computed_fields)
    for line in report.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or "ledger's" in line:
            continue
        for field in CODE.findall(line.split("|", 2)[2]):
            if not re.fullmatch(r"[a-z][\w]*(?:\[\])?(?:\.[a-z][\w]*)*", field):
                continue
            if field in {"sdo", "finding", "hint", "plan", "apply", "verify", "diagnose"}:
                continue
            if field in row_fields:
                continue
            descriptor = OutcomeRecord.DESCRIPTOR
            for part in field.replace("[]", "").split("."):
                assert part in descriptor.fields_by_name, f"{report}: unknown report field {field}"
                member = descriptor.fields_by_name[part]
                descriptor = member.message_type
            assert member is not None
