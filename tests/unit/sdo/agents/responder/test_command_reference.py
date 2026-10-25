"""The shipped command guide follows the real CLI and fails closed on drift."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pytest

from sdo.agents.responder import tools_cli
from sdo.agents.responder.credentials import AGENT_SKILLS, install_skills

ROOT = Path(__file__).resolve().parents[5]
REFERENCE = ROOT / "sdo/skills/sdo-incident-response/references/commands.md"


def test_generated_reference_covers_cli_and_installs_with_worker(tmp_path: Path) -> None:
    from sdo.agents.responder.command_reference import render_reference

    generated = render_reference()
    assert REFERENCE.read_text() == generated
    for command, usage in tools_cli.command_surface()["commands"].items():
        assert f"## {command}\n" in generated
        assert usage in generated
    install_skills(tmp_path, AGENT_SKILLS["responder"])
    assert (tmp_path / "skills/sdo-incident-response/references/commands.md").read_text() == generated


def test_drift_check_rejects_stale_and_missing_reference_then_regenerates(tmp_path: Path) -> None:
    reference = tmp_path / "commands.md"
    invocation = [sys.executable, "-m", "sdo.agents.responder.command_reference", "--output", str(reference)]
    for stale in (None, "deliberately stale command guide\n"):
        if stale is not None:
            reference.write_text(stale)
        checked = subprocess.run([*invocation, "--check"], capture_output=True, text=True)
        assert checked.returncode == 1
        assert "regenerate" in checked.stderr
    subprocess.run(invocation, check=True)
    subprocess.run([*invocation, "--check"], check=True)


def test_reference_tracks_parser_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.responder.command_reference import render_reference

    original = tools_cli._parser

    def changed_parser() -> argparse.ArgumentParser:
        parser = original()
        parser.add_argument("--future-switch", help="new public argument")
        return parser

    before = render_reference()
    monkeypatch.setattr(tools_cli, "_parser", changed_parser)
    assert render_reference() != before
    assert "--future-switch" in render_reference()


def test_model_image_installs_and_smokes_json_parser() -> None:
    dockerfile = (ROOT / "sdo/operator/Dockerfile.runtime").read_text()
    model_layer = dockerfile.split("FROM python-deps AS model-deps\n", 1)[1].split("FROM go-deps AS go-runtime", 1)[0]
    os_install = model_layer.split("RUN apt-get update", 1)[1].split("COPY --from=node-tools", 1)[0]
    assert "jq" in os_install.split("rm -rf", 1)[0].split()
    assert "jq -e" in os_install
    assert ".probe[0].ok == true" in os_install


def test_response_fields_are_the_cli_help_contract(capsys: pytest.CaptureFixture[str]) -> None:
    assert set(tools_cli.RESPONSE_FIELDS) == set(tools_cli.command_surface()["commands"])
    for command, fields in tools_cli.RESPONSE_FIELDS.items():
        with pytest.raises(SystemExit) as stopped:
            tools_cli.main([command, "--help"])
        assert stopped.value.code == 0
        assert fields in " ".join(capsys.readouterr().out.split())


def test_mutation_reference_distinguishes_envelope_from_complete_file(capsys: pytest.CaptureFixture[str]) -> None:
    from sdo.agents.responder.command_reference import render_reference

    reference = render_reference()
    for name in ("act", "apply", "mitigate", "rollback"):
        section = reference.split(f"## {name}\n", 1)[1].split("\n## ", 1)[0]
        assert "Complete report fields (`details_file`)" in section
        with pytest.raises(SystemExit):
            tools_cli.main([name, "--help"])
        assert "Complete report fields (details_file)" in " ".join(capsys.readouterr().out.split())


def test_reference_keeps_new_help_notes_without_repeating_shared_help(monkeypatch: pytest.MonkeyPatch) -> None:
    from sdo.agents.responder.command_reference import render_reference

    original = tools_cli._parser
    added = "New credential reference shapes belong in the authoritative guide."
    shared = "Shared mutation output contract belongs in the guide once."

    def changed_parser() -> argparse.ArgumentParser:
        parser = original()
        commands = next(action for action in parser._actions if isinstance(action, argparse._SubParsersAction))
        for name in ("act", "apply", "mitigate", "rollback"):
            command = commands.choices[name]
            command.epilog = (command.epilog or "") + "\n\n" + shared
        command = commands.choices["mitigate"]
        command.epilog = (command.epilog or "") + "\n\n" + added
        return parser

    monkeypatch.setattr(tools_cli, "_parser", changed_parser)
    generated = render_reference()
    assert added in generated
    assert generated.count(shared) == 1
    assert "apply, mitigate, act, rollback" in generated
