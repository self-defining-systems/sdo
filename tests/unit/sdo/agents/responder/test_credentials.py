from __future__ import annotations

from typing import TYPE_CHECKING

from sdo.agents.responder.credentials import prepare_claude_home, prepare_codex_home

if TYPE_CHECKING:
    from pathlib import Path


def test_prepare_codex_home_copies_auth_into_writable_runtime_directory(tmp_path: Path) -> None:
    credentials = tmp_path / "credentials"
    codex_home = tmp_path / "home" / ".codex"
    credentials.mkdir()
    (credentials / "auth.json").write_text('{"token":"secret"}', encoding="utf-8")

    prepare_codex_home(credentials_root=credentials, codex_home=codex_home)

    copied = codex_home / "auth.json"
    assert copied.read_text(encoding="utf-8") == '{"token":"secret"}'
    assert copied.stat().st_mode & 0o777 == 0o600


def test_prepare_claude_home_copies_credentials_into_writable_home(tmp_path: Path) -> None:
    credentials = tmp_path / "credentials"
    claude_home = tmp_path / "home" / ".claude"
    credentials.mkdir()
    (credentials / ".credentials.json").write_text('{"token":"secret"}', encoding="utf-8")

    prepare_claude_home(credentials_root=credentials, claude_home=claude_home)

    copied = claude_home / ".credentials.json"
    assert copied.read_text(encoding="utf-8") == '{"token":"secret"}'
    assert copied.stat().st_mode & 0o777 == 0o600
