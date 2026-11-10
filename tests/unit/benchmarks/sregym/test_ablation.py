from pathlib import Path

import pytest

from benchmarks.sregym.ablation import transform


@pytest.mark.parametrize("arm", ["full", "detectors-only", "playbooks-only"])
def test_arm_projection(tmp_path: Path, arm: str) -> None:
    files = {
        ".sdo/schema-version": "6\n",
        ".sdo/diagnostics/detectors/health/ready/manifest.yaml": "possiblePlaybooks: [fix]\n",
        ".sdo/diagnostics/detectors/incidents/down/detector.go": (
            'package down\nvar spec = struct { Playbooks []string }{Playbooks: []string{"fix"}}\n'
        ),
        ".sdo/playbooks/fix/README.md": "# Fix\n",
        ".sdo/playbooks/fix/diagnose.go": "package fix\n",
        ".sdo/playbooks/fix/scripts/fix.sh": "exit 0\n",
        ".sdo/lib/helper.go": "package helper\n",
        ".sdo/corpus/case/data.json": "{}\n",
    }
    for name, body in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    changes = transform(tmp_path, arm)
    assert (tmp_path / ".sdo/diagnostics/detectors").exists() == (arm != "playbooks-only")
    for directory in ("playbooks", "lib", "corpus"):
        assert (tmp_path / ".sdo" / directory).exists() == (arm != "detectors-only")
    if arm == "full":
        assert not changes
        assert {name: (tmp_path / name).read_text() for name in files} == files
    if arm == "detectors-only":
        manifest = tmp_path / ".sdo/diagnostics/detectors/health/ready/manifest.yaml"
        assert manifest.read_text() == files[str(manifest.relative_to(tmp_path))]
        assert not changes


def test_invalid_arm_changes_nothing(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="arm"):
        transform(tmp_path, "invalid")
    assert not list(tmp_path.iterdir())


def test_operator_error_preserves_validator_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from benchmarks.sregym.adapter import native_operator

    monkeypatch.setattr(native_operator, "operator_command", lambda: (["operator"], None))
    monkeypatch.setattr(
        native_operator.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[], returncode=1, stdout="[detector.compile] broken program", stderr="stage=test completed"
        ),
    )
    with pytest.raises(native_operator.NativeOperatorError, match="broken program"):
        native_operator.execute_operator("memory", [], timeout=1)


def test_failed_audit_never_publishes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from benchmarks.sregym.ablation import __main__ as generator
    from benchmarks.sregym.repository_git import host_git_command

    source = tmp_path / "source"
    source.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(host_git_command(source, *args), text=True).strip()

    git("init", "-b", "ported")
    (source / ".sdo").mkdir()
    (source / ".sdo/schema-version").write_text("6\n")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "synthetic")

    def rejected(*args: object, **kwargs: object) -> str:
        raise RuntimeError("audit rejected")

    monkeypatch.setattr(generator, "execute_operator", rejected)
    with pytest.raises(RuntimeError, match="audit rejected"):
        generator.generate(str(source), "ported", "full", tmp_path / "variant", push=True)
    assert not git("branch", "--list", "ablate/*")


def test_generator_names_the_branch_the_caller_chose(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    from benchmarks.sregym.ablation import __main__ as generator
    from benchmarks.sregym.repository_git import host_git_command

    source = tmp_path / "source"
    source.mkdir()

    def git(directory: Path, *args: str) -> str:
        return subprocess.check_output(host_git_command(directory, *args), text=True).strip()

    git(source, "init", "-b", "ported")
    (source / ".sdo").mkdir()
    (source / ".sdo/schema-version").write_text("6\n")
    git(source, "add", ".")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "synthetic")

    def rejected(*args: object, **kwargs: object) -> str:
        raise RuntimeError("audit rejected")

    monkeypatch.setattr(generator, "execute_operator", rejected)
    with pytest.raises(RuntimeError, match="audit rejected"):
        generator.generate(
            str(source), "ported", "playbooks-only", tmp_path / "variant", push=False, branch="ablate/20261106-x"
        )
    assert git(tmp_path / "variant", "branch", "--show-current") == "ablate/20261106-x"
