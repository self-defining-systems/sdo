"""Benchmark harness options: real flags with contracts, not environment-variable hooks."""

import json
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from benchmarks.sregym.adapter import NativeInstallConfig, allowed_namespaces
from benchmarks.sregym.fastloop import admission, cli

OVERRIDES = '{"response_stages": ["agent"]}'


def _install(**extra: object) -> NativeInstallConfig:
    root = Path("/fixture")
    return NativeInstallConfig(
        repository=root / "app",
        namespace="app",
        control_namespace="app-sdo",
        application="app",
        controller_image="controller:go",
        responder_image="responder:python",
        repository_pvc="repository",
        credentials_secret="credentials",
        model="model",
        **extra,  # type: ignore[arg-type]
    )


def test_code_path_overrides_reach_the_operator_launch_and_default_to_none() -> None:
    assert "code_path_overrides" not in _install().overlay().get("agent", {})
    assert _install(code_path_overrides=OVERRIDES).overlay()["agent"]["code_path_overrides"] == OVERRIDES
    assert not any(arg.startswith("--code-path") for arg in _install(code_path_overrides=OVERRIDES).common_args())


@pytest.mark.parametrize("bad", ["{not json", "[1]", '"text"'])
def test_code_path_overrides_must_be_a_json_object(bad: str) -> None:
    with pytest.raises(ValueError, match="code_path_overrides"):
        _install(code_path_overrides=bad)


def test_run_flag_carries_the_code_path_overrides(tmp_path: Path) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--code-path-overrides", OVERRIDES])
    assert args.code_path_overrides == OVERRIDES
    assert parser.parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"]).code_path_overrides == ""


def test_run_flag_names_the_pre_onboarding_hook(tmp_path: Path) -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo", "--pre-onboarding-hook", "/x/seed"])
    assert args.pre_onboarding_hook == Path("/x/seed")
    assert parser.parse_args(["run", "--run-dir", str(tmp_path), "--agent", "sdo"]).pre_onboarding_hook is None


def _gate(tmp_path: Path, hook: Path | None) -> admission.LaneAdmission:
    environment = SimpleNamespace(namespace="social-network", kubeconfig=tmp_path / "kubeconfig")
    return admission.LaneAdmission(environment, object(), tmp_path / "proof", pre_onboarding_hook=hook)  # type: ignore[arg-type]


def _script(path: Path, body: str) -> Path:
    path.write_text("#!/bin/sh\n" + body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def test_pre_onboarding_hook_gets_namespace_and_lane_kubeconfig(tmp_path: Path) -> None:
    out = tmp_path / "seen.json"
    hook = _script(tmp_path / "hook.sh", f'printf \'{{"ns":"%s","kc":"%s"}}\' "$1" "$KUBECONFIG" > {out}\n')
    _gate(tmp_path, hook)._run_pre_onboarding_hook()
    assert json.loads(out.read_text()) == {"ns": "social-network", "kc": str(tmp_path / "kubeconfig")}


def test_failing_pre_onboarding_hook_stops_admission_with_its_output(tmp_path: Path) -> None:
    hook = _script(tmp_path / "hook.sh", "echo seed failed >&2\nexit 3\n")
    with pytest.raises(ValueError, match=r"pre-onboarding hook.*exited 3.*seed failed"):
        _gate(tmp_path, hook)._run_pre_onboarding_hook()


def test_no_hook_is_a_no_op(tmp_path: Path) -> None:
    _gate(tmp_path, None)._run_pre_onboarding_hook()


def test_lane_admission_allows_the_sregym_helper_namespace() -> None:
    assert "sregym-injector" in allowed_namespaces({"social-network"})
