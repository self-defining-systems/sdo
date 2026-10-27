"""App deploy must succeed over a release a reused lane still holds (eval class C10)."""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.idempotent_helm import install_or_upgrade, make_deploy_idempotent

FAKE_HELM = textwrap.dedent(
    """\
    #!/bin/bash
    # Releases are files under $FAKE_HELM_STATE; like helm, a plain install refuses an existing name.
    echo "$*" >> "$FAKE_HELM_STATE/calls"
    case "$1" in
      dependency) exit 0 ;;
      install)
        if [ -e "$FAKE_HELM_STATE/release-$2" ]; then
          echo "Error: INSTALLATION FAILED: cannot re-use a name that is still in use" >&2; exit 1
        fi
        touch "$FAKE_HELM_STATE/release-$2" ;;
      upgrade) touch "$FAKE_HELM_STATE/release-$3" ;;
    esac
    """
)


@pytest.fixture
def helm_state(tmp_path, monkeypatch):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "helm").write_text(FAKE_HELM)
    (bin_dir / "helm").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_HELM_STATE", str(state))
    return state


class StockHelm:
    """SREGym's ``Helm.install`` today: a plain ``helm install``."""

    @staticmethod
    def install(**args):
        command = f"helm install {args['release_name']} {args['chart_path']} -n {args['namespace']} --create-namespace"
        done = subprocess.run(command, shell=True, capture_output=True, text=True)
        if done.returncode:
            raise RuntimeError(done.stderr.strip())


SOCIAL = {
    "release_name": "social-network",
    "chart_path": "/charts/social-network",
    "namespace": "social-network",
    "extra_args": ["--set global.replicas=1", "--set media-frontend.container.image=img"],
}


def test_a_plain_install_over_an_existing_release_fails(helm_state):
    StockHelm.install(**SOCIAL)
    with pytest.raises(RuntimeError, match="cannot re-use a name that is still in use"):
        StockHelm.install(**SOCIAL)


def test_deploy_succeeds_over_an_existing_release_for_every_app(helm_state):
    make_deploy_idempotent(StockHelm)
    apps = [SOCIAL, {**SOCIAL, "release_name": "astronomy-shop", "namespace": "astronomy-shop", "version": "1.2.3"}]
    for app in apps:
        StockHelm.install(**app)
        StockHelm.install(**app)  # a second deploy over the existing release
    calls = (helm_state / "calls").read_text().splitlines()
    assert any(
        call.startswith("upgrade --install astronomy-shop /charts/social-network -n astronomy-shop --create-namespace --version 1.2.3 ")
        for call in calls
    )
    assert not [call for call in calls if call.startswith("install ")]


def test_extra_args_and_remote_charts_keep_the_stock_contract(helm_state):
    install_or_upgrade(**{**SOCIAL, "remote_chart": True})
    calls = (helm_state / "calls").read_text().splitlines()
    assert calls == [
        "upgrade --install social-network /charts/social-network -n social-network --create-namespace "
        "--set global.replicas=1 --set media-frontend.container.image=img"
    ]


def test_a_failing_helm_still_raises_with_its_error(helm_state, monkeypatch):
    (Path(helm_state.parent) / "bin" / "helm").write_text("#!/bin/bash\necho boom >&2\nexit 3\n")
    with pytest.raises(RuntimeError, match="(?s)Helm install failed for release 'social-network'.*boom"):
        install_or_upgrade(**SOCIAL, remote_chart=True)


def test_incomplete_arguments_are_rejected():
    with pytest.raises(ValueError, match="release_name, chart_path and namespace"):
        install_or_upgrade(release_name="x")


@pytest.mark.requires_sregym
def test_the_real_sregym_helm_class_is_idempotent_after_the_bridge_patches_it(helm_state):
    """SREGym's own callers (`Helm.install(**configs)`) reach the replacement; unpatched, they fail."""
    root = Path(__file__).resolve().parents[5]
    script = textwrap.dedent(
        """\
        import sys
        from sregym.service.helm import Helm
        from benchmarks.sregym.fastloop.idempotent_helm import make_deploy_idempotent
        cfg = dict(release_name="r", chart_path="/c", namespace="n", remote_chart=True)
        Helm.install(**cfg)
        try:
            Helm.install(**cfg)
        except RuntimeError as exc:
            assert "cannot re-use a name" in str(exc), exc
        else:
            sys.exit("stock Helm.install unexpectedly succeeded twice")
        make_deploy_idempotent(Helm)
        Helm.install(**cfg)
        """
    )
    done = subprocess.run(
        ["uv", "run", "--directory", str(root / "third_party/sregym"), "python", "-c", script],
        env={**os.environ, "PYTHONPATH": str(root)},
        capture_output=True,
        text=True,
    )
    assert done.returncode == 0, done.stderr[-2000:]
