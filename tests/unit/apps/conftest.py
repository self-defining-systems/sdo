import shutil
from collections.abc import Callable
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
OVERLAY = ROOT / "apps/social_network/source-overlays/helm-chart"
STOCK_SOURCE = ROOT / "apps/deathstarbench/socialNetwork"


@pytest.fixture
def fresh_social_chart(tmp_path: Path) -> Callable[[bool], Path]:
    """Build a socialNetwork root: the stock chart, optionally with the source overlay applied.

    The overlay's chart file volumes link to source directories next to ``helm-chart``, so the root
    links those directories to the checkout instead of copying 86 MB.
    """

    def build(overlay: bool) -> Path:
        root = tmp_path / ("overlay" if overlay else "stock")
        shutil.copytree(STOCK_SOURCE / "helm-chart", root / "helm-chart")
        for entry in ("media-frontend", "nginx-web-server", "gen-lua", "keys"):
            (root / entry).symlink_to(STOCK_SOURCE / entry)
        (root / "docker").mkdir()
        (root / "docker/openresty-thrift").symlink_to(STOCK_SOURCE / "docker/openresty-thrift")
        if overlay:
            shutil.copytree(OVERLAY, root / "helm-chart", symlinks=True, dirs_exist_ok=True)
        return root

    return build
