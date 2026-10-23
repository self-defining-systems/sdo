"""The benchmark build inputs stay pinned and independent of runtime code."""

from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize(("app", "count"), [("hotel_reservation", 1), ("social_network", 3), ("astronomy_shop", 20)])
def test_app_build_declaration(app: str, count: int) -> None:
    root = Path(__file__).resolve().parents[2]
    spec = yaml.safe_load((root / "apps" / app / "build.yaml").read_text())
    assert spec["version"] == 1
    assert spec["app"]["filter"] == app
    assert spec["image_tag"] == "{tag}"
    assert len(spec["source"]["ref"]) == 40
    assert len(spec["images"]) == count
    assert len({(i["name"], i.get("suffix", "")) for i in spec["images"]}) == count
    assert spec["deployment"]["namespace"] == app.replace("_", "-")
    assert spec["deployment"]["kind"] in {"manifest", "helm"}
    assert len(spec["deployment"]["ref"]) == 40
    for image in spec["images"]:
        assert not Path(image["context"]).is_absolute()
        assert not Path(image["dockerfile"]).is_absolute()
        for download in image.get("downloads", []):
            assert len(download["sha256"]) == 64
