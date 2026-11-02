"""Pinned benchmark builds expose conventional Docker input scopes."""

from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize(("app", "count"), [("social_network", 3), ("astronomy_shop", 20)])
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
    assert len(spec["deployment"]["ref"]) == 40
    for image in spec["images"]:
        assert not Path(image["context"]).is_absolute()
        assert not Path(image["dockerfile"]).is_absolute()
        for field in ("dockerfile", "dockerfile_arm64"):
            if field in image:
                scope = root / "apps" / app / "build-inputs" / (image[field] + ".dockerignore")
                assert scope.is_file(), f"missing conventional scope for {image[field]}"
        for download in image.get("downloads", []):
            assert len(download["sha256"]) == 64
