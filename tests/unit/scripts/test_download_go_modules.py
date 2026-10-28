"""Cache warming must not change source-bound module inputs."""

from __future__ import annotations

import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
pytestmark = pytest.mark.go_integration


@pytest.mark.parametrize("existing_sum", [False, True])
def test_real_download_preserves_sources_and_feeds_an_offline_consumer(tmp_path: Path, existing_sum: bool) -> None:
    proxy = tmp_path / "proxy"
    versions = proxy / "fixture.test/dependency/@v"
    versions.mkdir(parents=True)
    metadata = b"module fixture.test/dependency\n\ngo 1.24.0\n"
    (versions / "v1.0.0.mod").write_bytes(metadata)
    (versions / "v1.0.0.info").write_text(json.dumps({"Version": "v1.0.0", "Time": "2024-01-01T00:00:00Z"}))
    with zipfile.ZipFile(versions / "v1.0.0.zip", "w") as archive:
        archive.writestr("fixture.test/dependency@v1.0.0/go.mod", metadata)
        archive.writestr("fixture.test/dependency@v1.0.0/value.go", "package dependency\nconst Value = 1\n")
    source = tmp_path / "source"
    source.mkdir()
    module = "module fixture.test/consumer\n\ngo 1.24.0\n\nrequire fixture.test/dependency v1.0.0\n"
    (source / "go.mod").write_text(module)
    if existing_sum:
        (source / "go.sum").write_bytes(b"")
    environment = {
        **os.environ,
        "GOPROXY": proxy.as_uri(),
        "GOSUMDB": "off",
        "GONOPROXY": "none",
        "GONOSUMDB": "*",
        "GOMODCACHE": str(tmp_path / "modcache"),
        "GOTOOLCHAIN": "local",
        "GOWORK": "off",
    }
    subprocess.run(["bash", str(ROOT / "scripts/download_go_modules.sh"), str(source)], env=environment, check=True)
    assert (source / "go.mod").read_text() == module
    assert (source / "go.sum").exists() is existing_sum
    if existing_sum:
        assert (source / "go.sum").read_bytes() == b""
    assert sorted(path.name for path in source.iterdir()) == ["go.mod", *(["go.sum"] if existing_sum else [])]
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    (consumer / "go.mod").write_text(module)
    (consumer / "main.go").write_text('package consumer\nimport _ "fixture.test/dependency"\n')
    # No network, no fixture answers: real Go consumes the real producer cache.
    result = subprocess.check_output(
        ["go", "list", "-mod=mod", "-deps", "."],
        cwd=consumer,
        env={**environment, "GOPROXY": "off"},
        text=True,
    )
    assert "fixture.test/dependency" in result.splitlines()
