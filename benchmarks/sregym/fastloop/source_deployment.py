"""Deploy the same immutable accepted render used by comparison and restoration."""

from __future__ import annotations

import json
import os
import subprocess
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from benchmarks.sregym.adapter import WaitKind, wait_for
from benchmarks.sregym.fastloop.accepted_source import AcceptedSource, manifest_paths_for


def configure_source_deployment(app: Any) -> None:
    """Bind deployment at the app boundary, including redeploy and row recreation."""
    workspace = os.environ.get("SREGYM_APP_SOURCE_DIR")
    if not workspace:
        raise ValueError("eval: source deployment requires an accepted application repository")
    application = app.name.lower().replace(" ", "-")
    source = AcceptedSource(Path(workspace), app.namespace, manifest_paths_for(application))

    def deploy() -> None:
        # Resolve the revision before any build or Kubernetes write. Never let
        # a modified worktree or an old capture supply desired application bytes.
        manifests = source.manifests
        if not manifests:
            raise ValueError(f"eval: accepted source {source.revision} has no deployment manifests")
        with TemporaryDirectory(prefix="accepted-deploy-") as directory:
            path = Path(directory) / "manifests.json"
            path.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests}))
            # Builds consume the same revision, including tracked file-generator
            # inputs; unaccepted worktree edits cannot change an image in place.
            archive = Path(directory) / "source.tar"
            with archive.open("wb") as output:
                subprocess.run(
                    ["git", "-C", str(source.workspace), "archive", source.revision],
                    stdout=output,
                    check=True,
                    timeout=wait_for(WaitKind.COMMAND),
                )
            frozen = Path(directory) / "source"
            frozen.mkdir()
            with tarfile.open(archive) as contents:
                contents.extractall(
                    frozen,
                    members=(
                        member for member in contents if member.name.split("/")[0] not in {".sdo", ".sdo-runtime"}
                    ),
                    filter="data",
                )
            updates = {"SREGYM_ACCEPTED_RENDER": str(path), "SREGYM_APP_SOURCE_DIR": str(frozen)}
            previous = {key: os.environ.get(key) for key in updates}
            os.environ.update(updates)
            try:
                original()
            finally:
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    original = app.deploy
    app.deploy = deploy
