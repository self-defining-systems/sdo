"""Deploy the same immutable accepted render used by comparison and restoration."""

from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

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
            previous = os.environ.get("SREGYM_ACCEPTED_RENDER")
            os.environ["SREGYM_ACCEPTED_RENDER"] = str(path)
            try:
                original()
            finally:
                if previous is None:
                    os.environ.pop("SREGYM_ACCEPTED_RENDER", None)
                else:
                    os.environ["SREGYM_ACCEPTED_RENDER"] = previous

    original = app.deploy
    app.deploy = deploy
