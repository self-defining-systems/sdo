"""Result identity binding at the Kubernetes HTTP boundary."""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING

from sdo.agents.responder import job

if TYPE_CHECKING:
    from pathlib import Path
    from urllib.request import Request

    import pytest


def test_result_publisher_retains_controller_request_binding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The transport reads only fixture credentials, never a cluster Secret.
    (tmp_path / "token").write_text("fixture-token")
    monkeypatch.setattr(job, "SERVICE_ACCOUNT_ROOT", tmp_path)
    monkeypatch.setenv("SDO_REQUEST_SHA256", "a" * 64)
    monkeypatch.setattr(job.ssl, "create_default_context", lambda **_kwargs: None)
    published = []

    def send(request: Request, **_kwargs: object) -> contextlib.AbstractContextManager[None]:
        assert request.data is not None
        published.append(json.loads(request.data))
        return contextlib.nullcontext()

    monkeypatch.setattr(job.urllib.request, "urlopen", send)
    job.KubernetesResultPublisher(namespace="control").publish_payload("task-result", "agent-result.json", "{}")
    assert published == [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "task-result",
                "namespace": "control",
                "annotations": {"sdo.dev/request-sha256": "a" * 64},
            },
            "data": {"agent-result.json": "{}"},
        }
    ]
