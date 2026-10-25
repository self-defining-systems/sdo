"""Private daemon handshake after worker imports, before reading any task."""

from __future__ import annotations

import json
import os
from pathlib import Path


def await_standby_claim() -> None:
    """A cold worker returns immediately; a preloaded worker awaits its daemon.

    The configured command and protobuf/result contracts are unchanged. Only
    the controller publishes the read-only inbox; the daemon releases this
    process after the exact accepted checkout and API history are bound.
    """
    raw_ready = os.environ.pop("SDO_STANDBY_READY_FD", None)
    if raw_ready is None:
        return
    raw_claim = os.environ.pop("SDO_STANDBY_CLAIM_FD")
    inbox = Path(os.environ.pop("SDO_STANDBY_INBOX"))
    with os.fdopen(int(raw_ready), "wb", buffering=0) as ready:
        ready.write(b"R")
    with os.fdopen(int(raw_claim), "rb", buffering=0) as claim:
        if claim.read(1) != b"C":
            raise RuntimeError("standby daemon closed before controller claim")
    environment = json.loads((inbox / "environment.json").read_text(encoding="utf-8"))
    if not isinstance(environment, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in environment.items()
    ):
        raise ValueError("standby launch environment is not a string mapping")
    os.environ.update(environment)
    os.environ["SDO_TASK_PATH"] = str(inbox / "agent-task.json")
