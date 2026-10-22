"""The agent provider and model a worker Job resolved from its environment."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from libs.agent_cli.structured import AGENT_PROVIDERS

if TYPE_CHECKING:
    from collections.abc import Mapping

_ENVIRONMENT = json.loads((Path(__file__).resolve().parents[1] / "contracts" / "agent_env.json").read_text("utf-8"))
#: The worker environment the controller sets (``worker_registry.AgentEnvironment``); one manifest names it for
#: both languages.
PROVIDER_ENV: str = _ENVIRONMENT["provider"]
#: Model variable by role: ``responder``, ``reflection``, ``lifecycle`` (health judge, architect) or ``deployment``.
MODEL_ENV: dict[str, str] = dict(_ENVIRONMENT["models"])
SESSION_DIR_ENV = "SDO_SESSION_DIR"
RESOLVED_LOG_NAME = "agent-resolved.jsonl"


@dataclass(frozen=True)
class ResolvedAgent:
    role: str
    provider: str
    model: str | None

    def __post_init__(self) -> None:
        if self.provider not in AGENT_PROVIDERS:
            raise ValueError(f"unsupported agent provider {self.provider!r}")


def resolve_agent(role: str, model: str, environ: Mapping[str, str] | None = None) -> ResolvedAgent:
    """Resolve *role*'s provider and model (*model* is a key of ``MODEL_ENV``), report them, and record them.

    The resolution is recorded beside the incident session.

    An unset provider means Codex; a provider that is set to anything else must be known.
    """

    environ = os.environ if environ is None else environ
    model_env = MODEL_ENV[model]
    provider = environ.get(PROVIDER_ENV, "").strip().lower() or "codex"
    if provider not in AGENT_PROVIDERS:
        raise ValueError(
            f"unsupported agent provider {provider!r} for {role} ({PROVIDER_ENV} must be one of {AGENT_PROVIDERS})"
        )
    resolved = ResolvedAgent(role, provider, environ.get(model_env, "").strip() or None)
    print(
        f"sdo-agent-resolved role={role} provider={provider} model={resolved.model or 'default'}",
        file=sys.stderr,
        flush=True,
    )
    session_dir = environ.get(SESSION_DIR_ENV, "").strip()
    if session_dir:
        record = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "role": role,
            "provider": provider,
            "model": resolved.model,
        }
        try:
            with (Path(session_dir) / RESOLVED_LOG_NAME).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
        except OSError as exc:
            # The stderr line above is the primary report; the record only feeds the run validity guard.
            print(f"sdo-agent-resolved: could not record resolution: {exc}", file=sys.stderr)
    return resolved
