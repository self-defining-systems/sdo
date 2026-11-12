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
#: Model variable by role: ``responder``, ``reflection``, ``lifecycle`` (designer, architect) or ``deployment``.
MODEL_ENV: dict[str, str] = dict(_ENVIRONMENT["models"])
#: The one reasoning effort every SDO agent (deployer, detection designer, responder, reflection) runs at. The
#: controller sets it in every worker Job from the operator config; a worker has no default of its own.
EFFORT_ENV: str = _ENVIRONMENT["effort"]
AGENT_EFFORTS: tuple[str, ...] = tuple(_ENVIRONMENT["efforts"])
#: Seconds one lifecycle session (detection designer) may run; a slower effort or a larger app needs more.
LIFECYCLE_TIMEOUT_ENV: str = _ENVIRONMENT["lifecycle_timeout"]
#: Seconds one ``sdo-operator memory check`` a worker runs on its checkout may take.
MEMORY_CHECK_TIMEOUT_ENV: str = _ENVIRONMENT["memory_check_timeout"]
SESSION_DIR_ENV = "SDO_SESSION_DIR"
RESOLVED_LOG_NAME = "agent-resolved.jsonl"


@dataclass(frozen=True)
class ResolvedAgent:
    role: str
    provider: str
    model: str | None
    effort: str

    def __post_init__(self) -> None:
        if self.provider not in AGENT_PROVIDERS:
            raise ValueError(f"unsupported agent provider {self.provider!r}")
        if self.effort not in AGENT_EFFORTS:
            raise ValueError(f"unsupported agent effort {self.effort!r} ({EFFORT_ENV} must be one of {AGENT_EFFORTS})")


def agent_effort(environ: Mapping[str, str] | None = None) -> str:
    """The configured effort every SDO agent runs at; the controller always sets it."""

    environ = os.environ if environ is None else environ
    effort = environ.get(EFFORT_ENV, "").strip().lower()
    if not effort:
        raise ValueError(f"{EFFORT_ENV} is not set; the controller sets it in every worker from the operator config")
    if effort not in AGENT_EFFORTS:
        raise ValueError(f"unsupported agent effort {effort!r} ({EFFORT_ENV} must be one of {AGENT_EFFORTS})")
    return effort


def lifecycle_timeout_seconds(environ: Mapping[str, str] | None = None) -> int:
    """The configured lifecycle session limit; the controller always sets it."""

    return positive_seconds(LIFECYCLE_TIMEOUT_ENV, "lifecycle timeout", environ)


def memory_check_timeout_seconds(environ: Mapping[str, str] | None = None) -> int:
    """The configured limit of one memory check; the controller always sets it."""

    return positive_seconds(MEMORY_CHECK_TIMEOUT_ENV, "memory check timeout", environ)


def positive_seconds(name: str, what: str, environ: Mapping[str, str] | None = None) -> int:
    """Whole positive seconds from the worker variable *name*, which the controller sets from the operator config."""

    environ = os.environ if environ is None else environ
    raw = environ.get(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is not set; the controller sets the {what} in every worker from the operator config")
    if not raw.isdigit() or int(raw) <= 0:
        raise ValueError(f"unsupported {what} {raw!r} ({name} must be positive whole seconds)")
    return int(raw)


def resolve_agent(role: str, model: str, environ: Mapping[str, str] | None = None) -> ResolvedAgent:
    """Resolve *role*'s provider, model (*model* is a key of ``MODEL_ENV``) and effort, report them, and record them.

    The resolution is recorded beside the incident session.

    An unset provider means Codex; the effort must be set and known, so a worker refuses to run at an effort other
    than the configured one.
    """

    environ = os.environ if environ is None else environ
    model_env = MODEL_ENV[model]
    provider = environ.get(PROVIDER_ENV, "").strip().lower() or "codex"
    if provider not in AGENT_PROVIDERS:
        raise ValueError(
            f"unsupported agent provider {provider!r} for {role} ({PROVIDER_ENV} must be one of {AGENT_PROVIDERS})"
        )
    effort = agent_effort(environ)
    resolved = ResolvedAgent(role, provider, environ.get(model_env, "").strip() or None, effort)
    print(
        f"sdo-agent-resolved role={role} provider={provider} model={resolved.model or 'default'} effort={effort}",
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
            "effort": effort,
        }
        try:
            with (Path(session_dir) / RESOLVED_LOG_NAME).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
        except OSError as exc:
            # The stderr line above is the primary report; the record only feeds the run validity guard.
            print(f"sdo-agent-resolved: could not record resolution: {exc}", file=sys.stderr)
    return resolved
