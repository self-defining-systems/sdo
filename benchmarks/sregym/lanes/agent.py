"""The agent of a lanes experiment: scripted (model-free) or a real SDO agent, from the spec's ``operator.settings``.

The settings are the ones the fuzzops hooks take (``provider``, ``model``, ``effort``, ``auth_file``, ``config``), so
one spec shape drives a real agent on either workload. Settings without a model (or ``model: stub``) are the scripted
agent: ``fastloop run`` gets the scripted registrations and the host's placeholder credentials hook. With a model the
row runs SDO's own responder and closing turn:

- ``fastloop run --provider P --model M --reasoning-effort E --config OVERLAY``: ``OVERLAY`` is ``settings.config``
  (``agent.extra_env`` for a Vertex-backed Claude, ``response.after_playbook``, ...), written beside the run;
- ``--credentials-hook credentials_hook.sh``: after each row-start wipe it puts ``settings.auth_file`` into the
  ``sdo-agent-credentials`` Secret by file reference (this module never reads the file);
- no ``--agent-registrations``: the production responder registrations in the lane's responder image, which must be a
  plain (not scripted-worker) image: ``settings.responder_image`` / ``settings.controller_image`` name one for ``up``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sdo.agents.provider import AGENT_EFFORTS

if TYPE_CHECKING:
    from collections.abc import Mapping

PROVIDERS = ("claude", "codex")
STUB_MODEL = "stub"
CREDENTIALS_HOOK = Path(__file__).with_name("credentials_hook.sh")
#: The Secret key the agent pod reads its credential from, per provider (``fuzzops.install.provision_namespaces``).
CREDENTIAL_KEYS = {"claude": ".credentials.json", "codex": "auth.json"}
DEFAULT_AUTH_FILES = {"claude": "~/.claude/.credentials.json", "codex": "~/.codex/auth.json"}


@dataclass(frozen=True)
class AgentSettings:
    """A real agent's settings; ``from_operator`` returns ``None`` for the scripted one."""

    provider: str
    model: str
    auth_file: Path
    effort: str = ""
    #: ``settings.config``: the operator config overlay (``docs/config.md``) passed to ``fastloop run --config``.
    config: dict[str, Any] = field(default_factory=dict)
    #: Full image references for ``fastloop up`` (``--responder-image`` / ``--controller-image``); empty: the host's.
    responder_image: str = ""
    controller_image: str = ""

    def __post_init__(self) -> None:
        if self.provider not in PROVIDERS:
            raise ValueError(f"operator.settings.provider must be one of {PROVIDERS}, not {self.provider!r}")
        if not self.model:
            raise ValueError("operator.settings.model is required with a provider")
        if self.effort and self.effort not in AGENT_EFFORTS:
            raise ValueError(f"operator.settings.effort must be one of {AGENT_EFFORTS}, not {self.effort!r}")
        if not isinstance(self.config, dict):
            raise TypeError("operator.settings.config must be an object (an operator config overlay)")
        if not self.auth_file.is_absolute():
            raise ValueError(f"operator.settings.auth_file must be an absolute path, not {str(self.auth_file)!r}")

    @classmethod
    def from_operator(cls, path: Path | None) -> AgentSettings | None:
        """The agent the operator file (``BENCH_OPERATOR``) names; ``None``: the scripted agent."""
        if path is None:
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
        settings = raw.get("settings") or {}
        if not isinstance(settings, dict):
            raise TypeError("operator.settings must be an object")
        model = str(settings.get("model") or "")
        provider = str(settings.get("provider") or "")
        if model == STUB_MODEL or not (model or provider):
            return None
        auth = str(settings.get("auth_file") or DEFAULT_AUTH_FILES.get(provider, ""))
        return cls(
            provider=provider,
            model=model,
            effort=str(settings.get("effort") or ""),
            auth_file=Path(auth).expanduser(),
            config=settings.get("config", {}),
            responder_image=str(settings.get("responder_image") or ""),
            controller_image=str(settings.get("controller_image") or ""),
        )

    @property
    def credential_key(self) -> str:
        return CREDENTIAL_KEYS[self.provider]

    def hook_env(self) -> dict[str, str]:
        """What the credentials hook needs: a reference to the file and the Secret key, never a value."""
        return {"SREGYM_LANES_AUTH_FILE": str(self.auth_file), "SREGYM_LANES_AUTH_KEY": self.credential_key}

    def run_flags(self, overlay: Path) -> list[str]:
        """The ``fastloop run`` flags of the agent; writes the config overlay to ``overlay``."""
        overlay.parent.mkdir(parents=True, exist_ok=True)
        # JSON is YAML: the overlay is read by the operator's config loader.
        overlay.write_text(json.dumps(self.config, indent=1) + "\n", encoding="utf-8")
        flags = ["--provider", self.provider, "--model", self.model]
        if self.effort:
            flags += ["--reasoning-effort", self.effort]
        return [*flags, "--config", str(overlay), "--credentials-hook", str(CREDENTIALS_HOOK)]

    def lane_refusal(self, *, virtual: bool, nfs_pool: bool = False) -> str:
        """Why this agent's model cannot run on the lane ("" when it can): an Opus session is admitted through a
        node-local pool (or an NFS-homed one, ``nfs_pool``), and a virtual lane has no node to provision it on."""
        if not virtual or nfs_pool or "opus" not in self.model.lower():
            return ""
        return (
            f"model {self.model} needs a node-local Opus session pool, and a virtual (vcluster) lane has no node it "
            "can provision one on: give the pool an NFS home (SDO_OPUS_POOL_NFS_SERVER and SDO_OPUS_POOL_NFS_PATH) "
            "or run the row on a lane with its own nodes"
        )

    def up_flags(self) -> list[str]:
        """``fastloop up`` flags that name the lane's operator images (after the host's, which they override)."""
        flags: list[str] = []
        if self.controller_image:
            flags += ["--controller-image", self.controller_image]
        if self.responder_image:
            flags += ["--responder-image", self.responder_image]
        return flags

    def image_mismatch(self, built: Mapping[str, str]) -> str:
        """Why a lane built with the images ``built`` (``fastloop.json`` ``images``) is not the spec's (``""``: it is).

        ``up`` is skipped on a reused lane, so a lane built for another agent keeps its images; a scripted-worker
        responder image under a real agent would fail at its first task, an hour in."""
        for role, wanted in (("controller", self.controller_image), ("responder", self.responder_image)):
            if wanted and built.get(role) != wanted:
                return f"the lane was built with {role} image {built.get(role)!r}, the spec names {wanted!r}"
        return ""

    def summary(self) -> dict[str, str]:
        """The non-secret part, for the subject's prepare/swap answer."""
        return {"provider": self.provider, "model": self.model, "effort": self.effort}


def settings_for(env: Mapping[str, str]) -> AgentSettings | None:
    """The agent the call's operator file names; a bad setting, or a model this lane cannot run, is an
    ``AdapterError`` (named before anything is built)."""
    from benchmarks.sregym.lanes.workload import AdapterError

    operator = env.get("BENCH_OPERATOR")
    try:
        settings = AgentSettings.from_operator(Path(operator) if operator else None)
    except (TypeError, ValueError) as error:
        raise AdapterError(f"operator settings: {error}") from error
    if settings is not None and (
        refusal := settings.lane_refusal(
            virtual=env.get("BENCH_LANE_VIRTUAL") == "1", nfs_pool=bool(env.get("SDO_OPUS_POOL_NFS_SERVER", "").strip())
        )
    ):
        raise AdapterError(refusal)
    return settings
