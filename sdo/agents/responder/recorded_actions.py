"""Repair-action receipts recorded by ``sdo act`` and merged into the responder's result.

The responder runs each live mutation as one ``sdo act`` call. The CLI executes the
command, writes its receipt (target, resources, timing, outcome and the exact command
line) to the session scratch directory, and the worker adds the receipts to the
``IncidentResult`` after the model's turn. The model therefore never hand-writes a
receipt or a wrapper script, and reflection still sees every mutation with its parameters.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from sdo.contracts import IncidentResult, RepairActionReceipt, validate

if TYPE_CHECKING:
    from collections.abc import Sequence

ACTIONS_FILE = "repair-actions.jsonl"
DEFAULT_SCRATCH = "/tmp/sdo-session-scratch"
_DETAILS_OUTPUT_CHARS = 400

#: Kinds a responder may report as mutated, with the apiVersion the receipt needs.
API_VERSIONS = {
    "ConfigMap": "v1",
    "Secret": "v1",
    "Service": "v1",
    "ServiceAccount": "v1",
    "PersistentVolumeClaim": "v1",
    "Pod": "v1",
    "Namespace": "v1",
    "Deployment": "apps/v1",
    "StatefulSet": "apps/v1",
    "DaemonSet": "apps/v1",
    "ReplicaSet": "apps/v1",
    "Job": "batch/v1",
    "CronJob": "batch/v1",
    "Ingress": "networking.k8s.io/v1",
    "NetworkPolicy": "networking.k8s.io/v1",
    "HorizontalPodAutoscaler": "autoscaling/v2",
}
_KUBECTL_VALUE_FLAGS = {"-n", "--namespace", "--context", "--kubeconfig", "-l", "--selector", "-f", "--filename"}
_KUBECTL_COMPOUND_VERBS = {"set", "rollout"}


def scratch_directory() -> Path:
    return Path(os.environ.get("SDO_SESSION_SCRATCH") or DEFAULT_SCRATCH)


def actions_path() -> Path:
    return scratch_directory() / ACTIONS_FILE


def reset() -> None:
    """Forget receipts from an earlier incident that shared this session directory."""

    actions_path().unlink(missing_ok=True)


def resource_ref(value: str) -> dict[str, str]:
    """Parse ``Kind/namespace/name`` into the receipt's object reference."""

    parts = value.split("/")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"resource must be Kind/namespace/name: {value!r}")
    kind, namespace, name = parts
    if kind not in API_VERSIONS:
        raise ValueError(f"unsupported resource kind {kind!r}; use one of {', '.join(sorted(API_VERSIONS))}")
    return {"api_version": API_VERSIONS[kind], "kind": kind, "namespace": namespace, "name": name}


def action_kind(command: Sequence[str]) -> str:
    """``kubectl-<verb>`` for kubectl commands, ``command-<program>`` otherwise."""

    program = Path(command[0]).name
    if program != "kubectl":
        return f"command-{program}"
    words: list[str] = []
    skip_value = False
    for token in command[1:]:
        if skip_value:
            skip_value = False
        elif token in _KUBECTL_VALUE_FLAGS:
            skip_value = True
        elif not token.startswith("-"):
            words.append(token)
    if not words:
        return "kubectl"
    if words[0] in _KUBECTL_COMPOUND_VERBS and len(words) > 1:
        return f"kubectl-{words[0]}-{words[1]}"
    return f"kubectl-{words[0]}"


def _timestamp(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _next_action_id(path: Path) -> str:
    count = len(path.read_text(encoding="utf-8").splitlines()) if path.exists() else 0
    return f"action-{count + 1}"


def run_and_record(
    command: Sequence[str],
    resources: Sequence[dict[str, str]],
    *,
    summary: str | None = None,
    reversible: bool = True,
) -> int:
    """Run *command*, print its output, append its receipt, and return its exit code."""

    if not command:
        raise ValueError("a command is required after --")
    if not resources:
        raise ValueError("at least one --resource is required")
    path = actions_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    action_id = _next_action_id(path)
    started = datetime.now(UTC)
    try:
        completed = subprocess.run(list(command), capture_output=True, text=True, check=False)
        code, output = completed.returncode, completed.stdout + completed.stderr
    except OSError as exc:
        code, output = 127, f"{exc}\n"
    finished = datetime.now(UTC)
    sys.stdout.write(output)
    target = "{kind}/{namespace}/{name}".format(**resources[0])
    kind = action_kind(command)
    clipped = output.strip()[-_DETAILS_OUTPUT_CHARS:]
    if any(resource["kind"] == "Secret" for resource in resources):
        details = f"exit {code}; the command line and output are withheld because a Secret is involved"
    else:
        details = f"$ {shlex.join(command)}\nexit {code}" + (f"\n{clipped}" if clipped else "")
    receipt: dict[str, Any] = {
        "action_id": action_id,
        "kind": kind,
        "target": target,
        "resources": list(resources),
        "summary": summary or f"{kind} {target}",
        "details": details,
        "started_at": _timestamp(started),
        "completed_at": _timestamp(finished),
        "success": code == 0,
        "reversible": reversible,
    }
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(receipt, sort_keys=True) + "\n")
    return code


def merge_into(result: IncidentResult) -> None:
    """Add the recorded receipts the model did not already return, then revalidate."""

    path = actions_path()
    if not path.exists():
        return
    known = {action.action_id for action in result.repair_actions}
    added = False
    for line in path.read_text(encoding="utf-8").splitlines():
        receipt = json_format.ParseDict(json.loads(line), RepairActionReceipt())
        if receipt.action_id not in known:
            result.repair_actions.append(receipt)
            known.add(receipt.action_id)
            added = True
    if added:
        validate(result)
