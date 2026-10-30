"""Pinned-tools lanes: prove the launched controller may reuse publication attestations.

The native broker reuses a trusted validation only when the controller runs without
the development mount and both its validator and compiler (toolchain) images are
immutable digest references (``sdo/operator/runtime/native_launch.go``). A lane that
launches from mutable tags or a dev mount silently validates every publication twice.
This module reads the controller Job the Go installer actually rendered and refuses
injection when that Job could not reuse an attestation.
"""

from __future__ import annotations

import json
import re
import subprocess
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

CONTROLLER_JOB = "sdo-controller-run"
_DIGEST_REFERENCE = re.compile(r"^[^@\s]+@sha256:[0-9a-f]{64}$")


class AttestationIneligibleError(RuntimeError):
    """A pinned-tools lane launched a controller that cannot reuse publication attestations."""

    reason = "attestation_ineligible_launch"

    def __init__(self, problems: tuple[str, ...]) -> None:
        self.problems = problems
        super().__init__("pinned-tools lane cannot reuse attestations: " + "; ".join(problems))


def immutable_image(reference: str) -> bool:
    """Mirror of the Go broker's predicate: a ``name@sha256:<64 hex>`` reference."""

    return bool(_DIGEST_REFERENCE.fullmatch(reference))


def _flag(args: list[str], name: str) -> str | None:
    for index, value in enumerate(args):
        if value == name and index + 1 < len(args):
            return args[index + 1]
        if value.startswith(name + "="):
            return value.removeprefix(name + "=")
    return None


def attestation_problems(job: Mapping[str, Any]) -> tuple[str, ...]:
    """Every reason the rendered controller Job disables attestation reuse; empty when eligible."""

    spec = job.get("spec", {}).get("template", {}).get("spec", {})
    containers = list(spec.get("containers", []))
    if not containers:
        return ("controller Job has no containers",)
    problems: list[str] = []
    controller = containers[0]
    args = [*controller.get("command", []), *controller.get("args", [])]
    for flag, role in (("--validator-image", "validator"), ("--toolchain-image", "compiler")):
        image = _flag(args, flag)
        if image is None:
            problems.append(f"controller has no {flag}")
        elif not immutable_image(image):
            problems.append(f"{role} image is not an immutable digest: {image}")
    if not immutable_image(str(controller.get("image", ""))):
        problems.append(f"controller image is not an immutable digest: {controller.get('image')}")
    if _flag(args, "--code-path") == "shell":
        problems.append("controller uses the shell memory code path")
    for container in [*spec.get("initContainers", []), *containers]:
        if any(env.get("name") == "SDO_DEV_MOUNT" and env.get("value") == "1" for env in container.get("env", [])):
            problems.append(f"container {container.get('name')!r} runs with SDO_DEV_MOUNT=1")
        if any(str(mount.get("name", "")).startswith("dev-") for mount in container.get("volumeMounts", [])):
            problems.append(f"container {container.get('name')!r} mounts development sources")
    return tuple(problems)


def assert_attestation_eligible(
    kubeconfig: str | None,
    control_namespace: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> None:
    """Read the live controller Job and fail before injection unless it can reuse attestations."""

    command = ["kubectl", "-n", control_namespace, "get", "job", CONTROLLER_JOB, "-o", "json"]
    if kubeconfig:
        command[1:1] = ["--kubeconfig", kubeconfig]
    completed = (run or subprocess.run)(command, capture_output=True, text=True, check=False, timeout=60)
    if completed.returncode != 0:
        raise AttestationIneligibleError((f"controller Job is unreadable: {completed.stderr.strip()}",))
    problems = attestation_problems(json.loads(completed.stdout))
    if problems:
        raise AttestationIneligibleError(problems)
