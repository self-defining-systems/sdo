"""Repair-action receipts recorded by ``sdo act`` and merged into the responder's result.

The responder runs each live mutation as one ``sdo act`` call. The CLI executes the
command, writes its receipt (target, resources, timing, outcome and the exact command
line) to the session scratch directory, and the worker adds the receipts to the
``IncidentResult`` after the model's turn. The model therefore never hand-writes a
receipt or a wrapper script, and reflection still sees every mutation with its parameters.

Each call appends a ``started`` record before the command runs and a ``finished`` record after
it, so a call killed mid-command still leaves a receipt of an unfinished mutation.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from sdo.contracts import IncidentResult, RepairActionReceipt

_LOG = logging.getLogger(__name__)

ACTIONS_FILE = "repair-actions.jsonl"
DEFAULT_SCRATCH = "/tmp/sdo-session-scratch"
_DETAILS_OUTPUT_CHARS = 400
_STARTED, _FINISHED = "started", "finished"

#: Mirror of ``objectKinds`` in ``sdo/operator/sdk/object_ref.go``: kubectl alias -> canonical kind.
#: ``test_recorded_actions.py`` checks the two tables are equal.
OBJECT_KINDS = {
    "clusterrole": "ClusterRole", "clusterroles": "ClusterRole",
    "clusterrolebinding": "ClusterRoleBinding", "clusterrolebindings": "ClusterRoleBinding",
    "cm": "ConfigMap", "configmap": "ConfigMap", "configmaps": "ConfigMap",
    "cj": "CronJob", "cronjob": "CronJob", "cronjobs": "CronJob",
    "ds": "DaemonSet", "daemonset": "DaemonSet", "daemonsets": "DaemonSet",
    "deploy": "Deployment", "deployment": "Deployment", "deployments": "Deployment",
    "hpa": "HorizontalPodAutoscaler", "horizontalpodautoscaler": "HorizontalPodAutoscaler",
    "horizontalpodautoscalers": "HorizontalPodAutoscaler",
    "ing": "Ingress", "ingress": "Ingress", "ingresses": "Ingress",
    "job": "Job", "jobs": "Job",
    "netpol": "NetworkPolicy", "networkpolicy": "NetworkPolicy", "networkpolicies": "NetworkPolicy",
    "pvc": "PersistentVolumeClaim", "persistentvolumeclaim": "PersistentVolumeClaim",
    "persistentvolumeclaims": "PersistentVolumeClaim",
    "po": "Pod", "pod": "Pod", "pods": "Pod",
    "rs": "ReplicaSet", "replicaset": "ReplicaSet", "replicasets": "ReplicaSet",
    "role": "Role", "roles": "Role", "rolebinding": "RoleBinding", "rolebindings": "RoleBinding",
    "secret": "Secret", "secrets": "Secret",
    "sa": "ServiceAccount", "serviceaccount": "ServiceAccount", "serviceaccounts": "ServiceAccount",
    "svc": "Service", "service": "Service", "services": "Service",
    "sts": "StatefulSet", "statefulset": "StatefulSet", "statefulsets": "StatefulSet",
}  # fmt: skip
#: Mirror of ``objectAPIVersion`` in the same file; other kinds keep an empty apiVersion, as in Go.
API_VERSIONS = {
    "Deployment": "apps/v1",
    "StatefulSet": "apps/v1",
    "DaemonSet": "apps/v1",
    "ReplicaSet": "apps/v1",
    "Job": "batch/v1",
    "CronJob": "batch/v1",
    "Ingress": "networking.k8s.io/v1",
    "NetworkPolicy": "networking.k8s.io/v1",
    "HorizontalPodAutoscaler": "autoscaling/v2",
    "Role": "rbac.authorization.k8s.io/v1",
    "RoleBinding": "rbac.authorization.k8s.io/v1",
    "ClusterRole": "rbac.authorization.k8s.io/v1",
    "ClusterRoleBinding": "rbac.authorization.k8s.io/v1",
    "ConfigMap": "v1",
    "Secret": "v1",
    "Pod": "v1",
    "Service": "v1",
    "ServiceAccount": "v1",
    "PersistentVolumeClaim": "v1",
}
_KUBECTL_VALUE_FLAGS = {"-n", "--namespace", "--context", "--kubeconfig", "-l", "--selector", "-f", "--filename"}
_KUBECTL_COMPOUND_VERBS = {"set", "rollout"}
_REF_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
#: Text whose command line and output must not reach receipts, outcomes or reflection: Secrets, literal or
#: file-sourced values, environment changes and credential-looking assignments.
_SENSITIVE = re.compile(
    r"secret|--from-literal|--from-file|--from-env-file"
    r"|\benv\b|create\s+token|printenv\b"
    r"|(?:pass(?:wo?r?d)?|api[_-]?key|token|credential|secret)\w*[\"']?\s*[:=]",
    re.IGNORECASE,
)
_SDO_ACT_LINE = re.compile(r"\bsdo\s+act\b|\btools_cli(?:\.py)?\s+act\b")


class RecordedActionsError(ValueError):
    """The recorded repair-action file holds a receipt that cannot be merged into the result."""


def scratch_directory() -> Path:
    return Path(os.environ.get("SDO_SESSION_SCRATCH") or DEFAULT_SCRATCH)


def actions_path() -> Path:
    return scratch_directory() / ACTIONS_FILE


def reset() -> None:
    """Forget receipts from an earlier incident that shared this session directory."""

    actions_path().unlink(missing_ok=True)


def canonical_kind(raw: str) -> str:
    """Go's ``CanonicalObjectKind``: aliases and group-qualified kinds resolve, unknown kind spelling is preserved."""

    kind = raw.strip().lower().split(".", 1)[0]
    return OBJECT_KINDS.get(kind, raw.strip().split(".", 1)[0])


def resource_ref(value: str) -> dict[str, str]:
    """Receipt projection of the shared Go citation grammar; pinned by the same corpus."""

    def reject() -> ValueError:
        example = "Kind/namespace/name"
        candidate = re.split(r"[,;\s]+", value.strip(), maxsplit=1)[0]
        parts = candidate.split("/")
        if len(parts) in (2, 3) and all(_REF_COMPONENT.fullmatch(part) for part in parts):
            example = candidate
        try:
            fields = json.loads(value)
            if isinstance(fields, dict):
                components = [fields.get("kind", "Kind"), fields.get("namespace", ""), fields.get("name", "name")]
                components = [part for part in components if isinstance(part, str) and _REF_COMPONENT.fullmatch(part)]
                if components:
                    example = "/".join(components)
        except ValueError:
            pass
        return ValueError(
            "resource must reference exactly one object: Kind/name, Kind/namespace/name, "
            f"or JSON {{kind,name,namespace?,api_version?}}; example: {example}"
        )

    def unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, field in pairs:
            key = "api_version" if key == "apiVersion" else key
            if key in result:
                raise reject()
            result[key] = field
        return result

    text = value.strip()
    if text.startswith("{"):
        try:
            fields = json.loads(text, object_pairs_hook=unique_fields)
        except ValueError as exc:
            raise reject() from exc
        if set(fields) - {"api_version", "kind", "namespace", "name"} or not all(
            isinstance(field, str) for field in fields.values()
        ):
            raise reject()
        kind, name, namespace = fields.get("kind", ""), fields.get("name", ""), fields.get("namespace", "")
        version = fields.get("api_version")
        if (
            not _REF_COMPONENT.fullmatch(kind)
            or not _REF_COMPONENT.fullmatch(name)
            or (namespace and not _REF_COMPONENT.fullmatch(namespace))
        ):
            raise reject()
    else:
        version = None
        if "@" in text:
            version, text = text.split("@", 1)
        parts = text.split("/")
        if len(parts) not in (2, 3) or not all(_REF_COMPONENT.fullmatch(part) for part in parts):
            raise reject()
        kind, name = parts[0], parts[-1]
        namespace = parts[1] if len(parts) == 3 else ""
        first_known, second_known = (part.lower().split(".", 1)[0] in OBJECT_KINDS for part in parts[:2])
        if len(parts) == 3 and not first_known and second_known:
            kind, namespace = parts[1], parts[0]
    kind = canonical_kind(kind)
    if version is None:
        version = API_VERSIONS.get(kind, "")
    if version and (
        len(version.split("/")) > 2 or not all(_REF_COMPONENT.fullmatch(part) for part in version.split("/"))
    ):
        raise reject()
    return {"api_version": version, "kind": kind, "namespace": namespace, "name": name}


def withholds_command(text: str) -> bool:
    """Whether a command line or free text may carry secret values and so must not be recorded."""

    return _SENSITIVE.search(text) is not None


def scrub_shell_command(line: str) -> str:
    """Replace a recorded ``sdo act`` shell line that may carry secret values with a placeholder."""

    if _SDO_ACT_LINE.search(line) and withholds_command(line):
        return "sdo act [command withheld: it may carry secret values]"
    return line


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


def _executed_resources(command: Sequence[str], declared: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    """Keep only explicit kubectl patch/delete targets, never arbitrary attribution."""
    if action_kind(command) not in {"kubectl-patch", "kubectl-delete"}:
        return [dict(resource) for resource in declared]
    words: list[str] = []
    namespace: str | None = None
    flags = _KUBECTL_VALUE_FLAGS | {
        "-p",
        "--patch",
        "--type",
        "--field-manager",
        "--timeout",
        "--request-timeout",
        "--grace-period",
        "--cascade",
    }
    args = iter(command[1:])
    for token in args:
        if token in flags:
            value = next(args, "")
            if token in {"-n", "--namespace"}:
                namespace = value
        elif token.startswith("--namespace="):
            namespace = token.split("=", 1)[1]
        elif not token.startswith("-"):
            words.append(token)
    # kubectl kind matching is case-insensitive even for kinds absent from the
    # alias table; keep the caller's original spelling in the actual receipt.
    targets: set[tuple[str, str]] = set()
    if len(words) > 1:
        arguments = words[1:]
        if "/" in arguments[0]:
            targets = {
                (canonical_kind(word.split("/", 1)[0]).casefold(), word.split("/", 1)[1])
                for word in arguments
                if word.count("/") == 1
            }
        elif len(arguments) > 1 and "," not in arguments[0]:
            names = arguments[1:] if words[0] == "delete" else arguments[1:2]
            targets = {(canonical_kind(arguments[0]).casefold(), name) for name in names}
    return [
        dict(resource)
        for resource in declared
        if (resource["kind"].casefold(), resource["name"]) in targets
        and (not resource["namespace"] or resource["namespace"] == namespace)
    ]


def _timestamp(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _unmentioned_kubectl_objects(command: Sequence[str], resources: Sequence[dict[str, str]]) -> list[str]:
    """``kind/name`` words in a kubectl command that no ``--resource`` covers."""

    if Path(command[0]).name != "kubectl":
        return []
    covered = {(resource["kind"], resource["name"]) for resource in resources}
    missing: list[str] = []
    skip_value = False
    for token in command[1:]:
        if skip_value:
            skip_value = False
        elif token in _KUBECTL_VALUE_FLAGS:
            skip_value = True
        elif not token.startswith("-") and token.count("/") == 1:
            kind, name = token.split("/")
            if canonical_kind(kind) in API_VERSIONS and (canonical_kind(kind), name) not in covered:
                missing.append(token)
    return missing


@contextlib.contextmanager
def _sigterm_kills_group(children: list[subprocess.Popen[str]]) -> Iterator[None]:
    """Turn SIGTERM into ``SystemExit`` after killing the running command's process group, grandchildren included."""

    def stop(signum: int, _frame: object) -> None:
        for child in children:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
        raise SystemExit(128 + signum)

    try:
        previous = signal.signal(signal.SIGTERM, stop)
    except ValueError:  # not the main thread
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _run_in_own_group(command: Sequence[str]) -> tuple[int, str]:
    """Run *command* in its own session so SIGTERM can stop it and everything it spawned."""

    children: list[subprocess.Popen[str]] = []
    with _sigterm_kills_group(children):
        try:
            child = subprocess.Popen(
                list(command),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                start_new_session=True,
            )
        except OSError as exc:
            return 127, f"{exc}\n"
        children.append(child)
        output, _ = child.communicate()
        return child.returncode, output


def _receipt(
    action_id: str,
    command: Sequence[str],
    resources: Sequence[dict[str, str]],
    *,
    summary: str | None,
    reversible: bool,
    started: datetime,
    finished: datetime,
    code: int | None,
    output: str,
) -> dict[str, Any]:
    try:
        for argument in command:
            argument.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("cannot record an invalid repair receipt: command contains invalid UTF-8") from None
    kind = action_kind(command)
    resources = _executed_resources(command, resources)
    target = (
        kind
        if not resources
        else (
            "{kind}/{namespace}/{name}".format(**resources[0])
            if resources[0]["namespace"]
            else ("{kind}/{name}".format(**resources[0]))
        )
    )
    withheld = (
        any(resource["kind"] == "Secret" for resource in resources)
        or withholds_command(shlex.join(command))
        or withholds_command(summary or "")
    )
    state = "interrupted before the command finished" if code is None else f"exit {code}"
    if withheld:
        details = f"{state}; the command line and output are withheld because they may carry secret values"
    else:
        clipped = output.strip()[-_DETAILS_OUTPUT_CHARS:]
        details = f"$ {shlex.join(command)}\n{state}" + (f"\n{clipped}" if clipped else "")
    receipt: dict[str, Any] = {
        "action_id": action_id,
        "kind": kind,
        "target": target,
        "resources": [dict(resource) for resource in resources],
        "summary": f"{kind} {target}" if not summary or withholds_command(summary) else summary,
        "details": details,
        "started_at": _timestamp(started),
        "completed_at": _timestamp(finished),
        "success": code == 0,
        "reversible": reversible,
        "failure_reason": (
            "outcome_unknown"
            if code is None
            else "authorization_denied"
            if code != 0 and ("(Forbidden)" in output or "(Unauthorized)" in output)
            else "execution_failed"
            if code != 0
            else ""
        ),
    }
    # Protobuf loads only when an action is recorded, so `sdo --help` and the other commands stay fast.
    from google.protobuf import json_format

    from sdo.contracts import RepairActionReceipt

    try:
        json_format.ParseDict(receipt, RepairActionReceipt())
    except json_format.ParseError as exc:
        raise ValueError(f"cannot record an invalid repair receipt: {exc}") from exc
    return receipt


def _append(path: Path, state: str, receipt: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"state": state, "receipt": receipt}, sort_keys=True) + "\n")


def _confirm_deleted(command: Sequence[str], resources: Sequence[dict[str, str]]) -> str:
    """A successful command does not prove every declared resource disappeared."""
    if not resources:
        return "delete_not_confirmed"
    context: list[str] = []
    flags = {"-n", "--namespace", "--context", "--kubeconfig", "--server", "--cluster", "--user", "--as", "--as-group"}
    args = iter(command[1:])
    for arg in args:
        if arg in flags:
            context.extend((arg, next(args, "")))
        elif arg.split("=", 1)[0] in flags:
            context.append(arg)
    deadline = time.monotonic() + 5
    for resource in resources:
        if resource["kind"].lower() == "secret":
            return "delete_not_confirmed"
        argv = [
            command[0],
            *context,
            "get",
            resource["kind"],
            resource["name"],
            "--ignore-not-found=true",
            "-o",
            "name",
            "--request-timeout=5s",
        ]
        if resource["namespace"]:
            argv.extend(("--namespace", resource["namespace"]))
        try:
            observed = subprocess.run(
                argv, capture_output=True, text=True, timeout=max(0.01, deadline - time.monotonic()), check=False
            )
        except (OSError, subprocess.TimeoutExpired):
            return "outcome_unknown"
        if observed.returncode:
            return (
                "authorization_denied"
                if "(Forbidden)" in observed.stderr or "(Unauthorized)" in observed.stderr
                else "delete_not_confirmed"
            )
        if observed.stdout.strip():
            return "delete_not_confirmed"
    return ""


@contextlib.contextmanager
def _mutation_lease() -> Iterator[dict[str, Any] | None]:
    directory = os.environ.get("SDO_API_HISTORY_DIR")
    if not directory:
        yield None
        return
    # The worker's existing history mount is read-only. Shared flock requires
    # only read access; the controller owns the lock inode and verification marker.
    with (Path(directory) / "mutation.lock").open("rb") as lease:
        fcntl.flock(lease, fcntl.LOCK_SH)
        marker = Path(directory) / "incident-verified.json"
        warning = json.loads(marker.read_text()) if marker.exists() else None
        yield warning


def run_and_record(
    command: Sequence[str],
    resources: Sequence[dict[str, str]],
    *,
    summary: str | None = None,
    reversible: bool = True,
    on_completed: Callable[[int, str, dict[str, Any]], None] | None = None,
) -> int:
    """Hold the incident write lease through execution and final receipt publication."""
    with _mutation_lease() as warning:
        if warning is not None:
            now = datetime.now(UTC)
            receipt = _receipt(
                f"sdo-act-{uuid.uuid4()}",
                command,
                resources,
                summary=summary,
                reversible=reversible,
                started=now,
                finished=now,
                code=1,
                output="incident_verified",
            )
            receipt["failure_reason"] = "incident_verified"
            path = actions_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            _append(path, _FINISHED, receipt)
            if on_completed is not None:
                on_completed(1, "incident_verified", receipt)
            return 1
        return _run_and_record(command, resources, summary=summary, reversible=reversible, on_completed=on_completed)


def _run_and_record(
    command: Sequence[str],
    resources: Sequence[dict[str, str]],
    *,
    summary: str | None = None,
    reversible: bool = True,
    on_completed: Callable[[int, str, dict[str, Any]], None] | None = None,
) -> int:
    """Run *command*, print its output, record its receipt, and return its exit code.

    A ``started`` record is written first and a ``finished`` record last, also when the call is
    killed by SIGTERM, so an unfinished mutation is never missing from the receipts.
    """

    if not command:
        raise ValueError("a command is required after --")
    if not resources:
        raise ValueError("at least one --resource is required")
    for missing in _unmentioned_kubectl_objects(command, resources):
        print(f"sdo: warning: {missing} is in the command but no --resource names it", file=sys.stderr)
    path = actions_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    action_id = f"sdo-act-{uuid.uuid4()}"
    started = datetime.now(UTC)

    def receipt(code: int | None, output: str, finished: datetime) -> dict[str, Any]:
        return _receipt(
            action_id,
            command,
            resources,
            summary=summary,
            reversible=reversible,
            started=started,
            finished=finished,
            code=code,
            output=output,
        )

    _append(path, _STARTED, receipt(None, "", started))
    code: int | None = None
    output = ""
    try:
        code, output = _run_in_own_group(command)
    finally:
        completed = receipt(code, output, datetime.now(UTC))
        if code == 0 and action_kind(command) == "kubectl-delete":
            reason = _confirm_deleted(command, _executed_resources(command, resources))
            if reason:
                completed["success"] = False
                completed["failure_reason"] = reason
        _append(path, _FINISHED, completed)
    assert code is not None  # an interrupted call raised out of the try block
    if on_completed is None:
        sys.stdout.write(output)
    else:
        withheld = (
            any(resource["kind"] == "Secret" for resource in resources)
            or withholds_command(shlex.join(command))
            or withholds_command(summary or "")
        )
        on_completed(code, "[withheld]" if withheld else output, completed)
    return code


def _read_receipts(path: Path) -> list[RepairActionReceipt]:
    """The last record of each action in file order; torn lines are logged and skipped."""

    from google.protobuf import json_format

    from sdo.contracts import RepairActionReceipt

    last: dict[str, dict[str, Any]] = {}
    for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            _LOG.warning("skipping a torn line %d in %s", number, path)
            continue
        raw = record.get("receipt") if isinstance(record, dict) else None
        if not isinstance(raw, dict) or not isinstance(raw.get("action_id"), str):
            raise RecordedActionsError(f"{path}:{number}: a record needs a receipt with an action_id")
        last[raw["action_id"]] = raw
    receipts: list[RepairActionReceipt] = []
    for action_id, raw in last.items():
        try:
            receipts.append(json_format.ParseDict(raw, RepairActionReceipt()))
        except json_format.ParseError as exc:
            raise RecordedActionsError(f"invalid recorded repair receipt {action_id}: {exc}") from exc
    return receipts


def merge_into(result: IncidentResult) -> None:
    """Add every recorded receipt to *result* and revalidate it.

    An action with only a ``started`` record was interrupted: it is a recorded unfinished mutation
    (``success`` false, details say so), never silently absent.
    """

    path = actions_path()
    if not path.exists():
        return
    receipts = _read_receipts(path)
    if not receipts:
        return
    from sdo.contracts import validate

    result.repair_actions.extend(receipts)
    try:
        validate(result)
    except ValueError as exc:
        raise RecordedActionsError(f"invalid result after merging recorded repair receipts: {exc}") from exc
