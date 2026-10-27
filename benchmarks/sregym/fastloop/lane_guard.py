"""Opt-in host lease and metadata-only Secret fence for delegated fastloop runs.

Invoke as ``python -m ...lane_guard --cluster NAME --kubeconfig PATH --lease ID
-- MODULE [ARGS...]``. The same fence follows worker and dry-run subprocesses.
No application, traffic, grading or cleanup behavior is changed by this wrapper.
"""

from __future__ import annotations

import argparse
import os
import re
import runpy
import subprocess
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks.sregym.adapter import WaitKind, wait_for

#: Namespace -> Secret names that the benchmark's own fault injector is handling right now.
_INJECTOR_SECRETS: ContextVar[Mapping[str, frozenset[str]] | None] = ContextVar("injector_secrets", default=None)
_READ_FLAGS_WITH_VALUE = {"-o", "--output", "--kubeconfig"}


@contextmanager
def injector_secret_scope(secrets: Mapping[str, frozenset[str]]) -> Iterator[None]:
    """Let the benchmark's fault inject/recover calls read back the Secrets its faults declare.

    The Secret rule fences this harness's own reads. A fault such as a credential rotation writes a
    Secret and reads it back to verify the rotation; that traffic stays inside the SREGym worker,
    which neither logs nor returns it. The scope covers one worker request on its calling thread,
    one named Secret per declared name, in the application namespace the fault names.
    """
    token = _INJECTOR_SECRETS.set({namespace: frozenset(names) for namespace, names in secrets.items() if names})
    try:
        yield
    finally:
        _INJECTOR_SECRETS.reset(token)


class FaultNotInstallableError(ValueError):
    """A fault of this row needs a Secret operation the lane guard still rejects, so it could never inject."""

    reason = "fault_not_installable"


def assert_faults_installable(descriptions: Mapping[str, Any], namespace: str, guard: LaneGuard) -> None:
    """Fail at row start, not after onboarding, when the guard would reject a declared Secret fault.

    Runs the fault's Secret read-back through the same ``command`` rule the worker is fenced with.
    """
    from benchmarks.sregym.faults.injector_secrets import INJECTOR_SECRETS

    rejected: dict[str, str] = {}
    for problem_id, description in sorted(descriptions.items()):
        for fault in description.faults:
            for name in sorted(INJECTOR_SECRETS.get(fault.class_name, ())):
                with injector_secret_scope({namespace: frozenset({name})}):
                    try:
                        guard.command(["kubectl", "get", "secret", name, "-n", namespace, "-o", "json"])
                    except ValueError as error:
                        rejected[f"{problem_id}:{fault.class_name}"] = str(error)
    if rejected:
        raise FaultNotInstallableError(
            f"{FaultNotInstallableError.reason}: "
            + "; ".join(f"{key}: {message}" for key, message in sorted(rejected.items()))
        )


def _reads_declared_secret(values: list[str], secrets: Mapping[str, frozenset[str]]) -> bool:
    """Whether ``kubectl get secret NAME -n NS`` names exactly one declared Secret and nothing wider."""
    positional: list[str] = []
    namespace: str | None = None
    index = 1
    while index < len(values):
        value = values[index]
        if value in {"-n", "--namespace"}:
            namespace = values[index + 1] if index + 1 < len(values) else None
            index += 2
        elif value.startswith("--namespace="):
            namespace = value.split("=", 1)[1]
            index += 1
        elif value in _READ_FLAGS_WITH_VALUE:
            index += 2
        elif value.startswith(("-o", "--output=", "--kubeconfig=")) or value == "--ignore-not-found":
            index += 1
        elif value.startswith("-"):
            return False  # -A, selectors, watches and anything else wider than one named Secret
        else:
            positional.append(value)
            index += 1
    if positional[:1] != ["get"] or namespace not in secrets:
        return False
    target = positional[1:]
    if len(target) == 1 and target[0].split("/", 1)[0] in {"secret", "secrets"} and "/" in target[0]:
        target = target[0].split("/", 1)
    elif len(target) == 2:
        pass
    else:
        return False
    return target[0] in {"secret", "secrets"} and target[1] in secrets[namespace]


@dataclass(frozen=True)
class LaneGuard:
    cluster: str
    kubeconfig: Path
    lease: str
    coord: str = "coord"

    def __post_init__(self) -> None:
        if not self.cluster or not self.lease:
            raise ValueError("lane guard requires cluster and lease identities")

    def command(self, args: list[str]) -> list[str]:
        values = list(args)
        executable = Path(values[0]).name if values else ""
        if executable not in {"kubectl", "docker", "kind"}:
            return values
        prefix = self.cluster.rsplit("-", 1)[0] + "-"
        for value in values:
            if value.startswith(prefix) and not re.fullmatch(
                re.escape(self.cluster) + r"(?:-control-plane|-worker\d*|-registry)?", value
            ):
                raise ValueError("lane guard rejects foreign cluster target")
        if executable != "kubectl":
            return values
        for index, value in enumerate(values):
            if value in {"--context", "--server"} or value.startswith(("--context=", "--server=")):
                raise ValueError("lane guard rejects alternate API target")
            if value == "--kubeconfig" and (index + 1 == len(values) or values[index + 1] != str(self.kubeconfig)):
                raise ValueError("lane guard rejects foreign kubeconfig")
            if value.startswith("--kubeconfig=") and value.split("=", 1)[1] != str(self.kubeconfig):
                raise ValueError("lane guard rejects foreign kubeconfig")
            if re.search(r"/secrets(?:/|\?|$)", value):
                raise ValueError("lane guard rejects Secret API paths")
        resources = {part.lower().split("/", 1)[0] for value in values for part in value.split(",")}
        if not resources & {"secret", "secrets"} or not resources & {"get", "list", "watch"}:
            return values
        injector = _INJECTOR_SECRETS.get()
        if injector and _reads_declared_secret(values, injector):
            return values
        if "get" not in values or any(value in {"--watch", "-w"} or value.startswith("--watch=") for value in values):
            raise ValueError("lane guard rejects Secret list/watch")
        outputs = []
        for index, value in enumerate(values):
            if value in {"-o", "--output"}:
                outputs.append(values[index + 1] if index + 1 < len(values) else "")
            elif value.startswith(("--output=", "-o=")):
                outputs.append(value.split("=", 1)[1])
            elif value.startswith("-o") and value != "-o":
                outputs.append(value[2:])
        allowed = {"name", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name"}
        if len(outputs) > 1 or any(value not in allowed for value in outputs):
            raise ValueError("lane guard permits only Secret namespace/name metadata")
        return values if outputs else [*values, "-o", "name"]

    def install(self) -> None:
        """Fence each subprocess and each long-lived worker request."""
        original = subprocess.Popen
        guard = self
        os.environ["KUBECONFIG"] = str(self.kubeconfig)

        def check() -> None:
            with original(
                [guard.coord, "check", guard.cluster, "--lease", guard.lease],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as process:
                _, error = process.communicate()
                if process.returncode:
                    raise RuntimeError("lane lease fence rejected operation: " + error.decode())

        class FencedPopen(original):
            def __init__(self, args: Any, *positional: Any, **kwargs: Any) -> None:
                check()
                if isinstance(args, (list, tuple)):
                    values = guard.command([str(value) for value in args])
                    for module in ("benchmarks.sregym.fastloop.worker_bridge", "benchmarks.sregym.fastloop.dry_run"):
                        if "-m" in values and values[values.index("-m") + 1] == module:
                            position = values.index("-m")
                            values[position + 1 : position + 2] = [
                                "benchmarks.sregym.fastloop.lane_guard",
                                "--cluster",
                                guard.cluster,
                                "--kubeconfig",
                                str(guard.kubeconfig),
                                "--lease",
                                guard.lease,
                                "--coord",
                                guard.coord,
                                "--",
                                module,
                            ]
                            break
                    args = values
                super().__init__(args, *positional, **kwargs)

        subprocess.Popen = FencedPopen
        from benchmarks.sregym.fastloop.worker_client import SregymWorker

        request = SregymWorker.request

        def fenced_request(
            self: SregymWorker, op: str, *, timeout: float = int(wait_for(WaitKind.CLUSTER_OP)), **args: Any
        ) -> dict[str, Any]:
            check()
            return request(self, op, timeout=timeout, **args)

        SregymWorker.request = fenced_request


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--lease", required=True)
    parser.add_argument("--coord", default="coord")
    parser.add_argument("module_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    module_args = args.module_args[1:] if args.module_args[:1] == ["--"] else args.module_args
    if not module_args:
        parser.error("a module is required after --")
    LaneGuard(args.cluster, args.kubeconfig, args.lease, args.coord).install()
    sys.argv = module_args
    runpy.run_module(module_args[0], run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
