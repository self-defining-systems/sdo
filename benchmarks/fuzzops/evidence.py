"""Runtime evidence of an attached SDO for manual fuzzops pairs without an evidence sink.

The fuzzops-bench hooks never use this: every lane installs SDO with an evidence sink on a host mount and reads
evidence from there (``benchmarks.fuzzops.sink``). A manual pair without a sink saves its evidence with ``sdo-operator
runtime-artifacts``: a sealed snapshot at every incident boundary (the observer's ``--operator-binary``) and a full
export before the control namespace is deleted. A snapshot left on the repository PVC is lost with the namespace.

``python -m benchmarks.fuzzops.evidence export|snapshot --operator-binary ... --control-namespace ... \
--controller-image ... --output-dir <sdo-exp dir> [--snapshot-id ID] [--kubeconfig ...]``
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPOSITORY_PVC = "sdo-application-repository"


class EvidenceError(RuntimeError):
    """sdo-operator runtime-artifacts failed or answered unexpectedly."""


@dataclass(frozen=True)
class EvidenceTarget:
    """Where one attached SDO keeps its runtime evidence; holds no credential."""

    operator_binary: Path
    control_namespace: str
    controller_image: str
    kubeconfig: Path | None = None
    repository_pvc: str = REPOSITORY_PVC

    def __post_init__(self) -> None:
        if not self.control_namespace or not self.controller_image or not self.repository_pvc:
            raise ValueError("evidence target requires a control namespace, controller image and repository PVC")
        if not self.operator_binary.is_absolute():
            raise ValueError("operator_binary must be an absolute path")
        if self.kubeconfig is not None and not self.kubeconfig.is_absolute():
            raise ValueError("kubeconfig must be an absolute path")

    def command(self, *extra: str) -> list[str]:
        args = [
            str(self.operator_binary),
            "runtime-artifacts",
            "--control-namespace",
            self.control_namespace,
            "--repository-pvc",
            self.repository_pvc,
            "--controller-image",
            self.controller_image,
            *extra,
        ]
        if self.kubeconfig is not None:
            args += ["--kubeconfig", str(self.kubeconfig)]
        return args


def _run(args: list[str]) -> dict[str, object]:
    proc = subprocess.run(args, capture_output=True, text=True, timeout=900, check=False)
    lines = [line for line in proc.stdout.splitlines() if line.startswith("{")]
    if proc.returncode != 0 or not lines:
        raise EvidenceError(f"runtime-artifacts failed ({proc.returncode}): {proc.stderr.strip()[-400:]}")
    value = json.loads(lines[-1])
    if not isinstance(value, dict):
        raise EvidenceError("runtime-artifacts answered with a non-object")
    return value


def snapshot_id_for(incident_id: str) -> str:
    """A DNS-label snapshot ID for an incident boundary (sealed once; a repeat returns the same archive)."""
    label = re.sub(r"[^a-z0-9-]", "-", incident_id.lower()).strip("-")[:56] or "incident"
    return f"close-{label}"[:63]


def snapshot(target: EvidenceTarget, snapshot_id: str) -> dict[str, object]:
    """Seal the current evidence under snapshot_id; repeated IDs return the sealed archive."""
    value = _run(target.command("--mode", "snapshot", "--snapshot-id", snapshot_id))
    if value.get("snapshot_id") != snapshot_id:
        raise EvidenceError("runtime-artifacts sealed a different snapshot")
    return value


def export(target: EvidenceTarget, output_dir: Path) -> Path:
    """Export every runtime artifact (sessions, rollouts, tasks, telemetry) into output_dir/sdo_runtime."""
    if not output_dir.is_absolute():
        raise ValueError("output_dir must be an absolute path")
    value = _run(target.command("--output-dir", str(output_dir)))
    expected = output_dir / "sdo_runtime"
    if value.get("directory") != str(expected):
        raise EvidenceError(f"runtime-artifacts exported to {value.get('directory')}, not {expected}")
    return expected


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=("export", "snapshot"))
    p.add_argument("--operator-binary", type=Path, required=True)
    p.add_argument("--control-namespace", required=True)
    p.add_argument("--controller-image", required=True)
    p.add_argument("--kubeconfig", type=Path)
    p.add_argument("--output-dir", type=Path, help="export: where sdo_runtime/ is written (an sdo-exp directory)")
    p.add_argument("--snapshot-id", help="snapshot: the boundary's ID")
    a = p.parse_args(argv)
    target = EvidenceTarget(
        a.operator_binary.resolve(),
        a.control_namespace,
        a.controller_image,
        a.kubeconfig.resolve() if a.kubeconfig else None,
    )
    if a.mode == "export":
        if a.output_dir is None:
            p.error("export needs --output-dir")
        print(json.dumps({"directory": str(export(target, a.output_dir.resolve()))}))
    else:
        if not a.snapshot_id:
            p.error("snapshot needs --snapshot-id")
        print(json.dumps(snapshot(target, a.snapshot_id)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
