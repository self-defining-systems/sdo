"""Pinned Calico setup required by the production validator's isolation canary."""

from __future__ import annotations

import hashlib
import subprocess
import urllib.request
from typing import TYPE_CHECKING

from benchmarks.sregym.adapter import WaitKind, wait_for

if TYPE_CHECKING:
    from pathlib import Path

import yaml

CALICO_VERSION = "v3.27.4"
CALICO_SHA256 = "53250439641223c04f25035d9855f980b640baa73cde99bcfac5457b242fc51f"
CALICO_URL = f"https://raw.githubusercontent.com/projectcalico/calico/{CALICO_VERSION}/manifests/calico.yaml"
CALICO_IMAGES = [f"calico/{name}:{CALICO_VERSION}" for name in ("cni", "node", "kube-controllers")]


def install_calico(kubeconfig: Path, destination: Path) -> None:
    """Use a pinned manifest and preloaded images; workload startup cannot pull images."""
    with urllib.request.urlopen(CALICO_URL, timeout=wait_for(WaitKind.COMMAND)) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != CALICO_SHA256:
        raise ValueError("Calico manifest checksum mismatch")
    resources = list(yaml.safe_load_all(data))
    for resource in resources:
        pod = resource.get("spec", {}).get("template", {}).get("spec", {})
        for container in pod.get("containers", []) + pod.get("initContainers", []):
            container["imagePullPolicy"] = "Never"
            if "readinessProbe" in container:
                container["readinessProbe"]["periodSeconds"] = 1
            if container.get("name") == "calico-node":
                # One node needs no BGP mesh; VXLAN works on the kind bridge.
                for item in container.get("env", []):
                    if item["name"] == "CALICO_IPV4POOL_IPIP":
                        item["value"] = "Never"
                    if item["name"] == "CALICO_IPV4POOL_VXLAN":
                        item["value"] = "Always"
    destination.write_text(yaml.safe_dump_all(resources), encoding="utf-8")
    subprocess.run(
        [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig),
            "apply",
            "--server-side",
            "--force-conflicts",
            "-f",
            str(destination),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    for kind in ("daemonset/calico-node", "deployment/calico-kube-controllers"):
        subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                str(kubeconfig),
                "-n",
                "kube-system",
                "rollout",
                "status",
                kind,
                "--timeout=120s",
            ],
            check=True,
        )
