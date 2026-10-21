"""Reusable toy fault definitions and cluster-state mitigation oracles."""

from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any

SERVICES = ("frontend", "backend", "catalog")
IMAGE = "sdo-toy-app:v1"


@dataclass(frozen=True)
class ToyFault:
    kind: str
    service: str
    clear_field_ownership: bool = False

    def __post_init__(self) -> None:
        if self.kind not in {"target-port", "image", "readiness", "network-policy"} or self.service not in SERVICES:
            raise ValueError(f"unsupported toy fault: {self.kind}/{self.service}")
        if self.clear_field_ownership and self.kind != "target-port":
            raise ValueError("only target-port faults can model an unowned Service")

    @property
    def patch(self) -> dict[str, Any]:
        if self.kind == "target-port":
            return {"spec": {"ports": [{"port": 8080, "targetPort": 9999}]}}
        container: dict[str, Any] = {"name": self.service}
        if self.kind == "image":
            container["image"] = "sdo-toy-app:nonexistent"
        else:
            container["readinessProbe"] = {"httpGet": {"path": "/missing", "port": 8080}}
        return {"spec": {"template": {"spec": {"containers": [container]}}}}

    def inject(self, kubeconfig: str, namespace: str) -> None:
        if self.clear_field_ownership:
            command(kubeconfig, namespace, "patch", "service", self.service, "--type=merge", "-p",
                    '{"metadata":{"managedFields":[{}]}}')
            # Model an imported object before introducing the latent fault. The
            # informer must observe the healthy, unowned object first.
            time.sleep(1)
        if self.kind == "network-policy":
            policy = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                      "metadata": {"name": f"isolate-{self.service}"},
                      "spec": {"podSelector": {"matchLabels": {"app": self.service}}, "policyTypes": ["Ingress"], "ingress": []}}
            command(kubeconfig, namespace, "apply", "-f", "-", input_text=json.dumps(policy))
            return
        kind = "service" if self.kind == "target-port" else "deployment"
        command(kubeconfig, namespace, "patch", kind, self.service, "--type=strategic", "-p", json.dumps(self.patch))

    def oracle(self, kubeconfig: str, namespace: str) -> bool:
        if self.kind == "network-policy":
            policies = json.loads(command(kubeconfig, namespace, "get", "networkpolicies", "-o", "json"))["items"]
            return not any(item["metadata"]["name"] == f"isolate-{self.service}" for item in policies)
        kind = "service" if self.kind == "target-port" else "deployment"
        live = json.loads(command(kubeconfig, namespace, "get", kind, self.service, "-o", "json"))
        if self.kind == "target-port":
            return live["spec"]["ports"][0].get("targetPort") == 8080
        container = live["spec"]["template"]["spec"]["containers"][0]
        correct = (
            container["image"] == IMAGE
            if self.kind == "image"
            else container["readinessProbe"]["httpGet"]["path"] == "/ready"
        )
        status = live.get("status", {})
        return (
            correct
            and status.get("observedGeneration", 0) >= live["metadata"].get("generation", 0)
            and all(status.get(field) == 1 for field in ("updatedReplicas", "readyReplicas", "replicas"))
        )


def fault_spec(problem_id: str) -> ToyFault:
    match = re.fullmatch(r"toy_(target_port|image|readiness|network_policy)(_unowned)?_(frontend|backend|catalog)", problem_id)
    if match is None:
        raise ValueError(f"unsupported toy fault: {problem_id}")
    return ToyFault(match[1].replace("_", "-"), match[3], clear_field_ownership=bool(match[2]))



def command(kubeconfig: str, namespace: str, *args: str, input_text: str | None = None) -> str:
    return subprocess.run(
        ["kubectl", "--kubeconfig", kubeconfig, "-n", namespace, *args],
        check=True,
        input=input_text,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout
