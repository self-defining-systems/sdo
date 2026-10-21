"""Describe SREGym faults for fastloop probes without contacting Kubernetes."""

from __future__ import annotations

import contextlib
import importlib
import json
import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch


def describe(problem_ids: list[str]) -> dict[str, list[dict[str, str]]]:
    # This entry point runs in SREGym's subprocess environment, like worker_client.
    config = importlib.import_module("kubernetes.config")
    IncorrectImage = importlib.import_module("sregym.conductor.problems.incorrect_image").IncorrectImage
    NetworkPolicyBlock = importlib.import_module("sregym.conductor.problems.network_policy_block").NetworkPolicyBlock
    ReadinessProbeMisconfiguration = importlib.import_module(
        "sregym.conductor.problems.readiness_probe_misconfiguration"
    ).ReadinessProbeMisconfiguration
    ProblemRegistry = importlib.import_module("sregym.conductor.problems.registry").ProblemRegistry
    K8STargetPortMisconfig = importlib.import_module("sregym.conductor.problems.target_port").K8STargetPortMisconfig
    WrongServiceSelector = importlib.import_module(
        "sregym.conductor.problems.wrong_service_selector"
    ).WrongServiceSelector
    KubeCtl = importlib.import_module("sregym.service.kubectl").KubeCtl

    def fault_spec(problem: Any) -> dict[str, str]:
        if isinstance(problem, K8STargetPortMisconfig):
            kind = "target_port"
        elif isinstance(problem, IncorrectImage):
            kind = "incorrect_image"
        elif isinstance(problem, ReadinessProbeMisconfiguration):
            kind = "readiness"
        elif isinstance(problem, WrongServiceSelector):
            kind = "wrong_selector"
        elif isinstance(problem, NetworkPolicyBlock):
            kind = "network_policy"
        else:
            raise ValueError(f"no fastloop probe for {type(problem).__name__}")
        service = problem.faulty_service
        if isinstance(service, list):
            if len(service) != 1:
                raise ValueError(f"fastloop needs one target service for {type(problem).__name__}")
            service = service[0]
        if not isinstance(service, str) or not service:
            raise ValueError(f"invalid target service for {type(problem).__name__}: {service!r}")
        if (
            not callable(problem.inject_fault)
            or not callable(problem.recover_fault)
            or problem.mitigation_oracle is None
        ):
            raise ValueError(f"{type(problem).__name__} lacks injection, recovery, or mitigation oracle")
        result = {"kind": kind, "component": service}
        if kind == "network_policy":
            result.update(policy_name=problem.policy_name, pod_label_key=problem.pod_label_key)
        return result

    # Problem constructors create Kubernetes clients but probe discovery needs only their metadata.
    with (
        patch.object(KubeCtl, "__init__", lambda self: None),
        patch.object(KubeCtl, "is_emulated_cluster", lambda self: False),
        patch.object(KubeCtl, "list_deployments", lambda self, namespace: SimpleNamespace(items=[])),
        patch.object(config, "load_kube_config", lambda *args, **kwargs: None),
    ):
        registry = ProblemRegistry()
        described = {}
        for problem_id in problem_ids:
            problem = registry.get_problem_instance(problem_id)
            indices = getattr(problem, "fault_indices", None)
            if indices is not None and any(problem.component_oracle(index) is None for index in indices):
                raise ValueError(f"{problem_id} lacks a component oracle")
            components = [problem.components[index] for index in indices] if indices is not None else [problem]
            described[problem_id] = [fault_spec(component) for component in components]
        return described


def main() -> None:
    problem_ids = json.load(sys.stdin)
    with contextlib.redirect_stdout(sys.stderr):
        result = describe(problem_ids)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
