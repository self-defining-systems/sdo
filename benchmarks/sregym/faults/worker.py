"""Describe SREGym faults for fastloop probes without contacting Kubernetes."""

from __future__ import annotations

import contextlib
import importlib
import json
import sys
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from benchmarks.sregym.faults.description import PROBE_KINDS, FaultSpec, ProblemDescription

# Class names are interpreted only at this benchmark subprocess boundary.
# Unknown faults remain describable but fail capability validation until reviewed.
KINDS = {
    "K8STargetPortMisconfig": "target_port",
    "IncorrectImage": "incorrect_image",
    "ReadinessProbeMisconfiguration": "readiness",
    "WrongServiceSelector": "wrong_selector",
    "NetworkPolicyBlock": "network_policy",
    "MissingConfigMap": "configmap",
    "ResourceRequestTooLarge": "resource_request",
    "ResourceRequestTooSmall": "resource_request",
}
CAPABILITIES = {
    **dict.fromkeys(
        (
            "WrongBinUsage",
            "ReadinessProbeMisconfiguration",
            "IncorrectImage",
            "ResourceRequestTooLarge",
            "ResourceRequestTooSmall",
            "SidecarPortConflict",
            "LivenessProbeTooAggressive",
        ),
        ("application-workloads",),
    ),
    "MissingConfigMap": ("application-configmaps", "application-workloads"),
    "ConfigMapDrift": ("application-configmaps", "application-workloads"),
    "WrongServiceSelector": ("application-services",),
    "K8STargetPortMisconfig": ("application-services",),
    "ServicePortConflict": ("application-services",),
    "NetworkPolicyBlock": ("application-networkpolicies",),
    "DuplicatePVCMounts": ("application-pvcs", "application-workloads"),
    "MongoDBUserUnregistered": ("application-pod-exec",),
    "PSARestrictedBlocksRecreation": ("Namespace-labels/PSA",),
    "NamespaceMemoryLimit": ("ResourceQuota",),
    "AdmissionWebhookTLSMismatch": ("Webhook-configuration", "foreign-namespace"),
    "AdmissionWebhookOutage": ("Webhook-configuration", "foreign-namespace"),
    "MutatingWebhookResourceLimits": ("Webhook-configuration",),
}


def describe_problem(problem_id: str, problem: Any) -> ProblemDescription:
    """Describe either composite implementation without injecting or evaluating an oracle."""
    indices = getattr(problem, "fault_indices", None)
    colocated = getattr(problem, "problems", None)
    parts: list[tuple[int, Any, Any]]
    if colocated is not None:
        draws = getattr(problem, "draws", [None] * len(colocated))
        parts = [(index, part, draws[index]) for index, part in enumerate(colocated)]
        order = tuple(getattr(problem, "injection_order", range(len(colocated))))
    elif indices is not None:
        parts = [(index, problem.components[index], None) for index in indices]
        order = tuple(indices)
    else:
        parts, order = [(0, problem, None)], (0,)
    faults: list[FaultSpec] = []
    for index, part, draw in parts:
        class_name = type(part).__name__
        kind = KINDS.get(class_name, draw.key if draw is not None else class_name)
        service = (
            getattr(part, "faulty_service", None)
            or getattr(part, "faulty_services", None)
            or getattr(part, "namespace", "unknown")
        )
        if isinstance(service, (list, tuple)):
            targets = cast("list[str] | tuple[str, ...]", service)
            service = targets[0] if len(targets) == 1 else ",".join(targets)
            single_target = len(targets) == 1
        else:
            single_target = isinstance(service, str)
        component = str(service)
        supported = kind in PROBE_KINDS and single_target
        params: dict[str, Any] = (
            dict(draw.param_dict())
            if draw is not None
            else {
                key: value
                for key, value in cast("dict[str, Any]", vars(part)).items()
                if key
                in {
                    "faulty_service",
                    "variant",
                    "mode",
                    "bad_port",
                    "policy_name",
                    "pod_label_key",
                    "probe_path",
                    "path",
                    "image",
                    "tag",
                    "requested_memory",
                    "requested_cpu",
                }
                and isinstance(value, (str, int, float, bool, list, dict, type(None)))
            }
        )
        if indices is not None and colocated is None and hasattr(problem, "spec"):
            params.update(dict(problem.spec.components[index].params))
        footprints = getattr(draw, "footprints", ())
        requirements = set(CAPABILITIES.get(class_name, ("unreviewed:" + class_name,)))
        for footprint in footprints:
            if footprint.kind in {
                "Namespace",
                "ResourceQuota",
                "ValidatingWebhookConfiguration",
                "MutatingWebhookConfiguration",
            }:
                requirements.add(footprint.kind)
        faults.append(
            FaultSpec(
                kind=kind,
                component=component,
                declared_index=index,
                class_name=class_name,
                fault_key=draw.key if draw is not None else class_name,
                parameters=params,
                tracking_supported=supported,
                tracking_reason=None if supported else f"no attributable probe for {class_name}",
                required_capabilities=tuple(sorted(requirements)),
                policy_name=getattr(part, "policy_name", None),
                pod_label_key=getattr(part, "pod_label_key", "io.kompose.service"),
            )
        )
    return ProblemDescription(
        problem_id=problem_id,
        composite=indices is not None or colocated is not None,
        faults=tuple(faults),
        injection_order=order,
    )


def _offline_init(_self: Any) -> None:
    pass


def _offline_emulated(_self: Any) -> bool:
    return False


def _offline_deployments(_self: Any, _namespace: Any) -> SimpleNamespace:
    return SimpleNamespace(items=[])


def _offline_config(*_args: Any, **_kwargs: Any) -> None:
    pass


def describe(problem_ids: list[str] | dict[str, Any]) -> dict[str, dict[str, Any]]:
    # Runs in the harness environment; constructors need metadata, never a real cluster.
    config = importlib.import_module("kubernetes.config")
    ProblemRegistry = importlib.import_module("sregym.conductor.problems.registry").ProblemRegistry
    KubeCtl = importlib.import_module("sregym.service.kubectl").KubeCtl
    with (
        patch.object(KubeCtl, "__init__", _offline_init),
        patch.object(KubeCtl, "is_emulated_cluster", _offline_emulated),
        patch.object(KubeCtl, "list_deployments", _offline_deployments),
        patch.object(config, "load_kube_config", _offline_config),
    ):
        registry = ProblemRegistry()
        if isinstance(problem_ids, dict):
            selection = problem_ids["selection"]
            variants = selection["variants"]
            runner = importlib.import_module("sregym.parallel_runner")
            args = SimpleNamespace(
                variants=variants["enabled"],
                variant_spec=variants["spec_names"] or None,
                variant_count=variants["count"],
                variant_offset=variants["offset"],
                variant_seed=variants["seed"],
                variant_order=variants["order"].replace("_", "-"),
                variant_max_per_class=variants["max_per_class"],
                problem=None,
                suite=None,
                tasklist=selection["tasklist"],
                problem_spec=selection["spec_names"] or None,
            )
            # Validate the complete adaptive pool; future dynamically selected problems
            # must be inside this checked set as well.
            if args.variant_order == "adaptive":
                selected = runner._variant_pool(args.variant_spec)
            else:
                selected = selection["problems"] or runner.select_problem_ids(args)
            app_filter = selection["app_filter"].lower().replace("_", " ")
            described: dict[str, dict[str, Any]] = {}
            for problem_id in dict.fromkeys(selected):
                problem = registry.get_problem_instance(problem_id)
                app_name = str(problem.app.app_name).lower().replace("_", " ")
                if not app_filter or app_filter in app_name:
                    described[problem_id] = describe_problem(problem_id, problem).model_dump(mode="json")
            if not described:
                raise ValueError("suite selection resolved no problems")
            return described
        return {
            problem_id: describe_problem(problem_id, registry.get_problem_instance(problem_id)).model_dump(mode="json")
            for problem_id in problem_ids
        }


def main() -> None:
    problem_ids = json.load(sys.stdin)
    with contextlib.redirect_stdout(sys.stderr):
        result = describe(problem_ids)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
