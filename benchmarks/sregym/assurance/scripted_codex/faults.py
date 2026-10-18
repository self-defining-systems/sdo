# ruff: noqa: E501 - Go, shell and Markdown templates keep their natural line lengths.
"""The scripted responder's fault catalog: how each fault class is diagnosed, repaired and learned.

Every plan works only from what a real responder could see: the incident
request, the application worktree and live ``kubectl`` output. The directive
names the fault class and its target workload; the object names a repair
needs (the NetworkPolicy, the missing ConfigMap and its manifest) are found
from live state, and the plan fails loudly when live state does not show the
fault.

Stdlib-only: this runs inside the controller and responder images.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from pathlib import Path

SERVICE_LABEL = "io.kompose.service"


class PlanError(RuntimeError):
    """Live state does not show the fault the directive names."""


class Runner(Protocol):
    def __call__(self, command: str, *, timeout: float = ...) -> tuple[int, str]: ...


@dataclass(frozen=True)
class MemoryProposal:
    """Logical detector and playbook mutations returned by scripted reflection."""

    detector_id: str
    playbook_id: str
    artifact_proposal: dict[str, Any]


@dataclass(frozen=True)
class Facts:
    """What the diagnosis established about the live fault."""

    namespace: str
    target: str
    objects: dict[str, str] = field(default_factory=dict)
    observations: tuple[tuple[str, str], ...] = ()


def _json(output: str) -> Any:
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise PlanError(f"kubectl returned non-JSON output: {output[:200]!r}") from exc


def _object_ref(api_version: str, kind: str, namespace: str, name: str) -> dict[str, str]:
    return {"api_version": api_version, "kind": kind, "namespace": namespace, "name": name}


def active_findings(request: dict[str, Any]) -> list[dict[str, Any]]:
    findings = request.get("findings")
    return [
        finding
        for finding in (findings if isinstance(findings, list) else [])
        if isinstance(finding, dict) and finding.get("status", "active") == "active"
    ]


def fired_detectors(request: dict[str, Any]) -> list[str]:
    return sorted(
        {str(finding.get("detector_id")) for finding in active_findings(request) if finding.get("detector_id")}
    )


def state_changes(request: dict[str, Any]) -> set[str]:
    changes = request.get("state_changes")
    items = changes.get("changes") if isinstance(changes, dict) else None
    return {
        f"{item.get('kind')}/{item.get('name')}"
        for item in (items if isinstance(items, list) else [])
        if isinstance(item, dict)
    }


def _finding_evidence(request: dict[str, Any]) -> list[dict[str, str]]:
    evidence: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for finding in active_findings(request):
        rule = str(finding.get("rule_id", ""))
        if rule.startswith("scenario-slo."):
            key = ("synthetic-traffic", rule.removeprefix("scenario-slo."))
        else:
            key = ("detector-finding", str(finding.get("detector_id", "")))
        if not key[1] or key in seen:
            continue
        seen.add(key)
        observation = str(finding.get("summary") or finding.get("evidence") or rule or "finding active").strip()
        evidence.append({"kind": key[0], "source": key[1], "observation": observation[:400] or "finding active"})
    return evidence


class FaultPlan(Protocol):
    name: str
    playbook_id: str

    def diagnose(self, run: Runner, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts: ...

    def wrong_repair(self, facts: Facts) -> tuple[str, str]: ...

    def correct_repair(self, facts: Facts) -> list[tuple[str, str]]: ...

    def root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]: ...

    def claimed_root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]: ...

    def memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal: ...

    def claimed_memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal: ...

    def warm_facts(self, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts: ...

    def warm_commands(self, facts: Facts) -> tuple[str, str, str]: ...


def _restart(namespace: str, target: str) -> tuple[str, str]:
    return (
        f"kubectl -n {namespace} rollout restart deployment/{target} && "
        f"kubectl -n {namespace} rollout status deployment/{target} --timeout=180s",
        f"rollout-restarted deployment/{target}",
    )


def _claimed_cause(facts: Facts, request: dict[str, Any], workload: str | None = None) -> dict[str, Any]:
    workload = workload or facts.target
    return {
        "summary": f"Pods of {workload} are wedged after a stale rollout and must be restarted",
        "resources": [_object_ref("apps/v1", "Deployment", facts.namespace, workload)],
        "evidence": [
            {
                "kind": "live-observation",
                "source": f"kubectl -n {facts.namespace} get pods -l {SERVICE_LABEL}={workload} -o wide",
                "observation": f"{workload} pods were running but their requests failed",
            }
        ],
        "explained_detectors": fired_detectors(request) or ["traffic-health"],
        "static_context": [],
    }


class NetworkPolicyBlock:
    name = "network_policy_block"
    detector_id = "deny-all-networkpolicy-isolation"
    package = "deny_all_networkpolicy_isolation"
    playbook_id = "deny-all-networkpolicy-isolation"

    def diagnose(self, run: Runner, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts:
        del worktree
        code, pods = run(f"kubectl -n {namespace} get pods -l {SERVICE_LABEL}={target} -o wide")
        code, output = run(f"kubectl -n {namespace} get networkpolicy -o json")
        if code != 0:
            raise PlanError(f"cannot list NetworkPolicies: {output[:300]}")
        policies = [
            item
            for item in _json(output).get("items", [])
            if _isolates(item)
            and item.get("spec", {}).get("podSelector", {}).get("matchLabels", {}).get(SERVICE_LABEL) == target
        ]
        if not policies:
            raise PlanError(f"no NetworkPolicy isolates {target} in {namespace}")
        name = str(policies[0]["metadata"]["name"])
        return Facts(
            namespace=namespace,
            target=target,
            objects={"policy": name},
            observations=(
                (
                    f"kubectl -n {namespace} get networkpolicy -o json",
                    f"NetworkPolicy {name} denies all ingress and egress of {target}",
                ),
                (
                    f"kubectl -n {namespace} get pods -l {SERVICE_LABEL}={target} -o wide",
                    pods.strip()[:300] or "pods listed",
                ),
            ),
        )

    #: The wrong fix restarts the isolated service's caller. Restarting the isolated
    #: service itself would crash-loop it past the fault's recovery window.
    wrong_workload = "frontend"

    def wrong_repair(self, facts: Facts) -> tuple[str, str]:
        return _restart(facts.namespace, self.wrong_workload)

    def correct_repair(self, facts: Facts) -> list[tuple[str, str]]:
        policy = facts.objects["policy"]
        return [(f"kubectl -n {facts.namespace} delete networkpolicy {policy}", f"deleted NetworkPolicy {policy}")]

    def root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]:
        policy = facts.objects["policy"]
        evidence = _finding_evidence(request)
        if f"NetworkPolicy/{policy}" in state_changes(request):
            evidence.append(
                {
                    "kind": "state-change",
                    "source": f"NetworkPolicy/{policy}",
                    "observation": "added after the last healthy state",
                }
            )
        evidence.extend(
            {"kind": "live-observation", "source": source, "observation": note}
            for source, note in facts.observations[:1]
        )
        return {
            "summary": f"NetworkPolicy {policy} denies all ingress and egress of the {facts.target} pods",
            "resources": [
                _object_ref("networking.k8s.io/v1", "NetworkPolicy", facts.namespace, policy),
                _object_ref("apps/v1", "Deployment", facts.namespace, facts.target),
            ],
            "evidence": evidence,
            "explained_detectors": fired_detectors(request),
            "static_context": [],
        }

    def claimed_root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]:
        return _claimed_cause(facts, request, self.wrong_workload)

    def warm_facts(self, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts:
        del worktree
        for finding in active_findings(request):
            primary = finding.get("primary_resource") or {}
            if finding.get("detector_id") == self.detector_id and primary.get("kind") == "NetworkPolicy":
                return Facts(namespace=namespace, target=target, objects={"policy": str(primary.get("name"))})
        # The learned detector may lag the health detectors: the playbook's script finds the policy itself.
        return Facts(namespace=namespace, target=target, objects={"policy": f"deny-all-{target}"})

    def warm_commands(self, facts: Facts) -> tuple[str, str, str]:
        policy = facts.objects["policy"]
        absent = f"! kubectl -n {facts.namespace} get networkpolicy {policy} -o name"
        return (
            absent,
            f"kubectl -n {facts.namespace} delete networkpolicy {policy}",
            f"{absent} && python3 -m sdo.agents.responder.status_cli",
        )

    def memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal:
        return _proposal(
            detector_id=self.detector_id,
            package=self.package,
            playbook_dir=self.playbook_id,
            title="Deny-all NetworkPolicy isolates a workload",
            description="Detects a NetworkPolicy that denies all ingress and egress of an application workload.",
            watches=(("networking.k8s.io/v1", "NetworkPolicy"), ("v1", "Pod")),
            detect_go=_NP_DETECT,
            test_go=_NP_TEST,
            imports='networkingv1 "k8s.io/api/networking/v1"',
            test_imports='networkingv1 "k8s.io/api/networking/v1"\n\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"',
            readme_body=_NP_README,
            scripts={"repair.sh": _NP_REPAIR, "verify.sh": _NP_VERIFY},
            incident_id=incident_id,
            outcome_commit=outcome_commit,
        )

    def claimed_memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal:
        return _claimed_proposal(incident_id=incident_id, outcome_commit=outcome_commit)


def _isolates(policy: dict[str, Any]) -> bool:
    spec = policy.get("spec") or {}
    types = set(spec.get("policyTypes") or [])
    return {"Ingress", "Egress"} <= types and not spec.get("ingress") and not spec.get("egress")


class MissingConfigMap:
    name = "missing_configmap"
    detector_id = "required-configmap-missing"
    package = "required_configmap_missing"
    playbook_id = "required-configmap-missing"

    def diagnose(self, run: Runner, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts:
        run(f"kubectl -n {namespace} get pods -l {SERVICE_LABEL}={target} -o wide")
        code, output = run(f"kubectl -n {namespace} get deployment {target} -o json")
        if code != 0:
            raise PlanError(f"cannot read deployment {target}: {output[:300]}")
        volumes = _json(output).get("spec", {}).get("template", {}).get("spec", {}).get("volumes") or []
        names = [str(v["configMap"]["name"]) for v in volumes if isinstance(v, dict) and v.get("configMap")]
        missing = None
        for name in names:
            code, _ = run(f"kubectl -n {namespace} get configmap {name} -o name")
            if code != 0:
                missing = name
                break
        if missing is None:
            raise PlanError(f"every ConfigMap {target} mounts exists ({names})")
        source, restore = _configmap_source(worktree, namespace, missing)
        run(f"grep -rlE 'name: {missing}|k8s-.*-mongo.sh' kubernetes/ .")
        return Facts(
            namespace=namespace,
            target=target,
            objects={"configmap": missing, "manifest": source, "restore": restore},
            observations=(
                (
                    f"kubectl -n {namespace} get configmap {missing} -o name",
                    f"ConfigMap {missing} mounted by {target} is NotFound",
                ),
            ),
        )

    def wrong_repair(self, facts: Facts) -> tuple[str, str]:
        return _restart(facts.namespace, facts.target)

    def correct_repair(self, facts: Facts) -> list[tuple[str, str]]:
        ns, target = facts.namespace, facts.target
        configmap, manifest = facts.objects["configmap"], facts.objects["manifest"]
        return [
            (facts.objects["restore"], f"restored ConfigMap {configmap} from {manifest}"),
            (
                f"kubectl -n {ns} delete pod -l {SERVICE_LABEL}={target} --wait=false && "
                f"kubectl -n {ns} rollout status deployment/{target} --timeout=180s",
                f"replaced the {target} pods stuck on the missing mount",
            ),
        ]

    def root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]:
        configmap = facts.objects["configmap"]
        evidence = _finding_evidence(request)
        if f"ConfigMap/{configmap}" in state_changes(request):
            evidence.append(
                {
                    "kind": "state-change",
                    "source": f"ConfigMap/{configmap}",
                    "observation": "deleted after the last healthy state",
                }
            )
        evidence.extend(
            {"kind": "live-observation", "source": source, "observation": note} for source, note in facts.observations
        )
        return {
            "summary": f"ConfigMap {configmap} required by deployment {facts.target} was deleted, so its pods cannot mount it",
            "resources": [
                _object_ref("v1", "ConfigMap", facts.namespace, configmap),
                _object_ref("apps/v1", "Deployment", facts.namespace, facts.target),
            ],
            "evidence": evidence,
            "explained_detectors": fired_detectors(request),
            "static_context": [facts.objects["manifest"]],
        }

    def claimed_root_cause(self, facts: Facts, request: dict[str, Any]) -> dict[str, Any]:
        return _claimed_cause(facts, request)

    def warm_facts(self, namespace: str, target: str, request: dict[str, Any], worktree: Path) -> Facts:
        configmap = ""
        for finding in active_findings(request):
            if finding.get("detector_id") != self.detector_id:
                continue
            for related in finding.get("related_resources") or []:
                if isinstance(related, dict) and related.get("kind") == "ConfigMap":
                    configmap = str(related.get("name"))
        return Facts(namespace=namespace, target=target, objects={"configmap": configmap})

    def warm_commands(self, facts: Facts) -> tuple[str, str, str]:
        configmap = facts.objects.get("configmap") or "-"
        return (
            f"kubectl -n {facts.namespace} get configmap {configmap} -o name",
            f"kubectl -n {facts.namespace} create configmap {configmap} --from-file=k8s-geo-mongo.sh",
            f"kubectl -n {facts.namespace} get configmap {configmap} -o name && python3 -m sdo.agents.responder.status_cli",
        )

    def memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal:
        return _proposal(
            detector_id=self.detector_id,
            package=self.package,
            playbook_dir=self.playbook_id,
            title="Required ConfigMap missing",
            description="Detects a Deployment whose required ConfigMap volume is absent while its pods fail to mount it.",
            watches=(("apps/v1", "Deployment"), ("v1", "ConfigMap"), ("v1", "Pod"), ("v1", "Event")),
            detect_go=_CM_DETECT,
            test_go=_CM_TEST,
            imports='"fmt"\n\t"strings"\n\n\tcorev1 "k8s.io/api/core/v1"',
            test_imports='appsv1 "k8s.io/api/apps/v1"\n\tcorev1 "k8s.io/api/core/v1"\n\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"',
            readme_body=_CM_README,
            scripts={"repair.sh": _CM_REPAIR, "verify.sh": _CM_VERIFY},
            incident_id=incident_id,
            outcome_commit=outcome_commit,
        )

    def claimed_memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryProposal:
        return _claimed_proposal(incident_id=incident_id, outcome_commit=outcome_commit)


def _configmap_source(worktree: Path, namespace: str, name: str) -> tuple[str, str]:
    """The source file that defines ConfigMap ``name`` and the command that restores it from that file.

    A manifest under ``kubernetes/`` wins; otherwise hotel_reservation builds ``mongo-<svc>-script``
    from the root ``k8s-<svc>-mongo.sh`` init script.
    """

    pattern = re.compile(rf"^\s*name:\s*['\"]?{re.escape(name)}['\"]?\s*$", re.MULTILINE)
    root = worktree / "kubernetes"
    for path in sorted(root.rglob("*.y*ml")) if root.is_dir() else []:
        text = path.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^kind:\s*ConfigMap\s*$", text, re.MULTILINE) and pattern.search(text):
            manifest = path.relative_to(worktree).as_posix()
            return manifest, f"kubectl -n {namespace} apply -f {manifest}"
    script = re.fullmatch(r"mongo-([a-z0-9-]+)-script", name)
    if script and (worktree / f"k8s-{script.group(1)}-mongo.sh").is_file():
        source = f"k8s-{script.group(1)}-mongo.sh"
        return source, f"kubectl -n {namespace} create configmap {name} --from-file={source}"
    raise PlanError(f"no manifest under kubernetes/ or init script defines ConfigMap {name}")


PLANS: dict[str, FaultPlan] = {"network_policy_block": NetworkPolicyBlock(), "missing_configmap": MissingConfigMap()}


# --- Operational-memory templates -------------------------------------------------------------------------


def _proposal(
    *,
    detector_id: str,
    package: str,
    playbook_dir: str,
    title: str,
    description: str,
    watches: tuple[tuple[str, str], ...],
    detect_go: str,
    test_go: str,
    imports: str,
    test_imports: str,
    readme_body: str,
    scripts: dict[str, str],
    incident_id: str,
    outcome_commit: str,
) -> MemoryProposal:
    fault_class = playbook_dir.rsplit("/", 1)[-1]
    playbook = fault_class
    watch_go = ", ".join(f'{{APIVersion: "{api}", Kind: "{kind}"}}' for api, kind in watches)
    detector_go = _DETECTOR_GO.format(
        package=package,
        imports=imports,
        detector_id=detector_id,
        playbook=playbook,
        description=description,
        watches=watch_go,
        incident=json.dumps(incident_id),
        commit=json.dumps(outcome_commit),
        detect=detect_go.strip("\n"),
    )
    detector_test = _TEST_GO.format(package=package, imports=test_imports, body=test_go.strip("\n"))
    readme = _README.format(
        fault_class=fault_class,
        incident=json.dumps(incident_id),
        commit=json.dumps(outcome_commit),
        title=title,
        body=readme_body.strip("\n").replace("{playbook_dir}", "."),
        detector_id=detector_id,
    )
    return MemoryProposal(
        detector_id=detector_id,
        playbook_id=playbook,
        artifact_proposal={
            "detectors": [
                {
                    "id": detector_id,
                    "detector_class": "incident",
                    "owner": "responder",
                    "constructor": "New",
                    "watches": [{"api_version": api, "kind": kind} for api, kind in watches],
                    "interval": "30s",
                    "firing": 1,
                    "clearing": 2,
                    "severity": "critical",
                    "debounce": "500ms",
                    "possible_playbook_ids": [playbook],
                    "originating_incident": incident_id,
                    "originating_commit": outcome_commit,
                    "sources": [
                        {"name": "detector.go", "content": detector_go},
                        {"name": "detector_test.go", "content": detector_test},
                    ],
                }
            ],
            "playbooks": [
                {
                    "id": playbook,
                    "owner": "responder",
                    "fault_class": fault_class,
                    "originating_incident": incident_id,
                    "markdown": readme,
                    "scripts": [{"name": name, "content": text.lstrip("\n")} for name, text in scripts.items()],
                }
            ],
        },
    )


def _claimed_proposal(*, incident_id: str, outcome_commit: str) -> MemoryProposal:
    return _proposal(
        detector_id="wedged-workload-restart",
        package="wedged_workload_restart",
        playbook_dir="wedged-workload-restart",
        title="Wedged workload needs a restart",
        description="Detects a Deployment with unavailable replicas, which the responder repaired by a restart.",
        watches=(("apps/v1", "Deployment"),),
        detect_go=_WEDGED_DETECT,
        test_go=_WEDGED_TEST,
        imports="",
        test_imports='appsv1 "k8s.io/api/apps/v1"\n\tmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"',
        readme_body=_WEDGED_README,
        scripts={"repair.sh": _WEDGED_REPAIR},
        incident_id=incident_id,
        outcome_commit=outcome_commit,
    )


_DETECTOR_GO = """package {package}

import (
\t"context"
\t"time"

\t{imports}
\t"sdo.dev/operator/sdk"
)

const detectorID = "{detector_id}"

const playbook = "{playbook}"

type Detector struct{{}}

func New() sdk.Detector {{ return Detector{{}} }}

func (Detector) Spec() sdk.DetectorSpec {{
\treturn sdk.DetectorSpec{{
\t\tID: detectorID, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
\t\tWatches:             []sdk.WatchKind{{{watches}}},
\t\tInterval:            30 * time.Second,
\t\tPersistence:         sdk.PersistencePolicy{{Firing: 1, Clearing: 2}},
\t\tBatching:            sdk.BatchingPolicy{{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond}},
\t\tPlaybooks:           []string{{playbook}},
\t\tOriginatingIncident: {incident},
\t\tOriginatingCommit:   {commit},
\t}}
}}

{detect}
"""

_TEST_GO = """package {package}

import (
\t"context"
\t"testing"

\t{imports}
\t"sdo.dev/operator/sdk/sdktest"
)

{body}
"""

_README = """# {title}

{body}

## Verification

This playbook was learned from an incident whose cause the controller confirmed: the incident detector
`{detector_id}` encodes the cause, the health detectors that fired at dispatch cleared after the repair, and
`python3 -m sdo.agents.responder.status_cli` reported healthy before the responder returned.
"""

_NP_DETECT = """
func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
\tnamespace := snapshot.Namespace()
\tvar findings []sdk.Finding
\tfor _, policy := range snapshot.NetworkPolicies() {
\t\tservice := policy.Spec.PodSelector.MatchLabels["io.kompose.service"]
\t\tif policy.Namespace != namespace || service == "" || !isolatesCompletely(policy) {
\t\t\tcontinue
\t\t}
\t\tfindings = append(findings, sdk.Finding{
\t\t\tDetectorID:       detectorID,
\t\t\tRuleID:           "total-isolation",
\t\t\tStatus:           sdk.FindingActive,
\t\t\tSeverity:         sdk.SeverityCritical,
\t\t\tSummary:          "A NetworkPolicy denies all ingress and egress of an application workload",
\t\t\tEvidence:         "NetworkPolicy " + namespace + "/" + policy.Name + " selects io.kompose.service=" + service + " with no ingress or egress rules",
\t\t\tPrimaryResource:  sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: namespace, Name: policy.Name},
\t\t\tRelatedResources: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: service}},
\t\t\tPlaybooks:        []string{playbook},
\t\t\tFingerprint:      detectorID + "/total-isolation/" + namespace + "/" + policy.Name,
\t\t})
\t}
\treturn findings, nil
}

func isolatesCompletely(policy networkingv1.NetworkPolicy) bool {
\tingress, egress := false, false
\tfor _, policyType := range policy.Spec.PolicyTypes {
\t\tingress = ingress || policyType == networkingv1.PolicyTypeIngress
\t\tegress = egress || policyType == networkingv1.PolicyTypeEgress
\t}
\treturn ingress && egress && len(policy.Spec.Ingress) == 0 && len(policy.Spec.Egress) == 0
}
"""

_NP_TEST = """
func policy(name string, ingress []networkingv1.NetworkPolicyIngressRule) networkingv1.NetworkPolicy {
\treturn networkingv1.NetworkPolicy{
\t\tObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
\t\tSpec: networkingv1.NetworkPolicySpec{
\t\t\tPodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"io.kompose.service": "recommendation"}},
\t\t\tPolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
\t\t\tIngress:     ingress,
\t\t},
\t}
}

func TestDetectsDenyAllPolicy(t *testing.T) {
\tfindings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
\t\tNamespaceName:     "demo",
\t\tNetworkPolicyList: []networkingv1.NetworkPolicy{policy("deny-all-recommendation", nil)},
\t})
\tif err != nil {
\t\tt.Fatal(err)
\t}
\tif len(findings) != 1 || findings[0].PrimaryResource.Name != "deny-all-recommendation" {
\t\tt.Fatalf("expected the deny-all policy, got %#v", findings)
\t}
}

func TestIgnoresNearMissPolicyThatAllowsIngress(t *testing.T) {
\tallow := []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}}}
\tfindings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{
\t\tNamespaceName:     "demo",
\t\tNetworkPolicyList: []networkingv1.NetworkPolicy{policy("allow-namespace", allow)},
\t})
\tif err != nil {
\t\tt.Fatal(err)
\t}
\tif len(findings) != 0 {
\t\tt.Fatalf("a policy with ingress rules must not match, got %#v", findings)
\t}
}
"""

_NP_README = """
The incident detector establishes that a NetworkPolicy in `<NAMESPACE>` selects the pods of `<DEPLOYMENT>` and
denies all of their ingress and egress. Delete that policy; nothing else needs to change.

Sanity check (fails while the policy is present):

```sh
sh {playbook_dir}/scripts/verify.sh <NAMESPACE> <DEPLOYMENT>
```

Repair:

```sh
sh {playbook_dir}/scripts/repair.sh <NAMESPACE> <NETWORK_POLICY>
```

Then run the verification and `python3 -m sdo.agents.responder.status_cli` together:

```sh
sh {playbook_dir}/scripts/verify.sh <NAMESPACE> <DEPLOYMENT> && python3 -m sdo.agents.responder.status_cli
```
"""

_NP_REPAIR = """
#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  echo "usage: $0 NAMESPACE NETWORK_POLICY" >&2
  exit 2
fi
kubectl -n "$1" delete networkpolicy "$2" --ignore-not-found
"""

_NP_VERIFY = """
#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  echo "usage: $0 NAMESPACE DEPLOYMENT" >&2
  exit 2
fi
kubectl -n "$1" get networkpolicy -o json | python3 -c '
import json, sys
target = sys.argv[1]
for item in json.load(sys.stdin).get("items", []):
    spec = item.get("spec", {})
    selected = spec.get("podSelector", {}).get("matchLabels", {}).get("io.kompose.service") == target
    types = set(spec.get("policyTypes") or [])
    if selected and {"Ingress", "Egress"} <= types and not spec.get("ingress") and not spec.get("egress"):
        raise SystemExit("NetworkPolicy " + item["metadata"]["name"] + " isolates " + target)
print("no NetworkPolicy isolates " + target)
' "$2"
"""

_CM_DETECT = """
func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
\tnamespace := snapshot.Namespace()
\tpresent := map[string]bool{}
\tfor _, configMap := range snapshot.ConfigMaps() {
\t\tif configMap.Namespace == namespace {
\t\t\tpresent[configMap.Name] = true
\t\t}
\t}
\tvar findings []sdk.Finding
\tfor _, deployment := range snapshot.Deployments() {
\t\tif deployment.Namespace != namespace {
\t\t\tcontinue
\t\t}
\t\tfor _, reference := range sdk.ConfigMapReferencesForDeployment(deployment) {
\t\t\tif reference.Optional || present[reference.Name] || !mountFailed(snapshot.Events(), namespace, deployment.Name, reference.Name) {
\t\t\t\tcontinue
\t\t\t}
\t\t\tfindings = append(findings, sdk.Finding{
\t\t\t\tDetectorID:       detectorID,
\t\t\t\tRuleID:           "required-configmap-missing",
\t\t\t\tStatus:           sdk.FindingActive,
\t\t\t\tSeverity:         sdk.SeverityCritical,
\t\t\t\tSummary:          "A workload is blocked by its missing required ConfigMap",
\t\t\t\tEvidence:         fmt.Sprintf("deployment %s/%s requires absent ConfigMap %s and its pods report FailedMount", namespace, deployment.Name, reference.Name),
\t\t\t\tPrimaryResource:  sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: deployment.Name},
\t\t\t\tRelatedResources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: reference.Name}},
\t\t\t\tPlaybooks:        []string{playbook},
\t\t\t\tFingerprint:      detectorID + "/" + namespace + "/" + deployment.Name + "/" + reference.Name,
\t\t\t})
\t\t}
\t}
\treturn findings, nil
}

func mountFailed(events []corev1.Event, namespace, deployment, configMap string) bool {
\tfor _, event := range events {
\t\tif event.Namespace == namespace && event.Reason == "FailedMount" && event.InvolvedObject.Kind == "Pod" &&
\t\t\tstrings.HasPrefix(event.InvolvedObject.Name, deployment+"-") &&
\t\t\tstrings.Contains(event.Message, fmt.Sprintf("configmap %q not found", configMap)) {
\t\t\treturn true
\t\t}
\t}
\treturn false
}
"""

_CM_TEST = """
func snapshot(configMaps []corev1.ConfigMap) sdktest.Snapshot {
\tdeployment := appsv1.Deployment{
\t\tObjectMeta: metav1.ObjectMeta{Name: "mongodb-geo", Namespace: "demo"},
\t\tSpec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
\t\t\tName:         "init-script",
\t\t\tVolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "mongo-geo-script"}}},
\t\t}}}}},
\t}
\treturn sdktest.Snapshot{
\t\tNamespaceName:  "demo",
\t\tDeploymentList: []appsv1.Deployment{deployment},
\t\tConfigMapList:  configMaps,
\t\tEventList: []corev1.Event{{
\t\t\tObjectMeta:     metav1.ObjectMeta{Name: "mongodb-geo-abc.1", Namespace: "demo"},
\t\t\tType:           corev1.EventTypeWarning,
\t\t\tReason:         "FailedMount",
\t\t\tMessage:        `MountVolume.SetUp failed for volume "init-script" : configmap "mongo-geo-script" not found`,
\t\t\tInvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "mongodb-geo-abc"},
\t\t}},
\t}
}

func TestDetectsMissingRequiredConfigMap(t *testing.T) {
\tfindings, err := (Detector{}).Detect(context.Background(), snapshot(nil))
\tif err != nil {
\t\tt.Fatal(err)
\t}
\tif len(findings) != 1 || findings[0].PrimaryResource.Name != "mongodb-geo" {
\t\tt.Fatalf("expected the missing ConfigMap, got %#v", findings)
\t}
}

func TestIgnoresNearMissWhenConfigMapExists(t *testing.T) {
\tpresent := []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "demo"}}}
\tfindings, err := (Detector{}).Detect(context.Background(), snapshot(present))
\tif err != nil {
\t\tt.Fatal(err)
\t}
\tif len(findings) != 0 {
\t\tt.Fatalf("an existing ConfigMap must not match, got %#v", findings)
\t}
}
"""

_CM_README = """
The incident detector establishes that `<DEPLOYMENT>` in `<NAMESPACE>` mounts the absent ConfigMap `<CONFIGMAP>`
and that its pods report `FailedMount`. Restore the ConfigMap from the manifest under `kubernetes/` that defines it,
or, for `mongo-<svc>-script`, from the root init script `k8s-<svc>-mongo.sh`; then replace the stuck pods.

Sanity check (fails while the ConfigMap is missing):

```sh
sh {playbook_dir}/scripts/verify.sh <NAMESPACE> <DEPLOYMENT>
```

Repair (`-` finds the missing ConfigMap from the Deployment's volumes):

```sh
sh {playbook_dir}/scripts/repair.sh <NAMESPACE> <DEPLOYMENT> <CONFIGMAP>
```

Then verify together with `python3 -m sdo.agents.responder.status_cli`:

```sh
sh {playbook_dir}/scripts/verify.sh <NAMESPACE> <DEPLOYMENT> && python3 -m sdo.agents.responder.status_cli
```
"""

_CM_MISSING = """
missing_configmap() {
  kubectl -n "$1" get deployment "$2" -o json | python3 -c '
import json, subprocess, sys
namespace = sys.argv[1]
volumes = json.load(sys.stdin)["spec"]["template"]["spec"].get("volumes") or []
for volume in volumes:
    name = (volume.get("configMap") or {}).get("name")
    if name and subprocess.run(["kubectl", "-n", namespace, "get", "configmap", name], capture_output=True).returncode:
        print(name)
        break
' "$1"
}
"""

_CM_REPAIR = (
    """
#!/bin/sh
set -eu
if [ "$#" -ne 3 ]; then
  echo "usage: $0 NAMESPACE DEPLOYMENT CONFIGMAP" >&2
  exit 2
fi
"""
    + _CM_MISSING
    + """
CONFIGMAP=$3
if [ "$CONFIGMAP" = "-" ]; then
  CONFIGMAP=$(missing_configmap "$1" "$2")
fi
MANIFEST=$(grep -rlE "^[[:space:]]*name:[[:space:]]*$CONFIGMAP[[:space:]]*$" kubernetes/ | xargs -r grep -lE "^kind:[[:space:]]*ConfigMap" | head -n 1)
SCRIPT="k8s-${CONFIGMAP#mongo-}"
SCRIPT="${SCRIPT%-script}-mongo.sh"
if [ -n "$MANIFEST" ]; then
  kubectl -n "$1" apply -f "$MANIFEST"
elif [ -f "$SCRIPT" ]; then
  kubectl -n "$1" create configmap "$CONFIGMAP" --from-file="$SCRIPT"
else
  echo "no manifest or init script defines ConfigMap $CONFIGMAP" >&2
  exit 1
fi
kubectl -n "$1" delete pod -l "io.kompose.service=$2" --wait=false
kubectl -n "$1" rollout status "deployment/$2" --timeout=180s
"""
)

_CM_VERIFY = (
    """
#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  echo "usage: $0 NAMESPACE DEPLOYMENT" >&2
  exit 2
fi
"""
    + _CM_MISSING
    + """
MISSING=$(missing_configmap "$1" "$2")
if [ -n "$MISSING" ]; then
  echo "ConfigMap $MISSING required by $2 is missing" >&2
  exit 1
fi
kubectl -n "$1" rollout status "deployment/$2" --timeout=180s
"""
)

_WEDGED_DETECT = """
func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
\tnamespace := snapshot.Namespace()
\tvar findings []sdk.Finding
\tfor _, deployment := range snapshot.Deployments() {
\t\tif deployment.Namespace != namespace || deployment.Status.UnavailableReplicas == 0 {
\t\t\tcontinue
\t\t}
\t\tfindings = append(findings, sdk.Finding{
\t\t\tDetectorID:      detectorID,
\t\t\tRuleID:          "unavailable-replicas",
\t\t\tStatus:          sdk.FindingActive,
\t\t\tSeverity:        sdk.SeverityCritical,
\t\t\tSummary:         "A Deployment has unavailable replicas",
\t\t\tEvidence:        "deployment " + namespace + "/" + deployment.Name + " has unavailable replicas",
\t\t\tPrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: namespace, Name: deployment.Name},
\t\t\tPlaybooks:       []string{playbook},
\t\t\tFingerprint:     detectorID + "/" + namespace + "/" + deployment.Name,
\t\t})
\t}
\treturn findings, nil
}
"""

_WEDGED_TEST = """
func deployment(unavailable int32) appsv1.Deployment {
\treturn appsv1.Deployment{
\t\tObjectMeta: metav1.ObjectMeta{Name: "recommendation", Namespace: "demo"},
\t\tStatus:     appsv1.DeploymentStatus{UnavailableReplicas: unavailable},
\t}
}

func TestDetectsUnavailableReplicas(t *testing.T) {
\tfindings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{deployment(1)}})
\tif err != nil || len(findings) != 1 {
\t\tt.Fatalf("expected one finding, got %#v %v", findings, err)
\t}
}

func TestIgnoresNearMissAvailableDeployment(t *testing.T) {
\tfindings, err := (Detector{}).Detect(context.Background(), sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{deployment(0)}})
\tif err != nil || len(findings) != 0 {
\t\tt.Fatalf("expected no finding, got %#v %v", findings, err)
\t}
}
"""

_WEDGED_README = """
Restart `<DEPLOYMENT>` in `<NAMESPACE>`; its pods were wedged after a stale rollout.

```sh
sh {playbook_dir}/scripts/repair.sh <NAMESPACE> <DEPLOYMENT>
```
"""

_WEDGED_REPAIR = """
#!/bin/sh
set -eu
kubectl -n "$1" rollout restart "deployment/$2"
kubectl -n "$1" rollout status "deployment/$2" --timeout=180s
"""
