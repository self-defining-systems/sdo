# ruff: noqa: E501 - Go, shell and Markdown templates keep their natural line lengths.
"""Source templates compiled into assurance agent scripts and learned memory."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

SERVICE_LABEL = "io.kompose.service"


@dataclass(frozen=True)
class MemoryFiles:
    """Final detector and playbook files authored by scripted reflection."""

    detector_id: str
    playbook_id: str
    files: dict[str, str]
    executable_paths: tuple[str, ...]


@dataclass(frozen=True)
class Facts:
    """What the diagnosis established about the live fault."""

    namespace: str
    target: str
    objects: dict[str, str] = field(default_factory=dict)
    observations: tuple[tuple[str, str], ...] = ()


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

    #: The wrong fix restarts the isolated service's caller. Restarting the isolated
    #: service itself would crash-loop it past the fault's recovery window.
    wrong_workload = "frontend"

    def wrong_repair(self, facts: Facts) -> tuple[str, str]:
        return _restart(facts.namespace, self.wrong_workload)

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

    def memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryFiles:
        proposal = _memory_files(
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
            readme_body="Diagnose the bound deny-all NetworkPolicy <NETWORK_POLICY> and remove it to restore <WORKLOAD>. "
            "Verify independent health after mitigation.",
            scripts={},
            incident_id=incident_id,
            outcome_commit=outcome_commit,
        )

        directory = f".sdo/playbooks/{self.playbook_id}"
        return MemoryFiles(
            proposal.detector_id,
            proposal.playbook_id,
            {**proposal.files, f"{directory}/diagnose.go": _NP_PROGRAM},
            (),
        )

    def claimed_memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryFiles:
        return _claimed_files(incident_id=incident_id, outcome_commit=outcome_commit)


class MissingConfigMap:
    name = "missing_configmap"
    detector_id = "required-configmap-missing"
    package = "required_configmap_missing"
    playbook_id = "required-configmap-missing"

    def wrong_repair(self, facts: Facts) -> tuple[str, str]:
        return _restart(facts.namespace, facts.target)

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

    def memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryFiles:
        return _memory_files(
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

    def claimed_memory(self, facts: Facts, *, incident_id: str, outcome_commit: str) -> MemoryFiles:
        return _claimed_files(incident_id=incident_id, outcome_commit=outcome_commit)


PLANS: dict[str, NetworkPolicyBlock | MissingConfigMap] = {
    "network_policy_block": NetworkPolicyBlock(),
    "missing_configmap": MissingConfigMap(),
}


# --- Operational-memory templates -------------------------------------------------------------------------


def _memory_files(
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
) -> MemoryFiles:
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
    detector_dir = f".sdo/diagnostics/detectors/incidents/{detector_id}"
    playbook_path = f".sdo/playbooks/{playbook}"
    manifest = (
        "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\n"
        f"id: {detector_id}\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n"
        + "".join(f"  - apiVersion: {api}\n    kind: {kind}\n" for api, kind in watches)
        + "interval: 30s\npersistence:\n  firing: 1\n  clearing: 2\n"
        "batching:\n  severity: critical\n  debounce: 500ms\n"
        f"possiblePlaybooks:\n  - {playbook}\noriginatingIncident: {json.dumps(incident_id)}\n"
        f"originatingCommit: {json.dumps(outcome_commit)}\n"
    )
    files = {
        f"{detector_dir}/manifest.yaml": manifest,
        f"{detector_dir}/detector.go": detector_go,
        f"{detector_dir}/detector_test.go": detector_test,
        f"{playbook_path}/README.md": (
            "---\nschema_version: 1\nowner: responder\n"
            f"fault_class: {fault_class}\noriginating_incident: {json.dumps(incident_id)}\n---\n\n{readme}"
        ),
        **{f"{playbook_path}/scripts/{name}": text.lstrip("\n") for name, text in scripts.items()},
    }
    return MemoryFiles(detector_id, playbook, files, tuple(f"{playbook_path}/scripts/{name}" for name in scripts))


def _claimed_files(*, incident_id: str, outcome_commit: str) -> MemoryFiles:
    return _memory_files(
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
`sdo verify` reported healthy before the responder returned.
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

_NP_PROGRAM = """package deny_all_networkpolicy_isolation

import (
	"context"

	networkingv1 "k8s.io/api/networking/v1"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "deny-all-networkpolicy-isolation", Params: []program.ParamSpec{{Name: "policy", Kind: "NetworkPolicy", Required: true}}}
}

func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	ref := bindings.Objects["policy"]
	for _, policy := range view.NetworkPolicies() {
		if policy.Namespace != ref.Namespace || policy.Name != ref.Name {
			continue
		}
		ingress, egress := false, false
		for _, direction := range policy.Spec.PolicyTypes {
			ingress = ingress || direction == networkingv1.PolicyTypeIngress
			egress = egress || direction == networkingv1.PolicyTypeEgress
		}
		if ingress && egress && len(policy.Spec.Ingress) == 0 && len(policy.Spec.Egress) == 0 && policy.Spec.PodSelector.MatchLabels["io.kompose.service"] != "" {
			return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "workload network isolation", Observed: "deny-all ingress and egress", Expected: "workload connectivity"}}}, nil
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (p Program) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, view, bindings)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: bindings.Objects["policy"], Reason: "remove the deny-all policy isolating the workload"}}}, nil
}
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

Then verify together with `sdo verify`:

```sh
sh {playbook_dir}/scripts/verify.sh <NAMESPACE> <DEPLOYMENT> && sdo verify
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
