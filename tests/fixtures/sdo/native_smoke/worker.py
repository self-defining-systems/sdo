"""Deterministic typed workers for an isolated Kubernetes integration smoke."""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import urllib.request
from pathlib import Path

from sdo.contracts import (
    AgentResult,
    AgentTask,
    ArchitectureRequest,
    ArchitectureResult,
    DeploymentPlan,
    HealthJudgeRequest,
    HealthJudgeResult,
    RawManifestStrategy,
    SourceDeploymentRequest,
    SourceDeploymentResult,
    parse_json,
    to_canonical_json,
)

IMAGE = "registry.k8s.io/pause@sha256:ee6521f290b2168b6e0935a181d4cff9be1ac3f505666ef0e3c98fae8199917a"

DETECTOR = r"""package objective

import (
    "context"
    "time"
    "sdo.dev/operator/sdk"
)

type Detector struct{}
func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
    return sdk.DetectorSpec{
        ID: "health-objective", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
        Description: "demo Deployment has an available replica",
        Interval: 30*time.Second,
        Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
        Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500*time.Millisecond},
        Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
        Playbooks: []string{"health-objective"},
        OriginatingCommit: "lifecycle-bootstrap",
    }
}
func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
    for _, deployment := range snapshot.Deployments() {
        if deployment.Namespace == "demo" && deployment.Name == "demo" && deployment.Status.AvailableReplicas > 0 {
            return nil, nil
        }
    }
    return []sdk.Finding{{
        RuleID: "deployment-unavailable", Status: sdk.FindingActive,
        Severity: sdk.SeverityCritical,
        Summary: "demo Deployment has no available replica",
        Evidence: "Deployment demo/demo is missing or unavailable",
        PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "demo"},
        Playbooks: []string{"health-objective"},
        Fingerprint: "demo/demo/unavailable",
    }}, nil
}
"""

PLAYBOOK = """# Deployment unavailable

Inspect <AFFECTED_RESOURCE>, repair application source, redeploy, and verify an available replica.
"""


def write_source_marker(checkout: Path, base_commit: str) -> None:
    """Make each deployment attempt a real source proposal against accepted HEAD."""

    marker = checkout / "source-smoke.txt"
    content = f"source proposal based on {base_commit}\n"
    if marker.exists() and marker.read_text() == content:
        content += "retry\n"
    marker.write_text(content)


def run() -> None:
    task = parse_json(Path("/sdo/request/agent-task.json").read_text(), AgentTask())
    checkout = Path(task.workspace.checkout_path)
    if task.task_type == "source-deployment":
        request = SourceDeploymentRequest()
        assert task.request.Unpack(request)
        write_source_marker(checkout, task.workspace.base_commit)
        deploy = checkout / "deploy"
        deploy.mkdir(exist_ok=True)
        (deploy / "demo.yaml").write_text(f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: demo
  namespace: {request.namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: demo
  template:
    metadata:
      labels:
        app: demo
    spec:
      containers:
      - name: pause
        image: {IMAGE}
""")
        raw = RawManifestStrategy(
            manifest_paths=["deploy/demo.yaml"], expected_workload_ids=[f"Deployment/{request.namespace}/demo"]
        )
        plan = DeploymentPlan(strategy_id="raw-manifests-v1")
        plan.strategy_config.Pack(raw)
        typed = SourceDeploymentResult(
            proposed=True, summary="deterministic source fixture", agent_session_id="fixture-source", plan=plan
        )
    elif task.task_type == "source-architecture":
        request = ArchitectureRequest()
        assert task.request.Unpack(request)
        typed = ArchitectureResult(
            summary_markdown="# Architecture\n\nOne Deployment runs a pinned pause image.",
            topology_fingerprint=request.source_topology_fingerprint,
            source_commit=request.accepted_source_commit,
            agent_session_id="fixture-architecture",
        )
    elif task.task_type == "health-judgment-direct-v1":
        request = HealthJudgeRequest()
        assert task.request.Unpack(request)
        detector = checkout / ".sdo/diagnostics/detectors/health/health-objective"
        detector.mkdir(parents=True, exist_ok=True)
        (detector / "manifest.yaml").write_text("""apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: health-objective
constructor: New
class: health
owner: health_judge
watches:
  - apiVersion: apps/v1
    kind: Deployment
interval: 30s
persistence:
  firing: 2
  clearing: 2
batching:
  severity: critical
  debounce: 500ms
possiblePlaybooks: [health-objective]
originatingCommit: lifecycle-bootstrap
""")
        (detector / "detector.go").write_text(DETECTOR)
        playbooks = checkout / ".sdo/playbooks"
        (playbooks / "health-objective").mkdir(parents=True, exist_ok=True)
        (playbooks / "health-objective/README.md").write_text(
            "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\n"
            "originating_incident: lifecycle-bootstrap\n---\n" + PLAYBOOK
        )
        (playbooks / "README.md").write_text("# Playbooks\n\n- [health-objective](health-objective/README.md)\n")
        digest = hashlib.sha256(request.health_objective.strip().encode()).hexdigest()
        typed = HealthJudgeResult(
            authored=True,
            summary="deterministic availability detector",
            agent_session_id="fixture-judge",
            objective_digest=digest,
            detector_ids=["health-objective"],
        )
    else:
        raise ValueError(f"unsupported task type {task.task_type}")
    result = AgentResult(
        schema_version="sdo.dev/v1alpha1",
        task_id=task.task_id,
        status="completed",
        proposal_id=task.workspace.proposal_id,
    )
    result.response.Pack(typed)
    publish_result(to_canonical_json(result))


def publish_result(payload: str) -> None:
    namespace = os.environ["SDO_NAMESPACE"]
    name = os.environ["SDO_RESULT_CONFIGMAP"]
    service_account = Path("/var/run/secrets/kubernetes.io/serviceaccount")
    token = (service_account / "token").read_text().strip()
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    body = json.dumps(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "namespace": namespace},
            "data": {"agent-result.json": payload},
        }
    ).encode()
    request = urllib.request.Request(
        f"https://{host}:{port}/api/v1/namespaces/{namespace}/configmaps",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    context = ssl.create_default_context(cafile=str(service_account / "ca.crt"))
    with urllib.request.urlopen(request, context=context, timeout=30):
        pass


if __name__ == "__main__":
    run()
