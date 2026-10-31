"""Write one ordinary synthetic health detector and a typed worker result.

This benchmark-only worker has no model, Kubernetes access or production imports.
Usage: python3 -m benchmarks.sregym.agent_script.synthetic_health_judge TASK_JSON RESULT_JSON
"""

import json
import subprocess
import sys
from pathlib import Path


def main() -> None:
    task = json.loads(Path(sys.argv[1]).read_text())
    root = Path(task["workspace"]["checkoutPath"])
    detector = root / ".sdo/diagnostics/detectors/health/application-config"
    detector.mkdir(parents=True, exist_ok=True)
    (detector / "manifest.yaml").write_text("""apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: application-config
constructor: New
class: health
owner: health_judge
watches:
  - apiVersion: v1
    kind: ConfigMap
interval: 17s
persistence: {firing: 1, clearing: 3}
batching: {severity: warn, debounce: "0"}
possiblePlaybooks: [health-objective]
originatingCommit: lifecycle-bootstrap
""")
    (detector / "detector.go").write_text("""package applicationconfig

import (
    "time"
    "sdo.dev/operator/sdk"
)

type detector struct{}

func New() sdk.Detector { return detector{} }
func (detector) Spec() sdk.DetectorSpec {
    return sdk.DetectorSpec{ID: "application-config", Class: sdk.DetectorClassHealth,
        Owner: sdk.DetectorOwnerHealthJudge, Watches: []sdk.Watch{{APIVersion: "v1", Kind: "ConfigMap"}},
        Interval: 17*time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 3},
        Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning},
        Playbooks: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap"}
}
func (detector) Detect(ctx sdk.DetectionContext) ([]sdk.Finding, error) {
    for _, config := range ctx.ConfigMaps() {
        if config.Data["synthetic-invalid"] == "true" {
            return []sdk.Finding{{RuleID: "invalid-config", Status: sdk.FindingActive,
                Severity: sdk.SeverityWarning, Summary: "synthetic invalid configuration",
                PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap",
                    Namespace: config.Namespace, Name: config.Name},
                Playbooks: []string{"health-objective"}}}, nil
        }
    }
    return nil, nil
}
""")
    (detector / "detector_test.go").write_text("""package applicationconfig

import (
    "testing"
    corev1 "k8s.io/api/core/v1"
    "sdo.dev/operator/sdk/sdktest"
)

func TestConfiguration(t *testing.T) {
    for _, invalid := range []bool{false, true} {
        value := "false"
        if invalid { value = "true" }
        snapshot := sdktest.Snapshot{ConfigMapList: []corev1.ConfigMap{
            {Data: map[string]string{"synthetic-invalid": value}},
        }}
        findings, err := New().Detect(snapshot)
        if err != nil || (len(findings) > 0) != invalid {
            t.Fatalf("invalid=%v findings=%v error=%v", invalid, findings, err)
        }
    }
}
""")
    books = root / ".sdo/playbooks"
    (books / "health-objective").mkdir(parents=True, exist_ok=True)
    (books / "health-objective/README.md").write_text(
        "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\n"
        "originating_incident: lifecycle-bootstrap\n---\n# Repair <AFFECTED_RESOURCE>\n"
    )
    (books / "README.md").write_text("# Playbooks\n\n- [health-objective](health-objective/README.md)\n")
    subprocess.run(["gofmt", "-w", str(detector)], check=True)
    Path(sys.argv[2]).write_text(
        json.dumps(
            {
                "schemaVersion": task["schemaVersion"],
                "taskId": task["taskId"],
                "status": "completed",
                "proposalId": task["workspace"]["proposalId"],
                "response": {
                    "@type": "type.googleapis.com/sdodev.contracts.v1alpha1.HealthJudgeResult",
                    "authored": True,
                    "summary": "scripted ordinary detector",
                    "agentSessionId": "scripted-judge",
                    "detectorIds": ["application-config"],
                },
            }
        )
    )


if __name__ == "__main__":
    main()
