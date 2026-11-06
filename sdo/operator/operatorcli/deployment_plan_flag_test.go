package operatorcli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/rendered"
)

const providedPlanJSON = `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}},"expectedWorkloadIds":["Deployment/app/web"]}`

func hostArgs(extra ...string) []string {
	args := []string{
		"host", "--repository", "/repo", "--namespace", "app", "--application", "app", "--health-objective", "stay healthy",
		"--operation-id", "op-1", "--controller-image", "controller:test", "--responder-image", "responder:test",
		"--buildkit-image", "buildkit@sha256:" + strings.Repeat("a", 64), "--registry-push-secret", "push",
		"--repository-pvc", "repo", "--credentials-secret", "creds", "--model", "m",
	}
	return append(args, extra...)
}

func TestOperateDeploymentPlanReachesTheLifecycle(t *testing.T) {
	launch, err := ParseOperateLaunch(operateArgs("--deployment-plan", providedPlanJSON))
	if err != nil {
		t.Fatal(err)
	}
	plan := launch.Lifecycle.ProvidedPlan
	if plan == nil || plan.GetSource().GetRendererId() != "raw" || len(plan.GetExpectedWorkloadIds()) != 1 {
		t.Fatalf("provided plan = %v", plan)
	}
	plain, err := ParseOperateLaunch(operateArgs())
	if err != nil || plain.Lifecycle.ProvidedPlan != nil {
		t.Fatalf("no flag = %v, %v", plain.Lifecycle.ProvidedPlan, err)
	}
	for name, bad := range map[string]string{"not json": "chart", "no source": `{"expectedWorkloadIds":["Deployment/app/web"]}`} {
		if _, err := ParseOperateLaunch(operateArgs("--deployment-plan", bad)); err == nil || !strings.Contains(err.Error(), "deployment plan") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestHostDeploymentPlanReachesTheOperateJobConfig(t *testing.T) {
	launch, err := ParseHostLaunch(hostArgs("--deployment-plan", providedPlanJSON))
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(launch.Install.DeploymentPlan, "\n\t ") || !strings.Contains(launch.Install.DeploymentPlan, `"rendererId":"raw"`) {
		t.Fatalf("install plan = %q, want the compact plan", launch.Install.DeploymentPlan)
	}
	plain, err := ParseHostLaunch(hostArgs())
	if err != nil || plain.Install.DeploymentPlan != "" {
		t.Fatalf("no flag = %q, %v", plain.Install.DeploymentPlan, err)
	}
	if _, err := ParseHostLaunch(hostArgs("--deployment-plan", `{"images":[]}`)); err == nil {
		t.Fatal("a plan without a source reached the cluster")
	}
}

// The adapter's own emitted host argv (tests/fixtures/sdo/contracts) is what
// the harness really sends: its plan, in the adapter's ProtoJSON spelling, must
// parse into a provided helm plan with a BuildKit image.
func TestHostParsesTheAdapterEmittedDeploymentPlan(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "adapter_launch_args.json"))
	if err != nil {
		t.Fatal(err)
	}
	var emitted map[string][]string
	if err := json.Unmarshal(data, &emitted); err != nil {
		t.Fatal(err)
	}
	launch, err := ParseHostLaunch(emitted["host"])
	if err != nil {
		t.Fatalf("adapter host argv does not parse: %v", err)
	}
	plan, err := rendered.ParseDeploymentPlan(launch.Install.DeploymentPlan)
	if err != nil || plan.GetSource().GetRendererId() != "helm" || len(plan.GetImages()) != 1 || plan.GetImages()[0].GetSourceId() != "buildkit" {
		t.Fatalf("plan = %v, %v", plan, err)
	}
}
