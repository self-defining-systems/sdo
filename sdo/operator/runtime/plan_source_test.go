package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const syntheticPlan = `{"source":{"rendererId":"kustomize","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.KustomizeRenderConfig","path":"kubernetes"}}}` + "\n"

func writePlanFile(t *testing.T, root, content string) {
	t.Helper()
	path := filepath.Join(root, ".sdo", "deployment", "plan.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedDeploymentSourceReadsThePlanFile(t *testing.T) {
	root := t.TempDir()
	if source, err := acceptedDeploymentSource(root); err != nil || source != nil {
		t.Fatalf("no plan file: source = %v, err = %v; want none", source, err)
	}
	writePlanFile(t, root, syntheticPlan)
	source, err := acceptedDeploymentSource(root)
	if err != nil || source.GetRendererId() != "kustomize" {
		t.Fatalf("source = %v, err = %v", source, err)
	}
}

func TestAcceptedDeploymentSourceRejectsAnInvalidPlan(t *testing.T) {
	root := t.TempDir()
	writePlanFile(t, root, `{"images":[]}`+"\n")
	if _, err := acceptedDeploymentSource(root); err == nil || !strings.Contains(err.Error(), "plan.yaml") {
		t.Fatalf("plan without a source: err = %v", err)
	}
}

func TestControllerConfigNamesNoDeploymentSourceFlag(t *testing.T) {
	config := &contractsv1alpha1.ControllerConfig{Namespace: "shop", AppRoot: "/repo", Application: "shop"}
	for _, arg := range controllerConfigToArgs(config) {
		if strings.Contains(arg, "deployment-source") || strings.Contains(arg, "deployed-manifest-path") {
			t.Fatalf("controller argument %q: the source comes from plan.yaml", arg)
		}
	}
}
