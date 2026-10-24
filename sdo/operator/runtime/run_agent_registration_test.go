package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunRejectsUnregisteredResponderBeforeStarting(t *testing.T) {
	directory := t.TempDir()
	manifest := `{"schema_version":"sdo.dev/v1alpha1","agent_id":"different-worker","capabilities":[{"task_type":"incident-response","request_type_url":"type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest","result_type_url":"type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult"}],"kubernetes_job":{"image":"example/worker:1","command":["/worker"]}}`
	if err := os.WriteFile(filepath.Join(directory, "worker.json"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{
		"--namespace", "demo", "--app-root", t.TempDir(), "--dispatcher-mode", "job",
		"--toolchain-image", "controller:compiler", "--repository-pvc", "repo", "--responder-credentials-secret", "agent-creds",
		"--agent-registration-directory", directory, "--responder-agent-id", "missing",
	}, NativeBroker: &NativeIncidentBroker{}})
	if err == nil || !strings.Contains(err.Error(), `unknown agent "missing"`) {
		t.Fatalf("unknown registered worker did not fail closed: %v", err)
	}
}
