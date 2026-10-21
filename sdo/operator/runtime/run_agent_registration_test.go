package runtime

import (
	"context"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/worker_registry"
)

func TestMissingDriftRegistrationWarnsOnlyWhenEnabled(t *testing.T) {
	registry, err := worker_registry.New(builtInResponderRegistration("worker:test", []string{"/worker"}))
	if err != nil {
		t.Fatal(err)
	}
	var status bytes.Buffer
	var log bytes.Buffer
	warnings := reportDriftRegistration(registry, true, &status, &log)
	var record struct {
		ControllerStatus struct {
			Warnings []string `json:"warnings"`
		} `json:"controller_status"`
	}
	if err := json.Unmarshal(status.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "sdo-drift-investigator") ||
		!strings.Contains(log.String(), warnings[0]) || len(record.ControllerStatus.Warnings) != 1 || record.ControllerStatus.Warnings[0] != warnings[0] {
		t.Fatalf("missing drift investigator was not surfaced: warnings=%v status=%q log=%q", warnings, status.String(), log.String())
	}
	status.Reset()
	log.Reset()
	if got := reportDriftRegistration(registry, false, &status, &log); len(got) != 0 || status.Len() != 0 || log.Len() != 0 {
		t.Fatalf("disabled drift emitted warning: %v %q %q", got, status.String(), log.String())
	}
}

func TestRunRejectsUnregisteredResponderBeforeStarting(t *testing.T) {
	directory := t.TempDir()
	manifest := `{"schema_version":"sdo.dev/v1alpha1","agent_id":"different-worker","capabilities":[{"task_type":"incident-response","request_type_url":"type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest","result_type_url":"type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult"}],"kubernetes_job":{"image":"example/worker:1","command":["/worker"]}}`
	if err := os.WriteFile(filepath.Join(directory, "worker.json"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{
		"--namespace", "demo", "--app-root", t.TempDir(), "--dispatcher-mode", "job",
		"--repository-pvc", "repo", "--responder-credentials-secret", "agent-creds",
		"--agent-registration-directory", directory, "--responder-agent-id", "missing",
	}, NativeBroker: &NativeIncidentBroker{}})
	if err == nil || !strings.Contains(err.Error(), `unknown agent "missing"`) {
		t.Fatalf("unknown registered worker did not fail closed: %v", err)
	}
}
