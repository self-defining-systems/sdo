package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func learningTrustRepository(t *testing.T) (string, string) {
	t.Helper()
	root := nativeRepository(t)
	files := map[string]string{
		".sdo/playbooks/service-targetport-mismatch/diagnose.go": "package targetport\n",
		".sdo/playbooks/service-targetport-mismatch/mitigate.go": "package targetport\n",
		".sdo/corpus/learning/open.json":                         `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":9999}]}}]}`,
		".sdo/corpus/learning/fixed.json":                        `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"api"},"spec":{"ports":[{"port":80,"targetPort":8080}]}}]}`,
	}
	files[".sdo/corpus/learning/healthy.json"] = files[".sdo/corpus/learning/fixed.json"]
	files[".sdo/corpus/learning/recordings.json"] = "[]"
	labels := map[string]any{"bindings": program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}, "root_cause_program_ids": []string{"service-targetport-mismatch"}, "provenance": map[string]string{
		"incident_id": "learning", "open_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/open.json"]))),
		"fixed_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(files[".sdo/corpus/learning/fixed.json"]))),
	}}
	data, _ := json.Marshal(labels)
	files[".sdo/corpus/learning/labels.json"] = string(data)
	for name, content := range files {
		full := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "learn\n\nSDO-Incident: learning\nSDO-Actor: responder\nSDO-Phase: reflection\nSDO-Validation: passed")
	return root, nativeGit(t, root, "rev-parse", "HEAD")
}

func TestAcceptedCorpusDoesNotInventTrustForAnUnmatchedOrigin(t *testing.T) {
	root, commit := learningTrustRepository(t)
	record := &contracts.OutcomeRecord{IncidentId: "learning", Classification: "success"}
	dispatcher := KubernetesJobDispatcher{ProgramTrustThreshold: 1, Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: "learning", Record: record}}}}
	trust, err := dispatcher.programTrust(context.Background(), IncidentRequest{RepositoryWorktree: root, SourceCommit: commit})
	if err != nil || trust["service-targetport-mismatch"].VerifiedSuccesses != 0 || trust["service-targetport-mismatch"].Trusted {
		t.Fatalf("corpus invented origin credit: %+v %v", trust, err)
	}
}
