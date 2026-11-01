package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"testing"
)

func writeReflectionCheckDecision(t *testing.T, root string, response *contracts.ReflectionResult) {
	t.Helper()
	data, err := protojson.Marshal(&contracts.ReflectionDecision{LearningDecision: response.LearningDecision, NoChangeReason: response.NoChangeReason, PlaybookIds: response.ProposedArtifactIds})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", validate.DecisionFile), data, 0600); err != nil {
		t.Fatal(err)
	}
}

// writeCapturedProgramCorpus copies the immutable evidence while authoring only
// logical labels and bindings, as a direct-edit reflection worker does.
func writeCapturedProgramCorpus(t *testing.T, root, capture, incidentID, programID, parameter, kind, namespace, match, peer, phase string) {
	t.Helper()
	dir := filepath.Join(root, ".sdo/corpus", programID+"-case")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	opened, err := os.ReadFile(filepath.Join(capture, "open@"+phase+".json"))
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := os.ReadFile(filepath.Join(capture, "fixed.json"))
	if err != nil {
		t.Fatal(err)
	}
	recordings, err := os.ReadFile(filepath.Join(capture, "open@"+phase+"-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	apiVersion := "v1"
	if kind == "NetworkPolicy" {
		apiVersion = "networking.k8s.io/v1"
	}
	labels, err := json.Marshal(map[string]any{
		"root_cause_program_ids": []string{programID},
		"bindings":               map[string]any{parameter: map[string]string{"api_version": apiVersion, "kind": kind, "namespace": namespace, "name": match}},
		"peer_bindings":          map[string]any{parameter: map[string]string{"api_version": apiVersion, "kind": kind, "namespace": namespace, "name": peer}},
		"provenance":             map[string]string{"incident_id": incidentID, "open_at": phase, "open_sha256": fmt.Sprintf("%x", sha256.Sum256(opened)), "fixed_sha256": fmt.Sprintf("%x", sha256.Sum256(fixed))},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"open.json": opened, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings, "labels.json": labels} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
