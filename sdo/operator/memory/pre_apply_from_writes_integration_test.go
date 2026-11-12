//go:build integration

package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

// A raw kubectl write has no plan application, so the proxy's recorded before-state of each written object stands in:
// open@pre-apply is the detection snapshot with those objects as they stood before the agent's first write to them.
func TestPreApplyCaptureOverlaysTheRecordedBeforeStatesOnTheDetectionSnapshot(t *testing.T) {
	r := newRecording(t, "incident-1")
	dir, err := IncidentCorpusDir(r.root, "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	detect := `{"namespace":"app","object_snapshot_version":2,"objects":[` +
		`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app"},"spec":{"replicas":1}},` +
		`{"apiVersion":"v1","kind":"Service","metadata":{"name":"front","namespace":"app"}}]}`
	if err := os.WriteFile(filepath.Join(dir, "open@detect.json"), []byte(detect), 0o600); err != nil {
		t.Fatal(err)
	}
	// The detection view is older than the incident's own earlier change: the object stood at replicas 5 when the agent wrote.
	r.now = r.now.Add(time.Minute)
	before := apihistory.Event{RequestID: "aaaaaaaab", Phase: "before", Started: r.now, Verb: "get", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "api"}
	r.store.Record(before, []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app"},"spec":{"replicas":5}}`))
	r.do(call{verb: "patch", group: "apps", version: "v1", resource: "deployments", namespace: "app", name: "api", status: 200, body: `{}`})
	r.store.Close()

	if err := CapturePreApplyFromWrites(r.root, "incident-1", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@pre-apply.json"))
	if err != nil {
		t.Fatalf("no open@pre-apply capture for a raw write: %v", err)
	}
	var snapshot struct {
		Objects []struct {
			Kind     string                 `json:"kind"`
			Metadata struct{ Name string }  `json:"metadata"`
			Spec     struct{ Replicas int } `json:"spec"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Objects) != 2 {
		t.Fatalf("objects = %s, want both detection objects kept", raw)
	}
	for _, object := range snapshot.Objects {
		if object.Kind == "Deployment" && object.Spec.Replicas != 5 {
			t.Fatalf("the written Deployment has replicas %d, want its recorded before-state (5)", object.Spec.Replicas)
		}
		if object.Kind == "Service" && object.Metadata.Name != "front" {
			t.Fatalf("an unwritten object changed: %s", raw)
		}
	}
	// A later call never replaces the capture.
	if err := CapturePreApplyFromWrites(r.root, "incident-1", time.Now().UTC()); err != nil || !strings.Contains(string(raw), "\"replicas\":5") {
		t.Fatalf("second call: %v", err)
	}
}
