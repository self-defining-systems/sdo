package operatorcli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

func TestEvidenceReadOnlyHistoryShowAndStateCuts(t *testing.T) {
	dir := t.TempDir()
	s, err := apihistory.Open(dir, "incident", apihistory.Limits{Queue: 16})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Record(apihistory.Event{Phase: "end", Started: start, Finished: start, Verb: "get", Resource: "configmaps", Namespace: "app", Name: "settings", Status: 200}, []byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"settings","namespace":"app"},"data":{"mode":"broken"}}`))
	s.Record(apihistory.Event{Phase: "start", Started: start.Add(time.Second), Verb: "patch", Resource: "configmaps", Namespace: "app", Name: "settings"}, nil)
	s.Record(apihistory.Event{Phase: "end", Started: start.Add(time.Second), Finished: start.Add(2 * time.Second), Verb: "patch", Resource: "configmaps", Namespace: "app", Name: "settings", Status: 200}, []byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"settings","namespace":"app"},"data":{"mode":"fixed"}}`))
	s.Close()
	before, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	env := func(k string) string {
		if k == "SDO_API_HISTORY_DIR" {
			return dir
		}
		return ""
	}
	for _, args := range [][]string{{"history"}, {"show", "--event", "1"}, {"state", "ConfigMap/app/settings", "--at", "before-first-mutation"}, {"state", "ConfigMap/app/settings", "--at", "after-fix"}} {
		var out bytes.Buffer
		if err := RunEvidence(args, env, &out); err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if args[0] == "state" {
			if result["unknown"] != true || result["stale"] != true {
				t.Fatalf("claimed reconstructed state: %s", out.String())
			}
			if args[3] == "before-first-mutation" && (!bytes.Contains(out.Bytes(), []byte(`"last_observed"`)) || bytes.Contains(out.Bytes(), []byte("broken"))) {
				t.Fatalf("lost last observation: %s", out.String())
			}
			if args[3] == "after-fix" && bytes.Contains(out.Bytes(), []byte("fixed")) {
				t.Fatal("last write treated as recovery")
			}
		}
	}
	after, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("CLI changed history")
	}
}

func TestEvidenceMissingHistoryIsUnknown(t *testing.T) {
	var out bytes.Buffer
	err := RunEvidence([]string{"state", "Pod/app/api", "--at", "2026-01-01T00:00:00Z"}, func(string) string { return t.TempDir() }, &out)
	if err != nil || !bytes.Contains(out.Bytes(), []byte(`"unknown":true`)) {
		t.Fatalf("missing history: %s %v", out.String(), err)
	}
}

func TestEvidenceStateDoesNotInferAbsenceFromPartialListsOrCrossingWrite(t *testing.T) {
	dir := t.TempDir()
	s, _ := apihistory.Open(dir, "incident", apihistory.Limits{Queue: 32})
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, partial := range []bool{true, false} {
		s.Record(apihistory.Event{Phase: "end", Started: start, Finished: start, Verb: "list", Resource: "pods", Namespace: "app", Status: 200, Partial: partial}, []byte(`{"apiVersion":"v1","kind":"PodList","metadata":{"continue":"more"},"items":[]}`))
	}
	s.Record(apihistory.Event{Phase: "end", Started: start, Finished: start.Add(3 * time.Second), Verb: "get", Resource: "pods", Namespace: "app", Name: "api", Status: 200}, []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"api","namespace":"app"}}`))
	s.Record(apihistory.Event{Phase: "start", Started: start.Add(time.Second), Verb: "get", Subresource: "exec"}, nil)
	s.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	result := stateAt(h, dir, "Pod/app/api", "before-first-mutation")
	if result.ObservedAbsent || !result.ObservedAt.IsZero() || !result.Unknown {
		t.Fatalf("unsafe cut: %+v", result)
	}
	if err := os.WriteFile(filepath.Join(dir, "recovery.json"), []byte(`{"verified_at":"2026-01-01T00:00:01Z"}`), 0400); err != nil {
		t.Fatal(err)
	}
	result = stateAt(h, dir, "Pod/app/api", "after-fix")
	if !result.ObservedAt.IsZero() {
		t.Fatal("crossing request counted as after recovery")
	}
}
