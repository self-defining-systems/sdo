package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type selfCheckDetector struct {
	id     string
	calls  *int
	firing *bool
}

func (d selfCheckDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth}
}
func (d selfCheckDetector) Detect(_ context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	*d.calls++
	if !*d.firing {
		return nil, nil
	}
	return []sdk.Finding{{DetectorID: d.id, RuleID: "synthetic", Status: sdk.FindingActive, Fingerprint: d.id, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: d.id}}}, nil
}

func exchangeSelfCheck(t *testing.T, server *SessionToolServer, payload string) ToolResponse {
	t.Helper()
	name := fmt.Sprintf("call-%d.json", len(mustToolEntries(t, filepath.Join(server.Directory, "responses"))))
	if err := os.WriteFile(filepath.Join(server.Directory, "requests", name), []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	if err := server.process(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(server.Directory, "responses", name))
	if err != nil {
		t.Fatal(err)
	}
	var result ToolResponse
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func mustToolEntries(t *testing.T, path string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestResponderDetectFreshSelectionThroughTransport(t *testing.T) {
	a, b := 0, 0
	firing := true
	loop := NewResponderLoop(IncidentRequest{Namespace: "app"}, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, []sdk.Detector{selfCheckDetector{"a", &a, &firing}, selfCheckDetector{"b", &b, &firing}}, PlanExecutor{}, 3)
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	all := exchangeSelfCheck(t, &server, `{"command":"detect"}`)
	if all.Error != "" || len(all.Findings) != 2 || a != 1 || b != 1 {
		t.Fatalf("all: %+v calls=%d,%d", all, a, b)
	}
	one := exchangeSelfCheck(t, &server, `{"command":"detect","detector_ids":["b","b"]}`)
	if one.Error != "" || len(one.Findings) != 1 || one.Findings[0].DetectorID != "b" || a != 1 || b != 2 {
		t.Fatalf("selected: %+v calls=%d,%d", one, a, b)
	}
	firing = false
	fresh := exchangeSelfCheck(t, &server, `{"command":"detect","detector_ids":["a"]}`)
	if fresh.Error != "" || len(fresh.Findings) != 0 || a != 2 {
		t.Fatalf("stale findings: %+v", fresh)
	}
	unknown := exchangeSelfCheck(t, &server, `{"command":"detect","detector_ids":["missing"]}`)
	if !strings.Contains(unknown.Error, "missing") || !strings.Contains(unknown.Error, "a, b") || a != 2 || b != 2 {
		t.Fatalf("unknown ID must fail before running: %+v", unknown)
	}
	if len(loop.PlanApplications()) != 0 || loop.LoopsUsed() != 0 || len(loop.ProgramRuns()) != 0 {
		t.Fatal("detect performed diagnosis or mutation")
	}
}
