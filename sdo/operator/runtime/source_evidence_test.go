package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// scriptedSourceView answers DiffAgainstSource per resource the way the live
// view does: a diff, or the typed error for an object with no acceptance record.
type scriptedSourceView struct {
	program.ReadView
	unknown map[string]bool
	calls   *[]sdk.ObjectRef
}

func (v scriptedSourceView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	*v.calls = append(*v.calls, ref)
	if v.unknown[ref.Name] {
		return program.Diff{}, &AcceptanceUnknownError{Ref: ref, Declared: program.Diff{Changed: true, Text: "spec.replicas: 1 != 3"}}
	}
	return program.Diff{Changed: true, Text: "spec.template differs from accepted source"}, nil
}

func sourceEvidenceLoop(t *testing.T, unknown ...string) (*ResponderLoop, *[]sdk.ObjectRef) {
	t.Helper()
	snapshot := sdktest.Snapshot{NamespaceName: "app"}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "source-evidence", Namespace: "app", RepairPolicy: "recorded-actions"}, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{}, 3)
	loop.CorpusSessionRoot = t.TempDir()
	calls := &[]sdk.ObjectRef{}
	flagged := map[string]bool{}
	for _, name := range unknown {
		flagged[name] = true
	}
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView {
		return scriptedSourceView{ReadView: &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}, unknown: flagged, calls: calls}
	}
	return loop, calls
}

func capturedCalls(t *testing.T, loop *ResponderLoop, phase string) []sdktest.PrimitiveCall {
	t.Helper()
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, loop.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, phase+"-recordings.json"))
	if err != nil {
		t.Fatalf("no %s recordings: %v", phase, err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	return calls
}

func diffCallFor(calls []sdktest.PrimitiveCall, name string) *sdktest.PrimitiveCall {
	for i := range calls {
		if calls[i].Method == "DiffAgainstSource" && strings.Contains(string(calls[i].Arguments), `"`+name+`"`) {
			return &calls[i]
		}
	}
	return nil
}

// Invariant: every resource a write touches has an accepted-source comparison in
// the incident capture, whichever write boundary names it, so reflection never
// sees a repaired resource without that evidence.
func TestEveryWriteBoundaryRecordsTheAcceptedSourceComparison(t *testing.T) {
	ctx := context.Background()
	loop, _ := sourceEvidenceLoop(t)
	refs := func(names ...string) []sdk.ObjectRef {
		var out []sdk.ObjectRef
		for _, name := range names {
			out = append(out, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: name})
		}
		return out
	}
	// First boundary: two resources. A later boundary names a third, which the
	// first-mutation snapshot never saw.
	if response := loop.Handle(ctx, ToolRequest{Command: "pre-apply", Refs: refs("user-service", "text-service")}); response.Error != "" {
		t.Fatal(response.Error)
	}
	if response := loop.Handle(ctx, ToolRequest{Command: "pre-apply", Refs: refs("media-service", "user-service")}); response.Error != "" {
		t.Fatal(response.Error)
	}
	calls := capturedCalls(t, loop, "open@pre-apply")
	for _, name := range []string{"user-service", "text-service", "media-service"} {
		if diffCallFor(calls, name) == nil {
			t.Fatalf("no accepted-source comparison recorded for %s: %v", name, calls)
		}
	}
	count := 0
	for _, call := range calls {
		if call.Method == "DiffAgainstSource" && strings.Contains(string(call.Arguments), "user-service") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("a resource compared twice at write boundaries is recorded %d times", count)
	}
}

// A missing acceptance record is evidence with a typed reason: the declared-field
// comparison is kept and the reason is named, never an empty result.
func TestMissingAcceptanceRecordIsRecordedWithTypedReason(t *testing.T) {
	ctx := context.Background()
	loop, _ := sourceEvidenceLoop(t, "text-service")
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "text-service"}
	if response := loop.Handle(ctx, ToolRequest{Command: "pre-apply", Refs: []sdk.ObjectRef{ref}}); response.Error != "" {
		t.Fatal(response.Error)
	}
	call := diffCallFor(capturedCalls(t, loop, "open@pre-apply"), "text-service")
	if call == nil {
		t.Fatal("unknown acceptance left no recorded comparison")
	}
	if !strings.HasPrefix(call.Error, "acceptance_record_unavailable") || !strings.Contains(call.Error, "declared fields differ") {
		t.Fatalf("reason is not typed: %q", call.Error)
	}
	var declared program.Diff
	if err := json.Unmarshal(call.Result, &declared); err != nil || !declared.Changed || declared.Text == "" {
		t.Fatalf("declared-field comparison lost: %s %v", call.Result, err)
	}
}

// Findings name the resources the incident is about; their comparison is taken
// at detect, before any repair can change them.
func TestDetectCaptureRecordsSourceComparisonForFindingResources(t *testing.T) {
	loop, _ := sourceEvidenceLoop(t)
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "social-graph"}
	loop.Request.Findings = []sdk.Finding{{DetectorID: "d", Status: sdk.FindingActive, PrimaryResource: ref}}
	loop.findings = loop.Request.Findings
	loop.InitialDiagnose(context.Background())
	if diffCallFor(capturedCalls(t, loop, "open@detect"), "social-graph") == nil {
		t.Fatal("no accepted-source comparison recorded at detect for the finding's resource")
	}
}
