package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Reduced SP1 sidecar case: accepted application port 8083, live fault 9999,
// learned repair 9100 (the exporter, which is not the application).
func TestPlanExecutorSourceAuthority(t *testing.T) {
	for _, tc := range []struct {
		name      string
		target    int
		evidence  bool
		wantError bool
	}{
		{"stale learned repair", 9100, true, true},
		{"accepted source restoration", 8083, true, false},
		{"missing source evidence", 8083, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"}
			live := planObject(ref, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}})
			source := live.DeepCopy()
			if err := program.ApplySetField(source.Object, "spec.ports[0].targetPort", float64(8083)); err != nil {
				t.Fatal(err)
			}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
			executor := PlanExecutor{AcceptedRevision: func() string { return "accepted" }, Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			if tc.evidence {
				executor.Source = planFakeSource{source: source}
			}
			incident := IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", DriftEvents: []sdk.DriftEvent{{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999", Source: "8083"}}}}}
			plan := program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(tc.target)}}}
			result, err := executor.Apply(context.Background(), incident, plan, false)
			if tc.wantError {
				if err == nil || len(client.patches) != 0 || result.Applied {
					t.Fatalf("unsafe learned repair: result=%+v err=%v writes=%d", result, err, len(client.patches))
				}
				if !strings.Contains(err.Error(), "investigate") {
					t.Fatalf("missing investigation feedback: %v", err)
				}
			} else if err != nil || len(client.patches) != 1 {
				t.Fatalf("source restoration: err=%v writes=%d", err, len(client.patches))
			}
		})
	}
}

func sourceAuthorityFixture(t *testing.T) (PlanExecutor, IncidentRequest, sdk.ObjectRef, *planFakeClient) {
	t.Helper()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"}
	live := planObject(ref, map[string]any{"ports": []any{map[string]any{"port": float64(80), "targetPort": float64(9999)}}, "selector": map[string]any{"app": "reservation"}})
	source := live.DeepCopy()
	if err := program.ApplySetField(source.Object, "spec.ports[0].targetPort", float64(8083)); err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}
	executor := PlanExecutor{AcceptedRevision: func() string { return "accepted" }, Client: client, Source: planFakeSource{source: source, revision: live}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	incident := IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions", DriftEvents: []sdk.DriftEvent{{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", New: "9999", Source: "9100"}}}}}
	return executor, incident, ref, client
}

func TestPlanExecutorSourceAuthorityValidatesWholePlan(t *testing.T) {
	for _, shape := range []string{"later action", "restored then stale", "aggregate", "rollback", "finding metadata", "dry run"} {
		t.Run(shape, func(t *testing.T) {
			e, i, ref, c := sourceAuthorityFixture(t)
			stale := program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(9100)}
			actions := []program.Action{program.SetField{Ref: ref, Path: "spec.selector.app", Value: testPlanValue("other")}, stale}
			switch shape {
			case "restored then stale":
				actions = []program.Action{program.SetField{Ref: ref, Path: stale.Path, Value: testPlanValue(8083)}, stale}
			case "aggregate":
				actions = []program.Action{&program.SetField{Ref: ref, Path: "spec.ports", Value: testPlanValue([]any{map[string]any{"port": 80, "targetPort": 9100}})}}
			case "rollback":
				actions = []program.Action{program.Rollback{Ref: ref, ToRevision: "old"}, stale}
			case "finding metadata":
				i.Findings = []sdk.Finding{{Status: sdk.FindingActive, Metadata: map[string]any{"drift_event": i.DriftEvents[0]}}}
				i.DriftEvents = nil
			}
			result, err := e.Apply(context.Background(), i, program.Plan{Actions: actions}, shape == "dry run")
			if err == nil || result.Applied || len(c.patches) != 0 {
				t.Fatalf("%s bypassed authority: result=%+v err=%v writes=%d", shape, result, err, len(c.patches))
			}
		})
	}
}

func TestPlanExecutorSourceAuthorityEvidenceAndRevision(t *testing.T) {
	for _, mode := range []string{"no commit", "no revision reader", "changed before prepare", "changed after prepare", "wrong object", "nil object", "source field missing", "source read error"} {
		t.Run(mode, func(t *testing.T) {
			e, i, ref, c := sourceAuthorityFixture(t)
			switch mode {
			case "no commit":
				i.SourceCommit = ""
			case "no revision reader":
				e.AcceptedRevision = nil
			case "changed before prepare":
				e.AcceptedRevision = func() string { return "new" }
			case "changed after prepare":
				calls := 0
				e.AcceptedRevision = func() string {
					calls++
					if calls > 1 {
						return "new"
					}
					return "accepted"
				}
			case "wrong object":
				obj := c.objects[ref].DeepCopy()
				obj.SetName("other")
				e.Source = planFakeSource{source: obj}
			case "nil object":
				e.Source = planFakeSource{}
			case "source field missing":
				e.Source = planFakeSource{source: planObject(ref, map[string]any{"ports": []any{map[string]any{"port": float64(80)}}})}
			case "source read error":
				e.Source = authorityErrorSource{}
			}
			result, err := e.Apply(context.Background(), i, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8083)}}}, false)
			if err == nil || result.Applied || len(c.patches) != 0 || !strings.Contains(err.Error(), "investigate") {
				t.Fatalf("%s: result=%+v err=%v writes=%d", mode, result, err, len(c.patches))
			}
		})
	}
}

type authorityErrorSource struct{ planFakeSource }

func (authorityErrorSource) AcceptedManifest(context.Context, string, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, errors.New("render evidence gap")
}

func TestPlanExecutorSourceAuthorityAllowsUnrelatedRepair(t *testing.T) {
	e, i, ref, c := sourceAuthorityFixture(t)
	result, err := e.Apply(context.Background(), i, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.selector.app", Value: testPlanValue("other")}}}, false)
	if err != nil || !result.Applied || len(c.patches) != 1 {
		t.Fatalf("unrelated repair: %+v %v", result, err)
	}
}

// Both paths must consult the executor even after the plan was prepared under
// earlier evidence. The trusted helper proposes 8080, while source now says 8083.
func TestPlanExecutorSourceAuthorityGuardsResponderAndFastPath(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprintf("fast=%v", fast), func(t *testing.T) {
			loop, client := trustedPortLoop(t, false, true, true)
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			loop.Executor.Source = planFakeSource{source: simService(8083)}
			loop.Executor.AcceptedRevision = func() string { return "tree-a" }
			loop.Request.DriftEvents = []sdk.DriftEvent{{Ref: ref, Fields: []sdk.DriftField{{Path: "spec.ports[0].targetPort", Source: "8083", New: "9999"}}}}
			if fast {
				attempt := loop.TryTrustedFastPath(context.Background(), 3)
				if attempt.Resolved || !strings.Contains(attempt.Summary, "investigate") {
					t.Fatalf("stale plan did not reach the guard: %+v", attempt)
				}
			} else {
				// Preview and apply each validate; a previously stored plan cannot bypass it.
				loop.plans["stale"] = selectedPlan{programID: "service-targetport-mismatch", plan: program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}}
				response := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"stale"}})
				if !strings.Contains(response.Error, "investigate") {
					t.Fatalf("no investigation feedback: %+v", response)
				}
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "patch" || a.GetVerb() == "update" {
					t.Fatalf("unsafe write: %+v", a)
				}
			}
		})
	}
}

func TestPlanExecutorSourceAuthoritySidecarEvidence(t *testing.T) {
	for _, name := range []string{"sidecar_reduced", "sp1_stage1_replay"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata", "source_authority", name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var fixture struct {
				Revision                 string
				Source, Live, Deployment *unstructured.Unstructured
				Action                   program.SetField
				Drift                    sdk.DriftEvent
			}
			if err = json.Unmarshal(data, &fixture); err != nil {
				t.Fatal(err)
			}
			ref := fixture.Action.Ref
			depRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: ref.Namespace, Name: ref.Name}
			containers, _, err := unstructured.NestedSlice(fixture.Deployment.Object, "spec", "template", "spec", "containers")
			if err != nil || len(containers) != 2 {
				t.Fatalf("sidecar fixture: %v", err)
			}
			if _, declared := containers[0].(map[string]any)["ports"]; declared {
				t.Fatal("fixture must preserve the undeclared application port")
			}
			if containers[1].(map[string]any)["name"] != "metrics-exporter" {
				t.Fatal("fixture must preserve the metrics sidecar")
			}
			for _, target := range []int{9100, 8083} {
				t.Run(fmt.Sprint(target), func(t *testing.T) {
					c := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: fixture.Live.DeepCopy(), depRef: fixture.Deployment.DeepCopy()}}
					e := PlanExecutor{Client: c, Source: planFakeSource{source: fixture.Source}, AcceptedRevision: func() string { return fixture.Revision }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
					i := IncidentRequest{SourceCommit: fixture.Revision, RepairPolicy: "recorded-actions", DriftEvents: []sdk.DriftEvent{fixture.Drift}}
					a := fixture.Action
					if a.Value.Origin() != program.ParameterValue {
						t.Fatalf("archived learned action changed: %+v", a)
					}
					a.Value = testPlanValue(target)
					result, err := e.Apply(context.Background(), i, program.Plan{Actions: []program.Action{a}}, false)
					if target == 9100 {
						if err == nil || len(c.patches) != 0 || result.Applied {
							t.Fatalf("archived unsafe write admitted: %+v %v", result, err)
						}
						t.Logf("archived 9999->9100 refused before any write: %v", err)
					} else if err != nil || !result.Applied || len(c.patches) != 1 {
						t.Fatalf("8083 restoration refused: %+v %v", result, err)
					}
					if !planValuesEqual(c.objects[depRef].Object, fixture.Deployment.Object) {
						t.Fatal("source intent changed: metrics sidecar or undeclared application port lost")
					}
				})
			}
		})
	}
}

func TestPlanExecutorSourceAuthorityAllowsSourceReconcileWithAPIDefaults(t *testing.T) {
	e, i, ref, c := sourceAuthorityFixture(t)
	if err := program.ApplySetField(c.objects[ref].Object, "spec.ports[0].protocol", "TCP"); err != nil {
		t.Fatal(err)
	}
	result, err := e.Apply(context.Background(), i, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied || len(c.patches) != 1 {
		t.Fatalf("API defaults must not turn source restoration into a refusal: %+v %v", result, err)
	}
}
