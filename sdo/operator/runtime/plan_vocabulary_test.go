package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestPlanExecutorStructuredSourceAndDocument(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	before.Object["data"] = map[string]any{"flags.json": `{"gc":true,"unrelated":7}`, "other": "preserved"}
	accepted := before.DeepCopy()
	accepted.Object["data"] = map[string]any{"flags.json": `{"gc":false,"unrelated":0}`, "other": "source"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["flags.json"] = `{"gc":false,"unrelated":7}`
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: accepted}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	action := program.SetField{Ref: ref, Path: "data[flags.json]", DocumentFormat: "json", DocumentPath: []string{"gc"}, Value: program.FromSource(ref, "data[flags.json]")}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{action}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("apply: %v", err)
	}
	if got := client.objects[ref].Object["data"].(map[string]any); got["flags.json"] != `{"gc":false,"unrelated":7}` || got["other"] != "preserved" {
		t.Fatal("unrelated values changed")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var restored PlanApplicationResult
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	proto, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	decoded := planApplicationState(proto).Result.Plan.Actions[0].(program.SetField)
	if decoded.Value.Origin() != program.SourceValue || len(decoded.DocumentPath) != 1 {
		t.Fatal("source selector lost across contract")
	}
	if err := executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if !planValuesEqual(client.objects[ref].Object["data"], before.Object["data"]) {
		t.Fatal("rollback did not restore data")
	}
}

func TestPlanExecutorStructuredObservationRejectsMissingEffect(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.Object["data"] = map[string]any{"key": "before"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	executor := PlanExecutor{Client: ignoreStructuredPatch{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[key]", Value: testPlanValue("after")}}}, false)
	if err == nil || result.VerificationPassed {
		t.Fatal("unobserved write reported success")
	}
}

type ignoreStructuredPatch struct{ *planFakeClient }

func (ignoreStructuredPatch) Patch(context.Context, sdk.ObjectRef, types.PatchType, []byte) error {
	return nil
}

func TestPlanCacheStructuredUnknown(t *testing.T) {
	cache := &KubernetesCache{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := cache.AwaitWrites(ctx, []ObservedWrite{{Ref: sdk.ObjectRef{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "app", Name: "app"}, ResourceVersion: "2", Strict: true}}); err == nil {
		t.Fatal("unwatched kind reported success")
	}
}

func TestPlanExecutorStructuredFieldFamilies(t *testing.T) {
	workload := func() map[string]any {
		return map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "app", "command": []any{"/before"}, "args": []any{"before"}, "env": []any{map[string]any{"name": "MODE", "value": "before"}}, "volumeMounts": []any{map[string]any{"name": "tmp", "mountPath": "/before"}}}}, "nodeSelector": map[string]any{"pool": "before"}, "tolerations": []any{}, "affinity": map[string]any{}}}}
	}
	cases := []struct {
		kind, api, path string
		spec            map[string]any
		value           any
	}{
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].command", workload(), []any{"/after"}},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].args", workload(), []any{"after"}},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].env[name=MODE].value", workload(), "after"},
		{"Deployment", "apps/v1", "spec.template.spec.containers[name=app].volumeMounts", workload(), []any{map[string]any{"name": "tmp", "mountPath": "/after"}}},
		{"Deployment", "apps/v1", "spec.template.spec.nodeSelector", workload(), map[string]any{"pool": "after"}},
		{"Deployment", "apps/v1", "spec.template.spec.tolerations", workload(), []any{map[string]any{"key": "dedicated", "operator": "Exists"}}},
		{"Deployment", "apps/v1", "spec.template.spec.affinity", workload(), map[string]any{"podAffinity": map[string]any{}}},
		{"Service", "v1", "spec.selector", map[string]any{"selector": map[string]any{"app": "before"}}, map[string]any{"app": "after"}},
		{"Service", "v1", "spec.ports[name=http].targetPort", map[string]any{"selector": map[string]any{"app": "app"}, "ports": []any{map[string]any{"name": "http", "port": float64(80), "targetPort": float64(9999)}}}, float64(8080)},
		{"NetworkPolicy", "networking.k8s.io/v1", "spec", map[string]any{"podSelector": map[string]any{}}, map[string]any{"podSelector": map[string]any{}, "policyTypes": []any{"Ingress"}}},
		{"HorizontalPodAutoscaler", "autoscaling/v2", "spec.maxReplicas", map[string]any{"maxReplicas": float64(1)}, float64(5)},
		{"PodDisruptionBudget", "policy/v1", "spec.minAvailable", map[string]any{"minAvailable": float64(0)}, float64(1)},
	}
	for _, c := range cases {
		t.Run(c.kind+"/"+c.path, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: c.api, Kind: c.kind, Namespace: "app", Name: "app"}
			before := planObject(ref, c.spec)
			before.SetUID("fixture-uid")
			after := before.DeepCopy()
			if err := program.ApplySetField(after.Object, c.path, c.value); err != nil {
				t.Fatal(err)
			}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
			e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			// Selector is existing vocabulary: FromSource also exercises source resolution.
			a := program.SetField{Ref: ref, Path: c.path, Value: testPlanValue(c.value)}
			if !program.ExpandedSetField(a) {
				a.Value = program.FromSource(a.Ref, a.Path)
				e.Source = planFakeSource{source: after}
			}
			result, err := e.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{a}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("apply: %v", err)
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPlanExecutorStructuredYAMLAndMissingKey(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
			before := planObject(ref, nil)
			delete(before.Object, "spec")
			before.SetUID("fixture-uid")
			text, want := `{"gc":true,"unrelated":7}`, `{"gc":false,"unrelated":7}`
			if format == "yaml" {
				text = "gc: true\nunrelated: 7\n"
				want = "gc: false\nunrelated: 7\n"
			}
			before.Object["data"] = map[string]any{"flags": text}
			after := before.DeepCopy()
			after.Object["data"].(map[string]any)["flags"] = want
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			a := program.SetField{Ref: ref, Path: "data[flags]", DocumentFormat: format, DocumentPath: []string{"gc"}, Value: testPlanValue(false)}
			result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatal(err)
			}
			if err := e.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		})
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing-key"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	after := before.DeepCopy()
	after.Object["data"] = map[string]any{"missing": "safe"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	e := PlanExecutor{Client: client, Source: planFakeSource{source: after}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	if result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[missing]", Value: program.FromSource(ref, "data[missing]")}}}, false); err != nil || !result.VerificationPassed {
		t.Fatal(err)
	}
}

type structuredFixtureProgram struct {
	action program.Action
	client *planFakeClient
	ref    sdk.ObjectRef
	want   any
	path   string
}

func (p structuredFixtureProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-structured", Params: []program.ParamSpec{{Name: "target", Kind: program.Kind(p.ref.Kind), Required: true}, {Name: "value", ValueKind: program.StringValue, Default: p.want}}}
}
func (p structuredFixtureProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	value, err := program.FieldValue(p.client.objects[p.ref].Object, p.path)
	if err == nil && planValuesEqual(value, p.want) {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p structuredFixtureProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}

func TestResponderToolsAcceptStructuredSetField(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.Object["data"] = map[string]any{"key": "before"}
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	p := structuredFixtureProgram{action: program.SetField{Ref: ref, Path: "data[key]", Value: program.FromParam("value")}, client: client, ref: ref, want: "after", path: "data[key]"}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "synthetic", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, registry, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, nil, e, 5)
	loop.ReadView = func(sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{} }
	planned := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: p.Spec().ID, Bindings: program.Bindings{Objects: program.ObjectBindings{"target": ref}}})
	if planned.Error != "" || planned.PlanID == "" {
		t.Fatalf("plan: %s", planned.Error)
	}
	applied := loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if applied.Error != "" || applied.ApplicationID == "" || len(client.patches) != 1 {
		t.Fatalf("apply: %s", applied.Error)
	}
}

func TestPlanExecutorStructuredVerifiesEachWriteAtItsBoundary(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	before := planObject(ref, nil)
	delete(before.Object, "spec")
	before.SetUID("fixture-uid")
	before.Object["data"] = map[string]any{"key": "before"}
	intermediate := before.DeepCopy()
	intermediate.Object["data"].(map[string]any)["key"] = "intermediate"
	after := before.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), IncidentRequest{SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[key]", Value: testPlanValue("intermediate")}, program.SetField{Ref: ref, Path: "data[key]", Value: testPlanValue("after")}}}, false)
	if err != nil || !result.VerificationPassed || len(client.patches) != 2 {
		t.Fatalf("ordered writes: %v", err)
	}
}
