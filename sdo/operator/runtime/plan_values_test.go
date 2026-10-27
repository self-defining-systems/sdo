package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestXV2ValueDefaultsDoNotBecomeObjectCandidates(t *testing.T) {
	spec := program.ProgramSpec{ID: "parameterized", Params: []program.ParamSpec{{Name: "count", ValueKind: program.IntValue, Required: true, Default: 2}, {Name: "service", Kind: "Service", Required: true}}}
	p := candidateProgram{spec: spec, run: func(_ context.Context, b program.Bindings) program.Verdict {
		count, err := b.Parameter(spec, "count")
		if err != nil || count != 2 {
			return program.Verdict{Result: program.Unknown}
		}
		return program.Verdict{Result: program.Match}
	}}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "one", Namespace: "app"}}}}}
	runs := SelectProgramCandidates(context.Background(), registry, view, []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "one"}}}, time.Second)
	if len(runs) != 1 || runs[0].Verdict.Result != program.Match || len(runs[0].Bindings.Objects) != 1 {
		t.Fatalf("scalar default interfered with finding binding: %+v", runs)
	}
}

func TestXV2BindingsAndValueProvenanceSurviveReceipts(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "workload"}
	bindings := program.Bindings{Objects: program.ObjectBindings{"workload": ref}, Values: map[string]any{"replicas": 3, "flag": false, "quantity": "128Mi", "duration": "2s", "mode": "safe"}}
	run := ProgramRun{ProgramID: "typed", Bindings: bindings, Verdict: program.Verdict{Result: program.Match}, Duration: time.Second}
	restored := programRunState(programRunContract(run))
	if !bindings.Equal(restored.Bindings) || bindingKey(bindings) != bindingKey(restored.Bindings) {
		t.Fatalf("value bindings lost across contract: %+v", restored.Bindings)
	}
	for _, origin := range []program.ValueOrigin{program.SourceValue, program.ObservedValue, program.ParameterValue} {
		t.Run(string(origin), func(t *testing.T) {
			before := planObject(ref, map[string]any{"replicas": int64(1)})
			accepted := planObject(ref, map[string]any{"replicas": int64(3)})
			observed := accepted.DeepCopy()
			observed.SetResourceVersion("snapshot-17")
			c := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			e := PlanExecutor{Client: c, Source: planFakeSource{source: accepted}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue}}}, Bindings: program.Bindings{Values: map[string]any{"replicas": 3}}, Observed: func(sdk.ObjectRef) (map[string]any, error) { return observed.Object, nil }}}
			value := program.FromSource(ref, "spec.replicas")
			if origin == program.ObservedValue {
				value = program.FromObserved(ref, "spec.replicas")
			}
			if origin == program.ParameterValue {
				value = program.FromParam("replicas")
			}
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: ref, Replicas: value}}}, false)
			if err != nil || !result.VerificationPassed {
				t.Fatalf("apply: %+v %v", result, err)
			}
			encoded, err := result.Proto()
			if err != nil {
				t.Fatal(err)
			}
			roundTrip := planApplicationState(encoded).Result
			if len(roundTrip.ActionReports) != 1 || len(roundTrip.ActionReports[0].Values) != 1 {
				t.Fatalf("provenance lost: %+v", roundTrip.ActionReports)
			}
			p := roundTrip.ActionReports[0].Values[0]
			if p.Origin != origin || origin == program.SourceValue && (p.SourceCommit != "accepted" || p.Path != "spec.replicas") || origin == program.ObservedValue && (p.ResourceVersion != "snapshot-17" || p.Path != "spec.replicas") || origin == program.ParameterValue && p.Parameter != "replicas" {
				t.Fatalf("wrong authority: %+v", p)
			}
			raw, _ := protojson.Marshal(encoded)
			if strings.Contains(string(raw), `"numberValue"`) {
				t.Fatal("receipt contains resolved scalar data")
			}
			report, _ := json.Marshal(result.ActionReports)
			if strings.Contains(string(report), `"replicas":3`) {
				t.Fatal("action report contains a resolved value")
			}
		})
	}
}

type observedScaleProgram struct{ source, target sdk.ObjectRef }

func (p observedScaleProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "observed-scale"}
}
func (p observedScaleProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: p.target, Replicas: program.FromObserved(p.source, "spec.replicas")}}}, nil
}

func TestXV2ExtendedObservedValuesStayOnMitigationSnapshot(t *testing.T) {
	source := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "database"}
	target := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "worker"}
	replicas := int32(2)
	snapshot := sdktest.Snapshot{NamespaceName: "app", StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: source.Name, Namespace: source.Namespace, ResourceVersion: "17"}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}}}
	p := observedScaleProgram{source: source, target: target}
	plan, err := BuildMitigationPlan(context.Background(), p, &sdktest.ReplayReadView{Snapshot: snapshot}, program.Bindings{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	*snapshot.StatefulSetList[0].Spec.Replicas = 3
	snapshot.StatefulSetList[0].ResourceVersion = "18"
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{target: planObject(target, map[string]any{"replicas": int64(1)})}}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	incident := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}
	first, err := e.Apply(context.Background(), incident, plan, false)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := program.FieldValue(client.objects[target].Object, "spec.replicas")
	if !planValuesEqual(got, 2) || first.ActionReports[0].Values[0].ResourceVersion != "17" {
		t.Fatalf("invocation lost pinned observation: value=%v report=%+v", got, first.ActionReports)
	}
	refreshed, err := BuildMitigationPlan(context.Background(), p, &sdktest.ReplayReadView{Snapshot: snapshot}, program.Bindings{}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Apply(context.Background(), incident, refreshed, false)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = program.FieldValue(client.objects[target].Object, "spec.replicas")
	if !planValuesEqual(got, 3) || second.ActionReports[0].Values[0].ResourceVersion != "18" {
		t.Fatalf("new invocation did not follow new observation: value=%v report=%+v", got, second.ActionReports)
	}
}

func TestXV2NoopValuesCannotVerifyForeignWrites(t *testing.T) {
	for _, scale := range []bool{false, true} {
		t.Run(fmt.Sprint(scale), func(t *testing.T) {
			client, ref := newServerPlanClient(deploymentWithDrift("safe:v1"))
			client.afterPatch = func(object *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(object.Object, int64(9), "spec", "replicas")
			}
			value := program.FromParam("replicas")
			var action program.Action = program.SetField{Ref: ref, Path: "spec.replicas", Value: value}
			if scale {
				action = program.ScaleWorkload{Ref: ref, Replicas: value}
			}
			e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}, Values: program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue, Default: 1}}}}}
			r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
			if err == nil || r.VerificationPassed {
				t.Fatalf("no-op value credited a foreign write: %+v %v", r, err)
			}
		})
	}
}

func TestXV2MapValueVerificationChecksUnchangedRequestedLeaves(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "front"}
	before := planObject(ref, map[string]any{"selector": map[string]any{"app": "safe"}})
	for _, changed := range []bool{false, true} {
		live := before.DeepCopy()
		selector := map[string]any{"app": "safe", "server-default": "allowed"}
		if changed {
			selector["app"] = "foreign"
		}
		_ = unstructured.SetNestedMap(live.Object, selector, "spec", "selector")
		e := PlanExecutor{Client: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: live}}}
		err := e.verifyValueWrite(context.Background(), program.SetField{Ref: ref, Path: "spec.selector", Value: program.FromSource(ref, "spec.selector")}, PlanPatch{Ref: ref, Before: before, After: before.DeepCopy()})
		if changed && err == nil || !changed && err != nil {
			t.Fatalf("changed=%v verification=%v", changed, err)
		}
	}
}
