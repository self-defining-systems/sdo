package sdktest

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
)

type scopedRepairDiagnoser struct{}

func (scopedRepairDiagnoser) Spec() program.ProgramSpec { return targetport.Program{}.Spec() }
func (scopedRepairDiagnoser) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	if _, err := sdk.ScopedSource(v).ScopedServices("foreign"); err != nil {
		return program.Verdict{}, err
	}
	return targetport.Program{}.Diagnose(ctx, v, b)
}

func TestScopedReadsSurviveProjectedLocalRepair(t *testing.T) {
	c := renameFixture(t)
	target := sdk.ObservationTarget{APIVersion: "v1", Kind: "Service", Namespace: "foreign"}
	c.Open.ScopedSnapshot = sdk.ScopedSnapshot{Version: 1, Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}, Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: "foreign"}}}}
	m := fixtureMitigator{actions: []program.Action{program.SetField{Ref: c.Bindings.Objects["service"], Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}
	if err := CheckMitigationRepairs(scopedRepairDiagnoser{}, m, c); err != nil {
		t.Fatal(err)
	}
	c.Open.ScopedSnapshot.Statuses[0].State = "forbidden"
	if err := CheckMitigationRepairs(scopedRepairDiagnoser{}, m, c); err == nil {
		t.Fatal("denied scoped repair diagnosis established clearance")
	}
}

type fixtureMitigator struct{ actions []program.Action }

func (fixtureMitigator) Spec() program.ProgramSpec { return targetport.Program{}.Spec() }
func (m fixtureMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: m.actions}, nil
}
func TestMitigationMustRepairFaultedFixture(t *testing.T) {
	c := renameFixture(t)
	ref := c.Bindings.Objects["service"]
	for _, test := range []struct {
		name    string
		actions []program.Action
		want    string
	}{
		{"repairs port", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}, ""},
		{"keeps faulty port", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: testPlanValue(9999)}}, "program.mitigation-not-repaired"},
		{"source effect skips replay", []program.Action{program.ReconcileFromSource{Ref: ref}}, ""},
		{"missing target", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[99].targetPort", Value: testPlanValue(8080)}}, "program.mitigation-unreplayable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := c.Open.Services()
			err := CheckMitigationRepairs(targetport.Program{}, fixtureMitigator{actions: test.actions}, c)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("got %v, want %s", err, test.want)
			}
			if !reflect.DeepEqual(before, c.Open.Services()) {
				t.Fatal("replay mutated protected faulted fixture")
			}
		})
	}
}

func TestMitigationDoesNotReusePrimitiveAnswersAfterPatching(t *testing.T) {
	c := renameFixture(t)
	args, _ := json.Marshal([]any{c.Bindings.Objects["service"]})
	c.Recordings = []PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":true}`)}, {Phase: "fixed", Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":false}`)}}
	m := fixtureMitigator{actions: []program.Action{program.SetField{Ref: c.Bindings.Objects["service"], Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}
	err := CheckMitigationRepairs(sourcePhaseProgram{}, m, c)
	if err == nil || !strings.Contains(err.Error(), "program.mitigation-unreplayable") {
		t.Fatalf("post-plan primitive effects were invented: %v", err)
	}
}

func TestScopedNamedRecordingsKeepExternalIdentity(t *testing.T) {
	for _, method := range []string{"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration"} {
		args, _ := json.Marshal([]any{"external-policy"})
		calls := []PrimitiveCall{{Method: method, Arguments: args}}
		if _, err := renameRecordings(calls, "app", newRenameMapping()); err != nil {
			t.Fatal(err)
		}
	}
}
