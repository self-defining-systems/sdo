package replay

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type admissionProgram struct{ ref sdk.ObjectRef }

func (p admissionProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "admission-recovery"}
}
func (p admissionProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, err := v.Object(p.ref)
	if err != nil {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p admissionProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.DeleteObject{Ref: p.ref, Reason: "remove synthetic missing backend"}}}, nil
}

func TestAdmissionReplayRecordsAutomaticRecoveryActions(t *testing.T) {
	replicas := int32(1)
	p := admissionProgram{ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}}
	open := sdktest.Snapshot{NamespaceName: "app", ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "app"}, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "policy.example": service not found`}}}}}, Objects: []unstructured.Unstructured{{Object: map[string]any{"apiVersion": p.ref.APIVersion, "kind": p.ref.Kind, "metadata": map[string]any{"name": p.ref.Name}, "webhooks": []any{map[string]any{"name": "policy.example"}}}}}}
	result := Check(context.Background(), p, p, nil, Case{ID: "admission-recovery", RepairOnly: true, Corpus: sdktest.CorpusCase{Open: open, Fixed: sdktest.Snapshot{NamespaceName: "app"}, Healthy: sdktest.Snapshot{NamespaceName: "app"}, RootCauseProgramIDs: []string{p.Spec().ID}}})
	if len(result.Actions) != 2 {
		t.Fatalf("executed recovery missing from replay: %+v", result)
	}
	found := false
	for _, effect := range result.Effects {
		found = found || strings.Contains(effect, "program.RetryAdmission")
	}
	if !found {
		t.Fatalf("automatic recovery action mislabeled: %+v", result)
	}
}
