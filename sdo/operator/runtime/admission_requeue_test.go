package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	contractwire "sdo.dev/operator/contracts/wire"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Synthetic reproduction of the recorded sequence: repeated admission
// FailedCreate, webhook repaired, ReplicaSet still short of its desired Pods.
func TestAdmissionRepairRequeuesBlockedReplicaSets(t *testing.T) {
	ctx := context.Background()
	replicas := int32(1)
	blocked := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "compose-post", Namespace: "app", UID: "rs", ResourceVersion: "1"}, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `Internal error occurred: failed calling webhook "policy.example": failed to call webhook: Post "https://missing.app.svc:443/validate?timeout=5s": service "missing" not found`}}}}
	foreign := blocked.DeepCopy()
	foreign.Namespace = "peer"
	unrelated := blocked.DeepCopy()
	unrelated.Name = "quota"
	unrelated.Status.Conditions[0].Message = "exceeded quota"
	policy := admissionv1.Fail
	config := &admissionv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "policy", UID: "policy", ResourceVersion: "1"}, Webhooks: []admissionv1.ValidatingWebhook{{Name: "policy.example", FailurePolicy: &policy}}}
	client := fake.NewSimpleClientset(config, blocked, foreign, unrelated, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "serving", Namespace: "app"}, Data: map[string][]byte{"ca.crt": []byte("synthetic")}})
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(ctx, IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: program.WebhookCA{Ref: ref, Webhook: "policy.example", SecretNamespace: "app", SecretName: "serving", SecretKey: "ca.crt"}, Reason: "remove admission blocker"}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.AppsV1().ReplicaSets("app").Get(ctx, blocked.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Annotations["sdo.dev/admission-retry"] == "" {
		t.Fatal("repair left admission-blocked ReplicaSet in controller backoff")
	}
	if *got.Spec.Replicas != *blocked.Spec.Replicas || !reflect.DeepEqual(got.Spec.Template, blocked.Spec.Template) {
		t.Fatal("retry changed replica count or Pod template")
	}
	if len(result.ActionsApplied) != 2 || len(result.ActionReports) != 2 {
		t.Fatalf("requeue missing from action receipts: %+v", result)
	}
	for _, item := range []*appsv1.ReplicaSet{foreign, unrelated} {
		got, err := client.AppsV1().ReplicaSets(item.Namespace).Get(ctx, item.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got.Annotations["sdo.dev/admission-retry"] != "" {
			t.Fatalf("unrelated ReplicaSet %s/%s was requeued", item.Namespace, item.Name)
		}
	}
}

func TestRetryAdmissionNamespaceAndLiveEvidence(t *testing.T) {
	for _, namespace := range []string{"app", "peer", ""} {
		for _, failure := range []string{"admission", "quota", "cleared", "healthy"} {
			t.Run(namespace+"/"+failure, func(t *testing.T) {
				count := int32(1)
				set := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "app", UID: "rs", ResourceVersion: "1"}, Spec: appsv1.ReplicaSetSpec{Replicas: &count}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "policy.example": service not found`}}}}
				switch failure {
				case "quota":
					set.Status.Conditions[0].Message = "exceeded quota"
				case "cleared":
					set.Status.Conditions = nil
				case "healthy":
					set.Status.Replicas = 1
				}
				client := fake.NewSimpleClientset(set)
				executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{false}}
				result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: namespace, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RetryAdmission{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "blocked"}}}}, false)
				want := namespace == "app" && (failure == "admission" || failure == "healthy")
				if (err == nil) != want {
					t.Fatalf("err=%v want success=%v", err, want)
				}
				got, _ := client.AppsV1().ReplicaSets("app").Get(context.Background(), "blocked", metav1.GetOptions{})
				if (got.Annotations[admissionRetryAnnotation] != "") != (want && failure == "admission") {
					t.Fatalf("wrong requeue: %+v", got.Annotations)
				}
				if result.VerificationPassed {
					t.Fatal("requeue alone granted independent health")
				}
			})
		}
	}
}

func TestRetryAdmissionPersistenceAndMenu(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "blocked"}
	for _, action := range []program.Action{program.RetryAdmission{Ref: ref}, &program.RetryAdmission{Ref: ref}} {
		original := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, Applied: true}
		raw, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var restored PlanApplicationResult
		if err = json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		wire, err := restored.Proto()
		if err != nil {
			t.Fatal(err)
		}
		if err = contractwire.Validate(wire); err != nil {
			t.Fatal(err)
		}
		roundtrip := planApplicationState(wire).Result
		got, ok := roundtrip.ActionsApplied[0].(program.RetryAdmission)
		if !ok || got.Ref != ref {
			t.Fatalf("lost retry identity: %+v", roundtrip)
		}
	}
	fields := rawMenuActions()
	entry, ok := fields["RetryAdmission"]
	if !ok {
		t.Fatal("retry absent from responder menu")
	}
	plan, err := menuRawPlan(entry.action, map[string]string{"ref": "ReplicaSet/app/blocked"}, &program.ValueInputs{})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := plan.Actions[0].(program.RetryAdmission); !ok || got.Ref != ref {
		t.Fatalf("menu lost retry: %+v", plan)
	}
}
