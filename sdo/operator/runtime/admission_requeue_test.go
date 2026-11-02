package runtime

import (
	"context"
	"encoding/json"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"reflect"
	contractwire "sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/core"
	"strings"
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
		for _, failure := range []string{"admission", "quota", "cleared", "healthy", "missing-uid", "missing-version", "terminating"} {
			t.Run(namespace+"/"+failure, func(t *testing.T) {
				count := int32(1)
				set := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "app", UID: "rs", ResourceVersion: "1"}, Spec: appsv1.ReplicaSetSpec{Replicas: &count}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "policy.example": service not found`}}}}
				switch failure {
				case "missing-uid":
					set.UID = ""
				case "missing-version":
					set.ResourceVersion = ""
				case "terminating":
					stamp := metav1.Now()
					set.DeletionTimestamp = &stamp
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
				if want && failure == "admission" {
					receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "retry", Result: result}}, nil)
					if len(receipts) != 1 || receipts[0].Reversible {
						t.Fatal("controller requeue claimed an undo")
					}
					if err := executor.Rollback(context.Background(), result); err == nil {
						t.Fatal("controller requeue was undone")
					}
					after, _ := client.AppsV1().ReplicaSets("app").Get(context.Background(), "blocked", metav1.GetOptions{})
					if after.Annotations[admissionRetryAnnotation] != got.Annotations[admissionRetryAnnotation] {
						t.Fatal("rollback changed the requeue signal")
					}
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

func TestAdmissionRepairPathsAndFailedRetryKeepRepair(t *testing.T) {
	for _, kind := range []string{"ValidatingWebhookConfiguration", "MutatingWebhookConfiguration"} {
		for _, operation := range []string{"delete", "fail-open", "source", "retry-denied", "readback-lost", "already-deleted", "explicit-before", "explicit-after", "other-webhook", "late-failure", "list-unavailable", "explicit-before-delete", "explicit-after-delete"} {
			t.Run(kind+"/"+operation, func(t *testing.T) {
				ctx := context.Background()
				rsRef := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "blocked"}
				ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: kind, Name: "policy"}
				count := int32(1)
				set := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: rsRef.Name, Namespace: "app", UID: "rs", ResourceVersion: "1"}, Spec: appsv1.ReplicaSetSpec{Replicas: &count}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "policy.example": service not found`}}}}
				object, err := planUnstructured(set, rsRef)
				if err != nil {
					t.Fatal(err)
				}
				name := "policy.example"
				if operation == "other-webhook" {
					name = "other.example"
				}
				config := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "uid": "policy", "resourceVersion": "1"}, "webhooks": []any{map[string]any{"name": name, "failurePolicy": "Fail", "clientConfig": map[string]any{"service": map[string]any{"name": "missing", "namespace": "app"}}}}}}
				source := config.DeepCopy()
				source.Object["webhooks"].([]any)[0].(map[string]any)["failurePolicy"] = "Ignore"
				scheme := kruntime.NewScheme()
				_ = appsv1.AddToScheme(scheme)
				dynamic := dynamicfake.NewSimpleDynamicClient(scheme, config, object)
				discovery := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: ref.APIVersion, APIResources: []metav1.APIResource{{Kind: kind, Name: strings.ToLower(kind) + "s", Verbs: []string{"get", "patch", "delete", "list"}}}}, {GroupVersion: "apps/v1", APIResources: []metav1.APIResource{{Kind: "ReplicaSet", Name: "replicasets", Namespaced: true, Verbs: []string{"get", "patch", "list"}}}}}}}
				client := GenericPlanClient{TypedPlanClient: TypedPlanClient{Client: fake.NewSimpleClientset(set)}, Objects: &core.GenericCapture{Namespace: "app", Client: dynamic, Discovery: discovery}}
				if operation == "retry-denied" || operation == "readback-lost" {
					dynamic.PrependReactor("patch", "replicasets", func(action clienttesting.Action) (bool, kruntime.Object, error) {
						if operation == "retry-denied" {
							return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "replicasets"}, rsRef.Name, errors.New("synthetic denial"))
						}
						return true, object.DeepCopy(), nil
					})
				}
				if operation == "late-failure" {
					// Recorded sequence shape: event count 13 before repair,
					// count 15 after another admission failure. A fresh RS
					// version must be used for the post-repair requeue.
					dynamic.PrependReactor("delete", strings.ToLower(kind)+"s", func(action clienttesting.Action) (bool, kruntime.Object, error) {
						current := object.DeepCopy()
						current.SetResourceVersion("2")
						conditions, _, _ := unstructured.NestedSlice(current.Object, "status", "conditions")
						conditions[0].(map[string]any)["message"] = `Error creating: Internal error occurred: failed calling webhook "policy.example": service "missing" not found on retry`
						_ = unstructured.SetNestedSlice(current.Object, conditions, "status", "conditions")
						if err := dynamic.Tracker().Update(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}, current, "app"); err != nil {
							return true, nil, err
						}
						return false, nil, nil
					})
				}
				if operation == "already-deleted" {
					if err = client.Delete(ctx, ref, "policy", "1"); err != nil {
						t.Fatal(err)
					}
				}
				var repair program.Action = program.DeleteObject{Ref: ref, Reason: "remove orphaned admission blocker"}
				if operation == "fail-open" || operation == "explicit-before" || operation == "explicit-after" {
					repair = program.PatchObject{Ref: ref, Type: "strategic", Patch: []byte(`{"webhooks":[{"name":"policy.example","failurePolicy":"Ignore"}]}`), Reason: "restore admission"}
				}
				if operation == "source" {
					repair = program.ReconcileFromSource{Ref: ref}
				}
				actions := []program.Action{repair}
				retry := program.RetryAdmission{Ref: rsRef}
				if strings.HasPrefix(operation, "explicit-before") {
					actions = []program.Action{retry, repair}
				}
				if strings.HasPrefix(operation, "explicit-after") {
					actions = append(actions, retry)
				}
				executor := PlanExecutor{Client: client, Source: admissionTestSource{planFakeSource{source: source}}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{false}}
				if operation == "list-unavailable" {
					executor.Client = admissionListFailure{client}
				}
				result, err := executor.Apply(ctx, IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: actions}, false)
				failed := operation == "retry-denied" || operation == "readback-lost" || operation == "list-unavailable"
				if (err != nil) != (failed || operation == "already-deleted") {
					t.Fatalf("unexpected result err=%v actions=%+v", err, result.ActionsApplied)
				}
				got, getErr := client.Get(ctx, rsRef)
				if getErr != nil {
					t.Fatal(getErr)
				}
				wantRetry := !failed && operation != "already-deleted" && operation != "other-webhook"
				if (got.GetAnnotations()[admissionRetryAnnotation] != "") != wantRetry {
					t.Fatalf("retry annotation=%v want=%v", got.GetAnnotations(), wantRetry)
				}
				if failed {
					if !result.Applied || result.RolledBack || result.VerificationCompleted {
						t.Fatalf("retry failure lost repair/claimed success: %+v", result)
					}
					if _, err = client.Get(ctx, ref); !apierrors.IsNotFound(err) {
						t.Fatalf("retry failure put blocker back: %v", err)
					}
				}
				if err == nil {
					if _, encodeErr := result.Proto(); encodeErr != nil {
						t.Fatalf("executed plan cannot be persisted: %v", encodeErr)
					}
				}
				if strings.HasPrefix(operation, "explicit-before") && len(result.ActionsApplied) != 3 {
					t.Fatal("early explicit retry suppressed post-repair recovery")
				}
				if strings.HasPrefix(operation, "explicit-after") && len(result.ActionsApplied) != 2 {
					t.Fatal("duplicated explicit post-repair recovery")
				}
			})
		}
	}
}

// The accepted-source edge binds cluster-scoped source to the application.
type admissionTestSource struct{ planFakeSource }

func (s admissionTestSource) AcceptedApplicationManifest(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if namespace != "app" {
		return nil, errors.New("source belongs to another application")
	}
	return s.AcceptedManifest(ctx, revision, ref)
}

// An auxiliary discovery outage cannot put the original availability repair
// behind ReplicaSet list permissions or transient observation errors.
type admissionListFailure struct{ GenericPlanClient }

func (c admissionListFailure) AdmissionReplicaSets(context.Context, string) ([]appsv1.ReplicaSet, error) {
	return nil, errors.New("synthetic ReplicaSet observation unavailable")
}

type admissionOwnerRollouts struct {
	owner sdk.ObjectRef
	seen  bool
}

func (r *admissionOwnerRollouts) Wait(_ context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		r.seen = r.seen || ref == r.owner
	}
	if !r.seen {
		return errors.New("owning Deployment was omitted from rollout verification")
	}
	return context.DeadlineExceeded
}

func TestRetryAdmissionVerifiesLiveOwningDeployment(t *testing.T) {
	for _, ownerUID := range []types.UID{"deployment", "replacement"} {
		t.Run(string(ownerUID), func(t *testing.T) {
			replicas := int32(1)
			controller := true
			set := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "app", UID: "rs", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", UID: ownerUID, Controller: &controller}}}, Spec: appsv1.ReplicaSetSpec{Replicas: &replicas}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "policy.example": service not found`}}}}
			owner := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", UID: "deployment"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
			rollouts := &admissionOwnerRollouts{owner: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}
			executor := PlanExecutor{Client: TypedPlanClient{Client: fake.NewSimpleClientset(set, owner)}, Rollouts: rollouts, Verifier: planFakeVerifier{true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.RetryAdmission{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "blocked"}}}}, false)
			if err == nil || !result.Applied || result.VerificationPassed || result.VerificationCompleted {
				t.Fatalf("unsettled Deployment granted recovery: %+v %v", result, err)
			}
			if ownerUID == "deployment" && (!rollouts.seen || result.RolloutPending == nil) {
				t.Fatalf("owner omitted from pending verification: %+v %v", result, err)
			}
			if ownerUID == "replacement" && rollouts.seen {
				t.Fatal("waited on a replacement owner")
			}
		})
	}
}
