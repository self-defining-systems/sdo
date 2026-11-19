package runtime

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// acceptingPlanClient is the typed fake client with a metadata-only acceptance
// writer: the fake clientset has no REST transport for the live writer.
type acceptingPlanClient struct {
	TypedPlanClient
	records *[]string
}

func (c acceptingPlanClient) RecordAcceptance(ctx context.Context, ref sdk.ObjectRef, record string, _ *unstructured.Unstructured) error {
	*c.records = append(*c.records, ref.Kind+"/"+ref.Name)
	live, err := c.Client.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if live.Annotations == nil {
		live.Annotations = map[string]string{}
	}
	live.Annotations[acceptedPathsAnnotation] = record
	_, err = c.Client.CoreV1().ConfigMaps(ref.Namespace).Update(ctx, live, metav1.UpdateOptions{})
	return err
}

// absentRestoreWorld is a cluster in which accepted source declares one
// ConfigMap and the live object is gone (deleted outside SDO).
func absentRestoreWorld(t *testing.T) (*fake.Clientset, PlanExecutor, IncidentRequest, sdk.ObjectRef) {
	t.Helper()
	accepted := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "init-script"}, Data: map[string]string{"init.sh": "echo synthetic"}}
	manifest, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(accepted)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	// The API server assigns identity on create; the fake does not by itself.
	client.PrependReactor("create", "configmaps", func(action k8stesting.Action) (bool, kruntime.Object, error) {
		object := action.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		object.UID, object.ResourceVersion = types.UID("uid-created"), "1"
		return false, nil, nil
	})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "init-script"}
	executor := PlanExecutor{Client: acceptingPlanClient{TypedPlanClient: TypedPlanClient{Client: client}, records: new([]string)}, Source: templateSource{&unstructured.Unstructured{Object: manifest}},
		Rollouts: recordedRollouts{new([]sdk.ObjectRef)}, Verifier: passingVerifier{}, Now: func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }}
	incident := IncidentRequest{IncidentID: "synthetic", Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "0123456789abcdef0123456789abcdef01234567",
		Findings: []sdk.Finding{{RuleID: "synthetic", PrimaryResource: ref}}}
	return client, executor, incident, ref
}

// Restoring a source-declared object that is gone means creating it from
// accepted source. The pre-image is recorded as absent, so the plan still has a
// defined "before": rollback deletes exactly the created incarnation, and
// settlement compares the final object with the one the plan created.
func TestReconcileFromSourceCreatesASourceDeclaredObjectThatIsAbsent(t *testing.T) {
	for _, action := range []program.Action{
		program.ReconcileFromSource{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "init-script"}},
		&program.ReconcileFromSource{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "init-script"}},
	} {
		client, executor, incident, ref := absentRestoreWorld(t)
		result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{action}}, false)
		if err != nil || !result.Applied {
			t.Fatalf("%T: Apply = %+v, %v", action, result, err)
		}
		live, err := client.CoreV1().ConfigMaps("app").Get(context.Background(), "init-script", metav1.GetOptions{})
		if err != nil || live.Data["init.sh"] != "echo synthetic" {
			t.Fatalf("the object was not created from accepted source: %+v, %v", live, err)
		}
		if len(result.PreImages) != 1 || !result.PreImages[0].Absent || result.PreImages[0].Object != nil {
			t.Fatalf("pre-image must be recorded as absent: %+v", result.PreImages)
		}
		if len(result.ObjectEffects) != 1 || result.ObjectEffects[0].Operation != "create" || !result.ObjectEffects[0].Confirmed || result.ObjectEffects[0].UID != "uid-created" {
			t.Fatalf("effect must be a confirmed, UID-bound create: %+v", result.ObjectEffects)
		}
		if len(result.ActionReports) != 1 || result.ActionReports[0].Status != "applied" {
			t.Fatalf("action reports %+v", result.ActionReports)
		}
		if create, ok := result.Plan.Actions[0].(program.CreateFromSource); !ok || create.Ref != ref || create.Reason == "" {
			t.Fatalf("the recorded plan must name what ran: %#v", result.Plan.Actions[0])
		}
		// Settlement checks the effect against the created object.
		loop := &ResponderLoop{}
		getFinal := func(r sdk.ObjectRef) *unstructured.Unstructured {
			object, _ := executor.Client.Get(context.Background(), r)
			return object
		}
		if !loop.recoveryPlanFieldsMatch(result, getFinal, func(sdk.ObjectRef) bool { return false }) {
			t.Fatal("settlement must confirm the created object took effect")
		}
		// Compensation removes exactly what the plan created.
		if err := executor.Rollback(context.Background(), result); err != nil {
			t.Fatalf("rollback: %v", err)
		}
		if _, err := client.CoreV1().ConfigMaps("app").Get(context.Background(), "init-script", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("rollback must restore the absent pre-image, got %v", err)
		}
	}
}

// A present object is still reconciled in place, and a dry run of an absent
// one previews the create without writing.
func TestReconcileFromSourceOfAPresentObjectStaysAFieldRestore(t *testing.T) {
	client, executor, incident, ref := absentRestoreWorld(t)
	preview, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, true)
	if err != nil || preview.Applied || len(preview.PreImages) != 1 || !preview.PreImages[0].Absent {
		t.Fatalf("dry run = %+v, %v", preview, err)
	}
	if _, err := client.CoreV1().ConfigMaps("app").Get(context.Background(), "init-script", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a dry run wrote: %v", err)
	}
	drifted := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "init-script", UID: "uid-live", ResourceVersion: "7"}, Data: map[string]string{"init.sh": "echo drifted"}}
	if _, err := client.CoreV1().ConfigMaps("app").Create(context.Background(), drifted, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("Apply = %+v, %v", result, err)
	}
	if _, ok := result.Plan.Actions[0].(program.ReconcileFromSource); !ok || len(result.ObjectEffects) != 0 {
		t.Fatalf("a present object must be patched, not created: %#v %+v", result.Plan.Actions[0], result.ObjectEffects)
	}
	live, _ := client.CoreV1().ConfigMaps("app").Get(context.Background(), "init-script", metav1.GetOptions{})
	if live.Data["init.sh"] != "echo synthetic" {
		t.Fatalf("data %v was not restored", live.Data)
	}
}
