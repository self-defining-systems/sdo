//go:build integration

package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func fakeAdoptionController(t *testing.T, fakeClient *fake.Clientset, sources adoptionSources) (*Controller, acceptedSourceBridge) {
	t.Helper()
	client := bridgeFake(t, fakeClient)
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", "accepted"
	controller, err := NewController(config, nil, fastloopObjects{client: fakeClient}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: sources, Client: client, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{Carrier: inputs.New(snapshot), Source: sources, SourceCommit: "accepted", Client: client}
	}})
	return controller, client
}

// One object's failed record write must not leave the objects after it
// unadopted, and its retries back off instead of running every pass.
func TestAdoptionIsolatesAcceptanceRecordFailure(t *testing.T) {
	ctx := context.Background()
	fakeClient := fake.NewSimpleClientset(settledDeployment("alpha"), settledDeployment("beta"))
	failing := true
	fakeClient.PrependReactor("patch", "deployments", func(action ktesting.Action) (bool, kruntime.Object, error) {
		patch := action.(ktesting.PatchAction)
		if failing && patch.GetName() == "alpha" && strings.Contains(string(patch.GetPatch()), acceptedPathsAnnotation) {
			return true, nil, errors.New("metadata.annotations: Too long")
		}
		return false, nil, nil
	})
	sources := adoptionSources{deploymentSource("alpha"), deploymentSource("beta")}
	controller, client := fakeAdoptionController(t, fakeClient, sources)
	alpha := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "alpha"}
	beta := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "beta"}
	controller.adoptionNeeded = true

	now := time.Unix(11, 0).UTC()
	err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"})
	var recordErr *AcceptanceRecordError
	if !errors.As(err, &recordErr) || recordErr.Ref != alpha {
		t.Fatalf("record failure not reported as a typed per-object error: %v", err)
	}
	controller.recordAdoptionError(now, err)
	if !recordedOn(t, client, beta, sources[1]) {
		t.Fatal("record failure on alpha left beta unrecorded")
	}
	first := controller.ownershipAdoption.RetryAt.Sub(now)
	if !controller.adoptionNeeded || first <= 0 {
		t.Fatalf("failed record not retried: %+v", controller.ownershipAdoption)
	}
	now = controller.ownershipAdoption.RetryAt
	err = controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"})
	controller.recordAdoptionError(now, err)
	if second := controller.ownershipAdoption.RetryAt.Sub(now); second <= first || second > time.Minute {
		t.Fatalf("record retries do not back off within a bound: %v then %v", first, second)
	}

	failing = false
	now = controller.ownershipAdoption.RetryAt
	if err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if !recordedOn(t, client, alpha, sources[0]) || controller.adoptionNeeded || controller.ownershipAdoption.LastError != "" {
		t.Fatalf("recovered record write not completed: %+v", controller.ownershipAdoption)
	}
}

// One object the API server refuses to adopt (live 52/006 and 52/009: a
// ServiceAccount dry run answered "unknown") must not leave every other
// object of every kind without an acceptance record, or each of their
// source comparisons fails closed.
func TestAdoptionIsolatesPerObjectAdoptionFailure(t *testing.T) {
	ctx := context.Background()
	automount := false
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "grafana", Namespace: "app", UID: "uid-grafana", ResourceVersion: "1"}, AutomountServiceAccountToken: &automount}
	fakeClient := fake.NewSimpleClientset(account, settledDeployment("beta"))
	failing := true
	fakeClient.PrependReactor("patch", "serviceaccounts", func(ktesting.Action) (bool, kruntime.Object, error) {
		if failing {
			return true, nil, errors.New("unknown")
		}
		return false, nil, nil
	})
	accountSource := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "grafana", "namespace": "app"}, "automountServiceAccountToken": false}}
	sources := adoptionSources{accountSource, deploymentSource("beta")}
	controller, client := fakeAdoptionController(t, fakeClient, sources)
	grafana := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "grafana"}
	beta := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "beta"}
	controller.adoptionNeeded = true

	now := time.Unix(11, 0).UTC()
	err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"})
	var objectErr *ObjectAdoptionError
	if !errors.As(err, &objectErr) || objectErr.Ref != grafana {
		t.Fatalf("adoption failure not reported as a typed per-object error: %v", err)
	}
	controller.recordAdoptionError(now, err)
	if !recordedOn(t, client, beta, sources[1]) {
		t.Fatal("adoption failure on the ServiceAccount left the Deployment unrecorded")
	}
	resource, resolveErr := resolveAcceptedResource(client, beta)
	if resolveErr != nil {
		t.Fatal(resolveErr)
	}
	live, getErr := resource.get(ctx, beta)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !acceptanceRecordBound(live, sources[1]) {
		t.Fatalf("Deployment record not bound to its object: %v", live.GetAnnotations())
	}
	if !controller.adoptionNeeded || controller.ownershipAdoption.RetryAt.Sub(now) <= 0 {
		t.Fatalf("failed object not retried: %+v", controller.ownershipAdoption)
	}

	failing = false
	now = controller.ownershipAdoption.RetryAt
	if err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if !recordedOn(t, client, grafana, accountSource) || controller.adoptionNeeded || controller.ownershipAdoption.LastError != "" {
		t.Fatalf("recovered object not adopted: %+v", controller.ownershipAdoption)
	}
}

// Roles and bindings are never patched by adoption: the API server demands
// escalate or bind for them, which the controller does not hold. They stay
// inventory-only with a visible reason, and the pass neither fails nor leaves
// other objects without a record.
func TestAdoptionLeavesRBACKindsToInventoryWithVisibleReason(t *testing.T) {
	ctx := context.Background()
	fakeClient := fake.NewSimpleClientset(settledDeployment("beta"))
	patched := false
	fakeClient.PrependReactor("patch", "*", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.GetResource().Resource == "roles" || action.GetResource().Resource == "rolebindings" {
			patched = true
		}
		return false, nil, nil
	})
	role := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": map[string]any{"name": "reader", "namespace": "app"}, "rules": []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"pods"}, "verbs": []any{"get"}}}}}
	sources := adoptionSources{role, deploymentSource("beta")}
	controller, client := fakeAdoptionController(t, fakeClient, sources)
	beta := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "beta"}
	controller.adoptionNeeded = true

	if err := controller.adoptOwnership(ctx, time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatalf("an RBAC declaration failed the adoption pass: %v", err)
	}
	if patched {
		t.Fatal("adoption patched an RBAC object")
	}
	if !recordedOn(t, client, beta, sources[1]) {
		t.Fatal("Deployment left unrecorded")
	}
	status := controller.ownershipAdoption
	if len(status.UnadoptedObjects) != 1 || !strings.HasPrefix(status.UnadoptedObjects[0], "rbac.authorization.k8s.io/v1/Role/app/reader: ") || status.LastError != "" {
		t.Fatalf("RBAC exclusion not visible: %+v", status)
	}
}

// A declared bare Pod the scheduler cannot place stays unsettled
// indefinitely. Adoption keeps waiting for it, but with growing spacing.
func TestAdoptionBacksOffWhileObjectStaysUnsettled(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "app", UID: "uid-worker", ResourceVersion: "1"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "worker:1"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}},
	}
	source := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "worker", "namespace": "app"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "worker", "image": "worker:1"}}}}}
	controller, client := fakeAdoptionController(t, fake.NewSimpleClientset(pod), adoptionSources{source})
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "worker"}
	controller.adoptionNeeded = true

	now := time.Unix(11, 0).UTC()
	previous := time.Duration(0)
	for pass := 0; pass < 6; pass++ {
		if err := controller.adoptOwnership(ctx, now, sdktest.Snapshot{NamespaceName: "app"}); err != nil {
			t.Fatal(err)
		}
		if recordedOn(t, client, ref, source) || !controller.adoptionNeeded {
			t.Fatal("unscheduled Pod recorded before it settled")
		}
		wait := controller.ownershipAdoption.RetryAt.Sub(now)
		if wait < previous || wait > acceptanceSettleRetryMax {
			t.Fatalf("pass %d waits %v after %v (bound %v)", pass, wait, previous, acceptanceSettleRetryMax)
		}
		previous = wait
		now = controller.ownershipAdoption.RetryAt
	}
	if previous < time.Minute {
		t.Fatalf("unsettled waits stayed short: %v", previous)
	}
}

