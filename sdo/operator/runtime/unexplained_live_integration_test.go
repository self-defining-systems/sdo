//go:build integration

package runtime

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

// Handwritten from Gate 2 inventory and the resource constructors: injected
// labels, no annotations/ownerRefs, no aggregation, one application SA subject.
// Informers supply typed Kubernetes objects, unlike the old in-memory fixture.
func TestUnexplainedLiveInformerMenu(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metadata := func(name, namespace string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(name + "-uid"), ResourceVersion: "17", Labels: map[string]string{"platform.io/injected": "true"}}
	}
	client := fake.NewSimpleClientset(
		&corev1.ServiceAccount{ObjectMeta: metadata("cleanup-controller", "hotel-reservation")},
		&rbacv1.ClusterRole{ObjectMeta: metadata("configmap-cleanup-controller", ""), Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list", "watch"}}, {APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"get", "patch"}}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metadata("configmap-cleanup-controller", ""), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "configmap-cleanup-controller"}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "cleanup-controller", Namespace: "hotel-reservation"}}},
	)
	inventory, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "hotel-reservation", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	inventory.Start(ctx)
	defer inventory.Stop()
	if err := inventory.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.Namespace = "hotel-reservation"
	controller := &Controller{config: ControllerConfig{Namespace: request.Namespace, SourceCommit: request.SourceCommit}, currentIncidentRequest: &request, sourceInventory: NewSourceInventory(request.Namespace, request.SourceCommit, nil), drift: &DriftControllerConfig{Inventory: true, Events: inventory}}
	_, request.Findings = controller.verifySourceInventoryLocked(nil)
	if len(request.Findings) != 3 {
		t.Fatalf("live-shaped inventory: %+v", request.Findings)
	}
	api := informerRemovalAPI{lifecycleTestClient: &lifecycleTestClient{}, Client: client}
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{Client: api, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.MenuConfig.Unexplained = func(context.Context) (UnexplainedRemovalAssessment, error) {
		return controller.unexplainedRemovalAssessment(request.IncidentID, "hotel-reservation-sdo")
	}
	loop.MenuConfig.RemoveUnexplained = func(ctx context.Context, selected UnexplainedRemoval, client PlanLifecycleClient, ref sdk.ObjectRef, uid types.UID, rv string) error {
		return controller.removeUnexplained(ctx, request.IncidentID, "hotel-reservation-sdo", selected, client, ref, uid, rv)
	}
	menu := loop.Handle(ctx, ToolRequest{Command: "menu"})
	if menu.Error != "" {
		t.Fatal(menu.Error)
	}
	offered := 0
	for _, option := range menu.Menu.GetOptions() {
		if option.ProgramId == "remove-unexplained" {
			offered++
		}
	}
	if offered != 3 {
		t.Fatalf("only %d of three current unexplained objects offered; menu=%+v", offered, menu.Menu)
	}
	for _, kind := range []string{"ClusterRole", "ClusterRoleBinding", "ServiceAccount"} {
		for _, option := range menu.Menu.GetOptions() {
			if option.ProgramId != "remove-unexplained" || option.Bindings[0].GetObject().GetKind() != kind {
				continue
			}
			removed := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option.OptionId, RoundLimit: 1}})
			if removed.Error != "" || removed.Report.GetReasonCode() != "" {
				t.Fatalf("guarded live-shaped removal failed: %+v", removed)
			}
		}
	}
	for {
		controller.mu.Lock()
		healthy, findings := controller.verifySourceInventoryLocked(nil)
		controller.mu.Unlock()
		if healthy {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("guarded removal did not clear inventory closure: %+v", findings)
		case <-time.After(time.Millisecond):
		}
	}
	if len(loop.MenuRepairActions()) != 3 {
		t.Fatal("removal receipts missing")
	}
}
