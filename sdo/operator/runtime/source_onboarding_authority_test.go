package runtime

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestSourceInventoryOnboardingDoesNotHideDeclaredChecks(t *testing.T) {
	config := inventoryObject("v1", "ConfigMap", "app", "source-config")
	role := inventoryObject("rbac.authorization.k8s.io/v1", "Role", "app", "source-role")
	role.Object["rules"] = []any{map[string]any{"verbs": []any{"get"}, "resources": []any{"configmaps"}, "apiGroups": []any{""}}}
	deployment := inventoryObject("apps/v1", "Deployment", "app", "source-app")
	_ = unstructured.SetNestedSlice(deployment.Object, []any{map[string]any{"name": "script", "configMap": map[string]any{"name": "source-script"}}}, "spec", "template", "spec", "volumes")
	index := NewSourceInventory("app", strings.Repeat("a", 40), []*unstructured.Unstructured{config, role, deployment})
	record := &contracts.NamespaceOnboarding{Namespace: "app", AcceptedCommit: strings.Repeat("a", 40), EstablishedAt: timestamppb.New(time.Unix(100, 0)), Objects: []*contracts.ObjectRef{{ApiVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "installer"}}}
	if err := index.SetNamespaceOnboarding(record); err != nil {
		t.Fatal(err)
	}
	changedConfig, changedRole := config.DeepCopy(), role.DeepCopy()
	script := inventoryObject("v1", "ConfigMap", "app", "source-script")
	for _, o := range []*unstructured.Unstructured{changedConfig, changedRole, script} {
		o.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "installer"}})
	}
	deleting := metav1.NewTime(time.Unix(200, 0))
	changedConfig.SetDeletionTimestamp(&deleting)
	script.SetDeletionTimestamp(&deleting)
	changedRole.Object["rules"] = []any{map[string]any{"verbs": []any{"delete"}, "resources": []any{"configmaps"}, "apiGroups": []any{""}}}
	if findings := index.Findings([]*unstructured.Unstructured{deployment, changedConfig, changedRole, script}, time.Unix(200, 0)); len(findings) != 3 {
		t.Fatalf("acknowledged ancestor hid explicit source/dependency obligations: %+v", findings)
	}
}
