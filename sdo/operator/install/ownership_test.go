package install

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func assertInstallOwnership(t *testing.T, object metav1.Object, namespace string) {
	t.Helper()
	if object.GetLabels()["app.kubernetes.io/managed-by"] != "sdo" || object.GetLabels()[ApplicationNamespaceLabel] != namespace {
		t.Fatalf("installed object %s/%s lacks scoped SDO ownership: %v", object.GetNamespace(), object.GetName(), object.GetLabels())
	}
}

func TestInstallPlanCarriesOwnershipOnEveryObject(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	resources := []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "services", Kind: "Service", Namespaced: true, Verbs: []string{"get", "list", "watch"}}}}}
	plan, err := Resources(cfg, resources...)
	if err != nil {
		t.Fatal(err)
	}
	// Inspect all resource-bearing plan fields so a future resource cannot silently
	// bypass the same producer/consumer ownership contract.
	value := reflect.ValueOf(plan)
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		if field.Kind() == reflect.Slice {
			for j := 0; j < field.Len(); j++ {
				assertInstallOwnership(t, field.Index(j).Interface().(metav1.Object), cfg.Namespace)
			}
		} else if !field.IsNil() {
			assertInstallOwnership(t, field.Interface().(metav1.Object), cfg.Namespace)
		}
	}
	assertInstallOwnership(t, &plan.Job.Spec.Template.ObjectMeta, cfg.Namespace)
	assertInstallOwnership(t, plan.SyncPod, cfg.Namespace)
}

func TestInstallReconcileAddsOwnershipToLegacyCaptureGrants(t *testing.T) {
	cfg := testConfig()
	cfg.Credentials = map[string][]byte{"fixture": []byte("synthetic")}
	resources := []*metav1.APIResourceList{{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "services", Kind: "Service", Namespaced: true, Verbs: []string{"get", "list"}}}}}
	plan, err := Resources(cfg, resources...)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	oldPVC := plan.PVC.DeepCopy()
	oldPVC.Labels = map[string]string{"user": "preserve"}
	if _, err := client.CoreV1().PersistentVolumeClaims(oldPVC.Namespace).Create(context.Background(), oldPVC, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, sa := range plan.ServiceAccounts {
		old := sa.DeepCopy()
		old.Labels = map[string]string{"user": "preserve"}
		if _, err := client.CoreV1().ServiceAccounts(sa.Namespace).Create(context.Background(), old, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, role := range plan.ClusterRoles {
		old := role.DeepCopy()
		old.Labels = nil
		if _, err := client.RbacV1().ClusterRoles().Create(context.Background(), old, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, binding := range plan.ClusterBindings {
		old := binding.DeepCopy()
		old.Labels = nil
		if _, err := client.RbacV1().ClusterRoleBindings().Create(context.Background(), old, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := (Installer{Client: client}).ensureBase(context.Background(), cfg, plan); err != nil {
		t.Fatal(err)
	}
	gotPVC, err := client.CoreV1().PersistentVolumeClaims(plan.PVC.Namespace).Get(context.Background(), plan.PVC.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertInstallOwnership(t, gotPVC, cfg.Namespace)
	if gotPVC.Labels["user"] != "preserve" {
		t.Fatal("metadata migration lost user labels")
	}
	for _, sa := range plan.ServiceAccounts {
		got, err := client.CoreV1().ServiceAccounts(sa.Namespace).Get(context.Background(), sa.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		assertInstallOwnership(t, got, cfg.Namespace)
		if got.Labels["user"] != "preserve" {
			t.Fatal("metadata migration lost user labels")
		}
	}
	for _, want := range plan.ClusterRoles {
		got, err := client.RbacV1().ClusterRoles().Get(context.Background(), want.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		assertInstallOwnership(t, got, cfg.Namespace)
	}
	for _, want := range plan.ClusterBindings {
		got, err := client.RbacV1().ClusterRoleBindings().Get(context.Background(), want.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		assertInstallOwnership(t, got, cfg.Namespace)
	}
}
