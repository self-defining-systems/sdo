package install

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
)

func TestObservationNarrowingPreservesPermissiveGrants(t *testing.T) {
	cfg := testConfig()
	cfg.PermissiveResponder = true
	cfg.ResponderNamespaces = []string{"policy-system"}
	permissive, err := Resources(cfg, secretPathDiscovery()...)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ObservationCeiling = sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{
		{APIVersion: "v1", Kind: "Service", Namespace: "policy-system"},
		{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"},
	}}
	combined, err := Resources(cfg, secretPathDiscovery()...)
	if err != nil {
		t.Fatal(err)
	}
	if len(combined.ObservationRoles) != 1 || len(combined.ObservationClusterRoles) != 1 {
		t.Fatal("combined plan lost observation grants")
	}
	if len(combined.ObservationClusterBindings) != 2 {
		t.Fatal("observation lifecycle must retain controller and script bindings")
	}
	scriptBinding := combined.ObservationClusterBindings[1]
	if scriptBinding.RoleRef.Name != combined.ObservationClusterRoles[0].Name || len(scriptBinding.Subjects) != 1 || scriptBinding.Subjects[0].Name != "sdo-script" || scriptBinding.Subjects[0].Namespace != cfg.Namespace {
		t.Fatal("script lost its inherited observation permission")
	}
	if !reflect.DeepEqual(combined.AppRoles, permissive.AppRoles) || !reflect.DeepEqual(combined.AppBindings, permissive.AppBindings) || !reflect.DeepEqual(combined.ClusterRoles, permissive.ClusterRoles) || !reflect.DeepEqual(combined.ClusterBindings, permissive.ClusterBindings) {
		t.Fatal("observation policy changed permissive grants")
	}
	objects := []kruntime.Object{}
	for _, r := range combined.AppRoles {
		objects = append(objects, r)
	}
	for _, b := range combined.AppBindings {
		objects = append(objects, b)
	}
	for _, r := range combined.ClusterRoles {
		objects = append(objects, r)
	}
	for _, b := range combined.ClusterBindings {
		objects = append(objects, b)
	}
	for _, r := range combined.ObservationRoles {
		objects = append(objects, r)
	}
	for _, b := range combined.ObservationBindings {
		objects = append(objects, b)
	}
	for _, r := range combined.ObservationClusterRoles {
		objects = append(objects, r)
	}
	for _, b := range combined.ObservationClusterBindings {
		objects = append(objects, b)
	}
	client := fake.NewSimpleClientset(objects...)
	if err := (Installer{Client: client}).reconcileObservationGrants(context.Background(), cfg, permissive); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, r := range permissive.AppRoles {
		if _, err := client.RbacV1().Roles(r.Namespace).Get(ctx, r.Name, metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range permissive.AppBindings {
		if _, err := client.RbacV1().RoleBindings(b.Namespace).Get(ctx, b.Name, metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range permissive.ClusterRoles {
		if _, err := client.RbacV1().ClusterRoles().Get(ctx, r.Name, metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range permissive.ClusterBindings {
		if _, err := client.RbacV1().ClusterRoleBindings().Get(ctx, b.Name, metav1.GetOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	roles, _ := client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if len(roles.Items) != len(permissive.ClusterRoles) {
		t.Fatal("observation cluster grant survived narrowing")
	}
	bindings, err := client.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil || len(bindings.Items) != len(permissive.ClusterBindings) {
		t.Fatal("observation binding survived narrowing")
	}
}
