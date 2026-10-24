package install

import (
	"context"
	"fmt"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"testing"
	"time"
)

// Permission-only canary: no controller, model, workload, Secret read or fault.
// A fresh subject receives the generated grants; all objects are cleaned up.
func TestPermissiveRealAPIAuthorizationCanary(t *testing.T) {
	path := os.Getenv("SDO_RBAC_CANARY_KUBECONFIG")
	if path == "" {
		t.Skip("requires lane-owned canary kubeconfig")
	}
	namespace := os.Getenv("SDO_RBAC_CANARY_NAMESPACE")
	if namespace == "" {
		t.Fatal("SDO_RBAC_CANARY_NAMESPACE required")
	}
	rest, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(rest)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cfg := testConfig()
	cfg.Namespace = namespace
	cfg.ControlNamespace = namespace + "-sdo"
	cfg.PermissiveResponder = true
	prefix := fmt.Sprintf("s42-rbac-%d-", time.Now().UnixNano())
	fault := prefix + "fault"
	subject := prefix + "responder"
	cfg.ResponderNamespaces = []string{fault}
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: fault}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	defer client.CoreV1().Namespaces().Delete(ctx, fault, metav1.DeleteOptions{})
	if _, err := client.CoreV1().ServiceAccounts(cfg.controlNamespace()).Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: subject}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	defer client.CoreV1().ServiceAccounts(cfg.controlNamespace()).Delete(ctx, subject, metav1.DeleteOptions{})
	p, err := (Installer{Client: client}).resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range p.ClusterRoles {
		r = r.DeepCopy()
		r.Name = prefix + r.Name
		if _, err := client.RbacV1().ClusterRoles().Create(ctx, r, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		defer client.RbacV1().ClusterRoles().Delete(ctx, r.Name, metav1.DeleteOptions{})
	}
	for _, r := range p.AppRoles {
		if r.Name != "sdo-responder" && r.Name != permissiveGrantName(cfg.controlNamespace()) {
			continue
		}
		r = r.DeepCopy()
		r.Name = prefix + r.Name
		if _, err := client.RbacV1().Roles(r.Namespace).Create(ctx, r, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		defer client.RbacV1().Roles(r.Namespace).Delete(ctx, r.Name, metav1.DeleteOptions{})
	}
	for _, b := range p.AppBindings {
		if len(b.Subjects) != 1 || b.Subjects[0].Name != "sdo-responder" {
			continue
		}
		b = b.DeepCopy()
		b.Name = prefix + b.Name
		b.RoleRef.Name = prefix + b.RoleRef.Name
		b.Subjects[0].Name = subject
		if _, err := client.RbacV1().RoleBindings(b.Namespace).Create(ctx, b, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		defer client.RbacV1().RoleBindings(b.Namespace).Delete(ctx, b.Name, metav1.DeleteOptions{})
	}
	for _, b := range p.ClusterBindings {
		b = b.DeepCopy()
		b.Name = prefix + b.Name
		b.RoleRef.Name = prefix + b.RoleRef.Name
		b.Subjects[0].Name = subject
		if _, err := client.RbacV1().ClusterRoleBindings().Create(ctx, b, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		defer client.RbacV1().ClusterRoleBindings().Delete(ctx, b.Name, metav1.DeleteOptions{})
	}
	for _, tc := range []struct {
		ns, group, resource, verb string
		allowed                   bool
	}{
		{fault, "", "configmaps", "get", true}, {fault, "", "configmaps", "patch", true}, {fault, "", "pods/exec", "create", true},
		{namespace, "apps", "deployments", "patch", true}, {"kube-system", "", "configmaps", "get", true}, {"kube-system", "", "configmaps", "patch", false},
		{"", "", "nodes", "list", true}, {"", "", "nodes/proxy", "get", false}, {"", "admissionregistration.k8s.io", "mutatingwebhookconfigurations", "patch", true},
		{cfg.controlNamespace(), "", "configmaps", "get", false}, {cfg.controlNamespace(), "", "configmaps", "patch", false}, {cfg.controlNamespace(), "apps", "deployments", "get", false},
		{fault, "", "secrets", "get", false}, {fault, "", "serviceaccounts/token", "create", false}, {fault, "rbac.authorization.k8s.io", "roles", "create", false},
	} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			sar, err := client.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{User: "system:serviceaccount:" + cfg.controlNamespace() + ":" + subject, Groups: []string{"system:authenticated", "system:serviceaccounts", "system:serviceaccounts:" + cfg.controlNamespace()}, ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: tc.ns, Group: tc.group, Resource: tc.resource, Verb: tc.verb}}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if sar.Status.Allowed == tc.allowed {
				t.Logf("%s/%s/%s/%s allowed=%v", tc.ns, tc.group, tc.resource, tc.verb, tc.allowed)
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("%s/%s/%s/%s allowed=%v want=%v", tc.ns, tc.group, tc.resource, tc.verb, sar.Status.Allowed, tc.allowed)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
