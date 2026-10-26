package install

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
)

func TestObservationInstallNarrowReinstall(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	cfg.ReuseExisting = true
	cfg.ObservationCeiling = sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{
		{APIVersion: "v1", Kind: "Service", Namespace: "foreign-a"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "foreign-a"},
		{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"},
	}}
	client := fake.NewSimpleClientset(&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "foreign-a"}})
	installer := Installer{Client: client}
	install := func() Plan {
		t.Helper()
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// Reuse an already running matching job to exercise the public installer
		// without pod execution, repository seeding or real credentials.
		_ = client.BatchV1().Jobs(cfg.controlNamespace()).Delete(ctx, ControllerJobName, metav1.DeleteOptions{})
		job := plan.Job.DeepCopy()
		job.Status = batchv1.JobStatus{Active: 1}
		if _, err = client.BatchV1().Jobs(cfg.controlNamespace()).Create(ctx, job, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err = client.CoreV1().Secrets(cfg.controlNamespace()).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.controlNamespace()}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
		if _, err = installer.Install(ctx, cfg); err != nil {
			t.Fatal(err)
		}
		return plan
	}
	// Retired index names also need revocation when upgrading an existing install.
	legacyRole := role("foreign-old", "sdo-observe-"+cfg.Namespace+"-7", []rbacv1.PolicyRule{rule("", []string{"services"}, "get", "list")})
	legacyBinding := binding("foreign-old", legacyRole.Name, "sdo-controller", cfg.controlNamespace())
	if _, err := client.RbacV1().Roles(legacyRole.Namespace).Create(ctx, legacyRole, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.RbacV1().RoleBindings(legacyBinding.Namespace).Create(ctx, legacyBinding, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	old := install()
	if _, err := client.RbacV1().Roles(legacyRole.Namespace).Get(ctx, legacyRole.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("legacy owned grant retained")
	}

	// Reordering targets must preserve object identities rather than renaming
	// grants by position; a namespace move must remove the previous grant.
	cfg.ObservationCeiling.Targets[0], cfg.ObservationCeiling.Targets[1] = cfg.ObservationCeiling.Targets[1], cfg.ObservationCeiling.Targets[0]
	reordered, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]string{}
	for _, role := range old.ObservationRoles {
		names[role.Rules[0].Resources[0]] = role.Name
	}
	for _, role := range reordered.ObservationRoles {
		if names[role.Rules[0].Resources[0]] != role.Name {
			t.Error("observation grant identity depends on target index")
		}
	}
	cfg.ControlNamespace = "replacement-control"
	cfg.ObservationCeiling.Targets = []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "foreign-b"}}
	current := install()
	roles, _ := client.RbacV1().Roles("").List(ctx, metav1.ListOptions{})
	bindings, _ := client.RbacV1().RoleBindings("").List(ctx, metav1.ListOptions{})
	clusterRoles, _ := client.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	clusterBindings, _ := client.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	for _, role := range roles.Items {
		if role.Namespace == "foreign-a" && role.Name != "unrelated" {
			t.Errorf("revoked Role retained: %s", role.Name)
		}
	}
	for _, binding := range bindings.Items {
		if binding.Namespace == "foreign-a" {
			t.Errorf("revoked RoleBinding retained: %s", binding.Name)
		}
	}
	for _, role := range clusterRoles.Items {
		if !strings.Contains(role.Name, "-object-capture") {
			t.Errorf("revoked cluster observation authority retained: %s", role.Name)
		}
	}
	for _, binding := range clusterBindings.Items {
		if !strings.Contains(binding.Name, "-object-capture") {
			t.Errorf("revoked cluster observation binding retained: %s", binding.Name)
		}
	}
	for _, binding := range current.ObservationBindings {
		got, err := client.RbacV1().RoleBindings(binding.Namespace).Get(ctx, binding.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, subject := range got.Subjects {
			if subject.Namespace != cfg.controlNamespace() {
				t.Error("old service account retained")
			}
		}
	}
	cfg.ObservationCeiling = sdk.ObservationScope{}
	install()
	install() // Idempotent narrowing leaves unrelated grants intact.
	roles, _ = client.RbacV1().Roles("").List(ctx, metav1.ListOptions{})
	for _, role := range roles.Items {
		if role.Namespace == "foreign-b" {
			t.Errorf("empty ceiling retained Role: %s", role.Name)
		}
	}
	bindings, _ = client.RbacV1().RoleBindings("foreign-b").List(ctx, metav1.ListOptions{})
	if len(bindings.Items) != 0 {
		t.Error("empty ceiling retained RoleBinding")
	}
	if _, err := client.RbacV1().Roles("foreign-a").Get(ctx, "unrelated", metav1.GetOptions{}); err != nil {
		t.Fatal("unrelated role deleted")
	}
}
