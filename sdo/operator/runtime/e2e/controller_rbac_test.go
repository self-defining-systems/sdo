//go:build integration

package e2e

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/install"
	"sdo.dev/operator/runtime/envtestio"
)

// The grants a real install applies (the installer's own grant step, which
// Install, install-existing and inspect-existing share) let the controller's
// ServiceAccount keep the follower's ConfigMaps: the holds, the obligations and
// the operability report it adds the obligations summary to. Checked against a
// real API server's RBAC authorizer as that identity, not by reading rules.
func TestControllerServiceAccountMayKeepItsConfigMaps(t *testing.T) {
	ctx := context.Background()
	env := envtestio.Start(t)
	const app, control = "app", "sdo-control"
	env.CreateNamespace(t, app)
	env.CreateNamespace(t, control)
	cfg := install.Config{
		Namespace: app, ControlNamespace: control, Application: "app", RepositoryPVC: "repo", CredentialsSecret: "creds",
		ControllerImage: "controller:test", ResponderImage: "responder:test", Model: "m", Replay: "on",
		NativeCommand: []string{"/usr/local/bin/sdo-operator"}, AcceptedCommit: strings.Repeat("c", 40), TimeoutSeconds: 60, VerificationTimeoutSeconds: 60,
		BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "push",
	}
	if err := (install.Installer{Client: env.Client(t)}).EnsureGrants(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	impersonated := rest.CopyConfig(env.Config)
	impersonated.Impersonate = rest.ImpersonationConfig{UserName: "system:serviceaccount:" + control + ":sdo-controller"}
	controller, err := kubernetes.NewForConfig(impersonated)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{install.OperabilityConfigMap, "sdo-upstream-obligations", "sdo-upstream-holds"} {
		configMaps := controller.CoreV1().ConfigMaps(control)
		created, err := configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"k": "v"}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		if _, err := configMaps.Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		created.Data["k"] = "w"
		if _, err := configMaps.Update(ctx, created, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update %s: %v", name, err)
		}
	}
	if err := install.PublishOperabilityObligations(ctx, controller, control, "[]"); err != nil {
		t.Fatal(err)
	}
	got, err := env.Client(t).CoreV1().ConfigMaps(control).Get(ctx, install.OperabilityConfigMap, metav1.GetOptions{})
	if err != nil || got.Data[install.OperabilityObligationsKey] != "[]" {
		t.Fatalf("obligations summary not written by the controller identity: %v %v", got.Data, err)
	}
}
