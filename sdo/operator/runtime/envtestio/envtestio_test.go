//go:build integration

package envtestio_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/runtime/envtestio"
)

// TestStartServesTheAPIAndAKubeconfig starts a control plane and reads it back
// through both the client configuration and the kubeconfig file.
func TestStartServesTheAPIAndAKubeconfig(t *testing.T) {
	env := envtestio.Start(t)
	env.CreateNamespace(t, "probe")
	list, err := env.Client(t).CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ns := range list.Items {
		found = found || ns.Name == "probe"
	}
	if !found {
		t.Fatalf("namespace probe missing from %d namespaces", len(list.Items))
	}
	if env.Kubeconfig == "" {
		t.Fatal("no kubeconfig path")
	}
}

// TestStartWithTokenAndServiceRange starts the control plane the way the
// end-to-end runner does: a static admin token mounted into Pods and a
// non-default service range. The client must authenticate with the token alone.
func TestStartWithTokenAndServiceRange(t *testing.T) {
	env := envtestio.StartWith(t, envtestio.Options{
		AdminToken:               "synthetic-admin-token",
		AuthorizationMode:        "Node,RBAC",
		DisabledAdmissionPlugins: []string{"LimitRanger"},
		APIServerFlags:           map[string]string{"service-cluster-ip-range": "10.96.0.0/16"},
	})
	if env.Config.BearerToken != "synthetic-admin-token" || env.Config.CertData != nil {
		t.Fatalf("client must use the token alone: %+v", env.Config.TLSClientConfig)
	}
	if env.Port == 0 || len(env.CA) == 0 || !strings.Contains(env.Config.Host, ":"+strconv.Itoa(env.Port)) {
		t.Fatalf("port %d, %d CA bytes, host %s", env.Port, len(env.CA), env.Config.Host)
	}
	raw, err := os.ReadFile(env.Kubeconfig)
	if err != nil || !strings.Contains(string(raw), "synthetic-admin-token") {
		t.Fatalf("kubeconfig lacks the token: %v", err)
	}
	service, err := env.Client(t).CoreV1().Services("default").Get(context.Background(), "kubernetes", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(service.Spec.ClusterIP, "10.96.") {
		t.Fatalf("service range flag ignored: kubernetes ClusterIP %s", service.Spec.ClusterIP)
	}
}
