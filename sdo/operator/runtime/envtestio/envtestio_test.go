//go:build integration

package envtestio_test

import (
	"context"
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
