//go:build integration && controller_e2e

package e2e

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterStarts(t *testing.T) {
	cluster := StartCluster(t, t.TempDir())
	list, err := cluster.Client.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.Items) == 0 {
		t.Fatalf("namespaces: %v %v", list, err)
	}
}
