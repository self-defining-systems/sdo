package kubeclient_test

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestReadClientMatchesPinnedKubernetesAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	check := exec.CommandContext(ctx, "bash", "scripts/gen_kube_read_clients.sh", "--check")
	check.Dir = "../../../.."
	if out, err := check.CombinedOutput(); err != nil {
		t.Fatalf("read-only Kubernetes API drift: %v\n%s", err, out)
	}
}
