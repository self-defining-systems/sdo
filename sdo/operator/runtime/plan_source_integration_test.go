//go:build integration

package runtime

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func syntheticPlanRepo(t *testing.T, plan string) string {
	t.Helper()
	dir := t.TempDir()
	if plan != "" {
		writePlanFile(t, dir, plan)
	}
	for _, args := range [][]string{
		{"init", "-q"},
		{"add", "-A"},
		{"-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "seed"},
	} {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return dir
}

func evaluateOnceAt(t *testing.T, root string) error {
	t.Helper()
	var stdout, stderr bytes.Buffer
	return RunWithOptions(context.Background(), nil, RuntimeOptions{
		Args:     []string{"--evaluate-once", "--namespace", "shop", "--app-root", root},
		Provider: staticProvider{snapshot: syntheticShopSnapshot()},
		Client:   fake.NewSimpleClientset(),
		Stdout:   &stdout, Stderr: &stderr,
	})
}

func TestEvaluateOnceReadsTheDeploymentSourceFromThePlan(t *testing.T) {
	if err := evaluateOnceAt(t, syntheticPlanRepo(t, syntheticPlan)); err != nil {
		t.Fatalf("repository with a plan: %v", err)
	}
	if err := evaluateOnceAt(t, syntheticPlanRepo(t, `{"images":[]}`+"\n")); err == nil || !strings.Contains(err.Error(), "plan.yaml") {
		t.Fatalf("plan without a source: err = %v", err)
	}
	for _, flag := range []string{"--deployment-source", "--deployed-manifest-path"} {
		var stderr bytes.Buffer
		err := RunWithOptions(context.Background(), nil, RuntimeOptions{
			Args:     []string{"--evaluate-once", "--namespace", "shop", "--app-root", syntheticPlanRepo(t, syntheticPlan), flag, "x"},
			Provider: staticProvider{snapshot: syntheticShopSnapshot()}, Client: fake.NewSimpleClientset(), Stdout: &bytes.Buffer{}, Stderr: &stderr,
		})
		if err == nil {
			t.Fatalf("retired flag %s accepted", flag)
		}
	}
}
