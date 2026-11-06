//go:build integration

package runtime

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/rendered"
)

func syntheticFlagRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q"},
		{"-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "--allow-empty", "-m", "seed"},
	} {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return dir
}

func evaluateOnceWithSourceFlags(t *testing.T, extra ...string) error {
	t.Helper()
	var stdout, stderr bytes.Buffer
	return RunWithOptions(context.Background(), nil, RuntimeOptions{
		Args:     append([]string{"--evaluate-once", "--namespace", "shop", "--app-root", syntheticFlagRepo(t)}, extra...),
		Provider: staticProvider{snapshot: syntheticShopSnapshot()},
		Client:   fake.NewSimpleClientset(),
		Stdout:   &stdout, Stderr: &stderr,
	})
}

func TestEvaluateOnceDeploymentSourceFlag(t *testing.T) {
	encoded, err := rendered.FormatDeploymentSource(flagTestSource(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := evaluateOnceWithSourceFlags(t, "--deployment-source", encoded); err != nil {
		t.Fatalf("valid source: %v", err)
	}
	if err := evaluateOnceWithSourceFlags(t, "--deployment-source", encoded, "--deployed-manifest-path", "k8s"); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("both flags: err = %v", err)
	}
	if err := evaluateOnceWithSourceFlags(t, "--deployment-source", "not json"); err == nil || !strings.Contains(err.Error(), "invalid deployment source") {
		t.Fatalf("malformed source: err = %v", err)
	}
}
