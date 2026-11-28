//go:build integration

package memory

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestCaptureSourceProposalsCommitsTheSourceEdit(t *testing.T) {
	root := t.TempDir()
	acceptedGit(t, root, "init", "-q")
	acceptedGit(t, root, "config", "user.email", "test@example.invalid")
	acceptedGit(t, root, "config", "user.name", "test")
	for name, text := range map[string]string{
		"k8s/web.yaml": proposalManifest,
		".sdo/arch.md": "# Manifest inventory\n\n- Deployment/shop/web (source: k8s/web.yaml)\n- Deployment/shop/chart (source: helm)\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	acceptedGit(t, root, "add", "-A")
	acceptedGit(t, root, "commit", "-q", "-m", "accepted")
	reason := "web saturates at one replica under the observed load; keep the replicas the playbook scaled to"
	created, skipped, err := CaptureSourceProposals(context.Background(), root, "inc-1", []program.SourceChangeProposal{
		{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "web"}, Path: "spec.replicas", Value: float64(3), Reason: reason},
		{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "chart"}, Path: "spec.replicas", Value: float64(3), Reason: reason},
	})
	if err != nil || len(created) != 1 || len(skipped) != 1 {
		t.Fatalf("created %v skipped %v: %v", created, skipped, err)
	}
	if created[0] != "k8s/web.yaml" {
		t.Fatalf("created = %v", created)
	}
	if status := gitOut(t, root, "status", "--porcelain"); strings.TrimSpace(status) != "" {
		t.Fatalf("the edit must be committed, not left as scratch: %q", status)
	}
	committed := gitOut(t, root, "show", "HEAD:k8s/web.yaml")
	if !strings.Contains(committed, "replicas: 3") {
		t.Fatalf("committed file lacks the proposal:\n%s", committed)
	}
	if message := gitOut(t, root, "log", "-1", "--format=%B"); !strings.Contains(message, "inc-1") || !strings.Contains(message, reason) {
		t.Fatalf("commit message = %q", message)
	}
	again, skippedAgain, err := CaptureSourceProposals(context.Background(), root, "inc-1", []program.SourceChangeProposal{
		{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "web"}, Path: "spec.replicas", Value: float64(3), Reason: reason},
	})
	if err != nil || len(again) != 0 || len(skippedAgain) != 1 {
		t.Fatalf("an applied proposal commits nothing again: %v %v %v", again, skippedAgain, err)
	}
}

func gitOut(t *testing.T, root string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}
